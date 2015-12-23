-module(minishard_shard).
-behavior(gen_server).

-export([start_link/2, name/1, status/1, info/1]).
-export([set_status/2]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%% Conflict resolution: get score
-export([get_score_or_kill/1]).

name(ClusterName) when is_atom(ClusterName) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_shard").


start_link(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    State = seed_state(ClusterName, CallbackMod),
    gen_server:start_link({local, name(ClusterName)}, ?MODULE, State, []).


%% Set shard status (for use by allocator)
set_status(ShardPid, Status) when is_pid(ShardPid) ->
    gen_server:call(ShardPid, {set_status, Status}).


%% Get shard status
status(ClusterOrShard) when ClusterOrShard /= undefined ->
    {dictionary, Dict} = process_info(local_pid(ClusterOrShard), dictionary),
    proplists:get_value(status, Dict, undefined).

info(ClusterOrShard) when ClusterOrShard /= undefined ->
    {dictionary, Dict} = process_info(local_pid(ClusterOrShard), dictionary),
    case proplists:get_value(status, Dict, undefined) of
        active ->
            {active, #{
                    since => proplists:get_value(active_since, Dict),
                    shard => proplists:get_value(shard, Dict)
                    }};
        Inactive ->
            {Inactive, #{}}
    end.


local_pid(ManagerPid) when is_pid(ManagerPid) ->
    ManagerPid;
local_pid(ClusterName) when is_atom(ClusterName), ClusterName /= undefined ->
    whereis(name(ClusterName)).


-record(shard, {
        cluster_name,
        callback_mod,
        callback_state,
        max_number,
        my_number,
        monitors,
        recheck_timer,
        status
        }).

seed_state(ClusterName, CallbackMod) ->
    #shard{
        cluster_name = ClusterName,
        callback_mod = CallbackMod,
        max_number = CallbackMod:shard_count(ClusterName),
        my_number = undefined,
        monitors = #{},
        status = starting
        }.


init(#shard{} = State) ->
    {ok, export_status(State), 0}.


%% Initial status discovery. Later watcher will notify us about status changes
handle_info(timeout, #shard{status = starting} = State0) ->
    {noreply, join_cluster(State0#shard{status = idle})};

handle_info({timeout, Timer, recheck_ownership}, #shard{recheck_timer = Timer, cluster_name = ClusterName} = State) ->
    % Ensure our allocator feels OK and responds to calls (did not stall)
    _ = minishard_allocator:leader(ClusterName),
    % OK, we did not crash, so allocator is running.
    % Now let's see if we missed deallocation
    handle_ownership_recheck(State#shard{recheck_timer = undefined});

handle_info(Unexpected, #shard{cluster_name = ClusterName} = State) ->
    error_logger:warning_msg("Minishard shard ~w got unexpected message: ~9999p", [ClusterName, Unexpected]),
    {noreply, State}.


handle_call(score, _From, #shard{status = active,
                                 callback_mod = CallbackMod, callback_state = CallbackState} = State) ->
    Score = CallbackMod:score(CallbackState),
    {reply, Score, State};

handle_call({set_status, {active, ShardNum}}, _From, #shard{status = active, my_number = ShardNum} = State) ->
    {reply, ok, State};
handle_call({set_status, {active, OtherShardNum}}, _From, #shard{status = active, my_number = ShardNum} = State) ->
    {stop, {wont_change_shard, ShardNum, OtherShardNum}, {error, shard_change}, State};
handle_call({set_status, {active, ShardNum}}, _From, #shard{} = State) ->
    {reply, ok, activate(ShardNum, State)};
handle_call({set_status, Inactive}, _From, #shard{status = active} = State)
        when Inactive == idle; Inactive == standby ->
    % This should not happen - allocator should send an allocation event
    NewState = callback_deallocate(undefined, State),
    {stop, {shutdown, suddenly_deallocated}, ok, idle(NewState)};
handle_call({set_status, idle}, _From, #shard{} = State) ->
    {reply, ok, idle(State)};
handle_call({set_status, standby}, _From, #shard{} = State) ->
    {reply, ok, standby(State)};
handle_call({set_status, Status}, _From, #shard{} = State) ->
    {reply, {error, {bad_status, Status}}, State};

handle_call(_, _From, #shard{} = State) ->
    {reply, {error, not_implemented}, State}.


handle_cast({allocation, Action, Challenger}, #shard{} = State) ->
    handle_allocation(Action, Challenger, State);

handle_cast(Unexpected, #shard{cluster_name = ClusterName} = State) ->
    error_logger:warning_msg("Minishard shard ~w got unexpected cast: ~9999p", [ClusterName, Unexpected]),
    {noreply, State}.


code_change(_, #shard{} = State, _) ->
    {ok, State}.

terminate(_, #shard{}) ->
    ok.



%% Allocation notification on conflict
handle_allocation(prolong, Loser, #shard{} = State) ->
    {noreply, callback_prolong(Loser, State)};
handle_allocation(cancel, Winner, #shard{} = State) ->
    NewState = callback_deallocate(Winner, State),
    % Gracefully shutdown for cleanup
    {stop, {shutdown, cluster_degraded}, idle(NewState)}.


%% Shard ownership recheck
handle_ownership_recheck(#shard{status = active, cluster_name = ClusterName, my_number = MyNum} = State) ->
    Owner = minishard:get_manager(ClusterName, MyNum),
    case (Owner == self()) of
        true -> % OK, we still own the shard
            {noreply, schedule_recheck(State)};
        false -> %% Oops...
            error_logger:error_msg("Minishard: cluster ~w shard #~w ownership lost!", [ClusterName, MyNum]),
            handle_allocation(cancel, undefined, State)
    end;
handle_ownership_recheck(#shard{} = State) ->
    {noreply, State}.


%%%
%%% Internals
%%%

export_status(#shard{status = active = Status, my_number = MyNum} = State) ->
    put(shard, MyNum),
    put(active_since, os:timestamp()),
    put(status, Status),
    State;
export_status(#shard{status = Status} = State) ->
    put(status, Status),
    erase(active_since),
    erase(shard),
    State.


%% Try to join a cluster and take a free shard if possible
join_cluster(#shard{status = standby} = State) ->
    % Already waiting for free shard number. This may happen after transition
    State;
join_cluster(#shard{status = active} = State) ->
    % Already active, do nothing. This may happen after transition
    State;
join_cluster(#shard{status = idle, cluster_name = ClusterName} = State) ->
    case minishard_allocator:bind(ClusterName) of
        {active, MyNumber} ->
            activate(MyNumber, State);
        standby ->
            export_status(State#shard{status = standby, my_number = undefined})
    end.

%% Perform all activation stuff when we capture a shard number
activate(MyNumber, #shard{} = State) ->
    Allocated = callback_allocate(State#shard{status = active, my_number = MyNumber}),
    RecheckScheduled = schedule_recheck(Allocated),
    export_status(RecheckScheduled).


%% Leave degraded cluster
idle(#shard{status = idle} = State) ->
    % Nothing to do
    State;
idle(#shard{status = standby} = State) ->
    export_status(State#shard{status = idle});
idle(#shard{status = active} = State) ->
    export_status(State#shard{status = idle, my_number = undefined}).

standby(#shard{status = standby} = State) ->
    % Nothing to do
    State;
standby(#shard{status = idle} = State) ->
    export_status(State#shard{status = standby}).


%% Due to some troubles allocator may have after netsplit, we need to periodically ensure we still own the shard number
schedule_recheck(#shard{cluster_name = minishard_demo} = State) ->
    State;
schedule_recheck(#shard{} = State) ->
    Timer = erlang:start_timer(100, self(), recheck_ownership),
    State#shard{recheck_timer = Timer}.


%% Callback management
callback_allocate(#shard{cluster_name = ClusterName, callback_mod = CallbackMod, my_number = MyNumber} = State) ->
    {ok, CallbackState} = CallbackMod:allocated(ClusterName, MyNumber),
    State#shard{callback_state = CallbackState}.

callback_prolong(Loser, #shard{callback_mod = CallbackMod, callback_state = CallbackState} = State) ->
    {ok, NewCallbackState} = CallbackMod:prolonged(Loser, CallbackState),
    State#shard{callback_state = NewCallbackState}.

callback_deallocate(Winner, #shard{callback_mod = CallbackMod, callback_state = CallbackState} = State) ->
    _ = CallbackMod:deallocated(Winner, CallbackState),
    State#shard{callback_state = undefined}.


%% We perform score getting in separate process to ensure allocator does not get garbage messages
get_score_or_kill(ShardPid) ->
    ScoreGetResult = rpc:call(node(), gen_server, call, [ShardPid, score, 1000]),
    handle_score_result(ShardPid, ScoreGetResult).

handle_score_result(_Pid, Score) when is_number(Score) ->
    Score;
handle_score_result(ShardPid, _) ->
    % We don't care what exactly goes wrong, we just kill it
    exit(ShardPid, kill),
    undefined.
