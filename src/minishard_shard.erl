-module(minishard_shard).
-behavior(gen_server).

-export([start_link/2, name/1, status/1, info/1, notify_cluster_status/2, allocation_map/2]).
-export([manager_pid/2, allocated_node/2]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%% global conflict resolver
-export([resolve_conflict/3]).

name(ClusterName) when is_atom(ClusterName) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_shard").

global_name(ClusterName, ShardNum) when is_atom(ClusterName), is_integer(ShardNum) ->
    {minishard, ClusterName, ShardNum}.


start_link(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    State = seed_state(ClusterName, CallbackMod),
    gen_server:start_link({local, name(ClusterName)}, ?MODULE, State, []).


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


%% Notify about cluster status change. This may make the shard manager to capture some shard or to shutdown
notify_cluster_status(ClusterName, Status)
        when is_atom(ClusterName), (Status == degraded orelse Status == transition orelse Status == available) ->
    gen_server:cast(name(ClusterName), {cluster_status, Status}).


%% Cluster status: get allocation map
allocation_map(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    MaxNum = CallbackMod:shard_count(ClusterName),
    AllShardNums = lists:seq(1, MaxNum),
    maps:from_list([{N, allocated_node(ClusterName, N)} || N <- AllShardNums]).

local_pid(ManagerPid) when is_pid(ManagerPid) ->
    ManagerPid;
local_pid(ClusterName) when is_atom(ClusterName), ClusterName /= undefined ->
    manager_pid(ClusterName, local).

manager_pid(ClusterName, local) when is_atom(ClusterName), ClusterName /= undefined ->
    whereis(name(ClusterName));
manager_pid(ClusterName, Num) when is_atom(ClusterName), is_integer(Num) ->
    global:whereis_name(global_name(ClusterName, Num)).

allocated_node(ClusterName, Num) ->
    case manager_pid(ClusterName, Num) of
        undefined ->
            undefined;
        Pid when is_pid(Pid) ->
            node(Pid)
    end.

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
handle_info(timeout, #shard{status = starting, cluster_name = ClusterName} = State0) ->
    State = State0#shard{status = idle},
    NewState = case minishard_watcher:status(ClusterName) of
        available ->
            join_cluster(State);
        _ ->
            idle(State)
    end,
    {noreply, NewState};

%% Some monitored process (we monitor other shards, but user code may monitor something else) dies
handle_info({'DOWN', MonRef, process, _Pid, _Reason}, #shard{monitors = Mons, status = standby} = State) ->
    NewState = case which_shard_failed(MonRef, Mons) of
        undefined -> % Unexpected monitor report, ignore it
            State;
        ShardNum when is_integer(ShardNum) -> % Ok, now we should try to register as a failed shard
            try_failover(ShardNum, State)
    end,
    {noreply, NewState};

handle_info({timeout, Timer, recheck_ownership}, #shard{recheck_timer = Timer} = State) ->
    handle_ownership_recheck(State#shard{recheck_timer = undefined});

handle_info(Unexpected, #shard{cluster_name = ClusterName} = State) ->
    error_logger:warning_msg("Minishard shard ~w got unexpected message: ~9999p", [ClusterName, Unexpected]),
    {noreply, State}.


handle_call(score, _From, #shard{status = active, callback_mod = CallbackMod, callback_state = CallbackState} = State) ->
    Score = CallbackMod:score(CallbackState),
    {reply, Score, State};

handle_call(_, _From, #shard{} = State) ->
    {reply, {error, not_implemented}, State}.


handle_cast({cluster_status, Status}, #shard{} = State) ->
    handle_cluster_status(Status, State);

handle_cast({allocation, Action, Challenger}, #shard{} = State) ->
    handle_allocation(Action, Challenger, State);

handle_cast(Unexpected, #shard{cluster_name = ClusterName} = State) ->
    error_logger:warning_msg("Minishard shard ~w got unexpected cast: ~9999p", [ClusterName, Unexpected]),
    {noreply, State}.


code_change(_, #shard{} = State, _) ->
    {ok, State}.

terminate(_, #shard{}) ->
    ok.


%% Business logic away from handle_cast
handle_cluster_status(available, #shard{} = State) ->
    % Wheeeeeeeeee!!!!
    {noreply, join_cluster(State)};
handle_cluster_status(degraded, #shard{} = State) ->
    % Cancel allocation with undefined winner
    handle_allocation(cancel, undefined, State);
handle_cluster_status(transition, #shard{} = State) ->
    % Do nothing during transition
    {noreply, State}.


%% Allocation notification on conflict
handle_allocation(prolong, Loser, #shard{} = State) ->
    {noreply, callback_prolong(Loser, State)};
handle_allocation(cancel, Winner, #shard{} = State) ->
    NewState = callback_deallocate(Winner, State),
    % Gracefully shutdown for cleanup
    {stop, {shutdown, cluster_degraded}, idle(NewState)}.


%% Shard ownership recheck
handle_ownership_recheck(#shard{status = active, cluster_name = ClusterName, my_number = MyNum} = State) ->
    Owner = manager_pid(ClusterName, MyNum),
    case (Owner == self()) of
        true -> % OK, we still own the shard
            {noreply, schedule_recheck(State)};
        false -> %% Oops...
            error_logger:error_msg("Minishard: cluster ~w shard #~w ownership lost! This could be some bug in global", [ClusterName, MyNum]),
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


%% Try to take failed shard
try_failover(ShardNum, #shard{cluster_name = ClusterName, monitors = MonMap} = State) ->
    case register_or_monitor(ClusterName, [ShardNum]) of
        {registered, ShardNum} ->
            _ = demonitor_all(MonMap),
            activate(ShardNum, State);
        {monitored, MonPatch} ->
            NewMonMap = maps:merge(MonMap, MonPatch),
            State#shard{monitors = NewMonMap}
    end.


%% Try to join a cluster and take a free shard if possible
join_cluster(#shard{status = standby} = State) ->
    % Already waiting for free shard number. This may happen after transition
    State;
join_cluster(#shard{status = active} = State) ->
    % Already active, do nothing. This may happen after transition
    State;
join_cluster(#shard{status = idle, cluster_name = ClusterName, max_number = MaxNumber} = State) ->
    ok = global:sync(), % Ensure we have fresh shard map
    AllShardNums = lists:seq(1, MaxNumber),
    case register_or_monitor(ClusterName, AllShardNums) of
        {registered, MyNumber} ->
            activate(MyNumber, State);
        {monitored, Monitors} ->
            export_status(State#shard{status = standby, my_number = undefined, monitors = Monitors})
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
idle(#shard{status = standby, monitors = Monitors} = State) ->
    demonitor_all(Monitors),
    export_status(State#shard{status = idle, monitors = #{}});
idle(#shard{status = active} = State) ->
    export_status(State#shard{status = idle, my_number = undefined}).


%% Due to some troubles global has after netsplit, we need to periodically ensure we still own the shard number
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


%% Resolve key by value
which_shard_failed(MonRef, MonMap) ->
    case maps:fold(fun(Num, Ref, Matched) -> cons_if(Ref == MonRef, Num, Matched) end, [], MonMap) of
        [] -> undefined;
        [Num] -> Num
    end.

%% Helper for Value-Key resolver
cons_if(false, _Hd, Tail) -> Tail;
cons_if(true, Hd, Tail) -> [Hd|Tail].


register_or_monitor(ClusterName, AllShardNums) ->
    register_or_monitor(ClusterName, AllShardNums, #{}).

register_or_monitor(_ClusterName, [], PidMap) ->
    MonMap = maps:map(fun(_N, Pid) -> erlang:monitor(process, Pid) end, PidMap),
    {monitored, MonMap};
register_or_monitor(ClusterName, [Num | ShardNums], PidMap) ->
    Name = global_name(ClusterName, Num),
    ExistingPid = global:whereis_name(Name),
    RegResult = (ExistingPid == undefined) andalso global:register_name(Name, self(), fun ?MODULE:resolve_conflict/3),
    case {ExistingPid, RegResult} of
        {undefined, yes} -> % We have successfully registered as a new shard
            {registered, Num};
        {undefined, no} -> % Race condition during registration, re-resolve pid by stupid recursion
            register_or_monitor(ClusterName, [Num | ShardNums], PidMap);
        {ExistingPid, _} when is_pid(ExistingPid) -> % Shard was already running
            NewPidMap = maps:put(Num, ExistingPid, PidMap),
            register_or_monitor(ClusterName, ShardNums, NewPidMap)
    end.


demonitor_all(Monitors) ->
    _ = maps:map(fun(_N, MonRef) -> erlang:demonitor(MonRef, [flush]) end, Monitors),
    ok.




%% This function is called when two shard managers are registered with one shard number
%% Here we ask both for their score, choose which of them should be terminated and send notifications
resolve_conflict(Name, Shard1, Shard2) ->
    Score1 = get_score_or_kill(Shard1),
    Score2 = is_integer(Score1) andalso get_score_or_kill(Shard2),
    if
        is_integer(Score1) andalso is_integer(Score2) ->
            error_logger:warning_msg("minishard conflict resolution for name ~w: ~w (score ~w) vs ~w (score ~w)", [Name, node(Shard1), Score1, node(Shard2), Score2]),
            select_conflict_winner(Score1 >= Score2, Shard1, Shard2);
        is_integer(Score1) ->
            Shard1;
        true ->
            Shard2
    end.

%% We perform score getting in separate process to ensure global manager does not get garbage messages
get_score_or_kill(ShardPid) ->
    ScoreGetResult = rpc:call(node(), gen_server, call, [ShardPid, score, 1000]),
    if
        is_integer(ScoreGetResult) ->
            ScoreGetResult;
        true ->
            % We don't care what exactly goes wrong, we just kill it
            exit(ShardPid, kill),
            undefined
    end.


select_conflict_winner(false, Loser, Winner) ->
    select_conflict_winner(true, Winner, Loser);
select_conflict_winner(true, Winner, Loser) ->
    % Notify both parties of our decision
    gen_server:cast(Winner, {allocation, prolong, Loser}),
    gen_server:cast(Loser, {allocation, cancel, Winner}),
    % global expect us to return the winner pid
    Winner.

