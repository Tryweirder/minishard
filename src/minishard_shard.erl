-module(minishard_shard).
-behavior(gen_server).

-export([start_link/2, name/1, status/1, notify_cluster_status/2, allocation_map/2]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).


name(ClusterName) when is_atom(ClusterName) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_shard").

global_name(ClusterName, ShardNum) when is_atom(ClusterName), is_integer(ShardNum) ->
    {minishard, ClusterName, ShardNum}.


start_link(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    State = seed_state(ClusterName, CallbackMod),
    gen_server:start_link({local, name(ClusterName)}, ?MODULE, State, []).


%% Get shard status
status(ClusterName) when is_atom(ClusterName), ClusterName /= undefined ->
    Shard = whereis(name(ClusterName)),
    status(Shard);

status(Shard) when is_pid(Shard) ->
    {dictionary, Dict} = process_info(Shard, dictionary),
    proplists:get_value(status, Dict, undefined).


%% Notify about cluster status change. This may make the shard manager to capture some shard or to shutdown
notify_cluster_status(ClusterName, Status)
        when is_atom(ClusterName), (Status == degraded orelse Status == transition orelse Status == available) ->
    gen_server:cast(name(ClusterName), {cluster_status, Status}).


%% Cluster status: get allocation map
allocation_map(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    MaxNum = CallbackMod:shard_count(ClusterName),
    AllShardNums = lists:seq(1, MaxNum),
    maps:from_list([{N, allocated_node(ClusterName, N)} || N <- AllShardNums]).

allocated_node(ClusterName, Num) ->
    case global:whereis_name(global_name(ClusterName, Num)) of
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
    {noreply, NewState}.

handle_call(_, _From, #shard{} = State) ->
    {reply, {error, not_implemented}, State}.

handle_cast({cluster_status, Status}, #shard{} = State) ->
    handle_cluster_status(Status, State);
handle_cast(_, #shard{} = State) ->
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
    % Gracefully shutdown for cleanup
    {stop, {shutdown, cluster_degraded}, idle(State)};
handle_cluster_status(transition, #shard{} = State) ->
    % Do nothing during transition
    {noreply, State}.


%%%
%%% Internals
%%%

export_status(#shard{status = Status} = State) ->
    put(status, Status),
    State.

join_cluster(#shard{status = standby} = State) ->
    % Already waiting for free shard number. This may happen after transition
    State;
join_cluster(#shard{status = active} = State) ->
    % Already active, do nothing. This may happen after transition
    State;
join_cluster(#shard{status = idle, cluster_name = ClusterName, max_number = MaxNumber} = State) ->
    ok = global:sync(), % Ensure we have fresh shard map
    AllShardNums = lists:seq(1, MaxNumber),
    NewState = case register_or_monitor(ClusterName, AllShardNums) of
        {registered, MyNumber} ->
            State#shard{status = active, my_number = MyNumber};
        {monitored, Monitors} ->
            State#shard{status = standby, my_number = undefined, monitors = Monitors}
    end,
    export_status(NewState).


idle(#shard{status = idle} = State) ->
    % Nothing to do
    State;
idle(#shard{status = standby, monitors = Monitors} = State) ->
    demonitor_all(Monitors),
    export_status(State#shard{status = idle, monitors = #{}});
idle(#shard{status = active} = State) ->
    % TODO: shut down user code
    export_status(State#shard{status = idle, my_number = undefined}).


%% Resolve key by value
which_shard_failed(MonRef, MonMap) ->
    case maps:fold(fun(Num, Ref, Matched) -> cons_if(Ref == MonRef, Num, Matched) end, [], MonMap) of
        [] -> undefined;
        [Num] -> Num
    end.

%% Helper for Value-Key resolver
cons_if(false, _Hd, Tail) -> Tail;
cons_if(true, Hd, Tail) -> [Hd|Tail].


%% Try to take failed shard
try_failover(ShardNum, #shard{cluster_name = ClusterName, monitors = MonMap} = State) ->
    case register_or_monitor(ClusterName, [ShardNum]) of
        {registered, ShardNum} ->
            _ = demonitor_all(MonMap),
            export_status(State#shard{status = active, my_number = ShardNum});
        {monitored, MonPatch} ->
            NewMonMap = maps:merge(MonMap, MonPatch),
            State#shard{monitors = NewMonMap}
    end.


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
    _ = maps:map(fun(_N, MonRef) -> erlang:demonitor(MonRef) end, Monitors),
    ok.


