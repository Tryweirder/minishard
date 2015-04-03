-module(minishard_pinger).
-behavior(gen_server).

-export([start_link/3, status/1]).

-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

name(ClusterName, Node) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_" ++ atom_to_list(Node) ++ "_pinger").

start_link(ClusterName, Node, Watcher) when is_atom(ClusterName), is_atom(Node), is_pid(Watcher) ->
    State = seed_state(ClusterName, Node, Watcher),
    gen_server:start_link({local, name(ClusterName, Node)}, ?MODULE, State, []).

status(Pinger) when is_pid(Pinger) ->
    case process_info(Pinger, dictionary) of
        undefined ->
            undefined;
        {dictionary, Dict} ->
            proplists:get_value(status, Dict)
    end.

-record(pinger, {
        cluster_name,
        node,
        watcher,
        timer,
        watchdog,
        cluster_mon,
        status
        }).

seed_state(ClusterName, Node, Watcher) ->
    #pinger{
        cluster_name = ClusterName,
        node = Node,
        watcher = Watcher,
        status = undefined
        }.



init(#pinger{} = State) ->
    {ok, post_action(State)}.

handle_info({timeout, Timer, Check}, #pinger{timer = Timer} = State) ->
    NewState = run_check(Check, State),
    {noreply, post_action(NewState)};
handle_info({timeout, BadTimer, Check}, #pinger{timer = Timer, cluster_name = ClusterName, node = Node, status = Status} = State) ->
    error_logger:warning_msg("Minishard pinger ~w/~s got wrong check ~w (~w vs ~w, status ~w)", [ClusterName, Node, Check, BadTimer, Timer, Status]),
    {noreply, State};
handle_info({nodedown, Node}, #pinger{node = Node} = State) ->
    NewState = State#pinger{status = unavailable},
    {noreply, post_action(NewState)};
handle_info({'DOWN', ClusterMon, process, _, Reason}, #pinger{cluster_mon = ClusterMon} = State) ->
    Status = case Reason of
        noconnection -> unavailable;
        _ -> not_my_cluster
    end,
    NewState = State#pinger{status = Status},
    {noreply, post_action(NewState)};
handle_info(Unexpected, #pinger{cluster_name = ClusterName, node = Node} = State) ->
    error_logger:warning_msg("Minishard pinger ~w/~s got unexpected message: ~9999p", [ClusterName, Node, Unexpected]),
    {noreply, State}.

handle_cast(Unexpected, #pinger{cluster_name = ClusterName, node = Node} = State) ->
    error_logger:warning_msg("Minishard pinger ~w/~s got unexpected cast: ~9999p", [ClusterName, Node, Unexpected]),
    {noreply, State}.

handle_call(_, _From, #pinger{} = State) ->
    Response = {error, not_implemented},
    {reply, Response, State}.

code_change(_, #pinger{} = State, _) ->
    {ok, State}.

terminate(_, #pinger{}) ->
    ok.


post_action(#pinger{watchdog = OldWD} = State) ->
    _ = timer:cancel(OldWD),
    {ok, NewWD} = timer:exit_after(5000, pinger_stalled),
    NewState = schedule_check(State#pinger{watchdog = NewWD}),
    export_status(NewState).

select_timer(undefined)      -> {0, node_check};
select_timer(node_up)        -> {0, cluster_check};
select_timer(available)      -> {1000, cluster_recheck};
select_timer(unavailable)    -> {1000, node_check};
select_timer(not_my_cluster) -> {1000, cluster_check}.

schedule_check(#pinger{status = Status, timer = OldTimer} = State) ->
    is_reference(OldTimer) andalso erlang:cancel_timer(OldTimer),
    {Timeout, Check} = select_timer(Status),
    Timer = erlang:start_timer(Timeout, self(), Check),
    State#pinger{timer = Timer}.

export_status(#pinger{status = Status, watcher = Watcher, node = Node} = State) ->
    minishard_watcher:notify_node_status(Watcher, Node, Status),
    put(status, Status),
    State.


run_check(node_check, #pinger{node = Node} = State) ->
    {Status, MonFlag} = case net_adm:ping(Node) of
        pong ->
            {node_up, true};
        pang ->
            {unavailable, false}
    end,
    true = erlang:monitor_node(Node, MonFlag),
    State#pinger{status = Status};

run_check(ClusterCheck, #pinger{cluster_name = ClusterName, node = Node} = State)
        when ClusterCheck == cluster_check; ClusterCheck == cluster_recheck ->
    case rpc:call(Node, minishard_watcher, get_pinger, [ClusterName, node()]) of
        Pid when is_pid(Pid) ->
            cluster_check_succeeded(ClusterCheck, Pid, State);
        {badrpc,nodedown} ->
            State#pinger{status = unavailable};
        {badrpc, _} ->
            State#pinger{status = not_my_cluster}
    end.

cluster_check_succeeded(cluster_check, Pid, #pinger{} = State) ->
    Mon = erlang:monitor(process, Pid),
    State#pinger{status = available, cluster_mon = Mon};
cluster_check_succeeded(cluster_recheck, _Pid, #pinger{} = State) ->
    State#pinger{status = available}.
