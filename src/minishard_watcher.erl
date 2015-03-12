-module(minishard_watcher).
-behavior(gen_server).

-export([start_link/2, name/1, status/1, current_statuses/1, get_pinger/2, notify_node_status/3]).

-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).


name(ClusterName) when is_atom(ClusterName) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_watcher").


-record(watcher, {
        cluster_name,
        callback_mod,
        node_statuses = #{},
        shard_count,
        status,
        pingers_sup,
        poll_timer,
        transition_timeout,
        transition_timer
        }).

-define(POLL_INTERVAL, 1000).

start_link(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    State = seed_state(ClusterName, CallbackMod),
    gen_server:start_link({local, name(ClusterName)}, ?MODULE, State, []).

get_pinger(ClusterName, Node) ->
    Pingers = minishard_sup:pingers(ClusterName),
    case proplists:get_value(Node, Pingers) of
        Pid when is_pid(Pid) ->
            Pid;
        _ ->
            Watcher = whereis(name(ClusterName)),
            erlang:is_process_alive(Watcher) orelse error(no_cluster),
            {ok, Pid} = minishard_sup:add_pinger(ClusterName, Node, Watcher),
            Pid
    end.

status(ClusterName) when is_atom(ClusterName), ClusterName /= undefined ->
    Watcher = whereis(name(ClusterName)),
    status(Watcher);

status(Watcher) when is_pid(Watcher) ->
    {dictionary, Dict} = process_info(Watcher, dictionary),
    proplists:get_value(status, Dict, undefined).


% Notify watcher about node status change
notify_node_status(Watcher, Node, Status) when is_pid(Watcher), is_atom(Node), is_atom(Status) ->
    gen_server:cast(Watcher, {node_status, Node, Status}).

seed_state(ClusterName, CallbackMod) ->
    % Get and check shard_count
    ShardCount = CallbackMod:shard_count(ClusterName),
    is_integer(ShardCount) orelse error({bad_shard_count, ShardCount}),
    % Get cluster nodes and make initial status map
    Nodes = CallbackMod:cluster_nodes(ClusterName),
    NStatuses = maps:from_list([{Node, undefined} || Node <- Nodes]),
    % Ensure we have enough possible nodes to host all shards
    (ShardCount =< length(Nodes)) orelse error({too_few_nodes, length(Nodes)}),
    % Construct initial state
    #watcher{
        cluster_name = ClusterName,
        callback_mod = CallbackMod,
        node_statuses = NStatuses,
        shard_count = ShardCount,
        status = degraded,
        poll_timer = undefined,
        transition_timeout = 4 * ?POLL_INTERVAL,
        transition_timer = undefined
        }.


init(#watcher{} = State0) ->
    State = set_poll_timer(State0),
    {ok, check_status(State)}.


handle_info({timeout, PollTimer, poll_pingers}, #watcher{poll_timer = PollTimer} = State) ->
    NewState = set_poll_timer(poll_pingers(State)),
    {noreply, check_status(NewState)};
handle_info({timeout, _WrongTimer, poll_pingers}, #watcher{} = State) ->
    {noreply, State};

handle_info({timeout, TransitionTimer, degrade}, #watcher{transition_timer = TransitionTimer} = State) ->
    NewState = State#watcher{status = degraded},
    {noreply, export_status(NewState)};
handle_info({timeout, _WrongTimer, degrade}, #watcher{} = State) ->
    {noreply, State};

handle_info(Unexpected, #watcher{cluster_name = ClusterName} = State) ->
    error_logger:warning_msg("Minishard watcher ~w got unexpected message: ~9999p", [ClusterName, Unexpected]),
    {noreply, State}.


handle_cast({node_status, Node, Status}, #watcher{} = State) ->
    NewState = save_node_status(Node, Status, State),
    {noreply, check_status(NewState)};
handle_cast(_, #watcher{} = State) ->
    {noreply, State}.

handle_call(_, _From, #watcher{} = State) ->
    Response = {error, not_implemented},
    {reply, Response, State}.

code_change(_, #watcher{} = State, _) ->
    {ok, State}.

terminate(_, #watcher{}) ->
    ok.





set_poll_timer(#watcher{poll_timer = Timer} = State) when is_reference(Timer) ->
    _ = erlang:cancel_timer(Timer),
    set_poll_timer(State#watcher{poll_timer = undefined});
set_poll_timer(#watcher{poll_timer = undefined} = State) ->
    Timer = erlang:start_timer(?POLL_INTERVAL, self(), poll_pingers),
    State#watcher{poll_timer = Timer}.


export_status(#watcher{cluster_name = ClusterName, status = Status} = State) ->
    error_logger:info_msg("Minishard watcher ~w changed status to ~w", [ClusterName, Status]),
    put(status, Status),
    minishard_shard:notify_cluster_status(ClusterName, Status),
    State.


poll_pingers(#watcher{pingers_sup = PingersSup, node_statuses = PrevStatuses} = State) when is_pid(PingersSup) ->
    RefreshedStatuses = current_statuses(PingersSup),

    SeenNodes = maps:keys(PrevStatuses),
    PingedNodes = maps:keys(RefreshedStatuses),

    UnpingedNodes = SeenNodes -- PingedNodes,
    _NewPingers = [{N, new_pinger(N, State)} || N <- UnpingedNodes],
    ZeroStatuses = maps:from_list([{N, undefined} || N <- UnpingedNodes]),

    NewStatuses = maps:merge(ZeroStatuses, RefreshedStatuses),

    maps:fold(fun save_node_status/3, State, NewStatuses);

poll_pingers(#watcher{cluster_name = ClusterName} = State) ->
    PingersSup = minishard_sup:get_pid(ClusterName, pingers),
    poll_pingers(State#watcher{pingers_sup = PingersSup}).


new_pinger(Node, #watcher{pingers_sup = PingersSup, cluster_name = ClusterName}) ->
    {ok, Pinger} = minishard_sup:add_pinger(PingersSup, ClusterName, Node, self()),
    Pinger.


current_statuses(Cluster) ->
    Pingers = minishard_sup:pingers(Cluster),
    maps:from_list([{N, poll_status(Pid)} || {N, Pid} <- Pingers]).

poll_status(Pid) when is_pid(Pid) ->
    minishard_pinger:status(Pid).

save_node_status(Node, Status, #watcher{cluster_name = ClusterName, node_statuses = PrevStatuses} = State) ->
    OldStatus = maps:get(Node, PrevStatuses, undefined),
    (Status /= OldStatus) andalso error_logger:info_msg("Minishard cluster ~w node ~s changed status from ~w to ~w", [ClusterName, Node, OldStatus, Status]),
    Statuses = maps:put(Node, Status, PrevStatuses),
    State#watcher{node_statuses = Statuses}.


% Check if we need to change status. Here we check if available node count is enough
check_status(#watcher{node_statuses = Statuses, shard_count = ReqNodes} = State) ->
    NodesAvailable = maps:fold(fun count_available_nodes/3, 0, Statuses),
    case (NodesAvailable >= ReqNodes) of
        true -> set_status(available, State);
        false -> set_status(degraded, State)
    end.

count_available_nodes(_, available, Count) -> Count + 1;
count_available_nodes(_, _bad, Count) -> Count.

set_status(OldStatus, #watcher{status = OldStatus} = State) ->
    % No status change
    State;
set_status(available, #watcher{transition_timer = TT} = State) ->
    % Stop the transition timer, we are okay
    is_reference(TT) andalso erlang:cancel_timer(TT),
    NewState = State#watcher{transition_timer = undefined, status = available},
    export_status(NewState);

set_status(degraded, #watcher{status = transition, transition_timer = TT} = State)
        when is_reference(TT) -> % Already in transition
    State;
set_status(degraded, #watcher{transition_timeout = TransTimeout} = State) ->
    TT = erlang:start_timer(TransTimeout, self(), degrade),
    NewState = State#watcher{status = transition, transition_timer = TT},
    export_status(NewState).

