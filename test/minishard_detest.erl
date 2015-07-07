-module(minishard_detest).

% mandatory detest functions
-export([cfg/1,run/1,setup/1,cleanup/1]).

-export([handle_worker_event/2, send_event/1]).

cfg(_TestArgs) ->
    % Compile test callback module
    compile:file("test/minishard_test", [{outdir, "test"}]),
    [
        % {global_cfg,[{"test/nodes.yaml",[{fixedvals,KV}]},"test/withoutparams.yaml"]},
        {per_node_cfg, ["test/test.config"]},
        {cmd         , "-pa test -s minishard -config test/test.config"},
        {wait_for_app, minishard},
        {nodes       , []},
        {nodes       , all_nodes_spec()},
        {erlenv      , [{"ERL_LIBS","deps"}]}
    ].


setup(_Param) ->
    ok.

cleanup(_Param) ->
    ok.


run(Param) ->
    random:seed(os:timestamp()),
    lager:info("Script params: ~p", [Param]),
    ClusterSize = proplists:get_value(cluster_size, Param, 10),


    IdsToStart = lists:seq(1, ClusterSize),
    NodeMap = maps:from_list([{Id, detest:add_node(node_spec(Id))} || Id <- IdsToStart]),
    %NodeMap = maps:from_list(pmap(fun(Id) -> {Id, detest:add_node(node_spec(Id))} end, IdsToStart)),

    lager:info("Started nodes, map: ~120p", [NodeMap]),

    Nodes = maps:values(NodeMap),
    MST_Config = [{shard_count, 3}, {nodes, Nodes}],
    {_, []} = rpc:multicall(Nodes, minishard_test, set_config, [test, MST_Config]),
    {_, []} = rpc:multicall(Nodes, minishard_test, start, [test]),

    timer:sleep(200 + 50*ClusterSize), % Let the leader allocate all shards

    lager:info("initial shard map: ~120p", [get_validate_map(Nodes, allocated)]),

    kill_standby_nodes_test(test, Nodes, 10),

    timer:sleep(50000),

    ok.
%    State0 = #{
%            nodes      => [],
%            nodes_spec => all_nodes_spec()
%            },
%
%    State1 = loop(Stratagy, State0),
%
%    #{worker:=stopped} = State1.


%% Helper: parallel map for faster cluster startup
pmap(Function, List) ->
    S = self(),
    Pids = [spawn_link(fun() -> execute(S, Function, El) end) || El <- List],
    gather(Pids).

execute(Recv, Function, Element) ->
    Recv ! {self(), Function(Element)}.

gather([]) -> [];
gather([H|T]) ->
    receive
        {H, Ret} -> [Ret|gather(T)]
    end.  

multicall(Nodes, M, F, A) ->
    pmap(fun(Node) ->
                {Node, rpc:call(Node, M, F, A)}
        end, Nodes).
    

get_validate_map(Nodes, ExpectedState) ->
    Map = get_same_map(Nodes),
    ok = validate_map(Map, ExpectedState),
    Map.

get_same_map(Nodes) ->
    NodeMaps = multicall(Nodes, minishard_test, map, [test]),
    case lists:ukeysort(2, NodeMaps) of
        [{_, #{} = Map}] ->
            Map;
        _Other ->
            error({different_maps, NodeMaps})
    end.

validate_map(Map, allocated) ->
    case missing_shards(Map) of
        [] -> ok;
        [_|_] = Missing -> {error, {missing, Missing}}
    end.

missing_shards(Map) ->
    maps:fold(fun collect_missing_shards/3, [], Map).

collect_missing_shards(Shard, undefined, Acc) -> [Shard|Acc];
collect_missing_shards(_Shard, _, Acc) -> Acc.


kill_standby_nodes_test(_Name, _Nodes, 0) ->
    ok;
kill_standby_nodes_test(Name, Nodes, Iterations) ->
    Map0 = get_validate_map(Nodes, allocated),
    BusyNodes = maps:values(Map0),
    KillCandidates = Nodes -- BusyNodes,
    NodesToKill = lists:filter(fun(_) -> crypto:rand_uniform(0, 2) == 1 end, KillCandidates),
    lager:info("kill_standby_nodes_test: (~w iters left) killing ~120p", [Iterations, NodesToKill]),
    RemainingNodes = Nodes -- NodesToKill,

    pmap(fun(Node) -> detest:stop_node(Node) end, NodesToKill),
    timer:sleep(200),
    Map0 = get_validate_map(RemainingNodes, allocated),

    lager:info("kill_standby_nodes_test: (~w iters left) starting back ~120p", [Iterations, NodesToKill]),
    pmap(fun(Node) -> detest:add_node(node_spec(Node)) end, NodesToKill),
    timer:sleep(500),
    Map0 = get_validate_map(RemainingNodes, allocated),

    kill_standby_nodes_test(Name, Nodes, Iterations - 1).



%%
%% testing fsm
%%
loop(start_stop, State) ->
    start_loop(State).

next_step(StepFun, State) ->
    StepFun(check_nodes_status(State)).


start_node(NodeSpec, State=#{nodes:=Nodes}) ->
    lager:info("starting node ~p", [NodeSpec]),
    Node = detest:add_node(NodeSpec),
    State#{nodes:=maps:put(NodeSpec, Node, Nodes)}.

stop_node(NodeSpec, State=#{nodes:=Nodes}) ->
    Node = maps:get(NodeSpec, Nodes),
    lager:info("stopping node ~p", [Node]),
    ok = detest:stop_node(Node),
    State#{nodes:=maps:remove(NodeSpec, Nodes)}.


%%
%% start-stop test
%%
start_loop(State=#{nodes:=Nodes, nodes_spec:=NodesSpec}) when map_size(Nodes) =:= length(NodesSpec) ->
    lager:info("all nodes has started"),
    next_step(fun stop_loop/1, State);
start_loop(State=#{nodes:=Nodes, nodes_spec:=NodesSpec}) ->
    NodeSpec = lists:nth(map_size(Nodes) + 1, NodesSpec),
    next_step(fun start_loop/1, start_node(NodeSpec, State)).
stop_loop(State=#{nodes:=Nodes}) when map_size(Nodes) =:= 0 ->
    lager:info("all nodes has stopped"),
    State;
stop_loop(State=#{nodes:=Nodes}) ->
    [NodeSpec|_] = maps:keys(Nodes),
    next_step(fun stop_loop/1, stop_node(NodeSpec, State)).




%%
%% node status checking
%%
check_nodes_status(State) ->
    process_events(recv_all(), State).
    % _ = lists:foldl(fun check_node_status/2, undefined, Nodes),
    % lager:info("cluster status: ok"),

% check_node_status(Node, _) ->
%     Status = rpc:call(Node, ggdist_worker, status, []),
%     lager:info("node ~p is ~p", [Node, Status]).

process_events(Events, State) ->
    lists:foldl(fun process_event/2, State, Events).

process_event({started, NewWorkerPid}, State=#{worker:=Worker})
    when Worker =:= starting; Worker =:= stopped
    ->
    Monitor = erlang:monitor(process, NewWorkerPid),
    State#{worker:={NewWorkerPid, Monitor}};
process_event({stopped, WorkerPid0}, State=#{worker:={WorkerPid1, Monitor}}) when WorkerPid0 =:= WorkerPid1 ->
    _ = erlang:demonitor(Monitor),
    State#{worker:=stopped};
process_event({'DOWN', Monitor0, process, WorkerPid0, _}, State=#{worker:={WorkerPid1, Monitor1}})
    when Monitor0 =:= Monitor1; WorkerPid0 =:= WorkerPid1
    ->
    State#{worker:=stopped};
process_event(Event, State) ->
    exit({incorrect_event, Event, State}).

%%
%% events
%%
handle_worker_event(MasterNode, Event) ->
    rpc:call(MasterNode, ?MODULE, send_event, [Event]).

send_event(Event) ->
    lager:info("event: ~p", [Event]),
    runproc ! Event.

recv_all() ->
    lists:reverse(recv_all([])).
recv_all(Acc) ->
    case recv_event(0) of
        {ok, Event} ->
            recv_all([Event|Acc]);
        {error, timeout} ->
            Acc
    end.

% recv_event() ->
%     case recv_event(5000) of
%         {ok, Event} -> Event;
%         {error, Error} -> exit(Error)
%     end.

recv_event(Timeout) ->
    receive
        Event ->
            {ok, Event}
    after Timeout ->
        {error, timeout}
    end.


%%
%% utils
%%
all_nodes_spec() ->
    [node_spec(N) || N <- lists:seq(1,5)].

node_spec(Node) when is_atom(Node) ->
    {ok, [N], "@" ++ _} = io_lib:fread("mst_~d", atom_to_list(Node)),
    node_spec(N);
node_spec(N) when is_integer(N) ->
    [{id, N}, {name, list_to_atom("mst_" ++ integer_to_list(N))}].

