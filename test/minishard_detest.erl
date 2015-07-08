-module(minishard_detest).

% mandatory detest functions
-export([cfg/1,run/1,setup/1,cleanup/1]).

cfg(_TestArgs) ->
    % Compile test callback module
    compile:file("test/minishard_test", [{outdir, "test"}]),
    [
        % {global_cfg,[{"test/nodes.yaml",[{fixedvals,KV}]},"test/withoutparams.yaml"]},
        {per_node_cfg, ["test/test.config"]},
        {cmd         , "-pa test -s minishard -config test/test.config"},
        {wait_for_app, minishard},
        {nodes       , []},
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
    % Warning: do not start new nodes with pmap because they will get a same address
    NodeMap = maps:from_list([{Id, detest:add_node(node_spec(Id))} || Id <- IdsToStart]),

    lager:info("Started nodes, map: ~120p", [NodeMap]),

    Nodes = maps:values(NodeMap),
    MST_Config = [{shard_count, 3}, {nodes, Nodes}],

    configure_and_start(test, Nodes, MST_Config),

    timer:sleep(1200 + 50*ClusterSize), % Let the leader allocate all shards

    lager:info("initial shard map: ~120p", [get_validate_map(Nodes, allocated)]),

    kill_standby_nodes_test(test, Nodes, MST_Config, 10),

    timer:sleep(50000),

    ok.


configure_and_start(Name, Nodes, MST_Config) ->
    ConfigResults = multicall(Nodes, minishard_test, set_config, [Name, MST_Config]),
    [{_, ok}] = lists:ukeysort(2, ConfigResults),
    StartResults = multicall(Nodes, minishard_test, start, [Name]),
    [{_, ok}] = lists:ukeysort(2, StartResults),
    ok.

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

%% This multicall is not compatible with rpc:multicall:
%% it takes only MFA and returns a tuplelist where results are tagged with node names
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


kill_standby_nodes_test(_Name, _Nodes, _Config, 0) ->
    ok;
kill_standby_nodes_test(Name, Nodes, MST_Config, Iterations) ->
    ClusterSize = length(Nodes),
    Map0 = get_validate_map(Nodes, allocated),
    BusyNodes = maps:values(Map0),
    KillCandidates = Nodes -- BusyNodes,
    NodesToKill = lists:filter(fun(_) -> crypto:rand_uniform(0, 2) == 1 end, KillCandidates),
    RemainingNodes = Nodes -- NodesToKill,

    lager:info("kill_standby_nodes_test: (~w iters left) killing ~120p", [Iterations, NodesToKill]),
    pmap(fun(Node) -> detest:stop_node(Node) end, NodesToKill),

    timer:sleep(1200),
    Map0 = get_validate_map(RemainingNodes, allocated),

    lager:info("kill_standby_nodes_test: (~w iters left) starting back ~120p", [Iterations, NodesToKill]),
    pmap(fun(Node) -> detest:add_node(node_spec(Node)) end, NodesToKill),
    configure_and_start(Name, NodesToKill, MST_Config),

    timer:sleep(1200 + 50*ClusterSize), % Let the leader allocate all shards
    Map0 = get_validate_map(RemainingNodes, allocated),

    kill_standby_nodes_test(Name, Nodes, MST_Config, Iterations - 1).




node_spec(Node) when is_atom(Node) ->
    {ok, [N], "@" ++ _} = io_lib:fread("mst_~d", atom_to_list(Node)),
    node_spec(N);
node_spec(N) when is_integer(N) ->
    [{id, N}, {name, list_to_atom("mst_" ++ integer_to_list(N))}].

