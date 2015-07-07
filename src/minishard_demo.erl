-module(minishard_demo).
-behavior(minishard).

-export([cluster_nodes/1, shard_count/1]).
-export([allocated/2, score/1, prolonged/2, deallocated/2]).

% Generate fake node list by changing a number in local node name
cluster_nodes(_) ->
    BinNode = atom_to_binary(node(), latin1),
    [make_node(BinNode, N) || N <- lists:seq(1, 5)].

% Shard count, needed to monitor cluster for degrades
shard_count(_) ->
    2.

% Helper for cluster node names generation
make_node(BinPattern, N) ->
    IOLNode = re:replace(BinPattern, "[0-9]+@", [integer_to_list(N), "@"]),
    binary_to_atom(iolist_to_binary(IOLNode), latin1).


-record(demo, {name, num, alloc_time}).

allocated(Cluster, Num) ->
    error_logger:info_msg("Woo-hoo!!! Minishard demo cluster (name ~w) has allocated us as shard #~w", [Cluster, Num]),
    {ok, #demo{name = Cluster, num = Num, alloc_time = os:timestamp()}}.

score(#demo{alloc_time = AllocTime}) ->
    % Let the score be number of seconds we are active
    timer:now_diff(os:timestamp(), AllocTime) div 10000000.

prolonged(Loser, #demo{name = Cluster, num = Num} = State) ->
    error_logger:info_msg("Wheeeeeee!!! We still own minishard cluster ~w shard #~w, and ~w at ~w is loser!",
                          [Cluster, Num, Loser, node(Loser)]),
    {ok, State}.

deallocated(undefined, #demo{name = Cluster, num = Num}) ->
    error_logger:info_msg("Bad news: minishard cluster ~w has degraded, so we lose the shard #~w :(", [Cluster, Num]),
    ok;
deallocated(Winner, #demo{name = Cluster, num = Num}) ->
    error_logger:info_msg("Bad news: ~w at ~w has won the competition for minishard cluster ~w shard #~w :(",
                          [Winner, node(Winner), Cluster, Num]),
    ok.
