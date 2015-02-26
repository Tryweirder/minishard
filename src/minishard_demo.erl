-module(minishard_demo).

-export([nodes/1]).
-compile({no_auto_import, nodes}).

% Generate fake node list by changing a number in local node name
nodes(_) ->
    BinNode = atom_to_binary(node(), latin1),
    [make_node(BinNode, N) || N <- lists:seq(1,5)].

make_node(BinPattern, N) ->
    IOLNode = re:replace(BinPattern, "[0-9]+@", [integer_to_list(N), "@"]),
    binary_to_atom(iolist_to_binary(IOLNode), latin1).
