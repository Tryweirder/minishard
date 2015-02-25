-module(minishard_demo).

-export([nodes/1]).
-compile({no_auto_import, nodes}).

nodes(_) ->
    [node()].
