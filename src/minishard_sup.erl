-module(minishard_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

sup_name(root) -> minishard.

start_link(Arg) ->
	supervisor:start_link({local, sup_name(Arg)}, ?MODULE, Arg).

init(root) ->
    {ok, {{one_for_one, 1, 5}, []}}.
