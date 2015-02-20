-module(minishard).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
	minishard_sup:start_link(root).

stop(_State) ->
	ok.
