-module(minishard).
-behaviour(application).

% API
-export([start/0]).

% Application callbacks
-export([start/2, stop/1]).


start() ->
    application:start(?MODULE, permanent).


start(_Type, _Args) ->
	minishard_sup:start_link(root).

stop(_State) ->
	ok.
