-module(minishard).
-behaviour(application).

%%% Behavior callbacks
% Configuration
-callback shard_count(ClusterName :: atom()) -> integer().
-callback cluster_nodes(ClusterName :: atom()) -> [node()].
% Life cycle
-callback allocated(ClusterName :: atom(), ShardNum :: integer()) -> State :: any().
-callback score(State :: any()) -> integer().
-callback prolonged(Loser :: pid(), State :: any()) -> {ok, NextState :: any()}.
-callback deallocated(Winner :: pid(), State :: any()) -> any().

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
