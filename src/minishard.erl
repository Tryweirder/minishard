-module(minishard).
-behaviour(application).

-type score() :: number().
-export_type([score/0]).

%%% Behavior callbacks
% Configuration
-callback shard_count(ClusterName :: atom()) -> integer().
-callback cluster_nodes(ClusterName :: atom()) -> [node()].
% Life cycle
-callback allocated(ClusterName :: atom(), ShardNum :: integer()) -> State :: any().
-callback score(State :: any()) -> score().
-callback prolonged(Loser :: pid(), State :: any()) -> {ok, NextState :: any()}.
-callback deallocated(Winner :: pid(), State :: any()) -> any().

% API
-export([start/0]).
-export([join/2, get_node/2, get_manager/2]).
-export([status/1, status/2]).

% Application callbacks
-export([start/2, stop/1]).


start() ->
    application:start(?MODULE, permanent).


start(_Type, _Args) ->
    minishard_sup:start_link(root).

stop(_State) ->
    ok.


%% Join the cluster
join(ClusterName, CallbackMod) ->
    minishard_sup:join_cluster(ClusterName, CallbackMod).

%% Resolve a shard number to the shard manager pid
get_manager(ClusterName, ShardNum) ->
    minishard_allocator:get_manager(ClusterName, ShardNum).

%% Resolve a shard number to the node currently hosting it
get_node(ClusterName, ShardNum) ->
    minishard_allocator:get_node(ClusterName, ShardNum).

%% Cluster status
status(ClusterName) ->
    minishard_allocator:cluster_status(ClusterName).

status(ClusterName, _CallbackMod) -> % old API compatibility
    {Status, _Counts, NodeMap} = status(ClusterName),
    {Status, NodeMap}.
