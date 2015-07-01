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
-export([status/2]).

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
%% TODO: store CallbackMod and do not require user to provide it
status(ClusterName, CallbackMod) ->
    NodesStatus = minishard_watcher:status(ClusterName),
    NodeStatuses = minishard_watcher:current_statuses(ClusterName),
    AllocationMap = minishard_shard:allocation_map(ClusterName, CallbackMod),
    NodeStatusMap = maps:fold(fun allocation_to_node_status/3, NodeStatuses, AllocationMap),
    ClusterStatus = case {NodesStatus, all_shards_allocated(AllocationMap)} of
        {available, false} -> % Enough nodes available, but not all shards allocated
            allocation_pending;
        {_, _} ->
            NodesStatus
    end,
    {ClusterStatus, NodeStatusMap}.

allocation_to_node_status(ShardNum, undefined, NodeStatuses) ->
    maps:put({not_allocated, ShardNum}, undefined, NodeStatuses);
allocation_to_node_status(ShardNum, ShardNode, NodeStatuses) ->
    maps:put(ShardNode, {active, ShardNum}, NodeStatuses).

all_shards_allocated(AllocationMap) ->
    not lists:member(undefined, maps:values(AllocationMap)).
