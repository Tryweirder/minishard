-module(minishard_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([join_cluster/2, get_pid/2]).
-export([init/1]).

%% For embedding minishard cluster in any supervision tree
-export([cluster_child_spec/2, cluster_internal_specs/2]).

sup_name(root) ->
    minishard;
sup_name({cluster, ClusterName, _}) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_sup").

% Helper: get pid of started infrastructure part
get_pid(undefined, _) ->
    throw(undefined_cluster);
get_pid(ClusterName, shard) when is_atom(ClusterName) ->
    strict_whereis(minishard_shard:name(ClusterName));
get_pid(ClusterName, PartName) when is_atom(ClusterName), is_atom(PartName) ->
    Sup = sup_name({cluster, ClusterName, undefined}),
    Children = supervisor:which_children(Sup),
    case lists:keyfind(PartName, 1, Children) of
        {PartName, Pid, _, _} -> Pid;
        false -> undefined
    end.

strict_whereis(ProcessName) when is_atom(ProcessName) ->
    Pid = whereis(ProcessName),
    Pid == undefined andalso error(no_cluster),
    Pid.

start_link(Arg) ->
    supervisor:start_link({local, sup_name(Arg)}, ?MODULE, Arg).

cluster_child_spec(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    {ClusterName,
     {?MODULE, start_link, [{cluster, ClusterName, CallbackMod}]},
     permanent, 10000, supervisor, []}.

allocator_spec(ClusterName, CallbackMod) ->
    {allocator,
     {minishard_allocator, start_link, [ClusterName, CallbackMod]},
     permanent, 1000, worker, [minishard_allocator]}.

shard_spec(ClusterName, CallbackMod) ->
    {shard,
     {minishard_shard, start_link, [ClusterName, CallbackMod]},
     permanent, 1000, worker, [minishard_shard]}.

cluster_internal_specs(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    [allocator_spec(ClusterName, CallbackMod), shard_spec(ClusterName, CallbackMod)].


join_cluster(ClusterName, CallbackMod) ->
    supervisor:start_child(sup_name(root), cluster_child_spec(ClusterName, CallbackMod)).



init(root) ->
    {ok, {{one_for_one, 1, 5}, []}};

init({cluster, ClusterName, CallbackMod}) ->
    {ok, {{one_for_all, 5, 10}, cluster_internal_specs(ClusterName, CallbackMod)}}.
