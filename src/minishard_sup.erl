-module(minishard_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([join_cluster/2, get_pid/2, add_pinger/3, add_pinger/4, pingers/1]).
-export([init/1]).

sup_name(root) ->
    minishard;
sup_name({cluster, ClusterName, _}) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_sup");
sup_name({pingers, ClusterName}) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_pingers").

% Helper: get pid of started infrastructure part
get_pid(ClusterName, pingers) when is_atom(ClusterName) ->
    whereis(sup_name({pingers, ClusterName}));
get_pid(ClusterName, PartName) when is_atom(ClusterName), is_atom(PartName) ->
    Sup = sup_name({cluster, ClusterName, undefined}),
    Children = supervisor:which_children(Sup),
    case lists:keyfind(PartName, 1, Children) of
        {PartName, Pid, _, _} -> Pid;
        false -> undefined
    end.


start_link(Arg) ->
	supervisor:start_link({local, sup_name(Arg)}, ?MODULE, Arg).

join_cluster(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    ClusterSpec = {ClusterName,
                   {?MODULE, start_link, [{cluster, ClusterName, CallbackMod}]},
                   permanent, 10000, supervisor, []},
    supervisor:start_child(sup_name(root), ClusterSpec).

add_pinger(ClusterName, Node, Watcher) when is_atom(ClusterName), is_atom(Node), is_pid(Watcher) ->
    PingersSup = whereis(sup_name({pingers, ClusterName})),
    add_pinger(PingersSup, ClusterName, Node, Watcher).

add_pinger(PingersSup, ClusterName, Node, Watcher) when is_pid(PingersSup), is_atom(Node), is_pid(Watcher) ->
    PingerSpec = {Node,
                  {minishard_pinger, start_link, [ClusterName, Node, Watcher]},
                  permanent, 100, worker, [minishard_pinger]},
    supervisor:start_child(PingersSup, PingerSpec).

pingers(ClusterName) when is_atom(ClusterName) ->
    pingers(get_pid(ClusterName, pingers));
pingers(PingersSup) when is_pid(PingersSup) ->
    Children = supervisor:which_children(PingersSup),
    [{Node, Pid} || {Node, Pid, _, _} <- Children].

init(root) ->
    {ok, {{one_for_one, 1, 5}, []}};

init({cluster, ClusterName, CallbackMod}) ->
    WatcherSpec = {watcher,
                   {minishard_watcher, start_link, [ClusterName, CallbackMod]},
                   permanent, 1000, worker, [minishard_watcher]},
    PingersSpec = {pingers,
                   {?MODULE, start_link, [{pingers, ClusterName}]},
                   permanent, 1000, supervisor, []},

    {ok, {{one_for_all, 1, 5}, [WatcherSpec, PingersSpec]}};

init({pingers, _}) ->
    {ok, {{one_for_one, 1, 5}, []}}.

