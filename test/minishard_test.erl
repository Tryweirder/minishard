-module(minishard_test).

-export([set_config/1, set_config/2, start/0, start/1, map/0, map/1]).

-behavior(minishard).
-export([shard_count/1, cluster_nodes/1, score/1, prolonged/2, allocated/2, deallocated/2]).


start() ->
    start(test).

start(Name) ->
    application:ensure_all_started(lager),
    application:ensure_all_started(minishard),
    %error_logger:info_msg("Minishard config: ~120p~n", [application:get_all_env(minishard)]),
    minishard:join(Name, ?MODULE).


map() ->
    map(test).
map(Name) ->
    minishard_allocator:shard_map(Name).


set_config(Config) ->
    set_config(test, Config).

set_config(Name, Config) ->
    application:load(minishard),
    ClustersConf = application:get_env(minishard, clusters, []),
    NewClustersConf = lists:ukeymerge(1, [{Name, Config}], ClustersConf),
    application:set_env(minishard, clusters, NewClustersConf),
    ok.

get_config(Name) ->
    ClustersConf = application:get_env(minishard, clusters, []),
    proplists:get_value(Name, ClustersConf, []).

get_conf_value(Name, Key, Default) ->
    MyConf = get_config(Name),
    proplists:get_value(Key, MyConf, Default).

shard_count(Name) ->
    get_conf_value(Name, shard_count, 3).
cluster_nodes(Name) ->
    get_conf_value(Name, nodes, [node()]).

-record(shaman, {
        name,
        shard,
        started_at
        }).

allocated(Name, Shard) ->
    {ok, #shaman{name = Name, shard = Shard, started_at = os:timestamp()}}.

score(#shaman{started_at = Started}) ->
    timer:now_diff(os:timestamp(), Started)/1000000.

prolonged(_, State) ->
    {ok, State}.

deallocated(_, State) ->
    {ok, State}.
