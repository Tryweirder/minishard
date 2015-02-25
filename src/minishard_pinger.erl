-module(minishard_pinger).
-behavior(gen_server).

-export([start_link/3, status/1]).

-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

name(ClusterName, Node) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_" ++ atom_to_list(Node) ++ "_watcher").

start_link(ClusterName, Node, Watcher) when is_atom(ClusterName), is_atom(Node), is_pid(Watcher) ->
    State = seed_state(ClusterName, Node, Watcher),
    gen_server:start_link({local, name(ClusterName, Node)}, ?MODULE, State, []).

status(Pinger) when is_pid(Pinger) ->
    case process_info(Pinger, dictionary) of
        undefined ->
            undefined;
        {dictionary, Dict} ->
            proplists:get_value(status, Dict)
    end.

-record(pinger, {
        cluster_name,
        node,
        watcher,
        status
        }).

seed_state(ClusterName, Node, Watcher) ->
    #pinger{
        cluster_name = ClusterName,
        node = Node,
        watcher = Watcher,
        status = undefined
        }.



init(#pinger{} = State) ->
    {ok, export_status(State)}.

handle_info(_, #pinger{} = State) ->
    {noreply, State}.

handle_cast(_, #pinger{} = State) ->
    {noreply, State}.

handle_call(_, _From, #pinger{} = State) ->
    Response = {error, not_implemented},
    {reply, Response, State}.

code_change(_, #pinger{} = State, _) ->
    {ok, State}.

terminate(_, #pinger{}) ->
    ok.



export_status(#pinger{status = Status} = State) ->
    put(status, Status),
    State.

