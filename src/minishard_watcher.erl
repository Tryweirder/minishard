-module(minishard_watcher).
-behavior(gen_server).

-export([start_link/2]).

-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).


name(ClusterName) when is_atom(ClusterName) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_watcher").


-record(watcher, {
        cluster_name,
        callback_mod,
        node_statuses = #{},
        status
        }).

start_link(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    State = seed_state(ClusterName, CallbackMod),
    gen_server:start_link({local, name(ClusterName)}, ?MODULE, State, []).


seed_state(ClusterName, CallbackMod) ->
    Nodes = CallbackMod:nodes(ClusterName),
    NStatuses = maps:from_list([{Node, undefined} || Node <- Nodes]),
    #watcher{
        cluster_name = ClusterName,
        callback_mod = CallbackMod,
        node_statuses = NStatuses,
        status = undefined
        }.


init(#watcher{} = State0) ->
    State = set_poll_timer(State0),
    {ok, export_status(State)}.

handle_info(_, #watcher{} = State) ->
    {noreply, State}.

handle_cast(_, #watcher{} = State) ->
    {noreply, State}.

handle_call(_, _From, #watcher{} = State) ->
    Response = {error, not_implemented},
    {reply, Response, State}.

code_change(_, #watcher{} = State, _) ->
    {ok, State}.

terminate(_, #watcher{}) ->
    ok.





set_poll_timer(#watcher{} = State) ->
    % TODO: cancel old timer and set new one
    State.

export_status(#watcher{status = Status} = State) ->
    put(status, Status),
    State.

