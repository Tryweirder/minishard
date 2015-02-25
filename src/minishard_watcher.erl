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
        status,
        pingers_sup,
        poll_timer
        }).

-define(POLL_INTERVAL, 1000).

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
        status = undefined,
        poll_timer = undefined
        }.


init(#watcher{} = State0) ->
    State = set_poll_timer(State0),
    {ok, export_status(State)}.

handle_info({timeout, PollTimer, poll_pingers}, #watcher{poll_timer = PollTimer} = State) ->
    NewState = set_poll_timer(poll_pingers(State)),
    {noreply, NewState};
handle_info({timeout, _WrongTimer, poll_pingers}, #watcher{} = State) ->
    {noreply, State};
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





set_poll_timer(#watcher{poll_timer = Timer} = State) when is_reference(Timer) ->
    _ = erlang:cancel_timer(Timer),
    set_poll_timer(State#watcher{poll_timer = undefined});
set_poll_timer(#watcher{poll_timer = undefined} = State) ->
    Timer = erlang:start_timer(?POLL_INTERVAL, self(), poll_pingers),
    State#watcher{poll_timer = Timer}.


export_status(#watcher{status = Status} = State) ->
    put(status, Status),
    State.


poll_pingers(#watcher{pingers_sup = PingersSup, node_statuses = NodeStatuses} = State) when is_pid(PingersSup) ->
    Pingers = minishard_sup:pingers(PingersSup),
    SeenNodes = maps:keys(NodeStatuses),
    PingedNodes = [N || {N, _} <- Pingers],

    UnpingedNodes = SeenNodes -- PingedNodes,
    _NewPingers = [{N, new_pinger(N, State)} || N <- UnpingedNodes],
    ZeroStatuses = maps:from_list([{N, undefined} || N <- UnpingedNodes]),

    RefreshedStatuses = maps:from_list([{N, poll_status(Pid)} || {N, Pid} <- Pingers]),

    NewStatuses = maps:merge(ZeroStatuses, RefreshedStatuses),
    State#watcher{node_statuses = NewStatuses};

poll_pingers(#watcher{cluster_name = ClusterName} = State) ->
    PingersSup = minishard_sup:get_pid(ClusterName, pingers),
    poll_pingers(State#watcher{pingers_sup = PingersSup}).


new_pinger(Node, #watcher{pingers_sup = PingersSup, cluster_name = ClusterName}) ->
    {ok, Pinger} = minishard_sup:add_pinger(PingersSup, ClusterName, Node, self()),
    Pinger.


poll_status(Pid) when is_pid(Pid) ->
    minishard_pinger:status(Pid).
