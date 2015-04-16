-module(minishard_global_fixer).
-behavior(gen_server).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, code_change/3, terminate/2]).

%% main logic for calling by user
-export([check_global_server/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, seed_state(), []).


-record(gfixer, {
        check_interval,
        check_timer
        }).

seed_state() ->
    #gfixer{
        check_interval = 5000
        }.

init(#gfixer{} = State) ->
    {ok, set_check_timer(State)}.


handle_info({timeout, Timer, check_global}, #gfixer{check_timer = Timer} = State) ->
    ok = check_global_server(),
    {noreply, set_check_timer(State)};

handle_info(Unexpected, #gfixer{} = State) ->
    error_logger:warning_msg("Minishard global fixer got unexpected message: ~9999p", [Unexpected]),
    {noreply, State}.


handle_cast(Unexpected, #gfixer{} = State) ->
    error_logger:warning_msg("Minishard global fixer got unexpected cast: ~9999p", [Unexpected]),
    {noreply, State}.


handle_call(_, _From, #gfixer{} = State) ->
    {reply, {error, not_implemented}, State}.


code_change(_, #gfixer{} = State, _) ->
    {ok, State}.

terminate(_, #gfixer{}) ->
    ok.


%%% Internals

%% Set timer for the next check
set_check_timer(#gfixer{check_interval = Interval} = State) ->
    Timer = erlang:start_timer(Interval, self(), check_global),
    State#gfixer{check_timer = Timer}.


%% Perform all the checks
check_global_server() ->
    case rpc:call(node(), global, sync, [], 5000) of
        ok ->
            ok;
        {badrpc, timeout} ->
            fix_global_server()
    end.

fix_global_server() ->
    % Hack: this is internal global API (get_synced is unused)
    Known = gen_server:call(global_name_server, get_known),
    Synced = gen_server:call(global_name_server, get_synced),
    % Construct list of potentially problem nodes
    Unsynced = Known -- Synced,
    % Force disconnect on them, causing global to re-sync
    error_logger:error("Minishard global fixer: sync timed out, resetting connections to nodes: ~9999p", [Unsynced]),
    [erlang:disconnect_node(N) || N <- Unsynced],
    check_repaired_global_server(Synced).

check_repaired_global_server(OldSyncedNodes) ->
    case rpc:call(node(), global, sync, [], 5000) of
        ok ->
            ok;
        {badrpc, timeout} ->
            % Something went wrong, so disconnect also nodes that were initially synced
            error_logger:error("Minishard global fixer: second sync timed out, resetting connections to nodes: ~9999p", [OldSyncedNodes]),
            [erlang:disconnect_node(N) || N <- OldSyncedNodes],
            % Ensure everything is OK now or retry
            check_global_server()
    end.

