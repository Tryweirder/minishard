%%% Minishard allocator
%%%
%%% This module is a callback module for gen_leader (well, local version of it)
%%% which tracks other members status and decides who runs which shard.
%%%
%%% Leader tracks the cluster status in a map. Each cluster node has a key in the map,
%%% corresponding value is its status. Possible statuses are:
%%%   * down            -- allocator on the node is down
%%%   * #transition{}   -- allocator has recently went down, waiting for it to come back
%%%   * #request{}      -- allocator is up, waiting for it to send its status
%%%   * #conflict{}     -- allocator is up and hosts a conflicting shard. Waiting for it to send its score
%%%   * idle            -- allocator is up, but shard manager has not been bound
%%%   * standby         -- allocator is up with a bound shard manager without a shard
%%%   * #active{}       -- allocator is up with a bound shard manager hosting shard N
%%%
-module(minishard_allocator).
-define(GEN_LEADER, minishard_gen_leader).

-behavior(?GEN_LEADER).


%% API
-export([name/1]).
-export([start_link/2, cluster_status/1, shard_map/1]).
-export([bind/1]).
-export([get_manager/2, get_node/2]).

%% Testing/debugging
-export([seed_state/3, set_hacks/2]).

%% gen_leader callbacks
-export([
    init/1,
    handle_cast/3,
    handle_call/4,
    handle_info/3,
    handle_leader_call/4,
    handle_leader_cast/3,
    handle_DOWN/3,
    elected/3,
    surrendered/3,
    from_leader/3,
    code_change/4,
    terminate/2]).


%% Candidate status request
-record(request, {
        ref :: reference()              % Request reference
        }).
-record(status_update, {
        ref :: reference(),             % Request reference
        node  :: node(),                % Reporting node
        status :: node_status(),        % Reported status
        manager :: undefined | pid()    % Current node's shard manager
        }).

%% Conflict resolution status
-record(conflict, {
        shard :: integer(),                     % Conflicting shard number
        ref   :: reference(),                   % Resolution reference
        score :: undefined | minishard:score()  % Score reported by a member
        }).
%% Conflict score report
-record(score_report, {
        ref   :: reference(),                   % Resolution reference
        node  :: node(),                        % Reporting node
        score :: minishard:score()              % Reported score
        }).

%% Temporary state for nodes going down. Without this shard is reallocated even on interconnect socket reset
-record(transition, {
        shard   :: integer(),                   % Shard number just before disconnect
        ref     :: reference()                  % transition reference
        }).

%% Active status
-record(active, {
        shard :: integer()   % Active shard number
        }).

-type request() :: #request{}.
-type conflict() :: #conflict{}.
-type transition() :: #transition{}.
-type active() :: #active{}.
-type node_status() :: down | request() | conflict() | transition() | idle | standby | active().
-type allocation_map() :: map(node(), node_status()).
-type manager_map() :: map(node(), undefined | pid()).

%% gen_leader callback state
-record(allocator, {
        name :: atom(),
        callback_mod :: module(),
        my_status :: node_status(),
        last_response :: reference(),
        shard_manager :: undefined | pid(),
        shard_count :: integer(),
        map :: allocation_map(),
        managers :: manager_map(),
        hacks :: map(atom(), any())
        }).

-type state() :: #allocator{}.


%% ETS data model for shard information
-define(ETS_SHARD_KEY(Shard), {shard, Shard}).
-define(ETS_SHARD_NODE_POS, 2).
-define(ETS_SHARD_MANAGER_POS, 3).
-define(ETS_SHARD_RECORD(Shard, Node, Manager), {?ETS_SHARD_KEY(Shard), Node, Manager}).

%% Generate a process/ets name for a cluster name
name(ClusterName) ->
    list_to_atom("minishard_" ++ atom_to_list(ClusterName) ++ "_allocator").

%% API: Resolve a shard number to the shard manager pid
get_manager(ClusterName, Shard) ->
    ets:lookup_element(name(ClusterName), ?ETS_SHARD_KEY(Shard), ?ETS_SHARD_MANAGER_POS).

%% API: Resolve a shard number to the node currently hosting it
get_node(ClusterName, Shard) ->
    ets:lookup_element(name(ClusterName), ?ETS_SHARD_KEY(Shard), ?ETS_SHARD_NODE_POS).

%% API: start the allocator for given cluster
start_link(ClusterName, CallbackMod) ->
    start_link(ClusterName, CallbackMod, #{}).

start_link(ClusterName, CallbackMod, #{} = Hacks) when is_atom(ClusterName), is_atom(CallbackMod) ->
    Name = name(ClusterName),
    State0 = #allocator{map = Map} = seed_state(ClusterName, CallbackMod, Hacks),
    Nodes = maps:keys(Map),
    Options = leader_worker_options(Nodes) ++ [{heartbeat, 5}, {bcast_type, all}, {seed_node, none}],
    ?GEN_LEADER:start_link(Name, Nodes, Options, ?MODULE, State0, []).

leader_worker_options(Nodes) ->
    case lists:member(node(), Nodes) of
        true -> [];
        false -> [{workers, [node()]}]
    end.

%% Test/debug API: set hacks for a running allocator
set_hacks(ClusterName, #{} = Hacks) when is_atom(ClusterName) ->
    ?GEN_LEADER:call(name(ClusterName), {set_hacks, Hacks}).

%% Seed state for a starting allocator
seed_state(ClusterName, CallbackMod, Hacks) ->
    Nodes = CallbackMod:cluster_nodes(ClusterName),
    MyStatus = case lists:member(node(), Nodes) of
        true -> idle;
        false -> worker
    end,
    SeedMap = maps:from_list([{N, down} || N <- Nodes]),
    SeedManagers = maps:from_list([{N, undefined} || N <- Nodes]),
    #allocator{
        name = name(ClusterName),
        callback_mod = CallbackMod,
        shard_manager = undefined,
        my_status = MyStatus,
        shard_count = CallbackMod:shard_count(ClusterName),
        map = SeedMap,
        managers = SeedManagers,
        hacks = Hacks }.


%% Register a shard manager ready to host a shard
bind(ClusterName) ->
    ?GEN_LEADER:call(name(ClusterName), {bind, self()}, 120000).

%% Helper for possible asynchronous manager reply
manager_reply(undefined, _) ->
    ok;
manager_reply(From, Reply) ->
    ?GEN_LEADER:reply(From, Reply).


%% Return cluster status in form {OverallStatusAtom, NodeStatusMap}
cluster_status(ClusterName) when is_atom(ClusterName) ->
    ?GEN_LEADER:call(name(ClusterName), cluster_status).

%% Return shard allocation map
-spec shard_map(ClusterName :: atom()) -> map(Shard :: integer(), node()).
shard_map(ClusterName) ->
    %ets:foldl(fun collect_shard_map/2, #{}, name(ClusterName)).
    maps:from_list([{Shard, Node} || ?ETS_SHARD_RECORD(Shard, Node, _) <- ets:tab2list(name(ClusterName))]).

%% Init: nothing special, we start with an empty map
init(#allocator{name = Name} = State) ->
    Name = ets:new(Name, [protected, named_table, set, {read_concurrency, true}]),
    ok = export_shard_map(State),
    {ok, State}.


handle_cast(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:warning_msg("Minishard allocator ~w got unexpected cast ~9999p", [Name, Msg]),
    {noreply, State}.

handle_info(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:warning_msg("Minishard allocator ~w got unexpected info ~9999p", [Name, Msg]),
    {noreply, State}.

handle_call({bind, ShardManager}, _From, #allocator{name = Name} = State, _Election) ->
    NewState = State#allocator{shard_manager = ShardManager},
    ok = ?GEN_LEADER:leader_cast(Name, {bind_manager, node(), ShardManager, undefined}),
    {reply, standby, NewState};
handle_call(cluster_status, _From, #allocator{} = State, Election) ->
    {reply, make_cluster_status(State, Election), State};
handle_call({set_hacks, Hacks}, _From, #allocator{} = State, _Election) ->
    {reply, ok, State#allocator{hacks = Hacks}};
handle_call(_Request, _From, #allocator{} = State, _Election) ->
    {reply, {error, not_implemented}, State}.



%% We are elected. Propagate our allocation map
elected(#allocator{name = Name} = State, Election, Loser) ->
    error_logger:info_msg("Minishard allocator ~w elected, ~w surrendered", [Name, Loser]),
    StateRequestsRestarted = restart_requests(State),
    NewState = handle_new_election(Election, StateRequestsRestarted),
    {ok, NewState, NewState}.


%% Node goes down. Deallocate its shard and remove from pool
handle_DOWN(Node, #allocator{name = Name} = State, Election) ->
    error_logger:info_msg("Minishard allocator ~w has seen ~w's death", [Name, Node]),
    NewState = handle_new_election(Election, State),
    {ok, NewState, NewState}.



get_allocation(Node, #{} = Map) ->
    case maps:find(Node, Map) of
        {ok, Status} -> Status;
        error -> undefined
    end.

%% We have surrendered. Inherit a new allocation map
surrendered(#allocator{name = Name} = State, #allocator{} = Synch, Election) ->
    error_logger:info_msg("Minishard allocator ~w surrendered, forwarding Synch to from_leader/3", [Name]),
    from_leader(Synch, State, Election);
surrendered(#allocator{name = Name} = State, _Synch, _Election) ->
    error_logger:info_msg("Minishard allocator ~w surrendered", [Name]),
    {ok, State}.


handle_leader_call(_Request, _From, State, _Election) ->
    {reply, {error, not_implemented}, State}.

handle_leader_cast({bind_manager, Node, ShardManager, From}, #allocator{map = Map, name = Name} = State, _Election) ->
    case maps:is_key(Node, Map) of
        true ->
            error_logger:info_msg("Minishard allocator ~w *** LEADER *** adds ~w as good node", [Name, Node]),
            StateWithManager = set_manager(Node, ShardManager, State),
            NewState = #allocator{map = NewMap} = set_realloc_install([Node], standby, StateWithManager),
            _ = manager_reply(From, get_allocation(Node, NewMap)),
            {ok, NewState, NewState};
        false ->
            _ = manager_reply(From, not_my_cluster),
            {noreply, State}
    end;

handle_leader_cast({request_timeout, RequestRef}, #allocator{name = Name} = State, _Election) ->
    case handle_request_timeout(RequestRef, State) of
        {updated, NewState} ->
            error_logger:info_msg("Minishard allocator ~w *** LEADER *** status update ~w timeout", [Name, RequestRef]),
            {ok, NewState, NewState};
        unchanged ->
            {noreply, State}
    end;

handle_leader_cast(#status_update{ref = RequestRef, node = Node, status = Status, manager = Manager},
                   #allocator{name = Name, map = Map} = State, _Election) ->
    error_logger:info_msg("Minishard allocator ~w *** LEADER *** got a status update from ~w (~w)",
                          [Name, Node, Status]),
    case get_allocation(Node, Map) of
        #request{ref = RequestRef} ->
            StateWithManager = set_manager(Node, Manager, State),
            NewState = handle_possible_conflicts(Node, Status, StateWithManager),
            {ok, NewState, NewState};
        _ ->
            {noreply, State}
    end;

handle_leader_cast({conflict_timeout, ConflictRef}, #allocator{name = Name, map = Map} = State, _Election) ->
    {Shard, NodeScores} = conflict_shard_and_scores(ConflictRef, Map),
    Shard /= undefined andalso error_logger:info_msg(
        "Minishard allocator ~w *** LEADER *** conflict ~w (shard ~w) timeout", [Name, ConflictRef, Shard]),
    NewState = resolve_conflict(Shard, NodeScores, State),
    {ok, NewState, NewState};

handle_leader_cast(#score_report{ref = ReportRef, node = Node, score = Score},
                   #allocator{name = Name, map = Map} = State, _Election) ->
    error_logger:info_msg("Minishard allocator ~w *** LEADER *** got a score report from ~w (~w)", [Name, Node, Score]),
    NewMap = case get_allocation(Node, Map) of
        #conflict{ref = ReportRef} = Conflict ->
            set_statuses([Node], Conflict#conflict{score = Score}, Map);
        _ ->
            Map
    end,
    NewState = install_new_map(NewMap, State),
    {Shard, NodeScores} = conflict_shard_and_scores(ReportRef, NewMap),
    case lists:keymember(undefined, 2, NodeScores) of
        true -> % Still have pending score requests, no action needed
            {noreply, NewState};
        false -> % All nodes have reported their scores, ok to resolve conflict now
            ResolvedState = resolve_conflict(Shard, NodeScores, State),
            {ok, ResolvedState, ResolvedState}
    end;

handle_leader_cast({transition_timeout, TransRef}, #allocator{name = Name} = State, _Election) ->
    case handle_transition_timeout(TransRef, State) of
        {updated, NewState} ->
            error_logger:info_msg("Minishard allocator ~w *** LEADER *** transition ~w finished", [Name, TransRef]),
            {ok, NewState, NewState};
        unchanged ->
            {noreply, State}
    end;

handle_leader_cast(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:warning_msg("Minishard allocator ~w got unexpected leader cast ~9999p", [Name, Msg]),
    {noreply, State}.


from_leader(#allocator{map = NewMap, managers = ManagerMap}, #allocator{name = Name} = State, _Election) ->
    error_logger:info_msg("Minishard allocator ~w got update from the leader.", [Name]),
    {ok, install_new_map(NewMap, State#allocator{managers = ManagerMap})};

from_leader(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:info_msg("Minishard allocator ~w got a message from the leader: ~9999p", [Name, Msg]),
    {ok, State}.



terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, #allocator{} = State, _Election, _Extra) ->
    {ok, State}.




%% When status request times out, we mark nodes which did not send their status update as idle
handle_request_timeout(RequestRef, #allocator{map = Map} = State) ->
    StalledNodes = maps:fold(fun
                (Node, #request{ref = NodeRef}, Acc) when NodeRef == RequestRef ->
                    [Node|Acc];
                (_Node, _Status, Acc) ->
                    Acc
            end, [], Map),
    case StalledNodes of
        [] ->
            unchanged;
        [_|_] ->
            NewState = set_realloc_install(StalledNodes, idle, State),
            {updated, NewState}
    end.

%% Transition timeout: here we mark nodes as really down
handle_transition_timeout(TransRef, #allocator{map = Map} = State) ->
    ReallyDownNodes = maps:fold(fun
                (Node, #transition{ref = NodeRef}, Acc) when NodeRef == TransRef ->
                    [Node|Acc];
                (_Node, _Status, Acc) ->
                    Acc
            end, [], Map),
    case ReallyDownNodes of
        [] ->
            unchanged;
        [_|_] ->
            NewState = set_realloc_install(ReallyDownNodes, down, State),
            {updated, NewState}
    end.

%% The leader has been elected. He has Election from a gen_leader and an outdated map.
%% Here we mark nodes going down as down and request a status from nodes going up
handle_new_election(Election, #allocator{name = Name} = State) ->
    % Determine which nodes need a status request
    OldAlive = alive_nodes(State),
    Alive = ?GEN_LEADER:alive(Election),
    BecameAlive = Alive -- OldAlive,

    % Determine which nodes should be marked as down
    OldDown = down_nodes(State),
    Down = ?GEN_LEADER:down(Election),
    BecameDown = Down -- OldDown,

    % Apply status changes
    StateDownMarked = lists:foldl(fun handle_node_down/2, State, BecameDown),

    AliveStatus = case BecameAlive of
        [] -> % No status will be set, so here we may return any one
            idle;
        [_|_] ->
            % Request statuses
            {Request, _Timer} = make_status_request(Name),
            Request
    end,

    set_realloc_install(BecameAlive, AliveStatus, StateDownMarked).


-spec make_status_request(Name :: atom()) -> {request(), timer:tref()}.
make_status_request(Name) ->
    % Set timer to handle possible troubles during status request
    RequestRef = make_ref(),
    Request = #request{ref = RequestRef},
    {ok, Timer} = timer:apply_after(2000, ?GEN_LEADER, leader_cast, [Name, {request_timeout, RequestRef}]),
    {Request, Timer}.

-spec make_conflict_request(Name :: atom(), Shard :: integer()) -> {conflict(), timer:tref()}.
make_conflict_request(Name, Shard) ->
    CRef = make_ref(),
    Conflict = #conflict{shard = Shard, ref = CRef, score = undefined},
    {ok, Timer} = timer:apply_after(2000, ?GEN_LEADER, leader_cast, [Name, {conflict_timeout, CRef}]),
    {Conflict, Timer}.

-spec make_transition(Name :: atom(), Shard :: integer()) -> {transition(), timer:tref()}.
make_transition(Name, Shard) ->
    TransRef = make_ref(),
    Request = #transition{ref = TransRef, shard = Shard},
    {ok, Timer} = timer:apply_after(5000, ?GEN_LEADER, leader_cast, [Name, {transition_timeout, TransRef}]),
    {Request, Timer}.


%% Restart all running requests. When leader changes during request, response may be lost.
%% So we search for all status and score requests, then generate new references for them, starting corresponding timers
restart_requests(#allocator{name = Name, map = Map} = State) ->
    {Name, NewMap, _RefMigration} = maps:fold(fun restart_request/3, {Name, #{}, #{}}, Map),
    State#allocator{map = NewMap}.

restart_request(Node, #request{ref = OldRef} = OldRequest, {Name, Map, RefMigration}) ->
    NewRequest = #request{ref = NewRef} = case maps:get(OldRef, RefMigration, undefined) of
        undefined ->
            {NewRequest_, _} = make_status_request(Name),
            NewRequest_;
        ExistingRef ->
            OldRequest#request{ref = ExistingRef}
    end,
    restart_request_store(Name, Node, NewRequest, OldRef, NewRef, Map, RefMigration);
restart_request(Node, #conflict{ref = OldRef, shard = Shard} = OldRequest, {Name, Map, RefMigration}) ->
    NewRequest = #conflict{ref = NewRef} = case maps:get(OldRef, RefMigration, undefined) of
        undefined ->
            {NewRequest_, _} = make_conflict_request(Name, Shard),
            NewRequest_;
        ExistingRef ->
            OldRequest#conflict{ref = ExistingRef, score = undefined}
    end,
    restart_request_store(Name, Node, NewRequest, OldRef, NewRef, Map, RefMigration);
restart_request(Node, #transition{ref = OldRef, shard = Shard} = OldRequest, {Name, Map, RefMigration}) ->
    NewRequest = #transition{ref = NewRef} = case maps:get(OldRef, RefMigration, undefined) of
        undefined ->
            {NewRequest_, _} = make_transition(Name, Shard),
            NewRequest_;
        ExistingRef ->
            OldRequest#transition{ref = ExistingRef}
    end,
    restart_request_store(Name, Node, NewRequest, OldRef, NewRef, Map, RefMigration);
restart_request(Node, NotRequest, {Name, Map, RefMigration}) ->
    NewMap = maps:put(Node, NotRequest, Map),
    {Name, NewMap, RefMigration}.

restart_request_store(Name, Node, NewRequest, OldRef, NewRef, Map, RefMigration) ->
    NewMap = maps:put(Node, NewRequest, Map),
    NewRefMigration = maps:put(OldRef, NewRef, RefMigration),
    {Name, NewMap, NewRefMigration}.


handle_node_down(Node, #allocator{name = Name, map = Map} = State) ->
    NewStatus = case get_allocation(Node, Map) of
        #active{shard = Shard} ->
            {Transition, _} = make_transition(Name, Shard),
            Transition;
        _ ->
            down
    end,

    NewMap = set_statuses([Node], NewStatus, Map),
    set_manager(Node, undefined, State#allocator{map = NewMap}).

%% Remember node's bound manager
-spec set_manager(Node :: node(), Manager :: undefined | pid(), state()) -> state().
set_manager(Node, ShardManager, #allocator{managers = ManMap} = State) ->
    true = maps:is_key(Node, ManMap),
    NewManMap = maps:update(Node, ShardManager, ManMap),
    State#allocator{managers = NewManMap}.


%% Set given status for a given list of nodes, reallocate shards, install the updated map
-spec set_realloc_install(Nodes :: [node()], Status :: node_status(), state()) -> state().
set_realloc_install(Nodes, Status, #allocator{map = Map, shard_count = ShardCount} = State) ->
    MapUpdated = set_statuses(Nodes, Status, Map),
    NewMap = reallocate(ShardCount, MapUpdated),
    install_new_map(NewMap, State).

%% batch set status for given list of nodes
-spec set_statuses(Nodes :: [node()], Status :: node_status(), Map :: allocation_map()) -> allocation_map().
set_statuses(Nodes, Status, Map) ->
    [] = Nodes -- maps:keys(Map),
    OverrideMap = maps:from_list([{N, Status} || N <- Nodes]),
    maps:merge(Map, OverrideMap).


%% Perform shard allocation when possible
-spec reallocate(ShardCount :: integer(), Map :: allocation_map()) -> allocation_map().
reallocate(ShardCount, #{} = Map) when is_integer(ShardCount) ->
    HaveQuorum = (length(alive_nodes(Map)) >= ShardCount),
    HavePendingReq = lists:any(fun is_request/1, maps:values(Map)),
    case {HaveQuorum, HavePendingReq} of
        {true, false} -> % Require quorum and no status requests for reallocation
            do_reallocate(ShardCount, Map);
        {_, _} ->
            Map
    end.

do_reallocate(ShardCount, #{} = Map) ->
    Shards = lists:seq(1, ShardCount),
    AllocatedShards = shards_in_use(Map),

    ShardsToAllocate = Shards -- AllocatedShards,
    StandbyNodes = maps:fold(fun collect_standby_nodes/3, [], Map),

    Allocations = safe_zip(StandbyNodes, ShardsToAllocate),

    MapOverride = maps:from_list([{Node, #active{shard = Shard}} || {Node, Shard} <- Allocations]),
    maps:merge(Map, MapOverride).


%% Install a new allocation map and perform all needed actions
-spec install_new_map(Map :: allocation_map(), state()) -> state().
install_new_map(NewMap, #allocator{name = Name} = State) ->
    error_logger:info_msg("Minishard allocator ~w: installing new map ~9999p", [Name, NewMap]),
    MyNewStatus = get_allocation(node(), NewMap),
    NewState = handle_my_new_status(MyNewStatus, State#allocator{map = NewMap}),
    ok = export_shard_map(NewState),
    NewState.


%% Leader has possibly changed our status. Let's see what we should do
-spec handle_my_new_status(node_status(), state()) -> state().
handle_my_new_status(undefined, #allocator{my_status = worker} = State) ->
    % I am worker, so my status is always worker, and I don't appear in a map
    State;
handle_my_new_status(OldStatus, #allocator{my_status = OldStatus} = State) ->
    % Unchanged, pass
    State;
handle_my_new_status(#request{ref = Ref}, #allocator{last_response = Ref} = State) ->
    % We have already sent a status update for this request
    State;
handle_my_new_status(#request{ref = Ref}, #allocator{
                name = Name, my_status = MyStatus, shard_manager = Manager, hacks = Hacks} = State) ->
    apply_hack(on_status_request, Hacks),
    % New status request. Send an update and wait
    Report = #status_update{ref = Ref, node = node(), status = MyStatus, manager = Manager},
    ?GEN_LEADER:leader_cast(Name, Report),
    State#allocator{last_response = Ref};
handle_my_new_status(#conflict{ref = Ref}, #allocator{last_response = Ref} = State) ->
    % We have already sent a score for this conflict
    State;
handle_my_new_status(#conflict{ref = Ref}, #allocator{name = Name, shard_manager = ShManager, hacks = Hacks} = State) ->
    apply_hack(on_conflict_request, Hacks),
    % New score request. Send an update and wait
    case minishard_shard:get_score_or_kill(ShManager) of
        undefined ->
            throw({stop, shard_score_timeout, State});
        Score when is_integer(Score) ->
            Report = #score_report{ref = Ref, node = node(), score = Score},
            ?GEN_LEADER:leader_cast(Name, Report),
            State#allocator{last_response = Ref}
    end;
handle_my_new_status(down, #allocator{shard_manager = Manager} = State) ->
    ok = set_manager_status(Manager, idle),
    throw({stop, {shutdown, shut_down_by_leader}, State});
handle_my_new_status(#active{shard = NewShard}, #allocator{my_status = #active{shard = OldShard}} = State)
        when NewShard /= OldShard ->
    throw({stop, {shard_suddenly_changed, OldShard, NewShard}, State});
handle_my_new_status(#active{shard = OldShard} = Status, #allocator{my_status = #active{shard = OldShard}} = State) ->
    State#allocator{my_status = Status};
handle_my_new_status(Status, #allocator{shard_manager = Manager} = State)
        when Status == idle; Status == standby; is_record(Status, active) ->
    ok = set_manager_status(Manager, Status),
    State#allocator{my_status = Status}.




%% Export a shard map to the ETS
export_shard_map(#allocator{name = Name, managers = ManagerMap} = State) ->
    ShardNodeMap = shard_node_map(State),
    _ = maps:fold(fun export_shard_info/3, {Name, ManagerMap}, ShardNodeMap),
    ok.

shard_node_map(#allocator{shard_count = ShardCount, map = NodeMap}) ->
    SeedShardMap = maps:from_list([{Shard, undefined} || Shard <- lists:seq(1, ShardCount)]),
    maps:fold(fun collect_active_shards/3, SeedShardMap, NodeMap).

collect_active_shards(Node, #active{shard = Shard}, ShardNodeMap) ->
    maps:update(Shard, Node, ShardNodeMap);
collect_active_shards(_Node, _Status, ShardNodeMap) ->
    ShardNodeMap.

export_shard_info(Shard, Node, {Name, ManagerMap}) ->
    Manager = maps:get(Node, ManagerMap, undefined),
    true = ets:insert(Name, ?ETS_SHARD_RECORD(Shard, Node, Manager)),
    {Name, ManagerMap}.


%% Set shard manager status when leader updates it
-spec set_manager_status(Manager :: undefined | pid(), Status :: idle | standby | active()) -> ok.
set_manager_status(undefined, _Status) -> % No manager, pass
    ok;
set_manager_status(Manager, Status) ->
    minishard_shard:set_status(Manager, node_status_for_manager(Status)).

node_status_for_manager(idle) -> idle;
node_status_for_manager(standby) -> standby;
node_status_for_manager(#active{shard = Shard}) -> {active, Shard}.


%% A node has sent a valid status update. We should check the updated map for conflicts and maybe start resolution
-spec handle_possible_conflicts(Node :: node(), Status :: node_status(), state()) -> state().
handle_possible_conflicts(Node, #active{shard = Shard} = Status,
                          #allocator{name = Name, map = Map, hacks = Hacks} = State) ->
    case shard_nodes(Shard, Map) of
        [] -> % no other candidates for this shard
            set_realloc_install([Node], Status, State);
        [_|_] = ConflictingNodes -> % Oops, we have a conflict
            apply_hack(on_conflict_detected, Hacks),
            {Conflict, _} = make_conflict_request(Name, Shard),
            set_realloc_install([Node|ConflictingNodes], Conflict, State)
    end;

handle_possible_conflicts(Node, Status, #allocator{} = State) ->
    set_realloc_install([Node], Status, State).


resolve_conflict(undefined, [], #allocator{} = State) ->
    % No conflict, pass
    State;
resolve_conflict(Shard, [_|_] = NodeScores, #allocator{map = Map} = State) ->
    {Winner, _} = select_winner(NodeScores),
    MapWithWinner = case Winner of
        undefined -> Map;
        _RealWinner -> set_statuses([Winner], #active{shard = Shard}, Map)
    end,
    Losers = [Node || {Node, _} <- NodeScores, Node /= Winner],
    NewMap = set_statuses(Losers, down, MapWithWinner),
    install_new_map(NewMap, State).


%% Helper: list nodes marked as alive in a map
alive_nodes(#allocator{map = Map}) ->
    alive_nodes(Map);
alive_nodes(#{} = Map) ->
    maps:fold(fun collect_alive/3, [], Map).

collect_alive(_Node, down, Acc) -> Acc;
collect_alive(_Node, #transition{}, Acc) -> Acc;
collect_alive(Node, _, Acc) -> [Node|Acc].

%% Helper: list nodes marked as down in a map
down_nodes(#allocator{map = Map}) ->
    maps:fold(fun collect_down/3, [], Map).

collect_down(Node, down, Acc) -> [Node|Acc];
collect_down(Node, #transition{}, Acc) -> [Node|Acc];
collect_down(_Node, _, Acc) -> Acc.


%% Helper: check if node status in map is 'status requested'
is_request(#request{}) -> true;
is_request(_) -> false.

%% Helper: return a list of shards in use
shards_in_use(Map) ->
    maps:fold(fun collect_shards_in_use/3, [], Map).

%% Helper: add active and conflicting shards to the accumulator
collect_shards_in_use(_Node, #active{shard = Shard}, Acc) -> [Shard|Acc];
collect_shards_in_use(_Node, #conflict{shard = Shard}, Acc) -> [Shard|Acc];
collect_shards_in_use(_Node, #transition{shard = Shard}, Acc) -> [Shard|Acc];
collect_shards_in_use(_Node, _, Acc) -> Acc.

%% Get a list of nodes pretending to host the shard
shard_nodes(Shard, Map) ->
    {Shard, Nodes} = maps:fold(fun collect_nodes_by_shard/3, {Shard, []}, Map),
    Nodes.

%% Helper: when node wants to host the shard, add it to the accumulator
collect_nodes_by_shard(Node, #active{shard = Shard}, {Shard, Acc}) -> {Shard, [Node|Acc]};
collect_nodes_by_shard(Node, #conflict{shard = Shard}, {Shard, Acc}) -> {Shard, [Node|Acc]};
collect_nodes_by_shard(_Node, _, {Shard, Acc}) -> {Shard, Acc}.

%% Helper: add standby nodes to the accumulator
collect_standby_nodes(Node, standby, Acc) -> [Node|Acc];
collect_standby_nodes(_Node, _, Acc) -> Acc.

%% Helper: do the same as lists:zip/2, but stop when any list ends
safe_zip([H1|L1], [H2|L2]) ->
    [{H1, H2}|safe_zip(L1, L2)];
safe_zip(_L1, _L2) ->
    [].

%% Helper: get conflict shard and node scores by a conflict ref
conflict_shard_and_scores(Ref, Map) ->
    {Ref, Shard, NodeScores} = maps:fold(fun collect_conflict_shard_scores/3, {Ref, undefined, []}, Map),
    {Shard, NodeScores}.

collect_conflict_shard_scores(Node, #conflict{ref = Ref, shard = Shard, score = Score}, {Ref, _, NodeScores}) ->
    {Ref, Shard, [{Node, Score}|NodeScores]};
collect_conflict_shard_scores(_Node, _, {Ref, Shard, NodeScores}) ->
    {Ref, Shard, NodeScores}.

%% Helper: select a winner from given {Node, Score} list when possible
-spec select_winner([{node(), number()}]) -> {Winner :: undefined | node(), BestScore :: undefined | number()}.
select_winner(NodeScores) ->
    lists:foldl(fun find_best_score/2, {undefined, undefined}, NodeScores).

find_best_score({BestNode, BestScore}, {_Node, undefined}) ->
    {BestNode, BestScore};
find_best_score({_BestNode, undefined}, {Node, Score}) ->
    {Node, Score};
find_best_score({BestNode, BestScore}, {_Node, Score}) when BestScore >= Score ->
    {BestNode, BestScore};
find_best_score({_Node, _PrevBestScore}, {BetterNode, BetterScore}) ->
    {BetterNode, BetterScore}.


%% Build cluster status report
make_cluster_status(#allocator{shard_count = ShardCount, map = NodeMap} = State, _Election) ->
    TotalNodeCnt = length(maps:keys(NodeMap)),
    AliveNodesCnt = length(alive_nodes(State)),
    ExportNodeMap = maps:map(fun export_node_status/2, NodeMap),
    ShardNodeMap = shard_node_map(State),
    NodeStatusMap = maps:fold(fun allocation_to_node_status/3, ExportNodeMap, ShardNodeMap),
    AllocatedShardCnt = length(lists:filter(fun(N) -> N /= undefined end, maps:values(ShardNodeMap))),
    OverallStatus = overall_status(ShardCount, AllocatedShardCnt, AliveNodesCnt),

    Counts = #{shards => {ShardCount, AllocatedShardCnt}, nodes => {TotalNodeCnt, AliveNodesCnt}},
    {OverallStatus, Counts, NodeStatusMap}.

-spec overall_status(ShardCount :: integer(), AllocatedShardCount :: integer(), AliveNodesCount :: integer()) -> atom().
overall_status(ShardCount, ShardCount, _AliveNodesCnt) ->
    available;
overall_status(ShardCount, _AllocCnt, AliveNodesCnt) when AliveNodesCnt < ShardCount ->
    degraded;
overall_status(_ShardCount, AllocatedShardCnt, AliveNodesCnt) when AllocatedShardCnt < AliveNodesCnt ->
    allocation_pending;
overall_status(_ShardCount, _AllocatedShardCnt, _AliveNodesCount) ->
    transition.

allocation_to_node_status(ShardNum, undefined, NodeStatuses) ->
    maps:put({not_allocated, ShardNum}, undefined, NodeStatuses);
allocation_to_node_status(ShardNum, ShardNode, NodeStatuses) ->
    maps:put(ShardNode, {active, ShardNum}, NodeStatuses).

%% Translate node statuses to minishard external status format
export_node_status(_Node, down) -> unavailable;
export_node_status(_Node, idle) -> not_my_cluster;
export_node_status(_Node, standby) -> available;
export_node_status(_Node, #active{shard = Shard}) -> {active, Shard};
export_node_status(_Node, _) -> transition.


%% Hacks: apply a hack
apply_hack(HackName, Hacks) ->
    case maps:get(HackName, Hacks, undefined) of
        undefined -> ok;
        Fun when is_function(Fun, 0) -> Fun();
        {M, F, A} when is_atom(M), is_atom(F), is_list(A) -> apply(M, F, A)
    end.


%collect_shard_map(?ETS_SHARD_RECORD(Shard, Node, _), Map) ->
%    maps:put(Shard, Node, Map);
%collect_shard_map(_, Map) ->
%    Map.
