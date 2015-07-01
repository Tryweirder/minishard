%%% Minishard allocator
%%%
%%% This module is a callback module for gen_leader (well, local version of it)
%%% which tracks other members status and decides who runs which shard.
%%%
%%% Leader tracks the cluster status in a map. Each cluster node has a key in the map,
%%% corresponding value is its status. Possible statuses are:
%%%   * down            -- allocator on the node is down
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
-export([start_link/2]).
-export([bind/1]).
-export([get_manager/2, get_node/2]).

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
        ref :: reference()    % Request reference
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

%% Active status
-record(active, {
        shard :: integer()   % Active shard number
        }).

-type node_status() :: down | #request{} | #conflict{} | idle | standby | #active{}.
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
        managers :: manager_map()
        }).


%% ETS data model for shard information
-define(ets_shard_key(Shard), {shard, Shard}).
-define(ets_shard_node_pos, 2).
-define(ets_shard_manager_pos, 3).
-define(ets_shard_record(Shard, Node, Manager), {?ets_shard_key(Shard), Node, Manager}).

%% API: Resolve a shard number to the shard manager pid
get_manager(ClusterName, Shard) ->
    ets:lookup_element(ClusterName, ?ets_shard_key(Shard), ?ets_shard_manager_pos).

%% API: Resolve a shard number to the node currently hosting it
get_node(ClusterName, Shard) ->
    ets:lookup_element(ClusterName, ?ets_shard_key(Shard), ?ets_shard_node_pos).

%% API: start the allocator for given cluster
start_link(ClusterName, CallbackMod) when is_atom(ClusterName), is_atom(CallbackMod) ->
    Options = [{heartbeat, 5}, {bcast_type, all}, {seed_node, none}],
    State0 = #allocator{map = Map} = seed_state(ClusterName, CallbackMod),
    Nodes = maps:keys(Map),
    ?GEN_LEADER:start_link(ClusterName, Nodes, Options, ?MODULE, State0, []).

%% Seed state for a starting allocator
seed_state(ClusterName, CallbackMod) ->
    Nodes = CallbackMod:cluster_nodes(ClusterName),
    SeedMap = maps:from_list([{N, down} || N <- Nodes]),
    SeedManagers = maps:from_list([{N, undefined} || N <- Nodes]),
    #allocator{
        name = ClusterName,
        callback_mod = CallbackMod,
        shard_manager = undefined,
        my_status = idle,
        shard_count = CallbackMod:shard_count(ClusterName),
        map = SeedMap,
        managers = SeedManagers }.


%% Register a shard manager ready to host a shard
bind(ClusterName) ->
    ?GEN_LEADER:call(ClusterName, {bind, self()}).


%% Init: nothing special, we start with an empty map
init(#allocator{name = Name} = State) ->
    Name = ets:new(Name, [protected, named_table, set, {read_concurrency, true}]),
    {ok, State}.


handle_cast(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:warning_msg("Minishard allocator ~w got unexpected cast ~9999p", [Name, Msg]),
    {noreply, State}.

handle_info(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:warning_msg("Minishard allocator ~w got unexpected info ~9999p", [Name, Msg]),
    {noreply, State}.

handle_call({bind, ShardManager}, From, #allocator{name = Name} = State, _Election) ->
    NewState = State#allocator{shard_manager = ShardManager},
    ok = ?GEN_LEADER:leader_cast(Name, {bind_manager, node(), ShardManager, From}),
    {noreply, NewState};
handle_call(_Request, _From, #allocator{} = State, _Election) ->
    {reply, {error, not_implemented}, State}.



%% We are elected. Propagate our allocation map
elected(#allocator{name = Name, map = Map, my_status = MyStatus} = State, Election, Loser) ->
    error_logger:info_msg("Minishard allocator ~w elected, ~w surrendered", [Name, Loser]),
    State1 = State#allocator{map = maps:update(node(), MyStatus, Map)},
    NewState = handle_new_election(Election, State1),
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
            error_logger:info_msg("Minishard allocator ~w adds ~w as good node", [Name, Node]),
            StateWithManager = set_manager(Node, ShardManager, State),
            NewState = #allocator{map = NewMap} = set_realloc_install([Node], standby, StateWithManager),
            _ = ?GEN_LEADER:reply(From, get_allocation(Node, NewMap)),
            {ok, NewState, NewState};
        false ->
            _ = ?GEN_LEADER:reply(From, not_my_cluster),
            {noreply, State}
    end;

handle_leader_cast({request_timeout, RequestRef}, #allocator{} = State, _Election) ->
    case handle_request_timeout(RequestRef, State) of
        {updated, NewState} ->
            {ok, NewState, NewState};
        unchanged ->
            {noreply, State}
    end;

handle_leader_cast({status_update, RequestRef, Node, Status}, #allocator{map = Map} = State, _Election) ->
    case get_allocation(Node, Map) of
        #request{ref = RequestRef} ->
            NewState = handle_possible_conflicts(Node, Status, State),
            {ok, NewState, NewState};
        _ ->
            {noreply, State}
    end;

handle_leader_cast({conflict_timeout, ConflictRef}, #allocator{map = Map} = State, _Election) ->
    {Shard, NodeScores} = conflict_shard_and_scores(ConflictRef, Map),
    NewState = resolve_conflict(Shard, NodeScores, State),
    {ok, NewState, NewState};

handle_leader_cast(#score_report{ref = ReportRef, node = Node, score = Score}, #allocator{map = Map} = State, _Election) ->
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

handle_leader_cast(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:warning_msg("Minishard allocator ~w got unexpected leader cast ~9999p", [Name, Msg]),
    {noreply, State}.


from_leader(#allocator{map = NewMap}, #allocator{name = Name} = State, _Election) ->
    error_logger:info_msg("Minishard allocator ~w got update from the leader.", [Name]),
    {ok, install_new_map(NewMap, State)};

from_leader(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:info_msg("Minishard allocator ~w got a message from the leader: ~9999p", [Name, Msg]),
    {ok, State}.



terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, #allocator{} = State, _Election, _Extra) ->
    {ok, State}.




%% When status request times out, we mark nodes which did not send their status update as idle
handle_request_timeout(RequestRef, #allocator{map = Map, shard_count = ShardCount} = State) ->
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
            NewMap = reallocate(ShardCount, set_statuses(StalledNodes, idle, Map)),
            {updated, install_new_map(NewMap, State)}
    end.


%% The leader has been elected. He has Election from a gen_leader and an outdated map.
%% Here we mark nodes going down as down and request a status from nodes going up
handle_new_election(Election, #allocator{name = Name, map = Map, shard_count = ShardCount} = State) ->
    % Determine which nodes need a status request
    OldAlive = alive_nodes(State),
    Alive = ?GEN_LEADER:alive(Election),
    BecameAlive = Alive -- OldAlive,

    % Determine which nodes should be marked as down
    OldDown = down_nodes(State),
    Down = ?GEN_LEADER:down(Election),
    BecameDown = Down -- OldDown,

    % Apply status changes
    MapMarkedDown = set_statuses(BecameDown, down, Map),
    MapMarkedReq = case BecameAlive of
        [] ->
            MapMarkedDown;
        [_|_] ->
            % Set timer to handle possible troubles during status request
            RequestRef = make_ref(),
            {ok, _} = timer:apply_after(2000, ?GEN_LEADER, leader_cast, [Name, {request_timeout, RequestRef}]),

            % Request statuses
            set_statuses(BecameAlive, #request{ref = RequestRef}, MapMarkedDown)
    end,

    StateManagersRemoved = lists:foldl(fun remove_manager/2, State, BecameDown),
    NewMap = reallocate(ShardCount, MapMarkedReq),
    install_new_map(NewMap, StateManagersRemoved).

%% Remember node's bound manager
-spec set_manager(Node :: node(), Manager :: undefined | pid(), #allocator{}) -> #allocator{}.
set_manager(Node, ShardManager, #allocator{managers = ManMap} = State) ->
    true = maps:is_key(Node, ManMap),
    NewManMap = maps:update(Node, ShardManager, ManMap),
    State#allocator{managers = NewManMap}.

%% manager map cleaner compatible with lists:foldl
-spec remove_manager(Node :: node(), #allocator{}) -> #allocator{}.
remove_manager(Node, #allocator{} = State) ->
    set_manager(Node, undefined, State).

%% Set given status for a given list of nodes, reallocate shards, install the updated map
-spec set_realloc_install(Nodes :: [node()], Status :: node_status(), #allocator{}) -> #allocator{}.
set_realloc_install(Nodes, Status, #allocator{map = Map, shard_count = ShardCount} = State) ->
    MapUpdated = set_statuses(Nodes, Status, Map),
    NewMap = reallocate(ShardCount, MapUpdated),
    NewState = install_new_map(NewMap, State),
    export_shard_map(NewState),
    NewState.

%% batch set status for given list of nodes
-spec set_statuses(Nodes :: [node()], Status :: node_status(), Map :: allocation_map()) -> allocation_map().
set_statuses(Nodes, Status, Map) ->
    [] = Nodes -- maps:keys(Map),
    OverrideMap = maps:from_list([{N, Status} || N <- Nodes]),
    maps:merge(Map, OverrideMap).


%% Perform shard allocation when possible
-spec reallocate(ShardCount :: integer(), Map :: allocation_map()) -> allocation_map().
reallocate(ShardCount, #{} = Map) when is_integer(ShardCount) ->
    AllStatuses = maps:values(Map),
    case lists:any(fun is_request/1, AllStatuses) of
        true -> % Do not reallocate when there is active status request
            Map;
        false ->
            do_reallocate(ShardCount, Map)
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
-spec install_new_map(Map :: allocation_map(), #allocator{}) -> #allocator{}.
install_new_map(OldMap, #allocator{map = OldMap} = State) ->
    State;
install_new_map(NewMap, #allocator{name = Name} = State) ->
    error_logger:info_msg("Minishard allocator ~w: installing new map ~9999p", [Name, NewMap]),
    MyNewStatus = get_allocation(node(), NewMap),
    handle_my_new_status(MyNewStatus, State#allocator{map = NewMap}).


%% Leader has possibly changed our status. Let's see what we should do
-spec handle_my_new_status(node_status(), #allocator{}) -> #allocator{}.
handle_my_new_status(OldStatus, #allocator{my_status = OldStatus} = State) ->
    % Unchanged, pass
    State;
handle_my_new_status(#request{ref = Ref}, #allocator{last_response = Ref} = State) ->
    % We have already sent a status update for this request
    State;
handle_my_new_status(#request{ref = Ref}, #allocator{name = Name, my_status = MyStatus} = State) ->
    % New status request. Send an update and wait
    ?GEN_LEADER:leader_cast(Name, {status_update, Ref, node(), MyStatus}),
    State#allocator{last_response = Ref};
handle_my_new_status(#conflict{ref = Ref}, #allocator{last_response = Ref} = State) ->
    % We have already sent a score for this conflict
    State;
handle_my_new_status(#conflict{ref = Ref}, #allocator{name = Name, shard_manager = ShManager} = State) ->
    % New score request. Send an update and wait
    case minishard_shard:get_score_or_kill(ShManager) of
        undefined ->
            throw({stop, shard_score_timeout, State});
        Score when is_integer(Score) ->
            Report = #score_report{ref = Ref, node = node(), score = Score},
            ?GEN_LEADER:leader_cast(Name, Report),
            State#allocator{last_response = Ref}
    end;
handle_my_new_status(down, State) ->
    throw({stop, shut_down_by_leader, State});
handle_my_new_status(Status, #allocator{} = State) ->
    State#allocator{my_status = Status}.


%% Export a shard map to the ETS
export_shard_map(#allocator{name = Name, shard_count = ShardCount, map = NodeMap, managers = ManagerMap}) ->
    SeedShardMap = maps:from_list([{Shard, undefined} || Shard <- lists:seq(1, ShardCount)]),
    ShardNodeMap = maps:fold(fun collect_active_shards/3, SeedShardMap, NodeMap),
    _ = maps:fold(fun export_shard_info/3, {Name, ManagerMap}, ShardNodeMap),
    ok.

collect_active_shards(Node, #active{shard = Shard}, ShardNodeMap) ->
    maps:update(Shard, Node, ShardNodeMap);
collect_active_shards(_Node, _Status, ShardNodeMap) ->
    ShardNodeMap.

export_shard_info(Shard, Node, {Name, ManagerMap}) ->
    Manager = maps:get(Node, ManagerMap, undefined),
    true = ets:insert(Name, ?ets_shard_record(Shard, Node, Manager)),
    {Name, ManagerMap}.

%% A node has sent a valid status update. We should check the updated map for conflicts and maybe start resolution
-spec handle_possible_conflicts(Node :: node(), Status :: node_status(), #allocator{}) -> #allocator{}.
handle_possible_conflicts(Node, #active{shard = Shard} = Status, #allocator{name = Name, map = Map} = State) ->
    case shard_nodes(Shard, Map) of
        [] -> % no other candidates for this shard
            set_realloc_install([Node], Status, State);
        [_|_] = ConflictingNodes -> % Oops, we have a conflict
            CRef = make_ref(),
            Conflict = #conflict{shard = Shard, ref = CRef, score = undefined},
            {ok, _} = timer:apply_after(2000, ?GEN_LEADER, leader_cast, [Name, {conflict_timeout, CRef}]),
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
    maps:fold(fun collect_alive/3, [], Map).

collect_alive(_Node, down, Acc) -> Acc;
collect_alive(Node, _, Acc) -> [Node|Acc].

%% Helper: list nodes marked as down in a map
down_nodes(#allocator{map = Map}) ->
    maps:fold(fun collect_down/3, [], Map).

collect_down(Node, down, Acc) -> [Node|Acc];
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
