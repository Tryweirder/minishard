%%% Minishard allocator
%%%
%%% This module is a callback module for gen_leader (well, local version of it)
%%% which tracks other members status and decides who runs which shard.
%%%
%%% Leader tracks the cluster status in a map. Each cluster node has a key in the map,
%%% corresponding value is its status. Possible statuses are:
%%%   * down            -- allocator on the node is down
%%%   * {request, Ref}  -- allocator is up, waiting for it to send its status
%%%   * idle            -- allocator is up, but shard manager has not been bound
%%%   * standby         -- allocator is up with a bound shard manager without a shard
%%%   * {active, N}     -- allocator is up with a bound shard manager hosting shard N
%%%
-module(minishard_allocator).
-define(GEN_LEADER, minishard_gen_leader).

-behavior(?GEN_LEADER).


%% API
-export([start_link/2]).
-export([bind/1]).

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


-record(allocator, {
        name,
        callback_mod,
        my_status,
        last_response,
        shard_manager,
        shard_count,
        map}).


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
    #allocator{
        name = ClusterName,
        callback_mod = CallbackMod,
        shard_manager = undefined,
        my_status = idle,
        shard_count = CallbackMod:shard_count(ClusterName),
        map = SeedMap }.


%% Register a shard manager ready to host a shard
bind(ClusterName) ->
    ?GEN_LEADER:call(ClusterName, {bind, self()}).


%% Init: nothing special, we start with an empty map
init(#allocator{} = State) ->
    {ok, State}.


handle_cast(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:warning_msg("Minishard allocator ~w got unexpected cast ~9999p", [Name, Msg]),
    {noreply, State}.

handle_info(Msg, #allocator{name = Name} = State, _Election) ->
    error_logger:warning_msg("Minishard allocator ~w got unexpected info ~9999p", [Name, Msg]),
    {noreply, State}.

handle_call({bind, Shard}, From, #allocator{name = Name} = State, _Election) ->
    link(Shard),
    NewState = State#allocator{shard_manager = Shard},
    ok = ?GEN_LEADER:leader_cast(Name, {node_ready, node(), From}),
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

%% Helper: batch set status for given list of nodes
set_statuses(Nodes, Status, Map) ->
    [] = Nodes -- maps:keys(Map),
    OverrideMap = maps:from_list([{N, Status} || N <- Nodes]),
    maps:merge(Map, OverrideMap).

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
            set_statuses(BecameAlive, {request, RequestRef}, MapMarkedDown)
    end,

    NewMap = reallocate(ShardCount, MapMarkedReq),
    install_new_map(NewMap, State).


%% Perform shard allocation when possible
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
    AllocatedShards = [Sh || {active, Sh} <- maps:values(Map)],

    ShardsToAllocate = Shards -- AllocatedShards,
    StandbyNodes = maps:fold(fun collect_standby_nodes/3, [], Map),

    Allocations = safe_zip(StandbyNodes, ShardsToAllocate),

    MapOverride = maps:from_list([{Node, {active, Shard}} || {Node, Shard} <- Allocations]),
    maps:merge(Map, MapOverride).

%% Helper: check if node status in map is 'status requested'
is_request({request, _}) -> true;
is_request(_) -> false.

%% Helper: add standby nodes to the accumulator
collect_standby_nodes(Node, standby, Acc) -> [Node|Acc];
collect_standby_nodes(_Node, _, Acc) -> Acc.

%% Helper: do the same as lists:zip/2, but stop when any list ends
safe_zip([H1|L1], [H2|L2]) ->
    [{H1, H2}|safe_zip(L1, L2)];
safe_zip(_L1, _L2) ->
    [].


get_allocation(Node, #{} = Map) ->
    case maps:find(Node, Map) of
        {ok, Status} -> Status;
        error -> undefined
    end.

%% We have surrendered. Inherit a new allocation map
surrendered(#allocator{name = Name} = State, _Synch, _Election) ->
    error_logger:info_msg("Minishard allocator ~w surrendered", [Name]),
    {ok, State}.


handle_leader_call(_Request, _From, State, _Election) ->
    {reply, {error, not_implemented}, State}.

handle_leader_cast({node_ready, Node, From}, #allocator{name = Name, map = Map, shard_count = ShardCount} = State, _Election) ->
    error_logger:info_msg("Minishard allocator ~w adds ~w as good node", [Name, Node]),
    NewMap = reallocate(ShardCount, set_statuses([Node], standby, Map)),
    NewState = install_new_map(NewMap, State),
    _ = ?GEN_LEADER:reply(From, get_allocation(Node, NewMap)),
    {ok, NewState, NewState};

handle_leader_cast({request_timeout, RequestRef}, #allocator{} = State, _Election) ->
    case handle_request_timeout(RequestRef, State) of
        {updated, NewState} ->
            {ok, NewState, NewState};
        unchanged ->
            {noreply, State}
    end;

handle_leader_cast({status_update, RequestRef, Node, Status}, #allocator{map = Map, shard_count = ShardCount} = State, _Election) ->
    case get_allocation(Node, Map) of
        {request, RequestRef} ->
            NewMap = reallocate(ShardCount, set_statuses([Node], Status, Map)),
            NewState = install_new_map(NewMap, State),
            {ok, NewState, NewState};
        _ ->
            {noreply, State}
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
    MatchingStatus = {request, RequestRef},
    StalledNodes = maps:fold(fun
                (Node, Status, Acc) when Status == MatchingStatus ->
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


install_new_map(OldMap, #allocator{map = OldMap} = State) ->
    State;
install_new_map(NewMap, #allocator{name = Name} = State) ->
    error_logger:info_msg("Minishard allocator ~w: installing new map ~9999p", [Name, NewMap]),
    MyNewStatus = get_allocation(node(), NewMap),
    handle_my_new_status(MyNewStatus, State#allocator{map = NewMap}).


%% Leader has possibly changed our status. Let's see what we should do
handle_my_new_status(OldStatus, #allocator{my_status = OldStatus} = State) ->
    % Unchanged, pass
    State;
handle_my_new_status({request, Ref}, #allocator{last_response = Ref} = State) ->
    % We have already sent a status update for this request
    State;
handle_my_new_status({request, Ref}, #allocator{name = Name, my_status = MyStatus} = State) ->
    % New status request. Send an update and wait
    ?GEN_LEADER:leader_cast(Name, {status_update, Ref, node(), MyStatus}),
    State#allocator{last_response = Ref};
handle_my_new_status(down, State) ->
    throw({stop, shut_down_by_leader, State});
handle_my_new_status(Status, #allocator{} = State) ->
    State#allocator{my_status = Status}.


