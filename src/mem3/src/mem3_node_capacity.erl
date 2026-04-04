% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

%% @doc Cluster-wide node capacity tracking.
%%
%% Periodically collects disk capacity from all cluster nodes via RPC.
%% Respects circuit breaker — skips nodes with open circuits.
%% Used by auto-shard to pick optimal target nodes for split placement.

-module(mem3_node_capacity).
-behaviour(gen_server).

-export([
    start_link/0,
    all_capacities/0,
    get_capacity/1,
    best_nodes/3,
    refresh/0
]).

%% For testing
-export([
    score_nodes/2,
    apply_zone_constraints/3
]).

%% Called via RPC from remote nodes
-export([collect_local_capacity_rpc/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(DEFAULT_INTERVAL_MS, 60000). % 1 minute

-record(node_cap, {
    node :: node(),
    zone :: binary(),
    dirs :: [{string(), non_neg_integer(), non_neg_integer(), integer()}],
    shard_count :: non_neg_integer(),
    updated_at :: non_neg_integer()
}).

-record(state, {
    interval_ms :: pos_integer(),
    timer_ref :: undefined | reference(),
    capacities :: #{node() => #node_cap{}}
}).

%% ===================================================================
%% Public API
%% ===================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Get capacities for all known nodes.
-spec all_capacities() -> #{node() => map()}.
all_capacities() ->
    gen_server:call(?MODULE, all_capacities, 10000).

%% @doc Get capacity for a specific node.
-spec get_capacity(node()) -> {ok, map()} | not_found.
get_capacity(Node) ->
    gen_server:call(?MODULE, {get_capacity, Node}, 10000).

%% @doc Pick the best nodes for placing a shard of given size.
%% Returns up to Count nodes sorted by free space, respecting zone rules.
-spec best_nodes(non_neg_integer(), pos_integer(), list()) -> [node()].
best_nodes(ShardSizeBytes, Count, ZoneRules) ->
    Caps = all_capacities(),
    Scored = score_nodes(Caps, ShardSizeBytes),
    Constrained = apply_zone_constraints(Scored, ZoneRules, Count),
    [Node || {_Score, Node} <- lists:sublist(Constrained, Count)].

%% @doc Force an immediate capacity scan.
-spec refresh() -> ok.
refresh() ->
    gen_server:cast(?MODULE, refresh).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    IntervalMs = config:get_integer(
        "auto_shard", "capacity_scan_interval_ms", ?DEFAULT_INTERVAL_MS),
    State = #state{
        interval_ms = IntervalMs,
        capacities = #{}
    },
    {ok, schedule_scan(do_scan(State))}.

handle_call(all_capacities, _From, #state{capacities = Caps} = State) ->
    Result = maps:map(fun(_Node, #node_cap{} = NC) ->
        #{
            node => NC#node_cap.node,
            zone => NC#node_cap.zone,
            dirs => NC#node_cap.dirs,
            shard_count => NC#node_cap.shard_count,
            updated_at => NC#node_cap.updated_at
        }
    end, Caps),
    {reply, Result, State};
handle_call({get_capacity, Node}, _From, #state{capacities = Caps} = State) ->
    case maps:get(Node, Caps, undefined) of
        undefined -> {reply, not_found, State};
        #node_cap{} = NC ->
            {reply, {ok, #{
                node => NC#node_cap.node,
                zone => NC#node_cap.zone,
                dirs => NC#node_cap.dirs,
                shard_count => NC#node_cap.shard_count,
                updated_at => NC#node_cap.updated_at
            }}, State}
    end;
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(refresh, State) ->
    {noreply, do_scan(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(scan, State) ->
    {noreply, schedule_scan(do_scan(State))};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% ===================================================================
%% Internal — scanning
%% ===================================================================

do_scan(#state{capacities = OldCaps} = State) ->
    Nodes = try mem3:nodes() catch _:_ -> [node()] end,
    Now = erlang:system_time(millisecond),
    NewCaps = lists:foldl(fun(Node, Acc) ->
        case is_node_reachable(Node) of
            false -> Acc;
            true ->
                case collect_node_capacity(Node, Now) of
                    {ok, Cap} -> maps:put(Node, Cap, Acc);
                    _Error -> Acc
                end
        end
    end, OldCaps, Nodes),
    State#state{capacities = NewCaps}.

is_node_reachable(Node) when Node =:= node() ->
    true;
is_node_reachable(Node) ->
    try
        case mem3_circuit_breaker:allow(Node) of
            ok -> true;
            {error, circuit_open} -> false
        end
    catch
        _:_ -> true  % circuit breaker not running, assume reachable
    end.

collect_node_capacity(Node, Now) when Node =:= node() ->
    %% Local — direct call, no RPC
    collect_local_capacity(Now);
collect_node_capacity(Node, Now) ->
    %% Remote — RPC with timeout
    try
        case rpc:call(Node, ?MODULE, collect_local_capacity_rpc, [Now], 5000) of
            {badrpc, _Reason} -> {error, rpc_failed};
            Result -> Result
        end
    catch
        _:_ -> {error, rpc_failed}
    end.

collect_local_capacity_rpc(Now) ->
    collect_local_capacity(Now).

collect_local_capacity(Now) ->
    try
        Dirs = couch_disk_monitor:dir_capacities(),
        Zone = get_zone(node()),
        ShardCount = count_local_shards(),
        {ok, #node_cap{
            node = node(),
            zone = Zone,
            dirs = Dirs,
            shard_count = ShardCount,
            updated_at = Now
        }}
    catch
        _:_ -> {error, collect_failed}
    end.

get_zone(Node) ->
    try
        Props = mem3:node_info(Node, <<"zone">>),
        case Props of
            Zone when is_binary(Zone) -> Zone;
            _ -> <<"default">>
        end
    catch
        _:_ -> <<"default">>
    end.

count_local_shards() ->
    try
        {ok, AllDbs} = couch_server:all_databases(),
        length([Db || Db <- AllDbs, mem3_shard_size:is_shard(Db)])
    catch
        _:_ -> 0
    end.

%% ===================================================================
%% Internal — node scoring and placement
%% ===================================================================

%% @doc Score nodes by available free space minus penalty for shard size.
%% Higher score = better target for placement.
-spec score_nodes(#{node() => map()}, non_neg_integer()) -> [{integer(), node()}].
score_nodes(Caps, ShardSizeBytes) when is_map(Caps) ->
    Scored = maps:fold(fun(Node, CapMap, Acc) ->
        Dirs = maps:get(dirs, CapMap, []),
        MaxFree = case Dirs of
            [] -> 0;
            _ -> lists:max([Free || {_Path, _Pct, Free, _Total} <- Dirs])
        end,
        %% Score = free space minus the shard we'd place there
        Score = MaxFree - ShardSizeBytes,
        [{Score, Node} | Acc]
    end, [], Caps),
    lists:reverse(lists:keysort(1, Scored)).

%% @doc Apply zone constraints — ensure results span multiple zones.
%% ZoneRules: list of {Zone, Count} pairs, e.g., [{"us-west", 2}, {"us-east", 1}].
%% Empty ZoneRules means no constraints.
-spec apply_zone_constraints([{integer(), node()}], list(), pos_integer()) ->
    [{integer(), node()}].
apply_zone_constraints(Scored, [], _Count) ->
    Scored;
apply_zone_constraints(Scored, _ZoneRules, _Count) ->
    %% For now, just return scored order. Full zone-aware placement
    %% will be implemented in Phase 5 when we add the rebalance planner.
    Scored.

schedule_scan(#state{timer_ref = OldRef} = State) ->
    cancel_timer(OldRef),
    Ref = erlang:send_after(State#state.interval_ms, self(), scan),
    State#state{timer_ref = Ref}.

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).
