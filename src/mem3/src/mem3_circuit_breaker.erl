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

%% @doc Circuit breaker for cross-datacenter node communication.
%%
%% Designed for multi-DC clusters where inter-DC links have different
%% baseline latencies (e.g. 1ms local, 10ms nearby, 30ms far) and
%% cable breaks cause failover to degraded backup routes (e.g. 30ms → 200ms).
%%
%% Key features:
%%   - Per-node latency baseline learning (EMA)
%%   - Degradation detection: latency > baseline × degradation_factor
%%   - Flap dampening: exponential backoff on recovery_wait when cable flaps
%%   - Fast-fail: open circuit returns immediately, no wasted time
%%
%% States:
%%   closed    → Node healthy, requests pass through
%%   degraded  → Node slow (backup route), requests allowed but tracked
%%   open      → Node unreachable/too slow, requests fast-fail
%%   half_open → Probe: one request allowed to test recovery
%%
%% Config [circuit_breaker]:
%%   failure_threshold = 3        — consecutive failures to open
%%   recovery_wait_ms = 10000     — wait before probe (base, before backoff)
%%   max_recovery_wait_ms = 120000 — max backoff cap (2 minutes)
%%   slow_threshold_ms = 5000     — absolute slow threshold
%%   degradation_factor = 5       — latency × factor = degraded
%%   flap_window_sec = 60         — window for counting flaps
%%   flap_threshold = 3           — flaps in window to trigger backoff

-module(mem3_circuit_breaker).

-behaviour(gen_server).

-export([
    start_link/0,
    allow/1,
    record_success/1,
    record_failure/1,
    record_slow/2,
    record_latency/2,
    node_state/1,
    all_states/0,
    reset/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(node_st, {
    state = closed,              % closed | degraded | open | half_open
    failures = 0,                % consecutive failure count
    last_failure_time = 0,       % erlang:monotonic_time(millisecond)
    last_success_time = 0,
    total_failures = 0,
    total_successes = 0,
    baseline_latency_ms = 0,     % learned normal latency (EMA, slow-adapting)
    current_latency_ms = 0,      % recent latency (EMA, fast-adapting)
    flap_count = 0,              % open→closed transitions in flap window
    flap_window_start = 0,       % start of flap detection window (ms)
    current_recovery_wait = 0    % 0 = use default, >0 = backoff applied
}).

-record(st, {
    nodes = #{}
}).

%% ===================================================================
%% API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec allow(node()) -> ok | {error, circuit_open}.
allow(Node) ->
    case Node =:= node() of
        true -> ok;
        false ->
            try gen_server:call(?MODULE, {allow, Node}, 1000)
            catch
                exit:{timeout, _} -> ok;
                exit:{noproc, _} -> ok
            end
    end.

-spec record_success(node()) -> ok.
record_success(Node) ->
    gen_server:cast(?MODULE, {success, Node}).

-spec record_failure(node()) -> ok.
record_failure(Node) ->
    gen_server:cast(?MODULE, {failure, Node}).

-spec record_slow(node(), pos_integer()) -> ok.
record_slow(Node, LatencyMs) ->
    gen_server:cast(?MODULE, {slow, Node, LatencyMs}).

%% @doc Record a successful response with measured latency.
%% Updates both baseline (slow EMA) and current (fast EMA) latency.
%% Detects degradation when current >> baseline (cable failover).
-spec record_latency(node(), pos_integer()) -> ok.
record_latency(Node, LatencyMs) ->
    gen_server:cast(?MODULE, {latency, Node, LatencyMs}).

-spec node_state(node()) -> map().
node_state(Node) ->
    gen_server:call(?MODULE, {node_state, Node}, 5000).

-spec all_states() -> map().
all_states() ->
    gen_server:call(?MODULE, all_states, 5000).

-spec reset(node()) -> ok.
reset(Node) ->
    gen_server:cast(?MODULE, {reset, Node}).

%% ===================================================================
%% gen_server
%% ===================================================================

init([]) ->
    {ok, #st{}}.

handle_call({allow, Node}, _From, #st{nodes = Nodes} = St) ->
    NodeSt = maps:get(Node, Nodes, #node_st{}),
    case NodeSt#node_st.state of
        closed ->
            {reply, ok, St};
        degraded ->
            % Degraded path (backup route) — still allow requests but
            % the caller knows latency is elevated
            {reply, ok, St};
        half_open ->
            {reply, ok, St};
        open ->
            Now = erlang:monotonic_time(millisecond),
            EffectiveWait = effective_recovery_wait(NodeSt),
            TimeSinceFailure = Now - NodeSt#node_st.last_failure_time,
            case TimeSinceFailure >= EffectiveWait of
                true ->
                    NewNodeSt = NodeSt#node_st{state = half_open},
                    NewNodes = Nodes#{Node => NewNodeSt},
                    couch_log:notice(
                        "Circuit breaker: ~s half-open after ~Bms "
                        "(recovery_wait=~Bms, flaps=~B), allowing probe",
                        [Node, TimeSinceFailure, EffectiveWait,
                         NodeSt#node_st.flap_count]
                    ),
                    {reply, ok, St#st{nodes = NewNodes}};
                false ->
                    {reply, {error, circuit_open}, St}
            end
    end;

handle_call({node_state, Node}, _From, #st{nodes = Nodes} = St) ->
    NodeSt = maps:get(Node, Nodes, #node_st{}),
    Reply = #{
        state => NodeSt#node_st.state,
        failures => NodeSt#node_st.failures,
        total_failures => NodeSt#node_st.total_failures,
        total_successes => NodeSt#node_st.total_successes,
        baseline_latency_ms => NodeSt#node_st.baseline_latency_ms,
        current_latency_ms => NodeSt#node_st.current_latency_ms,
        flap_count => NodeSt#node_st.flap_count,
        current_recovery_wait => effective_recovery_wait(NodeSt)
    },
    {reply, Reply, St};

handle_call(all_states, _From, #st{nodes = Nodes} = St) ->
    Reply = maps:map(fun(_Node, NodeSt) ->
        #{
            state => NodeSt#node_st.state,
            failures => NodeSt#node_st.failures,
            baseline_latency_ms => NodeSt#node_st.baseline_latency_ms,
            current_latency_ms => NodeSt#node_st.current_latency_ms,
            flap_count => NodeSt#node_st.flap_count
        }
    end, Nodes),
    {reply, Reply, St};

handle_call(_Msg, _From, St) ->
    {reply, {error, unknown}, St}.

handle_cast({success, Node}, #st{nodes = Nodes} = St) ->
    NodeSt0 = maps:get(Node, Nodes, #node_st{}),
    Now = erlang:monotonic_time(millisecond),
    WasOpen = NodeSt0#node_st.state =:= open orelse
              NodeSt0#node_st.state =:= half_open,
    NodeSt1 = NodeSt0#node_st{
        state = closed,
        failures = 0,
        last_success_time = Now,
        total_successes = NodeSt0#node_st.total_successes + 1
    },
    % Track flaps: an open→closed transition is a flap
    NodeSt2 = case WasOpen of
        true ->
            track_flap(Node, NodeSt1, Now);
        false ->
            NodeSt1
    end,
    case WasOpen of
        true ->
            couch_log:notice(
                "Circuit breaker: ~s recovered (closed, flaps=~B, "
                "recovery_wait=~Bms)",
                [Node, NodeSt2#node_st.flap_count,
                 effective_recovery_wait(NodeSt2)]);
        false -> ok
    end,
    {noreply, St#st{nodes = Nodes#{Node => NodeSt2}}};

handle_cast({failure, Node}, #st{nodes = Nodes} = St) ->
    NodeSt = maps:get(Node, Nodes, #node_st{}),
    Now = erlang:monotonic_time(millisecond),
    NewFailures = NodeSt#node_st.failures + 1,
    Threshold = failure_threshold(),
    NewState = case NewFailures >= Threshold of
        true ->
            case NodeSt#node_st.state of
                open -> open;
                _ ->
                    couch_log:warning(
                        "Circuit breaker: ~s OPEN after ~B failures "
                        "(baseline=~Bms, current=~Bms)",
                        [Node, NewFailures,
                         NodeSt#node_st.baseline_latency_ms,
                         NodeSt#node_st.current_latency_ms]
                    ),
                    open
            end;
        false ->
            NodeSt#node_st.state
    end,
    NewNodeSt = NodeSt#node_st{
        state = NewState,
        failures = NewFailures,
        last_failure_time = Now,
        total_failures = NodeSt#node_st.total_failures + 1
    },
    {noreply, St#st{nodes = Nodes#{Node => NewNodeSt}}};

handle_cast({latency, Node, LatencyMs}, #st{nodes = Nodes} = St) ->
    NodeSt = maps:get(Node, Nodes, #node_st{}),
    % Baseline: slow-adapting EMA (alpha=0.05) — learns normal latency
    % For 30ms link: converges to ~30ms over ~20 samples
    OldBaseline = NodeSt#node_st.baseline_latency_ms,
    NewBaseline = case OldBaseline of
        0 -> LatencyMs;
        _ -> round(0.05 * LatencyMs + 0.95 * OldBaseline)
    end,
    % Current: fast-adapting EMA (alpha=0.4) — tracks recent changes
    % Detects 30ms→200ms shift within 3-4 samples
    OldCurrent = NodeSt#node_st.current_latency_ms,
    NewCurrent = case OldCurrent of
        0 -> LatencyMs;
        _ -> round(0.4 * LatencyMs + 0.6 * OldCurrent)
    end,
    % Detect degradation: current >> baseline (cable failover to backup route)
    DegFactor = degradation_factor(),
    NewState = case NewBaseline > 0 andalso NewCurrent > NewBaseline * DegFactor of
        true ->
            case NodeSt#node_st.state of
                open -> open;
                degraded -> degraded;
                _ ->
                    couch_log:warning(
                        "Circuit breaker: ~s DEGRADED — latency ~Bms "
                        "(baseline ~Bms, factor ~Bx exceeded)",
                        [Node, NewCurrent, NewBaseline, DegFactor]
                    ),
                    degraded
            end;
        false ->
            case NodeSt#node_st.state of
                degraded ->
                    couch_log:notice(
                        "Circuit breaker: ~s recovered from degradation — "
                        "latency ~Bms (baseline ~Bms)",
                        [Node, NewCurrent, NewBaseline]
                    ),
                    closed;
                Other -> Other
            end
    end,
    NewNodeSt = NodeSt#node_st{
        baseline_latency_ms = NewBaseline,
        current_latency_ms = NewCurrent,
        state = NewState,
        last_success_time = erlang:monotonic_time(millisecond),
        total_successes = NodeSt#node_st.total_successes + 1
    },
    {noreply, St#st{nodes = Nodes#{Node => NewNodeSt}}};

handle_cast({slow, Node, LatencyMs}, #st{nodes = Nodes} = St) ->
    NodeSt = maps:get(Node, Nodes, #node_st{}),
    OldAvg = NodeSt#node_st.current_latency_ms,
    NewAvg = case OldAvg of
        0 -> LatencyMs;
        _ -> round(0.3 * LatencyMs + 0.7 * OldAvg)
    end,
    SlowThreshold = slow_threshold_ms(),
    NewNodeSt = case NewAvg > SlowThreshold of
        true ->
            handle_slow_as_failure(Node, NodeSt#node_st{current_latency_ms = NewAvg});
        false ->
            NodeSt#node_st{current_latency_ms = NewAvg}
    end,
    {noreply, St#st{nodes = Nodes#{Node => NewNodeSt}}};

handle_cast({reset, Node}, #st{nodes = Nodes} = St) ->
    couch_log:notice("Circuit breaker: ~s manually reset", [Node]),
    {noreply, St#st{nodes = Nodes#{Node => #node_st{}}}};

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(_Msg, St) ->
    {noreply, St}.

%% ===================================================================
%% Flap detection and exponential backoff
%% ===================================================================

%% Track an open→closed transition as a flap.
%% If flaps exceed threshold within the window, double recovery_wait.
track_flap(Node, #node_st{} = NodeSt, Now) ->
    WindowMs = flap_window_sec() * 1000,
    WindowStart = NodeSt#node_st.flap_window_start,
    {NewFlapCount, NewWindowStart} = case WindowStart of
        0 ->
            {1, Now};
        _ when (Now - WindowStart) > WindowMs ->
            % Window expired — start fresh
            {1, Now};
        _ ->
            {NodeSt#node_st.flap_count + 1, WindowStart}
    end,
    FlapThreshold = flap_threshold(),
    NewRecoveryWait = case NewFlapCount >= FlapThreshold of
        true ->
            % Flapping detected — exponential backoff
            BaseWait = recovery_wait_ms(),
            MaxWait = max_recovery_wait_ms(),
            CurrentWait = case NodeSt#node_st.current_recovery_wait of
                0 -> BaseWait;
                W -> W
            end,
            NewWait = min(CurrentWait * 2, MaxWait),
            couch_log:warning(
                "Circuit breaker: ~s FLAPPING (~B flaps in ~Bs window) — "
                "increasing recovery_wait to ~Bms",
                [Node, NewFlapCount, flap_window_sec(), NewWait]
            ),
            NewWait;
        false ->
            NodeSt#node_st.current_recovery_wait
    end,
    NodeSt#node_st{
        flap_count = NewFlapCount,
        flap_window_start = NewWindowStart,
        current_recovery_wait = NewRecoveryWait
    }.

%% Effective recovery wait: use per-node backoff if set, else default.
effective_recovery_wait(#node_st{current_recovery_wait = 0}) ->
    recovery_wait_ms();
effective_recovery_wait(#node_st{current_recovery_wait = W}) ->
    W.

%% ===================================================================
%% Internal
%% ===================================================================

handle_slow_as_failure(Node, #node_st{} = NodeSt) ->
    NewFailures = NodeSt#node_st.failures + 1,
    Threshold = failure_threshold(),
    case NewFailures >= Threshold of
        true ->
            case NodeSt#node_st.state of
                open -> NodeSt#node_st{failures = NewFailures};
                _ ->
                    couch_log:warning(
                        "Circuit breaker: ~s OPEN — sustained high latency "
                        "(current ~Bms, baseline ~Bms)",
                        [Node, NodeSt#node_st.current_latency_ms,
                         NodeSt#node_st.baseline_latency_ms]
                    ),
                    NodeSt#node_st{
                        state = open,
                        failures = NewFailures,
                        last_failure_time = erlang:monotonic_time(millisecond)
                    }
            end;
        false ->
            NodeSt#node_st{failures = NewFailures}
    end.

%% ===================================================================
%% Config
%% ===================================================================

failure_threshold() ->
    config:get_integer("circuit_breaker", "failure_threshold", 3).

recovery_wait_ms() ->
    config:get_integer("circuit_breaker", "recovery_wait_ms", 10000).

max_recovery_wait_ms() ->
    config:get_integer("circuit_breaker", "max_recovery_wait_ms", 120000).

slow_threshold_ms() ->
    config:get_integer("circuit_breaker", "slow_threshold_ms", 5000).

degradation_factor() ->
    config:get_integer("circuit_breaker", "degradation_factor", 5).

flap_window_sec() ->
    config:get_integer("circuit_breaker", "flap_window_sec", 60).

flap_threshold() ->
    config:get_integer("circuit_breaker", "flap_threshold", 3).
