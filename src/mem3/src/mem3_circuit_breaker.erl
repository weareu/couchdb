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
%% Tracks node health and provides fast-fail for requests to nodes that
%% are known to be unreachable or very slow. Prevents the cluster from
%% wasting time on nodes that are behind a broken cable.
%%
%% States per node:
%%   closed  - Node is healthy, requests pass through normally
%%   open    - Node is unhealthy, requests fail immediately (fast-fail)
%%   half_open - Probing: allow one request through to test recovery
%%
%% Transitions:
%%   closed -> open:     After `failure_threshold` consecutive failures
%%   open -> half_open:  After `recovery_wait_ms` has elapsed
%%   half_open -> closed: If the probe request succeeds
%%   half_open -> open:   If the probe request fails
%%
%% Config:
%%   [circuit_breaker]
%%   failure_threshold = 3
%%   recovery_wait_ms = 10000
%%   slow_threshold_ms = 5000
%%
%% Usage:
%%   case mem3_circuit_breaker:allow(Node) of
%%       ok -> proceed_with_request(Node);
%%       {error, circuit_open} -> skip_node_or_use_fallback()
%%   end
%%
%%   On success: mem3_circuit_breaker:record_success(Node)
%%   On failure: mem3_circuit_breaker:record_failure(Node)

-module(mem3_circuit_breaker).

-behaviour(gen_server).

-export([
    start_link/0,
    allow/1,
    record_success/1,
    record_failure/1,
    record_slow/2,
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
    state = closed,           % closed | open | half_open
    failures = 0,             % consecutive failure count
    last_failure_time = 0,    % erlang:monotonic_time(millisecond)
    last_success_time = 0,
    total_failures = 0,       % lifetime counter
    total_successes = 0,
    avg_latency_ms = 0        % exponential moving average
}).

-record(st, {
    nodes = #{}               % Node => #node_st{}
}).

%% ===================================================================
%% API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Check if a request to Node should be allowed.
%% Returns `ok` if the circuit is closed/half-open (request allowed),
%% or `{error, circuit_open}` if the node is known to be unreachable.
-spec allow(node()) -> ok | {error, circuit_open}.
allow(Node) ->
    case Node =:= node() of
        true -> ok;  % Local node always allowed
        false ->
            try
                gen_server:call(?MODULE, {allow, Node}, 1000)
            catch
                exit:{timeout, _} -> ok;  % If breaker itself is slow, allow
                exit:{noproc, _} -> ok    % If breaker not started, allow
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
%% gen_server callbacks
%% ===================================================================

init([]) ->
    {ok, #st{}}.

handle_call({allow, Node}, _From, #st{nodes = Nodes} = St) ->
    NodeSt = maps:get(Node, Nodes, #node_st{}),
    case NodeSt#node_st.state of
        closed ->
            {reply, ok, St};
        half_open ->
            {reply, ok, St};
        open ->
            Now = erlang:monotonic_time(millisecond),
            RecoveryWait = recovery_wait_ms(),
            TimeSinceFailure = Now - NodeSt#node_st.last_failure_time,
            case TimeSinceFailure >= RecoveryWait of
                true ->
                    % Transition to half_open — allow one probe request
                    NewNodeSt = NodeSt#node_st{state = half_open},
                    NewNodes = Nodes#{Node => NewNodeSt},
                    couch_log:notice(
                        "Circuit breaker: ~s half-open, allowing probe",
                        [Node]
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
        avg_latency_ms => NodeSt#node_st.avg_latency_ms
    },
    {reply, Reply, St};

handle_call(all_states, _From, #st{nodes = Nodes} = St) ->
    Reply = maps:map(fun(_Node, NodeSt) ->
        #{
            state => NodeSt#node_st.state,
            failures => NodeSt#node_st.failures,
            avg_latency_ms => NodeSt#node_st.avg_latency_ms
        }
    end, Nodes),
    {reply, Reply, St};

handle_call(_Msg, _From, St) ->
    {reply, {error, unknown}, St}.

handle_cast({success, Node}, #st{nodes = Nodes} = St) ->
    NodeSt = maps:get(Node, Nodes, #node_st{}),
    Now = erlang:monotonic_time(millisecond),
    NewNodeSt = NodeSt#node_st{
        state = closed,
        failures = 0,
        last_success_time = Now,
        total_successes = NodeSt#node_st.total_successes + 1
    },
    case NodeSt#node_st.state of
        open ->
            couch_log:notice(
                "Circuit breaker: ~s recovered (closed)", [Node]);
        half_open ->
            couch_log:notice(
                "Circuit breaker: ~s probe succeeded (closed)", [Node]);
        _ -> ok
    end,
    {noreply, St#st{nodes = Nodes#{Node => NewNodeSt}}};

handle_cast({failure, Node}, #st{nodes = Nodes} = St) ->
    NodeSt = maps:get(Node, Nodes, #node_st{}),
    Now = erlang:monotonic_time(millisecond),
    NewFailures = NodeSt#node_st.failures + 1,
    Threshold = failure_threshold(),
    NewState = case NewFailures >= Threshold of
        true ->
            case NodeSt#node_st.state of
                open -> open;  % Already open
                _ ->
                    couch_log:warning(
                        "Circuit breaker: ~s OPEN after ~B consecutive failures",
                        [Node, NewFailures]
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

handle_cast({slow, Node, LatencyMs}, #st{nodes = Nodes} = St) ->
    NodeSt = maps:get(Node, Nodes, #node_st{}),
    % Exponential moving average with alpha=0.3
    OldAvg = NodeSt#node_st.avg_latency_ms,
    NewAvg = case OldAvg of
        0 -> LatencyMs;
        _ -> round(0.3 * LatencyMs + 0.7 * OldAvg)
    end,
    SlowThreshold = slow_threshold_ms(),
    NewNodeSt = case NewAvg > SlowThreshold of
        true ->
            % Treat sustained slowness as a failure
            handle_slow_as_failure(Node, NodeSt#node_st{avg_latency_ms = NewAvg});
        false ->
            NodeSt#node_st{avg_latency_ms = NewAvg}
    end,
    {noreply, St#st{nodes = Nodes#{Node => NewNodeSt}}};

handle_cast({reset, Node}, #st{nodes = Nodes} = St) ->
    couch_log:notice("Circuit breaker: ~s manually reset to closed", [Node]),
    {noreply, St#st{nodes = Nodes#{Node => #node_st{}}}};

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(_Msg, St) ->
    {noreply, St}.

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
                        "Circuit breaker: ~s OPEN due to sustained high latency "
                        "(avg ~Bms > ~Bms threshold)",
                        [Node, NodeSt#node_st.avg_latency_ms, slow_threshold_ms()]
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

failure_threshold() ->
    config:get_integer("circuit_breaker", "failure_threshold", 3).

recovery_wait_ms() ->
    config:get_integer("circuit_breaker", "recovery_wait_ms", 10000).

slow_threshold_ms() ->
    config:get_integer("circuit_breaker", "slow_threshold_ms", 5000).
