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

%% @doc Tests for circuit breaker — cable break and flap resilience.
%%
%% Topology: 6 nodes
%%   2 × 1ms (local rack)
%%   2 × 10ms (nearby DC)
%%   2 × 30ms (far DC, e.g. Cape Town)
%%
%% Failure mode: CPT cable breaks → 30ms becomes 200ms via backup route.
%% Cable flaps: alternates between 30ms and 200ms or dead.

-module(mem3_circuit_breaker_tests).

-include_lib("eunit/include/eunit.hrl").

-define(CPT, 'couchdb@cpt-node.example.com').
-define(NEARBY, 'couchdb@nearby-node.example.com').
-define(LOCAL, 'couchdb@local-node.example.com').

setup() ->
    {ok, Apps} = application:ensure_all_started(config),
    % Stop any leftover instance from a previous test group
    catch gen_server:stop(mem3_circuit_breaker, normal, 1000),
    timer:sleep(10),
    {ok, Pid} = mem3_circuit_breaker:start_link(),
    % Set tight values for testing
    ok = config:set("circuit_breaker", "recovery_wait_ms", "100", false),
    ok = config:set("circuit_breaker", "flap_window_sec", "2", false),
    ok = config:set("circuit_breaker", "flap_threshold", "3", false),
    ok = config:set("circuit_breaker", "degradation_factor", "5", false),
    ok = config:set("circuit_breaker", "max_recovery_wait_ms", "5000", false),
    {Pid, Apps}.

teardown({Pid, _Apps}) ->
    unlink(Pid),
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 -> ok
    end.

%% ===================================================================
%% Core state machine
%% ===================================================================

state_machine_test_() ->
    {"Circuit breaker state machine", {
        foreach, fun setup/0, fun teardown/1,
        [
            fun t_starts_closed/1,
            fun t_stays_closed_under_threshold/1,
            fun t_opens_after_threshold/1,
            fun t_fast_fail_under_1ms/1,
            fun t_half_open_after_recovery/1,
            fun t_closes_on_probe_success/1,
            fun t_reopens_on_probe_failure/1,
            fun t_success_resets_failures/1,
            fun t_local_always_allowed/1,
            fun t_manual_reset/1,
            fun t_independent_per_node/1
        ]
    }}.

t_starts_closed(_) ->
    ?_assertEqual(ok, mem3_circuit_breaker:allow(?CPT)).

t_stays_closed_under_threshold(_) ->
    ?_test(begin
        mem3_circuit_breaker:record_failure(?CPT),
        mem3_circuit_breaker:record_failure(?CPT),
        timer:sleep(20),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?CPT))
    end).

t_opens_after_threshold(_) ->
    ?_test(begin
        trip_breaker(?CPT),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?CPT))
    end).

t_fast_fail_under_1ms(_) ->
    ?_test(begin
        trip_breaker(?CPT),
        {Time, Result} = timer:tc(fun() -> mem3_circuit_breaker:allow(?CPT) end),
        ?assertEqual({error, circuit_open}, Result),
        ?assert(Time < 1000) % < 1ms in microseconds
    end).

t_half_open_after_recovery(_) ->
    ?_test(begin
        trip_breaker(?CPT),
        timer:sleep(150),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?CPT))
    end).

t_closes_on_probe_success(_) ->
    ?_test(begin
        trip_breaker(?CPT),
        timer:sleep(150),
        mem3_circuit_breaker:allow(?CPT), % half-open probe
        mem3_circuit_breaker:record_success(?CPT),
        timer:sleep(20),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?CPT)),
        ?assertEqual(closed, get_state(?CPT))
    end).

t_reopens_on_probe_failure(_) ->
    ?_test(begin
        trip_breaker(?CPT),
        timer:sleep(150),
        mem3_circuit_breaker:allow(?CPT), % half-open
        mem3_circuit_breaker:record_failure(?CPT),
        timer:sleep(20),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?CPT))
    end).

t_success_resets_failures(_) ->
    ?_test(begin
        mem3_circuit_breaker:record_failure(?CPT),
        mem3_circuit_breaker:record_failure(?CPT),
        mem3_circuit_breaker:record_success(?CPT),
        mem3_circuit_breaker:record_failure(?CPT),
        mem3_circuit_breaker:record_failure(?CPT),
        timer:sleep(20),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?CPT))
    end).

t_local_always_allowed(_) ->
    ?_test(begin
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_failure(node())
        end, lists:seq(1, 10)),
        timer:sleep(20),
        ?assertEqual(ok, mem3_circuit_breaker:allow(node()))
    end).

t_manual_reset(_) ->
    ?_test(begin
        trip_breaker(?CPT),
        mem3_circuit_breaker:reset(?CPT),
        timer:sleep(20),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?CPT))
    end).

t_independent_per_node(_) ->
    ?_test(begin
        trip_breaker(?CPT),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?CPT)),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?NEARBY)),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?LOCAL))
    end).

%% ===================================================================
%% Cable break: CPT link 30ms → 200ms
%% ===================================================================

cable_break_test_() ->
    {"Cable break: CPT 30ms → 200ms failover", {
        foreach, fun setup/0, fun teardown/1,
        [
            fun t_learns_baseline_latency/1,
            fun t_detects_degradation/1,
            fun t_recovers_from_degradation/1,
            fun t_sustained_high_latency_trips/1,
            fun t_total_cable_cut_opens_fast/1
        ]
    }}.

t_learns_baseline_latency(_) ->
    ?_test(begin
        % Simulate normal CPT latency: 30ms
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_latency(?CPT, 28 + rand:uniform(5))
        end, lists:seq(1, 20)),
        timer:sleep(20),
        State = mem3_circuit_breaker:node_state(?CPT),
        Baseline = maps:get(baseline_latency_ms, State),
        % Baseline should converge near 30ms
        ?assert(Baseline >= 25 andalso Baseline =< 35)
    end).

t_detects_degradation(_) ->
    ?_test(begin
        % Learn baseline: 30ms
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_latency(?CPT, 30)
        end, lists:seq(1, 20)),
        timer:sleep(20),
        ?assertEqual(closed, get_state(?CPT)),
        % Cable breaks — latency jumps to 200ms
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_latency(?CPT, 200)
        end, lists:seq(1, 5)),
        timer:sleep(20),
        State = get_state(?CPT),
        % Should be degraded (200 > 30 * 5)
        ?assertEqual(degraded, State)
    end).

t_recovers_from_degradation(_) ->
    ?_test(begin
        % Learn baseline, degrade
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_latency(?CPT, 30)
        end, lists:seq(1, 20)),
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_latency(?CPT, 200)
        end, lists:seq(1, 5)),
        timer:sleep(20),
        ?assertEqual(degraded, get_state(?CPT)),
        % Cable fixed — back to 30ms
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_latency(?CPT, 30)
        end, lists:seq(1, 10)),
        timer:sleep(20),
        ?assertEqual(closed, get_state(?CPT))
    end).

t_sustained_high_latency_trips(_) ->
    ?_test(begin
        ok = config:set("circuit_breaker", "slow_threshold_ms", "150", false),
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_slow(?CPT, 200)
        end, lists:seq(1, 5)),
        timer:sleep(20),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?CPT))
    end).

t_total_cable_cut_opens_fast(_) ->
    ?_test(begin
        % 3 failures = open. Should be < 100ms total.
        {Time, _} = timer:tc(fun() ->
            trip_breaker(?CPT)
        end),
        ?assert(Time < 100000), % < 100ms
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?CPT))
    end).

%% ===================================================================
%% Flap dampening: cable oscillates
%% ===================================================================

flap_test_() ->
    {"Flap dampening: cable oscillates between working and dead", {
        foreach, fun setup/0, fun teardown/1,
        [
            fun t_flap_increases_recovery_wait/1,
            fun t_flap_backoff_caps_at_max/1,
            fun t_no_flap_no_backoff/1,
            fun t_flap_window_resets/1
        ]
    }}.

t_flap_increases_recovery_wait(_) ->
    ?_test(begin
        % Simulate cable flapping: open → recover → open → recover → open
        lists:foreach(fun(_Cycle) ->
            trip_breaker(?CPT),
            timer:sleep(150), % wait for recovery
            mem3_circuit_breaker:allow(?CPT), % probe
            mem3_circuit_breaker:record_success(?CPT),
            timer:sleep(20)
        end, lists:seq(1, 4)),
        timer:sleep(20),
        State = mem3_circuit_breaker:node_state(?CPT),
        RecoveryWait = maps:get(current_recovery_wait, State),
        % After 4 flaps with threshold=3, should have backed off
        ?assert(RecoveryWait > 100),
        % Should be at least 200ms (base 100 doubled once)
        ?assert(RecoveryWait >= 200)
    end).

t_flap_backoff_caps_at_max(_) ->
    ?_test(begin
        % Flap many times — recovery_wait should cap at max_recovery_wait_ms
        lists:foreach(fun(_) ->
            trip_breaker(?CPT),
            timer:sleep(300), % enough for any backoff
            case mem3_circuit_breaker:allow(?CPT) of
                ok -> mem3_circuit_breaker:record_success(?CPT);
                _ -> ok
            end,
            timer:sleep(20)
        end, lists:seq(1, 10)),
        timer:sleep(20),
        State = mem3_circuit_breaker:node_state(?CPT),
        RecoveryWait = maps:get(current_recovery_wait, State),
        MaxWait = 5000, % set in setup
        ?assert(RecoveryWait =< MaxWait)
    end).

t_no_flap_no_backoff(_) ->
    ?_test(begin
        % Single open→close cycle, no flapping
        trip_breaker(?CPT),
        timer:sleep(150),
        mem3_circuit_breaker:allow(?CPT),
        mem3_circuit_breaker:record_success(?CPT),
        timer:sleep(20),
        State = mem3_circuit_breaker:node_state(?CPT),
        FlapCount = maps:get(flap_count, State),
        ?assert(FlapCount < 3) % under threshold
    end).

t_flap_window_resets(_) ->
    ?_test(begin
        % Flap once, wait for window to expire, flap again
        % Should NOT accumulate across windows
        trip_breaker(?CPT),
        timer:sleep(150),
        mem3_circuit_breaker:allow(?CPT),
        mem3_circuit_breaker:record_success(?CPT),
        timer:sleep(20),
        % Wait for flap window to expire (2s in test config)
        timer:sleep(2500),
        % Another open/close cycle — should start fresh count
        trip_breaker(?CPT),
        timer:sleep(150),
        mem3_circuit_breaker:allow(?CPT),
        mem3_circuit_breaker:record_success(?CPT),
        timer:sleep(20),
        State = mem3_circuit_breaker:node_state(?CPT),
        FlapCount = maps:get(flap_count, State),
        ?assert(FlapCount =< 1)
    end).

%% ===================================================================
%% Performance: in-DC must be zero overhead
%% ===================================================================

performance_test_() ->
    {"In-DC zero overhead", {
        foreach, fun setup/0, fun teardown/1,
        [
            fun t_allow_local_under_1us/1,
            fun t_allow_healthy_remote_under_1ms/1,
            fun t_1000_allows_under_100ms/1
        ]
    }}.

t_allow_local_under_1us(_) ->
    ?_test(begin
        % Local node: should be pure function, no gen_server call
        {Time, ok} = timer:tc(fun() -> mem3_circuit_breaker:allow(node()) end),
        ?assert(Time < 10) % < 10 microseconds
    end).

t_allow_healthy_remote_under_1ms(_) ->
    ?_test(begin
        {Time, ok} = timer:tc(fun() -> mem3_circuit_breaker:allow(?NEARBY) end),
        ?assert(Time < 1000) % < 1ms
    end).

t_1000_allows_under_100ms(_) ->
    ?_test(begin
        {Time, _} = timer:tc(fun() ->
            lists:foreach(fun(_) ->
                mem3_circuit_breaker:allow(?CPT)
            end, lists:seq(1, 1000))
        end),
        ?assert(Time < 100000) % < 100ms for 1000 calls
    end).

%% ===================================================================
%% Helpers
%% ===================================================================

trip_breaker(Node) ->
    mem3_circuit_breaker:record_failure(Node),
    mem3_circuit_breaker:record_failure(Node),
    mem3_circuit_breaker:record_failure(Node),
    timer:sleep(20).

get_state(Node) ->
    State = mem3_circuit_breaker:node_state(Node),
    maps:get(state, State).
