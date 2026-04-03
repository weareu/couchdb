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

%% @doc Tests for the circuit breaker module — cable break resilience.
%%
%% Scenarios tested:
%%   1. Circuit starts closed (healthy)
%%   2. Opens after N consecutive failures (cable cut)
%%   3. Fast-fails while open (no wasted time)
%%   4. Transitions to half-open after recovery wait
%%   5. Closes on probe success (cable restored)
%%   6. Re-opens on probe failure (cable still broken)
%%   7. Sustained high latency trips the breaker
%%   8. Success resets failure count
%%   9. Manual reset works
%%  10. Local node always allowed

-module(mem3_circuit_breaker_tests).

-include_lib("eunit/include/eunit.hrl").

-define(REMOTE, 'remote@dc-b.example.com').
-define(REMOTE2, 'remote2@dc-c.example.com').

%% ===================================================================
%% Test lifecycle
%% ===================================================================

setup() ->
    {ok, Apps} = application:ensure_all_started(config),
    {ok, Pid} = mem3_circuit_breaker:start_link(),
    {Pid, Apps}.

teardown({Pid, _Apps}) ->
    unlink(Pid),
    exit(Pid, kill),
    ok.

circuit_breaker_test_() ->
    {
        "Circuit breaker state machine",
        {
            foreach,
            fun setup/0,
            fun teardown/1,
            [
                fun t_starts_closed/1,
                fun t_stays_closed_under_threshold/1,
                fun t_opens_after_threshold/1,
                fun t_fast_fails_when_open/1,
                fun t_half_open_after_recovery_wait/1,
                fun t_closes_on_probe_success/1,
                fun t_reopens_on_probe_failure/1,
                fun t_success_resets_failures/1,
                fun t_slow_latency_trips_breaker/1,
                fun t_local_node_always_allowed/1,
                fun t_manual_reset/1,
                fun t_independent_per_node/1,
                fun t_rapid_failure_recovery_cycles/1,
                fun t_mixed_success_failure/1
            ]
        }
    }.

%% ===================================================================
%% Tests
%% ===================================================================

t_starts_closed(_) ->
    ?_assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE)).

t_stays_closed_under_threshold(_) ->
    ?_test(begin
        % Default threshold is 3. Two failures should keep it closed.
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        timer:sleep(50),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE))
    end).

t_opens_after_threshold(_) ->
    ?_test(begin
        % 3 consecutive failures should open the circuit
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        timer:sleep(50),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?REMOTE))
    end).

t_fast_fails_when_open(_) ->
    ?_test(begin
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_failure(?REMOTE)
        end, lists:seq(1, 5)),
        timer:sleep(50),
        % Should fast-fail without delay
        {Time, Result} = timer:tc(fun() ->
            mem3_circuit_breaker:allow(?REMOTE)
        end),
        ?assertEqual({error, circuit_open}, Result),
        ?assert(Time < 100000) % < 100ms — fast fail, not timeout
    end).

t_half_open_after_recovery_wait(_) ->
    ?_test(begin
        % Set recovery_wait_ms very low for testing
        ok = config:set("circuit_breaker", "recovery_wait_ms", "100", false),
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_failure(?REMOTE)
        end, lists:seq(1, 3)),
        timer:sleep(50),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?REMOTE)),
        % Wait for recovery period
        timer:sleep(150),
        % Should now be half-open — allow probe
        ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE))
    end).

t_closes_on_probe_success(_) ->
    ?_test(begin
        ok = config:set("circuit_breaker", "recovery_wait_ms", "100", false),
        % Trip the breaker
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_failure(?REMOTE)
        end, lists:seq(1, 3)),
        timer:sleep(150),
        % Probe — allow returns ok (half-open)
        ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE)),
        % Record success — circuit closes
        mem3_circuit_breaker:record_success(?REMOTE),
        timer:sleep(50),
        % Should be fully closed now
        ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE)),
        State = mem3_circuit_breaker:node_state(?REMOTE),
        ?assertEqual(closed, maps:get(state, State))
    end).

t_reopens_on_probe_failure(_) ->
    ?_test(begin
        ok = config:set("circuit_breaker", "recovery_wait_ms", "100", false),
        % Trip the breaker
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_failure(?REMOTE)
        end, lists:seq(1, 3)),
        timer:sleep(150),
        % Probe allowed (half-open)
        ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE)),
        % Probe fails — back to open
        mem3_circuit_breaker:record_failure(?REMOTE),
        timer:sleep(50),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?REMOTE))
    end).

t_success_resets_failures(_) ->
    ?_test(begin
        % 2 failures, then success, then 2 more failures = still closed
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_success(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        timer:sleep(50),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE))
    end).

t_slow_latency_trips_breaker(_) ->
    ?_test(begin
        ok = config:set("circuit_breaker", "slow_threshold_ms", "100", false),
        % Report sustained high latency
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_slow(?REMOTE, 500)
        end, lists:seq(1, 5)),
        timer:sleep(50),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?REMOTE))
    end).

t_local_node_always_allowed(_) ->
    ?_test(begin
        % Local node should always be allowed, even if failures recorded
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_failure(node())
        end, lists:seq(1, 10)),
        timer:sleep(50),
        ?assertEqual(ok, mem3_circuit_breaker:allow(node()))
    end).

t_manual_reset(_) ->
    ?_test(begin
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_failure(?REMOTE)
        end, lists:seq(1, 5)),
        timer:sleep(50),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?REMOTE)),
        mem3_circuit_breaker:reset(?REMOTE),
        timer:sleep(50),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE))
    end).

t_independent_per_node(_) ->
    ?_test(begin
        % REMOTE is broken, REMOTE2 is healthy
        lists:foreach(fun(_) ->
            mem3_circuit_breaker:record_failure(?REMOTE)
        end, lists:seq(1, 3)),
        timer:sleep(50),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?REMOTE)),
        ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE2))
    end).

t_rapid_failure_recovery_cycles(_) ->
    ?_test(begin
        ok = config:set("circuit_breaker", "recovery_wait_ms", "50", false),
        % Simulate cable flapping: fail, recover, fail, recover
        lists:foreach(fun(_Cycle) ->
            % Trip breaker
            lists:foreach(fun(_) ->
                mem3_circuit_breaker:record_failure(?REMOTE)
            end, lists:seq(1, 3)),
            timer:sleep(50),
            ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?REMOTE)),
            % Wait and recover
            timer:sleep(100),
            ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE)),
            mem3_circuit_breaker:record_success(?REMOTE),
            timer:sleep(20),
            ?assertEqual(ok, mem3_circuit_breaker:allow(?REMOTE))
        end, lists:seq(1, 3))
    end).

t_mixed_success_failure(_) ->
    ?_test(begin
        % Pattern: success, fail, success, fail, success, fail, fail, fail
        % The 3 consecutive failures at the end should trip it
        mem3_circuit_breaker:record_success(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_success(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_success(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        mem3_circuit_breaker:record_failure(?REMOTE),
        timer:sleep(50),
        ?assertEqual({error, circuit_open}, mem3_circuit_breaker:allow(?REMOTE))
    end).
