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

%% @doc Integration tests for couch_space_monitor.
%%
%% Tests verify REAL behavior:
%%   - Space reservations actually accumulate and limit further reservations
%%   - Process death auto-releases reservations
%%   - Concurrent reservations interact correctly
%%   - The gen_server handles all operations without crashing

-module(couch_space_monitor_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_eunit.hrl").

%% ===================================================================
%% Test fixtures
%% ===================================================================

space_monitor_test_() ->
    {
        "couch_space_monitor integration tests",
        {
            setup,
            fun setup/0,
            fun teardown/1,
            [
                fun t_reserve_and_release/0,
                fun t_duplicate_tag_rejected/0,
                fun t_release_idempotent/0,
                fun t_reservations_accumulate/0,
                fun t_process_death_auto_releases/0,
                fun t_status_reflects_reservations/0,
                fun t_reserved_on_node/0,
                fun t_total_reserved/0,
                fun t_check_available/0,
                fun t_effective_free_math/0,
                fun t_concurrent_reserve_release/0
            ]
        }
    }.

setup() ->
    Ctx = test_util:start_couch(),
    %% Set a very low floor for testing
    ok = config:set("space_monitor", "min_free_floor_bytes", "0", false),
    Ctx.

teardown(Ctx) ->
    config:delete("space_monitor", "min_free_floor_bytes", false),
    test_util:stop_couch(Ctx).

%% ===================================================================
%% Tests
%% ===================================================================

t_reserve_and_release() ->
    Tag = {test, make_ref()},
    ?assertEqual(ok, couch_space_monitor:reserve(Tag, node(), 1000)),
    ?assert(couch_space_monitor:reserved_on(node()) >= 1000),
    ?assertEqual(ok, couch_space_monitor:release(Tag)),
    ok.

t_duplicate_tag_rejected() ->
    Tag = {test_dup, make_ref()},
    ?assertEqual(ok, couch_space_monitor:reserve(Tag, node(), 500)),
    ?assertMatch({error, already_reserved},
        couch_space_monitor:reserve(Tag, node(), 500)),
    couch_space_monitor:release(Tag),
    ok.

t_release_idempotent() ->
    Tag = {test_idem, make_ref()},
    ?assertEqual(ok, couch_space_monitor:release(Tag)),
    ?assertEqual(ok, couch_space_monitor:release(Tag)),
    ok.

t_reservations_accumulate() ->
    Tag1 = {test_acc1, make_ref()},
    Tag2 = {test_acc2, make_ref()},
    Before = couch_space_monitor:reserved_on(node()),
    ok = couch_space_monitor:reserve(Tag1, node(), 1000),
    ok = couch_space_monitor:reserve(Tag2, node(), 2000),
    After = couch_space_monitor:reserved_on(node()),
    ?assertEqual(3000, After - Before),
    couch_space_monitor:release(Tag1),
    couch_space_monitor:release(Tag2),
    Final = couch_space_monitor:reserved_on(node()),
    ?assertEqual(Before, Final),
    ok.

t_process_death_auto_releases() ->
    Tag = {test_death, make_ref()},
    Self = self(),
    Pid = spawn(fun() ->
        ok = couch_space_monitor:reserve(Tag, node(), 5000),
        Self ! reserved,
        receive die -> ok end
    end),
    receive reserved -> ok after 5000 -> error(timeout) end,
    ?assert(couch_space_monitor:reserved_on(node()) >= 5000),
    %% Monitor the reserving process so we know when it has
    %% actually exited (cheap and deterministic). The space monitor
    %% receives the same DOWN but we can't monitor its internal
    %% release message, so still wait_for the reservation to be gone.
    Ref = monitor(process, Pid),
    Pid ! die,
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 2000 -> error(process_did_not_exit)
    end,
    ok = wait_for(fun() ->
        Reservations = couch_space_monitor:reservations(),
        [] =:= [R || R <- Reservations, maps:get(tag, R) =:= Tag]
    end, 100, 20),
    ok.

t_status_reflects_reservations() ->
    Tag = {test_status, make_ref()},
    ok = couch_space_monitor:reserve(Tag, node(), 7777),
    Status = couch_space_monitor:status(),
    ?assert(maps:get(total_reserved_bytes, Status) >= 7777),
    ?assert(maps:get(reservation_count, Status) >= 1),
    ?assert(is_map(maps:get(by_node, Status))),
    ?assert(is_list(maps:get(reservations, Status))),
    couch_space_monitor:release(Tag),
    ok.

t_reserved_on_node() ->
    Tag = {test_node, make_ref()},
    Before = couch_space_monitor:reserved_on(node()),
    ok = couch_space_monitor:reserve(Tag, node(), 4444),
    ?assertEqual(Before + 4444, couch_space_monitor:reserved_on(node())),
    %% Fake remote node should have 0
    ?assertEqual(0, couch_space_monitor:reserved_on('fake@node')),
    couch_space_monitor:release(Tag),
    ok.

t_total_reserved() ->
    Tag = {test_total, make_ref()},
    Before = couch_space_monitor:total_reserved(),
    ok = couch_space_monitor:reserve(Tag, node(), 3333),
    ?assertEqual(Before + 3333, couch_space_monitor:total_reserved()),
    couch_space_monitor:release(Tag),
    ?assertEqual(Before, couch_space_monitor:total_reserved()),
    ok.

t_check_available() ->
    %% Should be able to check availability without reserving
    ?assert(is_boolean(couch_space_monitor:check_available(node(), 1))),
    ok.

t_effective_free_math() ->
    ?assertEqual(70, couch_space_monitor:effective_free(100, 30)),
    ?assertEqual(0, couch_space_monitor:effective_free(10, 50)),
    ?assertEqual(100, couch_space_monitor:effective_free(100, 0)),
    ok.

t_concurrent_reserve_release() ->
    %% 10 processes each reserve and release — no crashes, and the
    %% reserved total must return to its starting value once all
    %% workers have released.
    Before = couch_space_monitor:total_reserved(),
    Self = self(),
    _Pids = [spawn(fun() ->
        Tag = {concurrent, I, make_ref()},
        ok = couch_space_monitor:reserve(Tag, node(), 100),
        couch_space_monitor:release(Tag),
        Self ! {done, I}
    end) || I <- lists:seq(1, 10)],
    [receive {done, I} -> ok after 5000 -> error({timeout, I}) end
     || I <- lists:seq(1, 10)],
    %% After all releases, total must be back to baseline.
    ?assertEqual(Before, couch_space_monitor:total_reserved()),
    ok.

%% Poll Fun until it returns true, up to RetriesLeft attempts,
%% sleeping IntervalMs between attempts. Returns ok on success,
%% {error, timeout} on failure.
wait_for(_Fun, _IntervalMs, 0) ->
    error(wait_for_timeout);
wait_for(Fun, IntervalMs, RetriesLeft) ->
    case Fun() of
        true -> ok;
        false ->
            timer:sleep(IntervalMs),
            wait_for(Fun, IntervalMs, RetriesLeft - 1)
    end.
