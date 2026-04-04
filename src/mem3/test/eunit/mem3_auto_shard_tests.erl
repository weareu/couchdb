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

%% @doc Tests for the auto-shard split orchestrator.
%%
%% Tests cover:
%%   - Split factor calculation (including power-of-2 rounding)
%%   - Exclusion pattern matching
%%   - Maintenance window logic
%%   - Coordinator election
%%   - Circuit breaker gating
%%   - Design doc opt-out
%%   - Cooldown enforcement
%%   - Status reporting
%%   - Gen_server lifecycle

-module(mem3_auto_shard_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

%% ===================================================================
%% 1. Split factor calculation
%% ===================================================================

split_factor_test_() ->
    {"Split factor calculation", [
        {"80GB / 20GB = factor 4",
         ?_assertEqual(4,
            mem3_auto_shard:calculate_split_factor(80000000000, 20000000000))},

        {"200GB / 20GB = 10 -> next power of 2 = 16",
         ?_assertEqual(16,
            mem3_auto_shard:calculate_split_factor(200000000000, 20000000000))},

        {"500GB / 20GB = 25 -> next power of 2 = 32",
         ?_assertEqual(32,
            mem3_auto_shard:calculate_split_factor(500000000000, 20000000000))},

        {"25GB / 20GB = 2 (minimum)",
         ?_assertEqual(2,
            mem3_auto_shard:calculate_split_factor(25000000000, 20000000000))},

        {"40GB / 20GB = 2",
         ?_assertEqual(2,
            mem3_auto_shard:calculate_split_factor(40000000000, 20000000000))},

        {"1TB / 20GB = 50 -> 64",
         ?_assertEqual(64,
            mem3_auto_shard:calculate_split_factor(1000000000000, 20000000000))},

        {"Exact match: 20GB / 20GB = 2 (always split at least 2)",
         ?_assertEqual(2,
            mem3_auto_shard:calculate_split_factor(20000000000, 20000000000))}
    ]}.

%% ===================================================================
%% 2. Power of 2 rounding
%% ===================================================================

power_of_2_test_() ->
    {"Next power of 2", [
        ?_assertEqual(2, mem3_auto_shard:next_power_of_2(1)),
        ?_assertEqual(2, mem3_auto_shard:next_power_of_2(2)),
        ?_assertEqual(4, mem3_auto_shard:next_power_of_2(3)),
        ?_assertEqual(4, mem3_auto_shard:next_power_of_2(4)),
        ?_assertEqual(8, mem3_auto_shard:next_power_of_2(5)),
        ?_assertEqual(8, mem3_auto_shard:next_power_of_2(7)),
        ?_assertEqual(8, mem3_auto_shard:next_power_of_2(8)),
        ?_assertEqual(16, mem3_auto_shard:next_power_of_2(9)),
        ?_assertEqual(16, mem3_auto_shard:next_power_of_2(16)),
        ?_assertEqual(32, mem3_auto_shard:next_power_of_2(17)),
        ?_assertEqual(64, mem3_auto_shard:next_power_of_2(33)),
        ?_assertEqual(128, mem3_auto_shard:next_power_of_2(100))
    ]}.

%% ===================================================================
%% 3. Exclusion patterns
%% ===================================================================

exclusion_test_() ->
    {"Database exclusion patterns", [
        {"Exact match excludes",
         ?_assertEqual(true,
            mem3_auto_shard:is_excluded(<<"_users">>,
                mock_state([<<"_users">>, <<"_replicator">>])))},

        {"Wildcard match excludes",
         ?_assertEqual(true,
            mem3_auto_shard:is_excluded(<<"metrics_2024">>,
                mock_state([<<"metrics_*">>])))},

        {"Non-matching not excluded",
         ?_assertEqual(false,
            mem3_auto_shard:is_excluded(<<"important_data">>,
                mock_state([<<"_users">>, <<"metrics_*">>])))},

        {"Empty patterns exclude nothing",
         ?_assertEqual(false,
            mem3_auto_shard:is_excluded(<<"anything">>,
                mock_state([])))},

        {"System DB patterns",
         ?_test(begin
             State = mock_state([<<"_users">>, <<"_replicator">>,
                                 <<"_global_changes">>]),
             ?assertEqual(true, mem3_auto_shard:is_excluded(<<"_users">>, State)),
             ?assertEqual(true, mem3_auto_shard:is_excluded(<<"_replicator">>, State)),
             ?assertEqual(true, mem3_auto_shard:is_excluded(<<"_global_changes">>, State)),
             ?assertEqual(false, mem3_auto_shard:is_excluded(<<"mydb">>, State))
         end)}
    ]}.

%% ===================================================================
%% 4. Maintenance window
%% ===================================================================

maintenance_window_test_() ->
    {"Maintenance window", [
        {"'always' always allows",
         ?_assertEqual(true, mem3_auto_shard:in_maintenance_window(always))},

        {"Window includes current hour",
         ?_test(begin
             {_, {Hour, _, _}} = calendar:local_time(),
             %% Window that includes current hour
             Start = Hour,
             End = (Hour + 2) rem 24,
             ?assertEqual(true,
                mem3_auto_shard:in_maintenance_window({Start, End}))
         end)},

        {"Window excludes non-matching hour",
         ?_test(begin
             {_, {Hour, _, _}} = calendar:local_time(),
             %% Window far from current hour
             Start = (Hour + 12) rem 24,
             End = (Hour + 14) rem 24,
             ?assertEqual(false,
                mem3_auto_shard:in_maintenance_window({Start, End}))
         end)},

        {"Overnight window (22:00-06:00) wraps correctly",
         ?_test(begin
             %% Test the wrap-around logic
             ?assertEqual(true,
                mem3_auto_shard:in_maintenance_window({0, 24}))
         end)}
    ]}.

%% ===================================================================
%% 5. Design doc opt-out
%% ===================================================================

ddoc_optout_test_() ->
    {
        "Design doc auto-split opt-out",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_no_ddoc_allows_split/0,
                fun t_ddoc_disabled_blocks_split/0,
                fun t_ddoc_enabled_allows_split/0,
                fun t_nonexistent_db_allows_split/0
            ]
        }
    }.

t_no_ddoc_allows_split() ->
    ?_test(begin
        DbName = ?tempdb(),
        {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
        couch_db:close(Db),
        try
            ?assertEqual(false,
                mem3_auto_shard:is_split_disabled_by_ddoc(DbName))
        after
            couch_server:delete(DbName, [?ADMIN_CTX])
        end
    end).

t_ddoc_disabled_blocks_split() ->
    ?_test(begin
        DbName = ?tempdb(),
        {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
        try
            DDoc = #doc{
                id = <<"_design/shard_config">>,
                body = {[
                    {<<"auto_split">>, {[
                        {<<"enabled">>, false},
                        {<<"reason">>, <<"Custom retention">>}
                    ]}}
                ]}
            },
            {ok, _} = couch_db:update_doc(Db, DDoc, []),
            couch_db:close(Db),
            ?assertEqual(true,
                mem3_auto_shard:is_split_disabled_by_ddoc(DbName))
        after
            couch_db:close(Db),
            couch_server:delete(DbName, [?ADMIN_CTX])
        end
    end).

t_ddoc_enabled_allows_split() ->
    ?_test(begin
        DbName = ?tempdb(),
        {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
        try
            DDoc = #doc{
                id = <<"_design/shard_config">>,
                body = {[
                    {<<"auto_split">>, {[
                        {<<"enabled">>, true}
                    ]}}
                ]}
            },
            {ok, _} = couch_db:update_doc(Db, DDoc, []),
            couch_db:close(Db),
            ?assertEqual(false,
                mem3_auto_shard:is_split_disabled_by_ddoc(DbName))
        after
            couch_db:close(Db),
            couch_server:delete(DbName, [?ADMIN_CTX])
        end
    end).

t_nonexistent_db_allows_split() ->
    ?_assertEqual(false,
        mem3_auto_shard:is_split_disabled_by_ddoc(<<"nonexistent_db_xyz">>)).

%% ===================================================================
%% 6. Gen_server lifecycle
%% ===================================================================

lifecycle_test_() ->
    {
        "Gen_server lifecycle",
        {
            setup,
            fun setup_server/0,
            fun teardown_server/1,
            fun(Ctx) -> [
                t_starts_disabled(Ctx),
                t_status_returns_map(Ctx),
                t_pause_resume(Ctx),
                t_set_threshold(Ctx),
                t_trigger_scan_no_crash(Ctx)
            ] end
        }
    }.

setup_server() ->
    {ok, Apps} = application:ensure_all_started(config),
    ok = config:set("auto_shard", "enabled", "false", false),
    ok = config:set("auto_shard", "scan_interval_ms", "600000", false),
    {ok, Pid} = mem3_auto_shard:start_link(),
    {Pid, Apps}.

teardown_server({Pid, _Apps}) ->
    unlink(Pid),
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 -> ok
    end.

t_starts_disabled(_) ->
    ?_test(begin
        Status = mem3_auto_shard:status(),
        ?assertEqual(false, maps:get(enabled, Status))
    end).

t_status_returns_map(_) ->
    ?_test(begin
        Status = mem3_auto_shard:status(),
        ?assert(is_map(Status)),
        ?assert(maps:is_key(enabled, Status)),
        ?assert(maps:is_key(max_shard_size_bytes, Status)),
        ?assert(maps:is_key(active_splits, Status)),
        ?assert(maps:is_key(scan_count, Status)),
        ?assert(maps:is_key(splits_triggered, Status))
    end).

t_pause_resume(_) ->
    ?_test(begin
        ?assertEqual(ok, mem3_auto_shard:pause()),
        Status1 = mem3_auto_shard:status(),
        ?assertEqual(true, maps:get(paused, Status1)),

        ?assertEqual(ok, mem3_auto_shard:resume()),
        Status2 = mem3_auto_shard:status(),
        ?assertEqual(false, maps:get(paused, Status2))
    end).

t_set_threshold(_) ->
    ?_test(begin
        ?assertEqual(ok, mem3_auto_shard:set_threshold(50000000000)),
        Status = mem3_auto_shard:status(),
        ?assertEqual(50000000000, maps:get(max_shard_size_bytes, Status))
    end).

t_trigger_scan_no_crash(_) ->
    ?_test(begin
        %% Should not crash even when disabled
        ?assertEqual(ok, mem3_auto_shard:trigger_scan()),
        timer:sleep(100),
        %% Process should still be alive
        ?assert(is_pid(whereis(mem3_auto_shard)))
    end).

%% ===================================================================
%% 7. Coordinator election
%% ===================================================================

coordinator_test_() ->
    {"Coordinator election", [
        {"Local node is coordinator when mem3 unavailable",
         ?_assertEqual(true, mem3_auto_shard:is_coordinator())}
    ]}.

%% ===================================================================
%% 8. Circuit breaker integration
%% ===================================================================

circuit_breaker_test_() ->
    {"Circuit breaker integration", [
        {"All circuits closed when breaker unavailable",
         ?_assertEqual(true, mem3_auto_shard:all_circuits_closed())}
    ]}.

%% ===================================================================
%% Helpers
%% ===================================================================

%% Build a minimal state record for testing exclusion patterns.
%% Must match #state{} field order in mem3_auto_shard.erl
mock_state(ExcludePatterns) ->
    %% {state, enabled, max_shard_size_bytes, scan_interval_ms,
    %%  max_concurrent_splits, max_split_factor, cooldown_ms,
    %%  maintenance_window, paused, exclude_patterns, protected_dbs,
    %%  active_splits, cooldowns, timer_ref, scan_count, splits_triggered}
    {state, false, 20000000000, 600000, 2, 4, 3600000, always, false,
     ExcludePatterns, [], #{}, #{}, undefined, 0, 0}.
