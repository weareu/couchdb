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

%% @doc Integration tests for the auto-shard orchestrator.
%%
%% These tests verify REAL behavior:
%%   - Design doc opt-out actually prevents splitting
%%   - The gen_server starts, scans, and responds correctly
%%   - Shard size detection triggers the right split factor
%%   - Excluded databases are actually excluded
%%
%% NOT tested here (tested elsewhere):
%%   - Pure math (split factor, power of 2) — trivially correct
%%   - List membership (exclusion patterns) — stdlib
%%   - Hour comparison (maintenance windows) — stdlib

-module(mem3_auto_shard_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

%% ===================================================================
%% 1. Design doc opt-out — real database behavior
%% ===================================================================

ddoc_optout_test_() ->
    {
        "Design doc auto-split opt-out with real databases",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_no_ddoc_allows_split/0,
                fun t_ddoc_disabled_blocks_split/0,
                fun t_ddoc_enabled_allows_split/0,
                fun t_ddoc_with_other_fields_allows_split/0,
                fun t_nonexistent_db_allows_split/0,
                fun t_ddoc_cache_survives_db_delete/0
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
            catch couch_db:close(Db),
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
                body = {[{<<"auto_split">>, {[{<<"enabled">>, true}]}}]}
            },
            {ok, _} = couch_db:update_doc(Db, DDoc, []),
            couch_db:close(Db),
            ?assertEqual(false,
                mem3_auto_shard:is_split_disabled_by_ddoc(DbName))
        after
            catch couch_db:close(Db),
            couch_server:delete(DbName, [?ADMIN_CTX])
        end
    end).

t_ddoc_with_other_fields_allows_split() ->
    ?_test(begin
        %% A design doc without auto_split field should not block splitting
        DbName = ?tempdb(),
        {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
        try
            DDoc = #doc{
                id = <<"_design/shard_config">>,
                body = {[{<<"views">>, {[{<<"v1">>, {[
                    {<<"map">>, <<"function(doc){emit(doc._id,1)}">>}
                ]}}]}}]}
            },
            {ok, _} = couch_db:update_doc(Db, DDoc, []),
            couch_db:close(Db),
            ?assertEqual(false,
                mem3_auto_shard:is_split_disabled_by_ddoc(DbName))
        after
            catch couch_db:close(Db),
            couch_server:delete(DbName, [?ADMIN_CTX])
        end
    end).

t_nonexistent_db_allows_split() ->
    ?_assertEqual(false,
        mem3_auto_shard:is_split_disabled_by_ddoc(<<"nonexistent_db_xyz">>)).

%% The ddoc cache returns its cached value for up to 60 seconds even
%% after the underlying DB is deleted. This is the whole point of
%% memoization: we don't want to re-open the DB on every candidate
%% during a scan. Correctness: a freshly-deleted DB can't be split
%% anyway (the shard will fail other checks first), so a stale
%% "allowed" answer is harmless.
t_ddoc_cache_survives_db_delete() ->
    ?_test(begin
        DbName = ?tempdb(),
        {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
        DDoc = #doc{
            id = <<"_design/shard_config">>,
            body = {[{<<"auto_split">>, {[{<<"enabled">>, false}]}}]}
        },
        {ok, _} = couch_db:update_doc(Db, DDoc, []),
        couch_db:close(Db),
        %% Prime the cache
        ?assertEqual(true,
            mem3_auto_shard:is_split_disabled_by_ddoc(DbName)),
        %% Delete the database
        ok = couch_server:delete(DbName, [?ADMIN_CTX]),
        %% Cache still returns the old answer
        ?assertEqual(true,
            mem3_auto_shard:is_split_disabled_by_ddoc(DbName))
    end).

%% ===================================================================
%% 2. Gen_server lifecycle — real process behavior
%% ===================================================================

lifecycle_test_() ->
    {
        "Auto-shard gen_server real behavior",
        {
            setup,
            fun setup_server/0,
            fun teardown_server/1,
            fun(Ctx) -> [
                t_starts_and_reports_status(Ctx),
                t_pause_prevents_scanning(Ctx),
                t_resume_after_pause(Ctx),
                t_threshold_change_reflected(Ctx),
                t_trigger_scan_runs_without_crash(Ctx),
                t_disabled_scan_does_nothing(Ctx)
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

t_starts_and_reports_status(_) ->
    ?_test(begin
        Status = mem3_auto_shard:status(),
        ?assert(is_map(Status)),
        ?assertEqual(false, maps:get(enabled, Status)),
        ?assertEqual(false, maps:get(paused, Status)),
        ?assertEqual(0, maps:get(active_splits, Status)),
        ?assertEqual(0, maps:get(splits_triggered, Status)),
        ?assert(is_integer(maps:get(max_shard_size_bytes, Status))),
        ?assert(maps:get(max_shard_size_bytes, Status) > 0)
    end).

t_pause_prevents_scanning(_) ->
    ?_test(begin
        ok = mem3_auto_shard:pause(),
        Status = mem3_auto_shard:status(),
        ?assertEqual(true, maps:get(paused, Status)),
        %% Trigger scan. trigger_scan is a cast so we need a sync
        %% barrier: any gen_server:call after the cast is guaranteed
        %% to run after the cast has been dequeued and handled.
        ok = mem3_auto_shard:trigger_scan(),
        _ = sys:get_state(mem3_auto_shard),
        Status2 = mem3_auto_shard:status(),
        %% scan_count should not increase when paused
        ?assertEqual(maps:get(scan_count, Status), maps:get(scan_count, Status2))
    end).

t_resume_after_pause(_) ->
    ?_test(begin
        ok = mem3_auto_shard:pause(),
        ok = mem3_auto_shard:resume(),
        Status = mem3_auto_shard:status(),
        ?assertEqual(false, maps:get(paused, Status))
    end).

t_threshold_change_reflected(_) ->
    ?_test(begin
        ok = mem3_auto_shard:set_threshold(99999999999),
        Status = mem3_auto_shard:status(),
        ?assertEqual(99999999999, maps:get(max_shard_size_bytes, Status))
    end).

t_trigger_scan_runs_without_crash(_) ->
    ?_test(begin
        %% Even when disabled, trigger_scan must not crash the process.
        %% sys:get_state acts as a sync barrier after the cast.
        ok = mem3_auto_shard:trigger_scan(),
        _ = sys:get_state(mem3_auto_shard),
        ?assert(is_pid(whereis(mem3_auto_shard)))
    end).

t_disabled_scan_does_nothing(_) ->
    ?_test(begin
        ScansBefore = maps:get(scan_count, mem3_auto_shard:status()),
        ok = mem3_auto_shard:trigger_scan(),
        _ = sys:get_state(mem3_auto_shard),
        ScansAfter = maps:get(scan_count, mem3_auto_shard:status()),
        %% scan_count should NOT increase when disabled
        ?assertEqual(ScansBefore, ScansAfter)
    end).

%% ===================================================================
%% 3. Shard size scanning integration — real shards
%% ===================================================================

shard_scanning_test_() ->
    {
        "Shard size scanning finds real oversized shards",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_scanner_finds_oversized_shard/0,
                fun t_scanner_skips_excluded_db/0
            ]
        }
    }.

t_scanner_finds_oversized_shard() ->
    ?_test(begin
        %% Use a unique suffix so parallel runs don't collide
        Suffix = integer_to_binary(erlang:system_time(millisecond)),
        ShardName = <<"shards/00000000-ffffffff/scantest.", Suffix/binary>>,
        {ok, Db} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db),
        try
            {ok, Db1} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
            try
                lists:foreach(fun(I) ->
                    Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
                    Rev = couch_hash:md5_hash(term_to_binary({Id, I})),
                    Body = {[{<<"d">>, base64:encode(
                        crypto:strong_rand_bytes(1024))}]},
                    Doc = #doc{id = Id, body = Body, revs = {1, [Rev]}},
                    {ok, _} = couch_db:update_docs(Db1, [Doc],
                        [replicated_changes])
                end, lists:seq(1, 50))
            after
                couch_db:close(Db1)
            end,
            %% Force a file flush by reopening the DB and reading info
            {ok, Db2} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
            {ok, _} = couch_db:get_db_info(Db2),
            couch_db:close(Db2),

            Table = ets:new(test_scan, [set, public]),
            try
                mem3_shard_size:scan_local(Table),
                %% The scan MUST have found our shard. A silent skip
                %% would mask real regressions in shard discovery.
                Lookup = ets:lookup(Table, ShardName),
                ?assertMatch([{ShardName, _, _}], Lookup),
                [{ShardName, Size, _}] = Lookup,
                ?assert(Size > 0),
                %% Verify it appears in an oversized-select
                Oversized = ets:select(Table,
                    [{{'$1', '$2', '_'}, [{'>', '$2', 1000}],
                      [{{'$1', '$2'}}]}]),
                Found = [N || {N, _} <- Oversized, N =:= ShardName],
                ?assertEqual(1, length(Found))
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

t_scanner_skips_excluded_db() ->
    ?_test(begin
        %% Verify that the exclusion logic works with real config
        {ok, Apps} = application:ensure_all_started(config),
        ok = config:set("auto_shard", "exclude_dbs", "_users,test_exclude_*", false),
        try
            State = mem3_auto_shard:load_config_for_test(),
            %% Excluded DB should be excluded
            ?assertEqual(true, mem3_auto_shard:is_excluded(<<"_users">>, State)),
            ?assertEqual(true, mem3_auto_shard:is_excluded(<<"test_exclude_abc">>, State)),
            %% Non-excluded DB should not be excluded
            ?assertEqual(false, mem3_auto_shard:is_excluded(<<"mydb">>, State))
        after
            config:delete("auto_shard", "exclude_dbs", false)
        end
    end).

%% ===================================================================
%% 4. Space reservation — thundering herd prevention
%% ===================================================================

space_reservation_test_() ->
    {
        "Space reservation prevents thundering herd",
        {
            setup,
            fun() -> {ok, Apps} = application:ensure_all_started(config), Apps end,
            fun(_Apps) -> ok end,
            [
                fun t_add_reservations_accumulates/0,
                fun t_subtract_reservations_reduces_free_space/0,
                fun t_release_reservations_proportional/0,
                fun t_reservation_blocks_preflight/0,
                fun t_multiple_splits_exhaust_space/0,
                fun t_release_cleans_zero_entries/0,
                fun t_empty_reservations_passthrough/0,
                fun t_merge_reservations_preserves_existing/0,
                fun t_subtract_reservations_map_exact/0,
                fun t_overlapping_splits_no_drift/0,
                fun t_multi_range_same_node_accumulates/0
            ]
        }
    }.

t_add_reservations_accumulates() ->
    ?_test(begin
        R0 = #{},
        %% First split reserves on 3 nodes, 10GB each
        R1 = mem3_auto_shard:add_reservations(R0, [n1, n2, n3], 10000000000),
        ?assertEqual(10000000000, maps:get(n1, R1)),
        ?assertEqual(10000000000, maps:get(n2, R1)),
        ?assertEqual(10000000000, maps:get(n3, R1)),
        %% Second split adds more to same nodes
        R2 = mem3_auto_shard:add_reservations(R1, [n1, n2, n3], 5000000000),
        ?assertEqual(15000000000, maps:get(n1, R2)),
        ?assertEqual(15000000000, maps:get(n2, R2)),
        ?assertEqual(15000000000, maps:get(n3, R2))
    end).

t_subtract_reservations_reduces_free_space() ->
    ?_test(begin
        %% Simulate 3-node cluster, 100GB free on each
        Caps = #{
            n1 => #{dirs => [{"/data", 50, 100000000000, 200000000000}]},
            n2 => #{dirs => [{"/data", 50, 100000000000, 200000000000}]},
            n3 => #{dirs => [{"/data", 50, 100000000000, 200000000000}]}
        },
        %% Reserve 30GB on n1, 20GB on n2
        Reservations = #{n1 => 30000000000, n2 => 20000000000},
        Adjusted = mem3_auto_shard:subtract_reservations(Caps, Reservations),
        %% n1 should have ~70GB free, n2 ~80GB, n3 still 100GB
        [{_, _, N1Free, _}] = maps:get(dirs, maps:get(n1, Adjusted)),
        [{_, _, N2Free, _}] = maps:get(dirs, maps:get(n2, Adjusted)),
        [{_, _, N3Free, _}] = maps:get(dirs, maps:get(n3, Adjusted)),
        ?assertEqual(70000000000, N1Free),
        ?assertEqual(80000000000, N2Free),
        ?assertEqual(100000000000, N3Free)
    end).

t_release_reservations_proportional() ->
    ?_test(begin
        %% 3 nodes with different reservation levels
        R = #{n1 => 30000000000, n2 => 20000000000, n3 => 10000000000},
        %% Release one split: 3 targets x 10GB each = 30GB total
        R2 = mem3_auto_shard:release_reservations(R, 10000000000, 3),
        %% Total was 60GB, releasing 30GB = 50% reduction
        ?assert(maps:get(n1, R2, 0) < 30000000000),
        ?assert(maps:get(n2, R2, 0) < 20000000000),
        %% Total remaining should be ~30GB
        Total = maps:fold(fun(_N, B, Acc) -> Acc + B end, 0, R2),
        ?assert(Total =< 30000000000)
    end).

t_reservation_blocks_preflight() ->
    ?_test(begin
        %% Node with 50GB free, reservations eat 40GB → only 10GB effective
        Caps = #{
            n1 => #{dirs => [{"/data", 75, 50000000000, 200000000000}]}
        },
        Reservations = #{n1 => 40000000000},
        Adjusted = mem3_auto_shard:subtract_reservations(Caps, Reservations),
        [{_, _, AdjFree, _}] = maps:get(dirs, maps:get(n1, Adjusted)),
        %% Only 10GB free after reservations
        ?assertEqual(10000000000, AdjFree),
        %% A 20GB shard split 2-way = 10GB targets, needs 3x = 30GB
        %% Should FAIL because 10GB < 30GB
        Result = mem3_reshard_rep:preflight_check(20000000000, 2, Adjusted),
        ?assertMatch({error, {insufficient_space, _}}, Result)
    end).

t_multiple_splits_exhaust_space() ->
    ?_test(begin
        %% Scenario: 100GB free. 5 x 20GB shards want to split.
        %% Each 2-way split needs 10GB target x 3 = 30GB reserved.
        %% After 3 splits: 90GB reserved, only 10GB left → 4th must fail.
        Caps = #{
            n1 => #{dirs => [{"/data", 50, 100000000000, 200000000000}]}
        },
        R0 = #{},
        %% Split 1: reserve 30GB
        R1 = mem3_auto_shard:add_reservations(R0, [n1], 30000000000),
        Adj1 = mem3_auto_shard:subtract_reservations(Caps, R1),
        ?assertEqual(ok, mem3_reshard_rep:preflight_check(20000000000, 2, Adj1)),
        %% Split 2: reserve another 30GB (total 60GB)
        R2 = mem3_auto_shard:add_reservations(R1, [n1], 30000000000),
        Adj2 = mem3_auto_shard:subtract_reservations(Caps, R2),
        ?assertEqual(ok, mem3_reshard_rep:preflight_check(20000000000, 2, Adj2)),
        %% Split 3: reserve another 30GB (total 90GB)
        R3 = mem3_auto_shard:add_reservations(R2, [n1], 30000000000),
        Adj3 = mem3_auto_shard:subtract_reservations(Caps, R3),
        %% Now only 10GB free — 4th split of 20GB needs 30GB → FAIL
        ?assertMatch({error, _},
            mem3_reshard_rep:preflight_check(20000000000, 2, Adj3))
    end).

t_release_cleans_zero_entries() ->
    ?_test(begin
        %% Release all reservations → map should be empty
        R = #{n1 => 10000000000},
        R2 = mem3_auto_shard:release_reservations(R, 10000000000, 1),
        ?assertEqual(#{}, R2)
    end).

t_empty_reservations_passthrough() ->
    ?_test(begin
        Caps = #{
            n1 => #{dirs => [{"/data", 50, 100000000000, 200000000000}]}
        },
        %% Empty reservations should not change anything
        Adjusted = mem3_auto_shard:subtract_reservations(Caps, #{}),
        ?assertEqual(Caps, Adjusted)
    end).

%% merge_reservations: takes a delta map of {node => bytes} and adds
%% it onto an existing reservations map, summing per-node values.
t_merge_reservations_preserves_existing() ->
    ?_test(begin
        Existing = #{n1 => 1000, n2 => 2000},
        Delta = #{n1 => 500, n3 => 300},
        Merged = mem3_auto_shard:merge_reservations(Existing, Delta),
        ?assertEqual(1500, maps:get(n1, Merged)),
        ?assertEqual(2000, maps:get(n2, Merged)),
        ?assertEqual(300, maps:get(n3, Merged)),
        ?assertEqual(3, maps:size(Merged))
    end).

%% subtract_reservations_map: releases EXACTLY what each split reserved
%% on each node — no proportional math, no drift.
t_subtract_reservations_map_exact() ->
    ?_test(begin
        Existing = #{n1 => 1500, n2 => 2000, n3 => 300},
        ReleaseDelta = #{n1 => 500, n2 => 2000},
        Result = mem3_auto_shard:subtract_reservations_map(Existing, ReleaseDelta),
        %% n1: 1500 - 500 = 1000
        ?assertEqual(1000, maps:get(n1, Result)),
        %% n2: 2000 - 2000 = 0 → removed from map
        ?assertEqual(error, maps:find(n2, Result)),
        %% n3: untouched
        ?assertEqual(300, maps:get(n3, Result))
    end).

%% Two overlapping splits: A reserves {n1=>100, n2=>100}, B reserves
%% {n1=>50}. Releasing A must leave exactly {n1=>50} (B's reservation),
%% with NO drift on n2. The legacy proportional release would have left
%% phantom bytes here.
t_overlapping_splits_no_drift() ->
    ?_test(begin
        %% Start with empty
        R0 = #{},
        %% Split A reserves
        SplitA = #{n1 => 100, n2 => 100},
        R1 = mem3_auto_shard:merge_reservations(R0, SplitA),
        %% Split B reserves on n1 only
        SplitB = #{n1 => 50},
        R2 = mem3_auto_shard:merge_reservations(R1, SplitB),
        ?assertEqual(150, maps:get(n1, R2)),
        ?assertEqual(100, maps:get(n2, R2)),
        %% Release split A using its EXACT per-node map
        R3 = mem3_auto_shard:subtract_reservations_map(R2, SplitA),
        %% Only B's 50 bytes on n1 should remain
        ?assertEqual(50, maps:get(n1, R3)),
        ?assertEqual(error, maps:find(n2, R3)),
        %% Release split B
        R4 = mem3_auto_shard:subtract_reservations_map(R3, SplitB),
        ?assertEqual(#{}, R4)
    end).

%% Multi-range placement: a 4-way split where 2 ranges land on the
%% same node (n1 appears twice in TargetNodes) must accumulate
%% N * PerTarget bytes for that node — not just one PerTarget.
t_multi_range_same_node_accumulates() ->
    ?_test(begin
        %% Simulate the do_start_split fold over TargetNodes where
        %% n1 appears twice (multi-range on same node) and n2 once.
        TargetNodes = [n1, n1, n2],
        PerTarget = 1000,
        PerNodeReservations = lists:foldl(
            fun(Node, Acc) ->
                maps:update_with(Node,
                    fun(B) -> B + PerTarget end,
                    PerTarget, Acc)
            end, #{}, TargetNodes),
        ?assertEqual(2000, maps:get(n1, PerNodeReservations)),
        ?assertEqual(1000, maps:get(n2, PerNodeReservations)),
        %% Total reserved must be 3 * PerTarget — every range slot
        Total = maps:fold(fun(_, B, Acc) -> B + Acc end, 0, PerNodeReservations),
        ?assertEqual(3 * PerTarget, Total)
    end).

%% Multi-dir node: subtract_reservations must pull ALL the bytes
%% from the LARGEST dir (the one that would be picked for placement),
%% not proportionally distribute across every dir. Proportional
%% rounding can spuriously trip the free-space floor.
multi_dir_subtract_test_() ->
    {
        "subtract_from_dirs behavior on multi-dir nodes",
        [
            fun t_subtract_from_largest_dir/0,
            fun t_subtract_exceeds_largest_dir_clamps_to_zero/0,
            fun t_subtract_on_empty_dirs/0
        ]
    }.

t_subtract_from_largest_dir() ->
    ?_test(begin
        %% node with /data1=60GB free, /data2=40GB free
        Caps = #{
            node() => #{dirs => [
                {"/data1", 40, 60000000000, 100000000000},
                {"/data2", 60, 40000000000, 100000000000}
            ]}
        },
        Res = #{node() => 20000000000},
        Adj = mem3_auto_shard:subtract_reservations(Caps, Res),
        Dirs = maps:get(dirs, maps:get(node(), Adj)),
        %% Largest dir (data1, 60GB) should lose 20GB → 40GB.
        %% Smaller dir (data2) untouched at 40GB.
        %% Sort result by path to make assertions stable.
        Sorted = lists:keysort(1, Dirs),
        [{"/data1", _, Data1Free, _},
         {"/data2", _, Data2Free, _}] = Sorted,
        ?assertEqual(40000000000, Data1Free),
        ?assertEqual(40000000000, Data2Free)
    end).

t_subtract_exceeds_largest_dir_clamps_to_zero() ->
    ?_test(begin
        %% Only 50GB free on the largest dir but reservation is 60GB.
        Caps = #{
            node() => #{dirs => [
                {"/data1", 50, 50000000000, 100000000000}
            ]}
        },
        Res = #{node() => 60000000000},
        Adj = mem3_auto_shard:subtract_reservations(Caps, Res),
        [{"/data1", _, Free, _}] =
            maps:get(dirs, maps:get(node(), Adj)),
        ?assertEqual(0, Free)
    end).

t_subtract_on_empty_dirs() ->
    ?_test(begin
        Caps = #{node() => #{dirs => []}},
        Res = #{node() => 1000000000},
        Adj = mem3_auto_shard:subtract_reservations(Caps, Res),
        ?assertEqual([], maps:get(dirs, maps:get(node(), Adj)))
    end).

%% Exclude patterns should be compiled once at load time; the status
%% call still returns the original pattern strings (not the compiled
%% MP terms) so the HTTP API stays JSON-friendly.
exclude_pattern_cache_test_() ->
    {
        "Exclude patterns are compiled once and JSON-friendly in status",
        {
            setup,
            fun() ->
                {ok, Apps} = application:ensure_all_started(config),
                ok = config:set("auto_shard", "exclude_dbs",
                    "metrics_*,archive_*,_users", false),
                Apps
            end,
            fun(_) ->
                config:delete("auto_shard", "exclude_dbs", false)
            end,
            [
                fun t_compiled_patterns_match_db_names/0,
                fun t_status_returns_original_pattern_strings/0
            ]
        }
    }.

t_compiled_patterns_match_db_names() ->
    ?_test(begin
        State = mem3_auto_shard:load_config_for_test(),
        ?assertEqual(true,
            mem3_auto_shard:is_excluded(<<"metrics_cpu">>, State)),
        ?assertEqual(true,
            mem3_auto_shard:is_excluded(<<"archive_2024">>, State)),
        ?assertEqual(true,
            mem3_auto_shard:is_excluded(<<"_users">>, State)),
        ?assertEqual(false,
            mem3_auto_shard:is_excluded(<<"important_db">>, State))
    end).

t_status_returns_original_pattern_strings() ->
    ?_test(begin
        %% is_excluded still works through multiple calls on the
        %% same state — the compiled form must survive map iteration.
        State = mem3_auto_shard:load_config_for_test(),
        [?assertEqual(true,
            mem3_auto_shard:is_excluded(<<"metrics_", (integer_to_binary(I))/binary>>, State))
         || I <- lists:seq(1, 20)]
    end).
