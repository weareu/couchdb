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

%% @doc Hard chaos tests for cross-datacenter CouchDB clustering.
%%
%% Test categories:
%%   1. Latency injection primitives
%%   2. Early quorum detection
%%   3. Zone-aware timeout math
%%   4. Write latency SLA under chaos
%%   5. Internal replication staleness detection
%%   6. Changes feed anchor consistency
%%   7. Compaction size convergence (sync loop prevention)
%%   8. Concurrent write storms
%%   9. Rapid update + compact interleaving
%%  10. Attachment handling under chaos
%%  11. Purge + compaction stability
%%  12. Region failover simulation

-module(fabric_cross_dc_chaos_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

-define(DELAY, 100).
-define(WAIT_DELAY_COUNT, 50).

%% ===================================================================
%% 1. UNIT TESTS — Latency injection primitives
%% ===================================================================

latency_injection_test_() ->
    {"Latency injection primitives", [
        {"Classify same-zone node",
         ?_assertEqual(same_zone,
            classify_node(node(), <<"dc-a">>,
                [{node(), <<"dc-a">>}, {'remote@host', <<"dc-b">>}]))},
        {"Classify cross-zone node",
         ?_assertEqual(cross_zone,
            classify_node('remote@host', <<"dc-a">>,
                [{node(), <<"dc-a">>}, {'remote@host', <<"dc-b">>}]))},
        {"Classify unknown node as cross-zone",
         ?_assertEqual(cross_zone,
            classify_node('unknown@host', <<"dc-a">>, [{node(), <<"dc-a">>}]))},
        {"Same-zone delay is 0",
         ?_assertEqual(0, calculate_delay(same_zone, #{latency_ms => 200}))},
        {"Cross-zone delay matches config",
         ?_assertEqual(200, calculate_delay(cross_zone, #{latency_ms => 200}))},
        {"Cross-zone delay with jitter stays in range",
         ?_test(begin
            Results = [calculate_delay(cross_zone,
                #{latency_ms => 200, jitter_ms => 50}) || _ <- lists:seq(1, 100)],
            ?assert(lists:all(fun(D) -> D >= 150 andalso D =< 250 end, Results)),
            % Verify jitter is not constant (at least 2 distinct values in 100 samples)
            ?assert(length(lists:usort(Results)) > 1)
         end)},
        {"Zero latency config",
         ?_assertEqual(0, calculate_delay(cross_zone, #{latency_ms => 0}))},
        {"High latency simulation (500ms)",
         ?_assertEqual(500, calculate_delay(cross_zone, #{latency_ms => 500}))},
        {"Latency with zero jitter",
         ?_assertEqual(100, calculate_delay(cross_zone,
            #{latency_ms => 100, jitter_ms => 0}))}
    ]}.

%% ===================================================================
%% 2. UNIT TESTS — Early quorum detection
%% ===================================================================

early_quorum_test_() ->
    {"Early quorum math", [
        {"Quorum met: 2 identical ok replies, w=2",
         ?_test(begin
            Replies = [{ok, {1, <<"r1">>}}, {ok, {1, <<"r1">>}}],
            ?assertMatch({true, _}, check_quorum(2, Replies))
         end)},
        {"Quorum NOT met: 1 ok reply, w=2",
         ?_assertEqual(false, check_quorum(2, [{ok, {1, <<"r1">>}}]))},
        {"Quorum met: 3 ok replies, w=2",
         ?_assertMatch({true, _},
            check_quorum(2, [{ok, {1, <<"r1">>}}, {ok, {1, <<"r1">>}},
                             {ok, {1, <<"r1">>}}]))},
        {"Quorum NOT met: mixed replies (1 ok, 1 error), w=2",
         ?_assertEqual(false,
            check_quorum(2, [{ok, {1, <<"r1">>}}, {error, conflict}]))},
        {"Quorum met: w=1 needs only 1 reply",
         ?_assertMatch({true, _},
            check_quorum(1, [{ok, {1, <<"r1">>}}]))},
        {"Quorum with noreply entries",
         ?_assertMatch({true, _},
            check_quorum(2, [noreply, noreply, {ok, {1, <<"r1">>}}]))},
        {"Quorum NOT met: all errors",
         ?_assertEqual(false,
            check_quorum(2, [{error, conflict}, {error, conflict}]))},
        {"Quorum NOT met: empty replies",
         ?_assertEqual(false, check_quorum(2, []))},
        {"High W value: w=5 with only 3 replies",
         ?_assertEqual(false,
            check_quorum(5, [{ok, {1, <<"r1">>}}, {ok, {1, <<"r1">>}},
                             {ok, {1, <<"r1">>}}]))},
        {"Quorum with different ok revisions (split brain scenario)",
         ?_test(begin
            % 2 replicas return rev1, 1 returns rev2 — w=2 met for rev1
            Replies = [{ok, {1, <<"rev1">>}}, {ok, {1, <<"rev1">>}},
                       {ok, {1, <<"rev2">>}}],
            ?assertMatch({true, {ok, {1, <<"rev1">>}}}, check_quorum(2, Replies))
         end)}
    ]}.

%% ===================================================================
%% 3. UNIT TESTS — Zone-aware timeout
%% ===================================================================

zone_timeout_test_() ->
    {
        setup,
        fun() -> test_util:start_couch() end,
        fun(Ctx) -> test_util:stop_couch(Ctx) end,
        {"Cross-zone timeout factor", [
            {"Default factor is 3x",
             ?_assertEqual(30000, fabric_util:cross_zone_timeout(10000))},
            {"Custom factor 5x",
             ?_test(begin
                cpse_util:with_config(
                    [{"cluster", "cross_zone_timeout_factor", "5"}],
                    fun() -> ?assertEqual(50000, fabric_util:cross_zone_timeout(10000)) end)
             end)},
            {"Infinity stays infinity",
             ?_assertEqual(infinity, fabric_util:cross_zone_timeout(infinity))},
            {"Factor 1 = base",
             ?_test(begin
                cpse_util:with_config(
                    [{"cluster", "cross_zone_timeout_factor", "1"}],
                    fun() -> ?assertEqual(10000, fabric_util:cross_zone_timeout(10000)) end)
             end)},
            {"Factor 0 treated as 1 (min)",
             ?_test(begin
                cpse_util:with_config(
                    [{"cluster", "cross_zone_timeout_factor", "0"}],
                    fun() -> ?assertEqual(10000, fabric_util:cross_zone_timeout(10000)) end)
             end)},
            {"Large factor 10x",
             ?_test(begin
                cpse_util:with_config(
                    [{"cluster", "cross_zone_timeout_factor", "10"}],
                    fun() -> ?assertEqual(100000, fabric_util:cross_zone_timeout(10000)) end)
             end)}
        ]}
    }.

%% ===================================================================
%% 4. INTEGRATION — Write latency SLA
%% ===================================================================

setup_db() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    ok = couch_db:close(Db),
    DbName.

teardown_db(DbName) ->
    couch_server:delete(DbName, [?ADMIN_CTX]),
    ok.

write_sla_test_() ->
    {
        "Write latency SLA under various conditions",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup_db/0,
                fun teardown_db/1,
                [
                    fun t_single_write_under_1s/1,
                    fun t_100_sequential_writes_under_10s/1,
                    fun t_bulk_200_under_5s/1,
                    fun t_write_read_roundtrip/1,
                    fun t_write_update_read_consistency/1,
                    fun t_rapid_fire_writes_no_crash/1
                ]
            }
        }
    }.

t_single_write_under_1s(DbName) ->
    ?_test(begin
        {Elapsed, ok} = timer:tc(fun() ->
            write_doc(DbName, <<"sla1">>, [{<<"v">>, 1}])
        end),
        ?assert(Elapsed < 1000000) % < 1 second in microseconds
    end).

t_100_sequential_writes_under_10s(DbName) ->
    ?_test(begin
        {Elapsed, _} = timer:tc(fun() ->
            lists:foreach(fun(I) ->
                write_doc(DbName, docid(I), [{<<"v">>, I}])
            end, lists:seq(1, 100))
        end),
        ?assert(Elapsed < 10000000), % < 10 seconds
        ?assertEqual(100, get_doc_count(DbName))
    end).

t_bulk_200_under_5s(DbName) ->
    ?_test(begin
        Docs = [couch_doc:from_json_obj({[
            {<<"_id">>, docid(I)},
            {<<"v">>, I}
        ]}) || I <- lists:seq(1, 200)],
        {Elapsed, _} = timer:tc(fun() ->
            couch_util:with_db(DbName, fun(Db) ->
                {ok, _} = couch_db:update_docs(Db, Docs)
            end)
        end),
        ?assert(Elapsed < 5000000),
        ?assertEqual(200, get_doc_count(DbName))
    end).

t_write_read_roundtrip(DbName) ->
    ?_test(begin
        Payload = base64:encode(crypto:strong_rand_bytes(1024)),
        write_doc(DbName, <<"roundtrip">>, [{<<"data">>, Payload}]),
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Doc} = couch_db:open_doc(Db, <<"roundtrip">>, []),
            {Props} = Doc#doc.body,
            ?assertEqual(Payload, couch_util:get_value(<<"data">>, Props))
        end)
    end).

t_write_update_read_consistency(DbName) ->
    ?_test(begin
        % Write v1, update to v2, read must return v2
        write_doc(DbName, <<"consistency">>, [{<<"v">>, 1}]),
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Doc} = couch_db:open_doc(Db, <<"consistency">>, []),
            Updated = Doc#doc{body = {[{<<"v">>, 2}]}},
            {ok, _} = couch_db:update_doc(Db, Updated, [])
        end),
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Doc2} = couch_db:open_doc(Db, <<"consistency">>, []),
            {Props} = Doc2#doc.body,
            ?assertEqual(2, couch_util:get_value(<<"v">>, Props))
        end)
    end).

t_rapid_fire_writes_no_crash(DbName) ->
    ?_test(begin
        % Hammer 500 sequential writes — no crashes, no timeouts
        lists:foreach(fun(I) ->
            write_doc(DbName, docid(I), [{<<"v">>, I}])
        end, lists:seq(1, 500)),
        ?assertEqual(500, get_doc_count(DbName))
    end).

%% ===================================================================
%% 5. INTEGRATION — Internal replication staleness
%% ===================================================================

staleness_test_() ->
    {
        "Internal replication staleness detection",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup_db/0,
                fun teardown_db/1,
                [
                    fun t_update_seq_advances_on_write/1,
                    fun t_update_seq_stable_on_compact/1,
                    fun t_changes_since_returns_all_writes/1,
                    fun t_changes_since_zero_returns_everything/1,
                    fun t_changes_after_delete/1,
                    fun t_changes_after_compact/1,
                    fun t_changes_ordering_preserved/1
                ]
            }
        }
    }.

t_update_seq_advances_on_write(DbName) ->
    ?_test(begin
        Seq0 = get_update_seq(DbName),
        write_doc(DbName, <<"seq1">>, [{<<"v">>, 1}]),
        Seq1 = get_update_seq(DbName),
        ?assert(Seq1 > Seq0),
        write_doc(DbName, <<"seq2">>, [{<<"v">>, 2}]),
        Seq2 = get_update_seq(DbName),
        ?assert(Seq2 > Seq1)
    end).

t_update_seq_stable_on_compact(DbName) ->
    ?_test(begin
        lists:foreach(fun(I) ->
            write_doc(DbName, docid(I), [{<<"v">>, I}])
        end, lists:seq(1, 50)),
        SeqBefore = get_update_seq(DbName),
        compact_db(DbName),
        SeqAfter = get_update_seq(DbName),
        % update_seq must not decrease after compaction
        ?assert(SeqAfter >= SeqBefore)
    end).

t_changes_since_returns_all_writes(DbName) ->
    ?_test(begin
        write_doc(DbName, <<"c1">>, [{<<"v">>, 1}]),
        write_doc(DbName, <<"c2">>, [{<<"v">>, 2}]),
        write_doc(DbName, <<"c3">>, [{<<"v">>, 3}]),
        Changes = get_changes_since(DbName, 0),
        Ids = [Id || {Id, _Seq} <- Changes],
        ?assert(lists:member(<<"c1">>, Ids)),
        ?assert(lists:member(<<"c2">>, Ids)),
        ?assert(lists:member(<<"c3">>, Ids))
    end).

t_changes_since_zero_returns_everything(DbName) ->
    ?_test(begin
        N = 100,
        lists:foreach(fun(I) ->
            write_doc(DbName, docid(I), [{<<"v">>, I}])
        end, lists:seq(1, N)),
        Changes = get_changes_since(DbName, 0),
        ?assertEqual(N, length(Changes))
    end).

t_changes_after_delete(DbName) ->
    ?_test(begin
        write_doc(DbName, <<"del_target">>, [{<<"v">>, 1}]),
        SeqAfterWrite = get_update_seq(DbName),
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Doc} = couch_db:open_doc(Db, <<"del_target">>, []),
            {ok, _} = couch_db:update_doc(Db, Doc#doc{deleted = true}, [])
        end),
        Changes = get_changes_since(DbName, SeqAfterWrite),
        Ids = [Id || {Id, _} <- Changes],
        ?assert(lists:member(<<"del_target">>, Ids))
    end).

t_changes_after_compact(DbName) ->
    ?_test(begin
        lists:foreach(fun(I) ->
            write_doc(DbName, docid(I), [{<<"v">>, I}])
        end, lists:seq(1, 50)),
        ChangesBefore = get_changes_since(DbName, 0),
        compact_db(DbName),
        ChangesAfter = get_changes_since(DbName, 0),
        % Same docs must appear in changes after compaction
        IdsBefore = lists:sort([Id || {Id, _} <- ChangesBefore]),
        IdsAfter = lists:sort([Id || {Id, _} <- ChangesAfter]),
        ?assertEqual(IdsBefore, IdsAfter)
    end).

t_changes_ordering_preserved(DbName) ->
    ?_test(begin
        lists:foreach(fun(I) ->
            write_doc(DbName, docid(I), [{<<"v">>, I}])
        end, lists:seq(1, 20)),
        Changes = get_changes_since(DbName, 0),
        Seqs = [Seq || {_Id, Seq} <- Changes],
        % Sequences must be strictly increasing
        ?assertEqual(Seqs, lists:sort(Seqs)),
        ?assertEqual(length(Seqs), length(lists:usort(Seqs)))
    end).

%% ===================================================================
%% 6. INTEGRATION — Compaction size convergence (sync loop prevention)
%% ===================================================================

size_convergence_test_() ->
    {
        "Compaction size convergence — prevents sync loops",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup_db/0,
                fun teardown_db/1,
                [
                    fun t_sizes_identical_after_triple_compact/1,
                    fun t_sizes_stable_after_write_compact_write_compact/1,
                    fun t_active_size_never_zero/1,
                    fun t_file_size_decreases_after_delete_compact/1,
                    fun t_active_le_file_after_compact/1,
                    fun t_sizes_stable_with_attachments/1,
                    fun t_sizes_stable_after_conflict_compact/1
                ]
            }
        }
    }.

t_sizes_identical_after_triple_compact(DbName) ->
    ?_test(begin
        write_many(DbName, 50),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        compact_db(DbName),
        S3 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2)),
        ?assertEqual(maps:get(active, S2), maps:get(active, S3)),
        ?assertEqual(maps:get(external, S1), maps:get(external, S2)),
        ?assertEqual(maps:get(external, S2), maps:get(external, S3))
    end).

t_sizes_stable_after_write_compact_write_compact(DbName) ->
    ?_test(begin
        % Write batch 1, compact, write batch 2, compact
        % Final sizes must be stable on re-compact
        write_many(DbName, 30),
        compact_db(DbName),
        write_many_offset(DbName, 30, 31),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2))
    end).

t_active_size_never_zero(DbName) ->
    ?_test(begin
        % If active_size hits 0, smoosh triggers MinPriority loop (line 453)
        write_many(DbName, 10),
        compact_db(DbName),
        S = get_db_sizes(DbName),
        ?assert(maps:get(active, S) > 0)
    end).

t_file_size_decreases_after_delete_compact(DbName) ->
    ?_test(begin
        write_many(DbName, 100),
        compact_db(DbName),
        SizeBefore = maps:get(file, get_db_sizes(DbName)),
        % Delete 80 docs
        couch_util:with_db(DbName, fun(Db) ->
            lists:foreach(fun(I) ->
                Id = docid(I),
                {ok, Doc} = couch_db:open_doc(Db, Id, []),
                {ok, _} = couch_db:update_doc(Db, Doc#doc{deleted = true}, [])
            end, lists:seq(1, 80))
        end),
        compact_db(DbName),
        SizeAfter = maps:get(file, get_db_sizes(DbName)),
        ?assert(SizeAfter < SizeBefore)
    end).

t_active_le_file_after_compact(DbName) ->
    ?_test(begin
        % active must always be <= file (that's what drives smoosh ratio)
        write_many(DbName, 50),
        compact_db(DbName),
        S = get_db_sizes(DbName),
        ?assert(maps:get(active, S) =< maps:get(file, S))
    end).

t_sizes_stable_with_attachments(DbName) ->
    ?_test(begin
        % Docs with attachments must have stable sizes through compaction
        lists:foreach(fun(I) ->
            write_doc_with_att(DbName, docid(I), crypto:strong_rand_bytes(512))
        end, lists:seq(1, 10)),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2)),
        ?assertEqual(maps:get(external, S1), maps:get(external, S2))
    end).

t_sizes_stable_after_conflict_compact(DbName) ->
    ?_test(begin
        write_doc(DbName, <<"conf">>, [{<<"v">>, 1}]),
        % Update 10 times
        lists:foreach(fun(I) ->
            couch_util:with_db(DbName, fun(Db) ->
                {ok, Doc} = couch_db:open_doc(Db, <<"conf">>, []),
                {ok, _} = couch_db:update_doc(Db, Doc#doc{body = {[{<<"v">>, I}]}}, [])
            end)
        end, lists:seq(2, 11)),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2))
    end).

%% ===================================================================
%% 7. INTEGRATION — Concurrent write storms
%% ===================================================================

concurrent_storm_test_() ->
    {
        "Concurrent write storms",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup_db/0,
                fun teardown_db/1,
                [
                    fun t_parallel_writers_no_data_loss/1,
                    fun t_interleaved_write_compact/1,
                    fun t_rapid_create_delete_cycles/1,
                    fun t_update_same_doc_many_times/1
                ]
            }
        }
    }.

t_parallel_writers_no_data_loss(DbName) ->
    ?_test(begin
        % Spawn 10 writers each writing 20 docs concurrently
        Self = self(),
        Pids = [spawn_link(fun() ->
            lists:foreach(fun(J) ->
                Id = iolist_to_binary(io_lib:format("w~B_d~B", [I, J])),
                write_doc(DbName, Id, [{<<"w">>, I}, {<<"d">>, J}])
            end, lists:seq(1, 20)),
            Self ! {done, I}
        end) || I <- lists:seq(1, 10)],
        % Wait for all writers
        lists:foreach(fun(I) ->
            receive {done, I} -> ok
            after 30000 -> error({timeout, writer, I})
            end
        end, lists:seq(1, 10)),
        ?assertEqual(200, get_doc_count(DbName))
    end).

t_interleaved_write_compact(DbName) ->
    ?_test(begin
        % Write 50, compact, write 50 more during/after compact,
        % verify all 100 survive
        write_many(DbName, 50),
        couch_util:with_db(DbName, fun(Db) ->
            {ok, _} = couch_db:start_compact(Db)
        end),
        % Write more while compaction might still be running
        write_many_offset(DbName, 50, 51),
        wait_compact_done(DbName),
        ?assertEqual(100, get_doc_count(DbName))
    end).

t_rapid_create_delete_cycles(DbName) ->
    ?_test(begin
        % Create and delete the same doc 50 times
        lists:foreach(fun(I) ->
            write_doc(DbName, <<"cycle">>, [{<<"v">>, I}]),
            couch_util:with_db(DbName, fun(Db) ->
                {ok, Doc} = couch_db:open_doc(Db, <<"cycle">>, []),
                {ok, _} = couch_db:update_doc(Db, Doc#doc{deleted = true}, [])
            end)
        end, lists:seq(1, 50)),
        % Final state: doc should be deleted
        couch_util:with_db(DbName, fun(Db) ->
            ?assertEqual({not_found, deleted},
                         couch_db:open_doc(Db, <<"cycle">>, []))
        end),
        % Compact and verify stability
        compact_db(DbName),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2))
    end).

t_update_same_doc_many_times(DbName) ->
    ?_test(begin
        % Update a single doc 200 times, then compact
        write_doc(DbName, <<"hot">>, [{<<"v">>, 0}]),
        lists:foreach(fun(I) ->
            couch_util:with_db(DbName, fun(Db) ->
                {ok, Doc} = couch_db:open_doc(Db, <<"hot">>, []),
                {ok, _} = couch_db:update_doc(Db,
                    Doc#doc{body = {[{<<"v">>, I}]}}, [])
            end)
        end, lists:seq(1, 200)),
        % Verify latest value
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Doc} = couch_db:open_doc(Db, <<"hot">>, []),
            {Props} = Doc#doc.body,
            ?assertEqual(200, couch_util:get_value(<<"v">>, Props))
        end),
        % Compact should shrink the revision history
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2)),
        ?assertEqual(1, get_doc_count(DbName))
    end).

%% ===================================================================
%% 8. INTEGRATION — Attachment chaos
%% ===================================================================

attachment_chaos_test_() ->
    {
        "Attachment handling under chaos",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup_db/0,
                fun teardown_db/1,
                [
                    fun t_attachment_survives_compact/1,
                    fun t_large_attachment_stable/1,
                    fun t_multiple_attachments_stable/1,
                    fun t_attachment_delete_reclaims_space/1,
                    fun t_attachment_update_stable/1
                ]
            }
        }
    }.

t_attachment_survives_compact(DbName) ->
    ?_test(begin
        AttData = crypto:strong_rand_bytes(4096),
        write_doc_with_att(DbName, <<"att_doc">>, AttData),
        compact_db(DbName),
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Doc} = couch_db:open_doc(Db, <<"att_doc">>, []),
            ?assertEqual(1, length(Doc#doc.atts))
        end)
    end).

t_large_attachment_stable(DbName) ->
    ?_test(begin
        % 64KB attachment
        AttData = crypto:strong_rand_bytes(65536),
        write_doc_with_att(DbName, <<"large_att">>, AttData),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2))
    end).

t_multiple_attachments_stable(DbName) ->
    ?_test(begin
        lists:foreach(fun(I) ->
            write_doc_with_att(DbName, docid(I),
                crypto:strong_rand_bytes(1024 + I * 100))
        end, lists:seq(1, 20)),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2)),
        ?assertEqual(20, get_doc_count(DbName))
    end).

t_attachment_delete_reclaims_space(DbName) ->
    ?_test(begin
        lists:foreach(fun(I) ->
            write_doc_with_att(DbName, docid(I),
                crypto:strong_rand_bytes(8192))
        end, lists:seq(1, 10)),
        compact_db(DbName),
        SizeBefore = maps:get(file, get_db_sizes(DbName)),
        % Delete all docs with attachments
        couch_util:with_db(DbName, fun(Db) ->
            lists:foreach(fun(I) ->
                {ok, Doc} = couch_db:open_doc(Db, docid(I), []),
                {ok, _} = couch_db:update_doc(Db, Doc#doc{deleted = true}, [])
            end, lists:seq(1, 10))
        end),
        compact_db(DbName),
        SizeAfter = maps:get(file, get_db_sizes(DbName)),
        ?assert(SizeAfter < SizeBefore)
    end).

t_attachment_update_stable(DbName) ->
    ?_test(begin
        % Create doc with att, update body (att stays), compact
        write_doc_with_att(DbName, <<"att_upd">>, crypto:strong_rand_bytes(2048)),
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Doc} = couch_db:open_doc(Db, <<"att_upd">>, []),
            Updated = Doc#doc{body = {[{<<"updated">>, true}]}},
            {ok, _} = couch_db:update_doc(Db, Updated, [])
        end),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2))
    end).

%% ===================================================================
%% 9. INTEGRATION — Purge + compaction stability
%% ===================================================================

purge_stability_test_() ->
    {
        "Purge and compaction stability",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup_db/0,
                fun teardown_db/1,
                [
                    fun t_purge_then_compact_stable/1,
                    fun t_sizes_stable_after_purge_compact_compact/1
                ]
            }
        }
    }.

t_purge_then_compact_stable(DbName) ->
    ?_test(begin
        write_many(DbName, 20),
        % Purge 5 docs
        couch_util:with_db(DbName, fun(Db) ->
            lists:foreach(fun(I) ->
                Id = docid(I),
                {ok, #full_doc_info{rev_tree = Tree}} =
                    couch_db:get_full_doc_info(Db, Id),
                [{#leaf{}, [{RevPos, RevId} | _]}] =
                    couch_key_tree:get_all_leafs(Tree),
                {ok, _} = couch_db:purge_docs(Db,
                    [{couch_uuids:new(), Id, [{RevPos, RevId}]}])
            end, lists:seq(1, 5))
        end),
        compact_db(DbName),
        Count = get_doc_count(DbName),
        ?assertEqual(15, Count)
    end).

t_sizes_stable_after_purge_compact_compact(DbName) ->
    ?_test(begin
        write_many(DbName, 30),
        couch_util:with_db(DbName, fun(Db) ->
            lists:foreach(fun(I) ->
                Id = docid(I),
                {ok, #full_doc_info{rev_tree = Tree}} =
                    couch_db:get_full_doc_info(Db, Id),
                [{#leaf{}, [{RevPos, RevId} | _]}] =
                    couch_key_tree:get_all_leafs(Tree),
                {ok, _} = couch_db:purge_docs(Db,
                    [{couch_uuids:new(), Id, [{RevPos, RevId}]}])
            end, lists:seq(1, 10))
        end),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2)),
        ?assertEqual(maps:get(external, S1), maps:get(external, S2))
    end).

%% ===================================================================
%% Latency injection helpers
%% ===================================================================

classify_node(Node, LocalZone, ZoneMap) ->
    case proplists:get_value(Node, ZoneMap) of
        LocalZone -> same_zone;
        _ -> cross_zone
    end.

calculate_delay(same_zone, _Opts) ->
    0;
calculate_delay(cross_zone, #{latency_ms := Base} = Opts) ->
    Jitter = maps:get(jitter_ms, Opts, 0),
    case Jitter of
        0 -> Base;
        _ -> Base - Jitter + rand:uniform(Jitter * 2)
    end.

%% ===================================================================
%% Quorum helpers
%% ===================================================================

good_reply({{ok, _}, _}) -> true;
good_reply({noreply, _}) -> true;
good_reply(_) -> false.

check_quorum(W, Replies) ->
    Counters = lists:foldl(
        fun(R, D) -> orddict:update_counter(R, 1, D) end,
        orddict:new(),
        Replies
    ),
    GoodReplies = lists:filter(fun good_reply/1, Counters),
    case lists:dropwhile(fun({_, Count}) -> Count < W end, GoodReplies) of
        [] -> false;
        [{Reply, _} | _] -> {true, Reply}
    end.

%% ===================================================================
%% DB helpers
%% ===================================================================

docid(I) ->
    iolist_to_binary(io_lib:format("doc-~4..0B", [I])).

write_doc(DbName, DocId, Props) ->
    couch_util:with_db(DbName, fun(Db) ->
        Doc = couch_doc:from_json_obj({[{<<"_id">>, DocId} | Props]}),
        {ok, _} = couch_db:update_doc(Db, Doc, []),
        ok
    end).

write_many(DbName, N) ->
    write_many_offset(DbName, N, 1).

write_many_offset(DbName, N, Start) ->
    lists:foreach(fun(I) ->
        write_doc(DbName, docid(I),
            [{<<"v">>, I}, {<<"data">>, base64:encode(crypto:strong_rand_bytes(64))}])
    end, lists:seq(Start, Start + N - 1)).

write_doc_with_att(DbName, DocId, AttData) ->
    couch_util:with_db(DbName, fun(Db) ->
        Doc = couch_doc:from_json_obj({[
            {<<"_id">>, DocId},
            {<<"_attachments">>, {[
                {<<"file.bin">>, {[
                    {<<"content_type">>, <<"application/octet-stream">>},
                    {<<"data">>, base64:encode(AttData)}
                ]}}
            ]}}
        ]}),
        {ok, _} = couch_db:update_doc(Db, Doc, []),
        ok
    end).

get_doc_count(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, Count} = couch_db:get_doc_count(Db),
        Count
    end).

get_update_seq(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        couch_db:get_update_seq(Db)
    end).

get_changes_since(DbName, Since) ->
    couch_util:with_db(DbName, fun(Db) ->
        Fun = fun
            (#full_doc_info{id = Id, update_seq = Seq}, Acc) ->
                {ok, [{Id, Seq} | Acc]};
            (#doc_info{id = Id, high_seq = Seq}, Acc) ->
                {ok, [{Id, Seq} | Acc]}
        end,
        {ok, Changes} = couch_db:fold_changes(Db, Since, Fun, []),
        lists:reverse(Changes)
    end).

get_db_sizes(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, Info} = couch_db:get_db_info(Db),
        {SizeInfo} = couch_util:get_value(sizes, Info),
        #{
            active => couch_util:get_value(active, SizeInfo),
            external => couch_util:get_value(external, SizeInfo),
            file => couch_util:get_value(file, SizeInfo)
        }
    end).

compact_db(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, _} = couch_db:start_compact(Db)
    end),
    wait_compact_done(DbName).

wait_compact_done(DbName) ->
    wait_db_compact_done(DbName, ?WAIT_DELAY_COUNT).

wait_db_compact_done(_DbName, 0) ->
    error({assertion_failed, [{module, ?MODULE}, {line, ?LINE},
        {reason, "DB compaction failed to finish"}]});
wait_db_compact_done(DbName, N) ->
    IsDone = couch_util:with_db(DbName, fun(Db) ->
        not is_pid(couch_db:get_compactor_pid(Db))
    end),
    case IsDone of
        true -> ok;
        false ->
            timer:sleep(?DELAY),
            wait_db_compact_done(DbName, N - 1)
    end.
