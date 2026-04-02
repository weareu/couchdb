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

-module(couch_bt_engine_compactor_retention_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(DELAY, 100).
-define(WAIT_DELAY_COUNT, 50).
-define(COMPACTOR, couch_bt_engine_compactor).

%% ===================================================================
%% 1. UNIT TESTS — Date parsing
%% ===================================================================

date_parsing_test_() ->
    {"ISO 8601 date parsing", [
        {"Valid date with time",
         ?_assertEqual({ok, {2023, 6, 15}},
            ?COMPACTOR:parse_iso8601_date(<<"2023-06-15T10:30:00Z">>))},
        {"Valid date without time",
         ?_assertEqual({ok, {2024, 1, 1}},
            ?COMPACTOR:parse_iso8601_date(<<"2024-01-01">>))},
        {"Date with milliseconds",
         ?_assertEqual({ok, {2023, 12, 25}},
            ?COMPACTOR:parse_iso8601_date(<<"2023-12-25T10:30:00.000Z">>))},
        {"Date with timezone offset",
         ?_assertEqual({ok, {2024, 7, 4}},
            ?COMPACTOR:parse_iso8601_date(<<"2024-07-04T12:00:00+02:00">>))},
        {"Leap day",
         ?_assertEqual({ok, {2024, 2, 29}},
            ?COMPACTOR:parse_iso8601_date(<<"2024-02-29T00:00:00Z">>))},
        {"Invalid: Feb 29 on non-leap year",
         ?_assertMatch({error, _},
            ?COMPACTOR:parse_iso8601_date(<<"2023-02-29T00:00:00Z">>))},
        {"Invalid: month 13",
         ?_assertMatch({error, _},
            ?COMPACTOR:parse_iso8601_date(<<"2024-13-01T00:00:00Z">>))},
        {"Invalid: day 32",
         ?_assertMatch({error, _},
            ?COMPACTOR:parse_iso8601_date(<<"2024-01-32T00:00:00Z">>))},
        {"Invalid: not a date",
         ?_assertMatch({error, _},
            ?COMPACTOR:parse_iso8601_date(<<"not-a-date">>))},
        {"Invalid: empty binary",
         ?_assertMatch({error, _},
            ?COMPACTOR:parse_iso8601_date(<<>>))},
        {"Invalid: month 0",
         ?_assertMatch({error, _},
            ?COMPACTOR:parse_iso8601_date(<<"2024-00-15">>))},
        {"Invalid: just numbers",
         ?_assertMatch({error, _},
            ?COMPACTOR:parse_iso8601_date(<<"20240115">>))},
        {"Boundary: year 1970",
         ?_assertEqual({ok, {1970, 1, 1}},
            ?COMPACTOR:parse_iso8601_date(<<"1970-01-01T00:00:00Z">>))},
        {"Boundary: far future",
         ?_assertEqual({ok, {2099, 12, 31}},
            ?COMPACTOR:parse_iso8601_date(<<"2099-12-31T23:59:59Z">>))}
    ]}.

%% ===================================================================
%% 2. UNIT TESTS — Age comparison
%% ===================================================================

is_older_than_days_test_() ->
    {{CurY, CurM, CurD}, _} = calendar:universal_time(),
    TodayDays = calendar:date_to_gregorian_days({CurY, CurM, CurD}),
    {Old400Y, Old400M, Old400D} = calendar:gregorian_days_to_date(TodayDays - 400),
    {Recent10Y, Recent10M, Recent10D} = calendar:gregorian_days_to_date(TodayDays - 10),
    {ExactY, ExactM, ExactD} = calendar:gregorian_days_to_date(TodayDays - 365),
    {JustUnderY, JustUnderM, JustUnderD} = calendar:gregorian_days_to_date(TodayDays - 364),
    {"Age comparison", [
        {"400 days ago > 365 day threshold",
         ?_assertEqual(true,
            ?COMPACTOR:is_older_than_days({Old400Y, Old400M, Old400D}, 365))},
        {"10 days ago < 365 day threshold",
         ?_assertEqual(false,
            ?COMPACTOR:is_older_than_days({Recent10Y, Recent10M, Recent10D}, 365))},
        {"400 days ago < 500 day threshold",
         ?_assertEqual(false,
            ?COMPACTOR:is_older_than_days({Old400Y, Old400M, Old400D}, 500))},
        {"Today < 0 day threshold (edge case)",
         ?_assertEqual(false,
            ?COMPACTOR:is_older_than_days({CurY, CurM, CurD}, 0))},
        {"Exactly 365 days ago is NOT older (boundary: must be strictly older)",
         ?_assertEqual(false,
            ?COMPACTOR:is_older_than_days({ExactY, ExactM, ExactD}, 365))},
        {"364 days ago is NOT older than 365",
         ?_assertEqual(false,
            ?COMPACTOR:is_older_than_days({JustUnderY, JustUnderM, JustUnderD}, 365))},
        {"400 days ago > 1 day threshold",
         ?_assertEqual(true,
            ?COMPACTOR:is_older_than_days({Old400Y, Old400M, Old400D}, 1))},
        {"Very large threshold: 100000 days",
         ?_assertEqual(false,
            ?COMPACTOR:is_older_than_days({Old400Y, Old400M, Old400D}, 100000))}
    ]}.

%% ===================================================================
%% 3. UNIT TESTS — Date field lookup in document properties
%% ===================================================================

find_date_in_props_test_() ->
    {"Document date field lookup", [
        {"First field match",
         ?_assertEqual({ok, {2023, 6, 15}},
            ?COMPACTOR:find_date_in_props(
                [{<<"date">>, <<"2023-06-15T00:00:00Z">>}],
                [<<"date">>]))},
        {"Second field when first missing",
         ?_assertEqual({ok, {2024, 1, 1}},
            ?COMPACTOR:find_date_in_props(
                [{<<"created_at">>, <<"2024-01-01T00:00:00Z">>}],
                [<<"date">>, <<"created_at">>]))},
        {"No matching fields",
         ?_assertEqual(undefined,
            ?COMPACTOR:find_date_in_props(
                [{<<"name">>, <<"test">>}],
                [<<"date">>, <<"created_at">>]))},
        {"Skip non-binary date values (integer)",
         ?_assertEqual(undefined,
            ?COMPACTOR:find_date_in_props(
                [{<<"date">>, 12345}],
                [<<"date">>]))},
        {"Skip non-binary date values (null/atom)",
         ?_assertEqual(undefined,
            ?COMPACTOR:find_date_in_props(
                [{<<"date">>, null}],
                [<<"date">>]))},
        {"Skip non-binary date values (nested object)",
         ?_assertEqual(undefined,
            ?COMPACTOR:find_date_in_props(
                [{<<"date">>, {[{<<"year">>, 2024}]}}],
                [<<"date">>]))},
        {"Skip invalid format, find next valid field",
         ?_assertEqual({ok, {2024, 3, 15}},
            ?COMPACTOR:find_date_in_props(
                [{<<"date">>, <<"garbage">>},
                 {<<"created_at">>, <<"2024-03-15T12:00:00Z">>}],
                [<<"date">>, <<"created_at">>]))},
        {"Empty field list",
         ?_assertEqual(undefined,
            ?COMPACTOR:find_date_in_props(
                [{<<"date">>, <<"2024-01-01">>}],
                []))},
        {"Empty props list",
         ?_assertEqual(undefined,
            ?COMPACTOR:find_date_in_props([], [<<"date">>]))},
        {"Multiple date fields, first valid wins",
         ?_assertEqual({ok, {2020, 1, 1}},
            ?COMPACTOR:find_date_in_props(
                [{<<"date">>, <<"2020-01-01T00:00:00Z">>},
                 {<<"created_at">>, <<"2024-06-15T00:00:00Z">>}],
                [<<"date">>, <<"created_at">>]))}
    ]}.

%% ===================================================================
%% 4. UNIT TESTS — Path sanitization (security)
%% ===================================================================

sanitize_path_test_() ->
    {"Path traversal prevention", [
        {"Normal doc ID unchanged",
         ?_assertEqual("my-doc-123",
            ?COMPACTOR:sanitize_path_component("my-doc-123"))},
        {"Binary input works",
         ?_assertEqual("my-doc",
            ?COMPACTOR:sanitize_path_component(<<"my-doc">>))},
        {"Double dots replaced",
         ?_assertEqual("_/etc/passwd",
            ?COMPACTOR:sanitize_path_component("../etc/passwd"))},
        {"Forward slashes replaced",
         ?_assertEqual("a_b_c",
            ?COMPACTOR:sanitize_path_component("a/b/c"))},
        {"Backslashes replaced",
         ?_assertEqual("a_b_c",
            ?COMPACTOR:sanitize_path_component("a\\b\\c"))},
        {"Complex traversal attack",
         ?_assertEqual("___________etc_cron.d_evil",
            ?COMPACTOR:sanitize_path_component("../../../etc/cron.d/evil"))},
        {"Null bytes replaced",
         ?_test(begin
            Result = ?COMPACTOR:sanitize_path_component("doc\x00id"),
            ?assertNot(lists:member(0, Result))
         end)},
        {"Doc ID with colons (valid CouchDB ID)",
         ?_assertEqual("org.couchdb:user:admin",
            ?COMPACTOR:sanitize_path_component("org.couchdb:user:admin"))},
        {"Doc ID with spaces",
         ?_assertEqual("my doc",
            ?COMPACTOR:sanitize_path_component("my doc"))},
        {"Single dot is fine (not traversal)",
         ?_assertEqual("file.txt",
            ?COMPACTOR:sanitize_path_component("file.txt"))}
    ]}.

%% ===================================================================
%% 5. UNIT TESTS — Config reading from INI
%% ===================================================================

config_from_ini_test_() ->
    {
        setup,
        fun() -> test_util:start_couch() end,
        fun(Ctx) -> test_util:stop_couch(Ctx) end,
        {"INI config parsing", [
            {"Default config (no INI set)",
             ?_test(begin
                Opts = ?COMPACTOR:get_retention_opts_from_ini(),
                ?assertMatch(#{date_fields := [<<"date">>],
                               extract_after_days := 365,
                               remove_after_days := 0}, Opts)
             end)},
            {"Custom INI config",
             ?_test(begin
                Config = [
                    {"compaction_retention", "date_fields", "date,created_at,timestamp"},
                    {"compaction_retention", "extract_after_days", "180"},
                    {"compaction_retention", "remove_after_days", "730"}
                ],
                cpse_util:with_config(Config, fun() ->
                    Opts = ?COMPACTOR:get_retention_opts_from_ini(),
                    ?assertEqual([<<"date">>, <<"created_at">>, <<"timestamp">>],
                                 maps:get(date_fields, Opts)),
                    ?assertEqual(180, maps:get(extract_after_days, Opts)),
                    ?assertEqual(730, maps:get(remove_after_days, Opts))
                end)
             end)},
            {"Extraction disabled by default",
             ?_test(begin
                ?assertEqual(false, ?COMPACTOR:is_extraction_enabled("mydb"))
             end)},
            {"Extraction enabled for all databases",
             ?_test(begin
                Config = [
                    {"compaction_retention", "extract_enabled", "true"},
                    {"compaction_retention", "extract_databases", "all"}
                ],
                cpse_util:with_config(Config, fun() ->
                    ?assertEqual(true, ?COMPACTOR:is_extraction_enabled("mydb")),
                    ?assertEqual(true, ?COMPACTOR:is_extraction_enabled("otherdb"))
                end)
             end)},
            {"Extraction enabled for specific databases only",
             ?_test(begin
                Config = [
                    {"compaction_retention", "extract_enabled", "true"},
                    {"compaction_retention", "extract_databases", "db1,db2"}
                ],
                cpse_util:with_config(Config, fun() ->
                    ?assertEqual(true, ?COMPACTOR:is_extraction_enabled("db1")),
                    ?assertEqual(true, ?COMPACTOR:is_extraction_enabled("db2")),
                    ?assertEqual(false, ?COMPACTOR:is_extraction_enabled("db3"))
                end)
             end)}
        ]}
    }.

%% ===================================================================
%% 6. INTEGRATION TESTS — Compaction regression: no config = upstream behavior
%% ===================================================================

setup() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    ok = couch_db:close(Db),
    DbName.

teardown(DbName) when is_binary(DbName) ->
    couch_server:delete(DbName, [?ADMIN_CTX]),
    ok.

regression_compaction_test_() ->
    {
        "Regression: compaction without config matches upstream behavior",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_no_config_preserves_all_docs/1,
                    fun t_no_config_preserves_doc_count/1,
                    fun t_no_config_preserves_attachments/1,
                    fun t_no_config_sizes_stable_across_compactions/1,
                    fun t_no_config_deleted_docs_stay_deleted/1
                ]
            }
        }
    }.

t_no_config_preserves_all_docs(DbName) ->
    ?_test(begin
        create_dated_docs(DbName, 800),
        {Before, BeforeIds} = get_all_doc_ids(DbName),
        compact_db(DbName),
        {After, AfterIds} = get_all_doc_ids(DbName),
        ?assertEqual(Before, After),
        ?assertEqual(BeforeIds, AfterIds)
    end).

t_no_config_preserves_doc_count(DbName) ->
    ?_test(begin
        create_many_docs(DbName, 100),
        CountBefore = get_doc_count(DbName),
        compact_db(DbName),
        CountAfter = get_doc_count(DbName),
        ?assertEqual(CountBefore, CountAfter)
    end).

t_no_config_preserves_attachments(DbName) ->
    ?_test(begin
        AttData = crypto:strong_rand_bytes(4096),
        create_doc_with_attachment(DbName, <<"att_doc">>, <<"file.bin">>, AttData),
        compact_db(DbName),
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Doc} = couch_db:open_doc(Db, <<"att_doc">>, []),
            Atts = Doc#doc.atts,
            ?assertEqual(1, length(Atts))
        end)
    end).

t_no_config_sizes_stable_across_compactions(DbName) ->
    ?_test(begin
        % Verify that compacting twice produces the same sizes.
        % This is critical for shard sync: if sizes change on every
        % compaction, smoosh will endlessly re-trigger.
        create_dated_docs(DbName, 100),
        compact_db(DbName),
        Sizes1 = get_db_sizes(DbName),
        compact_db(DbName),
        Sizes2 = get_db_sizes(DbName),
        % Active and external sizes must be identical
        ?assertEqual(maps:get(active, Sizes1), maps:get(active, Sizes2)),
        ?assertEqual(maps:get(external, Sizes1), maps:get(external, Sizes2))
    end).

t_no_config_deleted_docs_stay_deleted(DbName) ->
    ?_test(begin
        create_dated_docs(DbName, 100),
        % Delete one doc
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Doc} = couch_db:open_doc(Db, <<"old1">>, []),
            DeletedDoc = Doc#doc{deleted = true},
            {ok, _} = couch_db:update_doc(Db, DeletedDoc, [])
        end),
        CountBefore = get_doc_count(DbName),
        compact_db(DbName),
        CountAfter = get_doc_count(DbName),
        ?assertEqual(CountBefore, CountAfter),
        % Deleted doc should still be gone
        couch_util:with_db(DbName, fun(Db) ->
            ?assertEqual({not_found, deleted},
                         couch_db:open_doc(Db, <<"old1">>, []))
        end)
    end).

%% ===================================================================
%% 7. INTEGRATION TESTS — Retention: document removal
%% ===================================================================

retention_removal_test_() ->
    {
        "Retention: document removal during compaction",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_retention_removes_old_docs/1,
                    fun t_retention_preserves_recent_docs/1,
                    fun t_retention_preserves_design_docs/1,
                    fun t_retention_preserves_local_docs/1,
                    fun t_retention_preserves_deleted_tombstones/1,
                    fun t_retention_preserves_docs_without_date/1,
                    fun t_retention_multiple_date_fields/1,
                    fun t_retention_zero_days_disables_removal/1,
                    fun t_retention_removes_nothing_when_all_recent/1,
                    fun t_retention_removes_all_when_all_old/1,
                    fun t_retention_idempotent/1
                ]
            }
        }
    }.

t_retention_removes_old_docs(DbName) ->
    ?_test(begin
        create_dated_docs(DbName, 800),
        ?assertEqual(5, get_doc_count(DbName)),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        % 2 old docs removed, 2 recent + 1 no-date remain
        ?assertEqual(3, get_doc_count(DbName)),
        couch_util:with_db(DbName, fun(Db) ->
            ?assertEqual({not_found, missing},
                         couch_db:open_doc(Db, <<"old1">>, [])),
            ?assertEqual({not_found, missing},
                         couch_db:open_doc(Db, <<"old2">>, []))
        end)
    end).

t_retention_preserves_recent_docs(DbName) ->
    ?_test(begin
        create_dated_docs(DbName, 800),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        couch_util:with_db(DbName, fun(Db) ->
            ?assertMatch({ok, _}, couch_db:open_doc(Db, <<"recent1">>, [])),
            ?assertMatch({ok, _}, couch_db:open_doc(Db, <<"recent2">>, []))
        end)
    end).

t_retention_preserves_design_docs(DbName) ->
    ?_test(begin
        OldDate = format_old_date(800),
        couch_util:with_db(DbName, fun(Db) ->
            DDoc = couch_doc:from_json_obj({[
                {<<"_id">>, <<"_design/myview">>},
                {<<"date">>, OldDate},
                {<<"views">>, {[
                    {<<"v1">>, {[{<<"map">>, <<"function(d){emit(d._id)}">>}]}}
                ]}}
            ]}),
            {ok, _} = couch_db:update_doc(Db, DDoc, [])
        end),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        couch_util:with_db(DbName, fun(Db) ->
            ?assertMatch({ok, _}, couch_db:open_doc(Db, <<"_design/myview">>, []))
        end)
    end).

t_retention_preserves_local_docs(DbName) ->
    ?_test(begin
        couch_util:with_db(DbName, fun(Db) ->
            LocalDoc = couch_doc:from_json_obj({[
                {<<"_id">>, <<"_local/checkpoint">>},
                {<<"date">>, format_old_date(800)},
                {<<"seq">>, 42}
            ]}),
            {ok, _} = couch_db:update_doc(Db, LocalDoc, [])
        end),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        couch_util:with_db(DbName, fun(Db) ->
            ?assertMatch({ok, _}, couch_db:open_doc(Db, <<"_local/checkpoint">>, []))
        end)
    end).

t_retention_preserves_deleted_tombstones(DbName) ->
    ?_test(begin
        OldDate = format_old_date(800),
        couch_util:with_db(DbName, fun(Db) ->
            Doc = couch_doc:from_json_obj({[
                {<<"_id">>, <<"to_delete">>},
                {<<"date">>, OldDate}
            ]}),
            {ok, {_, Rev}} = couch_db:update_doc(Db, Doc, []),
            DeletedDoc = Doc#doc{revs = {element(1, Rev), [element(2, Rev)]},
                                 deleted = true},
            {ok, _} = couch_db:update_doc(Db, DeletedDoc, [])
        end),
        CountBefore = get_doc_count(DbName),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        CountAfter = get_doc_count(DbName),
        % Tombstone should survive — it's already deleted
        ?assertEqual(CountBefore, CountAfter)
    end).

t_retention_preserves_docs_without_date(DbName) ->
    ?_test(begin
        couch_util:with_db(DbName, fun(Db) ->
            Doc = couch_doc:from_json_obj({[
                {<<"_id">>, <<"nodatefield">>},
                {<<"val">>, 1}
            ]}),
            {ok, _} = couch_db:update_doc(Db, Doc, [])
        end),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        couch_util:with_db(DbName, fun(Db) ->
            ?assertMatch({ok, _}, couch_db:open_doc(Db, <<"nodatefield">>, []))
        end)
    end).

t_retention_multiple_date_fields(DbName) ->
    ?_test(begin
        OldDate = format_old_date(800),
        RecentDate = format_old_date(10),
        couch_util:with_db(DbName, fun(Db) ->
            Docs = [
                % Has old "created_at" but no "date" field
                couch_doc:from_json_obj({[
                    {<<"_id">>, <<"old_created">>},
                    {<<"created_at">>, OldDate}
                ]}),
                % Has old "timestamp" as third option
                couch_doc:from_json_obj({[
                    {<<"_id">>, <<"old_timestamp">>},
                    {<<"timestamp">>, OldDate}
                ]}),
                % Has recent "created_at"
                couch_doc:from_json_obj({[
                    {<<"_id">>, <<"recent_created">>},
                    {<<"created_at">>, RecentDate}
                ]})
            ],
            {ok, _} = couch_db:update_docs(Db, Docs)
        end),
        Config = [
            {"compaction_retention", "date_fields", "date,created_at,timestamp"},
            {"compaction_retention", "remove_after_days", "365"}
        ],
        cpse_util:with_config(Config, fun() ->
            compact_db(DbName)
        end),
        couch_util:with_db(DbName, fun(Db) ->
            ?assertEqual({not_found, missing},
                         couch_db:open_doc(Db, <<"old_created">>, [])),
            ?assertEqual({not_found, missing},
                         couch_db:open_doc(Db, <<"old_timestamp">>, [])),
            ?assertMatch({ok, _},
                         couch_db:open_doc(Db, <<"recent_created">>, []))
        end)
    end).

t_retention_zero_days_disables_removal(DbName) ->
    ?_test(begin
        create_dated_docs(DbName, 800),
        Config = [
            {"compaction_retention", "date_fields", "date"},
            {"compaction_retention", "remove_after_days", "0"}
        ],
        cpse_util:with_config(Config, fun() ->
            compact_db(DbName)
        end),
        % Nothing should be removed when remove_after_days = 0
        ?assertEqual(5, get_doc_count(DbName))
    end).

t_retention_removes_nothing_when_all_recent(DbName) ->
    ?_test(begin
        RecentDate = format_old_date(10),
        couch_util:with_db(DbName, fun(Db) ->
            Docs = [couch_doc:from_json_obj({[
                {<<"_id">>, iolist_to_binary(io_lib:format("doc~B", [I]))},
                {<<"date">>, RecentDate}
            ]}) || I <- lists:seq(1, 20)],
            {ok, _} = couch_db:update_docs(Db, Docs)
        end),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        ?assertEqual(20, get_doc_count(DbName))
    end).

t_retention_removes_all_when_all_old(DbName) ->
    ?_test(begin
        OldDate = format_old_date(800),
        couch_util:with_db(DbName, fun(Db) ->
            Docs = [couch_doc:from_json_obj({[
                {<<"_id">>, iolist_to_binary(io_lib:format("old~B", [I]))},
                {<<"date">>, OldDate}
            ]}) || I <- lists:seq(1, 10)],
            {ok, _} = couch_db:update_docs(Db, Docs)
        end),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        ?assertEqual(0, get_doc_count(DbName))
    end).

t_retention_idempotent(DbName) ->
    ?_test(begin
        % Running retention compaction twice should produce the same result
        create_dated_docs(DbName, 800),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        Count1 = get_doc_count(DbName),
        Sizes1 = get_db_sizes(DbName),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        Count2 = get_doc_count(DbName),
        Sizes2 = get_db_sizes(DbName),
        ?assertEqual(Count1, Count2),
        ?assertEqual(maps:get(active, Sizes1), maps:get(active, Sizes2)),
        ?assertEqual(maps:get(external, Sizes1), maps:get(external, Sizes2))
    end).

%% ===================================================================
%% 8. INTEGRATION TESTS — Size stability (sync loop prevention)
%% ===================================================================

size_stability_test_() ->
    {
        "Size stability: prevent smoosh/replication sync loops",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_sizes_stable_no_config/1,
                    fun t_sizes_stable_with_retention/1,
                    fun t_sizes_decrease_after_retention_removal/1,
                    fun t_active_size_nonzero_after_compaction/1
                ]
            }
        }
    }.

t_sizes_stable_no_config(DbName) ->
    ?_test(begin
        % Without any retention config, compacting the same database
        % twice must produce identical active/external sizes.
        % If this fails, smoosh will endlessly re-trigger compaction.
        create_many_docs(DbName, 50),
        compact_db(DbName),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2)),
        ?assertEqual(maps:get(external, S1), maps:get(external, S2))
    end).

t_sizes_stable_with_retention(DbName) ->
    ?_test(begin
        % After retention removes docs, compacting again must produce
        % identical sizes. This prevents the post-removal compaction
        % from triggering yet another compaction cycle.
        create_dated_docs(DbName, 800),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        S1 = get_db_sizes(DbName),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2)),
        ?assertEqual(maps:get(external, S1), maps:get(external, S2))
    end).

t_sizes_decrease_after_retention_removal(DbName) ->
    ?_test(begin
        % Removing old documents should reduce the active size
        create_dated_docs(DbName, 800),
        compact_db(DbName),
        SizesBefore = get_db_sizes(DbName),
        with_retention_config(365, fun() ->
            compact_db(DbName)
        end),
        SizesAfter = get_db_sizes(DbName),
        ?assert(maps:get(active, SizesAfter) < maps:get(active, SizesBefore))
    end).

t_active_size_nonzero_after_compaction(DbName) ->
    ?_test(begin
        % After compaction, active size must not be 0.
        % If it is, smoosh line 453 returns MinPriority and loops.
        create_many_docs(DbName, 50),
        compact_db(DbName),
        Sizes = get_db_sizes(DbName),
        ?assert(maps:get(active, Sizes) > 0)
    end).

%% ===================================================================
%% Helpers
%% ===================================================================

format_old_date(DaysAgo) ->
    {{CurY, CurM, CurD}, _} = calendar:universal_time(),
    TodayDays = calendar:date_to_gregorian_days({CurY, CurM, CurD}),
    {Y, M, D} = calendar:gregorian_days_to_date(TodayDays - DaysAgo),
    iolist_to_binary(
        io_lib:format("~4..0B-~2..0B-~2..0BT00:00:00Z", [Y, M, D])
    ).

create_dated_docs(DbName, OldDaysAgo) ->
    OldDate = format_old_date(OldDaysAgo),
    RecentDate = format_old_date(10),
    couch_util:with_db(DbName, fun(Db) ->
        Docs = [
            couch_doc:from_json_obj({[
                {<<"_id">>, <<"old1">>},
                {<<"date">>, OldDate},
                {<<"val">>, 1}
            ]}),
            couch_doc:from_json_obj({[
                {<<"_id">>, <<"old2">>},
                {<<"date">>, OldDate},
                {<<"val">>, 2}
            ]}),
            couch_doc:from_json_obj({[
                {<<"_id">>, <<"recent1">>},
                {<<"date">>, RecentDate},
                {<<"val">>, 3}
            ]}),
            couch_doc:from_json_obj({[
                {<<"_id">>, <<"recent2">>},
                {<<"date">>, RecentDate},
                {<<"val">>, 4}
            ]}),
            couch_doc:from_json_obj({[
                {<<"_id">>, <<"nodatefield">>},
                {<<"val">>, 5}
            ]})
        ],
        {ok, _} = couch_db:update_docs(Db, Docs)
    end).

create_many_docs(DbName, N) ->
    couch_util:with_db(DbName, fun(Db) ->
        Docs = [couch_doc:from_json_obj({[
            {<<"_id">>, iolist_to_binary(io_lib:format("doc-~4..0B", [I]))},
            {<<"date">>, format_old_date(I)},
            {<<"value">>, I},
            {<<"payload">>, base64:encode(crypto:strong_rand_bytes(100))}
        ]}) || I <- lists:seq(1, N)],
        {ok, _} = couch_db:update_docs(Db, Docs)
    end).

create_doc_with_attachment(DbName, DocId, AttName, AttData) ->
    couch_util:with_db(DbName, fun(Db) ->
        Doc = couch_doc:from_json_obj({[
            {<<"_id">>, DocId},
            {<<"_attachments">>, {[
                {AttName, {[
                    {<<"content_type">>, <<"application/octet-stream">>},
                    {<<"data">>, base64:encode(AttData)}
                ]}}
            ]}}
        ]}),
        {ok, _} = couch_db:update_doc(Db, Doc, [])
    end).

get_doc_count(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, Count} = couch_db:get_doc_count(Db),
        Count
    end).

get_all_doc_ids(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, Count} = couch_db:get_doc_count(Db),
        FoldFun = fun(FDI, Acc) ->
            {ok, [FDI#full_doc_info.id | Acc]}
        end,
        {ok, Ids} = couch_db:fold_docs(Db, FoldFun, []),
        {Count, lists:sort(Ids)}
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

with_retention_config(RemoveAfterDays, Fun) ->
    Config = [
        {"compaction_retention", "extract_enabled", "false"},
        {"compaction_retention", "date_fields", "date"},
        {"compaction_retention", "remove_after_days",
         integer_to_list(RemoveAfterDays)}
    ],
    cpse_util:with_config(Config, Fun).

compact_db(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, _} = couch_db:start_compact(Db)
    end),
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
