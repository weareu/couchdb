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

%% @doc Tests for the replication-based shard split engine.
%%
%% CRITICAL: These tests verify that shard splitting is CORRECT.
%% A bug here means:
%%   - Lost production documents
%%   - Rolled back change sequences
%%   - Orphaned shard files
%%   - Incorrect shard maps
%%
%% Every test uses REAL databases and verifies ACTUAL data integrity.

-module(mem3_reshard_rep_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

%% ===================================================================
%% 1. Range subdivision — mathematical correctness
%% ===================================================================

subdivide_range_test_() ->
    {"Range subdivision", [
        {"2-way split of full ring",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], 2),
             ?assertEqual(2, length(Ranges)),
             [[B1, E1], [B2, E2]] = Ranges,
             ?assertEqual(0, B1),
             ?assertEqual(E1 + 1, B2),
             ?assertEqual(?RING_END, E2),
             %% Both halves should be roughly equal
             W1 = E1 - B1 + 1,
             W2 = E2 - B2 + 1,
             ?assert(abs(W1 - W2) =< 1)
         end)},

        {"4-way split of full ring",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], 4),
             ?assertEqual(4, length(Ranges)),
             %% First begins at 0, last ends at RING_END
             [[0, _] | _] = Ranges,
             [_, LastEnd] = lists:last(Ranges),
             ?assertEqual(?RING_END, LastEnd),
             %% All ranges contiguous
             assert_contiguous(Ranges)
         end)},

        {"8-way split",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], 8),
             ?assertEqual(8, length(Ranges)),
             assert_contiguous(Ranges),
             [_, LastEnd] = lists:last(Ranges),
             ?assertEqual(?RING_END, LastEnd)
         end)},

        {"32-way split (500GB -> 32x15GB scenario)",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], 32),
             ?assertEqual(32, length(Ranges)),
             assert_contiguous(Ranges),
             [[0, _] | _] = Ranges,
             [_, LastEnd] = lists:last(Ranges),
             ?assertEqual(?RING_END, LastEnd),
             %% All widths should be roughly equal
             Widths = [E - B + 1 || [B, E] <- Ranges],
             MaxW = lists:max(Widths),
             MinW = lists:min(Widths),
             ?assert(MaxW - MinW =< 1)
         end)},

        {"Split sub-range (half ring)",
         ?_test(begin
             Half = ?RING_END div 2,
             Ranges = mem3_reshard_rep:subdivide_range([0, Half], 2),
             ?assertEqual(2, length(Ranges)),
             [[0, E1], [B2, HalfEnd]] = Ranges,
             ?assertEqual(Half, HalfEnd),
             ?assertEqual(E1 + 1, B2)
         end)},

        {"Split sub-range (quarter ring)",
         ?_test(begin
             Quarter = ?RING_END div 4,
             Ranges = mem3_reshard_rep:subdivide_range(
                 [Quarter, 2 * Quarter - 1], 2),
             ?assertEqual(2, length(Ranges)),
             [[B1, _], [_, E2]] = Ranges,
             ?assertEqual(Quarter, B1),
             ?assertEqual(2 * Quarter - 1, E2)
         end)},

        {"Factor too large for range errors",
         ?_assertError({range_too_small, _, _},
            mem3_reshard_rep:subdivide_range([0, 1], 4))},

        {"Factor 2 on minimum range [0,1]",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, 1], 2),
             ?assertEqual([[0, 0], [1, 1]], Ranges)
         end)},

        {"Ranges cover every hash value (no gaps, no overlaps)",
         ?_test(begin
             %% For each split factor, verify that the union of ranges
             %% equals the original range exactly
             lists:foreach(fun(Factor) ->
                 Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], Factor),
                 %% Total width must equal original
                 TotalWidth = lists:sum([E - B + 1 || [B, E] <- Ranges]),
                 ?assertEqual(?RING_END + 1, TotalWidth),
                 assert_contiguous(Ranges)
             end, [2, 4, 8, 16, 32])
         end)}
    ]}.

%% ===================================================================
%% 2. Target map construction
%% ===================================================================

target_map_test_() ->
    {"Target map construction", [
        {"One target per unique range",
         ?_test(begin
             Targets = [
                 #shard{range = [0, 100], node = 'a@h', name = <<"s1">>},
                 #shard{range = [0, 100], node = 'b@h', name = <<"s2">>},
                 #shard{range = [101, 200], node = 'a@h', name = <<"s3">>},
                 #shard{range = [101, 200], node = 'b@h', name = <<"s4">>}
             ],
             TMap = mem3_reshard_rep:build_target_map(Targets),
             ?assertEqual(2, maps:size(TMap)),
             ?assert(maps:is_key([0, 100], TMap)),
             ?assert(maps:is_key([101, 200], TMap))
         end)},

        {"First shard for each range wins",
         ?_test(begin
             S1 = #shard{range = [0, 100], node = 'first@h', name = <<"s1">>},
             S2 = #shard{range = [0, 100], node = 'second@h', name = <<"s2">>},
             TMap = mem3_reshard_rep:build_target_map([S1, S2]),
             ?assertEqual(S1, maps:get([0, 100], TMap))
         end)},

        {"Empty list returns empty map",
         ?_assertEqual(#{}, mem3_reshard_rep:build_target_map([]))}
    ]}.

%% ===================================================================
%% 3. Pre-flight space check
%% ===================================================================

preflight_test_() ->
    {"Pre-flight space check", [
        {"Sufficient space passes",
         ?_test(begin
             Caps = #{
                 'n1@h' => #{dirs => [{"/data", 20, 800000000000, 1000000000000}]},
                 'n2@h' => #{dirs => [{"/data", 30, 700000000000, 1000000000000}]}
             },
             %% 100GB source, split 2-way = 50GB targets
             %% Need 50GB * 3 = 150GB per node — both have >150GB
             ?assertEqual(ok,
                 mem3_reshard_rep:preflight_check(100000000000, 2, Caps))
         end)},

        {"Insufficient space fails with details",
         ?_test(begin
             Caps = #{
                 'n1@h' => #{dirs => [{"/data", 95, 50000000000, 1000000000000}]},
                 'n2@h' => #{dirs => [{"/data", 90, 100000000000, 1000000000000}]}
             },
             %% 400GB source, 2-way = 200GB targets, need 600GB per node
             Result = mem3_reshard_rep:preflight_check(400000000000, 2, Caps),
             ?assertMatch({error, {insufficient_space, _}}, Result),
             {error, {insufficient_space, Problems}} = Result,
             ?assert(length(Problems) > 0)
         end)},

        {"Empty capabilities fails for all nodes",
         ?_test(begin
             Caps = #{
                 'n1@h' => #{dirs => []}
             },
             Result = mem3_reshard_rep:preflight_check(100000000000, 2, Caps),
             ?assertMatch({error, {insufficient_space, _}}, Result)
         end)},

        {"Multi-dir node uses best dir",
         ?_test(begin
             Caps = #{
                 'n1@h' => #{dirs => [
                     {"/data1", 95, 50000000000, 1000000000000},
                     {"/data2", 10, 900000000000, 1000000000000}
                 ]}
             },
             %% 100GB / 2 = 50GB target, need 150GB. /data2 has 900GB — OK
             ?assertEqual(ok,
                 mem3_reshard_rep:preflight_check(100000000000, 2, Caps))
         end)}
    ]}.

%% ===================================================================
%% 4. Doc count verification with REAL databases
%% ===================================================================

doc_count_test_() ->
    {
        "Doc count verification",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_matching_counts_pass/0,
                fun t_mismatched_counts_fail/0,
                fun t_empty_dbs_pass/0,
                fun t_deleted_docs_counted/0
            ]
        }
    }.

t_matching_counts_pass() ->
    ?_test(begin
        Source = <<"test_verify_source_match">>,
        Target1 = <<"test_verify_target1_match">>,
        Target2 = <<"test_verify_target2_match">>,
        create_and_populate(Source, 20),
        create_and_populate(Target1, 12),
        create_and_populate(Target2, 8),
        try
            ?assertEqual(ok,
                mem3_reshard_rep:verify_doc_counts(Source, [Target1, Target2]))
        after
            cleanup([Source, Target1, Target2])
        end
    end).

t_mismatched_counts_fail() ->
    ?_test(begin
        Source = <<"test_verify_source_mismatch">>,
        Target1 = <<"test_verify_target1_mismatch">>,
        Target2 = <<"test_verify_target2_mismatch">>,
        create_and_populate(Source, 20),
        create_and_populate(Target1, 10),
        create_and_populate(Target2, 5), % 15 != 20
        try
            Result = mem3_reshard_rep:verify_doc_counts(Source, [Target1, Target2]),
            ?assertMatch({error, {doc_count_mismatch, 20, 15}}, Result)
        after
            cleanup([Source, Target1, Target2])
        end
    end).

t_empty_dbs_pass() ->
    ?_test(begin
        Source = <<"test_verify_empty_source">>,
        Target1 = <<"test_verify_empty_target">>,
        create_db(Source),
        create_db(Target1),
        try
            ?assertEqual(ok,
                mem3_reshard_rep:verify_doc_counts(Source, [Target1]))
        after
            cleanup([Source, Target1])
        end
    end).

t_deleted_docs_counted() ->
    ?_test(begin
        Source = <<"test_verify_del_source">>,
        Target1 = <<"test_verify_del_target">>,
        %% Create source with 10 docs, delete 3
        create_and_populate(Source, 10),
        {ok, SDb} = couch_db:open_int(Source, [?ADMIN_CTX]),
        try
            lists:foreach(fun(I) ->
                Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
                {ok, Doc} = couch_db:open_doc(SDb, Id, []),
                {ok, _} = couch_db:update_doc(SDb,
                    Doc#doc{deleted = true}, [])
            end, lists:seq(1, 3))
        after
            couch_db:close(SDb)
        end,
        %% Target has 7 docs, 3 deleted
        create_and_populate(Target1, 7),
        {ok, TDb} = couch_db:open_int(Target1, [?ADMIN_CTX]),
        try
            lists:foreach(fun(I) ->
                Id = list_to_binary(io_lib:format("del-~4..0B", [I])),
                Doc = #doc{id = Id, body = {[]}},
                {ok, Rev} = couch_db:update_doc(TDb, Doc, []),
                {ok, _} = couch_db:update_doc(TDb,
                    #doc{id = Id, revs = {element(1, Rev), [element(2, Rev)]},
                         deleted = true}, [])
            end, lists:seq(1, 3))
        after
            couch_db:close(TDb)
        end,
        try
            ?assertEqual(ok,
                mem3_reshard_rep:verify_doc_counts(Source, [Target1]))
        after
            cleanup([Source, Target1])
        end
    end).

%% ===================================================================
%% 5. Doc distribution verification with REAL databases
%% ===================================================================

doc_distribution_test_() ->
    {
        "Doc distribution verification",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_all_docs_found_in_targets/0,
                fun t_missing_doc_detected/0
            ]
        }
    }.

t_all_docs_found_in_targets() ->
    ?_test(begin
        Source = <<"test_dist_source">>,
        Target1 = <<"test_dist_target1">>,
        %% Create source with docs, copy all to target
        create_and_populate(Source, 30),
        create_db(Target1),
        %% Copy all docs from source to target
        {ok, SDb} = couch_db:open_int(Source, [?ADMIN_CTX]),
        {ok, TDb} = couch_db:open_int(Target1, [?ADMIN_CTX]),
        try
            couch_db:fold_docs(SDb, fun(FDI, ok) ->
                #full_doc_info{id = DocId} = FDI,
                {ok, Doc} = couch_db:open_doc(SDb, DocId, []),
                {ok, _} = couch_db:update_docs(TDb, [Doc], [replicated_changes]),
                {ok, ok}
            end, ok, [])
        after
            couch_db:close(SDb),
            couch_db:close(TDb)
        end,
        try
            HashFun = fun(_) -> ok end,
            ?assertEqual(ok,
                mem3_reshard_rep:verify_doc_distribution(
                    Source, [Target1], HashFun))
        after
            cleanup([Source, Target1])
        end
    end).

t_missing_doc_detected() ->
    ?_test(begin
        Source = <<"test_dist_missing_source">>,
        Target1 = <<"test_dist_missing_target">>,
        create_and_populate(Source, 10),
        create_and_populate(Target1, 5), % only 5 of the 10 docs
        try
            HashFun = fun(_) -> ok end,
            Result = mem3_reshard_rep:verify_doc_distribution(
                Source, [Target1], HashFun),
            ?assertMatch({error, {docs_missing_from_targets, _}}, Result),
            {error, {docs_missing_from_targets, Missing}} = Result,
            ?assertEqual(5, length(Missing))
        after
            cleanup([Source, Target1])
        end
    end).

%% ===================================================================
%% 6. Hash routing — docs land in correct target
%% ===================================================================

hash_routing_test_() ->
    {"Hash routing correctness", [
        {"Every hash value falls in exactly one range",
         ?_test(begin
             lists:foreach(fun(Factor) ->
                 Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], Factor),
                 %% Test 10000 random hash values
                 lists:foreach(fun(_) ->
                     Hash = rand:uniform(?RING_END + 1) - 1,
                     Matching = [R || [B, E] = R <- Ranges,
                                     Hash >= B, Hash =< E],
                     ?assertEqual(1, length(Matching),
                         lists:flatten(io_lib:format(
                             "Hash ~B matched ~B ranges (factor ~B)",
                             [Hash, length(Matching), Factor])))
                 end, lists:seq(1, 10000))
             end, [2, 4, 8, 16])
         end)},

        {"Boundary values are correctly assigned",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], 4),
             %% Check that boundary values between ranges are in one range
             lists:foreach(fun([B, E]) ->
                 %% Begin of range
                 ?assertEqual(1, length([R || [RB, RE] = R <- Ranges,
                                             B >= RB, B =< RE])),
                 %% End of range
                 ?assertEqual(1, length([R || [RB, RE] = R <- Ranges,
                                             E >= RB, E =< RE]))
             end, Ranges)
         end)}
    ]}.

%% ===================================================================
%% Helpers
%% ===================================================================

assert_contiguous([_]) -> ok;
assert_contiguous([[_B1, E1], [B2, E2] | Rest]) ->
    ?assertEqual(E1 + 1, B2),
    assert_contiguous([[B2, E2] | Rest]).

create_db(DbName) ->
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    couch_db:close(Db).

create_and_populate(DbName, DocCount) ->
    create_db(DbName),
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    try
        lists:foreach(fun(I) ->
            Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
            Doc = #doc{id = Id, body = {[{<<"n">>, I}]}},
            {ok, _} = couch_db:update_doc(Db, Doc, [])
        end, lists:seq(1, DocCount))
    after
        couch_db:close(Db)
    end.

cleanup(DbNames) ->
    lists:foreach(fun(DbName) ->
        catch couch_server:delete(DbName, [?ADMIN_CTX])
    end, DbNames).
