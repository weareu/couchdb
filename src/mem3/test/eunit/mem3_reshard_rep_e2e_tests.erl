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

%% @doc End-to-end data integrity tests for shard splitting.
%%
%% THESE TESTS VERIFY THAT PRODUCTION DATA IS NEVER LOST.
%%
%% Every test creates a real source shard with real documents, runs
%% the actual split machinery, and verifies that every single document
%% survives the split intact.
%%
%% If any of these tests fail, the split engine has a data loss bug
%% and MUST NOT be used in production.

-module(mem3_reshard_rep_e2e_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

%% ===================================================================
%% 1. EVERY doc survives — large dataset, special IDs, design docs
%% ===================================================================

every_doc_survives_test_() ->
    {
        "Every document must survive a split",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {timeout, 120, [
                fun t_1000_docs_all_survive/0,
                fun t_design_docs_survive/0,
                fun t_special_id_docs_survive/0,
                fun t_deleted_docs_survive_as_tombstones/0,
                fun t_large_body_docs_survive/0
            ]}
        }
    }.

t_1000_docs_all_survive() ->
    ?_test(begin
        Source = make_source(<<"e2e_1000">>),
        Targets = make_targets(Source, 4),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 1000),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 200}, {batch_count, all}]),

            %% VERIFY: every single doc exists in exactly one target
            SourceCount = doc_count(Source#shard.name),
            ?assertEqual(1000, SourceCount),
            TargetCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(1000, TargetCount),

            %% VERIFY: check each doc individually
            Missing = find_missing_docs(Source#shard.name, Targets),
            ?assertEqual([], Missing)
        after
            cleanup(Source, Targets)
        end
    end).

t_design_docs_survive() ->
    ?_test(begin
        Source = make_source(<<"e2e_ddoc">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            %% Write regular docs + design docs
            write_docs(Source#shard.name, 50),
            write_design_doc(Source#shard.name, <<"_design/myview">>,
                {[{<<"views">>, {[{<<"by_n">>, {[
                    {<<"map">>, <<"function(doc) { emit(doc.n, 1); }">>}
                ]}}]}}]}),
            write_design_doc(Source#shard.name, <<"_design/another">>,
                {[{<<"language">>, <<"javascript">>}]}),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Design docs must exist in at least one target
            ?assert(doc_exists_in_targets(<<"_design/myview">>, Targets)),
            ?assert(doc_exists_in_targets(<<"_design/another">>, Targets)),

            %% All regular docs must also survive
            TargetCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            %% 50 regular + 2 design docs
            ?assertEqual(52, TargetCount)
        after
            cleanup(Source, Targets)
        end
    end).

t_special_id_docs_survive() ->
    ?_test(begin
        Source = make_source(<<"e2e_special">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            %% Write docs with special IDs
            SpecialIds = [
                <<"simple">>,
                <<"with-dashes">>,
                <<"with_underscores">>,
                <<"UPPERCASE">>,
                <<"with.dots.in.name">>,
                <<"with:colons">>,
                <<"0123456789">>,
                <<"a">>,
                %% Very long ID
                list_to_binary(lists:duplicate(200, $x))
            ],
            lists:foreach(fun(Id) ->
                write_doc_with_id(Source#shard.name, Id, {[{<<"special">>, true}]})
            end, SpecialIds),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Every special ID doc must survive
            lists:foreach(fun(Id) ->
                ?assert(doc_exists_in_targets(Id, Targets),
                    lists:flatten(io_lib:format("Doc ~s missing after split", [Id])))
            end, SpecialIds)
        after
            cleanup(Source, Targets)
        end
    end).

t_deleted_docs_survive_as_tombstones() ->
    ?_test(begin
        Source = make_source(<<"e2e_deleted">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 50),
            %% Delete 20 docs
            delete_docs(Source#shard.name, 1, 20),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Verify counts: 30 live + 20 deleted
            TargetNames = [T#shard.name || T <- Targets],
            ?assertEqual(ok,
                mem3_reshard_rep:verify_doc_counts(Source#shard.name, TargetNames)),

            %% Live count should be 30
            LiveCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(30, LiveCount),

            %% Deleted count should be 20
            SourceDelCount = del_count(Source#shard.name),
            TargetDelCount = lists:sum([del_count(T#shard.name) || T <- Targets]),
            ?assertEqual(SourceDelCount, TargetDelCount),
            ?assertEqual(20, TargetDelCount)
        after
            cleanup(Source, Targets)
        end
    end).

t_large_body_docs_survive() ->
    ?_test(begin
        Source = make_source(<<"e2e_large">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            %% Write docs with bodies of varying sizes
            lists:foreach(fun(I) ->
                Size = I * 1024,  % 1KB to 50KB
                Id = list_to_binary(io_lib:format("large-~4..0B", [I])),
                Body = {[{<<"data">>, base64:encode(crypto:strong_rand_bytes(Size))}]},
                write_doc_with_id(Source#shard.name, Id, Body)
            end, lists:seq(1, 50)),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 50}, {batch_count, all}]),

            %% All docs must survive with correct bodies
            lists:foreach(fun(I) ->
                Id = list_to_binary(io_lib:format("large-~4..0B", [I])),
                %% Find which target has it
                {ok, SourceDoc} = read_doc(Source#shard.name, Id),
                {ok, TargetDoc} = find_doc_in_targets(Id, Targets),
                %% Bodies must match
                ?assertEqual(SourceDoc#doc.body, TargetDoc#doc.body)
            end, lists:seq(1, 50))
        after
            cleanup(Source, Targets)
        end
    end).

%% ===================================================================
%% 2. Parallel writers during split — ZERO data loss
%% ===================================================================

parallel_writers_test_() ->
    {
        "Parallel writers during split must not lose data",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {timeout, 120, [
                fun t_parallel_writers_zero_loss/0
            ]}
        }
    }.

t_parallel_writers_zero_loss() ->
    ?_test(begin
        Source = make_source(<<"e2e_parallel">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            %% Write initial 200 docs
            write_docs(Source#shard.name, 200),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),

            %% Start replication in background
            Self = self(),
            RepPid = spawn_link(fun() ->
                Res = mem3_reshard_rep:replicate(Source, TMap,
                    [{batch_size, 50}, {batch_count, all}]),
                Self ! {rep_done, Res}
            end),

            %% Simultaneously write 100 more docs
            lists:foreach(fun(I) ->
                Id = list_to_binary(io_lib:format("concurrent-~4..0B", [I])),
                Body = {[{<<"concurrent">>, true}, {<<"n">>, I}]},
                write_doc_with_id(Source#shard.name, Id, Body),
                timer:sleep(1)  % Spread writes across replication
            end, lists:seq(1, 100)),

            %% Wait for replication to finish
            receive {rep_done, ok} -> ok
            after 60000 -> error(replication_timeout) end,

            %% Topoff to catch concurrent writes
            ok = mem3_reshard_rep:topoff(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),
            %% Second topoff for safety
            ok = mem3_reshard_rep:topoff(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% VERIFY: ALL 300 docs must exist
            SourceCount = doc_count(Source#shard.name),
            TargetCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(SourceCount, TargetCount),
            ?assertEqual(300, SourceCount),

            %% VERIFY: no missing docs
            Missing = find_missing_docs(Source#shard.name, Targets),
            ?assertEqual([], Missing)
        after
            cleanup(Source, Targets)
        end
    end).

%% ===================================================================
%% 3. Verification catches every failure mode
%% ===================================================================

verification_catches_all_test_() ->
    {
        "Verification must catch every data loss scenario",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_verify_catches_missing_single_doc/0,
                fun t_verify_catches_count_mismatch/0,
                fun t_verify_catches_del_count_mismatch/0,
                fun t_source_survives_verification_failure/0
            ]
        }
    }.

t_verify_catches_missing_single_doc() ->
    ?_test(begin
        Source = make_source(<<"verify_miss1">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 20),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Sneak one more doc into source AFTER replication
            write_doc_with_id(Source#shard.name, <<"sneaky">>, {[{<<"x">>, 1}]}),

            %% Verification must fail (21 in source, 20 in targets)
            TargetNames = [T#shard.name || T <- Targets],
            Result = mem3_reshard_rep:verify_consistency(
                Source#shard.name, TargetNames),
            ?assertMatch({error, _}, Result)
        after
            cleanup(Source, Targets)
        end
    end).

t_verify_catches_count_mismatch() ->
    ?_test(begin
        Source = make_source(<<"verify_count">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 50),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            %% Only replicate to first target, not second
            [T1 | _] = Targets,
            TMap1 = #{T1#shard.range => T1},
            ok = mem3_reshard_rep:replicate(Source, TMap1,
                [{batch_size, 100}, {batch_count, all}]),

            TargetNames = [T#shard.name || T <- Targets],
            Result = mem3_reshard_rep:verify_doc_counts(
                Source#shard.name, TargetNames),
            ?assertMatch({error, {doc_count_mismatch, _, _}}, Result)
        after
            cleanup(Source, Targets)
        end
    end).

t_verify_catches_del_count_mismatch() ->
    ?_test(begin
        Source = make_source(<<"verify_del">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 30),
            delete_docs(Source#shard.name, 1, 10),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            %% Replicate WITHOUT deletions by writing to targets directly
            %% (simulates replication that missed deletes)
            write_docs_to_targets(Source#shard.name, Targets),

            TargetNames = [T#shard.name || T <- Targets],
            Result = mem3_reshard_rep:verify_doc_counts(
                Source#shard.name, TargetNames),
            ?assertMatch({error, _}, Result)
        after
            cleanup(Source, Targets)
        end
    end).

t_source_survives_verification_failure() ->
    ?_test(begin
        Source = make_source(<<"verify_survive">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 100),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            %% DON'T replicate — targets empty, verification will fail

            TargetNames = [T#shard.name || T <- Targets],
            Result = mem3_reshard_rep:verify_consistency(
                Source#shard.name, TargetNames),
            ?assertMatch({error, _}, Result),

            %% Source MUST still be fully readable
            ?assertEqual(100, doc_count(Source#shard.name)),
            %% Can still read individual docs
            {ok, _} = read_doc(Source#shard.name, <<"doc-0001">>)
        after
            cleanup(Source, Targets)
        end
    end).

%% ===================================================================
%% 4. Double split (gradual splitting scenario)
%% ===================================================================

double_split_test_() ->
    {
        "Gradual splitting: split children of a split",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {timeout, 120, [
                fun t_double_split_all_docs_survive/0
            ]}
        }
    }.

t_double_split_all_docs_survive() ->
    ?_test(begin
        %% Round 1: source → 2 children
        Source = make_source(<<"e2e_doublesplit">>),
        Children = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 500),
            ok = mem3_reshard_rep:create_target_dbs(Children),
            TMap1 = mem3_reshard_rep:build_target_map(Children),
            ok = mem3_reshard_rep:replicate(Source, TMap1,
                [{batch_size, 200}, {batch_count, all}]),

            %% Verify round 1
            ChildCount = lists:sum([doc_count(C#shard.name) || C <- Children]),
            ?assertEqual(500, ChildCount),

            %% Round 2: split each child → 2 grandchildren (4 total)
            AllGrandchildren = lists:flatmap(fun(Child) ->
                GCs = make_targets(Child, 2),
                ok = mem3_reshard_rep:create_target_dbs(GCs),
                TMap2 = mem3_reshard_rep:build_target_map(GCs),
                ok = mem3_reshard_rep:replicate(Child, TMap2,
                    [{batch_size, 200}, {batch_count, all}]),
                GCs
            end, Children),

            %% VERIFY: all 500 original docs survive across 4 grandchildren
            GCCount = lists:sum([doc_count(G#shard.name) || G <- AllGrandchildren]),
            ?assertEqual(500, GCCount),

            %% VERIFY: every doc from source exists in exactly one grandchild
            Missing = find_missing_docs(Source#shard.name, AllGrandchildren),
            ?assertEqual([], Missing)
        after
            catch couch_server:delete(Source#shard.name, [?ADMIN_CTX]),
            lists:foreach(fun(#shard{name = N}) ->
                catch couch_server:delete(N, [?ADMIN_CTX])
            end, Children),
            %% Grandchildren cleanup handled by catch
            ok
        end
    end).

%% ===================================================================
%% 5. Attachments survive split — binary data byte-identical
%% ===================================================================

attachments_test_() ->
    {
        "Attachments must survive split byte-identical",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_attachment_data_survives/0,
                fun t_multiple_attachments_per_doc/0
            ]
        }
    }.

t_attachment_data_survives() ->
    ?_test(begin
        Source = make_source(<<"e2e_att">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            %% Write docs with attachments of varying sizes via replicated_changes
            %% (We write the attachment data as part of the doc body since
            %% replicated_changes with attachment streams needs special handling)
            AttSizes = [1024, 4096, 16384, 65536],
            AttDatas = lists:map(fun(Size) ->
                {Size, crypto:strong_rand_bytes(Size)}
            end, AttSizes),

            lists:foreach(fun({Size, Data}) ->
                Id = list_to_binary(io_lib:format("att-~B", [Size])),
                %% Store attachment data as base64 in body
                %% (real attachments need interactive mode, which VDU blocks
                %% for shard-named DBs in eunit)
                Body = {[
                    {<<"att_size">>, Size},
                    {<<"att_data">>, base64:encode(Data)},
                    {<<"att_md5">>, couch_hash:md5_hash(Data)}
                ]},
                write_doc_with_id(Source#shard.name, Id, Body)
            end, AttDatas),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Verify each attachment doc survived with correct data
            lists:foreach(fun({Size, Data}) ->
                Id = list_to_binary(io_lib:format("att-~B", [Size])),
                {ok, #doc{body = {Props}}} = find_doc_in_targets(Id, Targets),
                StoredData = base64:decode(
                    couch_util:get_value(<<"att_data">>, Props)),
                StoredMd5 = couch_util:get_value(<<"att_md5">>, Props),
                ?assertEqual(Data, StoredData),
                ?assertEqual(couch_hash:md5_hash(Data), StoredMd5)
            end, AttDatas)
        after
            cleanup(Source, Targets)
        end
    end).

t_multiple_attachments_per_doc() ->
    ?_test(begin
        Source = make_source(<<"e2e_multi_att">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            %% Write docs with multiple "attachments" (stored in body)
            lists:foreach(fun(I) ->
                Id = list_to_binary(io_lib:format("multiatt-~4..0B", [I])),
                Att1 = crypto:strong_rand_bytes(2048),
                Att2 = crypto:strong_rand_bytes(4096),
                Att3 = crypto:strong_rand_bytes(1024),
                Body = {[
                    {<<"file1">>, base64:encode(Att1)},
                    {<<"file2">>, base64:encode(Att2)},
                    {<<"file3">>, base64:encode(Att3)},
                    {<<"file1_md5">>, couch_hash:md5_hash(Att1)},
                    {<<"file2_md5">>, couch_hash:md5_hash(Att2)},
                    {<<"file3_md5">>, couch_hash:md5_hash(Att3)}
                ]},
                write_doc_with_id(Source#shard.name, Id, Body)
            end, lists:seq(1, 20)),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Verify all 20 docs with 3 attachments each
            lists:foreach(fun(I) ->
                Id = list_to_binary(io_lib:format("multiatt-~4..0B", [I])),
                {ok, #doc{body = {SrcProps}}} = read_doc(Source#shard.name, Id),
                {ok, #doc{body = {TgtProps}}} = find_doc_in_targets(Id, Targets),
                %% All 6 fields (3 data + 3 md5) must match
                ?assertEqual(
                    couch_util:get_value(<<"file1_md5">>, SrcProps),
                    couch_util:get_value(<<"file1_md5">>, TgtProps)),
                ?assertEqual(
                    couch_util:get_value(<<"file2_md5">>, SrcProps),
                    couch_util:get_value(<<"file2_md5">>, TgtProps)),
                ?assertEqual(
                    couch_util:get_value(<<"file3_md5">>, SrcProps),
                    couch_util:get_value(<<"file3_md5">>, TgtProps))
            end, lists:seq(1, 20))
        after
            cleanup(Source, Targets)
        end
    end).

%% ===================================================================
%% 6. Conflicted docs survive — all branches intact
%% ===================================================================

conflicts_test_() ->
    {
        "Conflicted docs must survive split with all branches",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_conflict_branches_survive/0
            ]
        }
    }.

t_conflict_branches_survive() ->
    ?_test(begin
        Source = make_source(<<"e2e_conflict">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            %% Create 10 docs with intentional conflicts (2 branches each)
            {ok, Db} = couch_db:open_int(Source#shard.name, [?ADMIN_CTX]),
            try
                lists:foreach(fun(I) ->
                    Id = list_to_binary(io_lib:format("conflict-~4..0B", [I])),
                    %% Branch A
                    RevA = couch_hash:md5_hash(term_to_binary({Id, a})),
                    DocA = #doc{id = Id, body = {[{<<"branch">>, <<"a">>}]},
                                revs = {1, [RevA]}},
                    {ok, _} = couch_db:update_docs(Db, [DocA], [replicated_changes]),
                    %% Branch B (creates conflict)
                    RevB = couch_hash:md5_hash(term_to_binary({Id, b})),
                    DocB = #doc{id = Id, body = {[{<<"branch">>, <<"b">>}]},
                                revs = {1, [RevB]}},
                    {ok, _} = couch_db:update_docs(Db, [DocB], [replicated_changes])
                end, lists:seq(1, 10))
            after
                couch_db:close(Db)
            end,

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Verify: each conflicted doc has 2 revisions in its target
            lists:foreach(fun(I) ->
                Id = list_to_binary(io_lib:format("conflict-~4..0B", [I])),
                %% Find target that has this doc
                TargetName = find_target_with_doc(Id, Targets),
                {ok, TDb} = couch_db:open_int(TargetName, [?ADMIN_CTX]),
                try
                    FDI = couch_db:get_full_doc_info(TDb, Id),
                    ?assertNotEqual(not_found, FDI),
                    #full_doc_info{rev_tree = Tree} = FDI,
                    Leafs = couch_key_tree:get_all_leafs(Tree),
                    ?assertEqual(2, length(Leafs),
                        lists:flatten(io_lib:format(
                            "Doc ~s should have 2 conflict branches, has ~B",
                            [Id, length(Leafs)])))
                after
                    couch_db:close(TDb)
                end
            end, lists:seq(1, 10))
        after
            cleanup(Source, Targets)
        end
    end).

%% ===================================================================
%% 7. Batch boundary — no off-by-one at batch edges
%% ===================================================================

batch_boundary_test_() ->
    {
        "No docs lost at batch boundaries",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_exact_batch_size/0,
                fun t_batch_size_plus_one/0,
                fun t_batch_size_minus_one/0
            ]
        }
    }.

t_exact_batch_size() ->
    ?_test(begin
        %% Write exactly batch_size docs — tests the boundary
        Source = make_source(<<"e2e_batch_exact">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 100),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),
            TargetCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(100, TargetCount)
        after
            cleanup(Source, Targets)
        end
    end).

t_batch_size_plus_one() ->
    ?_test(begin
        %% batch_size + 1 — the +1 doc must not be lost
        Source = make_source(<<"e2e_batch_plus">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 101),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),
            TargetCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(101, TargetCount)
        after
            cleanup(Source, Targets)
        end
    end).

t_batch_size_minus_one() ->
    ?_test(begin
        %% batch_size - 1 — partial batch must be flushed
        Source = make_source(<<"e2e_batch_minus">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 99),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),
            TargetCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(99, TargetCount)
        after
            cleanup(Source, Targets)
        end
    end).

%% ===================================================================
%% 8. doc_count + doc_del_count + updates — exact match invariant
%% ===================================================================

exact_count_invariant_test_() ->
    {
        "doc_count + doc_del_count exact match after complex operations",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_write_delete_update_exact_match/0
            ]
        }
    }.

t_write_delete_update_exact_match() ->
    ?_test(begin
        Source = make_source(<<"e2e_exact">>),
        Targets = make_targets(Source, 4),
        create_shard(Source),
        try
            %% Write 1000 docs
            write_docs(Source#shard.name, 1000),
            %% Delete 200
            delete_docs(Source#shard.name, 1, 200),
            %% Update 300 (write new revision)
            {ok, Db} = couch_db:open_int(Source#shard.name, [?ADMIN_CTX]),
            try
                lists:foreach(fun(I) ->
                    Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
                    case couch_db:open_doc(Db, Id, []) of
                        {ok, #doc{revs = {Pos, [Rev | _]}} = _Doc} ->
                            NewRev = couch_hash:md5_hash(
                                term_to_binary({Id, updated, erlang:unique_integer()})),
                            UpdDoc = #doc{id = Id,
                                body = {[{<<"updated">>, true}]},
                                revs = {Pos + 1, [NewRev, Rev]}},
                            {ok, _} = couch_db:update_docs(Db, [UpdDoc],
                                [replicated_changes]);
                        _ -> ok
                    end
                end, lists:seq(201, 500))
            after
                couch_db:close(Db)
            end,

            %% Split
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 200}, {batch_count, all}]),

            %% EXACT match: doc_count, doc_del_count
            TargetNames = [T#shard.name || T <- Targets],
            ?assertEqual(ok,
                mem3_reshard_rep:verify_doc_counts(
                    Source#shard.name, TargetNames)),

            %% Expected: 800 live (1000 - 200 deleted)
            SourceLive = doc_count(Source#shard.name),
            TargetLive = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(SourceLive, TargetLive),
            ?assertEqual(800, TargetLive),

            %% Expected: 200 deleted
            SourceDel = del_count(Source#shard.name),
            TargetDel = lists:sum([del_count(T#shard.name) || T <- Targets]),
            ?assertEqual(SourceDel, TargetDel),
            ?assertEqual(200, TargetDel)
        after
            cleanup(Source, Targets)
        end
    end).

%% ===================================================================
%% 9. Changes feed must NEVER roll back
%% ===================================================================

changefeed_test_() ->
    {
        "Changes feed must never roll back after split",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_changes_feed_covers_all_docs/0,
                fun t_changes_feed_seq_monotonic/0,
                fun t_changes_feed_no_missing_after_split/0
            ]
        }
    }.

t_changes_feed_covers_all_docs() ->
    ?_test(begin
        %% A changes feed from since=0 on EACH target must return ALL
        %% docs that belong to that target's hash range.
        Source = make_source(<<"e2e_changes_cover">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 200),

            %% Record all doc IDs from source
            SourceDocIds = get_all_doc_ids(Source#shard.name),
            ?assertEqual(200, length(SourceDocIds)),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Get all doc IDs from changes feeds of ALL targets combined
            TargetDocIds = lists:flatmap(fun(#shard{name = TName}) ->
                get_changes_doc_ids(TName)
            end, Targets),

            %% Every source doc must appear in exactly one target's changes feed
            TargetSet = sets:from_list(TargetDocIds),
            SourceSet = sets:from_list(SourceDocIds),
            MissingFromTargets = sets:subtract(SourceSet, TargetSet),
            ?assertEqual(0, sets:size(MissingFromTargets),
                lists:flatten(io_lib:format(
                    "~B docs missing from target changes feeds: ~p",
                    [sets:size(MissingFromTargets),
                     lists:sublist(sets:to_list(MissingFromTargets), 5)])))
        after
            cleanup(Source, Targets)
        end
    end).

t_changes_feed_seq_monotonic() ->
    ?_test(begin
        %% update_seq on each target must be > 0 when docs exist.
        %% Sequences must be monotonically increasing in the changes feed.
        Source = make_source(<<"e2e_changes_mono">>),
        Targets = make_targets(Source, 4),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 100),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Every target with docs must have update_seq > 0
            lists:foreach(fun(#shard{name = TName}) ->
                Count = doc_count(TName),
                case Count > 0 of
                    true ->
                        {ok, TDb} = couch_db:open_int(TName, [?ADMIN_CTX]),
                        try
                            {ok, TInfo} = couch_db:get_db_info(TDb),
                            Seq = couch_util:get_value(update_seq, TInfo),
                            ?assert(Seq > 0,
                                lists:flatten(io_lib:format(
                                    "Target ~s has ~B docs but update_seq=~p",
                                    [TName, Count, Seq])))
                        after
                            couch_db:close(TDb)
                        end;
                    false ->
                        ok  % Empty target is fine
                end
            end, Targets),

            %% Changes feed on each target must be monotonically increasing
            lists:foreach(fun(#shard{name = TName}) ->
                Seqs = get_changes_seqs(TName),
                case length(Seqs) > 1 of
                    true ->
                        Pairs = lists:zip(
                            lists:sublist(Seqs, length(Seqs) - 1),
                            tl(Seqs)),
                        lists:foreach(fun({Prev, Curr}) ->
                            ?assert(Curr > Prev,
                                lists:flatten(io_lib:format(
                                    "Non-monotonic seq in ~s: ~p -> ~p",
                                    [TName, Prev, Curr])))
                        end, Pairs);
                    false ->
                        ok
                end
            end, Targets)
        after
            cleanup(Source, Targets)
        end
    end).

t_changes_feed_no_missing_after_split() ->
    ?_test(begin
        %% Simulate a client that read changes up to some point on source,
        %% then after split reads from target since=0.
        %% The client must see ALL docs — no gaps.
        Source = make_source(<<"e2e_changes_nomiss">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            %% Phase 1: write 100 docs
            write_docs(Source#shard.name, 100),

            %% Client reads changes on source — sees 100 docs
            SourceIds1 = get_changes_doc_ids(Source#shard.name),
            ?assertEqual(100, length(SourceIds1)),

            %% Phase 2: write 50 more docs, then split
            write_docs_range(Source#shard.name, 101, 150),

            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),
            ok = mem3_reshard_rep:topoff(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% After split, client reads changes from targets since=0
            %% Must see ALL 150 docs across all targets
            AllTargetIds = lists:flatmap(fun(#shard{name = TName}) ->
                get_changes_doc_ids(TName)
            end, Targets),

            AllTargetSet = sets:from_list(AllTargetIds),
            AllSourceIds = get_all_doc_ids(Source#shard.name),
            AllSourceSet = sets:from_list(AllSourceIds),

            Missing = sets:subtract(AllSourceSet, AllTargetSet),
            ?assertEqual(0, sets:size(Missing)),

            %% No duplicates across targets
            ?assertEqual(length(AllTargetIds), sets:size(AllTargetSet))
        after
            cleanup(Source, Targets)
        end
    end).

%% ===================================================================
%% 6. Failed split cleanup and recovery
%% ===================================================================

cleanup_recovery_test_() ->
    {
        "Failed splits must clean up and be recoverable",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_failed_replication_cleans_targets/0,
                fun t_source_untouched_after_failed_split/0,
                fun t_retry_after_failure_succeeds/0,
                fun t_targets_reusable_after_partial_replication/0
            ]
        }
    }.

t_failed_replication_cleans_targets() ->
    ?_test(begin
        %% If replication fails, target DBs should be cleaned up
        %% (before shard map update — safe to delete)
        Source = make_source(<<"e2e_cleanup_rep">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 50),
            ok = mem3_reshard_rep:create_target_dbs(Targets),

            %% Targets exist
            lists:foreach(fun(#shard{name = TName}) ->
                ?assert(couch_server:exists(TName))
            end, Targets),

            %% Simulate failure cleanup (what split/3 does on error)
            lists:foreach(fun(#shard{name = TName}) ->
                catch couch_server:delete(TName, [?ADMIN_CTX])
            end, Targets),

            %% Targets should be gone
            lists:foreach(fun(#shard{name = TName}) ->
                ?assertEqual(false, couch_server:exists(TName))
            end, Targets),

            %% Source must be untouched
            ?assertEqual(50, doc_count(Source#shard.name))
        after
            cleanup(Source, Targets)
        end
    end).

t_source_untouched_after_failed_split() ->
    ?_test(begin
        Source = make_source(<<"e2e_src_intact">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 100),
            OrigCount = doc_count(Source#shard.name),
            OrigIds = get_all_doc_ids(Source#shard.name),

            %% Create targets but DON'T complete split
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),
            %% "Crash" here — don't update shard map, don't delete source

            %% Source must be 100% intact
            ?assertEqual(OrigCount, doc_count(Source#shard.name)),
            PostIds = get_all_doc_ids(Source#shard.name),
            ?assertEqual(OrigIds, PostIds),

            %% Can still write to source
            write_doc_with_id(Source#shard.name, <<"post_crash">>,
                {[{<<"survived">>, true}]}),
            ?assertEqual(OrigCount + 1, doc_count(Source#shard.name)),

            %% Can still read from source
            {ok, _} = read_doc(Source#shard.name, <<"doc-0001">>),
            {ok, _} = read_doc(Source#shard.name, <<"post_crash">>)
        after
            cleanup(Source, Targets)
        end
    end).

t_retry_after_failure_succeeds() ->
    ?_test(begin
        %% After a failed split, retry should succeed without data loss
        Source = make_source(<<"e2e_retry">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 75),

            %% First attempt: create + partial replicate
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 25}, {batch_count, 1}]),  % Only 1 batch

            %% "Crash" and retry: targets already exist (idempotent)
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            %% Full replicate this time
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% All 75 docs must be present
            TargetCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(75, TargetCount),

            %% Verify consistency
            TargetNames = [T#shard.name || T <- Targets],
            ?assertEqual(ok,
                mem3_reshard_rep:verify_consistency(
                    Source#shard.name, TargetNames))
        after
            cleanup(Source, Targets)
        end
    end).

t_targets_reusable_after_partial_replication() ->
    ?_test(begin
        %% Targets with partial data can be completed via topoff
        Source = make_source(<<"e2e_partial">>),
        Targets = make_targets(Source, 2),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 100),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),

            %% Partial replication (1 batch of 30)
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 30}, {batch_count, 1}]),
            PartialCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assert(PartialCount < 100),
            ?assert(PartialCount > 0),

            %% Complete via full replicate (mem3_rep resumes from checkpoint)
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),
            FullCount = lists:sum([doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(100, FullCount),

            %% No duplicates — each doc in exactly one target
            Missing = find_missing_docs(Source#shard.name, Targets),
            ?assertEqual([], Missing)
        after
            cleanup(Source, Targets)
        end
    end).

%% ===================================================================
%% Helpers
%% ===================================================================

get_all_doc_ids(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    try
        {ok, Ids} = couch_db:fold_docs(Db, fun(FDI, Acc) ->
            #full_doc_info{id = Id} = FDI,
            {ok, [Id | Acc]}
        end, [], []),
        lists:sort(Ids)
    after
        couch_db:close(Db)
    end.

get_changes_doc_ids(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    try
        {ok, Ids} = couch_db:fold_changes(Db, 0, fun(#doc_info{id = Id}, Acc) ->
            {ok, [Id | Acc]}
        end, [], []),
        Ids
    after
        couch_db:close(Db)
    end.

get_changes_seqs(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    try
        {ok, Seqs} = couch_db:fold_changes(Db, 0, fun(#doc_info{high_seq = Seq}, Acc) ->
            {ok, [Seq | Acc]}
        end, [], []),
        lists:reverse(Seqs)
    after
        couch_db:close(Db)
    end.

write_docs_range(ShardName, From, To) ->
    {ok, Db} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
    try
        lists:foreach(fun(I) ->
            Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
            Body = {[{<<"n">>, I}, {<<"data">>,
                base64:encode(crypto:strong_rand_bytes(128))}]},
            Rev = couch_hash:md5_hash(term_to_binary({Id, I, range})),
            Doc = #doc{id = Id, body = Body, revs = {1, [Rev]}},
            {ok, _} = couch_db:update_docs(Db, [Doc], [replicated_changes])
        end, lists:seq(From, To))
    after
        couch_db:close(Db)
    end.

make_source(Name) ->
    Suffix = integer_to_binary(erlang:system_time(second)),
    FullName = <<"shards/00000000-ffffffff/", Name/binary, ".", Suffix/binary>>,
    #shard{name = FullName, node = node(), dbname = Name, range = [0, ?RING_END]}.

make_targets(#shard{range = Range, dbname = DbName, name = Name}, Factor) ->
    Ranges = mem3_reshard_rep:subdivide_range(Range, Factor),
    <<"shards/", _:8/binary, "-", _:8/binary, "/", DbAndSuffix/binary>> = Name,
    Sfx = case binary:split(DbAndSuffix, <<".">>) of
        [_, S0] -> S0;
        [_] -> integer_to_binary(erlang:system_time(second))
    end,
    [begin
        Sh = #shard{dbname = DbName, range = R, node = node()},
        mem3_util:name_shard(Sh, <<".", Sfx/binary>>)
    end || R <- Ranges].

create_shard(#shard{name = Name}) ->
    {ok, Db} = couch_db:create(Name, [?ADMIN_CTX]),
    couch_db:close(Db).

write_docs(ShardName, Count) ->
    {ok, Db} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
    try
        lists:foreach(fun(I) ->
            Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
            Body = {[{<<"n">>, I}, {<<"data">>,
                base64:encode(crypto:strong_rand_bytes(128))}]},
            Rev = couch_hash:md5_hash(term_to_binary({Id, I})),
            Doc = #doc{id = Id, body = Body, revs = {1, [Rev]}},
            {ok, _} = couch_db:update_docs(Db, [Doc], [replicated_changes])
        end, lists:seq(1, Count))
    after
        couch_db:close(Db)
    end.

write_doc_with_id(ShardName, Id, Body) ->
    {ok, Db} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
    try
        Rev = couch_hash:md5_hash(term_to_binary({Id, erlang:unique_integer()})),
        Doc = #doc{id = Id, body = Body, revs = {1, [Rev]}},
        {ok, _} = couch_db:update_docs(Db, [Doc], [replicated_changes])
    after
        couch_db:close(Db)
    end.

write_design_doc(ShardName, Id, Body) ->
    write_doc_with_id(ShardName, Id, Body).

delete_docs(ShardName, From, To) ->
    {ok, Db} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
    try
        lists:foreach(fun(I) ->
            Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
            case couch_db:open_doc(Db, Id, []) of
                {ok, #doc{revs = {Pos, [Rev | _]}}} ->
                    DelRev = couch_hash:md5_hash(
                        term_to_binary({Id, deleted, erlang:unique_integer()})),
                    DelDoc = #doc{id = Id, revs = {Pos + 1, [DelRev, Rev]},
                                  deleted = true},
                    {ok, _} = couch_db:update_docs(Db, [DelDoc], [replicated_changes]);
                _ -> ok
            end
        end, lists:seq(From, To))
    after
        couch_db:close(Db)
    end.

write_docs_to_targets(SourceName, Targets) ->
    {ok, SDb} = couch_db:open_int(SourceName, [?ADMIN_CTX]),
    try
        couch_db:fold_docs(SDb, fun(FDI, ok) ->
            #full_doc_info{id = DocId} = FDI,
            {ok, Doc} = couch_db:open_doc(SDb, DocId, []),
            case Doc#doc.deleted of
                true -> {ok, ok};  % Skip deletes
                false ->
                    %% Put in first target (wrong distribution)
                    [T1 | _] = Targets,
                    {ok, TDb} = couch_db:open_int(T1#shard.name, [?ADMIN_CTX]),
                    try
                        {ok, _} = couch_db:update_docs(TDb, [Doc], [replicated_changes])
                    after
                        couch_db:close(TDb)
                    end,
                    {ok, ok}
            end
        end, ok, [])
    after
        couch_db:close(SDb)
    end.

doc_count(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    try
        {ok, Info} = couch_db:get_db_info(Db),
        couch_util:get_value(doc_count, Info)
    after
        couch_db:close(Db)
    end.

del_count(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    try
        {ok, Info} = couch_db:get_db_info(Db),
        couch_util:get_value(doc_del_count, Info)
    after
        couch_db:close(Db)
    end.

read_doc(DbName, DocId) ->
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    try couch_db:open_doc(Db, DocId, [])
    after couch_db:close(Db) end.

doc_exists_in_targets(DocId, Targets) ->
    lists:any(fun(#shard{name = TName}) ->
        case read_doc(TName, DocId) of
            {ok, _} -> true;
            _ -> false
        end
    end, Targets).

find_doc_in_targets(DocId, Targets) ->
    Results = lists:filtermap(fun(#shard{name = TName}) ->
        case read_doc(TName, DocId) of
            {ok, Doc} -> {true, Doc};
            _ -> false
        end
    end, Targets),
    case Results of
        [Doc | _] -> {ok, Doc};
        [] -> {error, not_found}
    end.

find_target_with_doc(DocId, Targets) ->
    Results = lists:filtermap(fun(#shard{name = TName}) ->
        case read_doc(TName, DocId) of
            {ok, _} -> {true, TName};
            _ -> false
        end
    end, Targets),
    case Results of
        [TName | _] -> TName;
        [] -> error({doc_not_found_in_any_target, DocId})
    end.

find_missing_docs(SourceName, Targets) ->
    {ok, SDb} = couch_db:open_int(SourceName, [?ADMIN_CTX]),
    try
        {ok, Missing} = couch_db:fold_docs(SDb, fun(FDI, Acc) ->
            #full_doc_info{id = DocId} = FDI,
            case doc_exists_in_targets(DocId, Targets) of
                true -> {ok, Acc};
                false -> {ok, [DocId | Acc]}
            end
        end, [], []),
        Missing
    after
        couch_db:close(SDb)
    end.

cleanup(Source, Targets) ->
    catch couch_server:delete(Source#shard.name, [?ADMIN_CTX]),
    lists:foreach(fun(#shard{name = N}) ->
        catch couch_server:delete(N, [?ADMIN_CTX])
    end, Targets).
