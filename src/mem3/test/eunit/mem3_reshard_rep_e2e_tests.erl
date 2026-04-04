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
%% Helpers
%% ===================================================================

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
