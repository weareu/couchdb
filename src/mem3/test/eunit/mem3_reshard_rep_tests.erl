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
%% Range subdivision and hash routing tests are CRITICAL — a bug here
%% means docs land in the wrong shard and are silently lost.
%%
%% Doc count/distribution tests use REAL databases.

-module(mem3_reshard_rep_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

%% ===================================================================
%% 1. Range subdivision — mathematical correctness
%%    A bug here = docs hash to wrong target = DATA LOSS
%% ===================================================================

subdivide_range_test_() ->
    {"Range subdivision (data-critical math)", [
        {"2-way split covers full ring with no gaps",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], 2),
             ?assertEqual(2, length(Ranges)),
             assert_covers_full_range(Ranges, 0, ?RING_END)
         end)},

        {"4-way split covers full ring with no gaps",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], 4),
             ?assertEqual(4, length(Ranges)),
             assert_covers_full_range(Ranges, 0, ?RING_END)
         end)},

        {"32-way split covers full ring (500GB scenario)",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], 32),
             ?assertEqual(32, length(Ranges)),
             assert_covers_full_range(Ranges, 0, ?RING_END),
             %% All widths roughly equal (max diff 1)
             Widths = [E - B + 1 || [B, E] <- Ranges],
             ?assert(lists:max(Widths) - lists:min(Widths) =< 1)
         end)},

        {"Sub-range split preserves boundaries",
         ?_test(begin
             Half = ?RING_END div 2,
             Ranges = mem3_reshard_rep:subdivide_range([0, Half], 2),
             ?assertEqual(2, length(Ranges)),
             [[0, _], [_, HalfEnd]] = Ranges,
             ?assertEqual(Half, HalfEnd),
             assert_contiguous(Ranges)
         end)},

        {"Factor too large for range errors",
         ?_assertError({range_too_small, _, _},
            mem3_reshard_rep:subdivide_range([0, 1], 4))},

        {"Minimum range [0,1] splits to [[0,0],[1,1]]",
         ?_assertEqual([[0, 0], [1, 1]],
            mem3_reshard_rep:subdivide_range([0, 1], 2))}
    ]}.

%% ===================================================================
%% 2. Hash routing — every hash value in exactly one range
%%    A bug here = doc in two shards OR doc in zero shards
%% ===================================================================

hash_routing_test_() ->
    {"Hash routing correctness (data-critical)", [
        {"10K random hashes each land in exactly one range",
         ?_test(begin
             lists:foreach(fun(Factor) ->
                 Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], Factor),
                 lists:foreach(fun(_) ->
                     Hash = rand:uniform(?RING_END + 1) - 1,
                     Matching = [R || [B, E] = R <- Ranges, Hash >= B, Hash =< E],
                     ?assertEqual(1, length(Matching))
                 end, lists:seq(1, 10000))
             end, [2, 4, 8, 16])
         end)},

        {"Edge values 0 and RING_END are in a range",
         ?_test(begin
             Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], 4),
             %% Hash value 0
             ?assertEqual(1, length([R || [B, E] = R <- Ranges, 0 >= B, 0 =< E])),
             %% Hash value RING_END
             ?assertEqual(1, length([R || [B, E] = R <- Ranges,
                                         ?RING_END >= B, ?RING_END =< E]))
         end)}
    ]}.

%% ===================================================================
%% 3. Doc count verification — real databases
%% ===================================================================

doc_count_test_() ->
    {
        "Doc count verification with real databases",
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
        create_and_populate(Target2, 5),
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
        create_and_populate(Source, 10),
        {ok, SDb} = couch_db:open_int(Source, [?ADMIN_CTX]),
        try
            lists:foreach(fun(I) ->
                Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
                {ok, Doc} = couch_db:open_doc(SDb, Id, []),
                {ok, _} = couch_db:update_doc(SDb, Doc#doc{deleted = true}, [])
            end, lists:seq(1, 3))
        after
            couch_db:close(SDb)
        end,
        %% Target: 7 live + 3 deleted
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
%% Helpers
%% ===================================================================

assert_covers_full_range(Ranges, ExpBegin, ExpEnd) ->
    [[FirstBegin, _] | _] = Ranges,
    [_, LastEnd] = lists:last(Ranges),
    ?assertEqual(ExpBegin, FirstBegin),
    ?assertEqual(ExpEnd, LastEnd),
    %% Total width must equal original
    TotalWidth = lists:sum([E - B + 1 || [B, E] <- Ranges]),
    ?assertEqual(ExpEnd - ExpBegin + 1, TotalWidth),
    assert_contiguous(Ranges).

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
            {ok, _} = couch_db:update_doc(Db, #doc{id = Id, body = {[{<<"n">>, I}]}}, [])
        end, lists:seq(1, DocCount))
    after
        couch_db:close(Db)
    end.

cleanup(DbNames) ->
    lists:foreach(fun(DbName) ->
        catch couch_server:delete(DbName, [?ADMIN_CTX])
    end, DbNames).
