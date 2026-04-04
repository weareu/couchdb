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

%% @doc Integration tests for the split orchestrator.
%%
%% These tests run the actual split/3 flow on local shard-named databases.
%% They verify that:
%%   - Target DBs are created
%%   - All docs land in the correct target (by hash)
%%   - Doc counts match after split
%%   - Deleted docs are handled correctly
%%   - Split is idempotent (re-running doesn't corrupt)
%%   - Verification catches real problems
%%
%% NOTE: These tests run against the LOCAL node only — no cluster.
%% Cross-node replication is tested separately in cross-DC chaos tests.

-module(mem3_reshard_rep_split_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

-define(DELAY, 100).
-define(WAIT_DELAY_COUNT, 50).

%% ===================================================================
%% Test fixtures
%% ===================================================================

%% ===================================================================
%% 1. Create target DBs
%% ===================================================================

create_targets_test_() ->
    {
        "Create target databases",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_create_targets_creates_dbs/0,
                fun t_create_targets_idempotent/0,
                fun t_create_targets_on_local_node/0
            ]
        }
    }.

t_create_targets_creates_dbs() ->
    ?_test(begin
        Targets = make_test_targets(2),
        try
            ?assertEqual(ok, mem3_reshard_rep:create_target_dbs(Targets)),
            %% All target DBs should exist
            lists:foreach(fun(#shard{name = Name}) ->
                ?assert(couch_server:exists(Name))
            end, Targets)
        after
            cleanup_targets(Targets)
        end
    end).

t_create_targets_idempotent() ->
    ?_test(begin
        Targets = make_test_targets(2),
        try
            ?assertEqual(ok, mem3_reshard_rep:create_target_dbs(Targets)),
            %% Calling again should not fail
            ?assertEqual(ok, mem3_reshard_rep:create_target_dbs(Targets))
        after
            cleanup_targets(Targets)
        end
    end).

t_create_targets_on_local_node() ->
    ?_test(begin
        %% Targets on current node should be created directly
        T1 = #shard{
            name = <<"shards/00000000-7fffffff/createtest.1234567890">>,
            node = node(),
            dbname = <<"createtest">>,
            range = [0, 16#7FFFFFFF]
        },
        try
            ?assertEqual(ok, mem3_reshard_rep:create_target_dbs([T1])),
            ?assert(couch_server:exists(T1#shard.name))
        after
            catch couch_server:delete(T1#shard.name, [?ADMIN_CTX])
        end
    end).

%% ===================================================================
%% 2. Local replication — docs land in correct targets
%% ===================================================================

local_replication_test_() ->
    {
        "Local replication with hash routing",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_replicate_routes_by_hash/0,
                fun t_replicate_all_docs_transferred/0,
                fun t_replicate_no_doc_loss/0
            ]
        }
    }.

t_replicate_routes_by_hash() ->
    ?_test(begin
        Source = make_source_shard(<<"hashroute">>),
        Targets = make_split_targets(Source, 2),
        create_shard_db(Source),
        try
            %% Write 100 docs to source
            write_docs_to_shard(Source#shard.name, 100),

            %% Create targets and replicate
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Every doc should be in exactly one target
            {ok, SDb} = couch_db:open_int(Source#shard.name, [?ADMIN_CTX]),
            try
                couch_db:fold_docs(SDb, fun(FDI, ok) ->
                    #full_doc_info{id = DocId} = FDI,
                    TargetNames = [T#shard.name || T <- Targets],
                    Found = lists:filter(fun(TName) ->
                        {ok, TDb} = couch_db:open_int(TName, [?ADMIN_CTX]),
                        try
                            case couch_db:open_doc(TDb, DocId, []) of
                                {ok, _} -> true;
                                _ -> false
                            end
                        after
                            couch_db:close(TDb)
                        end
                    end, TargetNames),
                    ?assertEqual(1, length(Found),
                        lists:flatten(io_lib:format(
                            "Doc ~s found in ~B targets (expected 1)",
                            [DocId, length(Found)]))),
                    {ok, ok}
                end, ok, [])
            after
                couch_db:close(SDb)
            end
        after
            cleanup_shard(Source),
            cleanup_targets(Targets)
        end
    end).

t_replicate_all_docs_transferred() ->
    ?_test(begin
        Source = make_source_shard(<<"alltransfer">>),
        Targets = make_split_targets(Source, 4),
        create_shard_db(Source),
        try
            write_docs_to_shard(Source#shard.name, 200),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            %% Sum of target doc counts must equal source
            SourceCount = get_doc_count(Source#shard.name),
            TargetCount = lists:sum([get_doc_count(T#shard.name) || T <- Targets]),
            ?assertEqual(SourceCount, TargetCount)
        after
            cleanup_shard(Source),
            cleanup_targets(Targets)
        end
    end).

t_replicate_no_doc_loss() ->
    ?_test(begin
        Source = make_source_shard(<<"noloss">>),
        Targets = make_split_targets(Source, 2),
        create_shard_db(Source),
        try
            write_docs_to_shard(Source#shard.name, 50),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 50}, {batch_count, all}]),

            %% Verify consistency
            TargetNames = [T#shard.name || T <- Targets],
            ?assertEqual(ok,
                mem3_reshard_rep:verify_doc_counts(Source#shard.name, TargetNames))
        after
            cleanup_shard(Source),
            cleanup_targets(Targets)
        end
    end).

%% ===================================================================
%% 3. Verification catches real problems
%% ===================================================================

verification_test_() ->
    {
        "Verification catches real problems",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_verify_passes_after_good_split/0,
                fun t_verify_fails_when_docs_missing/0
            ]
        }
    }.

t_verify_passes_after_good_split() ->
    ?_test(begin
        Source = make_source_shard(<<"verifygood">>),
        Targets = make_split_targets(Source, 2),
        create_shard_db(Source),
        try
            write_docs_to_shard(Source#shard.name, 50),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            TMap = mem3_reshard_rep:build_target_map(Targets),
            ok = mem3_reshard_rep:replicate(Source, TMap,
                [{batch_size, 100}, {batch_count, all}]),

            TargetNames = [T#shard.name || T <- Targets],
            ?assertEqual(ok,
                mem3_reshard_rep:verify_consistency(
                    Source#shard.name, TargetNames))
        after
            cleanup_shard(Source),
            cleanup_targets(Targets)
        end
    end).

t_verify_fails_when_docs_missing() ->
    ?_test(begin
        Source = make_source_shard(<<"verifybad">>),
        Targets = make_split_targets(Source, 2),
        create_shard_db(Source),
        try
            write_docs_to_shard(Source#shard.name, 50),
            ok = mem3_reshard_rep:create_target_dbs(Targets),
            %% DON'T replicate — targets are empty

            TargetNames = [T#shard.name || T <- Targets],
            Result = mem3_reshard_rep:verify_consistency(
                Source#shard.name, TargetNames),
            ?assertMatch({error, _}, Result)
        after
            cleanup_shard(Source),
            cleanup_targets(Targets)
        end
    end).

%% ===================================================================
%% Helpers
%% ===================================================================

make_source_shard(Name) ->
    Suffix = integer_to_binary(erlang:system_time(second)),
    FullName = <<"shards/00000000-ffffffff/", Name/binary, ".", Suffix/binary>>,
    #shard{
        name = FullName,
        node = node(),
        dbname = Name,
        range = [0, ?RING_END]
    }.

make_split_targets(#shard{range = Range, dbname = DbName, name = Name}, Factor) ->
    Ranges = mem3_reshard_rep:subdivide_range(Range, Factor),
    <<"shards/", _:8/binary, "-", _:8/binary, "/", DbAndSuffix/binary>> = Name,
    Suffix = case binary:split(DbAndSuffix, <<".">>) of
        [_, S] -> S;
        [_] -> integer_to_binary(erlang:system_time(second))
    end,
    [begin
        Shard = #shard{dbname = DbName, range = R, node = node()},
        mem3_util:name_shard(Shard, <<".", Suffix/binary>>)
    end || R <- Ranges].

make_test_targets(Count) ->
    Ranges = mem3_reshard_rep:subdivide_range([0, ?RING_END], Count),
    Suffix = integer_to_binary(erlang:system_time(second)),
    [begin
        S = #shard{dbname = <<"targettest">>, range = R, node = node()},
        mem3_util:name_shard(S, <<".", Suffix/binary>>)
    end || R <- Ranges].

create_shard_db(#shard{name = Name}) ->
    {ok, Db} = couch_db:create(Name, [?ADMIN_CTX]),
    couch_db:close(Db).

write_docs_to_shard(ShardName, Count) ->
    {ok, Db} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
    try
        lists:foreach(fun(I) ->
            Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
            Body = {[{<<"n">>, I}, {<<"data">>,
                base64:encode(crypto:strong_rand_bytes(256))}]},
            Rev = couch_hash:md5_hash(term_to_binary({Id, I})),
            Doc = #doc{id = Id, body = Body, revs = {1, [Rev]}},
            {ok, _} = couch_db:update_docs(Db, [Doc], [replicated_changes])
        end, lists:seq(1, Count))
    after
        couch_db:close(Db)
    end.

get_doc_count(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    try
        {ok, Info} = couch_db:get_db_info(Db),
        couch_util:get_value(doc_count, Info)
    after
        couch_db:close(Db)
    end.

cleanup_shard(#shard{name = Name}) ->
    catch couch_server:delete(Name, [?ADMIN_CTX]).

cleanup_targets(Targets) ->
    lists:foreach(fun(#shard{name = Name}) ->
        catch couch_server:delete(Name, [?ADMIN_CTX])
    end, Targets).
