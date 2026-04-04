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

%% @doc Tests for shard size cache.
%%
%% These tests use REAL databases and REAL compaction to verify that
%% mem3_shard_size correctly reports file sizes and that the size
%% cache is accurate for triggering auto-split decisions.
%%
%% The stakes: if shard sizes are wrong, auto-split either never fires
%% (data grows unbounded) or fires too aggressively (unnecessary splits).

-module(mem3_shard_size_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("kernel/include/file.hrl").

-define(DELAY, 100).
-define(WAIT_DELAY_COUNT, 50).

%% ===================================================================
%% Test fixtures
%% ===================================================================

setup() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    couch_db:close(Db),
    DbName.

teardown(DbName) ->
    couch_server:delete(DbName, [?ADMIN_CTX]),
    ok.

%% ===================================================================
%% 1. Core size scanning — does it report real sizes?
%% ===================================================================

size_scan_test_() ->
    {
        "Shard size scanning with real databases",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_scan_finds_shard_db/1,
                    fun t_scan_skips_non_shard/1,
                    fun t_size_increases_with_data/1,
                    fun t_size_decreases_after_compaction/1,
                    fun t_size_reflects_attachments/1,
                    fun t_get_sizes_over_threshold/1,
                    fun t_get_sizes_over_returns_empty_when_all_small/1,
                    fun t_deleted_db_handled_gracefully/1,
                    fun t_concurrent_scan_safe/1,
                    fun t_size_nonzero_for_empty_db/1
                ]
            }
        }
    }.

t_scan_finds_shard_db(_NonShardDb) ->
    ?_test(begin
        %% Create a shard-named database (simulates real shard)
        ShardName = <<"shards/00000000-ffffffff/testdb.1234567890">>,
        {ok, Db} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db),
        try
            Table = ets:new(test_shard_size, [set, public]),
            try
                mem3_shard_size:scan_local(Table),
                Result = ets:lookup(Table, ShardName),
                ?assertMatch([{ShardName, Size, _Ts}] when is_integer(Size)
                    andalso Size > 0, Result)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

t_scan_skips_non_shard(DbName) ->
    ?_test(begin
        %% Non-shard DBs (no "shards/" prefix) must be skipped
        Table = ets:new(test_shard_size, [set, public]),
        try
            mem3_shard_size:scan_local(Table),
            Result = ets:lookup(Table, DbName),
            ?assertEqual([], Result)
        after
            ets:delete(Table)
        end
    end).

t_size_increases_with_data(_DbName) ->
    ?_test(begin
        ShardName = <<"shards/00000000-ffffffff/sizetest.1234567890">>,
        {ok, Db0} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db0),
        try
            Table = ets:new(test_shard_size, [set, public]),
            try
                %% Measure empty size
                mem3_shard_size:scan_local(Table),
                [{_, EmptySize, _}] = ets:lookup(Table, ShardName),

                %% Write 50 docs with substantial bodies
                {ok, Db1} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
                try
                    lists:foreach(fun(I) ->
                        Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
                        Body = {[{<<"data">>, base64:encode(
                            crypto:strong_rand_bytes(1024))}]},
                        Doc = #doc{id = Id, body = Body},
                        {ok, _} = write_doc_bypass_vdu(Db1, Doc)
                    end, lists:seq(1, 50))
                after
                    couch_db:close(Db1)
                end,

                %% Re-scan and verify size increased
                mem3_shard_size:scan_local(Table),
                [{_, FullSize, _}] = ets:lookup(Table, ShardName),
                ?assert(FullSize > EmptySize)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

t_size_decreases_after_compaction(_DbName) ->
    ?_test(begin
        %% Compaction test uses a non-shard DB name to allow interactive
        %% updates (proper revision chains, not conflict branches).
        %% This verifies that file size reported by get_size_info is
        %% accurate for auto-split threshold decisions.
        CompactDb = ?tempdb(),
        {ok, Db0} = couch_db:create(CompactDb, [?ADMIN_CTX]),
        couch_db:close(Db0),
        try
            %% Write 50 docs with ~3KB bodies
            {ok, Db1} = couch_db:open_int(CompactDb, [?ADMIN_CTX]),
            try
                lists:foreach(fun(I) ->
                    Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
                    Body = {[{<<"data">>, base64:encode(
                        crypto:strong_rand_bytes(2048))}]},
                    {ok, _} = couch_db:update_doc(Db1,
                        #doc{id = Id, body = Body}, [])
                end, lists:seq(1, 50))
            after
                couch_db:close(Db1)
            end,
            %% Update each doc 3 times to create waste (old rev bodies)
            lists:foreach(fun(_Round) ->
                {ok, Db2} = couch_db:open_int(CompactDb, [?ADMIN_CTX]),
                try
                    lists:foreach(fun(I) ->
                        Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
                        {ok, Doc} = couch_db:open_doc(Db2, Id, []),
                        Updated = Doc#doc{body = {[{<<"v">>, I}]}},
                        {ok, _} = couch_db:update_doc(Db2, Updated, [])
                    end, lists:seq(1, 50))
                after
                    couch_db:close(Db2)
                end
            end, lists:seq(1, 3)),

            %% Get file size before compaction
            {ok, DbPre} = couch_db:open_int(CompactDb, [?ADMIN_CTX]),
            PreInfo = try couch_db_engine:get_size_info(DbPre)
                after couch_db:close(DbPre) end,
            BeforeCompact = couch_util:get_value(file, PreInfo, 0),

            %% Compact — removes old revision bodies
            {ok, DbC} = couch_db:open_int(CompactDb, [?ADMIN_CTX]),
            {ok, _} = couch_db:start_compact(DbC),
            couch_db:close(DbC),
            wait_db_compact_done(CompactDb),

            %% Get file size after compaction
            {ok, DbPost} = couch_db:open_int(CompactDb, [?ADMIN_CTX]),
            PostInfo = try couch_db_engine:get_size_info(DbPost)
                after couch_db:close(DbPost) end,
            AfterCompact = couch_util:get_value(file, PostInfo, 0),

            %% File must shrink — old revision bodies reclaimed
            ?assert(AfterCompact < BeforeCompact)
        after
            couch_server:delete(CompactDb, [?ADMIN_CTX])
        end
    end).

t_size_reflects_attachments(_DbName) ->
    ?_test(begin
        %% Verify that large document bodies increase file size proportionally.
        ShardName = <<"shards/00000000-ffffffff/bulktest.1234567890">>,
        {ok, Db0} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db0),
        try
            Table = ets:new(test_shard_size, [set, public]),
            try
                %% Measure empty
                mem3_shard_size:scan_local(Table),
                [{_, EmptySize, _}] = ets:lookup(Table, ShardName),

                %% Write 50 docs with 4KB bodies each (~200KB total)
                lists:foreach(fun(I) ->
                    {ok, Db1} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
                    try
                        Id = list_to_binary(io_lib:format("bulk-~4..0B", [I])),
                        Payload = base64:encode(crypto:strong_rand_bytes(3072)),
                        Doc = #doc{id = Id, body = {[{<<"data">>, Payload}]}},
                        {ok, _} = write_doc_bypass_vdu(Db1, Doc)
                    after
                        couch_db:close(Db1)
                    end
                end, lists:seq(1, 50)),

                %% Re-scan: size must reflect the bulk data
                mem3_shard_size:scan_local(Table),
                [{_, WithData, _}] = ets:lookup(Table, ShardName),
                %% Should have grown significantly
                ?assert(WithData > EmptySize * 2)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

t_get_sizes_over_threshold(_DbName) ->
    ?_test(begin
        Shard1 = <<"shards/00000000-7fffffff/overtest.1234567890">>,
        Shard2 = <<"shards/80000000-ffffffff/overtest.1234567890">>,
        {ok, D1} = couch_db:create(Shard1, [?ADMIN_CTX]),
        couch_db:close(D1),
        {ok, D2} = couch_db:create(Shard2, [?ADMIN_CTX]),
        couch_db:close(D2),
        try
            %% Write lots of data to shard1, little to shard2
            {ok, Db1} = couch_db:open_int(Shard1, [?ADMIN_CTX]),
            try
                lists:foreach(fun(I) ->
                    Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
                    Body = {[{<<"d">>, base64:encode(
                        crypto:strong_rand_bytes(4096))}]},
                    {ok, _} = write_doc_bypass_vdu(Db1, #doc{id = Id, body = Body})
                end, lists:seq(1, 100))
            after
                couch_db:close(Db1)
            end,

            Table = ets:new(test_shard_size, [set, public]),
            try
                mem3_shard_size:scan_local(Table),
                [{_, Size1, _}] = ets:lookup(Table, Shard1),
                [{_, Size2, _}] = ets:lookup(Table, Shard2),

                %% Use a threshold between the two sizes
                Threshold = (Size1 + Size2) div 2,
                ?assert(Size1 > Threshold),
                ?assert(Size2 =< Threshold),

                %% get_sizes_over should return only the big shard
                MatchSpec = [{{'$1', '$2', '_'}, [{'>', '$2', Threshold}],
                    [{{'$1', '$2'}}]}],
                Over = ets:select(Table, MatchSpec),
                ?assertEqual(1, length(Over)),
                [{OverName, OverSize}] = Over,
                ?assertEqual(Shard1, OverName),
                ?assertEqual(Size1, OverSize)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(Shard1, [?ADMIN_CTX]),
            couch_server:delete(Shard2, [?ADMIN_CTX])
        end
    end).

t_get_sizes_over_returns_empty_when_all_small(_DbName) ->
    ?_test(begin
        ShardName = <<"shards/00000000-ffffffff/smalldb.1234567890">>,
        {ok, Db0} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db0),
        try
            Table = ets:new(test_shard_size, [set, public]),
            try
                mem3_shard_size:scan_local(Table),
                %% Set threshold way above any test DB size (1 TB)
                MatchSpec = [{{'$1', '$2', '_'},
                    [{'>', '$2', 1099511627776}], [{{'$1', '$2'}}]}],
                Over = ets:select(Table, MatchSpec),
                ?assertEqual([], Over)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

t_deleted_db_handled_gracefully(_DbName) ->
    ?_test(begin
        ShardName = <<"shards/00000000-ffffffff/deleteme.1234567890">>,
        {ok, Db0} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db0),
        %% Delete it BEFORE scanning
        couch_server:delete(ShardName, [?ADMIN_CTX]),
        Table = ets:new(test_shard_size, [set, public]),
        try
            %% scan_local must not crash even though DB no longer exists
            ?assertEqual(ok, mem3_shard_size:scan_local(Table)),
            ?assertEqual([], ets:lookup(Table, ShardName))
        after
            ets:delete(Table)
        end
    end).

t_concurrent_scan_safe(_DbName) ->
    ?_test(begin
        ShardName = <<"shards/00000000-ffffffff/concurrent.1234567890">>,
        {ok, Db0} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db0),
        try
            Table = ets:new(test_shard_size, [set, public]),
            try
                %% Run 5 scans concurrently — must not crash
                Pids = lists:map(fun(_) ->
                    spawn_monitor(fun() -> mem3_shard_size:scan_local(Table) end)
                end, lists:seq(1, 5)),

                %% Wait for all to complete
                lists:foreach(fun({Pid, Ref}) ->
                    receive
                        {'DOWN', Ref, process, Pid, normal} -> ok;
                        {'DOWN', Ref, process, Pid, Reason} ->
                            error({scan_crashed, Reason})
                    after 10000 ->
                        error(scan_timeout)
                    end
                end, Pids),

                %% Table should have exactly one entry for our shard
                Result = ets:lookup(Table, ShardName),
                ?assertMatch([{ShardName, _, _}], Result)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

t_size_nonzero_for_empty_db(_DbName) ->
    ?_test(begin
        %% Even an empty DB has a file header — size must be > 0
        ShardName = <<"shards/00000000-ffffffff/emptydb.1234567890">>,
        {ok, Db0} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db0),
        try
            Table = ets:new(test_shard_size, [set, public]),
            try
                mem3_shard_size:scan_local(Table),
                [{_, Size, _}] = ets:lookup(Table, ShardName),
                ?assert(Size > 0)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

%% ===================================================================
%% 2. Size accuracy — can we trust these numbers for split decisions?
%% ===================================================================

size_accuracy_test_() ->
    {
        "Size accuracy for split decisions",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_size_matches_file_system/1,
                    fun t_size_stable_across_scans/1,
                    fun t_size_updates_after_writes/1
                ]
            }
        }
    }.

t_size_matches_file_system(_DbName) ->
    ?_test(begin
        %% Verify that reported size matches actual file size on disk
        ShardName = <<"shards/00000000-ffffffff/fscheck.1234567890">>,
        {ok, Db0} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db0),
        try
            %% Write some data
            {ok, Db1} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
            try
                lists:foreach(fun(I) ->
                    Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
                    {ok, _} = write_doc_bypass_vdu(Db1,
                        #doc{id = Id, body = {[{<<"x">>, I}]}})
                end, lists:seq(1, 20))
            after
                couch_db:close(Db1)
            end,

            Table = ets:new(test_shard_size, [set, public]),
            try
                mem3_shard_size:scan_local(Table),
                [{_, ReportedSize, _}] = ets:lookup(Table, ShardName),

                %% Get actual file path and stat it
                {ok, Db2} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
                FilePath = try couch_db:get_filepath(Db2)
                    after couch_db:close(Db2) end,
                {ok, FileInfo} = file:read_file_info(FilePath),
                ActualSize = FileInfo#file_info.size,

                %% Reported size should be close to actual file size
                %% (may differ slightly due to OS buffering)
                Ratio = ReportedSize / max(ActualSize, 1),
                ?assert(Ratio > 0.5 andalso Ratio < 2.0)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

t_size_stable_across_scans(_DbName) ->
    ?_test(begin
        %% Two consecutive scans without writes should return same size
        ShardName = <<"shards/00000000-ffffffff/stable.1234567890">>,
        {ok, Db0} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db0),
        try
            {ok, Db1} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
            try
                {ok, _} = write_doc_bypass_vdu(Db1,
                    #doc{id = <<"stable_doc">>, body = {[{<<"k">>, <<"v">>}]}})
            after
                couch_db:close(Db1)
            end,

            Table = ets:new(test_shard_size, [set, public]),
            try
                mem3_shard_size:scan_local(Table),
                [{_, Size1, _}] = ets:lookup(Table, ShardName),

                mem3_shard_size:scan_local(Table),
                [{_, Size2, _}] = ets:lookup(Table, ShardName),

                ?assertEqual(Size1, Size2)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

t_size_updates_after_writes(_DbName) ->
    ?_test(begin
        ShardName = <<"shards/00000000-ffffffff/growing.1234567890">>,
        {ok, Db0} = couch_db:create(ShardName, [?ADMIN_CTX]),
        couch_db:close(Db0),
        try
            Table = ets:new(test_shard_size, [set, public]),
            try
                mem3_shard_size:scan_local(Table),
                [{_, Size1, _}] = ets:lookup(Table, ShardName),

                %% Write more data
                {ok, Db1} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
                try
                    lists:foreach(fun(I) ->
                        Id = list_to_binary(io_lib:format("new-~4..0B", [I])),
                        Body = {[{<<"payload">>, base64:encode(
                            crypto:strong_rand_bytes(2048))}]},
                        {ok, _} = write_doc_bypass_vdu(Db1,
                            #doc{id = Id, body = Body})
                    end, lists:seq(1, 30))
                after
                    couch_db:close(Db1)
                end,

                %% Re-scan must show larger size
                mem3_shard_size:scan_local(Table),
                [{_, Size2, _}] = ets:lookup(Table, ShardName),
                ?assert(Size2 > Size1)
            after
                ets:delete(Table)
            end
        after
            couch_server:delete(ShardName, [?ADMIN_CTX])
        end
    end).

%% ===================================================================
%% 3. is_shard/1 — classification correctness
%% ===================================================================

is_shard_test_() ->
    {"Shard name classification", [
        ?_assertEqual(true, mem3_shard_size:is_shard(
            <<"shards/00000000-ffffffff/mydb.1234567890">>)),
        ?_assertEqual(true, mem3_shard_size:is_shard(
            <<"shards/00000000-7fffffff/users.1234567890">>)),
        ?_assertEqual(true, mem3_shard_size:is_shard(
            <<"shards/80000000-ffffffff/big_database.1234567890">>)),
        ?_assertEqual(false, mem3_shard_size:is_shard(<<"mydb">>)),
        ?_assertEqual(false, mem3_shard_size:is_shard(<<"_users">>)),
        ?_assertEqual(false, mem3_shard_size:is_shard(<<"_replicator">>)),
        ?_assertEqual(false, mem3_shard_size:is_shard(<<"_dbs">>)),
        ?_assertEqual(false, mem3_shard_size:is_shard(<<>>)),
        ?_assertEqual(false, mem3_shard_size:is_shard(<<"shards">>)),
        ?_assertEqual(false, mem3_shard_size:is_shard(<<"shard/something">>))
    ]}.

%% ===================================================================
%% Helpers
%% ===================================================================

%% Write a doc bypassing validation (VDU calls mem3:dbname which fails
%% for shard-named DBs in eunit without a full cluster).
write_doc_bypass_vdu(Db, #doc{} = Doc) ->
    case Doc#doc.revs of
        {0, []} ->
            %% New doc — generate a rev
            NewRev = couch_hash:md5_hash(term_to_binary({Doc#doc.id, erlang:monotonic_time()})),
            Doc1 = Doc#doc{revs = {1, [NewRev]}},
            {ok, _} = couch_db:update_docs(Db, [Doc1], [replicated_changes]),
            {ok, {1, NewRev}};
        {Pos, [Rev | _]} ->
            %% Update/delete with existing rev — create child revision
            NewRev = couch_hash:md5_hash(term_to_binary({Doc#doc.id, Rev, erlang:monotonic_time()})),
            Doc1 = Doc#doc{revs = {Pos + 1, [NewRev, Rev]}},
            {ok, _} = couch_db:update_docs(Db, [Doc1], [replicated_changes]),
            {ok, {Pos + 1, NewRev}}
    end.

compact_db(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
    {ok, _Pid} = couch_db:start_compact(Db),
    couch_db:close(Db),
    wait_db_compact_done(DbName).

wait_db_compact_done(DbName) ->
    wait_db_compact_done(DbName, ?WAIT_DELAY_COUNT).

wait_db_compact_done(_DbName, 0) ->
    error({assertion_failed, [{module, ?MODULE}, {line, ?LINE},
        {reason, "DB compaction failed to finish"}]});
wait_db_compact_done(DbName, N) ->
    IsDone = try
        {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
        try not is_pid(couch_db:get_compactor_pid(Db))
        after couch_db:close(Db) end
    catch _:_ -> false end,
    case IsDone of
        true -> ok;
        false ->
            timer:sleep(?DELAY),
            wait_db_compact_done(DbName, N - 1)
    end.
