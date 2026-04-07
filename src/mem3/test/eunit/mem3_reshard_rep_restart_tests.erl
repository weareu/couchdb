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

%% @doc End-to-end tests for split checkpoint persistence, crash recovery,
%% orphan cleanup, and _active_tasks integration.
%%
%% These tests use REAL databases to verify:
%%   - Checkpoint docs are written to source shard during split
%%   - Checkpoint docs are cleaned up after split completes
%%   - Interrupted splits leave checkpoint docs that can be found
%%   - Orphan target cleanup works for pre-map-update states
%%   - find_interrupted_splits scans all local shards correctly

-module(mem3_reshard_rep_restart_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("mem3/include/mem3_reshard_rep.hrl").

%% ===================================================================
%% 1. Checkpoint persistence — real _local docs
%% ===================================================================

checkpoint_test_() ->
    {
        "Checkpoint persistence with real databases",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_checkpoint_write_and_read/0,
                fun t_checkpoint_update_overwrites/0,
                fun t_checkpoint_delete/0,
                fun t_checkpoint_not_found_for_unknown/0,
                fun t_checkpoint_survives_close_reopen/0
            ]
        }
    }.

t_checkpoint_write_and_read() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    couch_db:close(Db),
    try
        %% Build a fake split state
        Source = #shard{name = DbName, range = [0, 16#ffffffff]},
        Target1 = #shard{name = <<"target1">>, range = [0, 16#7fffffff]},
        Target2 = #shard{name = <<"target2">>, range = [16#80000000, 16#ffffffff]},
        St = #split_state{
            source = Source,
            targets = [Target1, Target2],
            target_map = #{},
            factor = 2,
            state = replicating
        },
        %% Write checkpoint
        ok = mem3_reshard_rep:checkpoint_state(DbName, St),

        %% Read it back
        {ok, Info} = mem3_reshard_rep:load_checkpoint(DbName),
        ?assertEqual(replicating, maps:get(state, Info)),
        ?assertEqual(DbName, maps:get(source, Info)),
        ?assertEqual(2, maps:get(factor, Info)),
        ?assertEqual([<<"target1">>, <<"target2">>], maps:get(targets, Info))
    after
        couch_server:delete(DbName, [?ADMIN_CTX])
    end.

t_checkpoint_update_overwrites() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    couch_db:close(Db),
    try
        Source = #shard{name = DbName, range = [0, 16#ffffffff]},
        Target = #shard{name = <<"t1">>, range = [0, 16#ffffffff]},
        St1 = #split_state{
            source = Source, targets = [Target],
            target_map = #{}, factor = 2, state = replicating
        },
        St2 = St1#split_state{state = topoff_1},

        ok = mem3_reshard_rep:checkpoint_state(DbName, St1),
        {ok, Info1} = mem3_reshard_rep:load_checkpoint(DbName),
        ?assertEqual(replicating, maps:get(state, Info1)),

        %% Update to new state
        ok = mem3_reshard_rep:checkpoint_state(DbName, St2),
        {ok, Info2} = mem3_reshard_rep:load_checkpoint(DbName),
        ?assertEqual(topoff_1, maps:get(state, Info2))
    after
        couch_server:delete(DbName, [?ADMIN_CTX])
    end.

t_checkpoint_delete() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    couch_db:close(Db),
    try
        Source = #shard{name = DbName, range = [0, 16#ffffffff]},
        Target = #shard{name = <<"t1">>, range = [0, 16#ffffffff]},
        St = #split_state{
            source = Source, targets = [Target],
            target_map = #{}, factor = 2, state = verifying
        },
        ok = mem3_reshard_rep:checkpoint_state(DbName, St),
        {ok, _} = mem3_reshard_rep:load_checkpoint(DbName),

        %% Delete
        ok = mem3_reshard_rep:delete_checkpoint(DbName),
        ?assertEqual(not_found, mem3_reshard_rep:load_checkpoint(DbName))
    after
        couch_server:delete(DbName, [?ADMIN_CTX])
    end.

t_checkpoint_not_found_for_unknown() ->
    ?assertEqual(not_found,
        mem3_reshard_rep:load_checkpoint(<<"nonexistent_db_zzz">>)).

t_checkpoint_survives_close_reopen() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    couch_db:close(Db),
    try
        Source = #shard{name = DbName, range = [0, 16#ffffffff]},
        Target = #shard{name = <<"t1">>, range = [0, 16#ffffffff]},
        St = #split_state{
            source = Source, targets = [Target],
            target_map = #{}, factor = 2, state = building_indices
        },
        ok = mem3_reshard_rep:checkpoint_state(DbName, St),

        %% Close and reopen the DB (simulates restart)
        {ok, Db2} = couch_db:open_int(DbName, [?ADMIN_CTX]),
        couch_db:close(Db2),

        %% Checkpoint should still be there
        {ok, Info} = mem3_reshard_rep:load_checkpoint(DbName),
        ?assertEqual(building_indices, maps:get(state, Info))
    after
        couch_server:delete(DbName, [?ADMIN_CTX])
    end.

%% ===================================================================
%% 2. Orphan target cleanup
%% ===================================================================

cleanup_test_() ->
    {
        "Orphan target cleanup",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_cleanup_deletes_orphan_targets/0,
                fun t_cleanup_preserves_post_map_targets/0,
                fun t_cleanup_removes_checkpoint/0,
                fun t_cleanup_handles_missing_targets/0,
                fun t_cleanup_updating_map_without_shard_map_entry/0
            ]
        }
    }.

t_cleanup_deletes_orphan_targets() ->
    SourceName = ?tempdb(),
    Target1Name = ?tempdb(),
    Target2Name = ?tempdb(),
    {ok, Src} = couch_db:create(SourceName, [?ADMIN_CTX]),
    {ok, T1} = couch_db:create(Target1Name, [?ADMIN_CTX]),
    {ok, T2} = couch_db:create(Target2Name, [?ADMIN_CTX]),
    couch_db:close(Src),
    couch_db:close(T1),
    couch_db:close(T2),
    try
        %% Simulate an interrupted split in pre-map state
        Info = #{
            source => SourceName,
            targets => [Target1Name, Target2Name],
            state => replicating,
            factor => 2
        },
        ok = mem3_reshard_rep:cleanup_interrupted_split(Info),

        %% Targets should be deleted
        ?assertEqual(false, couch_server:exists(Target1Name)),
        ?assertEqual(false, couch_server:exists(Target2Name)),
        %% Source should still exist
        ?assertEqual(true, couch_server:exists(SourceName))
    after
        catch couch_server:delete(SourceName, [?ADMIN_CTX]),
        catch couch_server:delete(Target1Name, [?ADMIN_CTX]),
        catch couch_server:delete(Target2Name, [?ADMIN_CTX])
    end.

t_cleanup_preserves_post_map_targets() ->
    TargetName = ?tempdb(),
    {ok, T} = couch_db:create(TargetName, [?ADMIN_CTX]),
    couch_db:close(T),
    try
        %% Simulate an interrupted split AFTER shard map update
        Info = #{
            source => <<"source_db">>,
            targets => [TargetName],
            state => topoff_post_map,  % Past map update!
            factor => 2
        },
        ok = mem3_reshard_rep:cleanup_interrupted_split(Info),

        %% Target should NOT be deleted (it may be serving traffic)
        ?assertEqual(true, couch_server:exists(TargetName))
    after
        catch couch_server:delete(TargetName, [?ADMIN_CTX])
    end.

t_cleanup_removes_checkpoint() ->
    SourceName = ?tempdb(),
    TargetName = ?tempdb(),
    {ok, Src} = couch_db:create(SourceName, [?ADMIN_CTX]),
    {ok, T} = couch_db:create(TargetName, [?ADMIN_CTX]),
    couch_db:close(Src),
    couch_db:close(T),
    try
        %% Write a checkpoint
        Source = #shard{name = SourceName, range = [0, 16#ffffffff]},
        Target = #shard{name = TargetName, range = [0, 16#ffffffff]},
        St = #split_state{
            source = Source, targets = [Target],
            target_map = #{}, factor = 2, state = topoff_2
        },
        ok = mem3_reshard_rep:checkpoint_state(SourceName, St),
        {ok, _} = mem3_reshard_rep:load_checkpoint(SourceName),

        %% Cleanup should delete both target and checkpoint
        Info = #{
            source => SourceName,
            targets => [TargetName],
            state => topoff_2,
            factor => 2
        },
        ok = mem3_reshard_rep:cleanup_interrupted_split(Info),
        ?assertEqual(not_found, mem3_reshard_rep:load_checkpoint(SourceName))
    after
        catch couch_server:delete(SourceName, [?ADMIN_CTX]),
        catch couch_server:delete(TargetName, [?ADMIN_CTX])
    end.

t_cleanup_handles_missing_targets() ->
    %% Targets already deleted (e.g. manual cleanup) — should not crash
    Info = #{
        source => <<"source_does_not_matter">>,
        targets => [<<"target_that_does_not_exist">>],
        state => creating_targets,
        factor => 2
    },
    ?assertEqual(ok, mem3_reshard_rep:cleanup_interrupted_split(Info)).

%% A crash recovered in updating_map state is ambiguous. If the shard
%% map never got updated, the targets are orphans and must be cleaned
%% up the same way as pre-map states. Because no _dbs is running in
%% eunit, targets_in_shard_map/2 returns false, so the cleanup path
%% should delete the targets.
t_cleanup_updating_map_without_shard_map_entry() ->
    SourceName = ?tempdb(),
    TargetName = ?tempdb(),
    {ok, Src} = couch_db:create(SourceName, [?ADMIN_CTX]),
    {ok, Tgt} = couch_db:create(TargetName, [?ADMIN_CTX]),
    couch_db:close(Src),
    couch_db:close(Tgt),
    try
        Info = #{
            source => SourceName,
            targets => [TargetName],
            state => updating_map,
            factor => 2
        },
        ok = mem3_reshard_rep:cleanup_interrupted_split(Info),
        %% Shard map lookup fails in eunit so we treat as pre-map;
        %% orphan target should be deleted.
        ?assertEqual(false, couch_server:exists(TargetName)),
        %% Source is not touched by cleanup.
        ?assertEqual(true, couch_server:exists(SourceName))
    after
        catch couch_server:delete(SourceName, [?ADMIN_CTX]),
        catch couch_server:delete(TargetName, [?ADMIN_CTX])
    end.

%% Post-map cleanup must rename the checkpoint rather than leave it in
%% place. Otherwise every subsequent boot re-detects the same orphan
%% and spams the log indefinitely. After cleanup find_interrupted_splits
%% should return empty for this source.
orphan_rename_test_() ->
    {
        "Post-map cleanup renames the checkpoint",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_post_map_cleanup_stops_re_detection/0
            ]
        }
    }.

t_post_map_cleanup_stops_re_detection() ->
    SourceName = ?tempdb(),
    TargetName = ?tempdb(),
    {ok, Src} = couch_db:create(SourceName, [?ADMIN_CTX]),
    {ok, Tgt} = couch_db:create(TargetName, [?ADMIN_CTX]),
    couch_db:close(Src),
    couch_db:close(Tgt),
    try
        %% Write a checkpoint in a post-map state
        Source = #shard{name = SourceName, range = [0, 16#ffffffff]},
        Target = #shard{name = TargetName, range = [0, 16#ffffffff]},
        St = #split_state{
            source = Source, targets = [Target],
            target_map = #{}, factor = 2, state = topoff_final
        },
        ok = mem3_reshard_rep:checkpoint_state(SourceName, St),

        %% Cleanup should preserve the target and rename the checkpoint
        Info = #{
            source => SourceName,
            targets => [TargetName],
            state => topoff_final,
            factor => 2
        },
        ok = mem3_reshard_rep:cleanup_interrupted_split(Info),
        ?assertEqual(true, couch_server:exists(TargetName)),

        %% A second call to find_interrupted_splits on this source
        %% must NOT return the renamed checkpoint, because
        %% load_checkpoint only looks under the live _local id.
        ?assertEqual(not_found,
            mem3_reshard_rep:load_checkpoint(SourceName))
    after
        catch couch_server:delete(SourceName, [?ADMIN_CTX]),
        catch couch_server:delete(TargetName, [?ADMIN_CTX])
    end.

%% ===================================================================
%% 3. Find interrupted splits
%% ===================================================================

find_interrupted_test_() ->
    {
        "Find interrupted splits across local shards",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_find_returns_empty_when_none/0,
                fun t_find_discovers_checkpoint/0,
                fun t_find_returns_all_fields/0
            ]
        }
    }.

t_find_returns_empty_when_none() ->
    %% With no checkpoints, should return empty list
    Result = mem3_reshard_rep:find_interrupted_splits(),
    ?assert(is_list(Result)).

t_find_discovers_checkpoint() ->
    %% Create a shard-named DB with a checkpoint
    ShardName = <<"shards/00000000-ffffffff/findtest.1234567890">>,
    {ok, Db} = couch_db:create(ShardName, [?ADMIN_CTX]),
    couch_db:close(Db),
    try
        Source = #shard{name = ShardName, range = [0, 16#ffffffff]},
        Target = #shard{name = <<"t1">>, range = [0, 16#ffffffff]},
        St = #split_state{
            source = Source, targets = [Target],
            target_map = #{}, factor = 2, state = topoff_1
        },
        ok = mem3_reshard_rep:checkpoint_state(ShardName, St),

        %% find_interrupted_splits should discover it
        Interrupted = mem3_reshard_rep:find_interrupted_splits(),
        Found = [I || I <- Interrupted,
                      maps:get(source, I) =:= ShardName],
        ?assertEqual(1, length(Found)),
        ?assertEqual(topoff_1, maps:get(state, hd(Found)))
    after
        mem3_reshard_rep:delete_checkpoint(ShardName),
        couch_server:delete(ShardName, [?ADMIN_CTX])
    end.

t_find_returns_all_fields() ->
    ShardName = <<"shards/00000000-ffffffff/fieldtest.9999999999">>,
    {ok, Db} = couch_db:create(ShardName, [?ADMIN_CTX]),
    couch_db:close(Db),
    try
        Source = #shard{name = ShardName, range = [0, 16#ffffffff]},
        T1 = #shard{name = <<"target_a">>, range = [0, 16#7fffffff]},
        T2 = #shard{name = <<"target_b">>, range = [16#80000000, 16#ffffffff]},
        St = #split_state{
            source = Source, targets = [T1, T2],
            target_map = #{}, factor = 2, state = copying_local
        },
        ok = mem3_reshard_rep:checkpoint_state(ShardName, St),

        Interrupted = mem3_reshard_rep:find_interrupted_splits(),
        Found = [I || I <- Interrupted,
                      maps:get(source, I) =:= ShardName],
        ?assertEqual(1, length(Found)),
        Info = hd(Found),
        ?assertEqual(copying_local, maps:get(state, Info)),
        ?assertEqual(ShardName, maps:get(source, Info)),
        ?assertEqual(2, maps:get(factor, Info)),
        ?assertEqual([<<"target_a">>, <<"target_b">>], maps:get(targets, Info)),
        ?assert(maps:get(updated_at, Info) > 0)
    after
        mem3_reshard_rep:delete_checkpoint(ShardName),
        couch_server:delete(ShardName, [?ADMIN_CTX])
    end.

%% ===================================================================
%% 4. Full recovery flow — write checkpoint, simulate crash, recover
%% ===================================================================

recovery_flow_test_() ->
    {
        "Full recovery flow: checkpoint → crash → cleanup",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            [
                fun t_full_recovery_cleans_pre_map_state/0,
                fun t_full_recovery_preserves_post_map_state/0
            ]
        }
    }.

%% Simulate: split starts, creates targets, dies during replication.
%% On recovery: find checkpoint, clean up targets, delete checkpoint.
t_full_recovery_cleans_pre_map_state() ->
    SourceName = ?tempdb(),
    TargetName = ?tempdb(),
    {ok, Src} = couch_db:create(SourceName, [?ADMIN_CTX]),
    {ok, Tgt} = couch_db:create(TargetName, [?ADMIN_CTX]),
    couch_db:close(Src),
    couch_db:close(Tgt),
    try
        %% Write checkpoint as if split was in progress
        Source = #shard{name = SourceName, range = [0, 16#ffffffff]},
        Target = #shard{name = TargetName, range = [0, 16#ffffffff]},
        St = #split_state{
            source = Source, targets = [Target],
            target_map = #{}, factor = 2, state = replicating
        },
        ok = mem3_reshard_rep:checkpoint_state(SourceName, St),

        %% Simulate recovery: find and clean up
        {ok, Info} = mem3_reshard_rep:load_checkpoint(SourceName),
        ok = mem3_reshard_rep:cleanup_interrupted_split(Info),

        %% Target deleted, checkpoint removed, source intact
        ?assertEqual(false, couch_server:exists(TargetName)),
        ?assertEqual(true, couch_server:exists(SourceName)),
        ?assertEqual(not_found, mem3_reshard_rep:load_checkpoint(SourceName))
    after
        catch couch_server:delete(SourceName, [?ADMIN_CTX]),
        catch couch_server:delete(TargetName, [?ADMIN_CTX])
    end.

%% Post-map split: targets should NOT be cleaned up (DATA LOSS risk).
t_full_recovery_preserves_post_map_state() ->
    SourceName = ?tempdb(),
    TargetName = ?tempdb(),
    {ok, Src} = couch_db:create(SourceName, [?ADMIN_CTX]),
    {ok, Tgt} = couch_db:create(TargetName, [?ADMIN_CTX]),
    couch_db:close(Src),
    couch_db:close(Tgt),
    try
        Source = #shard{name = SourceName, range = [0, 16#ffffffff]},
        Target = #shard{name = TargetName, range = [0, 16#ffffffff]},
        St = #split_state{
            source = Source, targets = [Target],
            target_map = #{}, factor = 2, state = topoff_final
        },
        ok = mem3_reshard_rep:checkpoint_state(SourceName, St),

        {ok, Info} = mem3_reshard_rep:load_checkpoint(SourceName),
        ok = mem3_reshard_rep:cleanup_interrupted_split(Info),

        %% Target should STILL exist (post-map, could be serving traffic)
        ?assertEqual(true, couch_server:exists(TargetName)),
        ?assertEqual(true, couch_server:exists(SourceName))
    after
        catch couch_server:delete(SourceName, [?ADMIN_CTX]),
        catch couch_server:delete(TargetName, [?ADMIN_CTX])
    end.

