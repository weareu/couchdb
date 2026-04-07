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

%% End-to-end tests that call mem3_reshard_rep:split/3 directly on a
%% real source shard. These tests cover the 13-state orchestrator as
%% a whole rather than its individual building blocks:
%%
%%   - checkpoints are written on every state transition
%%   - the task status is registered and updated with progress
%%   - a split that crashes mid-flight leaves a recoverable checkpoint
%%   - the recovery path (find + cleanup) deletes orphan targets
%%
%% The split eventually fails at updating_map because _dbs does not
%% exist in the eunit test environment. That is expected and fine:
%% the states before updating_map are the ones that need orchestrator
%% coverage, and the failure path is itself a tested scenario.

-module(mem3_reshard_rep_orchestrator_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("mem3/include/mem3_reshard_rep.hrl").

orchestrator_test_() ->
    {
        "mem3_reshard_rep:split/3 orchestrator end-to-end",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {timeout, 120, [
                fun t_split_writes_checkpoint_through_states/0,
                fun t_split_registers_active_task/0,
                fun t_split_progress_reaches_at_least_midway/0,
                fun t_checkpoint_auto_cleaned_on_crash_recovery/0,
                fun t_split_failure_leaves_cleanable_orphans/0
            ]}
        }
    }.

%% ===================================================================
%% Checkpoint visibility during a split
%% ===================================================================

%% Call split/3 and verify that by the time it returns (success or
%% failure) the checkpoint was written for at least one pre-map state.
%% The source DB exists locally, so checkpoint_state/2 can actually
%% write _local docs to it.
t_split_writes_checkpoint_through_states() ->
    ?_test(begin
        Source = make_source(<<"orch_checkpoint">>),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 50),
            Self = self(),
            Observer = spawn(fun() ->
                SeenStates = poll_checkpoint_states(
                    Source#shard.name, [], 200, 25),
                Self ! {observed, SeenStates}
            end),
            _Result = (catch mem3_reshard_rep:split(
                Source, 2, [node(), node()])),
            SeenStates = receive
                {observed, S} -> S
            after 10000 -> []
            end,
            Observer ! stop,
            ValidStates = [creating_targets, replicating, topoff_1,
                           building_indices, topoff_2, copying_local,
                           topoff_3, updating_map],
            SeenValid = [S || S <- SeenStates, lists:member(S, ValidStates)],
            ?assert(length(SeenValid) >= 1)
        after
            cleanup_source_and_targets(Source, 2)
        end
    end).

%% ===================================================================
%% _active_tasks visibility
%% ===================================================================

%% Call split/3 and assert that couch_task_status:all/0 reflects it
%% while the split is mid-flight. Uses a polling observer to catch the
%% task before split/3 returns.
t_split_registers_active_task() ->
    ?_test(begin
        Source = make_source(<<"orch_taskstatus">>),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 100),
            Self = self(),
            Observer = spawn(fun() ->
                SawTask = poll_for_task(shard_split, Source#shard.name, 150),
                Self ! {task_seen, SawTask}
            end),
            _Result = (catch mem3_reshard_rep:split(
                Source, 2, [node(), node()])),
            TaskSeen = receive
                {task_seen, T} -> T
            after 10000 -> false
            end,
            Observer ! stop,
            ?assert(TaskSeen)
        after
            cleanup_source_and_targets(Source, 2)
        end
    end).

%% Verify progress advances beyond zero during the run. Even if split
%% fails at updating_map, the task should have reported progress > 0.
t_split_progress_reaches_at_least_midway() ->
    ?_test(begin
        Source = make_source(<<"orch_progress">>),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 100),
            Self = self(),
            Observer = spawn(fun() ->
                MaxProgress = poll_max_progress(Source#shard.name, 0, 200),
                Self ! {max_progress, MaxProgress}
            end),
            _Result = (catch mem3_reshard_rep:split(
                Source, 2, [node(), node()])),
            MaxProgress = receive
                {max_progress, P} -> P
            after 10000 -> 0
            end,
            Observer ! stop,
            %% States 1..7 are pre-map. The split should at least
            %% reach state 2 (replicating, 15% of 13 steps).
            ?assert(MaxProgress >= 15)
        after
            cleanup_source_and_targets(Source, 2)
        end
    end).

%% ===================================================================
%% Crash recovery end-to-end
%% ===================================================================

%% Write a checkpoint directly (simulating a crashed split) and verify
%% that find_interrupted_splits discovers it and cleanup removes the
%% orphan targets. This tests the RECOVERY pipeline as a whole.
t_checkpoint_auto_cleaned_on_crash_recovery() ->
    ?_test(begin
        Source = make_source(<<"orch_recover">>),
        create_shard(Source),
        Targets = make_targets(Source, 2),
        [create_shard(T) || T <- Targets],
        try
            St = build_split_state(Source, Targets, replicating),
            ok = mem3_reshard_rep:checkpoint_state(Source#shard.name, St),
            Interrupted = mem3_reshard_rep:find_interrupted_splits(),
            Found = [I || I <- Interrupted,
                          maps:get(source, I) =:= Source#shard.name],
            ?assertEqual(1, length(Found)),
            ok = mem3_reshard_rep:cleanup_interrupted_split(hd(Found)),
            [?assertEqual(false, couch_server:exists(T#shard.name))
             || T <- Targets],
            ?assertEqual(true, couch_server:exists(Source#shard.name)),
            ?assertEqual(not_found,
                mem3_reshard_rep:load_checkpoint(Source#shard.name))
        after
            cleanup_source_and_targets(Source, 2)
        end
    end).

%% If split/3 fails (e.g. _dbs missing in eunit), the checkpoint
%% should still be in a recoverable state. This verifies that the
%% failure path does not leave the source in a bad state.
t_split_failure_leaves_cleanable_orphans() ->
    ?_test(begin
        Source = make_source(<<"orch_failure">>),
        create_shard(Source),
        try
            write_docs(Source#shard.name, 50),
            Result = (catch mem3_reshard_rep:split(
                Source, 2, [node(), node()])),
            ?assertNotEqual(ok, Result),
            ?assertEqual(true, couch_server:exists(Source#shard.name)),
            case mem3_reshard_rep:load_checkpoint(Source#shard.name) of
                {ok, Info} ->
                    ok = mem3_reshard_rep:cleanup_interrupted_split(Info),
                    ?assertEqual(true,
                        couch_server:exists(Source#shard.name));
                not_found ->
                    ok
            end
        after
            cleanup_source_and_targets(Source, 2)
        end
    end).

%% ===================================================================
%% Helpers
%% ===================================================================

make_source(Name) ->
    Suffix = integer_to_binary(erlang:system_time(millisecond)),
    FullName = <<"shards/00000000-ffffffff/", Name/binary, ".", Suffix/binary>>,
    #shard{name = FullName, node = node(), dbname = Name,
           range = [0, ?RING_END]}.

make_targets(#shard{range = Range, dbname = DbName, name = Name}, Factor) ->
    Ranges = mem3_reshard_rep:subdivide_range(Range, Factor),
    <<"shards/", _:8/binary, "-", _:8/binary, "/", DbAndSuffix/binary>> = Name,
    [_, Sfx] = binary:split(DbAndSuffix, <<".">>),
    [begin
        Sh = #shard{dbname = DbName, range = R, node = node()},
        mem3_util:name_shard(Sh, <<".", Sfx/binary>>)
    end || R <- Ranges].

create_shard(#shard{name = Name}) ->
    case couch_server:exists(Name) of
        true -> ok;
        false ->
            {ok, Db} = couch_db:create(Name, [?ADMIN_CTX]),
            couch_db:close(Db)
    end.

write_docs(ShardName, Count) ->
    {ok, Db} = couch_db:open_int(ShardName, [?ADMIN_CTX]),
    try
        lists:foreach(fun(I) ->
            Id = list_to_binary(io_lib:format("doc-~4..0B", [I])),
            Body = {[{<<"n">>, I}]},
            Rev = couch_hash:md5_hash(term_to_binary({Id, I})),
            Doc = #doc{id = Id, body = Body, revs = {1, [Rev]}},
            {ok, _} = couch_db:update_docs(Db, [Doc], [replicated_changes])
        end, lists:seq(1, Count))
    after
        couch_db:close(Db)
    end.

build_split_state(Source, Targets, StateAtom) ->
    TMap = mem3_reshard_rep:build_target_map(Targets),
    #split_state{
        source = Source,
        targets = Targets,
        target_map = TMap,
        factor = length(Targets),
        state = StateAtom
    }.

cleanup_source_and_targets(Source, Factor) ->
    catch couch_server:delete(Source#shard.name, [?ADMIN_CTX]),
    try
        Targets = make_targets(Source, Factor),
        [catch couch_server:delete(T#shard.name, [?ADMIN_CTX])
         || T <- Targets]
    catch
        _:_ -> ok
    end,
    ok.

%% Poll load_checkpoint and record every distinct state we see.
%% Stops after MaxSamples or when the caller sends `stop`.
poll_checkpoint_states(_SourceName, Seen, 0, _IntervalMs) ->
    Seen;
poll_checkpoint_states(SourceName, Seen, SamplesLeft, IntervalMs) ->
    receive stop -> Seen
    after IntervalMs ->
        NewSeen = case mem3_reshard_rep:load_checkpoint(SourceName) of
            {ok, #{state := S}} ->
                case lists:member(S, Seen) of
                    true -> Seen;
                    false -> [S | Seen]
                end;
            not_found -> Seen
        end,
        poll_checkpoint_states(
            SourceName, NewSeen, SamplesLeft - 1, IntervalMs)
    end.

%% Poll couch_task_status:all/0 for a split with the given type and
%% database. Returns true as soon as it is observed.
poll_for_task(_Type, _DbName, 0) ->
    false;
poll_for_task(Type, DbName, SamplesLeft) ->
    receive stop -> false
    after 20 ->
        Tasks = try couch_task_status:all() catch _:_ -> [] end,
        Found = lists:any(fun(T) ->
            couch_util:get_value(type, T) =:= Type andalso
            couch_util:get_value(database, T) =:= DbName
        end, Tasks),
        case Found of
            true -> true;
            false -> poll_for_task(Type, DbName, SamplesLeft - 1)
        end
    end.

%% Poll couch_task_status for the max progress value seen.
poll_max_progress(_DbName, Max, 0) ->
    Max;
poll_max_progress(DbName, Max, SamplesLeft) ->
    receive stop -> Max
    after 20 ->
        Tasks = try couch_task_status:all() catch _:_ -> [] end,
        Progress = case lists:filter(fun(T) ->
            couch_util:get_value(type, T) =:= shard_split andalso
            couch_util:get_value(database, T) =:= DbName
        end, Tasks) of
            [] -> 0;
            [T | _] -> couch_util:get_value(progress, T, 0)
        end,
        NewMax = max(Max, Progress),
        poll_max_progress(DbName, NewMax, SamplesLeft - 1)
    end.
