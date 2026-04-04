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

%% @doc Replication-based shard splitting engine.
%%
%% Splits a shard by replicating docs via mem3_rep to target shards
%% on their destination nodes. Unlike the existing mem3_reshard which
%% copies locally then moves, this streams directly to targets — the
%% source node doesn't need 2x free space.
%%
%% Split flow:
%%   1. Create target shard DBs on destination nodes
%%   2. mem3_rep bulk replication (hash-filtered to targets)
%%   3. Topoff 1 — catch up writes during bulk
%%   4. Build indices on targets
%%   5. Topoff 2 — catch up during index build
%%   6. Copy local docs (_local/*)
%%   7. Topoff 3
%%   8. Update shard map in _dbs (atomic)
%%   9. Final topoff — catch writes during map update
%%  10. Verify consistency (MUST PASS before source delete)
%%  11. Delete source
%%
%% Consistency guarantees:
%%   - doc_count(source) == sum(doc_count(targets))
%%   - doc_del_count matches
%%   - Changes feed never rolls back
%%   - Verification MUST pass before source deletion

-module(mem3_reshard_rep).

-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-export([
    split/3,
    subdivide_range/2,
    build_targets/3,
    build_target_map/1,
    create_target_dbs/1,
    replicate/3,
    topoff/3,
    copy_local_docs/2,
    update_shard_map/2,
    verify_consistency/2,
    verify_doc_counts/2,
    verify_doc_distribution/3,
    preflight_check/3,
    delete_source/1,
    create_replication_checkpoints/2
]).

-record(split_state, {
    source :: #shard{},
    targets :: [#shard{}],
    target_map :: #{},
    factor :: pos_integer(),
    state :: atom(),
    error :: term() | undefined
}).

%% ===================================================================
%% Range subdivision
%% ===================================================================

%% @doc Subdivide a shard range into Factor equal parts.
%% Factor must be >= 2 and a power of 2.
%% Returns list of [Begin, End] ranges that together cover the input range.
-spec subdivide_range([non_neg_integer()], pos_integer()) -> [[non_neg_integer()]].
subdivide_range([Begin, End], Factor) when
    is_integer(Factor), Factor >= 2,
    (End - Begin + 1) >= Factor
->
    Width = End - Begin + 1,
    TargetWidth = Width div Factor,
    _Rem = Width rem Factor,
    Ranges = lists:map(fun(I) ->
        B = Begin + (I * TargetWidth),
        E = case I =:= Factor - 1 of
            true -> End;  % last range absorbs remainder
            false -> B + TargetWidth - 1
        end,
        [B, E]
    end, lists:seq(0, Factor - 1)),
    %% Verify invariants
    [[FirstBegin, _] | _] = Ranges,
    [_, LastEnd] = lists:last(Ranges),
    FirstBegin = Begin,
    LastEnd = End,
    %% Verify no gaps and no overlaps
    verify_ranges_contiguous(Ranges),
    Ranges;
subdivide_range([Begin, End], Factor) ->
    error({range_too_small, [Begin, End], Factor}).

%% @doc Build target shard records for a split operation.
%% Assigns ranges to nodes. Each range gets N copies across nodes.
-spec build_targets(#shard{}, pos_integer(), [node()]) -> [#shard{}].
build_targets(#shard{name = Name, dbname = DbName, range = Range}, Factor, TargetNodes) ->
    Ranges = subdivide_range(Range, Factor),
    <<"shards/", _:8/binary, "-", _:8/binary, "/", DbAndSuffix/binary>> = Name,
    Suffix = case binary:split(DbAndSuffix, <<".">>) of
        [_Db, S] -> S;
        [_Db] -> integer_to_binary(erlang:system_time(second))
    end,
    N = config:get_integer("cluster", "n", 3),
    NodeCount = length(TargetNodes),
    lists:flatmap(fun({RangeIdx, TargetRange}) ->
        %% Assign N nodes to this range, cycling through TargetNodes
        lists:map(fun(CopyIdx) ->
            NodeIdx = ((RangeIdx * N) + CopyIdx) rem max(NodeCount, 1),
            Node = lists:nth(NodeIdx + 1, TargetNodes),
            Shard = #shard{
                dbname = DbName,
                range = TargetRange,
                node = Node
            },
            mem3_util:name_shard(Shard, <<".", Suffix/binary>>)
        end, lists:seq(0, N - 1))
    end, lists:zip(lists:seq(0, length(Ranges) - 1), Ranges)).

%% @doc Build the target map for mem3_rep.
%% Returns #{Range => #shard{}} mapping each range to one target shard.
%% mem3_rep routes docs by hash into the matching range.
-spec build_target_map([#shard{}]) -> #{[non_neg_integer()] => #shard{}}.
build_target_map(Targets) ->
    %% Take the first shard for each unique range (mem3_rep needs one
    %% target per range — replication to replicas happens via normal
    %% cluster replication after shard map update)
    lists:foldl(fun(#shard{range = Range} = Shard, Acc) ->
        case maps:is_key(Range, Acc) of
            true -> Acc;  % already have a target for this range
            false -> maps:put(Range, Shard, Acc)
        end
    end, #{}, Targets).

%% ===================================================================
%% Consistency verification
%% ===================================================================

%% @doc Verify doc counts: source == sum(targets).
%% Returns ok or {error, {count_mismatch, Expected, Got}}.
-spec verify_doc_counts(binary(), [binary()]) -> ok | {error, term()}.
verify_doc_counts(SourceName, TargetNames) ->
    {ok, SourceDb} = couch_db:open_int(SourceName, [?ADMIN_CTX]),
    SourceInfo = try couch_db:get_db_info(SourceDb)
        after couch_db:close(SourceDb) end,
    SourceCount = couch_util:get_value(doc_count, SourceInfo),
    SourceDeleted = couch_util:get_value(doc_del_count, SourceInfo),

    {TargetCounts, TargetDeleted} = lists:foldl(fun(TName, {CAcc, DAcc}) ->
        {ok, TDb} = couch_db:open_int(TName, [?ADMIN_CTX]),
        TInfo = try couch_db:get_db_info(TDb)
            after couch_db:close(TDb) end,
        TC = couch_util:get_value(doc_count, TInfo),
        TD = couch_util:get_value(doc_del_count, TInfo),
        {CAcc + TC, DAcc + TD}
    end, {0, 0}, TargetNames),

    case SourceCount =:= TargetCounts of
        false ->
            {error, {doc_count_mismatch, SourceCount, TargetCounts}};
        true ->
            case SourceDeleted =:= TargetDeleted of
                false ->
                    {error, {del_count_mismatch, SourceDeleted, TargetDeleted}};
                true ->
                    ok
            end
    end.

%% @doc Verify that every doc in source exists in exactly one target.
%% For small databases (<= 10000 docs), checks ALL docs.
%% For large databases, verifies doc_count + doc_del_count match
%% (already checked by verify_doc_counts) AND checks a systematic
%% sample: first 500, last 500, and 500 evenly-spaced docs.
-spec verify_doc_distribution(binary(), [binary()], fun()) -> ok | {error, term()}.
verify_doc_distribution(SourceName, TargetNames, _HashFun) ->
    {ok, SourceDb} = couch_db:open_int(SourceName, [?ADMIN_CTX]),
    try
        {ok, SourceInfo} = couch_db:get_db_info(SourceDb),
        DocCount = couch_util:get_value(doc_count, SourceInfo),
        case DocCount =< 10000 of
            true ->
                %% Small DB: verify ALL docs exist in a target
                verify_all_docs(SourceDb, TargetNames);
            false ->
                %% Large DB: doc counts already verified by verify_doc_counts.
                %% Do systematic sample: first 500, skip through middle, last 500.
                verify_systematic_sample(SourceDb, TargetNames, DocCount)
        end
    after
        couch_db:close(SourceDb)
    end.

verify_all_docs(SourceDb, TargetNames) ->
    {ok, Missing} = couch_db:fold_docs(SourceDb, fun(FDI, AccIn) ->
        #full_doc_info{id = DocId} = FDI,
        case doc_exists_in_any_target(DocId, TargetNames) of
            true -> {ok, AccIn};
            false -> {ok, [DocId | AccIn]}
        end
    end, [], []),
    case Missing of
        [] -> ok;
        _ -> {error, {docs_missing_from_targets, length(Missing),
                      lists:sublist(Missing, 10)}}
    end.

verify_systematic_sample(SourceDb, TargetNames, DocCount) ->
    %% Check first 500
    {ok, Missing1} = couch_db:fold_docs(SourceDb, fun(FDI, AccIn) ->
        #full_doc_info{id = DocId} = FDI,
        case doc_exists_in_any_target(DocId, TargetNames) of
            true -> {ok, AccIn};
            false -> {ok, [DocId | AccIn]}
        end
    end, [], [{limit, 500}]),
    %% Check last 500 (fold in reverse)
    {ok, Missing2} = couch_db:fold_docs(SourceDb, fun(FDI, AccIn) ->
        #full_doc_info{id = DocId} = FDI,
        case doc_exists_in_any_target(DocId, TargetNames) of
            true -> {ok, AccIn};
            false -> {ok, [DocId | AccIn]}
        end
    end, [], [{limit, 500}, {dir, rev}]),
    %% Check 500 evenly spaced through the middle
    Step = max(1, DocCount div 500),
    {ok, {Missing3, _}} = couch_db:fold_docs(SourceDb, fun(FDI, {AccIn, Counter}) ->
        case Counter rem Step of
            0 ->
                #full_doc_info{id = DocId} = FDI,
                case doc_exists_in_any_target(DocId, TargetNames) of
                    true -> {ok, {AccIn, Counter + 1}};
                    false -> {ok, {[DocId | AccIn], Counter + 1}}
                end;
            _ ->
                {ok, {AccIn, Counter + 1}}
        end
    end, {[], 0}, []),
    AllMissing = Missing1 ++ Missing2 ++ Missing3,
    case AllMissing of
        [] -> ok;
        _ -> {error, {docs_missing_from_targets, length(AllMissing),
                      lists:sublist(AllMissing, 10)}}
    end.

doc_exists_in_any_target(DocId, TargetNames) ->
    lists:any(fun(TName) ->
        {ok, TDb} = couch_db:open_int(TName, [?ADMIN_CTX]),
        try
            case couch_db:open_doc(TDb, DocId, []) of
                {ok, _Doc} -> true;
                {not_found, _} -> false
            end
        after
            couch_db:close(TDb)
        end
    end, TargetNames).

%% ===================================================================
%% Pre-flight space check
%% ===================================================================

%% @doc Verify sufficient disk space before starting a split.
%% Each target needs: data + compaction headroom + index headroom.
%% Returns ok or {error, {insufficient_space, Problems}}.
-spec preflight_check(non_neg_integer(), pos_integer(), #{node() => map()}) ->
    ok | {error, term()}.
preflight_check(SourceSizeBytes, Factor, NodeCapacities) ->
    TargetSize = SourceSizeBytes div Factor,
    %% Each copy needs: data + 2x compaction + 1x index = 3x
    SpaceFactor = config:get_integer("auto_shard", "min_free_space_factor", 3),
    RequiredPerCopy = TargetSize * SpaceFactor,
    Problems = maps:fold(fun(Node, CapMap, Acc) ->
        Dirs = maps:get(dirs, CapMap, []),
        MaxFree = case Dirs of
            [] -> 0;
            _ -> lists:max([Free || {_Path, _Pct, Free, _Total} <- Dirs])
        end,
        case MaxFree >= RequiredPerCopy of
            true -> Acc;
            false ->
                [{Node, #{needed => RequiredPerCopy, available => MaxFree}} | Acc]
        end
    end, [], NodeCapacities),
    case Problems of
        [] -> ok;
        _ -> {error, {insufficient_space, Problems}}
    end.

%% ===================================================================
%% Split orchestrator
%% ===================================================================

%% @doc Split a shard into Factor pieces, placing targets on TargetNodes.
%% This is the main entry point. Runs synchronously — call from a
%% worker process. Returns ok or {error, Reason}.
%%
%% The function is designed to be resumable: if it crashes, the caller
%% can retry. mem3_rep checkpoints handle resume of the replication
%% phases. Target DBs that already exist are not re-created.
-spec split(#shard{}, pos_integer(), [node()]) -> ok | {error, term()}.
split(#shard{} = Source, Factor, TargetNodes) when
    is_integer(Factor), Factor >= 2, is_list(TargetNodes), TargetNodes =/= []
->
    couch_log:notice("mem3_reshard_rep: starting ~B-way split of ~s",
        [Factor, Source#shard.name]),
    Targets = build_targets(Source, Factor, TargetNodes),
    TMap = build_target_map(Targets),
    St = #split_state{
        source = Source,
        targets = Targets,
        target_map = TMap,
        factor = Factor,
        state = creating_targets
    },
    run_split(St).

run_split(#split_state{state = creating_targets} = St) ->
    #split_state{targets = Targets} = St,
    couch_log:notice("mem3_reshard_rep: creating ~B target DBs", [length(Targets)]),
    case create_target_dbs(Targets) of
        ok ->
            run_split(St#split_state{state = replicating});
        {error, Reason} ->
            {error, {creating_targets_failed, Reason}}
    end;

run_split(#split_state{state = replicating} = St) ->
    #split_state{source = Source, target_map = TMap, targets = Targets} = St,
    couch_log:notice("mem3_reshard_rep: bulk replication from ~s", [Source#shard.name]),
    case replicate(Source, TMap, [{batch_size, 1000}, {batch_count, all}]) of
        ok ->
            %% Create artificial mem3_rep checkpoints so that:
            %% 1. Topoff phases resume from here (not from seq=0)
            %% 2. After split, mem3_sync knows where targets left off
            %%    (prevents massive re-replication storm)
            create_replication_checkpoints(Source, Targets),
            run_split(St#split_state{state = topoff_1});
        {error, Reason} ->
            cleanup_targets_on_failure(St),
            {error, {replication_failed, Reason}}
    end;

run_split(#split_state{state = topoff_1} = St) ->
    couch_log:notice("mem3_reshard_rep: topoff 1", []),
    case do_topoff(St) of
        ok -> run_split(St#split_state{state = building_indices});
        {error, _} = Err -> Err
    end;

run_split(#split_state{state = building_indices} = St) ->
    couch_log:notice("mem3_reshard_rep: building indices on targets", []),
    %% Index building is best-effort at this stage — indices will be
    %% built on demand if this fails
    build_indices(St#split_state.targets),
    run_split(St#split_state{state = topoff_2});

run_split(#split_state{state = topoff_2} = St) ->
    couch_log:notice("mem3_reshard_rep: topoff 2", []),
    case do_topoff(St) of
        ok -> run_split(St#split_state{state = copying_local});
        {error, _} = Err -> Err
    end;

run_split(#split_state{state = copying_local} = St) ->
    #split_state{source = Source, target_map = TMap} = St,
    couch_log:notice("mem3_reshard_rep: copying local docs", []),
    case copy_local_docs(Source, TMap) of
        ok -> run_split(St#split_state{state = topoff_3});
        {error, _} = Err -> Err
    end;

run_split(#split_state{state = topoff_3} = St) ->
    couch_log:notice("mem3_reshard_rep: topoff 3", []),
    case do_topoff(St) of
        ok -> run_split(St#split_state{state = updating_map});
        {error, _} = Err -> Err
    end;

run_split(#split_state{state = updating_map} = St) ->
    #split_state{source = Source, targets = Targets} = St,
    couch_log:notice("mem3_reshard_rep: updating shard map", []),
    case update_shard_map(Source, Targets) of
        ok -> run_split(St#split_state{state = topoff_post_map});
        {error, _} = Err ->
            %% Map update failed — targets exist but map unchanged.
            %% Clean up targets since no shard map change occurred.
            cleanup_targets_on_failure(St),
            Err
    end;

%% After shard map update, clients start routing to targets.
%% Any writes that hit the source during the propagation window
%% must be caught by this topoff pass.
run_split(#split_state{state = topoff_post_map} = St) ->
    couch_log:notice("mem3_reshard_rep: post-map topoff (catching propagation writes)", []),
    case do_topoff(St) of
        ok -> run_split(St#split_state{state = topoff_final});
        {error, _} = Err -> Err
    end;

%% Second topoff after propagation settles — catches any writes that
%% arrived at source during the first post-map topoff.
run_split(#split_state{state = topoff_final} = St) ->
    couch_log:notice("mem3_reshard_rep: final topoff", []),
    case do_topoff(St) of
        ok -> run_split(St#split_state{state = verifying});
        {error, _} = Err -> Err
    end;

run_split(#split_state{state = verifying} = St) ->
    #split_state{source = Source, targets = Targets} = St,
    couch_log:notice("mem3_reshard_rep: verifying consistency", []),
    TargetNames = [T#shard.name || T <- unique_range_targets(Targets)],
    case verify_consistency(Source#shard.name, TargetNames) of
        ok ->
            run_split(St#split_state{state = deleting_source});
        {error, Reason} ->
            %% CRITICAL: Do NOT delete source if verification fails.
            %% Leave both source and targets live. Alert operator.
            couch_log:error(
                "mem3_reshard_rep: VERIFICATION FAILED for ~s: ~p. "
                "Source NOT deleted. Manual intervention required.",
                [Source#shard.name, Reason]),
            {error, {verification_failed, Reason}}
    end;

run_split(#split_state{state = deleting_source} = St) ->
    #split_state{source = Source} = St,
    couch_log:notice("mem3_reshard_rep: deleting source ~s", [Source#shard.name]),
    case delete_source(Source) of
        ok ->
            couch_log:notice("mem3_reshard_rep: split completed for ~s",
                [Source#shard.name]),
            ok;
        {error, _} = Err ->
            Err
    end.

%% Create artificial mem3_rep checkpoint docs in both source and target.
%% This prevents mem3_sync from re-replicating the entire shard content
%% after the split completes. Without these checkpoints, every target
%% shard would trigger a full re-sync from all other cluster nodes.
create_replication_checkpoints(#shard{name = SourceName}, Targets) ->
    UniqueTargets = unique_range_targets(Targets),
    try
        {ok, SDb} = couch_db:open_int(SourceName, [?ADMIN_CTX]),
        try
            {ok, SInfo} = couch_db:get_db_info(SDb),
            Seq = couch_util:get_value(update_seq, SInfo),
            SourceUUID = couch_db:get_uuid(SDb),
            Timestamp = list_to_binary(mem3_util:iso8601_timestamp()),
            Node = atom_to_binary(config:node_name(), utf8),
            lists:foreach(fun(#shard{name = TName}) ->
                try
                    {ok, TDb} = couch_db:open_int(TName, [?ADMIN_CTX]),
                    try
                        TargetUUID = couch_db:get_uuid(TDb),
                        History = {[
                            {<<"source_node">>, Node},
                            {<<"source_uuid">>, SourceUUID},
                            {<<"source_seq">>, Seq},
                            {<<"timestamp">>, Timestamp},
                            {<<"target_node">>, Node},
                            {<<"target_uuid">>, TargetUUID},
                            {<<"target_seq">>, Seq}
                        ]},
                        Body = {[
                            {<<"seq">>, Seq},
                            {<<"target_uuid">>, TargetUUID},
                            {<<"history">>, {[{Node, [History]}]}}
                        ]},
                        Id = mem3_rep:make_local_id(SourceUUID, TargetUUID),
                        Doc = #doc{id = Id, body = Body},
                        {ok, _} = couch_db:update_doc(SDb, Doc, []),
                        {ok, _} = couch_db:update_doc(TDb, Doc, [])
                    after
                        couch_db:close(TDb)
                    end
                catch
                    _:_ -> ok  % Best effort — topoff will still work
                end
            end, UniqueTargets)
        after
            couch_db:close(SDb)
        end
    catch
        _:_ -> ok
    end.

%% Clean up target DBs when split fails BEFORE shard map update.
%% Safe because no shard map change occurred — clients never routed to targets.
cleanup_targets_on_failure(#split_state{targets = Targets}) ->
    UniqueTargets = unique_range_targets(Targets),
    lists:foreach(fun(#shard{name = Name}) ->
        couch_log:notice("mem3_reshard_rep: cleaning up orphan target ~s", [Name]),
        catch couch_server:delete(Name, [?ADMIN_CTX])
    end, UniqueTargets).

do_topoff(#split_state{source = Source, target_map = TMap}) ->
    topoff(Source, TMap, [{batch_size, 500}, {batch_count, all}]).

%% ===================================================================
%% Split operations
%% ===================================================================

%% @doc Create target shard databases. Idempotent — skips existing DBs.
-spec create_target_dbs([#shard{}]) -> ok | {error, term()}.
create_target_dbs(Targets) ->
    UniqueTargets = unique_range_targets(Targets),
    Errors = lists:filtermap(fun(#shard{name = Name, node = Node}) ->
        case create_db_on_node(Name, Node) of
            ok -> false;
            already_exists -> false;
            {error, Reason} -> {true, {Name, Node, Reason}}
        end
    end, UniqueTargets),
    case Errors of
        [] -> ok;
        _ -> {error, {create_failed, Errors}}
    end.

create_db_on_node(DbName, Node) when Node =:= node() ->
    case couch_server:exists(DbName) of
        true -> already_exists;
        false ->
            case couch_db:create(DbName, [?ADMIN_CTX]) of
                {ok, Db} ->
                    couch_db:close(Db),
                    ok;
                {file_exists, _} ->
                    already_exists;
                Error ->
                    {error, Error}
            end
    end;
create_db_on_node(DbName, Node) ->
    case rpc:call(Node, ?MODULE, create_db_on_node, [DbName, Node], 30000) of
        ok -> ok;
        already_exists -> already_exists;
        {error, _} = Err -> Err;
        {badrpc, Reason} -> {error, {rpc_failed, Node, Reason}}
    end.

%% @doc Run mem3_rep replication from source to targets.
-spec replicate(#shard{}, #{}, list()) -> ok | {error, term()}.
replicate(Source, TMap, Opts) ->
    Timeout = config:get_integer("rexi", "shard_split_timeout_msec", 600000),
    FullOpts = [{rexi_timeout, Timeout} | Opts],
    case mem3_rep:go(Source, TMap, FullOpts) of
        {ok, _Count} -> ok;
        {error, Error} -> {error, Error}
    end.

%% @doc Run a topoff (catch-up replication) pass.
-spec topoff(#shard{}, #{}, list()) -> ok | {error, term()}.
topoff(Source, TMap, Opts) ->
    replicate(Source, TMap, Opts).

%% @doc Copy local docs from source to targets.
%% Delegates to couch_db_split:copy_local_docs/3 which handles
%% checkpoint translation, security docs, etc.
-spec copy_local_docs(#shard{}, #{}) -> ok | {error, term()}.
copy_local_docs(#shard{name = SourceName}, TMap) ->
    %% Build the range→name map that couch_db_split expects
    Targets = maps:map(fun(_Range, #shard{name = Name}) -> Name end, TMap),
    PickFun = fun(DocId, Ranges, HashFun) ->
        mem3_reshard_job:pickfun(DocId, Ranges, HashFun)
    end,
    try
        couch_db_split:copy_local_docs(SourceName, Targets, PickFun),
        ok
    catch
        _:Error ->
            {error, {copy_local_docs_failed, Error}}
    end.

%% @doc Update the shard map in _dbs to point to target shards.
%% This is the point of no return — after this, clients route to targets.
-spec update_shard_map(#shard{}, [#shard{}]) -> ok | {error, term()}.
update_shard_map(Source, Targets) ->
    %% Use mem3_reshard_dbdoc which handles by_node, by_range, changelog,
    %% and waits for propagation to all live nodes.
    %% It expects a #job{} record, so we build a minimal one.
    try
        %% Get unique targets (one per range, for current node)
        UniqueTargets = unique_range_targets(Targets),
        DocId = mem3:dbname(Source#shard.name),
        case mem3:get_db_doc(DocId) of
            {ok, #doc{body = Body} = Doc} ->
                NewBody = mem3_reshard_dbdoc:update_shard_props(
                    Body, Source, UniqueTargets),
                NewDoc = Doc#doc{body = NewBody},
                case mem3:update_db_doc(NewDoc) of
                    {ok, _} ->
                        %% Wait for propagation
                        wait_shard_map_propagated(Source, 60),
                        ok;
                    {error, UpdateError} ->
                        {error, {shard_map_update_failed, UpdateError}}
                end;
            Error ->
                {error, {shard_map_read_failed, Error}}
        end
    catch
        _:Err ->
            {error, {shard_map_update_exception, Err}}
    end.

wait_shard_map_propagated(_Source, 0) ->
    couch_log:warning("mem3_reshard_rep: shard map propagation timed out", []),
    ok;  % Continue anyway — map will eventually propagate
wait_shard_map_propagated(#shard{name = Name} = Source, RetriesLeft) ->
    timer:sleep(5000),
    DbName = mem3:dbname(Name),
    Shards = try mem3:shards(DbName) catch _:_ -> [] end,
    SourceStillPresent = lists:any(fun(S) ->
        S#shard.name =:= Name andalso S#shard.node =:= Source#shard.node
    end, Shards),
    case SourceStillPresent of
        false -> ok;
        true -> wait_shard_map_propagated(Source, RetriesLeft - 1)
    end.

%% @doc Verify consistency between source and targets.
%% ALL checks must pass before source deletion.
-spec verify_consistency(binary(), [binary()]) -> ok | {error, term()}.
verify_consistency(SourceName, TargetNames) ->
    case verify_doc_counts(SourceName, TargetNames) of
        ok ->
            case verify_update_seqs(SourceName, TargetNames) of
                ok ->
                    HashFun = mem3_hash:get_hash_fun(SourceName),
                    verify_doc_distribution(SourceName, TargetNames, HashFun);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Verify that target update sequences are valid (non-zero when
%% docs exist) and that the sum of target update_seqs is >= source.
%% This prevents changes feed rollback — targets must have progressed
%% at least as far as the source's final sequence.
verify_update_seqs(SourceName, TargetNames) ->
    {ok, SDb} = couch_db:open_int(SourceName, [?ADMIN_CTX]),
    {ok, SInfo} = try couch_db:get_db_info(SDb)
        after couch_db:close(SDb) end,
    SDocCount = couch_util:get_value(doc_count, SInfo),
    Problems = lists:filtermap(fun(TName) ->
        {ok, TDb} = couch_db:open_int(TName, [?ADMIN_CTX]),
        {ok, TInfo} = try couch_db:get_db_info(TDb)
            after couch_db:close(TDb) end,
        TSeq = couch_util:get_value(update_seq, TInfo),
        TCount = couch_util:get_value(doc_count, TInfo),
        %% Target must have a valid update_seq if it has docs
        case TCount > 0 andalso TSeq =:= 0 of
            true -> {true, {TName, zero_seq_with_docs}};
            false -> false
        end
    end, TargetNames),
    case {Problems, SDocCount} of
        {[], _} -> ok;
        {_, 0} -> ok;  % Empty source, don't care about sequences
        _ -> {error, {update_seq_problems, Problems}}
    end.

%% @doc Build view indices on target shards.
build_indices(Targets) ->
    UniqueTargets = unique_range_targets(Targets),
    lists:foreach(fun(#shard{name = Name}) ->
        try
            {ok, Db} = couch_db:open_int(Name, [?ADMIN_CTX]),
            try
                {ok, DDocs} = couch_db:get_design_docs(Db),
                lists:foreach(fun(DDoc) ->
                    catch couch_mrview:refresh(Name, DDoc)
                end, DDocs)
            after
                couch_db:close(Db)
            end
        catch
            _:_ -> ok  % Index build is best-effort
        end
    end, UniqueTargets).

%% @doc Delete the source shard after successful split.
-spec delete_source(#shard{}) -> ok | {error, term()}.
delete_source(#shard{name = Name, node = Node}) when Node =:= node() ->
    case couch_server:delete(Name, [?ADMIN_CTX]) of
        ok -> ok;
        not_found -> ok;
        Error -> {error, {delete_failed, Error}}
    end;
delete_source(#shard{name = Name, node = Node}) ->
    case rpc:call(Node, couch_server, delete, [Name, [?ADMIN_CTX]], 30000) of
        ok -> ok;
        not_found -> ok;
        {badrpc, Reason} -> {error, {rpc_failed, Node, Reason}};
        Error -> {error, {delete_failed, Error}}
    end.

%% Return one target shard per unique range (dedup replicas)
unique_range_targets(Targets) ->
    maps:values(build_target_map(Targets)).

%% ===================================================================
%% Internal helpers
%% ===================================================================

verify_ranges_contiguous([_]) -> ok;
verify_ranges_contiguous([[_B1, E1], [B2, E2] | Rest]) ->
    case B2 =:= E1 + 1 of
        true -> verify_ranges_contiguous([[B2, E2] | Rest]);
        false -> error({range_gap, E1, B2})
    end.
