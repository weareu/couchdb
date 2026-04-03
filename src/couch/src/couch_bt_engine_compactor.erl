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

-module(couch_bt_engine_compactor).

-export([
    start/4
]).

%% Exported for testing
-export([
    parse_iso8601_date/1,
    is_older_than_days/2,
    find_date_in_props/2,
    sanitize_path_component/1,
    get_retention_opts_from_ini/0,
    is_extraction_enabled/1
]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_bt_engine.hrl").

-record(comp_st, {
    db_name,
    old_st,
    new_st,
    meta_fd,
    retry
}).

-record(comp_header, {
    db_header,
    meta_st
}).

-record(merge_st, {
    src_fd,
    id_tree,
    seq_tree,
    curr,
    rem_seqs,
    locs
}).

-ifdef(TEST).
-define(COMP_EVENT(Name), couch_bt_engine_compactor_ev:event(Name)).
-else.
-define(COMP_EVENT(Name), ignore).
-endif.

start(#st{} = St, DbName, Options, Parent) ->
    erlang:put(io_priority, {db_compact, DbName}),
    couch_log:debug("Compaction process spawned for db \"~s\"", [DbName]),

    couch_db_engine:trigger_on_compact(DbName),

    ?COMP_EVENT(init),
    {ok, InitCompSt} = open_compaction_files(DbName, St, Options),
    ?COMP_EVENT(files_opened),

    Stages = [
        fun copy_purge_info/1,
        fun copy_compact/1,
        fun commit_compaction_data/1,
        fun sort_meta_data/1,
        fun commit_compaction_data/1,
        fun copy_meta_data/1,
        fun compact_final_sync/1
    ],

    FinalCompSt = lists:foldl(
        fun(Stage, CompSt) ->
            Stage(CompSt)
        end,
        InitCompSt,
        Stages
    ),

    #comp_st{
        new_st = FinalNewSt,
        meta_fd = MetaFd
    } = FinalCompSt,

    ok = couch_bt_engine:decref(FinalNewSt),
    ok = couch_file:close(MetaFd),

    ?COMP_EVENT(before_notify),
    Msg = {compact_done, couch_bt_engine, FinalNewSt#st.filepath},
    gen_server:cast(Parent, Msg).

open_compaction_files(DbName, OldSt, Options) ->
    #st{
        filepath = DbFilePath,
        header = SrcHdr
    } = OldSt,
    DataFile = DbFilePath ++ ".compact.data",
    MetaFile = DbFilePath ++ ".compact.meta",
    {ok, DataFd, DataHdr} = open_compaction_file(DataFile),
    {ok, MetaFd, MetaHdr} = open_compaction_file(MetaFile),
    DataHdrIsDbHdr = couch_bt_engine_header:is_header(DataHdr),
    CompSt =
        case {DataHdr, MetaHdr} of
            {#comp_header{} = A, #comp_header{} = A} ->
                % We're restarting a compaction that did not finish
                % before trying to swap out with the original db
                DbHeader = A#comp_header.db_header,
                St0 = couch_bt_engine:init_state(
                    DataFile, DataFd, DbHeader, Options
                ),
                St1 = bind_emsort(St0, MetaFd, A#comp_header.meta_st),
                #comp_st{
                    db_name = DbName,
                    old_st = OldSt,
                    new_st = St1,
                    meta_fd = MetaFd,
                    retry = St0#st.id_tree
                };
            _ when DataHdrIsDbHdr ->
                % We tried to swap out the compaction but there were
                % writes to the database during compaction. Start
                % a compaction retry.
                Header = couch_bt_engine_header:from(SrcHdr),
                ok = reset_compaction_file(MetaFd, Header),
                St0 = couch_bt_engine:init_state(
                    DataFile, DataFd, DataHdr, Options
                ),
                St1 = bind_emsort(St0, MetaFd, nil),
                #comp_st{
                    db_name = DbName,
                    old_st = OldSt,
                    new_st = St1,
                    meta_fd = MetaFd,
                    retry = St0#st.id_tree
                };
            _ ->
                % We're starting a compaction from scratch
                Header = couch_bt_engine_header:from(SrcHdr),
                ok = reset_compaction_file(DataFd, Header),
                ok = reset_compaction_file(MetaFd, Header),
                St0 = couch_bt_engine:init_state(DataFile, DataFd, Header, Options),
                St1 = bind_emsort(St0, MetaFd, nil),
                #comp_st{
                    db_name = DbName,
                    old_st = OldSt,
                    new_st = St1,
                    meta_fd = MetaFd,
                    retry = nil
                }
        end,
    unlink(DataFd),
    monitor(process, MetaFd),
    {ok, CompSt}.

copy_purge_info(#comp_st{} = CompSt) ->
    #comp_st{
        db_name = DbName,
        old_st = OldSt,
        new_st = NewSt,
        retry = Retry
    } = CompSt,
    ?COMP_EVENT(purge_init),
    MinPurgeSeq = couch_util:with_db(DbName, fun(Db) ->
        couch_db:get_minimum_purge_seq(Db)
    end),
    OldPSTree = OldSt#st.purge_seq_tree,
    StartSeq = couch_bt_engine:get_purge_seq(NewSt) + 1,
    BufferSize = config:get_integer(
        "database_compaction", "doc_buffer_size", 524288
    ),
    CheckpointAfter = config:get(
        "database_compaction", "checkpoint_after", BufferSize * 10
    ),

    EnumFun = fun(Info, _Reds, {StAcc0, InfosAcc, InfosSize, CopiedSize}) ->
        NewInfosSize = InfosSize + ?term_size(Info),
        if
            NewInfosSize >= BufferSize ->
                StAcc1 = copy_purge_infos(
                    OldSt, StAcc0, [Info | InfosAcc], MinPurgeSeq, Retry
                ),
                NewCopiedSize = CopiedSize + NewInfosSize,
                if
                    NewCopiedSize >= CheckpointAfter ->
                        StAcc2 = commit_compaction_data(StAcc1),
                        {ok, {StAcc2, [], 0, 0}};
                    true ->
                        {ok, {StAcc1, [], 0, NewCopiedSize}}
                end;
            true ->
                NewInfosAcc = [Info | InfosAcc],
                {ok, {StAcc0, NewInfosAcc, NewInfosSize, CopiedSize}}
        end
    end,

    InitAcc = {NewSt, [], 0, 0},
    Opts = [{start_key, StartSeq}],
    {ok, _, FinalAcc} = couch_btree:fold(OldPSTree, EnumFun, InitAcc, Opts),
    {NewStAcc, Infos, _, _} = FinalAcc,
    FinalNewSt = copy_purge_infos(OldSt, NewStAcc, Infos, MinPurgeSeq, Retry),

    ?COMP_EVENT(purge_done),
    CompSt#comp_st{
        new_st = FinalNewSt
    }.

copy_purge_infos(OldSt, NewSt0, Infos, MinPurgeSeq, Retry) ->
    #st{
        id_tree = OldIdTree
    } = OldSt,

    % Re-bind our id_tree to the backing btree
    NewIdTreeState = couch_bt_engine_header:id_tree_state(NewSt0#st.header),
    MetaFd = couch_emsort:get_fd(NewSt0#st.id_tree),
    MetaState = couch_emsort:get_state(NewSt0#st.id_tree),
    NewSt1 = bind_id_tree(NewSt0, NewSt0#st.fd, NewIdTreeState),

    #st{
        id_tree = NewIdTree0,
        seq_tree = NewSeqTree0,
        purge_tree = NewPurgeTree0,
        purge_seq_tree = NewPurgeSeqTree0
    } = NewSt1,

    % Copy over the purge infos
    InfosToAdd = lists:filter(
        fun({PSeq, _, _, _}) ->
            PSeq > MinPurgeSeq
        end,
        Infos
    ),
    {ok, NewPurgeTree1} = couch_btree:add(NewPurgeTree0, InfosToAdd),
    {ok, NewPurgeSeqTree1} = couch_btree:add(NewPurgeSeqTree0, InfosToAdd),

    NewSt2 = NewSt1#st{
        purge_tree = NewPurgeTree1,
        purge_seq_tree = NewPurgeSeqTree1
    },

    % If we're peforming a retry compaction we have to check if
    % any of the referenced docs have been completely purged
    % from the database. Any doc that has been completely purged
    % must then be removed from our partially compacted database.
    NewSt3 =
        if
            Retry == nil ->
                NewSt2;
            true ->
                AllDocIds = [DocId || {_PurgeSeq, _UUID, DocId, _Revs} <- Infos],
                UniqDocIds = lists:usort(AllDocIds),
                OldIdResults = couch_btree:lookup(OldIdTree, UniqDocIds),
                OldZipped = lists:zip(UniqDocIds, OldIdResults),

                % The list of non-existant docs in the database being compacted
                MaybeRemDocIds = [DocId || {DocId, not_found} <- OldZipped],

                % Removing anything that exists in the partially compacted database
                NewIdResults = couch_btree:lookup(NewIdTree0, MaybeRemDocIds),
                ToRemove = [Doc || {ok, Doc} <- NewIdResults, Doc /= {ok, not_found}],

                {RemIds, RemSeqs} = lists:unzip(
                    lists:map(
                        fun(FDI) ->
                            #full_doc_info{
                                id = Id,
                                update_seq = Seq
                            } = FDI,
                            {Id, Seq}
                        end,
                        ToRemove
                    )
                ),

                {ok, NewIdTree1} = couch_btree:add_remove(NewIdTree0, [], RemIds),
                {ok, NewSeqTree1} = couch_btree:add_remove(NewSeqTree0, [], RemSeqs),

                NewSt2#st{
                    id_tree = NewIdTree1,
                    seq_tree = NewSeqTree1
                }
        end,

    Header = couch_bt_engine:update_header(NewSt3, NewSt3#st.header),
    NewSt4 = NewSt3#st{
        header = Header
    },
    bind_emsort(NewSt4, MetaFd, MetaState).

copy_compact(#comp_st{} = CompSt) ->
    #comp_st{
        db_name = DbName,
        old_st = St,
        new_st = NewSt0,
        retry = Retry
    } = CompSt,

    Compression = couch_compress:get_compression_method(),
    NewSt = NewSt0#st{compression = Compression},
    NewUpdateSeq = couch_bt_engine:get_update_seq(NewSt0),
    TotalChanges = couch_bt_engine:count_changes_since(St, NewUpdateSeq),

    % Read retention/extraction config ONCE at compaction start.
    % This prevents inconsistent behavior if config changes mid-compaction.
    ShortDbName = binary_to_list(mem3:dbname(DbName)),
    RetentionOpts = get_retention_opts(ShortDbName),
    ExtractEnabled = is_extraction_enabled(ShortDbName),
    erlang:put(compaction_retention_opts, RetentionOpts),
    erlang:put(compaction_extract_enabled, ExtractEnabled),
    BufferSize = list_to_integer(
        config:get("database_compaction", "doc_buffer_size", "524288")
    ),
    CheckpointAfter = couch_util:to_integer(
        config:get(
            "database_compaction",
            "checkpoint_after",
            BufferSize * 10
        )
    ),

    EnumBySeqFun =
        fun(
            DocInfo,
            _Offset,
            {AccNewSt, AccUncopied, AccUncopiedSize, AccCopiedSize}
        ) ->
            Seq =
                case DocInfo of
                    #full_doc_info{} -> DocInfo#full_doc_info.update_seq;
                    #doc_info{} -> DocInfo#doc_info.high_seq
                end,

            AccUncopiedSize2 = AccUncopiedSize + ?term_size(DocInfo),
            if
                AccUncopiedSize2 >= BufferSize ->
                    NewSt2 = copy_docs(
                        St, AccNewSt, lists:reverse([DocInfo | AccUncopied]), Retry, DbName
                    ),
                    AccCopiedSize2 = AccCopiedSize + AccUncopiedSize2,
                    if
                        AccCopiedSize2 >= CheckpointAfter ->
                            {ok, NewSt3} = couch_bt_engine:set_update_seq(NewSt2, Seq),
                            CommNewSt3 = commit_compaction_data(NewSt3),
                            {ok, {CommNewSt3, [], 0, 0}};
                        true ->
                            {ok, NewSt3} = couch_bt_engine:set_update_seq(NewSt2, Seq),
                            {ok, {NewSt3, [], 0, AccCopiedSize2}}
                    end;
                true ->
                    {ok, {AccNewSt, [DocInfo | AccUncopied], AccUncopiedSize2, AccCopiedSize}}
            end
        end,

    TaskProps0 = [
        {type, database_compaction},
        {database, DbName},
        {phase, document_copy},
        {progress, 0},
        {changes_done, 0},
        {total_changes, TotalChanges}
    ],
    case (Retry =/= nil) and couch_task_status:is_task_added() of
        true ->
            couch_task_status:update([
                {retry, true},
                {phase, document_copy},
                {progress, 0},
                {changes_done, 0},
                {total_changes, TotalChanges}
            ]);
        false ->
            couch_task_status:add_task(TaskProps0),
            couch_task_status:set_update_frequency(500)
    end,

    ?COMP_EVENT(seq_init),
    {ok, _, {NewSt2, Uncopied, _, _}} =
        couch_btree:foldl(
            St#st.seq_tree,
            EnumBySeqFun,
            {NewSt, [], 0, 0},
            [{start_key, NewUpdateSeq + 1}]
        ),

    NewSt3 = copy_docs(St, NewSt2, lists:reverse(Uncopied), Retry, DbName),

    ?COMP_EVENT(seq_done),

    % Copy the security information over
    SecProps = couch_bt_engine:get_security(St),
    {ok, NewSt4} = couch_bt_engine:copy_security(NewSt3, SecProps),

    % Copy general properties over
    Props = couch_bt_engine:get_props(St),
    {ok, NewSt5} = couch_bt_engine:copy_props(NewSt4, Props),

    FinalUpdateSeq = couch_bt_engine:get_update_seq(St),
    {ok, NewSt6} = couch_bt_engine:set_update_seq(NewSt5, FinalUpdateSeq),

    CompSt#comp_st{
        new_st = NewSt6
    }.

copy_docs(St, #st{} = NewSt, MixedInfos, Retry, DbName) ->
    DocInfoIds = [Id || #doc_info{id = Id} <- MixedInfos],
    LookupResults = couch_btree:lookup(St#st.id_tree, DocInfoIds),
    % COUCHDB-968, make sure we prune duplicates during compaction
    NewInfos0 = lists:usort(
        fun(#full_doc_info{id = A}, #full_doc_info{id = B}) ->
            A =< B
        end,
        merge_lookups(MixedInfos, LookupResults)
    ),

    % Use config cached at compaction start (copy_compact) to avoid
    % inconsistent behavior if config changes mid-compaction.
    ShortDbName = binary_to_list(mem3:dbname(DbName)),
    RetentionOpts = erlang:get(compaction_retention_opts),

    % SPEED: Batch retention date check using a single pread_terms call
    % for all doc bodies, instead of reading one-at-a-time in should_remove_doc.
    % This reduces disk I/O syscalls from N to 1 for the retention filter pass.
    NewInfos0c = case RetentionOpts of
        #{remove_after_days := RemoveDays} when RemoveDays > 0 ->
            #{date_fields := DateFields} = RetentionOpts,
            batch_retention_filter(St, NewInfos0, DateFields, RemoveDays, ShortDbName);
        _ ->
            NewInfos0
    end,

    ExtractEnabled = erlang:get(compaction_extract_enabled),
    ExtractAfterDays = case RetentionOpts of
        #{extract_after_days := EAD} -> EAD;
        _ -> 365
    end,
    NewInfos1 = lists:map(
        fun(Info) ->
            DocId = Info#full_doc_info.id,
            {NewRevTree, FinalAcc} = couch_key_tree:mapfold(
                fun
                    ({RevPos, RevId}, #leaf{ptr = Sp} = Leaf, leaf, SizesAcc) ->
                        {Body, AttInfos, ExtractedAttSize} =
                            case ExtractEnabled of
                                true ->
                                    copy_doc_attachments_extract(
                                        St, Sp, NewSt, DocId, DbName,
                                        RetentionOpts, ExtractAfterDays
                                    );
                                false ->
                                    {B, A} = copy_doc_attachments(St, Sp, NewSt),
                                    {B, A, 0}
                            end,
                        #size_info{external = OldExternalSize} = Leaf#leaf.sizes,
                        ExternalSize =
                            case OldExternalSize of
                                0 when is_binary(Body) ->
                                    couch_compress:uncompressed_size(Body);
                                0 ->
                                    couch_ejson_size:encoded_size(Body);
                                N ->
                                    N
                            end,
                        Doc0 = #doc{
                            id = Info#full_doc_info.id,
                            revs = {RevPos, [RevId]},
                            deleted = Leaf#leaf.deleted,
                            body = Body,
                            atts = AttInfos
                        },
                        Doc1 = couch_bt_engine:serialize_doc(NewSt, Doc0),
                        {ok, Doc2, ActiveSize} =
                            couch_bt_engine:write_doc_body(NewSt, Doc1),
                        AttSizes = [{element(3, A), element(4, A)} || A <- AttInfos],
                        % Include extracted attachment size in active size
                        % calculation to prevent smoosh from seeing a false
                        % file/active ratio and triggering compaction loops
                        NewLeaf = Leaf#leaf{
                            ptr = Doc2#doc.body,
                            sizes = #size_info{
                                active = ActiveSize + ExtractedAttSize,
                                external = ExternalSize + ExtractedAttSize
                            },
                            atts = AttSizes
                        },
                        {NewLeaf, couch_db_updater:add_sizes(leaf, NewLeaf, SizesAcc)};
                    (_Rev, _Leaf, branch, SizesAcc) ->
                        {?REV_MISSING, SizesAcc}
                end,
                {0, 0, []},
                Info#full_doc_info.rev_tree
            ),
            {FinalAS, FinalES, FinalAtts} = FinalAcc,
            TotalAttSize = lists:foldl(fun({_, S}, A) -> S + A end, 0, FinalAtts),
            NewActiveSize = FinalAS + TotalAttSize,
            NewExternalSize = FinalES + TotalAttSize,
            ?COMP_EVENT(seq_copy),
            Info#full_doc_info{
                rev_tree = NewRevTree,
                sizes = #size_info{
                    active = NewActiveSize,
                    external = NewExternalSize
                }
            }
        end,
        NewInfos0c
    ),

    Limit = couch_bt_engine:get_revs_limit(St),
    NewInfos = lists:map(
        fun(FDI) ->
            FDI#full_doc_info{
                rev_tree = couch_key_tree:stem(FDI#full_doc_info.rev_tree, Limit)
            }
        end,
        NewInfos1
    ),

    RemoveSeqs =
        case Retry of
            nil ->
                [];
            OldDocIdTree ->
                % Compaction is being rerun to catch up to writes during the
                % first pass. This means we may have docs that already exist
                % in the seq_tree in the .data file. Here we lookup any old
                % update_seqs so that they can be removed.
                Ids = [Id || #full_doc_info{id = Id} <- NewInfos],
                Existing = couch_btree:lookup(OldDocIdTree, Ids),
                [Seq || {ok, #full_doc_info{update_seq = Seq}} <- Existing]
        end,

    {ok, SeqTree} = couch_btree:add_remove(
        NewSt#st.seq_tree, NewInfos, RemoveSeqs
    ),

    EMSortFd = couch_emsort:get_fd(NewSt#st.id_tree),
    {ok, LocSizes} = couch_file:append_terms(EMSortFd, NewInfos),
    EMSortEntries = lists:zipwith(
        fun(FDI, {Loc, _}) ->
            #full_doc_info{
                id = Id,
                update_seq = Seq
            } = FDI,
            {{Id, Seq}, Loc}
        end,
        NewInfos,
        LocSizes
    ),
    {ok, IdEms} = couch_emsort:add(NewSt#st.id_tree, EMSortEntries),
    update_compact_task(length(NewInfos)),
    NewSt#st{id_tree = IdEms, seq_tree = SeqTree}.

copy_doc_attachments(#st{} = SrcSt, SrcSp, DstSt) ->
    {ok, {BodyData, BinInfos0}} = couch_file:pread_term(SrcSt#st.fd, SrcSp),
    BinInfos =
        case BinInfos0 of
            _ when is_binary(BinInfos0) ->
                couch_compress:decompress(BinInfos0);
            _ when is_list(BinInfos0) ->
                % pre 1.2 file format
                BinInfos0
        end,
    % copy the bin values
    NewBinInfos = lists:map(
        fun
            ({Name, Type, BinSp, AttLen, RevPos, ExpectedMd5}) ->
                % 010 UPGRADE CODE
                {ok, SrcStream} = couch_bt_engine:open_read_stream(SrcSt, BinSp),
                {ok, DstStream} = couch_bt_engine:open_write_stream(DstSt, []),
                ok = couch_stream:copy(SrcStream, DstStream),
                {NewStream, AttLen, AttLen, ActualMd5, _IdentityMd5} =
                    couch_stream:close(DstStream),
                {ok, NewBinSp} = couch_stream:to_disk_term(NewStream),
                couch_util:check_md5(ExpectedMd5, ActualMd5),
                {Name, Type, NewBinSp, AttLen, AttLen, RevPos, ExpectedMd5, identity};
            ({Name, Type, BinSp, AttLen, DiskLen, RevPos, ExpectedMd5, Enc1}) ->
                {ok, SrcStream} = couch_bt_engine:open_read_stream(SrcSt, BinSp),
                {ok, DstStream} = couch_bt_engine:open_write_stream(DstSt, []),
                ok = couch_stream:copy(SrcStream, DstStream),
                {NewStream, AttLen, _, ActualMd5, _IdentityMd5} =
                    couch_stream:close(DstStream),
                {ok, NewBinSp} = couch_stream:to_disk_term(NewStream),
                couch_util:check_md5(ExpectedMd5, ActualMd5),
                Enc =
                    case Enc1 of
                        true ->
                            % 0110 UPGRADE CODE
                            gzip;
                        false ->
                            % 0110 UPGRADE CODE
                            identity;
                        _ ->
                            Enc1
                    end,
                {Name, Type, NewBinSp, AttLen, DiskLen, RevPos, ExpectedMd5, Enc}
        end,
        BinInfos
    ),
    {BodyData, NewBinInfos}.

%% -------------------------------------------------------------------
%% Attachment extraction: writes attachments to external storage
%% (e.g. GlusterFS) during compaction based on document age.
%% Returns {BodyData, AttInfos, ExtractedAttSize} where
%% ExtractedAttSize is the total bytes of extracted attachments
%% (used to keep active_size accurate and prevent smoosh loops).
%% -------------------------------------------------------------------
copy_doc_attachments_extract(#st{} = SrcSt, SrcSp, DstSt, DocId, DbName,
                             RetentionOpts, ExtractAfterDays) ->
    {ok, {BodyData0, BinInfos0}} = couch_file:pread_term(SrcSt#st.fd, SrcSp),
    BinInfos =
        case BinInfos0 of
            _ when is_binary(BinInfos0) ->
                couch_compress:decompress(BinInfos0);
            _ when is_list(BinInfos0) ->
                BinInfos0
        end,
    {BodyData, _WasCompressed} =
        case BodyData0 of
            _ when is_binary(BodyData0) ->
                {couch_compress:decompress(BodyData0), true};
            _ when is_list(BodyData0) ->
                {BodyData0, false}
        end,

    DateFields = case RetentionOpts of
        #{date_fields := DF} -> DF;
        _ -> [<<"date">>]
    end,
    ShouldExtract = case BodyData of
        {Props} when is_list(Props) ->
            case find_date_in_props(Props, DateFields) of
                undefined ->
                    % No date field found — extract by default (old/unknown data)
                    true;
                {ok, DateTuple} ->
                    is_older_than_days(DateTuple, ExtractAfterDays)
            end;
        _ ->
            % Non-JSON body (raw binary, etc.) — don't extract
            false
    end,

    case ShouldExtract andalso BinInfos =/= [] of
        true ->
            % Calculate total attachment size before extraction
            TotalExtractedSize = lists:foldl(
                fun(Att, Acc) -> Acc + att_disk_len(Att) end,
                0,
                BinInfos
            ),
            % Write each attachment to external storage
            lists:foreach(
                fun(Attachment) ->
                    write_attachment_to_file(SrcSt, DocId, Attachment, DbName)
                end,
                BinInfos
            ),
            couch_log:info(
                "Extracted ~B attachments (~B bytes) from ~s/~s to external storage",
                [length(BinInfos), TotalExtractedSize,
                 mem3:dbname(DbName), DocId]
            ),
            % Return original body, empty atts, and extracted size for
            % size tracking (prevents smoosh from seeing false ratio)
            {BodyData0, [], TotalExtractedSize};
        false ->
            % Keep attachments in database — standard copy
            NewBinInfos = copy_bin_infos(SrcSt, DstSt, BinInfos),
            {BodyData, NewBinInfos, 0}
    end.

%% -------------------------------------------------------------------
%% Retention: batch filter documents for removal.
%% SPEED: Collects all winning leaf pointers, reads bodies in one
%% batch call to couch_file:pread_terms, checks dates, filters.
%% This reduces disk I/O from N syscalls to 1 for the retention pass.
%% -------------------------------------------------------------------
batch_retention_filter(St, Infos, DateFields, RemoveDays, ShortDbName) ->
    % Partition into candidates (can be removed) and protected (never removed)
    {Candidates, Protected} = lists:partition(
        fun(#full_doc_info{id = Id, deleted = Del}) ->
            case Id of
                <<"_design/", _/binary>> -> false;
                <<"_local/", _/binary>> -> false;
                _ -> not Del
            end
        end,
        Infos
    ),
    case Candidates of
        [] ->
            Infos;
        _ ->
            % Collect winning leaf pointers for all candidates
            SpInfoPairs = lists:filtermap(
                fun(Info) ->
                    case get_winning_leaf_ptr(Info#full_doc_info.rev_tree) of
                        undefined -> false;
                        Sp -> {true, {Sp, Info}}
                    end
                end,
                Candidates
            ),
            {Sps, InfosWithSp} = lists:unzip(SpInfoPairs),
            % Batch read all bodies at once
            {ok, Bodies} = couch_file:pread_terms(St#st.fd, Sps),
            % Check each body's date field
            Kept = lists:filtermap(
                fun({{BodyData0, _BinInfos}, Info}) ->
                    BodyData = case BodyData0 of
                        _ when is_binary(BodyData0) ->
                            couch_compress:decompress(BodyData0);
                        _ -> BodyData0
                    end,
                    ShouldRemove = case BodyData of
                        {Props} when is_list(Props) ->
                            case find_date_in_props(Props, DateFields) of
                                undefined -> false;
                                {ok, DateTuple} ->
                                    is_older_than_days(DateTuple, RemoveDays)
                            end;
                        _ -> false
                    end,
                    case ShouldRemove of
                        true ->
                            log_retained_removal(ShortDbName, Info#full_doc_info.id),
                            false;
                        false ->
                            {true, Info}
                    end
                end,
                lists:zip(Bodies, InfosWithSp)
            ),
            % Also keep candidates that had no winning leaf pointer
            NoSpCandidates = [I || I <- Candidates,
                not lists:keymember(I, 2, SpInfoPairs)],
            % Maintain original ordering
            KeptSet = sets:from_list(
                [I#full_doc_info.id || I <- Kept ++ NoSpCandidates ++ Protected]),
            [I || I <- Infos, sets:is_element(I#full_doc_info.id, KeptSet)]
    end.

get_winning_leaf_ptr(RevTree) ->
    try
        FoldFun = fun
            ({_RevPos, _RevId}, #leaf{ptr = Sp} = _Leaf, leaf, _Acc) ->
                throw({found, Sp});
            (_Rev, _Leaf, branch, Acc) ->
                Acc
        end,
        couch_key_tree:mapfold(FoldFun, undefined, RevTree),
        undefined
    catch
        throw:{found, Sp} -> Sp
    end.

%% -------------------------------------------------------------------
%% Log removed document IDs to a retention log file and couch_log
%% -------------------------------------------------------------------
log_retained_removal(DbName, DocId) ->
    couch_log:warning(
        "Retention removal: db=~s doc_id=~s removed during compaction",
        [DbName, DocId]
    ),
    LogDir = config:get("compaction_retention", "log_dir",
        filename:join(config:get("couchdb", "database_dir", "/tmp/"),
                      "retention_logs")),
    LogFile = filename:join(LogDir, DbName ++ ".removal.log"),
    ok = filelib:ensure_dir(LogFile),
    {{Y, M, D}, {H, Mi, S}} = calendar:universal_time(),
    Timestamp = io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ",
        [Y, M, D, H, Mi, S]),
    Line = io_lib:format("~s ~s~n", [Timestamp, DocId]),
    case file:write_file(LogFile, Line, [append]) of
        ok -> ok;
        {error, Reason} ->
            % Never crash compaction on log write failure — the couch_log
            % warning above already recorded the removal. Losing the file
            % log is unfortunate but destroying the compaction run is worse.
            couch_log:error(
                "Failed to write retention log ~s: ~p (doc_id=~s)",
                [LogFile, Reason, DocId]
            )
    end.

%% -------------------------------------------------------------------
%% Date parsing and comparison utilities
%% -------------------------------------------------------------------
find_date_in_props(_Props, []) ->
    undefined;
find_date_in_props(Props, [Field | Rest]) ->
    case lists:keyfind(Field, 1, Props) of
        {_, DateString} when is_binary(DateString) ->
            case parse_iso8601_date(DateString) of
                {ok, DateTuple} -> {ok, DateTuple};
                {error, _} -> find_date_in_props(Props, Rest)
            end;
        _ ->
            find_date_in_props(Props, Rest)
    end.

is_older_than_days({Year, Month, Day}, Days) ->
    {{CurY, CurM, CurD}, _} = calendar:universal_time(),
    CutoffDays = calendar:date_to_gregorian_days({CurY, CurM, CurD}) - Days,
    DocDays = calendar:date_to_gregorian_days({Year, Month, Day}),
    DocDays < CutoffDays.

parse_iso8601_date(DateString) ->
    case binary:split(DateString, <<"T">>, [global]) of
        [DatePart | _] ->
            parse_date_part(DatePart);
        _ ->
            % Try without time component (just YYYY-MM-DD)
            parse_date_part(DateString)
    end.

parse_date_part(DatePart) ->
    case binary:split(DatePart, <<"-">>, [global]) of
        [YearBin, MonthBin, DayBin] ->
            try
                Y = binary_to_integer(YearBin),
                M = binary_to_integer(MonthBin),
                D = binary_to_integer(DayBin),
                case calendar:valid_date(Y, M, D) of
                    true -> {ok, {Y, M, D}};
                    false -> {error, invalid_date}
                end
            catch
                _:_ -> {error, invalid_format}
            end;
        _ ->
            {error, invalid_format}
    end.

%% -------------------------------------------------------------------
%% Attachment file I/O
%% -------------------------------------------------------------------
write_attachment_to_file(SrcSt, DocId, Attachment, DbName) ->
    {Name, DiskTerm} = case Attachment of
        {N, _Type, _BinSp, _AttLen, _RevPos, _ExpectedMd5} = T ->
            {N, T};
        {N, _Type, _BinSp, _AttLen, _DiskLen, _RevPos, _ExpectedMd5, _Enc} = T ->
            {N, T}
    end,
    ShortDbName = binary_to_list(mem3:dbname(DbName)),
    AttachmentPath = get_attachment_path(DocId, Name, ShortDbName),
    TmpPath = AttachmentPath ++ ".tmp",
    ok = filelib:ensure_dir(AttachmentPath),
    StreamSrc = fun(Sp) -> couch_bt_engine:open_read_stream(SrcSt, Sp) end,
    AttachmentObj = couch_att:from_disk_term(StreamSrc, DiskTerm),
    BinaryData = couch_att:to_binary(AttachmentObj),
    % Write to temp file first, verify MD5, then atomic rename.
    % If we crash between write and rename, the .tmp file is harmless
    % and will be overwritten on the next compaction attempt.
    ok = file:write_file(TmpPath, BinaryData),
    ExpectedMd5 = couch_att:fetch(md5, AttachmentObj),
    ActualMd5 = erlang:md5(BinaryData),
    case ActualMd5 =:= ExpectedMd5 of
        true ->
            ok = file:rename(TmpPath, AttachmentPath);
        false ->
            file:delete(TmpPath),
            throw({checksum_mismatch, DocId, Name, {ExpectedMd5, ActualMd5}})
    end.

get_attachment_path(DocId, AttachmentName, DbName) ->
    DataDir = config:get("couchdb", "database_dir", "/tmp/"),
    SafeDocId = sanitize_path_component(DocId),
    SafeAttName = sanitize_path_component(AttachmentName),
    SafeDbName = sanitize_path_component(DbName),
    filename:join([DataDir, "attachments", SafeDbName, SafeDocId, SafeAttName]).

sanitize_path_component(Bin) when is_binary(Bin) ->
    sanitize_path_component(binary_to_list(Bin));
sanitize_path_component(Str) when is_list(Str) ->
    % Reject path traversal: replace "..", "/", "\", and control chars
    re:replace(Str, "(\\.\\.|[/\\\\[:cntrl:]])", "_",
               [global, {return, list}]).

att_disk_len({_Name, _Type, _BinSp, AttLen, _RevPos, _Md5}) ->
    AttLen;
att_disk_len({_Name, _Type, _BinSp, _AttLen, DiskLen, _RevPos, _Md5, _Enc}) ->
    DiskLen.

copy_bin_infos(SrcSt, DstSt, BinInfos) ->
    lists:map(
        fun
            ({Name, Type, BinSp, AttLen, RevPos, ExpectedMd5}) ->
                {ok, SrcStream} = couch_bt_engine:open_read_stream(SrcSt, BinSp),
                {ok, DstStream} = couch_bt_engine:open_write_stream(DstSt, []),
                ok = couch_stream:copy(SrcStream, DstStream),
                {NewStream, AttLen, AttLen, ActualMd5, _IdentityMd5} =
                    couch_stream:close(DstStream),
                {ok, NewBinSp} = couch_stream:to_disk_term(NewStream),
                couch_util:check_md5(ExpectedMd5, ActualMd5),
                {Name, Type, NewBinSp, AttLen, AttLen, RevPos, ExpectedMd5, identity};
            ({Name, Type, BinSp, AttLen, DiskLen, RevPos, ExpectedMd5, Enc1}) ->
                {ok, SrcStream} = couch_bt_engine:open_read_stream(SrcSt, BinSp),
                {ok, DstStream} = couch_bt_engine:open_write_stream(DstSt, []),
                ok = couch_stream:copy(SrcStream, DstStream),
                {NewStream, AttLen, _, ActualMd5, _IdentityMd5} =
                    couch_stream:close(DstStream),
                {ok, NewBinSp} = couch_stream:to_disk_term(NewStream),
                couch_util:check_md5(ExpectedMd5, ActualMd5),
                Enc = case Enc1 of
                    true -> gzip;
                    false -> identity;
                    _ -> Enc1
                end,
                {Name, Type, NewBinSp, AttLen, DiskLen, RevPos, ExpectedMd5, Enc}
        end,
        BinInfos
    ).

%% -------------------------------------------------------------------
%% Configuration: reads retention/extraction settings.
%% INI points to a design doc name; the design doc is replicated
%% across all nodes automatically, ensuring cluster-wide consistency.
%%
%% INI config:
%%   [compaction_retention]
%%   config_ddoc = _design/retention_config
%%   log_dir = /var/lib/couchdb/retention_logs
%%
%% Design doc structure (per-database, replicated):
%%   {
%%     "_id": "_design/retention_config",
%%     "retention": {
%%       "date_fields": ["date", "created_at"],
%%       "extract_after_days": 365,
%%       "remove_after_days": 730
%%     }
%%   }
%%
%% If no design doc exists, falls back to INI-only config:
%%   [compaction_retention]
%%   date_fields = date,created_at
%%   extract_after_days = 365
%%   remove_after_days = 0
%% -------------------------------------------------------------------
get_retention_opts(ShortDbName) ->
    DDocId = config:get("compaction_retention", "config_ddoc", ""),
    case DDocId of
        "" ->
            get_retention_opts_from_ini();
        _ ->
            case get_retention_opts_from_ddoc(ShortDbName, DDocId) of
                {ok, Opts} -> Opts;
                {error, _} -> get_retention_opts_from_ini()
            end
    end.

get_retention_opts_from_ini() ->
    DateFieldsRaw = config:get("compaction_retention", "date_fields", "date"),
    DateFields = [list_to_binary(string:trim(F))
                  || F <- string:split(DateFieldsRaw, ",", all),
                     string:trim(F) =/= ""],
    ExtractDays = couch_util:to_integer(
        config:get("compaction_retention", "extract_after_days", "365")),
    RemoveDays = couch_util:to_integer(
        config:get("compaction_retention", "remove_after_days", "0")),
    #{
        date_fields => DateFields,
        extract_after_days => ExtractDays,
        remove_after_days => RemoveDays
    }.

get_retention_opts_from_ddoc(DbName, DDocId) ->
    try
        DDocIdBin = list_to_binary(DDocId),
        couch_util:with_db(list_to_binary(DbName), fun(Db) ->
            case couch_db:open_doc(Db, DDocIdBin, [ejson_body]) of
                {ok, #doc{body = {DocProps}}} ->
                    case lists:keyfind(<<"retention">>, 1, DocProps) of
                        {_, {RetentionProps}} ->
                            DateFields = case lists:keyfind(<<"date_fields">>, 1, RetentionProps) of
                                {_, Fields} when is_list(Fields) -> Fields;
                                _ -> [<<"date">>]
                            end,
                            ExtractDays = case lists:keyfind(<<"extract_after_days">>, 1, RetentionProps) of
                                {_, ED} when is_integer(ED) -> ED;
                                _ -> 365
                            end,
                            RemoveDays = case lists:keyfind(<<"remove_after_days">>, 1, RetentionProps) of
                                {_, RD} when is_integer(RD) -> RD;
                                _ -> 0
                            end,
                            {ok, #{
                                date_fields => DateFields,
                                extract_after_days => ExtractDays,
                                remove_after_days => RemoveDays
                            }};
                        _ ->
                            {error, no_retention_field}
                    end;
                {not_found, _} ->
                    {error, ddoc_not_found}
            end
        end)
    catch
        _:Reason ->
            couch_log:warning(
                "Failed to read retention config from ~s: ~p",
                [DDocId, Reason]
            ),
            {error, Reason}
    end.

is_extraction_enabled(ShortDbName) ->
    case config:get("compaction_retention", "extract_enabled", "false") of
        "true" ->
            % Check if specific databases are listed, or all
            case config:get("compaction_retention", "extract_databases", "all") of
                "all" -> true;
                DbList ->
                    Dbs = [string:trim(D) || D <- string:split(DbList, ",", all)],
                    lists:member(ShortDbName, Dbs)
            end;
        _ ->
            false
    end.

sort_meta_data(#comp_st{new_st = St0} = CompSt) ->
    ?COMP_EVENT(md_sort_init),
    NumKVs = couch_emsort:num_kvs(St0#st.id_tree),
    NumMerges = couch_emsort:num_merges(St0#st.id_tree),
    couch_task_status:update([
        {phase, docid_sort},
        {progress, 0},
        {changes_done, 0},
        {total_changes, NumMerges * NumKVs}
    ]),
    Reporter = fun update_compact_task/1,
    {ok, Ems} = couch_emsort:merge(St0#st.id_tree, Reporter),
    ?COMP_EVENT(md_sort_done),
    CompSt#comp_st{
        new_st = St0#st{
            id_tree = Ems
        }
    }.

copy_meta_data(#comp_st{new_st = St} = CompSt) ->
    #st{
        fd = Fd,
        header = Header,
        id_tree = Src
    } = St,
    SrcFd = couch_emsort:get_fd(Src),
    DstState = couch_bt_engine_header:id_tree_state(Header),
    {ok, IdTree0} = couch_btree:open(DstState, Fd, [
        {split, fun couch_bt_engine:id_tree_split/1},
        {join, fun couch_bt_engine:id_tree_join/2},
        {reduce, fun couch_bt_engine:id_tree_reduce/2}
    ]),
    {ok, Iter} = couch_emsort:iter(Src),
    Acc0 = #merge_st{
        src_fd = SrcFd,
        id_tree = IdTree0,
        seq_tree = St#st.seq_tree,
        rem_seqs = [],
        locs = []
    },
    ?COMP_EVENT(md_copy_init),
    NumKVs = couch_emsort:num_kvs(Src),
    couch_task_status:update([
        {phase, docid_copy},
        {progress, 0},
        {changes_done, 0},
        {total_changes, NumKVs}
    ]),
    Acc = merge_docids(Iter, Acc0),
    {ok, Infos} = couch_file:pread_terms(SrcFd, Acc#merge_st.locs),
    {ok, IdTree} = couch_btree:add(Acc#merge_st.id_tree, Infos),
    {ok, SeqTree} = couch_btree:add_remove(
        Acc#merge_st.seq_tree, [], Acc#merge_st.rem_seqs
    ),
    update_compact_task(NumKVs),
    ?COMP_EVENT(md_copy_done),
    CompSt#comp_st{
        new_st = St#st{
            id_tree = IdTree,
            seq_tree = SeqTree
        }
    }.

compact_final_sync(#comp_st{new_st = St0} = CompSt) ->
    ?COMP_EVENT(before_final_sync),
    {ok, St1} = couch_bt_engine:commit_data(St0),
    ?COMP_EVENT(after_final_sync),
    CompSt#comp_st{
        new_st = St1
    }.

open_compaction_file(FilePath) ->
    case couch_file:open(FilePath, [nologifmissing]) of
        {ok, Fd} ->
            case couch_file:read_header(Fd) of
                {ok, Header} -> {ok, Fd, Header};
                no_valid_header -> {ok, Fd, nil}
            end;
        {error, enoent} ->
            {ok, Fd} = couch_file:open(FilePath, [create]),
            {ok, Fd, nil}
    end.

reset_compaction_file(Fd, Header) ->
    ok = couch_file:truncate(Fd, 0),
    ok = couch_file:write_header(Fd, Header).

commit_compaction_data(#comp_st{new_st = St} = CompSt) ->
    % Compaction needs to write headers to both the data file
    % and the meta file so if we need to restart we can pick
    % back up from where we left off.
    CompSt#comp_st{
        new_st = commit_compaction_data(St)
    };
commit_compaction_data(#st{} = St) ->
    commit_compaction_data(St, couch_emsort:get_fd(St#st.id_tree)),
    commit_compaction_data(St, St#st.fd).

commit_compaction_data(#st{header = OldHeader} = St0, Fd) ->
    DataState = couch_bt_engine_header:id_tree_state(OldHeader),
    MetaFd = couch_emsort:get_fd(St0#st.id_tree),
    MetaState = couch_emsort:get_state(St0#st.id_tree),
    St1 = bind_id_tree(St0, St0#st.fd, DataState),
    Header = couch_bt_engine:update_header(St1, St1#st.header),
    CompHeader = #comp_header{
        db_header = Header,
        meta_st = MetaState
    },
    ok = couch_file:sync(Fd),
    ok = couch_file:write_header(Fd, CompHeader),
    St2 = St1#st{
        header = Header
    },
    bind_emsort(St2, MetaFd, MetaState).

bind_emsort(St, Fd, nil) ->
    {ok, Ems} = couch_emsort:open(Fd),
    St#st{id_tree = Ems};
bind_emsort(St, Fd, {BB, _} = Root) when is_list(BB) ->
    % Upgrade clause when we find old compaction files
    bind_emsort(St, Fd, [{root, Root}]);
bind_emsort(St, Fd, State) ->
    {ok, Ems} = couch_emsort:open(Fd, State),
    St#st{id_tree = Ems}.

bind_id_tree(St, Fd, State) ->
    {ok, IdBtree} = couch_btree:open(State, Fd, [
        {split, fun couch_bt_engine:id_tree_split/1},
        {join, fun couch_bt_engine:id_tree_join/2},
        {reduce, fun couch_bt_engine:id_tree_reduce/2}
    ]),
    St#st{id_tree = IdBtree}.

merge_lookups(Infos, []) ->
    Infos;
merge_lookups([], _) ->
    [];
merge_lookups([#doc_info{} = DI | RestInfos], [{ok, FDI} | RestLookups]) ->
    % Assert we've matched our lookups
    if
        DI#doc_info.id == FDI#full_doc_info.id -> ok;
        true -> error({mismatched_doc_infos, DI#doc_info.id})
    end,
    [FDI | merge_lookups(RestInfos, RestLookups)];
merge_lookups([FDI | RestInfos], Lookups) ->
    [FDI | merge_lookups(RestInfos, Lookups)].

merge_docids(Iter, #merge_st{locs = Locs} = Acc) when length(Locs) > 1000 ->
    #merge_st{
        src_fd = SrcFd,
        id_tree = IdTree0,
        seq_tree = SeqTree0,
        rem_seqs = RemSeqs
    } = Acc,
    {ok, Infos} = couch_file:pread_terms(SrcFd, Locs),
    {ok, IdTree1} = couch_btree:add(IdTree0, Infos),
    {ok, SeqTree1} = couch_btree:add_remove(SeqTree0, [], RemSeqs),
    Acc1 = Acc#merge_st{
        id_tree = IdTree1,
        seq_tree = SeqTree1,
        rem_seqs = [],
        locs = []
    },
    update_compact_task(length(Locs)),
    merge_docids(Iter, Acc1);
merge_docids(Iter, #merge_st{curr = Curr} = Acc) ->
    case next_info(Iter, Curr, []) of
        {NextIter, NewCurr, Loc, Seqs} ->
            Acc1 = Acc#merge_st{
                locs = [Loc | Acc#merge_st.locs],
                rem_seqs = Seqs ++ Acc#merge_st.rem_seqs,
                curr = NewCurr
            },
            ?COMP_EVENT(md_copy_row),
            merge_docids(NextIter, Acc1);
        {finished, Loc, Seqs} ->
            Acc#merge_st{
                locs = [Loc | Acc#merge_st.locs],
                rem_seqs = Seqs ++ Acc#merge_st.rem_seqs,
                curr = undefined
            };
        empty ->
            Acc
    end.

next_info(Iter, undefined, []) ->
    case couch_emsort:next(Iter) of
        {ok, {{Id, Seq}, Loc}, NextIter} ->
            next_info(NextIter, {Id, Seq, Loc}, []);
        finished ->
            empty
    end;
next_info(Iter, {Id, Seq, Loc}, Seqs) ->
    case couch_emsort:next(Iter) of
        {ok, {{Id, NSeq}, NLoc}, NextIter} ->
            next_info(NextIter, {Id, NSeq, NLoc}, [Seq | Seqs]);
        {ok, {{NId, NSeq}, NLoc}, NextIter} ->
            {NextIter, {NId, NSeq, NLoc}, Loc, Seqs};
        finished ->
            {finished, Loc, Seqs}
    end.

update_compact_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress =
        case Total of
            0 ->
                0;
            _ ->
                (Changes2 * 100) div Total
        end,
    couch_task_status:update([{changes_done, Changes2}, {progress, Progress}]).
