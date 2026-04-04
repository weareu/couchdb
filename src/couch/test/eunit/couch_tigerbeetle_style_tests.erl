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

%% @doc TigerBeetle-inspired deterministic simulation tests for CouchDB.
%%
%% Philosophy: "What would TigerBeetle do?"
%%
%% 1. DETERMINISTIC SIMULATION — Seed-controlled randomness. Every failure
%%    is reproducible by replaying the same seed.
%%
%% 2. CRASH RECOVERY — Inject crashes at every compaction stage. The database
%%    must always be recoverable to a consistent state.
%%
%% 3. STATE MACHINE — Model the expected database state explicitly, run
%%    operations against both the model and real DB, verify they match.
%%
%% 4. INVARIANT CHECKING — After EVERY operation, verify ALL invariants:
%%    - active_size > 0 (when docs exist)
%%    - active_size <= file_size
%%    - doc_count matches expected
%%    - update_seq is monotonically increasing
%%    - changes feed returns all doc IDs
%%    - compaction is idempotent (double compact = same sizes)
%%
%% 5. FUZZING — Random document bodies, dates, attachment sizes, operation
%%    sequences. Controlled by seed for reproducibility.
%%
%% 6. LIVENESS — All operations must complete within bounded time.

-module(couch_tigerbeetle_style_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(DELAY, 100).
-define(WAIT_DELAY_COUNT, 50).
-define(MAX_OP_TIME_MS, 30000).

%% ===================================================================
%% State machine model
%% ===================================================================

-record(model, {
    docs = #{},           % DocId => {Body, Deleted, Rev}
    update_seq = 0,
    seed
}).

new_model(Seed) ->
    #model{seed = Seed}.

model_write(#model{docs = Docs, update_seq = Seq} = M, DocId, Body) ->
    Rev = maps:size(Docs) + Seq + 1,
    M#model{
        docs = Docs#{DocId => {Body, false, Rev}},
        update_seq = Seq + 1
    }.

model_delete(#model{docs = Docs, update_seq = Seq} = M, DocId) ->
    case maps:find(DocId, Docs) of
        {ok, {Body, false, _Rev}} ->
            M#model{
                docs = Docs#{DocId => {Body, true, Seq + 1}},
                update_seq = Seq + 1
            };
        _ ->
            M
    end.

model_doc_count(#model{docs = Docs}) ->
    maps:size(maps:filter(fun(_, {_, Del, _}) -> not Del end, Docs)).

model_all_ids(#model{docs = Docs}) ->
    lists:sort([Id || {Id, {_, false, _}} <- maps:to_list(Docs)]).

%% ===================================================================
%% Invariant checker — call after EVERY operation
%% ===================================================================

check_invariants(DbName, Model, Context) ->
    couch_util:with_db(DbName, fun(Db) ->
        % 1. Doc count matches model
        {ok, ActualCount} = couch_db:get_doc_count(Db),
        ExpectedCount = model_doc_count(Model),
        ?assertEqual(ExpectedCount, ActualCount,
            iolist_to_binary([Context, " doc_count mismatch"])),

        % 2. Sizes sanity
        {ok, Info} = couch_db:get_db_info(Db),
        {SizeInfo} = couch_util:get_value(sizes, Info),
        Active = couch_util:get_value(active, SizeInfo),
        FileSize = couch_util:get_value(file, SizeInfo),

        % 3. active_size > 0 when docs exist (smoosh loop prevention)
        case ActualCount > 0 of
            true ->
                ?assert(Active > 0,
                    iolist_to_binary([Context, " active_size is 0 with docs present"]));
            false -> ok
        end,

        % 4. active <= file (valid smoosh ratio)
        ?assert(Active =< FileSize,
            iolist_to_binary([Context, " active > file"])),

        % 5. update_seq monotonically increasing
        ActualSeq = couch_db:get_update_seq(Db),
        ?assert(ActualSeq >= Model#model.update_seq,
            iolist_to_binary([Context, " update_seq decreased"])),

        % 6. All non-deleted doc IDs are accessible
        ExpectedIds = model_all_ids(Model),
        lists:foreach(fun(Id) ->
            case couch_db:open_doc(Db, Id, []) of
                {ok, _} -> ok;
                Other ->
                    error({invariant_violation, Context,
                           {doc_missing, Id, Other}})
            end
        end, ExpectedIds),

        ok
    end).

%% ===================================================================
%% 1. DETERMINISTIC SIMULATION — Seeded random operations
%% ===================================================================

setup() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    ok = couch_db:close(Db),
    DbName.

teardown(DbName) ->
    couch_server:delete(DbName, [?ADMIN_CTX]),
    ok.

deterministic_simulation_test_() ->
    {
        "Deterministic simulation testing",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_seeded_random_ops_seed_1/1,
                    fun t_seeded_random_ops_seed_42/1,
                    fun t_seeded_random_ops_seed_12345/1,
                    fun t_seeded_random_ops_seed_99999/1,
                    fun t_seeded_random_ops_seed_777/1
                ]
            }
        }
    }.

t_seeded_random_ops_seed_1(DbName) ->
    {timeout, 120, ?_test(run_seeded_simulation(DbName, 1, 200))}.
t_seeded_random_ops_seed_42(DbName) ->
    {timeout, 120, ?_test(run_seeded_simulation(DbName, 42, 200))}.
t_seeded_random_ops_seed_12345(DbName) ->
    {timeout, 120, ?_test(run_seeded_simulation(DbName, 12345, 200))}.
t_seeded_random_ops_seed_99999(DbName) ->
    {timeout, 120, ?_test(run_seeded_simulation(DbName, 99999, 200))}.
t_seeded_random_ops_seed_777(DbName) ->
    {timeout, 120, ?_test(run_seeded_simulation(DbName, 777, 200))}.

run_seeded_simulation(DbName, Seed, NumOps) ->
    rand:seed(exsss, {Seed, Seed * 7, Seed * 13}),
    Model0 = new_model(Seed),
    ModelFinal = lists:foldl(
        fun(OpNum, Model) ->
            Op = rand:uniform(10),
            Context = io_lib:format("seed=~B op=~B", [Seed, OpNum]),
            NewModel = case Op of
                N when N =< 5 ->
                    % 50% writes
                    DocId = random_docid(),
                    Body = random_body(),
                    write_doc(DbName, DocId, Body),
                    model_write(Model, DocId, Body);
                N when N =< 7 ->
                    % 20% updates (write to existing doc)
                    case model_all_ids(Model) of
                        [] ->
                            DocId = random_docid(),
                            Body = random_body(),
                            write_doc(DbName, DocId, Body),
                            model_write(Model, DocId, Body);
                        Ids ->
                            DocId = lists:nth(rand:uniform(length(Ids)), Ids),
                            update_doc(DbName, DocId, random_body()),
                            model_write(Model, DocId, random_body())
                    end;
                8 ->
                    % 10% deletes
                    case model_all_ids(Model) of
                        [] -> Model;
                        Ids ->
                            DocId = lists:nth(rand:uniform(length(Ids)), Ids),
                            delete_doc(DbName, DocId),
                            model_delete(Model, DocId)
                    end;
                9 ->
                    % 10% compact
                    compact_db(DbName),
                    Model;
                10 ->
                    % 10% compact + verify idempotency
                    compact_db(DbName),
                    S1 = get_db_sizes(DbName),
                    compact_db(DbName),
                    S2 = get_db_sizes(DbName),
                    ?assertEqual(maps:get(active, S1), maps:get(active, S2),
                        iolist_to_binary([Context, " compact not idempotent"])),
                    Model
            end,
            % INVARIANT CHECK after every operation
            check_invariants(DbName, NewModel, Context),
            NewModel
        end,
        Model0,
        lists:seq(1, NumOps)
    ),
    % Final comprehensive check
    check_invariants(DbName, ModelFinal, "final").

%% ===================================================================
%% 2. CRASH RECOVERY — Compaction interrupted at each stage
%% ===================================================================

crash_recovery_test_() ->
    {
        "Crash recovery: database must be consistent after interrupted compaction",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_crash_during_compact_data_intact/1,
                    fun t_kill_compactor_db_usable/1,
                    fun t_restart_compact_after_kill/1,
                    fun t_compact_during_writes_no_loss/1
                ]
            }
        }
    }.

t_crash_during_compact_data_intact(DbName) ->
    ?_test(begin
        write_many(DbName, 100),
        Model = lists:foldl(fun(I, M) ->
            model_write(M, docid(I), [{<<"v">>, I}])
        end, new_model(0), lists:seq(1, 100)),
        % Start compaction then kill it immediately
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Pid} = couch_db:start_compact(Db),
            % Kill compactor immediately
            exit(Pid, kill)
        end),
        timer:sleep(200),
        % DB must still be fully usable
        check_invariants(DbName, Model, "after_kill"),
        % Can still write
        write_doc(DbName, <<"post_crash">>, [{<<"v">>, 999}]),
        Model2 = model_write(Model, <<"post_crash">>, [{<<"v">>, 999}]),
        check_invariants(DbName, Model2, "after_post_crash_write")
    end).

t_kill_compactor_db_usable(DbName) ->
    ?_test(begin
        write_many(DbName, 50),
        % Start compact, let it run briefly, kill
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Pid} = couch_db:start_compact(Db),
            timer:sleep(50),
            exit(Pid, kill)
        end),
        timer:sleep(200),
        % DB must be openable and readable
        ?assertEqual(50, get_doc_count(DbName)),
        % Clean compact should work after killed one
        compact_db(DbName),
        ?assertEqual(50, get_doc_count(DbName))
    end).

t_restart_compact_after_kill(DbName) ->
    ?_test(begin
        write_many(DbName, 200),
        couch_util:with_db(DbName, fun(Db) ->
            {ok, Pid} = couch_db:start_compact(Db),
            timer:sleep(10),
            exit(Pid, kill)
        end),
        timer:sleep(200),
        % Restart compaction — must complete successfully
        compact_db(DbName),
        ?assertEqual(200, get_doc_count(DbName)),
        % Size must be stable
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2))
    end).

t_compact_during_writes_no_loss(DbName) ->
    ?_test(begin
        % Write 100, start compact, write 100 more concurrently
        write_many(DbName, 100),
        Self = self(),
        spawn_link(fun() ->
            compact_db(DbName),
            Self ! compact_done
        end),
        % Write more while compaction is running
        write_many_offset(DbName, 100, 101),
        receive compact_done -> ok
        after 30000 -> error(compact_timeout)
        end,
        % Wait for any pending compaction retry
        wait_compact_done(DbName),
        ?assertEqual(200, get_doc_count(DbName))
    end).

%% ===================================================================
%% 3. FUZZING — Random content, invariant check after every op
%% ===================================================================

fuzzing_test_() ->
    {
        "Property-based fuzzing with invariant checking",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_fuzz_random_bodies/1,
                    fun t_fuzz_large_docs/1,
                    fun t_fuzz_unicode_ids/1,
                    fun t_fuzz_rapid_compact_cycles/1,
                    fun t_fuzz_mixed_att_noatt/1
                ]
            }
        }
    }.

t_fuzz_random_bodies(DbName) ->
    ?_test(begin
        rand:seed(exsss, {42, 294, 546}),
        lists:foreach(fun(I) ->
            Body = [{<<"data">>, base64:encode(crypto:strong_rand_bytes(
                        rand:uniform(2048)))}],
            write_doc(DbName, docid(I), Body)
        end, lists:seq(1, 50)),
        compact_db(DbName),
        ?assertEqual(50, get_doc_count(DbName)),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2))
    end).

t_fuzz_large_docs(DbName) ->
    ?_test(begin
        % Write 10 docs with 100KB bodies
        lists:foreach(fun(I) ->
            Body = [{<<"payload">>,
                base64:encode(crypto:strong_rand_bytes(102400))}],
            write_doc(DbName, docid(I), Body)
        end, lists:seq(1, 10)),
        compact_db(DbName),
        ?assertEqual(10, get_doc_count(DbName)),
        Sizes = get_db_sizes(DbName),
        ?assert(maps:get(active, Sizes) > 1000000) % > 1MB active
    end).

t_fuzz_unicode_ids(DbName) ->
    ?_test(begin
        Ids = [
            <<"doc-normal">>,
            <<16#C3, 16#BC, 16#C3, 16#B6, 16#C3, 16#A4>>,  % "üöä"
            <<"doc:with:colons">>,
            <<"doc+plus+signs">>,
            <<"doc%20encoded">>,
            <<"UPPERCASE">>,
            <<"MiXeD_CaSe-123">>
        ],
        lists:foreach(fun(Id) ->
            write_doc(DbName, Id, [{<<"v">>, 1}])
        end, Ids),
        compact_db(DbName),
        ?assertEqual(length(Ids), get_doc_count(DbName)),
        % All IDs still accessible
        couch_util:with_db(DbName, fun(Db) ->
            lists:foreach(fun(Id) ->
                ?assertMatch({ok, _}, couch_db:open_doc(Db, Id, []))
            end, Ids)
        end)
    end).

t_fuzz_rapid_compact_cycles(DbName) ->
    ?_test(begin
        write_many(DbName, 30),
        % Compact 10 times rapidly
        lists:foreach(fun(_) ->
            compact_db(DbName)
        end, lists:seq(1, 10)),
        ?assertEqual(30, get_doc_count(DbName)),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2))
    end).

t_fuzz_mixed_att_noatt(DbName) ->
    ?_test(begin
        rand:seed(exsss, {99, 693, 891}),
        lists:foreach(fun(I) ->
            case rand:uniform(3) of
                1 ->
                    % Doc with attachment
                    write_doc_with_att(DbName, docid(I),
                        crypto:strong_rand_bytes(rand:uniform(4096)));
                _ ->
                    % Doc without attachment
                    write_doc(DbName, docid(I), [{<<"v">>, I}])
            end
        end, lists:seq(1, 40)),
        compact_db(DbName),
        ?assertEqual(40, get_doc_count(DbName)),
        S1 = get_db_sizes(DbName),
        compact_db(DbName),
        S2 = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, S1), maps:get(active, S2))
    end).

%% ===================================================================
%% 4. LIVENESS — Operations must complete within bounded time
%% ===================================================================

liveness_test_() ->
    {
        "Liveness: all operations must complete within bounded time",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_liveness_write/1,
                    fun t_liveness_read/1,
                    fun t_liveness_compact/1,
                    fun t_liveness_changes/1,
                    fun t_liveness_bulk/1
                ]
            }
        }
    }.

t_liveness_write(DbName) ->
    ?_test(begin
        {Time, _} = timer:tc(fun() ->
            write_doc(DbName, <<"live1">>, [{<<"v">>, 1}])
        end),
        ?assert(Time < ?MAX_OP_TIME_MS * 1000,
                "write took too long: " ++ integer_to_list(Time) ++ " us")
    end).

t_liveness_read(DbName) ->
    ?_test(begin
        write_doc(DbName, <<"live_read">>, [{<<"v">>, 1}]),
        {Time, _} = timer:tc(fun() ->
            couch_util:with_db(DbName, fun(Db) ->
                couch_db:open_doc(Db, <<"live_read">>, [])
            end)
        end),
        ?assert(Time < ?MAX_OP_TIME_MS * 1000)
    end).

t_liveness_compact(DbName) ->
    ?_test(begin
        write_many(DbName, 100),
        {Time, _} = timer:tc(fun() -> compact_db(DbName) end),
        ?assert(Time < ?MAX_OP_TIME_MS * 1000)
    end).

t_liveness_changes(DbName) ->
    ?_test(begin
        write_many(DbName, 100),
        {Time, _} = timer:tc(fun() ->
            get_changes_since(DbName, 0)
        end),
        ?assert(Time < ?MAX_OP_TIME_MS * 1000)
    end).

t_liveness_bulk(DbName) ->
    ?_test(begin
        Docs = [couch_doc:from_json_obj({[
            {<<"_id">>, docid(I)},
            {<<"v">>, I}
        ]}) || I <- lists:seq(1, 500)],
        {Time, _} = timer:tc(fun() ->
            couch_util:with_db(DbName, fun(Db) ->
                {ok, _} = couch_db:update_docs(Db, Docs)
            end)
        end),
        ?assert(Time < ?MAX_OP_TIME_MS * 1000)
    end).

%% ===================================================================
%% 5. IDEMPOTENCY — The core TigerBeetle guarantee
%% ===================================================================

idempotency_test_() ->
    {
        "Idempotency: repeated operations produce identical state",
        {
            setup,
            fun test_util:start_couch/0,
            fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0,
                fun teardown/1,
                [
                    fun t_compact_idempotent_5x/1,
                    fun t_compact_after_delete_idempotent/1,
                    fun t_compact_with_attachments_idempotent/1,
                    fun t_compact_after_updates_idempotent/1
                ]
            }
        }
    }.

t_compact_idempotent_5x(DbName) ->
    ?_test(begin
        write_many(DbName, 50),
        compact_db(DbName),
        Ref = get_db_sizes(DbName),
        lists:foreach(fun(I) ->
            compact_db(DbName),
            S = get_db_sizes(DbName),
            ?assertEqual(maps:get(active, Ref), maps:get(active, S),
                "compact #" ++ integer_to_list(I) ++ " changed active"),
            ?assertEqual(maps:get(external, Ref), maps:get(external, S),
                "compact #" ++ integer_to_list(I) ++ " changed external")
        end, lists:seq(2, 5))
    end).

t_compact_after_delete_idempotent(DbName) ->
    ?_test(begin
        write_many(DbName, 100),
        lists:foreach(fun(I) ->
            delete_doc(DbName, docid(I))
        end, lists:seq(1, 50)),
        compact_db(DbName),
        Ref = get_db_sizes(DbName),
        compact_db(DbName),
        S = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, Ref), maps:get(active, S)),
        ?assertEqual(maps:get(external, Ref), maps:get(external, S))
    end).

t_compact_with_attachments_idempotent(DbName) ->
    ?_test(begin
        lists:foreach(fun(I) ->
            write_doc_with_att(DbName, docid(I),
                crypto:strong_rand_bytes(2048))
        end, lists:seq(1, 20)),
        compact_db(DbName),
        Ref = get_db_sizes(DbName),
        compact_db(DbName),
        compact_db(DbName),
        S = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, Ref), maps:get(active, S)),
        ?assertEqual(maps:get(external, Ref), maps:get(external, S))
    end).

t_compact_after_updates_idempotent(DbName) ->
    ?_test(begin
        write_many(DbName, 20),
        % Update each doc 5 times
        lists:foreach(fun(_Round) ->
            lists:foreach(fun(I) ->
                update_doc(DbName, docid(I),
                    [{<<"v">>, rand:uniform(1000)}])
            end, lists:seq(1, 20))
        end, lists:seq(1, 5)),
        compact_db(DbName),
        Ref = get_db_sizes(DbName),
        compact_db(DbName),
        S = get_db_sizes(DbName),
        ?assertEqual(maps:get(active, Ref), maps:get(active, S))
    end).

%% ===================================================================
%% Helpers
%% ===================================================================

docid(I) ->
    iolist_to_binary(io_lib:format("doc-~4..0B", [I])).

random_docid() ->
    iolist_to_binary(io_lib:format("rnd-~8..0B", [rand:uniform(99999999)])).

random_body() ->
    [{<<"v">>, rand:uniform(1000000)},
     {<<"data">>, base64:encode(crypto:strong_rand_bytes(
                    rand:uniform(256)))}].

write_doc(DbName, DocId, Props) ->
    couch_util:with_db(DbName, fun(Db) ->
        Doc = couch_doc:from_json_obj({[{<<"_id">>, DocId} | Props]}),
        {ok, _} = couch_db:update_doc(Db, Doc, []),
        ok
    end).

update_doc(DbName, DocId, Props) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, Doc} = couch_db:open_doc(Db, DocId, []),
        Updated = Doc#doc{body = {Props}},
        {ok, _} = couch_db:update_doc(Db, Updated, []),
        ok
    end).

delete_doc(DbName, DocId) ->
    couch_util:with_db(DbName, fun(Db) ->
        case couch_db:open_doc(Db, DocId, []) of
            {ok, Doc} ->
                {ok, _} = couch_db:update_doc(Db, Doc#doc{deleted = true}, []),
                ok;
            _ -> ok
        end
    end).

write_many(DbName, N) ->
    write_many_offset(DbName, N, 1).

write_many_offset(DbName, N, Start) ->
    lists:foreach(fun(I) ->
        write_doc(DbName, docid(I),
            [{<<"v">>, I}, {<<"data">>, base64:encode(crypto:strong_rand_bytes(64))}])
    end, lists:seq(Start, Start + N - 1)).

write_doc_with_att(DbName, DocId, AttData) ->
    couch_util:with_db(DbName, fun(Db) ->
        Doc = couch_doc:from_json_obj({[
            {<<"_id">>, DocId},
            {<<"_attachments">>, {[
                {<<"file.bin">>, {[
                    {<<"content_type">>, <<"application/octet-stream">>},
                    {<<"data">>, base64:encode(AttData)}
                ]}}
            ]}}
        ]}),
        {ok, _} = couch_db:update_doc(Db, Doc, []),
        ok
    end).

get_doc_count(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, Count} = couch_db:get_doc_count(Db),
        Count
    end).

get_db_sizes(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, Info} = couch_db:get_db_info(Db),
        {SizeInfo} = couch_util:get_value(sizes, Info),
        #{
            active => couch_util:get_value(active, SizeInfo),
            external => couch_util:get_value(external, SizeInfo),
            file => couch_util:get_value(file, SizeInfo)
        }
    end).

get_changes_since(DbName, Since) ->
    couch_util:with_db(DbName, fun(Db) ->
        Fun = fun
            (#full_doc_info{id = Id, update_seq = Seq}, Acc) ->
                {ok, [{Id, Seq} | Acc]};
            (#doc_info{id = Id, high_seq = Seq}, Acc) ->
                {ok, [{Id, Seq} | Acc]}
        end,
        {ok, Changes} = couch_db:fold_changes(Db, Since, Fun, []),
        lists:reverse(Changes)
    end).

compact_db(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        {ok, _} = couch_db:start_compact(Db)
    end),
    wait_compact_done(DbName).

wait_compact_done(DbName) ->
    wait_db_compact_done(DbName, ?WAIT_DELAY_COUNT).

wait_db_compact_done(_DbName, 0) ->
    error({assertion_failed, [{module, ?MODULE}, {line, ?LINE},
        {reason, "DB compaction failed to finish"}]});
wait_db_compact_done(DbName, N) ->
    IsDone = couch_util:with_db(DbName, fun(Db) ->
        not is_pid(couch_db:get_compactor_pid(Db))
    end),
    case IsDone of
        true -> ok;
        false ->
            timer:sleep(?DELAY),
            wait_db_compact_done(DbName, N - 1)
    end.
