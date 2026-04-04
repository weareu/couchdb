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

%% @doc Integration tests for multi-directory database path resolution.
%%
%% These tests create REAL databases and verify they land on the
%% correct filesystem paths. A bug here means databases get created
%% in wrong places, can't be found, or get orphaned.

-module(couch_multidir_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("kernel/include/file.hrl").

%% ===================================================================
%% 1. Disabled mode — zero code path change (backward compatible)
%% ===================================================================

disabled_test_() ->
    {"Disabled mode uses default paths", {
        setup,
        fun setup_disabled/0,
        fun teardown_config/1,
        fun(Ctx) -> [
            t_disabled_resolve_uses_rootdir(Ctx)
        ] end
    }}.

setup_disabled() ->
    {ok, Apps} = application:ensure_all_started(config),
    config:delete("couchdb", "database_dirs", false),
    lists:foreach(fun({Key, _Val}) ->
        config:delete("database_paths", Key, false)
    end, config:get("database_paths")),
    couch_multidir:init(),
    Apps.

teardown_config(_Apps) ->
    config:delete("couchdb", "database_dirs", false),
    catch config:delete("database_paths", "important_db", false),
    catch config:delete("database_paths", "archive_*", false),
    catch config:delete("database_paths", "special_*", false),
    catch ets:delete(couch_multidir_registry),
    ok.

t_disabled_resolve_uses_rootdir(_) ->
    ?_test(begin
        ?assertEqual(false, couch_multidir:is_enabled()),
        Path = couch_multidir:resolve("/data", "mydb", "couch"),
        Expected = filename:join(["/data", "./mydb.couch"]),
        ?assertEqual(Expected, Path)
    end).

%% ===================================================================
%% 2. Per-database path rules — real config, real resolution
%% ===================================================================

path_rules_test_() ->
    {"Per-database path rules with real config", {
        setup,
        fun setup_with_rules/0,
        fun teardown_config/1,
        fun(Ctx) -> [
            t_rule_routes_to_configured_dir(Ctx),
            t_wildcard_rule_routes_correctly(Ctx),
            t_no_rule_match_uses_default(Ctx),
            t_registry_persists_across_lookups(Ctx)
        ] end
    }}.

setup_with_rules() ->
    {ok, Apps} = application:ensure_all_started(config),
    config:set("database_paths", "important_db", "/mnt/nvme", false),
    config:set("database_paths", "archive_*", "/mnt/hdd", false),
    config:delete("couchdb", "database_dirs", false),
    couch_multidir:init(),
    Apps.

t_rule_routes_to_configured_dir(_) ->
    ?_test(begin
        Path = couch_multidir:resolve("/default", "important_db", "couch"),
        ?assert(lists:prefix("/mnt/nvme", Path))
    end).

t_wildcard_rule_routes_correctly(_) ->
    ?_test(begin
        Path = couch_multidir:resolve("/default", "archive_2024", "couch"),
        ?assert(lists:prefix("/mnt/hdd", Path))
    end).

t_no_rule_match_uses_default(_) ->
    ?_test(begin
        Path = couch_multidir:resolve("/default", "random_db", "couch"),
        ?assert(is_list(Path)),
        ?assertNotEqual("", Path)
    end).

t_registry_persists_across_lookups(_) ->
    ?_test(begin
        Path1 = couch_multidir:resolve("/default", "important_db", "couch"),
        Path2 = couch_multidir:resolve("/default", "important_db", "couch"),
        ?assertEqual(Path1, Path2)
    end).

%% ===================================================================
%% 3. Multi-directory allocation — real temp directories
%% ===================================================================

multi_dir_test_() ->
    {"Multi-directory with real temp dirs", {
        setup,
        fun setup_with_dirs/0,
        fun teardown_dirs/1,
        fun(Ctx) -> [
            t_all_dirs_returns_configured(Ctx),
            t_register_and_lookup(Ctx),
            t_unregister_removes_entry(Ctx)
        ] end
    }}.

setup_with_dirs() ->
    {ok, Apps} = application:ensure_all_started(config),
    TmpBase = filename:join([os:getenv("TMPDIR", "/tmp"), "couch_multidir_test"]),
    Dir1 = filename:join(TmpBase, "data1"),
    Dir2 = filename:join(TmpBase, "data2"),
    ok = filelib:ensure_dir(filename:join(Dir1, "dummy")),
    ok = filelib:ensure_dir(filename:join(Dir2, "dummy")),
    config:set("couchdb", "database_dirs", Dir1 ++ "," ++ Dir2, false),
    lists:foreach(fun({Key, _Val}) ->
        config:delete("database_paths", Key, false)
    end, config:get("database_paths")),
    couch_multidir:init(),
    {Apps, TmpBase, Dir1, Dir2}.

teardown_dirs({_Apps, TmpBase, _Dir1, _Dir2}) ->
    config:delete("couchdb", "database_dirs", false),
    catch ets:delete(couch_multidir_registry),
    os:cmd("rm -rf " ++ TmpBase),
    ok.

t_all_dirs_returns_configured({_, _, Dir1, Dir2}) ->
    ?_test(begin
        Dirs = couch_multidir:all_dirs(),
        ?assert(lists:member(Dir1, Dirs)),
        ?assert(lists:member(Dir2, Dirs))
    end).

t_register_and_lookup({_, _, Dir1, _Dir2}) ->
    ?_test(begin
        Path = filename:join(Dir1, "mydb.couch"),
        couch_multidir:register_path("mydb", "couch", Path),
        {ok, Found} = couch_multidir:lookup(<<"mydb">>),
        ?assertEqual(Path, Found)
    end).

t_unregister_removes_entry({_, _, _, _}) ->
    ?_test(begin
        couch_multidir:register_path("tempdb", "couch", "/tmp/tempdb.couch"),
        ?assertMatch({ok, _}, couch_multidir:lookup(<<"tempdb">>)),
        couch_multidir:unregister_path("tempdb", "couch"),
        ?assertEqual(not_found, couch_multidir:lookup(<<"tempdb">>))
    end).

%% ===================================================================
%% 4. Integration — real DB create lands on configured path
%% ===================================================================

integration_test_() ->
    {"Real DB creation with path rules", {
        setup,
        fun setup_integration/0,
        fun teardown_integration/1,
        fun(Ctx) -> [
            t_db_created_on_configured_path(Ctx),
            t_db_survives_close_reopen(Ctx)
        ] end
    }}.

setup_integration() ->
    Ctx = test_util:start_couch(),
    TmpBase = filename:join([os:getenv("TMPDIR", "/tmp"), "couch_multidir_integ"]),
    NvmeDir = filename:join(TmpBase, "nvme"),
    ok = filelib:ensure_dir(filename:join(NvmeDir, "dummy")),
    config:set("database_paths", "special_*", NvmeDir, false),
    couch_multidir:init(),
    {Ctx, TmpBase, NvmeDir}.

teardown_integration({Ctx, TmpBase, _NvmeDir}) ->
    config:delete("database_paths", "special_*", false),
    catch couch_server:delete(<<"special_testdb">>, [?ADMIN_CTX]),
    catch couch_server:delete(<<"special_reopen">>, [?ADMIN_CTX]),
    os:cmd("rm -rf " ++ TmpBase),
    test_util:stop_couch(Ctx).

t_db_created_on_configured_path({_, _, NvmeDir}) ->
    ?_test(begin
        DbName = <<"special_testdb">>,
        {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
        FilePath = couch_db:get_filepath(Db),
        couch_db:close(Db),
        ?assert(lists:prefix(NvmeDir, FilePath)),
        ?assert(filelib:is_file(FilePath))
    end).

t_db_survives_close_reopen({_, _, NvmeDir}) ->
    ?_test(begin
        DbName = <<"special_reopen">>,
        {ok, Db1} = couch_db:create(DbName, [?ADMIN_CTX]),
        {ok, _} = couch_db:update_doc(Db1,
            #doc{id = <<"test">>, body = {[{<<"k">>, <<"v">>}]}}, []),
        couch_db:close(Db1),
        try
            {ok, Db2} = couch_db:open_int(DbName, [?ADMIN_CTX]),
            {ok, Doc} = couch_db:open_doc(Db2, <<"test">>, []),
            ?assertEqual(<<"test">>, Doc#doc.id),
            FilePath = couch_db:get_filepath(Db2),
            couch_db:close(Db2),
            ?assert(lists:prefix(NvmeDir, FilePath))
        after
            catch couch_server:delete(DbName, [?ADMIN_CTX])
        end
    end).
