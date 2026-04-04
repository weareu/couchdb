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

%% @doc Multi-directory database path resolver.
%%
%% Allows database files to live on different mount points with three
%% levels of path resolution:
%%
%% 1. Per-database explicit mapping via [database_paths] config section.
%%    Supports exact matches and glob patterns:
%%      [database_paths]
%%      big_database = /mnt/nvme1
%%      archive_* = /mnt/hdd_array
%%      shards/*/users.* = /mnt/ssd
%%
%% 2. Automatic allocation via [couchdb] database_dirs config.
%%    When no explicit mapping matches, picks the directory with most
%%    free space from the configured list.
%%
%% 3. Fallback to single [couchdb] database_dir (backward compatible).
%%
%% Maintains an ETS registry of {DbName, FilePath} for O(1) lookups.
%% On startup, scans all configured directories to discover existing
%% database files and populate the registry.

-module(couch_multidir).

-export([
    init/0,
    is_enabled/0,
    resolve/3,
    allocate/3,
    register_path/3,
    unregister_path/2,
    all_dirs/0,
    scan_existing/0,
    lookup/1
]).

%% For testing
-export([
    match_path_rule/2,
    get_path_rules/0
]).

-define(ETS_TABLE, couch_multidir_registry).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Initialize the multidir system. Called from couch_server init.
-spec init() -> ok.
init() ->
    case ets:info(?ETS_TABLE) of
        undefined ->
            ets:new(?ETS_TABLE, [named_table, set, public, {read_concurrency, true}]);
        _ ->
            ok
    end,
    case is_enabled() of
        true -> scan_existing();
        false -> ok
    end,
    ok.

%% @doc Check if multi-directory mode is enabled.
%% Enabled when either database_dirs or database_paths is configured.
-spec is_enabled() -> boolean().
is_enabled() ->
    has_database_dirs() orelse has_path_rules().

%% @doc Resolve the file path for a database.
%% Checks: ETS registry -> path rules -> allocate on least-used dir.
-spec resolve(string(), string(), string()) -> string().
resolve(RootDir, DbName, Extension) ->
    case is_enabled() of
        false ->
            make_default_path(RootDir, DbName, Extension);
        true ->
            DbNameBin = to_binary(DbName),
            case ets:lookup(?ETS_TABLE, DbNameBin) of
                [{DbNameBin, FilePath}] ->
                    FilePath;
                [] ->
                    %% Not in registry — check path rules, then allocate
                    allocate(RootDir, DbName, Extension)
            end
    end.

%% @doc Allocate a path for a new database.
%% Priority: 1) path rules, 2) least-used dir, 3) default root.
-spec allocate(string(), string(), string()) -> string().
allocate(RootDir, DbName, Extension) ->
    DbNameStr = to_list(DbName),
    Dir = case match_path_rule(DbNameStr, get_path_rules()) of
        {ok, TargetDir} ->
            TargetDir;
        nomatch ->
            case couch_disk_monitor:least_used_dir() of
                {ok, LeastUsed} -> LeastUsed;
                {error, all_full} -> RootDir
            end
    end,
    FilePath = make_default_path(Dir, DbName, Extension),
    register_path(DbName, Extension, FilePath),
    FilePath.

%% @doc Register a database's file path in the registry.
-spec register_path(string() | binary(), string(), string()) -> ok.
register_path(DbName, _Extension, FilePath) ->
    DbNameBin = to_binary(DbName),
    ets:insert(?ETS_TABLE, {DbNameBin, FilePath}),
    ok.

%% @doc Remove a database from the registry.
-spec unregister_path(string() | binary(), string()) -> ok.
unregister_path(DbName, _Extension) ->
    DbNameBin = to_binary(DbName),
    ets:delete(?ETS_TABLE, DbNameBin),
    ok.

%% @doc Look up a database's registered path.
-spec lookup(binary()) -> {ok, string()} | not_found.
lookup(DbName) when is_binary(DbName) ->
    case ets:lookup(?ETS_TABLE, DbName) of
        [{DbName, FilePath}] -> {ok, FilePath};
        [] -> not_found
    end.

%% @doc Return all configured database directories.
-spec all_dirs() -> [string()].
all_dirs() ->
    couch_disk_monitor:all_dirs().

%% @doc Scan all configured directories for existing database files.
%% Populates the ETS registry.
-spec scan_existing() -> ok.
scan_existing() ->
    Dirs = all_dirs(),
    %% Also include directories from path rules
    RuleDirs = lists:usort([Dir || {_Pattern, Dir} <- get_path_rules()]),
    AllDirs = lists:usort(Dirs ++ RuleDirs),
    Extensions = get_engine_extensions(),
    lists:foreach(fun(Dir) ->
        scan_dir(Dir, Extensions)
    end, AllDirs),
    ok.

%% ===================================================================
%% Path rule matching
%% ===================================================================

%% @doc Get configured path rules from [database_paths] section.
-spec get_path_rules() -> [{string(), string()}].
get_path_rules() ->
    case config:get("database_paths") of
        undefined -> [];
        [] -> [];
        Entries ->
            [{Pattern, Dir} || {Pattern, Dir} <- Entries, Dir =/= ""]
    end.

%% @doc Match a database name against path rules.
%% Returns {ok, Dir} if a rule matches, nomatch otherwise.
%% Rules are checked in order; first match wins.
-spec match_path_rule(string(), [{string(), string()}]) ->
    {ok, string()} | nomatch.
match_path_rule(_DbName, []) ->
    nomatch;
match_path_rule(DbName, [{Pattern, Dir} | Rest]) ->
    case glob_match(DbName, Pattern) of
        true -> {ok, Dir};
        false -> match_path_rule(DbName, Rest)
    end.

%% Simple glob matching: * matches any sequence of characters.
%% No recursive ** support needed for database names.
glob_match(String, Pattern) ->
    RegExp = glob_to_regexp(Pattern),
    case re:run(String, RegExp) of
        {match, _} -> true;
        nomatch -> false
    end.

glob_to_regexp(Pattern) ->
    %% Escape regex special chars, then convert * to .*
    Escaped = re:replace(Pattern, "[.+?^${}()|\\[\\]\\\\]", "\\\\&",
        [global, {return, list}]),
    WithStar = re:replace(Escaped, "\\*", ".*", [global, {return, list}]),
    "^" ++ WithStar ++ "$".

%% ===================================================================
%% Internal
%% ===================================================================

has_database_dirs() ->
    case config:get("couchdb", "database_dirs") of
        undefined -> false;
        "" -> false;
        _ -> true
    end.

has_path_rules() ->
    case config:get("database_paths") of
        undefined -> false;
        [] -> false;
        _ -> true
    end.

scan_dir(Dir, Extensions) ->
    ExtRegExp = "\\." ++ "(" ++ string:join(Extensions, "|") ++ ")" ++ "$",
    try
        couch_util:fold_files(Dir, ExtRegExp, true,
            fun(FilePath, ok) ->
                NormDir = couch_util:normpath(Dir),
                NormFile = couch_util:normpath(FilePath),
                RelPath = case NormFile -- NormDir of
                    [$/ | Rel] -> Rel;
                    Rel -> Rel
                end,
                Ext = filename:extension(RelPath),
                DbName = list_to_binary(filename:rootname(RelPath, Ext)),
                %% Only register if not already in registry
                %% (first directory wins for conflicts)
                case ets:lookup(?ETS_TABLE, DbName) of
                    [] ->
                        ets:insert(?ETS_TABLE, {DbName, FilePath});
                    _ ->
                        ok
                end,
                ok
            end, ok)
    catch
        _:_ ->
            %% Directory may not exist yet
            ok
    end.

get_engine_extensions() ->
    case config:get("couchdb_engines") of
        [] -> ["couch"];
        Entries -> [Ext || {Ext, _Mod} <- Entries]
    end.

make_default_path(RootDir, DbName, Extension) ->
    DbNameStr = to_list(DbName),
    ExtStr = to_list(Extension),
    filename:join([RootDir, "./" ++ DbNameStr ++ "." ++ ExtStr]).

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V) -> list_to_binary(V).

to_list(V) when is_list(V) -> V;
to_list(V) when is_binary(V) -> binary_to_list(V).
