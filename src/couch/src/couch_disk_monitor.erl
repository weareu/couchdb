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

-module(couch_disk_monitor).
-behaviour(gen_server).

% public api
-export([
    block_background_view_indexing/0,
    block_interactive_view_indexing/0,
    block_interactive_database_writes/0
]).

% multi-directory api (Phase 1 auto-shard)
-export([
    dir_capacities/0,
    least_used_dir/0,
    all_dirs/0
]).

% testing
-export([
    set_database_dir_percent_used/1,
    set_view_index_dir_percent_used/1
]).

% gen_server callbacks
-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("kernel/include/file.hrl").

-define(SECTION, "disk_monitor").

-record(st, {
    timer
}).

block_background_view_indexing() ->
    Enabled = enabled(),
    if
        Enabled ->
            view_index_dir_percent_used() > background_view_indexing_threshold();
        true ->
            false
    end.

block_interactive_view_indexing() ->
    Enabled = enabled(),
    if
        Enabled -> view_index_dir_percent_used() > interactive_view_indexing_threshold();
        true -> false
    end.

block_interactive_database_writes() ->
    Enabled = enabled(),
    if
        Enabled -> database_dir_percent_used() > interactive_database_writes_threshold();
        true -> false
    end.

%% @doc Get capacity info for all configured database directories.
%% Returns [{Path, PercentUsed, FreeBytes, TotalBytes}].
-spec dir_capacities() -> [{string(), integer(), non_neg_integer(), non_neg_integer()}].
dir_capacities() ->
    try ets:lookup(?MODULE, dir_capacities) of
        [{dir_capacities, Caps}] -> Caps;
        [] -> []
    catch
        error:badarg -> []
    end.

%% @doc Return the directory with the most free space.
-spec least_used_dir() -> {ok, string()} | {error, all_full}.
least_used_dir() ->
    case dir_capacities() of
        [] ->
            %% Fallback to single database_dir
            Dir = config:get("couchdb", "database_dir", "."),
            {ok, Dir};
        Caps ->
            %% Filter dirs below 90% usage, pick one with most free bytes
            Available = [{Free, Path} || {Path, Pct, Free, _Total} <- Caps, Pct < 90],
            case Available of
                [] -> {error, all_full};
                _ -> {ok, element(2, lists:max(Available))}
            end
    end.

%% @doc Return all configured database directories.
-spec all_dirs() -> [string()].
all_dirs() ->
    case config:get("couchdb", "database_dirs") of
        undefined -> [config:get("couchdb", "database_dir", ".")];
        "" -> [config:get("couchdb", "database_dir", ".")];
        DirsStr ->
            Dirs = string:tokens(DirsStr, ","),
            Trimmed = [string:trim(D) || D <- Dirs],
            case Trimmed of
                [] -> [config:get("couchdb", "database_dir", ".")];
                _ -> Trimmed
            end
    end.

set_database_dir_percent_used(PercentUsed) when PercentUsed >= 0, PercentUsed =< 100 ->
    gen_server:call(?MODULE, {set_database_dir_percent_used, PercentUsed}).

set_view_index_dir_percent_used(PercentUsed) when PercentUsed >= 0, PercentUsed =< 100 ->
    gen_server:call(?MODULE, {set_view_index_dir_percent_used, PercentUsed}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    ets:new(?MODULE, [named_table, {read_concurrency, true}]),
    ets:insert(?MODULE, {database_dir, 0}),
    ets:insert(?MODULE, {view_index_dir, 0}),
    ets:insert(?MODULE, {dir_capacities, []}),
    update_disk_data(),
    TRef = erlang:send_after(timer_interval(), self(), update_disk_data),
    {ok, #st{timer = TRef}}.

handle_call({set_database_dir_percent_used, PercentUsed}, _From, St) ->
    ets:insert(?MODULE, {database_dir, PercentUsed}),
    {reply, ok, St};
handle_call({set_view_index_dir_percent_used, PercentUsed}, _From, St) ->
    ets:insert(?MODULE, {view_index_dir, PercentUsed}),
    {reply, ok, St};
handle_call(_Msg, _From, St) ->
    {reply, {error, unknown_msg}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(update_disk_data, St) ->
    erlang:cancel_timer(St#st.timer),
    update_disk_data(),
    TRef = erlang:send_after(timer_interval(), self(), update_disk_data),
    {noreply, St#st{timer = TRef}};
handle_info(_Msg, St) ->
    {noreply, St}.

update_disk_data() ->
    DiskData = disksup:get_disk_data(),
    update_disk_data(DiskData),
    update_dir_capacities(DiskData).

update_disk_data([]) ->
    ok;
update_disk_data([{Id, _, PercentUsed} | Rest]) ->
    IsDatabaseDir = is_database_dir(Id),
    IsViewIndexDir = is_view_index_dir(Id),
    if
        IsDatabaseDir ->
            ets:insert(?MODULE, {database_dir, PercentUsed});
        true ->
            ok
    end,
    if
        IsViewIndexDir ->
            ets:insert(?MODULE, {view_index_dir, PercentUsed});
        true ->
            ok
    end,
    update_disk_data(Rest).

%% Update capacity info for all configured database directories.
%% disksup returns {MountPoint, TotalKBytes, PercentUsed}.
update_dir_capacities(DiskData) ->
    Dirs = all_dirs(),
    Caps = lists:filtermap(fun(Dir) ->
        case find_disk_for_dir(Dir, DiskData) of
            {ok, {_MntOn, TotalKB, PctUsed}} ->
                TotalBytes = TotalKB * 1024,
                FreeBytes = TotalBytes * (100 - PctUsed) div 100,
                {true, {Dir, PctUsed, FreeBytes, TotalBytes}};
            not_found ->
                false
        end
    end, Dirs),
    ets:insert(?MODULE, {dir_capacities, Caps}).

%% Find the disk entry that contains a given directory.
%% Matches by device ID (same physical device).
find_disk_for_dir(Dir, DiskData) ->
    DirDevId = device_id(Dir),
    find_disk_for_dir(Dir, DiskData, DirDevId).

find_disk_for_dir(_Dir, [], _DirDevId) ->
    not_found;
find_disk_for_dir(Dir, [{MntOn, TotalKB, PctUsed} | Rest], DirDevId) ->
    case device_id(MntOn) of
        DirDevId when DirDevId =/= {error, enoent} ->
            {ok, {MntOn, TotalKB, PctUsed}};
        _ ->
            find_disk_for_dir(Dir, Rest, DirDevId)
    end.

is_database_dir(MntOn) ->
    same_device(config:get("couchdb", "database_dir"), MntOn).

is_view_index_dir(MntOn) ->
    same_device(config:get("couchdb", "view_index_dir"), MntOn).

same_device(DirA, DirB) ->
    case {device_id(DirA), device_id(DirB)} of
        {{ok, DeviceId}, {ok, DeviceId}} ->
            true;
        _Else ->
            false
    end.

device_id(Dir) ->
    case file:read_file_info(Dir) of
        {ok, FileInfo} ->
            {ok, {FileInfo#file_info.minor_device, FileInfo#file_info.major_device}};
        {error, Reason} ->
            {error, Reason}
    end.

database_dir_percent_used() ->
    [{database_dir, PercentUsed}] = ets:lookup(?MODULE, database_dir),
    PercentUsed.

view_index_dir_percent_used() ->
    [{view_index_dir, PercentUsed}] = ets:lookup(?MODULE, view_index_dir),
    PercentUsed.

background_view_indexing_threshold() ->
    config:get_integer(?SECTION, "background_view_indexing_threshold", 80).

interactive_view_indexing_threshold() ->
    config:get_integer(?SECTION, "interactive_view_indexing_threshold", 90).

interactive_database_writes_threshold() ->
    config:get_integer(?SECTION, "interactive_database_writes_threshold", 90).

enabled() ->
    config:get_boolean(?SECTION, "enable", true).

%% Align our refresh with the os_mon refresh
timer_interval() ->
    disksup:get_check_interval().

-ifdef(TEST).
-include_lib("couch/include/couch_eunit.hrl").

%% Note: enabled() now defaults to true (changed from false).
%% Existing deployments that relied on it being disabled must
%% explicitly set enable=false if they don't want disk monitoring.

setup_all() ->
    Ctx = test_util:start_couch(),
    config:set_boolean("disk_monitor", "enable", true, _Persist = false),
    Ctx.

teardown_all(Ctx) ->
    test_util:stop_couch(Ctx).

setup() ->
    ok.

teardown(_) ->
    ok.

enabled_test_() ->
    {
        setup,
        fun setup_all/0,
        fun teardown_all/1,
        {
            foreach,
            fun setup/0,
            fun teardown/1,
            [
                ?TDEF_FE(block_background_view_indexing_test),
                ?TDEF_FE(block_interactive_view_indexing_test),
                ?TDEF_FE(block_interactive_database_writes_test),
                ?TDEF_FE(update_disk_data_test),
                ?TDEF_FE(dir_capacities_test),
                ?TDEF_FE(least_used_dir_test),
                ?TDEF_FE(all_dirs_default_test),
                ?TDEF_FE(all_dirs_multi_test)
            ]
        }
    }.

block_background_view_indexing_test(_) ->
    ?assert(enabled()),
    set_view_index_dir_percent_used(80),
    ?assertEqual(false, block_background_view_indexing()),
    set_view_index_dir_percent_used(81),
    ?assertEqual(true, block_background_view_indexing()).

block_interactive_view_indexing_test(_) ->
    ?assert(enabled()),
    set_view_index_dir_percent_used(90),
    ?assertEqual(false, block_interactive_view_indexing()),
    set_view_index_dir_percent_used(91),
    ?assertEqual(true, block_interactive_view_indexing()).

block_interactive_database_writes_test(_) ->
    ?assert(enabled()),
    set_database_dir_percent_used(90),
    ?assertEqual(false, block_interactive_database_writes()),
    set_database_dir_percent_used(91),
    ?assertEqual(true, block_interactive_database_writes()).

update_disk_data_test(_) ->
    whereis(?MODULE) ! update_disk_data,
    ?assertEqual({error, unknown_msg}, gen_server:call(?MODULE, foo)).

dir_capacities_test(_) ->
    %% After init, dir_capacities should be a list (may be empty if
    %% disksup doesn't match the configured dir's device)
    Caps = dir_capacities(),
    ?assert(is_list(Caps)),
    %% If there are entries, verify structure
    lists:foreach(fun({Path, Pct, Free, Total}) ->
        ?assert(is_list(Path)),
        ?assert(is_integer(Pct)),
        ?assert(Pct >= 0 andalso Pct =< 100),
        ?assert(is_integer(Free)),
        ?assert(Free >= 0),
        ?assert(is_integer(Total)),
        ?assert(Total >= 0)
    end, Caps).

least_used_dir_test(_) ->
    %% Should always return a directory (fallback to database_dir)
    Result = least_used_dir(),
    case Result of
        {ok, Dir} -> ?assert(is_list(Dir));
        {error, all_full} -> ok
    end.

all_dirs_default_test(_) ->
    %% Without database_dirs config, returns single database_dir
    config:delete("couchdb", "database_dirs", false),
    Dirs = all_dirs(),
    ?assertEqual(1, length(Dirs)).

all_dirs_multi_test(_) ->
    %% With database_dirs config, returns multiple directories
    config:set("couchdb", "database_dirs", "/data1,/data2,/data3", false),
    try
        Dirs = all_dirs(),
        ?assertEqual(3, length(Dirs)),
        ?assertEqual("/data1", lists:nth(1, Dirs)),
        ?assertEqual("/data2", lists:nth(2, Dirs)),
        ?assertEqual("/data3", lists:nth(3, Dirs))
    after
        config:delete("couchdb", "database_dirs", false)
    end.

-endif.
