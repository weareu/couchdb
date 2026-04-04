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

%% @doc Shard size cache — periodic O(1) scan of local shard file sizes.
%%
%% Maintains an ETS table of {ShardName, SizeBytes, Timestamp} for all
%% local shards. Size is read from DB file header via
%% couch_db_engine:get_size_info/1 (O(1) — no full scan).
%%
%% Used by mem3_auto_shard to find oversized shards for auto-splitting.

-module(mem3_shard_size).
-behaviour(gen_server).

-include_lib("couch/include/couch_db.hrl").

-export([
    start_link/0,
    get_size/1,
    get_sizes/0,
    get_sizes_over/1,
    refresh/0
]).

%% Exported for testing
-export([
    scan_local/1,
    scan_one/3,
    is_shard/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(ETS_TABLE, mem3_shard_size).
-define(DEFAULT_INTERVAL_MS, 300000). % 5 minutes

-record(state, {
    interval_ms :: pos_integer(),
    timer_ref :: undefined | reference(),
    scan_pid :: undefined | pid()
}).

%% ===================================================================
%% Public API
%% ===================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Get file size for a local shard.
-spec get_size(binary()) -> {ok, non_neg_integer()} | not_found.
get_size(ShardName) when is_binary(ShardName) ->
    try ets:lookup(?ETS_TABLE, ShardName) of
        [{ShardName, Size, _Ts}] -> {ok, Size};
        [] -> not_found
    catch
        error:badarg -> not_found
    end.

%% @doc Get all cached shard sizes.
-spec get_sizes() -> [{binary(), non_neg_integer()}].
get_sizes() ->
    try ets:tab2list(?ETS_TABLE) of
        Entries -> [{Name, Size} || {Name, Size, _Ts} <- Entries]
    catch
        error:badarg -> []
    end.

%% @doc Get all local shards exceeding the given size threshold.
-spec get_sizes_over(non_neg_integer()) -> [{binary(), non_neg_integer()}].
get_sizes_over(ThresholdBytes) when is_integer(ThresholdBytes), ThresholdBytes >= 0 ->
    try
        MatchSpec = [{{'$1', '$2', '_'}, [{'>', '$2', ThresholdBytes}], [{{'$1', '$2'}}]}],
        ets:select(?ETS_TABLE, MatchSpec)
    catch
        error:badarg -> []
    end.

%% @doc Force an immediate scan (async).
-spec refresh() -> ok.
refresh() ->
    gen_server:cast(?MODULE, refresh).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    ets:new(?ETS_TABLE, [named_table, set, public, {read_concurrency, true}]),
    IntervalMs = config:get_integer(
        "auto_shard", "size_scan_interval_ms", ?DEFAULT_INTERVAL_MS),
    State = #state{interval_ms = IntervalMs},
    {ok, schedule_scan(start_scan(State))}.

handle_call(get_state, _From, State) ->
    {reply, State, State};
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(refresh, State) ->
    {noreply, start_scan(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(scan, State) ->
    {noreply, schedule_scan(start_scan(State))};
handle_info({'DOWN', _Ref, process, Pid, _Reason}, #state{scan_pid = Pid} = State) ->
    {noreply, State#state{scan_pid = undefined}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% ===================================================================
%% Internal
%% ===================================================================

start_scan(#state{scan_pid = undefined} = State) ->
    Table = ?ETS_TABLE,
    {Pid, _Ref} = spawn_monitor(fun() -> scan_local(Table) end),
    State#state{scan_pid = Pid};
start_scan(State) ->
    % Scan already in progress
    State.

schedule_scan(#state{timer_ref = OldRef} = State) ->
    cancel_timer(OldRef),
    Ref = erlang:send_after(State#state.interval_ms, self(), scan),
    State#state{timer_ref = Ref}.

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

scan_local(Table) ->
    case catch couch_server:all_databases() of
        {ok, AllDbs} ->
            Now = erlang:system_time(millisecond),
            lists:foreach(fun(DbName) ->
                case is_shard(DbName) of
                    false -> ok;
                    true -> scan_one(Table, DbName, Now)
                end
            end, AllDbs);
        _Error ->
            ok
    end.

scan_one(Table, DbName, Now) ->
    try
        {ok, Db} = couch_db:open_int(DbName, [?ADMIN_CTX]),
        try
            SizeInfo = couch_db_engine:get_size_info(Db),
            FileSize = couch_util:get_value(file, SizeInfo, 0),
            ets:insert(Table, {DbName, FileSize, Now})
        after
            couch_db:close(Db)
        end
    catch
        _:_ ->
            % DB may have been deleted, compacted, or is otherwise
            % temporarily unavailable. Skip — next scan will retry.
            ok
    end.

is_shard(<<"shards/", _/binary>>) -> true;
is_shard(_) -> false.
