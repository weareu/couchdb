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

%% @doc Central space reservation service for all disk-consuming operations.
%%
%% Every operation that writes significant amounts of data to disk should
%% reserve space BEFORE starting and release it on completion. This prevents
%% thundering herd scenarios where concurrent operations (compaction, shard
%% splitting, index building) collectively exhaust disk space.
%%
%% Consumers:
%%   - smoosh (database + view compaction)
%%   - mem3_reshard (manual shard splitting)
%%   - mem3_auto_shard (automatic shard splitting)
%%   - fabric_rpc / chttpd (manual compaction via /_compact)
%%   - couch_index (view index builds)
%%
%% Each reservation is tagged with a unique identifier and monitored via
%% the calling process — if the process dies, the reservation is auto-released.
%%
%% Space estimation is conservative: we check available space from
%% couch_disk_monitor and subtract all active reservations.

-module(couch_space_monitor).
-behaviour(gen_server).

-export([
    start_link/0,
    reserve/3,
    release/1,
    check_available/2,
    reserved_on/1,
    total_reserved/0,
    reservations/0,
    status/0
]).

%% For testing
-export([
    effective_free/1,
    effective_free/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(TABLE, couch_space_reservations).

-record(reservation, {
    tag :: term(),
    node :: node(),
    bytes :: non_neg_integer(),
    pid :: pid(),
    monitor_ref :: reference(),
    created_at :: non_neg_integer(),
    description :: binary()
}).

%% ===================================================================
%% Public API
%% ===================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Reserve space on a node. Returns ok if the node has sufficient
%% free space (after subtracting existing reservations), or
%% {error, {insufficient_space, Available, Requested}} if not.
%%
%% The reservation is monitored — if the calling process dies, the
%% reservation is automatically released.
%%
%% Tag must be unique. Duplicate tags return {error, already_reserved}.
%%
%% Examples:
%%   reserve({compaction, <<"mydb">>}, node(), 1073741824)
%%   reserve({auto_split, <<"shards/00-ff/db.123">>}, 'n1@host', 60000000000)
%%   reserve({manual_compact, <<"mydb">>}, node(), 5368709120)
-spec reserve(term(), node(), non_neg_integer()) ->
    ok | {error, term()}.
reserve(Tag, Node, Bytes) when is_integer(Bytes), Bytes >= 0 ->
    gen_server:call(?MODULE, {reserve, Tag, Node, Bytes, self()}, 10000).

%% @doc Release a reservation by tag. Idempotent — releasing a
%% non-existent tag returns ok.
-spec release(term()) -> ok.
release(Tag) ->
    gen_server:call(?MODULE, {release, Tag}, 10000).

%% @doc Check if Bytes can be reserved on Node without actually reserving.
-spec check_available(node(), non_neg_integer()) -> boolean().
check_available(Node, Bytes) ->
    effective_free(Node) >= Bytes.

%% @doc Total bytes reserved on a specific node.
-spec reserved_on(node()) -> non_neg_integer().
reserved_on(Node) ->
    try
        ets:foldl(fun(#reservation{node = N, bytes = B}, Acc) ->
            case N =:= Node of true -> Acc + B; false -> Acc end
        end, 0, ?TABLE)
    catch
        error:badarg -> 0  % Table doesn't exist yet
    end.

%% @doc Total bytes reserved across all nodes.
-spec total_reserved() -> non_neg_integer().
total_reserved() ->
    try
        ets:foldl(fun(#reservation{bytes = B}, Acc) -> Acc + B end, 0, ?TABLE)
    catch
        error:badarg -> 0
    end.

%% @doc List all active reservations.
-spec reservations() -> [map()].
reservations() ->
    try
        ets:foldl(fun(#reservation{} = R, Acc) ->
            [#{
                tag => R#reservation.tag,
                node => R#reservation.node,
                bytes => R#reservation.bytes,
                pid => R#reservation.pid,
                created_at => R#reservation.created_at,
                description => R#reservation.description
            } | Acc]
        end, [], ?TABLE)
    catch
        error:badarg -> []
    end.

%% @doc Status summary for HTTP endpoints / debugging.
-spec status() -> map().
status() ->
    Res = reservations(),
    ByNode = lists:foldl(fun(#{node := N, bytes := B}, Acc) ->
        maps:put(N, maps:get(N, Acc, 0) + B, Acc)
    end, #{}, Res),
    #{
        total_reserved_bytes => total_reserved(),
        reservation_count => length(Res),
        by_node => ByNode,
        reservations => Res
    }.

%% @doc Effective free space on a node = raw free - reserved.
-spec effective_free(node()) -> non_neg_integer().
effective_free(Node) ->
    RawFree = raw_free(Node),
    Reserved = reserved_on(Node),
    max(0, RawFree - Reserved).

%% @doc Effective free space with explicit raw free (for testing / external caps).
-spec effective_free(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
effective_free(RawFree, Reserved) ->
    max(0, RawFree - Reserved).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    ets:new(?TABLE, [
        set,
        named_table,
        {keypos, #reservation.tag},
        {read_concurrency, true}
    ]),
    {ok, #{}}.

handle_call({reserve, Tag, Node, Bytes, CallerPid}, _From, State) ->
    case ets:lookup(?TABLE, Tag) of
        [_] ->
            {reply, {error, already_reserved}, State};
        [] ->
            Free = effective_free(Node),
            FloorBytes = config:get_integer(
                "space_monitor", "min_free_floor_bytes", 10000000000),
            case Free >= Bytes + FloorBytes of
                true ->
                    Ref = monitor(process, CallerPid),
                    R = #reservation{
                        tag = Tag,
                        node = Node,
                        bytes = Bytes,
                        pid = CallerPid,
                        monitor_ref = Ref,
                        created_at = erlang:system_time(millisecond),
                        description = tag_to_description(Tag)
                    },
                    ets:insert(?TABLE, R),
                    couch_log:info(
                        "couch_space_monitor: reserved ~B bytes on ~s for ~p "
                        "(~B bytes now reserved on node)",
                        [Bytes, Node, Tag, reserved_on(Node)]),
                    {reply, ok, maps:put(Ref, Tag, State)};
                false ->
                    couch_log:warning(
                        "couch_space_monitor: DENIED ~B bytes on ~s for ~p "
                        "(~B free, ~B reserved, ~B floor)",
                        [Bytes, Node, Tag, Free + reserved_on(Node),
                         reserved_on(Node), FloorBytes]),
                    {reply, {error, {insufficient_space,
                        #{available => Free, requested => Bytes,
                          reserved => reserved_on(Node),
                          floor => FloorBytes}}}, State}
            end
    end;

handle_call({release, Tag}, _From, State) ->
    NewState = do_release(Tag, State),
    {reply, ok, NewState};

handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    %% Auto-release when the reserving process dies
    case maps:get(Ref, State, undefined) of
        undefined ->
            {noreply, State};
        Tag ->
            couch_log:info(
                "couch_space_monitor: auto-releasing reservation ~p "
                "(process died)", [Tag]),
            {noreply, do_release(Tag, State)}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% ===================================================================
%% Internal
%% ===================================================================

do_release(Tag, State) ->
    case ets:lookup(?TABLE, Tag) of
        [#reservation{monitor_ref = Ref, bytes = Bytes, node = Node}] ->
            demonitor(Ref, [flush]),
            ets:delete(?TABLE, Tag),
            couch_log:info(
                "couch_space_monitor: released ~B bytes on ~s for ~p",
                [Bytes, Node, Tag]),
            maps:remove(Ref, State);
        [] ->
            State
    end.

%% Get raw free bytes for a node from couch_disk_monitor.
raw_free(Node) when Node =:= node() ->
    try
        Dirs = couch_disk_monitor:dir_capacities(),
        case Dirs of
            [] -> 0;
            _ -> lists:max([Free || {_Path, _Pct, Free, _Total} <- Dirs])
        end
    catch
        _:_ -> 0
    end;
raw_free(_RemoteNode) ->
    %% For remote nodes, we'd need RPC. For now, assume unlimited
    %% (remote checks happen via mem3_node_capacity for splits).
    infinity.

tag_to_description({compaction, Name}) ->
    iolist_to_binary(io_lib:format("database compaction: ~s", [Name]));
tag_to_description({view_compact, Name}) ->
    iolist_to_binary(io_lib:format("view compaction: ~s", [Name]));
tag_to_description({auto_split, Name}) ->
    iolist_to_binary(io_lib:format("auto shard split: ~s", [Name]));
tag_to_description({manual_split, Name}) ->
    iolist_to_binary(io_lib:format("manual shard split: ~s", [Name]));
tag_to_description({manual_compact, Name}) ->
    iolist_to_binary(io_lib:format("manual compaction: ~s", [Name]));
tag_to_description({index_build, Name}) ->
    iolist_to_binary(io_lib:format("index build: ~s", [Name]));
tag_to_description(Other) ->
    iolist_to_binary(io_lib:format("~p", [Other])).
