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

%% @doc Auto-shard split orchestrator.
%%
%% Periodically scans for oversized shards and triggers splits via
%% mem3_reshard_rep:split/3. The trigger is absolute shard size
%% (configurable, default 20GB) — not disk fullness.
%%
%% Safety gates:
%%   - All circuit breakers must be closed (no splits during partition)
%%   - Coordinator election: only lowest lexicographic node runs scans
%%   - Per-database cooldown (1 hour default)
%%   - Max concurrent splits (2 default)
%%   - Maintenance window support
%%   - Exclusion patterns (INI config + per-DB design doc)
%%   - Pre-flight disk space check before each split

-module(mem3_auto_shard).
-behaviour(gen_server).

-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-export([
    start_link/0,
    status/0,
    pause/0,
    resume/0,
    trigger_scan/0,
    set_threshold/1
]).

%% For testing
-export([
    is_excluded/2,
    is_split_disabled_by_ddoc/1,
    is_coordinator/0,
    all_circuits_closed/0,
    load_config_for_test/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(DEFAULT_MAX_SHARD_SIZE, 20000000000). % 20 GB
-define(DEFAULT_SCAN_INTERVAL_MS, 600000).    % 10 minutes
-define(DEFAULT_MAX_CONCURRENT, 2).
-define(DEFAULT_MAX_SPLIT_FACTOR, 4).
-define(DEFAULT_COOLDOWN_MS, 3600000).        % 1 hour

-record(state, {
    enabled :: boolean(),
    max_shard_size_bytes :: pos_integer(),
    scan_interval_ms :: pos_integer(),
    max_concurrent_splits :: pos_integer(),
    max_split_factor :: pos_integer(),
    cooldown_ms :: pos_integer(),
    maintenance_window :: always | {non_neg_integer(), non_neg_integer()},
    paused :: boolean(),
    exclude_patterns :: [binary()],
    protected_dbs :: [binary()],
    active_splits :: #{binary() => pid()},
    cooldowns :: #{binary() => non_neg_integer()},
    timer_ref :: undefined | reference(),
    scan_count :: non_neg_integer(),
    splits_triggered :: non_neg_integer()
}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec status() -> map().
status() ->
    gen_server:call(?MODULE, status, 10000).

-spec pause() -> ok.
pause() ->
    gen_server:call(?MODULE, pause).

-spec resume() -> ok.
resume() ->
    gen_server:call(?MODULE, resume).

-spec trigger_scan() -> ok.
trigger_scan() ->
    gen_server:cast(?MODULE, trigger_scan).

-spec set_threshold(pos_integer()) -> ok.
set_threshold(Bytes) when is_integer(Bytes), Bytes > 0 ->
    gen_server:call(?MODULE, {set_threshold, Bytes}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    State = load_config(#state{
        paused = false,
        active_splits = #{},
        cooldowns = #{},
        scan_count = 0,
        splits_triggered = 0
    }),
    {ok, schedule_scan(State)}.

handle_call(status, _From, State) ->
    Reply = #{
        enabled => State#state.enabled,
        paused => State#state.paused,
        max_shard_size_bytes => State#state.max_shard_size_bytes,
        scan_interval_ms => State#state.scan_interval_ms,
        max_concurrent_splits => State#state.max_concurrent_splits,
        active_splits => maps:size(State#state.active_splits),
        active_split_shards => maps:keys(State#state.active_splits),
        cooldowns_active => maps:size(State#state.cooldowns),
        scan_count => State#state.scan_count,
        splits_triggered => State#state.splits_triggered,
        is_coordinator => is_coordinator(),
        maintenance_window => State#state.maintenance_window,
        exclude_patterns => State#state.exclude_patterns
    },
    {reply, Reply, State};
handle_call(pause, _From, State) ->
    couch_log:notice("mem3_auto_shard: paused by operator", []),
    {reply, ok, State#state{paused = true}};
handle_call(resume, _From, State) ->
    couch_log:notice("mem3_auto_shard: resumed by operator", []),
    {reply, ok, State#state{paused = false}};
handle_call({set_threshold, Bytes}, _From, State) ->
    couch_log:notice("mem3_auto_shard: threshold changed to ~B bytes", [Bytes]),
    {reply, ok, State#state{max_shard_size_bytes = Bytes}};
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(trigger_scan, State) ->
    {noreply, do_scan(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(scan, State) ->
    {noreply, schedule_scan(do_scan(State))};
handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    {noreply, handle_split_done(Pid, Reason, State)};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% ===================================================================
%% Scan logic
%% ===================================================================

do_scan(State) ->
    case should_scan(State) of
        false ->
            State;
        true ->
            State1 = prune_cooldowns(
                State#state{scan_count = State#state.scan_count + 1}),
            Oversized = mem3_shard_size:get_sizes_over(
                State1#state.max_shard_size_bytes),
            Candidates = filter_candidates(Oversized, State1),
            %% Sort by size descending — split largest first
            Sorted = lists:reverse(lists:keysort(2, Candidates)),
            Slots = State1#state.max_concurrent_splits -
                    maps:size(State1#state.active_splits),
            start_splits(lists:sublist(Sorted, max(0, Slots)), State1)
    end.

should_scan(#state{enabled = false}) -> false;
should_scan(#state{paused = true}) -> false;
should_scan(State) ->
    maps:size(State#state.active_splits) < State#state.max_concurrent_splits
    andalso is_coordinator()
    andalso all_circuits_closed()
    andalso in_maintenance_window(State#state.maintenance_window).

filter_candidates(Shards, State) ->
    Now = erlang:system_time(millisecond),
    lists:filter(fun({ShardName, _Size}) ->
        DbName = mem3:dbname(ShardName),
        not is_excluded(DbName, State) andalso
        not is_protected(DbName, State) andalso
        not is_in_cooldown(DbName, State, Now) andalso
        not is_already_splitting(ShardName, State) andalso
        not is_split_disabled_by_ddoc(DbName) andalso
        is_range_splittable(ShardName, State)
    end, Shards).

start_splits([], State) ->
    State;
start_splits([{ShardName, Size} | Rest], State) ->
    Factor = calculate_split_factor(Size, State#state.max_shard_size_bytes),
    Factor2 = min(Factor, State#state.max_split_factor),
    case do_start_split(ShardName, Factor2, State) of
        {ok, NewState} ->
            start_splits(Rest, NewState);
        {error, Reason} ->
            couch_log:warning(
                "mem3_auto_shard: cannot split ~s: ~p",
                [ShardName, Reason]),
            start_splits(Rest, State)
    end.

do_start_split(ShardName, Factor, State) ->
    try
        Shard = mem3_reshard:shard_from_name(ShardName),
        N = config:get_integer("cluster", "n", 3),
        TargetNodes = mem3_node_capacity:best_nodes(
            byte_size_to_int(ShardName, State) div Factor,
            N * Factor, []),
        case TargetNodes of
            [] ->
                {error, no_target_nodes};
            _ ->
                %% Pre-flight space check
                Caps = mem3_node_capacity:all_capacities(),
                case mem3_reshard_rep:preflight_check(
                        byte_size_to_int(ShardName, State), Factor, Caps) of
                    ok ->
                        {Pid, _Ref} = spawn_monitor(fun() ->
                            MaxMs = config:get_integer(
                                "auto_shard", "max_split_timeout_ms", 14400000),
                            {ok, TRef} = timer:exit_after(MaxMs, self(), split_timeout),
                            Result = mem3_reshard_rep:split(Shard, Factor, TargetNodes),
                            timer:cancel(TRef),
                            exit({split_result, ShardName, Result})
                        end),
                        DbName = mem3:dbname(ShardName),
                        Now = erlang:system_time(millisecond),
                        NewState = State#state{
                            active_splits = maps:put(ShardName, Pid,
                                State#state.active_splits),
                            cooldowns = maps:put(DbName, Now,
                                State#state.cooldowns),
                            splits_triggered = State#state.splits_triggered + 1
                        },
                        couch_log:notice(
                            "mem3_auto_shard: started ~B-way split of ~s (~B bytes)",
                            [Factor, ShardName, byte_size_to_int(ShardName, State)]),
                        {ok, NewState};
                    {error, _} = Err ->
                        Err
                end
        end
    catch
        _:Error ->
            {error, Error}
    end.

byte_size_to_int(ShardName, _State) ->
    case mem3_shard_size:get_size(ShardName) of
        {ok, Size} -> Size;
        not_found -> 0
    end.

handle_split_done(Pid, Reason, State) ->
    %% Find which shard this pid was splitting
    case maps:fold(fun(Shard, P, Acc) ->
        case P =:= Pid of true -> Shard; false -> Acc end
    end, undefined, State#state.active_splits) of
        undefined ->
            State;
        ShardName ->
            NewActive = maps:remove(ShardName, State#state.active_splits),
            case Reason of
                {split_result, ShardName, ok} ->
                    couch_log:notice(
                        "mem3_auto_shard: split completed for ~s", [ShardName]);
                {split_result, ShardName, {error, Err}} ->
                    couch_log:error(
                        "mem3_auto_shard: split FAILED for ~s: ~p",
                        [ShardName, Err]);
                Other ->
                    couch_log:error(
                        "mem3_auto_shard: split process died for ~s: ~p",
                        [ShardName, Other])
            end,
            State#state{active_splits = NewActive}
    end.

%% ===================================================================
%% Coordinator election
%% ===================================================================

%% Only the lowest lexicographic node in the cluster runs scans.
%% This prevents duplicate split jobs across nodes.
-spec is_coordinator() -> boolean().
is_coordinator() ->
    try
        Nodes = lists:sort(mem3:nodes()),
        case Nodes of
            [] -> true;
            [First | _] -> First =:= node()
        end
    catch
        _:_ -> true  % If mem3 not available, assume we're the only node
    end.

%% ===================================================================
%% Circuit breaker integration
%% ===================================================================

-spec all_circuits_closed() -> boolean().
all_circuits_closed() ->
    try
        Nodes = mem3:nodes(),
        not lists:any(fun(N) ->
            case mem3_circuit_breaker:allow(N) of
                {error, circuit_open} -> true;
                _ -> false
            end
        end, Nodes)
    catch
        _:_ -> true
    end.

%% ===================================================================
%% Maintenance window
%% ===================================================================

-spec in_maintenance_window(always | {non_neg_integer(), non_neg_integer()}) -> boolean().
in_maintenance_window(always) ->
    true;
in_maintenance_window({StartHour, EndHour}) ->
    {_, {Hour, _, _}} = calendar:universal_time(),
    case StartHour =< EndHour of
        true -> Hour >= StartHour andalso Hour < EndHour;
        false -> Hour >= StartHour orelse Hour < EndHour
    end.

%% ===================================================================
%% Exclusion logic
%% ===================================================================

-spec is_excluded(binary(), #state{}) -> boolean().
is_excluded(DbName, #state{exclude_patterns = Patterns}) ->
    lists:any(fun(Pattern) ->
        glob_match(DbName, Pattern)
    end, Patterns).

is_protected(DbName, #state{protected_dbs = Protected}) ->
    lists:member(DbName, Protected).

is_in_cooldown(DbName, #state{cooldowns = Cooldowns, cooldown_ms = CooldownMs}, Now) ->
    case maps:get(DbName, Cooldowns, undefined) of
        undefined -> false;
        LastSplit -> (Now - LastSplit) < CooldownMs
    end.

is_already_splitting(ShardName, #state{active_splits = Active}) ->
    maps:is_key(ShardName, Active) orelse is_legacy_reshard_active(ShardName).

%% Check if the old mem3_reshard system has an active job for this shard.
%% Prevents conflicts between auto-split and manual split.
is_legacy_reshard_active(ShardName) ->
    try
        Jobs = mem3_reshard:jobs(),
        lists:any(fun(JobProps) ->
            Source = couch_util:get_value(source, JobProps, <<>>),
            State = couch_util:get_value(job_state, JobProps, <<>>),
            Source =:= ShardName andalso
            (State =:= <<"running">> orelse State =:= <<"new">>)
        end, Jobs)
    catch
        _:_ -> false
    end.

%% @doc Check if a database has auto-split disabled via design doc.
-spec is_split_disabled_by_ddoc(binary()) -> boolean().
is_split_disabled_by_ddoc(DbName) ->
    %% DbName might be a logical name ("mydb") or a shard name
    %% ("shards/00000000-ffffffff/mydb.1234567890").
    %% We need to open ANY local shard of this database to read the ddoc.
    try
        %% Try opening directly first (works for non-shard names in eunit)
        OpenName = case DbName of
            <<"shards/", _/binary>> ->
                %% It's a shard name — use it directly
                DbName;
            _ ->
                %% Logical DB name — try to find a local shard
                case catch mem3:shards(DbName) of
                    Shards when is_list(Shards), Shards =/= [] ->
                        LocalShards = [S || #shard{node = N} = S <- Shards,
                                            N =:= node()],
                        case LocalShards of
                            [#shard{name = SName} | _] -> SName;
                            [] -> DbName
                        end;
                    _ -> DbName
                end
        end,
        {ok, Db} = couch_db:open_int(OpenName, [?ADMIN_CTX]),
        try
            case couch_db:open_doc(Db, <<"_design/shard_config">>, []) of
                {ok, #doc{body = {Props}}} ->
                    case couch_util:get_value(<<"auto_split">>, Props) of
                        {SplitProps} ->
                            couch_util:get_value(<<"enabled">>, SplitProps) =:= false;
                        _ ->
                            false
                    end;
                _ ->
                    false
            end
        after
            couch_db:close(Db)
        end
    catch
        _:_ -> false
    end.

is_range_splittable(ShardName, #state{max_split_factor = MaxFactor}) ->
    try
        Shard = mem3_reshard:shard_from_name(ShardName),
        [B, E] = Shard#shard.range,
        (E - B + 1) >= MaxFactor
    catch
        _:_ -> false
    end.

%% ===================================================================
%% Split factor calculation
%% ===================================================================

-spec calculate_split_factor(non_neg_integer(), pos_integer()) -> pos_integer().
calculate_split_factor(SizeBytes, MaxShardSize) ->
    Raw = max(2, ceil(SizeBytes / MaxShardSize)),
    next_power_of_2(Raw).

-spec next_power_of_2(pos_integer()) -> pos_integer().
next_power_of_2(N) when N =< 2 -> 2;
next_power_of_2(N) ->
    round(math:pow(2, ceil(math:log2(N)))).

%% ===================================================================
%% Config
%% ===================================================================

load_config(State) ->
    Enabled = config:get_boolean("auto_shard", "enabled", false),
    MaxSize = config:get_integer("auto_shard", "max_shard_size_bytes",
        ?DEFAULT_MAX_SHARD_SIZE),
    ScanInterval = config:get_integer("auto_shard", "scan_interval_ms",
        ?DEFAULT_SCAN_INTERVAL_MS),
    MaxConcurrent = config:get_integer("auto_shard", "max_concurrent_splits",
        ?DEFAULT_MAX_CONCURRENT),
    MaxFactor = config:get_integer("auto_shard", "max_split_factor",
        ?DEFAULT_MAX_SPLIT_FACTOR),
    CooldownMs = config:get_integer("auto_shard", "cooldown_ms",
        ?DEFAULT_COOLDOWN_MS),
    Window = parse_maintenance_window(
        config:get("auto_shard", "maintenance_window", "always")),
    ExcludeStr = config:get("auto_shard", "exclude_dbs",
        "_users,_replicator,_global_changes"),
    ProtectedStr = config:get("auto_shard", "protected_dbs", "_dbs,_nodes"),
    State#state{
        enabled = Enabled,
        max_shard_size_bytes = MaxSize,
        scan_interval_ms = ScanInterval,
        max_concurrent_splits = MaxConcurrent,
        max_split_factor = MaxFactor,
        cooldown_ms = CooldownMs,
        maintenance_window = Window,
        exclude_patterns = parse_db_list(ExcludeStr),
        protected_dbs = parse_db_list(ProtectedStr)
    }.

parse_maintenance_window("always") -> always;
parse_maintenance_window(Str) ->
    %% Format: "HH-HH" or "HH:MM-HH:MM" (only hours used)
    case string:tokens(Str, "-") of
        [StartStr, EndStr] ->
            try
                Start = parse_hour(string:trim(StartStr)),
                End = parse_hour(string:trim(EndStr)),
                {Start, End}
            catch
                _:_ -> always
            end;
        _ ->
            always
    end.

parse_hour(Str) ->
    %% Accept "22", "22:00", "2:00"
    HourStr = case string:tokens(Str, ":") of
        [Hr | _] -> Hr;
        _ -> Str
    end,
    Hour = list_to_integer(HourStr),
    true = (Hour >= 0 andalso Hour =< 23),
    Hour.

parse_db_list(undefined) -> [];
parse_db_list("") -> [];
parse_db_list(Str) ->
    [list_to_binary(string:trim(S)) || S <- string:tokens(Str, ","),
                                        string:trim(S) =/= ""].

%% ===================================================================
%% Timer
%% ===================================================================

schedule_scan(#state{timer_ref = OldRef} = State) ->
    cancel_timer(OldRef),
    Ref = erlang:send_after(State#state.scan_interval_ms, self(), scan),
    State#state{timer_ref = Ref}.

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

%% For testing — load config into a state record without starting gen_server
load_config_for_test() ->
    load_config(#state{
        paused = false,
        active_splits = #{},
        cooldowns = #{},
        scan_count = 0,
        splits_triggered = 0
    }).

prune_cooldowns(#state{cooldowns = Cooldowns, cooldown_ms = CooldownMs} = State) ->
    Now = erlang:system_time(millisecond),
    Pruned = maps:filter(fun(_DbName, LastSplit) ->
        (Now - LastSplit) < CooldownMs
    end, Cooldowns),
    State#state{cooldowns = Pruned}.

%% ===================================================================
%% Glob matching
%% ===================================================================

glob_match(String, Pattern) when is_binary(String), is_binary(Pattern) ->
    glob_match(binary_to_list(String), binary_to_list(Pattern));
glob_match(String, Pattern) when is_list(String), is_binary(Pattern) ->
    glob_match(String, binary_to_list(Pattern));
glob_match(String, Pattern) when is_binary(String), is_list(Pattern) ->
    glob_match(binary_to_list(String), Pattern);
glob_match(String, Pattern) ->
    RegExp = couch_multidir:glob_to_regexp(Pattern),
    case re:run(String, RegExp) of
        {match, _} -> true;
        nomatch -> false
    end.
