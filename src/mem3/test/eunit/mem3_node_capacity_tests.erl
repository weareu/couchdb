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

%% @doc Integration tests for node capacity tracking.
%%
%% These tests start the real gen_server and verify it collects
%% actual capacity data from the local node.

-module(mem3_node_capacity_tests).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% Real gen_server behavior
%% ===================================================================

capacity_test_() ->
    {"Node capacity gen_server", {
        setup,
        fun setup/0,
        fun teardown/1,
        fun(Ctx) -> [
            t_collects_local_capacity(Ctx),
            t_best_nodes_returns_current_node(Ctx),
            t_refresh_updates_data(Ctx),
            t_all_capacities_returns_map(Ctx),
            t_get_capacity_for_unknown_node(Ctx)
        ] end
    }}.

setup() ->
    {ok, Apps} = application:ensure_all_started(config),
    ok = config:set("auto_shard", "capacity_scan_interval_ms", "300000", false),
    {ok, Pid} = mem3_node_capacity:start_link(),
    timer:sleep(100), % Let initial scan complete
    {Pid, Apps}.

teardown({Pid, _Apps}) ->
    unlink(Pid),
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 -> ok
    end.

t_collects_local_capacity(_) ->
    ?_test(begin
        %% Force a synchronous refresh before reading so the capacity
        %% scan is guaranteed to have run at least once, then assert
        %% the local node is actually present. Silent skip on
        %% "undefined" would mask real regressions in local collection.
        ok = mem3_node_capacity:refresh(),
        Caps = wait_for_local_capacity(10),
        ?assert(is_map(Caps)),
        Cap = maps:get(node(), Caps),
        ?assert(is_map(Cap)),
        ?assert(maps:is_key(dirs, Cap)),
        ?assert(maps:is_key(shard_count, Cap)),
        ?assert(maps:is_key(zone, Cap)),
        SC = maps:get(shard_count, Cap),
        ?assert(is_integer(SC) andalso SC >= 0)
    end).

t_best_nodes_returns_current_node(_) ->
    ?_test(begin
        ok = mem3_node_capacity:refresh(),
        _ = wait_for_local_capacity(10),
        %% With local capacity present, best_nodes(Bytes, 1, []) should
        %% return EXACTLY the local node (we're single-node in eunit).
        Nodes = mem3_node_capacity:best_nodes(1000, 1, []),
        ?assertEqual([node()], Nodes)
    end).

t_refresh_updates_data(_) ->
    ?_test(begin
        ok = mem3_node_capacity:refresh(),
        _ = wait_for_local_capacity(10),
        %% After refresh, all_capacities must contain the local node
        %% with a populated updated_at timestamp.
        Caps = mem3_node_capacity:all_capacities(),
        Cap = maps:get(node(), Caps),
        UpdatedAt = maps:get(updated_at, Cap, 0),
        ?assert(is_integer(UpdatedAt) andalso UpdatedAt > 0),
        ?assert(is_pid(whereis(mem3_node_capacity)))
    end).

%% Poll all_capacities until the local node is present or we time out.
wait_for_local_capacity(0) ->
    error({timeout, waiting_for_local_capacity});
wait_for_local_capacity(Retries) ->
    Caps = mem3_node_capacity:all_capacities(),
    case maps:is_key(node(), Caps) of
        true -> Caps;
        false ->
            timer:sleep(50),
            wait_for_local_capacity(Retries - 1)
    end.

t_all_capacities_returns_map(_) ->
    ?_test(begin
        Result = mem3_node_capacity:all_capacities(),
        ?assert(is_map(Result))
    end).

t_get_capacity_for_unknown_node(_) ->
    ?_test(begin
        Result = mem3_node_capacity:get_capacity('nonexistent@nowhere'),
        ?assertEqual(not_found, Result)
    end).
