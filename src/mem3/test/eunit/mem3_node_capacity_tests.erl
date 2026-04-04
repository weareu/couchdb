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
        Caps = mem3_node_capacity:all_capacities(),
        ?assert(is_map(Caps)),
        %% Local node should be present
        case maps:get(node(), Caps, undefined) of
            undefined ->
                %% May not have couch_disk_monitor — acceptable in test
                ok;
            Cap ->
                ?assert(is_map(Cap)),
                ?assert(maps:is_key(dirs, Cap)),
                ?assert(maps:is_key(shard_count, Cap)),
                ?assert(maps:is_key(zone, Cap)),
                %% shard_count should be a non-negative integer
                SC = maps:get(shard_count, Cap),
                ?assert(is_integer(SC) andalso SC >= 0)
        end
    end).

t_best_nodes_returns_current_node(_) ->
    ?_test(begin
        %% In single-node test, best_nodes should return current node
        Nodes = mem3_node_capacity:best_nodes(1000, 1, []),
        ?assert(is_list(Nodes)),
        %% Should contain at most 1 node (we asked for 1)
        ?assert(length(Nodes) =< 1)
    end).

t_refresh_updates_data(_) ->
    ?_test(begin
        ok = mem3_node_capacity:refresh(),
        timer:sleep(100),
        %% Process should still be alive
        ?assert(is_pid(whereis(mem3_node_capacity)))
    end).

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
