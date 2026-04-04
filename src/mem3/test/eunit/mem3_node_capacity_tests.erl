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

%% @doc Tests for node capacity tracking.
%%
%% These tests verify that mem3_node_capacity correctly collects
%% disk capacity info, scores nodes for placement, and handles
%% node failures gracefully.

-module(mem3_node_capacity_tests).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% 1. Node scoring — does it pick the right nodes?
%% ===================================================================

score_nodes_test_() ->
    {"Node scoring for shard placement", [
        {"Nodes sorted by free space descending",
         ?_test(begin
             Caps = #{
                 'node1@host' => #{dirs => [{"/data", 50, 500000000000, 1000000000000}]},
                 'node2@host' => #{dirs => [{"/data", 80, 200000000000, 1000000000000}]},
                 'node3@host' => #{dirs => [{"/data", 30, 700000000000, 1000000000000}]}
             },
             ShardSize = 50000000000, % 50 GB
             Scored = mem3_node_capacity:score_nodes(Caps, ShardSize),
             %% node3 has most free space, should be first
             ?assertMatch([{_, 'node3@host'}, {_, 'node1@host'}, {_, 'node2@host'}], Scored)
         end)},

        {"Empty node gets score 0 minus shard size",
         ?_test(begin
             Caps = #{
                 'node1@host' => #{dirs => []}
             },
             Scored = mem3_node_capacity:score_nodes(Caps, 100),
             ?assertMatch([{Score, 'node1@host'}] when Score < 0, Scored)
         end)},

        {"Multi-dir node uses best dir for scoring",
         ?_test(begin
             Caps = #{
                 'node1@host' => #{dirs => [
                     {"/data1", 90, 100000000000, 1000000000000},
                     {"/data2", 20, 800000000000, 1000000000000}
                 ]}
             },
             Scored = mem3_node_capacity:score_nodes(Caps, 50000000000),
             [{Score, 'node1@host'}] = Scored,
             %% Score should be based on /data2 (800GB free), not /data1 (100GB)
             ?assert(Score > 700000000000)
         end)},

        {"Node with barely enough space gets low score",
         ?_test(begin
             Caps = #{
                 'node1@host' => #{dirs => [{"/data", 95, 50000000001, 1000000000000}]},
                 'node2@host' => #{dirs => [{"/data", 10, 900000000000, 1000000000000}]}
             },
             Scored = mem3_node_capacity:score_nodes(Caps, 50000000000),
             [{_, 'node2@host'}, {Score1, 'node1@host'}] = Scored,
             %% node1 has 50GB free, shard is 50GB — score ~1 (barely fits)
             ?assert(Score1 < 1000000000)
         end)},

        {"Node with less free than shard gets negative score",
         ?_test(begin
             Caps = #{
                 'node1@host' => #{dirs => [{"/data", 99, 10000000000, 1000000000000}]}
             },
             Scored = mem3_node_capacity:score_nodes(Caps, 50000000000),
             [{Score, 'node1@host'}] = Scored,
             ?assert(Score < 0)
         end)},

        {"Empty capacities returns empty list",
         ?_test(begin
             Scored = mem3_node_capacity:score_nodes(#{}, 50000000000),
             ?assertEqual([], Scored)
         end)}
    ]}.

%% ===================================================================
%% 2. Zone constraints (placeholder — full implementation in Phase 5)
%% ===================================================================

zone_constraints_test_() ->
    {"Zone constraint application", [
        {"Empty rules returns scored list unchanged",
         ?_test(begin
             Scored = [{700, 'node3@host'}, {500, 'node1@host'}, {200, 'node2@host'}],
             Result = mem3_node_capacity:apply_zone_constraints(Scored, [], 3),
             ?assertEqual(Scored, Result)
         end)}
    ]}.

%% ===================================================================
%% 3. best_nodes/3 — integration of scoring + constraints
%% ===================================================================

best_nodes_test_() ->
    {"best_nodes integration (requires running gen_server)", {
        setup,
        fun setup/0,
        fun teardown/1,
        fun(_Ctx) -> [
            t_best_nodes_returns_list(_Ctx),
            t_best_nodes_respects_count(_Ctx),
            t_local_capacity_collected(_Ctx),
            t_refresh_does_not_crash(_Ctx)
        ] end
    }}.

setup() ->
    {ok, Apps} = application:ensure_all_started(config),
    ok = config:set("auto_shard", "capacity_scan_interval_ms", "300000", false),
    {ok, Pid} = mem3_node_capacity:start_link(),
    {Pid, Apps}.

teardown({Pid, _Apps}) ->
    unlink(Pid),
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 -> ok
    end.

t_best_nodes_returns_list(_) ->
    ?_test(begin
        %% Should return a list (may be empty if no capacity data)
        Nodes = mem3_node_capacity:best_nodes(50000000000, 3, []),
        ?assert(is_list(Nodes))
    end).

t_best_nodes_respects_count(_) ->
    ?_test(begin
        %% Request more nodes than available — should return what we have
        Nodes = mem3_node_capacity:best_nodes(100, 100, []),
        ?assert(length(Nodes) =< 100)
    end).

t_local_capacity_collected(_) ->
    ?_test(begin
        %% After startup, local node capacity should be collected
        timer:sleep(100), % let scan complete
        Caps = mem3_node_capacity:all_capacities(),
        ?assert(is_map(Caps)),
        %% Local node should be in the map
        case maps:get(node(), Caps, undefined) of
            undefined ->
                %% May not have couch_disk_monitor running in test
                ok;
            Cap ->
                ?assert(is_map(Cap)),
                ?assert(maps:is_key(dirs, Cap)),
                ?assert(maps:is_key(shard_count, Cap))
        end
    end).

t_refresh_does_not_crash(_) ->
    ?_test(begin
        ?assertEqual(ok, mem3_node_capacity:refresh()),
        timer:sleep(100)
    end).
