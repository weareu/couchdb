Automatic Shard Splitting (Developer Description)
==================================================

This document describes the automatic shard-splitting subsystem that
sits on top of the existing `mem3_reshard` machinery. It is a separate,
opt-in system designed for clusters where shards grow beyond a
manageable size and manual reshard operations have become impractical.

For the existing manual reshard system, see [README_reshard.md](README_reshard.md).

Overview
--------

The auto-shard system has three primary jobs:

1. **Detect** shards larger than a configurable threshold
2. **Split** them via internal replication (`mem3_rep`) — without
   needing 2x source size on the local node
3. **Recover** interrupted splits after node restart (same pattern as
   compaction `.compact.data` recovery)

It is composed of these modules:

- `mem3_auto_shard` — gen_server orchestrator with periodic scanner
- `mem3_reshard_rep` — replication-based split engine (13-state machine)
- `mem3_shard_size` — ETS cache of shard file sizes (O(1) header reads)
- `mem3_node_capacity` — cluster-wide capacity tracking via RPC
- `couch_space_monitor` — central space reservation service (in `couch` app)
- `couch_multidir` — per-database directory mapping

Difference From `mem3_reshard`
-------------------------------

| Aspect | `mem3_reshard` (manual) | `mem3_reshard_rep` (auto) |
|---|---|---|
| Trigger | HTTP `POST /_reshard/jobs` | Periodic scan of shard sizes |
| Copy mechanism | `couch_db_split` (local file copy) | `mem3_rep` (network replication) |
| Local space needed | 2x source size | ~0 (data flows to targets) |
| Job persistence | `_local` docs in `_dbs` | `_local` docs on source shard |
| Restart recovery | Auto-resumes from checkpoint | Cleans up orphans, re-splits next scan |
| Concurrency | Configured via `max_jobs` | `max_concurrent_splits` + space-aware queue |
| Suitable for | Manual rebalancing | Bulk runaway-shard cleanup |

Both systems can coexist. `mem3_auto_shard` checks for active
`mem3_reshard` jobs on the same shard and skips them.

The 13 Split States
-------------------

`mem3_reshard_rep:split/3` runs synchronously through these states.
Checkpoint is written to a `_local` doc on the source shard at every
transition, and progress is reported to `/_active_tasks`.

```
1.  creating_targets    — empty target shard DBs created on destination nodes
2.  replicating         — mem3_rep:go/3 streams docs (hash-routed to ranges)
3.  topoff_1            — catch up writes during bulk replication
4.  building_indices    — warm up view indices on targets (optimization)
5.  topoff_2            — catch up writes during index build
6.  copying_local       — _local docs (security, replication checkpoints)
7.  topoff_3            — catch up writes during local doc copy
8.  updating_map        — atomic update of _dbs shard map
9.  topoff_post_map     — catch writes during shard map propagation
10. topoff_final        — second post-map pass closes the propagation window
11. verifying           — doc count, deleted count, and distribution check
12. deleting_source     — only after verification passes
13. completed
```

If verification (state 11) fails, the source shard is **not deleted**.
Both source and targets remain live; an operator must investigate.

The artificial `mem3_rep` checkpoint creation in
`create_replication_checkpoints/2` is critical: without it, `mem3_sync`
would re-replicate the entire shard content from other cluster nodes
after the split completes (because targets would have no checkpoint
record showing they are caught up).

Crash Recovery
--------------

On startup, `mem3_auto_shard` sends itself a delayed `recover_interrupted`
message. The handler calls `mem3_reshard_rep:find_interrupted_splits/0`
which scans all local shards for `_local/auto_split_checkpoint_*` docs.

Each found checkpoint contains:

```json
{
    "type": "auto_split_checkpoint",
    "state": "replicating",
    "source": "shards/00000000-ffffffff/db.123",
    "targets": ["shards/00000000-7fffffff/db.123",
                "shards/80000000-ffffffff/db.123"],
    "factor": 2,
    "updated_at": 1712534400123
}
```

The state is validated against a whitelist (preventing atom-table
exhaustion via crafted docs) and then dispatched:

- **Pre-map states** (`creating_targets` through `topoff_3`): orphan
  targets are deleted via `couch_server:delete/2`, the checkpoint is
  removed, and the shard will be re-split on the next scan cycle
  (it is still oversized).

- **Post-map states** (`topoff_post_map` through `deleting_source`):
  targets are **preserved** because clients may already be routing
  to them. A warning is logged for operator investigation.

This is the same pattern as `couch_bt_engine` recovering from
`.compact.data` files on startup.

Space Reservation Architecture
------------------------------

The system uses **two layers** of space tracking:

1. **`mem3_auto_shard.space_reservations`** (in gen_server state)
   Per-node map of reserved bytes from auto-split jobs. This is the
   *cluster-wide* view used by the auto-shard pre-flight check.
   Released exactly when the split process exits, using the
   `{Pid, PerTarget, NumTargets}` tuple stored in `active_splits`.

2. **`couch_space_monitor`** (ETS-backed gen_server in `couch` app)
   Central reservation service shared by smoosh, manual reshard,
   manual compact, and auto-shard. Auto-shard registers reservations
   for **local** target nodes here so smoosh on the same node sees
   them. Remote-node space is tracked exclusively by layer 1 (via
   `mem3_node_capacity` RPC).

Pre-flight check sequence in `do_start_split/3`:

```
1. mem3_node_capacity:best_nodes/3   — pick target nodes by free space
2. Filter through [node() | nodes()]  — drop disconnected nodes
3. subtract_reservations              — reduce free space by in-flight splits
4. check_floor_and_preflight          — hard floor + 3x target size check
5. add_reservations                   — track for next preflight
6. spawn_monitor split process
```

When a split completes (or dies), `handle_split_done/3` releases the
exact reservation amount from both layers and sets the cooldown.
Cooldown is set on **completion**, not start, so the cluster
stabilizes after the split, not before.

Active Tasks Integration
------------------------

`mem3_reshard_rep:register_task/3` calls `couch_task_status:add_task/1`
with type `shard_split` (matching how `couch_bt_engine_compactor`
registers `database_compaction`). Each state transition calls
`update_task/2` to advance the phase and progress.

This means operators can monitor splits in the same place as
compactions:

```bash
$ curl -s $COUCH/_active_tasks | jq '.[] | select(.type == "shard_split")'
```

Related HTTP Endpoints
----------------------

- `GET /_reshard/auto` — auto-shard status (config, active splits, reservations)
- `PUT /_reshard/auto` — update auto-shard config (enabled, threshold, paused)
- `POST /_reshard/auto/scan` — trigger immediate scan
- `POST /_reshard/auto/pause` — pause auto-splitting
- `POST /_reshard/auto/resume` — resume auto-splitting
- `GET /_reshard/space` — view ALL space reservations across all consumers
- `GET /_active_tasks` — splits appear here with type `shard_split`

All endpoints require server admin auth (`chttpd:verify_is_server_admin/1`).

Configuration Reference
-----------------------

See:
- [`config/resharding.rst`](../docs/src/config/resharding.rst) — full config reference
- [`cluster/sharding.rst`](../docs/src/cluster/sharding.rst) — operator guide

Quick reference:

```ini
[auto_shard]
enabled = false                ; master switch
max_shard_size_bytes = 20000000000   ; 20 GB threshold
scan_interval_ms = 600000      ; 10 minutes
max_concurrent_splits = 2
max_split_factor = 4           ; gradual splitting
cooldown_ms = 3600000          ; 1 hour after completion
maintenance_window = always    ; or "22-06" for off-peak
min_free_space_factor = 3      ; 3x target size required
min_free_floor_bytes = 10000000000   ; 10 GB hard floor
exclude_dbs = _users,_replicator,_global_changes
protected_dbs = _dbs,_nodes

[space_monitor]
enabled = false                ; master switch for all space checks
min_free_floor_bytes = 10000000000   ; per-node hard floor

[smoosh]
check_space_before_compact = false   ; opt-in for compaction space checks

[reshard]
check_space_before_split = false     ; opt-in for manual split space checks
```

Test Coverage
-------------

The implementation has 344 mem3 + 31 couch_space_monitor + 57 smoosh
tests covering:

- Real database splits with doc-level integrity verification
- Crash recovery from every checkpoint state
- Orphan target cleanup with `_local` doc round-trip
- Concurrent space exhaustion (50 oversized DBs scenario)
- Process death auto-release of reservations
- Multi-directory proportional space tracking
- Floor enforcement at runtime
- View query against post-split targets
- Replication checkpoint preservation (no re-replication storm)
- Conflict prevention with legacy `mem3_reshard` jobs
