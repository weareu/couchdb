Fork Changes — Auto-Shard Rebalance Feature
============================================

This fork of Apache CouchDB adds automatic shard splitting, central
disk space reservation, multi-directory database paths, and crash
recovery for shard splits. None of these features change the on-disk
format or wire protocol of upstream CouchDB.

All features are **opt-in**. Default behavior matches upstream.

What's New
----------

### Automatic Shard Splitting

Periodically scans local shard sizes and splits any shard exceeding
`max_shard_size_bytes` (default 20 GB) using internal replication
(`mem3_rep`) — without needing 2x source size locally. Targets are
placed on nodes with the most free space.

- 13-state machine with checkpoint persistence
- Visible in `/_active_tasks` alongside compactions
- Survives node restart via `_local/auto_split_checkpoint_*` docs
- Configurable threshold, concurrency, cooldown, maintenance window
- Per-database opt-out via `_design/shard_config`

**Modules:** `mem3_auto_shard`, `mem3_reshard_rep`, `mem3_shard_size`,
`mem3_node_capacity`

**Config:** `[auto_shard]` section in `default.ini`

**Docs:** [src/mem3/README_auto_shard.md](src/mem3/README_auto_shard.md),
[src/docs/src/config/resharding.rst](src/docs/src/config/resharding.rst),
[src/docs/src/cluster/sharding.rst](src/docs/src/cluster/sharding.rst)

### Central Space Reservation Service

`couch_space_monitor` is a new ETS-backed gen_server that all
disk-consuming operations register with **before** they start
consuming space. Prevents thundering-herd scenarios:

- 50 oversized databases all triggering auto-split simultaneously
- Smoosh re-running compaction on every database after restart
- Manual reshard while compaction is already running

Consumers integrated:
- `smoosh_channel:try_compact/2` (database + view compaction)
- `chttpd_db:handle_compact_req/2` (manual `/_compact`)
- `mem3_reshard:handle_start_job/2` (manual shard split)
- `mem3_auto_shard:do_start_split/3` (automatic shard split)

All reservations auto-release on process death (process monitor).
Hard floor (`min_free_floor_bytes`, default 10 GB) prevents disk
exhaustion regardless of individual operation calculations.

**Modules:** `couch_space_monitor` (in `couch` app)

**Config:** `[space_monitor]`, `[smoosh] check_space_before_compact`,
`[reshard] check_space_before_split`

**HTTP:** `GET /_reshard/space`

**Docs:** [src/couch/src/couch_space_monitor.md](src/couch/src/couch_space_monitor.md)

### Multi-Directory Database Paths

CouchDB can now store databases on multiple mount points via either:

- Per-database explicit rules (`[database_paths]` config section)
- Automatic allocation across `database_dirs` based on free space

Useful for:
- Spreading I/O across multiple disks
- Placing hot databases on NVMe and archives on HDD
- Adding storage capacity without re-distributing existing databases

**Modules:** `couch_multidir`

**Config:** `[couchdb] database_dirs`, `[database_paths]`

### Shard Split `_active_tasks` Integration

Shard splits register as type `shard_split` in `couch_task_status`
(the same place compaction registers `database_compaction`). Each
of the 13 split states reports progress.

```bash
$ curl -s $COUCH/_active_tasks | jq '.[] | select(.type == "shard_split")'
```

### Split Checkpoint Persistence & Crash Recovery

Auto-shard splits write `_local/auto_split_checkpoint_*` docs on
the source shard at every state transition (the same concept as
`.compact.data` files for compaction). On startup, `mem3_auto_shard`
scans local shards for these checkpoints:

- **Pre-map-update crash:** orphan targets deleted, shard re-split next scan
- **Post-map-update crash:** targets preserved (may be serving traffic),
  operator alerted

State strings are validated against a whitelist before atom conversion
to prevent atom-table exhaustion from corrupted/malicious checkpoint docs.

### Fauxton UI Addon

The Fauxton web UI now includes an Auto-Shard panel with:

- Live status (enabled, paused, coordinator, threshold, statistics)
- Active tasks table (splits + compactions in one place, with progress bars)
- Space reservations table (cluster-wide, all consumers)
- Controls: enable/disable, pause/resume, trigger scan, update threshold

**Location:** `src/fauxton/app/addons/autoshard/`

New HTTP Endpoints
------------------

| Endpoint | Method | Purpose |
|---|---|---|
| `/_reshard/auto` | GET | Auto-shard status, config, in-flight reservations |
| `/_reshard/auto` | PUT | Update config (enable, threshold, pause) |
| `/_reshard/auto/scan` | POST | Trigger immediate scan |
| `/_reshard/auto/pause` | POST | Pause scanning |
| `/_reshard/auto/resume` | POST | Resume scanning |
| `/_reshard/space` | GET | View ALL space reservations cluster-wide |

All endpoints require server admin auth.

New Configuration Sections
--------------------------

```ini
[auto_shard]
enabled = false
max_shard_size_bytes = 20000000000
scan_interval_ms = 600000
max_concurrent_splits = 2
max_split_factor = 4
cooldown_ms = 3600000
maintenance_window = always
min_free_space_factor = 3
min_free_floor_bytes = 10000000000
weighted_placement = true
exclude_dbs = _users,_replicator,_global_changes
protected_dbs = _dbs,_nodes

[space_monitor]
enabled = false
min_free_floor_bytes = 10000000000

[reshard]
check_space_before_split = false

[smoosh]
check_space_before_compact = false

[database_paths]
;important_db = /mnt/nvme1
;archive_* = /mnt/hdd_array
;shards/*/users.* = /mnt/ssd

[couchdb]
;database_dirs = /data1,/data2,/data3
```

Test Coverage
-------------

| Suite | Tests |
|---|---|
| `mem3` | 344 |
| `couch_space_monitor` (unit + e2e) | 31 |
| `smoosh` | 57 |
| `chttpd` | 534 |

All tests use real databases and real disk operations — no mocks.

CRITICAL Bug Fixes Applied
--------------------------

During code review, three CRITICAL issues were found and fixed:

1. **Atom table exhaustion via crafted checkpoint docs** — fixed by
   whitelist validation in `validate_split_state/1`
2. **`raw_free` returned `infinity` for remote nodes** — fixed by
   returning `0` (conservative); remote tracking via `mem3_node_capacity`
3. **Space reservation leak on split timeout** — fixed by storing
   exact `{Pid, PerTarget, NumTargets}` tuple instead of recalculating
   from possibly-stale shard size cache

Plus 4 HIGH issues:

1. Cooldown set at split START → moved to COMPLETION
2. `check_space_before_compact` config used for reshard → new
   `[reshard] check_space_before_split`
3. No liveness check on target nodes → filter through `nodes()`
4. `verify_doc_counts` races concurrent writes → retry up to 3x

Compatibility
-------------

- **On-disk format:** unchanged
- **Wire protocol:** unchanged
- **Existing `mem3_reshard` system:** unchanged, coexists with auto-split
- **Existing smoosh:** unchanged unless `check_space_before_compact = true`
- **Default behavior:** matches upstream when all opt-in flags are off
- **`/_reshard/jobs` API:** unchanged

Upgrade and downgrade are safe as long as auto-split features are
disabled before downgrading. Active checkpoint docs are harmless to
older versions (they are `_local` docs that are not replicated).
