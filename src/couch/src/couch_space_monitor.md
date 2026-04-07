couch_space_monitor
===================

Central space reservation service for all disk-consuming operations.

Why
---

CouchDB has several subsystems that consume significant disk space
during their operations:

- **Smoosh** runs database and view compaction (writes a new file
  while the old one still exists, so peak usage ~2x current size)
- **mem3_reshard** runs manual shard splits via local file copy
  (needs ~2x source size on the local node)
- **mem3_auto_shard** runs automatic splits via internal replication
  (needs ~3x target size on each destination node)
- **Manual `/_compact`** triggers compaction directly
- **View index builds** triggered on demand by queries

Without coordination, any of these can independently fill the disk:

- 50 oversized databases all triggering auto-split simultaneously
- Smoosh re-running compaction on every database after a restart
- A manual reshard kicked off while compaction is already running
- An operator triggering `_compact` on a 500 GB database while disk
  is at 70% used

`couch_space_monitor` is a single ETS-backed gen_server that all of
the above register with **before** they start consuming space, and
release **on** completion (or automatically via process monitor on
crash). All components query the same reservation pool, so the
collective space budget is honored cluster-wide.

API
---

```erlang
%% Reserve space — fails if floor or available space would be exceeded
couch_space_monitor:reserve(Tag, Node, Bytes).

%% Release a reservation by tag (idempotent)
couch_space_monitor:release(Tag).

%% Check how much is reserved on a node
couch_space_monitor:reserved_on(Node).

%% Total reserved cluster-wide
couch_space_monitor:total_reserved().

%% List all active reservations as maps
couch_space_monitor:reservations().

%% Status summary for HTTP / debugging
couch_space_monitor:status().
```

Tag Format
----------

Tags are arbitrary terms but should be unique. The `tag_to_description/1`
function recognizes these standard forms:

- `{compaction, DbName}` — database compaction (smoosh)
- `{view_compact, {Shard, GroupId}}` — view compaction (smoosh)
- `{auto_split, ShardName}` — automatic shard split (mem3_auto_shard)
- `{manual_split, JobId}` — manual shard split (mem3_reshard)
- `{manual_compact, DbName}` — manual `/_compact` HTTP trigger
- `{index_build, IndexName}` — view index build

Auto-Release on Process Death
-----------------------------

Every reservation is monitored: `monitor(process, CallerPid)`. When
the calling process exits (normal or crash), the gen_server receives
a `{'DOWN', Ref, ...}` message and automatically releases the
reservation. This means crashed operations cannot leak reservations.

Floor Enforcement
-----------------

The `[space_monitor] min_free_floor_bytes` config option (default
10 GB) is a hard floor. No reservation is accepted if the requesting
node has less than this much free space. This prevents the system from
ever consuming the last sliver of disk, leaving room for emergency
operations like log rotation and cluster recovery.

Remote Nodes
------------

`raw_free/1` returns `0` for remote nodes (conservative refusal).
Remote space tracking is the responsibility of `mem3_node_capacity`
which uses RPC. This prevents a critical bug where `infinity` would
silently bypass all space checks for remote target nodes.

Integration Points
------------------

| Module | When | Tag |
|---|---|---|
| `smoosh_channel:try_compact/2` | Before compaction starts | `{compaction, _}` or `{view_compact, _}` |
| `chttpd_db:handle_compact_req/2` | On `POST /_compact` HTTP | `{manual_compact, _}` |
| `mem3_reshard:handle_start_job/2` | On `POST /_reshard/jobs` | `{manual_split, _}` |
| `mem3_auto_shard:do_start_split/3` | Before auto-split starts | `{auto_split, _}` |

Each integration is gated by an opt-in config flag so existing clusters
are not affected. See:

- `[smoosh] check_space_before_compact = true`
- `[reshard] check_space_before_split = true`
- `[space_monitor] enabled = true` (master fallback)

HTTP Endpoint
-------------

```bash
# View all active reservations across all consumers
$ curl -s http://admin:pw@localhost:5984/_reshard/space | jq .
{
  "total_reserved_bytes": 75000000000,
  "reservation_count": 3,
  "by_node": {
    "node1@host": 60000000000,
    "node2@host": 15000000000
  },
  "reservations": [
    {
      "tag": "{auto_split,<<\"shards/00-ff/bigdb.123\">>}",
      "node": "node1@host",
      "bytes": 60000000000,
      "created_at": 1712534400123,
      "description": "auto shard split: shards/00-ff/bigdb.123"
    }
  ]
}
```

Crash Recovery
--------------

The ETS table is **not persisted** — on gen_server restart, all
reservations are lost. This is intentional:

- Persisting would risk holding stale reservations from crashed
  operations forever
- All consumers re-reserve before restarting their work (compaction
  re-enqueues, splits re-checkpoint, etc.)
- Process monitors are reset on restart, so any consumer process
  whose reservation was lost will get a fresh reservation when it
  next reserves

This matches how compaction handles state: checkpoints are persisted
to disk in the data files, but in-memory bookkeeping is rebuilt on
startup.
