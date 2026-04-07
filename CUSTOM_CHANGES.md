# CouchDB 3.5.1 Custom Fork — Change Documentation

**Branch:** `couchdb-3.5.1-custom`  
**Base:** Apache CouchDB `3.5.1` tag  
**4 commits**, 3,454 lines added across 18 files

---

## Overview

This fork adds three capabilities to CouchDB 3.5.1:

1. **Attachment extraction** — Moves large attachments from `.couch` shard files to external filesystem (GlusterFS) during compaction, reducing database size while maintaining R3 redundancy on the filesystem layer.

2. **Time-based document retention** — Removes entire documents from the database during compaction based on configurable date field thresholds, eliminating the need for purge operations on 10TB+ databases.

3. **Cross-datacenter clustering** — Zone-aware quorum, timeout tuning, and failure detection improvements that enable running CouchDB clusters across multiple data centers with 50-200ms WAN latency.

---

## Commit History

```
1fe49e770 fix: address CRITICAL and HIGH code review findings
8de916d39 test: comprehensive cross-DC chaos and stability tests (59 tests)
5568cfe74 feat: cross-datacenter clustering improvements with chaos tests
15f3e5afb feat: attachment extraction, retention compaction, and smoosh loop prevention
```

---

## 1. Attachment Extraction & Retention Compaction

### Problem

- 10TB+ CouchDB databases with large binary attachments
- Attachments stored inside `.couch` shard files, consuming shard disk space
- No way to remove old documents without `purge` (too slow) or full replication to a new database
- Shard sync loops caused by size mismatches after extraction (upstream fixed in 3.5.1 via `f2f6cd72d`)

### Solution

During compaction, documents are evaluated against configurable rules:

| Action | Condition | Result |
|--------|-----------|--------|
| **Extract** | Document date > `extract_after_days` | Attachments written to `{DataDir}/attachments/{DbName}/{DocId}/{AttName}`, removed from shard file |
| **Remove** | Document date > `remove_after_days` | Document skipped entirely during compaction (not copied to new file) |
| **Keep** | Document is recent, has no date field, is a design doc, local doc, or tombstone | Normal compaction behavior |

### Configuration

**Option A: INI config (per-node)**
```ini
[compaction_retention]
config_ddoc = _design/retention_config
extract_enabled = true
extract_databases = all
date_fields = date
extract_after_days = 365
remove_after_days = 0
log_dir = /var/lib/couchdb/retention_logs
```

**Option B: Design document (replicated, cluster-wide)**
```json
{
  "_id": "_design/retention_config",
  "retention": {
    "date_fields": ["date", "created_at"],
    "extract_after_days": 365,
    "remove_after_days": 730
  }
}
```

INI's `config_ddoc` points to the design doc name. The design doc is replicated across all nodes automatically, ensuring cluster-wide consistency.

### Smoosh Loop Prevention

When attachments are extracted, their sizes are added back to the leaf's `active_size`:

```erlang
sizes = #size_info{
    active = ActiveSize + ExtractedAttSize,
    external = ExternalSize + ExtractedAttSize
}
```

This tells smoosh "this shard's active size accounts for the external attachments" — preventing the false `file/active` ratio that triggers compaction loops.

### Audit Trail

Removed document IDs are logged to:
- `couch_log:warning` — standard Erlang logging, searchable in log aggregation
- `{log_dir}/{DbName}.removal.log` — persistent file on disk, rotatable

### Safety Guarantees

- Design docs (`_design/*`) are NEVER removed regardless of date
- Local docs (`_local/*`) are NEVER removed
- Deleted tombstones are NEVER removed (already tiny)
- Documents without a date field are NEVER removed
- Path traversal prevention via `sanitize_path_component/1`
- Non-JSON document bodies handled gracefully (no crash)
- Attachment extraction is atomic (write to `.tmp`, verify MD5, rename)
- Log write failure does NOT crash compaction
- Config is read ONCE at compaction start, preventing mid-compaction inconsistency

### Files Changed

| File | Change |
|------|--------|
| `src/couch/src/couch_bt_engine_compactor.erl` | +479 lines: extraction, retention, config, size tracking |
| `src/couch_special_compact/` | Standalone escript tool for bulk extraction |
| `Makefile` | Build targets for couch_special_compact |
| `rebar.config.script` | Added couch_special_compact to subdirs |

---

## 2. Cross-Datacenter Clustering

### Problem

CouchDB's clustering assumes low-latency connections between all nodes. With nodes in different data centers (50-200ms RTT):

- Writes wait for ALL `n` replicas before checking quorum — every write takes 200ms+ minimum
- Dead node detection takes 60s regardless of proximity
- No timeout differentiation between local and remote DC nodes
- Smoosh/compaction doesn't account for cross-DC size differences

### Solution

#### Zone-Aware Quorum (`fabric_doc_update.erl`)

**Before:** `handle_message({ok, Replies})` waited until ALL docs had at least one reply from ALL workers before calling `maybe_reply` to check quorum.

**After:** Added `try_early_quorum/3` that checks if all documents already have W successful replies on EVERY worker response. With `n=3, w=2` across 2 DCs:
- 2 same-zone replicas respond in ~5ms → quorum met → return immediately
- Cross-zone replica still in flight → don't wait for it

#### Zone-Aware Timeouts (`fabric_util.erl`)

```erlang
cross_zone_timeout(BaseTimeout) ->
    Factor = config:get_integer("cluster", "cross_zone_timeout_factor", 3),
    BaseTimeout * max(1, Factor).
```

Local RPC: 60s timeout. Cross-DC RPC: 180s timeout (3x factor).

#### Zone-Aware Failure Detection (`mem3.erl`)

`ping_nodes/0` now:
1. Partitions nodes into same-zone and cross-zone
2. Pings both groups **in parallel** (not sequentially)
3. Same-zone: 10s timeout (fast failure detection)
4. Cross-zone: 60s timeout (WAN tolerance)

Result: Detect local DC failure in 10s instead of 60s.

#### VM Tuning (`vm.args`)

```
-kernel net_ticktime 30
```

Balanced between fast failure detection and WAN jitter tolerance.

### Configuration

```ini
[cluster]
n = 3
placement = dc-a:2,dc-b:1
cross_zone_timeout_factor = 3
same_zone_ping_timeout_ms = 10000
cross_zone_ping_timeout_ms = 60000
```

Set zone per node: `COUCHDB_ZONE=dc-a` environment variable.

### Files Changed

| File | Change |
|------|--------|
| `src/fabric/src/fabric_doc_update.erl` | Zone-aware early quorum return |
| `src/fabric/src/fabric_util.erl` | `cross_zone_timeout/1` function |
| `src/mem3/src/mem3.erl` | Zone-aware parallel ping, configurable timeouts |
| `rel/overlay/etc/vm.args` | `net_ticktime 30` |
| `rel/overlay/etc/default.ini` | Cross-zone config options |

---

## 3. Test Suite

### Retention Tests (`couch_bt_engine_compactor_retention_tests.erl`) — 35 tests

| Category | Count | What It Proves |
|----------|-------|----------------|
| Date parsing | 14 | ISO 8601, leap days, boundaries, invalid formats |
| Age comparison | 8 | Threshold math, boundaries, edge cases |
| Field lookup | 10 | Multi-field fallback, non-binary values, empty lists |
| Path sanitization | 10 | Traversal prevention, special chars, null bytes |
| INI config | 5 | Defaults, custom values, per-database extraction |
| Regression (no config) | 5 | All docs preserved, sizes stable, attachments intact |
| Retention removal | 11 | Old removed, recent kept, design/local/tombstone protected |
| Size stability | 4 | Triple compact identical, active > 0, active <= file |

### Chaos Tests (`fabric_cross_dc_chaos_tests.erl`) — 59 tests

| Category | Count | What It Proves |
|----------|-------|----------------|
| Latency injection | 9 | Zone classification, delay calculation, jitter |
| Quorum math | 10 | W met/not met, split brain, noreply, errors |
| Timeout factor | 6 | 3x default, custom, infinity, min/max |
| Write SLA | 6 | Single <1s, 100 seq <10s, bulk 200 <5s, rapid-fire 500 |
| Staleness detection | 7 | update_seq advances, changes_since completeness, ordering |
| Size convergence | 7 | Triple compact identical, active != 0, active <= file |
| Concurrent storms | 4 | 10 parallel writers, interleaved compact, create/delete cycles |
| Attachment chaos | 5 | Survive compact, large att stable, delete reclaims space |
| Purge stability | 2 | Purge + compact correct, double compact identical |

### Key Invariants Tested

- `active_size > 0` always (prevents smoosh MinPriority loop)
- `active <= file` always (valid smoosh ratio)
- Triple compact = identical sizes (no sync loop drift)
- Changes feed preserves all doc IDs through compaction
- Sequence ordering strictly increasing
- Parallel writers produce no data loss
- Config consistency across compaction batches

---

## Upstream Changes Inherited (3.5.1)

By rebasing onto 3.5.1, this fork gains:
- `f2f6cd72d` — Fix attachment size calculation (likely root cause of original sync loops)
- `5c37dfaa6` — Fix purge_infos when exceeding limit
- `9dee10d7b` — 30% purge optimization
- Multiple mem3_rep purge checkpoint fixes
- QuickJS scanner improvements
- Various bug fixes

---

## Deployment Notes

1. Build: `./configure && make`
2. Set `COUCHDB_ZONE=<dc-name>` per node before starting
3. Apply INI config for retention/extraction
4. Create `_design/retention_config` in databases that need retention
5. Retention removal only happens during compaction — trigger manually or wait for smoosh
6. Monitor `retention_logs/` directory for removal audit trail
7. External attachments are in `{database_dir}/attachments/{DbName}/{DocId}/`
