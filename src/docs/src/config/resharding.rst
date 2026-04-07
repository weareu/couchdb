.. Licensed under the Apache License, Version 2.0 (the "License"); you may not
.. use this file except in compliance with the License. You may obtain a copy of
.. the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
.. WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
.. License for the specific language governing permissions and limitations under
.. the License.

.. highlight:: ini

==========
Resharding
==========

.. _config/reshard:

Resharding Configuration
========================

.. config:section:: reshard :: Resharding Configuration

    .. config:option:: max_jobs :: Maximum resharding jobs per node

        Maximum number of resharding jobs per cluster node. This includes
        completed, failed, and running jobs. If the job appears in the
        _reshard/jobs HTTP API results it will be counted towards the limit.
        When more than ``max_jobs`` jobs have been created, subsequent requests
        will start to fail with the ``max_jobs_exceeded`` error::

             [reshard]
             max_jobs = 48

    .. config:option:: max_history :: Maximum size of the event log

        Each resharding job maintains a timestamped event log. This setting
        limits the maximum size of that log::

             [reshard]
             max_history = 20

    .. config:option:: max_retries :: Maximum number of retries before failing \
        resharding job

        How many times to retry shard splitting steps if they fail. For
        example, if indexing or topping off fails, it will be retried up to
        this many times before the whole resharding job fails::

             [reshard]
             max_retries = 1

    .. config:option:: retry_interval_sec :: Wait time between resharding retries

        How long to wait between subsequent retries::

             [reshard]
             retry_interval_sec = 10

    .. config:option:: delete_source :: Delete source after resharding

        Indicates if the source shard should be deleted after resharding has
        finished. By default, it is ``true`` as that would recover the space
        utilized by the shard. When debugging or when extra safety is required,
        this can be switched to ``false``::

             [reshard]
             delete_source = true

    .. config:option:: update_shard_map_timeout_sec :: Shard map update waiting time

        How many seconds to wait for the shard map update operation to
        complete. If there is a large number of shard db changes waiting to
        finish replicating, it might be beneficial to increase this timeout::

            [reshard]
            update_shard_map_timeout_sec = 60

    .. config:option:: source_close_timeout_sec :: Source shard wait time before close

        How many seconds to wait for the source shard to close. "Close" in this
        context means that client requests which keep the database open have
        all finished::

            [reshard]
            source_close_timeout_sec = 600

    .. config:option:: require_node_param :: Require node parameter when creating \
        resharding job

        Require users to specify a ``node`` parameter when creating resharding
        jobs. This can be used as a safety check to avoid inadvertently
        starting too many resharding jobs by accident::

            [reshard]
            require_node_param = false

    .. config:option:: require_range_param :: Require range parameter when creating \
        resharding job

        Require users to specify a ``range`` parameter when creating resharding
        jobs. This can be used as a safety check to avoid inadvertently
        starting too many resharding jobs by accident::

            [reshard]
            require_range_param = false

.. _config/auto_shard:

Automatic Shard Splitting
=========================

.. config:section:: auto_shard :: Automatic Shard Splitting Configuration

    CouchDB can automatically split shards that exceed a configurable size
    threshold. This prevents large shards from causing slow compaction,
    massive index rebuilds, and difficult resharding operations.

    Auto-splitting uses internal replication (``mem3_rep``) to stream
    documents directly to target shards on their destination nodes. The
    source node does not need 2x free space because data flows outward
    to targets rather than being copied locally.

    .. config:option:: enabled :: Enable automatic shard splitting

        Master switch for auto-shard splitting. Disabled by default.
        When enabled, the system periodically scans for oversized shards
        and triggers split operations::

            [auto_shard]
            enabled = false

    .. config:option:: max_shard_size_bytes :: Maximum shard size before splitting

        The trigger threshold in bytes. Any shard larger than this value
        will be considered for automatic splitting. Default is 20 GB::

            [auto_shard]
            max_shard_size_bytes = 20000000000

    .. config:option:: scan_interval_ms :: Scan interval for oversized shards

        How often (in milliseconds) the auto-shard system scans for
        oversized shards. Default is 10 minutes::

            [auto_shard]
            scan_interval_ms = 600000

    .. config:option:: max_concurrent_splits :: Maximum concurrent split jobs

        Maximum number of shard split operations running simultaneously
        across the cluster. Default is 2::

            [auto_shard]
            max_concurrent_splits = 2

    .. config:option:: max_split_factor :: Maximum split factor per round

        Maximum number of pieces to split a shard into per round.
        For example, a 200 GB shard with a 20 GB threshold would need a
        16-way split, but with ``max_split_factor = 4`` it will first
        split 4-way (50 GB each), then those will be split again in a
        subsequent round. Default is 4::

            [auto_shard]
            max_split_factor = 4

    .. config:option:: cooldown_ms :: Cooldown between splits of same database

        Minimum time in milliseconds between split operations on the same
        database. Prevents cascading splits. Default is 1 hour::

            [auto_shard]
            cooldown_ms = 3600000

    .. config:option:: maintenance_window :: Maintenance window for splitting

        Time window during which auto-splitting is allowed. Format is
        ``HH-HH`` (24-hour) or ``always``. Overnight windows that wrap
        past midnight are supported (e.g., ``22-06``). Default is
        ``always``::

            [auto_shard]
            maintenance_window = always

    .. config:option:: min_free_space_factor :: Free space requirement

        Multiplier for required free disk space before starting a split.
        Each target node must have at least this many times the target
        shard size free. Accounts for data, compaction headroom, and
        index space. Default is 3.

        This check is **cluster-aware** — space already reserved by
        in-flight splits is subtracted from available free space, so
        50 oversized databases cannot all start splitting simultaneously
        and exhaust disk::

            [auto_shard]
            min_free_space_factor = 3

    .. config:option:: min_free_floor_bytes :: Hard minimum free space floor

        Hard floor (in bytes) for free space per node. Even if the
        ``min_free_space_factor`` calculation would allow a split, no
        split starts if any target node has less than this much free
        space (after accounting for reservations from in-flight splits).
        Set this to at least 2x ``max_shard_size_bytes``. Default 10 GB::

            [auto_shard]
            min_free_floor_bytes = 10000000000

    .. config:option:: weighted_placement :: Use capacity-weighted placement

        When enabled, new shard targets are placed on nodes with the most
        free disk space instead of round-robin. Default is ``true``::

            [auto_shard]
            weighted_placement = true

    .. config:option:: exclude_dbs :: Databases to exclude from auto-splitting

        Comma-separated list of database names or glob patterns to exclude
        from auto-splitting. Wildcards (``*``) are supported::

            [auto_shard]
            exclude_dbs = _users,_replicator,_global_changes,metrics_*

    .. config:option:: protected_dbs :: System databases that are always protected

        Comma-separated list of databases that can never be split::

            [auto_shard]
            protected_dbs = _dbs,_nodes

    **Per-database opt-out:** Individual databases can disable auto-splitting
    by creating a design document::

        {
            "_id": "_design/shard_config",
            "auto_split": {
                "enabled": false,
                "reason": "Custom retention policy"
            }
        }

    **Safety gates:**

    - Only one node (the coordinator) runs scans to prevent duplicate jobs
    - All circuit breakers must be closed (no splits during network partitions)
    - Pre-flight disk space check before each split (cluster-aware)
    - Hard free-space floor (``min_free_floor_bytes``) prevents splits when
      disk is critically low, even if individual checks pass
    - Liveness check on target nodes — splits do not start if target nodes
      are not currently connected
    - Mandatory consistency verification before source shard deletion (with
      retry to handle races against concurrent writes)
    - If verification fails, source is NOT deleted and operator is alerted
    - Splits are tracked in ``/_active_tasks`` with progress (1-13 phases)
    - Splits checkpoint state to ``_local`` docs on the source shard so
      they can be recovered or cleaned up after a node restart

.. _config/space_monitor:

Central Space Reservation
=========================

.. config:section:: space_monitor :: Cluster-Wide Space Reservation Service

    CouchDB tracks space reservations across all disk-consuming operations
    (compaction, shard splitting, view compaction, manual operations) via
    a central ``couch_space_monitor`` ETS-backed gen_server. Every
    operation that writes significant data registers its expected disk
    usage **before** starting and releases it on completion.

    This prevents thundering-herd scenarios where concurrent operations
    collectively exhaust disk space — for example 50 oversized databases
    all starting splits simultaneously, or smoosh starting compactions on
    every database after a server restart.

    Reservations are auto-released when the calling process dies (via
    Erlang process monitors), so crashed operations do not leak space.

    .. config:option:: enabled :: Master enable for all space checks

        Master switch for space reservation checks across all components.
        Individual components (smoosh, reshard) can override with their
        own config keys. Default ``false``::

            [space_monitor]
            enabled = false

    .. config:option:: min_free_floor_bytes :: Minimum free per node

        Minimum free bytes that must remain on a node before
        :erlang:`couch_space_monitor:reserve/3` will accept new
        reservations. Default 10 GB::

            [space_monitor]
            min_free_floor_bytes = 10000000000

.. config:section:: smoosh :: Smoosh Space Check Integration

    .. config:option:: check_space_before_compact :: Check space before compacting

        When ``true``, smoosh queries ``couch_space_monitor`` before
        starting database or view compactions. Compactions are deferred
        if insufficient space is available. Default ``false``::

            [smoosh]
            check_space_before_compact = false

.. config:section:: reshard :: Manual Reshard Space Checks

    .. config:option:: check_space_before_split :: Check space before manual split

        When ``true``, manual shard splits via ``POST /_reshard/jobs``
        check ``couch_space_monitor`` before reserving 3x source
        size on the local node. Falls back to
        ``[space_monitor] enabled`` if unset::

            [reshard]
            check_space_before_split = false

Split Lifecycle
---------------

When the auto-shard system decides to split a shard, the following
sequence executes:

1. **Pre-flight check** — verify target nodes have ``min_free_space_factor``
   times the target shard size available (default 3x: data + compaction + index)
2. **Create target DBs** — empty shard databases on destination nodes
3. **Bulk replication** — ``mem3_rep:go/3`` streams docs from source to
   targets, routing each doc by CRC32 hash to the correct target range
4. **Topoff 1** — catch up any writes that occurred during bulk replication
5. **Build indices** — rebuild view indices on target shards (parallel)
6. **Topoff 2** — catch up writes during index build
7. **Copy local docs** — replication checkpoints, security docs
8. **Topoff 3** — catch up writes during local doc copy
9. **Update shard map** — atomic update of ``_dbs`` document; clients begin
   routing to target shards
10. **Post-map topoff** — catch writes that hit source during map propagation
11. **Final topoff** — second pass to close the propagation window
12. **Verify consistency** — doc counts, deleted counts, update sequences,
    and document distribution across targets must all match
13. **Delete source** — only after verification passes

If verification fails at step 12, the source shard is **NOT deleted**.
Both source and targets remain live. The operator must investigate and
resolve the inconsistency manually.

Split Factor Calculation
------------------------

The split factor is always rounded up to the nearest power of 2 for
clean hash range subdivision:

+----------------+-------------------+------------------+------------------+
| Shard Size     | Threshold (20 GB) | Raw Factor       | Actual Factor    |
+================+===================+==================+==================+
| 30 GB          | 20 GB             | ceil(30/20) = 2  | 2 (2-way split)  |
+----------------+-------------------+------------------+------------------+
| 80 GB          | 20 GB             | ceil(80/20) = 4  | 4 (4-way split)  |
+----------------+-------------------+------------------+------------------+
| 200 GB         | 20 GB             | ceil(200/20) = 10| 16 (16-way split)|
+----------------+-------------------+------------------+------------------+
| 500 GB         | 20 GB             | ceil(500/20) = 25| 32 (32-way split)|
+----------------+-------------------+------------------+------------------+

With ``max_split_factor = 4`` (default), a 200 GB shard splits in rounds:

- **Round 1:** 200 GB → 4 × 50 GB (capped at factor 4)
- **Round 2:** Each 50 GB → 4 × 12.5 GB (under threshold, done)

Each round triggers automatically at the next scan cycle after cooldown.

Failure Recovery
----------------

Auto-shard splits write checkpoint state to ``_local/auto_split_checkpoint_*``
docs on the source shard at every state transition. On node startup,
``mem3_auto_shard`` scans local shards for these checkpoints and recovers
interrupted splits using the same logic as compaction file recovery.

- **Crash before shard map update:** Target databases are cleaned up
  automatically by ``mem3_reshard_rep:cleanup_interrupted_split/1`` on
  the next startup. No shard map change occurred, so clients are
  unaffected. The shard is still oversized, so it will be re-split on
  the next scan cycle.

- **Crash after shard map update:** Target shards are live (clients are
  already routing to them). The source shard is kept. The recovery code
  logs a warning for operator investigation but does **not** delete
  targets — that would risk data loss for clients already routing to them.
  The operator should verify consistency manually and delete the source
  when satisfied.

- **Replication crash mid-transfer:** ``mem3_rep`` uses checkpointed
  replication. On retry, replication resumes from the last checkpoint,
  not from the beginning. A 4 TB shard that crashes at 3.5 TB resumes
  from 3.5 TB.

- **Circuit breaker opens during split:** Running splits continue to
  completion (they use internal replication which handles slow nodes).
  New scans are blocked until all circuits close.

- **Disk fills during split:** The pre-flight check requires 3x target
  shard size free, accounting for in-flight reservations. If disk fills
  despite this (due to other writes), the replication will fail and
  targets are cleaned up via the checkpoint recovery path.

- **Verification race with writes:** Doc count verification retries up to
  3 times with a 1 second delay if counts mismatch, to handle in-flight
  writes that arrive between reading source and target counts.

Active Task Visibility
----------------------

Shard splits register as tasks in ``/_active_tasks`` (the same place
compactions appear) so operators can monitor progress alongside other
background operations.

.. code-block:: bash

    $ curl -s $COUCH_URL:5984/_active_tasks | jq '.[] | select(.type == "shard_split")'
    {
        "type": "shard_split",
        "database": "shards/00000000-ffffffff/bigdb.1234567890",
        "split_factor": 4,
        "phase": "replicating",
        "progress": 15,
        "changes_done": 2,
        "total_changes": 13,
        "started_on": 1712534400,
        "updated_on": 1712534456,
        "pid": "<0.1234.0>"
    }

The 13 phases (matching ``changes_done``) are:

1. ``creating_targets``
2. ``replicating``
3. ``topoff_1``
4. ``building_indices``
5. ``topoff_2``
6. ``copying_local``
7. ``topoff_3``
8. ``updating_map``
9. ``topoff_post_map``
10. ``topoff_final``
11. ``verifying``
12. ``deleting_source``
13. ``completed``

Tuning Guide
------------

**Small clusters (3-6 nodes):** Start with defaults. Reduce
``max_concurrent_splits`` to 1 if I/O is a concern during splits.

**Large clusters (10+ nodes):** Increase ``max_concurrent_splits`` to 3-4.
The weighted placement will spread targets across many nodes.

**Cross-datacenter clusters:** Use ``maintenance_window`` to restrict
splits to off-peak hours. Circuit breakers prevent splits during
partitions.

**Very large databases (1 TB+):** Keep ``max_split_factor = 4`` to split
gradually. Each round takes time but uses less temporary space.

.. _config/database_paths:

Per-Database Directory Mapping
==============================

.. config:section:: database_paths :: Per-Database Directory Configuration

    CouchDB can store different databases on different mount points. This
    enables spreading I/O across multiple disks, placing hot databases on
    fast storage (NVMe), and archiving cold databases on slower storage.

    Path rules support exact matches and glob patterns. Rules are checked
    in order; the first match wins::

        [database_paths]
        important_db = /mnt/nvme1
        archive_* = /mnt/hdd_array
        shards/*/users.* = /mnt/ssd

    When no rule matches and ``database_dirs`` is configured, new databases
    are placed on the directory with the most free space::

        [couchdb]
        database_dirs = /data1,/data2,/data3

    When neither ``database_paths`` nor ``database_dirs`` is configured,
    all databases use the single ``database_dir`` (backward compatible).

Multi-Directory Setup
---------------------

To spread databases across multiple mount points:

1. Create the directories and ensure CouchDB has write access
2. Configure ``database_dirs`` for automatic allocation::

    [couchdb]
    database_dirs = /mnt/ssd1,/mnt/ssd2,/mnt/hdd1

3. Optionally add per-database rules for explicit placement::

    [database_paths]
    shards/*/users.* = /mnt/ssd1
    shards/*/archive_*.* = /mnt/hdd1

4. On startup, CouchDB scans all configured directories for existing
   ``.couch`` files and registers them in an in-memory path registry

**Limitations:**

- Moving a database between directories requires copying the file and
  restarting CouchDB (or updating the internal registry)
- If a directory becomes unavailable, databases on it become inaccessible
- Path rules are evaluated at database creation time; changing rules does
  not move existing databases
- If two directories contain the same database name, the first directory
  scanned wins
