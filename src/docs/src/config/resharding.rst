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
        index space. Default is 3::

            [auto_shard]
            min_free_space_factor = 3

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
    - Pre-flight disk space check before each split
    - Mandatory consistency verification before source shard deletion
    - If verification fails, source is NOT deleted and operator is alerted

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
