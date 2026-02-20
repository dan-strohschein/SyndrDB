# SyndrDB Profiler Metrics Reference

This document catalogs every metric in SyndrDB, organized for building a profiler application with filtering support. All information is derived directly from the source code.

---

## Metrics Architecture Overview

SyndrDB has four metric scopes, each backed by a separate Go struct with `atomic.Uint64` (or `atomic.Int64`) fields for lock-free concurrent access:

| Scope | Struct | Singleton | Access Pattern | Key Format |
|---|---|---|---|---|
| **Global Server** | `GlobalServerMetrics` | `GetGlobalServerMetrics()` | Single instance, server-wide | N/A |
| **Memory Tracking** | `GlobalMemoryMetrics` | `GetGlobalMemoryMetrics()` | Single instance, server-wide | N/A |
| **Per-Database** | `DatabaseMetrics` | `GetDatabaseMetrics(dbName)` | Lazy-init via `sync.Map` | `dbName` |
| **Per-Bundle** | `BundleMetrics` | `GetBundleMetrics(dbName, bundleName)` | Lazy-init via `sync.Map` | `"dbName:bundleName"` |

**Source file:** `src/internal/server/memory_metrics.go`

### Export Mechanisms

| Mechanism | What It Exports | Format | Interval |
|---|---|---|---|
| `GetMetrics()` method | `map[string]uint64` from any metric struct | Go map | On-demand |
| `GetMetricDescriptors()` | `[]MetricDescriptor` (136 entries, zero-alloc) | Name + `*atomic.Uint64` pointer | Pre-built at init |
| Memory-mapped exporter | All 136 GlobalServerMetrics via descriptors | Binary little-endian file | Configurable (default 1s) |
| `SHOW SERVER STATS` | GlobalServerMetrics grouped by category | SyndrQL result set | On-demand |
| `SHOW CACHE STATS` | Plan cache hit/miss/eviction rates | SyndrQL result set | On-demand |

### Memory-Mapped Binary Format

```
[version:4 bytes LE uint32][timestamp:8 bytes LE uint64][count:8 bytes LE uint64]
[key_length:4 bytes LE uint32][key:N bytes UTF-8][value:8 bytes LE uint64] ...repeated
```

- `version`: `MMAP_FORMAT_VERSION = 1` (defined in `memory_metrics.go:13`)
- `timestamp`: Unix epoch seconds
- `count`: Number of metric entries
- Source: `src/internal/monitoring/mmap_exporter.go`

### Metric Types

Throughout this document, each metric is classified as one of:

| Type | Behavior | Reset | Example |
|---|---|---|---|
| **counter** | Monotonically increasing via `.Add()` | Never (delta by consumer) | `hash_index_puts_total` |
| **gauge** | Can increase or decrease | Via `.Store()` or `.Add(^uint64(0))` | `sessions_active` |
| **histogram_bucket** | Counter for a specific latency range | Never | `query_latency_lt_1ms` |
| **snapshot** | Overwritten each cycle via `.Store()` | Overwritten | `ghost_cleanup_duration_ms` |

### Instrumentation Patterns

Metrics are incremented via two patterns:

1. **Direct:** Code calls `GetGlobalServerMetrics().FieldName.Add(1)` directly.
   - Source: `command_director.go`, `document_operations.go`, `session_manager.go`, `subquery_metrics.go`

2. **Callback (MetricsReporter):** A `func(metricName string, value uint64)` callback is passed to subsystems that cannot import the `server` package. The callback contains a switch statement routing string names to atomic fields.
   - Service manager callback: `src/internal/server/service_manager.go:177-265` (routes index + plan cache metrics)
   - Server callback: `src/internal/server/server.go:748-796` (routes ghost cleanup + MVCC GC metrics)

---

## Global Server Metrics (136 metrics)

All fields are `atomic.Uint64` on the `GlobalServerMetrics` struct. Exported via `GetMetrics()` as `map[string]uint64` and via `GetMetricDescriptors()` as `[]MetricDescriptor`.

### Category: Hash Index Operations

**Filter prefix:** `hash_index_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `hash_index_puts_total` | `HashIndexPutsTotal` | counter | ops | Total PUT operations across all hash indexes | Every successful `HashIndexV3.Put()` | `hash_index_api.go:1286` via `updatePutStats()` |
| `hash_index_gets_total` | `HashIndexGetsTotal` | counter | ops | Total GET operations across all hash indexes | Every `HashIndexV3.Get()` call (cache hit or miss) | `hash_index_api.go:1306` via `updateGetStats()` |
| `hash_index_deletes_total` | `HashIndexDeletesTotal` | counter | ops | Total DELETE (tombstone) operations across all hash indexes | Every successful `HashIndexV3.Delete()` | `hash_index_api.go:1328` via `updateDeleteStats()` |
| `hash_index_cache_hits` | `HashIndexCacheHits` | counter | ops | MemTable cache hits during Get operations | When entry found in MemTable during `Get()` | `hash_index_api.go:1346` via `updateCacheHit()` |
| `hash_index_cache_misses` | `HashIndexCacheMisses` | counter | ops | MemTable cache misses requiring disk scan | When entry not found in MemTable during `Get()` | `hash_index_api.go:1358` via `updateCacheMiss()` |
| `hash_index_disk_reads` | `HashIndexDiskReads` | counter | ops | Disk read operations for hash indexes | Routed via callback but only local stats incremented | `service_manager.go:190` (callback defined) |
| `hash_index_collisions` | `HashIndexCollisions` | counter | ops | Hash collisions encountered | **Placeholder** - defined but never incremented | `memory_metrics.go:112` |
| `hash_index_rehashes` | `HashIndexRehashes` | counter | ops | Rehash operations performed | **Placeholder** - defined but never incremented | `memory_metrics.go:113` |
| `hash_index_bucket_splits` | `HashIndexBucketSplits` | counter | ops | Bucket split operations | **Placeholder** - defined but never incremented | `memory_metrics.go:114` |
| `hash_index_bucket_merges` | `HashIndexBucketMerges` | counter | ops | Bucket merge operations | **Placeholder** - defined but never incremented | `memory_metrics.go:115` |

### Category: Hash Index Latency

**Filter prefix:** `hash_index_put_latency_`, `hash_index_get_latency_`

Latency is measured as `time.Since(start).Seconds()*1000` (milliseconds). Buckets are exclusive (each operation increments exactly one bucket). Reported via `reportLatencyBucket()` in `hash_index_api.go:1365-1380`.

| Export Name | Struct Field | Type | Unit | Bucket Range | Updated When | Source |
|---|---|---|---|---|---|---|
| `hash_index_put_latency_lt_1ms` | `HashIndexPutLatencyLt1ms` | histogram_bucket | ops | < 1ms | `updatePutStatsWithLatency()` called after Put | `hash_index_api.go:1291-1296` |
| `hash_index_put_latency_lt_10ms` | `HashIndexPutLatencyLt10ms` | histogram_bucket | ops | 1ms - 10ms | Same | Same |
| `hash_index_put_latency_lt_100ms` | `HashIndexPutLatencyLt100ms` | histogram_bucket | ops | 10ms - 100ms | Same | Same |
| `hash_index_put_latency_lt_1s` | `HashIndexPutLatencyLt1s` | histogram_bucket | ops | 100ms - 1s | Same | Same |
| `hash_index_put_latency_gte_1s` | `HashIndexPutLatencyGte1s` | histogram_bucket | ops | >= 1s | Same | Same |
| `hash_index_get_latency_lt_1ms` | `HashIndexGetLatencyLt1ms` | histogram_bucket | ops | < 1ms | `updateGetStatsWithLatency()` called after Get | `hash_index_api.go:1311-1316` |
| `hash_index_get_latency_lt_10ms` | `HashIndexGetLatencyLt10ms` | histogram_bucket | ops | 1ms - 10ms | Same | Same |
| `hash_index_get_latency_lt_100ms` | `HashIndexGetLatencyLt100ms` | histogram_bucket | ops | 10ms - 100ms | Same | Same |
| `hash_index_get_latency_lt_1s` | `HashIndexGetLatencyLt1s` | histogram_bucket | ops | 100ms - 1s | Same | Same |
| `hash_index_get_latency_gte_1s` | `HashIndexGetLatencyGte1s` | histogram_bucket | ops | >= 1s | Same | Same |

### Category: Hash Index Errors

**Filter prefix:** `hash_index_*_errors`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `hash_index_put_errors` | `HashIndexPutErrors` | counter | ops | PUT errors in hash indexes | Callback route exists (`service_manager.go:192`) but **not yet called from index code** | Placeholder |
| `hash_index_get_errors` | `HashIndexGetErrors` | counter | ops | GET errors in hash indexes | Callback route exists (`service_manager.go:194`) but **not yet called from index code** | Placeholder |
| `hash_index_delete_errors` | `HashIndexDeleteErrors` | counter | ops | DELETE errors in hash indexes | Callback route exists (`service_manager.go:196`) but **not yet called from index code** | Placeholder |

### Category: B-Tree Index Operations

**Filter prefix:** `btree_index_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `btree_index_inserts_total` | `BTreeIndexInsertsTotal` | counter | ops | Total INSERT operations across all B-tree indexes | Every successful `BTreeIndex.Insert()` | `btree_index_api.go:943` via metricsReporter callback |
| `btree_index_searches_total` | `BTreeIndexSearchesTotal` | counter | ops | Total SEARCH operations across all B-tree indexes | Every `BTreeIndex.Search()` call | `btree_index_api.go:1188` via metricsReporter callback |
| `btree_index_range_queries` | `BTreeIndexRangeQueries` | counter | ops | Total RANGE QUERY operations | **Placeholder** - range search methods exist but don't report | `memory_metrics.go:133` |
| `btree_index_deletes_total` | `BTreeIndexDeletesTotal` | counter | ops | Total DELETE operations across all B-tree indexes | Every successful `BTreeIndex.Delete()` | `btree_index_api.go:1016-1017` via metricsReporter callback |
| `btree_index_cache_hits` | `BTreeIndexCacheHits` | counter | ops | B-tree page cache hits | **Placeholder** - internal PageManager tracks but doesn't report | `memory_metrics.go:135` |
| `btree_index_cache_misses` | `BTreeIndexCacheMisses` | counter | ops | B-tree page cache misses | **Placeholder** - internal PageManager tracks but doesn't report | `memory_metrics.go:136` |
| `btree_index_node_splits` | `BTreeIndexNodeSplits` | counter | ops | B-tree node split operations | **Placeholder** - BTreeMetadata.SplitCount tracks internally but doesn't report | `memory_metrics.go:137` |
| `btree_index_node_merges` | `BTreeIndexNodeMerges` | counter | ops | B-tree node merge operations | **Placeholder** - BTreeMetadata.MergeCount tracks internally but doesn't report | `memory_metrics.go:138` |
| `btree_index_rebalances` | `BTreeIndexRebalances` | counter | ops | Tree rebalance operations | **Placeholder** - defined but never incremented | `memory_metrics.go:139` |
| `btree_index_tombstones` | `BTreeIndexTombstones` | counter | ops | Tombstone entries in B-tree indexes | **Placeholder** - BTreeMetadata.TotalTombstones tracks internally but doesn't report | `memory_metrics.go:140` |

### Category: B-Tree Index Latency

**Filter prefix:** `btree_insert_latency_`, `btree_search_latency_`

Latency measured as `time.Since(start).Seconds()*1000` (ms). Reported via `reportBTreeLatencyBucket()` in `btree_index_api.go:1540-1554`.

| Export Name | Struct Field | Type | Unit | Bucket Range | Updated When | Source |
|---|---|---|---|---|---|---|
| `btree_insert_latency_lt_1ms` | `BTreeInsertLatencyLt1ms` | histogram_bucket | ops | < 1ms | After every successful Insert | `btree_index_api.go:944` |
| `btree_insert_latency_lt_10ms` | `BTreeInsertLatencyLt10ms` | histogram_bucket | ops | 1ms - 10ms | Same | Same |
| `btree_insert_latency_lt_100ms` | `BTreeInsertLatencyLt100ms` | histogram_bucket | ops | 10ms - 100ms | Same | Same |
| `btree_insert_latency_lt_1s` | `BTreeInsertLatencyLt1s` | histogram_bucket | ops | 100ms - 1s | Same | Same |
| `btree_insert_latency_gte_1s` | `BTreeInsertLatencyGte1s` | histogram_bucket | ops | >= 1s | Same | Same |
| `btree_search_latency_lt_1ms` | `BTreeSearchLatencyLt1ms` | histogram_bucket | ops | < 1ms | After every Search | `btree_index_api.go:1189` |
| `btree_search_latency_lt_10ms` | `BTreeSearchLatencyLt10ms` | histogram_bucket | ops | 1ms - 10ms | Same | Same |
| `btree_search_latency_lt_100ms` | `BTreeSearchLatencyLt100ms` | histogram_bucket | ops | 10ms - 100ms | Same | Same |
| `btree_search_latency_lt_1s` | `BTreeSearchLatencyLt1s` | histogram_bucket | ops | 100ms - 1s | Same | Same |
| `btree_search_latency_gte_1s` | `BTreeSearchLatencyGte1s` | histogram_bucket | ops | >= 1s | Same | Same |

### Category: B-Tree Index Errors

**Filter prefix:** `btree_index_*_errors`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `btree_index_insert_errors` | `BTreeIndexInsertErrors` | counter | ops | INSERT errors in B-tree indexes | Callback route exists (`service_manager.go:226`) but **not yet called from index code** | Placeholder |
| `btree_index_search_errors` | `BTreeIndexSearchErrors` | counter | ops | SEARCH errors in B-tree indexes | Callback route exists (`service_manager.go:228`) but **not yet called from index code** | Placeholder |
| `btree_index_delete_errors` | `BTreeIndexDeleteErrors` | counter | ops | DELETE errors in B-tree indexes | Callback route exists (`service_manager.go:230`) but **not yet called from index code** | Placeholder |

### Category: Query Execution

**Filter prefix:** `query_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `query_executions_total` | `QueryExecutionsTotal` | counter | ops | Total SELECT queries executed | Start of every `ExecuteSelect()` | `command_director.go:1085` |
| `query_plan_cache_hits` | `QueryPlanCacheHits` | counter | ops | Plan cache hits (plan found, not stale) | `ShardedPlanCache.Get()` finds valid plan | `plan_cache.go:246` via metricsReporter |
| `query_plan_cache_misses` | `QueryPlanCacheMisses` | counter | ops | Plan cache misses (must build new plan) | `ShardedPlanCache.Get()` finds no plan | `plan_cache.go:257` via metricsReporter |
| `query_timeouts_total` | `QueryTimeoutsTotal` | counter | ops | Queries that exceeded timeout | `ctx.Err() == context.DeadlineExceeded` in ExecuteSelect | `command_director.go:1268` |
| `query_memory_limit_exceeded` | `QueryMemoryLimitExceeded` | counter | ops | Queries exceeding memory limit | `err == planner.ErrMemoryLimitExceeded` in ExecuteSelect | `command_director.go:1281` |
| `query_full_table_scans` | `QueryFullTableScans` | counter | ops | Queries requiring full table scans | **Placeholder** - defined but never incremented | `memory_metrics.go:161` |
| `query_index_scans` | `QueryIndexScans` | counter | ops | Queries using index scans | **Placeholder** - defined but never incremented | `memory_metrics.go:162` |
| `query_joins_total` | `QueryJoinsTotal` | counter | ops | JOIN operations | When parsed query has `HasJoin() == true` | `command_director.go:1109` |
| `query_group_bys_total` | `QueryGroupBysTotal` | counter | ops | GROUP BY operations | When parsed query has `HasGroupBy() == true` | `command_director.go:1112` |
| `query_order_bys_total` | `QueryOrderBysTotal` | counter | ops | ORDER BY operations | When parsed query has `HasOrderBy() == true` | `command_director.go:1115` |

### Category: Query Latency

**Filter prefix:** `query_latency_`

Latency measured as total execution time in milliseconds from `time.Since(startTime)`. Each query increments exactly one bucket. Updated at end of every `ExecuteSelect()`.

| Export Name | Struct Field | Type | Unit | Bucket Range | Source |
|---|---|---|---|---|---|
| `query_latency_lt_1ms` | `QueryLatencyLt1ms` | histogram_bucket | ops | < 1ms | `command_director.go:1465` |
| `query_latency_lt_10ms` | `QueryLatencyLt10ms` | histogram_bucket | ops | 1ms - 10ms | `command_director.go:1467` |
| `query_latency_lt_100ms` | `QueryLatencyLt100ms` | histogram_bucket | ops | 10ms - 100ms | `command_director.go:1469` |
| `query_latency_lt_1s` | `QueryLatencyLt1s` | histogram_bucket | ops | 100ms - 1s | `command_director.go:1471` |
| `query_latency_gte_1s` | `QueryLatencyGte1s` | histogram_bucket | ops | >= 1s | `command_director.go:1473` |

### Category: Subquery Execution

**Filter prefix:** `subquery_`

Subquery metrics are recorded via the adapter pattern in `src/internal/server/subquery_metrics.go`. The `SubqueryMetricsRecorder` implements the `subquery.MetricsRecorder` interface and bridges to `GlobalServerMetrics`.

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `subquery_executions_total` | `SubqueryExecutionsTotal` | counter | ops | Total subqueries executed | Every `RecordSubqueryExecution()` call | `subquery_metrics.go:32` |
| `subquery_inlist_strategy` | `SubqueryInListStrategy` | counter | ops | Subqueries using IN-list materialization | When `strategy == STRATEGY_INLIST` | `subquery_metrics.go:37` |
| `subquery_hashjoin_strategy` | `SubqueryHashJoinStrategy` | counter | ops | Subqueries using HashJoin strategy | When `strategy == STRATEGY_HASHJOIN` | `subquery_metrics.go:39` |
| `subquery_indexed_lookup_strategy` | `SubqueryIndexedLookupStrategy` | counter | ops | Subqueries using indexed lookup (Tier 3) | When `strategy == STRATEGY_INDEXED_LOOKUP` | `subquery_metrics.go:41` |
| `subquery_depth_exceeded` | `SubqueryDepthExceeded` | counter | ops | Subqueries rejected for max depth | **Placeholder** - defined but never incremented | `memory_metrics.go:172` |
| `subquery_memory_limit_exceeded` | `SubqueryMemoryLimitExceeded` | counter | ops | Subqueries forced to HashJoin for memory | **Placeholder** - defined but never incremented | `memory_metrics.go:173` |
| `subquery_contains_null` | `SubqueryContainsNull` | counter | ops | Subqueries with NULL in results | When `containsNull == true` | `subquery_metrics.go:46` |

### Category: Plan Cache Enhanced

**Filter prefix:** `query_plan_cache_`

Plan cache metrics are reported via the metricsReporter callback from `ShardedPlanCache` (8-shard LRU) in `src/internal/query/planner/plan_cache.go`.

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `query_plan_cache_evictions` | `QueryPlanCacheEvictions` | counter | ops | LRU evictions due to capacity limits | When shard is at capacity and oldest entry is removed during `insert()` | `plan_cache.go:401` via parent.metricsReporter |
| `query_plan_cache_invalidations` | `QueryPlanCacheInvalidations` | counter | ops | Invalidations due to writes/schema changes | `InvalidateBundle()` called (write threshold exceeded) | `plan_cache.go:656` via metricsReporter |
| `query_plan_cache_stale_serves` | `QueryPlanCacheStaleServes` | counter | ops | Stale plans served during async rebuild | When `Get()` returns a plan marked stale (version mismatch) | `plan_cache.go:240` via metricsReporter |
| `query_plan_cache_memory_bytes` | `QueryPlanCacheMemoryBytes` | gauge | bytes | Estimated memory used by plan cache | **Placeholder** - defined but never incremented | `memory_metrics.go:248` |

### Category: Transaction

**Filter prefix:** `transactions_`

Transaction metrics are incremented directly on `GlobalServerMetrics` at four call sites covering auto-commit (ADD/UPDATE/DELETE) and WAL-logged operations.

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `transactions_begun` | `TransactionsBegun` | counter | ops | Transactions started (explicit + auto-commit) | Start of every WAL-logged operation (ADD/UPDATE/DELETE auto-commit, DROP RELATIONSHIP) | `document_operations.go:71,355`, `command_director.go:472,901` |
| `transactions_committed` | `TransactionsCommitted` | counter | ops | Transactions successfully committed | WAL ExecuteWithLogging succeeds (err == nil) | `document_operations.go:87,381`, `command_director.go:492,919` |
| `transactions_rolled_back` | `TransactionsRolledBack` | counter | ops | Transactions rolled back | WAL ExecuteWithLogging fails (err != nil) | `document_operations.go:83,379`, `command_director.go:489,917` |
| `transactions_aborted` | `TransactionsAborted` | counter | ops | Transactions aborted by server | **Placeholder** - defined but never incremented | `memory_metrics.go:187` |

### Category: MVCC Lock Contention

**Filter prefix:** `rotation_lock_`, `atomic_operations_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `rotation_lock_wait_time_total_ns` | `RotationLockWaitTimeTotal` | counter | nanoseconds | Total time waiting for rotation locks | **Placeholder** - defined but never incremented | `memory_metrics.go:190` |
| `rotation_lock_acquisitions` | `RotationLockAcquisitions` | counter | ops | Rotation lock acquisitions | **Placeholder** - defined but never incremented | `memory_metrics.go:191` |
| `atomic_operations_total` | `AtomicOperationsTotal` | counter | ops | Total atomic operations | **Placeholder** - defined but never incremented | `memory_metrics.go:192` |

### Category: WAL (Write-Ahead Log)

**Filter prefix:** `wal_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `wal_writes_total` | `WALWritesTotal` | counter | ops | WAL write operations | **Placeholder** - defined but never incremented | `memory_metrics.go:207` |
| `wal_flushes_total` | `WALFlushesTotal` | counter | ops | WAL flush operations | **Placeholder** - defined but never incremented | `memory_metrics.go:208` |
| `wal_bytes_written` | `WALBytesWritten` | counter | bytes | Bytes written to WAL | **Placeholder** - defined but never incremented | `memory_metrics.go:209` |
| `wal_segment_rotations` | `WALSegmentRotations` | counter | ops | WAL segment file rotations | **Placeholder** - defined but never incremented | `memory_metrics.go:210` |
| `wal_syncs_total` | `WALSyncsTotal` | counter | ops | fsync operations on WAL | **Placeholder** - defined but never incremented | `memory_metrics.go:211` |

### Category: WAL Replay

**Filter prefix:** `wal_replay_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `wal_replay_entries_total` | `WALReplayEntriesTotal` | counter | ops | WAL entries replayed during recovery | **Placeholder** - defined but never incremented | `memory_metrics.go:269` |
| `wal_replay_errors_total` | `WALReplayErrorsTotal` | counter | ops | Errors during WAL replay | **Placeholder** - defined but never incremented | `memory_metrics.go:270` |
| `wal_replay_duration_ms` | `WALReplayDurationMs` | gauge | milliseconds | Time spent replaying WAL | **Placeholder** - defined but never incremented | `memory_metrics.go:271` |
| `wal_replay_last_timestamp` | `WALReplayLastTimestamp` | gauge | unix_epoch | Timestamp of last WAL replay | **Placeholder** - defined but never incremented | `memory_metrics.go:272` |

### Category: WAL Replication

**Filter prefix:** `wal_last_`, `wal_replication_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `wal_last_flush_timestamp` | `WALLastFlushTimestamp` | gauge | unix_epoch_seconds | Last WAL flush timestamp | **Placeholder** - defined but never incremented | `memory_metrics.go:274` |
| `wal_last_sync_timestamp` | `WALLastSyncTimestamp` | gauge | unix_epoch_seconds | Last WAL sync timestamp | **Placeholder** - defined but never incremented | `memory_metrics.go:275` |
| `wal_replication_lag_ms` | `WALReplicationLagMs` | gauge | milliseconds | Replication lag | **Placeholder** - defined but never incremented | `memory_metrics.go:276` |

### Category: Compaction

**Filter prefix:** `compaction_`, `compactions_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `compactions_total` | `CompactionsTotal` | counter | ops | Total compaction operations | **Placeholder** - defined but never incremented | `memory_metrics.go:214` |
| `compaction_bytes_read` | `CompactionBytesRead` | counter | bytes | Bytes read during compaction | **Placeholder** - defined but never incremented | `memory_metrics.go:215` |
| `compaction_bytes_written` | `CompactionBytesWritten` | counter | bytes | Bytes written during compaction | **Placeholder** - defined but never incremented | `memory_metrics.go:216` |
| `compaction_tombstones` | `CompactionTombstones` | counter | ops | Tombstones removed during compaction | **Placeholder** - defined but never incremented | `memory_metrics.go:217` |

### Category: Compaction Duration Histogram

**Filter prefix:** `compaction_duration_`

| Export Name | Struct Field | Type | Unit | Bucket Range | Updated When | Source |
|---|---|---|---|---|---|---|
| `compaction_duration_lt_100ms` | `CompactionDurationLt100ms` | histogram_bucket | ops | < 100ms | **Placeholder** | `memory_metrics.go:256` |
| `compaction_duration_lt_1s` | `CompactionDurationLt1s` | histogram_bucket | ops | 100ms - 1s | **Placeholder** | `memory_metrics.go:257` |
| `compaction_duration_lt_10s` | `CompactionDurationLt10s` | histogram_bucket | ops | 1s - 10s | **Placeholder** | `memory_metrics.go:258` |
| `compaction_duration_gte_10s` | `CompactionDurationGte10s` | histogram_bucket | ops | >= 10s | **Placeholder** | `memory_metrics.go:259` |

### Category: Compaction Triggers

**Filter prefix:** `compaction_triggered_`, `compaction_blocked_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `compaction_triggered_scheduled` | `CompactionTriggeredScheduled` | counter | ops | Compactions triggered by schedule | **Placeholder** | `memory_metrics.go:261` |
| `compaction_triggered_manual` | `CompactionTriggeredManual` | counter | ops | Compactions triggered manually | **Placeholder** | `memory_metrics.go:262` |
| `compaction_triggered_threshold` | `CompactionTriggeredThreshold` | counter | ops | Compactions triggered by threshold | **Placeholder** | `memory_metrics.go:263` |
| `compaction_triggered_emergency` | `CompactionTriggeredEmergency` | counter | ops | Compactions triggered by emergency | **Placeholder** | `memory_metrics.go:264` |
| `compaction_triggered_ghost` | `CompactionTriggeredGhost` | counter | ops | Compactions triggered by ghost cleanup | Callback route exists (`server.go:771`) but **not yet called from worker** | Placeholder |
| `compaction_blocked_by_lock` | `CompactionBlockedByLock` | counter | ops | Compactions blocked by lock contention | Callback route exists (`server.go:773`) but **not yet called from worker** | Placeholder |

### Category: Session

**Filter prefix:** `session`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `sessions_active` | `SessionsActive` | gauge | count | Current active sessions | Incremented in `CreateSession()`, decremented via `^uint64(0)` in `InvalidateSession()` | `session_manager.go:474,572` |
| `sessions_created` | `SessionsCreated` | counter | ops | Total sessions created since start | Every `CreateSession()` | `session_manager.go:473` |
| `sessions_terminated` | `SessionsTerminated` | counter | ops | Total sessions terminated | Every `InvalidateSession()` | `session_manager.go:571` |
| `session_auth_failures` | `SessionAuthFailures` | counter | ops | Authentication failures | **Placeholder** - defined but never incremented | `memory_metrics.go:223` |

### Category: Document Operations

**Filter prefix:** `document_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `document_inserts_total` | `DocumentInsertsTotal` | counter | ops | Document insertions across all bundles | After successful `AddDocument()` (post-WAL) | `document_operations.go:396` |
| `document_updates_total` | `DocumentUpdatesTotal` | counter | ops | Document updates across all bundles | After successful UPDATE (OCC path: line 90, pessimistic path: line 247) | `document_operations.go:90,247` |
| `document_deletes_total` | `DocumentDeletesTotal` | counter | docs | Document deletions across all bundles (batch count) | After successful DELETE; adds `len(docIDs)` | `command_director.go:933` |
| `document_reads_total` | `DocumentReadsTotal` | counter | ops | Document reads across all bundles | **Placeholder** - defined but never incremented | `memory_metrics.go:229` |

### Category: Ghost Cleanup

**Filter prefix:** `ghost_cleanup_`, `ghost_records_`

Ghost cleanup runs as a background worker (`src/internal/domain/compactor/ghost_cleanup_worker.go`). Metrics reported via callback routed through `server.go:748-796`.

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `ghost_cleanup_cycles_total` | `GhostCleanupCyclesTotal` | counter | ops | Ghost cleanup cycles executed | Every `performCleanup()` cycle | `ghost_cleanup_worker.go:226` via callback |
| `ghost_records_scanned` | `GhostRecordsScanned` | counter | ops | Ghost records scanned | Callback route exists (`server.go:753`) but **not yet called from worker** | Placeholder |
| `ghost_records_removed` | `GhostRecordsRemoved` | counter | ops | Ghost records (tombstones) removed | Callback route exists (`server.go:755`) but **not yet called from worker** | Placeholder |
| `ghost_cleanup_duration_ms` | `GhostCleanupDurationMs` | snapshot | milliseconds | Duration of last cleanup cycle (overwritten each cycle via `.Store()`) | End of every `performCleanup()` cycle | `ghost_cleanup_worker.go:235` via callback |
| `ghost_cleanup_paused_for_load` | `GhostCleanupPausedForLoad` | counter | ops | Times cleanup paused for high query load | When `activeQueryCount >= pauseThreshold` | `ghost_cleanup_worker.go:219` via callback |
| `ghost_cleanup_batches_processed` | `GhostCleanupBatchesProcessed` | counter | ops | Batches processed | Callback route exists (`server.go:761`) but **not yet called from worker** | Placeholder |

### Category: Tombstone Cache

**Filter prefix:** `tombstone_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `tombstone_cache_hits` | `TombstoneCacheHits` | counter | ops | Cache hits checking tombstone ratios | Callback route exists (`server.go:763`) but **not yet called** | Placeholder |
| `tombstone_cache_misses` | `TombstoneCacheMisses` | counter | ops | Cache misses requiring file scans | Callback route exists (`server.go:765`) but **not yet called** | Placeholder |
| `tombstone_scans_performed` | `TombstoneScansPerformed` | counter | ops | File scans performed | Callback route exists (`server.go:767`) but **not yet called** | Placeholder |
| `tombstone_cache_evictions` | `TombstoneCacheEvictions` | counter | ops | Cache entries evicted | Callback route exists (`server.go:769`) but **not yet called** | Placeholder |

### Category: Orphaned File Cleanup

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `orphaned_temp_files_removed` | `OrphanedTempFilesRemoved` | counter | ops | Orphaned `.tmp`/`.idx.tmp`/`.compact.tmp` files cleaned up | After `cleanupOrphanedTempFiles()` removes files older than 1 hour | `ghost_cleanup_worker.go:320` via callback |

### Category: MVCC Garbage Collection

**Filter prefix:** `mvcc_gc_`

MVCC GC runs as a background worker (`src/internal/domain/compactor/mvcc_gc_worker.go`). Metrics reported via callback routed through `server.go:777-794`.

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `mvcc_gc_cycles_total` | `MVCCGCCyclesTotal` | counter | ops | GC cycles executed (periodic + immediate) | Start of every `performGCCycleWithContext()` | `mvcc_gc_worker.go:319` via callback |
| `mvcc_gc_bundles_scanned` | `MVCCGCBundlesScanned` | counter | bundles | Bundles analyzed per cycle | End of GC cycle; adds count of bundles scanned | `mvcc_gc_worker.go:500` via callback |
| `mvcc_gc_versions_removed` | `MVCCGCVersionsRemoved` | counter | versions | Document versions removed by GC | End of GC cycle; adds count of dead versions reclaimed | `mvcc_gc_worker.go:502` via callback |
| `mvcc_gc_compactions_triggered` | `MVCCGCCompactionsTriggered` | counter | ops | Compactions triggered by dead ratio threshold | End of GC cycle; adds count of compactions triggered | `mvcc_gc_worker.go:501` via callback |
| `mvcc_gc_paused_for_load` | `MVCCGCPausedForLoad` | counter | ops | Times GC paused for high load | When `activeQueryCount >= pauseThreshold` (periodic triggers only) | `mvcc_gc_worker.go:312` via callback |
| `mvcc_gc_duration_ms` | `MVCCGCDurationMs` | snapshot | milliseconds | Duration of last GC cycle (overwritten each cycle via `.Store()`) | End of every GC cycle | `mvcc_gc_worker.go:499` via callback |
| `mvcc_gc_versions_preserved` | `MVCCGCVersionsPreserved` | counter | versions | Versions kept due to active snapshots | End of GC cycle; adds count of preserved versions | `mvcc_gc_worker.go:503` via callback |
| `mvcc_gc_startup_triggers` | `MVCCGCStartupTriggers` | counter | ops | GC cycles triggered on server startup | Callback route exists (`server.go:791`) but **not yet called** | Placeholder |
| `mvcc_gc_shutdown_triggers` | `MVCCGCShutdownTriggers` | counter | ops | GC cycles triggered on server shutdown | Callback route exists (`server.go:793`) but **not yet called** | Placeholder |

### Category: DateTime Parsing

**Filter prefix:** `datetime_`

| Export Name | Struct Field | Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `datetime_parse_attempts_total` | `DateTimeParseAttemptsTotal` | counter | ops | DateTime parse attempts | **Placeholder** - only incremented in test code (`datetime_e2e_test.go`) | `memory_metrics.go:317` |
| `datetime_parse_success_total` | `DateTimeParseSuccessTotal` | counter | ops | Successful datetime parses | **Placeholder** - only incremented in test code | `memory_metrics.go:318` |
| `datetime_parse_errors_total` | `DateTimeParseErrorsTotal` | counter | ops | Failed datetime parse attempts | **Placeholder** - only incremented in test code | `memory_metrics.go:319` |

---

## Global Memory Metrics (4 metrics)

Separate from GlobalServerMetrics. Uses `atomic.Int64` fields. Singleton via `GetGlobalMemoryMetrics()`. Updated per-query via `MemoryTracker.RecordMetrics()` at the end of query execution (`memory_tracker.go:161`).

**Source file:** `src/internal/server/memory_metrics.go:15-83`, `src/internal/server/memory_tracker.go`

**NOT included in MetricDescriptors or mmap export.** Accessed via `GetGlobalMemoryMetrics().GetMetrics()` which returns `map[string]interface{}`.

| Export Name | Type | Go Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|
| `memory_limit_exceeded_count` | counter | `int64` | ops | Queries that exceeded the memory limit | `RecordQuery(projected, exceeded=true)` | `memory_metrics.go:42-44` |
| `queries_checked_count` | counter | `int64` | ops | Queries with memory tracking enabled | Every `RecordQuery()` call | `memory_metrics.go:39` |
| `avg_projected_memory_bytes` | computed | `int64` | bytes | Average projected memory (totalProjected / totalChecked) | Computed on read in `GetMetrics()` | `memory_metrics.go:66-67` |
| `max_projected_memory_bytes` | gauge | `int64` | bytes | Maximum projected memory seen (CAS update - only increases) | `RecordQuery()` when new value > current max | `memory_metrics.go:47-55` |

**Update frequency:** Once per SELECT query completion. Called from `MemoryTracker.RecordMetrics(totalExpected)` in `memory_tracker.go:161`.

**Memory tracking parameters:**
- Sample rate: every 100th document (`constants.SampleRateDefault`)
- Projection: `avgDocSize * totalExpectedDocuments`
- Limit: configurable via `settings.QueryMaxMemoryMB` (default 25MB)

---

## Per-Database Metrics (13 metrics)

Struct: `DatabaseMetrics`. Lazy-initialized via `GetDatabaseMetrics(dbName)`. Stored in `sync.Map` keyed by database name.

**Source file:** `src/internal/server/memory_metrics.go:733-784`

| Export Name | Struct Field | Type | Go Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|---|
| `db_bundles_count` | `DBBundlesCount` | gauge | `atomic.Uint64` | count | Current bundle count in database | **Placeholder** | -- |
| `db_documents_count` | `DBDocumentsCount` | gauge | `atomic.Uint64` | count | Current document count in database | **Placeholder** | -- |
| `db_total_size_bytes` | `DBTotalSizeBytes` | gauge | `atomic.Uint64` | bytes | Total database size | **Placeholder** | -- |
| `db_queries_executed` | `DBQueriesExecuted` | counter | `atomic.Uint64` | ops | Queries executed on this database | **Placeholder** | -- |
| `db_transactions_committed` | `DBTransactionsCommitted` | counter | `atomic.Uint64` | ops | Transactions committed for this database | **Placeholder** | -- |
| `db_transactions_rolled_back` | `DBTransactionsRolledBack` | counter | `atomic.Uint64` | ops | Transactions rolled back for this database | **Placeholder** | -- |
| `db_wal_bytes_written` | `DBWALBytesWritten` | counter | `atomic.Uint64` | bytes | WAL bytes written for this database | **Placeholder** | -- |
| `db_compactions_performed` | `DBCompactionsPerformed` | counter | `atomic.Uint64` | ops | Compactions on this database | **Placeholder** | -- |
| `db_indexes_count` | `DBIndexesCount` | gauge | `atomic.Uint64` | count | Index count in this database | **Placeholder** | -- |
| `db_sessions_active` | `DBSessionsActive` | gauge | `atomic.Uint64` | count | Active sessions for this database | **Placeholder** | -- |
| `db_document_inserts_total` | `DBDocumentInsertsTotal` | counter | `atomic.Uint64` | ops | Document insertions in this database | After successful `AddDocument()` | `document_operations.go:398` |
| `db_document_updates_total` | `DBDocumentUpdatesTotal` | counter | `atomic.Uint64` | ops | Document updates in this database | After successful UPDATE (both OCC and pessimistic) | `document_operations.go:92,249` |
| `db_document_deletes_total` | `DBDocumentDeletesTotal` | counter | `atomic.Uint64` | docs | Document deletions in this database | After successful DELETE; adds `len(docIDs)` | `command_director.go:935` |

---

## Per-Bundle Metrics (19 metrics)

Struct: `BundleMetrics`. Lazy-initialized via `GetBundleMetrics(dbName, bundleName)`. Stored in `sync.Map` keyed by `"dbName:bundleName"`.

**Source file:** `src/internal/server/memory_metrics.go:786-851`

| Export Name | Struct Field | Type | Go Type | Unit | Description | Updated When | Source |
|---|---|---|---|---|---|---|---|
| `bundle_documents_inserted` | `BundleDocumentsInserted` | counter | `atomic.Uint64` | ops | Documents inserted into this bundle | After successful `AddDocument()` | `document_operations.go:400` |
| `bundle_documents_updated` | `BundleDocumentsUpdated` | counter | `atomic.Uint64` | ops | Documents updated in this bundle | After successful UPDATE (both paths) | `document_operations.go:94,251` |
| `bundle_documents_deleted` | `BundleDocumentsDeleted` | counter | `atomic.Uint64` | docs | Documents deleted from this bundle | After successful DELETE; adds `len(docIDs)` | `command_director.go:937` |
| `bundle_documents_read` | `BundleDocumentsRead` | counter | `atomic.Uint64` | ops | Documents read from this bundle | **Placeholder** | -- |
| `bundle_current_doc_count` | `BundleCurrentDocCount` | gauge | `atomic.Uint64` | count | Current document count | +1 on insert (`document_operations.go:401`), subtract on delete via `^uint64(len-1)` (`command_director.go:938`) | Active |
| `bundle_total_size_bytes` | `BundleTotalSizeBytes` | gauge | `atomic.Uint64` | bytes | Total bundle size | **Placeholder** | -- |
| `bundle_indexes_count` | `BundleIndexesCount` | gauge | `atomic.Uint64` | count | Index count on this bundle | **Placeholder** | -- |
| `bundle_queries_executed` | `BundleQueriesExecuted` | counter | `atomic.Uint64` | ops | Queries on this bundle | **Placeholder** | -- |
| `bundle_full_scans` | `BundleFullScans` | counter | `atomic.Uint64` | ops | Full table scans on this bundle | **Placeholder** | -- |
| `bundle_index_scans` | `BundleIndexScans` | counter | `atomic.Uint64` | ops | Index scans on this bundle | **Placeholder** | -- |
| `bundle_compactions_performed` | `BundleCompactionsPerformed` | counter | `atomic.Uint64` | ops | Compactions on this bundle | **Placeholder** | -- |
| `bundle_relationships_count` | `BundleRelationshipsCount` | gauge | `atomic.Uint64` | count | Relationships defined on bundle | **Placeholder** | -- |
| `bundle_join_operations` | `BundleJoinOperations` | counter | `atomic.Uint64` | ops | JOIN operations involving this bundle | **Placeholder** | -- |
| `bundle_cache_hits` | `BundleCacheHits` | counter | `atomic.Uint64` | ops | Cache hits for this bundle | **Placeholder** | -- |
| `bundle_cache_misses` | `BundleCacheMisses` | counter | `atomic.Uint64` | ops | Cache misses for this bundle | **Placeholder** | -- |
| `bundle_avg_doc_size_bytes` | `BundleAvgDocSizeBytes` | gauge | `atomic.Uint64` | bytes | Average document size | **Placeholder** | -- |
| `bundle_max_doc_size_bytes` | `BundleMaxDocSizeBytes` | gauge | `atomic.Uint64` | bytes | Maximum document size | **Placeholder** | -- |
| `bundle_min_doc_size_bytes` | `BundleMinDocSizeBytes` | gauge | `atomic.Uint64` | bytes | Minimum document size (0 = not set) | **Placeholder** | -- |
| `bundle_tombstone_count` | `BundleTombstoneCount` | gauge | `atomic.Uint64` | count | Tombstone entries in this bundle | **Placeholder** | -- |

---

## Observability Features (Non-Metric)

These are not atomic counters but are related features a profiler should be aware of:

### Request Trace ID

Every command gets a hex trace ID derived from `startTime.UnixNano()`. Stored in context and enriched on the logger.

- **Generated:** `command_director.go:96` - `traceID := strconv.FormatInt(startTime.UnixNano(), 16)`
- **Context key:** `traceIDKeyType{}` (unexported typed key)
- **Logger enrichment:** `logger = logger.With("traceID", traceID)`
- **Response field:** `CommandResponse.TraceID` (`command_response.go`)
- **Format:** 16-char hex string

### Slow Query Log

Queries exceeding a configurable threshold are logged at `Warn` level with structured fields.

- **Setting:** `SlowQueryLogEnabled` (default: `true`), `SlowQueryThresholdMs` (default: `1000`)
- **Source:** `command_director.go:240-256`
- **Log fields:** `traceID`, `query` (truncated), `duration_ms`, `rows_returned`
- **Trigger:** After SELECT execution when `elapsed > SlowQueryThresholdMs`

---

## Filter Categories for Profiler UI

Suggested filter groups for a profiler application:

| Filter Category | Metric Prefixes | Count |
|---|---|---|
| Hash Index | `hash_index_` | 23 |
| B-Tree Index | `btree_index_`, `btree_insert_`, `btree_search_` | 23 |
| Query Execution | `query_` | 15 |
| Subquery | `subquery_` | 7 |
| Plan Cache | `query_plan_cache_` | 6 |
| Query Latency | `query_latency_` | 5 |
| Transaction | `transactions_` | 4 |
| Session | `session` | 4 |
| Document Ops | `document_` | 4 |
| WAL | `wal_` | 12 |
| Compaction | `compaction_`, `compactions_` | 16 |
| Ghost Cleanup | `ghost_`, `tombstone_`, `orphaned_` | 11 |
| MVCC GC | `mvcc_gc_` | 9 |
| MVCC Lock | `rotation_lock_`, `atomic_operations_` | 3 |
| DateTime | `datetime_` | 3 |
| Memory Tracking | `memory_limit_`, `queries_checked_`, `*_projected_memory_*` | 4 |
| Per-Database | `db_` | 13 |
| Per-Bundle | `bundle_` | 19 |

---

## Deprecated Metrics (Excluded from Descriptors)

These struct fields exist on `GlobalServerMetrics` but are excluded from `GetMetricDescriptors()` and should be ignored by the profiler:

| Struct Field | Reason |
|---|---|
| `QueryPlanCacheSize` | DEPRECATED: Use `query_plan_cache_hits` + `query_plan_cache_misses` |
| `QueryPlanCacheMaxSize` | DEPRECATED: Use `settings.PlanCacheCapacity` |
| `QueryPlanCacheMemoryUsed` | DEPRECATED: Use `query_plan_cache_memory_bytes` |

---

## Summary Statistics

| Scope | Total Metrics | Actively Wired | Placeholder |
|---|---|---|---|
| Global Server | 136 | 55 | 81 |
| Memory Tracking | 4 | 4 | 0 |
| Per-Database | 13 | 3 | 10 |
| Per-Bundle | 19 | 4 | 15 |
| **Total** | **172** | **66** | **106** |
