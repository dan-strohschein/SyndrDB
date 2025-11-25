## Plan: Comprehensive Three-Level Metrics Collection Implementation Guide

This guide provides the complete specification for implementing metrics collection across global, per-database, and per-bundle scopes using atomic counters with lazy initialization.

---

## File Structure

### Primary File
- **Location**: memory_metrics.go
- **Action**: Extend existing file (currently 80 lines) to ~800-1000 lines
- **Sections to Add**: After line 80, add all new metric structures

---

## Metric Structures & Specifications

### 1. GLOBAL SERVER METRICS (`GlobalServerMetrics`)

**Location**: Lines 81-250 (new section)

**Struct Name**: `GlobalServerMetrics`

**Singleton Pattern**: `var globalServerMetrics = &GlobalServerMetrics{}` with `GetGlobalServerMetrics()` accessor

#### 1.1 Index Operations - Hash Index V3

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `hashindex_puts_total` | `atomic.Uint64` | Total Put operations across all hash indexes | Tracks write load; correlate with disk I/O to identify bottlenecks |
| `hashindex_gets_total` | `atomic.Uint64` | Total Get operations across all hash indexes | Tracks read load; primary indicator of query volume |
| `hashindex_deletes_total` | `atomic.Uint64` | Total Delete operations (tombstone creation) | Monitors deletion rate; high values trigger compaction |
| `hashindex_cache_hits` | `atomic.Uint64` | MemTable cache hits (data found in memory) | Cache effectiveness; >90% is optimal for read-heavy workloads |
| `hashindex_cache_misses` | `atomic.Uint64` | MemTable cache misses (required disk read) | Disk I/O pressure; high values indicate need for more memory |
| `hashindex_disk_reads` | `atomic.Uint64` | Disk scan operations for key lookups | Directly correlates to query latency; minimize via caching |
| `hashindex_tombstones_encountered` | `atomic.Uint64` | Tombstones encountered during reads | Compaction urgency indicator; high values degrade performance |
| `hashindex_memtable_updates` | `atomic.Uint64` | Successful MemTable updates | Write-path health check; failures indicate memory pressure |
| `hashindex_latency_lt_1ms` | `atomic.Uint64` | Operations completed in <1ms | Histogram bucket for p50/p95 calculation |
| `hashindex_latency_lt_10ms` | `atomic.Uint64` | Operations completed in <10ms | Histogram bucket for latency distribution |
| `hashindex_latency_lt_100ms` | `atomic.Uint64` | Operations completed in <100ms | Histogram bucket for slower operations |
| `hashindex_latency_lt_1s` | `atomic.Uint64` | Operations completed in <1s | Histogram bucket for problematic queries |
| `hashindex_latency_gte_1s` | `atomic.Uint64` | Operations completed in ≥1s | Histogram bucket for critical slow queries |

#### 1.2 Index Operations - B-Tree

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `btree_inserts_total` | `atomic.Uint64` | Total Insert operations across all B-trees | Write volume tracking; correlate with node splits |
| `btree_searches_total` | `atomic.Uint64` | Total Search (point lookup) operations | Read query volume for indexed fields |
| `btree_range_queries_total` | `atomic.Uint64` | Total RangeQuery operations | Range scan usage; expensive operation monitoring |
| `btree_deletes_total` | `atomic.Uint64` | Total Delete operations (lazy deletion) | Deletion rate; drives compaction scheduling |
| `btree_node_splits` | `atomic.Uint64` | Node split events during insertion | Tree growth indicator; high values suggest rebalancing need |
| `btree_height_increases` | `atomic.Uint64` | Tree height increase events | Structural changes; affects all query performance |
| `btree_tombstone_accumulation` | `atomic.Uint64` | Tombstones created but not compacted | Space waste metric; triggers compaction threshold |
| `btree_cache_hits` | `atomic.Uint64` | Page cache hits (LRU cache) | Page manager effectiveness; optimize cache size |
| `btree_cache_misses` | `atomic.Uint64` | Page cache misses (disk I/O required) | Disk pressure; high misses indicate insufficient cache |
| `btree_latency_lt_1ms` | `atomic.Uint64` | Operations <1ms | Histogram for latency percentiles |
| `btree_latency_lt_10ms` | `atomic.Uint64` | Operations <10ms | Histogram bucket |
| `btree_latency_lt_100ms` | `atomic.Uint64` | Operations <100ms | Histogram bucket |
| `btree_latency_lt_1s` | `atomic.Uint64` | Operations <1s | Histogram bucket |
| `btree_latency_gte_1s` | `atomic.Uint64` | Operations ≥1s | Critical slowness indicator |

#### 1.3 Query Execution

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `query_routed_join` | `atomic.Uint64` | Queries routed to JOIN executor | JOIN usage patterns; optimize JOIN algorithms |
| `query_routed_simple` | `atomic.Uint64` | Queries routed to simple executor | Basic query volume baseline |
| `query_routed_groupby` | `atomic.Uint64` | Queries routed to GROUP BY executor | Aggregation usage; memory pressure indicator |
| `query_plan_created` | `atomic.Uint64` | Query plans created | Plan cache effectiveness denominator |
| `query_plan_cache_hits` | `atomic.Uint64` | Query plans retrieved from cache | Plan reuse rate; >50% optimal for repeating queries |
| `query_plan_cache_misses` | `atomic.Uint64` | Query plans not in cache (cold start) | Cache sizing; high misses indicate cache too small |
| `query_index_selected` | `atomic.Uint64` | Queries using index scan strategy | Index utilization rate; low values indicate missing indexes |
| `query_full_scan` | `atomic.Uint64` | Queries using full bundle scan | Expensive operations; optimize with indexes |
| `query_execution_lt_10ms` | `atomic.Uint64` | Queries completed in <10ms | Fast query histogram bucket |
| `query_execution_lt_100ms` | `atomic.Uint64` | Queries completed in <100ms | Acceptable latency bucket |
| `query_execution_lt_1s` | `atomic.Uint64` | Queries completed in <1s | Slow query bucket |
| `query_execution_gte_1s` | `atomic.Uint64` | Queries completed in ≥1s | Critical slow query indicator; investigate immediately |

#### 1.4 Transaction & WAL

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `transactions_begun` | `atomic.Uint64` | Total transactions started | Transaction volume baseline |
| `transactions_committed` | `atomic.Uint64` | Successful transaction commits | Success rate numerator |
| `transactions_rolledback` | `atomic.Uint64` | Transaction rollbacks (failures) | Failure rate; high values indicate application errors |
| `transactions_active` | `atomic.Int64` | Currently active transactions (gauge) | Concurrency level; high values indicate lock contention |
| `wal_writes_total` | `atomic.Uint64` | Total WAL write operations | Durability operations; correlate with disk I/O |
| `wal_bytes_written` | `atomic.Uint64` | Total bytes written to WAL | I/O bandwidth consumption |
| `wal_flushes` | `atomic.Uint64` | WAL flush (fsync) operations | Disk sync frequency; affects durability vs performance |
| `wal_batch_size_sum` | `atomic.Uint64` | Sum of all batch sizes (for average) | Batch efficiency calculation |
| `wal_batch_count` | `atomic.Uint64` | Number of batches written | Batching effectiveness metric |
| `wal_flush_latency_lt_1ms` | `atomic.Uint64` | Flushes <1ms | Fast flush histogram |
| `wal_flush_latency_lt_10ms` | `atomic.Uint64` | Flushes <10ms | Acceptable flush latency |
| `wal_flush_latency_lt_100ms` | `atomic.Uint64` | Flushes <100ms | Slow flush (disk bottleneck) |
| `wal_flush_latency_gte_100ms` | `atomic.Uint64` | Flushes ≥100ms | Critical disk performance issue |

#### 1.5 Bundle Operations (Global Aggregates)

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `bundles_created` | `atomic.Uint64` | Total bundles created server-wide | Schema evolution tracking |
| `bundles_deleted` | `atomic.Uint64` | Total bundles deleted | Data lifecycle management |
| `documents_inserted` | `atomic.Uint64` | Total document inserts across all bundles | Write volume baseline |
| `documents_updated` | `atomic.Uint64` | Total document updates | Modification frequency |
| `documents_deleted` | `atomic.Uint64` | Total document deletes | Deletion rate for capacity planning |
| `schema_changes` | `atomic.Uint64` | ALTER BUNDLE operations | Schema stability metric |
| `index_creations` | `atomic.Uint64` | CREATE INDEX operations | Index management activity |
| `index_deletions` | `atomic.Uint64` | DROP INDEX operations | Index lifecycle tracking |

#### 1.6 Compaction

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `compaction_runs_total` | `atomic.Uint64` | Total compaction operations | Compaction frequency |
| `compaction_trigger_file_count` | `atomic.Uint64` | Compactions triggered by file count | Strategy effectiveness metric |
| `compaction_trigger_size` | `atomic.Uint64` | Compactions triggered by total size | Size-based threshold tuning |
| `compaction_trigger_tombstone_ratio` | `atomic.Uint64` | Compactions triggered by tombstone % | Deletion-driven compactions |
| `compaction_trigger_time` | `atomic.Uint64` | Compactions triggered by schedule | Time-based maintenance runs |
| `compaction_bytes_read` | `atomic.Uint64` | Total bytes read during compaction | I/O read load |
| `compaction_bytes_written` | `atomic.Uint64` | Total bytes written during compaction | Write amplification numerator |
| `compaction_bytes_reclaimed` | `atomic.Uint64` | Space reclaimed from tombstones | Space efficiency gain |
| `compaction_queue_depth` | `atomic.Int64` | Pending compaction operations (gauge) | Backlog indicator; high values = overload |

#### 1.7 Connection & Session

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `sessions_created` | `atomic.Uint64` | Total sessions created | User activity volume |
| `sessions_terminated_normal` | `atomic.Uint64` | Sessions ended gracefully (logout) | Normal termination rate |
| `sessions_terminated_timeout` | `atomic.Uint64` | Sessions ended by timeout | Idle timeout effectiveness |
| `sessions_terminated_admin` | `atomic.Uint64` | Sessions killed by admin | Administrative interventions |
| `sessions_active` | `atomic.Int64` | Currently active sessions (gauge) | Concurrent user load |
| `connections_accepted` | `atomic.Uint64` | Total connections accepted | Connection volume |
| `connections_rejected` | `atomic.Uint64` | Connections rejected (pool full) | Resource saturation indicator |
| `connection_churn_1min` | `atomic.Uint64` | Connections created in last minute | Short-term churn rate |
| `session_hijack_attempts` | `atomic.Uint64` | IP/user-agent mismatches detected | Security threat monitoring |

#### 1.8 Error Tracking

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `errors_query_parsing` | `atomic.Uint64` | Query syntax errors | Client error rate; user education needs |
| `errors_query_execution` | `atomic.Uint64` | Query runtime errors | Application logic issues |
| `errors_index_operations` | `atomic.Uint64` | Index operation failures | Index corruption detection |
| `errors_transaction` | `atomic.Uint64` | Transaction failures | Data integrity monitoring |
| `errors_wal` | `atomic.Uint64` | WAL write/flush failures | Durability risk; critical alert |
| `errors_compaction` | `atomic.Uint64` | Compaction failures | Maintenance health |
| `errors_io` | `atomic.Uint64` | File system I/O errors | Disk health indicator |
| `errors_memory` | `atomic.Uint64` | Out-of-memory errors | Capacity planning signal |
| `errors_permission` | `atomic.Uint64` | Authorization failures | Security monitoring |

---

### 2. PER-DATABASE METRICS (`DatabaseMetrics`)

**Location**: Lines 251-450 (new section)

**Struct Name**: `DatabaseMetrics`

**Storage**: `map[string]*DatabaseMetrics` (keyed by database name) with lazy initialization

**Singleton Pattern**: `var databaseMetricsMap sync.Map` with `GetDatabaseMetrics(dbName string)` accessor

#### 2.1 Database-Level Operations

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `db_bundles_count` | `atomic.Uint64` | Total bundles in this database | Schema size tracking |
| `db_documents_total` | `atomic.Uint64` | Total documents across all bundles | Database size estimation |
| `db_size_bytes` | `atomic.Int64` | Total database size on disk | Capacity planning per-database |
| `db_queries_executed` | `atomic.Uint64` | Queries executed on this database | Per-database query load |
| `db_transactions_committed` | `atomic.Uint64` | Transactions committed | Database-specific transaction volume |
| `db_transactions_rolledback` | `atomic.Uint64` | Transactions rolled back | Per-database failure rate |
| `db_sessions_active` | `atomic.Int64` | Active sessions on this database | Database-specific user load |
| `db_compactions_run` | `atomic.Uint64` | Compaction runs for this database | Maintenance frequency |
| `db_errors_total` | `atomic.Uint64` | Total errors in this database | Database health indicator |

#### 2.2 Database Relationships

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `db_relationships_1tomany_traversals` | `atomic.Uint64` | 1-to-Many relationship queries | Relationship usage patterns |
| `db_relationships_1toone_traversals` | `atomic.Uint64` | 1-to-One relationship queries | Relationship type distribution |
| `db_relationships_manytomany_traversals` | `atomic.Uint64` | Many-to-Many relationship queries | Complex relationship usage |
| `db_relationships_n_plus_1_detected` | `atomic.Uint64` | N+1 query pattern detections | Performance anti-pattern alert |

---

### 3. PER-BUNDLE METRICS (`BundleMetrics`)

**Location**: Lines 451-650 (new section)

**Struct Name**: `BundleMetrics`

**Storage**: Nested `map[databaseName]map[bundleName]*BundleMetrics` with lazy initialization

**Singleton Pattern**: `var bundleMetricsMap sync.Map` with `GetBundleMetrics(dbName, bundleName string)` accessor

#### 3.1 Bundle Document Operations

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `bundle_documents_inserted` | `atomic.Uint64` | Documents inserted into this bundle | Write volume per bundle |
| `bundle_documents_updated` | `atomic.Uint64` | Documents updated in this bundle | Modification frequency |
| `bundle_documents_deleted` | `atomic.Uint64` | Documents deleted from this bundle | Deletion rate tracking |
| `bundle_documents_count` | `atomic.Uint64` | Current document count | Bundle size gauge |
| `bundle_size_bytes` | `atomic.Int64` | Bundle size on disk | Storage consumption |
| `bundle_queries_total` | `atomic.Uint64` | Queries against this bundle | Bundle popularity metric |
| `bundle_full_scans` | `atomic.Uint64` | Full bundle scans executed | Index effectiveness indicator |
| `bundle_index_scans` | `atomic.Uint64` | Index-based scans executed | Index utilization rate |

#### 3.2 Bundle Index Metrics

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `bundle_hashindex_puts` | `atomic.Uint64` | Hash index Put operations | Bundle-specific write load |
| `bundle_hashindex_gets` | `atomic.Uint64` | Hash index Get operations | Bundle-specific read load |
| `bundle_hashindex_cache_hit_rate` | `atomic.Uint64` | Cache hits (for rate calculation) | Bundle cache effectiveness |
| `bundle_btree_searches` | `atomic.Uint64` | B-tree search operations | Range query usage |
| `bundle_btree_range_queries` | `atomic.Uint64` | B-tree range scans | Complex query patterns |
| `bundle_btree_node_splits` | `atomic.Uint64` | Node splits in this bundle's indexes | Growth pattern tracking |

#### 3.3 Bundle Schema & Maintenance

| Metric Name | Data Type | Description | Usage |
|-------------|-----------|-------------|-------|
| `bundle_schema_changes` | `atomic.Uint64` | ALTER BUNDLE operations | Schema evolution frequency |
| `bundle_indexes_created` | `atomic.Uint64` | Indexes created on this bundle | Index management activity |
| `bundle_indexes_dropped` | `atomic.Uint64` | Indexes dropped from this bundle | Index lifecycle tracking |
| `bundle_compactions` | `atomic.Uint64` | Compaction runs on this bundle | Maintenance frequency |
| `bundle_errors` | `atomic.Uint64` | Errors specific to this bundle | Bundle health monitoring |

---

## GetMetrics() Method Specifications

**Location**: Lines 651-800 (new section)

### Global Metrics Accessor
```
Function: (gsm *GlobalServerMetrics) GetMetrics() map[string]interface{}
Returns: All global metrics using atomic.LoadUint64() / atomic.LoadInt64()
Calculations: 
  - hash_index_cache_hit_rate = cache_hits / (cache_hits + cache_misses) * 100
  - btree_cache_hit_rate = btree_cache_hits / (btree_cache_hits + btree_cache_misses) * 100
  - transaction_commit_rate = commits / (commits + rollbacks) * 100
  - wal_avg_batch_size = wal_batch_size_sum / wal_batch_count
  - write_amplification = compaction_bytes_written / compaction_bytes_reclaimed
```

### Database Metrics Accessor
```
Function: (dm *DatabaseMetrics) GetMetrics() map[string]interface{}
Returns: Per-database metrics with atomic loads
Calculations:
  - db_transaction_success_rate = commits / (commits + rollbacks) * 100
  - db_avg_documents_per_bundle = documents_total / bundles_count
```

### Bundle Metrics Accessor
```
Function: (bm *BundleMetrics) GetMetrics() map[string]interface{}
Returns: Per-bundle metrics with atomic loads
Calculations:
  - bundle_index_scan_rate = index_scans / (index_scans + full_scans) * 100
  - bundle_cache_effectiveness = hashindex_cache_hit_rate
```

---

## Singleton Accessor Functions

**Location**: Lines 801-850

```
GetGlobalServerMetrics() *GlobalServerMetrics
  - Returns singleton instance
  
GetDatabaseMetrics(databaseName string) *DatabaseMetrics
  - Lazy initialization using sync.Map
  - LoadOrStore pattern
  - Returns existing or newly created instance
  
GetBundleMetrics(databaseName, bundleName string) *BundleMetrics
  - Nested lazy initialization
  - First level: database map (sync.Map)
  - Second level: bundle map (sync.Map)
  - Returns existing or newly created instance
```

---

## Integration Points (Instrumentation Locations)

### Hash Index V3
- **File**: hash_index_api.go
- **Put** (lines 348-420): Record latency, update counters
- **Get** (lines 423-535): Record cache hits/misses, latency
- **Delete** (lines 613-685): Record tombstone creation

### B-Tree Index
- **File**: btree_index_api.go
- **Insert** (~line 600): Record inserts, node splits
- **Search** (~line 700): Record searches, cache hits
- **RangeQuery** (~line 800): Record range queries

### Query Router
- **File**: `/Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB/src/internal/domain/query_router.go`
- **RouteQuery** (lines 106-120): Record routing decisions

### Transaction Manager
- **File**: `/Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB/src/internal/storage/wal_manager.go`
- **BeginTransaction**, **CommitTransaction**, **RollbackTransaction**: Record transaction lifecycle

### WAL Writer
- **File**: `/Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB/src/internal/storage/write_ahead_log.go`
- **LogOperationBinary** (lines 258-340): Record writes, bytes
- **Flush** (lines 418-440): Record flush latency

### Bundle Service
- **File**: bundle_service.go
- All CRUD operations: Record document operations per bundle

### Session Manager
- **File**: session_manager.go
- **CreateSession**, **InvalidateSession**, etc.: Record session lifecycle

---

## Memory Overhead Estimation

- Global: ~150 metrics × 8 bytes = 1.2 KB
- Per Database: ~20 metrics × 8 bytes = 160 bytes × N databases
- Per Bundle: ~25 metrics × 8 bytes = 200 bytes × N bundles
- Total for 100 databases, 1000 bundles: ~216 KB base + map overhead