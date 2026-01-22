package server

import (
	"sync"
	"sync/atomic"
)

// ============================================================================
// PLAN E: Memory-Mapped File Format Version
// ============================================================================
// MMAP_FORMAT_VERSION defines the binary format version for memory-mapped metrics export
// Increment this version when the metrics structure changes to maintain backward compatibility
const MMAP_FORMAT_VERSION uint32 = 1

// GlobalMemoryMetrics tracks aggregate memory limit metrics across all queries
// This provides insights into DoS protection effectiveness and query memory patterns
//
// Thread-safe: All methods use mutex protection for concurrent access
type GlobalMemoryMetrics struct {
	mu sync.Mutex

	// Counters
	totalQueriesChecked int64 // Total number of queries that had memory tracking enabled
	totalLimitExceeded  int64 // Number of queries that exceeded the memory limit

	// Memory statistics
	totalProjectedMemory int64 // Sum of all projected memory values (for calculating average)
	maxProjectedMemory   int64 // Maximum projected memory across all queries
}

// globalMemoryMetrics is the singleton instance
var globalMemoryMetrics = &GlobalMemoryMetrics{}

// GetGlobalMemoryMetrics returns the singleton metrics instance
func GetGlobalMemoryMetrics() *GlobalMemoryMetrics {
	return globalMemoryMetrics
}

// RecordQuery records metrics for a completed query
func (gmm *GlobalMemoryMetrics) RecordQuery(projectedMemory int64, exceeded bool) {
	gmm.mu.Lock()
	defer gmm.mu.Unlock()

	gmm.totalQueriesChecked++
	gmm.totalProjectedMemory += projectedMemory

	if exceeded {
		gmm.totalLimitExceeded++
	}

	if projectedMemory > gmm.maxProjectedMemory {
		gmm.maxProjectedMemory = projectedMemory
	}
}

// GetMetrics returns current metrics as key-value pairs
// Compatible with any monitoring system (Prometheus, DataDog, CloudWatch, etc.)
func (gmm *GlobalMemoryMetrics) GetMetrics() map[string]interface{} {
	gmm.mu.Lock()
	defer gmm.mu.Unlock()

	avgProjected := int64(0)
	if gmm.totalQueriesChecked > 0 {
		avgProjected = gmm.totalProjectedMemory / gmm.totalQueriesChecked
	}

	return map[string]interface{}{
		"memory_limit_exceeded_count": gmm.totalLimitExceeded,
		"queries_checked_count":       gmm.totalQueriesChecked,
		"avg_projected_memory_bytes":  avgProjected,
		"max_projected_memory_bytes":  gmm.maxProjectedMemory,
	}
}

// Reset clears all metrics (useful for testing)
func (gmm *GlobalMemoryMetrics) Reset() {
	gmm.mu.Lock()
	defer gmm.mu.Unlock()

	gmm.totalQueriesChecked = 0
	gmm.totalLimitExceeded = 0
	gmm.totalProjectedMemory = 0
	gmm.maxProjectedMemory = 0
}

// ============================================================================
// COMPREHENSIVE METRICS SYSTEM
// ============================================================================
// This section implements a comprehensive metrics collection system using
// atomic operations for lock-free performance. Metrics are organized into
// three levels: Global (server-wide), Database (per-database), and Bundle
// (per-bundle). All metrics use atomic counters for thread-safe increments
// and are designed for export to external monitoring systems via memory-mapped
// files.
//
// Design principles:
// - Lock-free atomic operations for high-frequency counters
// - Lazy initialization via sync.Map for per-database/bundle metrics
// - Monotonic counters (never reset) for delta calculation by consumers
// - Raw value exports compatible with memory-mapped file format
// ============================================================================

// GlobalServerMetrics tracks server-wide statistics across all operations
// Uses atomic counters for lock-free performance in high-concurrency scenarios
type GlobalServerMetrics struct {
	// Hash Index Metrics
	HashIndexPutsTotal    atomic.Uint64 // Total PUT operations across all hash indexes
	HashIndexGetsTotal    atomic.Uint64 // Total GET operations across all hash indexes
	HashIndexDeletesTotal atomic.Uint64 // Total DELETE operations across all hash indexes
	HashIndexCacheHits    atomic.Uint64 // Number of successful cache hits in hash indexes
	HashIndexCacheMisses  atomic.Uint64 // Number of cache misses requiring disk reads
	HashIndexDiskReads    atomic.Uint64 // Number of disk read operations for hash indexes
	HashIndexCollisions   atomic.Uint64 // Number of hash collisions encountered
	HashIndexRehashes     atomic.Uint64 // Number of rehash operations performed
	HashIndexBucketSplits atomic.Uint64 // Number of bucket split operations
	HashIndexBucketMerges atomic.Uint64 // Number of bucket merge operations

	// Hash Index Latency Histograms (separate counters for each bucket)
	HashIndexPutLatencyLt1ms   atomic.Uint64 // PUT operations < 1ms
	HashIndexPutLatencyLt10ms  atomic.Uint64 // PUT operations < 10ms
	HashIndexPutLatencyLt100ms atomic.Uint64 // PUT operations < 100ms
	HashIndexPutLatencyLt1s    atomic.Uint64 // PUT operations < 1s
	HashIndexPutLatencyGte1s   atomic.Uint64 // PUT operations >= 1s

	HashIndexGetLatencyLt1ms   atomic.Uint64 // GET operations < 1ms
	HashIndexGetLatencyLt10ms  atomic.Uint64 // GET operations < 10ms
	HashIndexGetLatencyLt100ms atomic.Uint64 // GET operations < 100ms
	HashIndexGetLatencyLt1s    atomic.Uint64 // GET operations < 1s
	HashIndexGetLatencyGte1s   atomic.Uint64 // GET operations >= 1s

	// B-Tree Index Metrics
	BTreeIndexInsertsTotal  atomic.Uint64 // Total INSERT operations across all B-tree indexes
	BTreeIndexSearchesTotal atomic.Uint64 // Total SEARCH operations across all B-tree indexes
	BTreeIndexRangeQueries  atomic.Uint64 // Total RANGE QUERY operations across all B-tree indexes
	BTreeIndexDeletesTotal  atomic.Uint64 // Total DELETE operations across all B-tree indexes
	BTreeIndexCacheHits     atomic.Uint64 // Number of successful cache hits in B-tree indexes
	BTreeIndexCacheMisses   atomic.Uint64 // Number of cache misses requiring disk reads
	BTreeIndexNodeSplits    atomic.Uint64 // Number of B-tree node split operations
	BTreeIndexNodeMerges    atomic.Uint64 // Number of B-tree node merge operations
	BTreeIndexRebalances    atomic.Uint64 // Number of tree rebalance operations
	BTreeIndexTombstones    atomic.Uint64 // Number of tombstone entries in B-tree indexes

	// B-Tree Index Latency Histograms
	BTreeInsertLatencyLt1ms   atomic.Uint64 // INSERT operations < 1ms
	BTreeInsertLatencyLt10ms  atomic.Uint64 // INSERT operations < 10ms
	BTreeInsertLatencyLt100ms atomic.Uint64 // INSERT operations < 100ms
	BTreeInsertLatencyLt1s    atomic.Uint64 // INSERT operations < 1s
	BTreeInsertLatencyGte1s   atomic.Uint64 // INSERT operations >= 1s

	BTreeSearchLatencyLt1ms   atomic.Uint64 // SEARCH operations < 1ms
	BTreeSearchLatencyLt10ms  atomic.Uint64 // SEARCH operations < 10ms
	BTreeSearchLatencyLt100ms atomic.Uint64 // SEARCH operations < 100ms
	BTreeSearchLatencyLt1s    atomic.Uint64 // SEARCH operations < 1s
	BTreeSearchLatencyGte1s   atomic.Uint64 // SEARCH operations >= 1s

	// Query Execution Metrics
	QueryExecutionsTotal     atomic.Uint64 // Total number of queries executed
	QueryPlanCacheHits       atomic.Uint64 // Number of query plan cache hits
	QueryPlanCacheMisses     atomic.Uint64 // Number of query plan cache misses
	QueryTimeoutsTotal       atomic.Uint64 // Number of queries that timed out
	QueryMemoryLimitExceeded atomic.Uint64 // Number of queries exceeding memory limit
	QueryFullTableScans      atomic.Uint64 // Number of queries requiring full table scans
	QueryIndexScans          atomic.Uint64 // Number of queries using index scans
	QueryJoinsTotal          atomic.Uint64 // Total number of JOIN operations
	QueryGroupBysTotal       atomic.Uint64 // Total number of GROUP BY operations
	QueryOrderBysTotal       atomic.Uint64 // Total number of ORDER BY operations

	// Subquery Execution Metrics (Tier 1 Implementation)
	SubqueryExecutionsTotal       atomic.Uint64 // Total number of subqueries executed
	SubqueryInListStrategy        atomic.Uint64 // Subqueries using IN-list materialization
	SubqueryHashJoinStrategy      atomic.Uint64 // Subqueries using HashJoin strategy
	SubqueryIndexedLookupStrategy atomic.Uint64 // Subqueries using indexed lookup (Tier 3)
	SubqueryDepthExceeded         atomic.Uint64 // Subqueries rejected due to max depth exceeded
	SubqueryMemoryLimitExceeded   atomic.Uint64 // Subqueries forced to HashJoin due to memory limit
	SubqueryContainsNull          atomic.Uint64 // Subqueries with NULL values in results

	// Query Latency Histograms
	QueryLatencyLt1ms   atomic.Uint64 // Queries < 1ms
	QueryLatencyLt10ms  atomic.Uint64 // Queries < 10ms
	QueryLatencyLt100ms atomic.Uint64 // Queries < 100ms
	QueryLatencyLt1s    atomic.Uint64 // Queries < 1s
	QueryLatencyGte1s   atomic.Uint64 // Queries >= 1s

	// Transaction Metrics
	TransactionsBegun      atomic.Uint64 // Number of transactions started
	TransactionsCommitted  atomic.Uint64 // Number of transactions successfully committed
	TransactionsRolledBack atomic.Uint64 // Number of transactions rolled back
	TransactionsAborted    atomic.Uint64 // Number of transactions aborted due to errors

	// PHASE 1: MVCC - Lock Contention Metrics (Priority 1)
	RotationLockWaitTimeTotal   atomic.Uint64 // Total time spent waiting for rotation locks (nanoseconds)
	RotationLockAcquisitions    atomic.Uint64 // Total number of rotation lock acquisitions
	AtomicOperationsTotal       atomic.Uint64 // Total number of atomic operations (AddInt64, LoadInt64, StoreInt64)
	// TODO: Add version chain length distribution metrics
	// TODO: Add conflict rate per bundle metrics
	// TODO: Add snapshot age distribution metrics

	// WAL (Write-Ahead Log) Metrics
	WALWritesTotal      atomic.Uint64 // Total number of WAL write operations
	WALFlushesTotal     atomic.Uint64 // Total number of WAL flush operations
	WALBytesWritten     atomic.Uint64 // Total bytes written to WAL
	WALSegmentRotations atomic.Uint64 // Number of WAL segment rotations
	WALSyncsTotal       atomic.Uint64 // Number of fsync operations on WAL

	// Compaction Metrics
	CompactionsTotal       atomic.Uint64 // Total number of compaction operations
	CompactionBytesRead    atomic.Uint64 // Total bytes read during compaction
	CompactionBytesWritten atomic.Uint64 // Total bytes written during compaction
	CompactionTombstones   atomic.Uint64 // Number of tombstones removed during compaction

	// Session Metrics
	SessionsActive      atomic.Uint64 // Current number of active sessions
	SessionsCreated     atomic.Uint64 // Total sessions created since server start
	SessionsTerminated  atomic.Uint64 // Total sessions terminated
	SessionAuthFailures atomic.Uint64 // Number of authentication failures

	// Document Operation Metrics (Global Aggregates)
	DocumentInsertsTotal atomic.Uint64 // Total document insertions across all bundles
	DocumentUpdatesTotal atomic.Uint64 // Total document updates across all bundles
	DocumentDeletesTotal atomic.Uint64 // Total document deletions across all bundles
	DocumentReadsTotal   atomic.Uint64 // Total document reads across all bundles

	// ============================================================================
	// PLAN A: Index Error Counters
	// ============================================================================
	HashIndexPutErrors     atomic.Uint64 // Number of PUT errors in hash indexes
	HashIndexGetErrors     atomic.Uint64 // Number of GET errors in hash indexes
	HashIndexDeleteErrors  atomic.Uint64 // Number of DELETE errors in hash indexes
	BTreeIndexInsertErrors atomic.Uint64 // Number of INSERT errors in B-tree indexes
	BTreeIndexSearchErrors atomic.Uint64 // Number of SEARCH errors in B-tree indexes
	BTreeIndexDeleteErrors atomic.Uint64 // Number of DELETE errors in B-tree indexes
	// TODO: Add per-operation latency tracking for error cases to identify slow failures

	// ============================================================================
	// PLAN B: Query Plan Cache Enhanced Metrics
	// ============================================================================
	QueryPlanCacheEvictions     atomic.Uint64 // Number of cache evictions due to capacity limits
	QueryPlanCacheInvalidations atomic.Uint64 // Number of cache invalidations due to writes/schema changes
	QueryPlanCacheStaleServes   atomic.Uint64 // Number of stale plans served during rebuild window
	QueryPlanCacheMemoryBytes   atomic.Uint64 // Estimated memory used by plan cache (bytes)
	QueryPlanCacheSize          atomic.Uint64 // DEPRECATED: Use QueryPlanCacheHits + QueryPlanCacheMisses
	QueryPlanCacheMaxSize       atomic.Uint64 // DEPRECATED: Use settings.PlanCacheCapacity
	QueryPlanCacheMemoryUsed    atomic.Uint64 // DEPRECATED: Use QueryPlanCacheMemoryBytes

	// ============================================================================
	// PLAN C: Compaction Duration Histograms & Trigger Tracking
	// ============================================================================
	CompactionDurationLt100ms atomic.Uint64 // Compactions < 100ms
	CompactionDurationLt1s    atomic.Uint64 // Compactions < 1s
	CompactionDurationLt10s   atomic.Uint64 // Compactions < 10s
	CompactionDurationGte10s  atomic.Uint64 // Compactions >= 10s

	CompactionTriggeredScheduled atomic.Uint64 // Compactions triggered by schedule
	CompactionTriggeredManual    atomic.Uint64 // Compactions triggered manually
	CompactionTriggeredThreshold atomic.Uint64 // Compactions triggered by threshold
	CompactionTriggeredEmergency atomic.Uint64 // Compactions triggered by emergency condition

	// ============================================================================
	// PLAN D: WAL Replay & Replication Lag Metrics
	// ============================================================================
	WALReplayEntriesTotal  atomic.Uint64 // Total WAL entries replayed during recovery
	WALReplayErrorsTotal   atomic.Uint64 // Total errors encountered during WAL replay
	WALReplayDurationMs    atomic.Uint64 // Total time spent replaying WAL (milliseconds)
	WALReplayLastTimestamp atomic.Uint64 // Timestamp of last successful WAL replay (Unix epoch)

	WALLastFlushTimestamp atomic.Uint64 // Timestamp of last WAL flush (Unix epoch seconds)
	WALLastSyncTimestamp  atomic.Uint64 // Timestamp of last WAL sync (Unix epoch seconds)
	WALReplicationLagMs   atomic.Uint64 // Replication lag in milliseconds (current time - last sync)

	// ============================================================================
	// Ghost Cleanup Metrics (SQL Server-style background compaction)
	// ============================================================================
	GhostCleanupCyclesTotal      atomic.Uint64 // Total number of ghost cleanup cycles executed
	GhostRecordsScanned          atomic.Uint64 // Total ghost records scanned
	GhostRecordsRemoved          atomic.Uint64 // Total ghost records (tombstones) removed
	GhostCleanupDurationMs       atomic.Uint64 // Duration of last cleanup cycle (milliseconds)
	GhostCleanupPausedForLoad    atomic.Uint64 // Number of times cleanup paused due to high query load
	GhostCleanupBatchesProcessed atomic.Uint64 // Number of batches processed

	// Tombstone Cache Metrics
	TombstoneCacheHits      atomic.Uint64 // Cache hits when checking tombstone ratios
	TombstoneCacheMisses    atomic.Uint64 // Cache misses requiring file scans
	TombstoneScansPerformed atomic.Uint64 // Number of file scans performed
	TombstoneCacheEvictions atomic.Uint64 // Number of cache entries evicted (age or size based)

	// Compaction Trigger Breakdown
	CompactionTriggeredGhost atomic.Uint64 // Compactions triggered by ghost cleanup worker
	CompactionBlockedByLock  atomic.Uint64 // Compactions blocked due to lock contention

	// Orphaned File Cleanup
	OrphanedTempFilesRemoved atomic.Uint64 // Number of orphaned .compact.tmp files cleaned up

	// ============================================================================
	// DateTime Parsing Metrics
	// ============================================================================
	DateTimeParseAttemptsTotal atomic.Uint64 // Total datetime parse attempts
	DateTimeParseSuccessTotal  atomic.Uint64 // Successful datetime parses
	DateTimeParseErrorsTotal   atomic.Uint64 // Failed datetime parse attempts

	// TODO: MVCC Future - Add version store metrics when transaction support (BEGIN/COMMIT/ROLLBACK) is implemented:
	// VersionStoreSize atomic.Uint64              // Total bytes consumed by old row versions in version store
	// VersionStorePurgedBytes atomic.Uint64       // Total bytes reclaimed by version cleanup
	// OldestActiveTransactionID atomic.Uint64     // Lowest transaction ID still active (blocks version cleanup)
	// VersionsCleanedPerCycle atomic.Uint64       // Row versions removed per cleanup cycle
	// VersionChainLengthAvg atomic.Uint64         // Average number of versions per document
	// VersionChainLengthMax atomic.Uint64         // Longest version chain observed (alert if >100)
	// These track version store health similar to SQL Server's sys.dm_tran_version_store metrics.
	// Version cleanup runs every 60 seconds (separate from ghost cleanup's 10 seconds) to balance
	// cleanup overhead vs space reclamation for long-running analytical queries.
}

// globalServerMetrics is the singleton instance
var globalServerMetrics = &GlobalServerMetrics{}

// GetGlobalServerMetrics returns the singleton global metrics instance
func GetGlobalServerMetrics() *GlobalServerMetrics {
	return globalServerMetrics
}

// GetMetrics returns current global metrics as a map (raw atomic values)
// Compatible with memory-mapped file export for external monitoring
func (gsm *GlobalServerMetrics) GetMetrics() map[string]uint64 {
	return map[string]uint64{
		// Hash Index Metrics
		"hash_index_puts_total":    gsm.HashIndexPutsTotal.Load(),
		"hash_index_gets_total":    gsm.HashIndexGetsTotal.Load(),
		"hash_index_deletes_total": gsm.HashIndexDeletesTotal.Load(),
		"hash_index_cache_hits":    gsm.HashIndexCacheHits.Load(),
		"hash_index_cache_misses":  gsm.HashIndexCacheMisses.Load(),
		"hash_index_disk_reads":    gsm.HashIndexDiskReads.Load(),
		"hash_index_collisions":    gsm.HashIndexCollisions.Load(),
		"hash_index_rehashes":      gsm.HashIndexRehashes.Load(),
		"hash_index_bucket_splits": gsm.HashIndexBucketSplits.Load(),
		"hash_index_bucket_merges": gsm.HashIndexBucketMerges.Load(),

		// Hash Index Latency Histograms
		"hash_index_put_latency_lt_1ms":   gsm.HashIndexPutLatencyLt1ms.Load(),
		"hash_index_put_latency_lt_10ms":  gsm.HashIndexPutLatencyLt10ms.Load(),
		"hash_index_put_latency_lt_100ms": gsm.HashIndexPutLatencyLt100ms.Load(),
		"hash_index_put_latency_lt_1s":    gsm.HashIndexPutLatencyLt1s.Load(),
		"hash_index_put_latency_gte_1s":   gsm.HashIndexPutLatencyGte1s.Load(),
		"hash_index_get_latency_lt_1ms":   gsm.HashIndexGetLatencyLt1ms.Load(),
		"hash_index_get_latency_lt_10ms":  gsm.HashIndexGetLatencyLt10ms.Load(),
		"hash_index_get_latency_lt_100ms": gsm.HashIndexGetLatencyLt100ms.Load(),
		"hash_index_get_latency_lt_1s":    gsm.HashIndexGetLatencyLt1s.Load(),
		"hash_index_get_latency_gte_1s":   gsm.HashIndexGetLatencyGte1s.Load(),

		// B-Tree Index Metrics
		"btree_index_inserts_total":  gsm.BTreeIndexInsertsTotal.Load(),
		"btree_index_searches_total": gsm.BTreeIndexSearchesTotal.Load(),
		"btree_index_range_queries":  gsm.BTreeIndexRangeQueries.Load(),
		"btree_index_deletes_total":  gsm.BTreeIndexDeletesTotal.Load(),
		"btree_index_cache_hits":     gsm.BTreeIndexCacheHits.Load(),
		"btree_index_cache_misses":   gsm.BTreeIndexCacheMisses.Load(),
		"btree_index_node_splits":    gsm.BTreeIndexNodeSplits.Load(),
		"btree_index_node_merges":    gsm.BTreeIndexNodeMerges.Load(),
		"btree_index_rebalances":     gsm.BTreeIndexRebalances.Load(),
		"btree_index_tombstones":     gsm.BTreeIndexTombstones.Load(),

		// B-Tree Index Latency Histograms
		"btree_insert_latency_lt_1ms":   gsm.BTreeInsertLatencyLt1ms.Load(),
		"btree_insert_latency_lt_10ms":  gsm.BTreeInsertLatencyLt10ms.Load(),
		"btree_insert_latency_lt_100ms": gsm.BTreeInsertLatencyLt100ms.Load(),
		"btree_insert_latency_lt_1s":    gsm.BTreeInsertLatencyLt1s.Load(),
		"btree_insert_latency_gte_1s":   gsm.BTreeInsertLatencyGte1s.Load(),
		"btree_search_latency_lt_1ms":   gsm.BTreeSearchLatencyLt1ms.Load(),
		"btree_search_latency_lt_10ms":  gsm.BTreeSearchLatencyLt10ms.Load(),
		"btree_search_latency_lt_100ms": gsm.BTreeSearchLatencyLt100ms.Load(),
		"btree_search_latency_lt_1s":    gsm.BTreeSearchLatencyLt1s.Load(),
		"btree_search_latency_gte_1s":   gsm.BTreeSearchLatencyGte1s.Load(),

		// Query Execution Metrics
		"query_executions_total":      gsm.QueryExecutionsTotal.Load(),
		"query_plan_cache_hits":       gsm.QueryPlanCacheHits.Load(),
		"query_plan_cache_misses":     gsm.QueryPlanCacheMisses.Load(),
		"query_timeouts_total":        gsm.QueryTimeoutsTotal.Load(),
		"query_memory_limit_exceeded": gsm.QueryMemoryLimitExceeded.Load(),
		"query_full_table_scans":      gsm.QueryFullTableScans.Load(),
		"query_index_scans":           gsm.QueryIndexScans.Load(),
		"query_joins_total":           gsm.QueryJoinsTotal.Load(),
		"query_group_bys_total":       gsm.QueryGroupBysTotal.Load(),
		"query_order_bys_total":       gsm.QueryOrderBysTotal.Load(),

		// Subquery Execution Metrics
		"subquery_executions_total":        gsm.SubqueryExecutionsTotal.Load(),
		"subquery_inlist_strategy":         gsm.SubqueryInListStrategy.Load(),
		"subquery_hashjoin_strategy":       gsm.SubqueryHashJoinStrategy.Load(),
		"subquery_indexed_lookup_strategy": gsm.SubqueryIndexedLookupStrategy.Load(),
		"subquery_depth_exceeded":          gsm.SubqueryDepthExceeded.Load(),
		"subquery_memory_limit_exceeded":   gsm.SubqueryMemoryLimitExceeded.Load(),
		"subquery_contains_null":           gsm.SubqueryContainsNull.Load(),

		// Query Latency Histograms
		"query_latency_lt_1ms":   gsm.QueryLatencyLt1ms.Load(),
		"query_latency_lt_10ms":  gsm.QueryLatencyLt10ms.Load(),
		"query_latency_lt_100ms": gsm.QueryLatencyLt100ms.Load(),
		"query_latency_lt_1s":    gsm.QueryLatencyLt1s.Load(),
		"query_latency_gte_1s":   gsm.QueryLatencyGte1s.Load(),

		// Transaction Metrics
		"transactions_begun":       gsm.TransactionsBegun.Load(),
		"transactions_committed":   gsm.TransactionsCommitted.Load(),
		"transactions_rolled_back": gsm.TransactionsRolledBack.Load(),
		"transactions_aborted":     gsm.TransactionsAborted.Load(),

		// PHASE 1: MVCC - Lock Contention Metrics
		"rotation_lock_wait_time_total_ns": gsm.RotationLockWaitTimeTotal.Load(),
		"rotation_lock_acquisitions":        gsm.RotationLockAcquisitions.Load(),
		"atomic_operations_total":            gsm.AtomicOperationsTotal.Load(),

		// WAL Metrics
		"wal_writes_total":      gsm.WALWritesTotal.Load(),
		"wal_flushes_total":     gsm.WALFlushesTotal.Load(),
		"wal_bytes_written":     gsm.WALBytesWritten.Load(),
		"wal_segment_rotations": gsm.WALSegmentRotations.Load(),
		"wal_syncs_total":       gsm.WALSyncsTotal.Load(),

		// Compaction Metrics
		"compactions_total":        gsm.CompactionsTotal.Load(),
		"compaction_bytes_read":    gsm.CompactionBytesRead.Load(),
		"compaction_bytes_written": gsm.CompactionBytesWritten.Load(),
		"compaction_tombstones":    gsm.CompactionTombstones.Load(),

		// Session Metrics
		"sessions_active":       gsm.SessionsActive.Load(),
		"sessions_created":      gsm.SessionsCreated.Load(),
		"sessions_terminated":   gsm.SessionsTerminated.Load(),
		"session_auth_failures": gsm.SessionAuthFailures.Load(),

		// Document Operation Metrics
		"document_inserts_total": gsm.DocumentInsertsTotal.Load(),
		"document_updates_total": gsm.DocumentUpdatesTotal.Load(),
		"document_deletes_total": gsm.DocumentDeletesTotal.Load(),
		"document_reads_total":   gsm.DocumentReadsTotal.Load(),

		// Plan A: Index Error Counters
		"hash_index_put_errors":     gsm.HashIndexPutErrors.Load(),
		"hash_index_get_errors":     gsm.HashIndexGetErrors.Load(),
		"hash_index_delete_errors":  gsm.HashIndexDeleteErrors.Load(),
		"btree_index_insert_errors": gsm.BTreeIndexInsertErrors.Load(),
		"btree_index_search_errors": gsm.BTreeIndexSearchErrors.Load(),
		"btree_index_delete_errors": gsm.BTreeIndexDeleteErrors.Load(),

		// Plan B: Query Plan Cache Enhanced Metrics
		"query_plan_cache_evictions":     gsm.QueryPlanCacheEvictions.Load(),
		"query_plan_cache_invalidations": gsm.QueryPlanCacheInvalidations.Load(),
		"query_plan_cache_stale_serves":  gsm.QueryPlanCacheStaleServes.Load(),
		"query_plan_cache_memory_bytes":  gsm.QueryPlanCacheMemoryBytes.Load(),

		// Plan C: Compaction Duration Histograms
		"compaction_duration_lt_100ms": gsm.CompactionDurationLt100ms.Load(),
		"compaction_duration_lt_1s":    gsm.CompactionDurationLt1s.Load(),
		"compaction_duration_lt_10s":   gsm.CompactionDurationLt10s.Load(),
		"compaction_duration_gte_10s":  gsm.CompactionDurationGte10s.Load(),

		// Plan C: Compaction Trigger Tracking
		"compaction_triggered_scheduled": gsm.CompactionTriggeredScheduled.Load(),
		"compaction_triggered_manual":    gsm.CompactionTriggeredManual.Load(),
		"compaction_triggered_threshold": gsm.CompactionTriggeredThreshold.Load(),
		"compaction_triggered_emergency": gsm.CompactionTriggeredEmergency.Load(),

		// Plan D: WAL Replay Metrics
		"wal_replay_entries_total":  gsm.WALReplayEntriesTotal.Load(),
		"wal_replay_errors_total":   gsm.WALReplayErrorsTotal.Load(),
		"wal_replay_duration_ms":    gsm.WALReplayDurationMs.Load(),
		"wal_replay_last_timestamp": gsm.WALReplayLastTimestamp.Load(),

		// Plan D: WAL Replication Lag Metrics
		"wal_last_flush_timestamp": gsm.WALLastFlushTimestamp.Load(),
		"wal_last_sync_timestamp":  gsm.WALLastSyncTimestamp.Load(),
		"wal_replication_lag_ms":   gsm.WALReplicationLagMs.Load(),

		// Ghost Cleanup Metrics (SQL Server-style background compaction)
		"ghost_cleanup_cycles_total":      gsm.GhostCleanupCyclesTotal.Load(),
		"ghost_records_scanned":           gsm.GhostRecordsScanned.Load(),
		"ghost_records_removed":           gsm.GhostRecordsRemoved.Load(),
		"ghost_cleanup_duration_ms":       gsm.GhostCleanupDurationMs.Load(),
		"ghost_cleanup_paused_for_load":   gsm.GhostCleanupPausedForLoad.Load(),
		"ghost_cleanup_batches_processed": gsm.GhostCleanupBatchesProcessed.Load(),
		"tombstone_cache_hits":            gsm.TombstoneCacheHits.Load(),
		"tombstone_cache_misses":          gsm.TombstoneCacheMisses.Load(),
		"tombstone_scans_performed":       gsm.TombstoneScansPerformed.Load(),
		"tombstone_cache_evictions":       gsm.TombstoneCacheEvictions.Load(),
		"compaction_triggered_ghost":      gsm.CompactionTriggeredGhost.Load(),
		"compaction_blocked_by_lock":      gsm.CompactionBlockedByLock.Load(),
		"orphaned_temp_files_removed":     gsm.OrphanedTempFilesRemoved.Load(),

		// DateTime Parsing Metrics
		"datetime_parse_attempts_total": gsm.DateTimeParseAttemptsTotal.Load(),
		"datetime_parse_success_total":  gsm.DateTimeParseSuccessTotal.Load(),
		"datetime_parse_errors_total":   gsm.DateTimeParseErrorsTotal.Load(),
	}
}

// DatabaseMetrics tracks per-database statistics
// Lazily initialized via sync.Map for memory efficiency
type DatabaseMetrics struct {
	// Database-level counters
	DBBundlesCount           atomic.Uint64 // Current number of bundles in this database
	DBDocumentsCount         atomic.Uint64 // Current number of documents in this database
	DBTotalSizeBytes         atomic.Uint64 // Total size of database in bytes
	DBQueriesExecuted        atomic.Uint64 // Total queries executed on this database
	DBTransactionsCommitted  atomic.Uint64 // Total transactions committed for this database
	DBTransactionsRolledBack atomic.Uint64 // Total transactions rolled back for this database
	DBWALBytesWritten        atomic.Uint64 // Total WAL bytes written for this database
	DBCompactionsPerformed   atomic.Uint64 // Total compactions performed on this database
	DBIndexesCount           atomic.Uint64 // Current number of indexes in this database
	DBSessionsActive         atomic.Uint64 // Current number of active sessions for this database
	DBDocumentInsertsTotal   atomic.Uint64 // Total document insertions in this database
	DBDocumentUpdatesTotal   atomic.Uint64 // Total document updates in this database
	DBDocumentDeletesTotal   atomic.Uint64 // Total document deletions in this database
}

// databaseMetricsMap stores per-database metrics (lazy initialization)
var databaseMetricsMap sync.Map // map[string]*DatabaseMetrics

// GetDatabaseMetrics returns metrics for a specific database (creates if not exists)
func GetDatabaseMetrics(dbName string) *DatabaseMetrics {
	if val, ok := databaseMetricsMap.Load(dbName); ok {
		return val.(*DatabaseMetrics)
	}

	// Create new metrics instance
	metrics := &DatabaseMetrics{}
	actual, _ := databaseMetricsMap.LoadOrStore(dbName, metrics)
	return actual.(*DatabaseMetrics)
}

// GetMetrics returns current database metrics as a map (raw atomic values)
func (dm *DatabaseMetrics) GetMetrics() map[string]uint64 {
	return map[string]uint64{
		"db_bundles_count":            dm.DBBundlesCount.Load(),
		"db_documents_count":          dm.DBDocumentsCount.Load(),
		"db_total_size_bytes":         dm.DBTotalSizeBytes.Load(),
		"db_queries_executed":         dm.DBQueriesExecuted.Load(),
		"db_transactions_committed":   dm.DBTransactionsCommitted.Load(),
		"db_transactions_rolled_back": dm.DBTransactionsRolledBack.Load(),
		"db_wal_bytes_written":        dm.DBWALBytesWritten.Load(),
		"db_compactions_performed":    dm.DBCompactionsPerformed.Load(),
		"db_indexes_count":            dm.DBIndexesCount.Load(),
		"db_sessions_active":          dm.DBSessionsActive.Load(),
		"db_document_inserts_total":   dm.DBDocumentInsertsTotal.Load(),
		"db_document_updates_total":   dm.DBDocumentUpdatesTotal.Load(),
		"db_document_deletes_total":   dm.DBDocumentDeletesTotal.Load(),
	}
}

// BundleMetrics tracks per-bundle statistics
// Lazily initialized via sync.Map for memory efficiency
type BundleMetrics struct {
	// Bundle-level counters
	BundleDocumentsInserted    atomic.Uint64 // Total documents inserted into this bundle
	BundleDocumentsUpdated     atomic.Uint64 // Total documents updated in this bundle
	BundleDocumentsDeleted     atomic.Uint64 // Total documents deleted from this bundle
	BundleDocumentsRead        atomic.Uint64 // Total documents read from this bundle
	BundleCurrentDocCount      atomic.Uint64 // Current number of documents in this bundle
	BundleTotalSizeBytes       atomic.Uint64 // Total size of bundle in bytes
	BundleIndexesCount         atomic.Uint64 // Number of indexes on this bundle
	BundleQueriesExecuted      atomic.Uint64 // Total queries executed on this bundle
	BundleFullScans            atomic.Uint64 // Number of full table scans on this bundle
	BundleIndexScans           atomic.Uint64 // Number of index scans on this bundle
	BundleCompactionsPerformed atomic.Uint64 // Total compactions performed on this bundle
	BundleRelationshipsCount   atomic.Uint64 // Number of relationships defined on this bundle
	BundleJoinOperations       atomic.Uint64 // Number of JOIN operations involving this bundle
	BundleCacheHits            atomic.Uint64 // Number of cache hits for this bundle
	BundleCacheMisses          atomic.Uint64 // Number of cache misses for this bundle
	BundleAvgDocSizeBytes      atomic.Uint64 // Average document size in bytes (updated periodically)
	BundleMaxDocSizeBytes      atomic.Uint64 // Maximum document size in bytes
	BundleMinDocSizeBytes      atomic.Uint64 // Minimum document size in bytes (0 = not set)
	BundleTombstoneCount       atomic.Uint64 // Number of tombstone entries in this bundle
}

// bundleMetricsMap stores per-bundle metrics (lazy initialization)
// Key format: "dbName:bundleName"
var bundleMetricsMap sync.Map // map[string]*BundleMetrics

// GetBundleMetrics returns metrics for a specific bundle (creates if not exists)
func GetBundleMetrics(dbName, bundleName string) *BundleMetrics {
	key := dbName + ":" + bundleName
	if val, ok := bundleMetricsMap.Load(key); ok {
		return val.(*BundleMetrics)
	}

	// Create new metrics instance
	metrics := &BundleMetrics{}
	actual, _ := bundleMetricsMap.LoadOrStore(key, metrics)
	return actual.(*BundleMetrics)
}

// GetMetrics returns current bundle metrics as a map (raw atomic values)
func (bm *BundleMetrics) GetMetrics() map[string]uint64 {
	return map[string]uint64{
		"bundle_documents_inserted":    bm.BundleDocumentsInserted.Load(),
		"bundle_documents_updated":     bm.BundleDocumentsUpdated.Load(),
		"bundle_documents_deleted":     bm.BundleDocumentsDeleted.Load(),
		"bundle_documents_read":        bm.BundleDocumentsRead.Load(),
		"bundle_current_doc_count":     bm.BundleCurrentDocCount.Load(),
		"bundle_total_size_bytes":      bm.BundleTotalSizeBytes.Load(),
		"bundle_indexes_count":         bm.BundleIndexesCount.Load(),
		"bundle_queries_executed":      bm.BundleQueriesExecuted.Load(),
		"bundle_full_scans":            bm.BundleFullScans.Load(),
		"bundle_index_scans":           bm.BundleIndexScans.Load(),
		"bundle_compactions_performed": bm.BundleCompactionsPerformed.Load(),
		"bundle_relationships_count":   bm.BundleRelationshipsCount.Load(),
		"bundle_join_operations":       bm.BundleJoinOperations.Load(),
		"bundle_cache_hits":            bm.BundleCacheHits.Load(),
		"bundle_cache_misses":          bm.BundleCacheMisses.Load(),
		"bundle_avg_doc_size_bytes":    bm.BundleAvgDocSizeBytes.Load(),
		"bundle_max_doc_size_bytes":    bm.BundleMaxDocSizeBytes.Load(),
		"bundle_min_doc_size_bytes":    bm.BundleMinDocSizeBytes.Load(),
		"bundle_tombstone_count":       bm.BundleTombstoneCount.Load(),
	}
}
