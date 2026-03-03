package settings

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"syndrdb/src/pkg/constants"
)

// TODO: I will add support for environment variable overrides with ENV > CLI > YAML > defaults precedence
// TODO: I will implement hot-reload functionality via SIGHUP signal to reload configuration without server restart

// getDefaultJoinMemoryLimitMB returns the default JOIN memory limit based on system RAM
// Systems with >8GB RAM get 128MB default, otherwise 64MB
// This mimics PostgreSQL's approach of scaling work_mem with available memory
func getDefaultJoinMemoryLimitMB() int {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	// Sys is the total memory obtained from the OS, which approximates available RAM
	totalMemoryGB := memStats.Sys / (1024 * 1024 * 1024)
	if totalMemoryGB >= 8 {
		return 128 // 128MB for systems with 8GB+ RAM
	}
	return 64 // 64MB default for smaller systems
}

// StorageConfig holds storage engine configuration
type StorageConfig struct {
	BundleFileMaxSizeMB       int `yaml:"bundle_file_max_size_mb"`        // Maximum bundle file size in MB before rotation (default: 32)
	MemTableWALBufferMaxSize int `yaml:"memtable_wal_buffer_max_size"`   // Max MemTable WAL buffer entries before swap/trim (default: 50000, 0 = use default)
}

// DefaultUser represents a default user to be created from configuration
type DefaultUser struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Arguments struct {
	// Storage Configuration
	DataDir    string `yaml:"data_dir"`
	LogDir     string `yaml:"log_dir"`
	LogFile    string `yaml:"log_file"`
	TempDir    string `yaml:"temp_dir"` // Temporary directory for intermediate files/indexes/sorts
	ConfigFile string `yaml:"config_file"`

	// Server Configuration
	CreateDefaultDB bool `yaml:"create_default_db"` // Create default database if it doesn't exist
	PrintToScreen   bool `yaml:"print_to_screen"`   // Print to screen

	// Logging Configuration
	Debug     bool   `yaml:"debug"`      // Debug mode
	UserDebug bool   `yaml:"user_debug"` // User debug mode
	LogLevel  string `yaml:"log_level"`  // Log level: debug, info, warn, error
	Verbose   bool   `yaml:"verbose"`    // Strongly verbose logging

	// Server Mode Configuration
	Mode string `yaml:"mode"` // The mode of operation: standalone, cluster
	Host string `yaml:"host"` // the host name or IP address to listen on
	Port int    `yaml:"port"` // the port number to listen on

	ExportRealtimeMetrics bool `yaml:"export_realtime_metrics"` // Export real-time metrics for monitoring

	// Journal Configuration
	MaxJournalFileSize int64 `yaml:"max_journal_file_size"`

	// Bundle Configuration
	BundleBufferSize    int    `yaml:"bundle_buffer_size"`    // Size of the buffer for bundle reads
	BundleStorageFormat string `yaml:"bundle_storage_format"` // Storage format: "json" or "binary" (default: "binary")

	// Storage Engine Configuration
	Storage StorageConfig `yaml:"storage"` // Storage engine configuration

	// Authentication Configuration
	AuthEnabled  bool          `yaml:"auth_enabled"`  // Enable authentication
	DefaultUsers []DefaultUser `yaml:"default_users"` // Default users to create when authentication is enabled (optional)

	// Session Management Configuration
	SessionTimeoutMinutes int `yaml:"session_timeout_minutes"` // Session timeout in minutes
	MaxSessions           int `yaml:"max_sessions"`            // Maximum number of concurrent sessions

	// Connection Pool Management
	MaxConnections               int `yaml:"max_connections"`         // Maximum connection pool size (default: 100)
	ConnectionIdleTimeoutMinutes int `yaml:"connection_idle_timeout"` // Connection idle timeout in minutes (default: 30)

	// Transaction Configuration
	TransactionIdleTimeout        string `yaml:"transaction_idle_timeout"`        // Transaction idle timeout (default: "5m")
	DefaultTransactionIsolation   string `yaml:"default_transaction_isolation"`   // Default isolation level: "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE" (default: "REPEATABLE READ")

	// TLS/SSL Configuration
	TLSEnabled            bool   `yaml:"tls_enabled"`              // Enable TLS/SSL
	TLSCertFile           string `yaml:"tls_cert_file"`            // Path to TLS certificate file
	TLSKeyFile            string `yaml:"tls_key_file"`             // Path to TLS private key file
	TLSGenerateSelfSigned bool   `yaml:"tls_generate_self_signed"` // Generate self-signed certificate if none exists
	TLSRequireClientCert  bool   `yaml:"tls_require_client_cert"`  // Require client certificates
	TLSCAFile             string `yaml:"tls_ca_file"`              // Path to CA file for client certificate validation

	// Encryption at Rest (Enterprise)
	EncryptionEnabled       bool   `yaml:"encryption_enabled"`        // Enable transparent data encryption
	EncryptionAlgorithm     string `yaml:"encryption_algorithm"`      // Encryption algorithm (default: "aes-256-gcm")
	EncryptionKeyFile       string `yaml:"encryption_key_file"`       // Path to master key file
	EncryptionWALEnabled    bool   `yaml:"encryption_wal_enabled"`    // Enable WAL encryption
	EncryptionBackupEnabled bool   `yaml:"encryption_backup_enabled"` // Enable backup encryption

	// Full-Text Search (Enterprise)
	FTSEnabled         bool   `yaml:"fts_enabled"`          // Enable full-text search indexes
	FTSDefaultAnalyzer string `yaml:"fts_default_analyzer"` // Default analyzer: "standard" (default: "standard")
	FTSAsyncUpdate     bool   `yaml:"fts_async_update"`     // Async index updates on DML (default: false)

	// Temporal Bundles (Enterprise)
	TemporalEnabled       bool `yaml:"temporal_enabled"`        // Enable system-versioned temporal bundles
	TemporalRetentionDays int  `yaml:"temporal_retention_days"` // History retention in days (default: 365)

	// High Availability (Enterprise)
	HAEnabled           bool   `yaml:"ha_enabled"`                // Enable high availability replication
	HARole              string `yaml:"ha_role"`                   // "leader", "follower", "standalone"
	HANodeID            string `yaml:"ha_node_id"`                // unique node identifier
	HALeaderHost        string `yaml:"ha_leader_host"`            // leader address (for followers)
	HALeaderPort        int    `yaml:"ha_leader_port"`            // leader replication port
	HAReplicationPort   int    `yaml:"ha_replication_port"`       // port for replication traffic
	HAReplicationMode   string `yaml:"ha_replication_mode"`       // "async" or "semisync"
	HAHeartbeatInterval int    `yaml:"ha_heartbeat_interval_ms"`  // heartbeat interval in ms
	HAFailoverTimeout   int    `yaml:"ha_failover_timeout_ms"`    // time before declaring leader dead
	HASemiSyncTimeout   int    `yaml:"ha_semisync_timeout_ms"`    // max wait for follower ACK
	HAReadRouting       bool   `yaml:"ha_read_routing"`           // enable automatic read routing

	// Performance + Operations (Enterprise Milestone 5)
	ParallelEnabled       bool    `yaml:"parallel_enabled"`
	ParallelMinDocs       int     `yaml:"parallel_min_docs"`           // minimum docs to trigger (default: 10000)
	ParallelWorkers       int     `yaml:"parallel_workers"`            // 0 = NumCPU
	QueryGovernorEnabled  bool    `yaml:"query_governor_enabled"`
	QueryTimeoutMs        int     `yaml:"query_timeout_ms"`            // max query duration (default: 30000)
	MaxConcurrentQueries  int     `yaml:"max_concurrent_queries"`      // 0 = unlimited
	MaxMemoryPerQueryMB   int     `yaml:"max_memory_per_query_mb"`     // default: 256
	MetricsEnabled        bool    `yaml:"metrics_enabled"`
	MetricsPort           int     `yaml:"metrics_port"`                // HTTP /metrics port (default: 9090)
	MetricsFormat         string  `yaml:"metrics_format"`              // "prometheus" or "otel"
	DLSEnabled            bool    `yaml:"dls_enabled"`
	CDCEnabled            bool    `yaml:"cdc_enabled"`
	CDCBufferSize         int     `yaml:"cdc_buffer_size"`             // default: 4096
	CDCWebhookURL         string  `yaml:"cdc_webhook_url"`
	VectorEnabled         bool    `yaml:"vector_enabled"`
	VectorDefaultDims     int     `yaml:"vector_default_dimensions"`   // default: 128
	VectorEfConstruction  int     `yaml:"vector_ef_construction"`      // HNSW build param (default: 200)
	VectorMaxConnections  int     `yaml:"vector_max_connections"`      // HNSW M param (default: 16)
	AdaptiveOptEnabled    bool    `yaml:"adaptive_opt_enabled"`
	AdaptiveHistorySize   int     `yaml:"adaptive_history_size"`       // executions to keep (default: 100)
	AdaptiveCostThreshold float64 `yaml:"adaptive_cost_threshold"`     // replan if actual > N*estimated (default: 2.0)
	MatViewIncrRefresh    bool    `yaml:"matview_incr_refresh"`
	MatViewRefreshIntervalMs int  `yaml:"matview_refresh_interval_ms"` // auto-refresh interval (default: 0 = manual)

	Version string `yaml:"version"` // Show version information

	// GraphQL Configuration
	EnableGraphQL bool `yaml:"enable_graphql"` // Enable GraphQL API

	// WAL Configuration (Phase 1 Performance Optimizations)
	WALEnabled           bool   `yaml:"wal_enabled"`             // Enable/disable WAL globally
	WALBulkModeThreshold int    `yaml:"wal_bulk_mode_threshold"` // Operations per second threshold for bulk mode
	WALDisableForBulkOps bool   `yaml:"wal_disable_for_bulk"`    // Disable WAL during bulk operations
	WALMode              string `yaml:"wal_mode"`                // WAL mode: "sync" or "async" (default: "sync")
	AsyncWALWorkers      int    `yaml:"async_wal_workers"`       // Number of async WAL workers (default: 2)
	AsyncWALQueueSize    int    `yaml:"async_wal_queue_size"`    // Async WAL queue size (default: 1000)

	// Durability Configuration (PostgreSQL-style fsync optimizations)
	DurabilityMode             string  `yaml:"durability_mode"`               // Durability mode: "strict", "balanced", "performance" (default: "performance")
	UseGroupCommit             bool    `yaml:"use_group_commit"`               // Use group-commit path for WAL writes; reduces contention (default: true)
	WALBatchSize               int     `yaml:"wal_batch_size"`                // Operations before WAL flush in balanced mode (default: 100)
	WALMaxFlushDelay           int     `yaml:"wal_max_flush_delay"`           // Max milliseconds before forcing WAL flush (default: 100)
	FsyncOnCommit              bool    `yaml:"fsync_on_commit"`               // Force immediate fsync on transaction commit (default: false for group commits)
	CheckpointCompletionTarget float64 `yaml:"checkpoint_completion_target"`  // Spread checkpoint writes over this fraction of checkpoint interval (default: 0.9)
	UniqueIndexMemoryBudgetMB  int     `yaml:"unique_index_memory_budget_mb"` // Memory budget for in-memory unique constraint B-tree indexes in MB (default: 500)
	BackgroundWriterDelay      int     `yaml:"background_writer_delay"`       // Milliseconds between background writer cycles (default: 200)
	MinBatchSize               int     `yaml:"min_batch_size"`                // Minimum operations per batch for auto-tuning (default: 10)
	MaxBatchSize               int     `yaml:"max_batch_size"`                // Maximum operations per batch for auto-tuning (default: 10000)
	LatencyBudgetP99Ms         int     `yaml:"latency_budget_p99_ms"`         // Target p99 latency budget in milliseconds (default: 50)
	AutoTuneWarmupCheckpoints  int     `yaml:"autotune_warmup_checkpoints"`   // Number of checkpoints for conservative warmup (default: 5)

	// Metadata Update Performance Settings
	MetadataBatchSize       int  `yaml:"metadata_batch_size"`       // Documents before metadata flush (default: 500)
	MetadataPersistInterval int  `yaml:"metadata_persist_interval"` // Documents before disk persistence (default: 1000)
	MetadataFlushInterval   int  `yaml:"metadata_flush_interval"`   // Time in seconds between forced flushes
	BulkOperationDetection  bool `yaml:"bulk_operation_detection"`  // Auto-detect bulk operations for optimization

	// Hash Index Configuration
	IndexSequenceSafetyMargin int `yaml:"index_sequence_safety_margin"` // Safety margin for sequence recovery (default: 100)

	// Response Compression Configuration
	EnableResponseCompression bool `yaml:"enable_response_compression"` // Allow zstd response compression when client requests it (default: true)

	// RCU (Read-Copy-Update) Write Mode Configuration
	// Enables lock-free concurrent writes using PostgreSQL-inspired MVCC patterns
	EnableRCUWrites  bool `yaml:"enable_rcu_writes"`   // Enable RCU-based lock-free updates (default: true)
	RCUGracePeriodMs int  `yaml:"rcu_grace_period_ms"` // Grace period in milliseconds before cleanup (default: 100)
	MaxOCCRetries    int  `yaml:"max_occ_retries"`     // Maximum OCC retry attempts on write-write conflict (default: 3)

	// B-Tree Index Persistence Configuration
	// TODO: Add B-Tree index persistence configuration settings (BTreeSyncMode, BTreeBatchSize, BTreeFlushInterval, BTreeMaxDirtyPages, BTreeEnableWAL, BTreeCheckpointInterval, BTreeWALRetentionSeconds, BTreeGroupCommitDelay)
	BTreeSyncMode            string `yaml:"btree_sync_mode"`             // Sync mode: "immediate", "batched", "scheduled" (default: "batched")
	BTreeBatchSize           int    `yaml:"btree_batch_size"`            // Operations before flush (default: 100)
	BTreeFlushInterval       int    `yaml:"btree_flush_interval"`        // Time in seconds between scheduled flushes (default: 5)
	BTreeMaxDirtyPages       int    `yaml:"btree_max_dirty_pages"`       // Memory pressure threshold for auto-flush (default: 1000)
	BTreeEnableWAL           bool   `yaml:"btree_enable_wal"`            // Enable WAL for batched durability (default: true)
	BTreeCheckpointInterval  int    `yaml:"btree_checkpoint_interval"`   // Time in seconds between checkpoints (default: 300)
	BTreeWALRetentionSeconds int    `yaml:"btree_wal_retention_seconds"` // WAL retention after checkpoint in seconds (default: 60)
	BTreeGroupCommitDelay    int    `yaml:"btree_group_commit_delay"`    // Group commit window in milliseconds (default: 10)

	// Parser Configuration
	UseNewParser bool `yaml:"use_new_parser"` // Use new SyndrQL parser instead of legacy parser (default: true)

	// Sorting Optimization Configuration (Phase 4)
	SortTopNThreshold       float64 `yaml:"sort_topn_threshold"`        // Ratio of LIMIT/total_rows for Top-N activation (default: 0.1)
	SortTopNMinSize         int     `yaml:"sort_topn_min_size"`         // Minimum dataset size for Top-N optimization (default: 100)
	SortHeapInitialCapacity int     `yaml:"sort_heap_initial_capacity"` // Initial heap capacity for Top-N queries (default: 1000)
	SortRadixMinSize        int     `yaml:"sort_radix_min_size"`        // Minimum dataset size for radix sort (default: 1000)
	SortRadixLimitRatio     float64 `yaml:"sort_radix_limit_ratio"`     // Minimum LIMIT/total_rows ratio for radix (default: 0.5)
	SortRadixMaxPasses      int     `yaml:"sort_radix_max_passes"`      // Maximum radix sort passes for wide integers (default: 8)
	SortSIMDEnabled         bool    `yaml:"sort_simd_enabled"`          // Enable SIMD string sorting optimization (default: true)
	SortSIMDAbbrevBytes     int     `yaml:"sort_simd_abbrev_bytes"`     // Bytes used for abbreviated string keys (default: 8)
	SortSIMDMinSize         int     `yaml:"sort_simd_min_size"`         // Minimum dataset size for SIMD activation (default: 100)
	SortEnableParallel      bool    `yaml:"sort_enable_parallel"`       // DEPRECATED: Use SortParallelEnabled instead
	SortParallelThreshold   int     `yaml:"sort_parallel_threshold"`    // DEPRECATED: Use SortParallelMinSize instead
	SortParallelEnabled     bool    `yaml:"sort_parallel_enabled"`      // Enable parallel sorting for large datasets (default: true)
	SortParallelMinSize     int     `yaml:"sort_parallel_min_size"`     // Minimum dataset size for parallel sort (default: 10000)
	SortMaxMemoryMB         int     `yaml:"sort_max_memory_mb"`         // Maximum memory in MB for sorting operations (default: 512)

	// JOIN SIMD Configuration
	JoinSIMDEnabled    bool `yaml:"join_simd_enabled"`     // Enable SIMD acceleration for JOIN operations (default: true)
	JoinSIMDAutoDetect bool `yaml:"join_simd_auto_detect"` // Auto-detect CPU SIMD support (default: true)

	// JOIN Concurrency: limits how many joins run full build+probe at once to avoid memory spikes
	// (e.g. 300 connections each doing a join). 0 = no limit.
	JoinConcurrencyLimit int `yaml:"join_concurrency_limit"` // Max concurrent join executions; 0 = no limit (default: 128)

	// JOIN Memory Configuration (PostgreSQL-style work_mem for joins)
	JoinMemoryLimitMB int `yaml:"join_memory_limit_mb"` // Memory limit per join in MB (default: 64, auto-detects higher for systems with >8GB RAM)

	// JOIN Memory Pool: Total memory budget (MB) for weighted semaphore - large JOINs (>10MB estimated)
	// acquire weighted slots from this pool to prevent OOM under high concurrency
	JoinMemoryPoolMB int `yaml:"join_memory_pool_mb"` // Total memory pool for large JOINs in MB (default: 2048)

	// JOIN Cache: Feature flag to enable sharded hash table cache (vs deprecated single-lock cache)
	UseShardedHashTableCache bool `yaml:"use_sharded_hash_table_cache"` // Use sharded cache for better concurrency (default: true)

	// TryLock Failure Monitoring: Log warning when TryLock failure rate exceeds this threshold (0.0-1.0)
	TryLockFailureWarningThreshold float64 `yaml:"trylock_failure_warning_threshold"` // Threshold for TryLock failure warnings (default: 0.50)

	// WHERE SIMD Configuration
	WhereSIMDEnabled            bool `yaml:"where_simd_enabled"`             // Enable SIMD acceleration for WHERE clause comparisons (default: true)
	WhereSIMDAutoDetect         bool `yaml:"where_simd_auto_detect"`         // Auto-detect CPU SIMD support for WHERE clauses (default: true)
	WhereBloomEnabled           bool `yaml:"where_bloom_enabled"`            // Enable Bloom filter pre-filtering for multi-condition WHERE clauses (default: true)
	WhereBloomMinDocuments      int  `yaml:"where_bloom_min_documents"`      // Minimum document count to activate Bloom filtering (default: 500)
	WhereBatchSIMDEnabled       bool `yaml:"where_batch_simd_enabled"`       // Enable batch/columnar SIMD processing for WHERE clauses (default: true)
	WhereBatchMinSize           int  `yaml:"where_batch_min_size"`           // Minimum document count for batch SIMD processing (default: 100)
	WhereExpressionCacheEnabled bool `yaml:"where_expression_cache_enabled"` // Enable expression caching and predicate reordering (default: true)
	WhereExpressionCacheSize    int  `yaml:"where_expression_cache_size"`    // LRU cache size for compiled expressions (default: 1000)

	// GROUP BY Strategy Configuration
	GroupByHashAggregateRowThreshold int `yaml:"groupby_hash_aggregate_row_threshold"` // Rows below this use HashAggregate, above use Sort+GroupAggregate (default: 10000)

	// GROUP BY Performance Optimization Configuration (Phases 1-3)
	GroupByParallelPageWorkers int  `yaml:"groupby_parallel_page_workers"` // Workers for parallel page loading (default: max(8, numCPU))
	GroupByCacheWarmingEnabled bool `yaml:"groupby_cache_warming_enabled"` // Enable cache warming on startup (default: true)
	GroupByCacheWarmingMaxMB   int  `yaml:"groupby_cache_warming_max_mb"`  // Memory budget for warmed cache in MB (default: 512)
	GroupBySnapshotStalenessMs int  `yaml:"groupby_snapshot_staleness_ms"` // Max staleness for COW snapshots in ms (default: 10)

	// Parallel Aggregation Configuration
	AggregationParallelEnabled bool `yaml:"aggregation_parallel_enabled"` // Enable parallel partial/finalize aggregation for GROUP BY (default: true)
	AggregationParallelMinDocs int  `yaml:"aggregation_parallel_min_docs"` // Minimum estimated docs to justify parallel aggregation overhead (default: 50000)
	AggregationParallelWorkers int  `yaml:"aggregation_parallel_workers"` // Number of parallel workers (0 = auto: min(NumCPU, 8)) (default: 0)

	// Backup & Restore Configuration
	BackupDir            string `yaml:"backup_dir"`             // Directory for backup files (default: "./backups")
	BackupCompression    string `yaml:"backup_compression"`     // Compression format: "gzip", "zstd", "none" (default: "gzip")
	BackupIncludeIndexes bool   `yaml:"backup_include_indexes"` // Include index files in backups (default: true)
	MaxRestoreSizeBytes  int64  `yaml:"max_restore_size_bytes"` // Maximum decompressed restore size in bytes (default: 10GB, 0=unlimited)

	// Incremental Backup Configuration
	BackupMaxChainDepth   int  `yaml:"backup_max_chain_depth"`     // Max incremental chain depth before requiring full backup (default: 7)
	BackupCatalogEnabled  bool `yaml:"backup_catalog_enabled"`     // Enable backup catalog tracking (default: true)
	BackupCatalogSyncToDB bool `yaml:"backup_catalog_sync_to_db"`  // Sync catalog entries to Primary DB (default: false)

	// Point-in-Time Recovery (PITR) Configuration
	WALArchiveEnabled      bool   `yaml:"wal_archive_enabled"`       // Enable WAL archiving for PITR (default: false)
	WALArchiveDir          string `yaml:"wal_archive_dir"`           // Directory for archived WAL files (default: "./wal_archive")
	WALArchivePollSeconds  int    `yaml:"wal_archive_poll_seconds"`  // Polling interval for WAL archiver in seconds (default: 60)
	WALArchiveRetentionDays int   `yaml:"wal_archive_retention_days"` // Retention period for archived WAL files in days (default: 30)
	WALArchiveCompression  string `yaml:"wal_archive_compression"`   // Compression for archived WAL files: "none", "gzip", "zstd" (default: "none")
	PITREnabled            bool   `yaml:"pitr_enabled"`              // Enable PITR features including backup LSN tracking (default: false)

	// Migration System Configuration
	MaxMigrationCommands          int     `yaml:"max_migration_commands"`           // Maximum commands per migration (default: 1000)
	MigrationPerformanceThreshold float64 `yaml:"migration_performance_threshold"`  // Performance warning threshold in seconds (default: 1.0)
	MaxValidationReportSize       int64   `yaml:"max_validation_report_size"`       // Maximum validation report size in bytes (default: 10MB)
	ValidationReportRetentionDays int     `yaml:"validation_report_retention_days"` // Days to retain validation reports (default: 30)
	EnableAutoReverse             bool    `yaml:"enable_auto_reverse"`              // Enable automatic reverse command generation (default: true)
	RequireExplicitDownCommands   bool    `yaml:"require_explicit_down_commands"`   // Require explicit DOWN commands in migrations (default: false)
	MigrationTimeoutSeconds       int     `yaml:"migration_timeout_seconds"`        // Timeout for migration operations in seconds (default: 300)

	// GraphQL Security Configuration (Layers 1-5)
	EnableComplexityLimit  bool   `yaml:"enable_complexity_limit"`   // Enable query complexity analysis (Layer 1, default: true)
	EnableDepthLimit       bool   `yaml:"enable_depth_limit"`        // Enable query depth limiting (Layer 2, default: true)
	EnableGraphQLRateLimit bool   `yaml:"enable_graphql_rate_limit"` // Enable per-user rate limiting (Layer 3, default: true)
	EnableQueryTimeout     bool   `yaml:"enable_query_timeout"`      // Enable query execution timeout (Layer 4, default: true)
	EnableQueryMonitoring  bool   `yaml:"enable_query_monitoring"`   // Enable query metrics monitoring (Layer 5, default: true)
	GraphQLRateAlgorithm   string `yaml:"graphql_rate_algorithm"`    // Rate limiting algorithm: "token-bucket" or "time-bucket" (default: "token-bucket")

	// SyndrQL Query Timeout Configuration
	QueryTimeoutSeconds      int `yaml:"query_timeout_seconds"`       // Default query execution timeout in seconds (default: 300)
	AdminQueryTimeoutSeconds int `yaml:"admin_query_timeout_seconds"` // Admin query execution timeout in seconds (default: 600)

	// Per-Query Memory Limit Configuration (DoS Protection)
	QueryMaxMemoryMB      int `yaml:"query_max_memory_mb"`       // Maximum memory per query in MB (default: 25)
	AdminQueryMaxMemoryMB int `yaml:"admin_query_max_memory_mb"` // Maximum memory per admin query in MB (default: 25)
	// Note: JOIN operations have a separate MemoryLimit in JoinRequest which remains unchanged

	// Security: Resource limits for GROUP BY and JOIN operations
	MaxGroupByCardinality int  `yaml:"max_group_by_cardinality"` // Maximum number of groups in GROUP BY (default: 1000000)
	MaxJoinResultSize     int  `yaml:"max_join_result_size"`     // Maximum number of rows in JOIN result (default: 10000000)
	RequireJoinCondition  bool `yaml:"require_join_condition"`   // Require ON clause for JOINs (default: true)

	// Ghost Cleanup (Automatic Compaction) Configuration
	GhostCleanupEnabled         bool    `yaml:"ghost_cleanup_enabled"`          // Enable automatic ghost record cleanup (default: true)
	GhostCleanupIntervalSeconds int     `yaml:"ghost_cleanup_interval_seconds"` // Cleanup cycle interval in seconds (default: 10)
	GhostCleanupBatchSize       int     `yaml:"ghost_cleanup_batch_size"`       // Batch size for processing (default: 10000)
	GhostCleanupPauseThreshold  int     `yaml:"ghost_cleanup_pause_threshold"`  // Active query pause threshold (default: 500)
	GhostCleanupTombstoneRatio  float64 `yaml:"ghost_cleanup_tombstone_ratio"`  // Tombstone ratio threshold for compaction (default: 0.3)

	// MVCC GC (Automatic Version Cleanup) Configuration
	MVCCGCEnabled              bool `yaml:"mvcc_gc_enabled"`                // Enable automatic MVCC version cleanup (default: true)
	MVCCGCIntervalSeconds      int  `yaml:"mvcc_gc_interval_seconds"`       // GC cycle interval in seconds (default: 30)
	MVCCGCBatchSize            int  `yaml:"mvcc_gc_batch_size"`             // Max bundles to process per cycle (default: 10)
	MVCCGCPauseThreshold       int  `yaml:"mvcc_gc_pause_threshold"`        // Active query pause threshold (default: 500)
	MVCCGCMaxVersionsThreshold int  `yaml:"mvcc_gc_max_versions_threshold"` // Trigger compaction if document has >N versions (default: 5)
	MVCCGCMinVersionAgeHours   int  `yaml:"mvcc_gc_min_version_age_hours"`  // Minimum age in hours before considering version for GC (default: 1)

	// Vacuum (Dead Version Reclamation) Configuration
	VacuumEnabled            bool    `yaml:"vacuum_enabled"`              // Enable page-level dead version reclamation (default: true)
	VacuumDeadRatioThreshold float64 `yaml:"vacuum_dead_ratio_threshold"` // Trigger full compaction at this dead ratio (default: 0.3)
	VacuumMaxPagesPerCycle   int     `yaml:"vacuum_max_pages_per_cycle"`  // Max pages to scan per GC cycle (default: 100)

	// Page-Level Bloom Filter Configuration
	PageBloomEnabled          bool    `yaml:"page_bloom_enabled"`            // Enable per-page bloom filters for scan skip optimization (default: true)
	PageBloomFalsePositiveRate float64 `yaml:"page_bloom_false_positive_rate"` // Target false positive rate for page blooms (default: 0.01 = 1%)
	PageBloomMinDocsPerPage   int     `yaml:"page_bloom_min_docs_per_page"`  // Min docs on a page to justify building bloom (default: 50)
	PageBloomMaxMemoryMB      int     `yaml:"page_bloom_max_memory_mb"`      // Global memory budget for all page blooms in MB (default: 64)
	PageBloomRefreshIntervalSec int   `yaml:"page_bloom_refresh_interval_sec"` // Background builder interval in seconds (default: 30)

	// Streaming & Cursor Configuration
	StreamingChunkSize       int `yaml:"streaming_chunk_size"`         // Documents per streaming chunk (default: 256)
	MaxOpenCursorsPerSession int `yaml:"max_open_cursors_per_session"` // Max server-side cursors per session (default: 64)
	CursorIdleTimeoutSeconds int `yaml:"cursor_idle_timeout_seconds"`  // Idle timeout for cursors in seconds (default: 300)

	// Monitor (Live Session Streaming) Configuration
	MonitorDefaultIntervalMS int `yaml:"monitor_default_interval_ms"` // Default snapshot interval in milliseconds (default: 1000)
	MonitorMinIntervalMS     int `yaml:"monitor_min_interval_ms"`     // Minimum allowed interval in milliseconds (default: 100)
	MonitorMaxIntervalMS     int `yaml:"monitor_max_interval_ms"`     // Maximum allowed interval in milliseconds (default: 60000)

	// Page-Level Storage Compression Configuration
	StorageCompression    string `yaml:"storage_compression"`      // "none" or "zstd" (default: "none")
	CompressionMinDocSize int    `yaml:"compression_min_doc_size"` // Min bytes to compress (default: 256)

	// Index Maintenance (Automatic Index Rebuild) Configuration
	IndexMaintenanceEnabled              bool    `yaml:"index_maintenance_enabled"`                // Enable automatic index rebuilding (default: true)
	IndexMaintenanceIntervalSeconds      int     `yaml:"index_maintenance_interval_seconds"`       // Evaluation interval in seconds (default: 300 = 5 minutes)
	IndexMaintenanceBatchSize            int     `yaml:"index_maintenance_batch_size"`             // Max indexes to rebuild per cycle (default: 3)
	IndexMaintenanceStalenessThreshold   float64 `yaml:"index_maintenance_staleness_threshold"`    // Staleness % to trigger rebuild (default: 0.20 = 20%)
	IndexMaintenanceMinRebuildInterval   int     `yaml:"index_maintenance_min_rebuild_interval"`   // Min seconds between rebuilds of same index (default: 3600 = 1 hour)
	IndexMaintenanceWorkerCount          int     `yaml:"index_maintenance_worker_count"`           // Concurrent rebuild workers (default: 2)
	IndexMaintenancePauseThreshold       int     `yaml:"index_maintenance_pause_threshold"`        // Pause if active queries >= this (default: 500)
	IndexMaintenanceQueryFrequencyWeight float64 `yaml:"index_maintenance_query_frequency_weight"` // Weight for query frequency in priority (default: 1.0)

	// Concurrency & Locking Configuration (HIGH-007)
	LockTimeoutSeconds           int `yaml:"lock_timeout_seconds"`             // Timeout for lock acquisition in seconds (default: 30)
	MaxWorkerPools               int `yaml:"max_worker_pools"`                 // Maximum number of worker pools (default: 10)
	WorkerPoolStopTimeoutSeconds int `yaml:"worker_pool_stop_timeout_seconds"` // Timeout for stopping worker pools in seconds (default: 30)

	// Query Plan Cache Configuration
	PlanCacheEnabled           bool `yaml:"plan_cache_enabled"`             // Enable query plan caching (default: true)
	PlanCacheCapacity          int  `yaml:"plan_cache_capacity"`            // Maximum cached plans per shard (default: 1000, 8 shards = 8000 total)
	PlanCacheAdaptivePlanning  bool `yaml:"plan_cache_adaptive_planning"`   // Enable PostgreSQL-style adaptive planning (default: true)
	PlanCacheCustomThreshold   int  `yaml:"plan_cache_custom_threshold"`    // Number of custom executions before generic plan evaluation (default: 5)
	PlanCacheWriteThreshold    int  `yaml:"plan_cache_write_threshold"`     // Write count threshold for invalidation (default: 1000)
	PlanCacheStaleServeSeconds int  `yaml:"plan_cache_stale_serve_seconds"` // Stale plan serving window in seconds (default: 60)
	// TODO: I will add PlanCacheMemoryLimitMB for memory-aware eviction when memory profiling reveals actual cache memory usage
	// TODO: I will add PlanCacheCompressionEnabled for large plan compression when we have plans exceeding 10KB

	// Page cache / memory-leak fix knobs
	MaxLoadedDocumentPages             int `yaml:"max_loaded_document_pages"`                // Max pages in BundleService.documentPages (shared); 0 = use default 500
	BundleAdapterMaxCachedPages        int `yaml:"bundle_adapter_max_cached_pages"`          // Max pages in BundleAdapter.cachedPages per scanner; 0 = use default 500
	DocumentPageMapMaxEntriesPerBundle int `yaml:"document_page_map_max_entries_per_bundle"` // Max documentID->pageID entries per bundle in documentPageMap; 0 = use default 100000

	// File/segment read cache: avoids repeated full-file reads when loading many pages (e.g. getAllDocumentsForIndexing).
	// Bounded by max entries; LRU eviction. Keyed by file path.
	FileReadCacheMaxEntries int `yaml:"file_read_cache_max_entries"` // Max file/segment buffers in cache; 0 = use default 32

	// Page cache GetDocumentPage: backoff (ms) when TryLock fails on cache miss before blocking Lock.
	// 0 = no sleep (low latency); 1–2 = better cache fill under contention (code-review Issue 4).
	PageCacheTryLockBackoffMs int `yaml:"page_cache_trylock_backoff_ms"` // Default 0

	// Prepared Statement Cache Configuration (for parameterized queries)
	PreparedStatementCacheEnabled  bool `yaml:"prepared_statement_cache_enabled"`  // Enable prepared statement caching (default: true)
	PreparedStatementCacheCapacity int  `yaml:"prepared_statement_cache_capacity"` // Maximum cached statements per shard (default: 1000, 8 shards = 8000 total)
	MaxParametersPerQuery          int  `yaml:"max_parameters_per_query"`          // Maximum parameters per query (default: 1000)

	// Input Length Limits (MED-003: Memory exhaustion DoS prevention)
	MaxParameterValueLength int `yaml:"max_parameter_value_length"` // Maximum length for individual parameter values in bytes (default: 10000 = 10KB)
	MaxFieldValueLength     int `yaml:"max_field_value_length"`     // Maximum length for individual field values in bytes (default: 50000 = 50KB)
	MaxFieldNameLength      int `yaml:"max_field_name_length"`      // Maximum length for field names in bytes (default: 256 = 256 bytes)
	// TODO: Could expose prepared statement metrics via SELECT * FROM 'system.prepared_statements' for observability in future metrics system expansion
	// TODO: Could support preloading common statements on server startup from configuration file for cache warming

	// TIER 1 SUBQUERY SUPPORT: Subquery execution configuration
	SubqueryEnabled           bool `yaml:"subquery_enabled"`              // Enable subquery support (default: true)
	SubquerySmallThreshold    int  `yaml:"subquery_small_threshold"`      // IN-list strategy threshold (default: 100)
	SubqueryHashThreshold     int  `yaml:"subquery_medium_threshold"`     // HashJoin strategy threshold (default: 10000)
	SubqueryMaxDepth          int  `yaml:"subquery_max_depth"`            // Maximum nesting depth (default: 3)
	SubqueryMaxInListMemoryMB int  `yaml:"subquery_max_inlist_memory_mb"` // Max IN-list memory in MB (default: 10)

	// Bundle Statistics Configuration (for correlated subquery optimization)
	StatsCompressionThresholdMB  int     `yaml:"stats_compression_threshold_mb"`   // Compression threshold in MB (default: 1)
	StatsAutoAnalyzeEnabled      bool    `yaml:"stats_auto_analyze_enabled"`       // Enable automatic statistics analysis (default: true)
	StatsAutoAnalyzeThreshold    int     `yaml:"stats_auto_analyze_threshold"`     // Document changes before auto-analyze (default: 1000)
	StatsAutoAnalyzeRatio        float64 `yaml:"stats_auto_analyze_ratio"`         // Ratio of changes to bundle size for auto-analyze (default: 0.10)
	StatsScheduledAnalyzeEnabled bool    `yaml:"stats_scheduled_analyze_enabled"`  // Enable scheduled background analysis (default: true)
	StatsScheduledAnalyzeTime    string  `yaml:"stats_scheduled_analyze_time"`     // Time for scheduled analysis in HH:MM format (default: "02:00")
	StatsScheduledAnalyzeMinDocs int     `yaml:"stats_scheduled_analyze_min_docs"` // Minimum documents for scheduled analysis (default: 100000)

	// DateTime Timezone Cache Configuration
	// TODO: I will add support for runtime cache size adjustment if profiling shows cache misses exceed 5%
	TimezoneCacheSize int `yaml:"timezone_cache_size"` // LRU cache size for rare timezones (default: 128, hot zones cached separately)

	// DROP BUNDLE RESTRICT Validation Configuration
	RestrictValidationThorough    bool `yaml:"restrict_validation_thorough"`     // Enable thorough validation (all documents) vs sampling mode (default: true)
	RestrictValidationSampleSize  int  `yaml:"restrict_validation_sample_size"`  // Sample size for probabilistic sampling mode (default: 1000, stops at 100 violations)
	RestrictValidationLogProgress bool `yaml:"restrict_validation_log_progress"` // Enable progress logging for DROP validation scans - useful for debugging large bundles but generates significant log output in production. Recommended: false for production, true for development/troubleshooting (default: false)

	// Error Reporting Configuration (MED-004)
	ErrorInternalLogFile string `yaml:"error_internal_log_file"` // Internal error log file path (default: "errors_internal.log")
	ErrorExternalLogFile string `yaml:"error_external_log_file"` // External/access error log file path (default: "errors_external.log")
	ErrorShowInConsole   bool   `yaml:"error_show_in_console"`   // Show errors in console (debug mode only, default: true when debug=true)
	ErrorIncludeStack    bool   `yaml:"error_include_stack"`     // Include stack traces in internal logs (default: true)

	// Observability: Slow Query Log Configuration
	SlowQueryLogEnabled  bool `yaml:"slow_query_log_enabled"`  // Enable slow query warning log (default: true)
	SlowQueryThresholdMs int  `yaml:"slow_query_threshold_ms"` // Threshold in ms for slow query detection (default: 1000)

	// PostgreSQL Import Configuration
	PGImportDefaultBatchSize int    `yaml:"pg_import_default_batch_size"` // Default batch size for PG import (default: 1000)
	PGImportProgressInterval int    `yaml:"pg_import_progress_interval"` // Log progress every N records (default: 10000)
	PGImportMaxErrors        int    `yaml:"pg_import_max_errors"`        // Maximum errors before abort (default: 100)
	PGImportTempDir          string `yaml:"pg_import_temp_dir"`          // Temp directory for pg_restore output (default: os.TempDir())

	// MySQL Import Configuration (Enterprise)
	MySQLImportDefaultBatchSize int `yaml:"mysql_import_default_batch_size"` // Default batch size for MySQL import (default: 1000)
	MySQLImportProgressInterval int `yaml:"mysql_import_progress_interval"` // Log progress every N records (default: 10000)
	MySQLImportMaxErrors        int `yaml:"mysql_import_max_errors"`        // Maximum errors before abort (default: 100)

	// MongoDB Import Configuration (Enterprise)
	MongoImportDefaultBatchSize  int `yaml:"mongo_import_default_batch_size"`  // Default batch size for MongoDB import (default: 1000)
	MongoImportProgressInterval  int `yaml:"mongo_import_progress_interval"`  // Log progress every N records (default: 10000)
	MongoImportMaxErrors         int `yaml:"mongo_import_max_errors"`         // Maximum errors before abort (default: 100)
	MongoImportSchemaSampleSize  int `yaml:"mongo_import_schema_sample_size"` // Documents to sample for schema inference (default: 1000)

	// MSSQL Import Configuration (Enterprise)
	MSSQLImportDefaultBatchSize int `yaml:"mssql_import_default_batch_size"` // Default batch size for MSSQL import (default: 1000)
	MSSQLImportProgressInterval int `yaml:"mssql_import_progress_interval"` // Log progress every N records (default: 10000)
	MSSQLImportMaxErrors        int `yaml:"mssql_import_max_errors"`        // Maximum errors before abort (default: 100)

	// Sharding Configuration (Enterprise)
	ShardingEnabled      bool   `yaml:"sharding_enabled"`       // Enable range-based sharding (default: false)
	ShardDefaultBehavior string `yaml:"shard_default_behavior"` // "reject" or "default" for missing shard key (default: "reject")
	ShardRebalanceWorkers int   `yaml:"shard_rebalance_workers"` // Worker count for parallel rebalance (default: 4)

	// Enterprise Extension Configuration
	// Captures any `enterprise:` section from syndrdb.yml for enterprise plugin settings.
	EnterpriseSettings map[string]interface{} `yaml:"enterprise" json:"enterprise,omitempty"`
}

var (
	instance *Arguments
	once     sync.Once
	mu       sync.RWMutex
)

// GetSettings returns the global settings instance
func GetSettings() *Arguments {
	once.Do(func() {
		instance = &Arguments{
			// Default values
			DataDir:             "./data",
			LogDir:              "",
			LogFile:             "", // Default to stdout
			ConfigFile:          "",
			Mode:                "standalone",
			Host:                "0.0.0.0",
			Port:                27017,
			Verbose:             false,
			AuthEnabled:         true,
			CreateDefaultDB:     true,
			Version:             "0.1.0",
			BundleStorageFormat: "binary", // Binary (BSON) format is the only supported format

			// Storage Engine Configuration
			Storage: StorageConfig{
				BundleFileMaxSizeMB: 32, // Default 32MB per file
			},

			ExportRealtimeMetrics: false, // Export real-time metrics for monitoring

			// PHASE 1 PERFORMANCE DEFAULTS
			WALEnabled:           true, // WAL enabled by default
			WALBulkModeThreshold: 50,   // >50 ops/sec = bulk mode
			WALDisableForBulkOps: true, // Disable WAL during bulk operations

			// PHASE 2 ASYNC WAL DEFAULTS
			WALMode:           "sync", // Start with sync mode for safety
			AsyncWALWorkers:   2,      // 2 workers for async WAL
			AsyncWALQueueSize: 1000,   // 1000 operation queue

			// Durability Configuration Defaults (PostgreSQL-style)
			DurabilityMode:             "performance", // Performance mode: group commits with async fsync
			UseGroupCommit:             true,          // Group commit by default for lower WAL contention
			WALBatchSize:               100,           // Batch 100 operations before flush
			WALMaxFlushDelay:           100,           // Force flush after 100ms max
			FsyncOnCommit:              false,         // Disable immediate fsync to enable group commits
			CheckpointCompletionTarget: 0.9,           // Spread writes over 90% of checkpoint interval
			UniqueIndexMemoryBudgetMB:  500,           // Default: 500MB memory budget for in-memory unique indexes
			BackgroundWriterDelay:      200,           // Background writer runs every 200ms
			MinBatchSize:               10,            // Minimum 10 ops per batch (auto-tune lower bound)
			MaxBatchSize:               10000,         // Maximum 10000 ops per batch (auto-tune upper bound)
			LatencyBudgetP99Ms:         50,            // Target p99 latency under 50ms
			AutoTuneWarmupCheckpoints:  5,             // Conservative warmup over first 5 checkpoints

			MetadataBatchSize:       500,  // Increased from 50 to 500
			MetadataPersistInterval: 1000, // Persist every 1000 documents
			MetadataFlushInterval:   10,   // Flush every 10 seconds
			BulkOperationDetection:  true, // Auto-detect bulk operations

			// B-Tree Index Persistence Defaults
			BTreeSyncMode:            "batched", // Batched mode for performance with WAL durability
			BTreeBatchSize:           100,       // Batch 100 operations before flush
			BTreeFlushInterval:       5,         // Flush every 5 seconds
			BTreeMaxDirtyPages:       1000,      // Auto-flush at 1000 dirty pages
			BTreeEnableWAL:           true,      // Enable WAL for crash recovery
			BTreeCheckpointInterval:  300,       // Checkpoint every 5 minutes
			BTreeWALRetentionSeconds: 60,        // Retain WAL for 60 seconds post-checkpoint
			BTreeGroupCommitDelay:    10,        // 10ms group commit window

			// Parser Configuration (default to false for safety)
			UseNewParser: true, // Use new parser by default

			// PHASE 4 SORTING OPTIMIZATION DEFAULTS
			SortTopNThreshold:       0.1,  // Top-N when LIMIT < 10% of dataset
			SortTopNMinSize:         100,  // Minimum 100 docs for Top-N
			SortHeapInitialCapacity: 1000, // Pre-allocate heap for 1000 items
			SortRadixMinSize:        1000, // Minimum 1000 docs for radix
			SortRadixLimitRatio:     0.5,  // Radix when LIMIT >= 50% of data
			SortRadixMaxPasses:      8,    // Support up to 64-bit integers
			SortSIMDEnabled:         true, // Enable SIMD string optimization
			SortSIMDAbbrevBytes:     8,    // 8-byte abbreviated keys
			SortSIMDMinSize:         100,  // SIMD for datasets >= 100 docs

			// JOIN SIMD Configuration
			JoinSIMDEnabled:    true, // Enable SIMD for JOIN operations
			JoinSIMDAutoDetect: true, // Auto-detect CPU capabilities

			// JOIN Concurrency: 128 by default to allow high parallelism; large joins use weighted semaphore
			JoinConcurrencyLimit: 128,

			// JOIN Memory Configuration (PostgreSQL-style work_mem for joins)
			JoinMemoryLimitMB: getDefaultJoinMemoryLimitMB(), // Default 64MB, or 128MB if system has >8GB RAM

			// JOIN Memory Pool: 2GB default for weighted semaphore to prevent OOM under extreme load
			JoinMemoryPoolMB: 2048,

			// JOIN Cache: Use sharded cache by default for better concurrency
			UseShardedHashTableCache: true,

			// TryLock Failure Monitoring: Warn when 50% of TryLock attempts fail
			TryLockFailureWarningThreshold: 0.50,

			// WHERE SIMD Configuration
			WhereSIMDEnabled:    true, // Enable SIMD for WHERE comparisons
			WhereSIMDAutoDetect: true, // Auto-detect CPU capabilities

			// WHERE Bloom Filter Configuration
			WhereBloomEnabled:      true, // Enable Bloom pre-filtering
			WhereBloomMinDocuments: 500,  // Activate Bloom for 500+ documents

			// WHERE Batch/Columnar SIMD Configuration
			WhereBatchSIMDEnabled: true, // Enable batch SIMD for WHERE clauses
			WhereBatchMinSize:     100,  // Activate batch SIMD for 100+ documents

			// WHERE Expression Caching Configuration (Priority 4)
			WhereExpressionCacheEnabled:      true,    // Enable expression caching and predicate reordering
			WhereExpressionCacheSize:         1000,    // Cache 1000 compiled expressions
			GroupByHashAggregateRowThreshold: 1000000, // Use HashAggregate for <1,000,000 rows, else Sort+GroupAggregate

			// GROUP BY Performance Optimization Defaults (Phases 1-3)
			GroupByParallelPageWorkers: func() int {
				workers := runtime.NumCPU()
				if workers < 8 {
					return 8 // Minimum 8 workers
				}
				return workers // Use all CPU cores
			}(),
			GroupByCacheWarmingEnabled: true,  // Opt-out: enabled by default
			GroupByCacheWarmingMaxMB:   512,   // 512MB default cache budget
			GroupBySnapshotStalenessMs: 5000,  // 5 seconds staleness (background cleaner handles cleanup)

			// Parallel Aggregation Defaults
			AggregationParallelEnabled: true,  // Enable parallel partial/finalize aggregation
			AggregationParallelMinDocs: 50000, // 50k+ docs to justify goroutine overhead
			AggregationParallelWorkers: 0,     // Auto: min(NumCPU, 8)

			SortParallelMinSize:        10000, // Phase 5: 10k+ docs for parallel sort
			SortMaxMemoryMB:            512,   // 512MB memory limit

			// Backup & Restore Defaults
			BackupDir:            "./backups", // Default backup directory
			BackupCompression:    "gzip",      // Use gzip compression by default
			BackupIncludeIndexes: true,                    // Include indexes in backups
			MaxRestoreSizeBytes:  10 * 1024 * 1024 * 1024, // 10GB max decompressed restore size

			// Incremental Backup Defaults
			BackupMaxChainDepth:   7,    // Max 7 incremental backups in a chain
			BackupCatalogEnabled:  true, // Track backups in catalog by default
			BackupCatalogSyncToDB: false, // Don't sync to Primary DB by default

			// PITR Defaults
			WALArchiveEnabled:       false,           // WAL archiving off by default
			WALArchiveDir:           "./wal_archive", // Default archive directory
			WALArchivePollSeconds:   60,              // Poll every 60 seconds
			WALArchiveRetentionDays: 30,              // Keep archives for 30 days
			WALArchiveCompression:   "none",          // No compression by default
			PITREnabled:             false,           // PITR off by default

			// Migration System Defaults
			MaxMigrationCommands:          1000,     // Maximum 1000 commands per migration
			MigrationPerformanceThreshold: 1.0,      // Warn if operation takes > 1 second
			MaxValidationReportSize:       10485760, // 10MB max report size
			ValidationReportRetentionDays: 30,       // Keep reports for 30 days
			EnableAutoReverse:             true,     // Auto-generate reverse commands
			RequireExplicitDownCommands:   false,    // Don't require explicit DOWN commands
			MigrationTimeoutSeconds:       300,      // 5 minute timeout for migrations

			// GraphQL Security Defaults (all layers enabled by default)
			EnableComplexityLimit:  true,           // Layer 1: Query complexity analysis ON
			EnableDepthLimit:       true,           // Layer 2: Query depth limiting ON
			EnableGraphQLRateLimit: true,           // Layer 3: Per-user rate limiting ON
			EnableQueryTimeout:     true,           // Layer 4: Query execution timeout ON
			EnableQueryMonitoring:  true,           // Layer 5: Query metrics monitoring ON
			GraphQLRateAlgorithm:   "token-bucket", // Default to token bucket algorithm

			// SyndrQL Query Timeout Defaults
			QueryTimeoutSeconds:      300, // 5 minute default timeout for regular queries
			AdminQueryTimeoutSeconds: 600, // 10 minute default timeout for admin queries

			// Per-Query Memory Limit Defaults (DoS Protection)
			QueryMaxMemoryMB:      25, // 25MB default per-query memory limit
			AdminQueryMaxMemoryMB: 25, // 25MB default per-query memory limit for admins

			// Security: Resource limits for GROUP BY and JOIN
			MaxGroupByCardinality: 1_000_000,  // 1M groups max
			MaxJoinResultSize:     10_000_000,  // 10M rows max
			RequireJoinCondition:  true,        // Require ON clause to prevent Cartesian products

			// Ghost Cleanup (Automatic Compaction) Defaults
			GhostCleanupEnabled:         true,  // Enable ghost cleanup by default
			GhostCleanupIntervalSeconds: 10,    // Run every 10 seconds (SQL Server pattern)
			GhostCleanupBatchSize:       10000, // Process up to 10,000 records per cycle
			GhostCleanupPauseThreshold:  500,   // Pause when 500+ queries active
			GhostCleanupTombstoneRatio:  0.3,   // Compact when 30% tombstones

			// MVCC GC (Automatic Version Cleanup) Defaults
			MVCCGCEnabled:              true, // Enable MVCC GC by default
			MVCCGCIntervalSeconds:      30,   // Run every 30 seconds
			MVCCGCBatchSize:            10,   // Process up to 10 bundles per cycle
			MVCCGCPauseThreshold:       500,  // Pause when 500+ queries active
			MVCCGCMaxVersionsThreshold: 5,    // Trigger compaction if document has >5 versions
			MVCCGCMinVersionAgeHours:   1,    // Minimum 1 hour age before considering version for GC

			// Vacuum (Dead Version Reclamation) Defaults
			VacuumEnabled:            true, // Enable page-level dead version reclamation
			VacuumDeadRatioThreshold: 0.3,  // Trigger full compaction at 30% dead ratio
			VacuumMaxPagesPerCycle:   100,  // Scan up to 100 pages per GC cycle

			// Page-Level Bloom Filter Defaults
			PageBloomEnabled:            true,  // Enable per-page bloom filters
			PageBloomFalsePositiveRate:  0.01,  // 1% false positive rate
			PageBloomMinDocsPerPage:     50,    // Min 50 docs to build bloom
			PageBloomMaxMemoryMB:        64,    // 64 MB global budget
			PageBloomRefreshIntervalSec: 30,    // Refresh every 30 seconds

			// Streaming & Cursor Defaults
			StreamingChunkSize:       256, // 256 documents per streaming chunk
			MaxOpenCursorsPerSession: 64,  // Max 64 open cursors per session
			CursorIdleTimeoutSeconds: 300, // 5-minute idle timeout for cursors

			// Monitor (Live Session Streaming) Defaults
			MonitorDefaultIntervalMS: 1000,  // 1-second default interval
			MonitorMinIntervalMS:     100,   // 100ms minimum interval
			MonitorMaxIntervalMS:     60000, // 60-second maximum interval

			// Page-Level Storage Compression Defaults
			StorageCompression:    "none", // Disabled by default for backward compatibility
			CompressionMinDocSize: 256,    // Only compress documents >= 256 bytes

			// Response Compression Defaults
			EnableResponseCompression: true, // Allow zstd compression when client requests it

			// RCU (Read-Copy-Update) Write Mode Defaults
			// Lock-free concurrent writes using PostgreSQL-inspired patterns
			EnableRCUWrites:  true, // Enable RCU-based lock-free updates by default
			RCUGracePeriodMs: 100,  // 100ms grace period for in-flight reads
			MaxOCCRetries:    3,    // Retry up to 3 times on write-write conflict

			// Concurrency & Locking Configuration Defaults (HIGH-007)
			LockTimeoutSeconds:           30, // 30 seconds timeout for lock acquisition
			MaxWorkerPools:               10, // Maximum 10 worker pools
			WorkerPoolStopTimeoutSeconds: 30, // 30 seconds timeout for stopping worker pools

			// Query Plan Cache Defaults
			PlanCacheEnabled:           true, // Enable plan caching by default
			PlanCacheCapacity:          1000, // 1000 plans per shard (8000 total)
			PlanCacheAdaptivePlanning:  true, // Enable adaptive planning
			PlanCacheCustomThreshold:   5,    // 5 custom executions before generic plan
			PlanCacheWriteThreshold:    1000, // Invalidate after 1000 writes
			PlanCacheStaleServeSeconds: 60,   // 60 second stale serving window

			// Page cache / memory-leak fix defaults
			MaxLoadedDocumentPages:             500,    // Max pages in BundleService.documentPages (shared)
			BundleAdapterMaxCachedPages:        500,    // Max pages in BundleAdapter.cachedPages per scanner
			DocumentPageMapMaxEntriesPerBundle: 100000, // Max documentID->pageID entries per bundle
			FileReadCacheMaxEntries:            32,     // Max file/segment buffers (avoids repeated full-file reads per page)
			PageCacheTryLockBackoffMs:          0,     // Backoff (ms) when GetDocumentPage TryLock fails; 0 = no sleep (low latency), 2 = better cache fill under contention

			// Prepared Statement Cache Defaults
			PreparedStatementCacheEnabled:  true, // Enable prepared statement caching by default
			PreparedStatementCacheCapacity: 1000, // 1000 statements per shard (8000 total)
			MaxParametersPerQuery:          1000, // Maximum 1000 parameters per query

			// Input Length Limits Defaults (MED-003)
			MaxParameterValueLength: 10000, // 10KB max per parameter value (prevents memory exhaustion)
			MaxFieldValueLength:     50000, // 50KB max per field value (allows JSON payloads, prevents DoS)
			MaxFieldNameLength:      256,   // 256 bytes max field name (reasonable limit)

			// TIER 1 SUBQUERY SUPPORT: Subquery Execution Defaults
			SubqueryEnabled:           true,  // Enable subquery support by default
			SubquerySmallThreshold:    100,   // Use IN-list for <100 rows
			SubqueryHashThreshold:     10000, // Use HashJoin for <10k rows
			SubqueryMaxDepth:          3,     // Maximum 3 levels of nesting
			SubqueryMaxInListMemoryMB: 10,    // 10MB max for IN-list materialization

			// Bundle Statistics Defaults
			StatsCompressionThresholdMB:  1,       // Compress statistics >1MB
			StatsAutoAnalyzeEnabled:      true,    // Enable auto-analyze by default
			StatsAutoAnalyzeThreshold:    1000,    // Auto-analyze after 1000 changes
			StatsAutoAnalyzeRatio:        0.10,    // Or 10% of bundle size
			StatsScheduledAnalyzeEnabled: true,    // Enable scheduled analysis
			StatsScheduledAnalyzeTime:    "02:00", // Run at 2 AM server local time
			StatsScheduledAnalyzeMinDocs: 100000,  // Only for bundles >100k docs

			// DateTime Timezone Cache Defaults
			TimezoneCacheSize: 128, // Cache 128 rare timezones (20 hot zones cached separately in sync.Map)

			// DROP BUNDLE RESTRICT Validation Defaults
			RestrictValidationThorough:    true,  // Thorough validation by default for safety
			RestrictValidationSampleSize:  1000,  // Sample 1000 DocumentIDs in sampling mode
			RestrictValidationLogProgress: false, // Disable progress logging by default (production)

			// Error Reporting Defaults (MED-004)
			ErrorInternalLogFile: "errors_internal.log", // Internal error log
			ErrorExternalLogFile: "errors_external.log", // External/access log
			ErrorShowInConsole:   true,                  // Show in console (will be set based on debug mode)
			ErrorIncludeStack:    true,                  // Include stack traces in logs

			// Observability: Slow Query Log Defaults
			SlowQueryLogEnabled:  true, // Enable slow query detection by default
			SlowQueryThresholdMs: 1000, // 1 second threshold

			// PostgreSQL Import Defaults
			PGImportDefaultBatchSize: 1000,
			PGImportProgressInterval: 10000,
			PGImportMaxErrors:        100,

			// MySQL Import Defaults
			MySQLImportDefaultBatchSize: 1000,
			MySQLImportProgressInterval: 10000,
			MySQLImportMaxErrors:        100,

			// MongoDB Import Defaults
			MongoImportDefaultBatchSize: 1000,
			MongoImportProgressInterval: 10000,
			MongoImportMaxErrors:        100,
			MongoImportSchemaSampleSize: 1000,

			// MSSQL Import Defaults
			MSSQLImportDefaultBatchSize: 1000,
			MSSQLImportProgressInterval: 10000,
			MSSQLImportMaxErrors:        100,
		}
	})
	return instance
}

// UpdateSettings updates the global settings with new values
func UpdateSettings(args Arguments) {
	mu.Lock()
	defer mu.Unlock()

	// Only update non-empty/non-zero values
	if args.DataDir != "" {
		instance.DataDir = args.DataDir
	}
	if args.LogDir != "" {
		instance.LogDir = args.LogDir
	}
	if args.LogFile != "" {
		instance.LogFile = args.LogFile
	}
	if args.ConfigFile != "" {
		instance.ConfigFile = args.ConfigFile
	}
	if args.Mode != "" {
		instance.Mode = args.Mode
	}
	if args.Host != "" {
		instance.Host = args.Host
	}
	if args.Port != 0 {
		instance.Port = args.Port
	}

	if args.CreateDefaultDB {
		instance.CreateDefaultDB = args.CreateDefaultDB
	}
	// Boolean flags need special handling since false is a valid value
	instance.Verbose = args.Verbose
	instance.AuthEnabled = args.AuthEnabled

	if args.Version != "" {
		instance.Version = args.Version
	}

	if args.BundleStorageFormat != "" {
		instance.BundleStorageFormat = args.BundleStorageFormat
	}

	// Backup settings
	if args.BackupDir != "" {
		instance.BackupDir = args.BackupDir
	}
	if args.BackupCompression != "" {
		instance.BackupCompression = args.BackupCompression
	}
	instance.BackupIncludeIndexes = args.BackupIncludeIndexes
}

// GraphQLSecurityConfig contains configuration for GraphQL security layers
type GraphQLSecurityConfig struct {
	// Layer enable/disable flags (from Arguments)
	EnableComplexityLimit  bool
	EnableDepthLimit       bool
	EnableGraphQLRateLimit bool
	EnableQueryTimeout     bool
	EnableQueryMonitoring  bool
	RateAlgorithm          string // "token-bucket" or "time-bucket"

	// Role-based complexity limits (Layer 1 & 2)
	AdminComplexityLimit         int // Unlimited (0 = no limit)
	AuthenticatedComplexityLimit int // Default: 200
	AnonymousComplexityLimit     int // Default: 50
	DepthLimit                   int // Default: 7 levels

	// Role-based rate limits (Layer 3) - queries per minute
	AdminQueryRateLimit         int // Unlimited (0 = no limit)
	AuthenticatedQueryRateLimit int // Default: 100/min
	AnonymousQueryRateLimit     int // Default: 20/min

	// Role-based rate limits - mutations per minute
	AdminMutationRateLimit         int // Unlimited (0 = no limit)
	AuthenticatedMutationRateLimit int // Default: 10/min
	AnonymousMutationRateLimit     int // Default: 2/min

	// Burst capacity multipliers for token bucket
	AuthenticatedBurstMultiplier float64 // Default: 2.0x (200 tokens for 100/min rate)
	AnonymousBurstMultiplier     float64 // Default: 1.5x (30 tokens for 20/min rate)

	// Operation costs for rate limiting
	QueryCost    int // Default: 1 token
	MutationCost int // Default: 5 tokens
	DDLCost      int // Default: 10 tokens

	// Rate limiter cleanup
	InactivityTimeout time.Duration // Default: 5 minutes

	// Role-based timeouts (Layer 4)
	AdminTimeout                 time.Duration // Default: 10 minutes
	AuthenticatedQueryTimeout    time.Duration // Default: 30 seconds
	AuthenticatedMutationTimeout time.Duration // Default: 60 seconds
	AnonymousQueryTimeout        time.Duration // Default: 5 seconds
	AnonymousMutationTimeout     time.Duration // Default: 10 seconds
	TimeoutWarningThreshold      float64       // Default: 0.8 (80%)

	// Monitoring configuration (Layer 5)
	MaxMetricsRetained       int           // Default: 100,000
	MaxMemoryMB              int           // Default: 250 MB
	MetricsPurgeInterval     time.Duration // Default: 5 minutes
	MetricsRetentionDuration time.Duration // Default: 30 minutes
	ExpensiveQueryThreshold  float64       // Default: 0.7 (70% of limit)
	ExpensiveQueryDuration   time.Duration // Default: 1 second
	AbuseWarningThreshold    int           // Default: 10 expensive queries in 5min
	AbuseErrorThreshold      int           // Default: 20 expensive queries in 5min
}

// DefaultGraphQLSecurityConfig returns the default security configuration
func DefaultGraphQLSecurityConfig() *GraphQLSecurityConfig {
	return &GraphQLSecurityConfig{
		// Layer flags (default all enabled)
		EnableComplexityLimit:  true,
		EnableDepthLimit:       true,
		EnableGraphQLRateLimit: true,
		EnableQueryTimeout:     true,
		EnableQueryMonitoring:  true,
		RateAlgorithm:          "token-bucket",

		// Complexity limits
		AdminComplexityLimit:         0, // Unlimited
		AuthenticatedComplexityLimit: 200,
		AnonymousComplexityLimit:     50,
		DepthLimit:                   7,

		// Query rate limits
		AdminQueryRateLimit:         0,   // Unlimited
		AuthenticatedQueryRateLimit: 100, // 100 queries per minute
		AnonymousQueryRateLimit:     20,  // 20 queries per minute

		// Mutation rate limits
		AdminMutationRateLimit:         0,  // Unlimited
		AuthenticatedMutationRateLimit: 10, // 10 mutations per minute
		AnonymousMutationRateLimit:     2,  // 2 mutations per minute

		// Burst multipliers for token bucket
		AuthenticatedBurstMultiplier: 2.0, // 200 token capacity for 100/min rate
		AnonymousBurstMultiplier:     1.5, // 30 token capacity for 20/min rate

		// Operation costs
		QueryCost:    1,
		MutationCost: 5,
		DDLCost:      10,

		// Rate limiter cleanup
		InactivityTimeout: 5 * time.Minute,

		// Timeouts
		AdminTimeout:                 10 * time.Minute,
		AuthenticatedQueryTimeout:    30 * time.Second,
		AuthenticatedMutationTimeout: 60 * time.Second,
		AnonymousQueryTimeout:        5 * time.Second,
		AnonymousMutationTimeout:     10 * time.Second,
		TimeoutWarningThreshold:      0.8, // Warn at 80% of timeout

		// Monitoring
		MaxMetricsRetained:       100000,
		MaxMemoryMB:              250,
		MetricsPurgeInterval:     5 * time.Minute,
		MetricsRetentionDuration: 30 * time.Minute,
		ExpensiveQueryThreshold:  0.7, // 70% of user's limit
		ExpensiveQueryDuration:   1 * time.Second,
		AbuseWarningThreshold:    10,
		AbuseErrorThreshold:      20,
	}
}

// BuildGraphQLSecurityConfig creates a GraphQLSecurityConfig from Arguments
func BuildGraphQLSecurityConfig(args *Arguments) *GraphQLSecurityConfig {
	config := DefaultGraphQLSecurityConfig()

	// Override with CLI flags
	config.EnableComplexityLimit = args.EnableComplexityLimit
	config.EnableDepthLimit = args.EnableDepthLimit
	config.EnableGraphQLRateLimit = args.EnableGraphQLRateLimit
	config.EnableQueryTimeout = args.EnableQueryTimeout
	config.EnableQueryMonitoring = args.EnableQueryMonitoring
	config.RateAlgorithm = args.GraphQLRateAlgorithm

	return config
}

// GetQueryTimeout returns the appropriate query timeout based on admin status
// Returns 0 if query timeout is disabled
// TODO: I can extend this to support role-based timeouts for different user tiers beyond just admin/non-admin
func (a *Arguments) GetQueryTimeout(isAdmin bool) time.Duration {
	if !a.EnableQueryTimeout {
		return 0 // No timeout
	}

	if isAdmin {
		return time.Duration(a.AdminQueryTimeoutSeconds) * time.Second
	}

	return time.Duration(a.QueryTimeoutSeconds) * time.Second
}

// GetQueryMemoryLimit returns the appropriate per-query memory limit in bytes based on admin status
// This is a server-wide limit applied to all SELECT queries across all databases
// Returns memory limit in bytes (converted from MB configuration)
// TODO: I could add support for admin users to override this limit via a query-level flag (e.g., ALLOW_UNLIMITED_MEMORY)
// TODO: I could implement dynamic limit adjustment based on available system memory (e.g., query limit = 10% of free RAM)
func (a *Arguments) GetQueryMemoryLimit(isAdmin bool) int64 {
	limitMB := a.QueryMaxMemoryMB
	if isAdmin {
		limitMB = a.AdminQueryMaxMemoryMB
	}
	return int64(limitMB) * constants.MemoryMBToBytes // Convert MB to bytes
}

// GetJoinMemoryLimit returns the memory limit for JOIN operations in bytes
// This is a PostgreSQL-style work_mem equivalent for hash join hash tables and merge join buffers
// Returns memory limit in bytes (converted from MB configuration)
func (a *Arguments) GetJoinMemoryLimit() int64 {
	return int64(a.JoinMemoryLimitMB) * constants.MemoryMBToBytes
}

// GetTransactionIdleTimeout returns the parsed transaction idle timeout duration
// Returns the configured timeout or 5 minutes as default if invalid
func (a *Arguments) GetTransactionIdleTimeout() time.Duration {
	if a.TransactionIdleTimeout == "" {
		return 5 * time.Minute
	}

	duration, err := time.ParseDuration(a.TransactionIdleTimeout)
	if err != nil || duration <= 0 {
		return 5 * time.Minute // Fallback to default
	}

	return duration
}

// ValidateSettings performs comprehensive validation of all configuration settings
// This function is called at server startup to ensure all settings are valid before
// the server starts accepting connections. It validates numeric ranges, string formats,
// and logical consistency across all configuration parameters.
//
// Returns a detailed error message listing all invalid settings if validation fails,
// or nil if all settings are valid.
//
// Design: Fail-hard on invalid configuration to prevent silent misconfigurations in production
func (a *Arguments) ValidateSettings() error {
	var errors []string

	// Validate DROP BUNDLE RESTRICT settings
	if a.RestrictValidationSampleSize < 1 || a.RestrictValidationSampleSize > 100000 {
		errors = append(errors, fmt.Sprintf(
			"RestrictValidationSampleSize must be between 1 and 100,000 (got: %d). "+
				"This setting controls sampling size for DROP BUNDLE validation. Recommended value: 1000 for balanced performance.",
			a.RestrictValidationSampleSize))
	}

	// Validate port numbers
	if a.Port < 1 || a.Port > 65535 {
		errors = append(errors, fmt.Sprintf(
			"Port must be between 1 and 65535 (got: %d)", a.Port))
	}

	// Validate session timeout
	if a.SessionTimeoutMinutes < 1 {
		errors = append(errors, fmt.Sprintf(
			"SessionTimeoutMinutes must be positive (got: %d)", a.SessionTimeoutMinutes))
	}

	// Validate connection pool settings
	if a.MaxConnections < 1 {
		errors = append(errors, fmt.Sprintf(
			"MaxConnections must be positive (got: %d)", a.MaxConnections))
	}

	if a.ConnectionIdleTimeoutMinutes < 1 {
		errors = append(errors, fmt.Sprintf(
			"ConnectionIdleTimeoutMinutes must be positive (got: %d)", a.ConnectionIdleTimeoutMinutes))
	}

	// Validate Transaction settings
	if a.TransactionIdleTimeout != "" {
		if duration, err := time.ParseDuration(a.TransactionIdleTimeout); err != nil || duration <= 0 {
			// Invalid timeout - append warning and use default
			errors = append(errors, fmt.Sprintf(
				"WARNING: Invalid transaction_idle_timeout '%s' (must be positive duration like '5m', '30s'). Using default: 5m",
				a.TransactionIdleTimeout))
			a.TransactionIdleTimeout = "5m"
		}
	} else {
		// Empty timeout - use default
		a.TransactionIdleTimeout = "5m"
	}

	// Validate DefaultTransactionIsolation
	if a.DefaultTransactionIsolation != "" {
		switch a.DefaultTransactionIsolation {
		case "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE":
			// Valid
		default:
			errors = append(errors, fmt.Sprintf(
				"WARNING: Invalid default_transaction_isolation '%s'. Valid values: 'READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE'. Using default: REPEATABLE READ",
				a.DefaultTransactionIsolation))
			a.DefaultTransactionIsolation = "REPEATABLE READ"
		}
	}

	// Validate WAL settings
	if a.WALMode != "sync" && a.WALMode != "async" {
		errors = append(errors, fmt.Sprintf(
			"WALMode must be 'sync' or 'async' (got: '%s')", a.WALMode))
	}

	if a.AsyncWALWorkers < 1 {
		errors = append(errors, fmt.Sprintf(
			"AsyncWALWorkers must be positive (got: %d)", a.AsyncWALWorkers))
	}

	if a.AsyncWALQueueSize < 1 {
		errors = append(errors, fmt.Sprintf(
			"AsyncWALQueueSize must be positive (got: %d)", a.AsyncWALQueueSize))
	}

	// Validate metadata settings
	if a.MetadataBatchSize < 1 {
		errors = append(errors, fmt.Sprintf(
			"MetadataBatchSize must be positive (got: %d)", a.MetadataBatchSize))
	}

	if a.MetadataPersistInterval < 1 {
		errors = append(errors, fmt.Sprintf(
			"MetadataPersistInterval must be positive (got: %d)", a.MetadataPersistInterval))
	}

	// Validate B-Tree settings
	if a.BTreeSyncMode != "immediate" && a.BTreeSyncMode != "batched" && a.BTreeSyncMode != "scheduled" {
		errors = append(errors, fmt.Sprintf(
			"BTreeSyncMode must be 'immediate', 'batched', or 'scheduled' (got: '%s')", a.BTreeSyncMode))
	}

	if a.BTreeBatchSize < 1 {
		errors = append(errors, fmt.Sprintf(
			"BTreeBatchSize must be positive (got: %d)", a.BTreeBatchSize))
	}

	// Validate sorting settings
	if a.SortTopNThreshold < 0 || a.SortTopNThreshold > 1.0 {
		errors = append(errors, fmt.Sprintf(
			"SortTopNThreshold must be between 0.0 and 1.0 (got: %.2f)", a.SortTopNThreshold))
	}

	if a.SortRadixLimitRatio < 0 || a.SortRadixLimitRatio > 1.0 {
		errors = append(errors, fmt.Sprintf(
			"SortRadixLimitRatio must be between 0.0 and 1.0 (got: %.2f)", a.SortRadixLimitRatio))
	}

	// Validate query timeout settings
	if a.QueryTimeoutSeconds < 1 {
		errors = append(errors, fmt.Sprintf(
			"QueryTimeoutSeconds must be positive (got: %d)", a.QueryTimeoutSeconds))
	}

	if a.AdminQueryTimeoutSeconds < 1 {
		errors = append(errors, fmt.Sprintf(
			"AdminQueryTimeoutSeconds must be positive (got: %d)", a.AdminQueryTimeoutSeconds))
	}

	// Validate memory limits
	if a.QueryMaxMemoryMB < 1 {
		errors = append(errors, fmt.Sprintf(
			"QueryMaxMemoryMB must be positive (got: %d)", a.QueryMaxMemoryMB))
	}

	if a.AdminQueryMaxMemoryMB < 1 {
		errors = append(errors, fmt.Sprintf(
			"AdminQueryMaxMemoryMB must be positive (got: %d)", a.AdminQueryMaxMemoryMB))
	}

	// Validate subquery settings
	if a.SubqueryMaxDepth < 1 || a.SubqueryMaxDepth > 10 {
		errors = append(errors, fmt.Sprintf(
			"SubqueryMaxDepth must be between 1 and 10 (got: %d)", a.SubqueryMaxDepth))
	}

	// Validate statistics settings
	if a.StatsAutoAnalyzeRatio < 0 || a.StatsAutoAnalyzeRatio > 1.0 {
		errors = append(errors, fmt.Sprintf(
			"StatsAutoAnalyzeRatio must be between 0.0 and 1.0 (got: %.2f)", a.StatsAutoAnalyzeRatio))
	}

	// If any errors were found, return them all as a single formatted error
	if len(errors) > 0 {
		return fmt.Errorf("FATAL: Invalid configuration detected - Server cannot start with invalid settings:\n\n%s",
			strings.Join(errors, "\n"))
	}

	return nil
}

// ResetSettingsForTesting resets the global settings singleton for testing purposes
// This should ONLY be called from test code to ensure test isolation
// WARNING: Not safe for concurrent use - tests must not run in parallel when using this
func ResetSettingsForTesting() {
	mu.Lock()
	defer mu.Unlock()
	instance = nil
	once = sync.Once{}
}
