package settings

import (
	"sync"
	"time"
)

// TODO: I will add support for environment variable overrides with ENV > CLI > YAML > defaults precedence
// TODO: I will implement hot-reload functionality via SIGHUP signal to reload configuration without server restart

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

	// Authentication Configuration
	AuthEnabled bool `yaml:"auth_enabled"` // Enable authentication

	// Session Management Configuration
	SessionTimeoutMinutes int `yaml:"session_timeout_minutes"` // Session timeout in minutes
	MaxSessions           int `yaml:"max_sessions"`            // Maximum number of concurrent sessions

	// Connection Pool Management
	MaxConnections               int `yaml:"max_connections"`         // Maximum connection pool size (default: 100)
	ConnectionIdleTimeoutMinutes int `yaml:"connection_idle_timeout"` // Connection idle timeout in minutes (default: 30)

	// TLS/SSL Configuration
	TLSEnabled            bool   `yaml:"tls_enabled"`              // Enable TLS/SSL
	TLSCertFile           string `yaml:"tls_cert_file"`            // Path to TLS certificate file
	TLSKeyFile            string `yaml:"tls_key_file"`             // Path to TLS private key file
	TLSGenerateSelfSigned bool   `yaml:"tls_generate_self_signed"` // Generate self-signed certificate if none exists
	TLSRequireClientCert  bool   `yaml:"tls_require_client_cert"`  // Require client certificates
	TLSCAFile             string `yaml:"tls_ca_file"`              // Path to CA file for client certificate validation

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

	// Metadata Update Performance Settings
	MetadataBatchSize       int  `yaml:"metadata_batch_size"`       // Documents before metadata flush (default: 500)
	MetadataPersistInterval int  `yaml:"metadata_persist_interval"` // Documents before disk persistence (default: 1000)
	MetadataFlushInterval   int  `yaml:"metadata_flush_interval"`   // Time in seconds between forced flushes
	BulkOperationDetection  bool `yaml:"bulk_operation_detection"`  // Auto-detect bulk operations for optimization

	// Hash Index Configuration
	IndexSequenceSafetyMargin int `yaml:"index_sequence_safety_margin"` // Safety margin for sequence recovery (default: 100)

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

	// WHERE SIMD Configuration
	WhereSIMDEnabled            bool `yaml:"where_simd_enabled"`             // Enable SIMD acceleration for WHERE clause comparisons (default: true)
	WhereSIMDAutoDetect         bool `yaml:"where_simd_auto_detect"`         // Auto-detect CPU SIMD support for WHERE clauses (default: true)
	WhereBloomEnabled           bool `yaml:"where_bloom_enabled"`            // Enable Bloom filter pre-filtering for multi-condition WHERE clauses (default: true)
	WhereBloomMinDocuments      int  `yaml:"where_bloom_min_documents"`      // Minimum document count to activate Bloom filtering (default: 500)
	WhereBatchSIMDEnabled       bool `yaml:"where_batch_simd_enabled"`       // Enable batch/columnar SIMD processing for WHERE clauses (default: true)
	WhereBatchMinSize           int  `yaml:"where_batch_min_size"`           // Minimum document count for batch SIMD processing (default: 100)
	WhereExpressionCacheEnabled bool `yaml:"where_expression_cache_enabled"` // Enable expression caching and predicate reordering (default: true)
	WhereExpressionCacheSize    int  `yaml:"where_expression_cache_size"`    // LRU cache size for compiled expressions (default: 1000)

	// Backup & Restore Configuration
	BackupDir            string `yaml:"backup_dir"`             // Directory for backup files (default: "./backups")
	BackupCompression    string `yaml:"backup_compression"`     // Compression format: "gzip", "zstd", "none" (default: "gzip")
	BackupIncludeIndexes bool   `yaml:"backup_include_indexes"` // Include index files in backups (default: true)
	// TODO: I will add BackupRetentionDays for automatic backup cleanup
	// TODO: I will add BackupEncryption settings for encrypted backups
	// TODO: I will add BackupCloudProvider for S3/GCS/Azure integration

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

	// Ghost Cleanup (Automatic Compaction) Configuration
	GhostCleanupEnabled         bool    `yaml:"ghost_cleanup_enabled"`          // Enable automatic ghost record cleanup (default: true)
	GhostCleanupIntervalSeconds int     `yaml:"ghost_cleanup_interval_seconds"` // Cleanup cycle interval in seconds (default: 10)
	GhostCleanupBatchSize       int     `yaml:"ghost_cleanup_batch_size"`       // Batch size for processing (default: 10000)
	GhostCleanupPauseThreshold  int     `yaml:"ghost_cleanup_pause_threshold"`  // Active query pause threshold (default: 500)
	GhostCleanupTombstoneRatio  float64 `yaml:"ghost_cleanup_tombstone_ratio"`  // Tombstone ratio threshold for compaction (default: 0.3)

	// Query Plan Cache Configuration
	PlanCacheEnabled           bool `yaml:"plan_cache_enabled"`             // Enable query plan caching (default: true)
	PlanCacheCapacity          int  `yaml:"plan_cache_capacity"`            // Maximum cached plans per shard (default: 1000, 8 shards = 8000 total)
	PlanCacheAdaptivePlanning  bool `yaml:"plan_cache_adaptive_planning"`   // Enable PostgreSQL-style adaptive planning (default: true)
	PlanCacheCustomThreshold   int  `yaml:"plan_cache_custom_threshold"`    // Number of custom executions before generic plan evaluation (default: 5)
	PlanCacheWriteThreshold    int  `yaml:"plan_cache_write_threshold"`     // Write count threshold for invalidation (default: 1000)
	PlanCacheStaleServeSeconds int  `yaml:"plan_cache_stale_serve_seconds"` // Stale plan serving window in seconds (default: 60)
	// TODO: I will add PlanCacheMemoryLimitMB for memory-aware eviction when memory profiling reveals actual cache memory usage
	// TODO: I will add PlanCacheCompressionEnabled for large plan compression when we have plans exceeding 10KB

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
			AuthEnabled:         false,
			CreateDefaultDB:     true,
			Version:             "0.1.0",
			BundleStorageFormat: "binary", // Binary (BSON) format is the only supported format

			ExportRealtimeMetrics: false, // Export real-time metrics for monitoring

			// PHASE 1 PERFORMANCE DEFAULTS
			WALEnabled:           true, // WAL enabled by default
			WALBulkModeThreshold: 50,   // >50 ops/sec = bulk mode
			WALDisableForBulkOps: true, // Disable WAL during bulk operations

			// PHASE 2 ASYNC WAL DEFAULTS
			WALMode:           "sync", // Start with sync mode for safety
			AsyncWALWorkers:   2,      // 2 workers for async WAL
			AsyncWALQueueSize: 1000,   // 1000 operation queue

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
			WhereExpressionCacheEnabled: true,  // Enable expression caching and predicate reordering
			WhereExpressionCacheSize:    1000,  // Cache 1000 compiled expressions
			SortEnableParallel:          false, // DEPRECATED: use SortParallelEnabled
			SortParallelThreshold:       10000, // DEPRECATED: use SortParallelMinSize
			SortParallelEnabled:         true,  // Phase 5: Enable parallel sorting
			SortParallelMinSize:         10000, // Phase 5: 10k+ docs for parallel sort
			SortMaxMemoryMB:             512,   // 512MB memory limit

			// Backup & Restore Defaults
			BackupDir:            "./backups", // Default backup directory
			BackupCompression:    "gzip",      // Use gzip compression by default
			BackupIncludeIndexes: true,        // Include indexes in backups

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

			// Ghost Cleanup (Automatic Compaction) Defaults
			GhostCleanupEnabled:         true,  // Enable ghost cleanup by default
			GhostCleanupIntervalSeconds: 10,    // Run every 10 seconds (SQL Server pattern)
			GhostCleanupBatchSize:       10000, // Process up to 10,000 records per cycle
			GhostCleanupPauseThreshold:  500,   // Pause when 500+ queries active
			GhostCleanupTombstoneRatio:  0.3,   // Compact when 30% tombstones

			// Query Plan Cache Defaults
			PlanCacheEnabled:           true, // Enable plan caching by default
			PlanCacheCapacity:          1000, // 1000 plans per shard (8000 total)
			PlanCacheAdaptivePlanning:  true, // Enable adaptive planning
			PlanCacheCustomThreshold:   5,    // 5 custom executions before generic plan
			PlanCacheWriteThreshold:    1000, // Invalidate after 1000 writes
			PlanCacheStaleServeSeconds: 60,   // 60 second stale serving window

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
	return int64(limitMB) * 1024 * 1024 // Convert MB to bytes
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
