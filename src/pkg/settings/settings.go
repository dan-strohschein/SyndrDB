package settings

import (
	"sync"
	"time"
)

type Arguments struct {
	DataDir    string
	LogDir     string
	LogFile    string
	TempDir    string // Temporary directory for intermediate files/indexes/sorts
	ConfigFile string

	CreateDefaultDB bool // Create default database if it doesn't exist
	PrintToScreen   bool // Print to screen

	Debug     bool // Debug mode
	UserDebug bool // User debug mode

	LogLevel string // Log level: debug, info, warn, error

	// The mode of operation
	// standalone, cluster
	Mode string

	// the host name or IP address to listen on
	Host string

	// Add to Journal struct
	MaxJournalFileSize int64

	BundleBufferSize int // Size of the buffer for bundle reads

	// the port number to listen on
	Port int

	// Strongly verbose logging
	Verbose bool

	AuthEnabled bool // Enable authentication

	// Session management configuration
	SessionTimeoutMinutes int // Session timeout in minutes
	MaxSessions           int // Maximum number of concurrent sessions

	// TLS/SSL configuration
	TLSEnabled            bool   // Enable TLS/SSL
	TLSCertFile           string // Path to TLS certificate file
	TLSKeyFile            string // Path to TLS private key file
	TLSGenerateSelfSigned bool   // Generate self-signed certificate if none exists
	TLSRequireClientCert  bool   // Require client certificates
	TLSCAFile             string // Path to CA file for client certificate validation

	Version string // Show version information

	EnableGraphQL bool // Enable GraphQL API

	// Bundle storage format configuration
	BundleStorageFormat string // Storage format: "json" or "binary" (default: "json")

	// PHASE 1 PERFORMANCE OPTIMIZATIONS
	// WAL Configuration for bulk operations
	WALEnabled           bool // Enable/disable WAL globally
	WALBulkModeThreshold int  // Operations per second threshold for bulk mode
	WALDisableForBulkOps bool // Disable WAL during bulk operations

	// PHASE 2 ASYNC WAL CONFIGURATION
	WALMode           string // WAL mode: "sync" or "async" (default: "sync")
	AsyncWALWorkers   int    // Number of async WAL workers (default: 2)
	AsyncWALQueueSize int    // Async WAL queue size (default: 1000)

	// Metadata Update Performance Settings
	MetadataBatchSize       int // Documents before metadata flush (default: 50 → 500)
	MetadataPersistInterval int // Documents before disk persistence (default: 1000)
	MetadataFlushInterval   int // Time in seconds between forced flushes

	// Performance Mode Detection
	BulkOperationDetection bool // Auto-detect bulk operations for optimization

	// Hash Index Configuration
	IndexSequenceSafetyMargin int // Safety margin for sequence recovery (default: 100)

	// Parser Configuration
	UseNewParser bool // Use new SyndrQL parser instead of legacy parser (default: false)

	// PHASE 4 SORTING OPTIMIZATION CONFIGURATION
	// Top-N Heapsort Configuration
	SortTopNThreshold       float64 // Ratio of LIMIT/total_rows for Top-N activation (default: 0.1)
	SortTopNMinSize         int     // Minimum dataset size for Top-N optimization (default: 100)
	SortHeapInitialCapacity int     // Initial heap capacity for Top-N queries (default: 1000)

	// Radix Sort Configuration
	SortRadixMinSize    int     // Minimum dataset size for radix sort (default: 1000)
	SortRadixLimitRatio float64 // Minimum LIMIT/total_rows ratio for radix (default: 0.5)
	SortRadixMaxPasses  int     // Maximum radix sort passes for wide integers (default: 8)

	// SIMD String Sort Configuration
	SortSIMDEnabled     bool // Enable SIMD string sorting optimization (default: true)
	SortSIMDAbbrevBytes int  // Bytes used for abbreviated string keys (default: 8)
	SortSIMDMinSize     int  // Minimum dataset size for SIMD activation (default: 100)

	// JOIN SIMD Configuration
	JoinSIMDEnabled    bool // Enable SIMD acceleration for JOIN operations (default: auto-detect)
	JoinSIMDAutoDetect bool // Auto-detect CPU SIMD support (default: true)

	// WHERE SIMD Configuration
	WhereSIMDEnabled    bool // Enable SIMD acceleration for WHERE clause comparisons (default: true)
	WhereSIMDAutoDetect bool // Auto-detect CPU SIMD support for WHERE clauses (default: true)

	// WHERE Bloom Filter Configuration
	WhereBloomEnabled      bool // Enable Bloom filter pre-filtering for multi-condition WHERE clauses (default: true)
	WhereBloomMinDocuments int  // Minimum document count to activate Bloom filtering (default: 500)

	// WHERE Batch/Columnar SIMD Configuration (Priority 3)
	WhereBatchSIMDEnabled bool // Enable batch/columnar SIMD processing for WHERE clauses (default: true)
	WhereBatchMinSize     int  // Minimum document count for batch SIMD processing (default: 100)

	// WHERE Expression Caching Configuration (Priority 4)
	WhereExpressionCacheEnabled bool // Enable expression caching and predicate reordering (default: true)
	WhereExpressionCacheSize    int  // LRU cache size for compiled expressions (default: 1000)

	// Parallel Sort Configuration (Phase 5)
	SortEnableParallel    bool // DEPRECATED: Use SortParallelEnabled instead
	SortParallelThreshold int  // DEPRECATED: Use SortParallelMinSize instead
	SortParallelEnabled   bool // Enable parallel sorting for large datasets (default: true)
	SortParallelMinSize   int  // Minimum dataset size for parallel sort (default: 10000)
	SortMaxMemoryMB       int  // Maximum memory in MB for sorting operations (default: 512)

	// Backup & Restore Configuration
	BackupDir            string // Directory for backup files (default: "./backups")
	BackupCompression    string // Compression format: "gzip", "zstd", "none" (default: "gzip")
	BackupIncludeIndexes bool   // Include index files in backups (default: true)
	// TODO: I will add BackupRetentionDays for automatic backup cleanup
	// TODO: I will add BackupEncryption settings for encrypted backups
	// TODO: I will add BackupCloudProvider for S3/GCS/Azure integration

	// Migration System Configuration
	MaxMigrationCommands          int     // Maximum commands per migration (default: 1000)
	MigrationPerformanceThreshold float64 // Performance warning threshold in seconds (default: 1.0)
	MaxValidationReportSize       int64   // Maximum validation report size in bytes (default: 10MB)
	ValidationReportRetentionDays int     // Days to retain validation reports (default: 30)
	EnableAutoReverse             bool    // Enable automatic reverse command generation (default: true)
	RequireExplicitDownCommands   bool    // Require explicit DOWN commands in migrations (default: false)
	MigrationTimeoutSeconds       int     // Timeout for migration operations in seconds (default: 300)

	// GraphQL Security Configuration (Layers 1-5)
	EnableComplexityLimit  bool   // Enable query complexity analysis (Layer 1, default: true)
	EnableDepthLimit       bool   // Enable query depth limiting (Layer 2, default: true)
	EnableGraphQLRateLimit bool   // Enable per-user rate limiting (Layer 3, default: true)
	EnableQueryTimeout     bool   // Enable query execution timeout (Layer 4, default: true)
	EnableQueryMonitoring  bool   // Enable query metrics monitoring (Layer 5, default: true)
	GraphQLRateAlgorithm   string // Rate limiting algorithm: "token-bucket" or "time-bucket" (default: "token-bucket")
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
