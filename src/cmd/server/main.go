package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof" // pprof for memory/CPU profiling at :6060
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/graphQL"
	"syndrdb/src/internal/graphQL/schema"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/fatal"
	"syndrdb/src/pkg/settings"

	"syndrdb/src/internal/monitoring"
	"syscall"
	"time"
)

// resolvePprofDir returns an absolute directory for pprof output. Tries TempDir, DataDir, cwd, then /tmp (Unix).
// Ensures the chosen directory exists (MkdirAll for TempDir/DataDir). Used so profile files can be found even
// when run in containers or under service managers where cwd/signals may differ.
func resolvePprofDir(tempDir, dataDir string) string {
	try := func(d string, mkdir bool) string {
		if d == "" {
			return ""
		}
		abs, err := filepath.Abs(d)
		if err != nil {
			return ""
		}
		if mkdir {
			if err := os.MkdirAll(abs, 0755); err != nil {
				return ""
			}
		}
		return abs
	}
	if p := try(tempDir, true); p != "" {
		return p
	}
	if p := try(dataDir, true); p != "" {
		return p
	}
	if wd, err := os.Getwd(); err == nil && wd != "" {
		return wd
	}
	if runtime.GOOS != "windows" {
		return "/tmp"
	}
	return "."
}

// writeHeapProfileToFile runs GC then writes a heap profile to dir/pprof_heap_<timestamp>.prof.
// Returns an absolute path. Use when memory balloons: kill -USR1 <pid> or GET /debug/write-heap
//
//	go tool pprof -alloc_space pprof_heap_<ts>.prof
//	(pprof) top20
func writeHeapProfileToFile(dir string) (path string, err error) {
	if dir == "" {
		dir = "."
	}
	path = filepath.Join(dir, fmt.Sprintf("pprof_heap_%s.prof", time.Now().Format("2006-01-02_15-04-05")))
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		return path, err
	}
	if abs, e := filepath.Abs(path); e == nil {
		path = abs
	}
	return path, nil
}

// writeGoroutineProfileToFile writes a goroutine profile to dir/pprof_goroutines_<timestamp>.txt.
// Returns an absolute path. Use for goroutine leaks: kill -USR2 <pid> or GET /debug/write-goroutines
func writeGoroutineProfileToFile(dir string) (path string, err error) {
	if dir == "" {
		dir = "."
	}
	path = filepath.Join(dir, fmt.Sprintf("pprof_goroutines_%s.txt", time.Now().Format("2006-01-02_15-04-05")))
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	p := pprof.Lookup("goroutine")
	if p == nil {
		return path, fmt.Errorf("pprof.Lookup(\"goroutine\") is nil")
	}
	if err := p.WriteTo(f, 2); err != nil {
		return path, err
	}
	if abs, e := filepath.Abs(path); e == nil {
		path = abs
	}
	return path, nil
}

// printUsage prints helpful usage information
func printUsage() {
	log.Println("SyndrDB - A relational document database by Dan Strohschein")
	log.Println("\nUsage:")
	log.Println("  syndrdb [options]")
	log.Println("\nOptions:")
	flag.PrintDefaults()

	log.Println("\nExamples:")
	log.Println("  syndrdb --datadir=/data")
	log.Println("  syndrdb --port=1776 --logfile=syndrdb.log")
}

func main() {
	// Create a new settings.Arguments instance
	// Get the global settings instance
	args := settings.GetSettings()

	// Capture panics to fatal_errors.log before the process exits (main goroutine).
	// LogFatalAndExit writes the log, prints only the panicking stack to stderr, then os.Exit(1)
	// so the runtime never prints the full goroutine dump.
	defer func() {
		if r := recover(); r != nil {
			fatal.LogFatalAndExit(r)
		}
	}()

	//args := settings.Arguments{}

	// Define command line flags that map to the Arguments struct
	flag.StringVar(&args.DataDir, "datadir", "./data_files", "Directory to store data files")
	flag.StringVar(&args.LogDir, "logdir", "./log_files", "Directory to store log files (default: stdout)")
	flag.StringVar(&args.TempDir, "tempdir", "./temp_files", "Temporary directory for intermediate files/indexes/sorts")
	flag.Int64Var(&args.MaxJournalFileSize, "maxjournalfilesize", 1000000, "Maximum size of journal files in bytes (default: 1MB)")
	flag.StringVar(&args.Host, "host", "127.0.0.1", "Host name or IP address to listen on")
	flag.IntVar(&args.Port, "port", 1776, "Port for the HTTP server")
	flag.BoolVar(&args.Verbose, "verbose", true, "Enable verbose logging")
	flag.StringVar(&args.ConfigFile, "config", "", "Path to YAML config file (default: ./syndrdb.yml if exists)")
	flag.StringVar(&args.Mode, "mode", "standalone", "Operation mode (standalone, cluster)")
	flag.BoolVar(&args.AuthEnabled, "auth", false, "Enable authentication")
	flag.IntVar(&args.SessionTimeoutMinutes, "session-timeout", 30, "Session timeout in minutes")
	flag.IntVar(&args.MaxSessions, "max-sessions", 1000, "Maximum number of concurrent sessions")
	flag.IntVar(&args.MaxConnections, "max-connections", 100, "Maximum connection pool size")
	flag.IntVar(&args.ConnectionIdleTimeoutMinutes, "connection-idle-timeout", 30, "Connection idle timeout in minutes")

	// HIGH-007: Concurrency & Locking Configuration flags
	flag.IntVar(&args.LockTimeoutSeconds, "lock-timeout", 30, "Timeout for lock acquisition in seconds (default: 30)")
	flag.IntVar(&args.MaxWorkerPools, "max-worker-pools", 10, "Maximum number of worker pools (default: 10)")
	flag.IntVar(&args.WorkerPoolStopTimeoutSeconds, "worker-pool-stop-timeout", 30, "Timeout for stopping worker pools in seconds (default: 30)")
	flag.StringVar(&args.Version, "version", "0.0.1alpha", "Shows version")
	flag.BoolVar(&args.PrintToScreen, "print", true, "Print Log Messages to screen")
	flag.BoolVar(&args.Debug, "debug", true, "Enable debug mode")
	flag.BoolVar(&args.UserDebug, "userdebug", false, "Enable user debug mode")
	flag.BoolVar(&args.EnableGraphQL, "graphql", false, "Enable GraphQL API")
	flag.StringVar(&args.LogLevel, "loglevel", "info", "Log level: debug, info, warn, error")
	flag.StringVar(&args.BundleStorageFormat, "bundle-format", "binary", "Bundle storage format (only 'binary' is supported)")

	flag.BoolVar(&args.ExportRealtimeMetrics, "export-RT-metrics", true, "Export Real Time Metrics for Monitoring")

	// PHASE 2 ASYNC WAL FLAGS
	flag.StringVar(&args.WALMode, "wal-mode", "sync", "WAL mode: sync or async")
	flag.IntVar(&args.AsyncWALWorkers, "async-wal-workers", 2, "Number of async WAL worker threads")
	flag.IntVar(&args.AsyncWALQueueSize, "async-wal-queue", 1000, "Async WAL queue size")

	// HASH INDEX FLAGS
	flag.IntVar(&args.IndexSequenceSafetyMargin, "index-seq-margin", 100, "Safety margin for index sequence recovery")

	// PARSER CONFIGURATION
	flag.BoolVar(&args.UseNewParser, "use-new-parser", true, "Use new SyndrQL parser (default: false, uses legacy parser)")

	// PHASE 4 SORTING OPTIMIZATION FLAGS
	// Top-N Heapsort flags
	flag.Float64Var(&args.SortTopNThreshold, "sort-topn-threshold", 0.1, "Top-N heapsort threshold: LIMIT/total ratio (0.01-0.5)")
	flag.IntVar(&args.SortTopNMinSize, "sort-topn-minsize", 100, "Minimum dataset size for Top-N optimization (10-10000)")
	flag.IntVar(&args.SortHeapInitialCapacity, "sort-heap-capacity", 1000, "Initial heap capacity for Top-N queries (10-100000)")

	// Radix Sort flags
	flag.IntVar(&args.SortRadixMinSize, "sort-radix-minsize", 1000, "Minimum dataset size for radix sort (100-100000)")
	flag.Float64Var(&args.SortRadixLimitRatio, "sort-radix-limitratio", 0.5, "Minimum LIMIT/total ratio for radix (0.1-1.0)")
	flag.IntVar(&args.SortRadixMaxPasses, "sort-radix-maxpasses", 8, "Maximum radix sort passes for wide integers (1-8)")

	// SIMD String Sort flags
	flag.BoolVar(&args.SortSIMDEnabled, "sort-simd-enabled", true, "Enable SIMD string sorting optimization")
	flag.IntVar(&args.SortSIMDAbbrevBytes, "sort-simd-abbrevbytes", 8, "Bytes for abbreviated string keys (4-16)")
	flag.IntVar(&args.SortSIMDMinSize, "sort-simd-minsize", 100, "Minimum dataset size for SIMD (10-10000)")

	// JOIN SIMD Configuration flags
	flag.BoolVar(&args.JoinSIMDEnabled, "join-simd-enabled", true, "Enable SIMD acceleration for JOIN hash/compare operations")
	flag.BoolVar(&args.JoinSIMDAutoDetect, "join-simd-autodetect", true, "Auto-detect CPU SIMD support (AVX2/NEON)")

	// WHERE SIMD Configuration flags
	flag.BoolVar(&args.WhereSIMDEnabled, "where-simd-enabled", true, "Enable SIMD acceleration for WHERE clause comparisons")
	flag.BoolVar(&args.WhereSIMDAutoDetect, "where-simd-autodetect", true, "Auto-detect CPU SIMD support for WHERE clauses")

	// WHERE Bloom Filter Configuration flags
	flag.BoolVar(&args.WhereBloomEnabled, "where-bloom-enabled", true, "Enable Bloom filter pre-filtering for multi-condition WHERE clauses")
	flag.IntVar(&args.WhereBloomMinDocuments, "where-bloom-min-docs", 500, "Minimum document count to activate Bloom filtering (100-100000)")

	// WHERE Batch/Columnar SIMD Configuration flags (Priority 3)
	flag.BoolVar(&args.WhereBatchSIMDEnabled, "where-batch-simd", true, "Enable batch/columnar SIMD processing for WHERE clauses")
	flag.IntVar(&args.WhereBatchMinSize, "where-batch-min-size", 100, "Minimum document count for batch SIMD processing (50-10000)")

	// Parallel Sort flags (Phase 5 - future)
	flag.BoolVar(&args.SortEnableParallel, "sort-parallel-enabled", false, "Enable parallel sorting (Phase 5)")
	flag.IntVar(&args.SortParallelThreshold, "sort-parallel-threshold", 10000, "Minimum size for parallel sort (1000-1000000)")
	flag.IntVar(&args.SortMaxMemoryMB, "sort-max-memory", 512, "Maximum sort memory in MB (10-10240)")

	// Migration System flags
	flag.IntVar(&args.MaxMigrationCommands, "max-migration-commands", 1000, "Maximum commands per migration (1-10000)")
	flag.Float64Var(&args.MigrationPerformanceThreshold, "migration-perf-threshold", 1.0, "Performance warning threshold in seconds (0.1-60.0)")
	flag.Int64Var(&args.MaxValidationReportSize, "max-validation-report-size", 10485760, "Maximum validation report size in bytes (1MB-100MB)")
	flag.IntVar(&args.ValidationReportRetentionDays, "validation-report-retention", 30, "Days to retain validation reports (1-365)")
	flag.BoolVar(&args.EnableAutoReverse, "enable-auto-reverse", true, "Enable automatic reverse command generation")
	flag.BoolVar(&args.RequireExplicitDownCommands, "require-explicit-down", false, "Require explicit DOWN commands in migrations")
	flag.IntVar(&args.MigrationTimeoutSeconds, "migration-timeout", 300, "Timeout for migration operations in seconds (10-3600)")

	// GraphQL Security Flags (Layers 1-5)
	flag.BoolVar(&args.EnableComplexityLimit, "enable-complexity-limit", true, "Enable GraphQL query complexity analysis (Layer 1)")
	flag.BoolVar(&args.EnableDepthLimit, "enable-depth-limit", true, "Enable GraphQL query depth limiting (Layer 2)")
	flag.BoolVar(&args.EnableGraphQLRateLimit, "enable-graphql-rate-limit", true, "Enable per-user GraphQL rate limiting (Layer 3)")
	flag.BoolVar(&args.EnableQueryTimeout, "enable-query-timeout", true, "Enable GraphQL query execution timeout (Layer 4)")
	flag.BoolVar(&args.EnableQueryMonitoring, "enable-query-monitoring", true, "Enable GraphQL query metrics monitoring (Layer 5)")
	flag.StringVar(&args.GraphQLRateAlgorithm, "graphql-rate-algorithm", "token-bucket", "GraphQL rate limiting algorithm: token-bucket or time-bucket")

	// SyndrQL Query Timeout Flags
	flag.IntVar(&args.QueryTimeoutSeconds, "query-timeout", 300, "Default query execution timeout in seconds (1-3600)")
	flag.IntVar(&args.AdminQueryTimeoutSeconds, "admin-query-timeout", 600, "Admin query execution timeout in seconds (60-3600)")

	// Per-Query Memory Limit Flags (DoS Protection)
	flag.IntVar(&args.QueryMaxMemoryMB, "query-max-memory", 25, "Maximum memory per query in MB (any positive integer)")
	flag.IntVar(&args.AdminQueryMaxMemoryMB, "admin-query-max-memory", 25, "Maximum memory per admin query in MB (any positive integer)")

	// STEP 1: Do an initial parse to check for --config flag
	flag.Parse()

	// STEP 2: Load YAML configuration if specified or if default exists
	configPath := args.ConfigFile
	configSource := "defaults and CLI flags only"

	// If no --config flag, check for default syndrdb.yml in current directory
	if configPath == "" {
		defaultConfigPath := "./syndrdb.yml"
		if _, err := os.Stat(defaultConfigPath); err == nil {
			configPath = defaultConfigPath
		}
	}

	// Load YAML config if a path was found
	if configPath != "" {
		yamlConfig, err := settings.LoadConfigFromYAML(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading configuration file: %v\n", err)
			os.Exit(1)
		}

		// Merge YAML values into current args (which has defaults)
		mergedArgs := settings.MergeYAMLIntoDefaults(args, yamlConfig)

		// Update args with merged values
		*args = *mergedArgs
		configSource = fmt.Sprintf("configuration file: %s", configPath)
	}

	// STEP 3: Re-parse command line flags to override YAML values
	// We need to reset and re-register flags with the merged values as defaults
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Re-register all flags with merged defaults
	flag.StringVar(&args.DataDir, "datadir", args.DataDir, "Directory to store data files")
	flag.StringVar(&args.LogDir, "logdir", args.LogDir, "Directory to store log files")
	flag.StringVar(&args.TempDir, "tempdir", args.TempDir, "Temporary directory for intermediate files")
	flag.StringVar(&args.ConfigFile, "config", configPath, "Path to YAML config file")
	flag.StringVar(&args.Host, "host", args.Host, "Host address to bind to")
	flag.IntVar(&args.Port, "port", args.Port, "Port number to listen on")
	flag.BoolVar(&args.Verbose, "verbose", args.Verbose, "Enable verbose logging")
	flag.BoolVar(&args.Debug, "debug", args.Debug, "Enable debug mode")
	flag.BoolVar(&args.UserDebug, "userdebug", args.UserDebug, "Enable user debug mode")
	flag.StringVar(&args.LogLevel, "loglevel", args.LogLevel, "Log level: debug, info, warn, error")
	flag.BoolVar(&args.PrintToScreen, "print", args.PrintToScreen, "Print logs to screen")
	flag.StringVar(&args.Mode, "mode", args.Mode, "Operation mode: standalone, cluster")
	flag.BoolVar(&args.AuthEnabled, "auth", args.AuthEnabled, "Enable authentication")
	flag.IntVar(&args.SessionTimeoutMinutes, "session-timeout", args.SessionTimeoutMinutes, "Session timeout in minutes")
	flag.IntVar(&args.MaxSessions, "max-sessions", args.MaxSessions, "Maximum concurrent sessions")
	flag.IntVar(&args.MaxConnections, "max-connections", args.MaxConnections, "Maximum connection pool size")
	flag.IntVar(&args.ConnectionIdleTimeoutMinutes, "connection-idle-timeout", args.ConnectionIdleTimeoutMinutes, "Connection idle timeout in minutes")
	// HIGH-007: Concurrency & Locking Configuration flags (re-register)
	flag.IntVar(&args.LockTimeoutSeconds, "lock-timeout", args.LockTimeoutSeconds, "Timeout for lock acquisition in seconds")
	flag.IntVar(&args.MaxWorkerPools, "max-worker-pools", args.MaxWorkerPools, "Maximum number of worker pools")
	flag.IntVar(&args.WorkerPoolStopTimeoutSeconds, "worker-pool-stop-timeout", args.WorkerPoolStopTimeoutSeconds, "Timeout for stopping worker pools in seconds")
	flag.StringVar(&args.Version, "version", args.Version, "Version information")
	flag.BoolVar(&args.EnableGraphQL, "graphql", args.EnableGraphQL, "Enable GraphQL API")
	flag.StringVar(&args.BundleStorageFormat, "bundle-format", args.BundleStorageFormat, "Bundle storage format")
	flag.Int64Var(&args.MaxJournalFileSize, "maxjournalfilesize", args.MaxJournalFileSize, "Maximum journal file size")
	flag.StringVar(&args.WALMode, "wal-mode", args.WALMode, "WAL mode: sync or async")
	flag.IntVar(&args.AsyncWALWorkers, "async-wal-workers", args.AsyncWALWorkers, "Number of async WAL workers")
	flag.IntVar(&args.AsyncWALQueueSize, "async-wal-queue", args.AsyncWALQueueSize, "Async WAL queue size")
	flag.IntVar(&args.IndexSequenceSafetyMargin, "index-seq-margin", args.IndexSequenceSafetyMargin, "Index sequence safety margin")
	flag.BoolVar(&args.UseNewParser, "use-new-parser", args.UseNewParser, "Use new SyndrQL parser")
	flag.Float64Var(&args.SortTopNThreshold, "sort-topn-threshold", args.SortTopNThreshold, "Top-N LIMIT/total ratio threshold")
	flag.IntVar(&args.SortTopNMinSize, "sort-topn-minsize", args.SortTopNMinSize, "Minimum dataset size for Top-N")
	flag.IntVar(&args.SortHeapInitialCapacity, "sort-heap-capacity", args.SortHeapInitialCapacity, "Initial heap capacity")
	flag.IntVar(&args.SortRadixMinSize, "sort-radix-minsize", args.SortRadixMinSize, "Minimum dataset size for radix sort")
	flag.Float64Var(&args.SortRadixLimitRatio, "sort-radix-limitratio", args.SortRadixLimitRatio, "Minimum LIMIT/total ratio for radix")
	flag.IntVar(&args.SortRadixMaxPasses, "sort-radix-maxpasses", args.SortRadixMaxPasses, "Maximum radix sort passes")
	flag.BoolVar(&args.SortSIMDEnabled, "sort-simd-enabled", args.SortSIMDEnabled, "Enable SIMD string sorting")
	flag.IntVar(&args.SortSIMDAbbrevBytes, "sort-simd-abbrevbytes", args.SortSIMDAbbrevBytes, "Abbreviated key bytes for SIMD")
	flag.IntVar(&args.SortSIMDMinSize, "sort-simd-minsize", args.SortSIMDMinSize, "Minimum size for SIMD sort")
	flag.BoolVar(&args.SortParallelEnabled, "sort-parallel-enabled", args.SortParallelEnabled, "Enable parallel sorting")
	flag.IntVar(&args.SortParallelMinSize, "sort-parallel-threshold", args.SortParallelMinSize, "Minimum size for parallel sort")
	flag.IntVar(&args.SortMaxMemoryMB, "sort-max-memory", args.SortMaxMemoryMB, "Maximum sort memory in MB")
	flag.BoolVar(&args.JoinSIMDEnabled, "join-simd-enabled", args.JoinSIMDEnabled, "Enable SIMD for JOINs")
	flag.BoolVar(&args.JoinSIMDAutoDetect, "join-simd-autodetect", args.JoinSIMDAutoDetect, "Auto-detect CPU SIMD for JOINs")
	flag.BoolVar(&args.WhereSIMDEnabled, "where-simd-enabled", args.WhereSIMDEnabled, "Enable SIMD for WHERE")
	flag.BoolVar(&args.WhereSIMDAutoDetect, "where-simd-autodetect", args.WhereSIMDAutoDetect, "Auto-detect CPU SIMD for WHERE")
	flag.BoolVar(&args.WhereBloomEnabled, "where-bloom-enabled", args.WhereBloomEnabled, "Enable Bloom filtering")
	flag.IntVar(&args.WhereBloomMinDocuments, "where-bloom-min-docs", args.WhereBloomMinDocuments, "Min docs for Bloom filter")
	flag.BoolVar(&args.WhereBatchSIMDEnabled, "where-batch-simd", args.WhereBatchSIMDEnabled, "Enable batch SIMD for WHERE")
	flag.IntVar(&args.WhereBatchMinSize, "where-batch-min-size", args.WhereBatchMinSize, "Min size for batch SIMD")
	flag.IntVar(&args.MaxMigrationCommands, "max-migration-commands", args.MaxMigrationCommands, "Max commands per migration")
	flag.Float64Var(&args.MigrationPerformanceThreshold, "migration-perf-threshold", args.MigrationPerformanceThreshold, "Performance warning threshold (seconds)")
	flag.Int64Var(&args.MaxValidationReportSize, "max-validation-report-size", args.MaxValidationReportSize, "Max validation report size")
	flag.IntVar(&args.ValidationReportRetentionDays, "validation-report-retention", args.ValidationReportRetentionDays, "Validation report retention (days)")
	flag.BoolVar(&args.EnableAutoReverse, "enable-auto-reverse", args.EnableAutoReverse, "Enable auto-reverse generation")
	flag.BoolVar(&args.RequireExplicitDownCommands, "require-explicit-down", args.RequireExplicitDownCommands, "Require explicit DOWN commands")
	flag.IntVar(&args.MigrationTimeoutSeconds, "migration-timeout", args.MigrationTimeoutSeconds, "Migration timeout (seconds)")
	flag.BoolVar(&args.EnableComplexityLimit, "enable-complexity-limit", args.EnableComplexityLimit, "Enable GraphQL query complexity analysis (Layer 1)")
	flag.BoolVar(&args.EnableDepthLimit, "enable-depth-limit", args.EnableDepthLimit, "Enable GraphQL query depth limiting (Layer 2)")
	flag.BoolVar(&args.EnableGraphQLRateLimit, "enable-graphql-rate-limit", args.EnableGraphQLRateLimit, "Enable per-user GraphQL rate limiting (Layer 3)")
	flag.BoolVar(&args.EnableQueryTimeout, "enable-query-timeout", args.EnableQueryTimeout, "Enable GraphQL query execution timeout (Layer 4)")
	flag.BoolVar(&args.EnableQueryMonitoring, "enable-query-monitoring", args.EnableQueryMonitoring, "Enable GraphQL query metrics monitoring (Layer 5)")
	flag.StringVar(&args.GraphQLRateAlgorithm, "graphql-rate-algorithm", args.GraphQLRateAlgorithm, "GraphQL rate limiting algorithm")

	// SyndrQL Query Timeout Flags (re-register)
	flag.IntVar(&args.QueryTimeoutSeconds, "query-timeout", args.QueryTimeoutSeconds, "Default query execution timeout in seconds (1-3600)")
	flag.IntVar(&args.AdminQueryTimeoutSeconds, "admin-query-timeout", args.AdminQueryTimeoutSeconds, "Admin query execution timeout in seconds (60-3600)")

	// Per-Query Memory Limit Flags (DoS Protection) - CRITICAL: These were missing!
	flag.IntVar(&args.QueryMaxMemoryMB, "query-max-memory", args.QueryMaxMemoryMB, "Maximum memory per query in MB (any positive integer)")
	flag.IntVar(&args.AdminQueryMaxMemoryMB, "admin-query-max-memory", args.AdminQueryMaxMemoryMB, "Maximum memory per admin query in MB (any positive integer)")

	// Page Cache Configuration (Memory Management)
	flag.IntVar(&args.MaxLoadedDocumentPages, "max-loaded-document-pages", args.MaxLoadedDocumentPages, "Max pages in shared documentPages cache (0 = use default 500)")
	flag.IntVar(&args.BundleAdapterMaxCachedPages, "bundle-adapter-max-cached-pages", args.BundleAdapterMaxCachedPages, "Max pages per BundleAdapter scanner (0 = use default 500)")
	flag.IntVar(&args.DocumentPageMapMaxEntriesPerBundle, "document-page-map-max-entries", args.DocumentPageMapMaxEntriesPerBundle, "Max documentID->pageID entries per bundle (0 = use default 100000)")
	flag.IntVar(&args.FileReadCacheMaxEntries, "file-read-cache-max-entries", args.FileReadCacheMaxEntries, "Max file/segment buffers in read cache (0 = use default 32)")

	// Query Plan Cache Configuration
	flag.BoolVar(&args.PlanCacheEnabled, "plan-cache-enabled", args.PlanCacheEnabled, "Enable query plan caching")
	flag.IntVar(&args.PlanCacheCapacity, "plan-cache-capacity", args.PlanCacheCapacity, "Maximum cached plans per shard (default: 1000, 8 shards = 8000 total)")
	flag.BoolVar(&args.PlanCacheAdaptivePlanning, "plan-cache-adaptive-planning", args.PlanCacheAdaptivePlanning, "Enable PostgreSQL-style adaptive planning")
	flag.IntVar(&args.PlanCacheCustomThreshold, "plan-cache-custom-threshold", args.PlanCacheCustomThreshold, "Custom executions before generic plan evaluation")
	flag.IntVar(&args.PlanCacheWriteThreshold, "plan-cache-write-threshold", args.PlanCacheWriteThreshold, "Write count threshold for invalidation")
	flag.IntVar(&args.PlanCacheStaleServeSeconds, "plan-cache-stale-serve-seconds", args.PlanCacheStaleServeSeconds, "Stale plan serving window in seconds")

	// WHERE Expression Cache Configuration
	flag.BoolVar(&args.WhereExpressionCacheEnabled, "where-expression-cache-enabled", args.WhereExpressionCacheEnabled, "Enable expression caching and predicate reordering")
	flag.IntVar(&args.WhereExpressionCacheSize, "where-expression-cache-size", args.WhereExpressionCacheSize, "LRU cache size for compiled expressions (default: 1000)")

	// GROUP BY Configuration
	flag.IntVar(&args.GroupByHashAggregateRowThreshold, "groupby-hash-aggregate-threshold", args.GroupByHashAggregateRowThreshold, "Rows below this use HashAggregate, above use Sort+GroupAggregate (default: 10000)")

	// JOIN Concurrency Configuration
	flag.IntVar(&args.JoinConcurrencyLimit, "join-concurrency-limit", args.JoinConcurrencyLimit, "Max concurrent join executions; 0 = no limit (default: 16)")

	// Final parse with CLI taking precedence
	flag.Parse()

	// Log which configuration source was used
	log.Printf("Configuration loaded from: %s", configSource)

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	logFilename := fmt.Sprintf("%s_%s_ServerLog.txt", timestamp, args.Host)

	// Combine with the directory path from args.LogFile
	args.LogFile = filepath.Join(args.LogDir, logFilename)

	// Validate the arguments
	if err := validateArguments(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n\n", err)
		printUsage()
		os.Exit(1)
	}

	// Configure logger
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	// Print the arguments if in verbose mode
	if args.Verbose {
		log.Println("SyndrDB starting with options:")
		log.Printf("  Data Directory: %s\n", args.DataDir)
		log.Printf("  Log File: %s\n", args.LogFile)
		log.Printf("  Host: %s\n", args.Host)
		log.Printf("  Port: %d\n", args.Port)
		log.Printf("  Verbose: %v\n", args.Verbose)
		log.Printf("  Config File: %s\n", args.ConfigFile)
		log.Printf("  Mode: %s\n", args.Mode)
		log.Printf("  Auth Enabled: %v\n", args.AuthEnabled)
		log.Printf("  Session Timeout: %d minutes\n", args.SessionTimeoutMinutes)
		log.Printf("  Max Sessions: %d\n", args.MaxSessions)
		log.Printf("  GraphQL Enabled: %v\n", args.EnableGraphQL)
		log.Printf("  Export Real-Time Metrics: %v\n", args.ExportRealtimeMetrics)
	}

	// Set up logging
	if args.LogDir != "" {
		// Create timestamped log filename
		// timestamp := time.Now().Format("2006-01-02_15-04-05")
		// logFilename := fmt.Sprintf("%s_%s_ServerLog.txt", timestamp, args.Host)

		// // Combine with the directory path from args.LogFile
		// logFilePath := filepath.Join(args.LogDir, logFilename)

		// Ensure log directory exists
		if err := os.MkdirAll(args.LogDir, constants.DirPermissionsDefault); err != nil {
			log.Fatalf("Failed to create log directory: %v", err)
		}

		log.Printf("Logging to file: %s", args.LogFile)

		logFile, err := os.OpenFile(args.LogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, constants.FilePermissionsDefault)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		defer logFile.Close()

		// Use MultiWriter to write logs to both file and stdout if PrintToScreen is enabled
		if args.PrintToScreen {
			mw := io.MultiWriter(os.Stdout, logFile)
			log.SetOutput(mw)
		} else {
			log.SetOutput(logFile)
		}
	}

	// Ensure data directory exists
	if err := os.MkdirAll(args.DataDir, constants.DirPermissionsDefault); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Create and start the server
	srv, err := server.InitServer(args)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}
	//srv := server.NewServer(args.Host, args.Port, db, args.AuthEnabled)

	// Run index cleanup (temp + schema-based orphans) before starting
	if err := bundle.RunIndexCleanup(args.DataDir, srv.Databases, srv.GetLogger()); err != nil {
		log.Printf("Warning: index cleanup failed: %v", err)
	}

	// Create default users from configuration if authentication is enabled
	if args.AuthEnabled && len(args.DefaultUsers) > 0 {
		serviceManager := server.GetServiceManager()
		if serviceManager != nil && serviceManager.UserService != nil {
			createdCount := 0
			for _, defaultUser := range args.DefaultUsers {
				_, err := serviceManager.UserService.CreateUser(defaultUser.Username, defaultUser.Password)
				if err != nil {
					// User might already exist, which is fine - log but continue
					if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "already taken") {
						log.Printf("Default user '%s' already exists, skipping creation", defaultUser.Username)
					} else {
						log.Printf("Warning: Failed to create default user '%s': %v", defaultUser.Username, err)
					}
				} else {
					log.Printf("Created default user '%s' from configuration", defaultUser.Username)
					createdCount++
				}
			}
			if createdCount > 0 {
				log.Printf("Created %d default user(s) from configuration", createdCount)
			}
		}
	}

	// Warn if authentication is enabled but no users exist
	if args.AuthEnabled {
		serviceManager := server.GetServiceManager()
		userCount := 0
		if serviceManager != nil && serviceManager.UserService != nil && srv.UserStore != nil {
			userCount = len(srv.UserStore.ListUsers())
		}
		if userCount == 0 && len(args.DefaultUsers) == 0 {
			log.Printf("⚠️  WARNING: Authentication is enabled but no users exist. Create users using 'ADD USER' command or configure default_users in config file.")
		}
	}

	// Pprof: resolve output dir (TempDir -> DataDir -> cwd -> /tmp) and ensure it exists
	pprofDir := resolvePprofDir(args.TempDir, args.DataDir)
	log.Printf("pprof: profile files will be written to: %s", pprofDir)

	// HTTP: write-heap / write-goroutines write to file and return JSON {path, ok} — no signals needed
	http.HandleFunc("/debug/write-heap", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path, err := writeHeapProfileToFile(pprofDir)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"ok": "false", "error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"ok": "true", "path": path})
	})
	http.HandleFunc("/debug/write-goroutines", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path, err := writeGoroutineProfileToFile(pprofDir)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"ok": "false", "error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"ok": "true", "path": path})
	})

	// Start pprof server on :6060 (serves /debug/pprof/* and /debug/write-heap, /debug/write-goroutines)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fatal.LogFatalAndExit(r)
			}
		}()
		log.Println("Starting pprof server on :6060 (http://localhost:6060/debug/pprof/ ; /debug/write-heap ; /debug/write-goroutines)")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Printf("pprof server error: %v", err)
		}
	}()

	// Signal-triggered dumps (Unix): kill -USR1 <pid> (heap), kill -USR2 <pid> (goroutines)
	if runtime.GOOS != "windows" {
		usrCh := make(chan os.Signal, 2)
		signal.Notify(usrCh, syscall.SIGUSR1, syscall.SIGUSR2)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					fatal.LogFatalAndExit(r)
				}
			}()
			for sig := range usrCh {
				switch sig {
				case syscall.SIGUSR1:
					log.Printf("pprof: received SIGUSR1, writing heap profile to %s ...", pprofDir)
					p, err := writeHeapProfileToFile(pprofDir)
					if err != nil {
						log.Printf("pprof: failed to write heap profile: %v", err)
					} else {
						log.Printf("pprof: wrote heap profile to %s (analyze: go tool pprof -alloc_space %s)", p, p)
					}
				case syscall.SIGUSR2:
					log.Printf("pprof: received SIGUSR2, writing goroutine profile to %s ...", pprofDir)
					p, err := writeGoroutineProfileToFile(pprofDir)
					if err != nil {
						log.Printf("pprof: failed to write goroutine profile: %v", err)
					} else {
						log.Printf("pprof: wrote goroutine profile to %s", p)
					}
				}
			}
		}()
		log.Printf("pprof: signal dumps. Heap: kill -USR1 %d  Goroutines: kill -USR2 %d", os.Getpid(), os.Getpid())
	}

	// Start the server
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Log WHERE clause SIMD configuration (Phase 1 optimization)
	if args.WhereSIMDEnabled {
		if args.WhereSIMDAutoDetect {
			log.Printf("✓ WHERE clause SIMD optimization enabled (CPU auto-detection: ON)")
			log.Printf("  SIMD will automatically use AVX2/NEON if available, fallback to scalar otherwise")
		} else {
			log.Printf("✓ WHERE clause SIMD optimization enabled (CPU auto-detection: OFF)")
		}
	} else {
		log.Printf("⚠️  WHERE clause SIMD optimization disabled - using scalar comparisons")
		log.Printf("  Enable with --where-simd-enabled for 4-6x performance improvement")
	}

	// Initialize GraphQL for TCP connections if enabled
	if args.EnableGraphQL {
		// Get the default database for GraphQL operations
		var defaultDB *models.Database
		for _, db := range srv.Databases {
			defaultDB = db
			break // Use the first database as default
		}

		if defaultDB != nil {
			// Create GraphQL schema manager
			schemaFilePath := filepath.Join(defaultDB.DataDirectory, fmt.Sprintf("%s_graphql_schemas.gqls", defaultDB.Name))
			schemaManager, err := schema.NewSchemaManager(schemaFilePath, defaultDB.Name, defaultDB.DatabaseID)
			if err != nil {
				log.Printf("Warning: Failed to initialize GraphQL schema manager: %v", err)
			} else {
				// Create GraphQL handler with schema manager and security config
				serviceManager := server.GetServiceManager()
				gqlSecurityConfig := settings.BuildGraphQLSecurityConfig(args)
				graphQLHandler, err := graphQL.NewGraphQLHandler(*serviceManager, defaultDB, schemaManager, srv.GetLogger(), gqlSecurityConfig)
				if err != nil {
					log.Printf("Warning: Failed to initialize GraphQL handler: %v", err)
				} else {
					// Set GraphQL processor for TCP socket connections
					server.SetGraphQLProcessor(graphQLHandler)
					log.Println("GraphQL enabled for TCP socket connections with GRAPHQL:: prefix")
				}
			}
		} else {
			log.Println("Warning: No database available for GraphQL")
		}
	}

	// Handle graceful shutdown
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	<-shutdownSignal
	fmt.Println("\nShutting down server...")

	// Create a context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Deal with the real time metrics exporter if enabled
	if args.ExportRealtimeMetrics {
		exporter, _ := monitoring.NewMemoryMappedExporter("/tmp/syndrdb_metrics.mmap", time.Second, srv.GetLogger())
		exporter.Start()
		defer exporter.Stop()
	}

	// Create a channel to signal shutdown completion
	done := make(chan error, 1)

	// Run shutdown in a goroutine
	go func() {
		done <- srv.Stop()
	}()

	// Wait for either shutdown to complete or timeout
	select {
	case err := <-done:
		if err != nil {
			log.Printf("Error stopping server: %v", err)
		} else {
			fmt.Println("Server shutdown complete")
		}
	case <-ctx.Done():
		fmt.Println("Server shutdown timed out after 30 seconds")
		log.Printf("Forcing server shutdown due to timeout")
	}
}

// validateArguments validates the arguments and returns an error if invalid
// MED-007: Comprehensive configuration validation including negative timeouts, invalid paths, and conflicting settings
func validateArguments(args *settings.Arguments) error {
	var validationErrors []string

	// Validate directory paths
	if err := validateDirectoryPath(args.DataDir, "data directory"); err != nil {
		validationErrors = append(validationErrors, err.Error())
	}

	if args.LogDir != "" {
		if err := validateDirectoryPath(args.LogDir, "log directory"); err != nil {
			validationErrors = append(validationErrors, err.Error())
		}

		// Check if we can create/open the log file
		if args.LogFile != "" {
			logFile, err := os.OpenFile(args.LogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, constants.FilePermissionsDefault)
			if err != nil {
				validationErrors = append(validationErrors, fmt.Sprintf("could not open log file for writing: %v", err))
			} else {
				logFile.Close()
			}
		}
	}

	if args.TempDir != "" {
		if err := validateDirectoryPath(args.TempDir, "temp directory"); err != nil {
			validationErrors = append(validationErrors, err.Error())
		}
	}

	// Validate port range
	if args.Port < constants.PortMin || args.Port > constants.PortMax {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid port number: %d (must be between %d and %d)", args.Port, constants.PortMin, constants.PortMax))
	}

	// If config file is specified, check if it exists and is readable
	if args.ConfigFile != "" {
		if _, err := os.Stat(args.ConfigFile); err != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("could not access config file: %v", err))
		}
	}

	// Validate mode
	validModes := map[string]bool{"standalone": true, "cluster": true}
	if _, valid := validModes[args.Mode]; !valid {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid mode: %s (must be 'standalone' or 'cluster')", args.Mode))
	}

	// MED-007: Validate all timeout values (prevent negative timeouts)
	if args.SessionTimeoutMinutes < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid session timeout: %d minutes (must be at least 1)", args.SessionTimeoutMinutes))
	}
	if args.ConnectionIdleTimeoutMinutes < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid connection idle timeout: %d minutes (must be at least 1)", args.ConnectionIdleTimeoutMinutes))
	}
	if args.LockTimeoutSeconds < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid lock timeout: %d seconds (must be at least 1)", args.LockTimeoutSeconds))
	}
	if args.WorkerPoolStopTimeoutSeconds < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid worker pool stop timeout: %d seconds (must be at least 1)", args.WorkerPoolStopTimeoutSeconds))
	}
	if args.MigrationTimeoutSeconds < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid migration timeout: %d seconds (must be at least 1)", args.MigrationTimeoutSeconds))
	}

	// Validate query timeout configuration
	if args.QueryTimeoutSeconds < constants.TimeoutMin || args.QueryTimeoutSeconds > constants.TimeoutMaxDefault {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid query timeout: %d seconds (must be between %d and %d)", args.QueryTimeoutSeconds, constants.TimeoutMin, constants.TimeoutMaxDefault))
	}
	if args.AdminQueryTimeoutSeconds < 60 || args.AdminQueryTimeoutSeconds > constants.TimeoutMaxDefault {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid admin query timeout: %d seconds (must be between 60 and %d)", args.AdminQueryTimeoutSeconds, constants.TimeoutMaxDefault))
	}

	// Validate transaction idle timeout string format
	if args.TransactionIdleTimeout != "" {
		if _, err := time.ParseDuration(args.TransactionIdleTimeout); err != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("invalid transaction idle timeout format: %s (must be a valid duration like '5m', '30s', '1h')", args.TransactionIdleTimeout))
		}
	}

	// MED-007: Validate memory limits (must be positive)
	if args.QueryMaxMemoryMB < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid query max memory: %d MB (must be at least 1)", args.QueryMaxMemoryMB))
	}
	if args.AdminQueryMaxMemoryMB < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid admin query max memory: %d MB (must be at least 1)", args.AdminQueryMaxMemoryMB))
	}
	if args.SortMaxMemoryMB < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid sort max memory: %d MB (must be at least 1)", args.SortMaxMemoryMB))
	}
	if args.UniqueIndexMemoryBudgetMB < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid unique index memory budget: %d MB (must be at least 1)", args.UniqueIndexMemoryBudgetMB))
	}

	// MED-007: Validate connection/session limits (must be positive)
	if args.MaxConnections < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid max connections: %d (must be at least 1)", args.MaxConnections))
	}
	if args.MaxSessions < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid max sessions: %d (must be at least 1)", args.MaxSessions))
	}
	if args.MaxWorkerPools < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid max worker pools: %d (must be at least 1)", args.MaxWorkerPools))
	}

	// MED-007: Validate WAL configuration
	if args.WALMode != "sync" && args.WALMode != "async" {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid WAL mode: %s (must be 'sync' or 'async')", args.WALMode))
	}
	if args.AsyncWALWorkers < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid async WAL workers: %d (must be at least 1)", args.AsyncWALWorkers))
	}
	if args.AsyncWALQueueSize < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid async WAL queue size: %d (must be at least 1)", args.AsyncWALQueueSize))
	}

	// MED-007: Validate bundle storage format
	if args.BundleStorageFormat != "json" && args.BundleStorageFormat != "binary" {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid bundle storage format: %s (must be 'json' or 'binary')", args.BundleStorageFormat))
	}

	// MED-007: Validate TLS configuration (check file paths if TLS is enabled)
	if args.TLSEnabled {
		if args.TLSCertFile == "" {
			validationErrors = append(validationErrors, "TLS is enabled but TLS certificate file is not specified")
		} else if _, err := os.Stat(args.TLSCertFile); err != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("TLS certificate file not found: %s", args.TLSCertFile))
		}

		if args.TLSKeyFile == "" {
			validationErrors = append(validationErrors, "TLS is enabled but TLS key file is not specified")
		} else if _, err := os.Stat(args.TLSKeyFile); err != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("TLS key file not found: %s", args.TLSKeyFile))
		}

		if args.TLSRequireClientCert && args.TLSCAFile == "" {
			validationErrors = append(validationErrors, "TLS client certificate required but CA file is not specified")
		}
		if args.TLSRequireClientCert && args.TLSCAFile != "" {
			if _, err := os.Stat(args.TLSCAFile); err != nil {
				validationErrors = append(validationErrors, fmt.Sprintf("TLS CA file not found: %s", args.TLSCAFile))
			}
		}
	}

	// MED-007: Validate input length limits (MED-003)
	if args.MaxParameterValueLength < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid max parameter value length: %d bytes (must be at least 1)", args.MaxParameterValueLength))
	}
	if args.MaxFieldValueLength < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid max field value length: %d bytes (must be at least 1)", args.MaxFieldValueLength))
	}
	if args.MaxFieldNameLength < 1 {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid max field name length: %d bytes (must be at least 1)", args.MaxFieldNameLength))
	}

	// MED-007: Validate conflicting settings
	if args.MaxSessions < args.MaxConnections {
		validationErrors = append(validationErrors, fmt.Sprintf("conflicting settings: max_sessions (%d) must be >= max_connections (%d)", args.MaxSessions, args.MaxConnections))
	}

	if args.AdminQueryTimeoutSeconds < args.QueryTimeoutSeconds {
		validationErrors = append(validationErrors, fmt.Sprintf("conflicting settings: admin_query_timeout_seconds (%d) should be >= query_timeout_seconds (%d)", args.AdminQueryTimeoutSeconds, args.QueryTimeoutSeconds))
	}

	// MED-007: Validate GraphQL rate limiting algorithm
	if args.GraphQLRateAlgorithm != "token-bucket" && args.GraphQLRateAlgorithm != "time-bucket" {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid GraphQL rate algorithm: %s (must be 'token-bucket' or 'time-bucket')", args.GraphQLRateAlgorithm))
	}

	// Return all validation errors as a single error
	if len(validationErrors) > 0 {
		errorMsg := "Configuration validation failed:\n  " + strings.Join(validationErrors, "\n  ")
		return errors.New(errors.ERR_SYSTEM_CONFIG, errorMsg, errors.LayerAPI)
	}

	return nil
}

// validateDirectoryPath validates that a directory path exists, is accessible, and can be created if needed
// MED-007: Comprehensive path validation
func validateDirectoryPath(path, description string) error {
	if path == "" {
		return fmt.Errorf("%s cannot be empty", description)
	}

	dirInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Try to create the directory
			err = os.MkdirAll(path, constants.DirPermissionsDefault)
			if err != nil {
				return fmt.Errorf("could not create %s at %s: %w", description, path, err)
			}
		} else {
			return fmt.Errorf("error accessing %s at %s: %w", description, path, err)
		}
	} else if !dirInfo.IsDir() {
		return fmt.Errorf("%s path exists but is not a directory: %s", description, path)
	}

	// Check if directory is writable
	if err := os.WriteFile(filepath.Join(path, ".syndrdb_write_test"), []byte("test"), constants.FilePermissionsDefault); err != nil {
		return fmt.Errorf("%s at %s is not writable: %w", description, path, err)
	}
	os.Remove(filepath.Join(path, ".syndrdb_write_test"))

	return nil
}
