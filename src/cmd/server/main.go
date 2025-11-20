package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/graphQL"
	"syndrdb/src/internal/graphQL/schema"
	"syndrdb/src/internal/server"
	"syndrdb/src/pkg/settings"

	"syscall"
	"time"
)

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

	//args := settings.Arguments{}

	// Define command line flags that map to the Arguments struct
	flag.StringVar(&args.DataDir, "datadir", "./data_files", "Directory to store data files")
	flag.StringVar(&args.LogDir, "logdir", "./log_files", "Directory to store log files (default: stdout)")
	flag.StringVar(&args.TempDir, "tempdir", "./temp_files", "Temporary directory for intermediate files/indexes/sorts")
	flag.Int64Var(&args.MaxJournalFileSize, "maxjournalfilesize", 1000000, "Maximum size of journal files in bytes (default: 1MB)")
	flag.StringVar(&args.Host, "host", "127.0.0.1", "Host name or IP address to listen on")
	flag.IntVar(&args.Port, "port", 1776, "Port for the HTTP server")
	flag.BoolVar(&args.Verbose, "verbose", true, "Enable verbose logging")
	flag.StringVar(&args.ConfigFile, "config", "", "Path to config file")
	flag.StringVar(&args.Mode, "mode", "standalone", "Operation mode (standalone, cluster)")
	flag.BoolVar(&args.AuthEnabled, "auth", false, "Enable authentication")
	flag.IntVar(&args.SessionTimeoutMinutes, "session-timeout", 30, "Session timeout in minutes")
	flag.IntVar(&args.MaxSessions, "max-sessions", 1000, "Maximum number of concurrent sessions")
	flag.StringVar(&args.Version, "version", "0.0.1alpha", "Shows version")
	flag.BoolVar(&args.PrintToScreen, "print", true, "Print Log Messages to screen")
	flag.BoolVar(&args.Debug, "debug", true, "Enable debug mode")
	flag.BoolVar(&args.UserDebug, "userdebug", false, "Enable user debug mode")
	flag.BoolVar(&args.EnableGraphQL, "graphql", false, "Enable GraphQL API")
	flag.StringVar(&args.LogLevel, "loglevel", "info", "Log level: debug, info, warn, error")
	flag.StringVar(&args.BundleStorageFormat, "bundle-format", "binary", "Bundle storage format (only 'binary' is supported)")

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

	// Parse command line arguments
	flag.Parse()

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

	}

	// Set up logging
	if args.LogDir != "" {
		// Create timestamped log filename
		// timestamp := time.Now().Format("2006-01-02_15-04-05")
		// logFilename := fmt.Sprintf("%s_%s_ServerLog.txt", timestamp, args.Host)

		// // Combine with the directory path from args.LogFile
		// logFilePath := filepath.Join(args.LogDir, logFilename)

		// Ensure log directory exists
		logDir := filepath.Dir(args.LogDir)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			log.Fatalf("Failed to create log directory: %v", err)
		}

		log.Printf("Logging to file: %s", args.LogFile)

		logFile, err := os.OpenFile(args.LogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
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
	if err := os.MkdirAll(args.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Create and start the server
	srv, err := server.InitServer(args)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}
	//srv := server.NewServer(args.Host, args.Port, db, args.AuthEnabled)

	// Add users if authentication is enabled
	if args.AuthEnabled {
		srv.AddUser("admin", "admin123")   // Example user
		srv.AddUser("syndrdb", "password") // Example user
	}

	// Start the server
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
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
func validateArguments(args *settings.Arguments) error {
	// Check if data directory exists and is accessible
	dirInfo, err := os.Stat(args.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			// Try to create the directory
			err = os.MkdirAll(args.DataDir, 0755)
			if err != nil {
				return fmt.Errorf("could not create data directory: %w", err)
			}
		} else {
			return fmt.Errorf("error accessing data directory: %w", err)
		}
	} else if !dirInfo.IsDir() {
		return fmt.Errorf("data directory path exists but is not a directory: %s", args.DataDir)
	}

	// Check if log file can be written to
	if args.LogDir != "" {
		logDir := filepath.Dir(args.LogDir)
		if logDir != "." {
			if _, err := os.Stat(logDir); os.IsNotExist(err) {
				err = os.MkdirAll(logDir, 0755)
				if err != nil {
					return fmt.Errorf("could not create log directory: %w", err)
				}
			}
		}

		// Check if we can create/open the log file
		logFile, err := os.OpenFile(args.LogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("could not open log file for writing: %w", err)
		}
		logFile.Close()
	}

	// Validate port range
	if args.Port < 1 || args.Port > 65535 {
		return fmt.Errorf("invalid port number: %d (must be between 1 and 65535)", args.Port)
	}

	// If config file is specified, check if it exists and is readable
	if args.ConfigFile != "" {
		_, err := os.Stat(args.ConfigFile)
		if err != nil {
			return fmt.Errorf("could not access config file: %w", err)
		}
	}

	// Validate mode
	validModes := map[string]bool{"standalone": true, "cluster": true}
	if _, valid := validModes[args.Mode]; !valid {
		return fmt.Errorf("invalid mode: %s (must be 'standalone' or 'cluster')", args.Mode)
	}

	return nil
}
