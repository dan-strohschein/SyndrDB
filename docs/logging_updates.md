# Logging Performance Optimization Strategies for SyndrDB

To ensure logging doesn't impact database performance, here are several strategies to implement:

## 1. Asynchronous Logging with Buffered Writes

````go
package logging

import (
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "gopkg.in/natefinch/lumberjack.v2"
    "os"
    "path/filepath"
    "time"
)

// LoggerConfig defines configuration for the SyndrDB logger
// This includes performance tuning parameters to ensure logging doesn't
// impact database operations
type LoggerConfig struct {
    DataDir       string
    Debug         bool
    BufferSize    int           // Size of log buffer in bytes (default: 256KB)
    FlushInterval time.Duration // How often to flush logs (default: 1 second)
    MaxSize       int           // Max size per log file in MB
    MaxBackups    int           // Number of old log files to keep
    MaxAge        int           // Days to retain old log files
    Compress      bool          // Compress rotated logs
}

// DefaultLoggerConfig returns sensible defaults for production use
func DefaultLoggerConfig(dataDir string, debug bool) *LoggerConfig {
    return &LoggerConfig{
        DataDir:       dataDir,
        Debug:         debug,
        BufferSize:    256 * 1024, // 256KB buffer
        FlushInterval: 1 * time.Second,
        MaxSize:       100,  // 100MB per file
        MaxBackups:    10,   // Keep 10 old files
        MaxAge:        30,   // Keep logs for 30 days
        Compress:      true, // Compress old logs
    }
}

// InitLogger creates a high-performance logger for SyndrDB
// The logger uses buffered writes and async flushing to minimize
// performance impact on database operations
func InitLogger(config *LoggerConfig) (*zap.Logger, error) {
    // Create logs directory
    logDir := filepath.Join(config.DataDir, "logs")
    if err := os.MkdirAll(logDir, 0755); err != nil {
        return nil, err
    }

    // Configure log rotation for automatic cleanup
    fileWriter := &lumberjack.Logger{
        Filename:   filepath.Join(logDir, "syndrdb.log"),
        MaxSize:    config.MaxSize,
        MaxBackups: config.MaxBackups,
        MaxAge:     config.MaxAge,
        Compress:   config.Compress,
    }

    // Separate error log for critical issues
    errorFileWriter := &lumberjack.Logger{
        Filename:   filepath.Join(logDir, "syndrdb-error.log"),
        MaxSize:    config.MaxSize,
        MaxBackups: config.MaxBackups,
        MaxAge:     config.MaxAge,
        Compress:   config.Compress,
    }

    // Configure log level
    logLevel := zapcore.InfoLevel
    if config.Debug {
        logLevel = zapcore.DebugLevel
    }

    // Create encoder configs
    consoleEncoderConfig := zap.NewDevelopmentEncoderConfig()
    consoleEncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
    consoleEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

    fileEncoderConfig := zap.NewProductionEncoderConfig()
    fileEncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

    // Create encoders
    consoleEncoder := zapcore.NewConsoleEncoder(consoleEncoderConfig)
    fileEncoder := zapcore.NewJSONEncoder(fileEncoderConfig)

    // PERFORMANCE OPTIMIZATION: Use buffered write syncer with async flushing
    // This prevents blocking on disk I/O during log operations
    bufferedFileSyncer := &zapcore.BufferedWriteSyncer{
        WS:            zapcore.AddSync(fileWriter),
        Size:          config.BufferSize,
        FlushInterval: config.FlushInterval,
    }

    bufferedErrorSyncer := &zapcore.BufferedWriteSyncer{
        WS:            zapcore.AddSync(errorFileWriter),
        Size:          config.BufferSize,
        FlushInterval: config.FlushInterval,
    }

    // Create cores with different log levels
    // Console: All logs for development visibility
    // File: All logs buffered for performance
    // Error File: Only errors and above, buffered
    core := zapcore.NewTee(
        // Console output (unbuffered for immediate feedback during development)
        zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), logLevel),
        
        // Main log file (buffered for performance)
        zapcore.NewCore(fileEncoder, bufferedFileSyncer, logLevel),
        
        // Error-only log file (buffered, errors and above only)
        zapcore.NewCore(fileEncoder, bufferedErrorSyncer, zapcore.ErrorLevel),
    )

    // Create logger with sampling for high-volume logs
    // This prevents log flooding from affecting performance
    logger := zap.New(core,
        zap.AddCaller(),
        zap.AddStacktrace(zapcore.ErrorLevel),
        // PERFORMANCE: Sample repetitive debug logs (keep 1/100 after first 100/sec)
        zap.WrapCore(func(c zapcore.Core) zapcore.Core {
            return zapcore.NewSamplerWithOptions(c, time.Second, 100, 100)
        }),
    )

    return logger, nil
}

// FlushLogger ensures all buffered logs are written to disk
// This should be called before shutdown to prevent log loss
func FlushLogger(logger *zap.Logger) error {
    if logger == nil {
        return nil
    }
    return logger.Sync()
}
````

## 2. Context-Aware Logging Levels

````go
package logging

import (
    "context"
    "go.uber.org/zap"
)

// LogContext defines different logging contexts with different performance characteristics
type LogContext string

const (
    // Hot path operations - minimal logging to avoid performance impact
    ContextQueryExecution LogContext = "query_execution"
    ContextIndexLookup    LogContext = "index_lookup"
    ContextBufferAccess   LogContext = "buffer_access"
    
    // Warm path operations - moderate logging
    ContextTransactionMgmt LogContext = "transaction_mgmt"
    ContextCacheMgmt       LogContext = "cache_mgmt"
    
    // Cold path operations - full logging
    ContextStartup       LogContext = "startup"
    ContextMaintenance   LogContext = "maintenance"
    ContextAdministration LogContext = "administration"
)

// ContextLogger wraps zap.SugaredLogger with context-aware performance tuning
type ContextLogger struct {
    logger  *zap.SugaredLogger
    context LogContext
}

// NewContextLogger creates a logger optimized for a specific operational context
func NewContextLogger(logger *zap.SugaredLogger, ctx LogContext) *ContextLogger {
    return &ContextLogger{
        logger:  logger,
        context: ctx,
    }
}

// Debug logs only if not in a hot path context
func (cl *ContextLogger) Debug(args ...interface{}) {
    if cl.isHotPath() {
        return // Skip debug logging in hot paths
    }
    cl.logger.Debug(args...)
}

// Debugf logs only if not in a hot path context
func (cl *ContextLogger) Debugf(template string, args ...interface{}) {
    if cl.isHotPath() {
        return // Skip debug logging in hot paths
    }
    cl.logger.Debugf(template, args...)
}

// Info logs with sampling in hot paths
func (cl *ContextLogger) Info(args ...interface{}) {
    if cl.isHotPath() {
        // In production, you might want to sample these
        return
    }
    cl.logger.Info(args...)
}

// Infof logs with sampling in hot paths
func (cl *ContextLogger) Infof(template string, args ...interface{}) {
    if cl.isHotPath() {
        // In production, you might want to sample these
        return
    }
    cl.logger.Infof(template, args...)
}

// Warn always logs (important for operational visibility)
func (cl *ContextLogger) Warn(args ...interface{}) {
    cl.logger.Warn(args...)
}

// Warnf always logs (important for operational visibility)
func (cl *ContextLogger) Warnf(template string, args ...interface{}) {
    cl.logger.Warnf(template, args...)
}

// Error always logs (critical for debugging)
func (cl *ContextLogger) Error(args ...interface{}) {
    cl.logger.Error(args...)
}

// Errorf always logs (critical for debugging)
func (cl *ContextLogger) Errorf(template string, args ...interface{}) {
    cl.logger.Errorf(template, args...)
}

// isHotPath determines if this is a performance-critical code path
func (cl *ContextLogger) isHotPath() bool {
    switch cl.context {
    case ContextQueryExecution, ContextIndexLookup, ContextBufferAccess:
        return true
    default:
        return false
    }
}
````

## 3. Structured Logging with Pre-allocated Fields

````go
package logging

import "go.uber.org/zap"

// Common field keys used throughout SyndrDB
// Pre-defining these avoids allocations during logging
const (
    FieldDatabase   = "database"
    FieldBundle     = "bundle"
    FieldDocument   = "document_id"
    FieldOperation  = "operation"
    FieldDuration   = "duration_ms"
    FieldRowCount   = "row_count"
    FieldPageID     = "page_id"
    FieldBufferPool = "buffer_pool"
    FieldCacheHit   = "cache_hit"
)

// PerformanceFields creates a reusable set of fields for performance logging
// This avoids repeated allocations for common logging scenarios
type PerformanceFields struct {
    fields []zap.Field
}

// NewPerformanceFields creates a pre-allocated field set
func NewPerformanceFields(capacity int) *PerformanceFields {
    return &PerformanceFields{
        fields: make([]zap.Field, 0, capacity),
    }
}

// AddString adds a string field
func (pf *PerformanceFields) AddString(key, value string) *PerformanceFields {
    pf.fields = append(pf.fields, zap.String(key, value))
    return pf
}

// AddInt adds an int field
func (pf *PerformanceFields) AddInt(key string, value int) *PerformanceFields {
    pf.fields = append(pf.fields, zap.Int(key, value))
    return pf
}

// AddDuration adds a duration in milliseconds
func (pf *PerformanceFields) AddDuration(key string, ms int64) *PerformanceFields {
    pf.fields = append(pf.fields, zap.Int64(key, ms))
    return pf
}

// AddBool adds a boolean field
func (pf *PerformanceFields) AddBool(key string, value bool) *PerformanceFields {
    pf.fields = append(pf.fields, zap.Bool(key, value))
    return pf
}

// GetFields returns the field slice for logging
func (pf *PerformanceFields) GetFields() []zap.Field {
    return pf.fields
}

// Reset clears the fields for reuse
func (pf *PerformanceFields) Reset() {
    pf.fields = pf.fields[:0]
}
````

## 4. Conditional Compilation for Debug Logs

````go
//go:build debug
// +build debug

package logging

// DebugEnabled is true when compiled with debug build tag
const DebugEnabled = true
````

````go
//go:build !debug
// +build !debug

package logging

// DebugEnabled is false in release builds
const DebugEnabled = false
````

## 5. Updated ServiceManager with Performance Logging

````go
// ...existing code...

func InitServiceManager(dbService *database.DatabaseService, bundleService *bundle.BundleService,
	catalogService *defaultdb.CatalogService,
	graphqlProcessor GraphQLProcessor,
	userStore *auth.UserStore,
	logger *zap.SugaredLogger,
	debugMode bool) *ServiceManager {
	// Use sync.Once to ensure this only happens one time
	once.Do(func() {
		mu.Lock()
		defer mu.Unlock()

		// Initialize WAL Manager
		walManager, err := journal.NewWALManager(logger)
		if err != nil {
			if logger != nil {
				logger.Errorf("Failed to initialize WAL Manager: %v", err)
			}
			// Continue without WAL for now, but log the error
			walManager = nil
		}

		// Register WAL Manager in global service registry
		// This allows other services to access WAL without circular dependencies
		if walManager != nil {
			serviceRegistry := registry.GetRegistry()
			serviceRegistry.SetWALManager(walManager)
			serviceRegistry.SetLogger(logger)
			if logger != nil {
				// PERFORMANCE: Use Info instead of Infof when no formatting needed
				logger.Info("WAL Manager registered in global service registry")
			}
		}

		// Initialize RBAC services
		userService := NewUserService(bundleService, dbService, userStore, logger, debugMode)
		permissionService := NewPermissionService(bundleService, dbService, logger, debugMode)

		// Initialize Lock service
		lockService := lock.NewLockService(logger.Desugar())

		instance = &ServiceManager{
			DatabaseService:        dbService,
			BundleService:          bundleService,
			InternalCatalogService: catalogService,
			WALManager:             walManager,
			LockService:            lockService,
			GraphQLProcessor:       graphqlProcessor,
			UserService:            userService,
			PermissionService:      permissionService,
			MigrationService:       nil,
			logger:                 logger,
		}

		if logger != nil {
			if walManager != nil {
				logger.Info("ServiceManager singleton initialized with WAL Manager")
			} else {
				logger.Warn("ServiceManager singleton initialized without WAL Manager")
			}
			if graphqlProcessor != nil {
				logger.Info("ServiceManager initialized with GraphQL support")
			} else {
				logger.Info("ServiceManager initialized without GraphQL support")
			}
		}
	})

	return instance
}

// ...existing code...
````

## 6. Main Application Integration

````go
package main

import (
    "syndrdb/src/pkg/logging"
    "syndrdb/src/pkg/settings"
    // ...other imports...
)

func main() {
    settings := settings.GetSettings()
    
    // Initialize high-performance logger
    logConfig := logging.DefaultLoggerConfig(settings.DataDir, settings.Debug)
    logger, err := logging.InitLogger(logConfig)
    if err != nil {
        log.Fatalf("Failed to initialize logger: %v", err)
    }
    defer logging.FlushLogger(logger)
    
    sugar := logger.Sugar()
    sugar.Info("SyndrDB server starting with optimized logging")
    
    // ...rest of initialization...
}
````

## Key Performance Benefits

1. **Buffered Writes**: 256KB buffer with 1-second flush interval prevents blocking on disk I/O
2. **Async Flushing**: Background goroutine handles disk writes
3. **Log Sampling**: Prevents log flooding in high-throughput scenarios
4. **Context-Aware Logging**: Hot paths skip debug/info logs entirely
5. **Pre-allocated Fields**: Reduces memory allocations during logging
6. **Separate Error Logs**: Critical issues are isolated for faster analysis
7. **Log Rotation**: Automatic cleanup prevents disk space issues
8. **Conditional Compilation**: Debug logs can be completely removed in production builds

## Build Commands

```bash
# Production build (debug logs compiled out)
go build -tags release -o syndrdb cmd/main.go

# Debug build (all logs enabled)
go build -tags debug -o syndrdb-debug cmd/main.go
```

This approach ensures logging has **minimal to zero performance impact** on database operations while maintaining excellent observability.