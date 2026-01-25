package server

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syndrdb/src/data"
	"syndrdb/src/internal/audit"
	"syndrdb/src/internal/auth"
	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/compactor"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/registry"

	jsoniter "github.com/json-iterator/go"

	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"

	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/fatal"
	"syndrdb/src/pkg/settings"

	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/argon2"
)

// PHASE D: Use json-iterator for faster, lower-allocation JSON encoding
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Server represents the main TCP server for SyndrDB
type Server struct {
	Host                  string
	Port                  int
	Databases             map[string]*models.Database
	Listener              net.Listener
	AuthEnabled           bool
	GraphQLEnabled        bool
	Users                 map[string]string // username -> hashed password (legacy, use UserStore instead)
	UserStore             *auth.UserStore   // Modern user authentication with rate limiting
	ActiveConnections     map[string]*Connection
	SessionManager        *SessionManager
	ServiceManager        *ServiceManager
	RateLimiter           *RateLimiter
	TLSConfig             *tls.Config
	SessionTimeout        time.Duration
	MaxSessions           int
	MaxConnections        int           // Maximum connection pool size
	ConnectionIdleTimeout time.Duration // Connection idle timeout duration
	mu                    sync.Mutex
	Running               bool
	databaseService       *database.DatabaseService
	logger                *zap.SugaredLogger
	errorLogger           *errors.ErrorLogger // MED-004: Error framework logger
	bufferPool            *buffer.BufferPool
	wg                    sync.WaitGroup                // WaitGroup for tracking active connections
	activeQueryCount      atomic.Uint64                 // Number of currently executing queries (for ghost cleanup pausing)
	GhostCleanupWorker    *compactor.GhostCleanupWorker // Background worker for automatic compaction
	MVCCGCWorker          *compactor.MVCCGCWorker       // Background worker for MVCC garbage collection
}

// Connection represents an active client connection
type Connection struct {
	ID           string
	Conn         net.Conn
	Reader       *bufio.Reader
	Writer       *bufio.Writer
	DatabaseName string
	Database     *models.Database // Current database for this connection
	User         string
	Authorized   bool
	Session      *Session // Associated session
	LastActive   time.Time
	Logger       *zap.SugaredLogger
}

// ConnectionString represents parsed MongoDB connection string
type ConnectionString struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Options  map[string]string
}

// // NewServer creates a new SyndrDB server instance
// func NewServer(host string, port int, database *engine.Database, authEnabled bool) *Server {

// 	databases, err := engine.LoadAllDatabases(settings.GetSettings().DataDir)
// 	if err != nil {
// 		log.Fatalf("Failed to load databases: %v", err)
// 	}

// 	return &Server{
// 		Host:              host,
// 		Port:              port,
// 		Databases:         make(map[string]*engine.Database),
// 		AuthEnabled:       authEnabled,
// 		Users:             make(map[string]string),
// 		ActiveConnections: make(map[string]*Connection),
// 	}
// }

// CommandTerminator is a non-printable character signaling end of complete command batch.
// Using ASCII EOT (End of Transmission) for semantic clarity and protocol framing.
// This allows multi-statement commands (migrations, transactions) to be processed as a unit.
const CommandTerminator = "\x04"

// splitOnCommandTerminator splits data on CommandTerminator (\x04), respecting
// \x04\x04 as an escaped literal (e.g. in parameterized command values).
// Returns complete commands and any incomplete remainder to carry to the next read.
func splitOnCommandTerminator(data string) (complete []string, incomplete string) {
	var seg strings.Builder
	for i := 0; i < len(data); i++ {
		if data[i] == '\x04' {
			if i+1 < len(data) && data[i+1] == '\x04' {
				// Escaped \x04\x04: keep both in segment, skip second
				seg.WriteByte('\x04')
				seg.WriteByte('\x04')
				i++
			} else {
				// Delimiter: end of command
				s := strings.TrimSpace(seg.String())
				if s != "" {
					complete = append(complete, s)
				}
				seg.Reset()
			}
		} else {
			seg.WriteByte(data[i])
		}
	}
	incomplete = seg.String()
	return complete, incomplete
}

// InitServer initializes the SyndrDB server
func InitServer(config *settings.Arguments) (*Server, error) {

	var logger *zap.Logger
	var err error

	if config.Debug {
		// Development configuration with more verbose output
		//logger, err = zap.NewDevelopment()
		z := zap.NewDevelopmentConfig()
		z.OutputPaths = []string{"stdout"}
		z.Level, _ = zap.ParseAtomicLevel(config.LogLevel)
		logger, err = z.Build()
	} else {
		// Production configuration
		logger, err = zap.NewProduction()
	}

	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
			"failed to initialize logger", errors.LayerAPI)
	}

	// Create a sugared logger for easier API
	sugar := logger.Sugar()

	// Replace standard log with zap
	zap.ReplaceGlobals(logger)

	// Initialize error logger (MED-004)
	errorLoggerConfig := &errors.ErrorLoggerConfig{
		InternalLogFile: config.ErrorInternalLogFile,
		ExternalLogFile: config.ErrorExternalLogFile,
		LogDir:          config.LogDir,
		DebugMode:       config.Debug && config.ErrorShowInConsole,
		IncludeStack:    config.ErrorIncludeStack,
	}

	// Set defaults if not configured
	if errorLoggerConfig.InternalLogFile == "" {
		errorLoggerConfig.InternalLogFile = "errors_internal.log"
	}
	if errorLoggerConfig.ExternalLogFile == "" {
		errorLoggerConfig.ExternalLogFile = "errors_external.log"
	}
	if !config.Debug {
		errorLoggerConfig.DebugMode = false // Always false in production
	}

	errorLogger, err := errors.NewErrorLogger(errorLoggerConfig)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
			"failed to initialize error logger", errors.LayerAPI)
	}

	// Create database storage
	databaseStore, err := databasestore.NewDatabaseStore(config.DataDir, logger.Sugar())
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
			"failed to create database store", errors.LayerAPI).WithContext("data_dir", config.DataDir)
	}
	databaseFactory := database.NewDatabaseFactory()

	// Create service
	databaseService := database.NewDatabaseService(databaseStore, databaseFactory, config, sugar)

	// Create the File Registry
	fileRegistry, err := buffer.NewFileRegistry(config.DataDir, buffer.SyncInterval, logger.Sugar())
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
			"failed to create file registry", errors.LayerAPI).WithContext("data_dir", config.DataDir)
	}

	// Create buffer pool
	bufferPool := buffer.NewBufferPool(config.BundleBufferSize, buffer.DefaultPageSize, fileRegistry, sugar)

	// Create bundle service
	bundleStore, err := bundlestore.NewBundleStore(config.DataDir, bufferPool, logger.Sugar(), config.BundleStorageFormat)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
			"failed to create bundle store", errors.LayerAPI).WithContext("data_dir", config.DataDir)
	}
	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, sugar, config)
	bundleStore.RegisterCompactionComplete(func(db, b string) { bundleService.InvalidateDocumentPageMapForBundle(b) })

	// Create the internal catalog service
	catalogService := defaultdb.NewCatalogService(databaseService, bundleService, sugar)

	// Inject catalog service into bundle service (resolves circular dependency)
	bundleService.SetCatalogService(catalogService)

	// Initialize GraphQL processor if enabled
	var graphqlProcessor GraphQLProcessor
	if config.EnableGraphQL {
		// For now, we'll set graphqlProcessor to nil and enable it later
		// This avoids the import cycle issue
		graphqlProcessor = nil
		sugar.Info("GraphQL enabled - processor will be initialized after server creation")
	}

	// Initialize the singleton (UserStore will be set later if auth is enabled)
	serviceManager := InitServiceManager(databaseService, bundleService, catalogService, graphqlProcessor, nil, sugar, false)

	// Create a new server
	server := &Server{
		Host:                  config.Host,
		Port:                  config.Port,
		Databases:             make(map[string]*models.Database),
		AuthEnabled:           config.AuthEnabled,
		GraphQLEnabled:        config.EnableGraphQL,
		Users:                 make(map[string]string),
		ActiveConnections:     make(map[string]*Connection),
		SessionTimeout:        time.Duration(config.SessionTimeoutMinutes) * time.Minute,
		MaxSessions:           config.MaxSessions,
		MaxConnections:        config.MaxConnections,
		ConnectionIdleTimeout: time.Duration(config.ConnectionIdleTimeoutMinutes) * time.Minute,
		databaseService:       databaseService,
		logger:                sugar,
		errorLogger:           errorLogger,
		bufferPool:            bufferPool,
		ServiceManager:        serviceManager,
	}

	// Set defaults if not configured
	if server.MaxConnections <= 0 {
		server.MaxConnections = 100 // Default 100 connections
	}
	if server.ConnectionIdleTimeout <= 0 {
		server.ConnectionIdleTimeout = 30 * time.Minute // Default 30 minutes
	}

	// Initialize rate limiter with same max connections
	rateLimitConfig := DefaultRateLimitConfig()
	rateLimitConfig.MaxGlobalConnections = server.MaxConnections
	server.RateLimiter = NewRateLimiter(rateLimitConfig)

	// Initialize TLS configuration
	tlsConfig := &TLSConfig{
		Enabled:            config.TLSEnabled,
		CertFile:           config.TLSCertFile,
		KeyFile:            config.TLSKeyFile,
		GenerateSelfSigned: config.TLSGenerateSelfSigned,
		RequireClientCert:  config.TLSRequireClientCert,
		CAFile:             config.TLSCAFile,
	}

	if tlsConfig.CertFile == "" {
		tlsConfig.CertFile = "server.crt"
	}
	if tlsConfig.KeyFile == "" {
		tlsConfig.KeyFile = "server.key"
	}

	server.TLSConfig, err = SetupTLS(tlsConfig)
	if err != nil {
		sugar.Errorf("Failed to setup TLS: %v", err)
		return nil, err
	}

	if server.TLSConfig != nil {
		sugar.Infof("TLS/SSL encryption enabled")
	} else {
		sugar.Warnf("TLS/SSL encryption disabled - connections are not encrypted")
	}

	// Initialize session manager
	if server.SessionTimeout <= 0 {
		server.SessionTimeout = 30 * time.Minute // Default 30 minutes
	}
	if server.MaxSessions <= 0 {
		server.MaxSessions = 1000 // Default max 1000 sessions
	}

	server.SessionManager = NewSessionManager(sugar, server.SessionTimeout, server.MaxSessions)

	// CRITICAL: Wire LockManager to SessionManager for proper lock cleanup on session termination
	// This fixes the bug where session cleanup only cleared internal maps but didn't release actual locks
	if server.ServiceManager != nil && server.ServiceManager.LockManager != nil {
		server.SessionManager.SetLockReleaser(server.ServiceManager.LockManager)
	}

	// Set session context in ServiceManager for RBAC FORCE operations
	SetSessionContext(server.SessionManager, server.ActiveConnections)

	// Initialize UserStore with authentication rate limiting if auth is enabled
	if config.AuthEnabled {
		userStorePath := filepath.Join(config.DataDir, "users.dat")

		// Create audit configuration
		auditConfig := audit.DefaultAuditConfig()
		auditConfig.LogDirectory = filepath.Join(config.LogDir, "security")

		// Create SecurityAuditor for comprehensive security event logging
		auditor, err := audit.NewSecurityAuditor(auditConfig, sugar)
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
				"failed to initialize security auditor", errors.LayerAPI)
		}

		// Create auth rate limiting config
		authConfig := auth.DefaultAuthRateLimitConfig()

		// Use a default encryption key for user store (in production, this should be configurable)
		encryptionKey := "SyndrDB-UserStore-Key-2025"

		// Initialize UserStore with rate limiting and security auditing
		userStore, err := auth.NewUserStoreWithAuditor(
			userStorePath,
			encryptionKey,
			sugar,
			authConfig,
			auditor,
		)
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
				"failed to initialize user store", errors.LayerAPI).WithContext("path", userStorePath)
		}

		server.UserStore = userStore
		sugar.Infof("User authentication store initialized with brute force protection and security auditing")

		// Update ServiceManager with UserStore for RBAC functionality
		// TODO: Determine debug mode from config
		debugMode := false
		server.ServiceManager.UserService = NewUserService(
			server.ServiceManager.BundleService,
			server.ServiceManager.DatabaseService,
			userStore,
			sugar,
			debugMode,
		)
		server.ServiceManager.PermissionService = NewPermissionService(
			server.ServiceManager.BundleService,
			server.ServiceManager.DatabaseService,
			server.SessionManager,
			sugar,
			debugMode,
		)
		sugar.Info("RBAC services initialized with UserStore")
	}

	// Load all databases (already done in the setup for newDatabaseService above)
	// databases, err := databaseStore.LoadAllDatabaseDataFiles(config.DataDir, logger.Sugar())
	server.Databases = databaseService.Databases
	// if err != nil {
	// 	log.Printf("Warning: Error loading databases: %v", err)
	// 	// Continue with empty database map - this allows creating new databases
	// } else {
	// 	server.Databases = databases
	// 	log.Printf("Loaded %d databases", len(databases))
	// }

	// If no databases were found, create a default Primary database
	if (len(server.Databases) == 0 || (len(server.Databases) > 0 && server.Databases["primary"] == nil)) && config.CreateDefaultDB {

		defaultDB := &models.Database{
			DatabaseID:    helpers.GenerateUUID(),
			Name:          "primary",
			Description:   "Primary database created at startup",
			DataDirectory: config.DataDir,
			Bundles:       make(map[string]models.Bundle),
			BundleFiles:   []string{},
		}

		// Save the default database
		err := databaseStore.CreateDatabaseDataFile(defaultDB)
		if err != nil {
			log.Printf("Warning: Failed to save default database: %v", err)
		} else {
			server.Databases[defaultDB.DatabaseID] = defaultDB
			log.Printf("Created primary default database with ID %s", defaultDB.DatabaseID)
		}

		databaseService.Databases[defaultDB.Name] = defaultDB

		err = defaultdb.InitPrimaryBundleCatalogs(databaseService, databaseStore, defaultDB, sugar, bundleService)
		if err != nil {
			log.Printf("Warning: Failed to initialize default database catalogs: %v", err)
		}

		err = defaultdb.HydrateBundlesPrimaryCatalogs(databaseService, databaseStore, sugar, bundleService)
		if err != nil {
			log.Printf("Warning: Failed to hydrate default database bundle catalog: %v", err)
		}

		err = defaultdb.HydratePermissionPrimaryCatalogs(databaseService, databaseStore, sugar, bundleService)
		if err != nil {
			log.Printf("Warning: Failed to hydrate default database permission catalog: %v", err)
		}

		err = defaultdb.HydrateRolesPrimaryCatalogs(databaseService, databaseStore, sugar, bundleService)
		if err != nil {
			log.Printf("Warning: Failed to hydrate default database roles catalog: %v", err)
		}

		// Hydrate users using UserStore API (ensures Argon2id password hashing)
		err = defaultdb.HydrateUserPrimaryCatalogs(databaseService, databaseStore, sugar, bundleService, server.UserStore)
		if err != nil {
			log.Printf("Warning: Failed to hydrate default database user catalog: %v", err)
		} else {
			sugar.Info("✓ Default users (Root, Admin, Reader, Writer) created with Argon2id hashed passwords")
			sugar.Info("✓ Root user created with default password 'root' - CHANGE THIS IMMEDIATELY")
		}

		// NOTE: UpdateDefaultUserPasswords is no longer needed since HydrateUserPrimaryCatalogs
		// now uses the UserStore API directly, which handles password hashing automatically

		// Verify default users exist
		if server.UserStore != nil {
			err = defaultdb.VerifyDefaultUsersExist(server.UserStore, sugar)
			if err != nil {
				log.Printf("Warning: Failed to verify default users: %v", err)
			}
		}

		// Now register the primary database itself in the system catalog
		// This needs to happen after the Databases bundle is created
		catalogService := GetServiceManager().InternalCatalogService
		if catalogService != nil {
			err = catalogService.AddDatabaseToCatalog(defaultDB)
			if err != nil {
				log.Printf("Warning: Failed to register primary database in system catalog: %v", err)
			} else {
				log.Printf("Successfully registered primary database in system catalog")
			}
		} else {
			log.Printf("Warning: CatalogService not available, primary database not registered in system catalog")
		}

		// Force flush all pending metadata updates to ensure PageCount and other metadata are correctly calculated
		// This is critical for primary database catalogs that were just populated with documents
		err = bundleService.FlushAllBuffers()
		if err != nil {
			log.Printf("Warning: Failed to flush bundle metadata after catalog initialization: %v", err)
		} else {
			log.Printf("Successfully flushed bundle metadata after catalog initialization")
		}
	}

	// CRITICAL FIX: Synchronize all loaded databases with the catalog
	// When databases are loaded from disk on startup, they need to be registered in the catalog
	// This ensures SHOW DATABASES and USE DATABASE work correctly for pre-existing databases
	if catalogService != nil && len(server.Databases) > 0 {
		sugar.Infof("Synchronizing %d loaded databases with system catalog", len(server.Databases))
		syncCount := 0
		for _, db := range server.Databases {
			// Skip primary database - it was already registered above
			// Note: server.Databases uses DatabaseID as keys, so we check db.Name
			if strings.ToLower(db.Name) == "primary" {
				sugar.Debugf("Skipping primary database in sync loop (already registered)")
				continue
			}

			// Check if database already exists in catalog before adding
			existingDB, err := catalogService.GetDatabaseFromCatalogByName(db.Name)
			if err != nil || existingDB == nil {
				// Database not in catalog, add it
				sugar.Debugf("Database '%s' not found in catalog, adding it", db.Name)
				err = catalogService.AddDatabaseToCatalog(db)
				if err != nil {
					sugar.Warnf("Failed to register database '%s' in catalog: %v", db.Name, err)
				} else {
					syncCount++
					sugar.Debugf("Registered database '%s' in system catalog", db.Name)
				}
			} else {
				sugar.Debugf("Database '%s' already exists in catalog, skipping", db.Name)
			}
		}
		if syncCount > 0 {
			sugar.Infof("Successfully synchronized %d databases with system catalog", syncCount)

			// Flush catalog updates to ensure persistence
			err := bundleService.FlushAllBuffers()
			if err != nil {
				sugar.Warnf("Failed to flush catalog updates: %v", err)
			}
		}
	}

	// Initialize ghost cleanup worker for automatic background compaction
	// Create metrics reporter callback to avoid import cycles
	metricsReporter := func(metricName string, value uint64) {
		metrics := GetGlobalServerMetrics()
		switch metricName {
		case "GhostCleanupCyclesTotal":
			metrics.GhostCleanupCyclesTotal.Add(value)
		case "GhostRecordsScanned":
			metrics.GhostRecordsScanned.Add(value)
		case "GhostRecordsRemoved":
			metrics.GhostRecordsRemoved.Add(value)
		case "GhostCleanupDurationMs":
			metrics.GhostCleanupDurationMs.Store(value)
		case "GhostCleanupPausedForLoad":
			metrics.GhostCleanupPausedForLoad.Add(value)
		case "GhostCleanupBatchesProcessed":
			metrics.GhostCleanupBatchesProcessed.Add(value)
		case "TombstoneCacheHits":
			metrics.TombstoneCacheHits.Add(value)
		case "TombstoneCacheMisses":
			metrics.TombstoneCacheMisses.Add(value)
		case "TombstoneScansPerformed":
			metrics.TombstoneScansPerformed.Add(value)
		case "TombstoneCacheEvictions":
			metrics.TombstoneCacheEvictions.Add(value)
		case "CompactionTriggeredGhost":
			metrics.CompactionTriggeredGhost.Add(value)
		case "CompactionBlockedByLock":
			metrics.CompactionBlockedByLock.Add(value)
		case "OrphanedTempFilesRemoved":
			metrics.OrphanedTempFilesRemoved.Add(value)
		case "MVCCGCCyclesTotal":
			metrics.MVCCGCCyclesTotal.Add(value)
		case "MVCCGCBundlesScanned":
			metrics.MVCCGCBundlesScanned.Add(value)
		case "MVCCGCVersionsRemoved":
			metrics.MVCCGCVersionsRemoved.Add(value)
		case "MVCCGCCompactionsTriggered":
			metrics.MVCCGCCompactionsTriggered.Add(value)
		case "MVCCGCPausedForLoad":
			metrics.MVCCGCPausedForLoad.Add(value)
		case "MVCCGCDurationMs":
			metrics.MVCCGCDurationMs.Store(value)
		case "MVCCGCVersionsPreserved":
			metrics.MVCCGCVersionsPreserved.Add(value)
		case "MVCCGCStartupTriggers":
			metrics.MVCCGCStartupTriggers.Add(value)
		case "MVCCGCShutdownTriggers":
			metrics.MVCCGCShutdownTriggers.Add(value)
		}
	}

	// Set metrics reporter for tombstone cache
	compactor.SetTombstoneCacheMetricsReporter(metricsReporter)

	// Create compaction manager with tombstone ratio from config
	compactionStrategy := compactor.NewDefaultCompactionStrategy()
	// TODO: I will expose SetTombstoneRatioThreshold when tombstone ratio becomes configurable at runtime
	compactionManager, err := compactor.NewCompactionManager(compactor.CompactionConfig{
		DataDir:         config.DataDir,
		Strategy:        compactionStrategy,
		Enabled:         config.GhostCleanupEnabled,
		Logger:          sugar,
		MetricsReporter: metricsReporter,
	})
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
			"failed to create compaction manager", errors.LayerAPI)
	}

	// PHASE 5: MVCC - Set oldest snapshot getter for MVCC-aware compaction
	// Access WALManager through service registry to get snapshot manager
	serviceRegistry := registry.GetRegistry()
	if walManager := serviceRegistry.GetWALManager(); walManager != nil {
		snapshotMgr := walManager.GetSnapshotManager()
		if snapshotMgr != nil {
			compactionManager.SetOldestSnapshotGetter(func() uint64 {
				return snapshotMgr.GetOldestActiveSnapshot()
			})
			sugar.Debug("Compaction manager configured with MVCC snapshot awareness")
		}
	}

	// Initialize ghost cleanup worker if enabled in configuration
	if config.GhostCleanupEnabled {
		ghostCleanupWorker := compactor.NewGhostCleanupWorker(compactor.GhostCleanupConfig{
			Interval:             time.Duration(config.GhostCleanupIntervalSeconds) * time.Second,
			BatchSize:            config.GhostCleanupBatchSize,
			PauseThreshold:       config.GhostCleanupPauseThreshold,
			DataDir:              config.DataDir,
			ServiceManagerGetter: func() interface{} { return GetServiceManager() },
			ActiveQueryCount:     &server.activeQueryCount,
			Compactor:            compactionManager,
			MetricsReporter:      metricsReporter,
			Logger:               sugar,
		})

		server.GhostCleanupWorker = ghostCleanupWorker

		// Start the ghost cleanup worker
		ghostCleanupWorker.Start()
		sugar.Infow("Ghost cleanup worker started for automatic background compaction",
			"interval", config.GhostCleanupIntervalSeconds,
			"batchSize", config.GhostCleanupBatchSize,
			"pauseThreshold", config.GhostCleanupPauseThreshold,
			"tombstoneRatio", config.GhostCleanupTombstoneRatio,
		)
	} else {
		sugar.Info("Ghost cleanup worker disabled in configuration")
	}

	// Initialize MVCC GC worker if enabled in configuration
	if config.MVCCGCEnabled {
		mvccGCWorker := compactor.NewMVCCGCWorker(compactor.MVCCGCConfig{
			Interval:             time.Duration(config.MVCCGCIntervalSeconds) * time.Second,
			BatchSize:            config.MVCCGCBatchSize,
			PauseThreshold:       config.MVCCGCPauseThreshold,
			MaxVersionsThreshold: config.MVCCGCMaxVersionsThreshold,
			MinVersionAge:        time.Duration(config.MVCCGCMinVersionAgeHours) * time.Hour,
			DataDir:              config.DataDir,
			ServiceManagerGetter: func() interface{} { return GetServiceManager() },
			SnapshotManagerGetter: func() interface{} {
				serviceRegistry := registry.GetRegistry()
				if walManager := serviceRegistry.GetWALManager(); walManager != nil {
					return walManager.GetSnapshotManager()
				}
				return nil
			},
			ActiveQueryCount: &server.activeQueryCount,
			Compactor:        compactionManager,
			MetricsReporter:  metricsReporter,
			Logger:           sugar,
		})

		server.MVCCGCWorker = mvccGCWorker

		// Start the MVCC GC worker
		mvccGCWorker.Start()
		sugar.Infow("MVCC GC worker started for automatic version cleanup",
			"interval", config.MVCCGCIntervalSeconds,
			"batchSize", config.MVCCGCBatchSize,
			"pauseThreshold", config.MVCCGCPauseThreshold,
			"maxVersionsThreshold", config.MVCCGCMaxVersionsThreshold,
			"minVersionAgeHours", config.MVCCGCMinVersionAgeHours,
		)

		// Trigger immediate GC on startup
		mvccGCWorker.TriggerImmediateGC("startup")
	} else {
		sugar.Info("MVCC GC worker disabled in configuration")
	}

	return server, nil
}

// Start begins listening for incoming connections
func (s *Server) Start() error {
	// Initialize trace helper if enabled via environment variable
	enableTrace := os.Getenv("ENABLE_TRACE") == "true"
	if err := InitTrace(s.logger, enableTrace); err != nil {
		s.logger.Warnf("Failed to initialize tracing: %v", err)
	}
	if enableTrace {
		defer StopTrace()
		s.logger.Infof("🔍 Execution tracing ENABLED - capturing detailed performance data")
	}

	// Start TCP server
	addr := fmt.Sprintf("%s:%d", s.Host, s.Port)

	var listener net.Listener
	var err error

	if s.TLSConfig != nil {
		// Start TLS server
		listener, err = tls.Listen("tcp", addr, s.TLSConfig)
		if err != nil {
			return errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
				fmt.Sprintf("error starting TLS server on %s", addr), errors.LayerAPI).WithContext("address", addr)
		}
		log.Printf("SyndrDB TLS server listening on %s", addr)
		s.logger.Infof("Server started with TLS encryption on %s", addr)
	} else {
		// Start regular TCP server
		listener, err = net.Listen("tcp", addr)
		if err != nil {
			return errors.WrapWithMessage(err, errors.ERR_SYSTEM_INIT,
				fmt.Sprintf("error starting TCP server on %s", addr), errors.LayerAPI).WithContext("address", addr)
		}
		log.Printf("SyndrDB TCP server listening on %s", addr)
		s.logger.Warnf("Server started WITHOUT encryption on %s - consider enabling TLS", addr)
	}

	s.Listener = listener
	s.Running = true

	// GraphQL is now handled via TCP socket with GRAPHQL:: prefix
	if s.GraphQLEnabled {
		s.logger.Info("GraphQL enabled for TCP connections - use GRAPHQL:: prefix in commands")
	}

	// TRANSACTION SUPPORT: Start background goroutine to cleanup orphaned locks
	// This runs every 60 seconds to remove locks from transactions that failed without
	// proper cleanup (server crashes, client disconnects, etc.)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fatal.LogFatalAndExit(r)
			}
		}()
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if !s.Running {
					return
				}
				// Get active sessions for lock cleanup
				// Convert transaction map to session ID set expected by CleanupOrphanedLocks
				txMap := s.SessionManager.GetActiveSessionsByTxID()
				activeSessionIDs := make(map[string]bool, len(txMap))
				for _, sessionID := range txMap {
					activeSessionIDs[sessionID] = true
				}

				// Call cleanup with active session IDs
				if s.ServiceManager.LockManager != nil {
					cleanedLocks, cleanedSessions := s.ServiceManager.LockManager.CleanupOrphanedLocks(activeSessionIDs)
					if cleanedLocks > 0 {
						s.logger.Infof("Cleaned up %d orphaned locks from %d inactive sessions", cleanedLocks, cleanedSessions)
					}
				}
			}
		}
	}()
	s.logger.Info("Transaction lock cleanup worker started (60s interval)")

	go s.acceptConnections()

	return nil
}

// Stop gracefully shuts down the server
func (s *Server) Stop() error {
	s.Running = false

	// Trigger immediate GC on shutdown before stopping workers
	if s.MVCCGCWorker != nil {
		s.logger.Info("Triggering immediate MVCC GC on shutdown...")
		s.MVCCGCWorker.TriggerImmediateGC("shutdown")
	}

	// Stop MVCC GC worker
	if s.MVCCGCWorker != nil {
		s.MVCCGCWorker.Stop()
		s.logger.Info("MVCC GC worker stopped")
	}

	// Stop ghost cleanup worker to prevent background compaction during shutdown
	if s.GhostCleanupWorker != nil {
		s.GhostCleanupWorker.Stop()
		s.logger.Info("Ghost cleanup worker stopped")
	}

	// Optional: clean up index temp files (*.tmp, *.idx.tmp, *.compact.tmp) on shutdown
	bundle.CleanupIndexTempFiles(settings.GetSettings().DataDir, s.logger)

	// Stop session manager first to cleanup all sessions
	if s.SessionManager != nil {
		s.SessionManager.Stop()
	}

	// Close UserStore and rate limiter
	if s.UserStore != nil {
		s.UserStore.Close()
	}

	// Send shutdown warning to all active connections and close them
	// TODO: I could add a configurable grace period to allow clients to finish in-flight operations
	s.mu.Lock()
	for id, conn := range s.ActiveConnections {
		// Best effort: send shutdown message to client
		if conn.Writer != nil {
			fmt.Fprintf(conn.Writer, "Server shutting down, your connection is being closed\n")
			conn.Writer.Flush()
		}
		conn.Conn.Close()
		delete(s.ActiveConnections, id)
	}
	s.mu.Unlock()

	// Close the TCP listener
	if s.Listener != nil {
		err := s.Listener.Close()
		if err != nil {
			return err
		}
	}

	s.wg.Wait()

	// Flush any dirty pages
	s.bufferPool.FlushAllDirty()

	// Shutdown bundle service to flush all pending operations
	if s.ServiceManager != nil && s.ServiceManager.BundleService != nil {
		if err := s.ServiceManager.BundleService.Shutdown(); err != nil {
			s.logger.Warnf("Error during bundle service shutdown: %v", err)
		}
	}

	// Close the buffer pool & Release buffer memory
	err := s.bufferPool.ShutDown()
	if err != nil {
		s.logger.Warnf("Error during buffer pool shutdown: %v", err)
	}
	// Close open files

	// Flush any buffered log entries
	s.logger.Info("Server shutdown complete")
	s.logger.Sync()

	return nil
}

// GetConnectionPoolStats returns detailed statistics about the connection pool
// TODO: I could add average idle time, peak connection count, and connection churn rate metrics
func (s *Server) GetConnectionPoolStats() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	connections := make([]map[string]interface{}, 0, len(s.ActiveConnections))

	for _, conn := range s.ActiveConnections {
		idleSeconds := now.Sub(conn.LastActive).Seconds()
		connInfo := map[string]interface{}{
			"id":               conn.ID,
			"last_active_unix": conn.LastActive.Unix(),
			"idle_seconds":     int64(idleSeconds),
			"database":         conn.DatabaseName,
			"user":             conn.User,
			"authorized":       conn.Authorized,
		}
		connections = append(connections, connInfo)
	}

	return map[string]interface{}{
		"active_count":         len(s.ActiveConnections),
		"max_connections":      s.MaxConnections,
		"idle_timeout_seconds": int64(s.ConnectionIdleTimeout.Seconds()),
		"connections":          connections,
	}
}

// GetLogger returns the server's logger
func (s *Server) GetLogger() *zap.SugaredLogger {
	return s.logger
}

// AddUser adds a user with the given password
func (s *Server) AddUser(username, password string) {
	hashedPassword := hashPassword(password)
	s.Users[username] = hashedPassword
}

// Authentication function with brute force protection
func (s *Server) authenticate(username, password string) bool {
	// Use new UserStore if available
	if s.UserStore != nil {
		isValid, _, err := s.UserStore.VerifyCredentials(username, password)
		if err != nil {
			s.logger.Warnw("Authentication error", "username", username, "error", err)
			return false
		}
		return isValid
	}

	// Fallback to legacy authentication
	hashedPassword, exists := s.Users[username]
	if !exists {
		return false
	}

	return verifyPassword(password, hashedPassword)
}

// authenticateWithIP provides authentication with IP-based rate limiting
func (s *Server) authenticateWithIP(username, password, clientIP string) error {
	// Use new UserStore if available
	if s.UserStore != nil {
		isValid, _, err := s.UserStore.VerifyCredentialsWithIP(username, password, clientIP)
		if err != nil {
			// Check if this is a delay error and apply the delay
			if authErr, ok := err.(*auth.AuthLockoutError); ok && authErr.Type == "delay" {
				s.logger.Infow("Applying progressive authentication delay",
					"username", username,
					"ip", clientIP,
					"delay", authErr.Delay)

				// Actually wait for the delay
				time.Sleep(authErr.Delay)
			}

			// Return the error (rate limiting, lockout, etc.)
			return err
		}
		if !isValid {
			return errors.New(errors.ERR_AUTH_FAILED, "invalid credentials", errors.LayerAuth)
		}
		return nil
	}

	// Fallback to legacy authentication (no rate limiting)
	if !s.authenticate(username, password) {
		return errors.New(errors.ERR_AUTH_FAILED, "authentication failed", errors.LayerAuth)
	}
	return nil
}

// acceptConnections handles incoming connection requests
func (s *Server) acceptConnections() {
	defer func() {
		if r := recover(); r != nil {
			fatal.LogFatalAndExit(r)
		}
	}()
	s.logger.Info("Server started accepting connections",
		zap.String("host", s.Host),
		zap.Int("port", s.Port))

	for s.Running {
		conn, err := s.Listener.Accept()
		if err != nil {
			if s.Running { // Only log if we're still supposed to be running
				s.logger.Errorw("Error accepting connection", "error", err)
			}
			continue
		}

		// Check connection pool limit
		s.mu.Lock()
		activeCount := len(s.ActiveConnections)
		s.mu.Unlock()

		if activeCount >= s.MaxConnections {
			// TODO: I could add metrics tracking for rejected connections to monitor pool pressure
			s.logger.Warnw("Connection rejected - pool limit reached",
				"active", activeCount,
				"max", s.MaxConnections,
				"remoteAddr", conn.RemoteAddr().String())
			// Best effort: send error message to client before closing
			writer := bufio.NewWriter(conn)
			fmt.Fprintf(writer, "ERROR: Server has reached maximum connection limit (%d)\n", s.MaxConnections)
			writer.Flush()
			conn.Close()
			continue
		}

		// Rate limiting check
		clientIP := ExtractIPFromConn(conn)
		if err := s.RateLimiter.CheckConnection(clientIP); err != nil {
			s.logger.Warnw("Connection rejected due to rate limiting",
				"ip", clientIP,
				"error", err)
			conn.Close()
			continue
		}

		s.wg.Add(1)

		s.logger.Info("New connection accepted",
			zap.String("remoteAddr", conn.RemoteAddr().String()),
			zap.String("clientIP", clientIP))

		// Handle each connection in a new goroutine. ReleaseConnection is called in
		// handleConnection's defer to avoid double-release (was here and in handleConnection).
		go func(c net.Conn) {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fatal.LogFatalAndExit(r)
				}
			}()
			s.handleConnection(c)
		}(conn)
	}
}

// handleConnection processes a single client connection
func (s *Server) handleConnection(conn net.Conn) {
	connID := generateConnectionID()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Create a connection-specific logger with connection ID context
	// Create a connection-specific logger with connection ID context
	connLogger := s.logger

	if settings.GetSettings().UserDebug {
		connLogger = connLogger.With(
			zap.String("connID", connID),
			zap.String("remoteAddr", conn.RemoteAddr().String()))
	}

	connection := &Connection{
		ID:         connID,
		Conn:       conn,
		Reader:     reader,
		Writer:     writer,
		Authorized: true, //!s.AuthEnabled, // If auth is disabled, connection is automatically authorized
		LastActive: time.Now(),
		Logger:     connLogger,
	}

	// Register the connection
	s.mu.Lock()
	s.ActiveConnections[connID] = connection
	s.mu.Unlock()

	// Ensure connection is removed when this function exits
	defer func() {
		// TODO: I could add connection lifetime metrics here for monitoring
		clientIP := ExtractIPFromConn(conn)

		// Invalidate the session associated with this connection before cleanup
		// This prevents race conditions where commands try to validate sessions after connection drops
		if connection.Session != nil {
			if err := s.SessionManager.InvalidateSession(connection.Session.SessionID); err != nil {
				// Log but don't fail - session may have already been cleaned up
				connLogger.Debugw("Session already invalidated or not found during connection cleanup",
					"sessionID", connection.Session.SessionID,
					"error", err)
			}
		}

		// Release from rate limiter FIRST (before removing from ActiveConnections)
		s.RateLimiter.ReleaseConnection(clientIP)

		// Then remove from active connections
		conn.Close()
		s.mu.Lock()
		delete(s.ActiveConnections, connID)
		s.mu.Unlock()
		connLogger.Info("Connection closed: %s", connID)
		connLogger.Sync()
	}()

	// Channel for received data
	dataCh := make(chan string)
	// Channel for errors
	errCh := make(chan error)
	// Done channel to signal termination
	doneCh := make(chan struct{})

	// Start a goroutine for reading
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fatal.LogFatalAndExit(r)
			}
		}()
		defer close(dataCh)
		defer close(errCh)

		buffer := make([]byte, constants.BufferSizeDefault)
		var partialData string // For storing incomplete data between reads

		for {
			select {
			case <-doneCh:
				return
			default:
				// Set a short deadline to avoid blocking
				conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

				// Try to read available data
				n, err := conn.Read(buffer)

				// Reset deadline
				conn.SetReadDeadline(time.Time{})

				if err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						// No data available, just continue
						continue
					}

					// Handle real errors (EOF, connection closed, etc.)
					errCh <- err
					return
				}

				if n > 0 {
					data := string(buffer[:n])

					connLogger.Sync()

					// Append to any previous partial data
					partialData += data

					// Split on CommandTerminator, respecting \x04\x04 in parameterized values.
					// Sends each complete command separately to avoid bunching when TCP
					// coalesces multiple commands into one read.
					complete, incomplete := splitOnCommandTerminator(partialData)
					for _, cmd := range complete {
						dataCh <- cmd
					}
					partialData = incomplete

					// if strings.Contains(data, "\n") {
					// 	lines := strings.Split(data, "\n")
					// 	for i := 0; i < len(lines); i++ {
					// 		line := strings.TrimSpace(lines[i])
					// 		if line != "" {
					// 			partialData += line + "\n" // accumulate lines, preserve newlines if needed
					// 			if strings.HasSuffix(line, ";") {
					// 				// Command is complete
					// 				cmd := strings.TrimSpace(partialData)
					// 				dataCh <- cmd
					// 				partialData = "" // reset for next command
					// 			}
					// 		}
					// 	}
					// } else {
					// 	// No newline, accumulate as partial
					// 	partialData += data
					// }

				} else {
					connLogger.Info("No Data Seen")
					connLogger.Sync()
				}
			}
		}
	}()

	// Send welcome message
	writer.WriteString(fmt.Sprintf("%s\n", data.Welcome))
	writer.Flush()

	// Main processing loop
	for {
		select {
		case line, ok := <-dataCh:
			if !ok {
				// Channel closed

				goto cleanup
			}
			// Process the line
			//connLogger.Infof("DEBUG DEBUG DEBUG \n\n\n Received: %s", line)

			// Your existing logic for handling commands
			//When the client connects, it should send the connection string
			//as the first command.
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "syndrdb://") {
				connLogger.Debug("Reading connection string")
				//TODO fix this - its sending the connection string attached to the first command sent.
				connStr, err := parseConnectionString(s, line)
				if err != nil {
					connLogger.Errorw("Error parsing connection string", "error", err, "input", line)
					connLogger.Sync()
					s.sendError(writer, errors.Wrap(err, errors.ERR_VALIDATION_SYNTAX, errors.LayerAPI).WithContext("input", line))
					// Give TCP stack time to send the data
					time.Sleep(100 * time.Millisecond)

					close(doneCh)
					return
				}

				connLogger.Infof("Client %s Connected", connection.ID)
				connLogger.Infof("Database: %s", connStr.Database)
				connLogger.Infof("User: %s", connStr.Username)

				connLogger.Sync()

				// Read client input

				//line = strings.TrimSpace(line)
				connection.LastActive = time.Now()
				connection.DatabaseName = connStr.Database
				//connection.Database = s.Databases[connStr.Database]

				connection.User = connStr.Username

				//if connection.Authorized { }

				//if !strings.EqualFold(connStr.Database, "primary") {
				db, err := s.databaseService.GetDatabaseByName(connStr.Database)
				if err != nil {
					s.sendError(writer, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, fmt.Sprintf("Database '%s' not found", connStr.Database), errors.LayerAPI))
					return
				}
				if db == nil {
					s.sendError(writer, errors.New(errors.ERR_NOT_FOUND_DATABASE, fmt.Sprintf("Database '%s' does not exist", connStr.Database), errors.LayerAPI))
					return
				}
				connection.Database = db
				//}

				// TODO: IF the db is legit, check to see if the user is allowed to access it
				clientIP := ExtractIPFromConn(connection.Conn)

				// Use enhanced authentication with rate limiting and progressive delays
				if s.AuthEnabled {
					err := s.authenticateWithIP(connStr.Username, connStr.Password, clientIP)
					if err != nil {
						// Check if this is a rate limiting error for better user feedback
						if authErr, ok := err.(*auth.AuthLockoutError); ok {
							var authError errors.SyndrDBError
							switch authErr.Type {
							case "user":
								authError = errors.New(errors.ERR_AUTH_LOCKOUT, fmt.Sprintf("Account locked until %s due to too many failed attempts", authErr.LockedUntil.Format("15:04:05")), errors.LayerAuth)
							case "ip":
								authError = errors.New(errors.ERR_AUTH_RATE_LIMIT, fmt.Sprintf("IP address blocked until %s due to suspicious activity", authErr.LockedUntil.Format("15:04:05")), errors.LayerAuth)
							case "delay":
								authError = errors.New(errors.ERR_AUTH_RATE_LIMIT, fmt.Sprintf("Too many attempts. Please wait %s before trying again", authErr.Delay.String()), errors.LayerAuth)
							default:
								authError = errors.New(errors.ERR_AUTH_RATE_LIMIT, "Authentication blocked due to security restrictions", errors.LayerAuth)
							}
							s.sendError(writer, authError)
						} else {
							s.sendError(writer, errors.New(errors.ERR_AUTH_FAILED, "Authentication failed", errors.LayerAuth))
						}
						return
					}
				}

				// Create a session for the authenticated user with IP binding
				connectionFingerprint := ExtractConnectionFingerprint(connection.Conn)

				session, err := s.SessionManager.CreateSession(
					connStr.Username,
					connStr.Username, // Using username as userID for now
					connStr.Database,
					connection.Database,
					connection.ID,
					s.SessionTimeout,
					clientIP,
					connectionFingerprint,
				)
				if err != nil {
					s.sendError(writer, errors.WrapWithMessage(err, errors.ERR_INTERNAL, "Failed to create session", errors.LayerAPI))
					return
				}

				connection.Authorized = true
				connection.DatabaseName = connStr.Database
				connection.User = connStr.Username
				connection.Session = session
				connection.Logger = connLogger.Desugar().Sugar()

				connLogger.Infow("Client authenticated and session created",
					"user", connection.User,
					"database", connection.DatabaseName,
					"sessionID", session.SessionID)

				sendSuccess(writer, fmt.Sprintf("Authentication successful - Session: %s", session.SessionID))
				continue

			}

			// Process command for authenticated clients
			//log.Printf("Processing command from %s: %s", connection.ID, line)
			result, err := s.processCommand(connection, line)
			if err != nil {
				// Convert error to SyndrDBError and send using error framework
				s.sendError(writer, err)
			} else {
				sendResult(writer, result, connLogger)
			}
		case err, ok := <-errCh:
			if !ok {
				// Channel closed
				connLogger.Errorw("Error reading from client", "error", err)
				connLogger.Sync()
				goto cleanup
			}
			//connLogger.Errorw("Error reading from client", "error", err)
			return

		case <-time.After(300 * time.Second):
			// Check for idle timeout (5-minute checks, but enforce configured timeout)
			idleTime := time.Since(connection.LastActive)
			if idleTime >= s.ConnectionIdleTimeout {
				// TODO: I could add configurable warning threshold before timeout (e.g., send warning at 80% of timeout)
				connLogger.Infow("Connection closed due to inactivity",
					"idle_duration", idleTime.String(),
					"timeout", s.ConnectionIdleTimeout.String())
				// Best effort: send warning message to client
				fmt.Fprintf(writer, "Connection closed due to inactivity\n")
				writer.Flush()
				return // Triggers defer cleanup which handles RateLimiter.ReleaseConnection
			}
			// Still within timeout - log activity check
			connLogger.Debugw("Connection idle check",
				"idle_duration", idleTime.String(),
				"timeout", s.ConnectionIdleTimeout.String())
		}
	}

cleanup:
	// Cleanup
	close(doneCh)

}

// Process a client command
func (s *Server) processCommand(conn *Connection, command string) (interface{}, error) {
	// Rate limiting check for requests
	clientIP := ExtractIPFromConn(conn.Conn)
	if err := s.RateLimiter.CheckRequest(clientIP); err != nil {
		s.logger.Warnw("Request rejected due to rate limiting",
			"ip", clientIP,
			"user", conn.User,
			"error", err)
		return nil, errors.New(errors.ERR_RESOURCE_EXHAUSTED, fmt.Sprintf("Rate limit exceeded: %v", err), errors.LayerAPI)
	}

	// Check if command is empty (don't use strings.Fields as it breaks multi-line commands)
	if strings.TrimSpace(command) == "" {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX, "Empty command", errors.LayerCommand)
	}
	// Use the new function to process and print the client data
	// Convert any errors returned from ProcessClientData to SyndrDBError if needed
	result, err := s.ProcessClientData(conn, command)
	if err != nil {
		// If error is already SyndrDBError, return as-is
		if _, ok := err.(errors.SyndrDBError); ok {
			return result, err
		}
		// Otherwise, convert to SyndrDBError
		return result, errors.ConvertError(err, errors.LayerCommand)
	}
	return result, err
}

func (s *Server) ProcessClientData(conn *Connection, data string) (interface{}, error) {
	// Log the received data
	// Get logger with connection context
	//logger := s.logger.With("connID", conn.ID)

	// Log the received data
	//logger.Infow("Received from client", "data", data)

	// If not JSON, treat as plain text command
	//fmt.Printf("\n--- Client Data (Plain Text) ---\n%s\n------------------------------\n", data)

	// Check if command is empty (don't use strings.Fields as it breaks multi-line commands)
	if strings.TrimSpace(data) == "" {
		return map[string]interface{}{
			"status":    "received",
			"message":   "Empty command",
			"timestamp": time.Now().Format(time.RFC3339),
		}, nil
	}

	// Process the command
	return s.handleTextCommand(conn, data) // parts[1:]}

}

// handleTextCommand processes commands received in plain text format
func (s *Server) handleTextCommand(conn *Connection, command string) (interface{}, error) {
	// Check if server is shutting down - reject new commands during shutdown
	if !s.Running {
		return nil, errors.New(errors.ERR_SYSTEM_SHUTDOWN,
			"server is shutting down", errors.LayerAPI)
	}

	serviceManager := GetServiceManager()

	// DEBUG: Log exact command received from client with boundaries
	//s.logger.Infof("[CLIENT INPUT] Received command (length=%d): |%s|", len(command), command)

	// Update session activity with security validation
	// Skip validation if server is shutting down to avoid errors from cleaned-up sessions
	if conn.Session != nil {
		clientIP := ExtractIPFromConn(conn.Conn)
		connectionFingerprint := ExtractConnectionFingerprint(conn.Conn)

		err := s.SessionManager.UpdateActivity(conn.Session.SessionID, clientIP, connectionFingerprint)
		if err != nil {
			// During shutdown, sessions may be cleaned up before connections close
			// Check if server is still running - if not, return graceful shutdown message
			if !s.Running {
				return nil, errors.New(errors.ERR_SYSTEM_SHUTDOWN,
					"server is shutting down", errors.LayerAPI)
			}
			s.logger.Warnw("Session security validation failed",
				"sessionID", conn.Session.SessionID,
				"error", err,
				"clientIP", clientIP)
			// Return security error - session may be compromised
			return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_FIELD,
				"session security validation failed", errors.LayerAuth).WithContext("session_id", conn.Session.SessionID).WithContext("client_ip", clientIP)
		}

		// Generate a query ID and start tracking the query
		queryID := fmt.Sprintf("q_%d", time.Now().UnixNano())
		conn.Session.StartQuery(queryID, command)
	}

	//s.logger.Infof("Debugging the command received: %s", command)
	//s.logger.Sync()

	stats := s.bufferPool.GetStats()
	s.logger.Debugf("Buffer stats before command: hits=%d, misses=%d, ratio=%.2f, used=%d/%d",
		stats.Hits, stats.Misses, stats.HitRatio, stats.UsedBuffers, stats.TotalBuffers)

	// Start timing for execution measurement
	startTime := time.Now()

	// Extract client IP for command processing (reuse if session exists)
	clientIP := ""
	if conn.Session != nil {
		clientIP = ExtractIPFromConn(conn.Conn)
	}

	// Create base context for query execution
	ctx := context.Background()

	// Execute command with context support
	result, err := CommandDirector(ctx, conn.Database, *serviceManager, command, s.logger, startTime, conn.Session, clientIP)

	stats = s.bufferPool.GetStats()
	s.logger.Debugf("Buffer stats after command: hits=%d, misses=%d, ratio=%.2f, used=%d/%d",
		stats.Hits, stats.Misses, stats.HitRatio, stats.UsedBuffers, stats.TotalBuffers)

	// Handle USE command - update session database context after successful execution
	if err == nil && conn.Session != nil && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(command)), "USE") {
		// Parse the database name from the USE command
		re := regexp.MustCompile(`(?i)use\s+"([^"]+)"`)
		matches := re.FindStringSubmatch(command)
		if len(matches) >= 2 {
			databaseName := matches[1]

			// Get the database object
			targetDatabase, dbErr := serviceManager.DatabaseService.GetDatabaseByName(databaseName)
			if dbErr == nil {
				// Update the session's database context
				sessionErr := s.SessionManager.SetDatabaseContext(conn.Session.SessionID, databaseName, targetDatabase)
				if sessionErr == nil {
					// Also update the connection's database reference for immediate use
					conn.Database = targetDatabase
					conn.DatabaseName = databaseName
					s.logger.Debugf("Session database context updated to db: %s", databaseName)
				} else {
					s.logger.Warnw("Failed to update session database context",
						"sessionID", conn.Session.SessionID,
						"database", databaseName,
						"error", sessionErr)
				}
			}
		}
	}

	// Update session with query result
	if conn.Session != nil {
		if err != nil {
			conn.Session.FailQuery(err)
		} else {
			// Try to extract affected rows from result if possible
			affectedRows := 0
			if cmdResp, ok := result.(*CommandResponse); ok {
				affectedRows = cmdResp.ResultCount
			}
			conn.Session.CompleteQuery(affectedRows)
		}
	}

	return result, err
}

func parseConnectionString(server *Server, connStr string) (ConnectionString, error) {
	result := ConnectionString{
		Options: make(map[string]string),
	}
	server.logger.Infof("Parsing Connection Strin: %s", connStr)
	// Strip syndr:// prefix if present

	// if strings.HasPrefix(connStr, prefix) {
	// 	connStr = connStr[len(prefix):]
	// }

	connStr = strings.TrimPrefix(connStr, "syndrdb://")
	// Extract options
	optionsParts := strings.Split(connStr, ":")
	// The Connection String is like this: host:port:database:username:password
	result.Host = optionsParts[0]
	// Convert port string to integer
	portNum, err := strconv.Atoi(optionsParts[1])
	if err != nil {
		return result, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("invalid port number: %v", err), errors.LayerAPI).WithContext("port", optionsParts[1])
	}
	result.Port = portNum
	result.Database = optionsParts[2]

	if result.Database == "" {
		return result, errors.New(errors.ERR_VALIDATION_REQUIRED,
			"database name cannot be empty", errors.LayerAPI)
	}

	// Check to make sure the database exists
	if !DatabaseExists(server.Databases, result.Database) {
		return result, errors.New(errors.ERR_NOT_FOUND_DATABASE,
			fmt.Sprintf("invalid database name: %s", result.Database), errors.LayerAPI).WithContext("database", result.Database)
	}

	result.Username = optionsParts[3]
	result.Password = optionsParts[4]
	// TODO Check to make sure the user exists
	// TODO Check to make sure the user has access to the database

	return result, nil
}

func DatabaseExists(databases map[string]*models.Database, dbName string) bool {
	for _, db := range databases {
		if strings.EqualFold(db.Name, dbName) {
			return true
		}
	}
	return false
}

// Helper functions
// sendError sends an error response to the client
// Accepts either a string message or a SyndrDBError
// If a SyndrDBError is provided, it's logged and sanitized before sending
func (s *Server) sendError(writer *bufio.Writer, err interface{}) {
	var sdbErr errors.SyndrDBError
	var response map[string]interface{}

	// Convert error to SyndrDBError if needed
	switch v := err.(type) {
	case errors.SyndrDBError:
		sdbErr = v
	case error:
		// Convert standard error to SyndrDBError
		sdbErr = errors.ConvertError(v, errors.LayerAPI)
	case string:
		// Legacy string error - convert to SyndrDBError
		sdbErr = errors.New(errors.ERR_INTERNAL, v, errors.LayerAPI)
	default:
		// Unknown type - create generic error
		sdbErr = errors.New(errors.ERR_INTERNAL, fmt.Sprintf("%v", err), errors.LayerAPI)
	}

	// Log the error using the error framework
	if s.errorLogger != nil {
		s.errorLogger.LogError(sdbErr)
	}

	// Format user response (sanitized)
	errorResponse := errors.FormatUserResponse(sdbErr)
	response = map[string]interface{}{
		"status": "error",
		"error":  errorResponse,
	}

	jsonResponse, _ := json.Marshal(response)
	writer.WriteString(string(jsonResponse) + "\n")
	writer.Flush()
}

// sendErrorLegacy maintains backward compatibility for callers using string errors
// DEPRECATED: Use sendError with SyndrDBError instead
func sendError(writer *bufio.Writer, message string) {
	response := map[string]interface{}{
		"status":  "error",
		"message": message,
	}
	jsonResponse, _ := json.Marshal(response)
	writer.WriteString(string(jsonResponse) + "\n")
	writer.Flush()
}

func sendSuccess(writer *bufio.Writer, message string) {
	response := map[string]interface{}{
		"status":  "success",
		"message": message,
	}
	jsonResponse, _ := json.Marshal(response)
	writer.WriteString(string(jsonResponse) + "\n")
	writer.Flush()
}

func sendResult(writer *bufio.Writer, result interface{}, logger *zap.SugaredLogger) {
	var data []byte

	// PHASE A: Return pooled maps after JSON marshaling (no closure = no allocation)
	defer func() {
		if cmdResp, ok := result.(*CommandResponse); ok {
			if len(cmdResp.PooledMaps) > 0 {
				helpers.FreeResultSet(cmdResp.PooledMaps)
			}
			// STEP 1: Return pooled documents after response sent
			if len(cmdResp.PooledDocuments) > 0 {
				document.FreeDocuments(cmdResp.PooledDocuments)
			}
		}
	}()

	switch typedResult := result.(type) {
	case *CommandResponse:
		// PHASE H: Check if we can use streaming encoder
		if len(typedResult.StreamDocuments) > 0 {
			// Stream documents directly to JSON without intermediate maps
			writer.WriteString("{\"ResultCount\":")
			writer.WriteString(strconv.Itoa(typedResult.ResultCount))
			writer.WriteString(",\"Result\":")

			// Stream the documents array
			err := helpers.StreamDocumentsToJSON(writer, typedResult.StreamDocuments, typedResult.StreamFields)
			if err != nil {
				logger.Errorf("Failed to stream documents: %v", err)
				// Fallback to regular marshaling
				data, _ = json.Marshal(result)
				writer.WriteString(string(data) + "\n")
				writer.Flush()
				return
			}

			writer.WriteString(",\"ExecutionTimeMS\":")
			writer.WriteString(strconv.FormatFloat(typedResult.ExecutionTimeMS, 'f', 2, 64))
			writer.WriteString("}\n")
			writer.Flush()
			return
		}
		// Not streaming - use regular JSON marshaling
		data, _ = json.Marshal(result)
		writer.WriteString(string(data) + "\n")
		writer.Flush()
	case *string:
		if typedResult != nil {
			// For string pointers, just write the string directly
			writer.WriteString(*typedResult + "\n")
			writer.Flush()
			return
		}
	case string:
		// For direct strings
		writer.WriteString(typedResult + "\n")
		writer.Flush()
		return
	default:
		// For other types, marshal to JSON

		data, _ = json.Marshal(result)
		//logger.Infof("Sending result: %s", data)

		logger.Sync()
		writer.WriteString(string(data) + "\n")
		writer.Flush()
	}
}

// Secure password hashing using Argon2id
type PasswordHash struct {
	Hash    []byte
	Salt    []byte
	Time    uint32
	Memory  uint32
	Threads uint8
	KeyLen  uint32
}

// hashPassword securely hashes a password using Argon2id
func hashPassword(password string) string {
	// Generate a random salt
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		// Fallback to less secure but functional method
		return hashPasswordSHA256(password)
	}

	// Argon2id parameters (recommended values)
	time := uint32(1)           // Number of iterations
	memory := uint32(64 * 1024) // Memory usage in KiB (64 MB)
	threads := uint8(4)         // Number of threads
	keyLen := uint32(32)        // Length of the derived key

	// Generate the hash
	hash := argon2.IDKey([]byte(password), salt, time, memory, threads, keyLen)

	// Encode salt and hash together (salt:hash format)
	encoded := fmt.Sprintf("%s:%s", hex.EncodeToString(salt), hex.EncodeToString(hash))
	return encoded
}

// verifyPassword verifies a password against its hash
func verifyPassword(password, encodedHash string) bool {
	// Handle legacy SHA-256 hashes (for backward compatibility)
	if !strings.Contains(encodedHash, ":") {
		return encodedHash == hashPasswordSHA256(password)
	}

	// Parse salt and hash
	parts := strings.Split(encodedHash, ":")
	if len(parts) != 2 {
		return false
	}

	salt, err := hex.DecodeString(parts[0])
	if err != nil {
		return false
	}

	expectedHash, err := hex.DecodeString(parts[1])
	if err != nil {
		return false
	}

	// Use same parameters as hashing
	time := uint32(1)
	memory := uint32(64 * 1024)
	threads := uint8(4)
	keyLen := uint32(32)

	// Generate hash from provided password
	hash := argon2.IDKey([]byte(password), salt, time, memory, threads, keyLen)

	// Compare hashes (constant time comparison)
	return subtle.ConstantTimeCompare(hash, expectedHash) == 1
}

// Legacy SHA-256 hashing (for backward compatibility only)
func hashPasswordSHA256(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

func generateConnectionID() string {
	now := time.Now().UnixNano()
	return fmt.Sprintf("conn_%x", now)
}
