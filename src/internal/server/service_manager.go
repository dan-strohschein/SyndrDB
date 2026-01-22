package server

import (
	"sync"
	"sync/atomic"
	"syndrdb/src/internal/auth"
	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/migration"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/lock"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/registry"
	"syndrdb/src/internal/storage"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// MigrationServiceInterface defines the migration service operations needed by command handlers
type MigrationServiceInterface interface {
	CreateMigration(cmd interface{}) (interface{}, error)
	ApplyMigration(databaseName string, version int, force bool) error
	RollbackToVersion(databaseName string, targetVersion int) error
	ValidateMigration(databaseName string, version int, validatedBy string) (interface{}, error)
	ValidateRollback(databaseName string, targetVersion int, validatedBy string) (interface{}, error)
	ListMigrations(databaseName string, filters map[string]interface{}) (interface{}, error)
	GetCurrentVersion(databaseName string) (int, error)
}

// GraphQLProcessor defines the interface for processing GraphQL commands
type GraphQLProcessor interface {
	ProcessGraphQLCommand(command string, session *Session, clientIP string) (interface{}, error)
}

type ServiceManager struct {
	// Add fields for managing services
	DatabaseService        *database.DatabaseService
	BundleService          *bundle.BundleService
	InternalCatalogService *defaultdb.CatalogService
	WALManager             *journal.WALManager
	LockService            *lock.LockService // Database locking for maintenance operations
	GraphQLProcessor       GraphQLProcessor
	UserService            *UserService              // RBAC: Manages user creation and authentication
	PermissionService      *PermissionService        // RBAC: Manages permissions and roles
	MigrationService       MigrationServiceInterface // Migration: Database versioning and schema migration

	// Transaction management
	LockManager *storage.LockManager // Document-level locking for multi-statement transactions

	// RBAC session management for FORCE operations
	SessionManager    *SessionManager        // Session manager for terminating active sessions
	ActiveConnections map[string]*Connection // Active connections for session termination

	// STEP 2: Query plan caching - shared planner with cache invalidation
	UnifiedPlanner *planner.UnifiedQueryPlanner

	// PHASE 3: MVCC - Conflict detection for write-write conflicts
	ConflictTracker *ConflictTracker

	logger *zap.SugaredLogger
}

// Private instance and mutex for thread safety
var (
	instance *ServiceManager
	once     sync.Once
	mu       sync.RWMutex
)

// GetServiceManager returns the singleton instance of ServiceManager
func GetServiceManager() *ServiceManager {
	mu.RLock()
	defer mu.RUnlock()

	if instance == nil {
		// If someone tries to get the instance before initialization,
		// return a basic empty instance
		return &ServiceManager{}
	}
	return instance
}

// InitServiceManager initializes the ServiceManager singleton with services
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
				logger.Infof("WAL Manager registered in global service registry")
			}
		}

		// Initialize RBAC services
		userService := NewUserService(bundleService, dbService, userStore, logger, debugMode)
		permissionService := NewPermissionService(bundleService, dbService, nil, logger, debugMode) // Initialize Lock service
		lockService := lock.NewLockService(logger.Desugar())

		// Initialize transaction lock manager (HIGH-007: uses configurable timeout from settings)
		lockManager := storage.NewLockManager(logger)

		// STEP 2: Initialize unified query planner with plan caching
		unifiedPlanner := planner.NewUnifiedQueryPlanner(logger, bundleService)

		// Register planner with bundle service for schema change invalidation
		bundle.SetQueryPlanner(unifiedPlanner)

		// PHASE 3: MVCC - Initialize conflict tracker for write-write conflict detection
		conflictTracker := NewConflictTracker()

		// Initialize Migration service with adapters
		migrationConfig := migration.LoadConfigFromSettings(settings.GetSettings())
		bundleServiceAdapter := NewBundleServiceAdapter(bundleService, dbService, catalogService, walManager, logger)
		migrationServiceCore := migration.NewMigrationService(bundleServiceAdapter, migrationConfig, logger.Desugar())
		migrationService := NewMigrationServiceAdapter(migrationServiceCore, logger)

		instance = &ServiceManager{
			DatabaseService:        dbService,
			BundleService:          bundleService,
			InternalCatalogService: catalogService,
			WALManager:             walManager,
			LockService:            lockService,
			LockManager:            lockManager,
			GraphQLProcessor:       graphqlProcessor,
			UserService:            userService,
			PermissionService:      permissionService,
			MigrationService:       migrationService,
			UnifiedPlanner:         unifiedPlanner,
			ConflictTracker:        conflictTracker,
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
			if migrationService != nil {
				logger.Info("ServiceManager initialized with Migration service")
			}
		}
	})

	return instance
}

// ResetServiceManager is useful for testing - it resets the singleton
func ResetServiceManager() {
	mu.Lock()
	defer mu.Unlock()
	instance = nil
	// Reset the sync.Once so InitServiceManager can be called again
	once = sync.Once{}
}

// ParserMetrics tracks usage statistics for the new SyndrQL parser
type ParserMetrics struct {
	NewParserAttempts  atomic.Int64 // Total attempts to use new parser
	NewParserSuccesses atomic.Int64 // Successful parses with new parser
	NewParserFailures  atomic.Int64 // Failed parses with new parser
	FallbacksTriggered atomic.Int64 // Times we fell back to legacy parser
}

// Global parser metrics instance
var globalParserMetrics = &ParserMetrics{}

// GetParserMetrics returns the current parser metrics
func GetParserMetrics() map[string]int64 {
	return map[string]int64{
		"new_parser_attempts":  globalParserMetrics.NewParserAttempts.Load(),
		"new_parser_successes": globalParserMetrics.NewParserSuccesses.Load(),
		"new_parser_failures":  globalParserMetrics.NewParserFailures.Load(),
		"fallbacks_triggered":  globalParserMetrics.FallbacksTriggered.Load(),
	}
}

// SetGraphQLProcessor sets the GraphQL processor for the singleton instance
func SetGraphQLProcessor(processor GraphQLProcessor) {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil {
		instance.GraphQLProcessor = processor
		if instance.logger != nil {
			instance.logger.Info("GraphQL processor has been set on ServiceManager")
		}
	}
}

// SetSessionContext sets the SessionManager and ActiveConnections for RBAC operations with FORCE support
// This must be called after server initialization to enable forced session termination in REVOKE commands
// TODO: I can add thread-safe session context updates for hot-reload scenarios
func SetSessionContext(sessionManager *SessionManager, activeConnections map[string]*Connection) {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil {
		instance.SessionManager = sessionManager
		instance.ActiveConnections = activeConnections
		if instance.logger != nil {
			instance.logger.Info("Session context has been set on ServiceManager for RBAC FORCE operations")
		}
	}
}
