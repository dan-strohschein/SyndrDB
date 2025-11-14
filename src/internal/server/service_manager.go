package server

import (
	"sync"
	"sync/atomic"
	"syndrdb/src/internal/auth"
	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/lock"
	"syndrdb/src/internal/registry"

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
	ProcessGraphQLCommand(command string) (interface{}, error)
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
	logger                 *zap.SugaredLogger
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
		permissionService := NewPermissionService(bundleService, dbService, logger, debugMode)

		// Initialize Lock service
		lockService := lock.NewLockService(logger.Desugar())

		// TODO: Initialize Migration service here
		// Example:
		// migrationConfig := migration.LoadConfigFromSettings(settings.GetSettings())
		// migrationService := migration.NewMigrationService(bundleService, migrationConfig, logger.Desugar())

		instance = &ServiceManager{
			DatabaseService:        dbService,
			BundleService:          bundleService,
			InternalCatalogService: catalogService,
			WALManager:             walManager,
			LockService:            lockService,
			GraphQLProcessor:       graphqlProcessor,
			UserService:            userService,
			PermissionService:      permissionService,
			MigrationService:       nil, // TODO: Set to migrationService once initialized
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
