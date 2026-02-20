package server

import (
	"sync"
	"sync/atomic"
	"syndrdb/src/internal/auth"
	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/domain/migration"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/lock"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/registry"
	"syndrdb/src/internal/storage"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// unifiedPlannerAdapter wraps UnifiedQueryPlanner to implement bundle.UnifiedPlannerInterface
// This adapter bridges the interface{} return type requirement to avoid import cycles
type unifiedPlannerAdapter struct {
	planner *planner.UnifiedQueryPlanner
}

// CreatePlan implements bundle.UnifiedPlannerInterface by wrapping the actual planner
func (a *unifiedPlannerAdapter) CreatePlan(query *queryparser.UnifiedSelectQuery, database *models.Database) (interface{}, error) {
	// Call the actual planner
	plan, err := a.planner.CreatePlan(query, database)
	if err != nil {
		return nil, err
	}
	
	// Return as interface{} to satisfy the interface
	return plan, nil
}

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

	// Observability: metrics reporting callback for index and plan cache metrics
	// Accepts metric name and increment value; routes to GlobalServerMetrics atomic counters
	MetricsReporter func(metricName string, value uint64)

	logger *zap.SugaredLogger
}

// Private instance and mutex for thread safety
var (
	instancePtr atomic.Pointer[ServiceManager] // lock-free singleton access
	once        sync.Once
	mu          sync.RWMutex // only used for rare admin mutations (SetGraphQLProcessor, etc.)
)

// GetServiceManager returns the singleton instance of ServiceManager.
// Uses atomic.Pointer for zero-contention reads on the hot path.
func GetServiceManager() *ServiceManager {
	if sm := instancePtr.Load(); sm != nil {
		return sm
	}
	// Pre-initialization fallback (should be rare)
	return &ServiceManager{}
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

		// COST INTELLIGENCE: Initialize column statistics store and wire into planner + bundle service
		statsStore := planner.NewStatsStore(logger)
		unifiedPlanner.SetStatsStore(statsStore)
		bundleService.SetStatsUpdater(statsStore)

		// Register planner with bundle service for schema change invalidation
		bundle.SetQueryPlanner(unifiedPlanner)
		
		// Register unified planner for WHERE clause optimization in UPDATE statements
		// Create adapter to bridge interface{} return type requirement
		plannerAdapter := &unifiedPlannerAdapter{planner: unifiedPlanner}
		bundle.SetUnifiedPlanner(plannerAdapter)

		// PHASE 3: MVCC - Initialize conflict tracker for write-write conflict detection
		conflictTracker := NewConflictTracker()

		// Initialize Migration service with adapters
		migrationConfig := migration.LoadConfigFromSettings(settings.GetSettings())
		bundleServiceAdapter := NewBundleServiceAdapter(bundleService, dbService, catalogService, walManager, logger)
		migrationServiceCore := migration.NewMigrationService(bundleServiceAdapter, migrationConfig, logger.Desugar())
		migrationService := NewMigrationServiceAdapter(migrationServiceCore, logger)

		// Observability: Wire metrics reporter into plan cache
		// (metricsReporter defined below; plan cache pointer set after reporter creation)

		// Observability: Create MetricsReporter callback that routes named metrics to GlobalServerMetrics
		gsm := GetGlobalServerMetrics()
		metricsReporter := func(metricName string, value uint64) {
			switch metricName {
			// Hash Index Metrics
			case "HashIndexPutsTotal":
				gsm.HashIndexPutsTotal.Add(value)
			case "HashIndexGetsTotal":
				gsm.HashIndexGetsTotal.Add(value)
			case "HashIndexDeletesTotal":
				gsm.HashIndexDeletesTotal.Add(value)
			case "HashIndexCacheHits":
				gsm.HashIndexCacheHits.Add(value)
			case "HashIndexCacheMisses":
				gsm.HashIndexCacheMisses.Add(value)
			case "HashIndexDiskReads":
				gsm.HashIndexDiskReads.Add(value)
			case "HashIndexPutErrors":
				gsm.HashIndexPutErrors.Add(value)
			case "HashIndexGetErrors":
				gsm.HashIndexGetErrors.Add(value)
			case "HashIndexDeleteErrors":
				gsm.HashIndexDeleteErrors.Add(value)
			// Hash Index Latency Histograms
			case "HashIndexPutLatencyLt1ms":
				gsm.HashIndexPutLatencyLt1ms.Add(value)
			case "HashIndexPutLatencyLt10ms":
				gsm.HashIndexPutLatencyLt10ms.Add(value)
			case "HashIndexPutLatencyLt100ms":
				gsm.HashIndexPutLatencyLt100ms.Add(value)
			case "HashIndexPutLatencyLt1s":
				gsm.HashIndexPutLatencyLt1s.Add(value)
			case "HashIndexPutLatencyGte1s":
				gsm.HashIndexPutLatencyGte1s.Add(value)
			case "HashIndexGetLatencyLt1ms":
				gsm.HashIndexGetLatencyLt1ms.Add(value)
			case "HashIndexGetLatencyLt10ms":
				gsm.HashIndexGetLatencyLt10ms.Add(value)
			case "HashIndexGetLatencyLt100ms":
				gsm.HashIndexGetLatencyLt100ms.Add(value)
			case "HashIndexGetLatencyLt1s":
				gsm.HashIndexGetLatencyLt1s.Add(value)
			case "HashIndexGetLatencyGte1s":
				gsm.HashIndexGetLatencyGte1s.Add(value)
			// B-Tree Index Metrics
			case "BTreeIndexInsertsTotal":
				gsm.BTreeIndexInsertsTotal.Add(value)
			case "BTreeIndexSearchesTotal":
				gsm.BTreeIndexSearchesTotal.Add(value)
			case "BTreeIndexDeletesTotal":
				gsm.BTreeIndexDeletesTotal.Add(value)
			case "BTreeIndexInsertErrors":
				gsm.BTreeIndexInsertErrors.Add(value)
			case "BTreeIndexSearchErrors":
				gsm.BTreeIndexSearchErrors.Add(value)
			case "BTreeIndexDeleteErrors":
				gsm.BTreeIndexDeleteErrors.Add(value)
			// B-Tree Index Latency Histograms
			case "BTreeInsertLatencyLt1ms":
				gsm.BTreeInsertLatencyLt1ms.Add(value)
			case "BTreeInsertLatencyLt10ms":
				gsm.BTreeInsertLatencyLt10ms.Add(value)
			case "BTreeInsertLatencyLt100ms":
				gsm.BTreeInsertLatencyLt100ms.Add(value)
			case "BTreeInsertLatencyLt1s":
				gsm.BTreeInsertLatencyLt1s.Add(value)
			case "BTreeInsertLatencyGte1s":
				gsm.BTreeInsertLatencyGte1s.Add(value)
			case "BTreeSearchLatencyLt1ms":
				gsm.BTreeSearchLatencyLt1ms.Add(value)
			case "BTreeSearchLatencyLt10ms":
				gsm.BTreeSearchLatencyLt10ms.Add(value)
			case "BTreeSearchLatencyLt100ms":
				gsm.BTreeSearchLatencyLt100ms.Add(value)
			case "BTreeSearchLatencyLt1s":
				gsm.BTreeSearchLatencyLt1s.Add(value)
			case "BTreeSearchLatencyGte1s":
				gsm.BTreeSearchLatencyGte1s.Add(value)
			// Plan Cache Metrics
			case "QueryPlanCacheHits":
				gsm.QueryPlanCacheHits.Add(value)
			case "QueryPlanCacheMisses":
				gsm.QueryPlanCacheMisses.Add(value)
			case "QueryPlanCacheEvictions":
				gsm.QueryPlanCacheEvictions.Add(value)
			case "QueryPlanCacheInvalidations":
				gsm.QueryPlanCacheInvalidations.Add(value)
			case "QueryPlanCacheStaleServes":
				gsm.QueryPlanCacheStaleServes.Add(value)
			}
		}

		// Wire metrics reporter into plan cache for hit/miss/eviction/invalidation tracking
		unifiedPlanner.SetMetricsReporter(metricsReporter)

		// Wire metrics reporter into bundle service for index operation tracking
		bundleService.SetMetricsReporter(metricsReporter)

		sm := &ServiceManager{
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
			MetricsReporter:        metricsReporter,
			logger:                 logger,
		}
		instancePtr.Store(sm)

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

	return instancePtr.Load()
}

// ResetServiceManager is useful for testing - it resets the singleton
func ResetServiceManager() {
	mu.Lock()
	defer mu.Unlock()
	instancePtr.Store(nil)
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
	if sm := instancePtr.Load(); sm != nil {
		sm.GraphQLProcessor = processor
		if sm.logger != nil {
			sm.logger.Info("GraphQL processor has been set on ServiceManager")
		}
	}
}

// SetSessionContext sets the SessionManager and ActiveConnections for RBAC operations with FORCE support
// This must be called after server initialization to enable forced session termination in REVOKE commands
// TODO: I can add thread-safe session context updates for hot-reload scenarios
func SetSessionContext(sessionManager *SessionManager, activeConnections map[string]*Connection) {
	mu.Lock()
	defer mu.Unlock()
	if sm := instancePtr.Load(); sm != nil {
		sm.SessionManager = sessionManager
		sm.ActiveConnections = activeConnections
		if sm.logger != nil {
			sm.logger.Info("Session context has been set on ServiceManager for RBAC FORCE operations")
		}
	}
}
