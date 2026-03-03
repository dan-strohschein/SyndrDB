package bundle

import (
	"context"
	"fmt"
	"time"

	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/pkg/settings"

	"syndrdb/src/internal/domain/index/btreeindexV2"

	hashindex "syndrdb/src/internal/domain/index/hashindexV3" // NEW - Sprint 5: LSM-style hash index
	"syndrdb/src/pkg/common/helpers"

	// Import the graphQL schema package for automatic schema generation
	graphQLSchema "syndrdb/src/internal/graphQL/schema"

	// Service Registry for dependency injection (breaks circular dependencies)

	// (joinexecutor moved to bundle_service_cache.go)

	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// PageCacheShardCount is the number of shards for page cache locks (must be power of 2)
const PageCacheShardCount = 64

// findDocumentPageScanLimit caps the fallback page scan when DocumentID index is missing (Issue 8).
const findDocumentPageScanLimit = 100

// QueryPlannerInterface defines the interface for plan cache invalidation
// This avoids circular dependencies with the server package
type QueryPlannerInterface interface {
	InvalidateBundleCache(bundleName string)
	RemoveBundleMetadata(bundleName string) // Remove from plan-cache metadata when bundle is dropped
}

// SessionInterface defines the interface for transaction-aware queries
// This avoids circular dependencies with the server package
type SessionInterface interface {
	IsInTransaction() bool
	GetActiveTransactionID() string
}

// Global query planner reference for plan cache invalidation
// Set by server during initialization to avoid circular dependencies
// PERFORMANCE: Uses atomic.Value for lock-free reads on hot paths
var globalQueryPlanner atomic.Value // stores QueryPlannerInterface

// UnifiedPlannerInterface defines the interface for creating execution plans
// This allows BundleService to use the query planner for WHERE clause optimization
// Uses interface{} to avoid import cycle with planner package
type UnifiedPlannerInterface interface {
	CreatePlan(query *queryparser.UnifiedSelectQuery, database *models.Database) (interface{}, error)
}

// Global unified planner reference for query planning
// Set by server during initialization to avoid circular dependencies
// PERFORMANCE: Uses atomic.Value for lock-free reads on hot paths
var globalUnifiedPlanner atomic.Value // stores UnifiedPlannerInterface

// SetQueryPlanner sets the global query planner reference
// Called by server during initialization
// Uses atomic.Store for thread-safe write (rare operation, only at startup)
func SetQueryPlanner(planner QueryPlannerInterface) {
	globalQueryPlanner.Store(planner)
}

// SetUnifiedPlanner sets the global unified planner reference
// Called by server during initialization
// Uses atomic.Store for thread-safe write (rare operation, only at startup)
func SetUnifiedPlanner(planner UnifiedPlannerInterface) {
	globalUnifiedPlanner.Store(planner)
}

// getQueryPlanner returns the global query planner using lock-free atomic load
func getQueryPlanner() QueryPlannerInterface {
	if v := globalQueryPlanner.Load(); v != nil {
		return v.(QueryPlannerInterface)
	}
	return nil
}

// getUnifiedPlanner returns the global unified planner using lock-free atomic load
func getUnifiedPlanner() UnifiedPlannerInterface {
	if v := globalUnifiedPlanner.Load(); v != nil {
		return v.(UnifiedPlannerInterface)
	}
	return nil
}

// IndexUpdate represents a deferred index update operation
type IndexUpdate struct {
	BundleName  string
	IndexName   string
	IndexType   string
	Operation   string // "insert", "delete", "update"
	DocumentID  string
	FieldValue  interface{}
	PageID      uint32      // Physical page where document resides
	OldValue    interface{} // For updates
	Timestamp   time.Time
	AppliedSync bool // True if already applied synchronously (for read-your-own-writes)

	// HashEntry: when set (single-write path), processHashIndexBatch writes this entry to disk only.
	// MemTable was already updated in scheduleIndexUpdate; same sequence is used once.
	HashEntry *hashindex.HashIndexEntry
}

// MetadataUpdate represents a deferred metadata update operation
type MetadataUpdate struct {
	BundleName string
	Operation  string // "increment_docs", "decrement_docs", "recalc_pages"
	Value      int64  // For increment/decrement operations
	Timestamp  time.Time
}

// btreeRollbackOp records one B-tree index update for rollback if UpdateDocumentsBatch fails.
// Rollback: Delete(newKey, documentID) then Insert(oldKey, documentID) to restore pre-update state.
type btreeRollbackOp struct {
	idx        *btreeindexV2.BTreeIndex
	oldKey     []byte
	newKey     []byte
	documentID string
}

// DocumentLockInfo contains information about pre-acquired document locks
// and optional MVCC version metadata.
type DocumentLockInfo struct {
	LockManager  interface{} // *storage.LockManager - use interface{} to avoid import cycle
	TxID         string      // Transaction ID for the locks
	SessionID    string      // Session ID for the locks
	LockedDocIDs []string    // Document IDs that are already locked
	// VersionTxID is the WAL transaction ID used for CreatedByTxID (Phase 2b).
	// When set, prefer over TxID for MVCC version metadata so versioning matches snapshot.
	VersionTxID string
	// PreFetchedDocs, when set, are the full documents matching LockedDocIDs from a single WHERE scan.
	// UpdateDocumentInBundle uses these directly instead of GetDocument-by-ID, avoiding index→page
	// lookup failures (e.g. after compaction) and duplicate I/O. Ensures accurate updates.
	PreFetchedDocs []*models.Document
}

// TypeConverter represents a fast type conversion function
type TypeConverter func(interface{}) (interface{}, error)

// Pre-compiled type converters for performance optimization
var typeConverters = map[string]TypeConverter{
	"string":   convertToString,
	"int":      convertToInt,
	"float":    convertToFloat,
	"number":   convertToFloat, // alias for float
	"bool":     convertToBool,
	"datetime": convertToDateTime,
	"date":     convertToDate,
	// "relationship" type removed - FK fields now preserve source field type
}

type BundleService struct {
	store           bundlestore.BundleStore
	factory         BundleFactory
	documentFactory document.DocumentFactory
	settings        *settings.Arguments

	// Changed: Store only bundle metadata, not full bundles with documents
	bundleMetadata map[string]*models.Bundle // Only schema/structure

	// DEADLOCK FIX: Fully sharded page cache - each shard has its own map, lock, and LRU
	// Previously: single documentPages map + documentPagesMutex caused RWMutex starvation
	// Now: 64 independent shards, so writers in shard N don't block readers in shard M
	pageShards [PageCacheShardCount]*pageCacheShard

	logger *zap.SugaredLogger

	// Configuration for page management
	defaultPageSize int // Default number of documents per page
	maxLoadedPages  int // Maximum number of pages to keep in memory (total across all shards)

	// Performance optimization: Deferred index updates
	indexUpdateBuffer    []IndexUpdate // Buffer for pending index updates
	indexUpdateMutex     sync.Mutex    // Protects indexUpdateBuffer (FIX: was missing, causing data race)
	indexUpdateBatchSize int           // Maximum updates to batch before flushing
	indexUpdateInterval  time.Duration // Maximum time to wait before flushing
	lastIndexFlush       time.Time     // Last time index updates were flushed

	// ASYNC INDEX FLUSH: Background goroutine to avoid blocking ADD callers on I/O
	indexFlushTrigger chan struct{} // Buffered(1), signals background flusher
	indexFlushDone    chan struct{} // Closed when background flusher exits (for graceful shutdown)

	// Performance optimization: Deferred metadata updates
	metadataUpdateBuffer    []MetadataUpdate // Buffer for pending metadata updates
	metadataPersistInterval int              // Number of operations before forcing metadata persist
	metadataOperationCount  int              // Count of operations since last metadata flush
	lastMetadataFlush       time.Time        // Last time metadata updates were flushed
	metadataUpdateMutex     sync.RWMutex     // Protects metadata update buffer and operation count (RWMutex for read optimization)
	metadataBufferLen       atomic.Int32     // Lock-free buffer emptiness check for the read path

	// PHASE 1 OPTIMIZATION: Bulk operation detection for WAL bypass
	bulkModeEnabled        bool      // Current bulk mode state
	operationCount         int       // Operations in current time window
	operationWindow        time.Time // Start of current measurement window
	bulkThresholdOpsPerSec int       // Operations per second threshold for bulk mode

	// DOCUMENT SCANNER INTEGRATION: Add scanner management
	scannerIntegration *documentscanner.ScannerIntegration // Scanner integration instance
	bundleScanners     sync.Map                            // bundle name -> documentscanner.DocumentScannerInterface (lock-free reads)
	scannerInitMu      sync.Mutex                          // Serializes scanner creation (rare, once per bundle)

	// PERFORMANCE OPTIMIZATION: Document location cache for O(1) page lookups
	// PHASE 5: Sharded across 64 buckets to reduce contention (replaces pageCacheMutex)
	documentPageCache *ShardedPageCacheMap // bundleName -> documentID -> pageID (sharded)

	// OPERATION LOCKING: Fine-grained locks for bundle operations
	// Tracks active read/write operations to ensure safety during administrative operations
	// PHASE 5: Sharded across 64 buckets to reduce contention (replaces bundleLockMutex)
	bundleLocks *ShardedBundleOperationLockMap // bundleName -> operation lock (sharded)

	// NULL HANDLER: Manages magic NULL values and field initialization
	nullHandler *NullHandler // Handles SYNDR_NULL, SYNDR_MISSING, etc.

	// CATALOG SERVICE: Reference for updating system catalog
	// Injected after construction to avoid circular dependency
	catalogService CatalogServiceInterface

	// GRAPHQL SCHEMA MANAGEMENT: Manages GraphQL schema generation and storage (Phase 5)
	// This system automatically generates GraphQL schemas from bundle structures
	// and maintains them in versioned, tombstone-based file format per database.
	//
	// Integration Points:
	// - AddBundle: Generates initial schema when bundle is created
	// - UpdateBundle: Creates new schema version when bundle structure changes
	// - Bundle field modifications: Triggers schema regeneration with breaking change detection
	//
	// Schema Lifecycle:
	// 1. Bundle Created → Generate GraphQL schema → Store in schema file → Cache
	// 2. Bundle Modified → Detect breaking changes → Create new version → Tombstone old → Update cache
	// 3. Bundle Deleted → Tombstone all schema versions
	//
	// Architecture:
	// - schemaManagers: One manager per database (lazy initialization on first bundle operation)
	// - schemaGenerator: Shared stateless generator for all databases
	// - graphQLEnabled: Global toggle from settings.EnableGraphQL
	//
	// The schema system is optional and only initializes if GraphQL support is enabled.
	// This allows the database to run without GraphQL overhead if not needed.
	// PHASE 5: Sharded across 64 buckets to reduce contention (replaces schemaManagerMutex)
	schemaManagers  *ShardedSchemaManagerMap       // databaseName -> schema manager (sharded)
	schemaGenerator *graphQLSchema.SchemaGenerator // Shared generator for all databases
	graphQLEnabled  bool                           // Global toggle from settings

	// PERFORMANCE OPTIMIZATION: Runtime-toggleable diagnostic logging (Priority 1)
	verboseLogging bool // Default: false - disable hot path diagnostic logs for performance

	// PERFORMANCE OPTIMIZATION: In-memory index instance cache
	// IndexInstance field in bundle.Indexes is not persisted (json:"-" tag), so we need
	// a separate cache to avoid reloading indexes from disk on every operation
	// PERFORMANCE: Sharded across 64 buckets to reduce contention under high concurrency
	loadedIndexes *bundlestore.ShardedIndexCache // bundleName -> indexName -> index instance (sharded)

	// UNIQUE INDEX MEMORY MANAGEMENT: In-memory B-tree indexes for unique constraints
	// PostgreSQL-style approach: load unique constraint indexes into memory on database context switch
	// with LRU eviction based on idle timeout and memory budget enforcement
	// PHASE 5: currentIndexMemoryUsage uses atomic.Int64 for lock-free updates
	//          loadedDatabases uses ShardedLoadedDatabasesMap for concurrent access
	uniqueIndexMemoryBudgetBytes int64                      // Memory budget for in-memory unique indexes (from settings)
	currentIndexMemoryUsage      atomic.Int64               // Current memory usage by loaded unique indexes (atomic for lock-free updates)
	loadedDatabases              *ShardedLoadedDatabasesMap // databaseName -> lastAccessTime for LRU eviction (sharded)

	// TODO: Implement bundle-level shared WAL for B-Tree indexes - single WAL per bundle reduces file handles and enables coordinated checkpoints. Add btreeWAL field, initialize in NewBundleService, log format: BTREE:idx_name:INSERT|DELETE|UPDATE:pageNum:key
	// IMPORTANT NOTE: B-Tree indexes share bundle-level WAL to minimize file handles and enable coordinated checkpoint/recovery
	// btreeWAL *journal.WriteAheadLog // Shared WAL for all B-Tree indexes in this bundle (reduces file handles)

	// INDEX MAINTENANCE: Automatic index rebuilding on staleness threshold
	indexMaintenanceScheduler IndexMaintenanceSchedulerInterface // Scheduler for automatic index rebuilds

	// PERFORMANCE FIX: Background COW snapshot cleaner context
	// Used to gracefully shut down the background goroutine on server shutdown
	cowCleanerCtx    context.Context    // Context for background cleaner goroutine
	cowCleanerCancel context.CancelFunc // Cancel function to stop background cleaner

	// PERFORMANCE FIX: Background fastLookup sync.Map compactor context
	// Periodically recreates sync.Map to remove accumulated "expunged" entries
	// that degrade Load() performance after many page evictions
	fastLookupCompactorCtx    context.Context    // Context for background compactor goroutine
	fastLookupCompactorCancel context.CancelFunc // Cancel function to stop background compactor

	// PERFORMANCE FIX: Background hash index MemTable compactor context
	// Periodically clears walBuffer in loaded hash indexes to prevent unbounded memory growth
	// during sustained high-throughput write workloads
	memTableCompactorCtx    context.Context    // Context for background compactor goroutine
	memTableCompactorCancel context.CancelFunc // Cancel function to stop background compactor

	// DIAGNOSTICS: Background buffer diagnostics logger
	// Logs buffer sizes after 30 seconds of idle to help debug latency degradation
	diagnosticsLoggerCtx    context.Context    // Context for background diagnostics goroutine
	diagnosticsLoggerCancel context.CancelFunc // Cancel function to stop diagnostics logger
	lastWriteActivity       atomic.Int64       // Unix timestamp (nanoseconds) of last write activity
	lastActivity            atomic.Int64       // Unix timestamp (nanoseconds) of last server activity (read or write)

	// PERFORMANCE FIX: Background idle buffer flusher context
	// Flushes all WriteBuffers after 5 seconds of idle to prevent stuck buffers
	// Root cause: WriteBuffer.flushTimeout only triggers on next write, so idle buffers stay full
	idleBufferFlusherCtx    context.Context    // Context for background flusher goroutine
	idleBufferFlusherCancel context.CancelFunc // Cancel function to stop flusher

	// PERFORMANCE FIX: Background idle cache flusher for test run isolation
	// When server is idle for 30 seconds, flush all document caches to ensure
	// clean state for next test run. This is more reliable than detecting when
	// all sessions disconnect, which has race conditions with rapid reconnects.
	idleCacheFlusherCtx       context.Context    // Context for background flusher goroutine
	idleCacheFlusherCancel    context.CancelFunc // Cancel function to stop flusher
	lastCacheFlushTime        atomic.Int64       // Unix timestamp (nanoseconds) of last cache flush
	idleCacheFlushThresholdNs int64              // Idle threshold in nanoseconds (default 30s)

	// Callback for external cache flush (e.g., JOIN hash table cache)
	// Set by server during initialization to avoid circular imports
	onCacheFlush func()

	// COLUMN STATISTICS: Incremental stats updater for planner cost estimation
	statsUpdater StatsUpdater

	// VISIBILITY MAP: Per-bundle all-visible page tracking for scan optimization.
	// When a page is all-visible (all docs committed, not deleted, not superseded),
	// scanners skip per-document IsVisibleToSnapshot() calls entirely.
	visibilityMaps sync.Map // bundleName -> *VisibilityMap

	// VISIBILITY MAP: Background refresher context
	vmRefresherCtx    context.Context    // Context for background VM refresher goroutine
	vmRefresherCancel context.CancelFunc // Cancel function to stop VM refresher

	// PAGE BLOOM FILTER: Per-bundle per-page bloom filters for scan skip optimization.
	// Tracks field-value membership per page so scanners can skip pages that
	// definitely don't contain matching values during filtered scans.
	pageBloomMaps sync.Map // bundleName -> *PageBloomMap

	// PAGE BLOOM FILTER: Background refresher context
	pbRefresherCtx    context.Context    // Context for background page bloom refresher
	pbRefresherCancel context.CancelFunc // Cancel function to stop page bloom refresher

	// Observability: metrics reporter callback for index operation tracking
	metricsReporter func(metricName string, value uint64)
}

// IndexMaintenanceSchedulerInterface defines the interface for scheduling index rebuilds
// This avoids circular imports while allowing BundleService to trigger rebuilds
type IndexMaintenanceSchedulerInterface interface {
	ScheduleRebuild(req IndexMaintenanceRequest) error
}

// IndexMaintenanceRequest represents a request to rebuild an index
type IndexMaintenanceRequest struct {
	DatabaseName  string
	BundleName    string
	IndexName     string
	IndexType     string
	StalenessRate float64
	QueryCount    int64
}

func NewBundleService(store bundlestore.BundleStore, factory BundleFactory,
	docFactory document.DocumentFactory,
	logger *zap.SugaredLogger,
	args *settings.Arguments) *BundleService {
	// Get performance settings from global configuration
	globalSettings := settings.GetSettings()

	maxLoaded := globalSettings.MaxLoadedDocumentPages
	if maxLoaded <= 0 {
		maxLoaded = 500
	}

	// Calculate max pages per shard (distribute evenly)
	maxPagesPerShard := (maxLoaded + PageCacheShardCount - 1) / PageCacheShardCount
	if maxPagesPerShard < 1 {
		maxPagesPerShard = 1
	}

	service := &BundleService{
		store:           store,
		factory:         factory,
		documentFactory: docFactory,
		settings:        args,
		logger:          logger,
		bundleMetadata:  make(map[string]*models.Bundle),
		// pageShards will be initialized below
		defaultPageSize: 4096, // Default: 4096 documents per page (power of 2 for fast bit-shift calculations)
		maxLoadedPages:  maxLoaded,
		// OPTIMIZATION: Use configurable performance settings
		indexUpdateBuffer:    make([]IndexUpdate, 0, globalSettings.MetadataBatchSize),
		indexUpdateBatchSize: globalSettings.MetadataBatchSize,                                       // INCREASED: 50 → 500
		indexUpdateInterval:  time.Duration(globalSettings.MetadataFlushInterval) * time.Millisecond, // Use proper unit conversion
		lastIndexFlush:       time.Now(),
		indexFlushTrigger:    make(chan struct{}, 1), // Buffered(1) for non-blocking signal + coalescing
		indexFlushDone:       make(chan struct{}),    // Closed when background flusher exits

		// OPTIMIZATION: Deferred metadata updates with configurable intervals
		metadataUpdateBuffer:    make([]MetadataUpdate, 0, globalSettings.MetadataBatchSize),
		metadataPersistInterval: globalSettings.MetadataPersistInterval, // NEW: 1000 docs before disk persist
		metadataOperationCount:  0,
		lastMetadataFlush:       time.Now(),

		// OPTIMIZATION: Bulk operation detection for WAL bypass
		bulkModeEnabled:        false,
		operationCount:         0,
		operationWindow:        time.Now(),
		bulkThresholdOpsPerSec: globalSettings.WALBulkModeThreshold, // 50 ops/sec threshold

		// DOCUMENT SCANNER INTEGRATION: Initialize scanner management
		scannerIntegration: documentscanner.NewScannerIntegration(logger),
		// bundleScanners is a sync.Map — zero-value is ready to use

		// PHASE 5: Initialize sharded document-page location cache
		documentPageCache: NewShardedPageCacheMap(globalSettings.MaxLoadedDocumentPages, logger),

		// PHASE 5: Initialize sharded bundle operation locks
		bundleLocks: NewShardedBundleOperationLockMap(logger),

		// NULL HANDLER: Initialize NULL value handler
		nullHandler: NewNullHandler(logger),

		// CATALOG SERVICE: Will be injected post-construction via SetCatalogService()
		catalogService: nil,

		// GRAPHQL INTEGRATION: Initialize GraphQL schema system
		// Schema managers are created lazily per database on first bundle operation
		// because they require database-specific directory paths not available at construction.
		// The schema generator is stateless and shared across all databases.
		// PHASE 5: Sharded schema manager map for concurrent access
		schemaManagers:  NewShardedSchemaManagerMap(),
		schemaGenerator: nil, // Initialized below if GraphQL is enabled
		graphQLEnabled:  globalSettings.EnableGraphQL,

		// PERFORMANCE OPTIMIZATION: Initialize sharded index instance cache (64 shards)
		loadedIndexes: bundlestore.NewShardedIndexCache(),

		// PHASE 5: Initialize memory tracking with atomic counter and sharded map
		uniqueIndexMemoryBudgetBytes: int64(globalSettings.UniqueIndexMemoryBudgetMB) * 1024 * 1024, // Convert MB to bytes
		// currentIndexMemoryUsage: atomic.Int64 zero-value is ready to use
		loadedDatabases: NewShardedLoadedDatabasesMap(),
	}

	// DEADLOCK FIX: Initialize sharded page cache (each shard is independent)
	for i := 0; i < PageCacheShardCount; i++ {
		service.pageShards[i] = newPageCacheShard(maxPagesPerShard)
	}

	// Initialize schema generator if GraphQL is enabled
	// Generator is stateless and can be created once, shared by all databases
	if service.graphQLEnabled {
		service.schemaGenerator = graphQLSchema.NewSchemaGenerator()
		logger.Debugf("GraphQL schema generator initialized (managers will be created per database on-demand)")
	} else {
		logger.Debugf("GraphQL support disabled - schema generation will be skipped")
	}

	// Don't load bundle metadata at startup - bundles should be loaded on-demand
	// Only primary database catalog bundles will be loaded during server initialization
	logger.Debugf("Bundle service initialized - bundles will be loaded on-demand")

	// PERFORMANCE FIX: Start background COW snapshot compactor to prevent sync.Map degradation
	// Recreates sync.Map every 30 seconds, removing both stale entries and expunged tombstones
	// Replaces old cleanStaleCOWSnapshots() approach that only deleted (added tombstones)
	service.cowCleanerCtx, service.cowCleanerCancel = context.WithCancel(context.Background())
	service.startCOWSnapshotCompactor(service.cowCleanerCtx)

	// PERFORMANCE FIX: Start background fastLookup compactor to prevent sync.Map degradation
	// Recreates sync.Map every 60 seconds to remove accumulated "expunged" entries
	service.fastLookupCompactorCtx, service.fastLookupCompactorCancel = context.WithCancel(context.Background())
	service.startFastLookupCompactor(service.fastLookupCompactorCtx)

	// PERFORMANCE FIX: Start background hash index MemTable compactor
	// Clears walBuffer every 30 seconds to prevent unbounded memory growth during sustained writes
	service.memTableCompactorCtx, service.memTableCompactorCancel = context.WithCancel(context.Background())
	service.startMemTableCompactor(service.memTableCompactorCtx)

	// DIAGNOSTICS: Start background buffer diagnostics logger
	// Logs buffer sizes after 30 seconds of idle to help debug latency degradation
	service.diagnosticsLoggerCtx, service.diagnosticsLoggerCancel = context.WithCancel(context.Background())
	service.lastWriteActivity.Store(time.Now().UnixNano())
	service.lastActivity.Store(time.Now().UnixNano()) // Initialize activity tracker for idle cache flushing
	service.startDiagnosticsLogger(service.diagnosticsLoggerCtx)

	// PERFORMANCE FIX: Start background idle buffer flusher
	// Flushes WriteBuffers after 5 seconds of idle to prevent stuck buffers
	service.idleBufferFlusherCtx, service.idleBufferFlusherCancel = context.WithCancel(context.Background())
	service.startIdleBufferFlusher(service.idleBufferFlusherCtx)

	// ASYNC INDEX FLUSH: Start background index flusher goroutine
	// Moves index I/O off the ADD hot path, reducing ADD P95 from ~2900ms to ~50ms
	service.startIndexFlushLoop()

	// PERFORMANCE FIX: Start background idle cache flusher for test run isolation
	// When server is idle for 30 seconds, flush all document caches
	service.idleCacheFlusherCtx, service.idleCacheFlusherCancel = context.WithCancel(context.Background())
	service.idleCacheFlushThresholdNs = 30 * int64(time.Second) // 30 seconds
	service.lastCacheFlushTime.Store(time.Now().UnixNano())
	service.startIdleCacheFlusher(service.idleCacheFlusherCtx)

	// VISIBILITY MAP: Start background VM refresher to set all-visible bits
	// Evaluates pages every 10 seconds and marks stable pages as all-visible
	// so scanners can skip per-document MVCC checks on those pages
	service.vmRefresherCtx, service.vmRefresherCancel = context.WithCancel(context.Background())
	go service.startVisibilityMapRefresher(service.vmRefresherCtx)

	// PAGE BLOOM FILTER: Start background page bloom refresher to build bloom filters
	// for pages that don't have one yet. Builds bloom from COW page snapshots.
	if globalSettings.PageBloomEnabled {
		service.pbRefresherCtx, service.pbRefresherCancel = context.WithCancel(context.Background())
		go service.startPageBloomRefresher(service.pbRefresherCtx)
	}

	// SCHEMA PROVIDER: Wire up schema resolution so documents loaded from disk have
	// correctly-populated doc.Values (schema-ordered field array). Without this,
	// documents are deserialized with a minimal schema (just DocumentID) and GROUP BY /
	// aggregate field lookups fail because doc.Values doesn't contain business fields.
	if bse, ok := store.(*bundlestore.BundleStorageEngine); ok {
		bse.SetSchemaProvider(func(bundleName, databaseName string) *models.BundleFieldSchema {
			bundle, exists := service.bundleMetadata[bundleName]
			if exists && bundle != nil {
				return bundle.DocumentStructure.FieldSchema()
			}
			// Fallback: load bundle metadata from disk. This handles the case where
			// background cache warming (warmParsedDocsCache) deserializes documents
			// before GetBundleByName has populated bundleMetadata. Without this
			// fallback, documents get Values=[DocumentID] only, and GROUP BY/aggregate
			// queries fail because all business fields are lost.
			databasePath := helpers.GetDatabaseFolderPath(databaseName)
			fileName := fmt.Sprintf("%s_%s.bnd", databaseName, bundleName)
			diskBundle, err := bse.LoadBundleMetadata(nil, databasePath, fileName)
			if err != nil || diskBundle == nil {
				return nil
			}
			return diskBundle.DocumentStructure.FieldSchema()
		})
		logger.Debugf("Schema provider wired to BundleStorageEngine for Values-based document access")
	}

	return service
}

// SetCatalogService injects the catalog service reference after construction.
// This is necessary to break the circular dependency between BundleService and CatalogService.
// Should be called during server initialization after all services are created.
func (s *BundleService) SetCatalogService(catalogService CatalogServiceInterface) {
	s.catalogService = catalogService
	s.logger.Debug("Catalog service injected into BundleService")
}

// RegisterBundleInCatalog delegates to the injected CatalogService to register
// a bundle in the primary.Bundles system catalog.
func (s *BundleService) RegisterBundleInCatalog(bundle *models.Bundle) error {
	if s.catalogService == nil {
		return fmt.Errorf("catalog service not available")
	}
	return s.catalogService.RegisterBundleInCatalog(bundle)
}

// AddDatabaseToCatalog delegates to the injected CatalogService to register
// a database in the primary.Databases system catalog.
func (s *BundleService) AddDatabaseToCatalog(db *models.Database) error {
	if s.catalogService == nil {
		return fmt.Errorf("catalog service not available")
	}
	return s.catalogService.AddDatabaseToCatalog(db)
}

// SetStatsUpdater injects a column statistics updater for incremental stats maintenance.
func (s *BundleService) SetStatsUpdater(updater StatsUpdater) {
	s.statsUpdater = updater
}

// SetMetricsReporter sets the callback used to export index operation metrics to GlobalServerMetrics.
func (s *BundleService) SetMetricsReporter(reporter func(string, uint64)) {
	s.metricsReporter = reporter
}

// SetOnCacheFlush registers a callback to be invoked when FlushAllDocumentCaches runs.
// This allows external components (like JOIN hash table cache) to be cleared without
// creating circular import dependencies.
func (s *BundleService) SetOnCacheFlush(callback func()) {
	s.onCacheFlush = callback
	s.logger.Debug("Cache flush callback registered")
}

// SetIndexMaintenanceScheduler injects the index maintenance scheduler reference after construction.
// This is necessary to break the circular dependency and allow automatic index rebuilding.
// Should be called during server initialization after the scheduler is created.
func (s *BundleService) SetIndexMaintenanceScheduler(scheduler IndexMaintenanceSchedulerInterface) {
	s.indexMaintenanceScheduler = scheduler
	s.logger.Debug("Index maintenance scheduler injected into BundleService")
}

func (s *BundleService) GetMetadataPersistInterval() int {
	return s.metadataPersistInterval
}

// SetMetadataPersistInterval updates the metadata persist interval threshold.
// This should only be called during configuration or testing.
func (s *BundleService) SetMetadataPersistInterval(interval int) {
	s.metadataUpdateMutex.Lock()
	defer s.metadataUpdateMutex.Unlock()
	s.metadataPersistInterval = interval
}


// Shutdown ensures all pending operations are completed before service termination
// This method should be called during graceful shutdown to maintain data consistency
func (s *BundleService) Shutdown() error {
	s.logger.Debugf("Shutting down BundleService, flushing pending operations...")

	// Stop background COW snapshot cleaner
	if s.cowCleanerCancel != nil {
		s.logger.Debug("Stopping background COW snapshot cleaner")
		s.cowCleanerCancel()
	}

	// Stop background fastLookup compactor
	if s.fastLookupCompactorCancel != nil {
		s.logger.Debug("Stopping background fastLookup compactor")
		s.fastLookupCompactorCancel()
	}

	// Stop background hash index MemTable compactor
	if s.memTableCompactorCancel != nil {
		s.logger.Debug("Stopping background hash index MemTable compactor")
		s.memTableCompactorCancel()
	}

	// Stop background diagnostics logger
	if s.diagnosticsLoggerCancel != nil {
		s.logger.Debug("Stopping background diagnostics logger")
		s.diagnosticsLoggerCancel()
	}

	// Stop background idle buffer flusher
	if s.idleBufferFlusherCancel != nil {
		s.logger.Debug("Stopping background idle buffer flusher")
		s.idleBufferFlusherCancel()
	}

	// Stop background idle cache flusher
	if s.idleCacheFlusherCancel != nil {
		s.logger.Debug("Stopping background idle cache flusher")
		s.idleCacheFlusherCancel()
	}

	// Stop background visibility map refresher
	if s.vmRefresherCancel != nil {
		s.logger.Debug("Stopping background visibility map refresher")
		s.vmRefresherCancel()
	}

	// Close scanners before other cleanup
	s.CloseAllScanners()

	// Stop background index flush goroutine and drain remaining updates
	if s.indexFlushTrigger != nil {
		close(s.indexFlushTrigger)
		<-s.indexFlushDone        // Wait for background flusher to exit
		s.indexFlushTrigger = nil // Prevent double-close on repeated Shutdown calls
	}

	// Force flush any remaining pending index updates (full, including B-tree disk flush)
	s.forceFlushIndexUpdatesFull()

	// CRITICAL: Flush all loaded indexes to disk before closing
	// This ensures memtable entries and write buffers are persisted
	if err := s.FlushAllIndexesToDisk(); err != nil {
		s.logger.Warnf("Error flushing indexes during shutdown: %v", err)
	}

	// Also flush and close write buffers
	s.store.CloseWriteBuffers()

	// Also force flush any remaining metadata updates during shutdown
	// CRITICAL: Use forceMetadataPersistence to ensure disk write happens
	if s.metadataBufferLen.Load() > 0 {
		s.logger.Debugf("Force flushing %d remaining metadata updates during shutdown", s.metadataBufferLen.Load())
		s.ForceMetadataPersistence()
	}

	// Persist SortedIndex and close all loaded indexes properly
	for bundleName, bundle := range s.bundleMetadata {
		// CRITICAL: Persist SortedIndex to disk - this maintains TotalDocuments count
		if bundle.SortedIndex != nil {
			if err := PersistBundleSortedIndex(bundle); err != nil {
				s.logger.Warnf("Failed to persist SortedIndex for bundle '%s': %v", bundleName, err)
			} else {
				s.logger.Debugf("Persisted SortedIndex for bundle '%s' (%d documents)", bundleName, bundle.SortedIndex.TotalDocuments())
			}
		}

		// Close all loaded indexes properly
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexInstance != nil {
					s.logger.Debugf("Closing index '%s' for bundle '%s'", indexName, bundleName)

					// Close hash index V3 (LSM-style)
					if hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3); ok {
						if err := hashIndex.Close(); err != nil {
							s.logger.Warnf("Failed to close hash index '%s' for bundle '%s': %v", indexName, bundleName, err)
						}
					}

					// Close BTree index if it has a Close method
					if btreeIndex, ok := indexRef.IndexInstance.(interface{ Close() error }); ok {
						if err := btreeIndex.Close(); err != nil {
							s.logger.Warnf("Failed to close BTree index '%s' for bundle '%s': %v", indexName, bundleName, err)
						}
					}
				}
			}
		}
	}

	s.logger.Debugf("BundleService shutdown completed")
	return nil
}

// RegisterBundleForTesting registers a bundle in the in-memory cache for testing purposes
// This allows E2E tests to set up bundle relationships without requiring full disk persistence
func (s *BundleService) RegisterBundleForTesting(bundle *models.Bundle) {
	if s.bundleMetadata == nil {
		s.bundleMetadata = make(map[string]*models.Bundle)
	}
	s.bundleMetadata[bundle.Name] = bundle
	s.logger.Debugf("[Testing] Registered bundle '%s' in memory cache", bundle.Name)
}

// setDocumentVersionFields sets MVCC version metadata on a document
// txID: Transaction ID as hex string (empty for autocommit)
// versionSequence: Version number within document ID (1, 2, 3...)
func (s *BundleService) setDocumentVersionFields(document *models.Document, txID string, versionSequence uint64) {
	if document == nil {
		return
	}

	// Convert txID string to uint64 (if present)
	var createdByTxID uint64 = 0
	if txID != "" {
		_, err := fmt.Sscanf(txID, "%016x", &createdByTxID)
		if err != nil {
			// If parsing fails, use 0 (autocommit)
			s.logger.Warnf("Failed to parse txID '%s' as uint64, using 0 (autocommit)", txID)
			createdByTxID = 0
		}
	}

	// Set version fields
	document.CreatedByTxID = createdByTxID
	document.DeletedByTxID = 0  // Not deleted
	document.CommitSequence = 0 // Uncommitted (will be set on commit)
	document.VersionSequence = versionSequence
}
