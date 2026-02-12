package bundle

import (
	"container/list"
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/storage/bundlestore"
	syndrQL "syndrdb/src/internal/syndrQL"
	"syndrdb/src/internal/utils"
	"syndrdb/src/pkg/common/conversion"
	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"

	"syndrdb/src/internal/domain/index/btreeindexV2"

	hashindex "syndrdb/src/internal/domain/index/hashindexV3" // NEW - Sprint 5: LSM-style hash index
	"syndrdb/src/pkg/common/helpers"

	// Import the graphQL schema package for automatic schema generation
	graphQLSchema "syndrdb/src/internal/graphQL/schema"

	// Service Registry for dependency injection (breaks circular dependencies)
	"syndrdb/src/internal/registry"

	// JOIN hash table cache invalidation
	joinexecutor "syndrdb/src/internal/query/join_executor"

	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// PageCacheShardCount is the number of shards for page cache locks (must be power of 2)
const PageCacheShardCount = 64

// findDocumentPageScanLimit caps the fallback page scan when DocumentID index is missing (Issue 8).
const findDocumentPageScanLimit = 100

// pageCacheShard represents a single shard of the page cache.
// Each shard has its own lock, map, and LRU tracking to eliminate global lock contention.
// DEADLOCK FIX: Previously a global documentPagesMutex caused RWMutex starvation under high concurrency.
// By sharding, writers in shard N don't block readers in shard M.
// POSTGRESQL-INSPIRED: Dual-map architecture with lock-free reads for cache hits (buffer manager pattern).
type pageCacheShard struct {
	mu          sync.RWMutex                    // Protects pages, lruOrder, lruElements
	pages       map[string]*models.DocumentPage // pageKey -> page (authoritative, protected by mu)
	fastLookup  sync.Map                        // Lock-free lookup cache (pageKey -> *DocumentPage)
	lruOrder    *list.List                      // LRU order for this shard
	lruElements map[string]*list.Element        // pageKey -> list element for O(1) promotion
	maxPages    int                             // Max pages per shard (total max / shard count)

	// PHASE 3: COW (Copy-On-Write) snapshot cache for GROUP BY optimization
	// Caches document snapshots to avoid RLock contention during parallel page loading
	// Key: pageKey, Value: cowSnapshotEntry with documents and timestamp
	// TODO: Expand to other SELECT paths beyond GROUP BY (per user requirement)
	cowSnapshot sync.Map // pageKey -> *cowSnapshotEntry

	// READER VIEW: Immutable snapshot per page for lock-free reads (READ_WRITE_CONTENTION_ANALYSIS).
	// Key: pageKey, Value: *models.DocumentPage (immutable; never mutated after store).
	// Readers load this without holding mu; writers update authoritative then store new snapshot.
	readerView sync.Map // pageKey -> *DocumentPage
}

// cowSnapshotEntry holds a cached document snapshot with timestamp for staleness checking
// PHASE 3: Copy-on-write snapshot to avoid RLock contention in GROUP BY parallel loading
type cowSnapshotEntry struct {
	documents []models.Document // Snapshot of page documents
	timestamp int64             // Unix timestamp (milliseconds) when snapshot was created
	pageKey   string            // Page key for invalidation
}

// newPageCacheShard creates a new page cache shard
func newPageCacheShard(maxPagesPerShard int) *pageCacheShard {
	return &pageCacheShard{
		pages:       make(map[string]*models.DocumentPage),
		lruOrder:    list.New(),
		lruElements: make(map[string]*list.Element),
		maxPages:    maxPagesPerShard,
	}
}

// insertLocked inserts a page into both the authoritative map and lock-free lookup cache.
// Caller must hold mu.Lock().
func (s *pageCacheShard) insertLocked(pageKey string, page *models.DocumentPage) {
	s.pages[pageKey] = page
	s.fastLookup.Store(pageKey, page)
	// PHASE 3: Invalidate COW snapshot on write
	s.cowSnapshot.Delete(pageKey)
}

// deleteLocked deletes a page from both the authoritative map and lock-free lookup cache.
// Also removes the reader view so evictions do not leave stale snapshots.
// Caller must hold mu.Lock().
func (s *pageCacheShard) deleteLocked(pageKey string) {
	delete(s.pages, pageKey)
	s.fastLookup.Delete(pageKey)
	s.readerView.Delete(pageKey)
	s.cowSnapshot.Delete(pageKey) // Invalidate COW snapshot on eviction to prevent stale reads
	// Clean up LRU tracking to prevent memory leaks
	if elem, exists := s.lruElements[pageKey]; exists {
		s.lruOrder.Remove(elem)
		delete(s.lruElements, pageKey)
	}
}

// evictOldestLocked evicts the oldest page from this shard. Caller must hold mu.Lock().
func (s *pageCacheShard) evictOldestLocked() {
	if s.lruOrder.Len() == 0 {
		return
	}
	oldest := s.lruOrder.Back()
	if oldest != nil {
		pageKey := oldest.Value.(string)
		s.lruOrder.Remove(oldest)
		delete(s.lruElements, pageKey)
		s.deleteLocked(pageKey)
	}
}

// compactFastLookup recreates the fastLookup sync.Map from the authoritative pages map.
// This removes accumulated "expunged" entries that slow down Load() operations.
//
// PERFORMANCE FIX: Go's sync.Map.Delete() doesn't free memory - it marks entries as
// "expunged" but they remain in internal structures. Over time, this causes:
// 1. More entries to scan during Load()
// 2. Periodic expensive "dirty to read" map promotions
// 3. Memory fragmentation
//
// After many page evictions, the sync.Map accumulates cruft that degrades performance.
// This function creates a fresh sync.Map with only current entries, eliminating the overhead.
func (s *pageCacheShard) compactFastLookup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create fresh sync.Map
	var newLookup sync.Map

	// Copy only current entries from authoritative pages map
	for pageKey, page := range s.pages {
		newLookup.Store(pageKey, page)
	}

	// Replace old sync.Map (old one will be GC'd)
	s.fastLookup = newLookup
}

// compactRegularMaps recreates the pages and lruElements Go maps to reclaim bucket memory.
//
// PERFORMANCE FIX: Go's regular maps (unlike sync.Map) never shrink their bucket arrays.
// When entries are deleted, buckets remain allocated but empty. During high-churn workloads
// (add/update/delete cycles), maps grow to peak size then accumulate empty buckets.
// This causes:
// 1. More buckets to scan during iterations
// 2. Memory fragmentation
// 3. Cache inefficiency due to sparse data
//
// Solution: Periodically recreate maps with only current entries, sized to exact current count.
// This reclaims wasted bucket memory and improves iteration performance.
//
// THREAD SAFETY: Must hold s.mu.Lock() during the entire operation.
func (s *pageCacheShard) compactRegularMaps() (entriesCompacted int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entriesCompacted = len(s.pages)

	// Skip if empty - no benefit to compacting empty maps
	if entriesCompacted == 0 {
		return 0
	}

	// Recreate pages map with exact size (no over-allocation of buckets)
	newPages := make(map[string]*models.DocumentPage, entriesCompacted)
	for k, v := range s.pages {
		newPages[k] = v
	}
	s.pages = newPages

	// Recreate lruElements map with exact size
	newLruElements := make(map[string]*list.Element, len(s.lruElements))
	for k, v := range s.lruElements {
		newLruElements[k] = v
	}
	s.lruElements = newLruElements

	return entriesCompacted
}

// compactCOWSnapshot recreates the cowSnapshot sync.Map with only fresh (non-stale) entries.
// This combines cleanup and compaction in one operation, eliminating both stale entries
// and accumulated "expunged" tombstones from Delete() operations.
//
// PERFORMANCE FIX: Same issue as fastLookup - sync.Map.Delete() marks entries as "expunged"
// but doesn't free memory. The old cleanStaleCOWSnapshots() approach used Delete() every 5s,
// which added tombstones without removing them, causing 17ms → 128ms latency degradation.
//
// COMBINED APPROACH:
// - Ranges over existing cowSnapshot entries
// - Filters out stale entries (age > GroupBySnapshotStalenessMs)
// - Creates fresh sync.Map with only current, non-stale entries
// - Eliminates expunged tombstones and cleans stale entries in one operation
//
// THREAD SAFETY: Holds s.mu.Lock() briefly during sync.Map replacement to prevent torn reads.
// The Range() operations are done on the old sync.Map before acquiring the lock.
func (s *pageCacheShard) compactCOWSnapshot(stalenessMs int64, now int64) (entriesBefore, entriesAfter int) {
	// Count entries before compaction (for metrics) - done without lock
	s.cowSnapshot.Range(func(key, value interface{}) bool {
		entriesBefore++
		return true
	})

	// Create fresh sync.Map and populate with only fresh entries - done without lock
	var newSnapshot sync.Map
	s.cowSnapshot.Range(func(key, value interface{}) bool {
		snapshot := value.(*cowSnapshotEntry)
		age := now - snapshot.timestamp
		if age <= stalenessMs {
			newSnapshot.Store(key, value)
			entriesAfter++
		}
		return true
	})

	// Replace old sync.Map atomically under lock (old one will be GC'd with all expunged entries)
	// CRITICAL: sync.Map is a value type - assignment is not atomic without lock
	s.mu.Lock()
	s.cowSnapshot = newSnapshot
	s.mu.Unlock()

	return entriesBefore, entriesAfter
}

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

// StalePageIDFallbackCounter tracks how often the cache-scan fallback is triggered
// due to stale pageID entries in indexes. High values indicate need for index rebuild.
var StalePageIDFallbackCounter atomic.Uint64

// GetStalePageIDFallbackCount returns the current count of stale pageID fallbacks
func GetStalePageIDFallbackCount() uint64 {
	return StalePageIDFallbackCounter.Load()
}

// ResetStalePageIDFallbackCount resets the counter (for testing or periodic reset)
func ResetStalePageIDFallbackCount() {
	StalePageIDFallbackCounter.Store(0)
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

// Fast type converter functions - eliminate reflection overhead
func convertToString(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a string
	if strVal, ok := value.(string); ok {
		// Allow NULL magic values to pass through without conversion
		if strings.HasPrefix(strVal, "::SYNDR_") {
			return strVal, nil
		}
		return strVal, nil
	}
	// Convert other types to string without reflection
	return conversion.ValueToString(value), nil
}

func convertToInt(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: direct type assertions (no reflection)
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		// Check if float64 represents a whole number
		if v != float64(int64(v)) {
			return nil, fmt.Errorf("expected integer but got float with decimal places: %v", v)
		}
		return int64(v), nil
	case float32:
		// Check if float32 represents a whole number
		if v != float32(int32(v)) {
			return nil, fmt.Errorf("expected integer but got float with decimal places: %v", v)
		}
		return int64(v), nil
	case string:
		// Allow NULL magic values to pass through - NULLs are valid for any type
		if strings.HasPrefix(v, "::SYNDR_") {
			return v, nil
		}
		// Parse string as integer - only expensive operation left
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to integer: %v", v, err)
		}
		return intVal, nil
	default:
		return nil, fmt.Errorf("expected integer but got %T", value)
	}
}

func convertToFloat(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: direct type assertions (no reflection)
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		// Allow NULL magic values to pass through - NULLs are valid for any type
		if strings.HasPrefix(v, "::SYNDR_") {
			return v, nil
		}
		// Parse string as float - only expensive operation left
		floatVal, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to float: %v", v, err)
		}
		return floatVal, nil
	default:
		return nil, fmt.Errorf("expected number but got %T", value)
	}
}

func convertToBool(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: direct type assertions (no reflection)
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		// Allow NULL magic values to pass through - NULLs are valid for any type
		if strings.HasPrefix(v, "::SYNDR_") {
			return v, nil
		}
		// Parse string as boolean
		if strings.EqualFold(v, "true") {
			return true, nil
		}
		if strings.EqualFold(v, "false") {
			return false, nil
		}
		return nil, fmt.Errorf("cannot convert string '%s' to boolean (expected 'true' or 'false')", v)
	default:
		return nil, fmt.Errorf("expected boolean but got %T", value)
	}
}

func convertToDateTime(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a time.Time
	if timeVal, ok := value.(time.Time); ok {
		// ✅ Return FieldValue directly so type info preserved (DateTime)
		return models.NewDateTimeValue(timeVal.UTC()), nil
	}
	// Handle string values
	if strVal, ok := value.(string); ok {
		// Allow NULL magic values to pass through
		if strings.HasPrefix(strVal, "::SYNDR_") {
			return strVal, nil
		}
		// Parse datetime string - this was already done in parseValue, but handle legacy cases
		if parsedTime, _, err := utils.ParseDateTime(strVal); err == nil {
			// ✅ Return FieldValue directly so type info preserved (DateTime)
			return models.NewDateTimeValue(parsedTime.UTC()), nil
		} else {
			return nil, fmt.Errorf("cannot convert string '%s' to datetime: %v", strVal, err)
		}
	}
	return nil, fmt.Errorf("expected datetime but got %T", value)
}

func convertToDate(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a time.Time
	if timeVal, ok := value.(time.Time); ok {
		// Date: zero out time component to midnight UTC
		utc := timeVal.UTC()
		dateTime := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
		// ✅ Return FieldValue directly so type info preserved (Date)
		return models.NewDateValue(dateTime), nil
	}
	// Handle string values
	if strVal, ok := value.(string); ok {
		// Allow NULL magic values to pass through
		if strings.HasPrefix(strVal, "::SYNDR_") {
			return strVal, nil
		}
		// Parse date string
		if parsedTime, _, err := utils.ParseDateTime(strVal); err == nil {
			// Zero out time to midnight UTC for Date type
			utc := parsedTime.UTC()
			dateTime := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
			// ✅ Return FieldValue directly so type info preserved (Date)
			return models.NewDateValue(dateTime), nil
		} else {
			return nil, fmt.Errorf("cannot convert string '%s' to date: %v", strVal, err)
		}
	}
	return nil, fmt.Errorf("expected date but got %T", value)
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

	// Performance optimization: Deferred metadata updates
	metadataUpdateBuffer    []MetadataUpdate // Buffer for pending metadata updates
	metadataPersistInterval int              // Number of operations before forcing metadata persist
	metadataOperationCount  int              // Count of operations since last metadata flush
	lastMetadataFlush       time.Time        // Last time metadata updates were flushed
	metadataUpdateMutex     sync.RWMutex     // Protects metadata update buffer and operation count (RWMutex for read optimization)

	// PHASE 1 OPTIMIZATION: Bulk operation detection for WAL bypass
	bulkModeEnabled        bool      // Current bulk mode state
	operationCount         int       // Operations in current time window
	operationWindow        time.Time // Start of current measurement window
	bulkThresholdOpsPerSec int       // Operations per second threshold for bulk mode

	// DOCUMENT SCANNER INTEGRATION: Add scanner management
	scannerIntegration *documentscanner.ScannerIntegration                 // Scanner integration instance
	bundleScanners     map[string]documentscanner.DocumentScannerInterface // Per-bundle scanners
	scannerMutex       sync.RWMutex                                        // Protects bundleScanners map

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
		bundleScanners:     make(map[string]documentscanner.DocumentScannerInterface),

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

	return service
}

// SetCatalogService injects the catalog service reference after construction.
// This is necessary to break the circular dependency between BundleService and CatalogService.
// Should be called during server initialization after all services are created.
func (s *BundleService) SetCatalogService(catalogService CatalogServiceInterface) {
	s.catalogService = catalogService
	s.logger.Debug("Catalog service injected into BundleService")
}

// SetStatsUpdater injects a column statistics updater for incremental stats maintenance.
func (s *BundleService) SetStatsUpdater(updater StatsUpdater) {
	s.statsUpdater = updater
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

// startCOWSnapshotCleaner starts a background goroutine that periodically sweeps
// and removes stale COW snapshots from all page cache shards.
// This prevents stale snapshot accumulation and eliminates hot-path cleanup overhead.
//
// PERFORMANCE FIX: Root cause of 17ms → 128ms latency degradation (7.5x) in write-heavy workloads.
// Go's sync.Map.Delete() doesn't free memory - it marks entries as "expunged" but they remain
// in internal structures. The old cleanStaleCOWSnapshots() approach used Delete() every 5s,
// which added tombstones without removing them. Over time, Load() operations had to scan
// through accumulated expunged entries, causing severe performance degradation.
//
// COMBINED APPROACH (replaces cleanStaleCOWSnapshots):
// - Rebuilds sync.Map every 30 seconds with only fresh entries
// - Eliminates expunged tombstones and cleans stale entries in one operation
// - Similar to fastLookup compaction but for cowSnapshot cache
// - 30s interval balances freshness (5s staleness threshold) with compaction overhead
func (s *BundleService) startCOWSnapshotCompactor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Compact every 30 seconds
	s.logger.Debug("Background COW snapshot compactor started (30s interval)")

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background COW snapshot compactor stopped")
				return
			case <-ticker.C:
				s.compactAllCOWSnapshots()
			}
		}
	}()
}

// compactAllCOWSnapshots compacts the cowSnapshot sync.Map in all page cache shards.
// This combines cleanup (remove stale entries) and compaction (remove expunged tombstones)
// in one operation by rebuilding each sync.Map with only fresh, non-stale entries.
//
// PERFORMANCE FIX: Root cause of 17ms → 128ms latency degradation in write-heavy workloads.
// The old cleanStaleCOWSnapshots() approach used Delete() every 5s, which marked entries as
// "expunged" but didn't free memory. This caused Load() operations to scan through accumulated
// tombstones, degrading performance on subsequent runs.
//
// COMBINED APPROACH (replaces cleanStaleCOWSnapshots):
// - Rebuilds sync.Map with only fresh entries (age ≤ GroupBySnapshotStalenessMs)
// - Eliminates expunged tombstones from previous Delete() operations
// - Cleans stale entries and compacts in one operation
// - Runs every 30 seconds (vs 5s for old cleaner) to balance freshness and overhead
func (s *BundleService) compactAllCOWSnapshots() {
	stalenessMs := settings.GetSettings().GroupBySnapshotStalenessMs
	now := time.Now().UnixMilli()
	totalBefore := 0
	totalAfter := 0

	for _, shard := range s.pageShards {
		before, after := shard.compactCOWSnapshot(int64(stalenessMs), now)
		totalBefore += before
		totalAfter += after
	}

	if totalBefore > 0 {
		removed := totalBefore - totalAfter
		s.logger.Debugf("COW snapshot compactor: compacted %d shards, %d entries → %d entries (%d removed, threshold: %dms)",
			PageCacheShardCount, totalBefore, totalAfter, removed, stalenessMs)
	}
}

// startFastLookupCompactor starts a background goroutine that periodically compacts
// the fastLookup sync.Map in all page cache shards.
//
// PERFORMANCE FIX: Root cause of remaining 50ms latency degradation after first run
// Go's sync.Map.Delete() doesn't free memory - it marks entries as "expunged" but they
// remain in internal structures. After many page evictions, the sync.Map accumulates
// cruft that degrades Load() performance:
// - First run: Fresh sync.Map, fast Load() operations
// - Second run: Accumulated expunged entries cause slower Load() operations
// - Solution: Periodically recreate sync.Map with only current entries
func (s *BundleService) startFastLookupCompactor(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second) // Compact every 60 seconds
	s.logger.Debug("Background fastLookup compactor started (60s interval)")

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background fastLookup compactor stopped")
				return
			case <-ticker.C:
				s.compactAllFastLookups()
			}
		}
	}()
}

// compactAllFastLookups compacts the fastLookup sync.Map and regular maps in all page cache shards.
// This recreates each sync.Map with only current entries, removing accumulated cruft
// from deleted entries that slow down Load() operations.
// Also compacts regular Go maps (pages, lruElements) to reclaim bucket memory from deletions.
func (s *BundleService) compactAllFastLookups() {
	totalPages := 0
	regularMapEntries := 0
	for _, shard := range s.pageShards {
		shard.compactFastLookup()
		// Also compact regular maps to reclaim bucket memory
		regularMapEntries += shard.compactRegularMaps()
		// Count pages for logging (requires lock, but we just released it)
		shard.mu.RLock()
		totalPages += len(shard.pages)
		shard.mu.RUnlock()
	}
	s.logger.Debugf("FastLookup compactor: compacted %d shards (%d total pages, %d regular map entries)",
		PageCacheShardCount, totalPages, regularMapEntries)
}

// CompactAllCaches compacts all caches in BundleService and the underlying store.
// PERFORMANCE FIX: Go's map delete() doesn't shrink the bucket array. After many
// connect/disconnect cycles with writes, caches accumulate empty bucket slots that
// degrade iteration and memory performance. Call this periodically (every 60s) to
// reclaim memory and restore lookup speed.
// Returns total entries across all caches after compaction.
func (s *BundleService) CompactAllCaches() int {
	total := 0

	// Compact page shards (both sync.Map and regular maps)
	for _, shard := range s.pageShards {
		shard.compactFastLookup()
		total += shard.compactRegularMaps()
	}

	// Compact underlying store's caches
	total += s.store.CompactAllCaches()

	return total
}

// FlushAllDocumentCaches aggressively clears ALL document-holding caches.
// This is more aggressive than CompactAllCaches - it completely removes cached
// document data rather than just compacting map structures.
//
// PERFORMANCE FIX: When all clients disconnect between test runs, document objects
// accumulate in caches (COW snapshots, page cache, file cache). While map compaction
// reclaims bucket memory, the actual document data remains. This method provides
// a "fresh start" equivalent to server restart, preventing latency degradation
// across consecutive test runs.
//
// This method should be called when all clients have disconnected to prepare for
// the next session cycle with clean caches.
func (s *BundleService) FlushAllDocumentCaches() {
	s.logger.Info("Flushing all document caches for clean state")

	// DIAGNOSTIC: Log cache sizes before flushing to track what's accumulating
	totalPageCacheEntries := 0
	for _, shard := range s.pageShards {
		shard.mu.RLock()
		totalPageCacheEntries += len(shard.pages)
		shard.mu.RUnlock()
	}
	loadedIndexCount := 0
	if s.loadedIndexes != nil {
		s.loadedIndexes.ForEach(func(bundleName string, indexes map[string]interface{}) bool {
			loadedIndexCount += len(indexes)
			return true
		})
	}
	s.logger.Infof("Cache state before flush: pageCache=%d entries, loadedIndexes=%d, bundleMetadata=%d bundles",
		totalPageCacheEntries, loadedIndexCount, len(s.bundleMetadata))

	// Clear page cache shards - these hold DocumentPage objects with document data
	for _, shard := range s.pageShards {
		shard.mu.Lock()
		// Clear authoritative page cache
		shard.pages = make(map[string]*models.DocumentPage)
		// Clear LRU tracking
		shard.lruOrder = list.New()
		shard.lruElements = make(map[string]*list.Element)
		// Clear fast lookup sync.Map by replacing with empty one
		shard.fastLookup = sync.Map{}
		// Clear COW snapshot cache by replacing with empty one
		shard.cowSnapshot = sync.Map{}
		shard.mu.Unlock()
	}

	// Clear document-to-page mapping cache (documentPageCache)
	// This cache maps documentID -> pageID and grows during document operations
	if s.documentPageCache != nil {
		s.documentPageCache.Flush()
	}

	// PERFORMANCE FIX: Compact MemTables instead of closing indexes
	// Closing indexes causes expensive reload on next access (open files, read headers,
	// restore sequences). Instead, we compact the MemTables which clears accumulated
	// data while keeping indexes hot in memory.
	//
	// The compactAllHashIndexMemTables function:
	// - Clears walBuffer (main memory hog during writes)
	// - Optionally rebuilds entries map to reclaim memory
	// - Keeps file handles open for fast subsequent access
	s.logger.Info("Compacting hash index MemTables instead of closing indexes")
	s.compactAllHashIndexMemTables()

	// For indexes loaded via EnsureHashIndexV3Loaded (bundleMetadata path),
	// we also need to compact their MemTables. Since they might not be tracked
	// in bundleMetadata's iteration, just force a full compaction.
	memtablesCompacted := 0
	if s.loadedIndexes != nil {
		s.loadedIndexes.ForEach(func(bundleName string, indexes map[string]interface{}) bool {
			for indexName, idx := range indexes {
				if hashIdx, ok := idx.(*hashindex.HashIndexV3); ok {
					// Force compaction with entries cleanup
					walCleared, _, err := hashIdx.CompactMemTableSafe(true)
					if err != nil {
						s.logger.Warnf("Error compacting MemTable for '%s.%s': %v", bundleName, indexName, err)
					} else if walCleared > 0 {
						memtablesCompacted++
					}
				}
			}
			return true
		})
	}
	if memtablesCompacted > 0 {
		s.logger.Infof("Compacted %d index MemTables during cache flush", memtablesCompacted)
	}

	// For bundleMetadata path indexes, compact but don't close
	for _, bundle := range s.bundleMetadata {
		if bundle.Indexes != nil {
			for indexName := range bundle.Indexes {
				indexRef := bundle.Indexes[indexName]
				if indexRef.IndexInstance != nil {
					if hashIdx, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3); ok {
						hashIdx.CompactMemTableSafe(true)
					}
				}
			}
		}
	}

	// Flush underlying store's document caches
	s.store.FlushAllDocumentCaches()

	// Call external flush callback if registered (e.g., JOIN hash table cache)
	if s.onCacheFlush != nil {
		s.onCacheFlush()
	}

	s.logger.Info("All document caches flushed")
}

// startMemTableCompactor starts a background goroutine that periodically compacts
// all loaded hash index MemTables to prevent unbounded memory growth.
//
// PERFORMANCE FIX: Root cause of sustained write workload latency degradation.
// Hash index MemTable's walBuffer grows unboundedly during continuous writes:
// - Each Put() appends to walBuffer for WAL tracking
// - During sustained writes, walBuffer can grow to millions of entries
// - This causes memory pressure and GC overhead
//
// SOLUTION:
// - Every 30 seconds, iterate all loaded hash indexes
// - Call CompactMemTableSafe() which atomically swaps walBuffer with fresh empty buffer
// - Writers blocked only for microseconds during swap
// - Old buffer is GC'd after swap completes
//
// THREAD SAFETY:
// - Uses CompactMemTableSafe() which only holds lock briefly for O(1) swap
// - Writers continue immediately on new buffer
// - No risk of data loss - disk writes happen during normal Put() operations
func (s *BundleService) startMemTableCompactor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Compact every 30 seconds
	s.logger.Debug("Background hash index MemTable compactor started (30s interval)")

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background hash index MemTable compactor stopped")
				return
			case <-ticker.C:
				s.compactAllHashIndexMemTables()
			}
		}
	}()
}

// compactAllHashIndexMemTables compacts the MemTable in all loaded hash indexes.
// This clears walBuffer and optionally rebuilds entries map to reclaim memory.
//
// DESIGN:
// - Always compacts walBuffer (main memory hog during sustained writes)
// - Entries map compaction triggered by time interval (60s) or idle timeout (30s)
// - Time-based compaction ensures memory reclamation without impacting active bursts
//
// ENTRIES COMPACTION TRIGGERS:
// - Interval (60s): Force compaction every 60s to prevent unbounded growth
// - Idle (30s): Compact when no activity for 30s (burst has ended)
const (
	entriesCompactIntervalSec = 60 // Force entries compaction every 60s
	entriesCompactIdleSec     = 30 // Compact entries after 30s of idle
)

func (s *BundleService) compactAllHashIndexMemTables() {
	totalWALCleared := 0
	indexesCompacted := 0
	entriesCompacted := 0

	// Iterate through all bundles and their loaded hash indexes
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Indexes == nil {
			continue
		}

		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexInstance == nil {
				continue // Index not loaded in memory
			}

			if indexRef.IndexType != "hash" {
				continue // Only compact hash indexes
			}

			// Cast to HashIndexV3
			hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3)
			if !ok {
				continue
			}

			// Check if entries map needs compaction (60s interval OR 30s idle)
			shouldCompactEntries, reason := hashIndex.NeedsEntriesCompaction(
				entriesCompactIntervalSec,
				entriesCompactIdleSec,
			)

			// Perform safe compaction
			// - Always clears walBuffer
			// - Conditionally rebuilds entries map based on time/idle triggers
			walCleared, entriesCount, err := hashIndex.CompactMemTableSafe(shouldCompactEntries)
			if err != nil {
				// Log but continue - don't let one index failure stop others
				s.logger.Warnf("Failed to compact MemTable for index '%s.%s': %v",
					bundleName, indexName, err)
				continue
			}

			if walCleared > 0 {
				totalWALCleared += walCleared
				indexesCompacted++
			}

			if shouldCompactEntries {
				entriesCompacted++
				s.logger.Debugf("Entries compaction triggered for '%s.%s': %s (entries=%d)",
					bundleName, indexName, reason, entriesCount)
			}
		}
	}

	if totalWALCleared > 0 || entriesCompacted > 0 {
		s.logger.Debugf("MemTable compactor: compacted %d indexes, cleared %d WAL entries, entries compacted=%d",
			indexesCompacted, totalWALCleared, entriesCompacted)
	}
}

// ============================================================================
// BUFFER DIAGNOSTICS
// These functions provide visibility into buffer sizes for debugging latency
// degradation issues during sustained write workloads.
// ============================================================================

// BufferDiagnostics contains a snapshot of all buffer sizes at a point in time
type BufferDiagnostics struct {
	Timestamp             time.Time                      `json:"timestamp"`
	IndexUpdateBufferSize int                            `json:"indexUpdateBufferSize"`
	MetadataBufferSize    int                            `json:"metadataBufferSize"`
	PageCacheStats        PageCacheDiagnostics           `json:"pageCacheStats"`
	HashIndexStats        []HashIndexDiagnostics         `json:"hashIndexStats"`
	WriteBufferStats      []bundlestore.WriteBufferStats `json:"writeBufferStats"`
	TotalDataFileSize     int64                          `json:"totalDataFileSize"`
}

// PageCacheDiagnostics contains page cache shard statistics
type PageCacheDiagnostics struct {
	TotalPages        int   `json:"totalPages"`
	TotalCOWSnapshots int   `json:"totalCOWSnapshots"`
	TotalFastLookup   int   `json:"totalFastLookup"`
	ShardSizes        []int `json:"shardSizes"`
}

// HashIndexDiagnostics contains MemTable statistics for a single hash index
type HashIndexDiagnostics struct {
	BundleName    string `json:"bundleName"`
	IndexName     string `json:"indexName"`
	EntriesCount  int    `json:"entriesCount"`
	WALBufferSize int    `json:"walBufferSize"`
	MaxSize       int    `json:"maxSize"`
}

// GetBufferDiagnostics returns a snapshot of all buffer sizes for debugging.
// Call this periodically or on-demand to track memory growth patterns.
//
// USAGE:
//
//	diag := service.GetBufferDiagnostics()
//	logger.Infof("Buffer diagnostics: %+v", diag)
//
// THREAD SAFETY: Acquires necessary locks briefly to read sizes.
func (s *BundleService) GetBufferDiagnostics() BufferDiagnostics {
	diag := BufferDiagnostics{
		Timestamp: time.Now(),
	}

	// Get indexUpdateBuffer size
	s.indexUpdateMutex.Lock()
	diag.IndexUpdateBufferSize = len(s.indexUpdateBuffer)
	s.indexUpdateMutex.Unlock()

	// Get metadataUpdateBuffer size
	s.metadataUpdateMutex.RLock()
	diag.MetadataBufferSize = len(s.metadataUpdateBuffer)
	s.metadataUpdateMutex.RUnlock()

	// Get page cache statistics from all shards
	diag.PageCacheStats.ShardSizes = make([]int, PageCacheShardCount)
	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		shard.mu.RLock()
		shardSize := len(shard.pages)
		diag.PageCacheStats.ShardSizes[i] = shardSize
		diag.PageCacheStats.TotalPages += shardSize

		// Count cowSnapshot entries (sync.Map doesn't have Len())
		// CRITICAL FIX: Must hold RLock while calling Range() because compactCOWSnapshot()
		// can replace the entire sync.Map variable. Without the lock, we could iterate
		// on a partially-constructed or GC'd sync.Map causing "unlock of unlocked mutex" panic.
		cowCount := 0
		shard.cowSnapshot.Range(func(_, _ interface{}) bool {
			cowCount++
			return true
		})
		diag.PageCacheStats.TotalCOWSnapshots += cowCount

		// Count fastLookup entries
		// Same issue: compactFastLookup() can replace this sync.Map
		fastCount := 0
		shard.fastLookup.Range(func(_, _ interface{}) bool {
			fastCount++
			return true
		})
		diag.PageCacheStats.TotalFastLookup += fastCount

		shard.mu.RUnlock()
	}

	// Get hash index MemTable statistics
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Indexes == nil {
			continue
		}

		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexInstance == nil {
				continue
			}
			if indexRef.IndexType != "hash" {
				continue
			}

			hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3)
			if !ok {
				continue
			}

			stats := hashIndex.MemTable.GetStats()
			diag.HashIndexStats = append(diag.HashIndexStats, HashIndexDiagnostics{
				BundleName:    bundleName,
				IndexName:     indexName,
				EntriesCount:  stats.Size,
				WALBufferSize: stats.WALBufferSize,
				MaxSize:       stats.MaxSize,
			})
		}
	}

	// Get write buffer statistics (file sizes)
	diag.WriteBufferStats = s.store.GetAllWriteBufferStats()
	for _, wbStats := range diag.WriteBufferStats {
		diag.TotalDataFileSize += wbStats.FileSize
	}

	return diag
}

// LogBufferDiagnostics logs current buffer sizes at INFO level.
// Call this when investigating latency degradation.
func (s *BundleService) LogBufferDiagnostics() {
	diag := s.GetBufferDiagnostics()

	s.logger.Infof("=== BUFFER DIAGNOSTICS at %s ===", diag.Timestamp.Format(time.RFC3339))
	s.logger.Infof("  IndexUpdateBuffer: %d entries", diag.IndexUpdateBufferSize)
	s.logger.Infof("  MetadataBuffer: %d entries", diag.MetadataBufferSize)
	s.logger.Infof("  PageCache: %d pages, %d COW snapshots, %d fastLookup entries",
		diag.PageCacheStats.TotalPages,
		diag.PageCacheStats.TotalCOWSnapshots,
		diag.PageCacheStats.TotalFastLookup)

	// Log write buffer (data file) statistics
	s.logger.Infof("  WriteBuffers: %d files, total size: %.2f MB",
		len(diag.WriteBufferStats), float64(diag.TotalDataFileSize)/(1024*1024))
	for _, wb := range diag.WriteBufferStats {
		s.logger.Infof("    %s: size=%.2f MB, buffer=%d, directWrites=%d",
			wb.FilePath, float64(wb.FileSize)/(1024*1024), wb.BufferLen, wb.DirectWrites)
	}

	for _, idx := range diag.HashIndexStats {
		s.logger.Infof("  HashIndex %s.%s: entries=%d, walBuffer=%d, maxSize=%d",
			idx.BundleName, idx.IndexName, idx.EntriesCount, idx.WALBufferSize, idx.MaxSize)
	}
	s.logger.Infof("=== END BUFFER DIAGNOSTICS ===")
}

// RecordWriteActivity updates the last write activity timestamp.
// Call this on every write operation to reset the idle timer for diagnostics.
// Thread-safe: Uses atomic operation.
func (s *BundleService) RecordWriteActivity() {
	s.lastWriteActivity.Store(time.Now().UnixNano())
}

// RecordActivity updates the last activity timestamp for any server activity.
// Call this on every command execution (read or write) to reset the idle cache flush timer.
// This ensures the server correctly detects idle state during read-only workloads.
// Thread-safe: Uses atomic operation.
func (s *BundleService) RecordActivity() {
	s.lastActivity.Store(time.Now().UnixNano())
}

// startDiagnosticsLogger starts a background goroutine that logs buffer diagnostics
// after 30 seconds of idle (no write activity).
//
// DESIGN:
// - Checks every 5 seconds if there's been 30+ seconds of idle time
// - If idle threshold exceeded, logs diagnostics and resets the timer
// - Does NOT log during active write bursts to avoid log spam
// - Useful for debugging latency degradation after workload ends
//
// THREAD SAFETY: Uses atomic reads for activity timestamp
func (s *BundleService) startDiagnosticsLogger(ctx context.Context) {
	const (
		checkInterval = 5 * time.Second  // How often to check for idle
		idleThreshold = 30 * time.Second // Log after this much idle time
	)

	ticker := time.NewTicker(checkInterval)
	s.logger.Debug("Background buffer diagnostics logger started (30s idle threshold)")

	go func() {
		defer ticker.Stop()
		var lastLoggedForActivity int64 // Track which activity timestamp we last logged for

		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background buffer diagnostics logger stopped")
				return
			case <-ticker.C:
				activityNano := s.lastWriteActivity.Load()
				lastActivity := time.Unix(0, activityNano)
				idleTime := time.Since(lastActivity)

				// Log if we've been idle for 30+ seconds AND haven't logged for this activity period
				// This ensures we log once per "burst" of activity after 30s idle
				if idleTime >= idleThreshold && lastLoggedForActivity != activityNano {
					s.LogBufferDiagnostics()
					lastLoggedForActivity = activityNano
				}
			}
		}
	}()
}

// startIdleBufferFlusher starts a background goroutine that flushes all WriteBuffers
// after a period of idle (no write activity).
//
// ROOT CAUSE FIX: WriteBuffer.flushTimeout (100ms) only triggers on the NEXT write.
// If no writes come after data is buffered, the buffer stays full forever.
// This caused stuck buffers in `order_items` (26KB) and `cart_items` (12KB) that
// never flushed because those bundles weren't being written to anymore.
//
// DESIGN:
// - Checks every 2 seconds if there's been 5+ seconds of idle time
// - If idle threshold exceeded, flushes all WriteBuffers
// - Uses lastWriteActivity from RecordWriteActivity() to detect idle
// - Safe to flush at any time (idempotent operation)
//
// THREAD SAFETY: Uses atomic reads for activity timestamp
func (s *BundleService) startIdleBufferFlusher(ctx context.Context) {
	const (
		checkInterval = 2 * time.Second // How often to check for idle
		idleThreshold = 5 * time.Second // Flush after this much idle time
	)

	ticker := time.NewTicker(checkInterval)
	s.logger.Debug("Background idle buffer flusher started (5s idle threshold)")

	go func() {
		defer ticker.Stop()
		var lastFlushTime time.Time

		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background idle buffer flusher stopped")
				return
			case <-ticker.C:
				lastActivity := time.Unix(0, s.lastWriteActivity.Load())
				idleTime := time.Since(lastActivity)

				// Flush if we've been idle for 5+ seconds AND haven't flushed since last activity
				if idleTime >= idleThreshold && lastFlushTime.Before(lastActivity) {
					if err := s.store.FlushAllWriteBuffers(); err != nil {
						s.logger.Warnf("Background idle buffer flush failed: %v", err)
					} else {
						s.logger.Debug("Background idle buffer flush completed")
					}
					lastFlushTime = time.Now()
				}
			}
		}
	}()
}

// startIdleCacheFlusher starts a background goroutine that flushes all document caches
// when the server has been idle for 30 seconds.
//
// PERFORMANCE FIX: Test run isolation
// When running consecutive test runs, document data accumulates in caches causing
// latency degradation (e.g., 15ms → 46ms → 66ms across runs). This was originally
// triggered when all sessions disconnected, but that approach had race conditions
// with rapid reconnects causing inconsistent results (sometimes 20ms, sometimes 60ms).
//
// DESIGN:
// - Checks every 5 seconds if there's been 30+ seconds of idle time
// - If idle threshold exceeded AND haven't flushed since last activity, flush all caches
// - Uses lastActivity to detect true server idle (reads AND writes, not just writes)
// - Closes indexes properly to release file handles
// - Safe to flush at any time (next access reloads from disk)
//
// THREAD SAFETY: Uses atomic reads for activity timestamp
func (s *BundleService) startIdleCacheFlusher(ctx context.Context) {
	const checkInterval = 5 * time.Second // How often to check for idle

	ticker := time.NewTicker(checkInterval)
	s.logger.Info("Background idle cache flusher started (30s idle threshold)")

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("Background idle cache flusher stopped")
				return
			case <-ticker.C:
				// Use lastActivity (reads + writes) for cache flush detection,
				// not lastWriteActivity which only tracks writes.
				// This prevents false idle detection during read-only workloads.
				lastActivityTime := time.Unix(0, s.lastActivity.Load())
				lastFlush := time.Unix(0, s.lastCacheFlushTime.Load())
				idleTimeNs := time.Since(lastActivityTime).Nanoseconds()

				// Flush if:
				// 1. We've been idle for 30+ seconds, AND
				// 2. We haven't already flushed since the last activity
				if idleTimeNs >= s.idleCacheFlushThresholdNs && lastFlush.Before(lastActivityTime) {
					s.logger.Info("Server idle for 30s - flushing all document caches for clean state")
					s.FlushAllDocumentCaches()
					s.lastCacheFlushTime.Store(time.Now().UnixNano())
				}
			}
		}
	}()
}

// ============================================================================
// WRITE-THROUGH CACHE HELPERS
// These methods implement the write-through cache pattern, ensuring that
// any document written to the WriteBuffer is also immediately available
// in the page cache without requiring a disk round-trip.
// ============================================================================

// getPageShardIndex computes which shard to use for a given page key.
// Uses xxhash for fast, high-quality hashing with bit-masking for modulo.
// Returns an index in [0, PageCacheShardCount).
// getOrCreateVisibilityMap returns the visibility map for the bundle,
// creating one if it doesn't exist. Thread-safe via sync.Map.
func (s *BundleService) getOrCreateVisibilityMap(bundleName string, pageCount uint32) *VisibilityMap {
	if v, ok := s.visibilityMaps.Load(bundleName); ok {
		vm := v.(*VisibilityMap)
		if pageCount > 0 {
			vm.Grow(pageCount)
		}
		return vm
	}
	vm := NewVisibilityMap(bundleName, pageCount)
	actual, _ := s.visibilityMaps.LoadOrStore(bundleName, vm)
	return actual.(*VisibilityMap)
}

// GetVisibilityMap returns the visibility map for a bundle, or nil if none exists.
// Called by the scanner integration layer to pass VM to SmartBundleScanner.
func (s *BundleService) GetVisibilityMap(bundleName string) *VisibilityMap {
	if v, ok := s.visibilityMaps.Load(bundleName); ok {
		return v.(*VisibilityMap)
	}
	return nil
}

// clearVisibilityForPage clears the visibility bit for a page after a write operation.
// Called from all write paths (insert, update, delete) that modify page content.
func (s *BundleService) clearVisibilityForPage(bundleName string, pageID uint32) {
	if v, ok := s.visibilityMaps.Load(bundleName); ok {
		v.(*VisibilityMap).ClearPage(pageID)
	}
}

// clearVisibilityForBundle clears all visibility bits for a bundle.
// Called after compaction or bulk operations that invalidate the entire bundle.
func (s *BundleService) clearVisibilityForBundle(bundleName string) {
	if v, ok := s.visibilityMaps.Load(bundleName); ok {
		v.(*VisibilityMap).ClearAll()
	}
}

// startVisibilityMapRefresher runs a background goroutine that periodically evaluates
// pages and sets all-visible bits. Similar to PostgreSQL's VACUUM setting VM bits.
// Pages marked all-visible allow scanners to skip per-document MVCC checks entirely.
func (s *BundleService) startVisibilityMapRefresher(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second) // Evaluate every 10 seconds
	defer ticker.Stop()
	s.logger.Debug("Background visibility map refresher started (10s interval)")

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("Background visibility map refresher stopped")
			return
		case <-ticker.C:
			s.refreshVisibilityMaps()
		}
	}
}

// refreshVisibilityMaps evaluates all tracked bundles and sets all-visible bits
// for pages where every document is committed, not deleted, and not superseded.
func (s *BundleService) refreshVisibilityMaps() {
	// Get oldest active snapshot from SnapshotManager
	var oldestSnapshot uint64
	serviceRegistry := registry.GetRegistry()
	if walManager := serviceRegistry.GetWALManager(); walManager != nil {
		if snapshotMgr := walManager.GetSnapshotManager(); snapshotMgr != nil {
			oldestSnapshot = snapshotMgr.GetOldestActiveSnapshot()
		}
	}

	// Iterate all known bundles with visibility maps
	s.visibilityMaps.Range(func(key, value interface{}) bool {
		bundleName := key.(string)
		vm := value.(*VisibilityMap)

		// Look up the bundle metadata to get database name
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists || bundle.Database == nil {
			return true // continue to next bundle
		}
		databaseName := bundle.Database.Name

		pageCount := vm.PageCount()
		pagesSet := 0

		for pageID := uint32(0); pageID < pageCount; pageID++ {
			if vm.IsAllVisible(pageID) {
				continue // Already marked, skip
			}

			// Load page documents using read-only snapshot (no allocation)
			docs, err := s.SnapshotPageDocumentsReadOnly(bundleName, databaseName, pageID)
			if err != nil || len(docs) == 0 {
				continue
			}

			if CheckPageAllVisible(docs, oldestSnapshot) {
				vm.SetAllVisible(pageID)
				pagesSet++
			}
		}

		if pagesSet > 0 {
			s.logger.Debugf("VM refresher: set %d pages all-visible for bundle '%s'", pagesSet, bundleName)
		}
		return true
	})
}

func (s *BundleService) getPageShardIndex(pageKey string) int {
	return int(xxhash.Sum64String(pageKey) % PageCacheShardCount)
}

// updatePageCacheWithDocument updates the page cache with a document after a successful write.
// This is the core write-through mechanism: after WriteBuffer commits, we immediately
// update the in-memory page cache so subsequent reads see the new data.
//
// READER VIEW: Prefer "copy outside lock, swap under brief Lock" — load current reader
// view (no lock), build new snapshot with this doc (no lock), then Lock only to update
// authoritative and store the new reader view. If no reader view exists yet, fall back to
// building under Lock (e.g. new page or legacy entry).
//
// Thread Safety:
// - Uses sharded locks to minimize contention (64 shards)
// - Under Lock we only touch shard state; no storage or other locks (deadlock-safe).
//
// Parameters:
//   - bundleName: The bundle containing the document
//   - pageID: The page ID where the document resides
//   - doc: The document to add/update in the cache
func (s *BundleService) updatePageCacheWithDocument(bundleName string, pageID uint32, doc *models.Document) {
	// Clear visibility map bit for this page (page content is changing)
	s.clearVisibilityForPage(bundleName, pageID)

	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// Phase 2 fast path: build new snapshot from current reader view outside the lock.
	if v, ok := shard.readerView.Load(pageKey); ok {
		if oldSnapshot, ok := v.(*models.DocumentPage); ok {
			newSnapshot := &models.DocumentPage{
				PageID:    pageID,
				BundleID:  bundleName,
				Documents: make(map[string]models.Document, len(oldSnapshot.Documents)+1),
			}
			for docID, d := range oldSnapshot.Documents {
				newSnapshot.Documents[docID] = d
			}
			newSnapshot.Documents[doc.DocumentID] = *doc

			// Build COW outside lock — newSnapshot is already a private copy
			cowDocs := make([]models.Document, 0, len(newSnapshot.Documents))
			for _, d := range newSnapshot.Documents {
				if d.IsVisibleReadCommitted() {
					cowDocs = append(cowDocs, d)
				}
			}
			freshCOW := &cowSnapshotEntry{
				documents: cowDocs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}

			shard.mu.Lock()
			page, exists := shard.pages[pageKey]
			if exists {
				page.Documents[doc.DocumentID] = *doc
				shard.readerView.Store(pageKey, newSnapshot)
				shard.cowSnapshot.Store(pageKey, freshCOW)
			}
			shard.mu.Unlock()
			if exists {
				return
			}
			// Page was evicted between Load and Lock; fall through to Phase 1 path.
		}
	}

	// Phase 1 path: no reader view yet or page missing (create + set reader view under Lock).
	shard.mu.Lock()

	page, exists := shard.pages[pageKey]
	if !exists {
		page = &models.DocumentPage{
			PageID:    pageID,
			BundleID:  bundleName,
			Documents: make(map[string]models.Document),
		}
		shard.insertLocked(pageKey, page)
		elem := shard.lruOrder.PushFront(pageKey)
		shard.lruElements[pageKey] = elem
		if len(shard.pages) > shard.maxPages {
			shard.evictOldestLocked()
		}
	}

	page.Documents[doc.DocumentID] = *doc
	safeCopy := s.createSafePageCopy(page)
	shard.readerView.Store(pageKey, safeCopy)
	// Build COW under lock to prevent stale overwrites from concurrent writers
	cowDocs := make([]models.Document, 0, len(safeCopy.Documents))
	for _, d := range safeCopy.Documents {
		if d.IsVisibleReadCommitted() {
			cowDocs = append(cowDocs, d)
		}
	}
	shard.cowSnapshot.Store(pageKey, &cowSnapshotEntry{
		documents: cowDocs,
		timestamp: time.Now().UnixMilli(),
		pageKey:   pageKey,
	})
	shard.mu.Unlock()
}

// removeFromPageCache removes a document from the page cache after a successful delete.
// Called after a tombstone is written to the WriteBuffer.
//
// Prefer copy-outside-lock: load current reader view, build new snapshot without docID,
// then Lock only to update authoritative and store new reader view.
//
// Thread Safety:
// - Uses sharded locks to minimize contention
// - Safe to call even if document/page not in cache
//
// Parameters:
//   - bundleName: The bundle containing the document
//   - pageID: The page ID where the document resided
//   - docID: The document ID to remove
func (s *BundleService) removeFromPageCache(bundleName string, pageID uint32, docID string) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// Copy outside lock: build new snapshot without docID from current reader view.
	if v, ok := shard.readerView.Load(pageKey); ok {
		if oldSnapshot, ok := v.(*models.DocumentPage); ok {
			newSnapshot := &models.DocumentPage{
				PageID:    pageID,
				BundleID:  bundleName,
				Documents: make(map[string]models.Document, len(oldSnapshot.Documents)),
			}
			for id, d := range oldSnapshot.Documents {
				if id != docID {
					newSnapshot.Documents[id] = d
				}
			}

			// Build COW outside lock — newSnapshot is already a private copy
			cowDocs := make([]models.Document, 0, len(newSnapshot.Documents))
			for _, d := range newSnapshot.Documents {
				if d.IsVisibleReadCommitted() {
					cowDocs = append(cowDocs, d)
				}
			}
			freshCOW := &cowSnapshotEntry{
				documents: cowDocs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}

			shard.mu.Lock()
			page, exists := shard.pages[pageKey]
			if exists {
				delete(page.Documents, docID)
				shard.readerView.Store(pageKey, newSnapshot)
				shard.cowSnapshot.Store(pageKey, freshCOW)
			}
			shard.mu.Unlock()
			if exists {
				return
			}
		}
	}

	// Fallback: no reader view or page missing; update under Lock.
	shard.mu.Lock()
	page, exists := shard.pages[pageKey]
	if !exists {
		shard.mu.Unlock()
		return
	}
	delete(page.Documents, docID)
	safeCopy := s.createSafePageCopy(page)
	shard.readerView.Store(pageKey, safeCopy)
	// Build COW under lock to prevent stale overwrites from concurrent writers
	cowDocs := make([]models.Document, 0, len(safeCopy.Documents))
	for _, d := range safeCopy.Documents {
		if d.IsVisibleReadCommitted() {
			cowDocs = append(cowDocs, d)
		}
	}
	shard.cowSnapshot.Store(pageKey, &cowSnapshotEntry{
		documents: cowDocs,
		timestamp: time.Now().UnixMilli(),
		pageKey:   pageKey,
	})
	shard.mu.Unlock()
}

// getOrCreateSchemaManager retrieves or creates a GraphQL schema manager for the specified database.
// Schema managers are created lazily on first use because they require database-specific directory paths.
// This method is thread-safe using the sharded schema manager map.
//
// PHASE 5 INTEGRATION: This method is called automatically by AddBundle and UpdateBundle operations
// when GraphQL support is enabled. It initializes the schema file in the database's data directory.
// PHASE 5: Refactored to use ShardedSchemaManagerMap for concurrent access without global lock.
//
// Parameters:
//   - db: The database model containing name, ID, and directory path information
//
// Returns:
//   - *graphQLSchema.SchemaManager: The schema manager for this database (may be nil if disabled)
//   - error: Any initialization errors (logged but operation continues)
func (s *BundleService) getOrCreateSchemaManager(db *models.Database) (*graphQLSchema.SchemaManager, error) {
	// Return nil immediately if GraphQL is disabled
	if !s.graphQLEnabled {
		return nil, nil
	}

	// PHASE 5: Use sharded map's GetOrCreate with creator callback
	return s.schemaManagers.GetOrCreate(db.Name, func() (*graphQLSchema.SchemaManager, error) {
		// Create schema file path in database directory
		// Schema files follow the pattern: {database_directory}/{database_name}_graphql.gql
		// This ensures schemas are stored alongside their respective database bundles
		schemaFilePath := filepath.Join(s.settings.DataDir, db.Name, db.Name+"_graphql.gql")

		// Initialize the schema manager with database context
		// The manager handles schema versioning, tombstoning, and caching
		manager, err := graphQLSchema.NewSchemaManager(schemaFilePath, db.Name, db.DatabaseID)
		if err != nil {
			s.logger.Warnf("Failed to initialize GraphQL schema manager for database '%s': %v. Schema generation disabled for this database.", db.Name, err)
			return nil, err
		}

		s.logger.Debugf("GraphQL schema manager initialized for database '%s' at: %s", db.Name, schemaFilePath)
		return manager, nil
	})
}

// regenerateGraphQLSchema regenerates the GraphQL schema for a bundle after structure changes.
// This method implements FR-6: automatic schema regeneration on bundle modifications.
//
// It handles the complete schema update lifecycle:
// 1. Generates new schema from current bundle structure
// 2. Retrieves old schema for comparison (if exists)
// 3. Detects breaking changes (field removals, type changes, nullability changes)
// 4. Creates new schema version with breaking change annotations
// 5. Tombstones old schema version
// 6. Updates schema cache for immediate availability
//
// Breaking changes are detected and logged but don't fail the operation.
// This ensures bundle modifications succeed even if clients may need schema updates.
//
// Design Principles:
// - Single Responsibility: Handles only schema regeneration, delegates storage to SchemaManager
// - DRY: Reuses existing getOrCreateSchemaManager, GenerateSchema, DetectBreakingChanges
// - Open/Closed: Extensible through SchemaGenerator type mapping
//
// Returns error only if schema generation or storage fails critically.
// Warnings are logged for breaking changes and non-critical failures.
func (s *BundleService) regenerateGraphQLSchema(bundle *models.Bundle) error {
	// Early exit if GraphQL is disabled or bundle has no database context
	if !s.graphQLEnabled || s.schemaGenerator == nil {
		return nil
	}
	if bundle == nil || bundle.Database == nil {
		return fmt.Errorf("bundle or database is nil")
	}

	s.logger.Debugf("[GraphQL] Regenerating schema for bundle '%s' in database '%s'",
		bundle.Name, bundle.Database.Name)

	// Get or create the schema manager for this database (reuses existing infrastructure)
	schemaManager, err := s.getOrCreateSchemaManager(bundle.Database)
	if err != nil {
		return fmt.Errorf("failed to get schema manager: %w", err)
	}
	if schemaManager == nil {
		// GraphQL disabled for this database
		return nil
	}

	// Retrieve current active schema for breaking change detection
	// This may be nil if this is the first schema or if it was tombstoned
	oldSchema, err := schemaManager.GetActiveSchemaForBundle(bundle.Name)
	if err != nil {
		s.logger.Warnf("[GraphQL] Failed to retrieve existing schema for bundle '%s': %v", bundle.Name, err)
		// Continue with schema generation even if we can't get old schema
	}

	// Generate new schema from the current bundle structure (reuses existing generator)
	// This converts SyndrDB field definitions → GraphQL types
	newSchemaDef, err := s.schemaGenerator.GenerateSchema(bundle)
	if err != nil {
		return fmt.Errorf("failed to generate schema: %w", err)
	}

	// Detect breaking changes by comparing old schema with new schema
	// Breaking changes: field removals, type changes, nullable → non-nullable
	var breakingChanges []graphQLSchema.BreakingChange
	if oldSchema != nil && oldSchema.Payload != nil {
		breakingChanges = s.schemaGenerator.DetectBreakingChanges(oldSchema.Payload, newSchemaDef)

		// Log breaking changes for visibility (critical for API consumers)
		if len(breakingChanges) > 0 {
			s.logger.Warnf("[GraphQL] Breaking changes detected in bundle '%s': %d change(s)",
				bundle.Name, len(breakingChanges))
			for _, change := range breakingChanges {
				s.logger.Warnf("[GraphQL]   - %s: Field '%s' %s → %s (Severity: %s)",
					change.ChangeType, change.FieldName, change.OldValue, change.NewValue, change.Severity)
			}
		} else {
			s.logger.Debugf("[GraphQL] No breaking changes detected (backward compatible update)")
		}
	}

	// Attach breaking changes to schema definition for storage and future reference
	newSchemaDef.BreakingChanges = breakingChanges

	// Get schema version for update operation
	var schemaIDBytes [16]byte
	if oldSchema != nil {
		// Updating existing schema - use same ID to link versions
		schemaIDBytes = oldSchema.SchemaID
	} else {
		// First schema for this bundle - generate new ID
		copy(schemaIDBytes[:], []byte(helpers.GenerateFastUUID()))
	}

	var bundleIDBytes [16]byte
	copy(bundleIDBytes[:], []byte(bundle.BundleID))

	// Update schema: creates new version, tombstones old, updates cache
	// This writes to the schema file with versioning and tombstone markers
	err = schemaManager.UpdateSchema(schemaIDBytes, bundleIDBytes, bundle.Name, newSchemaDef)
	if err != nil {
		return fmt.Errorf("failed to update schema: %w", err)
	}

	// Log success with version information
	newVersion, _ := schemaManager.GetLatestVersionForBundle(bundle.Name)
	s.logger.Debugf("[GraphQL] Schema updated for bundle '%s' (version %d, %d fields, %d breaking changes)",
		bundle.Name, newVersion, len(newSchemaDef.Fields), len(breakingChanges))

	return nil
}

// getBundleLock retrieves or creates an operation lock for the specified bundle.
// This method is thread-safe and uses the sharded lock map for concurrent access.
// PHASE 5: Simplified - ShardedBundleOperationLockMap handles all the locking internally.
func (s *BundleService) getBundleLock(bundleName string) *BundleOperationLock {
	return s.bundleLocks.Get(bundleName)
}

// AcquireBundleReadLock acquires a read lock for the specified bundle.
// Multiple concurrent readers are allowed. Returns an error if a rename
// operation is in progress or if the bundle doesn't exist.
//
// IMPORTANT: Every call to this method must be paired with ReleaseBundle ReadLock()
// using defer to ensure proper cleanup even in error cases.
//
// Example usage:
//
//	if err := service.AcquireBundleReadLock(bundle.Name); err != nil {
//	    return err
//	}
//	defer service.ReleaseBundleReadLock(bundle.Name)
func (s *BundleService) AcquireBundleReadLock(bundleName string) error {
	lock := s.getBundleLock(bundleName)
	return lock.AcquireReadLock()
}

// ReleaseBundleReadLock releases a previously acquired read lock.
// This should always be called via defer after AcquireBundleReadLock.
func (s *BundleService) ReleaseBundleReadLock(bundleName string) {
	lock := s.getBundleLock(bundleName)
	lock.ReleaseReadLock()
}

// AcquireBundleWriteLock acquires a write lock for the specified bundle.
// Only one writer is allowed at a time. Returns an error if a rename
// operation is in progress.
//
// IMPORTANT: Every call to this method must be paired with ReleaseBundleWriteLock()
// using defer to ensure proper cleanup even in error cases.
//
// Example usage:
//
//	if err := service.AcquireBundleWriteLock(bundle.Name); err != nil {
//	    return err
//	}
//	defer service.ReleaseBundleWriteLock(bundle.Name)
func (s *BundleService) AcquireBundleWriteLock(bundleName string) error {
	lock := s.getBundleLock(bundleName)
	return lock.AcquireWriteLock()
}

// ReleaseBundleWriteLock releases a previously acquired write lock.
// This should always be called via defer after AcquireBundleWriteLock.
func (s *BundleService) ReleaseBundleWriteLock(bundleName string) {
	lock := s.getBundleLock(bundleName)
	lock.ReleaseWriteLock()
}

// GetBundleOperationStats returns the current number of active readers and writers
// for a bundle. Useful for monitoring and debugging.
func (s *BundleService) GetBundleOperationStats(bundleName string) (readers int64, writers int64, renameInProgress bool) {
	lock := s.getBundleLock(bundleName)
	readers, writers = lock.GetActiveOperationCounts()
	renameInProgress = lock.IsRenameInProgress()
	return
}

// IsFieldForeignKey checks if a field is a foreign key based on relationships from other bundles
// Returns true if the field is used as a foreign key in any relationship
// This function checks both:
// 1. If this bundle has relationships where this field is a source field (outgoing FK)
// 2. If other bundles have relationships where this field is a destination field (incoming FK)
func IsFieldForeignKey(bundle *models.Bundle, fieldName string) (bool, string, string) {
	// Check if this field is used as a source field in any relationship within this bundle
	// This means it references another bundle (making it an outgoing foreign key)
	if bundle.Relationships != nil {
		for _, relationship := range bundle.Relationships {
			if relationship.SourceField == fieldName {
				return true, relationship.DestinationBundle, relationship.DestinationField
			}
		}
	}

	// Check if this field is referenced as a destination field by other bundles
	// This means other bundles reference this field (making it an incoming foreign key)
	if bundle.Database != nil {
		for _, otherBundle := range bundle.Database.Bundles {
			if otherBundle.Relationships != nil {
				for _, relationship := range otherBundle.Relationships {
					// Check if this relationship points to our bundle and field
					if relationship.DestinationBundle == bundle.Name && relationship.DestinationField == fieldName {
						return true, relationship.SourceBundle, relationship.SourceField
					}
				}
			}
		}
	}

	return false, "", ""
}

// scheduleIndexUpdate adds an index update to the deferred update buffer
// This optimizes write performance by batching index updates
// Parameters:
//   - pageID: Physical page number where the document resides (use 0 if unknown, will need update later)
//   - docMetadata: optional [commitSeq, versionSeq]; when len>=2, hash index uses these instead of GetDocument
func (s *BundleService) scheduleIndexUpdate(bundleName, indexName, indexType, operation, documentID string, fieldValue interface{}, pageID uint32, oldValue interface{}, deferred bool, docMetadata ...uint64) {
	// DIAGNOSTICS: Record write activity for idle-based diagnostics logging
	s.RecordWriteActivity()

	update := IndexUpdate{
		BundleName:  bundleName,
		IndexName:   indexName,
		IndexType:   indexType,
		Operation:   operation,
		DocumentID:  documentID,
		FieldValue:  fieldValue,
		PageID:      pageID,
		OldValue:    oldValue,
		Timestamp:   time.Now(),
		AppliedSync: !deferred, // Mark as synchronously applied if not deferred
	}

	// CRITICAL FIX: For hash indexes, update MemTable IMMEDIATELY for read-your-own-writes consistency
	// This ensures LSM semantics where reads always see recent writes via MemTable
	if indexType == "hash" {
		// Get the bundle to access the index
		bundle, exists := s.bundleMetadata[bundleName]
		if exists {
			indexRef, indexExists := bundle.Indexes[indexName]
			if indexExists {
				// Load or get the hash index
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err == nil {
					// Update MemTable synchronously (in-memory operation, very fast)
					// Use fieldValueToIndexKeyString so document FieldValue unwraps to same string as query lookups
					keyValue := fieldValueToIndexKeyString(fieldValue)
					if keyValue == "" || keyValue == "<nil>" {
						keyValue = documentID // Fallback for DocumentID indexes
					}

					// Single-write path: assign sequence once; same entry goes to MemTable and disk
					currentSequence := atomic.LoadUint64(&hashIndex.GlobalSequence)
					if err := constants.CheckUint64Increment(currentSequence, "GlobalSequence"); err != nil {
						s.logger.Warnw("GlobalSequence overflow, skipping hash index update",
							zap.String("bundle", bundleName),
							zap.String("index", indexName),
							zap.Error(err))
					} else {
						sequence := atomic.AddUint64(&hashIndex.GlobalSequence, 1)

						// PHASE 4: MVCC - Get document's version metadata
						var commitSeq, versionSeq uint64
						if len(docMetadata) >= 2 {
							commitSeq, versionSeq = docMetadata[0], docMetadata[1]
						} else if doc, err := s.GetDocument(bundleName, bundle.Database.Name, documentID); err == nil {
							commitSeq = doc.CommitSequence
							versionSeq = doc.VersionSequence
						}

						entry := hashindex.NewHashIndexEntry(keyValue, documentID, pageID, sequence, commitSeq, versionSeq)
						if operation == "delete" {
							entry.Deleted = true
						}
						// Set BucketNum so WriteEntryToDiskOnly (processHashIndexBatch) routes to the correct
						// bucket file; otherwise entry.BucketNum stays 0 and all entries land in bucket 0,
						// causing lookups for other buckets to miss (index appears empty for those keys).
						numBkts := hashIndex.NumBuckets()
						bucketNum, bucketErr := hashindex.ComputeBucketNum(entry.HashValue, numBkts)
						if bucketErr == nil {
							entry.BucketNum = bucketNum
						}

						err = hashIndex.MemTable.Put(entry)
						if err != nil {
							s.logger.Warnw("Failed to update MemTable immediately",
								zap.String("bundle", bundleName),
								zap.String("index", indexName),
								zap.Error(err))
						} else {
							// DIAG: Log scheduled index update with bucket assignment
							s.logger.Warnw("[BUCKET-DIAG] scheduleIndexUpdate: entry queued",
								"key", keyValue,
								"docID", documentID,
								"bucketNum", entry.BucketNum,
								"hashValue", entry.HashValue,
								"index", indexName,
								"operation", operation)
							update.HashEntry = entry // processHashIndexBatch will write this to disk only
							if operation == "insert" {
								s.logger.Debugw("Immediately updated MemTable for key",
									zap.String("key", keyValue),
									zap.String("index", indexName))
							} else {
								s.logger.Debugw("Immediately updated MemTable with tombstone",
									zap.String("key", keyValue),
									zap.String("index", indexName))
							}
						}

						if trimmed, oldSize := hashIndex.TrimMemTableWAL(10000); trimmed {
							s.logger.Debugf("Aggressive MemTable trim: cleared %d WAL entries for %s.%s",
								oldSize, bundleName, indexName)
						}
					}
				} else {
					s.logger.Warnw("Failed to load hash index for immediate MemTable update",
						zap.String("bundle", bundleName),
						zap.String("index", indexName),
						zap.Error(err))
				}
			}
		}
	}

	// CRITICAL FIX: For B-tree indexes, update in-memory cache IMMEDIATELY for read-your-own-writes consistency
	// This ensures PostgreSQL-style semantics where reads always see recent writes via page cache
	// PERFORMANCE: Skip synchronous updates when deferred=true (batch UPDATE operations)
	// Deferred operations will be applied in processBTreeIndexBatch without duplicate checking
	if indexType == "btree" && !deferred {
		// Get the bundle to access the index
		bundle, exists := s.bundleMetadata[bundleName]
		if exists {
			indexRef, indexExists := bundle.Indexes[indexName]
			if indexExists {
				// Load or get the B-tree index
				btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err == nil {
					// Convert field value to bytes for B-tree key
					keyBytes, err := convertValueToBytes(fieldValue)
					if err != nil {
						s.logger.Warnw("Failed to convert field value to bytes for B-tree",
							zap.String("bundle", bundleName),
							zap.String("index", indexName),
							zap.Error(err))
					} else {
						// Measure insert time (PostgreSQL baseline + 15% margin = 500μs target)
						insertStart := time.Now()

						// Attempt insert with retry logic (fixed 1ms backoff)
						var insertErr error
						switch operation {
						case "insert":
							insertErr = btreeIndex.Insert(keyBytes, documentID)
							if insertErr != nil {
								// Retry once after 1ms
								time.Sleep(1 * time.Millisecond)
								insertErr = btreeIndex.Insert(keyBytes, documentID)
								if insertErr != nil {
									s.logger.Warnw("Failed to insert into B-tree after retry",
										zap.String("bundle", bundleName),
										zap.String("index", indexName),
										zap.String("documentID", documentID),
										zap.Error(insertErr))
								}
							}
						case "delete":
							insertErr = btreeIndex.Delete(keyBytes, documentID)
							if insertErr != nil {
								// Retry once after 1ms
								time.Sleep(1 * time.Millisecond)
								insertErr = btreeIndex.Delete(keyBytes, documentID)
								if insertErr != nil {
									s.logger.Warnw("Failed to delete from B-tree after retry",
										zap.String("bundle", bundleName),
										zap.String("index", indexName),
										zap.String("documentID", documentID),
										zap.Error(insertErr))
								}
							}
						}

						insertDuration := time.Since(insertStart)

						// Log performance warning if insert exceeds PostgreSQL baseline + 15% (500μs)
						if insertErr == nil && insertDuration > 500*time.Microsecond {
							s.logger.Warnw("⚠️  B-tree synchronous insert exceeded performance target",
								zap.String("index", indexName),
								zap.String("operation", operation),
								zap.Duration("duration", insertDuration),
								zap.Duration("target", 500*time.Microsecond))
						} else if insertErr == nil {
							s.logger.Debugw("⚡ B-tree synchronous insert completed",
								zap.String("index", indexName),
								zap.String("operation", operation),
								zap.Duration("duration", insertDuration))
						}
					}
				} else {
					s.logger.Warnw("Failed to load B-tree index for immediate insert",
						zap.String("bundle", bundleName),
						zap.String("index", indexName),
						zap.Error(err))
				}
			}
		}
	}

	// Schedule disk persistence (deferred for performance)
	// FIX: Protect indexUpdateBuffer with mutex to prevent data race
	s.indexUpdateMutex.Lock()
	s.indexUpdateBuffer = append(s.indexUpdateBuffer, update)
	bufferLen := len(s.indexUpdateBuffer)
	lastFlush := s.lastIndexFlush
	s.indexUpdateMutex.Unlock()

	// Check if we should flush updates to disk
	shouldFlush := bufferLen >= s.indexUpdateBatchSize ||
		time.Since(lastFlush) >= s.indexUpdateInterval
	if shouldFlush {
		// flushStart := time.Now()
		s.flushIndexUpdates()
	}

	// PHASE 1 ENHANCEMENT: Additional flush check for idle periods on index updates
	s.indexUpdateMutex.Lock()
	idleFlushNeeded := len(s.indexUpdateBuffer) > 0 && time.Since(s.lastIndexFlush) >= (s.indexUpdateInterval*5)
	s.indexUpdateMutex.Unlock()
	if idleFlushNeeded {
		s.logger.Debugf("IDLE FLUSH: Flushing %d index updates after extended idle period", bufferLen)
		s.flushIndexUpdates()
	}
}

// scheduleMetadataUpdate adds a metadata update to the deferred update buffer
// This optimizes write performance by batching metadata calculations
// Thread-safe: Protected by metadataUpdateMutex
func (s *BundleService) scheduleMetadataUpdate(bundleName, operation string, value int64) {
	s.metadataUpdateMutex.Lock()
	defer s.metadataUpdateMutex.Unlock()

	update := MetadataUpdate{
		BundleName: bundleName,
		Operation:  operation,
		Value:      value,
		Timestamp:  time.Now(),
	}

	s.metadataUpdateBuffer = append(s.metadataUpdateBuffer, update)

	// PHASE 1 OPTIMIZATION: Track operations for deferred persistence
	s.metadataOperationCount++

	// Check if we should flush metadata updates
	// Release lock before flushing to prevent deadlock (flush will acquire its own lock)
	shouldFlush := len(s.metadataUpdateBuffer) >= s.indexUpdateBatchSize ||
		time.Since(s.lastMetadataFlush) >= s.indexUpdateInterval

	shouldIdleFlush := len(s.metadataUpdateBuffer) > 0 &&
		time.Since(s.lastMetadataFlush) >= (s.indexUpdateInterval*5)

	if shouldFlush || shouldIdleFlush {
		// Must unlock before calling flush to avoid deadlock
		s.metadataUpdateMutex.Unlock()
		s.FlushMetadataUpdates()
		s.metadataUpdateMutex.Lock() // Re-acquire for defer
	}
}

// flushIndexUpdates processes all pending index updates in a batch
// This significantly improves write performance by reducing I/O operations
// FIX: Now protected by indexUpdateMutex to prevent data race
func (s *BundleService) flushIndexUpdates() {
	s.indexUpdateMutex.Lock()
	if len(s.indexUpdateBuffer) == 0 {
		s.indexUpdateMutex.Unlock()
		return
	}

	startTime := time.Now()
	s.logger.Debugw("Flushing pending index updates",
		zap.Int("count", len(s.indexUpdateBuffer)))

	// Group updates by bundle and index for efficient processing
	updateGroups := make(map[string]map[string][]IndexUpdate)

	for _, update := range s.indexUpdateBuffer {
		if updateGroups[update.BundleName] == nil {
			updateGroups[update.BundleName] = make(map[string][]IndexUpdate)
		}
		updateGroups[update.BundleName][update.IndexName] = append(
			updateGroups[update.BundleName][update.IndexName], update)
	}

	// Clear the buffer and update flush time BEFORE processing (release lock faster)
	s.indexUpdateBuffer = s.indexUpdateBuffer[:0] // Reset slice but keep capacity
	s.lastIndexFlush = time.Now()
	s.indexUpdateMutex.Unlock()

	// Process updates in batches (outside lock to avoid blocking other operations)
	for bundleName, indexGroups := range updateGroups {
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists {
			s.logger.Warnf("Bundle '%s' not found in metadata during index update flush", bundleName)
			continue
		}

		for indexName, updates := range indexGroups {
			indexRef, exists := bundle.Indexes[indexName]
			if !exists {
				s.logger.Warnf("Index '%s' not found in bundle '%s' during flush", indexName, bundleName)
				continue
			}

			// Process updates for this specific index
			err := s.processIndexUpdateBatch(bundle, indexName, indexRef, updates)
			if err != nil {
				s.logger.Errorf("Failed to process index update batch for %s.%s: %v", bundleName, indexName, err)
			}
		}
	}

	flushTime := time.Since(startTime)
	s.logger.Debugw("Index update flush completed",
		zap.Duration("duration", flushTime))
}

// flushIndexUpdatesForBundle flushes only index updates for the given bundle (P4a scoped flush).
// Reduces tail latency by avoiding processing the entire global buffer when only this bundle's
// indexes were touched by the current UPDATE.
// FIX: Now protected by indexUpdateMutex to prevent data race
func (s *BundleService) flushIndexUpdatesForBundle(bundleName string) {
	s.indexUpdateMutex.Lock()
	var match, keep []IndexUpdate
	for _, u := range s.indexUpdateBuffer {
		if u.BundleName == bundleName {
			match = append(match, u)
		} else {
			keep = append(keep, u)
		}
	}
	if len(match) == 0 {
		s.indexUpdateMutex.Unlock()
		return
	}

	// Update buffer and flush time BEFORE processing (release lock faster)
	s.indexUpdateBuffer = keep
	s.lastIndexFlush = time.Now()
	s.indexUpdateMutex.Unlock()

	startTime := time.Now()
	s.logger.Debugw("Flushing scoped index updates for bundle",
		zap.String("bundle", bundleName),
		zap.Int("count", len(match)))

	updateGroups := make(map[string][]IndexUpdate)
	for _, u := range match {
		updateGroups[u.IndexName] = append(updateGroups[u.IndexName], u)
	}
	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		s.logger.Warnf("Bundle '%s' not found during scoped index flush", bundleName)
		return
	}
	for indexName, updates := range updateGroups {
		indexRef, ok := bundle.Indexes[indexName]
		if !ok {
			s.logger.Warnf("Index '%s' not found in bundle '%s' during scoped flush", indexName, bundleName)
			continue
		}
		if err := s.processIndexUpdateBatch(bundle, indexName, indexRef, updates); err != nil {
			s.logger.Errorf("Failed to process index update batch for %s.%s: %v", bundleName, indexName, err)
		}
	}
	s.logger.Debugw("Scoped index flush completed",
		zap.String("bundle", bundleName),
		zap.Duration("duration", time.Since(startTime)))
}

// FlushMetadataUpdates processes all pending metadata updates in a batch
// This significantly improves write performance by reducing metadata calculation overhead
// Thread-safe: Protected by metadataUpdateMutex
//
// DUAL PERSISTENCE STRATEGY:
//  1. ALWAYS apply updates to in-memory bundle metadata (consistency)
//  2. Mark affected bundles as dirty (IsDirty = true)
//  3. Persist to disk when EITHER condition is met:
//     a) Bundle is dirty AND flush triggered by time/size thresholds (efficiency)
//     b) Global operation counter >= metadataPersistInterval (safety net)
//
// This approach provides:
// - Single-bundle heavy writes: Immediate persistence after each flush
// - Multi-bundle operations: Batched persistence for performance
// - Safety guarantee: Operation counter ensures eventual persistence
func (s *BundleService) FlushMetadataUpdates() {
	s.metadataUpdateMutex.Lock()
	if len(s.metadataUpdateBuffer) == 0 {
		s.metadataUpdateMutex.Unlock()
		return
	}

	startTime := time.Now()
	bufferSize := len(s.metadataUpdateBuffer)
	s.logger.Debugf("Flushing %d pending metadata updates", bufferSize)

	// Group updates by bundle for efficient processing
	bundleUpdates := make(map[string][]MetadataUpdate)
	for _, update := range s.metadataUpdateBuffer {
		bundleUpdates[update.BundleName] = append(bundleUpdates[update.BundleName], update)
	}

	// Clear buffer and capture state before releasing lock
	s.metadataUpdateBuffer = s.metadataUpdateBuffer[:0]
	s.lastMetadataFlush = time.Now()

	// Release lock before expensive I/O operations
	s.metadataUpdateMutex.Unlock()

	// Process updates for each bundle
	for bundleName, updates := range bundleUpdates {
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists {
			s.logger.Warnf("Bundle '%s' not found in metadata during metadata update flush", bundleName)
			continue
		}

		// Apply all updates for this bundle
		docCountDelta := int64(0)
		for _, update := range updates {
			switch update.Operation {
			case "increment_docs":
				docCountDelta += update.Value
			case "decrement_docs":
				// CRITICAL FIX: Ignore decrement_docs operations to prevent corruption
				// In append-only storage, tombstones are still entries on disk, so TotalDocuments
				// should represent total document entries (including tombstones), not active documents.
				// Active document count is calculated dynamically by filtering tombstones during queries.
				// Decrementing TotalDocuments causes corruption when documents exist that were never counted.
				// docCountDelta -= update.Value // REMOVED: Causes corruption
				// Silently ignore - no logging needed for performance
			}
		}

		// Apply the accumulated changes
		bundle.TotalDocuments += docCountDelta

		// Mark bundle as dirty - needs persistence
		// This flag is cleared only after successful disk write
		bundle.IsDirty = true

		// Recalculate page count if documents changed
		if docCountDelta != 0 {
			// Ensure PageSize is never zero to prevent divide by zero
			// Use consistent PageSize with BundleService and factory defaults
			if bundle.PageSize == 0 {
				bundle.PageSize = s.defaultPageSize // Use service default (4096)
				s.logger.Debugf("Set default PageSize of %d for bundle '%s'", s.defaultPageSize, bundleName)
			}

			// CRITICAL: Proper virtual pagination calculation
			// PageCount = ceil(TotalDocuments / PageSize)
			newPageCount := (bundle.TotalDocuments + int64(bundle.PageSize) - 1) / int64(bundle.PageSize)
			if newPageCount != bundle.PageCount {
				s.logger.Debugf("Updated PageCount for bundle '%s': %d -> %d (TotalDocuments: %d, PageSize: %d)",
					bundleName, bundle.PageCount, newPageCount, bundle.TotalDocuments, bundle.PageSize)
				bundle.PageCount = newPageCount
			}
		}
	}

	// DUAL PERSISTENCE TRIGGERS:
	// Trigger 1: Dirty bundles on flush (efficiency - immediate persistence for active bundles)
	// Trigger 2: Operation counter threshold (safety - eventual persistence for all)
	s.metadataUpdateMutex.Lock()
	shouldPersistToDisk := s.metadataOperationCount >= s.metadataPersistInterval
	currentOperationCount := s.metadataOperationCount
	s.metadataUpdateMutex.Unlock()

	// Collect dirty bundles that need persistence
	var bundlesToPersist []*models.Bundle
	for bundleName := range bundleUpdates {
		bundle, exists := s.bundleMetadata[bundleName]
		if exists && bundle.IsDirty {
			bundlesToPersist = append(bundlesToPersist, bundle)
		}
	}

	s.logger.Debugf("METADATA FLUSH: operationCount=%d, threshold=%d, shouldPersist=%v, dirtyBundles=%d, bufferSize=%d",
		currentOperationCount, s.metadataPersistInterval, shouldPersistToDisk, len(bundlesToPersist), bufferSize)

	// Persist if EITHER dirty bundles exist OR threshold reached
	if len(bundlesToPersist) > 0 || shouldPersistToDisk {
		if shouldPersistToDisk {
			// Threshold reached - collect ALL dirty bundles across entire service
			bundlesToPersist = s.getAllDirtyBundles()
		}

		// Persist all dirty bundles
		successCount := 0
		for _, bundle := range bundlesToPersist {

			err := s.store.UpdateBundleFile(bundle.Database, bundle)
			if err != nil {
				s.logger.Errorf("Failed to persist metadata updates for bundle '%s': %v", bundle.Name, err)
				// TODO: Implement retry queue for failed persistence operations
				// Keep IsDirty = true on failure so next cycle will retry
			} else {
				// Clear dirty flag only on successful persistence
				bundle.IsDirty = false
				successCount++

			}
		}

		// Reset operation counter after persistence
		if shouldPersistToDisk {
			s.metadataUpdateMutex.Lock()
			s.metadataOperationCount = 0
			s.metadataUpdateMutex.Unlock()
			s.logger.Debugf("Performed deferred metadata persistence after %d operations", s.metadataPersistInterval)
		}
	} else {
		s.logger.Debugf("Skipping disk persistence - %d operations remaining until next persist (threshold: %d)",
			s.metadataPersistInterval-currentOperationCount, s.metadataPersistInterval)
	}

	flushTime := time.Since(startTime)
	s.logger.Debugf("Metadata update flush completed in %v", flushTime)
}

// ForceMetadataPersistence forces immediate persistence of all metadata updates to disk
// This should be called during shutdown, explicit flush requests, or before critical operations
// Thread-safe: Uses metadataUpdateMutex for operation count reset
func (s *BundleService) ForceMetadataPersistence() {
	// First flush any pending updates to memory
	s.FlushMetadataUpdates()

	// Now persist ALL dirty bundles regardless of operation count
	// This ensures all metadata is saved during shutdown
	s.logger.Info("Forcing metadata persistence for shutdown")

	// Get all dirty bundles across entire service
	dirtyBundles := s.getAllDirtyBundles()
	if len(dirtyBundles) == 0 {
		s.logger.Info("No dirty bundles to persist")
		return
	}

	s.logger.Infow("Persisting dirty bundles on shutdown",
		"bundleCount", len(dirtyBundles))

	successCount := 0
	for _, bundle := range dirtyBundles {

		err := s.store.UpdateBundleFile(bundle.Database, bundle)
		if err != nil {
			s.logger.Errorf("Failed to force persist bundle metadata: %v", err)
		} else {
			// Clear dirty flag only on success
			bundle.IsDirty = false
			successCount++
		}
	}

	s.logger.Infow("Shutdown metadata persistence complete",
		"attempted", len(dirtyBundles),
		"succeeded", successCount,
		"failed", len(dirtyBundles)-successCount)

	// Reset operation counter
	s.metadataUpdateMutex.Lock()
	s.metadataOperationCount = 0
	s.metadataUpdateMutex.Unlock()
}

// getAllDirtyBundles returns all bundles with IsDirty = true across all databases
// Thread-safe: Only reads bundle metadata, no lock needed (bundles accessed through factory)
// TODO: Consider adding a dirty bundle tracking map for O(1) access instead of O(n) scan
func (s *BundleService) getAllDirtyBundles() []*models.Bundle {
	var dirtyBundles []*models.Bundle

	// Iterate through all bundles in metadata map
	for _, bundle := range s.bundleMetadata {
		if bundle.IsDirty {
			dirtyBundles = append(dirtyBundles, bundle)
		}
	}

	return dirtyBundles
}

// trackOperationForBulkDetection tracks write operations to detect bulk scenarios
// Returns true if WAL should be bypassed due to bulk mode detection
func (s *BundleService) trackOperationForBulkDetection() bool {
	// Get global settings for WAL bulk operation configuration
	globalSettings := settings.GetSettings()

	// Skip tracking if bulk operation detection is disabled
	if !globalSettings.BulkOperationDetection {
		return false
	}

	now := time.Now()
	s.operationCount++

	// Check if we're in a new time window (1 second)
	windowDuration := now.Sub(s.operationWindow)
	if windowDuration >= time.Second {
		// Calculate operations per second in the previous window
		opsPerSecond := float64(s.operationCount) / windowDuration.Seconds()

		// Check if we should enter or exit bulk mode
		if opsPerSecond >= float64(s.bulkThresholdOpsPerSec) {
			if !s.bulkModeEnabled {
				s.bulkModeEnabled = true
				s.logger.Debugf("Entering bulk mode - detected %.1f ops/sec (threshold: %d)",
					opsPerSecond, s.bulkThresholdOpsPerSec)
			}
		} else {
			if s.bulkModeEnabled {
				s.bulkModeEnabled = false
				s.logger.Debugf("Exiting bulk mode - detected %.1f ops/sec (threshold: %d)",
					opsPerSecond, s.bulkThresholdOpsPerSec)

				// CRITICAL: Flush all buffers when exiting bulk mode
				// This ensures that any pending operations are persisted to disk
				if err := s.FlushAllBuffers(); err != nil {
					s.logger.Errorf("BULK END: Failed to flush buffers: %v", err)
				} else {
					s.logger.Debugf("BULK END: Successfully flushed all pending operations")
				}
			}
		}

		// Reset counters for new window
		s.operationCount = 0
		s.operationWindow = now
	}

	// Return true if WAL should be disabled due to bulk mode
	return s.bulkModeEnabled && globalSettings.WALDisableForBulkOps
}

// ShouldBypassWAL returns true if WAL should be bypassed for the current operation
// This method should be called by external services before WAL operations
func (s *BundleService) ShouldBypassWAL() bool {
	return s.trackOperationForBulkDetection()
}

// GetBulkModeStatus returns the current bulk mode status for monitoring
func (s *BundleService) GetBulkModeStatus() (bool, int, float64) {
	globalSettings := settings.GetSettings()
	if !globalSettings.BulkOperationDetection {
		return false, 0, 0
	}

	// Calculate current operations per second
	windowDuration := time.Since(s.operationWindow)
	var opsPerSecond float64
	if windowDuration > 0 {
		opsPerSecond = float64(s.operationCount) / windowDuration.Seconds()
	}

	return s.bulkModeEnabled, s.operationCount, opsPerSecond
}

// FlushAllIndexesToDisk forces all loaded hash and BTree indexes to flush their memtables to disk
// This ensures durability even for indexes that don't have pending updates in the buffer
// CRITICAL for test reliability and data consistency after bulk operations
func (s *BundleService) FlushAllIndexesToDisk() error {
	var errors []error
	flushedCount := 0

	// Iterate through all bundles and flush their loaded indexes
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Indexes == nil {
			continue
		}

		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexInstance == nil {
				continue // Index not loaded in memory, skip
			}

			switch indexRef.IndexType {
			case "hash":
				// Flush hash index V3 (LSM-style)
				if hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3); ok {
					if err := hashIndex.Flush(); err != nil {
						// Skip closed indexes - they were already flushed when closed
						if strings.Contains(err.Error(), "index is closed") {
							s.logger.Debugf("Skipping flush for closed hash index '%s' in bundle '%s'", indexName, bundleName)
							continue
						}
						errorMsg := fmt.Sprintf("failed to flush hash index '%s' in bundle '%s': %v", indexName, bundleName, err)
						s.logger.Warnf(errorMsg)
						errors = append(errors, fmt.Errorf("%s", errorMsg))
					} else {
						flushedCount++
						s.logger.Debugf("Flushed hash index '%s' in bundle '%s' to disk", indexName, bundleName)
					}
				}

			case "btree":
				// Flush BTree index if it has a Flush method
				if btreeIndex, ok := indexRef.IndexInstance.(interface{ Flush() error }); ok {
					if err := btreeIndex.Flush(); err != nil {
						// Skip closed indexes - they were already flushed when closed
						if strings.Contains(err.Error(), "index is closed") {
							s.logger.Debugf("Skipping flush for closed BTree index '%s' in bundle '%s'", indexName, bundleName)
							continue
						}
						errorMsg := fmt.Sprintf("failed to flush BTree index '%s' in bundle '%s': %v", indexName, bundleName, err)
						s.logger.Warnf(errorMsg)
						errors = append(errors, fmt.Errorf("%s", errorMsg))
					} else {
						flushedCount++
						s.logger.Debugf("Flushed BTree index '%s' in bundle '%s' to disk", indexName, bundleName)
					}
				}
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to flush %d of %d indexes: %v", len(errors), flushedCount+len(errors), errors)
	}

	if flushedCount > 0 {
		s.logger.Debugf("Successfully flushed %d indexes to disk", flushedCount)
	}

	return nil
}

// FlushAllBuffers forces immediate flush of all pending operations to disk
// This should be called at the end of bulk operations to ensure data persistence
func (s *BundleService) FlushAllBuffers() error {

	var errors []error

	// 1. Flush index updates first (they may affect metadata)
	// FIX: Use mutex to safely check buffer length
	s.indexUpdateMutex.Lock()
	hasIndexUpdates := len(s.indexUpdateBuffer) > 0
	s.indexUpdateMutex.Unlock()
	if hasIndexUpdates {
		s.flushIndexUpdates()
	}

	// 2. CRITICAL: Flush all loaded indexes to ensure memtables are persisted
	// This is essential for test reliability and durability after document operations
	if err := s.FlushAllIndexesToDisk(); err != nil {
		s.logger.Warnf("Failed to flush all indexes to disk: %v", err)
		errors = append(errors, err)
	}

	// 3. Force metadata persistence regardless of thresholds
	s.metadataUpdateMutex.RLock()
	needsMetaFlush := len(s.metadataUpdateBuffer) > 0
	s.metadataUpdateMutex.RUnlock()
	if needsMetaFlush {
		s.ForceMetadataPersistence()
	}

	// 4. Sync any file system buffers
	// Note: Individual stores should handle their own sync operations
	if err := s.store.FlushAllWriteBuffers(); err != nil {
		errors = append(errors, err)
	}

	// 5. Log completion
	if len(errors) > 0 {
		s.logger.Errorf("FLUSH: Completed with %d errors", len(errors))
		return fmt.Errorf("flush completed with %d errors", len(errors))
	}

	return nil
}

// IsDocumentBuffered checks if a document is currently in the write buffer
func (s *BundleService) IsDocumentBuffered(bundleName string, docID string) bool {
	return s.store.IsDocumentBuffered(bundleName, docID)
}

// MarkDocumentDiscarded marks a document as discarded (for rollback)
func (s *BundleService) MarkDocumentDiscarded(bundleName string, docID string) error {
	return s.store.MarkDocumentDiscarded(bundleName, docID)
}

// GetDiscardedDocuments returns list of document IDs marked as discarded in a bundle
func (s *BundleService) GetDiscardedDocuments(bundleName string) []string {
	return s.store.GetDiscardedDocuments(bundleName)
}

// ClearDiscardedDocuments removes document IDs from the discarded set after successful deletion
func (s *BundleService) ClearDiscardedDocuments(bundleName string, docIDs []string) {
	s.store.ClearDiscardedDocuments(bundleName, docIDs)
}

// DeleteDiscardedDocuments physically deletes documents that were marked as discarded
// This is called after FlushAllBuffers during rollback cleanup
func (s *BundleService) DeleteDiscardedDocuments(database *models.Database, bundleName string, docIDs []string) error {
	if len(docIDs) == 0 {
		return nil
	}

	bundle, err := s.GetBundleByName(database, bundleName)
	if err != nil {
		return fmt.Errorf("failed to get bundle %s: %w", bundleName, err)
	}

	// Create a minimal DocumentDeleteCommand for internal use
	docCommand := &models.DocumentDeleteCommand{
		BundleName: bundleName,
	}

	// Use internal deletion without metadata updates (we'll batch those)
	return s.deleteDocumentsInternal(bundle, docCommand, docIDs, true, nil)
}

// processIndexUpdateBatch handles a batch of updates for a specific index
func (s *BundleService) processIndexUpdateBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	switch indexRef.IndexType {
	case "hash":
		return s.processHashIndexBatch(bundle, indexName, indexRef, updates)
	case "btree":
		return s.processBTreeIndexBatch(bundle, indexName, indexRef, updates)
	default:
		return fmt.Errorf("unsupported index type: %s", indexRef.IndexType)
	}
}

// processHashIndexBatch optimizes hash index updates by batching operations.
// Single-write path: when update.HashEntry is set, we write that entry to disk only (no second MemTable/sequence).
// Otherwise we fall back to Put/Delete for backward compatibility.
func (s *BundleService) processHashIndexBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
	if err != nil {
		return fmt.Errorf("failed to load hash index: %w", err)
	}

	// CRITICAL FIX: Deduplicate updates to prevent processing the same document multiple times
	seen := make(map[string]bool)
	deduplicatedUpdates := make([]IndexUpdate, 0, len(updates))

	for _, update := range updates {
		key := update.Operation + ":" + update.DocumentID
		if !seen[key] {
			seen[key] = true
			deduplicatedUpdates = append(deduplicatedUpdates, update)
		} else {
			s.logger.Debugf("Skipping duplicate update for document '%s' in index '%s'", update.DocumentID, indexName)
		}
	}

	// Process all deduplicated updates for disk persistence
	// NOTE: MemTable was already updated synchronously in scheduleIndexUpdate()
	// We use Put/Delete here which will update MemTable again (idempotent) and persist to disk
	successCount := 0
	errorCount := 0

	for _, update := range deduplicatedUpdates {
		if update.HashEntry != nil {
			// Single-write path: same entry already applied to MemTable; write to disk only
			err := hashIndex.WriteEntryToDiskOnly(update.HashEntry)
			if err != nil {
				errorCount++
				s.logger.Warnf("Failed to persist entry to disk (doc '%s') in index V3 '%s': %v",
					update.DocumentID, indexName, err)
			} else {
				successCount++
			}
			continue
		}

		// Fallback: no pre-built entry (e.g. legacy or overflow skip)
		// Use fieldValueToIndexKeyString so document FieldValue unwraps to same string as query lookups
		keyValue := fieldValueToIndexKeyString(update.FieldValue)
		if keyValue == "" || keyValue == "<nil>" {
			keyValue = update.DocumentID
		}

		switch update.Operation {
		case "insert":
			var commitSeq, versionSeq uint64
			if bundle.Database != nil {
				if doc, err := s.GetDocument(update.BundleName, bundle.Database.Name, update.DocumentID); err == nil {
					commitSeq = doc.CommitSequence
					versionSeq = doc.VersionSequence
				}
			}
			err := hashIndex.Put(keyValue, update.DocumentID, update.PageID, commitSeq, versionSeq)
			if err != nil {
				errorCount++
				s.logger.Warnf("Failed to persist insert to disk for key '%s' (doc '%s') in index V3 '%s': %v",
					keyValue, update.DocumentID, indexName, err)
			} else {
				successCount++
			}

		case "delete":
			var commitSeq uint64
			if bundle.Database != nil {
				if doc, err := s.GetDocument(update.BundleName, bundle.Database.Name, update.DocumentID); err == nil {
					commitSeq = doc.CommitSequence
				}
			}
			_, err := hashIndex.Delete(keyValue, commitSeq)
			if err != nil {
				errorCount++
				s.logger.Warnf("Failed to persist delete to disk for key '%s' (doc '%s') in index V3 '%s': %v",
					keyValue, update.DocumentID, indexName, err)
			} else {
				successCount++
			}
		}
	}

	// Log batch processing results
	if errorCount > 0 {
		s.logger.Warnf("Hash index batch processing completed: %d successes, %d errors for index '%s'",
			successCount, errorCount, indexName)
	} else {
		s.logger.Debugf("Hash index batch processing completed: %d disk operations successful for index '%s'",
			successCount, indexName)
	}

	// Flush disk writes
	if err := hashIndex.Flush(); err != nil {
		s.logger.Warnf("Failed to flush hash index V3 '%s' to disk: %v", indexName, err)
	}

	return nil
}

// processBTreeIndexBatch optimizes BTree index updates by batching operations
//
// IMPORTANT: B-tree inserts now happen SYNCHRONOUSLY in scheduleIndexUpdate() for
// immediate visibility (read-your-own-writes consistency). This batch processing
// ONLY handles async disk persistence via Flush().
//
// The in-memory page cache is already updated during the synchronous insert in
// scheduleIndexUpdate(), so this function primarily ensures dirty pages are
// written to disk for durability.
//
// TODO: Potential optimization - track which keys are already in cache and skip
// redundant inserts during batch processing since they were applied synchronously.
//
// TODO: Expose cache metrics for production monitoring:
//   - Cache hit ratio (should be >95% with synchronous inserts)
//   - Dirty page ratio (should stay <80% with auto-flush)
//   - Sync insert latency (target: <500μs, PostgreSQL baseline + 15%)
func (s *BundleService) processBTreeIndexBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
	if err != nil {
		// If index file doesn't exist, log warning and skip updates gracefully
		// This can happen during index initialization or if file was deleted
		s.logger.Warnf("Cannot process BTree index updates for '%s': %v", indexName, err)
		return nil // Don't propagate error - just skip these updates
	}

	if btreeIndex == nil {
		s.logger.Warnf("BTree index '%s' is nil, skipping updates", indexName)
		return nil
	}

	// OPTIMIZATION: Deduplicate updates to avoid redundant inserts
	// Since synchronous inserts in scheduleIndexUpdate() already applied these updates
	// to the in-memory page cache, we only need to apply updates that aren't cached.
	// This eliminates ~10ms of redundant work per batch (50x performance improvement).
	skippedCount := 0
	appliedCount := 0

	for _, update := range updates {
		switch update.Operation {
		case "insert":
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			// PERFORMANCE: Skip dedup check if update was deferred (not applied synchronously)
			// Deferred updates haven't been applied yet, so no need to check for duplicates
			if update.AppliedSync {
				// Check if key+docID already exists in cache (applied synchronously)
				// Search() is fast (~100μs) because it checks PageManager cache first
				existingDocs, searchErr := btreeIndex.Search(keyBytes)
				if searchErr == nil {
					// Check if this specific docID is already present
					alreadyExists := false
					for _, existingDocID := range existingDocs {
						if existingDocID == update.DocumentID {
							alreadyExists = true
							break
						}
					}

					if alreadyExists {
						// Skip redundant insert - already applied synchronously
						skippedCount++
						s.logger.Debugf("Skipped duplicate insert for key in index '%s' (already in cache)", indexName)
						continue
					}
				}
			}

			// Apply insert (directly for deferred, or if not in cache for sync)
			err = btreeIndex.Insert(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to insert into BTree index '%s': %v", indexName, err)
			} else {
				appliedCount++
			}

		case "delete":
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			// For deferred deletes, apply directly (no dedup needed for deletes)
			// For sync deletes, they were already applied but we need to persist
			if update.AppliedSync {
				// Already applied synchronously - skip to avoid double-delete errors
				skippedCount++
				continue
			}

			err = btreeIndex.Delete(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to delete from BTree index '%s': %v", indexName, err)
			} else {
				appliedCount++
			}

		case "update":
			// Delete old value
			if update.OldValue != nil {
				oldKeyBytes, err := convertValueToBytes(update.OldValue)
				if err == nil {
					btreeIndex.Delete(oldKeyBytes, update.DocumentID)
				}
			}

			// Insert new value
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			err = btreeIndex.Insert(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to update BTree index '%s': %v", indexName, err)
			} else {
				appliedCount++
			}
		}
	}

	// Log deduplication statistics
	if skippedCount > 0 {
		s.logger.Debugw("B-tree batch deduplication summary",
			zap.String("index", indexName),
			zap.Int("skipped", skippedCount),
			zap.Int("applied", appliedCount),
			zap.Int("total", len(updates)))
	}

	// Flush dirty pages to disk with single fdatasync for durability
	// This uses batched mode: writes all pages without sync, then one fdatasync at the end
	// Much faster than individual fsync per page (8 pages = 1 sync vs 8 syncs)
	flushStart := time.Now()
	if err := btreeIndex.FlushDirtyPages(); err != nil {
		s.logger.Warnw("Failed to flush B-tree index to disk",
			zap.String("index", indexName),
			zap.Error(err))
	} else {
		flushDuration := time.Since(flushStart)
		if flushDuration > 10*time.Millisecond {
			s.logger.Warnw("⚠️  B-tree disk flush took longer than expected",
				zap.String("index", indexName),
				zap.Duration("duration", flushDuration))
		} else {
			s.logger.Debugw("✓ B-tree disk flush completed",
				zap.String("index", indexName),
				zap.Duration("duration", flushDuration))
		}
	}

	// Persist metadata once per batch (Insert/Delete no longer do it on the hot path)
	if err := btreeIndex.PersistMetadata(); err != nil {
		s.logger.Warnw("Failed to persist B-tree index metadata",
			zap.String("index", indexName),
			zap.Error(err))
	}

	return nil
}

// ForceFlushIndexUpdates is the exported entrypoint so the server can flush index updates
// after each command. Ensures hash index entries reach disk before response (avoids empty
// index after restart when batch size/interval would otherwise delay flush).
func (s *BundleService) ForceFlushIndexUpdates() {
	s.forceFlushIndexUpdates()
}

// forceFlushIndexUpdates ensures all pending updates are processed immediately
// This should be called before critical operations like shutdown
func (s *BundleService) forceFlushIndexUpdates() {
	// FIX: Use mutex to safely check buffer length
	s.indexUpdateMutex.Lock()
	indexCount := len(s.indexUpdateBuffer)
	s.indexUpdateMutex.Unlock()
	if indexCount > 0 {
		s.logger.Debugf("Force flushing %d pending index updates", indexCount)
		s.flushIndexUpdates()
	}
	s.metadataUpdateMutex.RLock()
	metaCount := len(s.metadataUpdateBuffer)
	s.metadataUpdateMutex.RUnlock()
	if metaCount > 0 {
		s.logger.Debugf("Force flushing %d pending metadata updates", metaCount)
		s.FlushMetadataUpdates()
	}
}

func (s *BundleService) AddBundle(databaseService *database.DatabaseService, db *models.Database, bundleCommand *models.BundleCommand) (*models.Bundle, error) {
	args := settings.GetSettings()

	// Validate bundle name (includes _mv_ prefix check)
	if err := s.validateBundleName(bundleCommand.BundleName); err != nil {
		return nil, errors.NewValidationError(
			errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Invalid bundle name '%s'", bundleCommand.BundleName),
			errors.LayerDomain,
			&errors.ValidationErrorDetails{
				SubmittedInput: bundleCommand.BundleName,
				ExpectedFormat: "valid bundle name (alphanumeric, underscores, no spaces)",
				Suggestions:    []string{"Bundle names must start with a letter and contain only alphanumeric characters and underscores"},
			},
		).WithContext("bundle_name", bundleCommand.BundleName)
	}

	// Check if the bundle already exists
	if _, err := s.GetBundleByName(db, bundleCommand.BundleName); err == nil {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT, fmt.Sprintf("Bundle '%s' already exists", bundleCommand.BundleName), errors.LayerDomain).WithContext("bundle_name", bundleCommand.BundleName)
	}

	// Create a new bundle
	bundle := s.factory.NewBundle(bundleCommand.BundleName, "")
	bundle.Database = db

	// Automatically add a DocumentID field to the bundle structure for all bundles
	bundle.DocumentStructure.FieldDefinitions["DocumentID"] = models.FieldDefinition{
		Name:         "DocumentID",
		Type:         "string",
		IsRequired:   true,
		IsUnique:     true,
		DefaultValue: "",
	}

	// Initialize the Document structure in the bundle
	for _, fieldDef := range bundleCommand.Fields {
		bundle.DocumentStructure.FieldDefinitions[fieldDef.Name] = models.FieldDefinition{
			Name:         fieldDef.Name,
			Type:         fieldDef.Type,
			IsRequired:   fieldDef.IsRequired,
			IsUnique:     fieldDef.IsUnique,
			DefaultValue: fieldDef.DefaultValue,
		}
		if args.Debug {
			s.logger.Debugf("Added field '%s' to bundle '%s'", fieldDef.Name, bundleCommand.BundleName)
		}

	}

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Error creating bundle file for bundle '%s'", bundle.Name), errors.LayerStorage)
	}

	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s_%s.bnd", db.Name, bundle.Name))

	// Write the updated database file
	err = databaseService.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Error updating database file after creating bundle '%s'", bundle.Name), errors.LayerStorage)
	}

	createHashIndexInternal(s, bundle, "DocumentID") // Create a hash index on DocumentID

	// Create unique indexes for all IsUnique fields automatically
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	err = uniqueValidator.CreateUniqueIndexesForBundle(bundle)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_INDEX, fmt.Sprintf("Failed to create unique indexes for bundle '%s'", bundle.Name), errors.LayerIndex)
	}

	s.bundleMetadata[bundleCommand.BundleName] = bundle

	// Register the new bundle in the Primary database's "Bundles" catalog
	// This will be handled by the catalog service at a higher level to avoid circular imports
	err = s.registerBundleInPrimary(bundle)
	if err != nil {
		s.logger.Warnf("Warning: Failed to register bundle '%s' in Primary catalog: %v", bundle.Name, err)
		// Don't fail the bundle creation if catalog registration fails
	}

	// GRAPHQL INTEGRATION: Generate and store GraphQL schema for new bundle
	// This creates the initial schema version (v1) and caches it for query execution.
	// Schema generation only occurs if GraphQL support is enabled globally.
	//
	// Steps:
	// 1. Get or create schema manager for this database (lazy initialization)
	// 2. Generate GraphQL schema definition from bundle structure
	// 3. Create schema ID and store in versioned file format
	// 4. Cache schema for fast GraphQL query processing
	//
	// Note: Schema generation failures are logged but don't fail bundle creation.
	// This ensures bundles can be created even if GraphQL has issues.
	if s.graphQLEnabled && s.schemaGenerator != nil {
		s.logger.Debugf("[GraphQL] Generating schema for new bundle '%s' in database '%s'", bundle.Name, db.Name)

		// Get or create the schema manager for this database (thread-safe lazy init)
		schemaManager, err := s.getOrCreateSchemaManager(db)
		if err != nil {
			s.logger.Warnf("[GraphQL] Failed to initialize schema manager for database '%s': %v. Skipping schema generation.", db.Name, err)
		} else if schemaManager != nil {
			// Generate GraphQL schema from bundle structure
			// Converts SyndrDB types → GraphQL types (string→String, int→Int, etc.)
			// Applies PascalCase naming convention (users → User, blog_posts → BlogPost)
			schemaDef, err := s.schemaGenerator.GenerateSchema(bundle)
			if err != nil {
				s.logger.Warnf("[GraphQL] Failed to generate schema for bundle '%s': %v", bundle.Name, err)
			} else {
				// Create unique schema ID for this schema version
				// Convert string UUIDs to [16]byte arrays for storage
				var schemaIDBytes, bundleIDBytes [16]byte
				copy(schemaIDBytes[:], []byte(helpers.GenerateFastUUID()))
				copy(bundleIDBytes[:], []byte(bundle.BundleID))

				// Store schema in versioned file format (creates version 1)
				// This writes to {database_dir}/{database_name}_graphql.gql
				err = schemaManager.AddNewSchema(schemaIDBytes, bundleIDBytes, bundle.Name, schemaDef)
				if err != nil {
					s.logger.Errorf("[GraphQL] Failed to store schema for bundle '%s': %v", bundle.Name, err)
				} else {
					s.logger.Debugf("[GraphQL] Schema created for bundle '%s' (version 1, %d fields)", bundle.Name, len(schemaDef.Fields))
				}
			}
		}
	}

	return bundle, nil
}

func (s *BundleService) AddBundleByStruct(databaseService *database.DatabaseService, db *models.Database, bundle *models.Bundle) error {
	// Set the database reference in the bundle
	bundle.Database = db

	// Initialize bundle properties if not set
	if bundle.PageSize == 0 {
		bundle.PageSize = s.defaultPageSize // Use service default (4096)

	}

	// Initialize TotalDocuments and PageCount - documents are now managed via page cache
	if bundle.TotalDocuments == 0 {
		bundle.TotalDocuments = 0
	}

	// Calculate initial PageCount
	// PageCount = ceil(TotalDocuments / PageSize)
	bundle.PageCount = (bundle.TotalDocuments + int64(bundle.PageSize) - 1) / int64(bundle.PageSize)
	if bundle.PageCount == 0 {
		bundle.PageCount = 1 // Always have at least 1 page for new bundles
	}

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	// Add the bundle to the service cache so it can be retrieved later with relationships intact
	s.bundleMetadata[bundle.Name] = bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("error creating bundle file from struct: %w", err)
	}

	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s_%s.bnd", db.Name, bundle.Name))

	// Write the updated database file
	err = databaseService.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("error updating database file: %w", err)
	}

	createHashIndexInternal(s, bundle, "DocumentID") // Create a hash index on DocumentID

	// Register the new bundle in the Primary database's "Bundles" catalog
	// This will be handled by the catalog service at a higher level to avoid circular imports
	err = s.registerBundleInPrimary(bundle)
	if err != nil {
		s.logger.Warnf("Warning: Failed to register bundle '%s' in Primary catalog: %v", bundle.Name, err)
		// Don't fail the bundle creation if catalog registration fails
	}

	return nil
}

// GetBundleMetadata retrieves only the bundle structure/metadata without documents
func (s *BundleService) GetBundleMetadata(database *models.Database, name string) (*models.Bundle, error) {
	//args := settings.GetSettings()
	fileExists := s.store.BundleFileExists(name, database.Name)

	// Check if the bundle file exists in the store
	if !fileExists {
		return nil, errors.New(errors.ERR_NOT_FOUND_BUNDLE, fmt.Sprintf("Bundle file '%s' does not exist on disk", name), errors.LayerStorage).WithContext("bundle_name", name)
	}

	bundle, exists := s.bundleMetadata[name]
	if !exists {
		if fileExists {
			// If the bundle exists in the store but not in memory, load metadata only
			// if args.Debug {
			// 	s.logger.Debugf("Bundle metadata '%s' not found in memory, loading from store", name)
			// }

			databasePath := helpers.GetDatabaseFolderPath(database.Name)

			bundle, err := s.store.LoadBundleMetadata(database, databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, name))
			if err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Failed to load bundle metadata '%s'", name), errors.LayerStorage).WithContext("bundle_name", name)
			}

			// Load the SortedIndex for this bundle (for pageID alignment)
			if err := InitializeBundleSortedIndex(bundle); err != nil {
				s.logger.Warnf("Failed to load sorted index for bundle '%s': %v (will use fallback pageID calculation)", name, err)
				// Ensure SortedIndex is never nil - create empty fallback
				if bundle.SortedIndex == nil {
					bundle.SortedIndex = models.NewShardedSortedIndex()
				}
			}

			// Discover and populate existing index files
			if len(bundle.Indexes) == 0 {
				err = s.discoverBundleIndexes(bundle)
				if err != nil {
					s.logger.Warnf("Failed to discover indexes for bundle '%s': %v", name, err)
					// Continue loading the bundle even if index discovery fails
				}
			}

			// if args.Debug {
			// 	s.logger.Debugf("Loaded bundle metadata '%s' from store", name)
			// }

			s.bundleMetadata[name] = bundle
			return bundle, nil
		} else {
			return nil, errors.New(errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Bundle file exists in memory but not on disk. '%s_%s.bnd' not found", database.Name, name), errors.LayerStorage).WithContext("bundle_name", name)
		}
	}

	// Bundle exists in memory, but check if SortedIndex needs initialization
	if bundle.SortedIndex == nil {
		s.logger.Debugf("Bundle '%s' is in memory but SortedIndex is nil, initializing", name)
		if err := InitializeBundleSortedIndex(bundle); err != nil {
			s.logger.Warnf("Failed to load sorted index for cached bundle '%s': %v (will use fallback)", name, err)
			// Ensure SortedIndex is never nil - create empty fallback
			if bundle.SortedIndex == nil {
				bundle.SortedIndex = models.NewShardedSortedIndex()
			}
		}
	}

	// Bundle exists in memory, but check if indexes need to be discovered
	if len(bundle.Indexes) == 0 {
		s.logger.Debugf("Bundle '%s' is in memory but has no indexes, attempting discovery", name)
		err := s.discoverBundleIndexes(bundle)
		if err != nil {
			s.logger.Warnf("Failed to discover indexes for in-memory bundle '%s': %v", name, err)
		}
	}

	return bundle, nil
}

// GetDocumentPage loads a specific page of documents for a bundle.
// documentPagesMutex is used to prevent concurrent map read/write (evictOldestPage range vs other goroutines' read/write).
// CRITICAL: Always clears projection fields before loading to ensure full pages are cached, not partial/projected pages.
// This prevents cache poisoning where a query with projection would cache partial documents that can't serve other queries.
// OPTIMIZATION: Uses O(1) LRU eviction via doubly-linked list instead of O(n) map scan.
//
// DEADLOCK FIX: Removed write lock for LRU promotion on cache hits. Under high read concurrency,
// taking a write lock on every cache hit caused RWMutex starvation:
// - Multiple readers waiting for RLock
// - Writer waiting to promote LRU position
// - New readers blocked behind writer
// - Result: deadlock
//
// Solution: Skip LRU promotion on cache hits. LRU is only updated on cache misses (insertions).
// This is acceptable because:
// 1. Frequently accessed pages will be re-inserted on eviction, naturally staying in cache
// 2. The eviction policy is approximate anyway - slightly suboptimal eviction is fine
// 3. Correctness is maintained - we just sacrifice some LRU accuracy for concurrency
func (s *BundleService) GetDocumentPage(bundleName string, databaseName string, pageID uint32) (*models.DocumentPage, error) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// READER VIEW: Lock-free read path (no shard mutex). Readers never block writers.
	if v, ok := shard.readerView.Load(pageKey); ok {
		if snapshot, ok := v.(*models.DocumentPage); ok {
			return s.createSafePageCopy(snapshot), nil
		}
	}

	// Fallback: authoritative page in fastLookup (requires RLock to copy)
	// CONCURRENCY FIX: Must take RLock before copying Documents map to prevent concurrent iteration/write
	if cached, ok := shard.fastLookup.Load(pageKey); ok {
		if page, ok := cached.(*models.DocumentPage); ok {
			shard.mu.RLock()
			safeCopy := s.createSafePageCopy(page)
			shard.mu.RUnlock()
			return safeCopy, nil
		}
	}

	// Cache miss - load from disk without holding any locks

	// CRITICAL: Clear any per-bundle projection before loading so we get full documents.
	// Projection pushdown (e.g. ORDER BY) sets projection on the storage engine, and if we don't clear it,
	// LoadDocumentPage will use getProjectionFieldsForBundle and return partial docs, which we'd then cache.
	// This causes cache poisoning: cached partial pages can't serve queries needing all fields.
	// Projection is applied in-memory after retrieval, not during disk load.
	s.SetProjectionFieldsForBundle(bundleName, nil)

	// Load the page from disk (outside RLock to avoid holding during I/O)
	s.logger.Debugf("Loading document page %s from disk", pageKey)
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	page, err := s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load document page %s: %w", pageKey, err)
	}

	// DEADLOCK FIX: Use TryLock instead of Lock to avoid blocking
	// Under high concurrency, we prefer to skip caching rather than wait for locks.
	// This matches PostgreSQL's approach: reads never wait, cache fills opportunistically.
	if shard.mu.TryLock() {
		// Double-check after acquiring write lock (another goroutine may have inserted it)
		if p, exists := shard.pages[pageKey]; exists {
			// Return safe copy so caller cannot mutate the cached page (Issue 1).
			safeCopy := s.createSafePageCopy(p)
			shard.mu.Unlock()
			return safeCopy, nil
		}
		// O(1) eviction: check capacity and evict from back of LRU list
		if len(shard.pages) >= shard.maxPages {
			shard.evictOldestLocked()
		}
		// Insert new page into both maps and add to front of LRU
		shard.insertLocked(pageKey, page)
		elem := shard.lruOrder.PushFront(pageKey)
		shard.lruElements[pageKey] = elem
		// Reader view: store immutable snapshot so future reads are lock-free.
		// Return the snapshot directly — readerView entries are immutable by contract,
		// so no second copy is needed (eliminates redundant createSafePageCopy).
		snapshot := s.createSafePageCopy(page)
		shard.readerView.Store(pageKey, snapshot)
		shard.mu.Unlock()
		return snapshot, nil
	}

	// TryLock failed (Issue 4): Optional backoff then blocking Lock so at least one goroutine
	// can cache the page. Backoff is configurable (page_cache_trylock_backoff_ms); 0 = no sleep for low latency.
	if backoffMs := settings.GetSettings().PageCacheTryLockBackoffMs; backoffMs > 0 {
		time.Sleep(time.Duration(backoffMs) * time.Millisecond)
	}
	shard.mu.Lock()
	if p, exists := shard.pages[pageKey]; exists {
		safeCopy := s.createSafePageCopy(p)
		shard.mu.Unlock()
		return safeCopy, nil
	}
	if len(shard.pages) >= shard.maxPages {
		shard.evictOldestLocked()
	}
	shard.insertLocked(pageKey, page)
	elem := shard.lruOrder.PushFront(pageKey)
	shard.lruElements[pageKey] = elem
	// Reader view: store immutable snapshot so future reads are lock-free.
	// Return directly — no second copy needed (immutable by contract).
	snapshot := s.createSafePageCopy(page)
	shard.readerView.Store(pageKey, snapshot)
	shard.mu.Unlock()
	return snapshot, nil
}

// SnapshotPageDocuments safely snapshots documents from a page to avoid concurrent map iteration.
// This is used by code that needs to iterate over page.Documents while writes may be happening.
//
// Thread Safety:
// - Atomically checks cache and snapshots under one lock acquisition
// - If page not in cache, loads it directly from disk WITHOUT caching to avoid write lock contention
// - Returns a slice copy, safe for concurrent iteration
//
// DEADLOCK FIX: Previously this called GetDocumentPage() when page wasn't cached, which requires
// a write lock. Under high concurrency with parallel page reads, this caused RWMutex starvation:
// - Multiple readers hold RLock iterating pages
// - One reader needs to load uncached page, releases RLock, requests write Lock
// - Write Lock blocked waiting for readers to finish
// - But readers are spawned in batches and new RLock requests queue behind the waiting writer
// - Result: deadlock where nothing can progress
//
// Solution: Load directly from disk without caching. This is safe because:
// 1. The write-through cache ensures any recent writes are already on disk
// 2. For bulk read operations (joins, scans), not caching is acceptable
// 3. Avoids the read-to-write lock upgrade pattern that causes deadlocks
//
// Parameters:
//   - bundleName: Name of the bundle
//   - databaseName: Name of the database
//   - pageID: Page identifier
//
// Returns:
//   - []models.Document: Snapshot of documents (safe for iteration)
//   - error: Any error encountered
//
// MVCC (Phase 1): Applies IsVisibleReadCommitted() filtering to exclude superseded
// and uncommitted document versions. This ensures lock-free reads only return
// committed, current documents without requiring bundle-level read locks.
func (s *BundleService) SnapshotPageDocuments(bundleName, databaseName string, pageID uint32) ([]models.Document, error) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// PHASE 3: Check COW snapshot cache first (avoids RLock entirely)
	// PERFORMANCE FIX: No staleness check on hot path - background cleaner handles cleanup
	// Issue 2: Return a copy of the slice so callers cannot mutate the COW cache.
	if cached, ok := shard.cowSnapshot.Load(pageKey); ok {
		snapshot := cached.(*cowSnapshotEntry)
		docsCopy := make([]models.Document, 0, len(snapshot.documents))
		docsCopy = append(docsCopy, snapshot.documents...)
		return docsCopy, nil
	}

	// READER VIEW: Lock-free read path (no shard mutex). Readers never block writers.
	if v, ok := shard.readerView.Load(pageKey); ok {
		if snapshot, ok := v.(*models.DocumentPage); ok {
			docs := make([]models.Document, 0, len(snapshot.Documents))
			for _, doc := range snapshot.Documents {
				if doc.IsVisibleReadCommitted() {
					docs = append(docs, doc)
				}
			}
			// PHASE 3: Store in COW cache for subsequent parallel GROUP BY reads
			snapshotEntry := &cowSnapshotEntry{
				documents: docs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}
			shard.cowSnapshot.Store(pageKey, snapshotEntry)
			return docs, nil
		}
	}

	// Fallback: authoritative page in fastLookup (requires RLock to copy)
	if cached, ok := shard.fastLookup.Load(pageKey); ok {
		if page, ok := cached.(*models.DocumentPage); ok {
			shard.mu.RLock()
			safePage := s.createSafePageCopy(page)
			shard.mu.RUnlock()
			docs := make([]models.Document, 0, len(safePage.Documents))
			for _, doc := range safePage.Documents {
				if doc.IsVisibleReadCommitted() {
					docs = append(docs, doc)
				}
			}
			snapshotEntry := &cowSnapshotEntry{
				documents: docs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}
			shard.cowSnapshot.Store(pageKey, snapshotEntry)
			return docs, nil
		}
	}

	// Page not in cache - load directly from disk

	// Load page directly from disk without caching
	// This avoids the write lock entirely for read-only snapshot operations
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	loadedPage, err := s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load document page %d for snapshot: %w", pageID, err)
	}

	// Snapshot the loaded page - no locking needed since we have our own copy
	docs := make([]models.Document, 0, len(loadedPage.Documents))
	for _, doc := range loadedPage.Documents {
		// MVCC (Phase 1): Filter out superseded/uncommitted versions
		if doc.IsVisibleReadCommitted() {
			docs = append(docs, doc)
		}
	}

	// Issue 10: Best-effort insert into authoritative cache so GetDocumentPage can hit it later.
	if shard.mu.TryLock() {
		if _, exists := shard.pages[pageKey]; !exists {
			if len(shard.pages) >= shard.maxPages {
				shard.evictOldestLocked()
			}
			shard.insertLocked(pageKey, loadedPage)
			elem := shard.lruOrder.PushFront(pageKey)
			shard.lruElements[pageKey] = elem
			shard.readerView.Store(pageKey, s.createSafePageCopy(loadedPage))
		}
		shard.mu.Unlock()
	}

	// PHASE 3: Cache snapshot (even for disk-loaded pages)
	snapshot := &cowSnapshotEntry{
		documents: docs,
		timestamp: time.Now().UnixMilli(),
		pageKey:   pageKey,
	}
	shard.cowSnapshot.Store(pageKey, snapshot)

	return docs, nil
}

// SnapshotPageDocumentsReadOnly returns a read-only view of the COW snapshot slice WITHOUT copying.
// This is a zero-allocation fast path for scan operations that only read documents (predicate evaluation,
// pointer collection) and never mutate them.
//
// Safety contract: Caller MUST NOT:
//   - Mutate the returned slice (no append, no element assignment)
//   - Mutate any Document struct in the slice (no writing to doc.Fields, doc.Data, etc.)
//
// This is safe because:
//   - COW entries are immutable after creation
//   - cowSnapshot.Delete() on write doesn't free the old slice (Go GC keeps it alive)
//   - Scan paths only read documents to evaluate predicates and collect pointers
//
// When callers need to mutate documents (e.g., projection), use SnapshotPageDocuments() instead.
func (s *BundleService) SnapshotPageDocumentsReadOnly(bundleName, databaseName string, pageID uint32) ([]models.Document, error) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// FAST PATH: Return COW snapshot slice directly (zero allocation)
	if cached, ok := shard.cowSnapshot.Load(pageKey); ok {
		snapshot := cached.(*cowSnapshotEntry)
		return snapshot.documents, nil
	}

	// READER VIEW: Lock-free read path
	if v, ok := shard.readerView.Load(pageKey); ok {
		if snapshot, ok := v.(*models.DocumentPage); ok {
			docs := make([]models.Document, 0, len(snapshot.Documents))
			for _, doc := range snapshot.Documents {
				if doc.IsVisibleReadCommitted() {
					docs = append(docs, doc)
				}
			}
			// Store in COW cache for subsequent reads
			snapshotEntry := &cowSnapshotEntry{
				documents: docs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}
			shard.cowSnapshot.Store(pageKey, snapshotEntry)
			return docs, nil
		}
	}

	// Fallback: authoritative page in fastLookup (requires RLock to copy)
	if cached, ok := shard.fastLookup.Load(pageKey); ok {
		if page, ok := cached.(*models.DocumentPage); ok {
			shard.mu.RLock()
			safePage := s.createSafePageCopy(page)
			shard.mu.RUnlock()
			docs := make([]models.Document, 0, len(safePage.Documents))
			for _, doc := range safePage.Documents {
				if doc.IsVisibleReadCommitted() {
					docs = append(docs, doc)
				}
			}
			snapshotEntry := &cowSnapshotEntry{
				documents: docs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}
			shard.cowSnapshot.Store(pageKey, snapshotEntry)
			return docs, nil
		}
	}

	// Page not in cache - load from disk
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	loadedPage, err := s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load document page %d for read-only snapshot: %w", pageID, err)
	}

	docs := make([]models.Document, 0, len(loadedPage.Documents))
	for _, doc := range loadedPage.Documents {
		if doc.IsVisibleReadCommitted() {
			docs = append(docs, doc)
		}
	}

	// Best-effort insert into authoritative cache
	if shard.mu.TryLock() {
		if _, exists := shard.pages[pageKey]; !exists {
			if len(shard.pages) >= shard.maxPages {
				shard.evictOldestLocked()
			}
			shard.insertLocked(pageKey, loadedPage)
			elem := shard.lruOrder.PushFront(pageKey)
			shard.lruElements[pageKey] = elem
			shard.readerView.Store(pageKey, s.createSafePageCopy(loadedPage))
		}
		shard.mu.Unlock()
	}

	// Cache snapshot for subsequent reads
	snapshot := &cowSnapshotEntry{
		documents: docs,
		timestamp: time.Now().UnixMilli(),
		pageKey:   pageKey,
	}
	shard.cowSnapshot.Store(pageKey, snapshot)

	return docs, nil
}

// createSafePageCopy creates an isolated copy of a DocumentPage with snapshotted Documents map.
// This prevents concurrent map access when the page is returned without being cached.
//
// Thread Safety:
// - Creates new map and copies all documents (value copy, not reference)
// - Returned page is safe for concurrent read access without locks
//
// Shallow copy (Issue 3): Document structs are copied by value, but each Document's
// Fields and Data maps are reference types and are shared with the original. Callers
// must not mutate any document's Fields or Data maps; mutation would affect the
// cached page. For true isolation, a deep copy (e.g. Document.Clone()) would be needed.
//
// Parameters:
//   - page: The source page to copy
//
// Returns:
//   - *models.DocumentPage: Isolated copy safe for concurrent access (shallow; do not mutate document Fields/Data)
func (s *BundleService) createSafePageCopy(page *models.DocumentPage) *models.DocumentPage {
	safePage := &models.DocumentPage{
		PageID:    page.PageID,
		BundleID:  page.BundleID,
		Documents: make(map[string]models.Document, len(page.Documents)),
	}
	// Copy all documents - this is a value copy so each goroutine gets its own data
	for docID, doc := range page.Documents {
		safePage.Documents[docID] = doc
	}
	return safePage
}

// snapshotPageDocumentsFromPointer takes an already-loaded page pointer and returns a safe snapshot.
// This is for cases where you already have the page and just need to snapshot its Documents map.
//
// Thread Safety:
// - Uses sharded read lock based on page metadata
// - Returns a slice copy, safe for concurrent iteration
//
// Parameters:
//   - page: The page pointer (already loaded)
//   - databaseName: Name of the database (needed for shard calculation)
//
// Returns:
//   - []models.Document: Snapshot of documents (safe for iteration)
func (s *BundleService) snapshotPageDocumentsFromPointer(page *models.DocumentPage, databaseName string) []models.Document {
	// CONCURRENCY FIX: page pointer may reference cached page with unsafe Documents map
	// Must protect Documents map iteration - get shard lock before copy
	pageKey := fmt.Sprintf("%s:%d", page.BundleID, page.PageID)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	shard.mu.RLock()
	safePage := s.createSafePageCopy(page)
	shard.mu.RUnlock()

	docs := make([]models.Document, 0, len(safePage.Documents))
	for _, doc := range safePage.Documents {
		docs = append(docs, doc)
	}
	return docs
}

// CountDocuments counts all documents in a bundle using optimized count-only parser
// This is much faster than loading all pages because it extracts only DocumentIDs
// without parsing full document data
//
// Parameters:
//   - bundleName: Name of the bundle to count
//   - databaseName: Name of the database containing the bundle
//
// Returns:
//   - int: Count of unique documents (excluding tombstones)
//   - error: Any error encountered during counting
func (s *BundleService) CountDocuments(bundleName, databaseName string) (int, error) {
	// OPTIMIZATION: Use in-memory SortedIndex if available (always up-to-date)
	// SortedIndex is updated immediately on INSERT/DELETE, before flush to disk
	// This ensures COUNT(*) returns accurate results even with buffered writes
	bundle, exists := s.bundleMetadata[bundleName]
	if exists && bundle.SortedIndex != nil {
		// CRITICAL: Verify SortedIndex has documents before trusting it
		// If load failed with EOF, an empty index was created as fallback
		// In that case, we must fall back to disk-based counting
		count := bundle.SortedIndex.TotalDocuments()
		if count > 0 {
			// SortedIndex maintains atomic counts across all shards
			return int(count), nil
		}
		// Empty index could mean:
		// 1. Bundle truly has 0 documents
		// 2. Index load failed and fallback empty index was created
		// We need to check disk to be sure
		s.logger.Debugf("SortedIndex for bundle '%s' is empty, verifying with disk-based count", bundleName)
	}

	// Fallback to disk-based counting (for bundles not yet loaded in memory or when index is empty)
	return s.store.CountDocuments(bundleName, databaseName)
}

// CopyProjectedFromCache copies projected documents from documentPages cache under one RLock
// OPTIMIZATION: One-time lock acquisition, iterates cached pages, copies only projected fields
// This is used for session-specific cache to reduce lock contention in GROUP BY queries
//
// Parameters:
//   - bundleName: Name of the bundle
//   - databaseName: Name of the database
//   - pageCount: Total number of pages to check (from bundle metadata)
//   - projectFields: Field names to copy (GROUP BY fields + DocumentID)
//   - effectiveLimit: 0 = no limit (GROUP BY), >0 = stop after that many docs (simple scan with LIMIT)
//
// Returns:
//   - map[string]*ProjectedDocument: Projected documents copied from cache (keyed by DocumentID)
//   - int: Number of documents copied
//   - int: Number of pages that were in cache
//   - int: Total pages checked
//   - error: Any error encountered
func (s *BundleService) CopyProjectedFromCache(bundleName, databaseName string, pageCount uint32, projectFields []string, effectiveLimit int) (map[string]*documentscanner.ProjectedDocument, int, int, int, error) {
	// Build field set for O(1) lookup
	fieldSet := make(map[string]bool, len(projectFields))
	for _, field := range projectFields {
		fieldSet[field] = true
	}
	// Always include DocumentID
	fieldSet["DocumentID"] = true

	projectedDocs := make(map[string]*documentscanner.ProjectedDocument, 4096) // Pre-allocate reasonable capacity
	docsCopied := 0
	cachedPages := 0

	// DEADLOCK FIX: Use per-shard locking instead of global mutex
	// Iterate through all pages, acquiring shard locks as needed
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
		shardIdx := s.getPageShardIndex(pageKey)
		shard := s.pageShards[shardIdx]

		var safePage *models.DocumentPage
		// READER VIEW: Lock-free lookup first (no shard mutex)
		if v, ok := shard.readerView.Load(pageKey); ok {
			if p, ok := v.(*models.DocumentPage); ok {
				safePage = p
				cachedPages++
			}
		}
		if safePage == nil {
			// Fallback: authoritative page under RLock
			cached, exists := shard.fastLookup.Load(pageKey)
			if !exists {
				continue
			}
			page, ok := cached.(*models.DocumentPage)
			if !ok {
				continue
			}
			cachedPages++
			shard.mu.RLock()
			safePage = s.createSafePageCopy(page)
			shard.mu.RUnlock()
		}
		if safePage == nil {
			continue
		}

		// Copy projected fields from each document in this page
		for docID, doc := range safePage.Documents {
			// Check effectiveLimit (Phase 4: LIMIT + no ORDER BY early termination)
			if effectiveLimit > 0 && docsCopied >= effectiveLimit {
				// Reached limit - stop copying
				return projectedDocs, docsCopied, cachedPages, int(pageID + 1), nil
			}

			// Create projected document with only needed fields
			projDoc := &documentscanner.ProjectedDocument{
				DocumentID:    docID,
				GroupByFields: make(map[string]interface{}),
			}

			// Copy only projected fields
			for fieldName, field := range doc.Fields {
				if fieldSet[fieldName] {
					// Copy field value
					projDoc.GroupByFields[fieldName] = field.Value.AsInterface()
				}
			}

			projectedDocs[docID] = projDoc
			docsCopied++
		}
		// No unlock needed - we used lock-free access with safe copy
	}

	return projectedDocs, docsCopied, cachedPages, int(pageCount), nil
}

// GetDocument retrieves a specific document by ID
// Uses memory-first architecture: checks in-memory documents before hitting disk
// This ensures dirty documents are readable before flush and provides optimal performance
//
// PHASE 4: MVCC - Optional snapshot filtering for visibility
// Parameters:
//   - bundleName: Name of the bundle
//   - databaseName: Name of the database
//   - documentID: Document ID to retrieve
//   - snapshotSequence: Optional snapshot sequence for MVCC filtering (0 = no filtering, return latest)
//   - txID: Optional transaction ID for uncommitted visibility (0 = no filtering)
//   - activeTxIDs: Optional map of active transaction IDs at snapshot time (nil = no filtering)
//
// Returns the first visible version of the document, or error if not found
func (s *BundleService) GetDocument(bundleName, databaseName, documentID string, snapshotParams ...interface{}) (*models.Document, error) {
	// PHASE 4: MVCC - Extract snapshot parameters if provided
	var snapshotSeq uint64
	var txID uint64
	var activeTxIDs map[uint64]bool
	if len(snapshotParams) >= 1 {
		if seq, ok := snapshotParams[0].(uint64); ok {
			snapshotSeq = seq
		}
	}
	if len(snapshotParams) >= 2 {
		if id, ok := snapshotParams[1].(uint64); ok {
			txID = id
		}
	}
	if len(snapshotParams) >= 3 {
		if ids, ok := snapshotParams[2].(map[uint64]bool); ok {
			activeTxIDs = ids
		}
	}

	// Get the bundle metadata (used for MVCC version scanning)
	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle %s not found", bundleName)
	}
	_ = bundle // Used in MVCC path below

	// PHASE 4: MVCC - If snapshot provided, use version scanning with visibility filtering
	if snapshotSeq > 0 {
		// Get all versions and filter by visibility
		versions, err := s.GetDocumentVersions(bundleName, databaseName, documentID)
		if err != nil {
			return nil, fmt.Errorf("failed to get document versions: %w", err)
		}

		// Scan backward (newest first) and return first visible version
		for _, doc := range versions {
			if doc.IsVisibleToSnapshot(snapshotSeq, txID, activeTxIDs) {
				return doc, nil
			}
		}

		// No visible version found
		return nil, fmt.Errorf("document %s not visible to snapshot (seq: %d)", documentID, snapshotSeq)
	}

	// No snapshot filtering - use fast path (latest version)
	// All reads now go through the write-through page cache

	// Load document from page cache (write-through cache)
	//s.logger.Debugf("Document %s not in memory, loading from disk for bundle %s", documentID, bundleName)

	// CRITICAL: Clear any projection fields before loading to ensure full document is retrieved.
	// GetDocumentPage already does this, but we do it here too as a safety measure for any direct callers.
	s.SetProjectionFieldsForBundle(bundleName, nil)

	// Find which page contains this document using the index
	pageID, err := s.findDocumentPage(bundleName, documentID)
	if err != nil {
		return nil, fmt.Errorf("could not find document %s in bundle %s: %w", documentID, bundleName, err)
	}

	// Load the page containing the document from disk
	// GetDocumentPage will also clear projection, ensuring full pages are cached
	page, err := s.GetDocumentPage(bundleName, databaseName, pageID)
	if err != nil {
		return nil, err
	}

	// CONCURRENCY FIX: Get safe copy to access Documents map without concurrent access
	safePage := s.createSafePageCopy(page)
	doc, exists := safePage.Documents[documentID]

	if exists {
		return &doc, nil
	}

	return nil, fmt.Errorf("document %s not found in page %d of bundle %s", documentID, pageID, bundleName)
}

// GetDocumentsByIDs loads multiple documents by ID in batch (P2b).
// OPTIMIZED: Uses parsed docs cache first, avoiding stale pageID lookups.
// Preserves input order; skips missing docs (logs warn) like convertDocIDsToDocuments.
func (s *BundleService) GetDocumentsByIDs(bundle *models.Bundle, docIDs []string) ([]*models.Document, error) {
	if len(docIDs) == 0 {
		return nil, nil
	}
	bundleName := bundle.Name
	dbName := bundle.Database.Name

	// All reads now go through the write-through page cache
	toLoad := docIDs
	byID := make(map[string]*models.Document, len(docIDs))

	// OPTIMIZATION: Try parsed docs cache first (avoids stale pageID lookups)
	// This is much faster than page-based lookup when indexes have stale pageIDs
	cachedDocs, notInCache := s.store.GetDocumentsByIDsFromCache(bundleName, dbName, toLoad)
	for docID, doc := range cachedDocs {
		cp := doc
		byID[docID] = &cp
	}

	// Only do page-based lookup for docs not in parsed cache
	stillToLoad := make([]string, 0, len(notInCache))
	for docID := range notInCache {
		stillToLoad = append(stillToLoad, docID)
	}

	if len(stillToLoad) > 0 {
		// Fallback: Resolve pageID for remaining docs
		pageToIDs := make(map[uint32][]string)
		for _, id := range stillToLoad {
			pageID, err := s.findDocumentPage(bundleName, id)
			if err != nil {
				s.logger.Warnf("GetDocumentsByIDs: document %s not found: %v", id, err)
				continue
			}
			pageToIDs[pageID] = append(pageToIDs[pageID], id)
		}

		// Load each page once and extract docs
		for pageID, ids := range pageToIDs {
			page, err := s.GetDocumentPage(bundleName, dbName, pageID)
			if err != nil {
				for _, id := range ids {
					s.logger.Warnf("GetDocumentsByIDs: failed to load page %d for %s: %v", pageID, id, err)
				}
				continue
			}

			// CONCURRENCY FIX: Get safe copy to access Documents map
			safePage := s.createSafePageCopy(page)

			for _, id := range ids {
				if d, ok := safePage.Documents[id]; ok {
					cp := d
					byID[id] = &cp
				} else {
					// Page-based lookup can miss due to stale pageID in index.
					// Invalidate stale entry so we don't keep using the bad pageID.
					s.invalidateDocumentPageMapEntry(bundleName, id)
					StalePageIDFallbackCounter.Add(1)

					// FIXED: Scan document page cache instead of GetDocument (which also uses stale pageID)
					foundDoc := s.findDocumentInPageCache(bundleName, id)
					if foundDoc != nil {
						byID[id] = foundDoc
						s.logger.Debugf("GetDocumentsByIDs: document %s not in page %d, found via cache scan", id, pageID)
					} else {
						// Last resort: try GetDocument (may work if doc is in memtable)
						doc, getErr := s.GetDocument(bundleName, dbName, id)
						if getErr == nil {
							byID[id] = doc
							s.logger.Debugf("GetDocumentsByIDs: document %s found via GetDocument fallback", id)
						} else {
							s.logger.Debugf("GetDocumentsByIDs: document %s not found (may be deleted)", id)
						}
					}
				}
			}
			// No unlock needed - we used lock-free access with safe copy
		}
	}

	// Preserve order of docIDs; skip missing
	out := make([]*models.Document, 0, len(docIDs))
	for _, id := range docIDs {
		if d := byID[id]; d != nil {
			out = append(out, d)
		}
	}
	return out, nil
}

// GetDocumentVersions retrieves all versions of a document for MVCC visibility filtering
// PHASE 0: MVCC Version Storage Foundation
// This scans backward through all bundle files to find all versions of a DocumentID
// Returns versions sorted by VersionSequence (descending - newest first)
//
// Parameters:
//   - bundleName: Name of the bundle
//   - databaseName: Name of the database
//   - documentID: The document ID to find versions for
//
// Returns:
//   - []*models.Document: All versions of the document, sorted by VersionSequence (descending)
//   - error: Any error encountered
func (s *BundleService) GetDocumentVersions(bundleName, databaseName, documentID string) ([]*models.Document, error) {
	// Delegate to storage engine's GetDocumentVersions
	return s.store.GetDocumentVersions(bundleName, databaseName, documentID)
}

// findDocumentInPageCache scans all cached pages to find a document by ID.
// This is used as a fallback when the pageID from the index is stale.
// Returns nil if document is not found in any cached page.
// PERFORMANCE: O(shards × docs_per_shard) but only used for fallback cases.
// DEADLOCK FIX: Uses per-shard locking instead of global mutex.
func (s *BundleService) findDocumentInPageCache(bundleName, documentID string) *models.Document {
	// Scan all shards for this bundle
	prefix := bundleName + ":"

	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		// POSTGRESQL-INSPIRED: Scan lock-free fastLookup for bundle prefix
		// Since sync.Map doesn't support prefix iteration, we need to scan the
		// authoritative map under RLock. This is acceptable because this is a
		// fallback path (not hot path), and we release lock immediately per page.
		shard.mu.RLock()
		for key, page := range shard.pages {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			// CONCURRENCY FIX: Create safe copy WHILE holding RLock to prevent concurrent map iteration/write
			// We must copy the Documents map while protected by the lock, then release
			safePage := s.createSafePageCopy(page)
			shard.mu.RUnlock()

			doc, exists := safePage.Documents[documentID]
			if exists {
				cp := doc
				return &cp
			}
			shard.mu.RLock() // Re-lock for next iteration
		}
		shard.mu.RUnlock()
	}
	return nil
}

// GetDocumentsByIDsFromCacheDirect retrieves documents by IDs directly from cache.
// Bypasses pageID lookups entirely - used when index pageIDs may be stale.
// Returns documents found as a slice preserving input order, skipping missing docs.
func (s *BundleService) GetDocumentsByIDsFromCacheDirect(bundle *models.Bundle, docIDs []string) []*models.Document {
	if len(docIDs) == 0 {
		return nil
	}

	bundleName := bundle.Name
	dbName := bundle.Database.Name

	// All reads now go through the write-through page cache
	byID := make(map[string]*models.Document, len(docIDs))
	toLoad := docIDs

	// Use storage engine's cache-based lookup (bypasses stale pageID issue)
	cachedDocs, _ := s.store.GetDocumentsByIDsFromCache(bundleName, dbName, toLoad)
	for docID, doc := range cachedDocs {
		cp := doc
		byID[docID] = &cp
	}

	// Preserve order of docIDs; skip missing
	out := make([]*models.Document, 0, len(docIDs))
	for _, id := range docIDs {
		if d := byID[id]; d != nil {
			out = append(out, d)
		}
	}
	return out
}

// evictDocumentPageMapOneLocked evicts one documentID->pageID entry from the bundle's documentPageMap.
// PHASE 5: DEPRECATED - This function is no longer needed as ShardedPageCacheMap handles eviction internally.
// Keeping as a no-op for backward compatibility in case any code still references it.
func (s *BundleService) evictDocumentPageMapOneLocked(bundleID string) {
	// PHASE 5: ShardedPageCacheMap handles eviction internally in SetPageID
	// This function is kept for backward compatibility but does nothing
}

// invalidateDocumentPageMapEntry removes one documentID from documentPageMap and its FIFO.
// Called when a doc's pageID is stale (e.g. after UPDATE, or when "document not in page").
// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.
func (s *BundleService) invalidateDocumentPageMapEntry(bundleName, documentID string) {
	s.documentPageCache.InvalidateDocument(bundleName, documentID)
}

// InvalidateDocumentPageMapForBundle clears all documentID->pageID entries for a bundle.
// Called when the whole mapping is stale (e.g. after compaction).
// Also rebuilds the SortedIndex to ensure correct pageID calculation after compaction.
// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.
func (s *BundleService) InvalidateDocumentPageMapForBundle(bundleName string) {
	s.documentPageCache.InvalidateBundle(bundleName)

	// Clear entire visibility map on compaction (page contents may have changed)
	s.clearVisibilityForBundle(bundleName)

	// PAGE ID ARCHITECTURE ALIGNMENT: Rebuild SortedIndex after compaction
	// Compaction removes tombstoned documents and rewrites the bundle file,
	// which changes document positions. Rebuild the SortedIndex from the
	// surviving documents to ensure correct pageID calculation.
	s.rebuildSortedIndexAfterCompaction(bundleName)
}

// rebuildSortedIndexAfterCompaction rebuilds a bundle's SortedIndex from the
// DocumentID hash index after compaction completes.
// This ensures the SortedIndex only contains live (non-tombstoned) documents.
// Also schedules async rebuilds for all user-created indexes to update their pageIDs.
func (s *BundleService) rebuildSortedIndexAfterCompaction(bundleName string) {
	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		s.logger.Warnf("Cannot rebuild SortedIndex: bundle %s not found in metadata", bundleName)
		return
	}

	if bundle.SortedIndex == nil {
		// Create a new SortedIndex if it doesn't exist
		bundle.SortedIndex = models.NewShardedSortedIndex()
	}

	// Get the DocumentID hash index to retrieve all live document IDs
	var docIDs []string
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load DocumentID index for SortedIndex rebuild: %v", err)
					return
				}

				// Get all document IDs grouped by page
				docIDsByPage, err := hashIndex.GetAllDocumentIDs()
				if err != nil {
					s.logger.Warnf("Failed to get document IDs for SortedIndex rebuild: %v", err)
					return
				}

				// Flatten to single slice
				for _, ids := range docIDsByPage {
					docIDs = append(docIDs, ids...)
				}
				break
			}
		}
	}

	if len(docIDs) == 0 {
		s.logger.Debugf("No documents found for SortedIndex rebuild of bundle %s", bundleName)
		return
	}

	// Rebuild the SortedIndex
	bundle.SortedIndex.RebuildFromDocuments(docIDs)

	// Persist the rebuilt index
	if err := PersistBundleSortedIndex(bundle); err != nil {
		s.logger.Warnf("Failed to persist rebuilt SortedIndex for bundle %s: %v", bundleName, err)
	}

	s.logger.Debugf("Rebuilt SortedIndex for bundle %s with %d documents after compaction",
		bundleName, len(docIDs))

	// Schedule async rebuilds for all user-created indexes (hash and btree)
	// This updates their pageIDs to match the post-compaction document locations
	if bundle.Indexes != nil && s.indexMaintenanceScheduler != nil {
		for indexName, indexRef := range bundle.Indexes {
			// Skip DocumentID index (it was used above)
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				continue
			}

			// Initialize maintenance metadata if needed
			if indexRef.Maintenance == nil {
				indexRef.Maintenance = &models.IndexMaintenanceMetadata{
					IsHealthy:     true,
					LastQueryTime: time.Now(),
				}
				bundle.Indexes[indexName] = indexRef
			}

			// Schedule rebuild with high priority (post-compaction is urgent)
			s.logger.Debugf("Scheduling post-compaction rebuild for index %s (type: %s)", indexName, indexRef.IndexType)

			// Use high priority (staleness=1.0) for post-compaction rebuilds
			_ = s.indexMaintenanceScheduler.ScheduleRebuild(IndexMaintenanceRequest{
				DatabaseName:  bundle.Database.Name,
				BundleName:    bundleName,
				IndexName:     indexName,
				IndexType:     indexRef.IndexType,
				StalenessRate: 1.0, // High priority
				QueryCount:    0,   // Not query-driven, compaction-driven
			})
		}
	}
}

// findDocumentPage uses the DocumentID hash index to determine which page contains a specific document
// This provides O(1) document location lookup instead of scanning all pages
// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.
func (s *BundleService) findDocumentPage(bundleID, documentID string) (uint32, error) {
	// PHASE 5: Check the sharded document-page cache first (O(1) lookup)
	if pageID, found := s.documentPageCache.GetPageID(bundleID, documentID); found {
		s.logger.Debugf("Cache hit: Found document %s in bundle %s at page %d", documentID, bundleID, pageID)
		return pageID, nil
	}

	// Get bundle metadata
	bundle, exists := s.bundleMetadata[bundleID]
	if !exists {
		return 0, fmt.Errorf("bundle metadata not found for %s", bundleID)
	}

	// HYBRID APPROACH: Use DocumentID hash index to get page location
	// This is the proper LSM-based solution that stores page IDs in the index
	if bundle.Indexes != nil {
		// Look for DocumentID index
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Load the DocumentID index
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load DocumentID index for page lookup: %v", err)
					break // Fall through to fallback
				}

				// Get document location from index
				docIDs, pageIDs, err := hashIndex.Get(documentID)
				if err != nil {
					s.logger.Warnf("Failed to query DocumentID index: %v", err)
					break // Fall through to fallback
				}

				if len(docIDs) > 0 && len(pageIDs) > 0 {
					pageID := pageIDs[0]
					s.logger.Debugf("Index lookup: Found document %s in bundle %s at page %d", documentID, bundleID, pageID)

					// Verify the document actually exists on the claimed page.
					// The index PageID can be stale (e.g., 0 as default/placeholder, or
					// outdated after page-boundary shifts from new inserts). If stale,
					// fall through to the page scan so we still find the document.
					verifyPage, verifyErr := s.GetDocumentPage(bundleID, bundle.Database.Name, pageID)
					if verifyErr == nil && verifyPage != nil {
						safePage := s.createSafePageCopy(verifyPage)
						if _, exists := safePage.Documents[documentID]; exists {
							// PHASE 5: Cache the verified result
							s.documentPageCache.SetPageID(bundleID, documentID, pageID)
							return pageID, nil
						}
					}

					// Stale pageID — fall through to page scan below
					s.logger.Warnf("findDocumentPage: DocumentID index has stale pageID %d for document %s in bundle %s, falling through to page scan",
						pageID, documentID, bundleID)
				} else {
					// Hash index returned empty — document may still exist on disk but
					// not yet be in the index. Fall through to page scan instead of
					// returning an error, so we still find the document.

					s.logger.Warnf("findDocumentPage: DocumentID index returned empty for %s in bundle %s, falling through to page scan",
						documentID, bundleID)
				}
			}
		}
	}

	// FALLBACK: Only used if index lookup fails or PageID is 0 (placeholder)
	// Issue 8: Limit scan to avoid O(N) timeouts; operators should fix DocumentID index.
	s.logger.Debugf("FALLBACK: Scanning pages to find document %s in bundle %s", documentID, bundleID)

	if bundle.PageCount == 0 {
		return 0, fmt.Errorf("bundle %s has no pages", bundleID)
	}

	maxToScan := bundle.PageCount
	if maxToScan > findDocumentPageScanLimit {
		maxToScan = findDocumentPageScanLimit
		s.logger.Warnf("findDocumentPage fallback: scanning at most %d pages for document %s in bundle %s; fix DocumentID index to avoid scan", findDocumentPageScanLimit, documentID, bundleID)
	}

	// UNIVERSAL CACHE: Use GetDocumentPage instead of store.LoadDocumentPage to populate shared cache
	for pageID := uint32(0); pageID < uint32(maxToScan); pageID++ {
		page, err := s.GetDocumentPage(bundleID, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d while searching for document %s: %v", pageID, documentID, err)
			continue
		}

		// Check if document exists in this page
		// CONCURRENCY FIX: Get safe copy to access Documents map
		safePage := s.createSafePageCopy(page)
		_, exists := safePage.Documents[documentID]

		if exists {
			// PHASE 5: Cache the result using sharded cache (handles eviction internally)
			s.documentPageCache.SetPageID(bundleID, documentID, pageID)

			return pageID, nil
		}
	}

	if bundle.PageCount > findDocumentPageScanLimit {
		return 0, fmt.Errorf("document %s not found in first %d pages of bundle %s (scan limit; fix DocumentID index)", documentID, findDocumentPageScanLimit, bundleID)
	}
	return 0, fmt.Errorf("document %s not found in any page of bundle %s", documentID, bundleID)
}

// getAllDocumentsForIndexing loads all documents from all pages for index building
// This is a temporary method during the transition to page-based architecture
// snapshotSeq: Optional snapshot sequence for MVCC filtering (0 = no filtering)
// txID: Optional transaction ID for read-your-own-writes (0 = no filtering)
// activeTxIDs: Optional map of active transaction IDs at snapshot time (nil = no filtering)
func (s *BundleService) getAllDocumentsForIndexing(bundleName string, snapshotSeq uint64, txID uint64, activeTxIDs map[uint64]bool) ([]*models.Document, error) {

	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle metadata not found for %s", bundleName)
	}

	// CRITICAL: Clear any per-bundle projection before loading so we get full documents.
	// Projection pushdown (e.g. ORDER BY) sets projection on the storage engine; it is never
	// cleared by BundleAdapter. Without this, readDocumentRange(nil) falls back to
	// getProjectionFieldsForBundle and returns partial docs (e.g. only name, rating, DocumentID),
	// causing WHERE on category/price/stock to fail with "Field does not exist".
	s.SetProjectionFieldsForBundle(bundleName, nil)

	// CRITICAL: Force flush pending metadata updates to ensure PageCount is current
	// This is necessary because document additions schedule deferred metadata updates
	// and SELECT TOP needs accurate PageCount to work correctly

	s.metadataUpdateMutex.RLock()
	needsMetaFlush4338 := len(s.metadataUpdateBuffer) > 0
	s.metadataUpdateMutex.RUnlock()
	if needsMetaFlush4338 {
		s.FlushMetadataUpdates()
	}
	//s.logger.Debugf("Bundle %s memtable state: Documents=%v, DocumentsComplete=%v",
	//	bundleName, bundle.Documents != nil, bundle.DocumentsComplete)
	// if bundle.Documents != nil {
	// 	s.logger.Debugf("Bundle %s memtable contains %d documents", bundleName, len(*bundle.Documents))
	// }

	var allDocuments []*models.Document

	// Special handling: If PageCount is 0, still check page 0 for documents
	// This handles cases where metadata might be out of sync
	if bundle.PageCount == 0 {
		// WRITE-THROUGH CACHE: Snapshot page documents safely
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, 0)
		if err != nil {
			// Page 0 doesn't exist - return empty
			return []*models.Document{}, nil
		}

		// Actually process the documents found in page 0
		for _, doc := range docs {
			docCopy := doc
			// Apply MVCC visibility filter if snapshot is provided
			if snapshotSeq > 0 {
				if !docCopy.IsVisibleToSnapshot(snapshotSeq, txID, activeTxIDs) {
					continue // Skip invisible documents
				}
			}
			allDocuments = append(allDocuments, &docCopy)
		}

		return allDocuments, nil
	}

	// WRITE-THROUGH CACHE: All pages now include recent writes via write-through updates
	// Load all pages from disk/cache (authoritative source)
	// PERFORMANCE: Pre-allocate slice with estimated capacity to avoid repeated allocations
	estimatedDocCount := int(bundle.TotalDocuments)
	if estimatedDocCount <= 0 {
		estimatedDocCount = int(bundle.PageCount) * 100 // Rough estimate: 100 docs per page
	}
	allDocuments = make([]*models.Document, 0, estimatedDocCount)

	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundleName, err)
			continue
		}

		// Convert map to slice - must copy since map values are not pointers
		// This is necessary for thread safety (pages may be evicted from cache)
		// PERFORMANCE: Use append with pre-allocated capacity (more efficient than manual indexing)
		for _, doc := range docs {
			docCopy := doc
			allDocuments = append(allDocuments, &docCopy)
		}
	}

	// Apply MVCC visibility filter if snapshot is provided
	if snapshotSeq > 0 {
		filteredDocuments := make([]*models.Document, 0, len(allDocuments))
		for _, doc := range allDocuments {
			if doc.IsVisibleToSnapshot(snapshotSeq, txID, activeTxIDs) {
				filteredDocuments = append(filteredDocuments, doc)
			}
		}
		allDocuments = filteredDocuments
	}

	return allDocuments, nil
}

// GetAllDocumentsForIndexing is a public wrapper for document scanner integration
// For backward compatibility, calls getAllDocumentsForIndexing without snapshot filtering
func (s *BundleService) GetAllDocumentsForIndexing(bundleName string) ([]*models.Document, error) {
	return s.getAllDocumentsForIndexing(bundleName, 0, 0, nil)
}

// GetDocumentChunksForIndexing streams documents in chunks (page-by-page) to avoid loading the full bundle.
// Used by the join executor for streaming probe. fn is called with each chunk; return false to stop.
// NOTE: Does not merge memtable; streams only persisted pages. Callers that need unflushed writes
// should use GetAllDocumentsForIndexing.
func (s *BundleService) GetDocumentChunksForIndexing(ctx context.Context, bundleName string, chunkSize int, fn func(chunk []*models.Document) (stop bool)) error {
	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return fmt.Errorf("bundle metadata not found for %s", bundleName)
	}
	s.metadataUpdateMutex.RLock()
	needsMetaFlush4436 := len(s.metadataUpdateBuffer) > 0
	s.metadataUpdateMutex.RUnlock()
	if needsMetaFlush4436 {
		s.FlushMetadataUpdates()
	}
	if chunkSize <= 0 {
		chunkSize = 4096
	}

	buffer := make([]*models.Document, 0, chunkSize)
	flush := func() bool {
		if len(buffer) == 0 {
			return true
		}
		chunk := make([]*models.Document, len(buffer))
		copy(chunk, buffer)
		if !fn(chunk) {
			return false
		}
		buffer = buffer[:0]
		return true
	}

	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	pageCount := uint32(bundle.PageCount)
	if pageCount == 0 {
		pageCount = 1
	}

	for pageID := uint32(0); pageID < pageCount; pageID++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundleName, err)
			continue
		}
		for _, doc := range docs {
			docCopy := doc
			buffer = append(buffer, &docCopy)
			if len(buffer) >= chunkSize {
				if !flush() {
					return nil
				}
			}
		}
	}
	if len(buffer) > 0 {
		flush()
	}
	return nil
}

// IndexingOptions configures streaming filter and parallel page loading for GetAllDocumentsForIndexingWithOptions.
// - Filter: if non-nil, only documents for which Filter(doc) is true are included (streaming filter-while-loading).
// - Concurrency: 1 = sequential; 0 = use default (4); otherwise min(Concurrency, NumCPU, 8) workers.
type IndexingOptions struct {
	Filter      func(*models.Document) bool // optional; nil means no filter
	Concurrency int                         // 1=sequential, 0=default 4, else min(Concurrency, NumCPU, 8)
}

// defaultIndexingConcurrency is the default number of parallel page-load workers when opts.Concurrency is 0.
const defaultIndexingConcurrency = 4

// maxIndexingConcurrency caps parallel workers to avoid I/O thrashing (e.g. on HDD).
const maxIndexingConcurrency = 8

// GetAllDocumentsForIndexingWithOptions supports streaming filter and parallel page loading.
// When opts is nil, delegates to GetAllDocumentsForIndexing (sequential, no filter).
// Safeguards: Concurrency is capped at min(opt, runtime.NumCPU(), 8). Use Concurrency=1 to force sequential.
func (s *BundleService) GetAllDocumentsForIndexingWithOptions(bundleName string, opts *IndexingOptions) ([]*models.Document, error) {
	if opts == nil {
		return s.GetAllDocumentsForIndexing(bundleName)
	}

	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle metadata not found for %s", bundleName)
	}

	s.metadataUpdateMutex.RLock()
	needsMetaFlush4520 := len(s.metadataUpdateBuffer) > 0
	s.metadataUpdateMutex.RUnlock()
	if needsMetaFlush4520 {
		s.FlushMetadataUpdates()
	}

	concurrency := opts.Concurrency
	if concurrency == 0 {
		concurrency = defaultIndexingConcurrency
	}
	if n := runtime.NumCPU(); concurrency > n {
		concurrency = n
	}
	if concurrency > maxIndexingConcurrency {
		concurrency = maxIndexingConcurrency
	}
	if concurrency < 1 {
		concurrency = 1
	}

	filter := opts.Filter

	// --- PageCount == 0 (reuse existing special-case structure, with filter) ---
	// WRITE-THROUGH CACHE: Use GetDocumentPage which now includes all recent writes
	if bundle.PageCount == 0 {
		var allDocuments []*models.Document
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, 0)
		if err != nil {
			// Page 0 doesn't exist - return empty
			return []*models.Document{}, nil
		}
		for _, doc := range docs {
			docCopy := doc
			if filter != nil && !filter(&docCopy) {
				continue
			}
			allDocuments = append(allDocuments, &docCopy)
		}
		return allDocuments, nil
	}

	pageCount := uint32(bundle.PageCount)

	// --- Sequential: load each page, filter, append ---
	// WRITE-THROUGH CACHE: All pages now include recent writes via write-through updates
	if concurrency <= 1 {
		var allDocuments []*models.Document
		for pageID := uint32(0); pageID < pageCount; pageID++ {
			docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
			if err != nil {
				s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundleName, err)
				continue
			}
			for _, doc := range docs {
				docCopy := doc
				if filter != nil && !filter(&docCopy) {
					continue
				}
				allDocuments = append(allDocuments, &docCopy)
			}
		}
		return allDocuments, nil
	}

	// --- Parallel: workers load page ranges, filter, send batches; main collects ---
	// WRITE-THROUGH CACHE: All pages now include recent writes via write-through updates
	type batch struct {
		docs []*models.Document
	}
	ch := make(chan batch, concurrency)
	var wg sync.WaitGroup

	partition := (int(pageCount) + concurrency - 1) / concurrency
	for w := 0; w < concurrency; w++ {
		start := w * partition
		end := start + partition
		if start >= int(pageCount) {
			break
		}
		if end > int(pageCount) {
			end = int(pageCount)
		}
		wg.Add(1)
		go func(pageStart, pageEnd int) {
			defer wg.Done()
			var local []*models.Document
			// WRITE-THROUGH CACHE: Use SnapshotPageDocuments which handles locking internally
			for pageID := uint32(pageStart); pageID < uint32(pageEnd); pageID++ {
				docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
				if err != nil {
					s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundleName, err)
					continue
				}

				// Iterate over snapshot safely (no manual locking needed)
				for _, doc := range docs {
					docCopy := doc
					if filter != nil && !filter(&docCopy) {
						continue
					}
					local = append(local, &docCopy)
				}
			}
			ch <- batch{docs: local}
		}(start, end)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var allDocuments []*models.Document
	for b := range ch {
		allDocuments = append(allDocuments, b.docs...)
	}

	return allDocuments, nil
}

// mergeMemtableWithFilter - DEPRECATED: Write-through cache makes this unnecessary
// Kept as no-op for any remaining callers; returns diskDocs unchanged
func (s *BundleService) mergeMemtableWithFilter(bundle *models.Bundle, diskDocs []*models.Document, filter func(*models.Document) bool) []*models.Document {
	// WRITE-THROUGH CACHE: All recent writes are now in the page cache
	// No memtable merge needed - just return diskDocs
	return diskDocs
}

func (s *BundleService) LoadDocumentPage(bundleName, databaseName string, pageID uint32, databasePath string) (*models.DocumentPage, error) {
	// Load the specified document page from the store
	return s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
}

// SetProjectionFieldsForBundle sets projection fields temporarily for a bundle
// PROJECTION PUSHDOWN: This allows BundleAdapter to pass projection through to readDocumentRange
// For ORDER BY queries, this saves ~80-90% deserialization overhead (e.g., only deserialize "name" field)
// Called from BundleAdapter before loading pages for ORDER BY queries
func (s *BundleService) SetProjectionFieldsForBundle(bundleName string, fields []string) {
	// Type assert store to BundleStorageEngine to access SetProjectionFieldsForBundle
	// PROJECTION PUSHDOWN: Pass projection through to storage engine for ORDER BY optimization
	if storageEngine, ok := s.store.(*bundlestore.BundleStorageEngine); ok {
		storageEngine.SetProjectionFieldsForBundle(bundleName, fields)
		if len(fields) > 0 {
			s.logger.Debugf("PROJECTION PUSHDOWN: Set projection fields %v for bundle '%s' via BundleService", fields, bundleName)
		}
	}
	// If store is not BundleStorageEngine (unlikely), projection is silently ignored
	// This is safe because projection is an optimization, not a correctness requirement
}

func (s *BundleService) LoadCatalogBundleDocuments(bundleName string) ([]*models.Document, error) {
	// Load all documents for the specified catalog bundle
	return s.getAllDocumentsForIndexing(bundleName, 0, 0, nil)
}

// simpleHash provides a basic hash function for document ID to page mapping
func (s *BundleService) simpleHash(input string) uint64 {
	hash := uint64(0)
	for _, c := range input {
		hash = hash*31 + uint64(c)
	}
	return hash
}

// DEPRECATED: GetBundleByName - replaced with GetBundleMetadata
// This method is kept temporarily for backward compatibility but should not load all documents
func (s *BundleService) GetBundleByName(database *models.Database, name string) (*models.Bundle, error) {
	// First, get the bundle metadata
	bundle, err := s.GetBundleMetadata(database, name)
	if err != nil {
		return nil, err
	}

	// Return metadata-only bundle - documents should be loaded on-demand via GetDocumentPage
	// WRITE-THROUGH CACHE: All document access goes through page cache now
	// The DocumentsComplete flag is no longer needed since we don't have a memtable
	bundle.DocumentsComplete = true // Always "complete" - all data in page cache

	return bundle, nil
}

func (s *BundleService) GetAllBundles() map[string]*models.Bundle {
	return s.bundleMetadata
}

func (s *BundleService) RemoveBundle(db *models.Database, name string) error {
	// Check if the bundle exists in metadata
	bundle, exists := s.bundleMetadata[name]
	if !exists {
		return errors.New(errors.ERR_NOT_FOUND_BUNDLE, fmt.Sprintf("Bundle '%s' not found", name), errors.LayerDomain)
	}

	// Remove the bundle from the store
	err := s.store.RemoveBundleFile(db, bundle.Name)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Failed to remove bundle '%s' from store", name), errors.LayerStorage).WithContext("bundle_name", name)
	}

	// Remove from metadata
	delete(s.bundleMetadata, name)

	// Remove any loaded document pages for this bundle from all shards
	prefix := name + ":"
	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		shard.mu.Lock()
		keysToDelete := make([]string, 0, 8)
		for pageKey := range shard.pages {
			if strings.HasPrefix(pageKey, prefix) {
				keysToDelete = append(keysToDelete, pageKey)
			}
		}
		for _, key := range keysToDelete {
			// Remove from LRU tracking
			if elem, exists := shard.lruElements[key]; exists {
				shard.lruOrder.Remove(elem)
				delete(shard.lruElements, key)
			}
			shard.deleteLocked(key)
		}
		shard.mu.Unlock()
	}

	return nil
}

func (s *BundleService) UpdateBundle(db *models.Database, bundleCommand models.BundleCommand) error {
	// Check if the bundle exists
	bundle, err := s.GetBundleByName(db, bundleCommand.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", bundleCommand.BundleName)
	}

	// Update the bundle in the store
	err = s.store.UpdateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("failed to update bundle in store: %w", err)
	}

	return nil
}

// RenameBundle renames a bundle and updates all related files and database entries.
// It validates the new name, updates the bundle metadata file, renames the directory,
// and updates the entry in the primary database's Bundles bundle.
func (s *BundleService) RenameBundle(database *models.Database, bundle *models.Bundle, newBundleName string) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if newBundleName == "" {
		return fmt.Errorf("new bundle name cannot be empty")
	}
	if bundle.Name == newBundleName {
		return fmt.Errorf("new bundle name is the same as current name")
	}

	oldName := bundle.Name

	// Validate new bundle name follows naming rules
	if err := s.validateBundleName(newBundleName); err != nil {
		return fmt.Errorf("invalid bundle name '%s': %w", newBundleName, err)
	}

	// Check that new name doesn't already exist
	existingBundle, _ := s.GetBundleByName(database, newBundleName)
	if existingBundle != nil {
		return fmt.Errorf("bundle with name '%s' already exists in database '%s'", newBundleName, database.Name)
	}

	// OPERATION SAFETY: Wait for all active operations to complete before renaming
	// This prevents data corruption or inconsistencies during the rename process.
	// The timeout prevents indefinite waits in case of stuck operations.
	lock := s.getBundleLock(oldName)

	// TODO: Make timeout configurable via settings (currently 30 seconds)
	timeout := 30 * time.Second

	if err := lock.WaitForActiveOperations(timeout); err != nil {
		return fmt.Errorf("cannot rename bundle while operations are active: %w", err)
	}

	// Ensure we clear the rename flag even if the operation fails
	defer lock.CompleteAdministrativeOperation()

	// Get the bundle's current directory path
	databasePath := helpers.GetDatabaseFolderPath(database.Name)
	oldBundlePath := filepath.Join(databasePath, oldName)
	newBundlePath := filepath.Join(databasePath, newBundleName)

	// Verify old directory exists
	if _, err := os.Stat(oldBundlePath); os.IsNotExist(err) {
		return fmt.Errorf("bundle directory does not exist: %s", oldBundlePath)
	}

	// Update bundle metadata
	bundle.Name = newBundleName
	bundle.UpdatedAt = time.Now()

	// Rename the bundle directory (this includes indexes subfolder)
	if err := os.Rename(oldBundlePath, newBundlePath); err != nil {
		return fmt.Errorf("failed to rename bundle directory: %w", err)
	}

	// Update the bundle metadata file with new name
	if err := s.store.UpdateBundleFilename(database, bundle, oldName); err != nil {
		// Try to rollback directory rename
		if rollbackErr := os.Rename(newBundlePath, oldBundlePath); rollbackErr != nil {
			s.logger.Errorf("Failed to rollback directory rename: %v", rollbackErr)
		}
		bundle.Name = oldName // Restore old name in memory
		return fmt.Errorf("failed to update bundle metadata file: %w", err)
	}

	// Update the cache
	delete(s.bundleMetadata, oldName)
	s.bundleMetadata[newBundleName] = bundle

	// Invalidate page cache for old name
	s.invalidateBundlePageCache(oldName)

	// Update entry in primary database's "Bundles" bundle
	// This updates the system catalog that tracks all bundles
	if err := s.updateBundleInSystemCatalog(database, oldName, newBundleName); err != nil {
		s.logger.Warnf("Failed to update system catalog for bundle rename: %v", err)
		// Don't fail the operation - the bundle is already renamed on disk
	}

	s.logger.Debugf("Successfully renamed bundle '%s' to '%s'", oldName, newBundleName)
	return nil
}

// validateBundleName validates that a bundle name follows the naming rules
func (s *BundleService) validateBundleName(name string) error {
	if name == "" {
		return fmt.Errorf("bundle name cannot be empty")
	}

	// Check for reserved _mv_ prefix (reserved for materialized views)
	if strings.HasPrefix(name, "_mv_") {
		return fmt.Errorf("bundle names cannot contain '_mv_' prefix (reserved for materialized views). Please choose a different bundle name")
	}

	// Bundle names should start with a letter and contain only alphanumeric characters and underscores
	if len(name) == 0 {
		return fmt.Errorf("bundle name cannot be empty")
	}

	// Check first character is a letter
	firstChar := rune(name[0])
	if !((firstChar >= 'a' && firstChar <= 'z') || (firstChar >= 'A' && firstChar <= 'Z')) {
		return fmt.Errorf("bundle name must start with a letter")
	}

	// Check remaining characters are alphanumeric or underscore
	for _, char := range name {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_') {
			return fmt.Errorf("bundle name can only contain letters, numbers, and underscores")
		}
	}

	return nil
}

// updateBundleInSystemCatalog updates the bundle entry in the primary database's Bundles bundle
func (s *BundleService) updateBundleInSystemCatalog(database *models.Database, oldName, newName string) error {
	// Check if catalog service is available (injected via SetCatalogService)
	if s.catalogService == nil {
		s.logger.Warnf("Catalog service not available, skipping system catalog update for bundle rename")
		return fmt.Errorf("catalog service not initialized")
	}

	// Get the bundle to find its BundleID
	bundle, err := s.GetBundleByName(database, newName)
	if err != nil {
		return fmt.Errorf("failed to get bundle after rename: %w", err)
	}

	// Update the catalog with the new bundle name
	if err := s.catalogService.UpdateBundleNameInCatalog(
		bundle.BundleID,
		database.Name,
		oldName,
		newName,
	); err != nil {
		return fmt.Errorf("failed to update bundle in system catalog: %w", err)
	}

	s.logger.Debugf("Updated system catalog for bundle rename: '%s' -> '%s'", oldName, newName)

	// GRAPHQL INTEGRATION: Regenerate GraphQL schema after bundle rename
	// Bundle rename changes the GraphQL TypeName (e.g., "users" -> "User", "blog_posts" -> "BlogPost")
	// This creates a new schema version with the updated type name.
	//
	// This reuses the regenerateGraphQLSchema method for consistency.
	// Schema update failures are logged but don't fail the rename operation.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema after rename to '%s': %v. Rename was successful.",
			newName, err)
	}

	return nil
}

// ApplyFieldChanges applies ADD/REMOVE/MODIFY field operations to a bundle.
// It validates constraints, performs type conversions, and rebuilds indexes as needed.
// This method handles the actual schema modification for UPDATE BUNDLE commands.
func (s *BundleService) ApplyFieldChanges(database *models.Database, bundle *models.Bundle, changes []models.FieldChange) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if len(changes) == 0 {
		return fmt.Errorf("no field changes specified")
	}

	// Track which indexes need rebuilding
	indexesToRebuild := make(map[string]bool)

	// Apply each field change
	for _, change := range changes {
		switch change.ChangeType {
		case "ADD":
			if err := s.applyAddField(bundle, &change); err != nil {
				return fmt.Errorf("failed to add field '%s': %w", change.NewField.Name, err)
			}

		case "REMOVE":
			fieldName := change.OldFieldName
			if fieldName == "" {
				fieldName = change.NewField.Name
			}
			if err := s.applyRemoveField(database, bundle, fieldName); err != nil {
				return fmt.Errorf("failed to remove field '%s': %w", fieldName, err)
			}
			// Indexes on this field are removed and their files deleted in applyRemoveField; no rebuild

		case "MODIFY":
			if err := s.applyModifyField(bundle, &change); err != nil {
				return fmt.Errorf("failed to modify field '%s': %w", change.OldFieldName, err)
			}
			// Track if old or new field is indexed (for renames)
			if s.isFieldIndexed(bundle, change.OldFieldName) {
				indexesToRebuild[change.OldFieldName] = true
			}
			if change.OldFieldName != change.NewField.Name && s.isFieldIndexed(bundle, change.NewField.Name) {
				indexesToRebuild[change.NewField.Name] = true
			}

			// Log appropriate message based on whether it's a rename or just a modification
			if change.OldFieldName != change.NewField.Name {
				s.logger.Debugf("Renamed and modified field '%s' to '%s' in bundle '%s'",
					change.OldFieldName, change.NewField.Name, bundle.Name)
			}

		default:
			return fmt.Errorf("unsupported change type: %s", change.ChangeType)
		}
	}

	// Invalidate plan cache for schema changes (field additions/removals/modifications)
	// This ensures queries use fresh plans reflecting the new schema
	s.invalidatePlanCacheForBundle(bundle.Name)

	// Rebuild affected indexes
	if len(indexesToRebuild) > 0 {
		//s.logger.Debugf("Rebuilding %d indexes for bundle '%s'", len(indexesToRebuild), bundle.Name)
		for fieldName := range indexesToRebuild {
			if err := s.rebuildFieldIndex(bundle, fieldName); err != nil {
				s.logger.Warnf("Failed to rebuild index for field '%s': %v", fieldName, err)
				// Don't fail the entire operation if index rebuild fails
			}
		}
	}

	// Persist bundle metadata changes
	err := s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		return fmt.Errorf("failed to persist bundle changes: %w", err)
	}

	// Update the cache with the modified bundle
	s.bundleMetadata[bundle.Name] = bundle

	// FR-6 GRAPHQL INTEGRATION: Regenerate GraphQL schema after bundle structure changes
	// This reuses the regenerateGraphQLSchema method which handles:
	// - Breaking change detection (field removals, type changes, nullability changes)
	// - Schema versioning (new version creation + old version tombstoning)
	// - Cache updates for immediate availability
	//
	// Schema update failures are logged but don't fail the field change operation.
	// This ensures bundle modifications succeed even if GraphQL clients may need updates.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema for bundle '%s': %v. Field changes were applied successfully.",
			bundle.Name, err)
	}

	s.logger.Debugf("Successfully applied all field changes to bundle '%s'", bundle.Name)
	return nil
}

// applyAddField adds a new field to the bundle schema and existing documents
func (s *BundleService) applyAddField(bundle *models.Bundle, change *models.FieldChange) error {
	fieldName := change.NewField.Name

	// Validate field doesn't already exist
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; exists {
		return fmt.Errorf("field '%s' already exists in bundle '%s'", fieldName, bundle.Name)
	}

	// If required, must have default value
	if change.NewField.IsRequired && change.NewField.DefaultValue == nil {
		return fmt.Errorf("cannot add required field '%s' without default value", fieldName)
	}

	// Add field to schema
	if bundle.DocumentStructure.FieldDefinitions == nil {
		bundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}
	bundle.DocumentStructure.FieldDefinitions[fieldName] = change.NewField

	// Apply default value to all existing documents if field is required
	if change.NewField.IsRequired && change.NewField.DefaultValue != nil {
		//s.logger.Debugf("Applying default value to all existing documents in bundle '%s'", bundle.Name)
		if err := s.applyDefaultToExistingDocuments(bundle, fieldName, change.NewField.DefaultValue); err != nil {
			return fmt.Errorf("failed to apply default value to existing documents: %w", err)
		}
	}

	return nil
}

// applyRemoveField removes a field from the bundle schema and all documents.
// Indexes on this field are removed from bundle.Indexes and their files deleted from disk.
func (s *BundleService) applyRemoveField(database *models.Database, bundle *models.Bundle, fieldName string) error {
	// Validate field exists
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", fieldName, bundle.Name)
	}

	// Cannot remove DocumentID field
	if fieldName == "DocumentID" {
		return fmt.Errorf("cannot remove system field 'DocumentID'")
	}

	// === VALIDATE REFERENTIAL INTEGRITY ===
	// Check if this field is used in any relationships (both directions)
	bundleCache := make(map[string]*models.Bundle)
	validator := NewReferentialIntegrityValidator(s, s.logger)
	violation := validator.ValidateFieldRemoval(database, bundle, fieldName, bundleCache)
	if violation != nil {
		s.logger.Warnf("[REFINT] %s | Suggested: %s", violation.Error(), violation.SuggestedAction)
		return fmt.Errorf("%s", violation.Error())
	}

	// Remove indexes on this field from bundle.Indexes and delete their files from disk
	if bundle.Indexes != nil && database != nil {
		var toRemove []string
		for indexName, indexRef := range bundle.Indexes {
			match := indexRef.BTreeIndexField.FieldName == fieldName ||
				indexRef.HashIndexField.FieldName == fieldName
			for _, f := range indexRef.Fields {
				if f.Name == fieldName {
					match = true
					break
				}
			}
			if match {
				toRemove = append(toRemove, indexName)
			}
		}
		indexesPath := filepath.Join(database.DataDirectory, database.Name, bundle.Name, "indexes")
		for _, indexName := range toRemove {
			ir := bundle.Indexes[indexName]
			_ = DeleteIndexFiles(indexesPath, indexName, ir.IndexType, s.logger)
			delete(bundle.Indexes, indexName)
			// Remove from IndexNames
			for i, n := range bundle.IndexNames {
				if n == indexName {
					bundle.IndexNames = append(bundle.IndexNames[:i], bundle.IndexNames[i+1:]...)
					break
				}
			}
			// Remove from sharded index cache (thread-safe)
			s.loadedIndexes.Delete(bundle.Name, indexName)
		}
	}

	// Remove from schema
	delete(bundle.DocumentStructure.FieldDefinitions, fieldName)

	// Remove field from all existing documents
	//s.logger.Debugf("Removing field '%s' from all documents in bundle '%s'", fieldName, bundle.Name)
	if err := s.removeFieldFromExistingDocuments(bundle, fieldName); err != nil {
		return fmt.Errorf("failed to remove field from existing documents: %w", err)
	}

	return nil
}

// applyModifyField modifies an existing field's properties
func (s *BundleService) applyModifyField(bundle *models.Bundle, change *models.FieldChange) error {
	oldFieldName := change.OldFieldName
	newFieldName := change.NewField.Name
	isRenaming := oldFieldName != newFieldName

	// Validate old field exists
	oldField, exists := bundle.DocumentStructure.FieldDefinitions[oldFieldName]
	if !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", oldFieldName, bundle.Name)
	}

	// Cannot rename system fields
	if isRenaming && oldFieldName == "DocumentID" {
		return fmt.Errorf("cannot rename system field 'DocumentID'")
	}

	// If renaming, validate new field name doesn't already exist
	if isRenaming {
		if _, exists := bundle.DocumentStructure.FieldDefinitions[newFieldName]; exists {
			return fmt.Errorf("cannot rename field '%s' to '%s': target field name already exists", oldFieldName, newFieldName)
		}

		// === VALIDATE REFERENTIAL INTEGRITY ===
		// Check if this field rename would break any relationships
		bundleCache := make(map[string]*models.Bundle)
		validator := NewReferentialIntegrityValidator(s, s.logger)
		violation := validator.ValidateFieldRename(nil, bundle, oldFieldName, newFieldName, bundleCache)
		if violation != nil {
			s.logger.Warnf("[REFINT] %s | Suggested: %s", violation.Error(), violation.SuggestedAction)
			return fmt.Errorf("%s", violation.Error())
		}

		s.logger.Debugf("Renaming field '%s' to '%s' in bundle '%s'", oldFieldName, newFieldName, bundle.Name)
	}

	// If type is changing, validate conversion is possible
	if oldField.Type != change.NewField.Type {
		s.logger.Debugf("Attempting type conversion for field '%s' from %s to %s",
			oldFieldName, oldField.Type, change.NewField.Type)
		if err := s.convertFieldType(bundle, oldFieldName, oldField.Type, change.NewField.Type); err != nil {
			return fmt.Errorf("cannot convert field '%s' from %s to %s - manual migration required: %w",
				oldFieldName, oldField.Type, change.NewField.Type, err)
		}
	}

	// If adding IsUnique constraint, validate no duplicates exist
	if !oldField.IsUnique && change.NewField.IsUnique {
		s.logger.Debugf("Validating uniqueness for field '%s'", oldFieldName)
		if err := s.validateFieldUniqueness(bundle, oldFieldName); err != nil {
			return err
		}
	}

	// If making field required, ensure it has a default or all documents have values
	if !oldField.IsRequired && change.NewField.IsRequired {
		if change.NewField.DefaultValue == nil {
			// Check if all documents already have this field
			if err := s.validateAllDocumentsHaveField(bundle, oldFieldName); err != nil {
				return fmt.Errorf("cannot make field '%s' required: %w. Provide a default value or ensure all documents have this field", oldFieldName, err)
			}
		} else {
			// Apply default to documents missing this field
			if err := s.applyDefaultToMissingField(bundle, oldFieldName, change.NewField.DefaultValue); err != nil {
				return fmt.Errorf("failed to apply default value: %w", err)
			}
		}
	}

	// If renaming, rename field in all documents
	if isRenaming {
		if err := s.renameFieldInDocuments(bundle, oldFieldName, newFieldName); err != nil {
			return fmt.Errorf("failed to rename field in documents: %w", err)
		}
	}

	// Update schema: remove old field and add new field definition
	if isRenaming {
		delete(bundle.DocumentStructure.FieldDefinitions, oldFieldName)
	}
	bundle.DocumentStructure.FieldDefinitions[newFieldName] = change.NewField

	return nil
}

// applyDefaultToExistingDocuments adds a field with default value to all documents
func (s *BundleService) applyDefaultToExistingDocuments(bundle *models.Bundle, fieldName string, defaultValue interface{}) error {
	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for _, doc := range docs {
			// Add field with default value if it doesn't exist
			if doc.Fields == nil {
				doc.Fields = make(map[string]models.Field)
			}

			if _, hasField := doc.Fields[fieldName]; !hasField {
				// Evaluate default value (supports Expression or literal)
				evaluatedValue, err := s.evaluateDefaultValue(defaultValue, &doc)
				if err != nil {
					return fmt.Errorf("failed to evaluate default value for field '%s': %w", fieldName, err)
				}

				doc.Fields[fieldName] = models.Field{
					Name:  fieldName,
					Value: models.NewInterfaceValue(evaluatedValue),
				}
				doc.CachedJSON = nil // Force lazy rebuild on next read

				// Update the document in the bundle file
				err = s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// removeFieldFromExistingDocuments removes a field from all documents
func (s *BundleService) removeFieldFromExistingDocuments(bundle *models.Bundle, fieldName string) error {
	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for _, doc := range docs {
			if doc.Fields == nil {
				continue
			}

			if _, hasField := doc.Fields[fieldName]; hasField {
				delete(doc.Fields, fieldName)
				doc.CachedJSON = nil // Force lazy rebuild on next read

				// Update the document in the bundle file
				err := s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// renameFieldInDocuments renames a field in all documents
func (s *BundleService) renameFieldInDocuments(bundle *models.Bundle, oldFieldName, newFieldName string) error {
	s.logger.Debugf("Renaming field '%s' to '%s' in all documents of bundle '%s'", oldFieldName, newFieldName, bundle.Name)

	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for _, doc := range docs {
			if doc.Fields == nil {
				continue
			}

			// Check if old field exists
			if oldFieldValue, hasField := doc.Fields[oldFieldName]; hasField {
				// Copy the field with new name
				doc.Fields[newFieldName] = models.Field{
					Name:  newFieldName,
					Value: oldFieldValue.Value,
				}

				// Remove old field
				delete(doc.Fields, oldFieldName)
				doc.CachedJSON = nil // Force lazy rebuild on next read

				// Update the document in the bundle file
				err := s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	s.logger.Debugf("Successfully renamed field '%s' to '%s' in bundle '%s'", oldFieldName, newFieldName, bundle.Name)
	return nil
}

// convertFieldType attempts to convert all values of a field to a new type
func (s *BundleService) convertFieldType(bundle *models.Bundle, fieldName, fromType, toType string) error {
	conversionErrors := []string{}

	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for _, doc := range docs {
			if doc.Fields == nil {
				continue
			}

			field, hasField := doc.Fields[fieldName]
			if !hasField {
				continue
			}

			// Attempt conversion
			convertedValue, err := s.convertValue(field.Value, fromType, toType)
			if err != nil {
				conversionErrors = append(conversionErrors,
					fmt.Sprintf("doc %s: %v", doc.DocumentID, err))
				continue
			}

			// Update field value
			field.Value = models.NewInterfaceValue(convertedValue) // ✅ Use NewInterfaceValue
			doc.Fields[fieldName] = field
			doc.CachedJSON = nil // Force lazy rebuild on next read

			// Persist the change
			err = s.store.UpdateDocumentInBundleFile(bundle, &doc)
			if err != nil {
				return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
			}
		}
	}

	if len(conversionErrors) > 0 {
		return fmt.Errorf("conversion failed for %d documents: %v", len(conversionErrors), conversionErrors[:min(5, len(conversionErrors))])
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// convertValue attempts to convert a single value from one type to another
func (s *BundleService) convertValue(value interface{}, fromType, toType string) (interface{}, error) {
	// Handle nil values
	if value == nil {
		return nil, nil
	}

	switch toType {
	case "string":
		return conversion.ValueToString(value), nil

	case "int":
		switch v := value.(type) {
		case int, int32, int64:
			return v, nil
		case float32, float64:
			return int(v.(float64)), nil
		case string:
			return strconv.Atoi(v)
		default:
			return nil, fmt.Errorf("cannot convert %T to int", value)
		}

	case "float":
		switch v := value.(type) {
		case float32, float64:
			return v, nil
		case int, int32, int64:
			return float64(v.(int)), nil
		case string:
			return strconv.ParseFloat(v, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to float", value)
		}

	case "bool":
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		case int, int32, int64:
			return v.(int) != 0, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to bool", value)
		}

	default:
		return nil, fmt.Errorf("unsupported target type: %s", toType)
	}
}

// validateFieldUniqueness checks that all values for a field are unique
func (s *BundleService) validateFieldUniqueness(bundle *models.Bundle, fieldName string) error {
	valuesSeen := make(map[string][]string) // value -> []documentIDs

	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		for _, doc := range docs {
			if doc.Fields == nil {
				continue
			}

			field, hasField := doc.Fields[fieldName]
			if !hasField {
				continue
			}

			// Convert to string for comparison (simple approach)
			valueKey := conversion.ValueToString(field.Value)
			valuesSeen[valueKey] = append(valuesSeen[valueKey], doc.DocumentID)
		}
	}

	// Check for duplicates
	duplicates := []string{}
	for value, docIDs := range valuesSeen {
		if len(docIDs) > 1 {
			duplicates = append(duplicates, fmt.Sprintf("%v (in docs: %v)", value, docIDs[:min(3, len(docIDs))]))
		}
	}

	if len(duplicates) > 0 {
		return fmt.Errorf("cannot add IsUnique to field '%s' - duplicate values exist: %v",
			fieldName, duplicates[:min(5, len(duplicates))])
	}

	return nil
}

// validateAllDocumentsHaveField checks that all documents have a non-nil value for a field
func (s *BundleService) validateAllDocumentsHaveField(bundle *models.Bundle, fieldName string) error {
	missingCount := 0

	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		for _, doc := range docs {
			if doc.Fields == nil {
				missingCount++
				continue
			}

			field, hasField := doc.Fields[fieldName]
			if !hasField || field.Value.IsNil() { // ✅ Use IsNil()
				missingCount++
			}
		}
	}

	if missingCount > 0 {
		return fmt.Errorf("%d documents are missing field '%s'", missingCount, fieldName)
	}

	return nil
}

// applyDefaultToMissingField adds default value to documents missing a field
func (s *BundleService) applyDefaultToMissingField(bundle *models.Bundle, fieldName string, defaultValue interface{}) error {
	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for _, doc := range docs {
			if doc.Fields == nil {
				doc.Fields = make(map[string]models.Field)
			}

			field, hasField := doc.Fields[fieldName]
			if !hasField || field.Value.IsNil() { // ✅ Use IsNil()
				// Evaluate default value (supports Expression or literal)
				evaluatedValue, err := s.evaluateDefaultValue(defaultValue, &doc)
				if err != nil {
					return fmt.Errorf("failed to evaluate default value for field '%s': %w", fieldName, err)
				}

				doc.Fields[fieldName] = models.Field{
					Name:  fieldName,
					Value: models.NewInterfaceValue(evaluatedValue),
				}
				doc.CachedJSON = nil // Force lazy rebuild on next read

				// Persist the change
				err = s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// evaluateDefaultValue evaluates a default value (supports Expression or literal)
func (s *BundleService) evaluateDefaultValue(defaultValue interface{}, doc *models.Document) (interface{}, error) {
	// Check if default value is an Expression (function call)
	if expr, isExpr := defaultValue.(syndrQL.Expression); isExpr {
		// Create evaluator for expression evaluation
		evaluator := syndrQL.NewExpressionEvaluator(s.logger)

		// Use the provided document for evaluation context
		// Field references will work if the field already exists in doc
		// Function calls like F:NOW() don't need document fields
		result, err := evaluator.Evaluate(expr, doc, nil, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("expression evaluation failed: %w", err)
		}

		// Result is already interface{}, return as-is
		return result, nil
	}

	// Literal value - return as-is
	return defaultValue, nil
}

// isFieldIndexed checks if a field has an index
func (s *BundleService) isFieldIndexed(bundle *models.Bundle, fieldName string) bool {
	if bundle.Indexes == nil {
		return false
	}

	// Check if any index references this field
	for _, index := range bundle.Indexes {
		// Check BTreeIndexField
		if index.BTreeIndexField.FieldName == fieldName {
			return true
		}
		// Check HashIndexField
		if index.HashIndexField.FieldName == fieldName {
			return true
		}
		// Check Fields array
		for _, field := range index.Fields {
			if field.Name == fieldName {
				return true
			}
		}
	}

	// DocumentID always has hash index
	return fieldName == "DocumentID"
}

// rebuildFieldIndex rebuilds the index for a specific field
// NOTE: For now, this is a placeholder. Full index rebuilding requires:
// 1. Access to index manager to close/reinitialize indexes
// 2. Knowledge of index storage paths
// 3. Proper handling of different index types (BTree, Hash)
// This is a complex operation that should be implemented when index
// management is refactored to be more modular.
func (s *BundleService) rebuildFieldIndex(bundle *models.Bundle, fieldName string) error {
	s.logger.Warnf("Index rebuilding for field '%s' in bundle '%s' not yet fully implemented", fieldName, bundle.Name)
	s.logger.Debugf("Indexes will be rebuilt on next server restart or when accessed")

	// TODO: Implement full index rebuilding
	// For now, we log a warning. Indexes will be rebuilt when:
	// 1. Server restarts and reloads bundles
	// 2. Index is accessed and found to be stale/corrupted
	// 3. Manual reindex command is run

	return nil
}

// invalidateBundlePageCache invalidates all cached pages for a bundle
func (s *BundleService) invalidateBundlePageCache(bundleName string) {
	prefix := bundleName + ":"
	totalDeleted := 0
	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		shard.mu.Lock()
		keysToDelete := make([]string, 0, 8)
		for pageKey := range shard.pages {
			if strings.HasPrefix(pageKey, prefix) {
				keysToDelete = append(keysToDelete, pageKey)
			}
		}
		for _, key := range keysToDelete {
			// Remove from LRU tracking
			if elem, exists := shard.lruElements[key]; exists {
				shard.lruOrder.Remove(elem)
				delete(shard.lruElements, key)
			}
			shard.deleteLocked(key)
		}
		totalDeleted += len(keysToDelete)
		shard.mu.Unlock()
	}
	s.logger.Debugf("Invalidated %d cached pages for bundle '%s'", totalDeleted, bundleName)

	// Clear entire visibility map when full page cache is invalidated
	s.clearVisibilityForBundle(bundleName)
}

// invalidateDocumentPagesForInsert invalidates only the affected page(s) after an INSERT
// UNIVERSAL CACHE: Instead of removing the entire scanner, we invalidate only the page where
// the new document was inserted (and optionally the last few pages to handle edge cases).
// This preserves cache for other pages and avoids cold scanner on every INSERT.
func (s *BundleService) invalidateDocumentPagesForInsert(bundleName string, pageID uint32) {
	// Invalidate the page where the document was inserted
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	shard.mu.Lock()
	if _, exists := shard.pages[pageKey]; exists {
		// Remove from LRU tracking
		if elem, exists := shard.lruElements[pageKey]; exists {
			shard.lruOrder.Remove(elem)
			delete(shard.lruElements, pageKey)
		}
		shard.deleteLocked(pageKey)
		s.logger.Debugf("Invalidated page %d in documentPages cache for bundle '%s' after INSERT", pageID, bundleName)
	}
	shard.mu.Unlock()

	// Clear visibility map bit for this page (page may contain uncommitted docs now)
	s.clearVisibilityForPage(bundleName, pageID)
}

// invalidatePlanCacheForBundle invalidates all cached query plans for a bundle
// Called on schema changes (index creation, field modifications) to ensure
// queries use fresh plans reflecting the updated schema
// PERFORMANCE: Uses lock-free atomic load for planner access
func (s *BundleService) invalidatePlanCacheForBundle(bundleName string) {
	planner := getQueryPlanner()
	if planner == nil {
		return
	}

	// Invalidate all plans for this bundle
	planner.InvalidateBundleCache(bundleName)
	s.logger.Debugf("Invalidated query plan cache for bundle '%s' (schema change)", bundleName)
}

// invalidateBundleCaches invalidates all caches for a bundle when data changes.
// This includes:
// - Query plan cache (ensures fresh query plans)
// - JOIN hash table cache (ensures hash tables reflect current data)
//
// MUST be called after any INSERT, UPDATE, or DELETE operation on a bundle.
// PERFORMANCE: This is a fast operation - just clears cache entries without rebuilding.
func (s *BundleService) invalidateBundleCaches(bundleName string) {
	// Invalidate query plan cache
	s.invalidatePlanCacheForBundle(bundleName)

	// Invalidate JOIN hash table cache
	joinexecutor.GetHashTableCacheInterface().Invalidate(bundleName)

	s.logger.Debugf("Invalidated all caches for bundle '%s' (data change)", bundleName)
}

// removeBundleFromPlanCacheMetadata removes the bundle from plan-cache metadata
// (bundleInvalidations, staleServesByBundle, collectionVersions). Call when a bundle is dropped.
// PERFORMANCE: Uses lock-free atomic load for planner access
func (s *BundleService) removeBundleFromPlanCacheMetadata(bundleName string) {
	planner := getQueryPlanner()
	if planner == nil {
		return
	}

	planner.RemoveBundleMetadata(bundleName)
	s.logger.Debugf("Removed bundle '%s' from plan cache metadata (bundle dropped)", bundleName)
}

// min returns the minimum of two integers
// I should probably redo this in assembly for speed
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *BundleService) AddRelationshipToBundle(bundle *models.Bundle, relationshipCommand *models.RelationshipCommand) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if relationshipCommand == nil {
		return fmt.Errorf("relationship command is nil")
	}

	// Generate relationship name with proper counter
	relationshipName := s.generateRelationshipName(bundle, relationshipCommand.SourceBundle, relationshipCommand.DestinationBundle)

	// Check if the relationship already exists
	for _, rel := range bundle.Relationships {
		if rel.Name == relationshipName {
			return fmt.Errorf("relationship '%s' already exists in bundle '%s'", relationshipName, bundle.Name)
		}
	}

	// Create the relationship with new structure
	relationship := models.Relationship{
		Name:              relationshipName,
		SourceField:       relationshipCommand.SourceField,
		DestinationBundle: relationshipCommand.DestinationBundle,
		DestinationField:  relationshipCommand.DestinationField,
		SourceBundle:      relationshipCommand.SourceBundle,
		RelationshipType:  relationshipCommand.RelationshipType,

		// Set legacy fields for backward compatibility
		SourceBundleName: relationshipCommand.SourceBundle,
		TargetBundleName: relationshipCommand.DestinationBundle,
	}

	// Validate source field exists before creating relationship
	sourceFieldDef, exists := bundle.DocumentStructure.FieldDefinitions[relationshipCommand.SourceField]
	if !exists {
		return fmt.Errorf("relationship validation failed: source field '%s.%s' does not exist", bundle.Name, relationshipCommand.SourceField)
	}

	// Add the relationship to the bundle
	if bundle.Relationships == nil {
		bundle.Relationships = make(map[string]models.Relationship)
	}
	bundle.Relationships[relationship.Name] = relationship

	s.logger.Debugf("Validating relationship %s: %s.%s (%s) -> %s.%s",
		relationship.Name,
		relationship.SourceBundle,
		relationship.SourceField,
		sourceFieldDef.Type,
		relationship.DestinationBundle,
		relationship.DestinationField)

	// Handle different relationship types and add appropriate fields
	switch relationship.RelationshipType {
	case "1toMany":
		// For 1toMany relationships, add a field to the destination bundle
		err := s.addFieldToDestinationBundle(bundle, &relationship, true, false) // required=true, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for 1toMany relationship: %w", err)
		}
	// TODO Add a 1To1 relationship type later
	case "0toMany":
		// For 0toMany relationships, add a field to the destination bundle (not required)
		err := s.addFieldToDestinationBundle(bundle, &relationship, false, false) // required=false, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for 0toMany relationship: %w", err)
		}

	case "ManyToMany":
		// For ManyToMany relationships, add fields to both bundles
		err := s.addFieldToDestinationBundle(bundle, &relationship, false, false) // required=false, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for ManyToMany relationship: %w", err)
		}

		// Get destination bundle to lookup its source field type for reverse FK
		destBundle, err := s.GetBundleByName(bundle.Database, relationship.DestinationBundle)
		if err != nil {
			return fmt.Errorf("failed to get destination bundle for ManyToMany reverse field: %w", err)
		}

		// Lookup destination bundle's source field for type preservation
		destSourceFieldDef, exists := destBundle.DocumentStructure.FieldDefinitions[relationship.DestinationField]
		if !exists {
			return fmt.Errorf("relationship validation failed: destination field '%s.%s' does not exist for ManyToMany reverse", destBundle.Name, relationship.DestinationField)
		}

		// Also add the reverse field to the source bundle with preserved destination type
		reverseFieldName := relationship.DestinationBundle + "ID"
		bundle.DocumentStructure.FieldDefinitions[reverseFieldName] = models.FieldDefinition{
			Name:         reverseFieldName,
			Type:         destSourceFieldDef.Type, // Preserve destination field type
			IsRequired:   false,
			IsUnique:     false,
			DefaultValue: nil,
		}

		s.logger.Debugf("Added reverse field '%s' (type %s) to source bundle '%s' for ManyToMany relationship",
			reverseFieldName, destSourceFieldDef.Type, bundle.Name)

	default:
		return fmt.Errorf("unsupported relationship type: %s", relationship.RelationshipType)
	}

	// Update the source bundle in the store
	err := s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		return fmt.Errorf("failed to update source bundle in store: %w", err)
	}

	// Update the cache with the modified bundle
	s.bundleMetadata[bundle.Name] = bundle

	// GRAPHQL INTEGRATION: Regenerate GraphQL schema after relationship changes
	// Relationships add new fields to bundles (e.g., user.posts: [Post], post.author: User)
	// Both source and destination bundles need schema regeneration.
	//
	// This reuses the regenerateGraphQLSchema method for consistency.
	// Schema update failures are logged but don't fail the relationship creation.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema for source bundle '%s': %v. Relationship was created successfully.",
			bundle.Name, err)
	}

	// Regenerate destination bundle schema if it's different from source
	// (some relationships may be self-referential, e.g., user.manager -> user)
	if relationshipCommand.DestinationBundle != bundle.Name {
		destBundle, err := s.GetBundleByName(bundle.Database, relationshipCommand.DestinationBundle)
		if err == nil {
			if err := s.regenerateGraphQLSchema(destBundle); err != nil {
				s.logger.Warnf("[GraphQL] Failed to regenerate schema for destination bundle '%s': %v. Relationship was created successfully.",
					destBundle.Name, err)
			}
		} else {
			s.logger.Warnf("[GraphQL] Could not get destination bundle '%s' for schema regeneration: %v",
				relationshipCommand.DestinationBundle, err)
		}
	}

	s.logger.Debugf("Successfully added relationship '%s' to bundle '%s'", relationshipName, bundle.Name)
	return nil
}

// RemoveRelationshipFromBundle removes a relationship from a bundle by name.
// This is a metadata-only operation that removes the relationship definition while preserving
// all fields, indexes, and document data. The foreign key fields remain in place on both
// the source and destination bundles, and any auto-created indexes are also preserved.
// Parameters:
//   - bundle: The source bundle containing the relationship to remove
//   - relationshipName: The name of the relationship to remove (e.g., "Authors_Books_1")
//
// Returns: error if bundle is nil, relationship name is empty, relationship not found, or persistence fails
func (s *BundleService) RemoveRelationshipFromBundle(bundle *models.Bundle, relationshipName string) error {
	// Validate inputs following SyndrDB defensive programming practices
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if relationshipName == "" {
		return fmt.Errorf("relationship name cannot be empty")
	}

	// Check if relationship exists in bundle
	relationship, exists := bundle.Relationships[relationshipName]
	if !exists {
		return fmt.Errorf("relationship '%s' not found on bundle '%s'", relationshipName, bundle.Name)
	}

	s.logger.Debugf("Removing relationship '%s' (type: %s) from bundle '%s' to bundle '%s'",
		relationshipName, relationship.RelationshipType, bundle.Name, relationship.DestinationBundle)

	// Remove relationship from bundle metadata (metadata-only operation)
	delete(bundle.Relationships, relationshipName)

	// TODO: Add CASCADE option to remove auto-created foreign key fields and hash indexes when dropping relationship
	// TODO: Add RESTRICT validation to block drop if documents contain non-null foreign key values

	// Persist bundle metadata changes to disk
	err := s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		return fmt.Errorf("failed to update bundle file: %w", err)
	}

	// Regenerate GraphQL schema for source bundle only (follows AddRelationshipToBundle pattern)
	// This reuses the regenerateGraphQLSchema method for consistency.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema for bundle '%s' after relationship drop: %v", bundle.Name, err)
		// Continue despite GraphQL error - schema regeneration is non-critical
	}

	s.logger.Debugf("Successfully removed relationship '%s' from bundle '%s'", relationshipName, bundle.Name)
	return nil
}

// generateRelationshipName generates a unique relationship name with counter
// TODO This should go into a seperate bundle utilities file
func (s *BundleService) generateRelationshipName(bundle *models.Bundle, sourceBundle, destinationBundle string) string {
	baseName := fmt.Sprintf("%s_%s", sourceBundle, destinationBundle)
	counter := 1

	// Check for existing relationships with similar names and increment counter
	for {
		relationshipName := fmt.Sprintf("%s_%d", baseName, counter)
		if _, exists := bundle.Relationships[relationshipName]; !exists {
			return relationshipName
		}
		counter++
	}
}

// addFieldToDestinationBundle adds a relationship field to the destination bundle
// and automatically creates a hash index on the foreign key field for referential integrity
func (s *BundleService) addFieldToDestinationBundle(sourceBundle *models.Bundle, relationship *models.Relationship, isRequired, isUnique bool) error {
	// Find the destination bundle
	destinationBundle, err := s.GetBundleByName(sourceBundle.Database, relationship.DestinationBundle)
	if err != nil {
		return fmt.Errorf("destination bundle '%s' not found: %w", relationship.DestinationBundle, err)
	}

	// Lookup source field to preserve its type
	sourceFieldDef, exists := sourceBundle.DocumentStructure.FieldDefinitions[relationship.SourceField]
	if !exists {
		return fmt.Errorf("source field '%s' not found in bundle '%s'", relationship.SourceField, sourceBundle.Name)
	}

	// Check if field definitions map is initialized
	if destinationBundle.DocumentStructure.FieldDefinitions == nil {
		destinationBundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}

	// Add the relationship field to the destination bundle with preserved source type
	fieldName := relationship.DestinationField
	destinationBundle.DocumentStructure.FieldDefinitions[fieldName] = models.FieldDefinition{
		Name:         fieldName,
		Type:         sourceFieldDef.Type, // Preserve source field type instead of hardcoding "relationship"
		IsRequired:   isRequired,
		IsUnique:     isUnique,
		DefaultValue: nil,
	}

	s.logger.Debugf("Creating FK field '%s' with preserved type '%s' (from %s.%s)",
		fieldName, sourceFieldDef.Type, sourceBundle.Name, relationship.SourceField)

	// Update the destination bundle in the store
	err = s.store.UpdateBundleFile(destinationBundle.Database, destinationBundle)
	if err != nil {
		return fmt.Errorf("failed to update destination bundle '%s' in store: %w", destinationBundle.Name, err)
	}

	// s.logger.Debugf("Added relationship field '%s' to destination bundle '%s' (required=%t, unique=%t)",
	// 	fieldName, destinationBundle.Name, isRequired, isUnique)

	// Automatically create hash index on the foreign key field for referential integrity
	// This ensures that ValidateDelete() can perform O(1) lookups
	// Note: Index name should NOT include bundle name as infrastructure adds it automatically
	indexName := fmt.Sprintf("%s_fk", fieldName)

	// Check if index already exists
	if _, exists := destinationBundle.Indexes[indexName]; !exists {
		s.logger.Debugf("Automatically creating hash index '%s' on foreign key field '%s' in bundle '%s'",
			indexName, fieldName, destinationBundle.Name)

		// Create index command using FieldDefinition type
		indexCommand := &models.CreateIndexCommand{
			IndexName:  indexName,
			BundleName: destinationBundle.Name,
			IndexType:  "hash",
			Fields: []models.FieldDefinition{
				{
					Name:     fieldName,
					Type:     sourceFieldDef.Type, // Use preserved source field type
					IsUnique: false,               // Foreign keys are typically not unique (1-to-many)
				},
			},
		}

		// Reuse existing AddIndexToBundle infrastructure
		err = s.AddIndexToBundle(destinationBundle.Database, destinationBundle, indexCommand)
		if err != nil {
			// Log the error but don't fail the relationship creation
			// The relationship will still work, just without automatic referential integrity validation
			s.logger.Warnf("Failed to automatically create index on foreign key field '%s': %v. "+
				"Referential integrity validation will require manual index creation.", fieldName, err)
		} else {
			s.logger.Debugf("Successfully created hash index '%s' for referential integrity validation", indexName)
		}
	} else {
		s.logger.Debugf("Hash index '%s' already exists on foreign key field '%s', skipping automatic creation",
			indexName, fieldName)
	}

	return nil
}

func (s *BundleService) AddIndexToBundle(database *models.Database, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	//args := settings.GetSettings()

	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add index")
		return fmt.Errorf("bundle '%s' is nil, cannot add index", indexCommand.BundleName)
	}

	bundle, err := s.GetBundleByName(database, bundle.Name)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", indexCommand.BundleName)
	}

	// Invalidate plan cache before index creation (schema change)
	// This ensures queries use fresh plans after index becomes available
	s.invalidatePlanCacheForBundle(bundle.Name)

	// Create the index based on the command type

	switch indexCommand.IndexType {
	case "btree":

		err1 := CreateBTreeIndex(s, bundle, indexCommand)

		return err1

		// Record the created index
		// bundle.Indexes[indexCommand.IndexName] = indexRef
		// err = s.store.UpdateBundleFile(bundle.Database, bundle)
		// if err != nil {
		// 	s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		// 	return fmt.Errorf("failed to update bundle file after creating index: %w", err)
		// }
	case "hash":
		err1 := CreateHashIndex(s, bundle, indexCommand)
		return err1

	default:
		return fmt.Errorf("unknown index type: %s", indexCommand.IndexType)
	}
}

func CreateHashIndex(s *BundleService, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	// === OLD V2 IMPLEMENTATION (Commented out) ===
	// config := hashindexV2.IndexConfig{
	// 	BundleName:  bundle.Name,
	// 	FieldName:   indexCommand.Fields[0].Name,
	// 	IsUnique:    indexCommand.Fields[0].IsUnique,
	// 	DataDir:     args.DataDir,
	// 	DebugMode:   args.Debug,
	// 	InitialSize: 16,
	// 	PageSize:    8192,
	// 	LoadFactor:  0.75,
	// 	CacheSize:   100,
	// }
	// hashIndex, err := hashindexV2.CreateHashIndex(&config, s.logger)

	// === NEW V3 IMPLEMENTATION (LSM-style) ===
	// Create configuration for hashindexV3
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// Check if this field is a foreign key
	isForeignKey, referencedBundle, referencedField := IsFieldForeignKey(bundle, indexCommand.Fields[0].Name)

	// Get global settings for sequence safety margin
	globalSettings := settings.GetSettings()

	config := hashindex.IndexConfig{
		IndexName:            indexCommand.IndexName,
		BundleName:           bundle.Name,
		DatabaseName:         bundle.Database.Name,
		FieldName:            indexCommand.Fields[0].Name,
		DataDir:              indexesPath,
		MaxFileSize:          128 * 1024 * 1024, // 128MB per entry file
		WriteBufferSize:      64 * 1024,         // 64KB write buffer
		MemTableMaxSize:      100000,            // 100K entries in MemTable
		SequenceSafetyMargin: globalSettings.IndexSequenceSafetyMargin,
		CompactionEnabled:    true,
		CompactionMaxFiles:   10,
		Logger:               s.logger,
		IsForeignKey:         isForeignKey,
		ReferencedBundle:     referencedBundle,
		ReferencedField:      referencedField,
	}

	// Create the hash index using hashindexV3 LSM implementation
	hashIndex, err := hashindex.NewHashIndexV3(config)
	if err != nil {
		s.logger.Errorf("Failed to create hash index V3: %v", err)
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	// Backfill: Populate the hash index with existing documents from the bundle.
	// Without this, indexes created AFTER documents are inserted would remain empty,
	// causing all hash index lookups to miss (returning 0 results).
	// This mirrors the backfill logic in CreateBTreeIndex.
	//
	// We iterate page-by-page so we can record the correct pageID per document,
	// which the query planner uses to skip directly to the right storage page.
	fieldName := indexCommand.Fields[0].Name
	if bundle.PageCount > 0 || bundle.TotalDocuments > 0 {
		s.logger.Infof("Backfilling hash index '%s' on field '%s' for bundle '%s' (%d pages)",
			indexCommand.IndexName, fieldName, bundle.Name, bundle.PageCount)

		insertedCount := 0
		skippedCount := 0

		// Ensure metadata is current so PageCount is accurate
		s.metadataUpdateMutex.RLock()
		needsMetaFlush6137 := len(s.metadataUpdateBuffer) > 0
		s.metadataUpdateMutex.RUnlock()
		if needsMetaFlush6137 {
			s.FlushMetadataUpdates()
		}

		pageCount := uint32(bundle.PageCount)
		// Handle edge case: PageCount=0 but documents may exist on page 0
		if pageCount == 0 {
			pageCount = 1
		}

		for pageID := uint32(0); pageID < pageCount; pageID++ {
			docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
			if err != nil {
				// Page doesn't exist or can't be loaded; skip
				continue
			}

			for _, doc := range docs {
				// Extract the field value for hashing
				fieldValue, err := extractFieldValueForIndex(doc, fieldName)
				if err != nil {
					// Field may not exist on every document (sparse fields); skip gracefully.
					skippedCount++
					continue
				}

				// Convert field value to the same key string used by query lookups
				keyValue := fieldValueToIndexKeyString(fieldValue)
				if keyValue == "" || keyValue == "<nil>" {
					skippedCount++
					continue
				}

				// Insert into the hash index using Put(), which correctly sets BucketNum
				err = hashIndex.Put(keyValue, doc.DocumentID, pageID, doc.CommitSequence, doc.VersionSequence)
				if err != nil {
					s.logger.Warnf("Failed to backfill hash index entry for doc '%s' key '%s': %v",
						doc.DocumentID, keyValue, err)
					skippedCount++
					continue
				}
				insertedCount++
			}
		}

		s.logger.Infof("Hash index '%s' backfill complete: %d inserted, %d skipped",
			indexCommand.IndexName, insertedCount, skippedCount)

		// Flush to ensure all backfilled entries are persisted to disk
		if flushErr := hashIndex.Flush(); flushErr != nil {
			s.logger.Warnf("Failed to flush hash index '%s' after backfill: %v", indexCommand.IndexName, flushErr)
		}
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: fieldName,
		IsUnique:  indexCommand.Fields[0].IsUnique,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:      indexCommand.IndexName,
		Fields:         indexCommand.Fields,
		IndexType:      indexCommand.IndexType,
		CreateTime:     time.Now(),
		IndexInstance:  hashIndex, // Store the V3 hash index instance
		HashIndexField: indexField,
	}

	// Add the index to the bundle
	bundle.Indexes[indexCommand.IndexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexCommand.IndexName)

	// Update the bundle file
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		return fmt.Errorf("failed to update bundle file after creating index: %w", err)
	}

	s.logger.Debugf("Successfully created V3 hash index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldName, bundle.Name)
	return nil
}

func createHashIndexInternal(s *BundleService, bundle *models.Bundle, name string) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// === OLD V2 IMPLEMENTATION (Sprint 5: Commented out) ===
	// config := hashindexV2.IndexConfig{
	// 	DatabaseName: bundle.Database.Name,
	// 	BundleName:   bundle.Name,
	// 	FieldName:    name,
	// 	IsUnique:     true,
	// 	DataDir:      databasePath,
	// 	DebugMode:    args.Debug,
	// 	InitialSize:  16,
	// 	PageSize:     8192,
	// 	LoadFactor:   0.75,
	// 	CacheSize:    100,
	// }
	// hashIndex, err := hashindexV2.CreateHashIndex(&config, s.logger)

	// === NEW V3 IMPLEMENTATION (Sprint 5: LSM-style) ===
	// Check if this field is a foreign key
	isForeignKey, referencedBundle, referencedField := IsFieldForeignKey(bundle, name)

	// Get global settings for sequence safety margin
	globalSettings := settings.GetSettings()

	config := hashindex.IndexConfig{
		IndexName:            name, //name + "_idx",
		BundleName:           bundle.Name,
		DatabaseName:         bundle.Database.Name,
		FieldName:            name,
		DataDir:              indexesPath,
		MaxFileSize:          128 * 1024 * 1024,
		WriteBufferSize:      64 * 1024,
		MemTableMaxSize:      100000,
		SequenceSafetyMargin: globalSettings.IndexSequenceSafetyMargin,
		CompactionEnabled:    true,
		CompactionMaxFiles:   10,
		Logger:               s.logger,
		IsForeignKey:         isForeignKey,
		ReferencedBundle:     referencedBundle,
		ReferencedField:      referencedField,
	}

	// Create the hash index using hashindexV3
	hashIndex, err := hashindex.NewHashIndexV3(config)
	if err != nil {
		s.logger.Errorf("Failed to create hash index V3: %v", err)
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: name,
		IsUnique:  true,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:      name,
		Fields:         []models.FieldDefinition{bundle.DocumentStructure.FieldDefinitions["DocumentID"]},
		IndexType:      "hash",
		CreateTime:     time.Now(),
		IndexInstance:  hashIndex, // Store the V2 hash index instance
		HashIndexField: indexField,
	}

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Add the index to the bundle
	bundle.Indexes[name] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, name)

	// Update the bundle file
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		return fmt.Errorf("failed to update bundle file after creating index: %w", err)
	}

	// Update the cache with the modified bundle
	s.bundleMetadata[bundle.Name] = bundle

	s.logger.Debugf("Successfully created hash index '%s' on field '%s' for bundle '%s'", name, name, bundle.Name)
	return nil
}

// CreateBTreeIndex creates a new BTree index for the specified bundle and field
// This function follows the same pattern as the hash index creation but uses
// the btreeindexV2 implementation for optimal B+ tree performance
// Parameters:
//   - s: The BundleService instance for logging and storage operations
//   - bundle: The bundle to create the index for
//   - indexCommand: The command containing index configuration details
//
// Returns:
//   - error: Any error that occurred during index creation
func CreateBTreeIndex(s *BundleService, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	args := settings.GetSettings()

	// Validate input parameters
	if len(indexCommand.Fields) == 0 {
		return fmt.Errorf("no fields specified for BTree index creation")
	}

	// For now, support single-field indexes (can be extended for composite indexes later)
	if len(indexCommand.Fields) > 1 {
		return fmt.Errorf("composite BTree indexes not yet supported, please create separate indexes for each field")
	}

	fieldDef := indexCommand.Fields[0]

	// Validate that the field exists in the bundle structure
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldDef.Name]; !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", fieldDef.Name, bundle.Name)
	}

	s.logger.Debugf("Creating BTree index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	// Then in the CreateBTreeIndex function:
	splitRatio := calculateOptimalSplitRatio(fieldDef, fieldDef.IsUnique)

	// Create configuration for the new BTree index
	config := btreeindexV2.IndexConfig{
		DatabaseName: bundle.Database.Name,
		BundleName:   bundle.Name,
		FieldName:    fieldDef.Name,
		IsUnique:     fieldDef.IsUnique,
		// IndexDir removed - use proper database/bundle/indexes/btree path structure
		DebugMode:    args.Debug,
		PageSize:     8192,       // 8KB pages (PostgreSQL-style)
		CacheSize:    100,        // Cache 100 pages for performance
		FillFactor:   0.7,        // 70% fill factor for optimal balance between space and performance
		MaxKeyLength: 2048,       // Set maximum key length to 2KB
		SplitRatio:   splitRatio, // Use the calculated split ratio
	}

	// Configure WAL manager for durability using dependency injection
	// DRY Principle: Use shared service registry to access WAL without circular dependencies
	// Open/Closed: Registry pattern allows adding new services without modifying existing code
	serviceRegistry := registry.GetRegistry()
	if serviceRegistry.IsWALAvailable() {
		config.WALManager = serviceRegistry.GetWALManager()
		s.logger.Debugf("WAL enabled for B-tree index '%s' on field '%s'", indexCommand.IndexName, fieldDef.Name)
	} else {
		s.logger.Debugf("WAL not available for B-tree index '%s' (proceeding without durability)", indexCommand.IndexName)
	}

	// Set IndexName for proper file path construction
	config.IndexName = indexCommand.IndexName

	// Get proper database path structure (same as hash index)
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	// CRITICAL: Construct full B-tree indexes path to match bundle structure
	// B-tree indexes must be stored in: database/bundle/indexes/btree/
	// Format: /data_dir/<database>/<bundle>/indexes/btree/<btree-index-file-name>.btidx
	btreeIndexesPath := filepath.Join(databasePath, bundle.Name, "indexes", "btree")

	// Ensure the btree indexes directory exists before creating the index
	if err := os.MkdirAll(btreeIndexesPath, 0755); err != nil {
		s.logger.Errorf("Failed to create btree indexes directory: %v", err)
		return fmt.Errorf("failed to create btree indexes directory: %w", err)
	}

	// Create the BTree index using the V2 implementation
	btreeIndex, err := btreeindexV2.CreateBTreeIndex(&config, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create BTree index: %v", err)
		return fmt.Errorf("failed to create BTree index: %w", err)
	}

	// Populate the index with existing documents from the bundle
	// TODO: Optimize this to work with paginated documents
	s.logger.Debugf("Populating BTree index with documents from bundle '%s'", bundle.Name)

	// For now, we need to load all documents to build the index
	// In the future, this should be done incrementally as pages are loaded
	allDocuments, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
	if err != nil {
		s.logger.Warnf("Failed to load documents for indexing: %v", err)
		return err
	}

	if len(allDocuments) > 0 {
		s.logger.Debugf("Populating BTree index with %d existing documents", len(allDocuments))

		insertedCount := 0
		skippedCount := 0
		for documentID, document := range allDocuments {
			// Extract the field value for indexing
			fieldValue, err := extractFieldValueForIndex(*document, fieldDef.Name)
			if err != nil {
				s.logger.Warnf("Failed to extract field value for document '%s': %v", documentID, err)
				continue
			}

			// Convert field value to bytes for BTree storage
			keyBytes, err := convertValueToBytes(fieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes for document '%s': %v", documentID, err)
				continue
			}

			// Insert into the BTree index
			// CRITICAL FIX: Make population idempotent - skip duplicates gracefully
			// This allows index creation to succeed even if:
			// - Index was partially populated from a previous failed attempt
			// - Same document appears multiple times in the document set
			// - Concurrent operations added entries
			err = btreeIndex.Insert(keyBytes, document.DocumentID)
			if err != nil {
				// Check if this is a duplicate document ID error (expected during idempotent population)
				if strings.Contains(err.Error(), "document ID already exists for this key") {
					// Document already in index - skip gracefully (idempotent behavior)
					skippedCount++
					s.logger.Debugf("Skipping document '%s' - already exists in index (idempotent population)", documentID)
					continue
				}
				// For other errors (e.g., unique constraint violations), log and fail
				s.logger.Errorf("Failed to insert document '%s' into BTree index: %v", documentID, err)
				btreeIndex.Close()
				return fmt.Errorf("failed to populate BTree index with existing documents: %w", err)
			}
			insertedCount++
		}

		s.logger.Debugf("BTree index population complete: inserted %d documents, skipped %d duplicates", insertedCount, skippedCount)

		if err := btreeIndex.PersistMetadata(); err != nil {
			s.logger.Warnf("Failed to persist B-tree index metadata after population: %v", err)
		}
		s.logger.Debugf("Successfully populated BTree index with existing documents")
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: fieldDef.Name,
		IsUnique:  fieldDef.IsUnique,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:       indexCommand.IndexName,
		Fields:          indexCommand.Fields,
		IndexType:       indexCommand.IndexType,
		CreateTime:      time.Now(),
		IndexInstance:   btreeIndex, // Store the V2 BTree index instance
		BTreeIndexField: indexField, // Add this field to models.IndexReference if not exists
	}

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Add the index to the bundle
	bundle.Indexes[indexCommand.IndexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexCommand.IndexName)

	// Update the bundle file with the new index information
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating BTree index: %v", err)
		// Close the index since we couldn't save the bundle state
		btreeIndex.Close()
		return fmt.Errorf("failed to update bundle file after creating BTree index: %w", err)
	}

	s.logger.Debugf("Successfully created BTree index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	return nil
}

// extractFieldValueForIndex extracts the value of a specific field from a document
// This function handles the document field structure and returns the raw value
// for index key generation
// Parameters:
//   - document: The document to extract the field value from
//   - fieldName: The name of the field to extract
//
// Returns:
//   - interface{}: The field value
//   - error: Any error that occurred during extraction
func extractFieldValueForIndex(document models.Document, fieldName string) (interface{}, error) {
	if document.Fields == nil {
		return nil, fmt.Errorf("document has no fields")
	}

	field, exists := document.Fields[fieldName]
	if !exists {
		return nil, fmt.Errorf("field '%s' not found in document", fieldName)
	}

	return field.Value, nil
}

// fieldValueToIndexKeyString converts a value (possibly models.FieldValue from a document field)
// to the same string representation used by query lookups. Query literals are converted with
// conversion.ValueToString(int64(6775)) -> "6775". Document fields are models.FieldValue; passing
// that directly to ValueToString would hit the default case and produce a struct dump (e.g.
// "{Type:2 IntVal:6775}"), so lookups for "6775" would miss. This helper unwraps FieldValue via
// AsInterface() so the key matches query lookups.
func fieldValueToIndexKeyString(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	if fv, ok := v.(models.FieldValue); ok {
		return conversion.ValueToString(fv.AsInterface())
	}
	return conversion.ValueToString(v)
}

// convertValueToBytes converts a field value to bytes for BTree key storage
// This function handles different data types and converts them to a consistent
// byte representation for use as BTree keys
// Parameters:
//   - value: The field value to convert
//
// Returns:
//   - []byte: The value converted to bytes
//   - error: Any error that occurred during conversion
func convertValueToBytes(value interface{}) ([]byte, error) {
	if value == nil {
		return []byte{}, nil
	}

	switch v := value.(type) {
	case string:
		// Magic values (SYNDR_NULL, SYNDR_MISSING, etc.) get consistent byte representation
		// This ensures they sort predictably in BTree indexes and can be efficiently queried
		if strings.HasPrefix(v, "::SYNDR_") {
			// Store magic values as-is for consistent indexing and querying
			return []byte(v), nil
		}
		return []byte(v), nil
	case []byte:
		return v, nil
	case int:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int32:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int64:
		return []byte(fmt.Sprintf("%d", v)), nil
	case float32:
		return []byte(fmt.Sprintf("%.6f", v)), nil
	case float64:
		return []byte(fmt.Sprintf("%.6f", v)), nil
	case bool:
		if v {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	default:
		// For complex types, convert to string representation
		return []byte(conversion.ValueToString(v)), nil
	}
}

// runBTreeRollback undoes B-tree index updates when UpdateDocumentsBatch fails.
// For each op: Delete(newKey, documentID) then Insert(oldKey) to restore pre-update state.
// Logs and continues on individual op failures since we are already in an error path.
func (s *BundleService) runBTreeRollback(ops []btreeRollbackOp) {
	for _, op := range ops {
		if err := op.idx.Delete(op.newKey, op.documentID); err != nil {
			s.logger.Warnf("rollback: B-tree Delete failed for doc %s: %v", op.documentID, err)
		}
		if err := op.idx.Insert(op.oldKey, op.documentID); err != nil {
			s.logger.Warnf("rollback: B-tree Insert failed for doc %s: %v", op.documentID, err)
		}
	}
}

// calculateOptimalSplitRatio determines the best split ratio based on field characteristics
// This function follows the Single Responsibility Principle for split ratio calculation
// Parameters:
//   - fieldDef: The field definition to analyze
//   - isUnique: Whether this is a unique index
//
// Returns:
//   - float64: The optimal split ratio for this index
func calculateOptimalSplitRatio(fieldDef models.FieldDefinition, isUnique bool) float64 {
	// For unique indexes, use 50% split for balanced structure
	if isUnique {
		return 0.5
	}

	/*
		Split Ratio = 0.5 (50%) is the recommended value from Copilot because:

		1.Balanced Tree Structure: When a node becomes full and needs to split, a 50% ratio creates two nodes
		that are equally balanced, maintaining optimal B+ tree characteristics.

		2.PostgreSQL Standard: PostgreSQL uses a similar 50% split ratio for B-tree indexes, which provides
		excellent performance characteristics.

		3.Optimal Performance: Equal splits minimize tree height and provide consistent performance for both
		insertions and searches.

		4.Space Efficiency: Balanced splits ensure good space utilization without excessive fragmentation.

		I will use this for now, but will eventually make it more intelligent, despite copilots claims
	*/

	// For non-unique indexes with potential duplicates, slightly favor left split
	// This can help with sequential insertion patterns
	switch fieldDef.Type {
	case "string":
		return 0.5 // Balanced split for string fields
	case "int", "int32", "int64":
		return 0.6 // Slightly favor left for numeric sequences
	case "float32", "float64":
		return 0.5 // Balanced split for floating point
	case "bool":
		return 0.5 // Balanced split for boolean fields
	default:
		return 0.5 // Default to balanced split
	}
}

// GetOrLoadHashIndex retrieves or loads a hash index instance for the specified bundle and index name
// This function follows the Single Responsibility Principle by handling only hash index loading
// Parameters:
//   - bundle: The bundle containing the index reference
//   - indexName: The name of the index to load
//   - indexRef: The index reference containing metadata
//
// Returns:
//   - *hashindex.HashIndexV3: The loaded hash index instance (V3 LSM-style)
//   - error: Any error that occurred during loading
func (s *BundleService) GetOrLoadHashIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*hashindex.HashIndexV3, error) {
	// CRITICAL FIX: Use dedicated in-memory cache instead of bundle.Indexes[].IndexInstance
	// The IndexInstance field has `json:"-"` tag so it's never persisted to disk
	// This caused the cache to be empty on every operation, forcing disk loads

	// Check the sharded cache first (fast path with read lock per shard)
	if cachedIndex, found := s.loadedIndexes.Get(bundle.Name, indexName); found {
		if hashIndex, ok := cachedIndex.(*hashindex.HashIndexV3); ok {
			s.logger.Debugf("✓ Hash index V3 '%s' CACHE HIT (already in memory)", indexName)
			return hashIndex, nil
		}
	}

	s.logger.Debugf("⚠️  Hash index V3 '%s' CACHE MISS - loading from disk for bundle '%s'", indexName, bundle.Name)

	// === OLD V2 IMPLEMENTATION (Commented out) ===
	// args := settings.GetSettings()
	// databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	// indexFilePath := fmt.Sprintf("%s%s_%s.hidx", databasePath, bundle.Name, indexRef.HashIndexField.FieldName)
	// hashIndex, err := hashindexV2.OpenHashIndex(indexFilePath, args.Debug, s.logger)

	// === NEW V3 IMPLEMENTATION (Sprint 5: LSM-style) ===
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	walBufferMax := s.settings.Storage.MemTableWALBufferMaxSize
	if walBufferMax <= 0 {
		walBufferMax = 50000 // default to bound memory
	}
	config := hashindex.IndexConfig{
		IndexName:                indexName,
		BundleName:               bundle.Name,
		DatabaseName:             bundle.Database.Name,
		FieldName:                indexRef.HashIndexField.FieldName,
		DataDir:                  indexesPath,
		MaxFileSize:              128 * 1024 * 1024,
		WriteBufferSize:          64 * 1024,
		MemTableMaxSize:          100000,
		MemTableWALBufferMaxSize: walBufferMax,
		CompactionEnabled:        true,
		CompactionMaxFiles:       10,
		Logger:                   s.logger,
	}

	hashIndex, err := hashindex.OpenHashIndexV3(config)
	if err != nil {
		return nil, fmt.Errorf("failed to load hash index V3 '%s' from disk: %w", indexName, err)
	}

	// Store the loaded instance in the sharded cache (thread-safe with GetOrSet)
	s.loadedIndexes.Set(bundle.Name, indexName, hashIndex)

	s.logger.Debugf("✅ Successfully loaded and cached hash index V3 '%s' from disk", indexName)
	return hashIndex, nil
}

// getOrLoadBTreeIndex retrieves or loads a BTree index instance for the specified bundle and index name
// This function follows the Single Responsibility Principle by handling only BTree index loading
// Uses persistent loadedIndexes cache (like hash indexes) to avoid reload overhead
// Parameters:
//   - bundle: The bundle containing the index reference
//   - indexName: The name of the index to load
//   - indexRef: The index reference containing metadata
//
// Returns:
//   - *btreeindexV2.BTreeIndex: The loaded BTree index instance
//   - error: Any error that occurred during loading
func (s *BundleService) getOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*btreeindexV2.BTreeIndex, error) {
	// FIX: Use proper double-checked locking pattern to prevent race condition
	// where multiple goroutines load separate BTreeIndex instances for the same file,
	// causing tree corruption (each instance has its own mutex but shares the file)
	// PERFORMANCE: Uses sharded cache (64 shards) with per-shard locking

	// Use GetOrLoad which provides atomic load-or-create semantics per shard
	// This prevents multiple goroutines from loading the same index simultaneously
	loaded, err := s.loadedIndexes.GetOrLoad(bundle.Name, indexName, func() (interface{}, error) {
		s.logger.Debugf("⚠️  BTree index '%s' CACHE MISS - loading from disk for bundle '%s'", indexName, bundle.Name)

		// TODO This is should be in a separate centrailized location so we can alter folders later
		// Construct proper B-tree index file path
		// Format: /data_dir/<database>/<bundle>/indexes/btree/<btree-index-file-name>.btidx
		databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
		btreeIndexesPath := filepath.Join(databasePath, bundle.Name, "indexes", "btree")
		indexFilePath := filepath.Join(btreeIndexesPath, fmt.Sprintf("%s.btidx", indexName))

		// Check if the index file exists before trying to open it
		if _, err := os.Stat(indexFilePath); os.IsNotExist(err) {
			// Index file doesn't exist - this can happen if:
			// 1. Index was just created but file creation failed
			// 2. Index metadata exists but file was deleted
			// 3. Race condition during index creation
			s.logger.Warnf("BTree index file '%s' does not exist for index '%s', skipping updates", indexFilePath, indexName)
			return nil, fmt.Errorf("index file does not exist: %s (index may still be initializing)", indexFilePath)
		}

		args := settings.GetSettings()
		btreeIndex, err := btreeindexV2.OpenBTreeIndex(indexFilePath, args.Debug, s.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to load BTree index '%s' from disk: %w", indexName, err)
		}

		s.logger.Debugf("✅ Successfully loaded and cached BTree index '%s' from disk", indexName)
		return btreeIndex, nil
	})

	if err != nil {
		return nil, err
	}

	return loaded.(*btreeindexV2.BTreeIndex), nil
}

// GetOrLoadBTreeIndex is a public wrapper for getOrLoadBTreeIndex to support query planner
func (s *BundleService) GetOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
}

// GetOrLoadHashIndexInterface is a wrapper to support query planner interface compatibility
func (s *BundleService) GetOrLoadHashIndexInterface(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.GetOrLoadHashIndex(bundle, indexName, indexRef)
}

// LoadDatabaseIndexes loads all unique constraint B-tree indexes for a database into memory
// This implements PostgreSQL-style in-memory index caching with LRU eviction on idle timeout
// Called automatically on database context switches (connection/USE command)
// PHASE 5: Refactored to use atomic counter and sharded map for concurrent access.
// Parameters:
//   - databaseName: The name of the database to load indexes for
//
// Returns:
//   - error: Any error that occurred during loading or LRU eviction
func (s *BundleService) LoadDatabaseIndexes(databaseName string) error {
	// PHASE 5: Fast path using sharded map - Touch updates timestamp if exists
	if s.loadedDatabases.Touch(databaseName) {
		s.logger.Debugf("Database '%s' indexes already loaded (touch updated access time)", databaseName)
		return nil
	}

	// Database not loaded yet - check again with get to avoid race
	if _, exists := s.loadedDatabases.Get(databaseName); exists {
		s.loadedDatabases.Set(databaseName, time.Now())
		s.logger.Debugf("Database '%s' indexes already loaded (race condition avoided)", databaseName)
		return nil
	}

	// LRU EVICTION: Find databases idle for more than 10 minutes
	idleTimeout := 10 * time.Minute
	evictedDatabases := s.loadedDatabases.ForEachWithEviction(idleTimeout)

	// Evict idle databases and free their memory
	for _, dbName := range evictedDatabases {
		lastAccess, _ := s.loadedDatabases.Get(dbName)
		s.logger.Debugf("📤 Evicting idle database '%s' indexes (idle for %v)", dbName, time.Since(lastAccess))

		// Find all bundles for this database and unload their unique indexes
		// Use ForEach to safely iterate over the sharded cache
		s.loadedIndexes.ForEach(func(bundleName string, indexes map[string]interface{}) bool {
			// TODO: I need to add database info to bundle name or use catalog to map bundle->database
			// For now, we'll unload all indexes for the evicted database (conservative approach)
			for indexName, indexInstance := range indexes {
				if btreeIndex, ok := indexInstance.(*btreeindexV2.BTreeIndex); ok {
					// Check if this is a unique index for the evicted database
					// Close the index to free file handles and memory
					if err := btreeIndex.Close(); err != nil {
						s.logger.Warnf("Failed to close B-tree index '%s' during eviction: %v", indexName, err)
					}
					// PHASE 5: Use atomic subtraction for memory tracking
					meta := btreeIndex.Metadata
					s.currentIndexMemoryUsage.Add(-meta.EstimatedMemorySizeBytes)
					// Delete from sharded cache (ForEach gives us a copy, so we delete separately)
					s.loadedIndexes.DeleteIndex(bundleName, indexName)
					s.logger.Debugf("  Unloaded B-tree index '%s' from bundle '%s' (%d MB freed)",
						indexName, bundleName, meta.EstimatedMemorySizeBytes/(1024*1024))
				}
			}
			return true // continue iteration
		})

		s.loadedDatabases.Delete(dbName)
	}

	// Find all bundles in this database and load unique constraint B-tree indexes
	var totalIndexes int
	var totalMemory int64
	var skippedIndexes int

	// Iterate through all bundles to find ones belonging to this database
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Database == nil || bundle.Database.Name != databaseName {
			continue
		}

		// Iterate through bundle indexes to find unique B-tree indexes
		for indexName, indexRef := range bundle.Indexes {
			// Only load B-tree indexes with unique constraints
			if indexRef.IndexType != "btree" || !indexRef.BTreeIndexField.IsUnique {
				continue
			}

			// Load the B-tree index to check memory size
			btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
			if err != nil {
				s.logger.Warnf("Failed to load unique B-tree index '%s' for memory check: %v", indexName, err)
				continue
			}

			// Check if we have budget for this index
			meta := btreeIndex.Metadata
			indexSize := meta.EstimatedMemorySizeBytes

			// PHASE 5: Use atomic Load for budget check
			currentUsage := s.currentIndexMemoryUsage.Load()
			if currentUsage+indexSize > s.uniqueIndexMemoryBudgetBytes {
				s.logger.Warnf("⚠️  Memory budget exceeded, skipping B-tree index '%s' (would use %d MB, budget: %d MB used / %d MB total)",
					indexName,
					indexSize/(1024*1024),
					currentUsage/(1024*1024),
					s.uniqueIndexMemoryBudgetBytes/(1024*1024))
				skippedIndexes++

				// Close the index since we won't keep it in memory
				if err := btreeIndex.Close(); err != nil {
					s.logger.Warnf("Failed to close B-tree index '%s': %v", indexName, err)
				}

				// Remove from sharded cache to force disk-based fallback
				s.loadedIndexes.Delete(bundleName, indexName)

				continue
			}

			// PHASE 5: Use atomic addition for memory tracking
			s.currentIndexMemoryUsage.Add(indexSize)
			totalIndexes++
			totalMemory += indexSize
			s.logger.Debugf("  ✓ Loaded unique B-tree index '%s.%s' (%d MB, %d records)",
				bundleName, indexName, indexSize/(1024*1024), meta.TotalRecords)
		}
	}

	// Mark database as loaded using sharded map
	s.loadedDatabases.Set(databaseName, time.Now())

	// Log summary at INFO level for visibility
	if skippedIndexes > 0 {
		s.logger.Debugf("📊 Loaded %d unique indexes for database '%s', using %d MB / %d MB budget (%d indexes skipped due to budget)",
			totalIndexes, databaseName,
			totalMemory/(1024*1024),
			s.uniqueIndexMemoryBudgetBytes/(1024*1024),
			skippedIndexes)
	} else {
		s.logger.Debugf("📊 Loaded %d unique indexes for database '%s', using %d MB / %d MB budget",
			totalIndexes, databaseName,
			totalMemory/(1024*1024),
			s.uniqueIndexMemoryBudgetBytes/(1024*1024))
	}

	return nil
}

func (s *BundleService) AddDocumentToBundle(database *models.Database, bundle *models.Bundle, docCommand *models.DocumentCommand) (string, error) {
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add document")
		return "", fmt.Errorf("bundle '%s' is nil, cannot add document ", docCommand.BundleName)
	}

	// bundle, err := s.GetBundleByName(database, docCommand.BundleName)
	// if err != nil {
	// 	return "", fmt.Errorf("bundle '%s' not found", docCommand.BundleName)
	// }

	// CRITICAL: Process NULL values and defaults FIRST, before validation
	// This allows default value substitution for required fields that are missing or NULL
	// Must happen before validation so that required fields with defaults can be satisfied
	nullStart := time.Now()
	err := s.processNullValues(bundle, docCommand)
	nullDuration := time.Since(nullStart)
	if nullDuration > 1*time.Millisecond {
		s.logger.Warnf("  ⚠️  processNullValues took %v", nullDuration)
	} else {
		s.logger.Debugf("  ✓ processNullValues took %v", nullDuration)
	}
	if err != nil {
		return "", fmt.Errorf("failed to process NULL values: %w", err)
	}

	// Validate document fields against bundle field definitions
	// This runs AFTER processNullValues so that default values are already substituted
	validateStart := time.Now()
	err = s.validateDocumentFields(bundle, docCommand)
	validateDuration := time.Since(validateStart)
	if validateDuration > 1*time.Millisecond {
		s.logger.Warnf("  ⚠️  validateDocumentFields took %v", validateDuration)
	} else {
		s.logger.Debugf("  ✓ validateDocumentFields took %v", validateDuration)
	}
	if err != nil {
		return "", fmt.Errorf("document field validation failed: %w", err)
	}

	// Validate unique constraints for all IsUnique fields
	uniqueStart := time.Now()
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	err = uniqueValidator.ValidateUniqueConstraints(bundle, docCommand)
	uniqueDuration := time.Since(uniqueStart)
	if uniqueDuration > 1*time.Millisecond {
		s.logger.Warnf("  ⚠️  ValidateUniqueConstraints took %v", uniqueDuration)
	} else {
		s.logger.Debugf("  ✓ ValidateUniqueConstraints took %v", uniqueDuration)
	}
	if err != nil {
		// Return the error directly - it's already a properly typed SyndrDBError
		// with ERR_VALIDATION_CONSTRAINT code that won't log stack traces
		return "", err
	}

	// Add the document to the bundle
	newDocument := s.documentFactory.NewDocument(*docCommand)

	// DIAGNOSTIC: Log bundle index status (only if verbose logging enabled)
	if s.verboseLogging {
		s.logger.Debugf("DIAGNOSTIC: Bundle '%s' has Indexes map: %v, count: %d", bundle.Name, bundle.Indexes != nil, len(bundle.Indexes))
		if len(bundle.Indexes) > 0 {
			for idxName := range bundle.Indexes {
				s.logger.Debugf("DIAGNOSTIC: Found index: %s", idxName)
			}
		}
	}

	// Schedule deferred index updates for optimal performance instead of immediate updates
	// Schedule deferred metadata update instead of immediate calculation
	s.scheduleMetadataUpdate(docCommand.BundleName, "increment_docs", 1)

	// Add document to bundle file (storage layer handles page allocation and returns pageID)
	pageID, err := s.store.AddDocumentToBundleFile(bundle, newDocument)
	if err != nil {
		// Note: Metadata updates are deferred, so no rollback needed here
		// Failed operations won't have their metadata updates applied
		return "", fmt.Errorf("failed to add document to bundle: %w", err)
	}

	// WRITE-THROUGH CACHE: Immediately update page cache so reads see this document
	// This is the core write-through mechanism - after WriteBuffer commits, we update
	// the in-memory cache so subsequent reads don't need a disk round-trip
	s.updatePageCacheWithDocument(bundle.Name, pageID, newDocument)

	// Update column statistics incrementally for the planner
	if s.statsUpdater != nil && newDocument.Fields != nil {
		for fieldName, field := range newDocument.Fields {
			s.statsUpdater.IncrementalUpdate(
				bundle.Name, fieldName,
				nil, // oldValue: nil for INSERT
				field.Value.AsInterface(),
				bundle.TotalDocuments,
			)
		}
	}

	// Now schedule index updates with the actual pageID from storage
	//indexStart := time.Now()
	indexCount := 0
	if bundle.Indexes != nil {
		// Look for indexes and schedule updates
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" {
				// Handle ALL hash indexes (DocumentID and foreign keys)
				fieldName := indexRef.HashIndexField.FieldName

				// Extract the field value for hash indexing
				var fieldValue interface{}
				if fieldName == "DocumentID" {
					fieldValue = newDocument.DocumentID
				} else {
					// Extract the foreign key or other field value
					extractedValue, err := extractFieldValueForIndex(*newDocument, fieldName)
					if err != nil {
						s.logger.Warnf("[HASH-IDX] Skipped indexing document %s for index %s: field %q not in document (%v); check field name case (e.g. ID vs id)",
							newDocument.DocumentID, indexName, fieldName, err)
						continue
					}
					fieldValue = extractedValue
				}

				// Schedule hash index update with actual pageID; pass commitSeq/versionSeq to avoid GetDocument.
				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", newDocument.DocumentID, fieldValue, pageID, nil, false, newDocument.CommitSequence, newDocument.VersionSequence)
				s.logger.Debugf("Scheduled hash index '%s' update for document '%s' on field '%s' (page %d)",
					indexName, newDocument.DocumentID, fieldName, pageID)
				indexCount++

			} else if indexRef.IndexType == "btree" {
				// Extract the field value for BTree indexing
				fieldValue, err := extractFieldValueForIndex(*newDocument, indexRef.BTreeIndexField.FieldName)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", newDocument.DocumentID, err)
					continue
				}

				// Schedule BTree index update with actual pageID
				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", newDocument.DocumentID, fieldValue, pageID, nil, false)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s' (page %d)",
					newDocument.DocumentID, indexRef.BTreeIndexField.FieldName, pageID)
				indexCount++
			}
		}
	}
	//indexDuration := time.Since(indexStart)
	// if indexDuration > 1*time.Millisecond {
	// 	s.logger.Warnf("  ⚠️  Index scheduling (%d indexes) took %v", indexCount, indexDuration)
	// } else {
	// 	s.logger.Debugf("  ✓ Index scheduling (%d indexes) took %v", indexCount, indexDuration)
	// }
	if bundle.Indexes == nil {
		s.logger.Warnf("No indexes found for bundle '%s'", bundle.Name)
	}

	// SNAPSHOT ISOLATION: No invalidation needed - scanners filter documents by MVCC visibility
	// Documents inserted after scanner creation are filtered out during iteration
	// This avoids cache churn and enables consistent reads without destroying scanners
	s.logger.Debugf("INSERT completed for bundle '%s' page %d - scanners use snapshot isolation", docCommand.BundleName, pageID)

	// PERFORMANCE: Invalidate JOIN hash table cache for this bundle
	// This ensures subsequent JOINs rebuild hash tables with the new document
	s.invalidateBundleCaches(bundle.Name)

	return newDocument.DocumentID, nil
}

// AddDocumentToBundleWithTxID is a transaction-aware wrapper for AddDocumentToBundle.
// When preallocDocID is non-empty, the document is created with that ID (Phase 3: document-level lock).
func (s *BundleService) AddDocumentToBundleWithTxID(database *models.Database, bundle *models.Bundle, docCommand *models.DocumentCommand, txID string, preallocDocID ...string) (string, error) {
	docID := ""
	if len(preallocDocID) > 0 {
		docID = preallocDocID[0]
	}
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add document")
		return "", fmt.Errorf("bundle '%s' is nil, cannot add document ", docCommand.BundleName)
	}

	// CRITICAL: Process NULL values and defaults FIRST, before validation
	err := s.processNullValues(bundle, docCommand)
	if err != nil {
		return "", fmt.Errorf("failed to process NULL values: %w", err)
	}

	// Validate document fields against bundle field definitions
	err = s.validateDocumentFields(bundle, docCommand)
	if err != nil {
		return "", fmt.Errorf("document field validation failed: %w", err)
	}

	// Validate unique constraints for all IsUnique fields
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	err = uniqueValidator.ValidateUniqueConstraints(bundle, docCommand)
	if err != nil {
		// Return the error directly - it's already a properly typed SyndrDBError
		// with ERR_VALIDATION_CONSTRAINT code that won't log stack traces
		return "", err
	}

	var newDocument *models.Document
	if docID != "" {
		newDocument = s.documentFactory.NewDocumentWithID(*docCommand, docID)
	} else {
		newDocument = s.documentFactory.NewDocument(*docCommand)
	}

	// Set MVCC version metadata
	s.setDocumentVersionFields(newDocument, txID, 1)

	s.scheduleMetadataUpdate(docCommand.BundleName, "increment_docs", 1)

	pageID, err := s.store.AppendDocumentToBundleFileWithTxID(bundle, newDocument, txID)
	if err != nil {
		return "", fmt.Errorf("failed to add document to bundle: %w", err)
	}

	// WRITE-THROUGH CACHE: Immediately update page cache so reads see this document
	s.updatePageCacheWithDocument(bundle.Name, pageID, newDocument)

	// Schedule index updates with the actual pageID from storage
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" {
				fieldName := indexRef.HashIndexField.FieldName
				var fieldValue interface{}
				if fieldName == "DocumentID" {
					fieldValue = newDocument.DocumentID
				} else {
					extractedValue, err := extractFieldValueForIndex(*newDocument, fieldName)
					if err != nil {
						s.logger.Warnf("[HASH-IDX] Skipped indexing document %s for index %s: field %q not in document (%v)",
							newDocument.DocumentID, indexName, fieldName, err)
						continue
					}
					fieldValue = extractedValue
				}

				// deferred=false for ADD operations; pass commitSeq/versionSeq to avoid GetDocument.
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", newDocument.DocumentID, fieldValue, pageID, nil, false, newDocument.CommitSequence, newDocument.VersionSequence)
				s.logger.Debugf("Scheduled hash index '%s' update for document '%s' on field '%s' (page %d)",
					indexName, newDocument.DocumentID, fieldName, pageID)

			} else if indexRef.IndexType == "btree" {
				fieldValue, err := extractFieldValueForIndex(*newDocument, indexRef.BTreeIndexField.FieldName)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", newDocument.DocumentID, err)
					continue
				}

				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", newDocument.DocumentID, fieldValue, pageID, nil, false)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s' (page %d)",
					newDocument.DocumentID, indexRef.BTreeIndexField.FieldName, pageID)
			}
		}
	}

	// SNAPSHOT ISOLATION: No invalidation needed - scanners filter documents by MVCC visibility
	// Documents inserted after scanner creation are filtered out during iteration
	// This avoids cache churn and enables consistent reads without destroying scanners
	s.logger.Debugf("INSERT completed for bundle '%s' page %d - scanners use snapshot isolation", docCommand.BundleName, pageID)

	return newDocument.DocumentID, nil
}

func (s *BundleService) AddDocumentToBundleByStruct(database *models.Database, bundle *models.Bundle, document *models.Document) error {
	return s.AddDocumentToBundleByStructWithTxID(database, bundle, document, "")
}

// AddDocumentToBundleByStructWithTxID adds a document with transaction tracking.
// PHASE 3: No bundle write lock; append path is concurrent-safe (WriteBuffer, rotationLock).
func (s *BundleService) AddDocumentToBundleByStructWithTxID(database *models.Database, bundle *models.Bundle, document *models.Document, txID string) error {
	// TODO: Unique constraint validation disabled for AddDocumentToBundleByStruct
	// This method is primarily used for primary catalog initialization where we trust
	// the developer to create bundles correctly. Enabling validation would require
	// creating unique indexes for all catalog bundles, which adds unnecessary overhead.
	// If needed in the future, add validation selectively based on bundle/database context.
	//
	// Validate unique constraints for all IsUnique fields
	// Convert Document struct to DocumentCommand for validation
	// docCommand := &models.DocumentCommand{
	// 	BundleName: bundle.Name,
	// 	Fields:     make([]models.KeyValue, 0, len(document.Fields)),
	// }
	// for fieldName, field := range document.Fields {
	// 	docCommand.Fields = append(docCommand.Fields, models.KeyValue{
	// 		Key:   fieldName,
	// 		Value: field.Value,
	// 	})
	// }
	//
	// uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	// err := uniqueValidator.ValidateUniqueConstraints(bundle, docCommand)
	// if err != nil {
	// 	return fmt.Errorf("unique constraint validation failed: %w", err)
	// }

	// Schedule deferred metadata update instead of immediate calculation
	s.scheduleMetadataUpdate(bundle.Name, "increment_docs", 1)

	// Add document to bundle file (storage layer handles page allocation and returns pageID)
	// Use transaction-aware method to track txID in buffer
	pageID, err := s.store.AppendDocumentToBundleFileWithTxID(bundle, document, txID)
	if err != nil {
		return fmt.Errorf("failed to add document to bundle: %w", err)
	}

	// Now schedule index updates with the actual pageID from storage
	if bundle.Indexes != nil {
		// Schedule deferred index updates instead of immediate updates
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" {
				// Handle ALL hash indexes (DocumentID and foreign keys)
				fieldName := indexRef.HashIndexField.FieldName

				// Extract the field value for hash indexing
				var fieldValue interface{}
				if fieldName == "DocumentID" {
					fieldValue = document.DocumentID
				} else {
					// Extract the foreign key or other field value
					extractedValue, err := extractFieldValueForIndex(*document, fieldName)
					if err != nil {
						s.logger.Warnf("[HASH-IDX] Skipped indexing document %s for index %s: field %q not in document (%v)",
							document.DocumentID, indexName, fieldName, err)
						continue
					}
					fieldValue = extractedValue
				}

				// Schedule hash index update with actual pageID; pass commitSeq/versionSeq to avoid GetDocument.
				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", document.DocumentID, fieldValue, pageID, nil, false, document.CommitSequence, document.VersionSequence)
				s.logger.Debugf("Scheduled hash index '%s' update for document '%s' on field '%s' (page %d)",
					indexName, document.DocumentID, fieldName, pageID)

			} else if indexRef.IndexType == "btree" {
				// Extract the field value for BTree indexing
				fieldValue, err := extractFieldValueForIndex(*document, indexRef.BTreeIndexField.FieldName)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", document.DocumentID, err)
					continue
				}

				// Schedule BTree index update with actual pageID
				// deferred=false for ADD operations (read-your-own-writes consistency)
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", document.DocumentID, fieldValue, pageID, nil, false)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s' (page %d)",
					document.DocumentID, indexRef.BTreeIndexField.FieldName, pageID)
			}
		}
	}

	// WRITE-THROUGH CACHE: Immediately update page cache so reads see this document
	// Use the write-through helper which handles sharded locking and page creation
	s.updatePageCacheWithDocument(bundle.Name, pageID, document)
	s.logger.Debugf("Added document '%s' to page cache via write-through (page %d)", document.DocumentID, pageID)

	return nil
}

// filterDeletedDocuments efficiently filters out documents that were deleted between read and write phases
// This handles the race condition where DELETE can happen between releasing read lock and acquiring write lock
// Uses lightweight checks (memtable, sharded page cache, cached pages) to avoid expensive GetDocument calls
// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.
func (s *BundleService) filterDeletedDocuments(bundle *models.Bundle, documents []*models.Document) []*models.Document {
	if len(documents) == 0 {
		return documents
	}

	// Build set of document IDs for fast lookup
	docIDSet := make(map[string]bool, len(documents))
	for _, doc := range documents {
		docIDSet[doc.DocumentID] = true
	}

	// PHASE 5: Use sharded cache for page lookup - no global lock needed
	stillExists := make(map[string]bool)
	pagesToCheck := make(map[uint32]bool)

	// Get all cached page mappings for this bundle (returns copy, safe to iterate)
	bundlePages := s.documentPageCache.GetAllForBundle(bundle.Name)
	if bundlePages != nil {
		// Check which pages we need to verify
		for docID := range docIDSet {
			if !stillExists[docID] {
				if pageID, exists := bundlePages[docID]; exists {
					pagesToCheck[pageID] = true
				}
			}
		}
	}

	if len(pagesToCheck) > 0 {
		// Check cached pages (no I/O - just memory lookup)
		// DEADLOCK FIX: Use per-shard locking instead of global mutex
		for pageID := range pagesToCheck {
			pageKey := fmt.Sprintf("%s:%d", bundle.Name, pageID)
			shardIdx := s.getPageShardIndex(pageKey)
			shard := s.pageShards[shardIdx]

			// READER VIEW: Lock-free lookup first (no shard mutex)
			if v, ok := shard.readerView.Load(pageKey); ok {
				if safePage, ok := v.(*models.DocumentPage); ok {
					for docID := range docIDSet {
						if !stillExists[docID] {
							if _, docExists := safePage.Documents[docID]; docExists {
								stillExists[docID] = true
							}
						}
					}
					continue
				}
			}
			// Fallback: authoritative page under RLock
			if cached, ok := shard.fastLookup.Load(pageKey); ok {
				if page, ok := cached.(*models.DocumentPage); ok {
					shard.mu.RLock()
					safePage := s.createSafePageCopy(page)
					shard.mu.RUnlock()
					for docID := range docIDSet {
						if !stillExists[docID] {
							if _, docExists := safePage.Documents[docID]; docExists {
								stillExists[docID] = true
							}
						}
					}
				}
			}
		}
	}

	// Filter documents to only those that still exist
	filtered := make([]*models.Document, 0, len(documents))
	skippedCount := 0
	for _, doc := range documents {
		if stillExists[doc.DocumentID] {
			filtered = append(filtered, doc)
		} else {
			skippedCount++
			// DEBUG level: This is expected behavior under high concurrency, not an error
			// DELETEs can happen between read and write lock acquisition - we handle it gracefully
			s.logger.Debugf("Document '%s' was deleted between read and write phases, skipping", doc.DocumentID)
		}
	}

	// INFO level summary: Useful for monitoring contention patterns
	if skippedCount > 0 {
		s.logger.Debugf("Filtered out %d deleted document(s) between read and write phases (expected under high concurrency)", skippedCount)
	}

	return filtered
}

// UpdateDocumentInBundle updates documents in a bundle based on the update command.
// TASK 2: Document-level locking support - if lockInfo is provided, uses document locks instead of bundle write lock.
// PHASE 1: ctx may carry snapshot (e.g. planner.WithSnapshotInfo) for MVCC visibility; use context.Background() if none.
// RCU MODE: When settings.EnableRCUWrites is true, uses lock-free RCU updates for better concurrency.
func (s *BundleService) UpdateDocumentInBundle(ctx context.Context, database *models.Database, bundle *models.Bundle, docCommand *models.DocumentUpdateCommand, lockInfo ...*DocumentLockInfo) (err error) {
	// PERFORMANCE TIMING: Track where time is spent in UPDATE operations
	updateStart := time.Now()
	defer func() {
		totalTime := time.Since(updateStart)
		if totalTime > 500*time.Millisecond {
			s.logger.Infof("⚠️⚠️⚠️UPDATE SLOW: Total time %v for bundle '%s' WHERE '%s'",
				totalTime, docCommand.BundleName, docCommand.WhereClause)
		}
	}()

	if ctx == nil {
		ctx = context.Background()
	}
	args := settings.GetSettings()

	// RCU MODE: Use lock-free updates when enabled and no document-level locks provided
	// Document-level locks indicate transaction mode which needs the old lock-based path
	useRCU := args.EnableRCUWrites //&& len(lockInfo) == 0
	if useRCU {
		s.logger.Debugf("Using RCU (lock-free) update path for bundle '%s'", bundle.Name)
		// OCC Retry Loop: If we detect a write-write conflict, retry with a fresh snapshot
		maxRetries := args.MaxOCCRetries
		if maxRetries <= 0 {
			maxRetries = 3 // Default fallback
		}
		var lastErr error
		for attempt := 1; attempt <= maxRetries; attempt++ {
			lastErr = s.UpdateDocumentInBundleRCU(ctx, database, bundle, docCommand)
			if lastErr == nil {
				return nil // Success
			}
			if !stderrors.Is(lastErr, ErrWriteConflict) {
				// Non-conflict error, don't retry
				return lastErr
			}
			s.logger.Debugf("OCC: Write conflict on attempt %d/%d for bundle '%s', retrying with fresh snapshot",
				attempt, maxRetries, bundle.Name)
		}
		// All retries exhausted
		return fmt.Errorf("update failed after %d OCC retries: %w", maxRetries, lastErr)
	}
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot update document")
		return fmt.Errorf("bundle '%s' is nil, cannot update document", docCommand.BundleName)
	}

	// SAFETY CHECK: Bulk update (empty WHERE clause) requires CONFIRMED keyword
	if docCommand.WhereClause == "" || strings.TrimSpace(docCommand.WhereClause) == "" {
		if !docCommand.Confirmed {
			return fmt.Errorf("bulk update requires CONFIRMED keyword for safety. "+
				"Syntax: UPDATE DOCUMENTS IN BUNDLE \"%s\" (field = value, ...) CONFIRMED\n"+
				"This safety mechanism prevents accidental modification of all documents in a bundle. "+
				"Use a WHERE clause to update specific documents without CONFIRMED keyword",
				docCommand.BundleName)
		}
		s.logger.Debugf("Bulk update CONFIRMED for bundle '%s' - proceeding to update all documents", bundle.Name)
	}

	// PHASE 1.1: READ PHASE - Perform read operations under read lock
	// This allows concurrent reads during WHERE clause evaluation and FK validation
	if err = s.AcquireBundleReadLock(bundle.Name); err != nil {
		return fmt.Errorf("failed to acquire read lock: %w", err)
	}

	// TASK 1: Use query planner for WHERE clause processing (same fast path as SELECT)
	// OPTIMIZATION: If lockInfo contains pre-fetched document IDs AND we have actual locks,
	// use those directly to avoid duplicate WHERE clause evaluation.
	// IMPORTANT: Only use pre-fetched IDs when we have actual document locks (LockManager != nil)
	// because locks guarantee the IDs are still valid. Without locks, concurrent transactions
	// could delete/modify documents, making the IDs stale.
	var filteredDocs []*models.Document

	// TIMING: WHERE clause evaluation
	whereStart := time.Now()

	// Use pre-fetched docs when available (avoids GetDocument-by-ID and index→page lookup failures)
	li := len(lockInfo) > 0 && lockInfo[0] != nil
	hasLocks := li && lockInfo[0].LockManager != nil && len(lockInfo[0].LockedDocIDs) > 0
	hasPreFetched := li && len(lockInfo[0].PreFetchedDocs) > 0

	if hasLocks && hasPreFetched {
		// Use PreFetchedDocs from single WHERE scan; no GetDocument-by-ID. Ensures accurate updates.
		lockedSet := make(map[string]bool, len(lockInfo[0].LockedDocIDs))
		for _, id := range lockInfo[0].LockedDocIDs {
			lockedSet[id] = true
		}
		filteredDocs = make([]*models.Document, 0, len(lockInfo[0].PreFetchedDocs))
		for _, doc := range lockInfo[0].PreFetchedDocs {
			if doc != nil && lockedSet[doc.DocumentID] {
				filteredDocs = append(filteredDocs, doc)
			}
		}
		s.logger.Debugf("OPTIMIZATION: Used pre-fetched documents with locks (%d docs), no GetDocument-by-ID", len(filteredDocs))
	} else if hasPreFetched {
		// OCC path: PreFetchedDocs available but no locks (bundle-level lock used instead)
		// Use the pre-fetched docs directly to avoid re-running the WHERE clause
		filteredDocs = lockInfo[0].PreFetchedDocs
		s.logger.Debugf("OPTIMIZATION: Used pre-fetched documents from OCC (%d docs), no re-filtering", len(filteredDocs))
	} else if hasLocks && docCommand.WhereClause != "" && strings.TrimSpace(docCommand.WhereClause) != "" {
		// Locked IDs but no PreFetchedDocs (e.g. legacy caller): re-run WHERE via filter path.
		// Use GetDocumentsByFilter only; do not use query planner (avoids JOIN/aggregation on write-only UPDATE).
		filteredDocs, err = s.GetDocumentsByFilter(bundle, docCommand.WhereClause, nil)
		if err != nil {
			s.ReleaseBundleReadLock(bundle.Name)
			return fmt.Errorf("failed to filter documents: %w", err)
		}
	} else if docCommand.WhereClause != "" && docCommand.WhereClause != strings.TrimSpace("") {
		// Non-empty WHERE: use GetDocumentsByFilter only. Do not use query planner (avoids JOIN/aggregation on write-only UPDATE).
		filteredDocs, err = s.GetDocumentsByFilter(bundle, docCommand.WhereClause, nil)
		if err != nil {
			s.ReleaseBundleReadLock(bundle.Name)
			return fmt.Errorf("failed to filter documents: %w", err)
		}
	} else {
		// Empty WHERE clause - get all documents (only if CONFIRMED)
		// Fallback to GetDocumentsByFilter for this case
		filteredDocs, err = s.GetDocumentsByFilter(bundle, docCommand.WhereClause, nil)
		if err != nil {
			s.ReleaseBundleReadLock(bundle.Name)
			return fmt.Errorf("failed to filter documents: %w", err)
		}
	}

	// TIMING: Log WHERE clause evaluation time
	whereTime := time.Since(whereStart)
	if whereTime > 100*time.Millisecond {
		s.logger.Warnf("UPDATE TIMING: WHERE clause took %v for %d docs, bundle '%s'",
			whereTime, len(filteredDocs), bundle.Name)
	}

	if args.Debug {
		s.logger.Debugf("Updating %d documents from bundle '%s' with filter '%s'", len(filteredDocs), docCommand.BundleName, docCommand.WhereClause)
	}

	// Validate document update fields against bundle field definitions
	err = s.validateUpdateFields(bundle, docCommand)
	if err != nil {
		s.ReleaseBundleReadLock(bundle.Name)
		return fmt.Errorf("document field validation failed: %w", err)
	}

	// PHASE 1.2: Move FK validation to read lock phase (it only reads data)
	// ==========  VALIDATE REFERENTIAL INTEGRITY FOR FOREIGN KEY UPDATES ==========
	// Check if any fields being updated are foreign keys and validate the new values
	// Note: Must check BOTH outgoing relationships (stored in bundle.Relationships)
	//       AND incoming relationships (stored in other bundles pointing to this one)
	s.logger.Debugf("[REFINT-UPDATE] Starting FK validation for bundle '%s', database=%v, bundle.Relationships=%d",
		bundle.Name, database != nil, len(bundle.Relationships))

	var docIDs []string
	if len(bundle.Relationships) > 0 || database != nil {
		// Create operation-scoped validation cache to avoid redundant hash lookups
		validationCache := make(map[string]*ForeignKeyViolation)
		bundleCache := make(map[string]*models.Bundle)

		// Create validator
		validator := NewReferentialIntegrityValidator(s, s.logger)

		// Build map of field updates for easier lookup
		updateFields := make(map[string]string)
		for _, kv := range docCommand.Fields {
			if strValue, ok := kv.Value.(string); ok {
				updateFields[kv.Key] = strValue
			}
		}
		s.logger.Debugf("[REFINT-UPDATE] Update fields: %v", updateFields)

		// Identify which fields are foreign keys (checks BOTH directions)
		foreignKeyUpdates := validator.IdentifyForeignKeyFields(database, bundle, updateFields, bundleCache)
		s.logger.Debugf("[REFINT-UPDATE] Identified %d FK fields being updated", len(foreignKeyUpdates))

		if len(foreignKeyUpdates) > 0 {
			// Extract document IDs being updated
			docIDs = make([]string, len(filteredDocs))
			for i, doc := range filteredDocs {
				docIDs[i] = doc.DocumentID
			}
			s.logger.Debugf("[REFINT-UPDATE] Validating %d document(s): %v", len(docIDs), docIDs)

			// Perform batch validation with caching (under read lock - only reads data)
			violation := validator.batchValidateForeignKeys(bundle, docIDs, foreignKeyUpdates, validationCache)
			if violation != nil {
				s.ReleaseBundleReadLock(bundle.Name)
				// Log the violation at WARN level with suggested action
				s.logger.Warnf("[REFINT] %s | Suggested: %s", violation.Error(), violation.SuggestedAction)
				return fmt.Errorf("%s", violation.Error())
			}

			s.logger.Debugf("[REFINT] Foreign key validation passed for %d document(s) updating %d FK field(s)",
				len(docIDs), len(foreignKeyUpdates))
		}
	}

	// Collect document IDs for re-validation under write lock
	if docIDs == nil {
		docIDs = make([]string, len(filteredDocs))
		for i, doc := range filteredDocs {
			docIDs[i] = doc.DocumentID
		}
	}

	// Release read lock before acquiring write lock
	s.ReleaseBundleReadLock(bundle.Name)

	// TASK 2: Document-level locking - use document locks if provided, otherwise use bundle write lock.
	// MUST match document_operations.lockEscalationThreshold (100_000). Phase 5: no bundle-level locking for typical updates.
	var useDocumentLocks bool
	var lockedDocIDsSet map[string]bool
	const lockEscalationThreshold = 100_000

	if len(lockInfo) > 0 && lockInfo[0] != nil && len(lockInfo[0].LockedDocIDs) > 0 {
		if len(docIDs) <= lockEscalationThreshold {
			useDocumentLocks = true
			lockedDocIDsSet = make(map[string]bool, len(lockInfo[0].LockedDocIDs))
			for _, docID := range lockInfo[0].LockedDocIDs {
				lockedDocIDsSet[docID] = true
			}
			s.logger.Debugf("Using document-level locks for %d documents (below threshold %d)", len(docIDs), lockEscalationThreshold)
		} else {
			s.logger.Debugf("Lock escalation: %d documents exceeds threshold %d, using bundle write lock", len(docIDs), lockEscalationThreshold)
		}
	}

	// Acquire bundle write lock only if not using document locks or if lock escalation needed
	if !useDocumentLocks {
		if err = s.AcquireBundleWriteLock(bundle.Name); err != nil {
			return fmt.Errorf("failed to acquire write lock: %w", err)
		}
		defer s.ReleaseBundleWriteLock(bundle.Name)
	}

	// CRITICAL PERFORMANCE FIX: Use documents we already have from query planner
	// Re-fetching via GetDocument for each document ID was causing 2-20 second delays
	// However, we need to handle race condition: DELETE can happen between read and write lock acquisition
	// Solution: Do lightweight existence check using memtable and documentPageMap (O(1) lookups)
	// This is much faster than GetDocument which does I/O
	// TASK 2: If using document locks, filter to only documents that are locked
	if useDocumentLocks {
		// Filter to only documents that have locks acquired
		filteredLockedDocs := make([]*models.Document, 0, len(filteredDocs))
		for _, doc := range filteredDocs {
			if lockedDocIDsSet[doc.DocumentID] {
				filteredLockedDocs = append(filteredLockedDocs, doc)
			}
		}
		filteredDocs = filteredLockedDocs
	}
	filteredDocs = s.filterDeletedDocuments(bundle, filteredDocs)
	if len(filteredDocs) == 0 {
		// DEBUG level: This is expected behavior under high concurrency, not an error
		// Early return is efficient - avoids all update work when all documents were deleted
		s.logger.Debugf("All documents were deleted between read and write phases - skipping update (expected under high concurrency)")
		return nil // No documents to update
	}

	// TASK 3: Identify which fields have B-tree indexes for deferred update scheduling
	// We don't need to pre-load indexes since updates are deferred via scheduleIndexUpdate
	updatedFieldsSet := make(map[string]bool)
	for _, kv := range docCommand.Fields {
		updatedFieldsSet[kv.Key] = true
	}

	// Track which indexes need updates (for logging/debugging)
	btreeIndexesToUpdate := make(map[string]string) // indexName -> fieldName
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" {
				fieldName := indexRef.BTreeIndexField.FieldName
				if updatedFieldsSet[fieldName] {
					btreeIndexesToUpdate[indexName] = fieldName
				}
			}
		}
	}

	// TASK 3: B-tree index updates are now deferred via scheduleIndexUpdate
	// Rollback for deferred updates is handled by the index update buffer system
	// If document update fails, the deferred index updates won't be applied (they're in buffer, not yet executed)

	// PHASE 2b: Resolve txID for CreatedByTxID (VersionTxID or TxID from lockInfo)
	var createdByTxIDUint64 uint64
	if len(lockInfo) > 0 && lockInfo[0] != nil {
		tidStr := lockInfo[0].VersionTxID
		if tidStr == "" {
			tidStr = lockInfo[0].TxID
		}
		if tidStr != "" {
			_, _ = fmt.Sscanf(tidStr, "%016x", &createdByTxIDUint64)
		}
	}

	// R1: Per-doc loop: update fields and schedule deferred index updates. Collect updatedDocs; call UpdateDocumentsBatch once after.
	// TIMING: Per-document processing
	perDocStart := time.Now()
	updatedDocs := make([]*models.Document, 0, len(filteredDocs))
	for _, doc := range filteredDocs {
		originalDoc := *doc

		// Avoid concurrent map read/write: doc.Fields may be shared with memtable or
		// page cache (from GetDocumentsByFilter). Copy so we only mutate our own map;
		// other goroutines can still read the original until UpdateDocumentsBatch replaces it.
		newFields := make(map[string]models.Field, len(doc.Fields))
		for k, v := range doc.Fields {
			newFields[k] = v
		}
		doc.Fields = newFields

		// Update the document fields
		for _, kv := range docCommand.Fields {
			foundField := doc.Fields[kv.Key]
			foundField.Name = kv.Key
			foundField.Value = models.NewInterfaceValue(kv.Value)
			doc.Fields[kv.Key] = foundField
		}

		// Rebuild pre-encoded JSON cache after field mutations
		helpers.BuildCachedJSON(doc)

		// TASK 3: Use deferred index updates for B-tree indexes instead of synchronous operations
		// This reduces per-document index overhead by batching updates
		for indexName, fieldName := range btreeIndexesToUpdate {
			s.logger.Debugf("Indexed field '%s' was updated, scheduling deferred BTree index '%s' update", fieldName, indexName)

			oldFieldValue, extErr := extractFieldValueForIndex(originalDoc, fieldName)
			if extErr != nil {
				s.logger.Warnf("Failed to extract old field value for document '%s': %v", doc.DocumentID, extErr)
				continue
			}

			newFieldValue, extErr := extractFieldValueForIndex(*doc, fieldName)
			if extErr != nil {
				s.logger.Warnf("Failed to extract new field value for document '%s': %v", doc.DocumentID, extErr)
				continue
			}

			// PHASE 5: Get pageID using sharded cache (no global lock)
			pageID, _ := s.documentPageCache.GetPageID(bundle.Name, doc.DocumentID)

			// TASK 3: Schedule deferred B-tree index updates (delete old, insert new)
			// The indexUpdateBuffer will batch these and apply them later via flushIndexUpdates
			// This reduces per-document index overhead significantly
			// deferred=true for UPDATE operations (batch performance, no read-your-own-writes needed)
			// Schedule delete for old value
			s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "delete", doc.DocumentID, oldFieldValue, pageID, nil, true)
			// Schedule insert for new value
			s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", doc.DocumentID, newFieldValue, pageID, nil, true)
			s.logger.Debugf("Scheduled deferred B-tree index updates (delete+insert) for document '%s' on field '%s'", doc.DocumentID, fieldName)
		}

		// PHASE 2b: MVCC version metadata for new version (append-only semantics)
		if doc.VersionSequence == 0 {
			doc.VersionSequence = 1
		} else {
			doc.VersionSequence++
		}
		doc.CreatedByTxID = createdByTxIDUint64
		doc.CommitSequence = 0

		updatedDocs = append(updatedDocs, doc)
	}

	// TIMING: Log per-document processing time
	perDocTime := time.Since(perDocStart)
	if perDocTime > 100*time.Millisecond {
		s.logger.Warnf("UPDATE TIMING: Per-doc loop took %v for %d docs, bundle '%s'",
			perDocTime, len(updatedDocs), bundle.Name)
	}

	// TASK 3: B-tree index updates are now deferred via scheduleIndexUpdate
	// The indexUpdateBuffer will batch these updates and apply them later via flushIndexUpdates
	// This reduces per-document index overhead significantly
	// No need to apply B-tree operations here - they're scheduled for deferred execution
	// Each document with B-tree indexed fields schedules 2 updates (delete old + insert new) per index
	if len(btreeIndexesToUpdate) > 0 && len(updatedDocs) > 0 {
		totalBTreeUpdates := len(btreeIndexesToUpdate) * len(updatedDocs) * 2 // 2 = delete + insert per update
		s.logger.Debugf("Scheduled %d B-tree index updates (delete+insert) for deferred execution (will be batched)", totalBTreeUpdates)
	}

	// TIMING: UpdateDocumentsBatch
	batchStart := time.Now()

	// R1: Single UpdateDocumentsBatch for all updated docs (was N calls to UpdateDocumentInBundleFile).
	// R7 audit: UpdateDocumentInBundle holds AcquireBundleWriteLock (application) before this call, which
	// acquires getWriteLock (storage) inside UpdateDocumentsBatch. Lock order: application then storage.
	// Other callers of UpdateDocumentInBundleFile: applyDefaultToExistingDocuments, removeFieldFromExistingDocuments,
	// renameFieldInDocuments, convertFieldType, applyDefaultToMissingField (via ApplyFieldChanges). Those do not
	// hold AcquireBundleWriteLock; they use only the storage lock. No deadlock: no path holds application then
	// waits on storage while another holds storage then waits on application.
	//
	// DOCUMENT-LEVEL LOCKING: Use UpdateDocumentsBatchWithLocks ONLY when caller has ACTUAL pre-acquired document locks
	// (LockManager != nil). If lockInfo only contains docIDs for optimization (LockManager == nil), use bundle lock.
	// P0d: Pass preLockedDocIDs that exactly match updatedDocs (same length, same IDs) to avoid fallback to bundle lock.
	if len(lockInfo) > 0 && lockInfo[0] != nil && lockInfo[0].LockManager != nil && len(lockInfo[0].LockedDocIDs) > 0 {
		updatedDocIDs := make([]string, 0, len(updatedDocs))
		for _, d := range updatedDocs {
			updatedDocIDs = append(updatedDocIDs, d.DocumentID)
		}
		err = s.store.UpdateDocumentsBatchWithLocks(bundle, updatedDocs, updatedDocIDs)
		if err != nil {
			return fmt.Errorf("failed to update documents with document locks: %w", err)
		}
		s.logger.Debugf("Updated %d documents using document-level locks", len(updatedDocs))
	} else {
		// Fall back to bundle-level lock (either no lockInfo, or lockInfo only has docIDs for optimization)
		err = s.store.UpdateDocumentsBatch(bundle, updatedDocs)
		if err != nil {
			return fmt.Errorf("failed to update documents in bundle: %w", err)
		}
	}

	// TIMING: Log UpdateDocumentsBatch time
	batchTime := time.Since(batchStart)
	if batchTime > 100*time.Millisecond {
		s.logger.Warnf("UPDATE TIMING: UpdateDocumentsBatch took %v for %d docs, bundle '%s'",
			batchTime, len(updatedDocs), bundle.Name)
	}

	// Invalidate documentPageMap for each updated docID (pageID is stale after UPDATE).
	for _, d := range updatedDocs {
		s.invalidateDocumentPageMapEntry(bundle.Name, d.DocumentID)
	}

	// WRITE-THROUGH CACHE: Update page cache with new document versions immediately
	// This replaces the old invalidation approach - instead of invalidating and forcing
	// a disk read, we update the cache directly so reads see the new data immediately
	for _, doc := range updatedDocs {
		// Look up the pageID from documentPageMap (before it was invalidated)
		// If not found, the update will just skip - cache will load from disk on next read
		pageID, err := s.findDocumentPage(bundle.Name, doc.DocumentID)
		if err == nil {
			s.updatePageCacheWithDocument(bundle.Name, pageID, doc)
		}
	}
	s.logger.Debugf("Write-through: Updated %d documents in page cache for bundle '%s'", len(updatedDocs), bundle.Name)

	// Update column statistics for updated fields
	if s.statsUpdater != nil {
		for _, doc := range updatedDocs {
			if doc.Fields == nil {
				continue
			}
			for fieldName, field := range doc.Fields {
				s.statsUpdater.IncrementalUpdate(
					bundle.Name, fieldName,
					nil, // oldValue unavailable after in-place mutation
					field.Value.AsInterface(),
					bundle.TotalDocuments,
				)
			}
		}
	}

	// NOTE: Metadata updates (TotalDocuments, PageCount) are handled via scheduleMetadataUpdate
	// which is atomic and uses its own mutex. No bundle write lock needed here.
	// The deferred metadata flush handles concurrent access safely.

	// TASK 3: Flush deferred index updates for this bundle only (P4a scoped flush)
	if len(btreeIndexesToUpdate) > 0 {
		s.flushIndexUpdatesForBundle(bundle.Name)
		s.logger.Debugf("Flushed deferred B-tree index updates for bundle %s", bundle.Name)
	}

	// PERFORMANCE: Invalidate JOIN hash table cache for this bundle
	// This ensures subsequent JOINs rebuild hash tables with updated data
	s.invalidateBundleCaches(bundle.Name)

	// TODO(P5): Memtable-first return — return after memtable+WAL, persist WriteBuffer→disk in background.
	// Requires explicit durability policy and recovery design. See plan §5 / §8.
	return nil
}

// ============================================================================
// OCC (Optimistic Concurrency Control) Write-Write Conflict Detection
// ============================================================================

// ErrWriteConflict is returned when a document was modified by another transaction
// after our snapshot was taken. The caller should retry with a fresh snapshot.
var ErrWriteConflict = fmt.Errorf("write-write conflict: document modified by concurrent transaction")

// ValidateNoConflict checks that none of the documents to be updated have been
// modified since the snapshot was taken. This is the "validate" phase of OCC.
//
// For each document, we verify that doc.CommitSequence <= snapshotSequence.
// If any document has a higher CommitSequence, it means another transaction
// committed a new version after our snapshot, causing a write-write conflict.
//
// Parameters:
//   - documents: Documents that passed the WHERE filter and will be updated
//   - snapshotSequence: The commit sequence boundary from our snapshot
//
// Returns:
//   - nil if no conflicts detected (safe to proceed with update)
//   - ErrWriteConflict if any document was modified after snapshot
func (s *BundleService) ValidateNoConflict(documents []*models.Document, snapshotSequence uint64) error {
	for _, doc := range documents {
		// Check if document was modified after our snapshot
		// CommitSequence > snapshotSequence means a newer version was committed
		if doc.CommitSequence > snapshotSequence {
			s.logger.Debugf("OCC conflict detected: document %s has CommitSequence %d > snapshot %d",
				doc.DocumentID, doc.CommitSequence, snapshotSequence)
			return ErrWriteConflict
		}
	}
	return nil
}

// ============================================================================
// RCU (Read-Copy-Update) UPDATE - Lock-Free Concurrent Writes
// ============================================================================
//
// UpdateDocumentInBundleRCU implements lock-free updates using Read-Copy-Update pattern:
//
// 1. Read: Find documents matching WHERE clause (no lock needed for read-committed)
// 2. Copy: Create new document versions with updated fields
// 3. Update: Append new versions to storage (concurrent-safe append)
// 4. Swap: Atomically update index pointers to new locations
// 5. Mark: Set SupersededAt on old versions for cleanup
//
// This eliminates the bundle-wide write lock that was causing serialization.
// PostgreSQL-inspired: Writers don't block writers, readers see consistent state.
//
// Performance: ~60x faster than lock-based approach at high concurrency
// - Old: 30ms @ 150 concurrent updates (serialized)
// - New: ~0.5ms @ 150 concurrent updates (parallel)
//
// Trade-offs:
// - Brief staleness window (~100ms grace period)
// - Old versions consume space until VACUUM
// - Last-writer-wins conflict resolution
func (s *BundleService) UpdateDocumentInBundleRCU(ctx context.Context, database *models.Database, bundle *models.Bundle, docCommand *models.DocumentUpdateCommand) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if bundle == nil {
		return fmt.Errorf("bundle '%s' is nil, cannot update document", docCommand.BundleName)
	}

	// SAFETY CHECK: Bulk update requires CONFIRMED keyword
	if (docCommand.WhereClause == "" || strings.TrimSpace(docCommand.WhereClause) == "") && !docCommand.Confirmed {
		return fmt.Errorf("bulk update requires CONFIRMED keyword for safety")
	}

	// SNAPSHOT ISOLATION: Create snapshot for autocommit operation
	// This enables lock-free WHERE evaluation with consistent visibility
	serviceRegistry := registry.GetRegistry()
	var snapshotSequence uint64
	var activeTxIDs map[uint64]bool

	if walManager := serviceRegistry.GetWALManager(); walManager != nil {
		if snapshotMgrFull := walManager.GetSnapshotManager(); snapshotMgrFull != nil {
			// Get current sequence as snapshot boundary (autocommit uses txID=0)
			snapshotSequence = snapshotMgrFull.GetCurrentSequence()
			// For autocommit, no active transactions to exclude
			activeTxIDs = make(map[uint64]bool)
		}
	}

	// Create snapshot info and add to context for lock-free visibility filtering
	args := settings.GetSettings()
	snapshotInfo := &models.SnapshotInfo{
		SnapshotSequence: snapshotSequence,
		TransactionID:    0, // txID=0 for autocommit
		ActiveTxIDs:      activeTxIDs,
		GracePeriodMs:    args.RCUGracePeriodMs,
	}
	ctx = models.WithSnapshotInfo(ctx, snapshotInfo)

	// STEP 1: READ - Find documents matching WHERE clause using snapshot isolation
	// Lock-free read with MVCC visibility filtering
	filteredDocs, err := s.GetDocumentsByFilterWithContext(ctx, bundle, docCommand.WhereClause, nil)
	if err != nil {
		return fmt.Errorf("failed to filter documents: %w", err)
	}

	if len(filteredDocs) == 0 {
		s.logger.Debugf("RCU UPDATE: No documents match WHERE clause, nothing to update")
		return nil
	}

	s.logger.Debugf("RCU UPDATE: Found %d documents to update in bundle '%s'", len(filteredDocs), bundle.Name)

	// Validate update fields against bundle schema
	if err := s.validateUpdateFields(bundle, docCommand); err != nil {
		return fmt.Errorf("field validation failed: %w", err)
	}

	// Get hash index for DocumentID (for atomic pointer swap)
	var hashIndex interface{}
	if bundle.Indexes != nil {
		if indexRef, exists := bundle.Indexes["DocumentID_idx"]; exists && indexRef.IndexType == "hash" {
			hashIndex = indexRef.IndexInstance
		}
	}

	// STEP 2-5: For each document, create new version and swap atomically
	successCount := 0

	// Get SnapshotManager for CommitSequence allocation
	// Reuse the registry lookup from above
	var snapshotMgr interface {
		GetNextCommitSequence() uint64
	}
	if walManager := serviceRegistry.GetWALManager(); walManager != nil {
		snapshotMgr = walManager.GetSnapshotManager()
	}

	// OCC VALIDATION: Check for write-write conflicts before modifying any documents
	// This is the "validate" phase of Optimistic Concurrency Control.
	// If any document was modified after our snapshot, return ErrWriteConflict
	// so the caller can retry with a fresh snapshot.
	if err := s.ValidateNoConflict(filteredDocs, snapshotSequence); err != nil {
		return err // ErrWriteConflict - caller should retry
	}

	for _, oldDoc := range filteredDocs {
		// Documents already filtered by IsVisibleToSnapshot in GetDocumentsByFilterWithContext
		// Double-check with read-committed for any in-flight changes
		if !oldDoc.IsVisibleReadCommitted() {
			s.logger.Debugf("RCU UPDATE: Skipping document %s - no longer visible", oldDoc.DocumentID)
			continue
		}

		// STEP 2: COPY - Create new document with updated fields
		newDoc := s.createUpdatedDocumentRCU(oldDoc, docCommand.Fields)

		// Get next commit sequence for this version (atomic, globally ordered)
		var commitSequence uint64
		if snapshotMgr != nil {
			commitSequence = snapshotMgr.GetNextCommitSequence()
		} else {
			// Fallback: Use nanosecond timestamp (less ideal but functional)
			commitSequence = uint64(time.Now().UnixNano())
		}

		// STEP 3: UPDATE - Append new version to storage using RCU path (no lock needed)
		// AppendVersionToBundleFile handles: VersionSequence increment, CommitSequence assignment
		pageID, err := s.store.AppendVersionToBundleFile(bundle, newDoc, oldDoc, commitSequence)
		if err != nil {
			s.logger.Warnf("RCU UPDATE: Failed to append new version for %s: %v", newDoc.DocumentID, err)
			continue
		}

		// STEP 4: SWAP - Atomically update index pointer to new location
		if hashIndex != nil {
			if idx, ok := hashIndex.(interface {
				UpdatePageLocation(keyValue, documentID string, newPageID uint32, commitSequence uint64) error
			}); ok {
				if err := idx.UpdatePageLocation(newDoc.DocumentID, newDoc.DocumentID, pageID, newDoc.CommitSequence); err != nil {
					s.logger.Warnf("RCU UPDATE: Failed to update index for %s: %v", newDoc.DocumentID, err)
					// Continue anyway - document is on disk, just index is stale
				}
			}
		}

		// STEP 5: MARK - Set SupersededAt on old version for cleanup
		// Note: oldDoc points to page cache - this marks it for VACUUM
		oldDoc.MarkSuperseded()

		// Update page cache with new document (write-through)
		s.updatePageCacheWithDocument(bundle.Name, pageID, newDoc)

		// Invalidate old page mapping (force re-lookup)
		s.invalidateDocumentPageMapEntry(bundle.Name, newDoc.DocumentID)

		successCount++
	}

	if successCount == 0 {
		return fmt.Errorf("RCU UPDATE: No documents were updated")
	}

	s.logger.Debugf("RCU UPDATE: Successfully updated %d/%d documents in bundle '%s'",
		successCount, len(filteredDocs), bundle.Name)

	// Invalidate JOIN caches for this bundle
	s.invalidateBundleCaches(bundle.Name)

	return nil
}

// createUpdatedDocumentRCU creates a new document version with updated fields
// This is the "Copy" step of RCU - creates an immutable new version
func (s *BundleService) createUpdatedDocumentRCU(oldDoc *models.Document, updates []models.KeyValue) *models.Document {
	// Create deep copy of old document
	newDoc := &models.Document{
		DocumentID:   oldDoc.DocumentID,
		CreatedAt:    oldDoc.CreatedAt,
		UpdatedAt:    time.Now(),
		PooledFields: false, // New allocation, not pooled

		// RCU fields - CommitSequence and VersionSequence are set by AppendVersionToBundleFile
		CommitSequence: 0,           // Set by AppendVersionToBundleFile from SnapshotManager
		SupersededAt:   time.Time{}, // Zero = current version

		// Legacy fields for compatibility
		CreatedByTxID:   0,
		DeletedByTxID:   0,
		VersionSequence: 0, // Set by AppendVersionToBundleFile (oldDoc.VersionSequence + 1)
	}

	// Copy fields from old document
	newDoc.Fields = make(map[string]models.Field, len(oldDoc.Fields))
	for k, v := range oldDoc.Fields {
		newDoc.Fields[k] = v
	}

	// Apply updates
	for _, kv := range updates {
		field := newDoc.Fields[kv.Key]
		field.Name = kv.Key
		field.Value = models.NewInterfaceValue(kv.Value)
		newDoc.Fields[kv.Key] = field
	}

	// Rebuild pre-encoded JSON cache after field mutations
	helpers.BuildCachedJSON(newDoc)

	// CommitSequence is now set by AppendVersionToBundleFile using SnapshotManager.GetNextCommitSequence()
	// This ensures globally ordered, atomic commit sequence allocation

	return newDoc
}

// ============================================================================
// RCU (Read-Copy-Update) DELETE - Lock-Free Concurrent Deletes
// ============================================================================
//
// DeleteDocumentFromBundleRCU implements lock-free deletes using RCU pattern:
//
// 1. Validate: Check referential integrity (FK constraints still enforced)
// 2. Find: Locate documents matching WHERE clause (no lock needed)
// 3. Mark: Write deletion tombstones (append-only, concurrent-safe)
// 4. Index: Atomically update indexes to mark as deleted
// 5. Cleanup: Mark documents as superseded for VACUUM
//
// This eliminates the bundle-wide write lock that was causing serialization.
// DELETE already uses append-only tombstones which are naturally RCU-compatible.
//
// Performance: ~60x faster than lock-based approach at high concurrency
// - Old: 30ms @ 150 concurrent deletes (serialized)
// - New: ~0.5ms @ 150 concurrent deletes (parallel)
func (s *BundleService) DeleteDocumentFromBundleRCU(bundle *models.Bundle, docCommand *models.DocumentDeleteCommand, docIDs []string, preFetchedDocs []*models.Document) error {
	args := settings.GetSettings()

	if args.Debug {
		s.logger.Debugf("RCU DELETE: Starting lock-free deletion from bundle '%s' with WHERE clause: %s",
			docCommand.BundleName, docCommand.WhereClause)
	}

	// ========== STEP 1: VALIDATE - Check for bulk delete safety ==========
	if (docCommand.WhereClause == "" || strings.TrimSpace(docCommand.WhereClause) == "") && len(docIDs) == 0 {
		if !docCommand.Confirmed {
			return fmt.Errorf("bulk delete requires CONFIRMED keyword for safety")
		}

		// Get all document IDs for bulk delete
		allDocs, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
		if err != nil {
			return fmt.Errorf("failed to retrieve documents for bulk delete: %w", err)
		}

		if len(allDocs) == 0 {
			s.logger.Debugf("RCU DELETE: Bundle '%s' is already empty", bundle.Name)
			docCommand.DeletedDocumentIDs = []string{}
			return nil
		}

		docIDs = make([]string, 0, len(allDocs))
		for _, doc := range allDocs {
			docIDs = append(docIDs, doc.DocumentID)
		}
		preFetchedDocs = allDocs
	}

	if len(docIDs) == 0 {
		s.logger.Debugf("RCU DELETE: No documents to delete")
		return nil
	}

	// ========== STEP 2: VALIDATE REFERENTIAL INTEGRITY ==========
	// FK validation is still required even in RCU mode
	fkStart := time.Now()
	validator := NewReferentialIntegrityValidator(s, s.logger)
	if err := validator.ValidateBulkDeleteOptimized(bundle, docIDs); err != nil {
		return fmt.Errorf("referential integrity: %w", err)
	}
	fkTime := time.Since(fkStart)
	if fkTime > 100*time.Millisecond {
		s.logger.Warnf("RCU DELETE TIMING: FK validation took %v for %d docs", fkTime, len(docIDs))
	}

	// ========== STEP 3: HARVEST - Collect index keys before deletion ==========
	harvestStart := time.Now()
	// Build lookup map from pre-fetched docs
	var docIDToDoc map[string]*models.Document
	if len(preFetchedDocs) > 0 {
		docIDToDoc = make(map[string]*models.Document, len(preFetchedDocs))
		for _, d := range preFetchedDocs {
			if d != nil && d.DocumentID != "" {
				docIDToDoc[d.DocumentID] = d
			}
		}
	}

	// Harvest B-tree keys and commit sequences
	btreeKeys := make(map[string]map[string][]byte)
	commitSeqs := make(map[string]uint64)
	harvestFailedDocIDs := make(map[string]struct{})

	const harvestSkipThreshold = 500
	if bundle.Indexes != nil && bundle.Database != nil {
		if len(docIDs) > harvestSkipThreshold && docIDToDoc == nil {
			for _, docID := range docIDs {
				harvestFailedDocIDs[docID] = struct{}{}
			}
			s.logger.Debugf("RCU DELETE: Skipped harvest for %d docs (>%d threshold)", len(docIDs), harvestSkipThreshold)
		} else {
			for _, docID := range docIDs {
				var doc *models.Document
				if docIDToDoc != nil {
					doc = docIDToDoc[docID]
				}
				if doc == nil {
					var err error
					doc, err = s.GetDocument(bundle.Name, bundle.Database.Name, docID)
					if err != nil {
						harvestFailedDocIDs[docID] = struct{}{}
						continue
					}
				}
				commitSeqs[docID] = doc.CommitSequence

				// Mark document as superseded for VACUUM cleanup
				doc.MarkSuperseded()

				// Harvest B-tree keys
				for indexName, indexRef := range bundle.Indexes {
					if indexRef.IndexType != "btree" {
						continue
					}
					fieldName := indexRef.BTreeIndexField.FieldName
					fv, err := extractFieldValueForIndex(*doc, fieldName)
					if err != nil {
						continue
					}
					kb, err := convertValueToBytes(fv)
					if err != nil {
						continue
					}
					if btreeKeys[docID] == nil {
						btreeKeys[docID] = make(map[string][]byte)
					}
					btreeKeys[docID][indexName] = kb
				}
			}
		}
	}

	harvestTime := time.Since(harvestStart)
	if harvestTime > 100*time.Millisecond {
		s.logger.Warnf("RCU DELETE TIMING: Harvest took %v for %d docs", harvestTime, len(docIDs))
	}

	// ========== STEP 4: WRITE TOMBSTONES (No Lock Required) ==========
	tombstoneStart := time.Now()
	// Flush write buffer first to ensure pending writes are on disk
	if err := s.store.FlushWriteBuffers(docCommand.BundleName); err != nil {
		s.logger.Warnf("RCU DELETE: Failed to flush write buffers: %v", err)
	}

	// Append deletion markers (tombstones) - this is already concurrent-safe
	// No bundle lock needed - append is atomic at file level
	if err := s.store.AppendDeletionMarkersBatch(bundle, docIDs); err != nil {
		return fmt.Errorf("failed to append deletion markers: %w", err)
	}
	s.logger.Debugf("RCU DELETE: Wrote %d deletion markers", len(docIDs))

	// Close write buffer to ensure tombstones are visible
	if err := s.store.CloseWriteBuffer(docCommand.BundleName); err != nil {
		s.logger.Warnf("RCU DELETE: Failed to close write buffer: %v", err)
	}

	tombstoneTime := time.Since(tombstoneStart)
	if tombstoneTime > 100*time.Millisecond {
		s.logger.Warnf("RCU DELETE TIMING: Tombstones took %v for %d docs", tombstoneTime, len(docIDs))
	}

	// ========== STEP 5: UPDATE INDEXES (Atomic Operations) ==========
	indexStart := time.Now()
	// Pre-load indexes once
	hashIndexes := make(map[string]*hashindex.HashIndexV3)
	btreeIndexes := make(map[string]*btreeindexV2.BTreeIndex)
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				idx, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err == nil {
					hashIndexes[indexName] = idx
				}
			} else if indexRef.IndexType == "btree" {
				idx, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err == nil {
					btreeIndexes[indexName] = idx
				}
			}
		}
	}

	// Delete from indexes - these operations are atomic per-entry
	// Track which documents had successful B-tree deletes (via harvested keys)
	btreeDeletedDocs := make(map[string]bool)
	for _, documentID := range docIDs {
		for indexName, hashIndex := range hashIndexes {
			commitSeq := commitSeqs[documentID]
			if _, err := hashIndex.Delete(documentID, commitSeq); err != nil {
				s.logger.Warnf("RCU DELETE: Failed to delete '%s' from hash index '%s': %v", documentID, indexName, err)
			}
		}

		for indexName, btreeIndex := range btreeIndexes {
			if m := btreeKeys[documentID]; m != nil {
				if keyBytes, ok := m[indexName]; ok {
					if err := btreeIndex.Delete(keyBytes, documentID); err != nil {
						s.logger.Warnf("RCU DELETE: Failed to delete '%s' from B-tree '%s': %v", documentID, indexName, err)
					} else {
						btreeDeletedDocs[documentID] = true
					}
				}
			}
		}
	}

	// Cleanup B-tree entries ONLY for documents where harvest failed (no keys extracted)
	// This avoids redundant work for documents already deleted above
	if len(harvestFailedDocIDs) > 0 {
		failedIDs := make([]string, 0, len(harvestFailedDocIDs))
		for docID := range harvestFailedDocIDs {
			if !btreeDeletedDocs[docID] {
				failedIDs = append(failedIDs, docID)
			}
		}
		if len(failedIDs) > 0 {
			for _, btreeIndex := range btreeIndexes {
				if btreeIndex != nil {
					btreeIndex.DeleteByDocumentIDs(failedIDs)
				}
			}
			s.logger.Debugf("RCU DELETE: Cleaned up %d B-tree entries via fallback path", len(failedIDs))
		}
	}

	indexTime := time.Since(indexStart)
	if indexTime > 100*time.Millisecond {
		s.logger.Warnf("RCU DELETE TIMING: Index updates took %v for %d docs", indexTime, len(docIDs))
	}

	// Update SortedIndex
	if bundle.SortedIndex != nil {
		for _, documentID := range docIDs {
			bundle.SortedIndex.Delete(documentID)
		}
	}

	// ========== STEP 6: INVALIDATE CACHES ==========
	// PHASE 5: Use sharded cache for invalidation
	invalidatedPages := make(map[uint32]bool)
	for _, docID := range docIDs {
		// Get page ID before invalidating the cache entry
		if pageID, found := s.documentPageCache.GetPageID(docCommand.BundleName, docID); found {
			if !invalidatedPages[pageID] {
				pageKey := fmt.Sprintf("%s:%d", docCommand.BundleName, pageID)
				shardIdx := s.getPageShardIndex(pageKey)
				shard := s.pageShards[shardIdx]
				shard.mu.Lock()
				shard.deleteLocked(pageKey)
				shard.mu.Unlock()
				invalidatedPages[pageID] = true
				// Clear visibility map bit for deleted page
				s.clearVisibilityForPage(docCommand.BundleName, pageID)
			}
		}
		// Invalidate the document->page cache entry
		s.documentPageCache.InvalidateDocument(docCommand.BundleName, docID)
	}

	// Invalidate scanner cache
	if scanner, exists := s.bundleScanners[docCommand.BundleName]; exists {
		if smartScanner, ok := scanner.(*documentscanner.SmartBundleScanner); ok {
			smartScanner.RemoveDocumentsFromCache(docIDs)
		} else {
			s.RemoveDocumentScanner(docCommand.BundleName)
		}
	}

	// Invalidate JOIN caches
	s.invalidateBundleCaches(docCommand.BundleName)

	// ========== STEP 7: SET RESPONSE ==========
	docCommand.DeletedDocumentIDs = docIDs

	s.logger.Debugf("RCU DELETE: Successfully deleted %d documents from bundle '%s'", len(docIDs), bundle.Name)
	return nil
}

// DeleteDocumentFromBundle is the public interface for deleting documents from a bundle.
// When lockInfo is provided with LockManager and LockedDocIDs, skips bundle write lock (Phase 4: document-level locking).
// preFetchedDocs: when provided (e.g. from GetDocumentsAndIDsByFilter), harvest uses these and skips GetDocument in the harvest loop.
// RCU MODE: When settings.EnableRCUWrites is true, uses lock-free RCU deletes for better concurrency.
func (s *BundleService) DeleteDocumentFromBundle(bundle *models.Bundle, docCommand *models.DocumentDeleteCommand, docIDs []string, preFetchedDocs []*models.Document, lockInfo ...*DocumentLockInfo) error {
	args := settings.GetSettings()

	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot delete document")
		return fmt.Errorf("bundle '%s' is nil, cannot delete document", docCommand.BundleName)
	}

	// RCU MODE: Use lock-free deletes when enabled and no document-level locks provided
	// Document-level locks indicate transaction mode which needs the old lock-based path
	useRCU := args.EnableRCUWrites && len(lockInfo) == 0
	if useRCU {
		s.logger.Debugf("Using RCU (lock-free) delete path for bundle '%s'", bundle.Name)
		return s.DeleteDocumentFromBundleRCU(bundle, docCommand, docIDs, preFetchedDocs)
	}

	useDocLocks := len(lockInfo) > 0 && lockInfo[0] != nil && lockInfo[0].LockManager != nil && len(lockInfo[0].LockedDocIDs) > 0
	if !useDocLocks {
		if err := s.AcquireBundleWriteLock(bundle.Name); err != nil {
			return fmt.Errorf("failed to acquire write lock: %w", err)
		}
		defer s.ReleaseBundleWriteLock(bundle.Name)
	}

	if args.Debug {
		s.logger.Debugf("Starting document deletion from bundle '%s' with WHERE clause: %s",
			docCommand.BundleName, docCommand.WhereClause)
	}

	// ========== FIND DOCUMENTS MATCHING WHERE CLAUSE ==========

	// STEP 2 Notes: Check for empty WHERE clause (bulk delete all documents)
	// If docIDs are explicitly provided, skip this check - the caller knows what they're doing
	if (docCommand.WhereClause == "" || strings.TrimSpace(docCommand.WhereClause) == "") && len(docIDs) == 0 {
		// Bulk delete requires CONFIRMED keyword for safety
		if !docCommand.Confirmed {
			return fmt.Errorf("bulk delete requires CONFIRMED keyword for safety. "+
				"Syntax: DELETE FROM \"%s\" CONFIRMED\n"+
				"This safety mechanism prevents accidental data loss when deleting all documents in a bundle. "+
				"The operation will validate referential integrity and cascade deletes as configured",
				docCommand.BundleName)
		}

		// Get all document IDs from the bundle
		allDocs, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
		if err != nil {
			return fmt.Errorf("failed to retrieve documents for bulk delete: %w", err)
		}

		if len(allDocs) == 0 {
			s.logger.Debugf("Bundle '%s' is already empty, nothing to delete", bundle.Name)
			docCommand.DeletedDocumentIDs = []string{}
			return nil
		}

		// Extract document IDs for validation and deletion
		bulkDocIDs := make([]string, 0, len(allDocs))
		for _, doc := range allDocs {
			bulkDocIDs = append(bulkDocIDs, doc.DocumentID)
		}

		s.logger.Debugf("Bulk delete will validate and delete %d documents from bundle '%s'", len(bulkDocIDs), bundle.Name)

		// Perform batch referential integrity validation
		validator := NewReferentialIntegrityValidator(s, s.logger)
		if err := validator.ValidateBulkDeleteOptimized(bundle, bulkDocIDs); err != nil {
			return fmt.Errorf("bulk delete failed referential integrity check: %w", err)
		}

		// All validations passed - proceed with deletion using internal method (lock already held)
		// Skip individual metadata updates - we'll do a single bulk update after all deletions.
		// Pass allDocs as preFetchedDocs so harvest avoids N×GetDocument.
		if err := s.deleteDocumentsInternal(bundle, docCommand, bulkDocIDs, true, allDocs); err != nil {
			return fmt.Errorf("bulk delete execution failed: %w", err)
		}

		// CRITICAL FIX: Do NOT schedule decrement_docs metadata update
		// In append-only storage, tombstones are still entries on disk, so TotalDocuments
		// should represent total document entries (including tombstones), not active documents.
		// See DeleteDocumentFromBundleFile for detailed explanation.
		// s.scheduleMetadataUpdate(docCommand.BundleName, "decrement_docs", int64(len(bulkDocIDs))) // REMOVED: Causes corruption

		// Flush all pending operations to ensure consistency
		if err := s.FlushAllBuffers(); err != nil {
			s.logger.Warnf("Failed to flush buffers after bulk delete: %v", err)
		}

		s.logger.Debugf("Successfully deleted %d documents from bundle '%s'", len(bulkDocIDs), bundle.Name)
		return nil
	}

	// D9: Use ValidateBulkDeleteOptimized for all deletes (not only >10). Removes N×ValidateDelete
	// (N×GetBundleByName + N×GetOrLoadHashIndex per relationship) for 1–10 doc deletes.
	s.logger.Debugf("[REFINT] Starting referential integrity validation for %d document(s) in bundle '%s'", len(docIDs), bundle.Name)
	validator := NewReferentialIntegrityValidator(s, s.logger)
	if err := validator.ValidateBulkDeleteOptimized(bundle, docIDs); err != nil {
		return fmt.Errorf("referential integrity: %w", err)
	}
	s.logger.Debugf("[REFINT] Referential integrity validated successfully for %d document(s) in bundle '%s'", len(docIDs), bundle.Name)

	// Delegate to internal delete logic (lock already held when useDocLocks)
	// Pass lockInfo when using document-level locks so we skip storage bundle lock (AppendDeletionMarkersBatchWithLocks).
	return s.deleteDocumentsInternal(bundle, docCommand, docIDs, false, preFetchedDocs, lockInfo...)
}

// deleteDocumentsInternal performs the actual document deletion logic without acquiring locks.
// When lockInfo is provided with LockedDocIDs, uses AppendDeletionMarkersBatchWithLocks (no bundle-level lock).
//
// Parameters:
//   - skipMetadataUpdate: if true, caller is responsible for scheduling metadata updates (used for bulk operations)
//   - preFetchedDocs: when non-nil, harvest uses these for B-tree keys and commitSeqs instead of GetDocument
//   - lockInfo: optional; when present with LockedDocIDs, storage skips bundle write lock (Phase 4).
func (s *BundleService) deleteDocumentsInternal(bundle *models.Bundle, docCommand *models.DocumentDeleteCommand, docIDs []string, skipMetadataUpdate bool, preFetchedDocs []*models.Document, lockInfo ...*DocumentLockInfo) error {
	// D1: Use AppendDeletionMarkersBatch for all deletes (threshold 1). Removes N×DeleteDocumentFromBundleFile,
	// N×verifyDocumentExistsStreaming, N×appendDeletionMarker, N×lock. Delete is idempotent: tombstones for
	// non-existent docs are acceptable in append-only storage; callers should pass IDs from a valid WHERE result.
	if len(docIDs) == 0 {
		return nil
	}

	// D6 (Phase 1a): Harvest B-tree index keys BEFORE in-memory removal. GetDocument after removal is
	// semantically broken (doc already cleared). Use WHERE result; here we GetDocument while docs are still
	// in bundle.Documents/documentPageMap. If harvest fails for a doc, we skip B-tree delete for that doc.
	// A: Log at Debug to avoid WARN storms. B: Hash delete for this docID still runs in the index cleanup loop.
	// C: B-tree entries for harvest-failed docIDs are removed via DeleteByDocumentIDs after the per-doc loop.
	//
	// Large-delete optimization: when len(docIDs) > harvestSkipThreshold, skip the harvest loop entirely.
	// N×GetDocument (each with findDocumentPage, GetDocumentPage, possible eviction) causes timeouts and
	// documentPages contention. Put all docIDs in harvestFailedDocIDs; C (DeleteByDocumentIDs) does one
	// full B-tree scan per index instead. Trade: N×GetDocument vs M×fullScan (M = number of B-trees).
	const harvestSkipThreshold = 500
	btreeKeys := make(map[string]map[string][]byte)  // docID -> indexName -> keyBytes
	harvestFailedDocIDs := make(map[string]struct{}) // C: docIDs where GetDocument failed; DeleteByDocumentIDs will clean B-trees
	commitSeqs := make(map[string]uint64)            // docID -> CommitSequence for hash index Delete; 0 when harvest skipped or not found
	// Build docIDToDoc from preFetchedDocs when provided so harvest can skip GetDocument
	var docIDToDoc map[string]*models.Document
	if len(preFetchedDocs) > 0 {
		docIDToDoc = make(map[string]*models.Document, len(preFetchedDocs))
		for _, d := range preFetchedDocs {
			if d != nil && d.DocumentID != "" {
				docIDToDoc[d.DocumentID] = d
			}
		}
	}
	if bundle.Indexes != nil && bundle.Database != nil {
		// Skip harvest loop only when over threshold AND no preFetchedDocs (avoids N×GetDocument)
		if len(docIDs) > harvestSkipThreshold && docIDToDoc == nil {
			for _, docID := range docIDs {
				harvestFailedDocIDs[docID] = struct{}{}
			}
			s.logger.Debugf("B-tree harvest: skipped for %d docs (>%d); C (DeleteByDocumentIDs) will clean B-trees", len(docIDs), harvestSkipThreshold)
		} else {
			for _, docID := range docIDs {
				var doc *models.Document
				if docIDToDoc != nil {
					if d, ok := docIDToDoc[docID]; ok {
						doc = d
					}
				}
				if doc == nil {
					var err error
					doc, err = s.GetDocument(bundle.Name, bundle.Database.Name, docID)
					if err != nil {
						harvestFailedDocIDs[docID] = struct{}{}
						s.logger.Debugf("B-tree harvest: failed to load document '%s': %v; B will clean hash, C will clean B-tree", docID, err)
						continue
					}
				}
				commitSeqs[docID] = doc.CommitSequence // PERF: harvest once for hash index cleanup; avoids N×GetDocument in hash loop
				for indexName, indexRef := range bundle.Indexes {
					if indexRef.IndexType != "btree" {
						continue
					}
					fieldName := indexRef.BTreeIndexField.FieldName
					fv, err := extractFieldValueForIndex(*doc, fieldName)
					if err != nil {
						s.logger.Warnf("B-tree harvest: extract %s for %s: %v; skipping", fieldName, docID, err)
						continue
					}
					kb, err := convertValueToBytes(fv)
					if err != nil {
						s.logger.Warnf("B-tree harvest: convert for %s: %v; skipping", docID, err)
						continue
					}
					if btreeKeys[docID] == nil {
						btreeKeys[docID] = make(map[string][]byte)
					}
					btreeKeys[docID][indexName] = kb
				}
			}
		}
	}

	// Flush write buffer FIRST so pending ADDs/UPDATEs are on disk before tombstones (D7: keep FlushWriteBuffers).
	// Otherwise a crash could leave a tombstone for a "never existed" doc.
	err := s.store.FlushWriteBuffers(docCommand.BundleName)
	if err != nil {
		s.logger.Warnf("Failed to flush write buffers before delete: %v", err)
	}

	// Write ALL deletion markers in one batch. D2: delete is idempotent.
	// When lockInfo has LockedDocIDs, use WithLocks to skip storage bundle lock (no bundle-level locking).
	if len(lockInfo) > 0 && lockInfo[0] != nil && len(lockInfo[0].LockedDocIDs) > 0 {
		err = s.store.AppendDeletionMarkersBatchWithLocks(bundle, docIDs, lockInfo[0].LockedDocIDs)
	} else {
		err = s.store.AppendDeletionMarkersBatch(bundle, docIDs)
	}
	if err != nil {
		return fmt.Errorf("failed to append batch deletion markers: %w", err)
	}
	s.logger.Debugf("DELETE: Wrote %d deletion markers to disk", len(docIDs))
	// Observability (Phase 1b): log batch delete. TODO: metrics.DeleteBatchDuration, DeleteVerifySkipCount, requested vs deleted.
	s.logger.Infow("Delete batch", "bundle", docCommand.BundleName, "docCount", len(docIDs))

	// Update column statistics for deleted documents
	if s.statsUpdater != nil && docIDToDoc != nil {
		for _, doc := range docIDToDoc {
			if doc == nil || doc.Fields == nil {
				continue
			}
			for fieldName, field := range doc.Fields {
				s.statsUpdater.IncrementalUpdate(
					bundle.Name, fieldName,
					field.Value.AsInterface(),
					nil, // newValue: nil for DELETE
					bundle.TotalDocuments,
				)
			}
		}
	}

	// Close the write buffer so subsequent opens see the correct file size (including tombstones).
	// Subsequent ADDs will recreate the buffer. Documented in plan §1.5 CloseWriteBuffer.
	err = s.store.CloseWriteBuffer(docCommand.BundleName)
	if err != nil {
		s.logger.Warnf("Failed to close write buffer after tombstones: %v", err)
	}

	// D5: In-memory and cache invalidation once per batch (unify bulk and non-bulk). Remove from all structures
	// after disk write to maintain durability.
	// CRITICAL ORDERING: Delete from indexes FIRST (Step 1), then invalidate caches (Step 2).
	// This ensures queries immediately see documents as missing via hash index lookup,
	// avoiding slow page scans when documentPageMap entries are removed.

	// STEP 1: Delete from all indexes IMMEDIATELY (before cache invalidation)
	// D6: Pre-load each index once per batch (Phase 2). TODO: batched B-tree Delete and BatchDelete for hash.
	hashIndexes := make(map[string]*hashindex.HashIndexV3)
	btreeIndexes := make(map[string]*btreeindexV2.BTreeIndex)
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				idx, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Errorf("Failed to load hash index '%s': %v", indexName, err)
					continue
				}
				hashIndexes[indexName] = idx
			} else if indexRef.IndexType == "btree" {
				idx, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Errorf("Failed to load BTree index '%s': %v", indexName, err)
					continue
				}
				btreeIndexes[indexName] = idx
			}
		}
	}

	// Process each document for index cleanup (hash Delete, B-tree Delete using harvested keys).
	for _, documentID := range docIDs {
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
					hashIndex := hashIndexes[indexName]
					if hashIndex == nil {
						continue
					}
					// PHASE 4: MVCC - use commitSeq harvested in D6 (or 0 when harvest skipped / doc not found)
					commitSeq := commitSeqs[documentID]
					deleted, err := hashIndex.Delete(documentID, commitSeq)
					if err != nil {
						s.logger.Warnf("Failed to delete DocumentID '%s' from hash index '%s': %v", documentID, indexName, err)
					} else if deleted {
						s.logger.Debugf("Successfully deleted DocumentID '%s' from hash index '%s'", documentID, indexName)
					}
				} else if indexRef.IndexType == "btree" {
					btreeIndex := btreeIndexes[indexName]
					if btreeIndex == nil {
						continue
					}
					if m := btreeKeys[documentID]; m != nil {
						if keyBytes, ok := m[indexName]; ok {
							err := btreeIndex.Delete(keyBytes, documentID)
							if err != nil {
								s.logger.Warnf("Failed to delete document '%s' from BTree index '%s': %v", documentID, indexName, err)
							} else {
								s.logger.Debugf("Successfully deleted document '%s' from BTree index '%s'", documentID, indexName)
							}
						}
					}
				}
			}
		}
	}

	// C: ALWAYS run DeleteByDocumentIDs for ALL deleted docIDs to clean any stale B-tree entries.
	for _, btreeIndex := range btreeIndexes {
		if btreeIndex != nil {
			n, err := btreeIndex.DeleteByDocumentIDs(docIDs)
			if err != nil {
				s.logger.Warnf("B-tree DeleteByDocumentIDs cleanup: %v", err)
			} else if n > 0 {
				s.logger.Debugf("B-tree DeleteByDocumentIDs: removed %d total entries for deleted docIDs", n)
			}
		}
	}

	// Update SortedIndex to decrement count for COUNT(*) optimization
	if bundle.SortedIndex != nil {
		for _, documentID := range docIDs {
			if bundle.SortedIndex.Delete(documentID) {
				s.logger.Debugf("Removed DocumentID '%s' from SortedIndex", documentID)
			}
		}
	}

	// NOTE: Hash index flush to disk is deferred to avoid I/O bottleneck on every delete.
	// In-memory deletions above are sufficient for query correctness.
	// Indexes will be flushed during: clean shutdown, background compaction, or batch completion.
	// If server crashes, indexes may be stale but will be rebuilt automatically on next load.

	// STEP 2: WRITE-THROUGH CACHE: Invalidate pages containing deleted documents
	//    We invalidate entire pages to ensure deleted documents are not visible to subsequent reads.
	//    On next read, pages will be reloaded from disk with tombstone filtering applied.
	// PHASE 5: Use sharded cache for invalidation
	invalidatedPages := make(map[uint32]bool) // Track unique pages to invalidate
	for _, docID := range docIDs {
		if pageID, found := s.documentPageCache.GetPageID(docCommand.BundleName, docID); found {
			if !invalidatedPages[pageID] {
				// Invalidate the entire page containing this document
				pageKey := fmt.Sprintf("%s:%d", docCommand.BundleName, pageID)
				shardIdx := s.getPageShardIndex(pageKey)
				shard := s.pageShards[shardIdx]
				shard.mu.Lock()
				shard.deleteLocked(pageKey)
				shard.mu.Unlock()
				invalidatedPages[pageID] = true
				// Clear visibility map bit for deleted page
				s.clearVisibilityForPage(docCommand.BundleName, pageID)
			}
		}
		// Invalidate the document->page cache entry
		s.documentPageCache.InvalidateDocument(docCommand.BundleName, docID)
	}
	s.logger.Debugf("Write-through: Invalidated %d pages containing %d deleted documents for bundle '%s'",
		len(invalidatedPages), len(docIDs), docCommand.BundleName)

	// 3. Remove deleted docs from scanner's cache when SmartBundleScanner; keep scanner alive to
	//    avoid 20–30s stall on next SELECT (new scanner would have empty cachedPages + cold
	//    documentPages → full reload from disk). Only tear down when we can't do targeted invalidation.
	if scanner, exists := s.bundleScanners[docCommand.BundleName]; exists {
		if smartScanner, ok := scanner.(*documentscanner.SmartBundleScanner); ok {
			smartScanner.RemoveDocumentsFromCache(docIDs)
			// do NOT call RemoveDocumentScanner — keep scanner and its cachedPages
		} else {
			s.RemoveDocumentScanner(docCommand.BundleName)
		}
	}

	// PERFORMANCE: Invalidate all caches for this bundle (query planner + JOIN hash tables)
	s.invalidateBundleCaches(docCommand.BundleName)

	// STEP 7: Update command with deleted document IDs for response
	docCommand.DeletedDocumentIDs = docIDs //deletedDocumentIDs

	return nil
}

// DeleteAllDocumentsFromBundle performs a bulk delete of all documents in a bundle with referential integrity checks
// This method implements the CONFIRMED bulk delete operation: DELETE FROM "BundleName" CONFIRMED
//
// Performance: Uses batch validation with HashIndexV3.BatchGet() for O(1) parallel lookups
// Safety: Requires CONFIRMED keyword (validated by caller) and performs full referential integrity validation
// Transaction: Caller must wrap in WAL transaction at server layer for atomicity
//
// Error Format: Returns aggregated violation counts (e.g., "423 references in 'Books' via 'author_id'")
// instead of individual document errors for better UX with large datasets.
//
// TODO: I will add configurable soft-delete flag for bulk operations to enable tombstone-only mode
// with background compaction instead of immediate physical deletion
func (s *BundleService) DeleteAllDocumentsFromBundle(
	docCommand *models.DocumentDeleteCommand,
	bundle *models.Bundle,
) error {
	args := settings.GetSettings()
	if args.Debug {
		s.logger.Debugf("Starting bulk delete of all documents from bundle '%s'", docCommand.BundleName)
	}

	// Acquire write lock for the bundle
	if err := s.AcquireBundleWriteLock(bundle.Name); err != nil {
		return fmt.Errorf("failed to acquire write lock for bundle '%s': %w", bundle.Name, err)
	}
	defer s.ReleaseBundleWriteLock(bundle.Name)

	// Get all document IDs from the bundle
	allDocs, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
	if err != nil {
		return fmt.Errorf("failed to retrieve documents for bulk delete: %w", err)
	}

	if len(allDocs) == 0 {
		s.logger.Debugf("Bundle '%s' is already empty, nothing to delete", bundle.Name)
		docCommand.DeletedDocumentIDs = []string{}
		return nil
	}

	// Extract document IDs for validation and deletion
	docIDs := make([]string, 0, len(allDocs))
	for _, doc := range allDocs {
		docIDs = append(docIDs, doc.DocumentID)
	}

	s.logger.Debugf("Bulk delete will validate and delete %d documents from bundle '%s'", len(docIDs), bundle.Name)

	// Perform batch referential integrity validation
	validator := NewReferentialIntegrityValidator(s, s.logger)
	if err := validator.ValidateBulkDeleteOptimized(bundle, docIDs); err != nil {
		return fmt.Errorf("bulk delete failed referential integrity check: %w", err)
	}

	// All validations passed - proceed with deletion using internal method (lock already held)
	// Skip individual metadata updates - we'll do a single bulk update after all deletions.
	// Pass allDocs as preFetchedDocs so harvest avoids N×GetDocument.
	if err := s.deleteDocumentsInternal(bundle, docCommand, docIDs, true, allDocs); err != nil {
		return fmt.Errorf("bulk delete execution failed: %w", err)
	}

	// CRITICAL FIX: Do NOT schedule decrement_docs metadata update
	// In append-only storage, tombstones are still entries on disk, so TotalDocuments
	// should represent total document entries (including tombstones), not active documents.
	// See DeleteDocumentFromBundleFile for detailed explanation.
	// s.scheduleMetadataUpdate(docCommand.BundleName, "decrement_docs", int64(len(docIDs))) // REMOVED: Causes corruption

	// Flush all pending operations to ensure consistency
	if err := s.FlushAllBuffers(); err != nil {
		s.logger.Warnf("Failed to flush buffers after bulk delete: %v", err)
	}

	s.logger.Debugf("Successfully deleted %d documents from bundle '%s'", len(docIDs), bundle.Name)
	return nil
}

// GetDocumentByID retrieves a document by its ID using the hash index for fast lookup
//
// LOCK-FREE (Phase 1): No bundle read lock required. This function reads only from:
// 1. Hash index (thread-safe via its own internal locking)
// 2. Page cache via GetDocument() (uses sharded sync.Map with lock-free lookups)
// MVCC visibility is ensured by IsVisibleReadCommitted() filtering in the page cache layer.
func (s *BundleService) GetDocumentByID(bundle *models.Bundle, documentID string) (*models.Document, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle is nil")
	}

	// Try to use hash index for fast lookup first
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Load hash index on-demand
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load hash index '%s': %v", indexName, err)
					// Fall back to linear search
					break
				}

				results, _, err := hashIndex.Search(documentID)
				if err != nil {
					s.logger.Warnf("Failed to search hash index '%s' for DocumentID '%s': %v",
						indexName, documentID, err)
					// Fall back to linear search
					break
				}

				if len(results) > 0 {
					// Found in index, now get the actual document using page-based loading
					return s.GetDocument(bundle.Name, bundle.Database.Name, documentID)
				} else {
					// Not found in index
					return nil, fmt.Errorf("document with ID '%s' not found", documentID)
				}
			}
		}
	}

	// Fall back to page-based document lookup if hash index is not available or failed
	return s.GetDocument(bundle.Name, bundle.Database.Name, documentID)
}

// getDocumentsByQueryPlanner uses the unified query planner to get documents matching a WHERE clause.
// This provides the same fast index-optimized execution path as SELECT statements.
// PHASE 1: ctx may carry snapshot (planner.WithSnapshotInfo) for MVCC visibility; use Background if nil.
func (s *BundleService) getDocumentsByQueryPlanner(ctx context.Context, bundle *models.Bundle, whereClause string, database *models.Database) ([]*models.Document, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// PERFORMANCE: Check if unified planner is available using lock-free atomic load
	planner := getUnifiedPlanner()
	if planner == nil {
		s.logger.Debugf("Unified planner not available, falling back to GetDocumentsByFilter")
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	expr, err := syndrQL.ParseExpression(whereClause)
	if err != nil {
		s.logger.Warnf("Failed to parse WHERE clause as Expression, falling back to GetDocumentsByFilter: %v", err)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	query := &queryparser.UnifiedSelectQuery{
		QueryType:       queryparser.SimpleQuery,
		FromBundle:      bundle.Name,
		WhereExpression: expr,
		SelectFields:    []string{},
	}

	planInterface, err := planner.CreatePlan(query, database)
	if err != nil {
		s.logger.Warnf("Failed to create execution plan, falling back to GetDocumentsByFilter: %v", err)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	type executablePlan interface {
		Execute(ctx context.Context) (interface{}, error)
	}

	plan, ok := planInterface.(executablePlan)
	if !ok {
		s.logger.Warnf("Plan type %T does not implement Execute method, falling back to GetDocumentsByFilter", planInterface)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	// Join cleanup: same key as planner.JoinCleanupContextKey ("join_cleanup"); avoid planner import (cycle).
	joinCleanupFns := []func(){}
	ctx = context.WithValue(ctx, "join_cleanup", &joinCleanupFns)
	defer func() {
		for _, fn := range joinCleanupFns {
			fn()
		}
	}()

	result, err := plan.Execute(ctx)
	if err != nil {
		s.logger.Warnf("Failed to execute plan, falling back to GetDocumentsByFilter: %v", err)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	// Type assert result to map[string]*models.Document
	documentsMap, ok := result.(map[string]*models.Document)
	if !ok {
		s.logger.Warnf("Plan execution returned unexpected type %T, falling back to GetDocumentsByFilter", result)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	// Convert map to slice
	documents := make([]*models.Document, 0, len(documentsMap))
	for _, doc := range documentsMap {
		documents = append(documents, doc)
	}

	s.logger.Debugf("Query planner returned %d documents for WHERE clause: %s", len(documents), whereClause)
	return documents, nil
}

// GetDocumentsByFilterWithContext retrieves documents using MVCC snapshot isolation.
// This is the lock-free version that uses snapshot visibility instead of bundle read locks.
//
// SNAPSHOT ISOLATION: When a snapshot is present in the context, documents are filtered
// using IsVisibleToSnapshot() which provides full MVCC visibility semantics:
// - Read-your-own-writes for the current transaction
// - Only see documents committed before snapshot boundary
// - Invisible to concurrent transactions that were active at snapshot time
//
// LOCK-FREE: This function does NOT acquire bundle read locks. Visibility is guaranteed
// by the snapshot's commit sequence boundary. This enables true lock-free reads for RCU.
//
// Parameters:
//   - ctx: Context containing optional SnapshotInfo for MVCC visibility
//   - bundle: The bundle to filter documents from
//   - whereParts: The WHERE clause string for filtering
//   - session: Optional session for transaction context (nil for non-transactional reads)
//
// Returns:
//   - []*models.Document: Array of documents matching the filter criteria and visible to snapshot
//   - error: Any error that occurred during filtering
func (s *BundleService) GetDocumentsByFilterWithContext(ctx context.Context, bundle *models.Bundle, whereParts string, session SessionInterface) ([]*models.Document, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Validate input parameters
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot filter documents")
		return nil, fmt.Errorf("bundle is nil, cannot filter documents")
	}

	// Get snapshot info from context for MVCC visibility filtering
	snapshotInfo := models.GetSnapshotInfoFromContext(ctx)

	// LOCK-FREE: When snapshot is present, we use MVCC visibility instead of locks.
	// When no snapshot AND session has active transaction, fall back to old lock behavior
	// for write buffer coordination.
	needsBundleLock := snapshotInfo == nil && session != nil && session.IsInTransaction()
	if needsBundleLock {
		if err := s.AcquireBundleReadLock(bundle.Name); err != nil {
			return nil, fmt.Errorf("failed to acquire read lock: %w", err)
		}
		defer s.ReleaseBundleReadLock(bundle.Name)
	}

	// PERFORMANCE: Only flush metadata if buffer is non-empty
	s.metadataUpdateMutex.RLock()
	needsFlush := len(s.metadataUpdateBuffer) > 0
	s.metadataUpdateMutex.RUnlock()
	if needsFlush {
		s.FlushMetadataUpdates()
	}

	// Clear projection for full document loading
	s.SetProjectionFieldsForBundle(bundle.Name, nil)

	// Get buffered documents if in transaction
	var bufferedDocs []*models.Document
	if session != nil && session.IsInTransaction() {
		var err error
		bufferedDocs, err = s.store.GetBufferedDocumentsForTransaction(
			bundle.Name,
			session.GetActiveTransactionID(),
		)
		if err != nil {
			s.logger.Warnf("Failed to get buffered documents for transaction %s: %v",
				session.GetActiveTransactionID(), err)
		} else if len(bufferedDocs) > 0 {
			s.logger.Debugf("Found %d buffered documents for transaction %s in bundle %s",
				len(bufferedDocs), session.GetActiveTransactionID(), bundle.Name)
		}
	}

	// Get disk documents
	var diskDocs []*models.Document
	var err error
	if whereParts == "" {
		diskDocs, err = s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
	} else {
		diskDocs, err = s.filterDocumentsWithIndexOptimization(bundle, nil, whereParts)
	}
	if err != nil {
		return nil, err
	}

	// Merge buffered docs
	allDocs := s.mergeDocuments(diskDocs, bufferedDocs)

	// MVCC VISIBILITY FILTERING: When snapshot is present, filter by visibility
	if snapshotInfo != nil {
		visibleDocs := make([]*models.Document, 0, len(allDocs))
		for _, doc := range allDocs {
			if doc.IsVisibleToSnapshot(snapshotInfo.SnapshotSequence, snapshotInfo.TransactionID, snapshotInfo.ActiveTxIDs, snapshotInfo.GracePeriodMs) {
				visibleDocs = append(visibleDocs, doc)
			}
		}
		return visibleDocs, nil
	}

	// No snapshot - return all documents (legacy read-committed behavior)
	return allDocs, nil
}

// GetDocumentsByFilter retrieves documents from a bundle based on filter criteria
// This function follows the Single Responsibility Principle by handling only document filtering
// Following SyndrDB comprehensive error handling, it optimizes queries using available indexes
//
// LOCK-FREE (Phase 1): When session is nil or has no active transaction, no bundle read lock
// is required. The page cache uses sharded sync.Map with lock-free lookups, and MVCC visibility
// filtering ensures we only return committed, current documents.
// When session has an active transaction, we still need coordination for write buffer access,
// which is provided by the storage engine's bufferMutex.RLock() in GetBufferedDocumentsForTransaction.
//
// Parameters:
//   - bundle: The bundle to filter documents from
//   - whereParts: The WHERE clause string for filtering
//   - session: Optional session for transaction context (nil for non-transactional reads)
//
// Returns:
//   - []*models.Document: Array of documents matching the filter criteria
//   - error: Any error that occurred during filtering
func (s *BundleService) GetDocumentsByFilter(bundle *models.Bundle, whereParts string, session SessionInterface) ([]*models.Document, error) {
	// Validate input parameters following SyndrDB defensive programming practices
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot filter documents")
		return nil, fmt.Errorf("bundle is nil, cannot filter documents")
	}

	// LOCK-FREE (Phase 1): Only acquire bundle read lock when session has active transaction.
	// Non-transactional reads use lock-free page cache lookups with MVCC visibility filtering.
	// Transactional reads need coordination for write buffer access (handled by storage engine).
	needsBundleLock := session != nil && session.IsInTransaction()
	if needsBundleLock {
		if err := s.AcquireBundleReadLock(bundle.Name); err != nil {
			return nil, fmt.Errorf("failed to acquire read lock: %w", err)
		}
		defer s.ReleaseBundleReadLock(bundle.Name)
	}

	// PERFORMANCE: Only flush metadata if buffer is non-empty (avoid write lock contention)
	// Use RLock to check buffer size without blocking other readers
	s.metadataUpdateMutex.RLock()
	needsFlush := len(s.metadataUpdateBuffer) > 0
	s.metadataUpdateMutex.RUnlock()
	if needsFlush {
		// Only flush if actually needed - this avoids write lock contention under high concurrency
		s.FlushMetadataUpdates()
	}

	// CRITICAL: Clear any per-bundle projection so we load full documents.
	// A prior ORDER BY (or similar) sets projection on the storage engine and never clears it.
	// readDocumentRange(nil) then uses that projection and returns partial docs, so WHERE
	// on non-projected fields (e.g. category) fails. Clearing here ensures both the
	// index path (GetDocument) and full-scan path get full docs.
	// PHASE 4.2: Note - SetProjectionFieldsForBundle already efficiently handles nil (just deletes from map)
	// The mutex acquisition is necessary for thread safety, so optimization here would require
	// adding a public method to check projection state, which adds complexity for minimal gain.
	s.SetProjectionFieldsForBundle(bundle.Name, nil)

	// Get buffered documents if in transaction
	var bufferedDocs []*models.Document
	if session != nil && session.IsInTransaction() {
		var err error
		bufferedDocs, err = s.store.GetBufferedDocumentsForTransaction(
			bundle.Name,
			session.GetActiveTransactionID(),
		)
		if err != nil {
			s.logger.Warnf("Failed to get buffered documents for transaction %s: %v",
				session.GetActiveTransactionID(), err)
			// Continue with disk-only - don't fail query
		} else if len(bufferedDocs) > 0 {
			s.logger.Debugf("Found %d buffered documents for transaction %s in bundle %s",
				len(bufferedDocs), session.GetActiveTransactionID(), bundle.Name)
		}
	}

	// If no WHERE clause, return all documents (disk + buffered)
	if whereParts == "" {
		//s.logger.Debugf("DEBUG: GetDocumentsByFilter - empty filter, calling getAllDocumentsForIndexing")
		diskDocs, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
		if err != nil {
			return nil, err
		}
		//s.logger.Debugf("DEBUG: GetDocumentsByFilter - getAllDocumentsForIndexing returned %d documents, error: %v", len(result), err)
		return s.mergeDocuments(diskDocs, bufferedDocs), nil
	}

	// CRITICAL: Use index-optimized filtering following SyndrDB performance optimization
	// This replaces the direct queryparser.FilterDocuments call with index-aware filtering
	//s.logger.Debugf("DEBUG: GetDocumentsByFilter - non-empty filter, calling filterDocumentsWithIndexOptimization")
	diskDocs, err := s.filterDocumentsWithIndexOptimization(bundle, nil, whereParts)
	if err != nil {
		return nil, err
	}

	// If we have buffered docs, filter them and merge with disk results
	if len(bufferedDocs) > 0 {
		filteredBuffered, err := s.filterBufferedDocuments(bufferedDocs, whereParts)
		if err != nil {
			s.logger.Warnf("Failed to filter buffered documents: %v", err)
			// Fall back to just disk docs
			return diskDocs, nil
		}
		return s.mergeDocuments(diskDocs, filteredBuffered), nil
	}

	//s.logger.Debugf("DEBUG: GetDocumentsByFilter - filterDocumentsWithIndexOptimization returned %d documents, error: %v", len(result), err)
	return diskDocs, nil
}

// mergeDocuments combines disk and buffered documents, avoiding duplicates
func (s *BundleService) mergeDocuments(diskDocs []*models.Document, bufferedDocs []*models.Document) []*models.Document {
	if len(bufferedDocs) == 0 {
		return diskDocs
	}

	// Build set of disk document IDs for duplicate checking
	diskIDs := make(map[string]bool, len(diskDocs))
	for _, doc := range diskDocs {
		diskIDs[doc.DocumentID] = true
	}

	// Start with disk docs, add buffered docs that aren't duplicates
	result := make([]*models.Document, len(diskDocs), len(diskDocs)+len(bufferedDocs))
	copy(result, diskDocs)

	for _, doc := range bufferedDocs {
		if !diskIDs[doc.DocumentID] {
			result = append(result, doc)
		}
	}

	return result
}

// filterBufferedDocuments applies WHERE clause filtering to buffered documents using SyndrQL
func (s *BundleService) filterBufferedDocuments(docs []*models.Document, whereParts string) ([]*models.Document, error) {
	return s.filterDocumentsWithSyndrQL(docs, whereParts)
}

// filterDocumentsWithSyndrQL filters documents using the SyndrQL expression parser and evaluator.
// This is the preferred method for filtering documents with WHERE clauses.
// Uses cached expression parsing for performance.
func (s *BundleService) filterDocumentsWithSyndrQL(docs []*models.Document, whereClause string) ([]*models.Document, error) {
	if whereClause == "" {
		return docs, nil
	}

	// Use SyndrQL expression parser with caching
	expr, err := syndrQL.ParseExpressionCached(whereClause, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	if expr == nil {
		return docs, nil
	}

	// Create evaluator for filtering
	evaluator := syndrQL.NewExpressionEvaluator(s.logger)

	var result []*models.Document
	for _, doc := range docs {
		matches, err := evaluator.EvaluateAsBool(expr, doc, nil, nil, nil)
		if err != nil {
			s.logger.Debugf("Expression evaluation failed for doc %s: %v", doc.DocumentID, err)
			continue
		}
		if matches {
			result = append(result, doc)
		}
	}
	return result, nil
}

// filterDocumentsWithIndexOptimization performs intelligent document filtering using available indexes
// This function Handles only index-optimized filtering
// Following SyndrDB modular development practices, it coordinates between indexes and query parsing
// Parameters:
//   - bundle: The bundle containing the documents and indexes
//   - docs: The documents to filter
//   - whereClause: The WHERE clause for filtering
//
// Returns:
//   - []*models.Document: Filtered documents
//   - error: Any error that occurred during filtering
func (s *BundleService) filterDocumentsWithIndexOptimization(bundle *models.Bundle, docs []*models.Document, whereClause string) ([]*models.Document, error) {
	// TODO This whole function may not be necessary anymore with the new execution model
	// Validate input parameters
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	if whereClause == "" {
		return docs, nil
	}

	// Enhanced logging following SyndrDB comprehensive error handling
	s.logger.Debugf("Starting index-optimized filtering for bundle '%s'", bundle.Name)
	s.logger.Debugf("Available indexes: %d", len(bundle.Indexes))

	// Log available indexes for debugging
	for indexName, indexRef := range bundle.Indexes {
		s.logger.Debugf("  Index '%s': type=%s, field=%s",
			indexName, indexRef.IndexType, s.getIndexFieldName(indexRef))
	}

	// Try to use hash indexes first for optimal performance
	// Following SyndrDB performance optimization, prioritize fastest index types
	if result, used, err := s.tryHashIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("Hash index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used hash index optimization, found %d documents", len(result))
		return result, nil
	}

	// Try to use BTree indexes for range queries and equality
	// Following SyndrDB modular development, handle different index types appropriately
	if result, used, err := s.tryBTreeIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("BTree index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used BTree index optimization, found %d documents", len(result))
		return result, nil
	}

	// Try AND-aware index optimization for compound conditions like:
	// ("category" == "Gaming Accessories") AND ("rating" <= 3.5)
	// This uses the best indexed clause to narrow results, then filters by remaining clauses
	if result, used, err := s.tryANDIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("AND index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used AND index optimization, found %d documents", len(result))
		return result, nil
	}

	// Fallback to streaming/chunked full scan (no load-all). Required for non-indexed WHERE
	// to match PostgreSQL-like throughput; adding indexes is not a viable product fix.
	s.logger.Debugf("No suitable index found, performing streaming full scan")
	s.logger.Infow("GetDocumentsByFilter: no suitable index, using streaming full scan",
		"bundle", bundle.Name,
		"whereClause", whereClause,
	)

	return s.getDocumentsByFilterStreaming(bundle, whereClause)
}

// getDocumentsByFilterStreaming streams pages, filters each chunk, and merges memtable.
// Avoids loading all documents into memory for non-indexed WHERE (getAllDocumentsForIndexing).
// Does not call FlushMetadataUpdates in the hot path; PageCount may be briefly stale.
// OPTIMIZED: Uses parallel page processing for bundles with multiple pages.
// PROJECTION PUSHDOWN: Only deserializes fields needed for WHERE clause evaluation,
// then fetches full documents only for matching IDs.
func (s *BundleService) getDocumentsByFilterStreaming(bundle *models.Bundle, whereClause string) ([]*models.Document, error) {
	pageCount := uint32(bundle.PageCount)
	if pageCount == 0 {
		pageCount = 1
	}

	// PROJECTION PUSHDOWN: Extract field names from WHERE clause for optimized deserialization
	// This dramatically reduces CPU time by only deserializing fields needed for filtering
	var projectionFields []string
	var useProjection bool
	if whereClause != "" {
		expr, err := syndrQL.ParseExpressionCached(whereClause, s.logger)
		if err == nil && expr != nil {
			projectionFields = syndrQL.ExtractFieldNames(expr)
			// Always include DocumentID for result lookup
			projectionFields = append(projectionFields, "DocumentID")
			// Only use projection if we have meaningful field extraction (not all fields)
			// This avoids overhead when WHERE references many fields
			if len(projectionFields) <= 5 && len(projectionFields) > 0 {
				useProjection = true
				s.logger.Debugf("PROJECTION PUSHDOWN for WHERE: fields=%v", projectionFields)
			}
		}
	}

	// PHASE 1: If using projection, scan with projected fields to find matching IDs and their page locations
	if useProjection {
		s.SetProjectionFieldsForBundle(bundle.Name, projectionFields)

		// Scan and filter with projection - returns docID -> pageID mapping
		var matchedDocsWithPages map[string]uint32
		var scanErr error
		if pageCount <= 2 {
			matchedDocsWithPages, scanErr = s.scanForMatchingIDsWithPagesSequential(bundle, whereClause, pageCount)
		} else {
			matchedDocsWithPages, scanErr = s.scanForMatchingIDsWithPagesParallel(bundle, whereClause, pageCount)
		}

		// Clear projection before fetching full documents
		s.SetProjectionFieldsForBundle(bundle.Name, nil)

		if scanErr != nil {
			return nil, scanErr
		}

		if len(matchedDocsWithPages) == 0 {
			return nil, nil
		}

		// PHASE 2: Fetch full documents from their known pages (no GetDocument lookup needed)
		s.logger.Debugf("PROJECTION PUSHDOWN: Fetching %d full documents from known pages", len(matchedDocsWithPages))

		// Group by page to minimize page loads
		pageToDocIDs := make(map[uint32][]string)
		for docID, pageID := range matchedDocsWithPages {
			pageToDocIDs[pageID] = append(pageToDocIDs[pageID], docID)
		}

		fullDocs := make([]*models.Document, 0, len(matchedDocsWithPages))
		for pageID, docIDs := range pageToDocIDs {
			// Load full page (no projection)
			pageDocs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
			if err != nil {
				s.logger.Warnf("Failed to load page %d for full documents: %v", pageID, err)
				continue
			}

			// Build lookup for this page
			docIDSet := make(map[string]bool, len(docIDs))
			for _, id := range docIDs {
				docIDSet[id] = true
			}

			// Extract matching full documents
			for _, doc := range pageDocs {
				if docIDSet[doc.DocumentID] {
					docCopy := doc
					fullDocs = append(fullDocs, &docCopy)
				}
			}
		}

		// Merge memtable
		fullDocs = s.mergeMemtableResults(bundle, fullDocs, whereClause)
		return fullDocs, nil
	}

	// FALLBACK: No projection - use original full document scan
	if pageCount <= 2 {
		return s.getDocumentsByFilterStreamingSequential(bundle, whereClause, pageCount)
	}
	return s.getDocumentsByFilterStreamingParallel(bundle, whereClause, pageCount)
}

// scanForMatchingIDsWithPagesSequential scans pages with projection and returns docID -> pageID mapping
func (s *BundleService) scanForMatchingIDsWithPagesSequential(bundle *models.Bundle, whereClause string, pageCount uint32) (map[string]uint32, error) {
	matchedDocsWithPages := make(map[string]uint32)
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundle.Name, err)
			continue
		}
		chunk := make([]*models.Document, 0, len(docs))
		for _, d := range docs {
			dc := d
			chunk = append(chunk, &dc)
		}
		if len(chunk) == 0 {
			continue
		}
		filtered, err := s.filterDocumentsWithSyndrQL(chunk, whereClause)
		if err != nil {
			return nil, fmt.Errorf("streaming full scan failed on page %d: %w", pageID, err)
		}
		for _, doc := range filtered {
			matchedDocsWithPages[doc.DocumentID] = pageID
		}
	}
	return matchedDocsWithPages, nil
}

// scanForMatchingIDsWithPagesParallel parallelizes page scanning with projection, returns docID -> pageID mapping
func (s *BundleService) scanForMatchingIDsWithPagesParallel(bundle *models.Bundle, whereClause string, pageCount uint32) (map[string]uint32, error) {
	numWorkers := int(pageCount)
	if numWorkers > 8 {
		numWorkers = 8
	}

	type pageResult struct {
		pageID      uint32
		docIDsFound []string
		err         error
	}

	pagesChan := make(chan uint32, pageCount)
	resultsChan := make(chan pageResult, pageCount)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageID := range pagesChan {
				docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
				if err != nil {
					s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundle.Name, err)
					resultsChan <- pageResult{pageID: pageID, docIDsFound: nil, err: nil}
					continue
				}
				chunk := make([]*models.Document, 0, len(docs))
				for _, d := range docs {
					dc := d
					chunk = append(chunk, &dc)
				}
				if len(chunk) == 0 {
					resultsChan <- pageResult{pageID: pageID, docIDsFound: nil, err: nil}
					continue
				}
				filtered, err := s.filterDocumentsWithSyndrQL(chunk, whereClause)
				if err != nil {
					resultsChan <- pageResult{pageID: pageID, docIDsFound: nil, err: fmt.Errorf("page %d: %w", pageID, err)}
					continue
				}
				var docIDs []string
				for _, doc := range filtered {
					docIDs = append(docIDs, doc.DocumentID)
				}
				resultsChan <- pageResult{pageID: pageID, docIDsFound: docIDs, err: nil}
			}
		}()
	}

	for pageID := uint32(0); pageID < pageCount; pageID++ {
		pagesChan <- pageID
	}
	close(pagesChan)

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	matchedDocsWithPages := make(map[string]uint32)
	for result := range resultsChan {
		if result.err != nil {
			return nil, result.err
		}
		// Map each docID to its page
		for _, docID := range result.docIDsFound {
			matchedDocsWithPages[docID] = result.pageID
		}
	}

	return matchedDocsWithPages, nil
}

// getDocumentsByFilterStreamingSequential is the original sequential implementation
func (s *BundleService) getDocumentsByFilterStreamingSequential(bundle *models.Bundle, whereClause string, pageCount uint32) ([]*models.Document, error) {
	var result []*models.Document
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundle.Name, err)
			continue
		}
		chunk := make([]*models.Document, 0, len(docs))
		for _, d := range docs {
			dc := d
			chunk = append(chunk, &dc)
		}
		if len(chunk) == 0 {
			continue
		}
		filtered, err := s.filterDocumentsWithSyndrQL(chunk, whereClause)
		if err != nil {
			return nil, fmt.Errorf("streaming full scan failed on page %d: %w", pageID, err)
		}
		result = append(result, filtered...)
	}

	// Merge memtable
	result = s.mergeMemtableResults(bundle, result, whereClause)
	s.logger.Debugf("Streaming full scan (sequential) completed, found %d matching documents", len(result))
	return result, nil
}

// getDocumentsByFilterStreamingParallel parallelizes page scanning for faster full table scans
func (s *BundleService) getDocumentsByFilterStreamingParallel(bundle *models.Bundle, whereClause string, pageCount uint32) ([]*models.Document, error) {
	// Use a worker pool with limited concurrency to avoid overwhelming the system
	// Cap at 8 workers to balance parallelism vs resource usage
	numWorkers := int(pageCount)
	if numWorkers > 8 {
		numWorkers = 8
	}

	type pageResult struct {
		pageID uint32
		docs   []*models.Document
		err    error
	}

	// Channel for page IDs to process
	pagesChan := make(chan uint32, pageCount)
	// Channel for results
	resultsChan := make(chan pageResult, pageCount)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageID := range pagesChan {
				docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
				if err != nil {
					s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundle.Name, err)
					resultsChan <- pageResult{pageID: pageID, docs: nil, err: nil} // Don't fail, just skip
					continue
				}
				chunk := make([]*models.Document, 0, len(docs))
				for _, d := range docs {
					dc := d
					chunk = append(chunk, &dc)
				}

				if len(chunk) == 0 {
					resultsChan <- pageResult{pageID: pageID, docs: nil, err: nil}
					continue
				}

				filtered, err := s.filterDocumentsWithSyndrQL(chunk, whereClause)
				if err != nil {
					resultsChan <- pageResult{pageID: pageID, docs: nil, err: fmt.Errorf("page %d: %w", pageID, err)}
					continue
				}

				resultsChan <- pageResult{pageID: pageID, docs: filtered, err: nil}
			}
		}()
	}

	// Send all page IDs to workers
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		pagesChan <- pageID
	}
	close(pagesChan)

	// Wait for all workers to complete, then close results channel
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	var result []*models.Document
	for pr := range resultsChan {
		if pr.err != nil {
			return nil, pr.err
		}
		if len(pr.docs) > 0 {
			result = append(result, pr.docs...)
		}
	}

	// Merge memtable
	result = s.mergeMemtableResults(bundle, result, whereClause)
	s.logger.Debugf("Streaming full scan (parallel, %d workers) completed, found %d matching documents", numWorkers, len(result))
	return result, nil
}

// mergeMemtableResults - DEPRECATED: Write-through cache makes this unnecessary
// Kept as no-op for any remaining callers; returns diskResult unchanged
func (s *BundleService) mergeMemtableResults(bundle *models.Bundle, diskResult []*models.Document, whereClause string) []*models.Document {
	// WRITE-THROUGH CACHE: All recent writes are now in the page cache
	// No memtable merge needed - just return diskResult
	return diskResult
}

// tryHashIndexOptimization attempts to use hash indexes for query optimization
// This function handles only hash index optimization
// Following SyndrDB comprehensive error handling, it safely attempts hash index usage
// Parameters:
//   - bundle: The bundle containing hash indexes
//   - whereClause: The WHERE clause to analyze
//
// Returns:
//   - []*models.Document: Documents found via hash index (if used)
//   - bool: Whether hash index optimization was used
//   - error: Any error that occurred during hash index optimization
func (s *BundleService) tryHashIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse WHERE clause using SyndrQL for Expression-based optimization
	// Following SyndrDB modular development, use SyndrQL tokenizer + parser for AST generation
	tokenizer := syndrQL.NewTokenizer(whereClause)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, false, fmt.Errorf("failed to tokenize WHERE clause: %w", err)
	}

	parser := syndrQL.NewExpressionParser(tokens, s.logger)
	expr, err := parser.Parse()
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}
	expr = syndrQL.UnwrapGrouped(expr) // e.g. ("category" == "A")

	// Use Expression helper to extract simple equality conditions
	// Hash indexes are optimal for simple equality conditions (field == value)
	fieldName, value, ok := syndrQL.ExtractSimpleEquality(expr)
	if ok {
		// Check if we have a hash index for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && s.getIndexFieldName(indexRef) == fieldName {
				s.logger.Debugf("Found hash index '%s' for field '%s'", indexName, fieldName)

				// Load the hash index on-demand
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load hash index '%s': %v", indexName, err)
					continue
				}

				// Search the hash index for the value
				// CRITICAL: Remove surrounding quotes from the search key if present
				// The parser might include quotes in the value, but DocumentIDs are stored without quotes
				searchKey := conversion.ValueToString(value)
				searchKey = strings.Trim(searchKey, "\"'") // Remove both double and single quotes

				s.logger.Debugf("Hash index searching for key '%s' (original value: %v)", searchKey, value)

				docIDs, _, err := hashIndex.Search(searchKey)
				if err != nil {
					s.logger.Warnf("Hash index search failed for '%s': %v", searchKey, err)
					continue
				}

				s.logger.Debugf("Hash index found %d document IDs for value '%s'", len(docIDs), searchKey)

				// P2b: Batch load by docIDs instead of N×GetDocument
				result, err := s.GetDocumentsByIDs(bundle, docIDs)
				if err != nil {
					return nil, false, err
				}
				s.logger.Debugf("Successfully retrieved %d documents via hash index '%s'", len(result), indexName)
				return result, true, nil
			}
		}
	}

	// Hash index optimization not applicable
	return nil, false, nil
}

// tryBTreeIndexOptimization attempts to use BTree indexes for query optimization
// This function handles only BTree index optimization
// Following SyndrDB comprehensive error handling, it safely attempts BTree index usage
// Parameters:
//   - bundle: The bundle containing BTree indexes
//   - whereClause: The WHERE clause to analyze
//
// Returns:
//   - []*models.Document: Documents found via BTree index (if used)
//   - bool: Whether BTree index optimization was used
//   - error: Any error that occurred during BTree index optimization
func (s *BundleService) tryBTreeIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse WHERE clause using SyndrQL for Expression-based optimization
	// Following SyndrDB modular development, use SyndrQL tokenizer + parser for AST generation
	tokenizer := syndrQL.NewTokenizer(whereClause)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, false, fmt.Errorf("failed to tokenize WHERE clause: %w", err)
	}

	parser := syndrQL.NewExpressionParser(tokens, s.logger)
	expr, err := parser.Parse()
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}
	expr = syndrQL.UnwrapGrouped(expr)

	// Try simple equality first (can use BTree as well as hash)
	// Following SyndrDB performance optimization, check for equality before range
	if fieldName, value, ok := syndrQL.ExtractSimpleEquality(expr); ok {
		// Check if we have a BTree index for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" && s.getIndexFieldName(indexRef) == fieldName {
				s.logger.Debugf("Found BTree index '%s' for field '%s' with equality operator",
					indexName, fieldName)

				// Load the BTree index on-demand
				btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load BTree index '%s': %v", indexName, err)
					continue
				}

				// Convert search value to bytes for BTree search
				keyBytes, err := convertValueToBytes(value)
				if err != nil {
					s.logger.Warnf("Failed to convert search value to bytes: %v", err)
					continue
				}

				s.logger.Debugf("Performing BTree equality search on '%v' with key '%v'",
					btreeIndex, keyBytes)

				// PHASE 0.1: Enable B-tree search - uncommented and activated
				// Use Search method for equality queries
				docIDs, err := btreeIndex.Search(keyBytes)
				if err != nil {
					s.logger.Warnf("BTree index search failed: %v", err)
					continue
				}

				return s.convertDocIDsToDocuments(bundle, docIDs, indexName)
			}
		}
	}

	// Try range conditions (>, >=, <, <=, !=)
	// Use Expression helper to extract range query information (expr already UnwrapGrouped)
	if fieldName, operator, value, ok := syndrQL.ExtractRangeCondition(expr); ok {
		// Check if we have a BTree index for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" && s.getIndexFieldName(indexRef) == fieldName {
				s.logger.Debugf("Found BTree index '%s' for field '%s' with operator '%s'",
					indexName, fieldName, operator)

				// Load the BTree index on-demand
				btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load BTree index '%s': %v", indexName, err)
					continue
				}

				// Convert search value to bytes for BTree search
				keyBytes, convErr := convertValueToBytes(value)
				if convErr != nil {
					s.logger.Warnf("Failed to convert search value to bytes: %v", convErr)
					continue
				}

				s.logger.Debugf("Performing BTree range search with operator '%s' on '%v' with key '%v'",
					operator, btreeIndex, keyBytes)

				// PHASE 0.1: Enable B-tree range search - using RangeSearchWithBounds
				var docIDs []string
				var searchErr error

				switch operator {
				case ">":
					// key > value: search from (value, max] - exclude start, include end
					// Use a sentinel max key (very large byte array)
					maxKey := s.createMaxKeyForBTree(keyBytes)
					docIDs, searchErr = btreeIndex.RangeSearchWithBounds(keyBytes, maxKey, true, false)
				case ">=":
					// key >= value: search from [value, max] - include both
					maxKey := s.createMaxKeyForBTree(keyBytes)
					docIDs, searchErr = btreeIndex.RangeSearchWithBounds(keyBytes, maxKey, false, false)
				case "<":
					// key < value: search from [min, value) - include start, exclude end
					minKey := s.createMinKeyForBTree(keyBytes)
					docIDs, searchErr = btreeIndex.RangeSearchWithBounds(minKey, keyBytes, false, true)
				case "<=":
					// key <= value: search from [min, value] - include both
					minKey := s.createMinKeyForBTree(keyBytes)
					docIDs, searchErr = btreeIndex.RangeSearchWithBounds(minKey, keyBytes, false, false)
				case "!=":
					// For inequality, combine two range searches: [min, value) and (value, max]
					// This is less efficient but works without SearchAll
					minKey := s.createMinKeyForBTree(keyBytes)
					maxKey := s.createMaxKeyForBTree(keyBytes)

					// Get documents less than value
					lessDocIDs, err1 := btreeIndex.RangeSearchWithBounds(minKey, keyBytes, false, true)
					if err1 != nil {
						searchErr = err1
						break
					}

					// Get documents greater than value
					greaterDocIDs, err2 := btreeIndex.RangeSearchWithBounds(keyBytes, maxKey, true, false)
					if err2 != nil {
						searchErr = err2
						break
					}

					// Combine results (no duplicates possible since ranges don't overlap)
					docIDs = append(lessDocIDs, greaterDocIDs...)
				default:
					s.logger.Warnf("Unsupported BTree operator: %s", operator)
					continue
				}

				if searchErr != nil {
					s.logger.Warnf("BTree index search failed: %v", searchErr)
					continue
				}

				return s.convertDocIDsToDocuments(bundle, docIDs, indexName)
			}
		}
	}

	// BTree index optimization not applicable
	return nil, false, nil
}

// tryANDIndexOptimization handles compound AND conditions like:
// ("category" == "Gaming Accessories") AND ("rating" <= 3.5)
// Strategy: Find the best indexed clause, use it to narrow results, then filter remaining clauses.
// This avoids full table scans when at least one condition in an AND has an index.
func (s *BundleService) tryANDIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse WHERE clause
	tokenizer := syndrQL.NewTokenizer(whereClause)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, false, nil // Not a parse error we can handle
	}

	parser := syndrQL.NewExpressionParser(tokens, s.logger)
	expr, err := parser.Parse()
	if err != nil {
		return nil, false, nil
	}
	expr = syndrQL.UnwrapGrouped(expr)

	// Extract AND clauses - if there's only one, the simpler optimizers already handled it
	andClauses := syndrQL.ExtractANDClauses(expr)
	if len(andClauses) <= 1 {
		return nil, false, nil // Single condition already handled by tryHash/tryBTree
	}

	s.logger.Debugf("AND optimization: found %d AND clauses", len(andClauses))

	// Find the best indexed clause (priority: hash equality > btree equality > btree range)
	type indexedClause struct {
		clauseIdx  int
		indexName  string
		indexType  string // "hash" or "btree"
		isEquality bool
		fieldName  string
		value      interface{}
		operator   string
	}

	var bestClause *indexedClause

	for i, clause := range andClauses {
		clause = syndrQL.UnwrapGrouped(clause)

		// Check for simple equality
		if fieldName, value, ok := syndrQL.ExtractSimpleEquality(clause); ok {
			// Check for hash index on this field (highest priority)
			for indexName, indexRef := range bundle.Indexes {
				idxField := s.getIndexFieldName(indexRef)
				if indexRef.IndexType == "hash" && idxField == fieldName {
					// Hash index equality - best option
					bestClause = &indexedClause{
						clauseIdx:  i,
						indexName:  indexName,
						indexType:  "hash",
						isEquality: true,
						fieldName:  fieldName,
						value:      value,
						operator:   "==",
					}
					s.logger.Debugf("AND optimization: found hash index '%s' for field '%s' (clause %d)", indexName, fieldName, i)
					break // Hash equality is best, no need to look further for this clause
				}
				if indexRef.IndexType == "btree" && idxField == fieldName {
					// BTree equality - good if no hash found
					if bestClause == nil || bestClause.indexType != "hash" {
						bestClause = &indexedClause{
							clauseIdx:  i,
							indexName:  indexName,
							indexType:  "btree",
							isEquality: true,
							fieldName:  fieldName,
							value:      value,
							operator:   "==",
						}
						s.logger.Debugf("AND optimization: found btree index '%s' for field '%s' (clause %d)", indexName, fieldName, i)
					}
				}
			}
		}

		// Check for range condition (only if we don't have a hash index yet)
		if bestClause == nil || (bestClause.indexType == "btree" && !bestClause.isEquality) {
			if fieldName, operator, value, ok := syndrQL.ExtractRangeCondition(clause); ok {
				for indexName, indexRef := range bundle.Indexes {
					idxField := s.getIndexFieldName(indexRef)
					if indexRef.IndexType == "btree" && idxField == fieldName {
						// BTree range - use if nothing better
						if bestClause == nil || (!bestClause.isEquality && bestClause.indexType != "hash") {
							bestClause = &indexedClause{
								clauseIdx:  i,
								indexName:  indexName,
								indexType:  "btree",
								isEquality: false,
								fieldName:  fieldName,
								value:      value,
								operator:   operator,
							}
							s.logger.Debugf("AND optimization: found btree index '%s' for field '%s' with operator '%s' (clause %d)", indexName, fieldName, operator, i)
						}
					}
				}
			}
		}
	}

	if bestClause == nil {
		s.logger.Debugf("AND optimization: no indexed clause found among %d AND clauses", len(andClauses))
		return nil, false, nil
	}

	s.logger.Debugf("AND optimization: using %s index '%s' on field '%s' to narrow results", bestClause.indexType, bestClause.indexName, bestClause.fieldName)

	// Use the indexed clause to get initial document set
	var indexedDocs []*models.Document
	var used bool

	// Reconstruct the single clause as a WHERE string for the existing helpers
	singleClauseWhere := syndrQL.ExpressionToString(andClauses[bestClause.clauseIdx])

	if bestClause.indexType == "hash" {
		indexedDocs, used, err = s.tryHashIndexOptimization(bundle, singleClauseWhere)
	} else {
		indexedDocs, used, err = s.tryBTreeIndexOptimization(bundle, singleClauseWhere)
	}

	if err != nil || !used {
		s.logger.Debugf("AND optimization: index lookup failed or not applicable")
		return nil, false, nil
	}

	s.logger.Debugf("AND optimization: index returned %d documents, now filtering by remaining clauses", len(indexedDocs))

	if len(indexedDocs) == 0 {
		return indexedDocs, true, nil // No matches from index, done
	}

	// Build remaining WHERE clause (all clauses except the indexed one)
	var remainingClauses []syndrQL.Expression
	for i, clause := range andClauses {
		if i != bestClause.clauseIdx {
			remainingClauses = append(remainingClauses, clause)
		}
	}

	if len(remainingClauses) == 0 {
		return indexedDocs, true, nil // Only one clause and it was indexed
	}

	// Reconstruct remaining WHERE as string and filter
	var remainingWhere string
	if len(remainingClauses) == 1 {
		remainingWhere = syndrQL.ExpressionToString(remainingClauses[0])
	} else {
		// Join with AND
		remainingWhere = syndrQL.ExpressionToString(remainingClauses[0])
		for i := 1; i < len(remainingClauses); i++ {
			remainingWhere = remainingWhere + " AND " + syndrQL.ExpressionToString(remainingClauses[i])
		}
	}

	s.logger.Debugf("AND optimization: filtering %d docs by remaining WHERE: %s", len(indexedDocs), remainingWhere)

	// Filter by remaining clauses using SyndrQL (in-memory, fast since we have a small set)
	filteredDocs, err := s.filterDocumentsWithSyndrQL(indexedDocs, remainingWhere)
	if err != nil {
		return nil, false, fmt.Errorf("AND optimization filter failed: %w", err)
	}

	s.logger.Debugf("AND optimization: narrowed from %d to %d documents using index + filter", len(indexedDocs), len(filteredDocs))
	return filteredDocs, true, nil
}

// convertDocIDsToDocuments is a helper to convert document IDs to documents
// P2b: Uses GetDocumentsByIDs for batch load instead of N×GetDocument
// SELF-HEALING: If documents aren't found, removes stale entries from B-tree index
func (s *BundleService) convertDocIDsToDocuments(bundle *models.Bundle, docIDs []string, indexName string) ([]*models.Document, bool, error) {
	if len(docIDs) == 0 {
		s.logger.Debugf("BTree index search returned no document IDs")
		return []*models.Document{}, true, nil
	}
	result, err := s.GetDocumentsByIDs(bundle, docIDs)
	if err != nil {
		return nil, false, err
	}

	// SELF-HEALING: Check if any docIDs were not found and clean them from B-tree
	if len(result) < len(docIDs) {
		// Build set of found docIDs
		foundIDs := make(map[string]bool, len(result))
		for _, doc := range result {
			foundIDs[doc.DocumentID] = true
		}

		// Collect stale docIDs (in B-tree but not found in storage)
		var staleDocIDs []string
		for _, docID := range docIDs {
			if !foundIDs[docID] {
				staleDocIDs = append(staleDocIDs, docID)
			}
		}

		// Remove stale entries from B-tree index asynchronously to not block the query
		if len(staleDocIDs) > 0 {
			s.logger.Debugf("SELF-HEALING: Found %d stale B-tree entries in index '%s', removing...", len(staleDocIDs), indexName)
			go func(bundleName, idxName string, staleIDs []string) {
				// Load B-tree index and remove stale entries
				if indexRef, exists := bundle.Indexes[idxName]; exists && indexRef.IndexType == "btree" {
					btreeIndex, loadErr := s.getOrLoadBTreeIndex(bundle, idxName, indexRef)
					if loadErr == nil {
						n, delErr := btreeIndex.DeleteByDocumentIDs(staleIDs)
						if delErr != nil {
							s.logger.Warnf("SELF-HEALING: Failed to remove stale B-tree entries: %v", delErr)
						} else if n > 0 {
							s.logger.Debugf("SELF-HEALING: Removed %d stale entries from B-tree index '%s'", n, idxName)
						}
					}
				}
			}(bundle.Name, indexName, staleDocIDs)
		}
	}

	s.logger.Debugf("Successfully retrieved %d documents via BTree index '%s'", len(result), indexName)
	return result, true, nil
}

// createMinKeyForBTree creates a minimum sentinel key for B-tree range searches
// This returns an empty byte array which is guaranteed to be less than any non-empty key
// PHASE 0.1: Helper for B-tree range search implementation
func (s *BundleService) createMinKeyForBTree(keyBytes []byte) []byte {
	// Empty byte array is the minimum for byte comparison
	return []byte{}
}

// createMaxKeyForBTree creates a maximum sentinel key for B-tree range searches
// This returns a byte array with maximum values that is guaranteed to be greater than any key
// PHASE 0.1: Helper for B-tree range search implementation
func (s *BundleService) createMaxKeyForBTree(keyBytes []byte) []byte {
	// Create a sentinel max key: use 256 bytes of 0xFF which should be greater than most keys
	// For keys longer than this, the range search will still work correctly as byte comparison
	// will handle it properly
	maxKey := make([]byte, 256)
	for i := range maxKey {
		maxKey[i] = 0xFF
	}
	return maxKey
}

// getIndexFieldName extracts the field name from an index reference
// This function follows the Single Responsibility Principle by handling only field name extraction
// Following SyndrDB comprehensive error handling, it safely handles different index types
// Parameters:
//   - indexRef: The index reference to extract field name from
//
// Returns:
//   - string: The field name being indexed
func (s *BundleService) getIndexFieldName(indexRef models.IndexReference) string {
	switch indexRef.IndexType {
	case "hash":
		return indexRef.HashIndexField.FieldName
	case "btree":
		return indexRef.BTreeIndexField.FieldName
	default:
		s.logger.Warnf("Unknown index type: %s", indexRef.IndexType)
		return ""
	}
}

// validateDocumentFields validates that document fields match bundle field definitions
// This function ensures that:
// 1. All fields in the document command exist in the bundle's field definitions
// 2. Field data types match the bundle field definition types
// 3. Required fields are present
// 4. Field values are compatible with their defined types
func (s *BundleService) validateDocumentFields(bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		s.logger.Warnf("[VALIDATION] Bundle '%s' has nil FieldDefinitions - cannot validate", bundle.Name)
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	//s.logger.Debugf("[VALIDATION] Bundle '%s' has %d field definition(s)", bundle.Name, len(bundle.DocumentStructure.FieldDefinitions))

	// Log all field definitions for debugging
	// for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
	// 	s.logger.Debugf("[VALIDATION] Field '%s': Type=%s, Required=%v, Unique=%v",
	// 		fieldName, fieldDef.Type, fieldDef.IsRequired, fieldDef.IsUnique)
	// }

	// Track which required fields are provided
	providedFields := make(map[string]bool)

	// Validate each field in the document command
	for i, field := range docCommand.Fields {
		fieldName := field.Key
		fieldValue := field.Value

		// Check if the field exists in bundle field definitions
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' is not defined in bundle '%s'", fieldName, bundle.Name)
		}

		// Check if user provided explicit NULL for a required field
		// This should fail just like if the field was missing
		if fieldDef.IsRequired && s.nullHandler.IsNullValue(fieldValue) {
			return fmt.Errorf("required field '%s' cannot be set to NULL", fieldName)
		}

		// Validate and convert field data type using fast pre-compiled converter
		convertedValue, err := s.validateAndConvertFieldTypeFast(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Update the field value with the converted value
		docCommand.Fields[i].Value = convertedValue

		// Mark this field as provided (only if not NULL)
		// NULL values should be treated as if the field was not provided for required field validation
		if !s.nullHandler.IsNullValue(convertedValue) {
			providedFields[fieldName] = true
		}
	}

	//s.logger.Debugf("[VALIDATION] Provided %d field(s) in document command", len(providedFields))

	// Check that all required fields are provided
	missingFields := make([]string, 0, 5)
	for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
		if fieldDef.IsRequired && !providedFields[fieldName] {
			// Skip DocumentID if it's auto-generated
			if fieldName == "DocumentID" {
				continue
			}
			missingFields = append(missingFields, fieldName)
		}
	}

	// If any required fields are missing, return detailed error
	if len(missingFields) > 0 {
		if len(missingFields) == 1 {
			s.logger.Warnf("[VALIDATION] Required field '%s' is missing from document", missingFields[0])
			return fmt.Errorf("required field '%s' is missing from document", missingFields[0])
		}
		s.logger.Warnf("[VALIDATION] Multiple required fields missing: %v", missingFields)
		return fmt.Errorf("required fields are missing from document: %v", missingFields)
	}

	//s.logger.Debugf("[VALIDATION] All required fields validated successfully")
	return nil
}

// processNullValues handles NULL value processing, default value substitution, and field initialization.
// Uses a single-pass algorithm for O(n) performance where n is the number of fields in the schema.
//
// This function:
// 1. Substitutes default values for NULL or missing fields (required or optional)
// 2. Converts user nil values to SYNDR_NULL magic value (if no default exists)
// 3. Escapes user strings that look like magic values
// 4. Initializes missing optional fields with defaults or SYNDR_NULL
//
// CRITICAL: Must run BEFORE validation so required fields with defaults are satisfied
//
// Performance: O(n) time, O(1) space where n = schema field count
func (s *BundleService) processNullValues(bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	// Build providedFields map while processing existing fields (single pass)
	providedFields := make(map[string]bool, len(docCommand.Fields))

	// PASS 1: Process provided fields in-place - substitute defaults for NULL values
	for i := range docCommand.Fields {
		fieldName := docCommand.Fields[i].Key
		fieldValue := docCommand.Fields[i].Value

		// Get field definition for default value lookup
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			// Field doesn't exist in schema - validation will catch this later
			providedFields[fieldName] = true
			continue
		}

		// Mark as provided (even if NULL - we'll check for defaults)
		providedFields[fieldName] = true

		// CRITICAL: Check if user explicitly set a required field to NULL
		// This must happen BEFORE default value substitution
		// Required fields cannot be NULL, even if they have a default value
		if fieldDef.IsRequired && (fieldValue == nil || fieldValue == SYNDR_NULL) {
			return fmt.Errorf("required field '%s' cannot be set to NULL", fieldName)
		}

		// Handle nil or SYNDR_NULL -> check for default value substitution
		if fieldValue == nil || fieldValue == SYNDR_NULL {
			if fieldDef.DefaultValue != nil {
				// Evaluate default value (supports Expression or literal)
				// Create a temporary document for evaluation context
				tempDoc := &models.Document{
					Data: make(map[string]interface{}),
				}
				evaluatedValue, err := s.evaluateDefaultValue(fieldDef.DefaultValue, tempDoc)
				if err != nil {
					s.logger.Errorf("Failed to evaluate default value for field '%s': %v", fieldName, err)
					return fmt.Errorf("failed to evaluate default value for field '%s': %w", fieldName, err)
				}
				// Substitute the evaluated default value
				docCommand.Fields[i].Value = evaluatedValue
				s.logger.Debugf("Substituted evaluated default value for field '%s': %v", fieldName, evaluatedValue)
			} else {
				// No default - use SYNDR_NULL
				docCommand.Fields[i].Value = SYNDR_NULL
			}
			continue
		}

		// Escape magic-like values (fast path: string prefix check)
		if strValue, ok := fieldValue.(string); ok {
			if strings.HasPrefix(strValue, "::SYNDR_") {
				// Only escape if it's NOT already a valid magic value
				switch strValue {
				case SYNDR_NULL, SYNDR_MISSING, SYNDR_DELETED, SYNDR_DEFAULT:
					// Valid magic value - keep as-is
					continue
				default:
					// User string that looks like magic value - escape it
					docCommand.Fields[i].Value = SYNDR_ESCAPED + strValue
				}
			}
		}
	}

	// PASS 2: Add missing fields (required or optional) with defaults or SYNDR_NULL
	missingFieldCount := 0
	for fieldName := range bundle.DocumentStructure.FieldDefinitions {
		// Skip DocumentID (auto-generated)
		if fieldName == "DocumentID" {
			continue
		}

		// Count ALL missing fields (required or optional)
		if !providedFields[fieldName] {
			missingFieldCount++
		}
	}

	// Pre-allocate slice capacity to avoid multiple allocations
	if missingFieldCount > 0 {
		originalLen := len(docCommand.Fields)
		// Grow slice once with exact capacity needed
		newFields := make([]models.KeyValue, originalLen, originalLen+missingFieldCount)
		copy(newFields, docCommand.Fields)

		// Append missing fields with defaults or SYNDR_NULL
		for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
			// Skip DocumentID (auto-generated)
			if fieldName == "DocumentID" {
				continue
			}

			// Skip provided fields
			if providedFields[fieldName] {
				continue
			}

			// Determine value: use default if available, otherwise SYNDR_NULL
			var fieldValue interface{}
			if fieldDef.DefaultValue != nil {
				// Evaluate default value (supports Expression or literal)
				// Create a temporary document for evaluation context
				tempDoc := &models.Document{
					Data: make(map[string]interface{}),
				}
				evaluatedValue, err := s.evaluateDefaultValue(fieldDef.DefaultValue, tempDoc)
				if err != nil {
					s.logger.Errorf("Failed to evaluate default value for field '%s': %v", fieldName, err)
					return fmt.Errorf("failed to evaluate default value for field '%s': %w", fieldName, err)
				}
				fieldValue = evaluatedValue
				s.logger.Debugf("Using evaluated default value for missing field '%s': %v", fieldName, evaluatedValue)
			} else {
				fieldValue = SYNDR_NULL
			}

			newFields = append(newFields, models.KeyValue{
				Key:   fieldName,
				Value: fieldValue,
			})
		}

		docCommand.Fields = newFields
	}

	return nil
}

// validateAndConvertFieldTypeFast uses pre-compiled converters for optimal performance
// This eliminates reflection overhead and provides 60-80% faster field validation
func (s *BundleService) validateAndConvertFieldTypeFast(fieldName string, value interface{}, expectedType string) (interface{}, error) {
	if value == nil {
		return nil, nil // nil values are handled by required field validation
	}

	// Use pre-compiled converter for fast type conversion (O(1) map lookup)
	converter, exists := typeConverters[strings.ToLower(expectedType)]
	if !exists {
		// Unknown field type - log warning but allow it as string (fallback)
		s.logger.Warnf("Unknown field type '%s' for field '%s', treating as string", expectedType, fieldName)
		return convertToString(value)
	}

	// Fast conversion using pre-compiled function
	return converter(value)
}

// validateUpdateFields validates that document update fields match bundle field definitions
// This function ensures that:
// 1. All fields being updated exist in the bundle's field definitions
// 2. Field data types match the bundle field definition types
// 3. Field values are compatible with their defined types
// Note: Unlike document creation, updates don't require all required fields to be present
func (s *BundleService) validateUpdateFields(bundle *models.Bundle, docCommand *models.DocumentUpdateCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	// Single-pass processing: validate, escape, and convert in one loop
	// Performance: O(m) where m = number of fields being updated
	for i := range docCommand.Fields {
		fieldName := docCommand.Fields[i].Key
		fieldValue := docCommand.Fields[i].Value

		// REFERENTIAL INTEGRITY: DocumentID is read-only and cannot be updated
		if fieldName == "DocumentID" {
			return fmt.Errorf("cannot update DocumentID: this is a read-only system field")
		}

		// Check if the field exists in bundle field definitions
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' is not defined in bundle '%s'", fieldName, bundle.Name)
		}

		// Handle NULL values (fast path: nil check first)
		if fieldValue == nil {
			docCommand.Fields[i].Value = SYNDR_NULL
			continue // Skip type validation for NULL
		}

		// Escape magic-like values (inline, no function call overhead)
		// Fast path: only check strings that start with ::SYNDR_
		if strValue, ok := fieldValue.(string); ok {
			if strings.HasPrefix(strValue, "::SYNDR_") {
				// Check if it's a valid magic value
				switch strValue {
				case SYNDR_NULL, SYNDR_MISSING, SYNDR_DELETED, SYNDR_DEFAULT:
					// Valid magic value - keep as-is, skip type validation
					continue
				default:
					// User string that looks like magic value - escape it
					docCommand.Fields[i].Value = SYNDR_ESCAPED + strValue
					fieldValue = docCommand.Fields[i].Value // Update for validation
				}
			}
		}

		// Validate and convert field data type
		convertedValue, err := s.validateAndConvertFieldTypeFast(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Update the field value with the converted value
		docCommand.Fields[i].Value = convertedValue

		// TODO: Unique constraint validation for updates (future work)
		// if fieldDef.IsUnique {
		//     // Validate that new value doesn't violate uniqueness
		// }
	}

	return nil
}

// registerBundleInPrimary adds the bundle information to the "Bundles" bundle in the Primary database
func (s *BundleService) registerBundleInPrimary(bundle *models.Bundle) error {
	// Since we can't directly import the server package due to circular dependency,
	// this method is meant to be overridden or called through the service manager
	// The actual registration logic is implemented in CatalogService.AddBundleToCatalog

	// TODO There are better ways to do this, gotta clean up the architecture in places
	s.logger.Debugf("Bundle '%s' needs to be registered in primary catalog (handled by CatalogService)", bundle.Name)
	return nil
}

// discoverBundleIndexes scans for existing index files and populates the bundle's Indexes field
// UPDATED By Dan: Now supports both legacy (.idx) and new header-based (.hidx) index files
// New naming convention: FieldName-fk.N.hidx (FK) or FieldName.N.hidx (regular)
func (s *BundleService) discoverBundleIndexes(bundle *models.Bundle) error {
	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// 1: Look for NEW header-based index files (.hidx)
	// Pattern: *.hidx (includes both FieldName-fk.N.hidx and FieldName.N.hidx)
	newHashPattern := filepath.Join(indexesPath, "*.hidx")
	newHashFiles, err := filepath.Glob(newHashPattern)
	if err != nil {
		s.logger.Warnf("Failed to scan for new hash index files: %v", err)
		newHashFiles = []string{} // Continue with legacy discovery
	}

	// Process new format files (.hidx with headers)
	for _, hashFile := range newHashFiles {
		baseName := filepath.Base(hashFile)

		// Parse new naming convention: FieldName-fk.N.hidx or FieldName.N.hidx
		// Remove extension
		nameWithoutExt := strings.TrimSuffix(baseName, ".hidx")

		// Split by last dot to separate file number
		parts := strings.Split(nameWithoutExt, ".")
		if len(parts) != 2 {
			s.logger.Warnf("Invalid new index file name format: %s", baseName)
			continue
		}

		fieldPart := parts[0]
		// fileNum := parts[1] // Not needed for discovery

		// Check if it's a foreign key index
		isForeignKey := strings.HasSuffix(fieldPart, "-fk")
		var fieldName string
		var indexName string

		if isForeignKey {
			// Foreign key: FieldName-fk
			fieldName = strings.TrimSuffix(fieldPart, "-fk")
			indexName = fieldName + "_fk" // Restore _fk for index name
		} else {
			// Regular index: FieldName
			fieldName = fieldPart
			indexName = fieldName
		}

		// Check if this field exists in the bundle's field definitions
		if bundle.DocumentStructure.FieldDefinitions != nil {
			if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
				s.logger.Warnf("Found hash index file for field '%s' but field not defined in bundle '%s'", fieldName, bundle.Name)
				continue
			}
		}

		// Resolve field type from DocumentStructure when available
		fdType := "string"
		if bundle.DocumentStructure.FieldDefinitions != nil {
			if fd, ok := bundle.DocumentStructure.FieldDefinitions[fieldName]; ok {
				fdType = fd.Type
			}
		}
		// Create index reference (preserving _fk suffix in index name).
		// Fields must be set so HasIndexOnField/GetHashIndexForField can match by document field (e.g. product_id).
		indexRef := models.IndexReference{
			IndexName: indexName,
			IndexType: "hash",
			Fields:    []models.FieldDefinition{{Name: fieldName, Type: fdType}},
			HashIndexField: models.IndexField{
				FieldName: indexName, // Includes _fk if foreign key
			},
		}

		bundle.Indexes[indexName] = indexRef
		s.logger.Debugf("Discovered NEW hash index '%s' for field '%s' in bundle '%s' (FK=%v)",
			indexName, fieldName, bundle.Name, isForeignKey)
	}

	// 2: Look for LEGACY index files (.idx) - OLD FORMAT
	// Pattern: BundleName_*_*.idx
	legacyHashPattern := fmt.Sprintf("%s/%s_*_*.idx", indexesPath, bundle.Name)
	legacyHashFiles, err := filepath.Glob(legacyHashPattern)
	if err != nil {
		return fmt.Errorf("failed to scan for legacy hash index files: %w", err)
	}

	// Process legacy format files (.idx without headers)
	for _, hashFile := range legacyHashFiles {
		var fieldName string

		// Extract field name from filename: BundleName_FieldName_N.idx
		baseName := filepath.Base(hashFile)
		// Remove .idx extension
		baseName = strings.TrimSuffix(baseName, ".idx")
		// remove the bundle name prefix
		baseName = strings.TrimPrefix(baseName, bundle.Name+"_")

		// Strip the trailing index number by working backwards from the end of the string
		underscoreIndex := strings.LastIndex(baseName, "_")
		if underscoreIndex != -1 {
			baseName = baseName[:underscoreIndex]
		}

		// What is left SHOULD be the field name (with _fk if foreign key)
		indexName := baseName

		// For field validation, strip _fk suffix
		fieldName = strings.TrimSuffix(baseName, "_fk")

		// Check if this field exists in the bundle's field definitions
		if bundle.DocumentStructure.FieldDefinitions != nil {
			if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
				s.logger.Warnf("Found legacy hash index file for field '%s' but field not defined in bundle '%s'", fieldName, bundle.Name)
				continue
			}
		}

		// Only add if not already discovered as new format
		if _, exists := bundle.Indexes[indexName]; !exists {
			fdType := "string"
			if bundle.DocumentStructure.FieldDefinitions != nil {
				if fd, ok := bundle.DocumentStructure.FieldDefinitions[fieldName]; ok {
					fdType = fd.Type
				}
			}
			// Create index reference (preserving _fk suffix). Fields required for join index lookup.
			indexRef := models.IndexReference{
				IndexName: indexName,
				IndexType: "hash",
				Fields:    []models.FieldDefinition{{Name: fieldName, Type: fdType}},
				HashIndexField: models.IndexField{
					FieldName: indexName, // Preserve _fk suffix
				},
			}

			bundle.Indexes[indexName] = indexRef
			s.logger.Debugf("Discovered LEGACY hash index '%s' for field '%s' in bundle '%s'", indexName, fieldName, bundle.Name)
		}
	}

	// TODO: Add discovery for BTree indexes when they have a consistent file pattern
	// Look for btree index files if there's a predictable naming pattern

	s.logger.Debugf("Discovered %d total indexes for bundle '%s' (%d new format, %d legacy format)",
		len(bundle.Indexes), bundle.Name, len(newHashFiles), len(legacyHashFiles))
	return nil
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

	// Force flush any pending index updates
	s.forceFlushIndexUpdates()

	// CRITICAL: Flush all loaded indexes to disk before closing
	// This ensures memtable entries and write buffers are persisted
	if err := s.FlushAllIndexesToDisk(); err != nil {
		s.logger.Warnf("Error flushing indexes during shutdown: %v", err)
	}

	// Also flush and close write buffers
	s.store.CloseWriteBuffers()

	// Also force flush any remaining metadata updates during shutdown
	// CRITICAL: Use forceMetadataPersistence to ensure disk write happens
	s.metadataUpdateMutex.RLock()
	shutdownMetaCount := len(s.metadataUpdateBuffer)
	s.metadataUpdateMutex.RUnlock()
	if shutdownMetaCount > 0 {
		s.logger.Debugf("Force flushing %d remaining metadata updates during shutdown", shutdownMetaCount)
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

// DOCUMENT SCANNER INTEGRATION: Scanner management methods
// This should be put in its own file. Clean up phase coming soon!
// GetOrCreateDocumentScanner returns a document scanner for the specified bundle
// Creates and caches scanners per bundle for optimal performance
func (s *BundleService) GetOrCreateDocumentScanner(bundle *models.Bundle) (documentscanner.DocumentScannerInterface, error) {
	s.scannerMutex.RLock()
	if scanner, exists := s.bundleScanners[bundle.Name]; exists {
		s.scannerMutex.RUnlock()
		return scanner, nil
	}
	s.scannerMutex.RUnlock()

	// Create new scanner
	s.scannerMutex.Lock()
	defer s.scannerMutex.Unlock()

	// Double-check after acquiring write lock
	if scanner, exists := s.bundleScanners[bundle.Name]; exists {
		return scanner, nil
	}

	// Create scanner using integration
	scanner, err := s.scannerIntegration.CreateScannerForBundle(bundle, s, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create document scanner for bundle '%s': %w", bundle.Name, err)
	}

	// VISIBILITY MAP: Wire the VM into the scanner for page-level MVCC skip optimization
	if smartScanner, ok := scanner.(*documentscanner.SmartBundleScanner); ok {
		pageCount := uint32(bundle.PageCount)
		if pageCount == 0 {
			pageCount = 1
		}
		vm := s.getOrCreateVisibilityMap(bundle.Name, pageCount)
		smartScanner.SetVisibilityMap(vm)
	}

	// Cache the scanner
	s.bundleScanners[bundle.Name] = scanner

	return scanner, nil
}

// GetScannerMetrics returns metrics manager for performance monitoring
func (s *BundleService) GetScannerMetrics() *documentscanner.MetricsManager {
	return s.scannerIntegration.GetMetricsManager()
}

// RemoveDocumentScanner removes a cached scanner (called when bundle is deleted)
func (s *BundleService) RemoveDocumentScanner(bundleName string) {
	s.scannerMutex.Lock()
	defer s.scannerMutex.Unlock()

	//s.logger.Debugf("DEBUG: RemoveDocumentScanner called for bundle '%s'", bundleName)
	if scanner, exists := s.bundleScanners[bundleName]; exists {
		//s.logger.Debugf("DEBUG: RemoveDocumentScanner - Scanner EXISTS in map, closing it...")
		scanner.Close()
		delete(s.bundleScanners, bundleName)
		//s.logger.Debugf("DEBUG: RemoveDocumentScanner - Scanner REMOVED from map for bundle '%s'", bundleName)
	} else {
		//s.logger.Debugf("DEBUG: RemoveDocumentScanner - Scanner NOT FOUND in map for bundle '%s'", bundleName)
	}
}

// CloseAllScanners closes all document scanners (called during service shutdown)
func (s *BundleService) CloseAllScanners() {
	s.scannerMutex.Lock()
	defer s.scannerMutex.Unlock()

	for bundleName, scanner := range s.bundleScanners {
		scanner.Close()
		s.logger.Debugf("Closed document scanner for bundle '%s'", bundleName)
	}

	s.bundleScanners = make(map[string]documentscanner.DocumentScannerInterface)
	s.scannerIntegration.Close()
	s.logger.Debug("Closed all document scanners")
}

// invalidateDocumentPage invalidates the page cache for a specific document
// Uses the sharded documentPageCache for O(1) lookup when available, otherwise invalidates all pages.
// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.
func (s *BundleService) invalidateDocumentPage(bundleName, documentID string) {
	// PHASE 5: Check sharded cache for page ID
	if pageID, found := s.documentPageCache.GetPageID(bundleName, documentID); found {
		pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
		shardIdx := s.getPageShardIndex(pageKey)
		shard := s.pageShards[shardIdx]

		shard.mu.Lock()
		// Remove from LRU tracking
		if elem, exists := shard.lruElements[pageKey]; exists {
			shard.lruOrder.Remove(elem)
			delete(shard.lruElements, pageKey)
		}
		shard.deleteLocked(pageKey)
		shard.mu.Unlock()

		// Invalidate the document->page cache entry
		s.documentPageCache.InvalidateDocument(bundleName, documentID)
		s.logger.Debugf("Invalidated page %d for document %s in bundle %s", pageID, documentID, bundleName)
		return
	}

	// Fall back to invalidating all pages for this bundle
	prefix := bundleName + ":"
	totalDeleted := 0
	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		shard.mu.Lock()
		keysToDelete := make([]string, 0, 8)
		for pageKey := range shard.pages {
			if strings.HasPrefix(pageKey, prefix) {
				keysToDelete = append(keysToDelete, pageKey)
			}
		}
		for _, key := range keysToDelete {
			// Remove from LRU tracking
			if elem, exists := shard.lruElements[key]; exists {
				shard.lruOrder.Remove(elem)
				delete(shard.lruElements, key)
			}
			shard.deleteLocked(key)
		}
		totalDeleted += len(keysToDelete)
		shard.mu.Unlock()
	}

	// Invalidate entire bundle in sharded cache
	s.documentPageCache.InvalidateBundle(bundleName)
	s.logger.Debugf("Invalidated %d pages for bundle %s (document %s not in page map)", totalDeleted, bundleName, documentID)
}

// flushHashIndexToDisk persists hash index changes to disk for durability
func (s *BundleService) flushHashIndexToDisk(hashIndex *hashindex.HashIndexV3, bundle *models.Bundle, indexName string) error {
	// === OLD V2 IMPLEMENTATION (Commented out) ===
	// Flush all dirty pages to ensure index changes are persisted
	// if err := hashIndex.FlushAllDirtyPages(); err != nil {
	// 	return fmt.Errorf("failed to flush dirty pages for index '%s': %w", indexName, err)
	// }
	// Persist metadata to record updated record counts
	// if err := hashIndex.PersistMetadata(); err != nil {
	// 	return fmt.Errorf("failed to persist metadata for index '%s': %w", indexName, err)
	// }

	// === NEW V3 IMPLEMENTATION (LSM-style) ===
	// In V3, Flush() handles both memtable and metadata persistence
	if err := hashIndex.Flush(); err != nil {
		return fmt.Errorf("failed to flush hash index V3 '%s': %w", indexName, err)
	}

	if settings.GetSettings().Debug {
		s.logger.Debugf("Hash index V3 '%s' successfully persisted to disk", indexName)
	}

	return nil
}

func (s *BundleService) DeleteBundle(database *models.Database, bundleCommand *models.BundleCommand) error {

	bundle, err := s.GetBundleByName(database, bundleCommand.BundleName)
	if err != nil {
		return fmt.Errorf("failed to find bundle '%s' for deletion: %w", bundleCommand.BundleName, err)
	}

	// === VALIDATE REFERENTIAL INTEGRITY ===
	// Only perform validation if FORCE flag was not specified
	if !bundleCommand.HasForceSwitch {
		// Check if any other bundles have relationships pointing to this bundle
		validator := NewReferentialIntegrityValidator(s, s.logger)

		// Create operation-scoped cache
		bundleCache := make(map[string]*models.Bundle)

		// STEP 1: Validate relationship metadata (schema-level validation)
		violations := validator.ValidateIncomingRelationships(database, bundle.Name, bundleCache)
		if len(violations) > 0 {
			// Log first violation and return error
			firstViolation := violations[0]
			s.logger.Warnf("[REFINT] Found %d incoming relationship(s) that would be orphaned by deleting bundle '%s'",
				len(violations), bundle.Name)
			return fmt.Errorf("%s", firstViolation.Error())
		}

		// STEP 2: Validate document-level foreign key references (data-level validation)
		// This checks if any documents in other bundles actually reference documents in this bundle
		thorough := settings.GetSettings().RestrictValidationThorough
		sampleSize := settings.GetSettings().RestrictValidationSampleSize
		logProgress := settings.GetSettings().RestrictValidationLogProgress

		s.logger.Debugf("[DROP-RESTRICT] Starting document-level validation for bundle '%s' (thorough=%v, sampleSize=%d, logProgress=%v)",
			bundle.Name, thorough, sampleSize, logProgress)

		if err := validator.ValidateDropBundleDocumentReferences(database, bundle, bundleCache, thorough, sampleSize, logProgress); err != nil {
			s.logger.Warnf("[DROP-RESTRICT] Document-level validation failed for bundle '%s': %v", bundle.Name, err)
			return err
		}

		s.logger.Debugf("[DROP-RESTRICT] All validations passed for bundle '%s' - no violations found", bundle.Name)
	} else {
		s.logger.Warnf("[DROP-RESTRICT] FORCE flag specified - skipping referential integrity validation for bundle '%s'", bundle.Name)
	}

	// Close all indexes for this bundle
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexInstance != nil {
				switch idx := indexRef.IndexInstance.(type) {
				case *hashindex.HashIndexV3:
					if err := idx.Close(); err != nil {
						s.logger.Warnf("Failed to close hash index '%s': %v", indexName, err)
					}
				case *btreeindexV2.BTreeIndex:
					if err := idx.Close(); err != nil {
						s.logger.Warnf("Failed to close btree index '%s': %v", indexName, err)
					}
				}
			}
		}
	}

	// Remove the bundle from the file system
	if err := s.store.RemoveBundleFile(database, bundle.Name); err != nil {
		return fmt.Errorf("failed to delete bundle '%s': %w", bundle.Name, err)
	}

	// Remove the bundle from in-memory metadata
	delete(database.Bundles, bundle.Name)

	// CRITICAL FIX: Remove from bundleMetadata cache to prevent stale index references
	// When a bundle is dropped and recreated with the same name, the old bundle object
	// with closed index instances must be fully removed from memory. Without this,
	// the stale bundle entry remains in bundleMetadata with closed indexes, causing
	// "document not found in bundle" errors when the recreated bundle tries to use
	// the old closed index instances instead of creating fresh ones.
	delete(s.bundleMetadata, bundle.Name)

	// PHASE 5: Clear the document-page location cache using sharded cache
	s.documentPageCache.InvalidateBundle(bundle.Name)

	s.logger.Debugf("Cleared document-page cache for deleted bundle: %s", bundle.Name)

	// Remove bundle from plan-cache metadata to avoid unbounded growth
	s.removeBundleFromPlanCacheMetadata(bundle.Name)

	// GRAPHQL INTEGRATION: Tombstone all GraphQL schemas when bundle is deleted
	// This marks all schema versions as deleted in the schema file, preventing their use in queries.
	// The schema manager handles tombstoning all versions atomically.
	//
	// Important: This doesn't physically delete schemas from the file (they remain for audit/rollback),
	// but marks them as deleted so GraphQL queries will not use them.
	//
	// Note: Tombstoning failures are logged but don't fail the bundle deletion.
	// The bundle is already deleted from disk, so the operation is considered successful.
	if s.graphQLEnabled && database != nil {
		s.logger.Debugf("[GraphQL] Tombstoning schemas for deleted bundle '%s' in database '%s'", bundle.Name, database.Name)

		// PHASE 5: Get the schema manager using sharded map (no global lock)
		schemaManager, exists := s.schemaManagers.Get(database.Name)

		if exists && schemaManager != nil {
			// Tombstone all schema versions for this bundle
			// This is an atomic operation that marks all versions as deleted
			err := schemaManager.TombstoneAllSchemasForBundle(bundle.Name)
			if err != nil {
				s.logger.Warnf("[GraphQL] Failed to tombstone schemas for deleted bundle '%s': %v. Schemas may remain in cache.", bundle.Name, err)
			} else {
				s.logger.Debugf("[GraphQL] All schema versions tombstoned for deleted bundle '%s'", bundle.Name)
			}
		} else {
			s.logger.Debugf("[GraphQL] No schema manager found for database '%s' - skipping schema tombstoning", database.Name)
		}
	}

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
