package hashindexV3

/*
HASH INDEX API - PUBLIC LSM-STYLE HASH INDEX INTERFACE

This file provides the public API for the LSM-style hash index implementation.
It coordinates between MemTable (in-memory cache), EntryStorage (disk persistence),
and CompactionManager (file optimization) to provide a complete hash index solution.

KEY RESPONSIBILITIES:
- Provide simple Put/Get/Delete operations for hash index entries
- Coordinate between MemTable and EntryStorage layers
- Manage global sequence numbers for temporal ordering
- Trigger compaction when needed
- Handle index initialization and persistence

DESIGN PRINCIPLES:
- Single Responsibility: Only provides public API coordination
- Open/Closed: Extensible through strategy patterns (compaction, etc.)
- Liskov Substitution: Compatible with existing HashIndex interface
- Dependency Inversion: Depends on abstractions (CompactionStrategy)

LSM ARCHITECTURE:
┌──────────────────────────────────────────────────────────┐
│                    HashIndexV3 API                       │
├──────────────────────────────────────────────────────────┤
│  Put(key, docID)  │  Get(key)  │  Delete(key)           │
└────────┬────────────────┬──────────────┬─────────────────┘
         │                │              │
         ▼                ▼              ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   MemTable      │ │  EntryStorage   │ │CompactionManager│
│ (Fast Lookup)   │ │ (Durable Store) │ │ (Optimization)  │
└─────────────────┘ └─────────────────┘ └─────────────────┘

WRITE PATH:
1. Append entry to EntryStorage (durability first)
2. Update MemTable (fast reads)
3. Check if compaction needed
4. Return success

READ PATH:
1. Check MemTable first (O(1) if cached)
2. If miss, scan EntryStorage backward (latest wins)
3. Cache result in MemTable
4. Return result

DELETE PATH:
1. Append tombstone to EntryStorage
2. Update MemTable with tombstone
3. Return success
(Tombstones removed during compaction)

TODO: Future extensions
- Bloom filters for negative lookups
- Range queries (currently only point lookups)
- Multi-version reads (MVCC)
- Batch operations for efficiency
- Statistics dashboard
- Background compaction threads
*/

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syndrdb/src/pkg/constants"
	"time"

	"go.uber.org/zap"
)

// HashIndexV3 is the main LSM-style hash index coordinator
// It combines MemTable, EntryStorage, and CompactionManager
type HashIndexV3 struct {
	// Configuration
	config IndexConfig

	// Core components
	MemTable  *HashMemTable     // In-memory cache layer (exposed for direct access)
	storage   *EntryStorage     // Disk persistence layer
	compactor CompactionManager // File optimization (future integration)

	// Sequence management for temporal ordering
	// Uses atomic operations for thread-safe increments
	GlobalSequence uint64 // Exposed for atomic access from bundle service

	// Concurrency control
	// Note: Individual components have their own locks
	// This mutex is for coordinating between components
	mutex sync.RWMutex

	// State management
	isOpen  bool
	closed  bool
	isDirty bool // Metadata needs persistence

	// Statistics
	stats         IndexStats
	statsMutex    sync.RWMutex       // Separate mutex for stats
	queryOptStats *IndexStatsTracker // Query optimization statistics for join planning

	// Logging
	logger *zap.SugaredLogger

	// Observability: metrics reporter callback
	metricsReporter func(metricName string, value uint64)
}

// IndexConfig holds configuration for the hash index
type IndexConfig struct {
	// Identity
	IndexName    string // Name of the index (e.g., "DocumentID_idx")
	BundleName   string // Name of the bundle
	DatabaseName string // Name of the database
	FieldName    string // Field being indexed

	// Index metadata flags
	IsForeignKey bool // True if this is a foreign key index
	IsUnique     bool // True if index enforces uniqueness
	IsPrimaryKey bool // True if this is the primary key index

	// Foreign key relationship (only if IsForeignKey == true)
	ReferencedBundle string // Target bundle name
	ReferencedField  string // Target field name

	// Storage configuration
	DataDir         string // Directory for index files
	MaxFileSize     int64  // Maximum entry file size before rotation
	WriteBufferSize int    // Write buffer size for EntryStorage

	// Bucket configuration (for hash-based bucketing optimization)
	NumBuckets        uint32 // Number of buckets for hash distribution (default: 256)
	BucketFileMaxSize int64  // Maximum size per bucket file before rotation (default: 64MB)
	BucketConcurrency int    // Number of parallel bucket operations (default: 4)

	// MemTable configuration
	MemTableMaxSize         int // Maximum entries in MemTable before flush recommended
	MemTableWALBufferMaxSize int // Maximum WAL buffer entries before swap (0 = use default 50000)

	// Sequence recovery configuration
	SequenceSafetyMargin int // Safety margin for sequence recovery (default: 100)

	// Compaction configuration
	CompactionEnabled  bool // Enable automatic compaction
	CompactionMaxFiles int  // Trigger compaction when file count exceeds this

	// TODO: Future compaction configuration
	// CompactionStrategy: Pluggable strategy (file count, size, tombstone ratio)
	// CompactionSchedule: Time-based compaction schedule
	// CompactionConcurrency: Number of parallel compaction threads

	// Logging
	Logger *zap.SugaredLogger

	// Observability: metrics reporter callback for exporting index stats to GlobalServerMetrics
	MetricsReporter func(metricName string, value uint64)
}

// IndexStats tracks index performance metrics
// Note: This struct is exported and copied, so it should not contain a mutex
type IndexStats struct {
	// Operation counts
	TotalPuts    uint64
	TotalGets    uint64
	TotalDeletes uint64
	TotalScans   uint64

	// Performance metrics
	CacheHits   uint64
	CacheMisses uint64
	DiskReads   uint64

	// Storage metrics
	TotalEntries   uint64
	TotalFiles     uint64
	TotalSizeBytes uint64
	TombstoneCount uint64

	// Compaction metrics
	CompactionCount    uint64
	LastCompactionTime time.Time

	// Sequence tracking (for LSM ordering)
	MaxSequence uint64 // Highest sequence number assigned

	// Lifecycle
	CreatedAt    time.Time
	LastModified time.Time
}

// CompactionManager interface for LSM-style file compaction
// Implemented by the compactor package's CompactionManager
// Using interface to avoid circular dependencies
type CompactionManager interface {
	// ShouldCompact determines if compaction is needed based on file list
	// The implementation examines file count, sizes, etc.
	ShouldCompact(files []string) bool

	// CompactHashIndexFiles merges multiple entry files into one
	// Parameters:
	//   - bundleName: Name of the bundle
	//   - indexName: Name of the index
	//   - entryFiles: List of entry files to compact (absolute paths)
	// Returns the path to the compacted file and any error
	CompactHashIndexFiles(bundleName, indexName string, entryFiles []string) (string, error)
}

// NewHashIndexV3 creates a new LSM-style hash index
// Parameters:
//   - config: Index configuration
//
// Returns initialized index ready for operations
func NewHashIndexV3(config IndexConfig) (*HashIndexV3, error) {
	// Validate configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Set defaults
	if config.MaxFileSize == 0 {
		config.MaxFileSize = DefaultMaxFileSize
	}
	if config.WriteBufferSize == 0 {
		config.WriteBufferSize = DefaultWriteBufferSize
	}
	if config.MemTableMaxSize == 0 {
		config.MemTableMaxSize = 100000 // 100K entries default
	}
	if config.MemTableWALBufferMaxSize == 0 {
		config.MemTableWALBufferMaxSize = 50000 // 50K default to bound memory
	}
	if config.CompactionMaxFiles == 0 {
		config.CompactionMaxFiles = 10
	}
	if config.NumBuckets == 0 {
		config.NumBuckets = 256 // 256 buckets default
	}
	if config.BucketFileMaxSize == 0 {
		config.BucketFileMaxSize = 64 * 1024 * 1024 // 64MB default
	}
	if config.BucketConcurrency == 0 {
		config.BucketConcurrency = 4 // 4 parallel compactions default
	}
	if config.Logger == nil {
		logger, _ := zap.NewProduction()
		config.Logger = logger.Sugar()
	}

	// Create MemTable (with WAL buffer cap to prevent unbounded growth)
	memTable := NewHashMemTable(config.MemTableMaxSize, config.MemTableWALBufferMaxSize)

	// Create EntryStorage
	storageConfig := EntryStorageConfig{
		IndexName:         config.IndexName,
		FieldName:         config.FieldName,
		BundleName:        config.BundleName,
		DataDir:           config.DataDir,
		MaxFileSize:       config.MaxFileSize,
		WriteBufferSize:   config.WriteBufferSize,
		IsForeignKey:      config.IsForeignKey,
		IsUnique:          config.IsUnique,
		IsPrimaryKey:      config.IsPrimaryKey,
		ReferencedBundle:  config.ReferencedBundle,
		ReferencedField:   config.ReferencedField,
		UseBuckets:        true, // Enable bucket-based storage (Phase 2: xxHash)
		NumBuckets:        config.NumBuckets,
		BucketFileMaxSize: config.BucketFileMaxSize,
		Logger:            config.Logger,
	}

	storage, err := NewEntryStorage(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create entry storage: %w", err)
	}

	// Create index
	idx := &HashIndexV3{
		config:          config,
		MemTable:        memTable,
		storage:         storage,
		GlobalSequence:  0,
		isOpen:          true,
		closed:          false,
		metricsReporter: config.MetricsReporter,
		stats: IndexStats{
			CreatedAt:    time.Now(),
			LastModified: time.Now(),
		},
		queryOptStats: NewIndexStatsTracker(),
		logger:        config.Logger,
	}

	// TODO: Sprint 4 - Initialize CompactionManager
	// idx.compactor = compactor.NewCompactionManager(compactorConfig)

	config.Logger.Debugw("Created new LSM-style hash index",
		"indexName", config.IndexName,
		"bundleName", config.BundleName,
		"dataDir", config.DataDir)

	return idx, nil
}

// OpenHashIndexV3 opens an existing hash index from disk
// Parameters:
//   - config: Index configuration
//
// Returns opened index with data loaded from disk
func OpenHashIndexV3(config IndexConfig) (*HashIndexV3, error) {
	// Create index structure
	idx, err := NewHashIndexV3(config)
	if err != nil {
		return nil, err
	}

	// Restore global sequence from metadata (Option B: hybrid approach)
	// This ensures new entries always have higher sequence than existing ones
	err = idx.RestoreGlobalSequence()
	if err != nil {
		return nil, fmt.Errorf("failed to restore global sequence: %w", err)
	}

	// Update header with restored sequence to prevent mismatch warnings on next open
	// This is especially important for indexes created before the header update fix
	currentSequence := atomic.LoadUint64(&idx.GlobalSequence)
	if err := idx.storage.UpdateHeaderStatistics(currentSequence); err != nil {
		idx.logger.Warnw("Failed to update header after sequence restoration",
			"error", err,
			"sequence", currentSequence)
		// Don't fail the open operation, just log the warning
	}

	// Load entries from disk into MemTable
	// This provides fast startup for recently accessed data
	// TODO: Sprint 4 - Add configurable MemTable preloading
	// For large indexes, we may want to lazily load on first access
	// rather than loading all entries upfront

	err = idx.loadRecentEntries()
	if err != nil {
		return nil, fmt.Errorf("failed to load recent entries: %w", err)
	}

	idx.logger.Infow("Opened existing hash index",
		"indexName", config.IndexName,
		"memTableSize", idx.MemTable.GetStats().Size,
		"globalSequence", atomic.LoadUint64(&idx.GlobalSequence))

	return idx, nil
}

// Put inserts or updates a key-value mapping in the index with document location
// Parameters:
//   - keyValue: The value being indexed
//   - documentID: The document UUID
//   - pageID: The physical page number where the document resides
//   - commitSequence: Document's commit sequence (0 = uncommitted)
//   - versionSequence: Document's version sequence within DocumentID
//
// Returns error if operation fails
// PHASE 4: MVCC - Added commitSequence and versionSequence parameters for visibility filtering
func (idx *HashIndexV3) Put(keyValue, documentID string, pageID uint32, commitSequence, versionSequence uint64) error {
	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	// Validate inputs
	if keyValue == "" {
		return fmt.Errorf("key value cannot be empty")
	}
	if documentID == "" {
		return fmt.Errorf("document ID cannot be empty")
	}

	// Get next sequence number (atomic for thread safety)
	// MED-011: Check for overflow before incrementing sequence
	currentSequence := atomic.LoadUint64(&idx.GlobalSequence)
	if err := constants.CheckUint64Increment(currentSequence, "GlobalSequence"); err != nil {
		return fmt.Errorf("sequence number overflow: %w", err)
	}
	sequence := atomic.AddUint64(&idx.GlobalSequence, 1)

	// Update max sequence in stats and mark dirty
	idx.statsMutex.Lock()
	if sequence > idx.stats.MaxSequence {
		idx.stats.MaxSequence = sequence
		idx.isDirty = true
	}
	idx.statsMutex.Unlock()

	// Create entry with page location and version metadata
	entry := NewHashIndexEntry(keyValue, documentID, pageID, sequence, commitSequence, versionSequence)

	// Populate bucket number based on hash value
	// This enables bucket-based file organization and O(1) lookups
	// MED-011: Added overflow protection for bucket calculation
	bucketNum, err := ComputeBucketNum(entry.HashValue, idx.config.NumBuckets)
	if err != nil {
		return fmt.Errorf("bucket calculation failed: %w", err)
	}
	entry.BucketNum = bucketNum

	// WRITE PATH (LSM-style):
	// 1. Write to disk first (durability)
	// 2. Then update MemTable (performance)
	// This ensures we never lose writes even if we crash

	// Step 1: Append to disk storage
	err = idx.storage.AppendEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to append entry to storage: %w", err)
	}

	// Step 2: Update MemTable
	err = idx.MemTable.Put(entry)
	if err != nil {
		// Entry is on disk but not in cache - this is OK
		// Future reads will find it via disk scan
		idx.logger.Warnw("Failed to update MemTable (entry is on disk)",
			"key", keyValue,
			"error", err)
	}

	// Step 3: Periodically update header with current global sequence
	// This ensures the header stays reasonably in sync with actual data
	// We do this after every N entries to balance durability vs performance
	count := atomic.AddUint64(&idx.storage.entriesSinceHeaderUpdate, 1)
	if count >= idx.storage.headerUpdateInterval {
		// Reset counter atomically
		atomic.StoreUint64(&idx.storage.entriesSinceHeaderUpdate, 0)

		// Update header with current sequence
		currentSequence := atomic.LoadUint64(&idx.GlobalSequence)
		if err := idx.storage.UpdateHeaderStatistics(currentSequence); err != nil {
			// Log warning but don't fail the Put operation
			// Header update is important but not critical for correctness
			idx.logger.Warnw("Failed to update header statistics",
				"error", err,
				"sequence", currentSequence)
		}
	}

	// Update statistics
	idx.updatePutStats()

	// Periodically check if compaction is needed (only when compactor is set)
	// Check every 1000 writes to avoid overhead; skip goroutine when compactor is nil
	if idx.compactor != nil && atomic.LoadUint64(&idx.GlobalSequence)%1000 == 0 {
		// Run in background to avoid blocking writes
		go idx.triggerCompaction()
	}

	return nil
}

// Get retrieves document IDs and page locations for a given key value
// PHASE 4: MVCC - Optional snapshot filtering for visibility
// Parameters:
//   - keyValue: The value to search for
//   - snapshotSequence: Optional snapshot sequence for MVCC filtering (0 = no filtering, return latest)
//   - txID: Optional transaction ID for uncommitted visibility (0 = no filtering)
//   - activeTxIDs: Optional map of active transaction IDs at snapshot time (nil = no filtering)
//
// Returns:
//   - documentIDs: List of document IDs matching the key (filtered by visibility if snapshot provided)
//   - pageIDs: Corresponding page locations for each document (parallel array)
//   - error: Any error that occurred
func (idx *HashIndexV3) Get(keyValue string, snapshotSequence ...uint64) ([]string, []uint32, error) {
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if keyValue == "" {
		return nil, nil, fmt.Errorf("key value cannot be empty")
	}

	// Track lookup performance for query optimization
	startTime := time.Now()

	// READ PATH (LSM-style with bucket optimization):
	// 1. Check MemTable first (O(1) if cached)
	// 2. If not found, compute bucket and scan only that bucket's files (O(1) bucket + O(files_per_bucket))
	// 3. Cache result in MemTable for future reads
	//
	// Performance: MemTable is always fast (O(1)). Bucket-optimized disk scan is 50-100x faster
	// than full disk scan because we only read 1/256th of the files.

	// PHASE 4: MVCC - Extract snapshot parameters if provided
	var snapshotSeq uint64
	if len(snapshotSequence) > 0 {
		snapshotSeq = snapshotSequence[0]
	}

	// Step 1: Check MemTable
	entry, found := idx.MemTable.Get(keyValue)
	if found {
		idx.updateCacheHit()
		idx.updateGetStats()

		// Check if this is a tombstone (deleted)
		if entry.Deleted {
			idx.logger.Debugw("Get: Found tombstone in MemTable",
				"key", keyValue,
				"sequence", entry.Sequence,
				"elapsed", time.Since(startTime))
			// Track lookup with zero results
			idx.queryOptStats.RecordLookup(0, time.Since(startTime))
			return []string{}, []uint32{}, nil
		}
		idx.logger.Debugw("Get: Found NON-deleted entry in MemTable",
			"key", keyValue,
			"deleted", entry.Deleted,
			"sequence", entry.Sequence,
			"documentID", entry.DocumentID,
			"pageID", entry.PageID,
			"elapsed", time.Since(startTime))

		// PHASE 4: MVCC - If snapshot provided, check visibility
		if snapshotSeq > 0 {
			if !idx.isEntryVisible(entry, snapshotSeq) {
				// Entry not visible to snapshot - need to scan disk for visible versions
				// Fall through to disk scan
			} else {
				// Entry is visible - return it
				idx.queryOptStats.RecordLookup(1, time.Since(startTime))
				return []string{entry.DocumentID}, []uint32{entry.PageID}, nil
			}
		} else {
			// No snapshot filtering - return latest from memtable
			idx.queryOptStats.RecordLookup(1, time.Since(startTime))
			return []string{entry.DocumentID}, []uint32{entry.PageID}, nil
		}
	}

	// Step 2: MemTable miss or snapshot filtering needed - use bucket-optimized disk scan
	idx.updateCacheMiss()
	idx.logger.Debugw("Get: MemTable miss, scanning disk",
		"key", keyValue,
		"elapsed", time.Since(startTime))

	// PHASE 4: MVCC - Collect all versions if snapshot filtering is needed
	var allEntries []*HashIndexEntry
	var latestEntry *HashIndexEntry
	var err error

	if idx.storage.useBuckets {
		// OPTIMIZED PATH: Scan only the specific bucket (50-100x faster)
		// MED-011: Added overflow protection for bucket calculation
		hashValue := ComputeHash(keyValue)
		bucketNum, err := ComputeBucketNum(hashValue, idx.config.NumBuckets)
		if err != nil {
			return nil, nil, fmt.Errorf("bucket calculation failed: %w", err)
		}

		// DIAG: Log the bucket lookup path for read-side tracing
		idx.logger.Warnw("[BUCKET-DIAG] Get: disk lookup",
			"key", keyValue,
			"hashValue", hashValue,
			"bucketNum", bucketNum,
			"numBuckets", idx.config.NumBuckets,
			"snapshotSeq", snapshotSeq,
			"useBuckets", idx.storage.useBuckets)

		if snapshotSeq > 0 {
			// PHASE 4: MVCC - Collect all versions for visibility filtering
			allEntries, err = idx.getAllEntriesFromBucket(keyValue, bucketNum)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan bucket for versions: %w", err)
			}
			// Filter by visibility and get latest visible
			latestEntry = idx.getLatestVisibleEntry(allEntries, snapshotSeq)
		} else {
			// No snapshot - just get latest
			latestEntry, err = idx.storage.GetLatestEntryFromBucket(keyValue, bucketNum)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan bucket: %w", err)
			}
		}

		// Backward compatibility: entries written before BucketNum fix all landed in bucket 0.
		// If the correct bucket returned nothing and it is not bucket 0, try bucket 0.
		if latestEntry == nil && bucketNum != 0 {
			// DIAG: Log fallback attempt to bucket 0
			idx.logger.Warnw("[BUCKET-DIAG] Get: primary bucket empty, trying bucket 0 fallback",
				"key", keyValue,
				"primaryBucket", bucketNum)

			if snapshotSeq > 0 {
				var fallbackEntries []*HashIndexEntry
				fallbackEntries, _ = idx.getAllEntriesFromBucket(keyValue, 0)
				if len(fallbackEntries) > 0 {
					latestEntry = idx.getLatestVisibleEntry(fallbackEntries, snapshotSeq)
				}
			} else {
				latestEntry, _ = idx.storage.GetLatestEntryFromBucket(keyValue, 0)
			}

			// DIAG: Log fallback result
			idx.logger.Warnw("[BUCKET-DIAG] Get: bucket 0 fallback result",
				"key", keyValue,
				"found", latestEntry != nil)
		}
	} else {
		// LEGACY PATH: Scan all files (backward compatibility)
		if snapshotSeq > 0 {
			// PHASE 4: MVCC - Collect all versions
			allEntries = make([]*HashIndexEntry, 0)
			err = idx.storage.ScanBackward(func(entry *HashIndexEntry) bool {
				if entry.KeyValue == keyValue {
					allEntries = append(allEntries, entry)
				}
				return true // Continue scanning to collect all versions
			})
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan storage: %w", err)
			}
			// Filter by visibility
			latestEntry = idx.getLatestVisibleEntry(allEntries, snapshotSeq)
		} else {
			// No snapshot - just get latest
			err = idx.storage.ScanBackward(func(entry *HashIndexEntry) bool {
				if entry.KeyValue == keyValue {
					latestEntry = entry
					return false // Found it, stop scanning
				}
				return true // Keep scanning
			})
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan storage: %w", err)
			}
		}
	}

	if latestEntry == nil {
		// Not found anywhere or no visible versions
		idx.queryOptStats.RecordLookup(0, time.Since(startTime))
		return []string{}, []uint32{}, nil
	}

	// Step 3: Re-check MemTable for concurrent writes, then cache.
	// Another goroutine may have written a newer entry after our initial miss.
	// If we blindly cache our disk result, MemTable.Put would reject an older
	// entry and we could return stale data. Re-check and use MemTable's value
	// when it is newer; on Put "attempted to insert older" use the in-memory
	// entry as the authoritative result (no WARN—expected under concurrency).

	// CRITICAL: Before caching, ensure GlobalSequence is at least as high as this entry
	// This handles the case where RestoreGlobalSequence() didn't find all entries
	// (e.g., headers not yet updated) and we're loading an older high-sequence entry from disk
	if latestEntry.Sequence > 0 {
		for {
			currentGlobal := atomic.LoadUint64(&idx.GlobalSequence)
			if latestEntry.Sequence < currentGlobal {
				break // GlobalSequence is already higher
			}
			// Need to update GlobalSequence to at least latestEntry.Sequence + safety margin
			newGlobal := latestEntry.Sequence + 100
			if atomic.CompareAndSwapUint64(&idx.GlobalSequence, currentGlobal, newGlobal) {
				idx.logger.Debugw("Updated GlobalSequence after loading high-sequence entry from disk",
					"keyValue", keyValue,
					"entrySequence", latestEntry.Sequence,
					"oldGlobalSequence", currentGlobal,
					"newGlobalSequence", newGlobal)
				break
			}
			// CAS failed, retry
		}
	}

	entryNow, foundNow := idx.MemTable.Get(keyValue)
	if foundNow && entryNow.IsNewer(latestEntry) {
		latestEntry = entryNow
	} else {
		err = idx.MemTable.Put(latestEntry)
		if err != nil {
			if entryFinal, foundFinal := idx.MemTable.Get(keyValue); foundFinal {
				latestEntry = entryFinal
			}
		}
	}

	// Check if this is a tombstone
	if latestEntry.Deleted {
		idx.queryOptStats.RecordLookup(0, time.Since(startTime))
		return []string{}, []uint32{}, nil
	}

	idx.updateGetStats()

	// Track lookup with one result
	idx.queryOptStats.RecordLookup(1, time.Since(startTime))
	return []string{latestEntry.DocumentID}, []uint32{latestEntry.PageID}, nil
}

// getAllEntriesFromBucket collects all entries for a key from a specific bucket
// PHASE 4: MVCC - Helper to collect all versions for visibility filtering
func (idx *HashIndexV3) getAllEntriesFromBucket(keyValue string, bucketNum uint32) ([]*HashIndexEntry, error) {
	allEntries := make([]*HashIndexEntry, 0)
	err := idx.storage.ScanBucket(bucketNum, func(entry *HashIndexEntry) bool {
		if entry.KeyValue == keyValue && entry.BucketNum == bucketNum {
			allEntries = append(allEntries, entry)
		}
		return true // Continue scanning to collect all versions
	})
	return allEntries, err
}

// isEntryVisible checks if an index entry is visible to a snapshot
// PHASE 4: MVCC - Visibility filtering for index entries
// Uses same rules as Document.IsVisibleToSnapshot but works with index entry metadata
func (idx *HashIndexV3) isEntryVisible(entry *HashIndexEntry, snapshotSeq uint64) bool {
	if entry == nil {
		return false
	}

	// Rule 1: Uncommitted entries (CommitSequence == 0) are only visible to creating transaction
	// For index entries, we can't check CreatedByTxID directly, so we skip uncommitted entries
	// The document-level check will handle read-your-own-writes
	if entry.CommitSequence == 0 {
		return false // Uncommitted - not visible (document-level will handle own writes)
	}

	// Rule 2: Must be committed before snapshot boundary
	if entry.CommitSequence > snapshotSeq {
		return false
	}

	// Rule 3: Not deleted (tombstones are not visible)
	if entry.Deleted {
		return false
	}

	return true
}

// getLatestVisibleEntry finds the latest visible entry from a list of entries
// PHASE 4: MVCC - Filters entries by visibility and returns newest visible version
func (idx *HashIndexV3) getLatestVisibleEntry(entries []*HashIndexEntry, snapshotSeq uint64) *HashIndexEntry {
	var latestVisible *HashIndexEntry
	var latestVersionSeq uint64

	for _, entry := range entries {
		if idx.isEntryVisible(entry, snapshotSeq) {
			// Keep the entry with highest VersionSequence (newest version)
			if latestVisible == nil || entry.VersionSequence > latestVersionSeq {
				latestVisible = entry
				latestVersionSeq = entry.VersionSequence
			}
		}
	}

	return latestVisible
}

// BatchGet retrieves document IDs for multiple key values in parallel
// This is optimized for JOIN operations where we need to look up many keys at once
// Uses goroutines to parallelize lookups across buckets
// Parameters:
//   - keyValues: Slice of key values to look up
//
// Returns:
//   - results: Map from key value -> document IDs
//   - error: Any error that occurred during batch lookup
//
// TODO: Add batching configuration (currently uses 256 keys per goroutine batch)
// TODO: Consider adding context for cancellation
func (idx *HashIndexV3) BatchGet(keyValues []string) (map[string][]string, error) {
	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	if len(keyValues) == 0 {
		return make(map[string][]string), nil
	}

	// Use a map to collect results from parallel goroutines
	results := make(map[string][]string, len(keyValues))
	var resultsMutex sync.Mutex
	var wg sync.WaitGroup
	var firstError error
	var errorOnce sync.Once

	// Process keys in batches of 256 for optimal parallelism
	const batchSize = 256
	numBatches := (len(keyValues) + batchSize - 1) / batchSize

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		wg.Add(1)

		// Capture batch range for this goroutine
		start := batchIdx * batchSize
		end := start + batchSize
		if end > len(keyValues) {
			end = len(keyValues)
		}
		batchKeys := keyValues[start:end]

		go func(keys []string) {
			defer wg.Done()

			// Process each key in this batch
			for _, keyValue := range keys {
				if keyValue == "" {
					continue // Skip empty keys
				}

				// Use the existing Get() method which has MemTable caching and bucket optimization
				docIDs, _, err := idx.Get(keyValue)
				if err != nil {
					// Record first error and continue (fail-fast on first error)
					errorOnce.Do(func() {
						firstError = fmt.Errorf("failed to get key %s: %w", keyValue, err)
					})
					return
				}

				// Store results
				if len(docIDs) > 0 {
					resultsMutex.Lock()
					results[keyValue] = docIDs
					resultsMutex.Unlock()
				}
			}
		}(batchKeys)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	if firstError != nil {
		return nil, firstError
	}

	return results, nil
}

// Delete marks a key as deleted by appending a tombstone
// The actual entry is not removed until compaction runs
// Parameters:
//   - keyValue: The value to delete
//   - commitSequence: Commit sequence when deletion was committed (0 = uncommitted)
//
// Returns true if key was found and deleted, false if not found
// PHASE 4: MVCC - Added commitSequence parameter for visibility filtering
func (idx *HashIndexV3) Delete(keyValue string, commitSequence uint64) (bool, error) {
	if idx.closed {
		return false, fmt.Errorf("index is closed")
	}

	if keyValue == "" {
		return false, fmt.Errorf("key value cannot be empty")
	}

	// IMPORTANT: Always write tombstone even if Get() returns no results
	// This handles race conditions where:
	// 1. Entry exists on disk but not in MemTable (cache miss)
	// 2. Concurrent readers might have cached the entry
	// 3. Ensures all future Get() calls see the deletion
	// The tombstone will be a no-op during compaction if key never existed

	// Get next sequence number (with overflow check, same as Put)
	currentSequence := atomic.LoadUint64(&idx.GlobalSequence)
	if err := constants.CheckUint64Increment(currentSequence, "GlobalSequence"); err != nil {
		return false, err
	}
	sequence := atomic.AddUint64(&idx.GlobalSequence, 1)

	// Update max sequence in stats and mark dirty
	idx.statsMutex.Lock()
	if sequence > idx.stats.MaxSequence {
		idx.stats.MaxSequence = sequence
		idx.isDirty = true
	}
	idx.statsMutex.Unlock()

	// Create tombstone entry with commit sequence
	tombstone := NewTombstoneEntry(keyValue, sequence, commitSequence)

	// DELETE PATH (LSM-style):
	// 1. Append tombstone to disk (durability)
	// 2. Update MemTable with tombstone (future reads will see deletion)
	// 3. Tombstone will be removed during compaction

	// Step 1: Append tombstone to disk
	err := idx.storage.AppendEntry(tombstone)
	if err != nil {
		return false, fmt.Errorf("failed to append tombstone: %w", err)
	}

	// Step 2: Update MemTable
	err = idx.MemTable.Put(tombstone)
	if err != nil {
		// If MemTable rejects tombstone (e.g., older sequence), force delete directly
		idx.logger.Warnw("MemTable rejected tombstone, forcing direct deletion",
			"key", keyValue,
			"tombstoneSeq", tombstone.Sequence,
			"error", err)
		// Use MemTable.Delete which doesn't check sequences
		if delErr := idx.MemTable.Delete(keyValue, tombstone.Sequence, commitSequence); delErr != nil {
			idx.logger.Warnw("Failed to force delete in MemTable",
				"key", keyValue,
				"error", delErr)
		} else {
			idx.logger.Infow("Delete: Tombstone created via MemTable.Delete fallback",
				"key", keyValue,
				"sequence", tombstone.Sequence)
		}
	} else {
		idx.logger.Debugw("Delete: Tombstone created successfully",
			"key", keyValue,
			"sequence", tombstone.Sequence)
	}

	idx.updateDeleteStats()

	// Return true to indicate tombstone was written (even if key didn't exist before)
	// This matches LSM semantics: delete is idempotent
	return true, nil
}

// GetAllDocumentIDs returns all document IDs from the index grouped by page ID
// Used for hash join build phase to avoid GetAllDocuments() lock contention
// PostgreSQL-style: Uses index to get document IDs, then loads by page
//
// Returns:
//   - docIDsByPage: Map from pageID -> []documentID for efficient page-based loading
//   - error: Any error that occurred during index scan
//
// Performance: Scans index files once, groups by page to minimize page loads
// Filters out tombstones (deleted documents) automatically
// MemTable entries take precedence (newer than disk)
func (idx *HashIndexV3) GetAllDocumentIDs() (map[uint32][]string, error) {
	if idx.closed {
		return nil, fmt.Errorf("index is closed")
	}

	docIDsByPage := make(map[uint32][]string)
	seenDocs := make(map[string]bool) // Deduplicate: docID -> pageID mapping

	// First, collect from MemTable via iterator (encapsulation; no direct mutex/entries access)
	memTableDocs := make(map[string]*HashIndexEntry)
	idx.MemTable.IterateNonDeleted(func(key string, entry *HashIndexEntry) bool {
		memTableDocs[key] = entry
		return true
	})

	// Add MemTable entries first (they're the latest)
	for _, entry := range memTableDocs {
		docKey := fmt.Sprintf("%s:%d", entry.DocumentID, entry.PageID)
		if !seenDocs[docKey] {
			seenDocs[docKey] = true
			docIDsByPage[entry.PageID] = append(docIDsByPage[entry.PageID], entry.DocumentID)
		}
	}

	// Then scan disk entries, but skip if already in MemTable (MemTable wins)
	err := idx.storage.ScanForward(func(entry *HashIndexEntry) bool {
		// Skip tombstones
		if entry.Deleted {
			return true
		}

		// Check if this entry is already in MemTable (MemTable has latest version)
		keyStr := entry.KeyValue
		if _, inMemTable := memTableDocs[keyStr]; inMemTable {
			return true // Skip - MemTable has newer version
		}

		// Deduplicate: same document ID might appear multiple times (updates)
		docKey := fmt.Sprintf("%s:%d", entry.DocumentID, entry.PageID)
		if !seenDocs[docKey] {
			seenDocs[docKey] = true
			docIDsByPage[entry.PageID] = append(docIDsByPage[entry.PageID], entry.DocumentID)
		}
		return true
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan index for document IDs: %w", err)
	}

	return docIDsByPage, nil
}

// Search is an alias for Get to maintain compatibility with hashindexV2
// Returns document IDs and page IDs; forwards optional snapshot sequence to Get()
func (idx *HashIndexV3) Search(keyValue string, snapshotSequence ...uint64) ([]string, []uint32, error) {
	docIDs, pageIDs, err := idx.Get(keyValue, snapshotSequence...)
	return docIDs, pageIDs, err
}

// NumBuckets returns the index's bucket count (for bucket-based file layout).
// Used by callers that build HashIndexEntry outside the API (e.g. single-write path)
// so they can set entry.BucketNum correctly before WriteEntryToDiskOnly.
func (idx *HashIndexV3) NumBuckets() uint32 {
	return idx.config.NumBuckets
}

// WriteEntryToDiskOnly appends the given entry to disk without updating MemTable or GlobalSequence.
// Used by the single-write path: scheduleIndexUpdate applies to MemTable and enqueues this entry;
// processHashIndexBatch calls this so the same entry/sequence is persisted once.
// Caller must ensure the entry has the correct sequence and BucketNum already assigned when
// the index uses bucket-based storage (so lookups find the entry in the right bucket).
func (idx *HashIndexV3) WriteEntryToDiskOnly(entry *HashIndexEntry) error {
	if idx.closed {
		return fmt.Errorf("index is closed")
	}
	if entry == nil {
		return fmt.Errorf("entry cannot be nil")
	}
	return idx.storage.AppendEntry(entry)
}

// Flush ensures all buffered data is written to disk
func (idx *HashIndexV3) Flush() error {
	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	// Get current global sequence for header update
	currentSequence := atomic.LoadUint64(&idx.GlobalSequence)

	// Flush entry storage with header update to persist current global sequence
	if err := idx.storage.FlushWithHeaderUpdate(currentSequence); err != nil {
		return err
	}

	// Clear WAL buffer after successful flush to prevent unbounded memory growth
	idx.MemTable.ClearWALBuffer()

	// Save metadata if dirty
	if idx.isDirty {
		return idx.SaveMetadata()
	}

	return nil
}

// CompactMemTableSafe performs a concurrent-safe compaction of the index's MemTable.
// This method can be called during high-concurrency writes without blocking writers
// for extended periods or causing crashes.
//
// DESIGN:
// Unlike Flush(), which persists data to disk, CompactMemTableSafe only reclaims
// memory by clearing the walBuffer and optionally rebuilding the entries map.
// The entries map is preserved for read performance (it's a cache, not WAL data).
//
// WHEN TO USE:
// - During sustained high-throughput write workloads
// - When memory usage is growing due to walBuffer accumulation
// - Periodically in a background goroutine (every 30-60 seconds)
//
// THREAD SAFETY:
// - Writers blocked only for microseconds during atomic swap
// - Reads continue normally during and after compaction
// - No risk of data loss - disk writes happen separately via normal Put() path
//
// Parameters:
//   - compactEntries: If true, also rebuild entries map to reclaim internal map memory
//
// Returns:
//   - walEntriesCleared: Number of WAL buffer entries that were cleared
//   - entriesCount: Current number of entries in the cache after compaction
//   - error: Any error during compaction
func (idx *HashIndexV3) CompactMemTableSafe(compactEntries bool) (int, int, error) {
	if idx.closed {
		return 0, 0, fmt.Errorf("index is closed")
	}

	// Use the MemTable's safe compaction method
	// threshold=0 means always compact entries if compactEntries=true
	oldWAL, entriesCount, err := idx.MemTable.CompactSafe(compactEntries, 0)
	if err != nil {
		return 0, 0, fmt.Errorf("memtable compaction failed: %w", err)
	}

	walEntriesCleared := len(oldWAL)

	// Log if significant compaction occurred
	if walEntriesCleared > 1000 {
		idx.logger.Debugw("MemTable compaction completed",
			"indexName", idx.config.IndexName,
			"walEntriesCleared", walEntriesCleared,
			"entriesCount", entriesCount,
			"compactedEntriesMap", compactEntries)
	}

	return walEntriesCleared, entriesCount, nil
}

// TrimMemTableWAL clears only the WAL buffer if it exceeds a threshold.
// This is a lighter-weight alternative to full compaction when only the
// walBuffer is causing memory pressure.
//
// Parameters:
//   - threshold: Only trim if buffer has more than this many entries
//
// Returns:
//   - trimmed: True if buffer was actually trimmed
//   - oldSize: Size of buffer before trim operation
func (idx *HashIndexV3) TrimMemTableWAL(threshold int) (bool, int) {
	if idx.closed {
		return false, 0
	}

	return idx.MemTable.TrimWALBuffer(threshold)
}

// NeedsEntriesCompaction checks if the entries map should be compacted
// based on time interval (60s) or idle timeout (30s).
//
// Thread-safe wrapper around MemTable.NeedsEntriesCompaction.
//
// Parameters:
//   - intervalSeconds: Force compaction after this many seconds since last compaction
//   - idleSeconds: Compact if no activity for this many seconds
//
// Returns:
//   - needsCompaction: True if compaction should be triggered
//   - reason: Description of why compaction is needed (for logging)
func (idx *HashIndexV3) NeedsEntriesCompaction(intervalSeconds, idleSeconds int) (bool, string) {
	if idx.closed {
		return false, ""
	}

	return idx.MemTable.NeedsEntriesCompaction(intervalSeconds, idleSeconds)
}

// Close closes the index and releases resources
func (idx *HashIndexV3) Close() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return nil
	}

	// Get current global sequence for header update
	currentSequence := atomic.LoadUint64(&idx.GlobalSequence)

	// Flush any remaining data with header update
	if err := idx.storage.FlushWithHeaderUpdate(currentSequence); err != nil {
		idx.logger.Warnw("Failed to flush storage on close", "error", err)
	} else {
		// Clear WAL buffer after successful flush to prevent unbounded memory growth
		idx.MemTable.ClearWALBuffer()
	}

	// Save metadata before closing
	if err := idx.SaveMetadata(); err != nil {
		idx.logger.Warnw("Failed to save metadata on close", "error", err)
	}

	// Close storage
	if err := idx.storage.Close(); err != nil {
		idx.logger.Warnw("Failed to close storage", "error", err)
	}

	idx.closed = true
	idx.isOpen = false

	idx.logger.Infow("Closed hash index",
		"indexName", idx.config.IndexName,
		"totalPuts", idx.stats.TotalPuts,
		"totalGets", idx.stats.TotalGets,
		"totalDeletes", idx.stats.TotalDeletes)

	return nil
}

// GetStats returns current index statistics
func (idx *HashIndexV3) GetStats() IndexStats {
	idx.statsMutex.RLock()
	defer idx.statsMutex.RUnlock()

	// Copy stats to avoid race conditions
	statsCopy := IndexStats{
		TotalPuts:          idx.stats.TotalPuts,
		TotalGets:          idx.stats.TotalGets,
		TotalDeletes:       idx.stats.TotalDeletes,
		TotalScans:         idx.stats.TotalScans,
		CacheHits:          idx.stats.CacheHits,
		CacheMisses:        idx.stats.CacheMisses,
		DiskReads:          idx.stats.DiskReads,
		TotalEntries:       idx.stats.TotalEntries,
		TotalFiles:         idx.stats.TotalFiles,
		TotalSizeBytes:     idx.stats.TotalSizeBytes,
		TombstoneCount:     idx.stats.TombstoneCount,
		CompactionCount:    idx.stats.CompactionCount,
		LastCompactionTime: idx.stats.LastCompactionTime,
		CreatedAt:          idx.stats.CreatedAt,
		LastModified:       idx.stats.LastModified,
	}
	return statsCopy
}

// GetMemTableStats returns MemTable statistics
func (idx *HashIndexV3) GetMemTableStats() MemTableStats {
	return idx.MemTable.GetStats()
}

// GetQueryOptimizationStats returns statistics for query optimization and cost estimation
// This is used by the join executor to decide between full scan vs index-assisted strategies
// Returns a copy of the statistics to avoid race conditions
func (idx *HashIndexV3) GetQueryOptimizationStats() QueryOptimizationStats {
	return idx.queryOptStats.GetStats()
}

// TODO: Sprint 5 - Batch Operations
// BatchPut performs multiple puts in a single operation
// More efficient than individual puts for bulk loading
// func (idx *HashIndexV3) BatchPut(entries map[string]string) error {
//     // 1. Create batch of entries with sequential sequence numbers
//     // 2. Append batch to storage (one I/O operation)
//     // 3. Update MemTable with batch
//     // 4. Return aggregated results
// }

// TODO: Sprint 5 - Range Queries
// GetRange retrieves all keys in a given range
// Requires sorted index files for efficient scanning
// func (idx *HashIndexV3) GetRange(startKey, endKey string) (map[string][]string, error) {
//     // 1. Scan MemTable for keys in range
//     // 2. Scan storage files for keys in range
//     // 3. Merge results (latest wins)
//     // 4. Return sorted results
// }

// TODO: Sprint 5 - Statistics Dashboard
// GetDetailedStats returns comprehensive statistics
// func (idx *HashIndexV3) GetDetailedStats() DetailedStats {
//     return DetailedStats{
//         IndexStats:     idx.GetStats(),
//         MemTableStats:  idx.GetMemTableStats(),
//         StorageStats:   idx.storage.GetStats(),
//         CompactionStats: idx.compactor.GetStats(),
//     }
// }

// Private helper functions

// validateConfig validates index configuration
func validateConfig(config IndexConfig) error {
	if config.IndexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}
	if config.BundleName == "" {
		return fmt.Errorf("bundle name cannot be empty")
	}
	if config.DataDir == "" {
		return fmt.Errorf("data directory cannot be empty")
	}
	return nil
}

// loadRecentEntries loads recent entries from disk into MemTable
// This provides fast startup for frequently accessed data
func (idx *HashIndexV3) loadRecentEntries() error {
	// TODO: Sprint 4 - Configurable preloading strategy
	// Options:
	// 1. Load N most recent entries
	// 2. Load entries from most recent file only
	// 3. Load entries accessed in last N days
	// 4. Lazy load on first access (current default)

	// For now, we do lazy loading (no preloading)
	// Entries are loaded on-demand during Get operations

	idx.logger.Debugw("Skipping MemTable preloading (using lazy loading)",
		"indexName", idx.config.IndexName)

	return nil
}

// updatePutStats updates statistics for Put operations
func (idx *HashIndexV3) updatePutStats() {
	idx.statsMutex.Lock()
	defer idx.statsMutex.Unlock()

	idx.stats.TotalPuts++
	idx.stats.LastModified = time.Now()

	if idx.metricsReporter != nil {
		idx.metricsReporter("HashIndexPutsTotal", 1)
	}
}

// updatePutStatsWithLatency updates statistics for Put operations with latency tracking
func (idx *HashIndexV3) updatePutStatsWithLatency(latencyMs float64) {
	idx.updatePutStats()
	if idx.metricsReporter != nil {
		idx.reportLatencyBucket("HashIndexPutLatency", latencyMs)
	}
}

// updateGetStats updates statistics for Get operations
func (idx *HashIndexV3) updateGetStats() {
	idx.statsMutex.Lock()
	defer idx.statsMutex.Unlock()

	idx.stats.TotalGets++

	if idx.metricsReporter != nil {
		idx.metricsReporter("HashIndexGetsTotal", 1)
	}
}

// updateGetStatsWithLatency updates statistics for Get operations with latency tracking
func (idx *HashIndexV3) updateGetStatsWithLatency(latencyMs float64) {
	idx.updateGetStats()
	if idx.metricsReporter != nil {
		idx.reportLatencyBucket("HashIndexGetLatency", latencyMs)
	}
}

// updateDeleteStats updates statistics for Delete operations
func (idx *HashIndexV3) updateDeleteStats() {
	idx.statsMutex.Lock()
	defer idx.statsMutex.Unlock()

	idx.stats.TotalDeletes++
	idx.stats.TombstoneCount++
	idx.stats.LastModified = time.Now()

	if idx.metricsReporter != nil {
		idx.metricsReporter("HashIndexDeletesTotal", 1)
	}
}

// updateDeleteStatsWithLatency updates statistics for Delete operations with latency tracking
func (idx *HashIndexV3) updateDeleteStatsWithLatency(latencyMs float64) {
	idx.updateDeleteStats()
	// Latency histograms not defined for Delete in GlobalServerMetrics
}

// updateCacheHit updates cache hit statistics
func (idx *HashIndexV3) updateCacheHit() {
	idx.statsMutex.Lock()
	defer idx.statsMutex.Unlock()

	idx.stats.CacheHits++

	if idx.metricsReporter != nil {
		idx.metricsReporter("HashIndexCacheHits", 1)
	}
}

// updateCacheMiss updates cache miss statistics
func (idx *HashIndexV3) updateCacheMiss() {
	idx.statsMutex.Lock()
	defer idx.statsMutex.Unlock()

	idx.stats.CacheMisses++

	if idx.metricsReporter != nil {
		idx.metricsReporter("HashIndexCacheMisses", 1)
	}
	idx.stats.DiskReads++
}

// reportLatencyBucket reports a latency measurement to the appropriate histogram bucket.
// prefix should be "HashIndexPutLatency" or "HashIndexGetLatency".
func (idx *HashIndexV3) reportLatencyBucket(prefix string, latencyMs float64) {
	var bucket string
	switch {
	case latencyMs < 1:
		bucket = "Lt1ms"
	case latencyMs < 10:
		bucket = "Lt10ms"
	case latencyMs < 100:
		bucket = "Lt100ms"
	case latencyMs < 1000:
		bucket = "Lt1s"
	default:
		bucket = "Gte1s"
	}
	idx.metricsReporter(prefix+bucket, 1)
}

// GetIndexFilePath returns the path for index files
// This follows the same pattern as hashindexV2 for compatibility
func (config *IndexConfig) GetIndexFilePath() string {
	return filepath.Join(config.DataDir, fmt.Sprintf("%s_%s.hidx",
		config.BundleName, config.FieldName))
}

// TODO: Sprint 4 - Background Compaction
// triggerCompaction checks if compaction is needed and runs it
// Called periodically from Put() to maintain LSM efficiency
func (idx *HashIndexV3) triggerCompaction() {
	// Skip if compaction disabled or compactor not set
	if idx.compactor == nil {
		return
	}

	// Get all entry files
	files, err := idx.storage.GetEntryFiles()
	if err != nil {
		idx.logger.Errorw("Failed to get entry files for compaction check",
			"error", err)
		return
	}

	// Check if compaction needed (compactor examines file count/sizes)
	if !idx.compactor.ShouldCompact(files) {
		return
	}

	idx.logger.Infow("Triggering compaction",
		"indexName", idx.config.IndexName,
		"bundleName", idx.config.BundleName,
		"fileCount", len(files))

	// Call the compactor's CompactHashIndexFiles method
	compactedFile, err := idx.compactor.CompactHashIndexFiles(
		idx.config.BundleName,
		idx.config.IndexName,
		files,
	)
	if err != nil {
		idx.logger.Errorw("Compaction failed",
			"indexName", idx.config.IndexName,
			"bundleName", idx.config.BundleName,
			"error", err)
		return
	}

	// Replace old files with compacted file
	err = idx.storage.ReplaceFiles(files, compactedFile)
	if err != nil {
		idx.logger.Errorw("Failed to replace files after compaction",
			"indexName", idx.config.IndexName,
			"error", err,
			"compactedFile", compactedFile)
		return
	}

	// Update statistics
	idx.statsMutex.Lock()
	idx.stats.CompactionCount++
	idx.stats.LastCompactionTime = time.Now()
	idx.statsMutex.Unlock()

	idx.logger.Infow("Compaction completed successfully",
		"indexName", idx.config.IndexName,
		"compactedFile", compactedFile,
		"oldFileCount", len(files))
}

// SetCompactor injects a CompactionManager implementation
// This allows external configuration without creating import cycles
// Must be called after NewHashIndexV3() and before compaction is triggered
func (idx *HashIndexV3) SetCompactor(cm CompactionManager) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
	idx.compactor = cm
	idx.logger.Infow("Compactor configured",
		"indexName", idx.config.IndexName)
}

// GetStorage returns the underlying EntryStorage for testing purposes
// This allows tests to inspect file state and trigger operations
func (idx *HashIndexV3) GetStorage() *EntryStorage {
	return idx.storage
}

// ============================================================================
// RCU (Read-Copy-Update) Support Methods
// These methods enable lock-free concurrent writes using atomic pointer swaps
// ============================================================================

// UpdatePageLocation atomically updates the page location for a document
// This is the key primitive for RCU-style updates:
// 1. New document version is appended to storage (no lock needed)
// 2. This method atomically swaps the index pointer to the new location
// 3. Old location remains valid until grace period passes
//
// Parameters:
//   - keyValue: The indexed value (e.g., DocumentID)
//   - documentID: The document UUID
//   - newPageID: The new page location where the updated document resides
//   - commitSequence: The commit sequence for this update
//
// Returns error if key not found or operation fails
// Thread-safe: Uses internal locking to ensure atomic update
func (idx *HashIndexV3) UpdatePageLocation(keyValue, documentID string, newPageID uint32, commitSequence uint64) error {
	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	if keyValue == "" {
		return fmt.Errorf("key value cannot be empty")
	}

	// Get next sequence number for this update
	sequence := atomic.AddUint64(&idx.GlobalSequence, 1)

	// Create new entry with updated location
	// VersionSequence is incremented to indicate this is a newer version
	entry := NewHashIndexEntry(keyValue, documentID, newPageID, sequence, commitSequence, 0)

	// Populate bucket number
	bucketNum, err := ComputeBucketNum(entry.HashValue, idx.config.NumBuckets)
	if err != nil {
		return fmt.Errorf("bucket calculation failed: %w", err)
	}
	entry.BucketNum = bucketNum

	// ATOMIC UPDATE PATH:
	// 1. Append new entry to disk (for durability)
	// 2. Update MemTable atomically
	// The key insight: readers see either old or new location, never partial state

	// Step 1: Append to disk storage
	err = idx.storage.AppendEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to append location update to storage: %w", err)
	}

	// Step 2: Atomic update in MemTable
	// MemTable.Put handles sequence ordering - newer entries win
	err = idx.MemTable.Put(entry)
	if err != nil {
		// Entry is on disk but MemTable rejected (shouldn't happen with higher sequence)
		idx.logger.Warnw("MemTable rejected location update (entry is on disk)",
			"key", keyValue,
			"newPageID", newPageID,
			"error", err)
	}

	idx.logger.Debugw("Updated page location atomically",
		"key", keyValue,
		"documentID", documentID,
		"newPageID", newPageID,
		"sequence", sequence)

	return nil
}

// GetCurrentPageID returns the current page location for a key
// Used to find the old document location before RCU update
//
// Parameters:
//   - keyValue: The indexed value to look up
//
// Returns pageID and found boolean
func (idx *HashIndexV3) GetCurrentPageID(keyValue string) (uint32, bool) {
	if idx.closed {
		return 0, false
	}

	// Check MemTable first (most recent)
	entry, found := idx.MemTable.Get(keyValue)
	if found && !entry.Deleted {
		return entry.PageID, true
	}

	// Fall back to disk scan if not in MemTable
	docIDs, pageIDs, err := idx.Get(keyValue)
	if err != nil || len(pageIDs) == 0 {
		return 0, false
	}

	// Return first result (should be only one for unique keys like DocumentID)
	_ = docIDs // Suppress unused warning
	return pageIDs[0], true
}
