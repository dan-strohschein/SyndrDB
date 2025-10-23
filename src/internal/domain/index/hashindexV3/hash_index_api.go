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
	"time"

	"go.uber.org/zap"
)

// HashIndexV3 is the main LSM-style hash index coordinator
// It combines MemTable, EntryStorage, and CompactionManager
type HashIndexV3 struct {
	// Configuration
	config IndexConfig

	// Core components
	memTable  *HashMemTable     // In-memory cache layer
	storage   *EntryStorage     // Disk persistence layer
	compactor CompactionManager // File optimization (future integration)

	// Sequence management for temporal ordering
	// Uses atomic operations for thread-safe increments
	globalSequence uint64

	// Concurrency control
	// Note: Individual components have their own locks
	// This mutex is for coordinating between components
	mutex sync.RWMutex

	// State management
	isOpen bool
	closed bool

	// Statistics
	stats      IndexStats
	statsMutex sync.RWMutex // Separate mutex for stats

	// Logging
	logger *zap.SugaredLogger
}

// IndexConfig holds configuration for the hash index
type IndexConfig struct {
	// Identity
	IndexName    string // Name of the index (e.g., "DocumentID_idx")
	BundleName   string // Name of the bundle
	DatabaseName string // Name of the database
	FieldName    string // Field being indexed

	// Storage configuration
	DataDir         string // Directory for index files
	MaxFileSize     int64  // Maximum entry file size before rotation
	WriteBufferSize int    // Write buffer size for EntryStorage

	// MemTable configuration
	MemTableMaxSize int // Maximum entries in MemTable before flush recommended

	// Compaction configuration
	CompactionEnabled  bool // Enable automatic compaction
	CompactionMaxFiles int  // Trigger compaction when file count exceeds this

	// TODO: Future compaction configuration
	// CompactionStrategy: Pluggable strategy (file count, size, tombstone ratio)
	// CompactionSchedule: Time-based compaction schedule
	// CompactionConcurrency: Number of parallel compaction threads

	// Logging
	Logger *zap.SugaredLogger
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

	// Lifecycle
	CreatedAt    time.Time
	LastModified time.Time
}

// CompactionManager interface for future integration
// TODO: Sprint 4 - Integrate with actual compactor package
// This will be replaced with real compaction manager from /src/internal/domain/compactor
type CompactionManager interface {
	ShouldCompact(files []string) bool
	CompactFiles(files []string) (string, error)
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
	if config.CompactionMaxFiles == 0 {
		config.CompactionMaxFiles = 10
	}
	if config.Logger == nil {
		logger, _ := zap.NewProduction()
		config.Logger = logger.Sugar()
	}

	// Create MemTable
	memTable := NewHashMemTable(config.MemTableMaxSize)

	// Create EntryStorage
	storageConfig := EntryStorageConfig{
		IndexName:       config.IndexName,
		BundleName:      config.BundleName,
		DataDir:         config.DataDir,
		MaxFileSize:     config.MaxFileSize,
		WriteBufferSize: config.WriteBufferSize,
		Logger:          config.Logger,
	}

	storage, err := NewEntryStorage(storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create entry storage: %w", err)
	}

	// Create index
	idx := &HashIndexV3{
		config:         config,
		memTable:       memTable,
		storage:        storage,
		globalSequence: 0,
		isOpen:         true,
		closed:         false,
		stats: IndexStats{
			CreatedAt:    time.Now(),
			LastModified: time.Now(),
		},
		logger: config.Logger,
	}

	// TODO: Sprint 4 - Initialize CompactionManager
	// idx.compactor = compactor.NewCompactionManager(compactorConfig)

	config.Logger.Infow("Created new LSM-style hash index",
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
		"memTableSize", idx.memTable.GetStats().Size)

	return idx, nil
}

// Put inserts or updates a key-value mapping in the index with document location
// Parameters:
//   - keyValue: The value being indexed
//   - documentID: The document UUID
//   - pageID: The physical page number where the document resides
//
// Returns error if operation fails
func (idx *HashIndexV3) Put(keyValue, documentID string, pageID uint32) error {
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
	sequence := atomic.AddUint64(&idx.globalSequence, 1)

	// Create entry with page location
	entry := NewHashIndexEntry(keyValue, documentID, pageID, sequence)

	// WRITE PATH (LSM-style):
	// 1. Write to disk first (durability)
	// 2. Then update MemTable (performance)
	// This ensures we never lose writes even if we crash

	// Step 1: Append to disk storage
	err := idx.storage.AppendEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to append entry to storage: %w", err)
	}

	// Step 2: Update MemTable
	err = idx.memTable.Put(entry)
	if err != nil {
		// Entry is on disk but not in cache - this is OK
		// Future reads will find it via disk scan
		idx.logger.Warnw("Failed to update MemTable (entry is on disk)",
			"key", keyValue,
			"error", err)
	}

	// Update statistics
	idx.updatePutStats()

	// Check if compaction is needed
	// TODO: Sprint 4 - Integrate with CompactionManager
	// if idx.config.CompactionEnabled && idx.compactor != nil {
	//     files := idx.storage.GetAllFiles()
	//     if idx.compactor.ShouldCompact(files) {
	//         go idx.triggerCompaction() // Background compaction
	//     }
	// }

	return nil
}

// Get retrieves document IDs and page locations for a given key value
// Parameters:
//   - keyValue: The value to search for
//
// Returns:
//   - documentIDs: List of document IDs matching the key
//   - pageIDs: Corresponding page locations for each document (parallel array)
//   - error: Any error that occurred
func (idx *HashIndexV3) Get(keyValue string) ([]string, []uint32, error) {
	if idx.closed {
		return nil, nil, fmt.Errorf("index is closed")
	}

	if keyValue == "" {
		return nil, nil, fmt.Errorf("key value cannot be empty")
	}

	// READ PATH (LSM-style):
	// 1. Check MemTable first (O(1) if cached)
	// 2. If not found, scan disk storage backward
	// 3. Cache result in MemTable for future reads

	// Step 1: Check MemTable
	entry, found := idx.memTable.Get(keyValue)
	if found {
		idx.updateCacheHit()
		idx.updateGetStats()

		// Check if this is a tombstone (deleted)
		if entry.Deleted {
			return []string{}, []uint32{}, nil
		}

		return []string{entry.DocumentID}, []uint32{entry.PageID}, nil
	}

	// Step 2: MemTable miss - scan disk storage
	idx.updateCacheMiss()

	var latestEntry *HashIndexEntry
	err := idx.storage.ScanBackward(func(entry *HashIndexEntry) bool {
		if entry.KeyValue == keyValue {
			latestEntry = entry
			return false // Found it, stop scanning
		}
		return true // Keep scanning
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to scan storage: %w", err)
	}

	if latestEntry == nil {
		// Not found anywhere
		return []string{}, []uint32{}, nil
	}

	// Step 3: Cache in MemTable for future reads
	err = idx.memTable.Put(latestEntry)
	if err != nil {
		// Just log warning - we still have the result
		idx.logger.Warnw("Failed to cache entry in MemTable",
			"key", keyValue,
			"error", err)
	}

	// Check if this is a tombstone
	if latestEntry.Deleted {
		return []string{}, []uint32{}, nil
	}

	idx.updateGetStats()

	return []string{latestEntry.DocumentID}, []uint32{latestEntry.PageID}, nil
}

// Delete marks a key as deleted by appending a tombstone
// The actual entry is not removed until compaction runs
// Parameters:
//   - keyValue: The value to delete
//
// Returns true if key was found and deleted, false if not found
func (idx *HashIndexV3) Delete(keyValue string) (bool, error) {
	if idx.closed {
		return false, fmt.Errorf("index is closed")
	}

	if keyValue == "" {
		return false, fmt.Errorf("key value cannot be empty")
	}

	// Check if key exists
	results, _, err := idx.Get(keyValue)
	if err != nil {
		return false, err
	}

	if len(results) == 0 {
		// Key doesn't exist
		return false, nil
	}

	// Get next sequence number
	sequence := atomic.AddUint64(&idx.globalSequence, 1)

	// Create tombstone entry
	tombstone := NewTombstoneEntry(keyValue, sequence)

	// DELETE PATH (LSM-style):
	// 1. Append tombstone to disk (durability)
	// 2. Update MemTable with tombstone (future reads will see deletion)
	// 3. Tombstone will be removed during compaction

	// Step 1: Append tombstone to disk
	err = idx.storage.AppendEntry(tombstone)
	if err != nil {
		return false, fmt.Errorf("failed to append tombstone: %w", err)
	}

	// Step 2: Update MemTable
	err = idx.memTable.Put(tombstone)
	if err != nil {
		idx.logger.Warnw("Failed to update MemTable with tombstone",
			"key", keyValue,
			"error", err)
	}

	idx.updateDeleteStats()

	return true, nil
}

// Search is an alias for Get to maintain compatibility with hashindexV2
// Returns document IDs only (discards page IDs) for backward compatibility
func (idx *HashIndexV3) Search(keyValue string) ([]string, error) {
	docIDs, _, err := idx.Get(keyValue)
	return docIDs, err
}

// Flush ensures all buffered data is written to disk
func (idx *HashIndexV3) Flush() error {
	if idx.closed {
		return fmt.Errorf("index is closed")
	}

	return idx.storage.Flush()
}

// Close closes the index and releases resources
func (idx *HashIndexV3) Close() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return nil
	}

	// Flush any remaining data
	if err := idx.storage.Flush(); err != nil {
		idx.logger.Warnw("Failed to flush storage on close", "error", err)
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
	return idx.memTable.GetStats()
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
}

// updateGetStats updates statistics for Get operations
func (idx *HashIndexV3) updateGetStats() {
	idx.statsMutex.Lock()
	defer idx.statsMutex.Unlock()

	idx.stats.TotalGets++
}

// updateDeleteStats updates statistics for Delete operations
func (idx *HashIndexV3) updateDeleteStats() {
	idx.statsMutex.Lock()
	defer idx.statsMutex.Unlock()

	idx.stats.TotalDeletes++
	idx.stats.TombstoneCount++
	idx.stats.LastModified = time.Now()
}

// updateCacheHit updates cache hit statistics
func (idx *HashIndexV3) updateCacheHit() {
	idx.statsMutex.Lock()
	defer idx.statsMutex.Unlock()

	idx.stats.CacheHits++
}

// updateCacheMiss updates cache miss statistics
func (idx *HashIndexV3) updateCacheMiss() {
	idx.statsMutex.Lock()
	defer idx.statsMutex.Unlock()

	idx.stats.CacheMisses++
	idx.stats.DiskReads++
}

// GetIndexFilePath returns the path for index files
// This follows the same pattern as hashindexV2 for compatibility
func (config *IndexConfig) GetIndexFilePath() string {
	return filepath.Join(config.DataDir, fmt.Sprintf("%s_%s.hidx",
		config.BundleName, config.FieldName))
}

// TODO: Sprint 4 - Background Compaction
// triggerCompaction runs compaction in background
// func (idx *HashIndexV3) triggerCompaction() {
//     idx.logger.Infow("Starting background compaction",
//         "indexName", idx.config.IndexName)
//
//     files := idx.storage.GetAllFiles()
//     compactedFile, err := idx.compactor.CompactFiles(files)
//     if err != nil {
//         idx.logger.Errorw("Compaction failed",
//             "indexName", idx.config.IndexName,
//             "error", err)
//         return
//     }
//
//     // Atomically replace old files with compacted file
//     err = idx.storage.ReplaceFiles(files, compactedFile)
//     if err != nil {
//         idx.logger.Errorw("Failed to replace files after compaction",
//             "error", err)
//         return
//     }
//
//     idx.stats.mutex.Lock()
//     idx.stats.CompactionCount++
//     idx.stats.LastCompactionTime = time.Now()
//     idx.stats.mutex.Unlock()
//
//     idx.logger.Infow("Compaction completed successfully",
//         "indexName", idx.config.IndexName,
//         "compactedFile", compactedFile)
// }
