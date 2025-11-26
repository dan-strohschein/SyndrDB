package hashindexV2

/*
⚠️  DEPRECATED - NON-LSM HASH INDEX IMPLEMENTATION ⚠️

This package (hashindexV2) is DEPRECATED and replaced by hashindexV3.

REPLACED BY: src/internal/domain/index/hashindexV3 (LSM-style hash index)

REASON FOR DEPRECATION:
- hashindexV2 uses traditional bucket-based linear hashing
- hashindexV3 uses modern LSM (Log-Structured Merge) architecture
- LSM provides better write performance and simpler recovery
- LSM architecture is more suitable for append-only operations

DO NOT USE THIS PACKAGE FOR NEW CODE.
All references to hashindexV2 have been commented out in the codebase.

TODO: Remove this package entirely after migration is complete (Phase 12+)

═══════════════════════════════════════════════════════════════════

HASH INDEX SYSTEM - MAIN API (LEGACY)

This file provides the primary interface for hash index operations in SyndrDB.
It implements a PostgreSQL-style linear hashing algorithm with the following features:

ALGORITHM OVERVIEW:
- Linear Hashing: Dynamic hash table that grows incrementally
- Bucket splitting occurs when load factor exceeds threshold
- Uses Bob Jenkins hash function for key distribution
- File-based storage with human-readable ASCII format (when debug=true)
- Page-based architecture with metadata, bucket, and overflow pages

ARCHITECTURE:
- Page 0: Metadata page containing index parameters
- Pages 1-N: Bucket pages containing hash items
- Pages N+1+: Overflow pages for bucket overflow
- Each page is 8KB with structured header and item storage

FEATURES:
- Automatic bucket splitting based on load factor
- Overflow page management for hash collisions
- Thread-safe operations with read/write locks
- LRU page cache for performance
- ASCII file format for debugging

This implementation follows the Single Responsibility Principle where this file
provides the public API while delegating specific operations to specialized files.
*/

import (
	"fmt"
	"os"

	"sync"
	"time"

	"go.uber.org/zap"
)

// HashIndex represents the main hash index structure
type HashIndex struct {
	FilePath string

	//PageManager *PageManager
	Storage *HashIndexStorage
	//Logger      *zap.SugaredLogger
	mutex       sync.RWMutex
	IsDebugMode bool
	bundleName  string
	//indexPath     string
	fileManager   *FileManager
	pageManager   *PageManager
	metadata      *HashIndexMetadata
	bucketManager *BucketManager
	logger        *zap.SugaredLogger
	isOpen        bool
}

// HashIndexMetadata contains index configuration and statistics
type HashIndexMetadata struct {
	Version    uint32 // File format version
	PageSize   uint32 // Size of each page in bytes
	MaxBucket  uint32 // Highest bucket number currently in use
	HighMask   uint32 // Mask for hash value (power of 2 - 1)
	LowMask    uint32 // Mask for rehashing during splits
	FillFactor uint32 // Target fill percentage (75 = 75%)

	NumOverflows uint32 // Number of overflow pages
	IndexField   string // Field name being indexed
	IsUnique     bool   // Whether index enforces uniqueness

	Created      time.Time // Creation timestamp
	LastModified time.Time // Last modification timestamp

	CreatedAt time.Time // When the index was created

	BucketCount  uint32  // Current number of buckets
	TotalRecords uint64  // Total number of records in the index
	LoadFactor   float64 // Target load factor (when to split)

	SplitPointer uint32 // Current split pointer for linear hashing
	HashSeed     uint32 // Seed for the hash function
	NextPageNum  uint32 // Next available page number for allocation
	DebugMode    bool   // Whether to write human-readable format

	BitmapPages   uint32   // Number of bitmap pages (for future use)
	OverflowPages uint32   // Number of overflow pages allocated
	DocumentCount uint64   // Total number of documents indexed
	FreePageList  []uint32 // List of free pages available for reuse
}

// CreateHashIndex creates a new hash index for the specified bundle and field
// Parameters:
//   - bundleName: Name of the bundle this index belongs to
//   - fieldName: Name of the field being indexed (typically "DocumentID")
//   - isUnique: Whether the index should enforce uniqueness
//   - dataDir: Directory where index files are stored
//   - debugMode: Whether to use human-readable ASCII format
//   - logger: Logger for debug/error messages
//
// Returns:
//   - *HashIndex: The created hash index instance
//   - error: Any error that occurred during creation
func CreateHashIndex(config *IndexConfig, logger *zap.SugaredLogger) (*HashIndex, error) {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	logger.Debugf("Creating hash index for bundle '%s' field '%s'", config.BundleName, config.FieldName)

	// Create file manager
	indexFilePath := config.GetIndexFilePath()
	fileManager, err := NewFileManager(indexFilePath, config.PageSize, config.DebugMode, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}

	// Create page manager
	pageManager, err := NewPageManager(config.PageSize, config.CacheSize, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}

	// Create storage
	storage := NewHashIndexStorage(fileManager, logger)

	// Create metadata
	metadata := &HashIndexMetadata{
		Version:       1,
		PageSize:      config.PageSize,
		MaxBucket:     config.InitialSize - 1,
		HighMask:      config.InitialSize - 1,
		LowMask:       (config.InitialSize >> 1) - 1,
		FillFactor:    uint32(config.LoadFactor * 100),
		NumOverflows:  0,
		IndexField:    config.FieldName,
		IsUnique:      config.IsUnique,
		Created:       time.Now(),
		LastModified:  time.Now(),
		CreatedAt:     time.Now(),
		BucketCount:   config.InitialSize,
		TotalRecords:  0,
		LoadFactor:    config.LoadFactor,
		SplitPointer:  0,
		HashSeed:      GenerateHashSeed(),
		NextPageNum:   config.InitialSize + 1, // After metadata and initial buckets
		DebugMode:     config.DebugMode,
		BitmapPages:   0,
		OverflowPages: 0,
	}

	// Create bucket manager
	bucketManager, err := NewBucketManager(storage, pageManager, fileManager, metadata, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create bucket manager: %w", err)
	}

	// Create hash index instance
	hashIndex := &HashIndex{
		FilePath:      indexFilePath,
		Storage:       storage,
		mutex:         sync.RWMutex{},
		IsDebugMode:   config.DebugMode,
		bundleName:    config.BundleName,
		fileManager:   fileManager,
		pageManager:   pageManager,
		metadata:      metadata,
		bucketManager: bucketManager,
		logger:        logger,
		isOpen:        true,
	}

	// Set up page manager flush function to prevent data loss during eviction
	pageManager.SetFlushFunction(func(pageNum uint32, pageData interface{}) error {
		return fileManager.WritePage(pageNum, pageData)
	})

	// Initialize the index
	if err := hashIndex.initializeIndex(); err != nil {
		return nil, fmt.Errorf("failed to initialize index: %w", err)
	}

	logger.Infof("Successfully created hash index '%s' for bundle '%s'", config.FieldName, config.BundleName)
	return hashIndex, nil
}

// OpenHashIndex opens an existing hash index from file
// Parameters:
//   - filePath: Path to the index file
//   - debugMode: Whether to use human-readable ASCII format
//   - logger: Logger for debug/error messages
//
// Returns:
//   - *HashIndex: The opened hash index instance
//   - error: Any error that occurred during opening
func OpenHashIndex(filePath string, debugMode bool, logger *zap.SugaredLogger) (*HashIndex, error) {
	logger.Infof("Opening hash index: %s", filePath)

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("hash index file does not exist: %s", filePath)
	}

	// Create file manager with proper initialization
	// Note: We'll read the page size from the existing file metadata
	fileManager, err := NewFileManager(filePath, 0, debugMode, logger) // 0 means read from file
	if err != nil {
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}

	// Validate that file manager was created successfully
	if fileManager == nil {
		return nil, fmt.Errorf("file manager creation returned nil without error")
	}

	// Initialize storage
	storage := NewHashIndexStorage(fileManager, logger)
	if storage == nil {
		// Clean up file manager if storage creation fails
		if closeErr := fileManager.Close(); closeErr != nil {
			logger.Warnf("Failed to close file manager during cleanup: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}

	// Load metadata from file
	metadata, err := storage.LoadMetadata()
	if err != nil {
		// Clean up resources if metadata loading fails
		if closeErr := fileManager.Close(); closeErr != nil {
			logger.Warnf("Failed to close file manager during cleanup: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	// Validate metadata
	if metadata == nil {
		// Clean up resources if metadata is nil
		if closeErr := fileManager.Close(); closeErr != nil {
			logger.Warnf("Failed to close file manager during cleanup: %v", closeErr)
		}
		return nil, fmt.Errorf("loaded metadata is nil")
	}

	// Initialize page manager
	pageManager, err := NewPageManager(metadata.PageSize, 2000, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}

	// Create bucket manager with proper initialization
	bucketManager, err := NewBucketManager(storage, pageManager, fileManager, metadata, logger)
	if err != nil {
		// Clean up resources if bucket manager creation fails
		if closeErr := fileManager.Close(); closeErr != nil {
			logger.Warnf("Failed to close file manager during cleanup: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to create bucket manager: %w", err)
	}

	// Validate bucket manager
	if bucketManager == nil {
		// Clean up resources if bucket manager is nil
		if closeErr := fileManager.Close(); closeErr != nil {
			logger.Warnf("Failed to close file manager during cleanup: %v", closeErr)
		}
		return nil, fmt.Errorf("bucket manager creation returned nil without error")
	}

	// Create the hash index instance
	hashIndex := &HashIndex{
		FilePath:      filePath,
		Storage:       storage,
		metadata:      metadata,
		pageManager:   pageManager,
		bucketManager: bucketManager,
		fileManager:   fileManager,
		isOpen:        true,
		logger:        logger,
	}

	// CRITICAL FIX: Ensure metadata masks are properly initialized
	// This fixes the performance regression where HighMask/LowMask were 0
	if metadata.HighMask == 0 || metadata.MaxBucket == 0 {
		logger.Infof("Initializing hash index metadata masks: BucketCount=%d", metadata.BucketCount)
		metadata.HighMask = metadata.BucketCount - 1
		metadata.LowMask = (metadata.BucketCount >> 1) - 1
		metadata.MaxBucket = metadata.BucketCount - 1
		logger.Infof("Initialized masks: HighMask=%d, LowMask=%d, MaxBucket=%d",
			metadata.HighMask, metadata.LowMask, metadata.MaxBucket)
	}

	// Set up page manager flush function to prevent data loss during eviction
	pageManager.SetFlushFunction(func(pageNum uint32, pageData interface{}) error {
		return fileManager.WritePage(pageNum, pageData)
	})

	logger.Infof("Successfully opened hash index with %d buckets", hashIndex.metadata.BucketCount)
	return hashIndex, nil
}

// Insert adds a new key-value pair to the hash index
// Parameters:
//   - key: The key to insert (typically a DocumentID)
//   - value: The value associated with the key
//
// Returns:
//   - error: Any error that occurred during insertion
func (hi *HashIndex) Insert(key, value string) error {
	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	hi.logger.Debugf("Inserting key: %s, value: %s", key, value)

	// Check for uniqueness if required
	if hi.metadata.IsUnique {
		exists, err := hi.keyExists(key)
		if err != nil {
			return fmt.Errorf("failed to check key existence: %w", err)
		}
		if exists {
			return fmt.Errorf("duplicate key in unique index: %s", key)
		}
	}

	// Compute hash and bucket
	hashValue := jenkinsHash(key, hi.metadata.HashSeed)
	bucketNum := hi.computeBucket(hashValue)

	hi.logger.Debugf("Hash value: %d, Bucket: %d", hashValue, bucketNum)

	// Create hash item
	item := &IndexRecord{
		DocumentID: key,
		HashValue:  hashValue,
		Timestamp:  time.Now(),
	}

	// Insert into bucket
	err := hi.insertIntoBucket(bucketNum, item)
	if err != nil {
		return fmt.Errorf("failed to insert into bucket: %w", err)
	}

	// Update metadata
	hi.metadata.TotalRecords++
	hi.metadata.LastModified = time.Now()

	// PERFORMANCE FIX: Only check for split periodically, not on every insert
	// Check split condition every 100 documents to avoid O(n) cost per insert
	if hi.metadata.TotalRecords%100 == 0 {
		if shouldSplit, err := hi.shouldSplitFast(); err != nil {
			hi.logger.Errorf("Failed to check if bucket should split: %v", err)
		} else if shouldSplit {
			hi.logger.Infof("Load factor exceeded, splitting bucket...")
			if err := hi.splitBucket(); err != nil {
				hi.logger.Errorf("Failed to split bucket: %v", err)
			}
		}
	}

	// PERFORMANCE FIX: Remove immediate metadata saving - let batch operations handle this
	// return hi.Storage.SaveMetadata(hi.metadata)
	return nil
}

func (hi *HashIndex) InsertDocument(documentID string) error {
	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	// Calculate hash value
	// keyBytes := []byte(documentID)
	// hashValue := calculateHash(keyBytes, hi.metadata.HashSeed)

	// // Determine target bucket
	// bucketNum := hi.calculateBucket(hashValue)

	hashValue := jenkinsHash(documentID, hi.metadata.HashSeed)
	bucketNum := hi.computeBucket(hashValue)

	// Create index record
	record := &IndexRecord{
		DocumentID: documentID,
		HashValue:  hashValue,
		Timestamp:  time.Now(),
	}

	// Insert into bucket
	if err := hi.insertIntoBucket(bucketNum, record); err != nil {
		return fmt.Errorf("failed to insert document %s: %w", documentID, err)
	}

	// Update metadata
	hi.metadata.TotalRecords++

	// PERFORMANCE FIX: Only check for split periodically, not on every insert
	// Check split condition every 250 documents to reduce overhead during high-volume inserts
	if hi.metadata.TotalRecords%250 == 0 {
		if shouldSplit, err := hi.shouldSplitFast(); err != nil {
			hi.logger.Errorf("Failed to check if bucket should split: %v", err)
		} else if shouldSplit {
			hi.logger.Infof("Load factor exceeded, splitting bucket...")
			if err := hi.splitBucket(); err != nil {
				hi.logger.Errorf("Failed to split bucket: %v", err)
			}
		}
	}

	return nil
}

// FlushToDisk forces all pending changes to be written to disk
// This should be called by batch operations to persist changes efficiently
func (hi *HashIndex) FlushToDisk() error {
	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	// Save metadata to disk
	if err := hi.Storage.SaveMetadata(hi.metadata); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// Force dirty pages to be written (implementation may vary)
	// Note: Specific page flushing depends on page manager implementation
	hi.logger.Debugf("Flushed hash index metadata and pages to disk")

	return nil
}

// FlushAllDirtyPages writes all dirty (modified) pages from the cache to disk
// This ensures durability of index changes without closing the index
// Should be called after DELETE operations or other modifications that require immediate persistence
func (hi *HashIndex) FlushAllDirtyPages() error {
	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	if hi.pageManager == nil {
		return fmt.Errorf("page manager not initialized")
	}

	if hi.fileManager == nil {
		return fmt.Errorf("file manager not initialized")
	}

	hi.logger.Debugf("Flushing all dirty pages for hash index")

	// Flush all dirty pages using the file manager's WritePage function
	err := hi.pageManager.FlushDirtyPages(func(pageNum uint32, pageData interface{}) error {
		return hi.fileManager.WritePage(pageNum, pageData)
	})

	if err != nil {
		return fmt.Errorf("failed to flush dirty pages: %w", err)
	}

	hi.logger.Debugf("Successfully flushed all dirty pages")
	return nil
}

// PersistMetadata saves the current index metadata to disk
// This should be called after operations that modify metadata (inserts, deletes, splits)
// Ensures metadata changes are durable without requiring a full index flush
func (hi *HashIndex) PersistMetadata() error {
	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	if hi.Storage == nil {
		return fmt.Errorf("storage not initialized")
	}

	hi.logger.Debugf("Persisting hash index metadata")

	// Update last modified timestamp
	hi.metadata.LastModified = time.Now()

	// Save metadata to disk
	if err := hi.Storage.SaveMetadata(hi.metadata); err != nil {
		return fmt.Errorf("failed to persist metadata: %w", err)
	}

	hi.logger.Debugf("Successfully persisted metadata: %d records, %d buckets",
		hi.metadata.TotalRecords, hi.metadata.BucketCount)
	return nil
}

func (hi *HashIndex) DeleteDocument(documentID string) (bool, error) {
	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	// Calculate hash value
	//keyBytes := []byte(documentID)
	hashValue := jenkinsHash(documentID, hi.metadata.HashSeed) //calculateHash(keyBytes, hi.metadata.HashSeed)

	// Determine target bucket
	bucketNum := hi.computeBucket(hashValue)

	// Delete from bucket
	if err := hi.deleteFromBucket(bucketNum, documentID); err != nil {
		return false, fmt.Errorf("failed to delete document %s: %w", documentID, err)
	}

	// Update metadata
	//hi.metadata.DecrementRecordCount()
	hi.metadata.TotalRecords--

	return true, nil
}

// Search finds all values associated with the given key
// Parameters:
//   - key: The key to search for
//
// Returns:
//   - []string: List of values associated with the key
//   - error: Any error that occurred during search
func (hi *HashIndex) Search(key string) ([]string, error) {
	hi.mutex.RLock()
	defer hi.mutex.RUnlock()

	// Calculate hash value
	//keyBytes := []byte(key)
	//hashValue := calculateHash(keyBytes, hi.metadata.HashSeed)
	hashValue := jenkinsHash(key, hi.metadata.HashSeed)
	bucketNum := hi.computeBucket(hashValue)
	// Determine target bucket
	//bucketNum := hi.calculateBucket(hashValue)

	// Search in bucket
	return hi.searchInBucket(bucketNum, key)

}

// Close closes the hash index and flushes any pending changes
// Returns:
//   - error: Any error that occurred during closing
func (hi *HashIndex) Close() error {
	hi.mutex.Lock()
	defer hi.mutex.Unlock()

	hi.logger.Debugf("Closing hash index: %s", hi.FilePath)

	// Save final metadata
	err := hi.Storage.SaveMetadata(hi.metadata)
	if err != nil {
		hi.logger.Errorf("Failed to save metadata during close: %v", err)
	}

	// Close storage
	return hi.Storage.fileManager.file.Close()
}

// GetStats returns current index statistics
// Returns:
//   - *HashIndexStats: Current statistics
func (hi *HashIndex) GetStats() *HashIndexStats {
	hi.mutex.RLock()
	defer hi.mutex.RUnlock()

	loadFactor := float64(hi.metadata.TotalRecords) / float64(hi.metadata.BucketCount)

	return &HashIndexStats{
		TotalRecords: hi.metadata.TotalRecords,
		BucketCount:  hi.metadata.BucketCount,
		NumOverflows: hi.metadata.NumOverflows,
		LoadFactor:   loadFactor,
		FillFactor:   hi.metadata.FillFactor,
		IndexField:   hi.metadata.IndexField,
		IsUnique:     hi.metadata.IsUnique,
	}
}

// HashIndexStats contains index performance statistics
type HashIndexStats struct {
	TotalRecords uint64
	BucketCount  uint32
	NumOverflows uint32
	LoadFactor   float64
	FillFactor   uint32
	IndexField   string
	IsUnique     bool
}
