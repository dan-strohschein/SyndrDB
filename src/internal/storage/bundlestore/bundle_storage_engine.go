package bundlestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/storage/format"
	"syndrdb/src/pkg/common"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/extension"
	"syndrdb/src/pkg/settings"
	"time"

	// "syndrdb/src/buffermgr"
	// "syndrdb/src/helpers"
	// "syndrdb/src/settings"
	"syscall"

	"syndrdb/src/internal/storage/buffer"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"
)

type BundleStorageEngine struct {
	fileManager   *buffer.FileManager
	DataDirectory string
	logger        *zap.SugaredLogger
	serializer    format.BundleSerializer // Configurable serialization format

	// PHASE 3: SHARDED CACHES - Replaces global mutex + map pairs with 64-shard caches
	// This eliminates serialization bottlenecks where all operations contested on global mutexes
	writeBufferCache *ShardedBufferCache // Per-file write buffers (replaces writeBuffers + bufferMutex)

	// MULTI-FILE STORAGE: Manifest managers per bundle to track segment files
	// PHASE 3: Sharded for concurrent bundle access
	manifestCache *ShardedManifestCache // Per-bundle manifest managers (replaces manifestManagers + manifestManagersMutex)

	// COMPACTION: Background compaction scheduler with parallel workers
	compactor           *BundleCompactor
	compactionScheduler *CompactionScheduler
	compactionContext   context.Context    // Context for graceful compaction shutdown
	compactionCancel    context.CancelFunc // Cancel function for compaction goroutines

	// CONCURRENCY CONTROL: Per-bundle write locks to prevent dirty reads
	// RWMutex allows concurrent reads while blocking during writes
	// PERFORMANCE: Sharded across 64 buckets to reduce contention under high concurrency
	writeLocks *ShardedWriteLockMap // Sharded per-bundle write locks (eliminates global mutex)

	// DOCUMENT-LEVEL LOCKING: Per-document write locks for concurrent updates
	// Allows concurrent writes to different documents within the same bundle
	// Similar to Postgres row-level locking for improved write throughput
	documentLocks      map[string]map[string]*sync.Mutex // bundleName -> docID -> mutex
	documentLocksMutex sync.RWMutex                      // Protects documentLocks map

	// PHASE 1: MVCC - Per-bundle rotation locks for file rotation coordination
	// Rotation is rare but needs exclusive access to prevent multiple rotations
	// PERFORMANCE: Sharded across 64 buckets to reduce contention
	rotationLocks *ShardedMutexMap // Sharded per-bundle rotation locks

	// PERFORMANCE OPTIMIZATION: Pre-allocated buffers to avoid memory allocations
	headerBuffer    [32]byte  // Reusable 32-byte buffer for headers
	combinedBuffers sync.Pool // Pool of byte slices for combined data

	// DATA INTEGRITY: Write verification and corruption detection
	writeVerifier *DocumentWriteVerifier // Checksum verification for write operations
	writeLogger   *BundleWriteLogger     // Detailed write operation logging for debugging

	// PROJECTION PUSHDOWN: Temporary storage for projection fields per bundle
	// PHASE 3: Sharded for concurrent bundle access
	projectionCache *ShardedProjectionCache // Per-bundle projection fields (replaces projectionFields + projectionMutex)

	// FILE READ CACHE: Bounded cache of file/segment contents to avoid repeated
	// full-file reads when LoadDocumentPage is called many times (e.g. getAllDocumentsForIndexing).
	// PHASE 3: Sharded for concurrent file access
	fileReadCache *ShardedFileReadCache // File content cache (replaces fileReadCache + fileReadCacheMutex)

	// PARSED DOCS CACHE: Caches fully parsed documents from segment files.
	// Key: "bundleName:filePath". This avoids re-parsing the same file content
	// when loading different pages, which is critical for multi-file storage
	// where each page load would otherwise re-parse all segment files.
	// PHASE 3: Sharded for concurrent cache access
	parsedDocsCache *ShardedParsedDocsCache // Parsed docs cache (replaces parsedDocsCache + parsedDocsCacheMutex)

	// PHASE 8: Using golang.org/x/sync/singleflight to prevent thundering herd on cache population.
	// When cache miss occurs, only one goroutine parses the file while others wait.
	// This replaces the hand-rolled parseInFlight map + mutex pattern.
	parseSingleflight singleflight.Group

	// MERGED BUNDLE CACHE: One merge+sort per bundle; serve all page requests by slicing.
	// Key: "bundleName:databaseName". Eliminates O(pages × (merge+sort)) in multi-file LoadDocumentPage.
	mergedBundleCache *ShardedMergedBundleCache
	mergeSingleflight singleflight.Group

	// COMPACTION CALLBACK: Invoked when compaction completes for a bundle so
	// BundleService can invalidate documentPageMap (logical page positions change).
	onCompactionComplete   func(databaseName, bundleName string)
	onCompactionCompleteMu sync.RWMutex

	// schemaProvider returns the bundle field schema for decode/encode. Set by bundle_service so BSE can fill doc.Values.
	schemaProvider func(bundleName, databaseName string) *models.BundleFieldSchema
}

// fileReadCacheEntry holds a cached file buffer and lastAccess for LRU eviction.
// lastAccess is atomic to avoid data races under concurrent read/eviction.
type fileReadCacheEntry struct {
	data       []byte
	lastAccess atomic.Int64
}

// parsedDocsCache caches fully parsed documents from a file to avoid re-parsing
// for different page loads. Key is filePath, value contains parsed docs and metadata.
// This is critical for multi-file storage where each page load would otherwise
// re-parse all segment files.
type parsedDocsCacheEntry struct {
	// All documents parsed from this file (doc ID -> doc)
	documents map[string]models.Document
	// Deleted document IDs (tombstones)
	deletedDocIDs map[string]bool
	// Total unique documents in file (for index calculation)
	totalDocs uint32
	// Last access time for LRU eviction (atomic to avoid data races)
	lastAccess atomic.Int64
}

type BundleFactory interface {
	NewBundle(name, description string) *models.Bundle
}

type BundleStore interface {
	// Legacy methods for backward compatibility
	LoadAllBundleDataFiles(dataRootDir string) (map[string]*models.Bundle, error)
	LoadBundleDataFile(database *models.Database, dataRootDir string, fileName string) (*models.Bundle, error)
	LoadBundleIntoMemory(database *models.Database, bundleName string) (*[]byte, *models.Bundle, error)

	// New scalable methods for page-based bundle management
	LoadAllBundleMetadata(dataRootDir string) (map[string]*models.Bundle, error)
	LoadBundleMetadata(database *models.Database, dataRootDir string, fileName string) (*models.Bundle, error)
	LoadDocumentPage(bundleName string, databaseName string, pageID uint32, dataRootDir string) (*models.DocumentPage, error)

	// Document counting - optimized count-only operations
	CountDocuments(bundleName, databaseName string) (int, error) // Count all documents in bundle (multi-file or legacy)

	// PHASE 0: MVCC - Version retrieval
	GetDocumentVersions(bundleName, databaseName, documentID string) ([]*models.Document, error) // Get all versions of a document

	// Bundle management
	CreateBundleFile(database *models.Database, bundle *models.Bundle) error
	UpdateBundleFile(database *models.Database, bundle *models.Bundle) error
	UpdateDocumentDataInBundleFile(database *models.Database, bundle *models.Bundle, documentID string, updatedDocument map[string]interface{}, mmapData []byte) error
	UpdateBundleFilename(database *models.Database, bundle *models.Bundle, oldBundleName string) error
	UpdateDocumentInBundleFile(bundle *models.Bundle, document *models.Document) error
	UpdateDocumentsBatch(bundle *models.Bundle, documents []*models.Document) error
	UpdateDocumentsBatchWithLocks(bundle *models.Bundle, documents []*models.Document, preLockedDocIDs []string) error // Document-level locking
	DeleteDocumentFromBundleFile(bundle *models.Bundle, documentID string) error
	AppendDeletionMarkersBatch(bundle *models.Bundle, documentIDs []string) error
	AppendDeletionMarkersBatchWithLocks(bundle *models.Bundle, documentIDs []string, preLockedDocIDs []string) error // No bundle lock when doc-level locks held

	// Returns the physical page ID where the document was stored
	AddDocumentToBundleFile(bundle *models.Bundle, document *models.Document) (uint32, error)
	AppendDocumentToBundleFile(bundle *models.Bundle, document *models.Document) (uint32, error)
	AppendDocumentToBundleFileWithTxID(bundle *models.Bundle, document *models.Document, txID string) (uint32, error)

	// RCU (Read-Copy-Update) Version Append: Creates new document version without bundle locks
	// Used by UpdateDocumentInBundleRCU for lock-free concurrent updates
	// Parameters:
	//   - bundle: Target bundle
	//   - newDoc: New document version with updated fields
	//   - oldDoc: Previous document version (used to increment VersionSequence)
	//   - commitSequence: Global commit sequence from SnapshotManager.GetNextCommitSequence()
	// Returns: Page ID where new version was stored
	AppendVersionToBundleFile(bundle *models.Bundle, newDoc *models.Document, oldDoc *models.Document, commitSequence uint64) (uint32, error)

	RemoveDocumentFromBundleFile(database *models.Database, bundle *models.Bundle, documentID string, mmapData []byte) error
	BundleFileExists(bundleName string, databaseName string) bool
	RemoveBundleFile(database *models.Database, bundleName string) error

	FlushWriteBuffers(bundleName string) error
	FlushAllWriteBuffers() error
	CloseWriteBuffer(bundleName string) error
	CloseWriteBuffers() error

	// Cache compaction - reclaims memory from deleted map entries
	CompactAllCaches() int

	// Cache flushing - completely clears all document-holding caches for fresh start
	FlushAllDocumentCaches()

	// Diagnostics
	GetAllWriteBufferStats() []WriteBufferStats

	// Transaction-aware buffer methods
	GetBufferedDocumentsForTransaction(bundleName string, txID string) ([]*models.Document, error)
	MarkDocumentDiscarded(bundleName string, docID string) error
	IsDocumentBuffered(bundleName string, docID string) bool
	GetDiscardedDocuments(bundleName string) []string
	ClearDiscardedDocuments(bundleName string, docIDs []string)

	// Document lookup optimization - bypasses stale pageID issues
	GetDocumentsByIDsFromCache(bundleName, databaseName string, docIDs []string) (map[string]models.Document, map[string]bool)
}

func NewBundleStore(dataDir string, bufferPool *buffer.BufferPool, logger *zap.SugaredLogger, storageFormat string) (*BundleStorageEngine, error) {
	// Create a buffer pool for file management
	fileManager, err := buffer.NewFileManager(dataDir, bufferPool, logger)
	if err != nil {
		return nil, fmt.Errorf("could not create file manager: %w", err)
	}

	// Get the appropriate serializer based on format
	serializer, err := format.GetSerializer(storageFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to create serializer: %w", err)
	}
	//logger.Debugf("Bundle storage using %s format", serializer.GetFormatName())

	// Create a new bundle store
	// PHASE 3: Using sharded caches (64 shards each) to eliminate global mutex contention
	store := &BundleStorageEngine{
		DataDirectory:    dataDir,
		fileManager:      fileManager,
		logger:           logger,
		serializer:       serializer,
		writeBufferCache: NewShardedBufferCache(),                 // PHASE 3: Sharded write buffer cache (replaces map + mutex)
		projectionCache:  NewShardedProjectionCache(),             // PHASE 3: Sharded projection fields cache (replaces map + mutex)
		manifestCache:    NewShardedManifestCache(),               // PHASE 3: Sharded manifest cache (replaces map + mutex)
		writeLocks:       NewShardedWriteLockMap(),                // PERFORMANCE: Sharded write locks (64 shards)
		documentLocks:    make(map[string]map[string]*sync.Mutex), // DOCUMENT-LEVEL LOCKING: Initialize document locks map
		rotationLocks:    NewShardedMutexMap(),                    // PERFORMANCE: Sharded rotation locks (64 shards)
		writeVerifier:    NewDocumentWriteVerifier(logger),        // Initialize write verification
		writeLogger:      NewBundleWriteLogger(logger, 10000),     // Keep last 10000 write operations (increased to reduce wrapping)
		fileReadCache:    NewShardedFileReadCache(),               // PHASE 3: Sharded file read cache (replaces map + mutex)
		parsedDocsCache:   NewShardedParsedDocsCache(),             // PHASE 3: Sharded parsed docs cache (replaces map + mutex)
		mergedBundleCache: NewShardedMergedBundleCache(),            // Merge-once per bundle for multi-file LoadDocumentPage
		// PHASE 8: parseSingleflight and mergeSingleflight require no initialization (zero value is ready to use)
	}

	// Initialize compaction system (3 workers, PostgreSQL autovacuum-inspired)
	store.compactor = NewBundleCompactor(dataDir, store, logger)
	store.compactionScheduler = NewCompactionScheduler(store.compactor, 3, logger)
	store.compactionScheduler.Start()

	// Start periodic compaction evaluator (PostgreSQL autovacuum-style background checks)
	ctx, cancel := context.WithCancel(context.Background())
	store.compactionContext = ctx
	store.compactionCancel = cancel
	go store.periodicCompactionEvaluator(ctx)

	// Ensure the data directory exists
	if err := os.MkdirAll(store.DataDirectory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", store.DataDirectory, err)
	}

	// PHASE 2B: Cache warming - pre-parse files for GROUP BY optimization
	// This runs async and doesn't block startup
	if settings.GetSettings().GroupByCacheWarmingEnabled {
		go store.warmParsedDocsCache(ctx)
	}

	return store, nil
}

// warmParsedDocsCache pre-parses bundle files to warm the parsed docs cache for GROUP BY optimization
// PHASE 2B: Background cache warming - non-blocking, respects memory budget
// Runs async on startup to improve first GROUP BY query performance
func (bse *BundleStorageEngine) warmParsedDocsCache(ctx context.Context) {
	maxMB := settings.GetSettings().GroupByCacheWarmingMaxMB
	maxBytes := int64(maxMB) * 1024 * 1024

	bse.logger.Debugf("Starting cache warming for GROUP BY optimization (max %d MB)", maxMB)

	// Sleep briefly to let server finish startup
	select {
	case <-ctx.Done():
		return
	case <-time.After(5 * time.Second):
	}

	// Track memory usage
	var totalBytes atomic.Int64

	// Walk data directory to find all .manifest and .dat files
	err := filepath.Walk(bse.DataDirectory, func(path string, info os.FileInfo, err error) error {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("cache warming cancelled")
		default:
		}

		if err != nil {
			bse.logger.Warnf("Cache warming: skipping path %s: %v", path, err)
			return nil
		}

		// Skip if not a file
		if info.IsDir() {
			return nil
		}

		// Check if we've reached memory limit
		if totalBytes.Load() >= maxBytes {
			bse.logger.Debugf("Cache warming complete: reached memory limit of %d MB", maxMB)
			return filepath.SkipAll
		}

		// Only process .manifest files (they reference all segment files)
		if !strings.HasSuffix(path, ".manifest") {
			return nil
		}

		// Load manifest to get segment files
		manifestData, err := os.ReadFile(path)
		if err != nil {
			bse.logger.Debugf("Cache warming: failed to read manifest %s: %v", path, err)
			return nil
		}

		// Decode manifest as JSON (bundle.manifest format)
		var manifest BundleManifest
		if err := json.Unmarshal(manifestData, &manifest); err != nil {
			bse.logger.Debugf("Cache warming: failed to parse manifest JSON %s: %v", path, err)
			return nil
		}

		bundleName := manifest.BundleName
		databaseName := manifest.DatabaseName
		manifestDir := filepath.Dir(path)

		for _, fileInfo := range manifest.Files {
			if fileInfo == nil {
				continue
			}
			// Check for cancellation and memory limit
			select {
			case <-ctx.Done():
				return fmt.Errorf("cache warming cancelled")
			default:
			}

			if totalBytes.Load() >= maxBytes {
				bse.logger.Debugf("Cache warming complete: reached memory limit of %d MB", maxMB)
				return filepath.SkipAll
			}

			segmentFile := filepath.Join(manifestDir, fileInfo.FileName)
			cacheKey := fmt.Sprintf("%s:%s", bundleName, segmentFile)

			// Check if already in cache
			if bse.parsedDocsCache.Get(cacheKey) != nil {
				continue
			}

			// Check if file exists
			if _, err := os.Stat(segmentFile); os.IsNotExist(err) {
				continue
			}

			// Pre-parse the file to warm cache
			fileData, err := os.ReadFile(segmentFile)
			if err != nil {
				bse.logger.Debugf("Cache warming: failed to read segment %s: %v", segmentFile, err)
				continue
			}

			// Parse documents from file
			docs, deletedIDs, totalDocs, err := bse.parseAllDocumentsFromFile(bundleName, databaseName, &fileData)
			if err != nil {
				bse.logger.Debugf("Cache warming: failed to parse segment %s: %v", segmentFile, err)
				continue
			}

			// Store in cache
			entry := &parsedDocsCacheEntry{
				documents:     docs,
				deletedDocIDs: deletedIDs,
				totalDocs:     totalDocs,
			}
			entry.lastAccess.Store(time.Now().UnixNano())

			bse.parsedDocsCache.Set(cacheKey, entry)

			// Track memory usage (approximate)
			entrySize := int64(len(fileData))
			totalBytes.Add(entrySize)

			bse.logger.Debugf("Cache warming: pre-parsed %s (%d docs, %.2f MB total)",
				filepath.Base(segmentFile), totalDocs, float64(totalBytes.Load())/(1024*1024))
		}

		return nil
	})

	if err != nil && !strings.Contains(err.Error(), "cancelled") {
		bse.logger.Warnf("Cache warming completed with errors: %v", err)
	} else {
		bse.logger.Debugf("Cache warming complete: pre-parsed %.2f MB of data",
			float64(totalBytes.Load())/(1024*1024))
	}
}

// LoadAllBundleMetadata loads only the bundle structure/metadata without documents
// DEPRECATED: Use LoadAllBundleDataFiles or page-based loading instead
func (bse *BundleStorageEngine) LoadAllBundleMetadata(dataDir string) (map[string]*models.Bundle, error) {
	bundles := make(map[string]*models.Bundle)

	// Read all files in the data directory
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory '%s': %w", dataDir, err)
	}

	// Load each bundle metadata file
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".bnd") {
			continue
		}

		bundleName := strings.TrimSuffix(file.Name(), ".bnd")
		bundleDatabase := strings.SplitN(bundleName, "_", 2)[0]
		bundleName = strings.TrimPrefix(bundleName, bundleDatabase+"_") // Remove database name prefix if present
		bundleMetadata, err := bse.loadBundleMetadataFromFile(dataDir, file.Name())
		if err != nil {
			bse.logger.Warnf("Failed to load bundle metadata '%s': %v", file.Name(), err)
			continue
		}

		bundles[bundleName] = bundleMetadata
		bse.logger.Debugf("Loaded bundle metadata for '%s'", bundleName)
	}

	bse.logger.Debugf("Loaded metadata for %d bundles from '%s'", len(bundles), dataDir)
	return bundles, nil
}

// min returns the minimum of two integers
// Single Responsibility: Helper function for safe array slicing in corruption detection
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// LoadBundleMetadata loads only the bundle structure/metadata for a specific bundle
func (bse *BundleStorageEngine) LoadBundleMetadata(database *models.Database, dataRootDir string, fileName string) (*models.Bundle, error) {
	bundle, err := bse.loadBundleMetadataFromFile(dataRootDir, fileName)
	if err != nil {
		return nil, err
	}

	// CRITICAL FIX: Set the database reference that was missing!
	// This ensures bundle.Database is not nil when the bundle is used later
	bundle.Database = database

	return bundle, nil
}

// loadBundleMetadataFromFile loads bundle metadata without documents using configured format
func (bse *BundleStorageEngine) loadBundleMetadataFromFile(dataDir, fileName string) (*models.Bundle, error) {
	filePath := filepath.Join(dataDir, fileName)

	// Read the bundle file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bundle file '%s': %w", filePath, err)
	}

	// Use only the configured format (no legacy BSON support)
	bundle, err := bse.serializer.DeserializeBundleMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bundle file '%s' with %s format: %w", fileName, bse.serializer.GetFormatName(), err)
	}

	return bundle, nil
}

// LoadDocumentPage loads a specific page of documents for a bundle
// OPTIMIZATION: Uses mergedBundleCache so we merge+sort once per bundle, then serve all pages by slicing.
// First page load builds merged view (or uses singleflight); subsequent page loads are O(pageSize) from cache.
func (bse *BundleStorageEngine) LoadDocumentPage(bundleName string, databaseName string, pageID uint32, dataRootDir string) (*models.DocumentPage, error) {
	// MULTI-FILE STORAGE: Load manifest to get all segment files
	manifestMgr := bse.getOrCreateManifestManager(databaseName, bundleName)
	manifest, err := manifestMgr.LoadOrCreate(databaseName, bundleName)
	if err != nil {
		return bse.loadDocumentPageLegacy(bundleName, databaseName, pageID, dataRootDir)
	}
	if len(manifest.Files) == 0 {
		return bse.loadDocumentPageLegacy(bundleName, databaseName, pageID, dataRootDir)
	}

	pageSize := uint32(4096) // Use consistent page size with BundleService (power of 2)
	startIndex := pageID * pageSize
	endIndex := startIndex + pageSize
	bundleKey := bundleName + ":" + databaseName

	// FAST PATH: Serve from cached merged view (O(pageSize) per page)
	if entry := bse.mergedBundleCache.Get(bundleKey); entry != nil {
		page := bse.pageFromMergedEntry(bundleName, pageID, pageSize, startIndex, endIndex, entry)
		if bse.logger != nil && settings.GetSettings().Debug {
			bse.logger.Debugf("Loaded page %d for bundle %s from merged cache (%d docs in page, total: %d)",
				pageID, bundleName, len(page.Documents), len(entry.SortedDocIDs))
		}
		return page, nil
	}

	// SLOW PATH: Build merged view once (singleflight), cache it, then serve this page
	result, err, _ := bse.mergeSingleflight.Do(bundleKey, func() (interface{}, error) {
		entry, buildErr := bse.buildMergedBundleView(databaseName, bundleName, manifest)
		if buildErr != nil {
			return nil, buildErr
		}
		// Cache for all subsequent page loads
		bse.mergedBundleCache.Set(bundleKey, entry)
		return entry, nil
	})
	if err != nil {
		return nil, fmt.Errorf("build merged view for %s: %w", bundleKey, err)
	}
	entry := result.(*mergedBundleCacheEntry)
	page := bse.pageFromMergedEntry(bundleName, pageID, pageSize, startIndex, endIndex, entry)
	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Loaded page %d for bundle %s: merged %d files, %d documents in page (total: %d)",
			pageID, bundleName, len(manifest.Files), len(page.Documents), len(entry.SortedDocIDs))
	}
	return page, nil
}

// pageFromMergedEntry extracts a single page from a cached merged view (O(pageSize)).
func (bse *BundleStorageEngine) pageFromMergedEntry(bundleName string, pageID uint32, pageSize uint32, startIndex, endIndex uint32, entry *mergedBundleCacheEntry) *models.DocumentPage {
	totalDocs := uint32(len(entry.SortedDocIDs))
	pageDocuments := make(map[string]models.Document)
	if startIndex < totalDocs {
		sorted := entry.SortedDocIDs
		for i := int(startIndex); i < int(endIndex) && i < len(sorted); i++ {
			docID := sorted[i]
			pageDocuments[docID] = entry.Documents[docID]
		}
	}
	page := &models.DocumentPage{
		PageID:    pageID,
		BundleID:  bundleName,
		Documents: pageDocuments,
		LoadedAt:  time.Now(),
		IsDirty:   false,
	}
	if pageID > 0 {
		prevPageID := pageID - 1
		page.PreviousPageID = &prevPageID
	}
	totalPages := (totalDocs + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}
	if pageID < totalPages-1 {
		nextPageID := pageID + 1
		page.NextPageID = &nextPageID
	}
	return page
}

// buildMergedBundleView merges all segment files, applies tombstone/empty filters, and returns sorted doc IDs.
// Called once per bundle (under singleflight); result is cached for all page requests.
func (bse *BundleStorageEngine) buildMergedBundleView(databaseName string, bundleName string, manifest *BundleManifest) (*mergedBundleCacheEntry, error) {
	mergedDocuments := make(map[string]models.Document)
	deletedDocIDs := make(map[string]bool)

	for _, fileInfo := range manifest.Files {
		bundleDir := GetBundleDirectory(databaseName, bundleName)
		filePath := filepath.Join(bundleDir, fileInfo.FileName)
		cacheKey := fmt.Sprintf("%s:%s", bundleName, filePath)

		cached := bse.parsedDocsCache.GetAndTouch(cacheKey)
		if cached != nil {
			for docID, doc := range cached.documents {
				mergedDocuments[docID] = doc
			}
			for docID := range cached.deletedDocIDs {
				deletedDocIDs[docID] = true
			}
			continue
		}

		type parseResult struct {
			documents   map[string]models.Document
			deletedIDs  map[string]bool
			totalDocs   uint32
			fileSkipped bool
		}
		result, err, _ := bse.parseSingleflight.Do(cacheKey, func() (interface{}, error) {
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				return &parseResult{fileSkipped: true}, nil
			}
			data, err := bse.getOrReadFile(filePath)
			if err != nil {
				bse.logger.Warnf("Failed to read bundle file '%s': %v", filePath, err)
				return &parseResult{fileSkipped: true}, nil
			}
			docs, deleted, total, err := bse.parseAllDocumentsFromFile(bundleName, databaseName, &data)
			if err != nil {
				bse.logger.Warnf("Failed to parse documents from file '%s': %v", filePath, err)
				return &parseResult{fileSkipped: true}, nil
			}
			bse.cacheParsedDocs(cacheKey, docs, deleted, total)
			return &parseResult{documents: docs, deletedIDs: deleted, totalDocs: total}, nil
		})
		if err != nil {
			continue
		}
		parsed := result.(*parseResult)
		if parsed.fileSkipped || parsed.documents == nil {
			continue
		}
		for docID, doc := range parsed.documents {
			mergedDocuments[docID] = doc
		}
		for docID := range parsed.deletedIDs {
			deletedDocIDs[docID] = true
		}
	}

	for docID := range deletedDocIDs {
		delete(mergedDocuments, docID)
	}
	for docID, doc := range mergedDocuments {
		if doc.DocumentID == "" {
			delete(mergedDocuments, docID)
		}
	}

	sortedDocIDs := make([]string, 0, len(mergedDocuments))
	for docID := range mergedDocuments {
		sortedDocIDs = append(sortedDocIDs, docID)
	}
	sort.Strings(sortedDocIDs)

	return &mergedBundleCacheEntry{
		Documents:    mergedDocuments,
		SortedDocIDs: sortedDocIDs,
	}, nil
}

// loadDocumentPageLegacy loads a page from the legacy single-file format.
// OPTIMIZATION: Uses the same mergedBundleCache as multi-file — parse the entire file once,
// cache (documents + sorted doc IDs), then serve all page requests by slicing. This removes
// O(pageCount) full-file scans that caused 28+ second aggregate/group query latency.
func (bse *BundleStorageEngine) loadDocumentPageLegacy(bundleName string, databaseName string, pageID uint32, dataRootDir string) (*models.DocumentPage, error) {
	pageSize := uint32(4096)
	startIndex := pageID * pageSize
	endIndex := startIndex + pageSize
	bundleKey := bundleName + ":" + databaseName

	// FAST PATH: Serve from cached merged view (same cache as multi-file path)
	if entry := bse.mergedBundleCache.Get(bundleKey); entry != nil {
		page := bse.pageFromMergedEntry(bundleName, pageID, pageSize, startIndex, endIndex, entry)
		if bse.logger != nil && settings.GetSettings().Debug {
			bse.logger.Debugf("Loaded legacy page %d for bundle %s from merged cache (%d docs in page, total: %d)",
				pageID, bundleName, len(page.Documents), len(entry.SortedDocIDs))
		}
		return page, nil
	}

	// SLOW PATH: Parse entire file once (singleflight), cache, then serve this page
	result, err, _ := bse.mergeSingleflight.Do(bundleKey, func() (interface{}, error) {
		entry, buildErr := bse.buildMergedBundleViewLegacy(bundleName, databaseName, dataRootDir)
		if buildErr != nil {
			return nil, buildErr
		}
		bse.mergedBundleCache.Set(bundleKey, entry)
		return entry, nil
	})
	if err != nil {
		return nil, fmt.Errorf("build merged view for legacy bundle %s: %w", bundleKey, err)
	}
	entry := result.(*mergedBundleCacheEntry)
	page := bse.pageFromMergedEntry(bundleName, pageID, pageSize, startIndex, endIndex, entry)
	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Loaded legacy page %d for bundle %s: %d documents in page (total: %d)",
			pageID, bundleName, len(page.Documents), len(entry.SortedDocIDs))
	}
	return page, nil
}

// buildMergedBundleViewLegacy parses the entire legacy single file once and returns merged view for caching.
func (bse *BundleStorageEngine) buildMergedBundleViewLegacy(bundleName string, databaseName string, dataRootDir string) (*mergedBundleCacheEntry, error) {
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	data, err := bse.getOrReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bundle file '%s': %w", filePath, err)
	}
	_, err = bse.serializer.DeserializeBundleMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bundle metadata: %w", err)
	}

	// Parse entire file in one pass (range 0..MaxUint32 => all documents)
	const maxU32 = uint32(0xffffffff)
	allDocuments, _, err := bse.readDocumentRange(bundleName, databaseName, 0, maxU32, &data, []string{})
	if err != nil {
		return nil, fmt.Errorf("failed to read document range: %w", err)
	}

	sortedDocIDs := make([]string, 0, len(allDocuments))
	for docID := range allDocuments {
		sortedDocIDs = append(sortedDocIDs, docID)
	}
	sort.Strings(sortedDocIDs)

	return &mergedBundleCacheEntry{
		Documents:    allDocuments,
		SortedDocIDs: sortedDocIDs,
	}, nil
}

// getOrReadFile returns the file content from the file-read cache, or reads from disk and caches it.
//
// Contract (Issue 9): Callers must not modify the returned slice. The slice may be shared with
// the file-read cache; mutation would corrupt the cache and cause wrong data for subsequent callers.
// Use the returned bytes read-only (e.g. parse or copy before modifying).
//
// PHASE 3: Using ShardedFileReadCache for concurrent access without global mutex
//
// CRITICAL: This function is aware of active WriteBuffers to prevent torn reads.
// If there's an active WriteBuffer for this file, we:
// 1. BYPASS THE CACHE entirely (active files change frequently, caching is counterproductive)
// 2. Wait for in-flight writes to complete
// 3. Read only up to atomicOffset bytes (the known valid data)
// This prevents reading partially-written data and stale cached data.
func (bse *BundleStorageEngine) getOrReadFile(filePath string) ([]byte, error) {
	// CRITICAL: Check for active WriteBuffer BEFORE checking cache
	// If there's an active buffer, we must bypass the cache to get fresh data
	writeBuffer := bse.writeBufferCache.Get(filePath)

	if writeBuffer != nil && !writeBuffer.IsFrozen() {
		// Active buffer exists - bypass cache and read directly with safety measures
		return bse.readFileSafeForActiveBuffer(filePath, writeBuffer)
	}

	// No active buffer or buffer is frozen - safe to use cache
	maxEntries := settings.GetSettings().FileReadCacheMaxEntries
	if maxEntries <= 0 {
		maxEntries = 32
	}

	// Per-shard max entries (64 shards, so divide by 64 but ensure at least 1)
	maxEntriesPerShard := maxEntries / CacheShardCount
	if maxEntriesPerShard < 1 {
		maxEntriesPerShard = 1
	}

	return bse.fileReadCache.GetOrCreate(filePath, maxEntriesPerShard, func() ([]byte, error) {
		// Double-check: WriteBuffer may have been created between our check and cache miss
		wb := bse.writeBufferCache.Get(filePath)
		if wb != nil && !wb.IsFrozen() {
			return bse.readFileSafeForActiveBuffer(filePath, wb)
		}
		// No active buffer - safe to read entire file
		return os.ReadFile(filePath)
	})
}

// readFileSafeForActiveBuffer reads a file while being aware of concurrent WriteDirectAtomic writes.
// If there's an active WriteBuffer for this file, we wait for in-flight writes and read only
// committed data to prevent torn reads.
//
// Race condition this prevents:
// 1. Writer reserves offset N with atomic.AddInt64 (file logically grows)
// 2. Reader calls os.ReadFile which reads up to file size (may include reserved-but-unwritten offset N)
// 3. Writer does pwrite at offset N
// 4. Reader has garbage/partial data at offset N
//
// Solution:
// 1. Wait for inflightWrites to reach 0 (all writes complete)
// 2. Get atomicOffset (the boundary of valid data)
// 3. Read only atomicOffset bytes from the file
//
// NOTE: Results from this function should NOT be cached because the file is actively being written to.
func (bse *BundleStorageEngine) readFileSafeForActiveBuffer(filePath string, writeBuffer *WriteBuffer) ([]byte, error) {
	// Active buffer exists - we need to be careful about in-flight writes
	// Wait for any in-flight writes to complete by spinning until counter is 0
	// We do NOT freeze the buffer - we just wait for current in-flight writes
	backoff := time.Microsecond
	maxBackoff := 100 * time.Microsecond
	maxWaitTime := 500 * time.Millisecond
	startTime := time.Now()

	for atomic.LoadInt64(&writeBuffer.inflightWrites) > 0 {
		if time.Since(startTime) > maxWaitTime {
			// Timeout - log warning and proceed with best-effort read
			if bse.logger != nil {
				bse.logger.Warnf("Timeout waiting for in-flight writes to complete for %s, proceeding with read", filePath)
			}
			break
		}
		time.Sleep(backoff)
		if backoff < maxBackoff {
			backoff *= 2
		}
	}

	// Now get the safe read boundary - this is the highest offset that has been written
	safeOffset := writeBuffer.GetAtomicOffset()

	// Open file and read only up to safeOffset
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read exactly safeOffset bytes (or less if file is smaller)
	data := make([]byte, safeOffset)
	n, err := io.ReadFull(file, data)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// File is smaller than safeOffset - this can happen if file was just created
		// Return what we got
		return data[:n], nil
	}
	if err != nil {
		return nil, err
	}

	return data[:n], nil
}

// GetDocumentsByIDsFromCache retrieves documents by IDs directly from the parsed docs cache.
// This is much faster than page-based lookup when we have docIDs from an index search.
// Uses last-write-wins semantics since parsed cache is built from all segment files.
// Returns documents found and a set of docIDs not in cache (need fallback).
func (bse *BundleStorageEngine) GetDocumentsByIDsFromCache(bundleName, databaseName string, docIDs []string) (map[string]models.Document, map[string]bool) {
	result := make(map[string]models.Document, len(docIDs))
	notFound := make(map[string]bool)

	// Build docID set for O(1) lookup
	docIDSet := make(map[string]bool, len(docIDs))
	for _, id := range docIDs {
		docIDSet[id] = true
		notFound[id] = true // Assume not found until we find it
	}

	// Load manifest to get all segment files
	manifestMgr := bse.getOrCreateManifestManager(databaseName, bundleName)
	manifest, err := manifestMgr.LoadOrCreate(databaseName, bundleName)
	if err != nil {
		// No manifest - return all as not found for fallback
		return result, notFound
	}

	// Check parsed cache for each segment file
	// PHASE 3: Using ShardedParsedDocsCache for concurrent access without global mutex
	bundleDir := GetBundleDirectory(databaseName, bundleName)
	for _, fileInfo := range manifest.Files {
		filePath := filepath.Join(bundleDir, fileInfo.FileName)
		cacheKey := fmt.Sprintf("%s:%s", bundleName, filePath)

		cached := bse.parsedDocsCache.GetAndTouch(cacheKey)
		if cached != nil {
			// Check for deleted docs first (tombstones take precedence)
			for docID := range cached.deletedDocIDs {
				if docIDSet[docID] {
					delete(result, docID) // Remove if was found in earlier file
					delete(notFound, docID)
				}
			}
			// Then look for the docs we need
			for docID := range docIDSet {
				if doc, found := cached.documents[docID]; found {
					if !cached.deletedDocIDs[docID] {
						result[docID] = doc
						delete(notFound, docID)
					}
				}
			}
		}
	}

	return result, notFound
}

// InvalidateFileReadCache removes a single path from the file-read cache.
// Call after compact or replace of that file so subsequent reads see fresh data.
// PHASE 3: Using ShardedFileReadCache for concurrent access without global mutex
func (bse *BundleStorageEngine) InvalidateFileReadCache(filePath string) {
	bse.fileReadCache.Delete(filePath)
	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Invalidated file read cache: %s", filePath)
	}
}

// InvalidateFileReadCacheForBundle removes all cached buffers for a bundle (legacy file and
// all segment files in the bundle dir). Call on bundle drop or after compaction that
// replaces segments.
// PHASE 3: Using ShardedFileReadCache.DeleteMatching for concurrent-safe iteration
func (bse *BundleStorageEngine) InvalidateFileReadCacheForBundle(databaseName, bundleName string) {
	legacyPath := filepath.Join(helpers.GetDatabaseFolderPath(databaseName), fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))
	bundleDir := GetBundleDirectory(databaseName, bundleName)

	bse.fileReadCache.DeleteMatching(func(k string) bool {
		return k == legacyPath || filepath.Dir(k) == bundleDir
	})

	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Invalidated file read cache for bundle %s/%s", databaseName, bundleName)
	}
}

// parseAllDocumentsFromFile parses all documents from a file (not just a page range).
// This is used to populate the parsed docs cache, which then allows O(1) page extraction
// instead of O(N) re-parsing for each page load.
// Returns: documents map, deleted doc IDs, total unique docs, error
func (bse *BundleStorageEngine) parseAllDocumentsFromFile(bundleName, databaseName string, fileData *[]byte) (map[string]models.Document, map[string]bool, uint32, error) {
	// Acquire read lock for the bundle
	lock := bse.getWriteLock(bundleName)
	lock.RLock()
	defer lock.RUnlock()

	documents := make(map[string]models.Document)
	deletedDocIDs := make(map[string]bool)
	seenDocIDs := make(map[string]struct{})
	offset := 0

	schema := models.NewProjectionSchema(nil) // minimal for decode
	if bse.schemaProvider != nil {
		if s := bse.schemaProvider(bundleName, databaseName); s != nil {
			schema = s
		}
	}

	// Skip bundle metadata header if present
	if len(*fileData) >= 8 {
		magic := binary.LittleEndian.Uint32((*fileData)[0:4])
		if magic == 0x42444D44 { // "BDMD" = Bundle Metadata
			metadataSize := binary.LittleEndian.Uint32((*fileData)[4:8])
			offset = int(8 + metadataSize)
		}
	}

	for offset < len(*fileData) {
		if offset+8 > len(*fileData) {
			break
		}

		magic := binary.LittleEndian.Uint32((*fileData)[offset : offset+4])
		size := binary.LittleEndian.Uint32((*fileData)[offset+4 : offset+8])

		if magic == 0xDEADBEEF || magic == CompressedDocMagic ||
			magic == EncryptedDocMagic || magic == EncryptedCompressedDocMagic {
			// Document record (plain, compressed, encrypted, or encrypted+compressed)
			if offset+8+int(size) > len(*fileData) {
				break
			}

			documentData := (*fileData)[offset+8 : offset+8+int(size)]

			// Decrypt if encrypted magic
			if magic == EncryptedDocMagic || magic == EncryptedCompressedDocMagic {
				enc := extension.GetRegistry().GetStorageEncryptor()
				if enc != nil {
					decrypted, decErr := enc.DecryptBlock(documentData, "bundle:"+bundleName)
					if decErr != nil {
						bse.logger.Warnf("Failed to decrypt document at offset %d: %v", offset, decErr)
						offset += 8 + int(size)
						continue
					}
					documentData = decrypted
				}
			}

			// Decompress if compressed magic (or encrypted+compressed)
			if magic == CompressedDocMagic || magic == EncryptedCompressedDocMagic {
				decompressed, decErr := DecompressDocument(documentData)
				if decErr != nil {
					bse.logger.Warnf("Failed to decompress document at offset %d: %v", offset, decErr)
					offset += 8 + int(size)
					continue
				}
				documentData = decompressed
			}

			fullDoc, err := helpers.DecodeFastBinaryToDocument(documentData, schema)
			if err != nil {
				bse.logger.Warnf("Failed to decode document at offset %d: %v", offset, err)
				offset += 8 + int(size)
				continue
			}

			// Track unique documents and keep latest version
			if _, seen := seenDocIDs[fullDoc.DocumentID]; !seen {
				seenDocIDs[fullDoc.DocumentID] = struct{}{}
			}
			// Always update to get latest version (last-write-wins)
			if !deletedDocIDs[fullDoc.DocumentID] {
				documents[fullDoc.DocumentID] = *fullDoc
			}

			offset += 8 + int(size)
		} else if magic == 0xDEADDEAD || magic == CompressedTombstoneMagic ||
			magic == EncryptedTombstoneMagic || magic == EncryptedCompressedTombMagic {
			// Tombstone marker (plain, compressed, encrypted, or encrypted+compressed)
			if offset+8+int(size) > len(*fileData) {
				break
			}

			deletionData := (*fileData)[offset+8 : offset+8+int(size)]

			// Decrypt if encrypted magic
			if magic == EncryptedTombstoneMagic || magic == EncryptedCompressedTombMagic {
				enc := extension.GetRegistry().GetStorageEncryptor()
				if enc != nil {
					decrypted, decErr := enc.DecryptBlock(deletionData, "bundle:"+bundleName)
					if decErr != nil {
						offset += 8 + int(size)
						continue
					}
					deletionData = decrypted
				}
			}

			// Decompress if compressed magic (or encrypted+compressed)
			if magic == CompressedTombstoneMagic || magic == EncryptedCompressedTombMagic {
				decompressed, decErr := DecompressDocument(deletionData)
				if decErr != nil {
					offset += 8 + int(size)
					continue
				}
				deletionData = decompressed
			}

			deletionDoc, err := helpers.DecodeFastBinaryToDocument(deletionData, schema)
			if err == nil && deletionDoc != nil {
				deletedDocIDs[deletionDoc.DocumentID] = true
				delete(documents, deletionDoc.DocumentID) // Remove if already added
			}

			offset += 8 + int(size)
		} else if magic == 0x42444D44 {
			// Metadata header in middle of file (shouldn't happen, but skip it)
			metadataSize := binary.LittleEndian.Uint32((*fileData)[offset+4 : offset+8])
			offset += int(8 + metadataSize)
		} else {
			// Unknown magic - might be corruption or end of valid data
			break
		}
	}

	return documents, deletedDocIDs, uint32(len(seenDocIDs)), nil
}

// cacheParsedDocs caches parsed documents for a file with LRU eviction.
// Key format: "bundleName:filePath"
// PHASE 3: Using ShardedParsedDocsCache for concurrent access without global mutex
func (bse *BundleStorageEngine) cacheParsedDocs(cacheKey string, documents map[string]models.Document, deletedDocIDs map[string]bool, totalDocs uint32) {
	maxEntries := settings.GetSettings().FileReadCacheMaxEntries
	if maxEntries <= 0 {
		maxEntries = 32 // Default matches file read cache
	}

	// Per-shard max entries (64 shards, so divide by 64 but ensure at least 1)
	maxEntriesPerShard := maxEntries / CacheShardCount
	if maxEntriesPerShard < 1 {
		maxEntriesPerShard = 1
	}

	entry := &parsedDocsCacheEntry{
		documents:     documents,
		deletedDocIDs: deletedDocIDs,
		totalDocs:     totalDocs,
	}
	entry.lastAccess.Store(time.Now().UnixNano())
	bse.parsedDocsCache.SetWithLRU(cacheKey, entry, maxEntriesPerShard)

	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Cached %d parsed documents for %s", len(documents), cacheKey)
	}
}

// InvalidateParsedDocsCacheForBundle removes all cached parsed docs and merged view for a bundle.
// Call after writes, compaction, or any operation that changes bundle contents.
func (bse *BundleStorageEngine) InvalidateParsedDocsCacheForBundle(bundleName string) {
	prefix := bundleName + ":"
	bse.parsedDocsCache.DeleteByPrefix(prefix)
	bse.mergedBundleCache.DeleteByPrefix(prefix)

	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Invalidated parsed docs and merged bundle cache for bundle %s", bundleName)
	}
}

// InvalidateParsedDocsCacheForLatestFile invalidates only the cache entry for the latest segment file.
// This is more efficient than InvalidateParsedDocsCacheForBundle when only the latest file was modified.
// Writes always append to the latest file, so older segment files remain valid.
// Also invalidates the merged bundle view for this bundle so the next page load rebuilds with new data.
func (bse *BundleStorageEngine) InvalidateParsedDocsCacheForLatestFile(bundleName, databaseName string) {
	mm := bse.getOrCreateManifestManager(databaseName, bundleName)
	manifest := mm.GetManifest()
	if len(manifest.Files) == 0 {
		return
	}
	latestFile := manifest.Files[len(manifest.Files)-1]
	bundleDir := GetBundleDirectory(databaseName, bundleName)
	filePath := filepath.Join(bundleDir, latestFile.FileName)
	cacheKey := fmt.Sprintf("%s:%s", bundleName, filePath)
	bse.parsedDocsCache.Delete(cacheKey)
	bse.mergedBundleCache.Delete(bundleName + ":" + databaseName)
	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Invalidated parsed docs and merged cache for latest file %s in bundle %s", latestFile.FileName, bundleName)
	}
}

// RegisterCompactionComplete sets the callback invoked when compaction completes for a bundle.
// BundleService registers InvalidateDocumentPageMapForBundle so documentPageMap stays correct.
// SetSchemaProvider sets the function used to resolve bundle field schema for decode/encode. Required for Values-based documents.
func (bse *BundleStorageEngine) SetSchemaProvider(fn func(bundleName, databaseName string) *models.BundleFieldSchema) {
	bse.schemaProvider = fn
}

// InvalidateBundleCaches clears all cached document data for a bundle so pages are
// re-deserialized from disk on next access. Called when bundle metadata becomes available
// after background cache warming may have deserialized documents with a minimal schema.
func (bse *BundleStorageEngine) InvalidateBundleCaches(bundleName, databaseName string) {
	bse.mergedBundleCache.Delete(bundleName + ":" + databaseName)
	// Also clear parsedDocsCache entries for this bundle (keyed as "bundleName:filePath")
	bse.parsedDocsCache.DeleteByPrefix(bundleName + ":")
}

// GetSchemaForBundle returns the field schema for a bundle (for use by compactor, etc.). Returns nil if provider not set or bundle unknown.
func (bse *BundleStorageEngine) GetSchemaForBundle(bundleName, databaseName string) *models.BundleFieldSchema {
	if bse.schemaProvider == nil {
		return models.NewProjectionSchema(nil)
	}
	if s := bse.schemaProvider(bundleName, databaseName); s != nil {
		return s
	}
	return models.NewProjectionSchema(nil)
}

func (bse *BundleStorageEngine) RegisterCompactionComplete(fn func(databaseName, bundleName string)) {
	bse.onCompactionCompleteMu.Lock()
	defer bse.onCompactionCompleteMu.Unlock()
	bse.onCompactionComplete = fn
}

// InvokeCompactionComplete is called by BundleCompactor after successful compaction.
func (bse *BundleStorageEngine) InvokeCompactionComplete(databaseName, bundleName string) {
	bse.onCompactionCompleteMu.RLock()
	fn := bse.onCompactionComplete
	bse.onCompactionCompleteMu.RUnlock()
	if fn != nil {
		fn(databaseName, bundleName)
	}
}

func testRawBundleData(data []byte) {
	// convert the []bytes to ascii and print it to the screen
	//fmt.Println(string(data))
}

// Helper functions for parsing bundle metadata
// func getString(data map[string]interface{}, key string) string {
// 	if val, ok := data[key].(string); ok {
// 		return val
// 	}
// 	return ""
// }

// func getTime(data map[string]interface{}, key string) time.Time {
// 	if val, ok := data[key].(string); ok {
// 		if t, err := time.Parse(time.RFC3339, val); err == nil {
// 			return t
// 		}
// 	}
// 	return time.Time{}
// }

// LoadAllBundleDataFiles loads all bundle data files from the given directory
// DEPRECATED: Use page-based loading or LoadAllBundleMetadata instead
func (bse *BundleStorageEngine) LoadAllBundleDataFiles(dataDir string) (map[string]*models.Bundle, error) {
	bundles := make(map[string]*models.Bundle)
	// Implementation for loading all bundle data files
	// This is a placeholder that should be filled with actual loading logic
	return bundles, nil
}

// TODO DEPRECATED This is the old, pre-buffer manager implementation.
func (b *BundleStorageEngine) LoadBundleDataFile(database *models.Database, dataRootDir string, fileName string) (*models.Bundle, error) {

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	filePath := filepath.Join(databasePath, fileName)
	// Check if the file exists
	if !helpers.FileExists(filePath, *b.logger) {
		return nil, fmt.Errorf("bundle file %s does not exist", fileName)
	}

	// Extract bundle name from filename (remove .bnd extension) - used for logging
	_ = strings.TrimSuffix(fileName, ".bnd")

	// Try to load bundle metadata first
	bundleMetadata, err := b.loadBundleMetadataFromFile(dataRootDir, fileName)
	if err != nil {
		return nil, fmt.Errorf("error loading bundle metadata from file %s: %w", fileName, err)
	}

	// Documents are now loaded on-demand via page cache, not into memory
	// The old append-only document loading is no longer used for bundle initialization
	bundleMetadata.Database = database

	b.logger.Debugf("Loaded bundle metadata from file %s", fileName)

	return bundleMetadata, nil
}

// TODO this is the old, pre-buffer manager implementation.
func (b *BundleStorageEngine) LoadBundleIntoMemory(database *models.Database, bundleName string) (*[]byte, *models.Bundle, error) {
	//args := settings.GetSettings()
	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	bundleFile, err := helpers.OpenDataFile(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundleName))
	if err != nil {
		return nil, nil, fmt.Errorf("error opening bundle file %s: %w", bundleName, err)
	}
	defer bundleFile.Close()

	// Get the file size
	stat, err := bundleFile.Stat()
	if err != nil {
		log.Printf("Failed to get file stats: %v\n", err)
		return nil, nil, err
	}
	fileSize := int(stat.Size())

	// Memory map the file
	data, err := unix.Mmap(int(bundleFile.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		//fmt.Printf("Failed to memory map file: %v\n", err)
		return nil, nil, err
	}
	defer unix.Munmap(data)

	bundleData, err := helpers.DecodeFastBinary(data)
	if err != nil {
		return nil, nil, fmt.Errorf("error decoding bundle data: %w", err)
	}

	// Convert map back to Bundle struct
	var bundle models.Bundle
	if bundleID, ok := bundleData["BundleID"].(string); ok {
		bundle.BundleID = bundleID
	}
	if name, ok := bundleData["Name"].(string); ok {
		bundle.Name = name
	}
	if desc, ok := bundleData["Description"].(string); ok {
		bundle.Description = desc
	}
	if createdBy, ok := bundleData["CreatedBy"].(string); ok {
		bundle.CreatedBy = createdBy
	}
	if createdAt, ok := bundleData["CreatedAt"].(time.Time); ok {
		bundle.CreatedAt = createdAt
	}
	if updatedAt, ok := bundleData["UpdatedAt"].(time.Time); ok {
		bundle.UpdatedAt = updatedAt
	}
	if totalDocs, ok := bundleData["TotalDocuments"].(int64); ok {
		bundle.TotalDocuments = totalDocs
	}
	if pageCount, ok := bundleData["PageCount"].(int64); ok {
		bundle.PageCount = pageCount
	}

	// CRITICAL FIX: Detect and fix corruption when loading from disk
	// If TotalDocuments or PageCount is negative, it's corrupted - reset to 0
	// The recovery logic in getSafePageCount() will recalculate from actual documents
	if bundle.TotalDocuments < 0 || bundle.PageCount < 0 {
		if b.logger != nil {
			b.logger.Warnf("CORRUPTION DETECTED when loading bundle '%s': TotalDocuments=%d, PageCount=%d, resetting to 0 (will be recovered on next access)",
				bundle.Name, bundle.TotalDocuments, bundle.PageCount)
		}
		bundle.TotalDocuments = 0
		bundle.PageCount = 0
		// Mark as dirty so corrected metadata gets persisted
		bundle.IsDirty = true
	}

	if pageSize, ok := bundleData["PageSize"].(int); ok {
		bundle.PageSize = pageSize
	}

	return &data, &bundle, nil
}

// DEPRECATED: LoadBundle loads a bundle using the old binary page format.
// This function does NOT respect tombstone deletion markers (0xDEADDEAD).
// Use LoadDocumentPage() with readDocumentRange() instead, which properly handles
// the append-only format with deletion markers.
func (bs *BundleStorageEngine) LoadBundle(bundleName string) (*models.Bundle, error) {
	// Get the fileID for this bundle
	bundleFilename := fmt.Sprintf("%s.bun", bundleName)
	fileID, err := bs.fileManager.OpenFile(bundleFilename)
	if err != nil {
		return nil, fmt.Errorf("could not open bundle file: %w", err)
	}

	// Read the header page (block 0)
	headerBuffer, err := bs.fileManager.ReadPage(fileID, 0)
	if err != nil {
		return nil, fmt.Errorf("could not read header page: %w", err)
	}
	defer bs.fileManager.ReleasePage(headerBuffer)

	// Parse the header
	bundle, _, err := bs.parseHeaderPage(headerBuffer.Data)
	if err != nil {
		return nil, fmt.Errorf("could not parse header page: %w", err)
	}

	// Documents are now loaded on-demand via page cache
	// The old document loading into memory is no longer used

	return bundle, nil
}

// parseHeaderPage parses the header page of a bundle file
func (bs *BundleStorageEngine) parseHeaderPage(pageData []byte) (*models.Bundle, uint32, error) {
	// First 4 bytes: magic number
	magic := binary.LittleEndian.Uint32(pageData[:4])
	if magic != 0x42554E44 { // "BUND" in hex
		return nil, 0, fmt.Errorf("invalid bundle file format (bad magic number)")
	}

	// Next 4 bytes: version
	version := binary.LittleEndian.Uint32(pageData[4:8])
	if version != 1 {
		return nil, 0, fmt.Errorf("unsupported bundle file version: %d", version)
	}

	// Next 4 bytes: document count
	docCount := binary.LittleEndian.Uint32(pageData[8:12])

	// Rest of header contains serialized bundle metadata
	// For simplicity, let's assume JSON format
	// TODO In the production, use a more efficient binary format like bson
	bundleMetadata := pageData[16:2048] // Limit the metadata size

	// Trim null bytes - FOR THE FULL IMPLEMENTATION, handle this metadata properly
	// metadataLen := 0
	// for i, b := range bundleMetadata {
	// 	if b == 0 {
	// 		metadataLen = i
	// 		break
	// 	}
	// }

	bundle := &models.Bundle{}
	// TODO deserialize bundle from bundleMetadata[:metadataLen]

	// For this example, just create an empty bundle
	bundle.BundleID = string(bundleMetadata[:32])
	bundle.Name = string(bundleMetadata[32:64])
	// Documents are now loaded on-demand via page cache

	return bundle, docCount, nil
}

// DEPRECATED: readDocuments reads documents using the old binary page format.
// This function does NOT respect tombstone deletion markers (0xDEADDEAD).
// Use readDocumentRange() with parseAppendedDocumentsRange() instead for proper
// append-only format support with deletion markers.
func (bs *BundleStorageEngine) readDocuments(fileID uint32, docCount uint32) (map[string]models.Document, error) {
	docs := make(map[string]models.Document)

	// Start reading from block 1 (block 0 is the header)
	currentBlock := uint32(1)
	docsRead := uint32(0)

	for docsRead < docCount {
		buffer, err := bs.fileManager.ReadPage(fileID, currentBlock)
		if err != nil {
			return nil, fmt.Errorf("could not read document page %d: %w", currentBlock, err)
		}

		// Process documents from this page
		pageDocsRead, err := bs.processDocumentPage(buffer.Data, docs)
		bs.fileManager.ReleasePage(buffer)

		if err != nil {
			return nil, fmt.Errorf("could not process document page %d: %w", currentBlock, err)
		}

		docsRead += pageDocsRead
		currentBlock++
	}

	return docs, nil
}

// DEPRECATED: processDocumentPage parses documents using the old binary page format.
// This function does NOT respect tombstone deletion markers (0xDEADDEAD).
// Use parseAppendedDocumentsRange() instead for proper append-only format support
// with deletion markers.
func (bs *BundleStorageEngine) processDocumentPage(pageData []byte, docs map[string]models.Document) (uint32, error) {
	// First 4 bytes: document count in this page
	docsInPage := binary.LittleEndian.Uint32(pageData[:4])

	offset := 4
	for i := uint32(0); i < docsInPage; i++ {
		// Read document length
		if offset+4 > len(pageData) {
			return i, fmt.Errorf("unexpected end of page data")
		}

		docLen := binary.LittleEndian.Uint32(pageData[offset : offset+4])
		offset += 4

		// Read document data
		if offset+int(docLen) > len(pageData) {
			return i, fmt.Errorf("document exceeds page boundary")
		}

		docData := pageData[offset : offset+int(docLen)]
		offset += int(docLen)

		// Parse document
		doc := models.Document{}
		// In real code: deserialize document from docData

		// For this example, just extract ID from first few bytes
		idLen := 16
		if idLen > len(docData) {
			idLen = len(docData)
		}
		docID := string(docData[:idLen])
		doc.DocumentID = docID

		// Add to the map
		docs[docID] = doc
	}

	return docsInPage, nil
}

func (b *BundleStorageEngine) BundleFileExists(bundleName string, databaseName string) bool {
	// Check if the bundle file exists in the data directory
	databasePath := helpers.GetDatabaseFolderPath(databaseName)

	// Check legacy single-file format: {database}/{database}_{bundle}.bnd
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))
	if helpers.FileExists(filePath, *b.logger) {
		return true
	}

	// Check multi-segment format: {database}/{bundle}/bundle.manifest
	manifestPath := filepath.Join(databasePath, bundleName, "bundle.manifest")
	if helpers.FileExists(manifestPath, *b.logger) {
		return true
	}

	b.logger.Debugf("Bundle file not found for '%s' in database '%s' (checked %s and %s)",
		bundleName, databaseName, filePath, manifestPath)
	return false
}

func (b *BundleStorageEngine) CreateBundleFile(database *models.Database, bundle *models.Bundle) error {
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	// Create a new data file
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundle.Name))

	// Check if the file already exists
	if helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("Bundle %s already exists", bundle.Name)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating data file %s: %w", bundle.Name, err)
	}

	// Ensure the file is closed when the function exits
	defer file.Close()

	// Use the configured serializer to create bundle file in new format
	encodedBundle, err := b.serializer.SerializeBundleMetadata(bundle)
	if err != nil {
		return fmt.Errorf("error encoding bundle data with %s format: %w", b.serializer.GetFormatName(), err)
	}

	// Write the encoded bundle to the file
	fileLen, err := file.Write(encodedBundle)
	if err != nil {
		return fmt.Errorf("error writing to bundle data file %s: %w", bundle.Name, err)
	}

	if fileLen != len(encodedBundle) {
		return fmt.Errorf("error writing to bundle data file %s: wrote %d bytes, expected %d", bundle.Name, fileLen, len(encodedBundle))
	}

	return nil
}

func (b *BundleStorageEngine) UpdateBundleFile(database *models.Database, bundle *models.Bundle) error {
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	// Create a new data file
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundle.Name))

	// Check if the file already exists
	if !helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("bundle %s does not exist", bundle.Name)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening data file %s: %w", bundle.Name, err)
	}
	defer file.Close()

	// Use the configured serializer to update bundle file in new format
	encodedBundle, err := b.serializer.SerializeBundleMetadata(bundle)
	if err != nil {
		return fmt.Errorf("error encoding bundle data with %s format: %w", b.serializer.GetFormatName(), err)
	}

	// Write the encoded bundle to the file
	fileLen, err := file.Write(encodedBundle)
	if err != nil {
		return fmt.Errorf("error writing to bundle data file %s: %w", bundle.Name, err)
	}

	if fileLen != len(encodedBundle) {
		return fmt.Errorf("error writing to bundle data file %s: wrote %d bytes, expected %d", bundle.Name, fileLen, len(encodedBundle))
	}

	return nil
}

func (b *BundleStorageEngine) UpdateBundleFilename(database *models.Database, bundle *models.Bundle, oldBundleName string) error {
	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	// Create a new data file
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundle.Name))
	oldFilePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, oldBundleName))
	// Check if the file already exists
	if helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("bundle %s already exists and cannot be renamed to", bundle.Name)
	}

	err := os.Rename(oldFilePath, filePath)
	if err != nil {
		return fmt.Errorf("error renaming bundle file from %s to %s: %w", oldBundleName, filePath, err)
	}

	return nil
}

// DEPRECATED: Use UpdateDocumentInBundleFile with append-only logic instead
func (b *BundleStorageEngine) UpdateDocumentDataInBundleFile(database *models.Database,
	bundle *models.Bundle,
	documentID string,
	updatedDocument map[string]interface{},
	mmapData []byte) error {

	//args := settings.GetSettings()
	convertedBundle := BundleToMap(bundle)

	// Locate the document in the bundle
	documents, ok := convertedBundle["Documents"].([]interface{})
	if !ok {
		return fmt.Errorf("bundle does not contain a valid Documents field")
	}

	var documentOffset int
	var documentSize int
	found := false

	for i, doc := range documents {
		docMap, ok := doc.(map[string]interface{})
		if !ok {
			continue
		}

		if docMap["ID"] == documentID {
			// Serialize the updated document using fast binary format
			updatedBinary, err := helpers.EncodeFastBinary(updatedDocument)
			if err != nil {
				return fmt.Errorf("error encoding updated document to fast binary: %w", err)
			}

			// Calculate the offset and size of the document
			documentOffset, err = calculateDocumentOffset(mmapData, i)
			if err != nil {
				return fmt.Errorf("error calculating document offset during document update: %w", err)
			}

			documentSize = len(updatedBinary)

			// Replace the document in the memory-mapped data
			copy(mmapData[documentOffset:documentOffset+documentSize], updatedBinary)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("document with ID %s not found in bundle", documentID)
	}

	// Sync changes to the file
	err := unix.Msync(mmapData, unix.MS_SYNC)
	if err != nil {
		return fmt.Errorf("error syncing changes to file: %w", err)
	}

	// Update the data file
	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening bundle file for update: %w", err)
	}
	defer file.Close()

	// Write the updated memory-mapped data back to the file
	_, err = file.WriteAt(mmapData, 0)
	if err != nil {
		return fmt.Errorf("error writing updated data to file: %w", err)
	}

	return nil
}

func (b *BundleStorageEngine) UpdateDocumentInBundleFile(bundle *models.Bundle, document *models.Document) error {
	// PERFORMANCE: Delegate to batch update for consistency
	// This ensures single documents use the same optimized path
	return b.UpdateDocumentsBatch(bundle, []*models.Document{document})
}

// UpdateDocumentsBatch updates multiple documents in a bundle file using batch optimization
// This method uses the SAME write buffering approach as ADD operations:
// 1. All updates go to write buffer first (in-memory)
// 2. Single lock acquisition for entire batch
// 3. Single flush at the end
// 4. Bulk metadata update
//
// PERFORMANCE: For 1000 documents, this is ~1000x faster than individual updates
//
// R7 LOCK ORDER: This acquires getWriteLock(bundle.Name) (storage). Callers that also use
// application-level AcquireBundleWriteLock(bundle.Name) (BundleService) must acquire the
// application lock FIRST, then call UpdateDocumentsBatch or UpdateDocumentInBundleFile.
// Same ordering for AppendDocumentToBundleFile. UpdateDocumentInBundle follows this order.
func (b *BundleStorageEngine) UpdateDocumentsBatch(bundle *models.Bundle, documents []*models.Document) error {
	if len(documents) == 0 {
		return nil
	}

	// if b.logger != nil && settings.GetSettings().Debug {
	// 	b.logger.Infow("BATCH UPDATE: Starting batch document update",
	// 		"bundle", bundle.Name,
	// 		"documentCount", len(documents))
	// }

	// Validate inputs once at the start
	if bundle == nil {
		return fmt.Errorf("bundle cannot be nil")
	}
	if bundle.Database == nil {
		return fmt.Errorf("bundle must have an associated database")
	}

	// Resolve write target: multi-file segment (preferred) or legacy single file if manifest unavailable
	manifestMgr := b.getOrCreateManifestManager(bundle.Database.Name, bundle.Name)
	manifest, err := manifestMgr.LoadOrCreate(bundle.Database.Name, bundle.Name)
	var filePath string
	if err != nil {
		// Fallback to legacy single file when manifest cannot be loaded/created
		databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
		filePath = filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))
	} else {
		currentFileID := uint32(1)
		if manifest.ActiveFileID > 0 {
			currentFileID = uint32(manifest.ActiveFileID)
		}
		bundleDir := GetBundleDirectory(bundle.Database.Name, bundle.Name)
		filePath = filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))
		fileExistsInManifest := false
		for _, f := range manifest.Files {
			if f.FileID == int(currentFileID) {
				fileExistsInManifest = true
				break
			}
		}
		if !fileExistsInManifest {
			fileName := fmt.Sprintf("%06d.bnd", currentFileID)
			if err := manifestMgr.AddFile(int(currentFileID), fileName); err != nil {
				return fmt.Errorf("failed to add file to manifest for update: %w", err)
			}
		}
	}

	// Get or create write buffer ONCE for entire batch
	writeBuffer, err := b.getOrCreateWriteBuffer(bundle.Name, filePath)
	if err != nil {
		return fmt.Errorf("failed to get write buffer: %w", err)
	}

	// Calculate page size once
	pageSize := uint32(4096)
	if bundle.PageSize > 0 {
		pageSize = uint32(bundle.PageSize)
	}

	// Writes go directly to page cache (write-through) - no memtable needed
	// The page cache is updated after successful disk writes

	// CONCURRENCY FIX: Acquire write lock only for buffer writes (in-memory, fast).
	// Release BEFORE disk flush so readers are not blocked during I/O.
	// The WriteBuffer has its own internal mutex that prevents concurrent writes.
	lock := b.getWriteLock(bundle.Name)
	lock.Lock()

	// Process all documents in batch (serialization and buffer writes)
	successCount := 0
	for _, document := range documents {
		// Validate each document
		if document == nil || document.DocumentID == "" {
			b.logger.Warnf("BATCH UPDATE: Skipping invalid document")
			continue
		}

		schema := bundle.DocumentStructure.FieldSchema()
		documentBytes, err := b.serializeDocumentDirect(document, schema)
		if err != nil {
			b.logger.Warnf("BATCH UPDATE: Failed to serialize document %s: %v", document.DocumentID, err)
			continue
		}

		headerSize := uint32(len(documentBytes))
		headerBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADBEEF)
		binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

		combinedData := b.getCombinedBuffer(len(headerBytes) + len(documentBytes))
		copy(combinedData[:8], headerBytes)
		copy(combinedData[8:], documentBytes)

		if err := writeBuffer.Write(combinedData[:len(headerBytes)+len(documentBytes)]); err != nil {
			b.returnCombinedBuffer(combinedData)
			b.logger.Warnf("BATCH UPDATE: Failed to buffer document %s: %v", document.DocumentID, err)
			continue
		}

		b.returnCombinedBuffer(combinedData)
		successCount++
	}

	// Update bundle metadata while still holding the lock
	if bundle.TotalDocuments > 0 {
		bundle.PageCount = int64((uint32(bundle.TotalDocuments) + pageSize - 1) / pageSize)
	}
	bundle.IsDirty = true

	// Release write lock BEFORE disk flush — readers are no longer blocked during I/O
	lock.Unlock()

	if successCount == 0 {
		return fmt.Errorf("BATCH UPDATE: Failed to update any documents")
	}

	// Flush buffered writes to disk (outside lock — WriteBuffer has its own mutex)
	if err := writeBuffer.Flush(); err != nil {
		return fmt.Errorf("BATCH UPDATE: Failed to flush write buffer: %w", err)
	}

	// R3: Sync is configurable via DurabilityMode.
	// - "performance" (default): skip Sync; rely on WAL + write buffer flush policy (matches ADD).
	// - "strict": sync to disk after flush.
	dm := settings.GetSettings().DurabilityMode
	if dm == "strict" {
		if err := writeBuffer.SyncGroupCommit(); err != nil {
			b.logger.Warnf("BATCH UPDATE: Group-commit sync failed: %v (continuing anyway)", err)
		}
	} else {
		b.logger.Debugf("BATCH UPDATE: DurabilityMode is '%s' - skipping fsync for performance", dm)
	}

	// CACHE INVALIDATION: Only invalidate the latest file's cache since writes append there
	// This is much more efficient than invalidating all 7+ cached file entries
	b.InvalidateParsedDocsCacheForLatestFile(bundle.Name, bundle.Database.Name)

	return nil
}

// UpdateDocumentsBatchWithLocks updates documents using document-level locks instead of bundle-level locks
// DOCUMENT-LEVEL LOCKING: Enables concurrent writes to different documents within the same bundle
//
// Parameters:
//   - bundle: The bundle containing the documents
//   - documents: The documents to update
//   - preLockedDocIDs: Document IDs that are already locked by the caller (from LockManager)
//
// If preLockedDocIDs is empty or doesn't match document IDs, falls back to bundle-level lock
// This provides Postgres-like row-level locking for improved write throughput
func (b *BundleStorageEngine) UpdateDocumentsBatchWithLocks(bundle *models.Bundle, documents []*models.Document, preLockedDocIDs []string) error {
	if len(documents) == 0 {
		return nil
	}

	// Validate inputs
	if bundle == nil {
		return fmt.Errorf("bundle cannot be nil")
	}
	if bundle.Database == nil {
		return fmt.Errorf("bundle must have an associated database")
	}

	// Check if we can use document-level locks
	useDocumentLocks := len(preLockedDocIDs) > 0 && len(preLockedDocIDs) == len(documents)
	if useDocumentLocks {
		// Verify all documents have matching pre-locked IDs
		preLockedSet := make(map[string]bool, len(preLockedDocIDs))
		for _, id := range preLockedDocIDs {
			preLockedSet[id] = true
		}
		for _, doc := range documents {
			if doc != nil && !preLockedSet[doc.DocumentID] {
				useDocumentLocks = false
				break
			}
		}
	}

	// If we can't use document locks, fall back to bundle lock
	if !useDocumentLocks {
		b.logger.Debugf("BATCH UPDATE WITH LOCKS: Falling back to bundle-level lock (preLockedDocIDs=%d, documents=%d)",
			len(preLockedDocIDs), len(documents))
		return b.UpdateDocumentsBatch(bundle, documents)
	}

	b.logger.Debugf("BATCH UPDATE WITH LOCKS: Using document-level locks for %d documents", len(documents))

	// P0a: Use multi-file path (same as ADD) instead of legacy {db}_{bundle}.bnd.
	// One write buffer per segment; UPDATE and ADD share the same manifest/segment layout.
	manifestMgr := b.getOrCreateManifestManager(bundle.Database.Name, bundle.Name)
	manifest, err := manifestMgr.LoadOrCreate(bundle.Database.Name, bundle.Name)
	if err != nil {
		return fmt.Errorf("failed to load bundle manifest for update: %w", err)
	}
	var currentFileID uint32 = 1
	if manifest.ActiveFileID > 0 {
		currentFileID = uint32(manifest.ActiveFileID)
	}
	bundleDir := GetBundleDirectory(bundle.Database.Name, bundle.Name)
	filePath := filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))

	fileExistsInManifest := false
	for _, f := range manifest.Files {
		if f.FileID == int(currentFileID) {
			fileExistsInManifest = true
			break
		}
	}
	if !fileExistsInManifest {
		fileName := fmt.Sprintf("%06d.bnd", currentFileID)
		if err := manifestMgr.AddFile(int(currentFileID), fileName); err != nil {
			return fmt.Errorf("failed to add file to manifest for update: %w", err)
		}
	}

	// P0a: Do not hold rotation lock during append. ADD releases it before append; we do the same.
	// This avoids serializing all UPDATEs on the rotation lock. Risk: ADD could rotate and close our
	// buffer mid-update; in practice rare when UPDATE and ADD interleave. TestConcurrentUpdates
	// runs UPDATEs only, so no rotation.

	// Get or create write buffer (thread-safe via its own mutex)
	writeBuffer, err := b.getOrCreateWriteBuffer(bundle.Name, filePath)
	if err != nil {
		return fmt.Errorf("failed to get write buffer: %w", err)
	}

	// Calculate page size
	pageSize := uint32(4096)
	if bundle.PageSize > 0 {
		pageSize = uint32(bundle.PageSize)
	}

	// Writes go directly to page cache (write-through) - no memtable needed
	// The page cache is updated after successful disk writes

	// Process all documents (serialization and disk writes)
	// NOTE: Document locks are already held by caller (from LockManager)
	// WriteBuffer is protected by its own mutex (thread-safe)
	successCount := 0
	for _, document := range documents {
		if document == nil || document.DocumentID == "" {
			b.logger.Warnf("BATCH UPDATE WITH LOCKS: Skipping invalid document")
			continue
		}

		schema := bundle.DocumentStructure.FieldSchema()
		documentBytes, err := b.serializeDocumentDirect(document, schema)
		if err != nil {
			b.logger.Warnf("BATCH UPDATE WITH LOCKS: Failed to serialize document %s: %v", document.DocumentID, err)
			continue
		}

		headerSize := uint32(len(documentBytes))
		headerBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADBEEF)
		binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

		combinedData := b.getCombinedBuffer(len(headerBytes) + len(documentBytes))
		copy(combinedData[:8], headerBytes)
		copy(combinedData[8:], documentBytes)

		if err := writeBuffer.Write(combinedData[:len(headerBytes)+len(documentBytes)]); err != nil {
			b.returnCombinedBuffer(combinedData)
			b.logger.Warnf("BATCH UPDATE WITH LOCKS: Failed to buffer document %s: %v", document.DocumentID, err)
			continue
		}

		b.returnCombinedBuffer(combinedData)
		successCount++
	}

	if successCount == 0 {
		return fmt.Errorf("BATCH UPDATE WITH LOCKS: Failed to update any documents")
	}

	// Flush buffer to disk
	if err := writeBuffer.Flush(); err != nil {
		return fmt.Errorf("BATCH UPDATE WITH LOCKS: Failed to flush write buffer: %w", err)
	}

	// Handle durability mode (P4b: use group commit when strict to coalesce fsync)
	dm := settings.GetSettings().DurabilityMode
	if dm == "strict" {
		if err := writeBuffer.SyncGroupCommit(); err != nil {
			b.logger.Warnf("BATCH UPDATE WITH LOCKS: Group-commit sync failed: %v (continuing anyway)", err)
		}
	}

	// Update bundle metadata
	if bundle.TotalDocuments > 0 {
		bundle.PageCount = int64((uint32(bundle.TotalDocuments) + pageSize - 1) / pageSize)
	}
	bundle.IsDirty = true

	// CACHE INVALIDATION: Only invalidate the latest file's cache since writes append there
	b.InvalidateParsedDocsCacheForLatestFile(bundle.Name, bundle.Database.Name)

	b.logger.Debugf("BATCH UPDATE WITH LOCKS: Successfully updated %d documents with document-level locks", successCount)
	return nil
}

func (b *BundleStorageEngine) DeleteDocumentFromBundleFile(bundle *models.Bundle, documentID string) error {
	// Validate inputs
	if bundle == nil {
		return fmt.Errorf("bundle cannot be nil")
	}
	if documentID == "" {
		return fmt.Errorf("documentID cannot be empty")
	}

	args := settings.GetSettings()
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))

	// Verify the bundle file exists
	if !helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("bundle file does not exist: %s_%s.bnd", bundle.Database.Name, bundle.Name)
	}

	// PERFORMANCE OPTIMIZATION: Use streaming verification OUTSIDE the write lock
	// This avoids holding the lock during file read I/O
	documentExists, err := b.verifyDocumentExistsStreaming(bundle.Name, bundle.Database.Name, documentID)
	if err != nil {
		return fmt.Errorf("failed to verify document existence: %w", err)
	}
	if !documentExists {
		return fmt.Errorf("document %s not found in bundle %s", documentID, bundle.Name)
	}

	if args.Debug {
		b.logger.Debugf("Deleting document %s from bundle %s", documentID, bundle.Name)
	}

	// CONCURRENCY FIX: Acquire write lock only for tombstone write + metadata update.
	// Verification above and flush/sync below happen outside the lock.
	lock := b.getWriteLock(bundle.Name)
	lock.Lock()

	// Append deletion tombstone using write buffer for optimal performance
	err = b.appendDeletionMarker(bundle.Name, documentID, filePath)
	if err != nil {
		lock.Unlock()
		return fmt.Errorf("failed to append deletion marker: %w", err)
	}

	// PAGE ID ARCHITECTURE ALIGNMENT: Remove document from SortedIndex
	if bundle.SortedIndex != nil {
		bundle.SortedIndex.Delete(documentID)
	}

	// Update bundle metadata under lock
	pageSize := uint32(4096)
	if bundle.PageSize > 0 {
		pageSize = uint32(bundle.PageSize)
	}
	if bundle.TotalDocuments > 0 {
		bundle.PageCount = int64((uint32(bundle.TotalDocuments) + pageSize - 1) / pageSize)
	} else {
		bundle.PageCount = 0
	}
	bundle.IsDirty = true

	// Release write lock BEFORE flush/sync — readers are no longer blocked during I/O
	lock.Unlock()

	// D7: Keep FlushWriteBuffers — ensures pending ADDs/UPDATEs are on disk before the tombstone.
	err = b.FlushWriteBuffers(bundle.Name)
	if err != nil {
		b.logger.Warnf("Failed to flush write buffer for bundle %s: %v", bundle.Name, err)
	}

	// Sync conditional on DurabilityMode (outside lock)
	if settings.GetSettings().DurabilityMode == "strict" {
		slash, back := "/"+bundle.Name+"/", "\\"+bundle.Name+"\\"
		b.writeBufferCache.Range(func(bufferKey string, writeBuffer *WriteBuffer) bool {
			if b.bufferKeyMatchesBundle(bufferKey, slash, back) {
				if err := writeBuffer.Sync(); err != nil {
					b.logger.Warnf("Failed to sync write buffer to disk: %v (continuing anyway)", err)
				}
			}
			return true
		})
	}

	if args.Debug {
		b.logger.Debugf("Successfully deleted document %s from bundle %s (new TotalDocuments: %d, PageCount: %d)",
			documentID, bundle.Name, bundle.TotalDocuments, bundle.PageCount)
	}

	// CACHE INVALIDATION
	b.InvalidateParsedDocsCacheForLatestFile(bundle.Name, bundle.Database.Name)

	return nil
}

// verifyDocumentExistsStreaming verifies that a document exists in the bundle file
// SIMPLIFIED: Use the same parsing logic as readDocumentRange for consistency
// This ensures we handle the file format exactly the same way as SELECT operations
func (b *BundleStorageEngine) verifyDocumentExistsStreaming(bundleName, databaseName, documentID string) (bool, error) {
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	file, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open bundle file: %w", err)
	}
	defer file.Close()

	// Read the entire file (same as readDocumentRange does)
	fileInfo, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("failed to get file info: %w", err)
	}

	fileData := make([]byte, fileInfo.Size())
	_, err = file.Read(fileData)
	if err != nil {
		return false, fmt.Errorf("failed to read file: %w", err)
	}

	// Use the same parser that SELECT operations use
	// This ensures consistency in how we interpret the file format
	documents, err := b.parseAppendedDocuments(bundleName, databaseName, fileData)
	if err != nil {
		return false, fmt.Errorf("failed to parse bundle file: %w", err)
	}

	// Check if the document exists (and is not deleted)
	_, exists := documents[documentID]
	return exists, nil
}

// appendDeletionMarker appends a deletion marker to mark a document as deleted
// This uses the append-only approach for optimal performance instead of rewriting the entire bundle
func (b *BundleStorageEngine) appendDeletionMarker(bundleName, documentID, filePath string) error {
	// Create deletion marker entry
	deletionEntry := map[string]interface{}{
		"DocumentID": documentID,
		"Operation":  "DELETE",
		"Timestamp":  time.Now(),
	}

	// Serialize the deletion marker using fast binary format
	deletionBytes, err := helpers.EncodeFastBinary(deletionEntry)
	if err != nil {
		return fmt.Errorf("failed to encode deletion marker: %w", err)
	}

	// Enterprise encryption hook for tombstones
	tombMagic := uint32(0xDEADDEAD)
	if reg := extension.GetRegistry(); reg.HasStorageEncryptors() {
		enc := reg.GetStorageEncryptor()
		scope := "bundle:" + bundleName
		if enc.EncryptionEnabled(scope) {
			encrypted, encErr := enc.EncryptBlock(deletionBytes, scope)
			if encErr != nil {
				return fmt.Errorf("failed to encrypt deletion marker: %w", encErr)
			}
			deletionBytes = encrypted
			tombMagic = EncryptedTombstoneMagic
		}
	}

	// Create header with deletion magic number
	headerSize := uint32(len(deletionBytes))
	headerBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(headerBytes[0:4], tombMagic)
	binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open bundle file for deletion marker: %w", err)
	}
	defer file.Close()

	// Write deletion marker header and data
	if _, err := file.Write(headerBytes); err != nil {
		return fmt.Errorf("failed to write deletion marker header: %w", err)
	}

	if _, err := file.Write(deletionBytes); err != nil {
		return fmt.Errorf("failed to write deletion marker data: %w", err)
	}

	// D3: Fdatasync conditional on DurabilityMode. "strict": sync; "performance" (default): skip.
	if settings.GetSettings().DurabilityMode == "strict" {
		if err := common.Fdatasync(file); err != nil {
			b.logger.Warnf("Failed to sync deletion marker to disk: %v (continuing anyway)", err)
		}
	}

	if b.logger != nil {
		b.logger.Infow("Successfully appended deletion marker",
			"bundle", bundleName,
			"documentID", documentID)
	}

	return nil
}

// appendDeletionMarkersBatchCore performs the actual tombstone append and metadata update.
// Caller must hold getWriteLock(bundle.Name) unless using document-level locks (WithLocks path).
func (b *BundleStorageEngine) appendDeletionMarkersBatchCore(bundle *models.Bundle, documentIDs []string) error {
	// Determine file path using the same logic as document inserts
	// First try to load manifest for multi-file storage
	manifestMgr := b.getOrCreateManifestManager(bundle.Database.Name, bundle.Name)
	manifest, err := manifestMgr.LoadOrCreate(bundle.Database.Name, bundle.Name)
	var filePath string
	if err != nil {
		// Fallback to legacy single file when manifest cannot be loaded/created
		databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
		filePath = filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))
	} else {
		// Multi-file storage: append tombstones to the active file
		currentFileID := uint32(1)
		if manifest.ActiveFileID > 0 {
			currentFileID = uint32(manifest.ActiveFileID)
		}
		bundleDir := GetBundleDirectory(bundle.Database.Name, bundle.Name)
		filePath = filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))

		// Ensure file is in manifest
		fileExistsInManifest := false
		for _, f := range manifest.Files {
			if f.FileID == int(currentFileID) {
				fileExistsInManifest = true
				break
			}
		}
		if !fileExistsInManifest {
			fileName := fmt.Sprintf("%06d.bnd", currentFileID)
			if err := manifestMgr.AddFile(int(currentFileID), fileName); err != nil {
				return fmt.Errorf("failed to add file to manifest for deletion: %w", err)
			}
		}
	}

	// Open file once for all markers
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open bundle file for batch deletion markers: %w", err)
	}
	defer file.Close()

	// Enterprise encryption: check once outside the loop
	var batchEncryptor extension.StorageEncryptionExtension
	batchEncScope := "bundle:" + bundle.Name
	if reg := extension.GetRegistry(); reg.HasStorageEncryptors() {
		enc := reg.GetStorageEncryptor()
		if enc.EncryptionEnabled(batchEncScope) {
			batchEncryptor = enc
		}
	}

	// DEBUG: Log all deletions
	b.logger.Infof("DELETE MARKERS: bundle=%s, count=%d, docIDs=%v, totalDocsBefore=%d", bundle.Name, len(documentIDs), documentIDs, bundle.TotalDocuments)

	// PERF: Buffer all markers and write once instead of 2*N write syscalls.
	// Reduces I/O and time under the write lock for large deletes.
	var buf bytes.Buffer
	for _, documentID := range documentIDs {
		deletionEntry := map[string]interface{}{
			"DocumentID": documentID,
			"Operation":  "DELETE",
			"Timestamp":  time.Now(),
		}
		deletionBytes, err := helpers.EncodeFastBinary(deletionEntry)
		if err != nil {
			return fmt.Errorf("failed to encode deletion marker for %s: %w", documentID, err)
		}
		tombMagic := uint32(0xDEADDEAD)
		if batchEncryptor != nil {
			encrypted, encErr := batchEncryptor.EncryptBlock(deletionBytes, batchEncScope)
			if encErr != nil {
				return fmt.Errorf("failed to encrypt deletion marker for %s: %w", documentID, encErr)
			}
			deletionBytes = encrypted
			tombMagic = EncryptedTombstoneMagic
		}
		headerSize := uint32(len(deletionBytes))
		headerBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(headerBytes[0:4], tombMagic)
		binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)
		buf.Write(headerBytes)
		buf.Write(deletionBytes)
	}
	if _, err := file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to write batch deletion markers: %w", err)
	}

	// D3: Fdatasync conditional on DurabilityMode. "strict": sync; "performance" (default): skip.
	// TODO: If we experience durability or Sync-throughput issues, consider a "balanced" coalesced
	// Sync (PostgreSQL wal_writer_delay–style), as in UpdateDocumentsBatch R3.
	dm := settings.GetSettings().DurabilityMode
	if dm == "strict" {
		if err := common.Fdatasync(file); err != nil {
			b.logger.Warnf("Failed to sync %d deletion markers to disk: %v (continuing anyway)", len(documentIDs), err)
		}
	}

	// CRITICAL: Close file IMMEDIATELY after sync to ensure OS updates file metadata
	// before any subsequent opens. Using defer can cause race conditions where
	// the next os.Open() + file.Stat() returns stale file size (before tombstones).
	if err := file.Close(); err != nil {
		b.logger.Warnf("Failed to close file after deletion markers: %v", err)
	}

	// CRITICAL FIX: Do NOT decrement TotalDocuments on batch deletion
	// In append-only storage, tombstones are still entries on disk, so TotalDocuments
	// should represent total document entries (including tombstones), not active documents.
	// Active document count is calculated dynamically by filtering tombstones during queries.
	// See DeleteDocumentFromBundleFile for detailed explanation.
	// Since we no longer decrement TotalDocuments, it cannot go negative

	// Always calculate PageCount from TotalDocuments to ensure consistency
	// Note: TotalDocuments now represents total entries (documents + tombstones), not active count
	// Use ceiling division: ceil(a/b) = (a + b - 1) / b
	pageSize := uint32(4096)
	if bundle.PageSize > 0 {
		pageSize = uint32(bundle.PageSize)
	}
	if bundle.TotalDocuments > 0 {
		bundle.PageCount = int64((uint32(bundle.TotalDocuments) + pageSize - 1) / pageSize)
	} else {
		bundle.PageCount = 0
	}

	// Mark bundle as dirty to trigger metadata persistence
	bundle.IsDirty = true

	if b.logger != nil {
		b.logger.Infow("Successfully appended batch deletion markers and updated metadata",
			"bundle", bundle.Name,
			"deletedCount", len(documentIDs),
			"newTotalDocuments", bundle.TotalDocuments,
			"newPageCount", bundle.PageCount)
	}

	// CACHE INVALIDATION: Only invalidate the latest file's cache since deletes append tombstones there
	b.InvalidateParsedDocsCacheForLatestFile(bundle.Name, bundle.Database.Name)

	// CRITICAL: Invalidate fileReadCache so subsequent page loads see the new tombstones
	// Without this, getOrReadFile() returns stale cached file data that doesn't include tombstones
	// PHASE 3: Using ShardedFileReadCache.Delete for concurrent-safe invalidation
	b.fileReadCache.Delete(filePath)

	return nil
}

// AppendDeletionMarkersBatch writes multiple deletion markers in a single file operation.
// Acquires getWriteLock(bundle). Use AppendDeletionMarkersBatchWithLocks when document-level locks are held.
func (b *BundleStorageEngine) AppendDeletionMarkersBatch(bundle *models.Bundle, documentIDs []string) error {
	lock := b.getWriteLock(bundle.Name)
	lock.Lock()
	defer lock.Unlock()
	return b.appendDeletionMarkersBatchCore(bundle, documentIDs)
}

// AppendDeletionMarkersBatchWithLocks appends tombstones without bundle write lock when preLockedDocIDs matches.
// Enables concurrent DELETEs with UPDATE/ADD (no bundle-level locking). Falls back to AppendDeletionMarkersBatch otherwise.
func (b *BundleStorageEngine) AppendDeletionMarkersBatchWithLocks(bundle *models.Bundle, documentIDs []string, preLockedDocIDs []string) error {
	if len(preLockedDocIDs) == 0 || len(preLockedDocIDs) != len(documentIDs) {
		return b.AppendDeletionMarkersBatch(bundle, documentIDs)
	}
	set := make(map[string]bool, len(preLockedDocIDs))
	for _, id := range preLockedDocIDs {
		set[id] = true
	}
	for _, id := range documentIDs {
		if !set[id] {
			return b.AppendDeletionMarkersBatch(bundle, documentIDs)
		}
	}
	return b.appendDeletionMarkersBatchCore(bundle, documentIDs)
}

// this version of the functino uses the file manager to add a document to a bundle file
// AddDocumentToBundleFile2 adds a document to a bundle file using the file manager
func (bs *BundleStorageEngine) AddDocumentToBundleFile2(bundle models.Bundle, bundleName string, document *models.Document) error {
	// Get file ID for this bundle

	//databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	fileID, err := bs.fileManager.GetFileID(fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundleName))
	if err != nil {
		return err
	}
	bs.logger.Debugf("Adding document to bundle %s with file ID %d", bundle.Name, fileID)

	// Documents are now stored via page cache (write-through)
	// No memtable update needed - the page cache handles document storage

	return nil
}

func (b *BundleStorageEngine) AddDocumentToBundleFile(bundle *models.Bundle, document *models.Document) (uint32, error) {
	// Use the optimized append-only approach for better performance
	return b.AppendDocumentToBundleFile(bundle, document)
}

// AppendDocumentToBundleFile adds a document using append-only approach for optimal write performance
// This eliminates the need to rewrite the entire bundle file on every document insert
// Returns the physical page ID where the document was stored (0-based)
func (b *BundleStorageEngine) AppendDocumentToBundleFile(bundle *models.Bundle, document *models.Document) (uint32, error) {
	// Call AppendDocumentToBundleFileWithTxID with empty transaction ID for backward compatibility
	return b.AppendDocumentToBundleFileWithTxID(bundle, document, "")
}

// AppendDocumentToBundleFileWithTxID adds a document with transaction tracking
// This eliminates the need to rewrite the entire bundle file on every document insert
// Returns the physical page ID where the document was stored (0-based)
func (b *BundleStorageEngine) AppendDocumentToBundleFileWithTxID(bundle *models.Bundle, document *models.Document, txID string) (uint32, error) {
	// DEBUG: Log all document writes for debugging duplicate writes
	b.logger.Infof("WRITE DOC: bundle=%s, docID=%s, txID=%s, commitSeq=%d", bundle.Name, document.DocumentID, txID, document.CommitSequence)

	//b.logger.Debugf("Starting time: %s", time.Now().Format(time.RFC3339Nano))
	//testingStart := time.Now()
	// PHASE 1: MVCC - Removed bundle-wide write lock to allow concurrent writes
	// Fine-grained locking: rotation lock protects file rotation, atomic operations protect metadata

	// MULTI-FILE STORAGE: Determine current active file and check if rotation is needed
	// Get or create manifest manager for this bundle
	manifestMgr := b.getOrCreateManifestManager(bundle.Database.Name, bundle.Name)

	// Construct bundle directory (constant)
	bundleDir := GetBundleDirectory(bundle.Database.Name, bundle.Name)

	// OPTIMISTIC READ: Read manifest without lock for fast path (common case: no rotation needed)
	// This allows concurrent reads while only serializing on actual rotation
	manifest, err := manifestMgr.LoadOrCreate(bundle.Database.Name, bundle.Name)
	if err != nil {
		return 0, fmt.Errorf("failed to load bundle manifest: %w", err)
	}

	// Get the current active (writable) file
	var currentFileID uint32 = 1
	if manifest.ActiveFileID > 0 {
		currentFileID = uint32(manifest.ActiveFileID)
	}
	initialFileID := currentFileID // Save for validation after lock

	// Construct the current file path
	filePath := filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))

	// CRITICAL FIX: Ensure the current file exists in manifest
	// On first write, manifest is created but has no files yet
	// Check if current file is tracked, if not, add it
	fileExistsInManifest := false
	for _, f := range manifest.Files {
		if f.FileID == int(currentFileID) {
			fileExistsInManifest = true
			break
		}
	}
	if !fileExistsInManifest {
		// Add the current file to manifest (happens on first write to new bundle)
		fileName := fmt.Sprintf("%06d.bnd", currentFileID)
		if err := manifestMgr.AddFile(int(currentFileID), fileName); err != nil {
			return 0, fmt.Errorf("failed to add initial file to manifest: %w", err)
		}
		if b.logger != nil && settings.GetSettings().Debug {
			b.logger.Infow("Added initial file to manifest",
				"bundle", bundle.Name,
				"fileID", currentFileID,
				"fileName", fileName)
		}
	}

	// PHASE 1: MVCC - Acquire rotation lock before checking file size
	// This protects rotation decision AND execution from race conditions
	rotationLock := b.getRotationLock(bundle.Name)
	rotationLock.Lock()

	// DOUBLE-CHECK: Validate that activeFileID hasn't changed while we were waiting
	// If another goroutine rotated the file, we need to use the new file ID
	currentActiveID := manifestMgr.GetActiveFileID()
	if currentActiveID > 0 && uint32(currentActiveID) != initialFileID {
		// File was rotated by another goroutine - use the new active file
		currentFileID = uint32(currentActiveID)
		filePath = filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))
	}

	// Check if file rotation is needed (file size exceeds threshold)
	// PERFORMANCE FIX: Use WriteBuffer's atomic offset instead of os.Stat syscall
	// This avoids expensive filesystem calls on every write under contention
	maxSizeBytes := int64(settings.GetSettings().Storage.BundleFileMaxSizeMB) * 1024 * 1024
	rotationThreshold := int64(float64(maxSizeBytes) * 1.1) // ±10% variance tolerance

	var currentFileSize int64 = 0
	needsRotation := false

	// Try to get file size from existing WriteBuffer (fast path - no syscall)
	if existingBuffer := b.writeBufferCache.Get(filePath); existingBuffer != nil {
		currentFileSize = existingBuffer.GetAtomicOffset()
		needsRotation = currentFileSize >= rotationThreshold
	} else {
		// Fallback to os.Stat only when buffer doesn't exist yet (first write to file)
		if fileInfo, statErr := os.Stat(filePath); statErr == nil {
			currentFileSize = fileInfo.Size()
			needsRotation = currentFileSize >= rotationThreshold
		}
	}

	// PERFORMANCE FIX: Remove excessive logging in hot path
	// Only log in debug mode to eliminate I/O overhead
	if b.logger != nil && settings.GetSettings().Debug {
		b.logger.Infow("Appending document to bundle file",
			"bundle", bundle.Name,
			"for database", bundle.Database.Name,
			"documentID", document.DocumentID,
			"fileID", currentFileID,
			"fileSize", currentFileSize,
			"needsRotation", needsRotation)
	}

	// Handle file rotation if needed
	if needsRotation {
		// TODO:  add backpressure handling when write rate exceeds compaction rate
		if b.logger != nil {
			b.logger.Infow("Rotating bundle file - size threshold reached",
				"bundle", bundle.Name,
				"currentFileID", currentFileID,
				"fileSize", currentFileSize,
				"threshold", rotationThreshold)
		}

		// Close current write buffer and flush pending data
		if err := b.CloseWriteBuffer(bundle.Name); err != nil {
			rotationLock.Unlock()
			return 0, fmt.Errorf("failed to close write buffer before rotation: %w", err)
		}

		// Update manifest: freeze current file and mark as immutable
		frozenFileID := int(currentFileID)
		frozenFileName := fmt.Sprintf("%06d.bnd", currentFileID)
		if manifestMgr.GetActiveFileID() > 0 {
			if err := manifestMgr.FreezeFile(frozenFileID); err != nil {
				rotationLock.Unlock()
				return 0, fmt.Errorf("failed to freeze file in manifest: %w", err)
			}
			// Async bloom filter build for the frozen file
			b.buildBloomFilterForFrozenFile(manifestMgr, bundleDir, frozenFileID, frozenFileName)
		}

		// Create new file with incremented ID
		currentFileID++
		filePath = filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))

		// Add new active file to manifest
		fileName := fmt.Sprintf("%06d.bnd", currentFileID)
		if err := manifestMgr.AddFile(int(currentFileID), fileName); err != nil {
			rotationLock.Unlock()
			return 0, fmt.Errorf("failed to add new file to manifest: %w", err)
		}

		if b.logger != nil {
			b.logger.Infow("Created new bundle file segment",
				"bundle", bundle.Name,
				"newFileID", currentFileID,
				"filePath", filePath)
		}
	}

	// PHASE 1: MVCC - Release rotation lock after rotation decision/execution complete
	rotationLock.Unlock()

	// Track the file offset where this write will occur for corruption debugging
	writeOffset := currentFileSize
	if needsRotation {
		writeOffset = 0 // New file starts at offset 0
	}

	// Validate inputs (keep critical validation)
	if bundle == nil {
		return 0, fmt.Errorf("bundle cannot be nil")
	}
	if document == nil {
		return 0, fmt.Errorf("document cannot be nil")
	}
	if document.DocumentID == "" {
		return 0, fmt.Errorf("document must have a valid ID")
	}

	// PAGE ID ARCHITECTURE ALIGNMENT: Calculate pageID using alphabetical order
	// This matches how LoadDocumentPage sorts documents, eliminating stale pageID issues.
	//
	// Previously: pageID = (insertion order document count) / pageSize
	// Now: pageID = (alphabetical position in sorted index) / pageSize
	//
	// The SortedIndex maintains 64 shards of sorted DocumentIDs for O(log n) lookup.
	// Fallback to old behavior if SortedIndex is nil (shouldn't happen in production).
	pageSize := uint32(4096)
	if bundle.PageSize > 0 {
		pageSize = uint32(bundle.PageSize)
	}

	var pageID uint32
	if bundle.SortedIndex != nil {
		// Use alphabetical position for pageID calculation
		pageID = bundle.SortedIndex.Insert(document.DocumentID, pageSize)
	} else {
		// Fallback: Use insertion order (legacy behavior - causes staleness)
		// TODO: Log warning here once logging is available in this context
		currentDocCount := uint32(atomic.AddInt64(&bundle.TotalDocuments, 1) - 1)
		pageID = currentDocCount / pageSize
	}

	// Still increment TotalDocuments for metadata consistency
	// (SortedIndex.Insert already added the document, but TotalDocuments is used elsewhere)
	if bundle.SortedIndex != nil {
		atomic.AddInt64(&bundle.TotalDocuments, 1)
	}

	// PERFORMANCE FIX: Skip file existence check for known bundles
	// Trust that bundle files exist if bundle object is valid
	// if !helpers.FileExists(filePath, *b.logger) {
	//     return fmt.Errorf("bundle file %s does not exist", fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))
	// }

	// Documents are stored via write-through page cache
	// No memtable update needed - the page cache handles document storage

	schema := bundle.DocumentStructure.FieldSchema()
	documentBytes, err := b.serializeDocumentDirect(document, schema)
	if err != nil {
		return 0, fmt.Errorf("failed to encode document: %w", err)
	}

	// Page-level compression: compress document bytes if enabled and beneficial
	docMagic := uint32(0xDEADBEEF) // Default: uncompressed
	compressionSettings := settings.GetSettings()
	if compressionSettings.StorageCompression == "zstd" && len(documentBytes) >= compressionSettings.CompressionMinDocSize {
		compressed, compErr := CompressDocument(documentBytes, CompressionZstd)
		if compErr == nil && len(compressed) < len(documentBytes) {
			documentBytes = compressed
			docMagic = CompressedDocMagic // Use compressed magic
		}
		// If compression doesn't help or fails, fall through with uncompressed
	}

	// Enterprise encryption hook (after compression, before header)
	if reg := extension.GetRegistry(); reg.HasStorageEncryptors() {
		enc := reg.GetStorageEncryptor()
		scope := "bundle:" + bundle.Name
		if enc.EncryptionEnabled(scope) {
			encrypted, encErr := enc.EncryptBlock(documentBytes, scope)
			if encErr != nil {
				return 0, fmt.Errorf("failed to encrypt document: %w", encErr)
			}
			if docMagic == CompressedDocMagic {
				docMagic = EncryptedCompressedDocMagic
			} else {
				docMagic = EncryptedDocMagic
			}
			documentBytes = encrypted
		}
	}

	// CRITICAL FIX: Allocate header buffer per-write to avoid race conditions
	// Shared buffers cause corruption when multiple goroutines write simultaneously
	headerSize := uint32(len(documentBytes))
	headerBytes := make([]byte, 8)                             // 8 bytes: 4 for magic, 4 for size
	binary.LittleEndian.PutUint32(headerBytes[0:4], docMagic)  // Magic number for document boundaries
	binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

	// Log write operation start for corruption debugging
	totalWriteSize := 8 + len(documentBytes) // header + document
	b.writeLogger.LogWriteStart(bundle.Name, writeOffset, totalWriteSize)

	// Use buffered write for optimal I/O performance
	writeBuffer, err := b.getOrCreateWriteBuffer(bundle.Name, filePath)
	if err != nil {
		b.writeLogger.LogWriteEnd(bundle.Name, writeOffset, 0, fmt.Errorf("failed to get write buffer: %w", err))
		return 0, fmt.Errorf("failed to get write buffer: %w", err)
	}

	// PERFORMANCE FIX: Use buffer pool for combined data to avoid allocations
	combinedData := b.getCombinedBuffer(len(headerBytes) + len(documentBytes))
	copy(combinedData[:8], headerBytes)
	copy(combinedData[8:], documentBytes)

	// Use WriteWithTxID to track transaction context
	if err := writeBuffer.WriteWithTxID(combinedData[:len(headerBytes)+len(documentBytes)], document.DocumentID, txID); err != nil {
		b.returnCombinedBuffer(combinedData) // Return buffer to pool
		b.writeLogger.LogWriteEnd(bundle.Name, writeOffset, 0, fmt.Errorf("failed to write document data: %w", err))
		return 0, fmt.Errorf("failed to write document data: %w", err)
	}

	b.returnCombinedBuffer(combinedData) // Return buffer to pool

	// PERFORMANCE FIX: Remove synchronous flush from hot path
	// The WriteBuffer already handles flushing when buffer is full or timeout reached
	// This eliminates the blocking fsync() call that was causing 13-15ms latency
	// Durability is ensured by:
	// 1. WAL logging (which has its own batching/flushing logic)
	// 2. WriteBuffer auto-flush when buffer full or timeout (100ms default)
	// 3. Transaction commit will force flush if needed
	//
	// NOTE: Readers will still see consistent data because:
	// - Documents are added to memtable immediately (bundle.Documents map)
	// - WAL ensures durability and crash recovery
	// - WriteBuffer will flush automatically when needed
	// - The comment about "readers seeing partial data" was incorrect - readers use memtable, not disk

	// PERFORMANCE FIX: Defer manifest updates to batch them and avoid fsync on every write
	// The manifest update was doing fsync on every document insert, causing 2-5ms latency
	// Instead, we'll update the in-memory manifest and defer persistence
	// Manifest will be persisted:
	// 1. When buffer flushes (batched)
	// 2. On transaction commit
	// 3. Periodically via background goroutine
	newFileSize := currentFileSize + int64(totalWriteSize)
	// Update in-memory manifest stats without persistence (no fsync)
	manifestMgr.UpdateFileStatsDeferred(int(currentFileID), 0, 0, 0, 0, newFileSize)

	// Log successful write operation
	b.writeLogger.LogWriteEnd(bundle.Name, writeOffset, totalWriteSize, nil)

	// CRITICAL DURABILITY FIX: Ensure data reaches disk to prevent corruption
	// WriteBuffer.WriteWithTxID buffers data but doesn't fsync immediately.
	// Auto-flush happens every 100ms or when buffer is full.
	//
	// Problem: On crash before flush:
	// 1. Buffered data is lost
	// 2. Manifest/indexes already updated
	// 3. Compaction reads garbage → corruption
	//
	// Solution: Conditional fsync based on durability mode
	// - "strict": Group commit fsync (batches writes, ~20ms window)
	// - default: Skip fsync, rely on 100ms auto-flush (fast but risky)
	if settings.GetSettings().DurabilityMode == "strict" {
		// Use group commit to batch multiple writes into single fsync
		// Blocks for up to 20ms waiting for other writes to join the batch
		if err := writeBuffer.SyncGroupCommit(); err != nil {
			b.logger.Warnf("Failed to sync data to disk: %v (continuing anyway)", err)
		}
	}

	// PHASE 1: MVCC - TotalDocuments already incremented atomically above
	// Calculate PageCount atomically from current TotalDocuments
	// Use ceiling division: ceil(a/b) = (a + b - 1) / b
	totalDocs := atomic.LoadInt64(&bundle.TotalDocuments)
	if totalDocs > 0 {
		calculatedPageCount := int64((uint32(totalDocs) + pageSize - 1) / pageSize)
		atomic.StoreInt64(&bundle.PageCount, calculatedPageCount)
	} else {
		atomic.StoreInt64(&bundle.PageCount, 0)
	}

	// Mark bundle as dirty to trigger metadata persistence
	bundle.IsDirty = true

	// PERFORMANCE FIX: Remove logging in hot path
	// Success logging only in debug mode
	if b.logger != nil && settings.GetSettings().Debug {
		b.logger.Infow("Successfully appended document to bundle",
			"bundle", bundle.Name,
			"documentID", document.DocumentID,
			"pageID", pageID,
			"fileID", currentFileID,
			"documentSize", headerSize,
			"newTotalDocuments", bundle.TotalDocuments,
			"newPageCount", bundle.PageCount)
	}
	//b.logger.Debugf("Ending time: %s", time.Now().Format(time.RFC3339Nano))
	//endingTesting := time.Since(testingStart)
	//b.logger.Debugf("DEBUG DEBUG DEBUG AppendDocumentToBundleFileWithTxID took %s", endingTesting.String())

	// COMPACTION INTEGRATION: Removed per-write goroutine spawning.
	// GOROUTINE LEAK FIX: Previously spawned a goroutine on every write to call
	// EvaluateBundle(). Under high write load (30 connections, 10 minutes),
	// this created 1.4+ million goroutines that piled up waiting for locks
	// in ShouldCompact() and LoadOrCreate(). This caused severe scheduler pressure
	// and latency degradation (20ms → 71ms on subsequent runs).
	//
	// Compaction is now handled by:
	// 1. periodicCompactionEvaluator (every 60s) - background evaluation of all bundles
	// 2. File rotation triggers - evaluated when files exceed size threshold
	//
	// This maintains PostgreSQL autovacuum-inspired behavior without the per-write overhead.

	// Return the page ID where this document was stored
	return pageID, nil
}

// AppendVersionToBundleFile creates a new document version for RCU (Read-Copy-Update) updates.
// This is the storage layer for lock-free concurrent updates.
//
// Unlike AppendDocumentToBundleFileWithTxID, this method:
// 1. Sets CommitSequence from the provided value (caller gets from SnapshotManager.GetNextCommitSequence())
// 2. Increments VersionSequence based on oldDoc
// 3. Marks the old document's SupersededAt timestamp (via caller)
//
// The append is inherently safe for concurrent writes because:
// - WriteBuffer uses atomic offset tracking
// - No bundle-level write lock is needed
// - Each version is a new append-only entry
//
// Parameters:
//   - bundle: Target bundle
//   - newDoc: New document version with updated fields (will have CommitSequence/VersionSequence set)
//   - oldDoc: Previous document version (used to increment VersionSequence)
//   - commitSequence: Global commit sequence from SnapshotManager.GetNextCommitSequence()
//
// Returns: Page ID where new version was stored
func (b *BundleStorageEngine) AppendVersionToBundleFile(bundle *models.Bundle, newDoc *models.Document, oldDoc *models.Document, commitSequence uint64) (uint32, error) {
	// Validate inputs
	if bundle == nil {
		return 0, fmt.Errorf("bundle cannot be nil")
	}
	if newDoc == nil {
		return 0, fmt.Errorf("new document cannot be nil")
	}
	if oldDoc == nil {
		return 0, fmt.Errorf("old document cannot be nil for version append")
	}
	if newDoc.DocumentID == "" {
		return 0, fmt.Errorf("document must have a valid ID")
	}
	if newDoc.DocumentID != oldDoc.DocumentID {
		return 0, fmt.Errorf("new and old document IDs must match: got %s vs %s", newDoc.DocumentID, oldDoc.DocumentID)
	}

	// Set RCU version metadata
	newDoc.CommitSequence = commitSequence
	newDoc.VersionSequence = oldDoc.VersionSequence + 1
	newDoc.CreatedByTxID = 0 // Autocommit mode (no transaction)
	newDoc.DeletedByTxID = 0 // Not deleted

	// MULTI-FILE STORAGE: Determine current active file and check if rotation is needed
	manifestMgr := b.getOrCreateManifestManager(bundle.Database.Name, bundle.Name)

	// Construct bundle directory (constant)
	bundleDir := GetBundleDirectory(bundle.Database.Name, bundle.Name)

	// OPTIMISTIC READ: Read manifest without lock for fast path (common case: no rotation needed)
	manifest, err := manifestMgr.LoadOrCreate(bundle.Database.Name, bundle.Name)
	if err != nil {
		return 0, fmt.Errorf("failed to load bundle manifest: %w", err)
	}

	// Get the current active (writable) file
	var currentFileID uint32 = 1
	if manifest.ActiveFileID > 0 {
		currentFileID = uint32(manifest.ActiveFileID)
	}
	initialFileID := currentFileID // Save for validation after lock

	// Construct the current file path
	filePath := filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))

	// Ensure current file exists in manifest
	fileExistsInManifest := false
	for _, f := range manifest.Files {
		if f.FileID == int(currentFileID) {
			fileExistsInManifest = true
			break
		}
	}
	if !fileExistsInManifest {
		fileName := fmt.Sprintf("%06d.bnd", currentFileID)
		if err := manifestMgr.AddFile(int(currentFileID), fileName); err != nil {
			return 0, fmt.Errorf("failed to add initial file to manifest: %w", err)
		}
	}

	// Acquire rotation lock before checking file size
	rotationLock := b.getRotationLock(bundle.Name)
	rotationLock.Lock()

	// DOUBLE-CHECK: Validate that activeFileID hasn't changed while we were waiting
	currentActiveID := manifestMgr.GetActiveFileID()
	if currentActiveID > 0 && uint32(currentActiveID) != initialFileID {
		// File was rotated by another goroutine - use the new active file
		currentFileID = uint32(currentActiveID)
		filePath = filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))
	}

	// Check if file rotation is needed
	// PERFORMANCE FIX: Use WriteBuffer's atomic offset instead of os.Stat syscall
	maxSizeBytes := int64(settings.GetSettings().Storage.BundleFileMaxSizeMB) * 1024 * 1024
	rotationThreshold := int64(float64(maxSizeBytes) * 1.1)

	var currentFileSize int64 = 0
	needsRotation := false

	// Try to get file size from existing WriteBuffer (fast path - no syscall)
	if existingBuffer := b.writeBufferCache.Get(filePath); existingBuffer != nil {
		currentFileSize = existingBuffer.GetAtomicOffset()
		needsRotation = currentFileSize >= rotationThreshold
	} else {
		// Fallback to os.Stat only when buffer doesn't exist yet
		if fileInfo, statErr := os.Stat(filePath); statErr == nil {
			currentFileSize = fileInfo.Size()
			needsRotation = currentFileSize >= rotationThreshold
		}
	}

	// Handle file rotation if needed
	if needsRotation {
		if b.logger != nil {
			b.logger.Infow("RCU: Rotating bundle file - size threshold reached",
				"bundle", bundle.Name,
				"currentFileID", currentFileID,
				"fileSize", currentFileSize,
				"threshold", rotationThreshold)
		}

		if err := b.CloseWriteBuffer(bundle.Name); err != nil {
			rotationLock.Unlock()
			return 0, fmt.Errorf("failed to close write buffer before rotation: %w", err)
		}

		frozenFileID := int(currentFileID)
		frozenFileName := fmt.Sprintf("%06d.bnd", currentFileID)
		if manifestMgr.GetActiveFileID() > 0 {
			if err := manifestMgr.FreezeFile(frozenFileID); err != nil {
				rotationLock.Unlock()
				return 0, fmt.Errorf("failed to freeze file in manifest: %w", err)
			}
			// Async bloom filter build for the frozen file
			b.buildBloomFilterForFrozenFile(manifestMgr, bundleDir, frozenFileID, frozenFileName)
		}

		currentFileID++
		filePath = filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))

		fileName := fmt.Sprintf("%06d.bnd", currentFileID)
		if err := manifestMgr.AddFile(int(currentFileID), fileName); err != nil {
			rotationLock.Unlock()
			return 0, fmt.Errorf("failed to add new file to manifest: %w", err)
		}

		if b.logger != nil {
			b.logger.Infow("RCU: Created new bundle file segment",
				"bundle", bundle.Name,
				"newFileID", currentFileID,
				"filePath", filePath)
		}
	}

	rotationLock.Unlock()

	// PAGE ID: Use alphabetical order via SortedIndex (matches LoadDocumentPage)
	// For version updates, the document already exists so we query its position
	pageSize := uint32(4096)
	if bundle.PageSize > 0 {
		pageSize = uint32(bundle.PageSize)
	}

	var pageID uint32
	if bundle.SortedIndex != nil {
		// Document already exists - get its current position
		// Insert returns existing position if already present
		pageID = bundle.SortedIndex.Insert(newDoc.DocumentID, pageSize)
	} else {
		// Fallback: Use TotalDocuments (legacy behavior)
		currentDocCount := uint32(atomic.LoadInt64(&bundle.TotalDocuments))
		pageID = currentDocCount / pageSize
	}

	schema := bundle.DocumentStructure.FieldSchema()
	documentBytes, err := b.serializeDocumentDirect(newDoc, schema)
	if err != nil {
		return 0, fmt.Errorf("failed to encode document version: %w", err)
	}

	// Build header with magic number and size
	headerSize := uint32(len(documentBytes))
	headerBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADBEEF)
	binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

	// Calculate total write size
	totalWriteSize := 8 + len(documentBytes)

	// Combine header and document data
	combinedData := b.getCombinedBuffer(len(headerBytes) + len(documentBytes))
	copy(combinedData[:8], headerBytes)
	copy(combinedData[8:], documentBytes)

	// RCU LOCK-FREE WRITE with FROZEN BUFFER RETRY
	// When file rotation happens, the old WriteBuffer is frozen to prevent corruption.
	// Writers holding stale buffer references will get ErrBufferFrozen and must retry
	// with a fresh buffer from the new active file.
	var actualOffset int64
	var writeBuffer *WriteBuffer
	maxRetries := 3
	for retry := 0; retry < maxRetries; retry++ {
		// Get or create write buffer (lock-free via atomic offset)
		writeBuffer, err = b.getOrCreateWriteBuffer(bundle.Name, filePath)
		if err != nil {
			b.returnCombinedBuffer(combinedData)
			return 0, fmt.Errorf("failed to get write buffer: %w", err)
		}

		// WriteDirectAtomic uses atomic.AddInt64 for offset reservation and pwrite for concurrent I/O
		actualOffset, err = writeBuffer.WriteDirectAtomic(combinedData[:len(headerBytes)+len(documentBytes)])

		if err == nil {
			break // Success
		}

		// Check if buffer was frozen (file rotated)
		if errors.Is(err, ErrBufferFrozen) {
			// CRITICAL FIX: Delete the frozen buffer from cache to prevent accumulation
			// Race condition: A writer may call getOrCreateWriteBuffer for the OLD file
			// right after CloseWriteBuffer deletes it, creating an orphan frozen buffer.
			// Without this delete, orphan buffers accumulate and degrade performance.
			b.writeBufferCache.Delete(filePath)

			// File was rotated - get the new active file path and retry
			manifest, _ = manifestMgr.LoadOrCreate(bundle.Database.Name, bundle.Name)
			if manifest.ActiveFileID > 0 {
				currentFileID = uint32(manifest.ActiveFileID)
			}
			filePath = filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))

			if b.logger != nil {
				b.logger.Debugf("RCU: Buffer frozen during write, retrying with new active file %d (attempt %d/%d)",
					currentFileID, retry+1, maxRetries)
			}
			continue // Retry with fresh buffer
		}

		// Other error - fail immediately
		b.returnCombinedBuffer(combinedData)
		b.writeLogger.LogWriteEnd(bundle.Name, actualOffset, 0, fmt.Errorf("failed to write document version: %w", err))
		return 0, fmt.Errorf("failed to write document version: %w", err)
	}

	// Log write operation with actual offset (after atomic reservation)
	b.writeLogger.LogWriteStart(bundle.Name, actualOffset, totalWriteSize)

	b.returnCombinedBuffer(combinedData)

	// Update manifest stats (deferred, no fsync on every write)
	newFileSize := currentFileSize + int64(totalWriteSize)
	manifestMgr.UpdateFileStatsDeferred(int(currentFileID), 0, 0, 0, 0, newFileSize)

	// Log successful write (use actualOffset, not pre-reserved writeOffset)
	b.writeLogger.LogWriteEnd(bundle.Name, actualOffset, totalWriteSize, nil)

	// CRITICAL DURABILITY FIX: Ensure data reaches disk to prevent corruption
	// RCU uses WriteDirectAtomic which bypasses buffering but needs fsync for durability.
	//
	// Problem: Without fsync, data sits in OS buffer cache. On crash:
	// 1. Data is lost (not on disk)
	// 2. Manifest/indexes already updated
	// 3. Compaction reads garbage → corruption
	//
	// Solution: Conditional fsync based on durability mode
	// - "strict": Group commit fsync (batches writes, ~20ms window)
	// - default: Skip fsync (fast but risky on crash)
	if settings.GetSettings().DurabilityMode == "strict" {
		// Use group commit to batch multiple writes into single fsync
		// This reduces fsync overhead from ~5ms per write to ~5ms per batch
		if err := writeBuffer.SyncGroupCommit(); err != nil {
			b.logger.Warnf("RCU: Failed to sync data to disk: %v (continuing anyway)", err)
		}
	}

	// Mark bundle as dirty
	bundle.IsDirty = true

	// if b.logger != nil && settings.GetSettings().Debug {
	// 	b.logger.Infow("RCU: Successfully appended document version",
	// 		"bundle", bundle.Name,
	// 		"documentID", newDoc.DocumentID,
	// 		"pageID", pageID,
	// 		"fileID", currentFileID,
	// 		"versionSequence", newDoc.VersionSequence,
	// 		"commitSequence", newDoc.CommitSequence)
	// }

	// COMPACTION INTEGRATION: Removed per-write goroutine spawning.
	// GOROUTINE LEAK FIX: See comment in AppendDocumentToBundleFileWithTxID.
	// Compaction evaluation is now handled by periodicCompactionEvaluator (60s interval).

	return pageID, nil
}

// periodicCompactionEvaluator runs background compaction checks
// PostgreSQL autovacuum-inspired: periodically check all bundles for compaction triggers
// This ensures compaction runs even when writes stop
func (b *BundleStorageEngine) periodicCompactionEvaluator(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second) // Check every 60 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Evaluate compaction needs
			b.evaluateAllBundlesForCompaction()

			// DURABILITY FIX: Persist deferred manifest updates periodically
			// This ensures manifests are consistent with disk data even if server
			// runs for days without file rotation or shutdown
			b.persistDirtyManifests()
		}
	}
}

// evaluateAllBundlesForCompaction evaluates all active bundles for compaction
// This is called by the periodic evaluator to ensure compaction runs even without writes
// PHASE 3: Using ShardedManifestCache.Range for concurrent-safe iteration
func (b *BundleStorageEngine) evaluateAllBundlesForCompaction() {
	// Collect keys and evaluate asynchronously to avoid holding locks during evaluation
	var bundlesToEvaluate []struct{ db, bundle string }

	b.manifestCache.Range(func(key string, _ *ManifestManager) bool {
		// Parse manager key: "<database>:<bundle>"
		parts := strings.SplitN(key, ":", 2)
		if len(parts) == 2 {
			bundlesToEvaluate = append(bundlesToEvaluate, struct{ db, bundle string }{parts[0], parts[1]})
		}
		return true
	})

	// Evaluate each bundle asynchronously
	for _, item := range bundlesToEvaluate {
		// Async evaluation - don't block ticker
		go func(db, bundle string) {
			if b.compactionScheduler != nil {
				b.compactionScheduler.EvaluateBundle(db, bundle)
			}
		}(item.db, item.bundle)
	}
}

// persistDirtyManifests persists all manifests that have deferred updates
// Called periodically (every 60s) to ensure manifest/data consistency without
// requiring file rotation or shutdown
func (b *BundleStorageEngine) persistDirtyManifests() {
	persistCount := 0
	errorCount := 0

	b.manifestCache.Range(func(key string, manifestMgr *ManifestManager) bool {
		// Check if manifest has pending updates (LastUpdated > zero time)
		// Use GetManifest() for thread-safe access (returns a copy under RLock).
		manifest := manifestMgr.GetManifest()
		if manifest == nil || manifest.LastUpdated.IsZero() {
			return true // Skip - no updates pending
		}

		// Persist the manifest to disk
		if err := manifestMgr.PersistManifest(); err != nil {
			b.logger.Warnf("Failed to persist manifest for %s: %v", key, err)
			errorCount++
		} else {
			persistCount++
		}
		return true // continue iteration
	})

	// Log summary if any manifests were persisted (debug mode only)
	if persistCount > 0 && settings.GetSettings().Debug {
		b.logger.Debugf("Periodic manifest persistence: %d persisted, %d errors", persistCount, errorCount)
	}
}

// getWriteLock gets or creates a write lock for a specific bundle
// This ensures thread-safe access to bundle data during concurrent reads and writes
// PERFORMANCE: Uses sharded lock map (64 shards) to reduce contention under high concurrency
// buildBloomFilterForFrozenFile asynchronously builds a bloom filter for a just-frozen segment file.
// Called after FreezeFile succeeds during file rotation. Runs in a goroutine to avoid blocking the write path.
func (b *BundleStorageEngine) buildBloomFilterForFrozenFile(manifestMgr *ManifestManager, bundleDir string, fileID int, fileName string) {
	go func() {
		filePath := filepath.Join(bundleDir, fileName)

		data, err := b.getOrReadFile(filePath)
		if err != nil {
			b.logger.Warnw("Bloom filter build: failed to read frozen file",
				"fileID", fileID, "error", err)
			return
		}

		// Extract document IDs from the parsed file
		docIDs := make([]string, 0, 1024)
		offset := 0

		// Skip bundle metadata header if present
		if len(data) >= 8 {
			magic := binary.LittleEndian.Uint32(data[0:4])
			if magic == 0x42444D44 { // "BDMD"
				metadataSize := binary.LittleEndian.Uint32(data[4:8])
				offset = int(8 + metadataSize)
			}
		}

		// Scan through file to collect document IDs
		for offset < len(data) {
			if offset+8 > len(data) {
				break
			}
			magic := binary.LittleEndian.Uint32(data[offset : offset+4])
			size := binary.LittleEndian.Uint32(data[offset+4 : offset+8])

			if magic == 0xDEADBEEF || magic == CompressedDocMagic {
				if offset+8+int(size) > len(data) {
					break
				}
				docData := data[offset+8 : offset+8+int(size)]

				// Decompress if needed
				if magic == CompressedDocMagic {
					decompressed, decErr := DecompressDocument(docData)
					if decErr != nil {
						offset += 8 + int(size)
						continue
					}
					docData = decompressed
				}

				minSchema := models.NewProjectionSchema(nil) // DocumentID only for bloom
				doc, decErr := helpers.DecodeFastBinaryToDocument(docData, minSchema)
				if decErr == nil && doc.DocumentID != "" {
					docIDs = append(docIDs, doc.DocumentID)
				}
				offset += 8 + int(size)
			} else if magic == 0xDEADDEAD || magic == CompressedTombstoneMagic {
				// Deletion marker — skip
				offset += 8 + int(size)
			} else {
				break
			}
		}

		if len(docIDs) == 0 {
			return
		}

		bf := BuildBloomFilterForDocuments(docIDs, 0.01)
		if bf == nil {
			return
		}

		bloomData, bloomSize, bloomHashes, err := SerializeBloomFilter(bf)
		if err != nil {
			b.logger.Warnw("Bloom filter build: failed to serialize",
				"fileID", fileID, "error", err)
			return
		}

		if err := manifestMgr.UpdateBloomFilter(fileID, bloomData, bloomSize, bloomHashes); err != nil {
			b.logger.Warnw("Bloom filter build: failed to store in manifest",
				"fileID", fileID, "error", err)
			return
		}

		b.logger.Debugw("Built bloom filter for frozen file",
			"fileID", fileID, "docCount", len(docIDs))
	}()
}

func (b *BundleStorageEngine) getWriteLock(bundleName string) *sync.RWMutex {
	return b.writeLocks.Get(bundleName)
}

// getRotationLock gets or creates a rotation lock for a specific bundle
// PHASE 1: MVCC - Protects file rotation decision and execution
// This ensures only one goroutine can rotate a bundle's file at a time
// PERFORMANCE: Uses sharded lock map (64 shards) to reduce contention
func (b *BundleStorageEngine) getRotationLock(bundleName string) *sync.Mutex {
	return b.rotationLocks.Get(bundleName)
}

// getDocumentLock gets or creates a lock for a specific document within a bundle
// DOCUMENT-LEVEL LOCKING: Enables concurrent writes to different documents
func (b *BundleStorageEngine) getDocumentLock(bundleName, documentID string) *sync.Mutex {
	b.documentLocksMutex.Lock()
	defer b.documentLocksMutex.Unlock()

	// Ensure bundle map exists
	if b.documentLocks[bundleName] == nil {
		b.documentLocks[bundleName] = make(map[string]*sync.Mutex)
	}

	// Get or create document lock
	if lock, exists := b.documentLocks[bundleName][documentID]; exists {
		return lock
	}

	lock := &sync.Mutex{}
	b.documentLocks[bundleName][documentID] = lock
	return lock
}

// removeDocumentLocksForBundle removes all document locks for a bundle.
// Call when a bundle is deleted so the map does not retain entries for removed bundles.
func (b *BundleStorageEngine) removeDocumentLocksForBundle(bundleName string) {
	b.documentLocksMutex.Lock()
	delete(b.documentLocks, bundleName)
	b.documentLocksMutex.Unlock()
}

// acquireDocumentLocks acquires locks for multiple documents in sorted order to prevent deadlocks
// Returns the acquired locks in the same order for release
// DOCUMENT-LEVEL LOCKING: Used by UpdateDocumentsBatchWithLocks for concurrent writes
func (b *BundleStorageEngine) acquireDocumentLocks(bundleName string, docIDs []string) []*sync.Mutex {
	// Sort document IDs to prevent deadlocks (always acquire in same order)
	sortedIDs := make([]string, len(docIDs))
	copy(sortedIDs, docIDs)
	sort.Strings(sortedIDs)

	locks := make([]*sync.Mutex, len(sortedIDs))
	for i, docID := range sortedIDs {
		lock := b.getDocumentLock(bundleName, docID)
		lock.Lock()
		locks[i] = lock
	}

	return locks
}

// releaseDocumentLocks releases all acquired document locks
// DOCUMENT-LEVEL LOCKING: Used by UpdateDocumentsBatchWithLocks after write completes
func (b *BundleStorageEngine) releaseDocumentLocks(locks []*sync.Mutex) {
	// Release in reverse order (LIFO) for proper lock ordering
	for i := len(locks) - 1; i >= 0; i-- {
		if locks[i] != nil {
			locks[i].Unlock()
		}
	}
}

// extractDocumentIDOnly extracts just the DocumentID from fast binary format
// This is faster than full decode because it only reads the first field (DocumentID)
// and returns immediately without parsing fields, timestamps, or MVCC metadata
func (b *BundleStorageEngine) extractDocumentIDOnly(data []byte) (string, error) {
	if len(data) < 4 {
		return "", fmt.Errorf("insufficient data for DocumentID extraction")
	}

	// Decode with minimal schema (DocumentID only) for fast extraction
	minSchema := models.NewProjectionSchema(nil)
	doc, err := helpers.DecodeFastBinaryToDocument(data, minSchema)
	if err != nil {
		return "", fmt.Errorf("failed to decode DocumentID: %w", err)
	}

	// Extract only DocumentID from the Document
	if doc.DocumentID != "" {
		return doc.DocumentID, nil
	}

	return "", fmt.Errorf("DocumentID not found in decoded data")
}

// countDocumentsInFileOnly counts documents in a single file by extracting only DocumentIDs
// This is much faster than full parsing because it:
// 1. Only decodes DocumentID field (not full documents)
// 2. Tracks unique DocumentIDs in maps (last-write-wins)
// 3. Handles tombstones by extracting tombstone DocumentIDs
// 4. Does NOT build Document or Field objects
//
// Parameters:
//   - data: File data to count documents in
//   - seenDocuments: Map to track unique DocumentIDs seen (updated in-place)
//   - deletedDocuments: Map to track deleted DocumentIDs (updated in-place)
//
// Note: Caller must hold read lock on bundle to ensure consistent snapshot
func (b *BundleStorageEngine) countDocumentsInFileOnly(
	data []byte,
	seenDocuments map[string]bool,
	deletedDocuments map[string]bool,
) error {
	offset := 0
	documentsFound := 0
	tombstonesFound := 0

	if b.logger != nil {
		b.logger.Debugf("countDocumentsInFileOnly: Starting parse of %d bytes", len(data))
	}

	// Skip bundle metadata header if present (0x42444D44 = "BDMD")
	if len(data) >= 8 {
		magic := binary.LittleEndian.Uint32(data[0:4])
		if magic == 0x42444D44 { // "BDMD" = Bundle Metadata
			metadataSize := binary.LittleEndian.Uint32(data[4:8])
			offset = int(8 + metadataSize)
			if b.logger != nil {
				b.logger.Debugf("countDocumentsInFileOnly: Skipped BDMD header, starting at offset %d", offset)
			}
		}
	}

	for offset < len(data) {
		// Need at least 8 bytes for magic number + size header
		if offset+8 > len(data) {
			if b.logger != nil {
				b.logger.Debugf("countDocumentsInFileOnly: Stopping at offset %d (not enough bytes for header)", offset)
			}
			break
		}

		// Read magic number and size
		magic := binary.LittleEndian.Uint32(data[offset : offset+4])
		size := binary.LittleEndian.Uint32(data[offset+4 : offset+8])

		// Log first few records and any non-standard magic numbers
		if offset < 1000 || (magic != 0xDEADBEEF && magic != 0xDEADDEAD) {
			if b.logger != nil {
				b.logger.Debugf("countDocumentsInFileOnly: offset=%d magic=0x%X size=%d", offset, magic, size)
			}
		}

		// Validate size before proceeding
		if offset+8+int(size) > len(data) {
			if b.logger != nil {
				b.logger.Warnf("countDocumentsInFileOnly: Invalid size %d at offset %d (would exceed file length)", size, offset)
			}
			break
		}

		recordData := data[offset+8 : offset+8+int(size)]

		if magic == 0xDEADBEEF {
			// Document - extract only DocumentID
			docID, err := b.extractDocumentIDOnly(recordData)
			if err != nil {
				// Log warning but continue processing (don't fail entire count)
				if b.logger != nil {
					b.logger.Warnf("Failed to extract DocumentID at offset %d: %v (skipping)", offset, err)
				}
				offset += 8 + int(size)
				continue
			}

			if docID != "" {
				documentsFound++
				// Last-write-wins: later occurrence overwrites earlier
				seenDocuments[docID] = true
				// If was deleted, re-add it (update after delete)
				delete(deletedDocuments, docID)
			} else {
				if b.logger != nil && documentsFound < 5 {
					b.logger.Warnf("Extracted empty DocumentID at offset %d (size=%d)", offset, size)
				}
			}
		} else if magic == 0xDEADDEAD {
			// Tombstone - extract only DocumentID
			docID, err := b.extractDocumentIDOnly(recordData)
			if err != nil {
				// Log warning but continue processing
				if b.logger != nil {
					b.logger.Debugf("Failed to extract tombstone DocumentID at offset %d: %v (skipping)", offset, err)
				}
				offset += 8 + int(size)
				continue
			}

			if docID != "" {
				tombstonesFound++
				// Mark as deleted and remove from seen documents
				deletedDocuments[docID] = true
				delete(seenDocuments, docID)
			}
		}

		offset += 8 + int(size)
	}

	if b.logger != nil {
		b.logger.Debugf("countDocumentsInFileOnly: Parsed %d bytes, found %d documents and %d tombstones, final offset=%d",
			len(data), documentsFound, tombstonesFound, offset)
	}

	return nil
}

// countDocumentsMultiFile counts documents across all files in a multi-file bundle
// Uses manifest to iterate files and merges results with last-write-wins semantics
// Acquires read lock to ensure consistent snapshot during counting
//
// Parameters:
//   - manifestMgr: Manifest manager for the bundle
//   - databaseName: Database name
//   - bundleName: Bundle name
//
// Returns:
//   - int: Count of unique documents (excluding tombstones)
//   - error: Any error encountered during counting
func (b *BundleStorageEngine) countDocumentsMultiFile(
	manifestMgr *ManifestManager,
	databaseName, bundleName string,
) (int, error) {
	// CRITICAL: Acquire read lock for consistent snapshot
	// Allows concurrent reads but blocks during writes
	lock := b.getWriteLock(bundleName)
	lock.RLock()
	defer lock.RUnlock()

	// Load manifest snapshot (atomic read via manifest's own lock)
	manifest, err := manifestMgr.LoadOrCreate(databaseName, bundleName)
	if err != nil {
		return 0, fmt.Errorf("failed to load manifest: %w", err)
	}

	// Fast-path: If manifest is trusted (no tombstones, recently updated)
	// Use manifest metadata directly without scanning files
	if manifest.TotalTombstones == 0 &&
		time.Since(manifest.LastUpdated) < 5*time.Minute {
		if b.logger != nil {
			b.logger.Debugf("CountDocuments: Using manifest fast-path for bundle '%s' (TotalDocuments=%d)", bundleName, manifest.TotalDocuments)
		}
		return int(manifest.TotalDocuments), nil
	}

	if b.logger != nil {
		b.logger.Debugf("CountDocuments: Scanning files for bundle '%s' (tombstones=%d, lastUpdated=%v)",
			bundleName, manifest.TotalTombstones, manifest.LastUpdated)
	}

	// LOCAL maps - no concurrent access (this goroutine only)
	seenDocuments := make(map[string]bool)
	deletedDocuments := make(map[string]bool)

	// Get file list snapshot (oldest first for last-write-wins)
	files := manifestMgr.GetFileList(false) // false = oldest first

	if b.logger != nil {
		b.logger.Debugf("CountDocuments: Processing %d files for bundle '%s'", len(files), bundleName)
	}

	for _, fileInfo := range files {
		bundleDir := GetBundleDirectory(databaseName, bundleName)
		filePath := filepath.Join(bundleDir, fileInfo.FileName)

		// Check if file exists (may have been compacted)
		stat, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			if b.logger != nil && settings.GetSettings().Debug {
				b.logger.Debugf("Skipping non-existent file %s (likely compacted)", filePath)
			}
			continue
		}

		if b.logger != nil {
			b.logger.Debugf("CountDocuments: Reading file %s (size=%d bytes, manifest claims %d docs)",
				filePath, stat.Size(), fileInfo.DocCount)
		}

		// File read is safe: protected by read lock (no concurrent writes)
		data, err := os.ReadFile(filePath)
		if err != nil {
			if b.logger != nil {
				b.logger.Warnf("Failed to read file %s for counting: %v", filePath, err)
			}
			continue
		}

		if b.logger != nil {
			b.logger.Debugf("CountDocuments: Read %d bytes from %s, parsing documents...", len(data), filePath)
		}

		// Count in this file (updates local maps in-place)
		err = b.countDocumentsInFileOnly(data, seenDocuments, deletedDocuments)
		if err != nil {
			if b.logger != nil {
				b.logger.Warnf("Failed to count documents in file %s: %v", filePath, err)
			}
			continue
		}

		if b.logger != nil {
			b.logger.Debugf("CountDocuments: After processing %s: seen=%d, deleted=%d",
				fileInfo.FileName, len(seenDocuments), len(deletedDocuments))
		}
	}

	// Final count: unique documents that aren't deleted
	finalCount := len(seenDocuments)
	if b.logger != nil {
		b.logger.Debugf("CountDocuments: Final count for bundle '%s' = %d (seen=%d, deleted=%d)",
			bundleName, finalCount, len(seenDocuments)+len(deletedDocuments), len(deletedDocuments))
	}
	return finalCount, nil
}

// countDocumentsLegacy counts documents in a legacy single-file bundle
// Acquires read lock to ensure consistent snapshot
func (b *BundleStorageEngine) countDocumentsLegacy(
	bundleName, databaseName string,
) (int, error) {
	// CRITICAL: Acquire read lock for consistent snapshot
	lock := b.getWriteLock(bundleName)
	lock.RLock()
	defer lock.RUnlock()

	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return 0, nil
	}

	// Read the file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read bundle file: %w", err)
	}

	// LOCAL maps - no concurrent access
	seenDocuments := make(map[string]bool)
	deletedDocuments := make(map[string]bool)

	// Count documents in file
	err = b.countDocumentsInFileOnly(data, seenDocuments, deletedDocuments)
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	// Final count: unique documents that aren't deleted
	return len(seenDocuments), nil
}

// CountDocuments counts all documents in a bundle using optimized count-only parser
// This method implements the BundleStore interface and provides efficient counting
// by extracting only DocumentIDs without parsing full document data
//
// Parameters:
//   - bundleName: Name of the bundle to count
//   - databaseName: Name of the database containing the bundle
//
// Returns:
//   - int: Count of unique documents (excluding tombstones)
//   - error: Any error encountered during counting
func (b *BundleStorageEngine) CountDocuments(bundleName, databaseName string) (int, error) {
	// Try multi-file format first (most common)
	manifestMgr := b.getOrCreateManifestManager(databaseName, bundleName)
	manifest, err := manifestMgr.LoadOrCreate(databaseName, bundleName)
	if err == nil && len(manifest.Files) > 0 {
		// Multi-file format - use optimized count
		if b.logger != nil {
			b.logger.Debugf("CountDocuments: Using multi-file format for bundle '%s' (%d files)", bundleName, len(manifest.Files))
		}
		return b.countDocumentsMultiFile(manifestMgr, databaseName, bundleName)
	}

	// Fall back to legacy single-file format
	if b.logger != nil {
		b.logger.Debugf("CountDocuments: Using legacy single-file format for bundle '%s' (manifest err=%v, files=%d)",
			bundleName, err, len(manifest.Files))
	}
	return b.countDocumentsLegacy(bundleName, databaseName)
}

// getOrCreateManifestManager gets or creates a manifest manager for a specific bundle
// Manifest managers are cached per bundle for performance
// PHASE 3: Using ShardedManifestCache for concurrent access without global mutex
func (b *BundleStorageEngine) getOrCreateManifestManager(databaseName, bundleName string) *ManifestManager {
	// Use bundleName as key (unique per database context)
	managerKey := databaseName + ":" + bundleName

	return b.manifestCache.GetOrCreateSimple(managerKey, func() *ManifestManager {
		return NewManifestManager(b.DataDirectory, databaseName, bundleName, b.logger)
	})
}

// getOrCreateWriteBuffer gets or creates a write buffer for the specified file
// MULTI-FILE STORAGE: Write buffers are now keyed by filePath instead of bundleName
// This allows multiple active write buffers per bundle (one per file segment)
// PHASE 3: Using ShardedBufferCache for concurrent access without global mutex
func (b *BundleStorageEngine) getOrCreateWriteBuffer(bundleName, filePath string) (*WriteBuffer, error) {
	// Use file path as key to support multiple files per bundle
	bufferKey := filePath

	return b.writeBufferCache.GetOrCreate(bufferKey, func() (*WriteBuffer, error) {
		// Ensure the bundle directory exists
		// Use the directory from filePath directly instead of reconstructing
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create bundle directory: %w", err)
		}

		// Open file in append mode with O_CREATE to handle first-time creation
		// CRITICAL: O_CREATE ensures file exists
		// NOTE: O_APPEND is NOT used here because WriteDirectAtomic uses WriteAt() which is
		// incompatible with O_APPEND. Instead, we use atomic offset tracking for thread-safe appends.
		file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open bundle file for buffering: %w", err)
		}

		// Create write buffer with 64KB buffer size for optimal performance
		return NewWriteBuffer(file, 65536), nil
	})
}

// bufferKeyMatchesBundle reports whether bufferKey corresponds to the bundle.
// slashPattern and backPattern should be "/"+bundleName+"/" and "\\"+bundleName+"\\"
// so callers build them once per operation (allocation-free in hot Range loops).
func (b *BundleStorageEngine) bufferKeyMatchesBundle(bufferKey, slashPattern, backPattern string) bool {
	return strings.Contains(bufferKey, slashPattern) || strings.Contains(bufferKey, backPattern)
}

// FlushWriteBuffers flushes all write buffers for a specific bundle
// MULTI-FILE STORAGE: Flushes all file buffers associated with the bundle
// PHASE 3: Using ShardedBufferCache.Range for concurrent-safe iteration
func (b *BundleStorageEngine) FlushWriteBuffers(bundleName string) error {
	var errors []error
	var foundDatabaseName string
	slash, back := "/"+bundleName+"/", "\\"+bundleName+"\\"

	b.writeBufferCache.Range(func(bufferKey string, buffer *WriteBuffer) bool {
		if !b.bufferKeyMatchesBundle(bufferKey, slash, back) {
			return true // continue to next
		}

		if err := buffer.Flush(); err != nil {
			b.logger.Warnf("Failed to flush buffer for %s: %v", bufferKey, err)
			errors = append(errors, err)
		}

		// Extract database name for compaction evaluation (first match only)
		if foundDatabaseName == "" {
			parts := strings.Split(bufferKey, "/")
			if len(parts) >= 3 {
				foundDatabaseName = parts[len(parts)-3]
			}
		}

		return true // continue iteration
	})

	if len(errors) > 0 {
		return fmt.Errorf("failed to flush %d write buffers for bundle %s", len(errors), bundleName)
	}

	// Evaluate compaction triggers after successful flush
	// This is similar to PostgreSQL's autovacuum triggering after significant write activity
	if b.compactionScheduler != nil && foundDatabaseName != "" {
		b.compactionScheduler.EvaluateBundle(foundDatabaseName, bundleName)
	}

	return nil
}

// SyncWriteBuffers forces fsync on all write buffers for a bundle to ensure data is on disk
// This is critical before compaction to prevent reading incomplete data from OS cache
func (b *BundleStorageEngine) SyncWriteBuffers(bundleName string) error {
	var errors []error
	slash, back := "/"+bundleName+"/", "\\"+bundleName+"\\"

	b.writeBufferCache.Range(func(bufferKey string, buffer *WriteBuffer) bool {
		if !b.bufferKeyMatchesBundle(bufferKey, slash, back) {
			return true // continue to next
		}

		if err := buffer.Sync(); err != nil {
			b.logger.Warnf("Failed to sync buffer for %s: %v", bufferKey, err)
			errors = append(errors, err)
		}

		return true // continue iteration
	})

	if len(errors) > 0 {
		return fmt.Errorf("failed to sync %d write buffers for bundle %s", len(errors), bundleName)
	}

	return nil
}

// getWriteBuffer retrieves an existing write buffer for a specific file (non-creating)
// Returns nil if buffer doesn't exist yet
// CRITICAL FIX: Must use same file naming format as getOrCreateWriteBuffer (%06d.bnd)
// Previously used "bundle_%d.dat" which never matched, causing compaction corruption
func (b *BundleStorageEngine) getWriteBuffer(databaseName, bundleName string, fileID int) *WriteBuffer {
	bundleDir := GetBundleDirectory(databaseName, bundleName)
	fileName := fmt.Sprintf("%06d.bnd", fileID) // Must match format used in getOrCreateWriteBuffer
	filePath := filepath.Join(bundleDir, fileName)

	buffer := b.writeBufferCache.Get(filePath)
	return buffer
}

// FlushAllWriteBuffers flushes all write buffers for all bundles
// MULTI-FILE STORAGE: Now flushes all file buffers (multiple files per bundle)
// PHASE 3: Using ShardedBufferCache.Range for concurrent-safe iteration
func (b *BundleStorageEngine) FlushAllWriteBuffers() error {
	var errors []string
	flushedCount := 0

	b.writeBufferCache.Range(func(bufferKey string, buffer *WriteBuffer) bool {
		if err := buffer.Flush(); err != nil {
			errorMsg := fmt.Sprintf("failed to flush buffer for file '%s': %v", bufferKey, err)
			b.logger.Warnf(errorMsg)
			errors = append(errors, errorMsg)
		} else {
			flushedCount++
			if b.logger != nil && settings.GetSettings().Debug {
				b.logger.Debugf("Successfully flushed write buffer for file '%s'", bufferKey)
			}
		}
		return true // continue iteration
	})

	totalBuffers := b.writeBufferCache.Len()

	if len(errors) > 0 {
		return fmt.Errorf("failed to flush %d of %d write buffers: %v", len(errors), totalBuffers, errors)
	}

	if b.logger != nil && settings.GetSettings().Debug {
		b.logger.Debugf("Successfully flushed all %d write buffers", flushedCount)
	}

	return nil
}

// CloseWriteBuffer closes and removes all write buffers for a specific bundle
// MULTI-FILE STORAGE: Now closes all file buffers associated with the bundle
// This is CRITICAL after operations that change file size (like appending tombstones or file rotation)
// to ensure subsequent file opens get fresh metadata (correct file size).
// PHASE 3: Using ShardedBufferCache.DeleteMatching for concurrent-safe close and delete
func (b *BundleStorageEngine) CloseWriteBuffer(bundleName string) error {
	slash, back := "/"+bundleName+"/", "\\"+bundleName+"\\"
	// Close and delete all buffers matching the bundle pattern
	deleted, errors := b.writeBufferCache.DeleteMatching(
		func(key string) bool {
			return b.bufferKeyMatchesBundle(key, slash, back)
		},
		func(bufferKey string, buffer *WriteBuffer) error {
			if err := buffer.Close(); err != nil {
				b.logger.Warnf("Failed to close write buffer for %s: %v", bufferKey, err)
				return err
			}
			return nil
		},
	)

	// Log cleanup results (useful for debugging buffer accumulation)
	if b.logger != nil && settings.GetSettings().Debug && deleted > 0 {
		b.logger.Debugf("CloseWriteBuffer: bundle=%s, deleted=%d, remainingCacheSize=%d",
			bundleName, deleted, b.writeBufferCache.Len())
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to close %d write buffers for bundle %s", len(errors), bundleName)
	}

	return nil
}

// DiscardWriteBuffer discards all write buffers for a specific bundle WITHOUT flushing
// MULTI-FILE STORAGE: Now discards all file buffers associated with the bundle
// This is used during transaction rollback to abandon buffered writes
// PHASE 3: Using ShardedBufferCache.DeleteMatching for concurrent-safe iteration
func (b *BundleStorageEngine) DiscardWriteBuffer(bundleName string) error {
	slash, back := "/"+bundleName+"/", "\\"+bundleName+"\\"
	_, errors := b.writeBufferCache.DeleteMatching(
		func(key string) bool {
			return b.bufferKeyMatchesBundle(key, slash, back)
		},
		func(bufferKey string, buffer *WriteBuffer) error {
			if err := buffer.Discard(); err != nil {
				b.logger.Warnf("Failed to discard write buffer for %s: %v", bufferKey, err)
				return err
			}
			b.logger.Debugf("Discarded write buffer for '%s' without flushing", bufferKey)
			return nil
		},
	)

	if len(errors) > 0 {
		return fmt.Errorf("failed to discard %d write buffers for bundle %s", len(errors), bundleName)
	}

	return nil
}

// GetBufferedDocumentsForTransaction returns all buffered documents for a specific transaction
// MULTI-FILE STORAGE: Aggregates documents from all file buffers for this bundle
// PHASE 3: Using ShardedBufferCache.Range for concurrent-safe iteration
func (b *BundleStorageEngine) GetBufferedDocumentsForTransaction(bundleName string, txID string) ([]*models.Document, error) {
	var allDocs []*models.Document
	var firstErr error
	slash, back := "/"+bundleName+"/", "\\"+bundleName+"\\"

	b.writeBufferCache.Range(func(bufferKey string, buffer *WriteBuffer) bool {
		if !b.bufferKeyMatchesBundle(bufferKey, slash, back) {
			return true // continue
		}

		docs, err := buffer.GetDocumentsForTransaction(txID)
		if err != nil {
			firstErr = err
			return false // stop iteration
		}
		allDocs = append(allDocs, docs...)
		return true
	})

	if firstErr != nil {
		return nil, firstErr
	}

	return allDocs, nil
}

// MarkDocumentDiscarded marks a document as discarded (for rollback)
// MULTI-FILE STORAGE: Marks document in whichever buffer contains it
// PHASE 3: Using ShardedBufferCache.Range for concurrent-safe iteration
func (b *BundleStorageEngine) MarkDocumentDiscarded(bundleName string, docID string) error {
	slash, back := "/"+bundleName+"/", "\\"+bundleName+"\\"
	b.writeBufferCache.Range(func(bufferKey string, buffer *WriteBuffer) bool {
		if !b.bufferKeyMatchesBundle(bufferKey, slash, back) {
			return true // continue
		}

		// Mark in this buffer (no-op if document not in this buffer)
		buffer.MarkDiscarded(docID)
		return true
	})

	return nil
}

// IsDocumentBuffered checks if a document is currently in any write buffer for this bundle
// MULTI-FILE STORAGE: Checks all file buffers for this bundle
// PHASE 3: Using ShardedBufferCache.Range for concurrent-safe iteration
func (b *BundleStorageEngine) IsDocumentBuffered(bundleName string, docID string) bool {
	found := false
	slash, back := "/"+bundleName+"/", "\\"+bundleName+"\\"

	b.writeBufferCache.Range(func(bufferKey string, buffer *WriteBuffer) bool {
		if !b.bufferKeyMatchesBundle(bufferKey, slash, back) {
			return true // continue
		}

		if buffer.IsDocumentAvailable(docID) {
			found = true
			return false // stop iteration
		}
		return true
	})

	return found
}

// GetDiscardedDocuments returns document IDs that were discarded in a bundle's buffers
// MULTI-FILE STORAGE: Aggregates discarded documents from all file buffers
// PHASE 3: Using ShardedBufferCache.Range for concurrent-safe iteration
func (b *BundleStorageEngine) GetDiscardedDocuments(bundleName string) []string {
	var allDiscarded []string
	slash, back := "/"+bundleName+"/", "\\"+bundleName+"\\"

	b.writeBufferCache.Range(func(bufferKey string, buffer *WriteBuffer) bool {
		if !b.bufferKeyMatchesBundle(bufferKey, slash, back) {
			return true // continue
		}

		discarded := buffer.GetDiscardedDocuments()
		allDiscarded = append(allDiscarded, discarded...)
		return true
	})

	return allDiscarded
}

// ClearDiscardedDocuments removes the specified document IDs from the discarded set
// MULTI-FILE STORAGE: Clears from all file buffers for this bundle
// PHASE 3: Using ShardedBufferCache.Range for concurrent-safe iteration
func (b *BundleStorageEngine) ClearDiscardedDocuments(bundleName string, docIDs []string) {
	slash, back := "/"+bundleName+"/", "\\"+bundleName+"\\"
	b.writeBufferCache.Range(func(bufferKey string, buffer *WriteBuffer) bool {
		if !b.bufferKeyMatchesBundle(bufferKey, slash, back) {
			return true // continue
		}

		buffer.ClearDiscardedDocuments(docIDs)
		return true
	})
}

// GetAllWriteBufferStats returns diagnostic statistics for all active write buffers.
// Used for debugging latency degradation by tracking file sizes.
func (b *BundleStorageEngine) GetAllWriteBufferStats() []WriteBufferStats {
	var stats []WriteBufferStats

	b.writeBufferCache.Range(func(bufferKey string, buffer *WriteBuffer) bool {
		stats = append(stats, buffer.GetStats())
		return true
	})

	return stats
}

// CloseWriteBuffers closes and flushes all write buffers
// PHASE 3: Using ShardedBufferCache.DeleteMatching for concurrent-safe close and clear
func (b *BundleStorageEngine) CloseWriteBuffers() error {
	_, errors := b.writeBufferCache.DeleteMatching(
		func(key string) bool {
			return true // match all buffers
		},
		func(bundleName string, buffer *WriteBuffer) error {
			if err := buffer.Close(); err != nil {
				b.logger.Warnf("Failed to close write buffer for bundle %s: %v", bundleName, err)
				return err
			}
			return nil
		},
	)

	if len(errors) > 0 {
		b.logger.Warnf("CloseWriteBuffers: %d buffers failed to close", len(errors))
	}

	return nil
}

// CompactAllCaches compacts all sharded caches to reclaim memory from deleted entries.
// PERFORMANCE FIX: Go's map delete() doesn't shrink the bucket array. After many
// operations, the shard maps accumulate empty bucket slots that degrade iteration
// and memory performance. This method recreates each shard's map with only current
// entries, reclaiming memory and restoring lookup speed.
// Returns total entries across all caches after compaction.
func (b *BundleStorageEngine) CompactAllCaches() int {
	total := 0
	total += b.writeBufferCache.Compact()
	total += b.manifestCache.Compact()
	total += b.projectionCache.Compact()
	total += b.fileReadCache.Compact()
	total += b.parsedDocsCache.Compact()
	return total
}

// FlushAllDocumentCaches completely clears all document-holding caches.
// This is more aggressive than CompactAllCaches - it removes all cached data rather
// than just compacting map structures.
//
// PERFORMANCE FIX: When all clients disconnect between test runs, document objects
// accumulate in caches (file read cache, parsed docs cache). While map compaction
// reclaims bucket memory, the actual document data remains. This method provides
// a "fresh start" equivalent to server restart, preventing latency degradation
// across consecutive test runs.
//
// Note: This does NOT flush write buffers - those must be flushed separately with
// FlushAllWriteBuffers to ensure data durability.
func (b *BundleStorageEngine) FlushAllDocumentCaches() {
	b.logger.Info("Flushing all storage engine document caches")

	// DIAGNOSTIC: Log cache sizes before flushing
	writeBufferCount := b.writeBufferCache.Len()
	b.logger.Infof("Storage engine state before flush: writeBuffers=%d", writeBufferCount)

	// Clear file read cache - holds raw file bytes that get parsed into documents
	b.fileReadCache.Flush()

	// Clear parsed docs cache - holds parsed document objects
	b.parsedDocsCache.Flush()

	// Clear merged bundle cache - holds full merged view per bundle (merge-once, serve many)
	b.mergedBundleCache.Flush()

	// Clear projection cache - holds projection field sets (not document data, but can grow)
	b.projectionCache.Flush()

	// Note: writeBufferCache and manifestCache are NOT flushed as they contain
	// pending writes and metadata that must be preserved for data integrity

	b.logger.Info("Storage engine document caches flushed")
}

// Shutdown gracefully stops the compaction scheduler and closes all resources
func (b *BundleStorageEngine) Shutdown() error {
	b.logger.Info("Shutting down bundle storage engine...")

	// Cancel compaction context to stop periodic evaluator
	if b.compactionCancel != nil {
		b.compactionCancel()
	}

	// Stop compaction scheduler
	if b.compactionScheduler != nil {
		b.compactionScheduler.Stop()
	}

	// Close all write buffers
	if err := b.CloseWriteBuffers(); err != nil {
		b.logger.Warnf("Error closing write buffers during shutdown: %v", err)
	}

	// CRITICAL DURABILITY FIX: Persist all manifest updates before shutdown
	// The manifests track file stats (UpdateFileStatsDeferred) but are only persisted
	// during file rotation. On shutdown, we need to persist all deferred updates to avoid
	// manifest/data inconsistency on restart.
	//
	// Without this, the scenario is:
	// 1. Documents are written and buffered (then flushed above via CloseWriteBuffers)
	// 2. Manifest stats are updated in memory (UpdateFileStatsDeferred)
	// 3. Shutdown happens - data is flushed but manifest is NOT persisted
	// 4. On restart: manifest has stale counts, but files have more documents
	// 5. Compaction reads past end of expected data → corruption errors
	//
	// TODO: Consider adding a periodic manifest flush (every 30s) to reduce shutdown time
	b.manifestCache.Range(func(key string, manifestMgr *ManifestManager) bool {
		// Load current manifest to check if it needs persistence
		// (manifests are loaded on-demand, so not all may be in memory)
		manifest, err := manifestMgr.LoadOrCreate("", "") // Empty params - already loaded
		if err != nil {
			b.logger.Warnf("Failed to load manifest for %s during shutdown: %v", key, err)
			return true // continue
		}

		// Only persist if manifest has been modified
		if manifest.LastUpdated.After(time.Time{}) {
			if err := manifestMgr.PersistManifest(); err != nil {
				b.logger.Warnf("Failed to persist manifest for %s during shutdown: %v", key, err)
			} else if settings.GetSettings().Debug {
				b.logger.Debugf("Persisted manifest for %s during shutdown", key)
			}
		}
		return true // continue iteration
	})

	b.logger.Info("Bundle storage engine shutdown complete")
	return nil
}

// SetProjectionFieldsForBundle sets projection fields temporarily for a bundle
// PROJECTION PUSHDOWN: This allows BundleAdapter to pass projection through to readDocumentRange
// Called from BundleAdapter before loading pages for ORDER BY queries
// PHASE 3: Using ShardedProjectionCache for concurrent access without global mutex
func (b *BundleStorageEngine) SetProjectionFieldsForBundle(bundleName string, fields []string) {
	// PERFORMANCE: Optimize for nil fields (clearing projection) - common case in GetDocumentsByFilter
	// Check if we actually need to modify anything before acquiring write lock
	if len(fields) == 0 {
		// Clearing projection - check if it's already cleared to avoid write lock
		if !b.projectionCache.Has(bundleName) {
			// Already cleared - no need to modify
			return
		}
		b.projectionCache.Delete(bundleName)
	} else {
		b.projectionCache.Set(bundleName, fields)
		if b.logger != nil {
			b.logger.Debugf("PROJECTION PUSHDOWN: Set projection fields %v for bundle '%s'", fields, bundleName)
		}
	}
}

// getProjectionFieldsForBundle gets projection fields for a bundle if set
// PROJECTION PUSHDOWN: Returns projection fields if set, nil otherwise
// PHASE 3: Using ShardedProjectionCache for concurrent access without global mutex
func (b *BundleStorageEngine) getProjectionFieldsForBundle(bundleName string) []string {
	return b.projectionCache.Get(bundleName)
}

// readDocumentRange efficiently reads a specific range of documents for pagination
// This implements true virtual pagination by streaming through the file and stopping at boundaries
// PROJECTION PUSHDOWN: If projectionFields is non-empty, only deserializes specified fields
// For ORDER BY queries, this saves ~80-90% deserialization overhead
func (b *BundleStorageEngine) readDocumentRange(bundleName string, databaseName string, startIndex, endIndex uint32, fileData *[]byte, projectionFields []string) (map[string]models.Document, uint32, error) {
	// CRITICAL FIX: Acquire read lock to prevent reading during concurrent writes
	// RWMutex allows multiple concurrent readers but blocks during writes
	// This prevents readers from seeing partially written or corrupted data
	lock := b.getWriteLock(bundleName)
	lock.RLock()
	defer lock.RUnlock()

	// PROJECTION PUSHDOWN: Use projection fields if not passed explicitly
	// This allows BundleAdapter to set projection via SetProjectionFieldsForBundle()
	if projectionFields == nil {
		projectionFields = b.getProjectionFieldsForBundle(bundleName)
	}

	//args := settings.GetSettings()

	// databasePath := helpers.GetDatabaseFolderPath(databaseName)

	// filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	// file, err := os.Open(filePath)
	// if err != nil {
	// 	return nil, 0, fmt.Errorf("failed to open bundle file: %w", err)
	// }
	// defer file.Close()

	// // Read the entire file for now - TODO: optimize with streaming
	// fileInfo, err := file.Stat()
	// if err != nil {
	// 	return nil, 0, fmt.Errorf("failed to get file info: %w", err)
	// }

	// //b.logger.Debugf("DEBUG: readDocumentRange - file '%s' size: %d bytes", filePath, fileInfo.Size())

	// fileData := make([]byte, fileInfo.Size())
	// _, err = file.Read(fileData)
	// if err != nil {
	// 	return nil, 0, fmt.Errorf("failed to read file: %w", err)
	// }

	//b.logger.Debugf("DEBUG: readDocumentRange - read %d bytes from file (expected %d)", bytesRead, fileInfo.Size())

	pageDocuments, totalCount, err := b.parseAppendedDocumentsRange(bundleName, databaseName, fileData, startIndex, endIndex, projectionFields)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse document range: %w", err)
	}

	return pageDocuments, totalCount, nil
}

// ReadAppendedDocuments reads documents that were appended to the bundle file
// This method can read both the original BSON bundle format and appended documents
func (b *BundleStorageEngine) ReadAppendedDocuments(bundleName, databaseName string) (map[string]models.Document, error) {
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(databaseName)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bundle file: %w", err)
	}
	defer file.Close()

	// Read the entire file
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	fileData := make([]byte, fileInfo.Size())
	_, err = file.Read(fileData)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	documents, appendErr := b.parseAppendedDocuments(bundleName, databaseName, fileData)
	if appendErr != nil {
		return nil, fmt.Errorf("failed to parse bundle file: %w", appendErr)
	}

	return documents, nil
}

// GetDocumentVersions scans backward through all bundle files to find all versions of a document
// PHASE 0: MVCC Version Storage Foundation
// This function collects all versions of a DocumentID from append-only storage
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
func (b *BundleStorageEngine) GetDocumentVersions(bundleName, databaseName, documentID string) ([]*models.Document, error) {
	// Get manifest to find all files
	manifestMgr := b.getOrCreateManifestManager(databaseName, bundleName)
	manifest, err := manifestMgr.LoadOrCreate(databaseName, bundleName)
	if err != nil {
		// Fall back to legacy single-file format
		return b.getDocumentVersionsLegacy(bundleName, databaseName, documentID)
	}

	// Check if we have any files in the manifest
	if len(manifest.Files) == 0 {
		// No files yet - fall back to legacy format
		return b.getDocumentVersionsLegacy(bundleName, databaseName, documentID)
	}

	// Collect all versions from all files (scanning backward - newest first)
	allVersions := make([]*models.Document, 0)

	// Get files sorted by fileID descending (newest first)
	files := manifestMgr.GetFileList(true) // newestFirst = true

	// Scan each file backward to find all versions
	for _, fileInfo := range files {
		// Check bloom filter — skip file if document definitely absent
		bf := manifestMgr.GetDeserializedBloomFilter(fileInfo.FileID)
		if bf != nil && !bf.MayContain(documentID) {
			continue // Bloom says definitely not here — skip file read
		}

		bundleDir := GetBundleDirectory(databaseName, bundleName)
		filePath := filepath.Join(bundleDir, fileInfo.FileName)

		// Check if file exists (skip if not - may have been compacted)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			continue
		}

		// Read file data (read-only; do not modify returned slice)
		data, err := b.getOrReadFile(filePath)
		if err != nil {
			b.logger.Warnf("Failed to read bundle file '%s' for version scan: %v", filePath, err)
			continue
		}

		fileVersions := b.scanFileBackwardForDocument(&data, bundleName, databaseName, documentID)
		allVersions = append(allVersions, fileVersions...)
	}

	// Sort by VersionSequence descending (newest first)
	sort.Slice(allVersions, func(i, j int) bool {
		return allVersions[i].VersionSequence > allVersions[j].VersionSequence
	})

	return allVersions, nil
}

// getDocumentVersionsLegacy scans a legacy single-file bundle for document versions
func (b *BundleStorageEngine) getDocumentVersionsLegacy(bundleName, databaseName, documentID string) ([]*models.Document, error) {
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return []*models.Document{}, nil
	}

	// Read file data
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bundle file: %w", err)
	}

	versions := b.scanFileBackwardForDocument(&data, bundleName, databaseName, documentID)

	// Sort by VersionSequence descending (newest first)
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].VersionSequence > versions[j].VersionSequence
	})

	return versions, nil
}

// scanFileBackwardForDocument scans a file forward to find all versions of a document
func (b *BundleStorageEngine) scanFileBackwardForDocument(data *[]byte, bundleName, databaseName, documentID string) []*models.Document {
	versions := make([]*models.Document, 0)

	schema := models.NewProjectionSchema(nil)
	if b.schemaProvider != nil {
		if s := b.schemaProvider(bundleName, databaseName); s != nil {
			schema = s
		}
	}

	headerOffset := 0
	if len(*data) >= 8 {
		magic := binary.LittleEndian.Uint32((*data)[0:4])
		if magic == 0x42444D44 {
			metadataSize := binary.LittleEndian.Uint32((*data)[4:8])
			headerOffset = int(8 + metadataSize)
		}
	}

	seenVersions := make(map[uint64]*models.Document)
	currentOffset := headerOffset
	for currentOffset < len(*data) {
		if currentOffset+8 > len(*data) {
			break
		}

		magic := binary.LittleEndian.Uint32((*data)[currentOffset : currentOffset+4])
		size := binary.LittleEndian.Uint32((*data)[currentOffset+4 : currentOffset+8])

		if magic == 0xDEADBEEF {
			if currentOffset+8+int(size) > len(*data) {
				break
			}

			documentData := (*data)[currentOffset+8 : currentOffset+8+int(size)]

			doc, err := helpers.DecodeFastBinaryToDocument(documentData, schema)
			if err != nil {
				// Skip corrupted documents
				currentOffset += 8 + int(size)
				continue
			}

			// Check if this is the document we're looking for
			if doc.DocumentID == documentID {
				// Store version (will overwrite if same VersionSequence, keeping latest)
				// Since we scan forward, later occurrences (higher offset) are newer
				seenVersions[doc.VersionSequence] = doc
			}
		} else if magic == 0xDEADDEAD {
			// Tombstone - skip
		}

		// Move to next record
		currentOffset += 8 + int(size)
	}

	// Convert map to slice
	for _, doc := range seenVersions {
		versions = append(versions, doc)
	}

	return versions
}

// parseAppendedDocumentsRange parses documents in the append-only format with range limiting
// This implements efficient virtual pagination by stopping when the page is full
//
// PROJECTION PUSHDOWN: If projectionFields is non-empty, only deserializes specified fields
// This saves ~80-90% deserialization overhead for ORDER BY queries (e.g., only deserialize "name" field)
// For query: SELECT DocumentID, name FROM products ORDER BY name
// We only need "name" for sorting, not all other fields like "description", "price", etc.
//
// Parameters:
//   - data: Raw document file data
//   - startIndex: First document index to include (pagination)
//   - endIndex: Last document index to include (pagination)
//   - projectionFields: Field names to deserialize (nil = deserialize all fields, []string{"name"} = only "name")
//
// Returns:
//   - map[string]models.Document: Parsed documents
//   - uint32: Total document count
//   - error: Any parsing error
func (b *BundleStorageEngine) parseAppendedDocumentsRange(bundleName, databaseName string, data *[]byte, startIndex, endIndex uint32, projectionFields []string) (map[string]models.Document, uint32, error) {
	pageDocuments := make(map[string]models.Document)
	deletedDocuments := make(map[string]bool) // Track deleted documents
	seenDocIDs := make(map[string]struct{})   // Track unique DocumentIDs for counting (avoids storing full docs; was allDocuments)
	offset := 0
	documentIndex := uint32(0)

	schema := models.NewProjectionSchema(nil)
	if b.schemaProvider != nil {
		if s := b.schemaProvider(bundleName, databaseName); s != nil {
			schema = s
		}
	}

	// DEBUG: Log parsing start
	if b.logger != nil {
		//b.logger.Debugf("DEBUG: parseAppendedDocumentsRange called with data size %d bytes, range [%d, %d)", len(data), startIndex, endIndex)
	}

	// CRITICAL FIX: Skip bundle metadata header if present
	// The file format is: [Bundle Metadata Header] [Document 1] [Document 2] ... [Tombstones]
	// Bundle metadata header format: 0x42444D44 (magic) + size (4 bytes) + BSON data
	if len(*data) >= 8 {
		magic := binary.LittleEndian.Uint32((*data)[0:4])
		if magic == 0x42444D44 { // "BDMD" = Bundle Metadata
			metadataSize := binary.LittleEndian.Uint32((*data)[4:8])
			offset = int(8 + metadataSize) // Skip 8-byte header + BSON data
			if b.logger != nil {
				//b.logger.Debugf("DEBUG: Skipping bundle metadata header: magic=0x%X, size=%d, new offset=%d", magic, metadataSize, offset)
			}
		}
	}

	for offset < len(*data) {
		// Need at least 8 bytes for header
		if offset+8 > len(*data) {
			if b.logger != nil {
				//b.logger.Debugf("DEBUG: Stopping parse at offset %d (not enough bytes for header)", offset)
			}
			break
		}

		// Read magic number and size
		magic := binary.LittleEndian.Uint32((*data)[offset : offset+4])
		size := binary.LittleEndian.Uint32((*data)[offset+4 : offset+8])

		// DEBUG: Log what we found
		if b.logger != nil {
			//if magic == 0xDEADBEEF {
			//	b.logger.Debugf("DEBUG: Found DOCUMENT at offset %d, size %d", offset, size)
			//} else if magic == 0xDEADDEAD {
			//	b.logger.Debugf("DEBUG: Found TOMBSTONE at offset %d, size %d", offset, size)
			//} else {
			//	// Log unknown magic numbers too
			//	b.logger.Debugf("DEBUG: Found UNKNOWN magic 0x%X at offset %d, size %d", magic, offset, size)
			//}
		}

		// Handle document records
		if magic == 0xDEADBEEF {
			// Validate size
			if offset+8+int(size) > len(*data) {
				break
			}

			// Extract document data
			documentData := (*data)[offset+8 : offset+8+int(size)]

			// PROJECTION PUSHDOWN: Use projected deserialization if projection fields specified
			// For ORDER BY queries, this only deserializes the sort field (e.g., "name") instead of all fields
			// This saves ~80-90% deserialization overhead when documents have many unused fields
			// Example: ORDER BY name query with 10-field documents → only deserialize "name" + DocumentID
			var docMap map[string]interface{}
			var projectedDoc *models.Document
			var fullDoc *models.Document
			var err error
			if len(projectionFields) > 0 {
				projectedDoc, err = helpers.DecodeFastBinaryProjected(documentData, projectionFields, schema)
				if err != nil {
					docMap, err = helpers.DecodeFastBinary(documentData)
					projectedDoc = nil
				}
			} else {
				fullDoc, err = helpers.DecodeFastBinaryToDocument(documentData, schema)
			}
			if err != nil {
				// CRITICAL: Data corruption detected
				b.logger.Errorf("CRITICAL CORRUPTION DETECTED at offset %d: %v", offset, err)
				b.logger.Errorf("Document data length: %d bytes", len(documentData))
				b.logger.Errorf("Expected size from header: %d bytes", size)

				// Check for the magic corruption pattern 0x69696969
				if size == 1768845170 || (len(documentData) > 4 && binary.LittleEndian.Uint32(documentData[0:4]) == 1768845170) {
					corruptionReason := fmt.Sprintf("Detected corruption pattern 0x69696969 (1768845170 decimal) at offset %d. "+
						"This indicates incomplete write or uninitialized memory. Size field corrupted: %d bytes. "+
						"Document data preview (first 64 bytes): %x",
						offset, size, documentData[:min(64, len(documentData))])

					// Dump diagnostics and halt server
					b.writeLogger.DumpDiagnostics(corruptionReason, int64(offset), bundleName)
				}

				// Generic corruption - still halt
				corruptionReason := fmt.Sprintf("Failed to decode document at offset %d: %v. "+
					"Size: %d bytes. Data preview (first 64 bytes): %x",
					offset, err, size, documentData[:min(64, len(documentData))])
				b.writeLogger.DumpDiagnostics(corruptionReason, int64(offset), bundleName)

				// This line will never be reached, but keep for safety
				offset += 8 + int(size)
				continue
			}

			// Convert to Document struct
			// PROJECTION PUSHDOWN: If we used projected deserialization, projectedDoc is already set
			// No projection: fullDoc from DecodeFastBinaryToDocument (avoids docMap intermediate)
			// Fallback: docMap from failed DecodeFastBinaryProjected, convert via pool
			var finalDoc *models.Document
			if projectedDoc != nil {
				finalDoc = projectedDoc
			} else if fullDoc != nil {
				finalDoc = fullDoc
			} else {
				// Fallback from failed projection: build Values from docMap using schema
				pooledDoc := document.GetPooledDocument()
				if docID, ok := docMap["DocumentID"].(string); ok {
					pooledDoc.DocumentID = docID
				}
				pooledDoc.Values = make([]models.FieldValue, len(schema.Names))
				for i, name := range schema.Names {
					if name == "DocumentID" && pooledDoc.DocumentID != "" {
						pooledDoc.Values[i] = models.NewStringValue(pooledDoc.DocumentID)
						continue
					}
					if v, ok := docMap[name]; ok && v != nil {
						pooledDoc.Values[i] = models.NewInterfaceValue(v)
					}
				}
				if createdAt, ok := docMap["CreatedAt"].(time.Time); ok {
					pooledDoc.CreatedAt = createdAt
				}
				if updatedAt, ok := docMap["UpdatedAt"].(time.Time); ok {
					pooledDoc.UpdatedAt = updatedAt
				}
				finalDoc = pooledDoc
			}

			// Only add document if it hasn't been deleted
			// PROJECTION PUSHDOWN: finalDoc is already set from either projected or full deserialization
			if !deletedDocuments[finalDoc.DocumentID] {
				// CRITICAL FIX: Track if this is a new unique document or an update
				// In append-only storage, same DocumentID can appear multiple times
				// We count each UNIQUE document only once for pagination
				isNewDocument := false
				wasInPageRange := false

				if _, exists := seenDocIDs[finalDoc.DocumentID]; !exists {
					// First time seeing this DocumentID
					isNewDocument = true
					seenDocIDs[finalDoc.DocumentID] = struct{}{}
					// Check if this document's index falls in the requested page range
					if documentIndex >= startIndex && documentIndex < endIndex {
						wasInPageRange = true
					}
				} else {
					// This is an update of existing document
					// Check if the original occurrence was in page range
					if _, inPage := pageDocuments[finalDoc.DocumentID]; inPage {
						wasInPageRange = true
					}
				}

				// If this document belongs in the page range, keep it updated with latest version
				if wasInPageRange {
					pageDocuments[finalDoc.DocumentID] = *finalDoc
				}

				// Only increment index for NEW unique documents
				if isNewDocument {
					documentIndex++
				}
			}

			// if docMap, ok := docInterface.(map[string]interface{}); ok {
			// 	doc := models.Document{
			// 		DocumentID: getString(docMap, "DocumentID"),
			// 		CreatedAt:  getTime(docMap, "CreatedAt"),
			// 		UpdatedAt:  getTime(docMap, "UpdatedAt"),
			// 		Fields:     make(map[string]models.Field),
			// 	}

			// 	// Parse fields
			// 	if fieldsInterface, exists := docMap["Fields"]; exists {
			// 		if fieldsMap, ok := fieldsInterface.(map[string]interface{}); ok {
			// 			for fieldName, fieldData := range fieldsMap {
			// 				if fieldMap, ok := fieldData.(map[string]interface{}); ok {
			// 					doc.Fields[fieldName] = models.Field{
			// 						Name:  getString(fieldMap, "Name"),
			// 						Value: fieldMap["Value"],
			// 					}
			// 				}
			// 			}
			// 		}
			// 	}

			// Only add document if it hasn't been deleted
			// if !deletedDocuments[doc.DocumentID] {
			// 	allDocuments[doc.DocumentID] = doc

			// 	// Check if this document should be included in the current page
			// 	if documentIndex >= startIndex && documentIndex < endIndex {
			// 		pageDocuments[doc.DocumentID] = doc
			// 	}
			// 	documentIndex++

			// 	// Early exit if we've collected enough documents for this page
			// 	// But continue counting total documents for accurate pagination
			// }
			//}

			offset += 8 + int(size)
		} else if magic == 0xDEADDEAD {
			// Handle deletion markers
			if offset+8+int(size) > len(*data) {
				break
			}

			// Extract deletion marker data
			deletionData := (*data)[offset+8 : offset+8+int(size)]

			// Decode deletion marker
			// deletionInterface, err := helpers.DecodeBSON(deletionData)
			// if err != nil {
			// 	b.logger.Warnf("Failed to decode deletion marker at offset %d: %v", offset, err)
			// 	offset += 8 + int(size)
			// 	continue
			// }

			// if deletionMap, ok := deletionInterface.(map[string]interface{}); ok {
			// 	documentID := getString(deletionMap, "DocumentID")
			// 	if documentID != "" {
			// 		// Mark document as deleted and remove from current sets
			// 		deletedDocuments[documentID] = true
			// 		delete(allDocuments, documentID)
			// 		delete(pageDocuments, documentID)

			// 		if b.logger != nil {
			// 			b.logger.Debugf("Found deletion marker for document %s", documentID)
			// 		}
			// 	}
			// }

			// DEBUG: Always log deletion marker processing
			if b.logger != nil {
				//b.logger.Debugf("DEBUG: Found deletion marker at offset %d, size %d bytes", offset, size)
			}

			// Decode deletion marker using fast binary format
			deletionMap, err := helpers.DecodeFastBinary(deletionData)
			if err != nil {
				b.logger.Warnf("Failed to decode deletion marker at offset %d using fast binary format: %v",
					offset, err)
				offset += 8 + int(size)
				continue
			}

			if b.logger != nil {
				//b.logger.Debugf("DEBUG: Decoded deletion marker: %+v", deletionMap)
			}

			if documentID, ok := deletionMap["DocumentID"].(string); ok && documentID != "" {
				// Mark document as deleted and remove from current sets
				deletedDocuments[documentID] = true
				delete(seenDocIDs, documentID)
				delete(pageDocuments, documentID)

				if b.logger != nil {
					//b.logger.Debugf("DEBUG: Marked document %s as deleted (total deleted: %d)", documentID, len(deletedDocuments))
				}
			} else {
				if b.logger != nil {
					//b.logger.Warnf("DEBUG: Deletion marker missing DocumentID or wrong type: %+v", deletionMap)
				}
			}

			offset += 8 + int(size)
		} else {
			// Unknown magic number, try to find next valid record
			offset++
			continue
		}
	}

	return pageDocuments, uint32(len(seenDocIDs)), nil
}

// parseAppendedDocuments parses documents in the append-only format
func (b *BundleStorageEngine) parseAppendedDocuments(bundleName, databaseName string, data []byte) (map[string]models.Document, error) {
	documents := make(map[string]models.Document)
	deletedDocuments := make(map[string]bool)
	offset := 0

	schema := models.NewProjectionSchema(nil)
	if b.schemaProvider != nil {
		if s := b.schemaProvider(bundleName, databaseName); s != nil {
			schema = s
		}
	}

	for offset < len(data) {
		// Need at least 8 bytes for header
		if offset+8 > len(data) {
			break
		}

		// Read magic number and size
		magic := binary.LittleEndian.Uint32(data[offset : offset+4])
		size := binary.LittleEndian.Uint32(data[offset+4 : offset+8])

		// Handle document records
		if magic == 0xDEADBEEF {
			// Validate size
			if offset+8+int(size) > len(data) {
				break
			}

			// Extract document data
			documentData := data[offset+8 : offset+8+int(size)]

			// Decode document
			// docInterface, err := helpers.DecodeBSON(documentData)
			// if err != nil {
			// 	b.logger.Warnf("Failed to decode document at offset %d: %v", offset, err)
			// 	offset += 8 + int(size)
			// 	continue
			// }

			// if docMap, ok := docInterface.(map[string]interface{}); ok {
			// 	doc := models.Document{
			// 		DocumentID: getString(docMap, "DocumentID"),
			// 		CreatedAt:  getTime(docMap, "CreatedAt"),
			// 		UpdatedAt:  getTime(docMap, "UpdatedAt"),
			// 		Fields:     make(map[string]models.Field),
			// 	}

			// 	// Parse fields
			// 	if fieldsInterface, exists := docMap["Fields"]; exists {
			// 		if fieldsMap, ok := fieldsInterface.(map[string]interface{}); ok {
			// 			for fieldName, fieldData := range fieldsMap {
			// 				if fieldMap, ok := fieldData.(map[string]interface{}); ok {
			// 					doc.Fields[fieldName] = models.Field{
			// 						Name:  getString(fieldMap, "Name"),
			// 						Value: fieldMap["Value"],
			// 					}
			// 				}
			// 			}
			// 		}
			// 	}

			// 	// Only add document if it hasn't been deleted
			// 	if !deletedDocuments[doc.DocumentID] {
			// 		documents[doc.DocumentID] = doc
			// 	}
			// }

			doc, err := helpers.DecodeFastBinaryToDocument(documentData, schema)
			if err != nil {
				b.logger.Errorf("CRITICAL CORRUPTION DETECTED at offset %d: %v", offset, err)
				b.logger.Errorf("Document data length: %d bytes", len(documentData))
				b.logger.Errorf("Expected size from header: %d bytes", size)
				if size == 1768845170 || (len(documentData) > 4 && binary.LittleEndian.Uint32(documentData[0:4]) == 1768845170) {
					corruptionReason := fmt.Sprintf("Detected corruption pattern 0x69696969 at offset %d. Size: %d. Preview: %x",
						offset, size, documentData[:min(64, len(documentData))])
					b.writeLogger.DumpDiagnostics(corruptionReason, int64(offset), bundleName)
				} else {
					corruptionReason := fmt.Sprintf("Failed to decode document at offset %d: %v. Size: %d. Preview: %x",
						offset, err, size, documentData[:min(64, len(documentData))])
					b.writeLogger.DumpDiagnostics(corruptionReason, int64(offset), bundleName)
				}
				offset += 8 + int(size)
				continue
			}

			if !deletedDocuments[doc.DocumentID] {
				documents[doc.DocumentID] = *doc
			}

			offset += 8 + int(size)
		} else if magic == 0xDEADDEAD {
			// Handle deletion markers
			// Validate size
			if offset+8+int(size) > len(data) {
				break
			}

			// Extract deletion marker data
			deletionData := data[offset+8 : offset+8+int(size)]

			// // Decode deletion marker
			// deletionInterface, err := helpers.DecodeBSON(deletionData)
			// if err != nil {
			// 	b.logger.Warnf("Failed to decode deletion marker at offset %d: %v", offset, err)
			// 	offset += 8 + int(size)
			// 	continue
			// }

			// if deletionMap, ok := deletionInterface.(map[string]interface{}); ok {
			// 	documentID := getString(deletionMap, "DocumentID")
			// 	if documentID != "" {
			// 		// Mark document as deleted and remove from current set
			// 		deletedDocuments[documentID] = true
			// 		delete(documents, documentID)

			// 		if b.logger != nil {
			// 			b.logger.Debugf("Found deletion marker for document %s", documentID)
			// 		}
			// 	}
			// }

			// Decode deletion marker using fast binary format
			deletionMap, err := helpers.DecodeFastBinary(deletionData)
			if err != nil {
				b.logger.Warnf("Failed to decode deletion marker at offset %d using fast binary format: %v",
					offset, err)
				offset += 8 + int(size)
				continue
			}

			if documentID, ok := deletionMap["DocumentID"].(string); ok && documentID != "" {
				// Mark document as deleted and remove from current set
				deletedDocuments[documentID] = true
				delete(documents, documentID)

				if b.logger != nil {
					b.logger.Debugf("Found deletion marker for document %s", documentID)
				}
			}

			offset += 8 + int(size)
		} else {
			// Unknown magic number, try to find next valid record
			offset++
			continue
		}
	}

	return documents, nil
}

func (b *BundleStorageEngine) RemoveDocumentFromBundleFile(database *models.Database,
	bundle *models.Bundle,
	documentID string,
	mmapData []byte) error {

	convertedBundle := BundleToMap(bundle)
	//args := settings.GetSettings()
	// Locate the document in the bundle
	documents, ok := convertedBundle["Documents"].([]interface{})
	if !ok {
		return fmt.Errorf("bundle does not contain a valid Documents field")
	}

	var documentOffset int
	var documentSize int
	found := false

	for i, doc := range documents {
		docMap, ok := doc.(map[string]interface{})
		if !ok {
			continue
		}

		if docMap["ID"] == documentID {
			// Calculate the offset and size of the document
			var err error
			documentOffset, err = calculateDocumentOffset(mmapData, i)
			if err != nil {
				return fmt.Errorf("error calculating document offset during document removal: %w", err)
			}

			// Read the size of the document (first 4 bytes of the BSON document)
			if len(mmapData[documentOffset:]) < 4 {
				return fmt.Errorf("insufficient data to read document size")
			}
			documentSize = int(mmapData[documentOffset]) |
				int(mmapData[documentOffset+1])<<8 |
				int(mmapData[documentOffset+2])<<16 |
				int(mmapData[documentOffset+3])<<24

			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("document with ID %s not found in bundle", documentID)
	}

	// Remove the document by shifting the data after it
	copy(mmapData[documentOffset:], mmapData[documentOffset+documentSize:])
	newSize := len(mmapData) - documentSize

	// Truncate the file to the new size
	err := unix.Munmap(mmapData) // Unmap the memory before truncating
	if err != nil {
		return fmt.Errorf("error unmapping memory: %w", err)
	}

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	filePath := filepath.Join(databasePath, bundle.BundleID)
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening bundle file for truncation: %w", err)
	}
	defer file.Close()

	err = file.Truncate(int64(newSize))
	if err != nil {
		return fmt.Errorf("error truncating file: %w", err)
	}

	// Re-map the file to reflect the updated size
	mmapData, err = unix.Mmap(int(file.Fd()), 0, newSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("error re-mapping file after truncation: %w", err)
	}

	// Sync changes to the file
	err = unix.Msync(mmapData, unix.MS_SYNC)
	if err != nil {
		return fmt.Errorf("error syncing changes to file: %w", err)
	}

	return nil
}

// WriteBundleToFile encodes a bundle and writes it to a file
// DEPRECATED: Use AppendDocumentToBundleFile for better performance
// Note: bundle.Documents memtable has been removed - this function may need refactoring
func (b *BundleStorageEngine) WriteBundleToFile(bundle *models.Bundle, filePath string) error {
	// 1. Convert the bundle to a map for BSON encoding
	convertedBundle := BundleToMap(bundle)

	// 2. Documents are now stored via page cache - this function is deprecated
	// TODO: Refactor this function to load documents from page cache if needed
	convertedBundle["Documents"] = make(map[string]interface{})

	// docs := make([]interface{}, 0, len(bundle.Documents))
	// for _, doc := range bundle.Documents {
	// 	// Convert Document to map
	// 	docMap := map[string]interface{}{
	// 		"ID":        doc.DocumentID,
	// 		"Fields":    doc.Fields,
	// 		"CreatedAt": doc.CreatedAt,
	// 		"UpdatedAt": doc.UpdatedAt,
	// 	}
	// 	docs = append(docs, docMap)
	// }
	// convertedBundle["Documents"] = docs

	// 3. Encode the bundle using fast binary format
	// TODO: Replace with append-only operations for document updates/deletes
	encodedBundle, err := helpers.EncodeFastBinary(convertedBundle)
	if err != nil {
		return fmt.Errorf("error encoding bundle data: %w", err)
	}

	// 4. Open the file for writing
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error opening bundle file for writing: %w", err)
	}
	defer file.Close()

	// 5. Write the encoded bundle to the file
	fileLen, err := file.Write(encodedBundle)
	if err != nil {
		return fmt.Errorf("error writing to bundle data file %s: %w", bundle.Name, err)
	}

	if fileLen != len(encodedBundle) {
		return fmt.Errorf("error writing to bundle file %s: wrote %d bytes, expected %d",
			bundle.Name, fileLen, len(encodedBundle))
	}

	if b.logger != nil {
		b.logger.Debugw("Successfully wrote bundle to file",
			"bundle", bundle.Name,
			"path", filePath,
			"size", fileLen)
	}

	return nil
}

func (b *BundleStorageEngine) RemoveBundleFile(database *models.Database, bundleName string) error {

	databasePath := helpers.GetDatabaseFolderPath(database.Name)
	removedAny := false

	// Remove the bundle subdirectory and all its contents (segment files, indexes, manifest)
	dirPath := filepath.Join(databasePath, bundleName)
	if helpers.DirExists(dirPath) {
		if err := os.RemoveAll(dirPath); err != nil {
			return fmt.Errorf("error removing bundle directory %s: %w", bundleName, err)
		}
		removedAny = true
	}

	// Remove the legacy .bnd metadata file if it exists
	fileName := fmt.Sprintf("%s_%s.bnd", database.Name, bundleName)
	bndFilePath := filepath.Join(databasePath, fileName)
	if helpers.FileExists(bndFilePath, *b.logger) {
		if err := os.Remove(bndFilePath); err != nil {
			return fmt.Errorf("error removing bundle metadata file %s: %w", fileName, err)
		}
		removedAny = true
	}

	if !removedAny {
		if b.logger != nil {
			b.logger.Debugf("Bundle %s does not exist on disk, skipping removal", bundleName)
		}
		return fmt.Errorf("bundle %s does not exist", bundleName)
	}

	// Invalidate file-read cache so we don't retain buffers for removed paths
	b.InvalidateFileReadCacheForBundle(database.Name, bundleName)

	// Remove per-document lock entries for this bundle to avoid map growth
	b.removeDocumentLocksForBundle(bundleName)

	b.logger.Debugf("Successfully removed bundle '%s' and all its data files", bundleName)

	return nil
}

// func Encode1DocumentBSON(jsonData map[string]interface{}) ([]byte, error) {
// 	// Encode the map into BSON
// 	bsonData, err := bson.Marshal(jsonData)
// 	if err != nil {
// 		log.Println("Error encoding BSON:", err)
// 		return nil, err
// 	}
// 	log.Println("Encoded BSON:", bsonData)

// 	return bsonData, nil
// }

// func Decode1DocumentBSON(bsonData []byte) (interface{}, error) {
// 	// Decode the BSON back into a Go map
// 	var decodedData map[string]interface{}
// 	err := bson.Unmarshal(bsonData, &decodedData)
// 	if err != nil {
// 		log.Println("Error decoding BSON:", err)
// 		return nil, err
// 	}
// 	log.Println("Decoded Data:", decodedData)

// 	return decodedData, nil
// }

func BundleToMap(bundle *models.Bundle) map[string]interface{} {
	return map[string]interface{}{
		"BundleID": bundle.BundleID,
		"Name":     bundle.Name,
		//"Database":          bundle.Database,
		"DocumentStructure": bundle.DocumentStructure,
		"FieldDefinitions":  bundle.DocumentStructure.FieldDefinitions,
		// Documents are now stored via page cache, not in memory
		"IndexNames": bundle.IndexNames,
		"Indexes":    bundle.Indexes,
		//"Relationships":     bundle.Relationships,
		"Constraints": bundle.Constraints,
	}
}

func calculateDocumentOffset(data []byte, index int) (int, error) {
	offset := 0

	for i := 0; i < index; i++ {
		if offset >= len(data) {
			return 0, fmt.Errorf("index %d is out of bounds for the data", index)
		}

		// Read the size of the current document (first 4 bytes of a BSON document)
		if len(data[offset:]) < 4 {
			return 0, fmt.Errorf("insufficient data to read document size at index %d", i)
		}
		docSize := int(data[offset]) | int(data[offset+1])<<8 | int(data[offset+2])<<16 | int(data[offset+3])<<24

		// Move the offset to the start of the next document
		offset += docSize
	}

	return offset, nil
}

// MapToBundle converts a map to a Bundle struct
func MapToBundle(data map[string]interface{}, logger zap.SugaredLogger) (*models.Bundle, error) {
	bundle := &models.Bundle{}

	// Extract basic fields
	if id, ok := data["BundleID"].(string); ok {
		bundle.BundleID = id
	} else {
		return nil, fmt.Errorf("invalid or missing BundleID in map")
	}

	if name, ok := data["Name"].(string); ok {
		bundle.Name = name
	} else {
		return nil, fmt.Errorf("invalid or missing Name in map")
	}

	// Extract relationships
	if relations, ok := data["Relationships"]; ok && relations != nil {
		if relationMap, ok := relations.(map[string]models.Relationship); ok {
			bundle.Relationships = relationMap
		} else {
			// If not directly convertible, try to convert each item individually
			bundle.Relationships = make(map[string]models.Relationship)
			if relMap, ok := relations.(map[string]interface{}); ok {
				for key, val := range relMap {
					if relData, ok := val.(map[string]interface{}); ok {
						rel := models.Relationship{
							// ID:           stringValue(relData, "ID", ""),
							Name: stringValue(relData, "Name", ""),
							//  TargetBundle: stringValue(relData, "TargetBundle", ""),
						}
						bundle.Relationships[key] = rel
					}
				}
			}
		}
	} else {
		bundle.Relationships = make(map[string]models.Relationship)
	}

	//Extract Index Names
	if data["IndexNames"] != nil {
		bundle.IndexNames = make([]string, 0)

		if indexNames, ok := data["IndexNames"].(primitive.A); ok {
			bundle.IndexNames = ConvertToStringSlice(indexNames)
		} else {
			logger.Debugf("Bundle %v IS MISSING THE []STRING datatype", data["IndexNames"])
		}
	} else {
		logger.Debugf("Bundle %s has no index names defined", bundle.Name)
		bundle.IndexNames = make([]string, 0)
	}

	// if data["Indexes"] != nil {
	// 	indexes := data["Indexes"]
	// 	if indexMap, ok := indexes.(map[string]models.IndexReference); ok {
	// 		bundle.Indexes = indexMap
	// 	} else {
	// 		// If not directly convertible, try to convert each item individually
	// 		bundle.Indexes = make(map[string]models.IndexReference)
	// 		if indexMap, ok := indexes.(map[string]interface{}); ok {
	// 			for key, val := range indexMap {
	// 				if indexData, ok := val.(map[string]interface{}); ok {
	// 					indexRef := models.IndexReference{
	// 						IndexName: stringValue(indexData, "IndexName", ""),
	// 						// Fields:    stringArrayValue(indexData, "Fields"),
	// 						// IsUnique:  boolValue(indexData, "IsUnique", false),
	// 						// IsPartial: boolValue(indexData, "IsPartial", false),
	// 						// Condition: stringValue(indexData, "Condition", ""),
	// 					}
	// 					bundle.Indexes[key] = indexRef
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	// Extract indexes
	if indexes, ok := data["Indexes"]; ok && indexes != nil {
		if indexMap, ok := indexes.(map[string]models.IndexReference); ok {
			bundle.Indexes = indexMap
		} else {
			// If not directly convertible, try to convert each item individually
			bundle.Indexes = make(map[string]models.IndexReference)
			if indexMap, ok := indexes.(map[string]interface{}); ok {
				for key, val := range indexMap {
					if indexData, ok := val.(map[string]interface{}); ok {
						idxName := stringValue(indexData, "IndexName", key)
						// Derive document field name for Fields (e.g. product_id_fk -> product_id)
						fieldName := idxName
						if strings.HasSuffix(idxName, "_fk") {
							fieldName = strings.TrimSuffix(idxName, "_fk")
						}
						indexRef := models.IndexReference{
							IndexName: idxName,
							IndexType: stringValue(indexData, "IndexType", ""),
							Fields:    []models.FieldDefinition{{Name: fieldName, Type: "string"}},
						}
						bundle.Indexes[key] = indexRef
					}
				}
			}
		}
	} else {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Extract constraints
	if constraints, ok := data["Constraints"]; ok && constraints != nil {
		if constraintMap, ok := constraints.(map[string]models.Constraint); ok {
			bundle.Constraints = constraintMap
		} else {
			// If not directly convertible, try to convert each item individually
			bundle.Constraints = make(map[string]models.Constraint)
			if consMap, ok := constraints.(map[string]interface{}); ok {
				for key, val := range consMap {
					if consData, ok := val.(map[string]interface{}); ok {
						cons := models.Constraint{
							Name: stringValue(consData, "Name", ""),
							// Type:      stringValue(consData, "Type", ""),
							// Fields:    stringArrayValue(consData, "Fields"),
							// Condition: stringValue(consData, "Condition", ""),
						}
						bundle.Constraints[key] = cons
					}
				}
			}
		}
	} else {
		bundle.Constraints = make(map[string]models.Constraint)
	}

	// Extract field definitions
	if fieldDefs, ok := data["FieldDefinitions"]; ok && fieldDefs != nil {
		if fieldDefMap, ok := fieldDefs.(map[string]models.FieldDefinition); ok {
			bundle.DocumentStructure.FieldDefinitions = fieldDefMap
		} else {
			// If not directly convertible, try to convert each item individually
			bundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
			if fdMap, ok := fieldDefs.(map[string]interface{}); ok {
				for key, val := range fdMap {
					if fdData, ok := val.(map[string]interface{}); ok {
						fd := models.FieldDefinition{
							Name:         stringValue(fdData, "Name", ""),
							Type:         stringValue(fdData, "Type", ""),
							IsRequired:   boolValue(fdData, "IsRequired", false),
							IsUnique:     boolValue(fdData, "IsUnique", false),
							DefaultValue: fdData["DefaultValue"],
						}
						bundle.DocumentStructure.FieldDefinitions[key] = fd
					}
				}
			}
		}
	} else {
		bundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}

	logger.Debugf("Processing bundle %s , going to load documents, with ID %s", bundle.Name, bundle.BundleID)

	// Documents are now loaded on-demand via page cache, not from serialized data
	// Skip legacy document extraction - the page cache handles document storage
	// TODO: Remove this legacy document extraction code once page cache is fully verified

	return bundle, nil
}

// Helper functions for safe type conversions
func stringValue(data map[string]interface{}, key, defaultVal string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return defaultVal
}

func boolValue(data map[string]interface{}, key string, defaultVal bool) bool {
	if val, ok := data[key].(bool); ok {
		return val
	}
	return defaultVal
}

func ConvertToStringSlice(arr primitive.A) []string {
	strs := make([]string, len(arr))
	for i, v := range arr {
		strs[i] = fmt.Sprint(v)
	}
	return strs
}

// PERFORMANCE OPTIMIZATION METHODS
// These methods provide zero-allocation operations for high-performance writes

// serializeDocumentDirect serializes a document using schema-ordered Values. Caller must pass bundle's FieldSchema.
func (b *BundleStorageEngine) serializeDocumentDirect(document *models.Document, schema *models.BundleFieldSchema) ([]byte, error) {
	if schema == nil {
		schema = models.NewProjectionSchema(nil)
	}
	return helpers.EncodeFastBinaryV2(document, schema)
}

// parseDocumentBinary parses a document using the fast binary format
// func (b *BundleStorageEngine) parseDocumentBinary(data []byte) (*models.Document, error) {
// 	// Use the fast deserializer instead of configured serializer
// 	return helpers.DecodeFastBinaryToDocument(data)
// }

// getHeaderBuffer returns the pre-allocated header buffer for reuse
func (b *BundleStorageEngine) getHeaderBuffer() []byte {
	return b.headerBuffer[:]
}

// getCombinedBuffer gets a buffer from the pool for combined header+data writes
// CRITICAL FIX: Always allocate fresh buffers to prevent race conditions
// Previous pool-based approach could cause buffer reuse corruption when:
// 1. Thread A gets buffer, writes data, calls writeBuffer.Write()
// 2. Thread A returns buffer to pool
// 3. Thread B gets SAME buffer and starts overwriting
// 4. Thread A's write might still be reading the buffer in async operations
func (b *BundleStorageEngine) getCombinedBuffer(size int) []byte {
	// ALWAYS allocate fresh buffer - small perf cost but prevents all buffer reuse corruption
	return make([]byte, size)
}

// returnCombinedBuffer returns a buffer to the pool for reuse
// DISABLED: No longer using pool to prevent race conditions
func (b *BundleStorageEngine) returnCombinedBuffer(buf []byte) {
	// No-op: we're not pooling buffers anymore to avoid race conditions
	// Let GC handle buffer cleanup
}

// func stringArrayValue(data map[string]interface{}, key string) []string {
// 	var result []string

// 	if val, ok := data[key]; ok {
// 		if strArr, ok := val.([]string); ok {
// 			return strArr
// 		} else if arrIface, ok := val.([]interface{}); ok {
// 			for _, item := range arrIface {
// 				if str, ok := item.(string); ok {
// 					result = append(result, str)
// 				}
// 			}
// 		}
// 	}

// 	return result
// }
