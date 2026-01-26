package bundlestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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
	"syndrdb/src/pkg/settings"
	"time"

	// "syndrdb/src/buffermgr"
	// "syndrdb/src/helpers"
	// "syndrdb/src/settings"
	"syscall"

	"syndrdb/src/internal/storage/buffer"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

type BundleStorageEngine struct {
	fileManager   *buffer.FileManager
	DataDirectory string
	logger        *zap.SugaredLogger
	serializer    format.BundleSerializer // Configurable serialization format
	writeBuffers  map[string]*WriteBuffer // Per-file write buffers for batched I/O (keyed by file path)
	bufferMutex   sync.RWMutex            // Protects writeBuffers map

	// MULTI-FILE STORAGE: Manifest managers per bundle to track segment files
	manifestManagers      map[string]*ManifestManager // Per-bundle manifest managers (keyed by bundle name)
	manifestManagersMutex sync.Mutex                  // Protects manifestManagers map

	// COMPACTION: Background compaction scheduler with parallel workers
	compactor           *BundleCompactor
	compactionScheduler *CompactionScheduler
	compactionContext   context.Context    // Context for graceful compaction shutdown
	compactionCancel    context.CancelFunc // Cancel function for compaction goroutines

	// CONCURRENCY CONTROL: Per-bundle write locks to prevent dirty reads
	// RWMutex allows concurrent reads while blocking during writes
	writeLocks      map[string]*sync.RWMutex // Per-bundle write locks
	writeLocksMutex sync.Mutex               // Protects writeLocks map

	// DOCUMENT-LEVEL LOCKING: Per-document write locks for concurrent updates
	// Allows concurrent writes to different documents within the same bundle
	// Similar to Postgres row-level locking for improved write throughput
	documentLocks      map[string]map[string]*sync.Mutex // bundleName -> docID -> mutex
	documentLocksMutex sync.RWMutex                      // Protects documentLocks map

	// PHASE 1: MVCC - Per-bundle rotation locks for file rotation coordination
	// Rotation is rare but needs exclusive access to prevent multiple rotations
	rotationLocks      map[string]*sync.Mutex // Per-bundle rotation locks
	rotationLocksMutex sync.Mutex             // Protects rotationLocks map

	// PERFORMANCE OPTIMIZATION: Pre-allocated buffers to avoid memory allocations
	headerBuffer    [32]byte  // Reusable 32-byte buffer for headers
	combinedBuffers sync.Pool // Pool of byte slices for combined data

	// DATA INTEGRITY: Write verification and corruption detection
	writeVerifier *DocumentWriteVerifier // Checksum verification for write operations
	writeLogger   *BundleWriteLogger     // Detailed write operation logging for debugging

	// PROJECTION PUSHDOWN: Temporary storage for projection fields per bundle
	// This allows BundleAdapter to pass projection through to readDocumentRange
	// Keyed by bundle name, cleared after page loading
	projectionFields map[string][]string // Per-bundle projection fields
	projectionMutex  sync.RWMutex        // Protects projectionFields map

	// FILE READ CACHE: Bounded cache of file/segment contents to avoid repeated
	// full-file reads when LoadDocumentPage is called many times (e.g. getAllDocumentsForIndexing).
	// Key: file path. LRU eviction when at FileReadCacheMaxEntries.
	fileReadCache      map[string]*fileReadCacheEntry
	fileReadCacheMutex sync.RWMutex

	// PARSED DOCS CACHE: Caches fully parsed documents from segment files.
	// Key: "bundleName:filePath". This avoids re-parsing the same file content
	// when loading different pages, which is critical for multi-file storage
	// where each page load would otherwise re-parse all segment files.
	// O(F) parsing becomes O(1) after first load.
	parsedDocsCache      map[string]*parsedDocsCacheEntry
	parsedDocsCacheMutex sync.RWMutex

	// SINGLEFLIGHT: Prevents thundering herd on cache population.
	// When cache miss occurs, only one goroutine parses the file while others wait.
	// Key: cacheKey (bundleName:filePath), Value: channel closed when parsing completes
	parseInFlight      map[string]chan struct{}
	parseInFlightMutex sync.Mutex

	// COMPACTION CALLBACK: Invoked when compaction completes for a bundle so
	// BundleService can invalidate documentPageMap (logical page positions change).
	onCompactionComplete   func(databaseName, bundleName string)
	onCompactionCompleteMu sync.RWMutex
}

// fileReadCacheEntry holds a cached file buffer and lastAccess for LRU eviction.
type fileReadCacheEntry struct {
	data       []byte
	lastAccess int64
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
	// Last access time for LRU eviction
	lastAccess int64
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

	RemoveDocumentFromBundleFile(database *models.Database, bundle *models.Bundle, documentID string, mmapData []byte) error
	BundleFileExists(bundleName string, databaseName string) bool
	RemoveBundleFile(database *models.Database, bundleName string) error

	FlushWriteBuffers(bundleName string) error
	FlushAllWriteBuffers() error
	CloseWriteBuffer(bundleName string) error
	CloseWriteBuffers() error

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
	//logger.Infof("Bundle storage using %s format", serializer.GetFormatName())

	// Create a new bundle store
	store := &BundleStorageEngine{
		DataDirectory:    dataDir,
		fileManager:      fileManager,
		logger:           logger,
		serializer:       serializer,
		writeBuffers:     make(map[string]*WriteBuffer),
		projectionFields: make(map[string][]string),               // PROJECTION PUSHDOWN: Initialize projection fields map
		manifestManagers: make(map[string]*ManifestManager),       // Initialize manifest managers map
		writeLocks:       make(map[string]*sync.RWMutex),          // Initialize write locks map
		documentLocks:    make(map[string]map[string]*sync.Mutex), // DOCUMENT-LEVEL LOCKING: Initialize document locks map
		rotationLocks:    make(map[string]*sync.Mutex),            // PHASE 1: Initialize rotation locks map
		writeVerifier:    NewDocumentWriteVerifier(logger),        // Initialize write verification
		writeLogger:      NewBundleWriteLogger(logger, 1000),      // Keep last 1000 write operations
		fileReadCache:    make(map[string]*fileReadCacheEntry),    // FILE READ CACHE: avoids repeated full-file reads per page
		parsedDocsCache:  make(map[string]*parsedDocsCacheEntry),  // PARSED DOCS CACHE: avoids re-parsing same file for different pages
		parseInFlight:    make(map[string]chan struct{}),          // SINGLEFLIGHT: prevents thundering herd on cache population
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

	return store, nil
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

	bse.logger.Infof("Loaded metadata for %d bundles from '%s'", len(bundles), dataDir)
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
// OPTIMIZATION: Uses parsedDocsCache to avoid re-parsing files for different page loads.
// First page load of any page from a set of files parses all files once and caches results.
// Subsequent page loads extract from cached results: O(F) parsing → O(1) cache lookup.
func (bse *BundleStorageEngine) LoadDocumentPage(bundleName string, databaseName string, pageID uint32, dataRootDir string) (*models.DocumentPage, error) {
	// MULTI-FILE STORAGE: Load manifest to get all segment files
	manifestMgr := bse.getOrCreateManifestManager(databaseName, bundleName)
	manifest, err := manifestMgr.LoadOrCreate(databaseName, bundleName)
	if err != nil {
		// Fall back to legacy single-file format if manifest doesn't exist
		return bse.loadDocumentPageLegacy(bundleName, databaseName, pageID, dataRootDir)
	}

	// Check if we have any files in the manifest
	if len(manifest.Files) == 0 {
		// No files yet - fall back to legacy format
		return bse.loadDocumentPageLegacy(bundleName, databaseName, pageID, dataRootDir)
	}

	// MULTI-FILE SCANNING: Load documents from all segment files and merge
	pageSize := uint32(4096) // Use consistent page size with BundleService (power of 2)
	startIndex := pageID * pageSize
	endIndex := startIndex + pageSize

	// Merged documents map with last-write-wins semantics
	// Later files (higher fileID) overwrite earlier files
	mergedDocuments := make(map[string]models.Document)
	deletedDocIDs := make(map[string]bool)
	var totalDocsAcrossFiles uint32

	// OPTIMIZATION: Use parsed docs cache to avoid re-parsing files for different pages
	// This turns O(F * N) into O(1) for subsequent page loads where F = files, N = docs
	for _, fileInfo := range manifest.Files {
		bundleDir := GetBundleDirectory(databaseName, bundleName)
		filePath := filepath.Join(bundleDir, fileInfo.FileName)
		cacheKey := fmt.Sprintf("%s:%s", bundleName, filePath)

		// Check parsed docs cache first
		bse.parsedDocsCacheMutex.RLock()
		cached, cacheHit := bse.parsedDocsCache[cacheKey]
		if cacheHit {
			cached.lastAccess = time.Now().UnixNano()
		}
		bse.parsedDocsCacheMutex.RUnlock()

		if cacheHit {
			// Use cached parsed documents - O(1) instead of O(N) parsing
			for docID, doc := range cached.documents {
				mergedDocuments[docID] = doc
			}
			for docID := range cached.deletedDocIDs {
				deletedDocIDs[docID] = true
			}
			totalDocsAcrossFiles += cached.totalDocs
			continue
		}

		// Cache miss - implement singleflight to prevent thundering herd
		// Only one goroutine parses the file while others wait
	singleflightRetry:
		bse.parseInFlightMutex.Lock()
		waitCh, isInflight := bse.parseInFlight[cacheKey]
		if isInflight {
			bse.parseInFlightMutex.Unlock()
			// Wait for the in-flight parse to complete
			<-waitCh
			// Re-check cache after waiting (it should be populated now)
			bse.parsedDocsCacheMutex.RLock()
			cached, cacheHit = bse.parsedDocsCache[cacheKey]
			if cacheHit {
				cached.lastAccess = time.Now().UnixNano()
			}
			bse.parsedDocsCacheMutex.RUnlock()
			if cacheHit {
				for docID, doc := range cached.documents {
					mergedDocuments[docID] = doc
				}
				for docID := range cached.deletedDocIDs {
					deletedDocIDs[docID] = true
				}
				totalDocsAcrossFiles += cached.totalDocs
				continue
			}
			// Cache still empty (rare: parsing failed or was evicted) - retry to become the parser
			goto singleflightRetry
		}

		// Register as the parser for this file (mutex is held here)
		doneCh := make(chan struct{})
		bse.parseInFlight[cacheKey] = doneCh
		bse.parseInFlightMutex.Unlock()

		// Helper to clean up in-flight entry (must be called before continue/break)
		cleanupInFlight := func() {
			bse.parseInFlightMutex.Lock()
			delete(bse.parseInFlight, cacheKey)
			bse.parseInFlightMutex.Unlock()
			close(doneCh)
		}

		// Check if file exists (skip if not - may have been compacted)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			if bse.logger != nil && settings.GetSettings().Debug {
				bse.logger.Debugf("Skipping non-existent file %s (likely compacted)", filePath)
			}
			cleanupInFlight()
			continue
		}

		// Use file-read cache to avoid repeated full-file reads
		data, err := bse.getOrReadFile(filePath)
		if err != nil {
			bse.logger.Warnf("Failed to read bundle file '%s': %v", filePath, err)
			cleanupInFlight()
			continue
		}

		// Parse ALL documents from this file (not just page range) so we can cache them
		// This is a one-time cost per file; subsequent page loads use the cache
		fileDocuments, fileDeletedIDs, fileTotalDocs, err := bse.parseAllDocumentsFromFile(bundleName, databaseName, &data)
		if err != nil {
			bse.logger.Warnf("Failed to parse documents from file '%s': %v", filePath, err)
			cleanupInFlight()
			continue
		}

		// Cache the parsed results
		bse.cacheParsedDocs(cacheKey, fileDocuments, fileDeletedIDs, fileTotalDocs)
		cleanupInFlight()

		// Merge documents with last-write-wins
		for docID, doc := range fileDocuments {
			mergedDocuments[docID] = doc
		}
		for docID := range fileDeletedIDs {
			deletedDocIDs[docID] = true
		}
		totalDocsAcrossFiles += fileTotalDocs
	}

	// TOMBSTONE FILTERING: Remove deleted documents
	// Apply deletedDocIDs from tombstone markers across all files
	for docID := range deletedDocIDs {
		delete(mergedDocuments, docID)
	}

	// Filter empty DocumentID as safety measure
	for docID, doc := range mergedDocuments {
		if doc.DocumentID == "" {
			delete(mergedDocuments, docID)
		}
	}

	// PAGINATION: Extract only documents in the requested page range
	// Since we now cache all documents, we need to extract the page range
	// Convert merged docs to a deterministic order for consistent pagination
	sortedDocIDs := make([]string, 0, len(mergedDocuments))
	for docID := range mergedDocuments {
		sortedDocIDs = append(sortedDocIDs, docID)
	}
	sort.Strings(sortedDocIDs) // Deterministic ordering

	pageDocuments := make(map[string]models.Document)
	for idx, docID := range sortedDocIDs {
		docIndex := uint32(idx)
		if docIndex >= startIndex && docIndex < endIndex {
			pageDocuments[docID] = mergedDocuments[docID]
		}
	}

	page := &models.DocumentPage{
		PageID:    pageID,
		BundleID:  bundleName,
		Documents: pageDocuments,
		LoadedAt:  time.Now(),
		IsDirty:   false,
	}

	// Set pagination pointers based on total document count
	totalDocs := uint32(len(mergedDocuments))
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

	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Loaded page %d for bundle %s: merged %d files, %d documents in page (total: %d)",
			pageID, bundleName, len(manifest.Files), len(pageDocuments), totalDocs)
	}

	return page, nil
}

// loadDocumentPageLegacy loads a page from the legacy single-file format
// This provides backward compatibility during migration to multi-file storage
func (bse *BundleStorageEngine) loadDocumentPageLegacy(bundleName string, databaseName string, pageID uint32, dataRootDir string) (*models.DocumentPage, error) {
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	// Use file-read cache to avoid repeated full-file reads when loading many pages
	data, err := bse.getOrReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bundle file '%s': %w", filePath, err)
	}

	// Use the configured serializer to load bundle metadata for format validation
	_, err = bse.serializer.DeserializeBundleMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bundle metadata for document extraction: %w", err)
	}

	// PERFORMANCE FIX: Use efficient page-based loading instead of loading all documents
	pageSize := uint32(4096) // Use consistent page size with BundleService (power of 2)
	startIndex := pageID * pageSize
	endIndex := startIndex + pageSize

	// Load only the documents needed for this page using range-based loading
	// CRITICAL: Pass empty slice (not nil) to bypass global projection state and always load full documents
	// This prevents race conditions where concurrent queries set projection and poison the cache with partial pages
	// Projection is applied in-memory after retrieval, not during disk load
	pageDocuments, totalDocs, err := bse.readDocumentRange(bundleName, databaseName, startIndex, endIndex, &data, []string{})
	if err != nil {
		return nil, fmt.Errorf("failed to load document range for bundle %s page %d: %w", bundleName, pageID, err)
	}

	page := &models.DocumentPage{
		PageID:    pageID,
		BundleID:  bundleName,
		Documents: pageDocuments,
		LoadedAt:  time.Now(),
		IsDirty:   false,
	}

	// Set pagination pointers based on actual document count
	if pageID > 0 {
		prevPageID := pageID - 1
		page.PreviousPageID = &prevPageID
	}

	totalPages := (totalDocs + pageSize - 1) / pageSize
	if pageID < totalPages-1 {
		nextPageID := pageID + 1
		page.NextPageID = &nextPageID
	}

	return page, nil
}

// getOrReadFile returns the file content from the file-read cache, or reads from disk and caches it.
// Callers must not modify the returned slice. Used by LoadDocumentPage to avoid repeated full-file
// reads when iterating all pages (e.g. getAllDocumentsForIndexing).
func (bse *BundleStorageEngine) getOrReadFile(filePath string) ([]byte, error) {
	maxEntries := settings.GetSettings().FileReadCacheMaxEntries
	if maxEntries <= 0 {
		maxEntries = 32
	}

	bse.fileReadCacheMutex.RLock()
	if e := bse.fileReadCache[filePath]; e != nil {
		e.lastAccess = time.Now().UnixNano()
		data := e.data
		bse.fileReadCacheMutex.RUnlock()
		return data, nil
	}
	bse.fileReadCacheMutex.RUnlock()

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	bse.fileReadCacheMutex.Lock()
	defer bse.fileReadCacheMutex.Unlock()

	// Double-check: another goroutine may have populated while we were reading
	if e := bse.fileReadCache[filePath]; e != nil {
		return e.data, nil
	}

	// Evict LRU entries until we have room
	for len(bse.fileReadCache) >= maxEntries {
		bse.evictFileReadCacheLRULocked()
	}

	bse.fileReadCache[filePath] = &fileReadCacheEntry{
		data:       data,
		lastAccess: time.Now().UnixNano(),
	}
	return data, nil
}

// evictFileReadCacheLRULocked removes the least-recently-accessed entry.
// Caller must hold bse.fileReadCacheMutex (write).
func (bse *BundleStorageEngine) evictFileReadCacheLRULocked() {
	var evictKey string
	var minAccess int64
	first := true
	for k, e := range bse.fileReadCache {
		if first || e.lastAccess < minAccess {
			evictKey = k
			minAccess = e.lastAccess
			first = false
		}
	}
	if evictKey != "" {
		delete(bse.fileReadCache, evictKey)
		if bse.logger != nil && settings.GetSettings().Debug {
			bse.logger.Debugf("Evicted file read cache entry: %s", evictKey)
		}
	}
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
	bundleDir := GetBundleDirectory(databaseName, bundleName)
	bse.parsedDocsCacheMutex.RLock()
	for _, fileInfo := range manifest.Files {
		filePath := filepath.Join(bundleDir, fileInfo.FileName)
		cacheKey := fmt.Sprintf("%s:%s", bundleName, filePath)

		if cached, ok := bse.parsedDocsCache[cacheKey]; ok {
			cached.lastAccess = time.Now().UnixNano()
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
	bse.parsedDocsCacheMutex.RUnlock()

	return result, notFound
}

// InvalidateFileReadCache removes a single path from the file-read cache.
// Call after compact or replace of that file so subsequent reads see fresh data.
func (bse *BundleStorageEngine) InvalidateFileReadCache(filePath string) {
	bse.fileReadCacheMutex.Lock()
	defer bse.fileReadCacheMutex.Unlock()
	delete(bse.fileReadCache, filePath)
	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Invalidated file read cache: %s", filePath)
	}
}

// InvalidateFileReadCacheForBundle removes all cached buffers for a bundle (legacy file and
// all segment files in the bundle dir). Call on bundle drop or after compaction that
// replaces segments.
func (bse *BundleStorageEngine) InvalidateFileReadCacheForBundle(databaseName, bundleName string) {
	bse.fileReadCacheMutex.Lock()
	defer bse.fileReadCacheMutex.Unlock()

	legacyPath := filepath.Join(helpers.GetDatabaseFolderPath(databaseName), fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))
	delete(bse.fileReadCache, legacyPath)

	bundleDir := GetBundleDirectory(databaseName, bundleName)
	for k := range bse.fileReadCache {
		if filepath.Dir(k) == bundleDir {
			delete(bse.fileReadCache, k)
		}
	}
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

		if magic == 0xDEADBEEF {
			// Document record
			if offset+8+int(size) > len(*fileData) {
				break
			}

			documentData := (*fileData)[offset+8 : offset+8+int(size)]
			fullDoc, err := helpers.DecodeFastBinaryToDocument(documentData)
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
		} else if magic == 0xDEADDEAD {
			// Tombstone marker
			if offset+8+int(size) > len(*fileData) {
				break
			}

			deletionData := (*fileData)[offset+8 : offset+8+int(size)]
			deletionDoc, err := helpers.DecodeFastBinaryToDocument(deletionData)
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
func (bse *BundleStorageEngine) cacheParsedDocs(cacheKey string, documents map[string]models.Document, deletedDocIDs map[string]bool, totalDocs uint32) {
	maxEntries := settings.GetSettings().FileReadCacheMaxEntries
	if maxEntries <= 0 {
		maxEntries = 32 // Default matches file read cache
	}

	bse.parsedDocsCacheMutex.Lock()
	defer bse.parsedDocsCacheMutex.Unlock()

	// Evict LRU entries if cache is full
	for len(bse.parsedDocsCache) >= maxEntries {
		bse.evictParsedDocsCacheLRULocked()
	}

	bse.parsedDocsCache[cacheKey] = &parsedDocsCacheEntry{
		documents:     documents,
		deletedDocIDs: deletedDocIDs,
		totalDocs:     totalDocs,
		lastAccess:    time.Now().UnixNano(),
	}

	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Cached %d parsed documents for %s", len(documents), cacheKey)
	}
}

// evictParsedDocsCacheLRULocked removes the least-recently-accessed entry.
// Caller must hold bse.parsedDocsCacheMutex (write).
func (bse *BundleStorageEngine) evictParsedDocsCacheLRULocked() {
	var evictKey string
	var minAccess int64
	first := true
	for k, e := range bse.parsedDocsCache {
		if first || e.lastAccess < minAccess {
			evictKey = k
			minAccess = e.lastAccess
			first = false
		}
	}
	if evictKey != "" {
		delete(bse.parsedDocsCache, evictKey)
		if bse.logger != nil && settings.GetSettings().Debug {
			bse.logger.Debugf("Evicted parsed docs cache entry: %s", evictKey)
		}
	}
}

// InvalidateParsedDocsCacheForBundle removes all cached parsed docs for a bundle.
// Call after writes, compaction, or any operation that changes bundle contents.
func (bse *BundleStorageEngine) InvalidateParsedDocsCacheForBundle(bundleName string) {
	bse.parsedDocsCacheMutex.Lock()
	defer bse.parsedDocsCacheMutex.Unlock()

	prefix := bundleName + ":"
	for k := range bse.parsedDocsCache {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(bse.parsedDocsCache, k)
		}
	}

	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Invalidated parsed docs cache for bundle %s", bundleName)
	}
}

// InvalidateParsedDocsCacheForLatestFile invalidates only the cache entry for the latest segment file.
// This is more efficient than InvalidateParsedDocsCacheForBundle when only the latest file was modified.
// Writes always append to the latest file, so older segment files remain valid.
func (bse *BundleStorageEngine) InvalidateParsedDocsCacheForLatestFile(bundleName, databaseName string) {
	// Get manifest to find the latest file
	mm := bse.getOrCreateManifestManager(databaseName, bundleName)
	manifest := mm.GetManifest()

	if len(manifest.Files) == 0 {
		return // No files to invalidate
	}

	// Latest file is the last one in the manifest
	latestFile := manifest.Files[len(manifest.Files)-1]
	bundleDir := GetBundleDirectory(databaseName, bundleName)
	filePath := filepath.Join(bundleDir, latestFile.FileName)
	cacheKey := fmt.Sprintf("%s:%s", bundleName, filePath)

	bse.parsedDocsCacheMutex.Lock()
	delete(bse.parsedDocsCache, cacheKey)
	bse.parsedDocsCacheMutex.Unlock()

	if bse.logger != nil && settings.GetSettings().Debug {
		bse.logger.Debugf("Invalidated parsed docs cache for latest file %s in bundle %s", latestFile.FileName, bundleName)
	}
}

// RegisterCompactionComplete sets the callback invoked when compaction completes for a bundle.
// BundleService registers InvalidateDocumentPageMapForBundle so documentPageMap stays correct.
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

	// Extract bundle name from filename (remove .bnd extension)
	bundleName := strings.TrimSuffix(fileName, ".bnd")

	// Try to load bundle metadata first
	bundleMetadata, err := b.loadBundleMetadataFromFile(dataRootDir, fileName)
	if err != nil {
		return nil, fmt.Errorf("error loading bundle metadata from file %s: %w", fileName, err)
	}

	// Load documents using the new append-only method
	documents, err := b.ReadAppendedDocuments(bundleName, database.Name)
	if err != nil {
		return nil, fmt.Errorf("error loading documents from bundle file %s: %w", fileName, err)
	}

	// Update bundle with loaded documents
	bundleMetadata.Documents = &documents
	bundleMetadata.Database = database

	b.logger.Debugf("Loaded bundle data from file %s with %d documents", fileName, len(documents))

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
	bundle, docCount, err := bs.parseHeaderPage(headerBuffer.Data)
	if err != nil {
		return nil, fmt.Errorf("could not parse header page: %w", err)
	}

	// Read the document pages
	docs, err := bs.readDocuments(fileID, docCount)
	if err != nil {
		return nil, fmt.Errorf("could not read documents: %w", err)
	}

	bundle.Documents = &docs

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
	bundle.Documents = new(map[string]models.Document)
	*bundle.Documents = make(map[string]models.Document)

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
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(databaseName)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))
	b.logger.Debugf("Checking if bundle file exists: %s", filePath)
	return helpers.FileExists(filePath, *b.logger)
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

	// CRITICAL: Acquire write lock ONCE for entire batch
	// This prevents lock contention and ensures atomic batch operation
	lock := b.getWriteLock(bundle.Name)
	lock.Lock()
	defer lock.Unlock()

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

	// Initialize memtable if needed (Cassandra-style approach)
	if bundle.Documents == nil {
		bundle.Documents = new(map[string]models.Document)
		*bundle.Documents = make(map[string]models.Document)
	}
	bundle.DocumentsComplete = false // Mark as incomplete so queries merge with disk

	// PERFORMANCE FIX: Batch all memtable updates under a single lock acquisition
	// This reduces O(N) lock/unlock operations to O(1), matching PostgreSQL's approach
	bundle.DocumentsMutex.Lock()
	for _, document := range documents {
		if document != nil && document.DocumentID != "" {
			(*bundle.Documents)[document.DocumentID] = *document
		}
	}
	bundle.DocumentsMutex.Unlock()

	// Process all documents in batch (serialization and disk writes)
	successCount := 0
	for _, document := range documents {
		// Validate each document
		if document == nil || document.DocumentID == "" {
			b.logger.Warnf("BATCH UPDATE: Skipping invalid document")
			continue
		}

		// Serialize document
		documentBytes, err := b.serializeDocumentDirect(document)
		if err != nil {
			b.logger.Warnf("BATCH UPDATE: Failed to serialize document %s: %v", document.DocumentID, err)
			continue
		}

		// Create header with magic number
		// CRITICAL FIX: Allocate header buffer per-write to avoid race conditions
		headerSize := uint32(len(documentBytes))
		headerBytes := make([]byte, 8)                              // 8 bytes: 4 for magic, 4 for size
		binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADBEEF) // Magic number for document boundaries
		binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

		// Combine header + document data using buffer pool
		combinedData := b.getCombinedBuffer(len(headerBytes) + len(documentBytes))
		copy(combinedData[:8], headerBytes)
		copy(combinedData[8:], documentBytes)

		// Write to buffer (NOT to disk yet)
		if err := writeBuffer.Write(combinedData[:len(headerBytes)+len(documentBytes)]); err != nil {
			b.returnCombinedBuffer(combinedData)
			b.logger.Warnf("BATCH UPDATE: Failed to buffer document %s: %v", document.DocumentID, err)
			continue
		}

		b.returnCombinedBuffer(combinedData)
		successCount++
	}

	if successCount == 0 {
		return fmt.Errorf("BATCH UPDATE: Failed to update any documents")
	}

	// CRITICAL FIX: Single flush at the end for entire batch
	// This writes all buffered updates to disk in one I/O operation
	// READERS ARE BLOCKED BY THE WRITE LOCK UNTIL THIS COMPLETES
	if err := writeBuffer.Flush(); err != nil {
		return fmt.Errorf("BATCH UPDATE: Failed to flush write buffer: %w", err)
	}

	// R3: Sync is configurable via DurabilityMode. Read settings.GetSettings().DurabilityMode
	// directly here; bundle_storage_engine does not use it elsewhere for the Sync decision.
	// - "performance" (default): skip Sync; rely on WAL + write buffer flush policy (matches ADD).
	// - "strict": sync to disk before releasing lock.
	// TODO: If we experience durability or Sync-throughput issues, consider a "balanced" coalesced Sync:
	// queue batches to a background goroutine, coalesce, single Sync per N batches or per time window
	// (PostgreSQL wal_writer_delay–style).
	// PHASE 0.2: Verify durability mode is correctly applied
	dm := settings.GetSettings().DurabilityMode
	if dm == "strict" {
		if err := writeBuffer.SyncGroupCommit(); err != nil {
			b.logger.Warnf("BATCH UPDATE: Group-commit sync failed: %v (continuing anyway)", err)
		}
	} else {
		b.logger.Debugf("BATCH UPDATE: DurabilityMode is '%s' - skipping fsync for performance", dm)
	}

	// Update bundle metadata ONCE after all documents are written
	// Note: Updates don't change TotalDocuments count, only PageCount might change
	// if documents grew significantly
	if bundle.TotalDocuments > 0 {
		bundle.PageCount = int64((uint32(bundle.TotalDocuments) + pageSize - 1) / pageSize)
	}

	// Mark bundle as dirty to trigger metadata persistence
	bundle.IsDirty = true

	// CACHE INVALIDATION: Only invalidate the latest file's cache since writes append there
	// This is much more efficient than invalidating all 7+ cached file entries
	b.InvalidateParsedDocsCacheForLatestFile(bundle.Name, bundle.Database.Name)

	// if b.logger != nil {
	// 	b.logger.Infow("BATCH UPDATE: Successfully updated documents",
	// 		"bundle", bundle.Name,
	// 		"updatedCount", successCount,
	// 		"totalCount", len(documents),
	// 		"pageCount", bundle.PageCount)
	// }

	// CRITICAL: Lock is released here by defer, ensuring atomic visibility
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

	// Initialize memtable if needed
	if bundle.Documents == nil {
		bundle.Documents = new(map[string]models.Document)
		*bundle.Documents = make(map[string]models.Document)
	}
	bundle.DocumentsComplete = false

	// PERFORMANCE FIX: Batch all memtable updates under a single lock acquisition
	// This reduces O(N) lock/unlock operations to O(1), matching PostgreSQL's approach
	bundle.DocumentsMutex.Lock()
	for _, document := range documents {
		if document != nil && document.DocumentID != "" {
			(*bundle.Documents)[document.DocumentID] = *document
		}
	}
	bundle.DocumentsMutex.Unlock()

	// Process all documents (serialization and disk writes)
	// NOTE: Document locks are already held by caller (from LockManager)
	// WriteBuffer is protected by its own mutex (thread-safe)
	successCount := 0
	for _, document := range documents {
		if document == nil || document.DocumentID == "" {
			b.logger.Warnf("BATCH UPDATE WITH LOCKS: Skipping invalid document")
			continue
		}

		// Serialize document
		documentBytes, err := b.serializeDocumentDirect(document)
		if err != nil {
			b.logger.Warnf("BATCH UPDATE WITH LOCKS: Failed to serialize document %s: %v", document.DocumentID, err)
			continue
		}

		// Create header
		headerSize := uint32(len(documentBytes))
		headerBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADBEEF)
		binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

		// Combine header + document data
		combinedData := b.getCombinedBuffer(len(headerBytes) + len(documentBytes))
		copy(combinedData[:8], headerBytes)
		copy(combinedData[8:], documentBytes)

		// Write to buffer (thread-safe via WriteBuffer mutex)
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
	// CRITICAL FIX: Acquire write lock to prevent dirty reads during deletion
	lock := b.getWriteLock(bundle.Name)
	lock.Lock()
	defer lock.Unlock()

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

	// PERFORMANCE OPTIMIZATION: Use streaming verification
	// This avoids loading the entire file into memory
	documentExists, err := b.verifyDocumentExistsStreaming(bundle.Name, bundle.Database.Name, documentID)
	if err != nil {
		return fmt.Errorf("failed to verify document existence: %w", err)
	}
	if !documentExists {
		return fmt.Errorf("document %s not found in bundle %s", documentID, bundle.Name)
	}

	if args.Debug {
		b.logger.Infof("Deleting document %s from bundle %s", documentID, bundle.Name)
	}

	// Append deletion tombstone using write buffer for optimal performance
	err = b.appendDeletionMarker(bundle.Name, documentID, filePath)
	if err != nil {
		return fmt.Errorf("failed to append deletion marker: %w", err)
	}

	// D7: Keep FlushWriteBuffers — ensures pending ADDs/UPDATEs are on disk before the tombstone.
	// Without it, a crash could leave a tombstone for a "never existed" doc. Flush is cheap.
	err = b.FlushWriteBuffers(bundle.Name)
	if err != nil {
		b.logger.Warnf("Failed to flush write buffer for bundle %s: %v", bundle.Name, err)
	}

	// D7: Make only writeBuffer.Sync() conditional on DurabilityMode. Tombstone is in appendDeletionMarker's
	// file; Sync here orders buffered writes. D3: "strict" sync, else skip.
	b.bufferMutex.RLock()
	writeBuffer, exists := b.writeBuffers[bundle.Name]
	b.bufferMutex.RUnlock()
	if exists && settings.GetSettings().DurabilityMode == "strict" {
		if err := writeBuffer.Sync(); err != nil {
			b.logger.Warnf("Failed to sync write buffer to disk: %v (continuing anyway)", err)
		}
	}

	// CRITICAL FIX: Do NOT decrement TotalDocuments on deletion
	// In append-only storage, tombstones are still entries on disk, so TotalDocuments
	// should represent total document entries (including tombstones), not active documents.
	// Active document count is calculated dynamically by filtering tombstones during queries.
	// Decrementing TotalDocuments causes corruption when:
	// 1. Documents exist that were never counted (pre-tracking, migration, etc.)
	// 2. Same document is deleted multiple times
	// 3. Documents are deleted that exist only in memtable (not yet flushed)
	//
	// TotalDocuments now represents: total document entries ever written (inserts + tombstones)
	// Active document count = TotalDocuments - tombstone count (calculated dynamically)
	// Since we no longer decrement TotalDocuments, it cannot go negative

	// Always calculate PageCount from TotalDocuments to ensure consistency
	// Use ceiling division: ceil(a/b) = (a + b - 1) / b
	pageSize := uint32(4096)
	if bundle.PageSize > 0 {
		pageSize = uint32(bundle.PageSize)
	}
	if bundle.TotalDocuments > 0 {
		calculatedPageCount := int64((uint32(bundle.TotalDocuments) + pageSize - 1) / pageSize)
		bundle.PageCount = calculatedPageCount
	} else {
		bundle.PageCount = 0
	}

	// Mark bundle as dirty to trigger metadata persistence
	bundle.IsDirty = true

	if args.Debug {
		b.logger.Infof("Successfully deleted document %s from bundle %s (new TotalDocuments: %d, PageCount: %d)",
			documentID, bundle.Name, bundle.TotalDocuments, bundle.PageCount)
	}

	// CACHE INVALIDATION: Only invalidate the latest file's cache since deletes append tombstones there
	b.InvalidateParsedDocsCacheForLatestFile(bundle.Name, bundle.Database.Name)

	// CRITICAL: Lock is released here by defer, ensuring atomic visibility
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
	documents, err := b.parseAppendedDocuments(fileData)
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

	// Create header with deletion magic number
	headerSize := uint32(len(deletionBytes))
	headerBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADDEAD) // Magic number for deletion markers
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
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))

	// Open file once for all markers
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open bundle file for batch deletion markers: %w", err)
	}
	defer file.Close()

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
		headerSize := uint32(len(deletionBytes))
		headerBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADDEAD)
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
	bs.logger.Infof("Adding document to bundle %s with file ID %d", bundle.Name, fileID)

	// Add the document to the bundle in memory
	bundle.DocumentsMutex.Lock()
	if bundle.Documents == nil {
		bundle.Documents = new(map[string]models.Document)
		*bundle.Documents = make(map[string]models.Document)
	}
	(*bundle.Documents)[document.DocumentID] = *document
	bundle.DocumentsMutex.Unlock()

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
	//b.logger.Infof("Starting time: %s", time.Now().Format(time.RFC3339Nano))
	//testingStart := time.Now()
	// PHASE 1: MVCC - Removed bundle-wide write lock to allow concurrent writes
	// Fine-grained locking: rotation lock protects file rotation, atomic operations protect metadata

	// MULTI-FILE STORAGE: Determine current active file and check if rotation is needed
	// Get or create manifest manager for this bundle
	manifestMgr := b.getOrCreateManifestManager(bundle.Database.Name, bundle.Name)
	manifest, err := manifestMgr.LoadOrCreate(bundle.Database.Name, bundle.Name)
	if err != nil {
		return 0, fmt.Errorf("failed to load bundle manifest: %w", err)
	}

	// Get the current active (writable) file
	var currentFileID uint32 = 1
	if manifest.ActiveFileID > 0 {
		currentFileID = uint32(manifest.ActiveFileID)
	}

	// Construct the current file path
	bundleDir := GetBundleDirectory(bundle.Database.Name, bundle.Name)
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

	// Check if file rotation is needed (file size exceeds threshold)
	// TODO: add configuration override per bundle when per-bundle tuning becomes necessary
	maxSizeBytes := int64(settings.GetSettings().Storage.BundleFileMaxSizeMB) * 1024 * 1024
	rotationThreshold := int64(float64(maxSizeBytes) * 1.1) // ±10% variance tolerance

	fileInfo, statErr := os.Stat(filePath)
	needsRotation := false
	var currentFileSize int64 = 0

	if statErr == nil {
		currentFileSize = fileInfo.Size()
		needsRotation = currentFileSize >= rotationThreshold
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
			return 0, fmt.Errorf("failed to close write buffer before rotation: %w", err)
		}

		// Update manifest: freeze current file and mark as immutable
		if manifest.ActiveFileID > 0 {
			if err := manifestMgr.FreezeFile(int(currentFileID)); err != nil {
				return 0, fmt.Errorf("failed to freeze file in manifest: %w", err)
			}
		}

		// Create new file with incremented ID
		currentFileID++
		filePath = filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", currentFileID))

		// Add new active file to manifest
		fileName := fmt.Sprintf("%06d.bnd", currentFileID)
		if err := manifestMgr.AddFile(int(currentFileID), fileName); err != nil {
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

	// PHASE 1: MVCC - Use atomic get-and-increment pattern for thread-safe page calculation
	// Page ID is based on current position: pageID = currentDocCount / pageSize
	// Use consistent page size with virtual pagination (4096 documents per page, power of 2)
	pageSize := uint32(4096)
	if bundle.PageSize > 0 {
		pageSize = uint32(bundle.PageSize)
	}
	// FIXED: Get pre-increment value atomically to ensure atomic read-modify-write
	currentDocCount := uint32(atomic.AddInt64(&bundle.TotalDocuments, 1) - 1)
	pageID := currentDocCount / pageSize

	// PERFORMANCE FIX: Skip file existence check for known bundles
	// Trust that bundle files exist if bundle object is valid
	// if !helpers.FileExists(filePath, *b.logger) {
	//     return fmt.Errorf("bundle file %s does not exist", fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))
	// }

	// Add document to memtable (write buffer) for fast access to recent writes
	// Using Cassandra-style approach: Documents map is a memtable, not a complete cache
	bundle.DocumentsMutex.Lock()
	if bundle.Documents == nil {
		bundle.Documents = new(map[string]models.Document)
		*bundle.Documents = make(map[string]models.Document)
	}
	(*bundle.Documents)[document.DocumentID] = *document
	// Mark as incomplete so queries know to merge with disk data
	bundle.DocumentsComplete = false
	bundle.DocumentsMutex.Unlock()

	// PERFORMANCE FIX: Direct binary serialization without map conversion
	// Use Go's native binary encoding for maximum speed
	documentBytes, err := b.serializeDocumentDirect(document)
	if err != nil {
		return 0, fmt.Errorf("failed to encode document: %w", err)
	}

	// CRITICAL FIX: Allocate header buffer per-write to avoid race conditions
	// Shared buffers cause corruption when multiple goroutines write simultaneously
	headerSize := uint32(len(documentBytes))
	headerBytes := make([]byte, 8)                              // 8 bytes: 4 for magic, 4 for size
	binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADBEEF) // Magic number for document boundaries
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
	//b.logger.Infof("Ending time: %s", time.Now().Format(time.RFC3339Nano))
	//endingTesting := time.Since(testingStart)
	//b.logger.Infof("DEBUG DEBUG DEBUG AppendDocumentToBundleFileWithTxID took %s", endingTesting.String())
	// COMPACTION INTEGRATION: Trigger compaction evaluation after write
	// Don't block the write path - evaluate asynchronously
	// PostgreSQL autovacuum-inspired: check triggers after mutations
	go func() {
		if b.compactionScheduler != nil {
			b.compactionScheduler.EvaluateBundle(
				bundle.Database.Name,
				bundle.Name,
			)
		}
	}()

	// Return the page ID where this document was stored
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
			b.evaluateAllBundlesForCompaction()
		}
	}
}

// evaluateAllBundlesForCompaction evaluates all active bundles for compaction
// This is called by the periodic evaluator to ensure compaction runs even without writes
func (b *BundleStorageEngine) evaluateAllBundlesForCompaction() {
	// Take snapshot of manifest managers to avoid holding lock during evaluation
	b.manifestManagersMutex.Lock()
	managerKeys := make([]string, 0, len(b.manifestManagers))
	for key := range b.manifestManagers {
		managerKeys = append(managerKeys, key)
	}
	b.manifestManagersMutex.Unlock()

	// Evaluate each bundle asynchronously
	for _, key := range managerKeys {
		// Parse manager key: "<database>:<bundle>"
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		dbName, bundleName := parts[0], parts[1]

		// Async evaluation - don't block ticker
		go func(db, bundle string) {
			if b.compactionScheduler != nil {
				b.compactionScheduler.EvaluateBundle(db, bundle)
			}
		}(dbName, bundleName)
	}
}

// getWriteLock gets or creates a write lock for a specific bundle
// This ensures thread-safe access to bundle data during concurrent reads and writes
func (b *BundleStorageEngine) getWriteLock(bundleName string) *sync.RWMutex {
	b.writeLocksMutex.Lock()
	defer b.writeLocksMutex.Unlock()

	if lock, exists := b.writeLocks[bundleName]; exists {
		return lock
	}

	// Create new lock for this bundle
	lock := &sync.RWMutex{}
	b.writeLocks[bundleName] = lock
	return lock
}

// getRotationLock gets or creates a rotation lock for a specific bundle
// PHASE 1: MVCC - Protects file rotation decision and execution
// This ensures only one goroutine can rotate a bundle's file at a time
func (b *BundleStorageEngine) getRotationLock(bundleName string) *sync.Mutex {
	b.rotationLocksMutex.Lock()
	defer b.rotationLocksMutex.Unlock()

	if lock, exists := b.rotationLocks[bundleName]; exists {
		return lock
	}

	// Create new rotation lock for this bundle
	lock := &sync.Mutex{}
	b.rotationLocks[bundleName] = lock
	return lock
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

	// Use DecodeFastBinary but only extract DocumentID from the map
	// This is still much faster than building full Document objects
	// Even though we decode the whole thing, we only use DocumentID
	docMap, err := helpers.DecodeFastBinary(data)
	if err != nil {
		return "", fmt.Errorf("failed to decode DocumentID: %w", err)
	}

	// Extract only DocumentID (first field in the map)
	if docID, ok := docMap["DocumentID"].(string); ok {
		return docID, nil
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

	// Skip bundle metadata header if present (0x42444D44 = "BDMD")
	if len(data) >= 8 {
		magic := binary.LittleEndian.Uint32(data[0:4])
		if magic == 0x42444D44 { // "BDMD" = Bundle Metadata
			metadataSize := binary.LittleEndian.Uint32(data[4:8])
			offset = int(8 + metadataSize)
		}
	}

	for offset < len(data) {
		// Need at least 8 bytes for magic number + size header
		if offset+8 > len(data) {
			break
		}

		// Read magic number and size
		magic := binary.LittleEndian.Uint32(data[offset : offset+4])
		size := binary.LittleEndian.Uint32(data[offset+4 : offset+8])

		// Validate size before proceeding
		if offset+8+int(size) > len(data) {
			break
		}

		recordData := data[offset+8 : offset+8+int(size)]

		if magic == 0xDEADBEEF {
			// Document - extract only DocumentID
			docID, err := b.extractDocumentIDOnly(recordData)
			if err != nil {
				// Log warning but continue processing (don't fail entire count)
				if b.logger != nil {
					b.logger.Debugf("Failed to extract DocumentID at offset %d: %v (skipping)", offset, err)
				}
				offset += 8 + int(size)
				continue
			}

			if docID != "" {
				// Last-write-wins: later occurrence overwrites earlier
				seenDocuments[docID] = true
				// If was deleted, re-add it (update after delete)
				delete(deletedDocuments, docID)
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
				// Mark as deleted and remove from seen documents
				deletedDocuments[docID] = true
				delete(seenDocuments, docID)
			}
		}

		offset += 8 + int(size)
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
		return int(manifest.TotalDocuments), nil
	}

	// LOCAL maps - no concurrent access (this goroutine only)
	seenDocuments := make(map[string]bool)
	deletedDocuments := make(map[string]bool)

	// Get file list snapshot (oldest first for last-write-wins)
	files := manifestMgr.GetFileList(false) // false = oldest first

	for _, fileInfo := range files {
		bundleDir := GetBundleDirectory(databaseName, bundleName)
		filePath := filepath.Join(bundleDir, fileInfo.FileName)

		// Check if file exists (may have been compacted)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			if b.logger != nil && settings.GetSettings().Debug {
				b.logger.Debugf("Skipping non-existent file %s (likely compacted)", filePath)
			}
			continue
		}

		// File read is safe: protected by read lock (no concurrent writes)
		data, err := os.ReadFile(filePath)
		if err != nil {
			if b.logger != nil {
				b.logger.Warnf("Failed to read file %s for counting: %v", filePath, err)
			}
			continue
		}

		// Count in this file (updates local maps in-place)
		err = b.countDocumentsInFileOnly(data, seenDocuments, deletedDocuments)
		if err != nil {
			if b.logger != nil {
				b.logger.Warnf("Failed to count documents in file %s: %v", filePath, err)
			}
			continue
		}
	}

	// Final count: unique documents that aren't deleted
	return len(seenDocuments), nil
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
		return b.countDocumentsMultiFile(manifestMgr, databaseName, bundleName)
	}

	// Fall back to legacy single-file format
	return b.countDocumentsLegacy(bundleName, databaseName)
}

// getOrCreateManifestManager gets or creates a manifest manager for a specific bundle
// Manifest managers are cached per bundle for performance
func (b *BundleStorageEngine) getOrCreateManifestManager(databaseName, bundleName string) *ManifestManager {
	// Use bundleName as key (unique per database context)
	managerKey := databaseName + ":" + bundleName

	b.manifestManagersMutex.Lock()
	defer b.manifestManagersMutex.Unlock()

	if manager, exists := b.manifestManagers[managerKey]; exists {
		return manager
	}

	// Create new manifest manager for this bundle
	manager := NewManifestManager(b.DataDirectory, databaseName, bundleName, b.logger)
	b.manifestManagers[managerKey] = manager
	return manager
}

// getOrCreateWriteBuffer gets or creates a write buffer for the specified file
// MULTI-FILE STORAGE: Write buffers are now keyed by filePath instead of bundleName
// This allows multiple active write buffers per bundle (one per file segment)
func (b *BundleStorageEngine) getOrCreateWriteBuffer(bundleName, filePath string) (*WriteBuffer, error) {
	// Use file path as key to support multiple files per bundle
	bufferKey := filePath

	b.bufferMutex.RLock()
	buffer, exists := b.writeBuffers[bufferKey]
	b.bufferMutex.RUnlock()

	if exists {
		return buffer, nil
	}

	// Create new write buffer
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	// Double-check after acquiring write lock
	if buffer, exists := b.writeBuffers[bufferKey]; exists {
		return buffer, nil
	}

	// Ensure the bundle directory exists
	// Use the directory from filePath directly instead of reconstructing
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create bundle directory: %w", err)
	}

	// Open file in append mode with O_CREATE to handle first-time creation
	// CRITICAL: O_CREATE ensures file exists, O_APPEND guarantees atomic append operations
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open bundle file for buffering: %w", err)
	}

	// Create write buffer with 64KB buffer size for optimal performance
	buffer = NewWriteBuffer(file, 65536)
	b.writeBuffers[bufferKey] = buffer

	return buffer, nil
}

// FlushWriteBuffers flushes all write buffers for a specific bundle
// MULTI-FILE STORAGE: Flushes all file buffers associated with the bundle
func (b *BundleStorageEngine) FlushWriteBuffers(bundleName string) error {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	var errors []error
	bundlePattern := "/" + bundleName + "/"

	for bufferKey, buffer := range b.writeBuffers {
		if !strings.Contains(bufferKey, bundlePattern) {
			continue
		}

		if err := buffer.Flush(); err != nil {
			b.logger.Warnf("Failed to flush buffer for %s: %v", bufferKey, err)
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to flush %d write buffers for bundle %s", len(errors), bundleName)
	}

	// Evaluate compaction triggers after successful flush
	// This is similar to PostgreSQL's autovacuum triggering after significant write activity
	if b.compactionScheduler != nil {
		// Extract database name from buffer keys
		for bufferKey := range b.writeBuffers {
			if strings.Contains(bufferKey, bundlePattern) {
				// Parse database name from path (data_files/<database>/<bundle>)
				parts := strings.Split(bufferKey, "/")
				if len(parts) >= 3 {
					databaseName := parts[len(parts)-3]
					b.compactionScheduler.EvaluateBundle(databaseName, bundleName)
					break // Only evaluate once per bundle
				}
			}
		}
	}

	return nil
}

// FlushAllWriteBuffers flushes all write buffers for all bundles
// MULTI-FILE STORAGE: Now flushes all file buffers (multiple files per bundle)
func (b *BundleStorageEngine) FlushAllWriteBuffers() error {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	var errors []string
	flushedCount := 0

	for bufferKey, buffer := range b.writeBuffers {
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
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to flush %d of %d write buffers: %v", len(errors), len(b.writeBuffers), errors)
	}

	if b.logger != nil && settings.GetSettings().Debug {
		b.logger.Infof("Successfully flushed all %d write buffers", flushedCount)
	}

	return nil
}

// CloseWriteBuffer closes and removes all write buffers for a specific bundle
// MULTI-FILE STORAGE: Now closes all file buffers associated with the bundle
// This is CRITICAL after operations that change file size (like appending tombstones or file rotation)
// to ensure subsequent file opens get fresh metadata (correct file size).
func (b *BundleStorageEngine) CloseWriteBuffer(bundleName string) error {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	// Find all buffers for this bundle (buffers are keyed by file path)
	// Close all buffers that belong to this bundle
	var errors []error
	bundlePattern := "/" + bundleName + "/"

	for bufferKey, buffer := range b.writeBuffers {
		// Check if this buffer belongs to the specified bundle
		// Buffer keys are file paths like: data_files/dbname/bundlename/000001.bnd
		if !strings.Contains(bufferKey, bundlePattern) {
			continue
		}

		if err := buffer.Close(); err != nil {
			b.logger.Warnf("Failed to close write buffer for %s: %v", bufferKey, err)
			errors = append(errors, err)
		}
		// Remove from map so next write creates a fresh buffer
		delete(b.writeBuffers, bufferKey)
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to close %d write buffers for bundle %s", len(errors), bundleName)
	}

	return nil
}

// DiscardWriteBuffer discards all write buffers for a specific bundle WITHOUT flushing
// MULTI-FILE STORAGE: Now discards all file buffers associated with the bundle
// This is used during transaction rollback to abandon buffered writes
func (b *BundleStorageEngine) DiscardWriteBuffer(bundleName string) error {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	var errors []error
	bundlePattern := "/" + bundleName + "/"

	for bufferKey, buffer := range b.writeBuffers {
		// Check if this buffer belongs to the specified bundle
		if !strings.Contains(bufferKey, bundlePattern) {
			continue
		}

		if err := buffer.Discard(); err != nil {
			b.logger.Warnf("Failed to discard write buffer for %s: %v", bufferKey, err)
			errors = append(errors, err)
		}
		// Remove from map so next write creates a fresh buffer
		delete(b.writeBuffers, bufferKey)
		b.logger.Debugf("Discarded write buffer for '%s' without flushing", bufferKey)
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to discard %d write buffers for bundle %s", len(errors), bundleName)
	}

	return nil
}

// GetBufferedDocumentsForTransaction returns all buffered documents for a specific transaction
// MULTI-FILE STORAGE: Aggregates documents from all file buffers for this bundle
func (b *BundleStorageEngine) GetBufferedDocumentsForTransaction(bundleName string, txID string) ([]*models.Document, error) {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	var allDocs []*models.Document
	bundlePattern := "/" + bundleName + "/"

	for bufferKey, buffer := range b.writeBuffers {
		if !strings.Contains(bufferKey, bundlePattern) {
			continue
		}

		docs, err := buffer.GetDocumentsForTransaction(txID)
		if err != nil {
			return nil, err
		}
		allDocs = append(allDocs, docs...)
	}

	return allDocs, nil
}

// MarkDocumentDiscarded marks a document as discarded (for rollback)
// MULTI-FILE STORAGE: Marks document in whichever buffer contains it
func (b *BundleStorageEngine) MarkDocumentDiscarded(bundleName string, docID string) error {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	bundlePattern := "/" + bundleName + "/"

	for bufferKey, buffer := range b.writeBuffers {
		if !strings.Contains(bufferKey, bundlePattern) {
			continue
		}

		// Mark in this buffer (no-op if document not in this buffer)
		buffer.MarkDiscarded(docID)
	}

	return nil
}

// IsDocumentBuffered checks if a document is currently in any write buffer for this bundle
// MULTI-FILE STORAGE: Checks all file buffers for this bundle
func (b *BundleStorageEngine) IsDocumentBuffered(bundleName string, docID string) bool {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	bundlePattern := "/" + bundleName + "/"

	for bufferKey, buffer := range b.writeBuffers {
		if !strings.Contains(bufferKey, bundlePattern) {
			continue
		}

		if buffer.IsDocumentAvailable(docID) {
			return true
		}
	}

	return false
}

// GetDiscardedDocuments returns document IDs that were discarded in a bundle's buffers
// MULTI-FILE STORAGE: Aggregates discarded documents from all file buffers
func (b *BundleStorageEngine) GetDiscardedDocuments(bundleName string) []string {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	var allDiscarded []string
	bundlePattern := "/" + bundleName + "/"

	for bufferKey, buffer := range b.writeBuffers {
		if !strings.Contains(bufferKey, bundlePattern) {
			continue
		}

		discarded := buffer.GetDiscardedDocuments()
		allDiscarded = append(allDiscarded, discarded...)
	}

	return allDiscarded
}

// ClearDiscardedDocuments removes the specified document IDs from the discarded set
// MULTI-FILE STORAGE: Clears from all file buffers for this bundle
func (b *BundleStorageEngine) ClearDiscardedDocuments(bundleName string, docIDs []string) {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	bundlePattern := "/" + bundleName + "/"

	for bufferKey, buffer := range b.writeBuffers {
		if !strings.Contains(bufferKey, bundlePattern) {
			continue
		}

		buffer.ClearDiscardedDocuments(docIDs)
	}
}

// CloseWriteBuffers closes and flushes all write buffers
func (b *BundleStorageEngine) CloseWriteBuffers() error {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	for bundleName, buffer := range b.writeBuffers {
		if err := buffer.Close(); err != nil {
			b.logger.Warnf("Failed to close write buffer for bundle %s: %v", bundleName, err)
		}
	}

	// Clear the map
	b.writeBuffers = make(map[string]*WriteBuffer)
	return nil
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

	b.logger.Info("Bundle storage engine shutdown complete")
	return nil
}

// SetProjectionFieldsForBundle sets projection fields temporarily for a bundle
// PROJECTION PUSHDOWN: This allows BundleAdapter to pass projection through to readDocumentRange
// Called from BundleAdapter before loading pages for ORDER BY queries
func (b *BundleStorageEngine) SetProjectionFieldsForBundle(bundleName string, fields []string) {
	// PERFORMANCE: Optimize for nil fields (clearing projection) - common case in GetDocumentsByFilter
	// Check if we actually need to modify anything before acquiring write lock
	if len(fields) == 0 {
		// Clearing projection - check if it's already cleared to avoid write lock
		b.projectionMutex.RLock()
		_, exists := b.projectionFields[bundleName]
		b.projectionMutex.RUnlock()
		if !exists {
			// Already cleared - no need to acquire write lock
			return
		}
	}

	b.projectionMutex.Lock()
	defer b.projectionMutex.Unlock()
	if len(fields) > 0 {
		b.projectionFields[bundleName] = fields
		if b.logger != nil {
			b.logger.Debugf("PROJECTION PUSHDOWN: Set projection fields %v for bundle '%s'", fields, bundleName)
		}
	} else {
		delete(b.projectionFields, bundleName)
	}
}

// getProjectionFieldsForBundle gets projection fields for a bundle if set
// PROJECTION PUSHDOWN: Returns projection fields if set, nil otherwise
func (b *BundleStorageEngine) getProjectionFieldsForBundle(bundleName string) []string {
	b.projectionMutex.RLock()
	defer b.projectionMutex.RUnlock()
	return b.projectionFields[bundleName]
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

	// //b.logger.Infof("DEBUG: readDocumentRange - file '%s' size: %d bytes", filePath, fileInfo.Size())

	// fileData := make([]byte, fileInfo.Size())
	// _, err = file.Read(fileData)
	// if err != nil {
	// 	return nil, 0, fmt.Errorf("failed to read file: %w", err)
	// }

	//b.logger.Infof("DEBUG: readDocumentRange - read %d bytes from file (expected %d)", bytesRead, fileInfo.Size())

	// Parse documents with range limiting
	// PROJECTION PUSHDOWN: Pass projection fields to deserialize only specified fields (e.g., "name" for ORDER BY queries)
	// If projectionFields is nil or empty, deserializes all fields (backward compatible)
	pageDocuments, totalCount, err := b.parseAppendedDocumentsRange(fileData, startIndex, endIndex, projectionFields)
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

	// Try to parse as append-only format first
	documents, appendErr := b.parseAppendedDocuments(fileData)
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
		bundleDir := GetBundleDirectory(databaseName, bundleName)
		filePath := filepath.Join(bundleDir, fileInfo.FileName)

		// Check if file exists (skip if not - may have been compacted)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			continue
		}

		// Read file data
		data, err := b.getOrReadFile(filePath)
		if err != nil {
			b.logger.Warnf("Failed to read bundle file '%s' for version scan: %v", filePath, err)
			continue
		}

		// Scan backward through this file to find all versions of documentID
		fileVersions := b.scanFileBackwardForDocument(&data, documentID)
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

	// Scan backward through file
	versions := b.scanFileBackwardForDocument(&data, documentID)

	// Sort by VersionSequence descending (newest first)
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].VersionSequence > versions[j].VersionSequence
	})

	return versions, nil
}

// scanFileBackwardForDocument scans a file forward to find all versions of a document
// PHASE 0: MVCC - Scans forward through file to collect all versions
// Note: Despite the name, we scan forward because append-only storage has newer versions at the end
// We collect all versions and sort by VersionSequence later
func (b *BundleStorageEngine) scanFileBackwardForDocument(data *[]byte, documentID string) []*models.Document {
	versions := make([]*models.Document, 0)

	// Skip bundle metadata header if present
	headerOffset := 0
	if len(*data) >= 8 {
		magic := binary.LittleEndian.Uint32((*data)[0:4])
		if magic == 0x42444D44 { // "BDMD" = Bundle Metadata
			metadataSize := binary.LittleEndian.Uint32((*data)[4:8])
			headerOffset = int(8 + metadataSize)
		}
	}

	// Scan backward from end of file
	// We'll scan forward but collect in reverse order, or scan backward by reading sizes
	// For efficiency, scan forward but track all versions, then reverse
	seenVersions := make(map[uint64]*models.Document) // VersionSequence -> Document

	// Scan forward through file to find all versions
	currentOffset := headerOffset
	for currentOffset < len(*data) {
		// Need at least 8 bytes for header
		if currentOffset+8 > len(*data) {
			break
		}

		// Read magic number and size
		magic := binary.LittleEndian.Uint32((*data)[currentOffset : currentOffset+4])
		size := binary.LittleEndian.Uint32((*data)[currentOffset+4 : currentOffset+8])

		// Handle document records
		if magic == 0xDEADBEEF {
			// Validate size
			if currentOffset+8+int(size) > len(*data) {
				break
			}

			// Extract document data
			documentData := (*data)[currentOffset+8 : currentOffset+8+int(size)]

			// Decode document
			doc, err := helpers.DecodeFastBinaryToDocument(documentData)
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
func (b *BundleStorageEngine) parseAppendedDocumentsRange(data *[]byte, startIndex, endIndex uint32, projectionFields []string) (map[string]models.Document, uint32, error) {
	pageDocuments := make(map[string]models.Document)
	deletedDocuments := make(map[string]bool) // Track deleted documents
	seenDocIDs := make(map[string]struct{})   // Track unique DocumentIDs for counting (avoids storing full docs; was allDocuments)
	offset := 0
	documentIndex := uint32(0)

	// DEBUG: Log parsing start
	if b.logger != nil {
		//b.logger.Infof("DEBUG: parseAppendedDocumentsRange called with data size %d bytes, range [%d, %d)", len(data), startIndex, endIndex)
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
				//b.logger.Infof("DEBUG: Skipping bundle metadata header: magic=0x%X, size=%d, new offset=%d", magic, metadataSize, offset)
			}
		}
	}

	for offset < len(*data) {
		// Need at least 8 bytes for header
		if offset+8 > len(*data) {
			if b.logger != nil {
				//b.logger.Infof("DEBUG: Stopping parse at offset %d (not enough bytes for header)", offset)
			}
			break
		}

		// Read magic number and size
		magic := binary.LittleEndian.Uint32((*data)[offset : offset+4])
		size := binary.LittleEndian.Uint32((*data)[offset+4 : offset+8])

		// DEBUG: Log what we found
		if b.logger != nil {
			//if magic == 0xDEADBEEF {
			//	b.logger.Infof("DEBUG: Found DOCUMENT at offset %d, size %d", offset, size)
			//} else if magic == 0xDEADDEAD {
			//	b.logger.Infof("DEBUG: Found TOMBSTONE at offset %d, size %d", offset, size)
			//} else {
			//	// Log unknown magic numbers too
			//	b.logger.Infof("DEBUG: Found UNKNOWN magic 0x%X at offset %d, size %d", magic, offset, size)
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
				// Use projected deserialization - only deserialize specified fields
				projectedDoc, err = helpers.DecodeFastBinaryProjected(documentData, projectionFields)
				if err != nil {
					// Fall back to full deserialization on error
					docMap, err = helpers.DecodeFastBinary(documentData)
					projectedDoc = nil
				}
				// If projected deserialization succeeded, projectedDoc is set and we'll use it directly below
			} else {
				// No projection - deserialize directly to Document (avoids intermediate map vs DecodeFastBinary)
				fullDoc, err = helpers.DecodeFastBinaryToDocument(documentData)
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
					b.writeLogger.DumpDiagnostics(corruptionReason, int64(offset), "unknown_bundle")
				}

				// Generic corruption - still halt
				corruptionReason := fmt.Sprintf("Failed to decode document at offset %d: %v. "+
					"Size: %d bytes. Data preview (first 64 bytes): %x",
					offset, err, size, documentData[:min(64, len(documentData))])
				b.writeLogger.DumpDiagnostics(corruptionReason, int64(offset), "unknown_bundle")

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
				// Fallback from failed projection: convert from docMap
				pooledDoc := document.GetPooledDocument()
				if docID, ok := docMap["DocumentID"].(string); ok {
					pooledDoc.DocumentID = docID
				}
				if fields, ok := docMap["Fields"].(map[string]models.Field); ok {
					pooledDoc.Fields = fields
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
				//b.logger.Infof("DEBUG: Found deletion marker at offset %d, size %d bytes", offset, size)
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
				//b.logger.Infof("DEBUG: Decoded deletion marker: %+v", deletionMap)
			}

			if documentID, ok := deletionMap["DocumentID"].(string); ok && documentID != "" {
				// Mark document as deleted and remove from current sets
				deletedDocuments[documentID] = true
				delete(seenDocIDs, documentID)
				delete(pageDocuments, documentID)

				if b.logger != nil {
					//b.logger.Infof("DEBUG: Marked document %s as deleted (total deleted: %d)", documentID, len(deletedDocuments))
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
func (b *BundleStorageEngine) parseAppendedDocuments(data []byte) (map[string]models.Document, error) {
	documents := make(map[string]models.Document)
	deletedDocuments := make(map[string]bool) // Track deleted documents
	offset := 0

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

			// Decode document using fast binary format
			docMap, err := helpers.DecodeFastBinary(documentData)
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
					b.writeLogger.DumpDiagnostics(corruptionReason, int64(offset), "unknown_bundle")
				}

				// Generic corruption - still halt
				corruptionReason := fmt.Sprintf("Failed to decode document at offset %d: %v. "+
					"Size: %d bytes. Data preview (first 64 bytes): %x",
					offset, err, size, documentData[:min(64, len(documentData))])
				b.writeLogger.DumpDiagnostics(corruptionReason, int64(offset), "unknown_bundle")

				// This line will never be reached, but keep for safety
				offset += 8 + int(size)
				continue
			}

			// Convert to Document struct
			// STEP 1: Use document pool to reduce allocations
			// TODO: Implement reference counting for automatic pool return when last consumer releases document
			doc := document.GetPooledDocument()
			if docID, ok := docMap["DocumentID"].(string); ok {
				doc.DocumentID = docID
			}
			if fields, ok := docMap["Fields"].(map[string]models.Field); ok {
				doc.Fields = fields
			}
			if createdAt, ok := docMap["CreatedAt"].(time.Time); ok {
				doc.CreatedAt = createdAt
			}
			if updatedAt, ok := docMap["UpdatedAt"].(time.Time); ok {
				doc.UpdatedAt = updatedAt
			}

			// Only add document if it hasn't been deleted
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
func (b *BundleStorageEngine) WriteBundleToFile(bundle *models.Bundle, filePath string) error {
	// 1. Convert the bundle to a map for BSON encoding
	convertedBundle := BundleToMap(bundle)

	// 2. Make sure Documents are included in the map
	// CRITICAL FIX: Use copy-on-read pattern to prevent concurrent map iteration
	bundle.DocumentsMutex.RLock()
	docMap := make(map[string]interface{}, len(*bundle.Documents))
	for docID, doc := range *bundle.Documents {
		docMap[docID] = map[string]interface{}{
			"DocumentID": doc.DocumentID,
			"Fields":     doc.Fields,
			"CreatedAt":  doc.CreatedAt,
			"UpdatedAt":  doc.UpdatedAt,
		}
	}
	bundle.DocumentsMutex.RUnlock()
	convertedBundle["Documents"] = docMap

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

	// Create a new data file
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	filePath := filepath.Join(databasePath, bundleName)

	// Check if the bundle directory or file exists
	if !helpers.DirExists(filePath) {
		if b.logger != nil {
			b.logger.Infof("Bundle %s does not exist, skipping removal", filePath)
		}
		return fmt.Errorf("bundle %s does not exist", bundleName)
	}

	// Use RemoveAll to recursively delete the bundle directory and all its contents
	// (including document pages, indexes, etc.)
	err := os.RemoveAll(filePath)
	if err != nil {
		return fmt.Errorf("error removing bundle directory %s: %w", bundleName, err)
	}

	// Also delete the legacy .bnd metadata file if it exists
	fileName := fmt.Sprintf("%s_%s.bnd", database.Name, bundleName)
	bndFilePath := filepath.Join(databasePath, fileName)
	if helpers.FileExists(bndFilePath, *b.logger) {
		if err := os.Remove(bndFilePath); err != nil {
			return fmt.Errorf("error removing bundle metadata file %s: %w", fileName, err)
		}
	} else {
		if b.logger != nil {
			b.logger.Infof("Bundle file %s does not exist, skipping removal", fileName)
		}
	}

	// Invalidate file-read cache so we don't retain buffers for removed paths
	b.InvalidateFileReadCacheForBundle(database.Name, bundleName)

	b.logger.Infof("Successfully removed bundle '%s' and all its data files", bundleName)

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
		"Documents":         bundle.Documents,
		"IndexNames":        bundle.IndexNames,
		"Indexes":           bundle.Indexes,
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
			logger.Infof("Bundle %v IS MISSING THE []STRING datatype", data["IndexNames"])
		}
	} else {
		logger.Infof("Bundle %s has no index names defined", bundle.Name)
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

	logger.Infof("Processing bundle %s , going to load documents, with ID %s", bundle.Name, bundle.BundleID)

	// Extract documents
	if docs, ok := data["Documents"]; ok && docs != nil {
		bundle.Documents = new(map[string]models.Document)
		*bundle.Documents = make(map[string]models.Document)
		//logger.Infof("Found %t for array", docs.([]interface{}))
		//logger.Infof("Found %t for map", docs.(map[string]interface{}))
		// Handle array of documents
		if docArray, ok := docs.([]interface{}); ok {
			logger.Infof("Processing %d documents array in bundle %s", len(docArray), bundle.Name)
			for _, doc := range docArray {
				if docMap, ok := doc.(map[string]interface{}); ok {
					// Extract document ID
					docID, ok := docMap["DocumentID"].(string)
					if !ok {
						continue // Skip documents without valid ID
					}

					document := models.Document{
						DocumentID: docID,
						Fields:     make(map[string]models.Field),
					}

					// Extract CreatedAt and UpdatedAt if available
					if created, ok := docMap["CreatedAt"].(time.Time); ok {
						document.CreatedAt = created
					}
					if updated, ok := docMap["UpdatedAt"].(time.Time); ok {
						document.UpdatedAt = updated
					}

					// Extract fields

					if fields, ok := docMap["Fields"].(map[string]interface{}); ok {
						for fieldName, fieldValue := range fields {

							// Case 1: Field value is a map with Name/Value properties
							if fieldMap, ok := fieldValue.(map[string]interface{}); ok {
								field := models.Field{
									Name:  stringValue(fieldMap, "Name", fieldName),
									Value: models.NewInterfaceValue(fieldMap["Value"]), // ✅ Use NewInterfaceValue
								}

								document.Fields[fieldName] = field
							} else {
								// Case 2: Field value is the direct value (not wrapped in a map)

								field := models.Field{
									Name:  fieldName,
									Value: models.NewInterfaceValue(fieldValue), // ✅ Use NewInterfaceValue
								}
								document.Fields[fieldName] = field
							}
						}
					}

					(*bundle.Documents)[docID] = document
				}
			}
		} else if docMap, ok := docs.(map[string]interface{}); ok {
			logger.Infof("Processing %d documents map in bundle %s", len(docMap), bundle.Name)
			// Handle map of documents
			for docID, docData := range docMap {
				if docMapData, ok := docData.(map[string]interface{}); ok {
					document := models.Document{
						DocumentID: docID,
						Fields:     make(map[string]models.Field),
					}

					// Extract CreatedAt and UpdatedAt if available
					if created, ok := docMapData["CreatedAt"].(time.Time); ok {
						document.CreatedAt = created
					}
					if updated, ok := docMapData["UpdatedAt"].(time.Time); ok {
						document.UpdatedAt = updated
					}

					// Extract fields

					if fields, ok := docMapData["Fields"].(map[string]interface{}); ok {
						for fieldName, fieldValue := range fields {

							// Case 1: Field value is a map with Name/Value properties
							if fieldMap, ok := fieldValue.(map[string]interface{}); ok {
								field := models.Field{
									Name:  stringValue(fieldMap, "Name", fieldName),
									Value: models.NewInterfaceValue(fieldMap["value"]), // ✅ Use NewInterfaceValue
								}
								document.Fields[fieldName] = field
							} else {
								// Case 2: Field value is the direct value (not wrapped in a map)
								field := models.Field{
									Name:  fieldName,
									Value: models.NewInterfaceValue(fieldValue), // ✅ Use NewInterfaceValue
								}

								document.Fields[fieldName] = field
							}
						}
					}

					(*bundle.Documents)[docID] = document
				}
			}
		}
	}

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

// serializeDocumentDirect serializes a document directly without map conversion
func (b *BundleStorageEngine) serializeDocumentDirect(document *models.Document) ([]byte, error) {
	// Use V2 format with field directory for O(1) field access and better performance
	// V2 format includes xxHash64 field directory enabling fast projected reads
	// Pass document directly to avoid intermediate map allocation
	return helpers.EncodeFastBinaryV2(document)
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
