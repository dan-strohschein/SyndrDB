package documentscanner

import (
	"fmt"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

// Constants for safety limits to prevent DoS while allowing legitimate large datasets
const (
	// MaxReasonablePageCount prevents runaway queries from corrupt metadata
	// This represents 100M documents at 1000 docs/page - a reasonable upper bound
	MaxReasonablePageCount = uint32(100000)

	// MinPageCount prevents zero-page bundles from being treated as empty
	MinPageCount = uint32(1)
)

// ScannerFactory creates and configures document scanners
// This factory pattern allows for easy testing and configuration management
type ScannerFactory struct {
	logger        *zap.SugaredLogger
	defaultConfig *ScannerConfig
	cacheFactory  func(size int) CacheInterface
}

// NewScannerFactory creates a new scanner factory with default configuration
// logger: Logger for debugging and monitoring
// config: Default configuration (use nil for built-in defaults)
func NewScannerFactory(logger *zap.SugaredLogger, config *ScannerConfig) *ScannerFactory {
	if config == nil {
		config = DefaultScannerConfig()
	}

	return &ScannerFactory{
		logger:        logger,
		defaultConfig: config,
		cacheFactory:  DefaultCacheFactory,
	}
}

// CreateScanner creates a new SmartBundleScanner for the specified bundle
// bundle: The bundle to scan
// config: Configuration overrides (use nil for factory defaults)
func (sf *ScannerFactory) CreateScanner(bundle BundleInterface, config *ScannerConfig) (DocumentScannerInterface, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	// Use provided config or fall back to factory defaults
	if config == nil {
		config = sf.defaultConfig
	}

	// Create cache instance
	cache := sf.cacheFactory(config.CacheSize)

	// Create and return scanner
	scanner := NewSmartBundleScanner(bundle, config, cache, sf.logger)

	sf.logger.Debugf("Created smart scanner for bundle '%s' ",
		bundle.GetName())

	return scanner, nil
}

// CreateScannerWithCache creates a scanner with a custom cache implementation
// This allows for integration with external caching systems
func (sf *ScannerFactory) CreateScannerWithCache(
	bundle BundleInterface,
	config *ScannerConfig,
	cache CacheInterface,
) (DocumentScannerInterface, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	if cache == nil {
		return nil, fmt.Errorf("cache cannot be nil")
	}

	// Use provided config or fall back to factory defaults
	if config == nil {
		config = sf.defaultConfig
	}

	scanner := NewSmartBundleScanner(bundle, config, cache, sf.logger)

	sf.logger.Infof("Created scanner for bundle '%s' with custom cache", bundle.GetName())

	return scanner, nil
}

// SetCacheFactory allows setting a custom cache factory function
// This enables integration with different caching backends
func (sf *ScannerFactory) SetCacheFactory(factory func(size int) CacheInterface) {
	sf.cacheFactory = factory
}

// GetDefaultConfig returns a copy of the factory's default configuration
func (sf *ScannerFactory) GetDefaultConfig() *ScannerConfig {
	configCopy := *sf.defaultConfig
	return &configCopy
}

// UpdateDefaultConfig updates the factory's default configuration
// This affects all future scanners created by this factory
func (sf *ScannerFactory) UpdateDefaultConfig(config *ScannerConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	// Validate configuration
	if err := ValidateScannerConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	sf.defaultConfig = config
	sf.logger.Info("Updated factory default configuration")

	return nil
}

// DefaultCacheFactory creates a SimpleLRUCache with the specified size
// This is the default cache implementation used by the scanner factory
func DefaultCacheFactory(size int) CacheInterface {
	return NewSimpleLRUCache(size)
}

// ValidateScannerConfig validates a scanner configuration
// Returns an error if the configuration has invalid values
func ValidateScannerConfig(config *ScannerConfig) error {
	if config.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got %d", config.BatchSize)
	}

	if config.MaxBatches <= 0 {
		return fmt.Errorf("max batches must be positive, got %d", config.MaxBatches)
	}

	if config.HotThreshold <= 0 {
		return fmt.Errorf("hot threshold must be positive, got %d", config.HotThreshold)
	}

	if config.OptimizeAfter <= 0 {
		return fmt.Errorf("optimize after must be positive, got %d", config.OptimizeAfter)
	}

	if config.MemoryThreshold <= 0 {
		return fmt.Errorf("memory threshold must be positive, got %d", config.MemoryThreshold)
	}

	if config.CacheSize <= 0 {
		return fmt.Errorf("cache size must be positive, got %d", config.CacheSize)
	}

	if config.MaxGoroutines <= 0 {
		return fmt.Errorf("max goroutines must be positive, got %d", config.MaxGoroutines)
	}

	if config.ChannelBuffer <= 0 {
		return fmt.Errorf("channel buffer must be positive, got %d", config.ChannelBuffer)
	}

	if config.MetricsInterval <= 0 {
		return fmt.Errorf("metrics interval must be positive, got %v", config.MetricsInterval)
	}

	return nil
}

// BundleAdapter adapts a SyndrDB Bundle to the BundleInterface with streaming support
// This allows the scanner to work with the existing Bundle model without loading all documents
type BundleAdapter struct {
	bundle        *models.Bundle         // SyndrDB Bundle model
	bundleService BundleServiceInterface // Service for loading documents
	// Cached metadata (small and efficient)
	totalDocuments *int                            // Cached total document count
	documentIDs    []string                        // Cached document IDs (loaded lazily)
	cachedPages    map[uint32]*models.DocumentPage // Page-level cache
	logger         *zap.SugaredLogger              // Logger for debugging and monitoring
	projectionFields []string // PROJECTION PUSHDOWN: Field names to deserialize during page loading (nil = all fields)
}

// NewBundleAdapter creates a new adapter for a SyndrDB Bundle with streaming support
// This adapter provides efficient lazy loading without loading all documents at once
func NewBundleAdapter(bundle *models.Bundle, bundleService BundleServiceInterface, logger *zap.SugaredLogger) *BundleAdapter {
	// SAFETY: Log bundle metadata immediately to diagnose potential infinite loop causes
	if bundle != nil {
		//logger.Infof("DEBUG: Creating NEW BundleAdapter for bundle '%s' (instance %p)", bundle.Name, bundle)
		// logger.Infof("SAFETY CHECK: Creating BundleAdapter for bundle '%s'", bundle.Name)
		// logger.Infof("SAFETY CHECK: Bundle.PageCount = %d", bundle.PageCount)
		// logger.Infof("SAFETY CHECK: Bundle.TotalDocuments = %d", bundle.TotalDocuments)
		// logger.Infof("SAFETY CHECK: Bundle.DocumentsComplete = %v", bundle.DocumentsComplete)
		if bundle.Documents != nil {
			logger.Debugf("SAFETY CHECK: Bundle.Documents is not nil, has %d documents", len(*bundle.Documents))
		} else {
			logger.Debugf("SAFETY CHECK: Bundle.Documents is nil (expected for page-based loading)")
		}

		// CRITICAL SAFETY: If PageCount is suspiciously high, log error and set reasonable limit
		if bundle.PageCount > 10000 {
			logger.Errorf("CRITICAL SAFETY: Bundle PageCount (%d) is dangerously high! This would cause infinite loops.", bundle.PageCount)
			logger.Errorf("CRITICAL SAFETY: Setting PageCount to safe value of 0 to prevent crashes")
			bundle.PageCount = 0 // Force safe value
		}
	} else {
		logger.Errorf("SAFETY CHECK: Bundle is nil!")
	}

	adapter := &BundleAdapter{
		bundle:        bundle,
		bundleService: bundleService,
		cachedPages:   make(map[uint32]*models.DocumentPage),
		logger:        logger,
	}

	//logger.Infof("DEBUG: Created BundleAdapter instance %p with fresh cachedPages map %p", adapter, adapter.cachedPages)

	return adapter
}

// SetProjectionFields sets the projection fields for projection pushdown optimization
// PROJECTION PUSHDOWN: This allows the storage layer to deserialize only specified fields
// For ORDER BY queries, this saves ~80-90% deserialization overhead (e.g., only deserialize "name" field)
// Called from FullScanNode when ORDER BY is present to optimize document loading
func (ba *BundleAdapter) SetProjectionFields(fields []string) {
	ba.projectionFields = fields
	if len(fields) > 0 {
		ba.logger.Debugf("PROJECTION PUSHDOWN: Set projection fields on BundleAdapter: %v", fields)
	}
}

// ===== STREAMING IMPLEMENTATION - NO MORE INFINITE LOOPS =====

// loadDocumentPage loads a specific page with caching
// PROJECTION PUSHDOWN: Passes projection fields to storage layer if set
func (ba *BundleAdapter) loadDocumentPage(pageID uint32) (*models.DocumentPage, error) {
	// Check cache first
	if page, exists := ba.cachedPages[pageID]; exists {
		return page, nil
	}

	if ba.bundleService == nil {
		return nil, fmt.Errorf("bundle service not available")
	}

	// Get database path from bundle
	databasePath := helpers.GetDatabaseFolderPath(ba.bundle.Name) //fmt.Sprintf("data_files/%s", ba.bundle.Database.Name)

	// PROJECTION PUSHDOWN: Set projection fields on storage engine before loading page
	// For ORDER BY queries, this allows the storage layer to deserialize only specified fields
	// This saves ~80-90% deserialization overhead when documents have many unused fields
	// Example: ORDER BY name query with 10-field documents → only deserialize "name" + DocumentID
	if len(ba.projectionFields) > 0 {
		// Type assert bundleService to access SetProjectionFieldsForBundle method
		// This passes projection through BundleService → BundleStorageEngine → readDocumentRange
		if projectionService, ok := ba.bundleService.(interface {
			SetProjectionFieldsForBundle(bundleName string, fields []string)
		}); ok {
			projectionService.SetProjectionFieldsForBundle(ba.bundle.Name, ba.projectionFields)
			ba.logger.Debugf("PROJECTION PUSHDOWN: Set projection fields %v on storage engine for bundle '%s'", ba.projectionFields, ba.bundle.Name)
		}
	}

	// Load page from storage
	page, err := ba.bundleService.LoadDocumentPage(ba.bundle.Name, ba.bundle.Database.Name, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load page %d: %w", pageID, err)
	}

	// Cache the page for future access
	ba.cachedPages[pageID] = page

	return page, nil
}

// getSafePageCount returns a validated page count with defensive bounds checking
// This replaces hard-coded maxSafePages with metadata-driven limits
// Returns: validated page count that is safe to iterate over
func (ba *BundleAdapter) getSafePageCount() uint32 {
	pageCount := uint32(ba.bundle.PageCount)

	// CRITICAL FIX: Handle negative TotalDocuments (corruption from over-deletion)
	// If TotalDocuments is negative, it's corrupted - set to 0 to trigger recovery below
	if ba.bundle.TotalDocuments < 0 {
		ba.logger.Errorf("CORRUPTION DETECTED: Bundle '%s' has negative TotalDocuments (%d), will recover by scanning pages",
			ba.bundle.Name, ba.bundle.TotalDocuments)
		ba.bundle.TotalDocuments = 0
		ba.bundle.PageCount = 0
		pageCount = 0
		// Mark bundle as dirty so corrected metadata gets persisted
		ba.bundle.IsDirty = true
	}

	// Calculate expected PageCount from TotalDocuments and PageSize for validation
	pageSize := uint32(4096) // Standard page size (power of 2)
	if ba.bundle.PageSize > 0 {
		pageSize = uint32(ba.bundle.PageSize)
	}
	expectedPageCount := uint32(0)
	if ba.bundle.TotalDocuments > 0 {
		expectedPageCount = uint32((ba.bundle.TotalDocuments + int64(pageSize) - 1) / int64(pageSize))
	}

	// CRITICAL FIX: If PageCount is 0 but TotalDocuments > 0, calculate from documents
	// This handles cases where metadata persistence failed but documents exist
	if pageCount == 0 && ba.bundle.TotalDocuments > 0 {
		pageCount = expectedPageCount
		ba.bundle.PageCount = int64(pageCount)
		ba.bundle.IsDirty = true
		ba.logger.Warnf("RECOVERY: PageCount was 0 but TotalDocuments=%d, calculated pageCount=%d",
			ba.bundle.TotalDocuments, pageCount)
	}

	// CRITICAL FIX: Validate PageCount matches expected value based on TotalDocuments and PageSize
	// This fixes cases where PageCount was calculated with incorrect PageSize (e.g., old default of 100)
	if pageCount > 0 && expectedPageCount > 0 && pageCount != expectedPageCount {
		ba.logger.Warnf("CORRECTION: PageCount (%d) doesn't match expected value (%d) based on TotalDocuments=%d, PageSize=%d. Correcting...",
			pageCount, expectedPageCount, ba.bundle.TotalDocuments, ba.bundle.PageSize)
		pageCount = expectedPageCount
		ba.bundle.PageCount = int64(pageCount)
		ba.bundle.IsDirty = true
	}

	// DEFENSIVE: If both are 0, try to recover by scanning pages
	// This is expensive but necessary when metadata is completely corrupted
	if pageCount == 0 {
		ba.logger.Warnf("SAFETY: Both PageCount and TotalDocuments are 0 for bundle '%s', attempting recovery by scanning pages",
			ba.bundle.Name)
		
		// Try to find actual page count by scanning pages up to a reasonable limit
		// Scan up to MaxReasonablePageCount pages to find where documents actually end
		maxScanPages := MaxReasonablePageCount
		actualPageCount := uint32(0)
		
		for testPageID := uint32(0); testPageID < maxScanPages; testPageID++ {
			page, err := ba.loadDocumentPage(testPageID)
			if err != nil {
				// Page doesn't exist or error loading - we've reached the end
				break
			}
			if len(page.Documents) == 0 {
				// Empty page - might be the end, but continue a bit more to be sure
				// Check a few more pages to confirm
				emptyCount := 0
				for i := uint32(1); i <= 3 && (testPageID+i) < maxScanPages; i++ {
					testPage, testErr := ba.loadDocumentPage(testPageID + i)
					if testErr != nil || len(testPage.Documents) == 0 {
						emptyCount++
					} else {
						emptyCount = 0
						break
					}
				}
				if emptyCount >= 3 {
					// Found 3+ consecutive empty pages - we've reached the end
					break
				}
			}
			actualPageCount = testPageID + 1
		}
		
		if actualPageCount > 0 {
			// Recovered page count - now count documents
			actualDocCount := 0
			for pageID := uint32(0); pageID < actualPageCount; pageID++ {
				page, err := ba.loadDocumentPage(pageID)
				if err != nil {
					continue
				}
				actualDocCount += len(page.Documents)
			}
			
			ba.bundle.TotalDocuments = int64(actualDocCount)
			ba.bundle.PageCount = int64(actualPageCount)
			pageCount = actualPageCount
			ba.bundle.IsDirty = true
			
			ba.logger.Warnf("RECOVERY SUCCESS: Recovered TotalDocuments=%d, PageCount=%d for bundle '%s'",
				ba.bundle.TotalDocuments, pageCount, ba.bundle.Name)
		} else {
			// Could not recover - use minimum
			ba.logger.Warnf("RECOVERY FAILED: Could not recover page count for bundle '%s', using MinPageCount",
				ba.bundle.Name)
			return MinPageCount
		}
	}

	// DEFENSIVE: Cap at reasonable maximum to prevent DoS from corrupt metadata
	if pageCount > MaxReasonablePageCount {
		ba.logger.Errorf("SAFETY: Bundle '%s' PageCount (%d) exceeds maximum reasonable limit (%d), capping",
			ba.bundle.Name, pageCount, MaxReasonablePageCount)
		return MaxReasonablePageCount
	}

	return pageCount
}

// getTotalDocumentsCount gets the accurate count using optimized count-only parser
// CRITICAL: This count CANNOT be cached because document deletions create tombstones
// which reduce the count dynamically. The count-only parser filters tombstones,
// so we get accurate counts after bulk deletes.
func (ba *BundleAdapter) getTotalDocumentsCount() int {
	// Fast path: If documents are complete in memory, count them directly
	if ba.bundle.Documents != nil && ba.bundle.DocumentsComplete {
		return len(*ba.bundle.Documents)
	}

	// OPTIMIZATION: Use count-only parser instead of loading all pages
	// This is much faster because it extracts only DocumentIDs without parsing full documents
	if ba.bundleService != nil {
		// Get database name from bundle
		databaseName := ""
		if ba.bundle.Database != nil {
			databaseName = ba.bundle.Database.Name
		}

		// Use optimized count-only parser
		count, err := ba.bundleService.CountDocuments(ba.bundle.Name, databaseName)
		if err == nil {
			ba.logger.Debugf("Count-only parser returned %d documents for bundle '%s'", count, ba.bundle.Name)
			return count
		}

		// Fallback: If count-only parser fails, log warning and use old method
		ba.logger.Warnf("Count-only parser failed for bundle '%s': %v, falling back to page-based count", ba.bundle.Name, err)
	}

	// FALLBACK: Old method - load all pages (slower but reliable)
	// CRITICAL FIX: Do NOT use cached value or ba.bundle.TotalDocuments metadata
	// The append-only storage format means tombstones reduce the actual count
	// Always count by iterating pages to get accurate count after deletions
	count := 0
	pageCount := ba.getSafePageCount()

	for pageID := uint32(0); pageID < pageCount; pageID++ {
		page, err := ba.loadDocumentPage(pageID)
		if err != nil {
			continue
		}
		count += len(page.Documents)
	}

	return count
}

// GetDocumentIDs returns all document IDs lazily without loading full documents
func (ba *BundleAdapter) GetDocumentIDs() []string {
	// CRITICAL DEBUG: Log entry point
	//ba.logger.Infof("🔍 GetDocumentIDs CALLED - Bundle: %s, TotalDocuments: %d, PageCount: %d, DocumentsComplete: %v, Documents map size: %d",
	//ba.bundle.Name, ba.bundle.TotalDocuments, ba.bundle.PageCount, ba.bundle.DocumentsComplete,
	// func() int {
	// 	if ba.bundle.Documents != nil {
	// 		return len(*ba.bundle.Documents)
	// 	}
	// 	return 0
	// }())

	// CASSANDRA-STYLE MEMTABLE MERGE:
	// Don't use cached IDs - always build fresh list from disk + memtable
	// This ensures we see both persisted documents AND recent writes

	var ids []string

	// Get safe page count using validated metadata
	pageCount := ba.getSafePageCount()

	// Load document IDs from all pages on disk
	diskIDSet := make(map[string]bool)
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		page, err := ba.loadDocumentPage(pageID)
		if err != nil {
			continue
		}

		// Extract only document IDs, not full documents
		for docID := range page.Documents {
			ids = append(ids, docID)
			diskIDSet[docID] = true
		}
	}

	ba.logger.Debugf("Loaded %d document IDs from disk", len(ids))

	// CASSANDRA-STYLE MEMTABLE MERGE: Add memtable document IDs
	// Only merge if Documents exists AND is marked as incomplete (memtable mode)
	// CRITICAL: Must hold DocumentsMutex to prevent concurrent map iteration/write
	ba.bundle.DocumentsMutex.RLock()
	if ba.bundle.Documents != nil && !ba.bundle.DocumentsComplete {
		ba.logger.Debugf("MEMTABLE MERGE: Adding documents from memtable (has %d docs)", len(*ba.bundle.Documents))
		memtableCount := 0
		for docID := range *ba.bundle.Documents {
			if !diskIDSet[docID] {
				ids = append(ids, docID)
				memtableCount++
			}
		}
		ba.logger.Debugf("MEMTABLE MERGE: Added %d new documents from memtable", memtableCount)
	}
	ba.bundle.DocumentsMutex.RUnlock()

	//ba.logger.Debugf("Returning %d total document IDs (disk + memtable)", len(ids))
	return ids
}

// GetDocument returns a single document by ID using streaming approach
func (ba *BundleAdapter) GetDocument(docID string) *models.Document {
	// CASSANDRA-STYLE MEMTABLE CHECK FIRST:
	// Check memtable before going to disk (recent writes have priority)
	// CRITICAL: Acquire read lock to prevent race condition with concurrent batch updates
	ba.bundle.DocumentsMutex.RLock()
	if ba.bundle.Documents != nil && !ba.bundle.DocumentsComplete {
		if doc, exists := (*ba.bundle.Documents)[docID]; exists {
			ba.bundle.DocumentsMutex.RUnlock()
			// ba.logger.Debugf("Document '%s' found in memtable", docID) // PERF: Disabled - causes 2.2M allocs/query
			// PHASE E: For read-only SELECT, use pointer directly (no copy needed)
			return &doc
		}
	}
	ba.bundle.DocumentsMutex.RUnlock()

	// Get safe page count using validated metadata
	pageCount := ba.getSafePageCount()

	// Stream through pages to find the specific document on disk
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		page, err := ba.loadDocumentPage(pageID)
		if err != nil {
			continue
		}

		if doc, exists := page.Documents[docID]; exists {
			// PHASE E: For read-only SELECT, use pointer directly (no copy needed)
			return &doc
		}
	}

	return nil // Document not found
}

// GetAllDocuments returns all documents - WARNING: Use sparingly for large bundles!
// This method is kept for compatibility but should be avoided for large datasets
func (ba *BundleAdapter) GetAllDocuments() map[string]*models.Document {
	ba.logger.Debugf("GetAllDocuments called for bundle '%s'", ba.bundle.Name)

	// EARLY RETURN OPTIMIZATION: Check DocumentsComplete at the start to skip all disk I/O
	// If Documents is marked complete, use it directly without loading any pages
	ba.bundle.DocumentsMutex.RLock()
	if ba.bundle.Documents != nil && ba.bundle.DocumentsComplete {
		ba.bundle.DocumentsMutex.RUnlock()
		ba.logger.Debugf("Using complete Documents cache with %d documents (skipping disk I/O)", len(*ba.bundle.Documents))
		allDocs := make(map[string]*models.Document, len(*ba.bundle.Documents))
		for docID, doc := range *ba.bundle.Documents {
			// Copy document to avoid loop variable aliasing
			docCopy := new(models.Document)
			*docCopy = doc
			allDocs[docID] = docCopy
		}
		return allDocs
	}
	ba.bundle.DocumentsMutex.RUnlock()

	ba.logger.Debugf("GetAllDocuments: PageCount=%d, TotalDocuments=%d, DocumentsComplete=%v, Documents!=nil=%v",
		ba.bundle.PageCount, ba.bundle.TotalDocuments, ba.bundle.DocumentsComplete, ba.bundle.Documents != nil)

	if ba.bundle.Documents != nil {
		ba.logger.Debugf("GetAllDocuments: Bundle.Documents has %d entries", len(*ba.bundle.Documents))
	}

	allDocs := make(map[string]*models.Document)

	// Get safe page count using validated metadata
	pageCount := ba.getSafePageCount()

	// WARN if loading a very large bundle completely into memory
	estimatedDocs := int64(pageCount) * 1000 // Rough estimate
	if estimatedDocs > 100000 {
		ba.logger.Warnf("Loading all documents from large bundle '%s' (est. %d docs, %d pages) - consider using streaming",
			ba.bundle.Name, estimatedDocs, pageCount)
	}

	// Load all pages from disk
	if pageCount > 0 {
		ba.logger.Debugf("Loading documents from disk (PageCount=%d)", pageCount)

		// Stream through pages and collect all documents
		for pageID := uint32(0); pageID < pageCount; pageID++ {
			page, err := ba.loadDocumentPage(pageID)
			if err != nil {
				ba.logger.Errorf("Failed to load page %d: %v", pageID, err)
				continue
			}

			for docID, doc := range page.Documents {
				// Copy document to avoid loop variable aliasing
				docCopy := new(models.Document)
				*docCopy = doc
				allDocs[docID] = docCopy
			}
		}
		ba.logger.Debugf("Loaded %d documents from disk", len(allDocs))
	}

	// Merge with memtable (recent writes not yet flushed to new pages)
	// Only merge if Documents exists AND is marked as incomplete (memtable mode)
	// CRITICAL: Must hold DocumentsMutex to prevent concurrent map iteration/write
	ba.bundle.DocumentsMutex.RLock()
	if ba.bundle.Documents != nil && !ba.bundle.DocumentsComplete {
		ba.logger.Debugf("Merging %d documents from memtable with %d from disk",
			len(*ba.bundle.Documents), len(allDocs))

		// Add memtable documents that aren't already in disk results
		// Disk wins for conflicts (should never happen, but defensive)
		mergedCount := 0
		for docID, doc := range *ba.bundle.Documents {
			if _, exists := allDocs[docID]; !exists {
				// Copy document to avoid loop variable aliasing
				docCopy := new(models.Document)
				*docCopy = doc
				allDocs[docID] = docCopy
				mergedCount++
			}
		}
		ba.logger.Debugf("Merged %d new documents from memtable", mergedCount)
	} else {
		ba.logger.Debugf("No memtable to merge (Documents=nil or DocumentsComplete=true)")
	}
	ba.bundle.DocumentsMutex.RUnlock()

	ba.logger.Debugf("Returning %d total documents (disk + memtable)", len(allDocs))
	return allDocs
}

// GetAllDocumentsWithLimit returns documents up to the specified limit with early termination
// OPTIMIZATION: Stops loading pages once limit is reached for simple LIMIT-only queries
func (ba *BundleAdapter) GetAllDocumentsWithLimit(limit int) map[string]*models.Document {
	if limit <= 0 {
		// No limit or invalid limit - delegate to regular GetAllDocuments()
		return ba.GetAllDocuments()
	}

	ba.logger.Debugf("GetAllDocumentsWithLimit called for bundle '%s' with limit=%d", ba.bundle.Name, limit)

	// EARLY RETURN OPTIMIZATION: Check DocumentsComplete at the start
	ba.bundle.DocumentsMutex.RLock()
	if ba.bundle.Documents != nil && ba.bundle.DocumentsComplete {
		ba.bundle.DocumentsMutex.RUnlock()
		ba.logger.Debugf("Using complete Documents cache with %d documents (limit=%d)", len(*ba.bundle.Documents), limit)
		allDocs := make(map[string]*models.Document, limit)
		count := 0
		for docID, doc := range *ba.bundle.Documents {
			if count >= limit {
				break // Early termination
			}
			// Copy document to avoid loop variable aliasing
			docCopy := new(models.Document)
			*docCopy = doc
			allDocs[docID] = docCopy
			count++
		}
		return allDocs
	}
	ba.bundle.DocumentsMutex.RUnlock()

	allDocs := make(map[string]*models.Document, limit)

	// Get safe page count
	pageCount := ba.getSafePageCount()

	// Load pages until we reach the limit
	if pageCount > 0 {
		ba.logger.Debugf("Loading documents from disk with early termination (PageCount=%d, limit=%d)", pageCount, limit)

		// Stream through pages and collect documents until limit is reached
		for pageID := uint32(0); pageID < pageCount; pageID++ {
			// Early termination: stop if we've reached the limit
			if len(allDocs) >= limit {
				ba.logger.Debugf("Early termination: Reached limit of %d documents at page %d", limit, pageID)
				break
			}

			page, err := ba.loadDocumentPage(pageID)
			if err != nil {
				ba.logger.Errorf("Failed to load page %d: %v", pageID, err)
				continue
			}

			for docID, doc := range page.Documents {
				if len(allDocs) >= limit {
					// Reached limit during this page - stop early
					break
				}
				// Copy document to avoid loop variable aliasing
				docCopy := new(models.Document)
				*docCopy = doc
				allDocs[docID] = docCopy
			}
		}
		ba.logger.Debugf("Loaded %d documents from disk (limit was %d)", len(allDocs), limit)
	}

	// Merge with memtable (recent writes not yet flushed to new pages)
	// Only merge if Documents exists AND is marked as incomplete (memtable mode)
	ba.bundle.DocumentsMutex.RLock()
	if ba.bundle.Documents != nil && !ba.bundle.DocumentsComplete {
		ba.logger.Debugf("Merging documents from memtable with limit=%d (disk has %d)", limit, len(allDocs))

		// Add memtable documents that aren't already in disk results, up to limit
		mergedCount := 0
		for docID, doc := range *ba.bundle.Documents {
			if len(allDocs) >= limit {
				break // Early termination: reached limit
			}
			if _, exists := allDocs[docID]; !exists {
				// Copy document to avoid loop variable aliasing
				docCopy := new(models.Document)
				*docCopy = doc
				allDocs[docID] = docCopy
				mergedCount++
			}
		}
		ba.logger.Debugf("Merged %d new documents from memtable (total now: %d, limit: %d)", mergedCount, len(allDocs), limit)
	} else {
		ba.logger.Debugf("No memtable to merge (Documents=nil or DocumentsComplete=true)")
	}
	ba.bundle.DocumentsMutex.RUnlock()

	ba.logger.Debugf("Returning %d documents (limit was %d)", len(allDocs), limit)
	return allDocs
}

// GetName returns the bundle name for logging and metrics
func (ba *BundleAdapter) GetName() string {
	return ba.bundle.Name
}

// GetTotalDocuments returns the total number of documents efficiently
func (ba *BundleAdapter) GetTotalDocuments() int {
	return ba.getTotalDocumentsCount()
}

// GetHashIndexForField retrieves the hash index for a specific field
// Returns nil if no index exists for the field
// This is used by join executors for index-assisted operations
// Supports both foreign key indexes and regular hash indexes
func (ba *BundleAdapter) GetHashIndexForField(fieldName string) interface{} {
	if ba.bundle == nil || ba.bundle.Indexes == nil {
		return nil
	}

	// Iterate through all indexes to find one that has this field
	for _, indexRef := range ba.bundle.Indexes {
		// Only consider hash indexes (skip btree, etc.)
		if indexRef.IndexType != "hash" {
			continue
		}

		// Check if any field in this index matches the requested field name
		for _, field := range indexRef.Fields {
			if field.Name == fieldName {
				// Return the actual index instance
				return indexRef.IndexInstance
			}
		}
	}

	return nil
}

// HasIndexOnField checks if an index exists for the specified field
// This is a quick check without loading the index
// Supports both foreign key indexes and regular hash indexes
func (ba *BundleAdapter) HasIndexOnField(fieldName string) bool {
	if ba.bundle == nil || ba.bundle.Indexes == nil {
		return false
	}

	// Iterate through all indexes to find one that has this field
	for _, indexRef := range ba.bundle.Indexes {
		// Only consider hash indexes (skip btree, etc.)
		if indexRef.IndexType != "hash" {
			continue
		}

		// Check if any field in this index matches the requested field name
		for _, field := range indexRef.Fields {
			if field.Name == fieldName {
				return true
			}
		}
	}

	return false
}

// validatePageCount performs smart validation of PageCount to prevent infinite loops
// while allowing legitimate large datasets. Returns validated pageCount.
// DEPRECATED: Use getSafePageCount() instead for consistency
func (ba *BundleAdapter) validatePageCount(caller string) uint32 {
	const pageSize = uint32(4096)                // Standard page size
	maxReasonablePages := MaxReasonablePageCount // Use global constant

	pageCount := uint32(ba.bundle.PageCount)

	// Calculate expected page count based on TotalDocuments
	var expectedPages uint32
	if ba.bundle.TotalDocuments > 0 {
		expectedPages = uint32((ba.bundle.TotalDocuments + int64(pageSize) - 1) / int64(pageSize))
	}

	// Detect obvious corruption: PageCount wildly exceeds what TotalDocuments suggests
	if expectedPages > 0 && pageCount > expectedPages*10 {
		ba.logger.Errorf("CORRUPTION DETECTED in %s: PageCount (%d) is 10x higher than expected (%d) based on TotalDocuments (%d). Using expected value.",
			caller, pageCount, expectedPages, ba.bundle.TotalDocuments)
		return expectedPages
	}

	// Hard limit: Prevent truly dangerous values (>10M documents)
	if pageCount > maxReasonablePages {
		ba.logger.Errorf("SAFETY LIMIT in %s: PageCount (%d) exceeds maximum safe limit (%d). Capping to limit.",
			caller, pageCount, maxReasonablePages)
		return maxReasonablePages
	}

	// Value seems reasonable - use it
	return pageCount
}

// ===== STREAMING ITERATOR FOR SCANNER =====

// CreateDocumentIterator creates a streaming iterator for the scanner
// This allows the scanner to process documents without loading everything into memory
func (ba *BundleAdapter) CreateDocumentIterator() DocumentIterator {
	return &BundleDocumentIterator{
		adapter:     ba,
		currentPage: 0,
		pageIDs:     []string{},
		pageIndex:   0,
	}
}

// DocumentIterator interface for streaming document access
type DocumentIterator interface {
	HasNext() bool
	Next() (*models.Document, error)
	Reset()
}

// BundleDocumentIterator implements streaming document iteration
type BundleDocumentIterator struct {
	adapter     *BundleAdapter
	currentPage uint32
	pageIDs     []string
	pageIndex   int
}

func (iter *BundleDocumentIterator) HasNext() bool {
	// Use the safe page count from adapter
	pageCount := iter.adapter.getSafePageCount()

	// Check if we have more pages to process
	return iter.currentPage < pageCount
}

func (iter *BundleDocumentIterator) Next() (*models.Document, error) {
	// PROTECTION: Prevent infinite recursion with maximum attempts
	maxAttempts := 1000
	attempts := 0

	for attempts < maxAttempts {
		if !iter.HasNext() {
			return nil, fmt.Errorf("no more documents")
		}

		// Load current page if not loaded or if we've exhausted current page
		if iter.pageIndex >= len(iter.pageIDs) {
			page, err := iter.adapter.loadDocumentPage(iter.currentPage)
			if err != nil {
				iter.currentPage++
				attempts++
				continue // Try next page instead of recursion
			}

			// Reset page iteration
			iter.pageIDs = make([]string, 0, len(page.Documents))
			for docID := range page.Documents {
				iter.pageIDs = append(iter.pageIDs, docID)
			}
			iter.pageIndex = 0

			// If page is empty, move to next page
			if len(iter.pageIDs) == 0 {
				iter.currentPage++
				attempts++
				continue // Try next page instead of recursion
			}
		}

		// If we reach here, we should have a valid page with documents
		break
	}

	if attempts >= maxAttempts {
		return nil, fmt.Errorf("exceeded maximum attempts (%d) to find valid page", maxAttempts)
	}

	// Get document from current page
	docID := iter.pageIDs[iter.pageIndex]
	page := iter.adapter.cachedPages[iter.currentPage]
	doc := page.Documents[docID]

	iter.pageIndex++

	// If we've exhausted this page, move to next page
	if iter.pageIndex >= len(iter.pageIDs) {
		iter.currentPage++
	}

	// Return copy of document
	docCopy := doc
	return &docCopy, nil
}

func (iter *BundleDocumentIterator) Reset() {
	iter.currentPage = 0
	iter.pageIDs = []string{}
	iter.pageIndex = 0
}
