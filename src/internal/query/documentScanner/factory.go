package documentscanner

import (
	"fmt"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
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

	sf.logger.Infof("Created scanner for bundle '%s' with %d documents",
		bundle.GetName(), bundle.GetTotalDocuments())

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
}

// NewBundleAdapter creates a new adapter for a SyndrDB Bundle with streaming support
// This adapter provides efficient lazy loading without loading all documents at once
func NewBundleAdapter(bundle *models.Bundle, bundleService BundleServiceInterface) *BundleAdapter {
	return &BundleAdapter{
		bundle:        bundle,
		bundleService: bundleService,
		cachedPages:   make(map[uint32]*models.DocumentPage),
	}
}

// ===== STREAMING IMPLEMENTATION - NO MORE INFINITE LOOPS =====

// loadDocumentPage loads a specific page with caching
func (ba *BundleAdapter) loadDocumentPage(pageID uint32) (*models.DocumentPage, error) {
	// Check cache first
	if page, exists := ba.cachedPages[pageID]; exists {
		return page, nil
	}

	if ba.bundleService == nil {
		return nil, fmt.Errorf("bundle service not available")
	}

	// Get database path from bundle
	databasePath := fmt.Sprintf("data_files/%s", ba.bundle.Database.Name)

	// Load page from storage
	page, err := ba.bundleService.LoadDocumentPage(ba.bundle.Name, ba.bundle.Database.Name, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load page %d: %w", pageID, err)
	}

	// Cache the page for future access
	ba.cachedPages[pageID] = page

	return page, nil
}

// getTotalDocumentsCount gets cached total or calculates it efficiently
func (ba *BundleAdapter) getTotalDocumentsCount() int {
	if ba.totalDocuments != nil {
		return *ba.totalDocuments
	}

	// Use bundle metadata if available
	if ba.bundle.TotalDocuments > 0 {
		count := int(ba.bundle.TotalDocuments)
		ba.totalDocuments = &count
		return count
	}

	// Fallback: count by iterating pages (but don't load all documents)
	count := 0
	for pageID := uint32(0); pageID < uint32(ba.bundle.PageCount); pageID++ {
		page, err := ba.loadDocumentPage(pageID)
		if err != nil {
			continue
		}
		count += len(page.Documents)
	}

	ba.totalDocuments = &count
	return count
}

// GetDocumentIDs returns all document IDs lazily without loading full documents
func (ba *BundleAdapter) GetDocumentIDs() []string {
	// Return cached IDs if available
	if ba.documentIDs != nil {
		return ba.documentIDs
	}

	var ids []string

	// Stream through pages and collect only document IDs (not full documents)
	for pageID := uint32(0); pageID < uint32(ba.bundle.PageCount); pageID++ {
		page, err := ba.loadDocumentPage(pageID)
		if err != nil {
			continue
		}

		// Extract only document IDs, not full documents
		for docID := range page.Documents {
			ids = append(ids, docID)
		}
	}

	// Cache the IDs for future calls
	ba.documentIDs = ids
	return ids
}

// GetDocument returns a single document by ID using streaming approach
func (ba *BundleAdapter) GetDocument(docID string) *models.Document {
	// Stream through pages to find the specific document
	for pageID := uint32(0); pageID < uint32(ba.bundle.PageCount); pageID++ {
		page, err := ba.loadDocumentPage(pageID)
		if err != nil {
			continue
		}

		if doc, exists := page.Documents[docID]; exists {
			// Return a copy to prevent external modification
			docCopy := doc
			return &docCopy
		}
	}

	return nil // Document not found
}

// GetAllDocuments returns all documents - WARNING: Use sparingly for large bundles!
// This method is kept for compatibility but should be avoided for large datasets
func (ba *BundleAdapter) GetAllDocuments() map[string]*models.Document {
	allDocs := make(map[string]*models.Document)

	// Stream through pages and collect all documents
	for pageID := uint32(0); pageID < uint32(ba.bundle.PageCount); pageID++ {
		page, err := ba.loadDocumentPage(pageID)
		if err != nil {
			continue
		}

		// Copy all documents from this page
		for docID, doc := range page.Documents {
			docCopy := doc
			allDocs[docID] = &docCopy
		}
	}

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
	// Check if we have more pages to process
	return iter.currentPage < uint32(iter.adapter.bundle.PageCount)
}

func (iter *BundleDocumentIterator) Next() (*models.Document, error) {
	if !iter.HasNext() {
		return nil, fmt.Errorf("no more documents")
	}

	// Load current page if not loaded or if we've exhausted current page
	if iter.pageIndex >= len(iter.pageIDs) {
		page, err := iter.adapter.loadDocumentPage(iter.currentPage)
		if err != nil {
			iter.currentPage++
			return iter.Next() // Try next page
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
			return iter.Next()
		}
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
