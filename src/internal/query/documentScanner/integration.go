package documentscanner

import (
	"fmt"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// VisibilityMapInterface provides lock-free page visibility checking for scan optimization.
// When a page is all-visible (all docs committed, not deleted, not superseded),
// scanners can skip per-document IsVisibleToSnapshot() calls entirely.
type VisibilityMapInterface interface {
	IsAllVisible(pageID uint32) bool
	Generation() uint64
}

// BundleServiceInterface for document loading with streaming support
type BundleServiceInterface interface {
	GetAllDocumentsForIndexing(bundleName string) ([]*models.Document, error)
	LoadDocumentPage(bundleName, databaseName string, pageID uint32, databasePath string) (*models.DocumentPage, error)
	GetDocumentPage(bundleName, databaseName string, pageID uint32) (*models.DocumentPage, error)         // GetDocumentPage uses shared documentPages cache
	GetDocumentPageReadOnly(bundleName, databaseName string, pageID uint32) (*models.DocumentPage, error) // Zero-allocation immutable snapshot (caller must not mutate)
	CountDocuments(bundleName, databaseName string) (int, error)                                  // Count all documents using optimized count-only parser
	// SnapshotPageDocuments safely snapshots documents from a page to avoid concurrent map iteration
	SnapshotPageDocuments(bundleName, databaseName string, pageID uint32) ([]models.Document, error)
	// SnapshotPageDocumentsReadOnly returns a read-only view of COW snapshot without copying.
	// Caller MUST NOT mutate the returned slice or any Document in it.
	SnapshotPageDocumentsReadOnly(bundleName, databaseName string, pageID uint32) ([]models.Document, error)
	// CopyProjectedFromCache copies projected documents from documentPages cache under one RLock
	// OPTIMIZATION: One-time lock acquisition, iterates cached pages, copies only projected fields
	// Returns: (projected docs map, docs copied, pages that were cached, total pages, error)
	// effectiveLimit: 0 = no limit, >0 = stop after that many docs
	CopyProjectedFromCache(bundleName, databaseName string, pageCount uint32, projectFields []string, effectiveLimit int, schema *models.BundleFieldSchema) (map[string]*ProjectedDocument, int, int, int, error)
}

// ScannerIntegration provides example integration between the document scanner and SyndrDB
// This demonstrates how to use the scanner module with existing SyndrDB components
type ScannerIntegration struct {
	factory        *ScannerFactory
	metricsManager *MetricsManager
	logger         *zap.SugaredLogger
}

// NewScannerIntegration creates a new scanner integration instance
// This sets up the factory and metrics manager with sensible defaults
func NewScannerIntegration(logger *zap.SugaredLogger) *ScannerIntegration {
	factory := NewScannerFactory(logger, DefaultScannerConfig())
	metricsManager := NewMetricsManager(logger, DefaultScannerConfig().MetricsInterval)

	return &ScannerIntegration{
		factory:        factory,
		metricsManager: metricsManager,
		logger:         logger,
	}
}

// CreateScannerForBundle creates and registers a scanner for a specific bundle
// This is the main entry point for integrating scanning with query execution
func (si *ScannerIntegration) CreateScannerForBundle(bundle *models.Bundle, bundleService BundleServiceInterface, logger *zap.SugaredLogger) (DocumentScannerInterface, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	// Create bundle adapter with BundleService
	bundleAdapter := NewBundleAdapter(bundle, bundleService, logger)

	// Create scanner with factory
	scanner, err := si.factory.CreateScanner(bundleAdapter, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create scanner: %w", err)
	}

	// SNAPSHOT ISOLATION: Set scanner reference in adapter for MVCC filtering
	// bundleAdapter is already *BundleAdapter, so we can directly assign
	bundleAdapter.scanner = scanner

	// Register with metrics manager
	si.metricsManager.RegisterScanner(bundle.Name, scanner)

	si.logger.Debugf("Created and registered scanner for bundle '%s'", bundle.Name)

	return scanner, nil
}

// GetMetricsManager returns the metrics manager for accessing aggregated metrics
func (si *ScannerIntegration) GetMetricsManager() *MetricsManager {
	return si.metricsManager
}

// GetFactory returns the scanner factory for creating additional scanners
func (si *ScannerIntegration) GetFactory() *ScannerFactory {
	return si.factory
}

// Close cleans up resources
func (si *ScannerIntegration) Close() error {
	si.metricsManager.Stop()
	si.logger.Info("Scanner integration closed")
	return nil
}

// Example usage function to demonstrate integration
func ExampleUsage(logger *zap.SugaredLogger, bundle *models.Bundle, bundleService BundleServiceInterface) {
	// Create integration
	integration := NewScannerIntegration(logger)
	defer integration.Close()

	// Create scanner for bundle
	scanner, err := integration.CreateScannerForBundle(bundle, bundleService, logger)
	if err != nil {
		logger.Errorf("Failed to create scanner: %v", err)
		return
	}

	// Perform a sample scan
	query := &ScanQuery{
		KeyName:       "status",
		Value:         "active",
		Operator:      "=",
		CaseSensitive: false,
	}

	result, err := scanner.ScanForKeyValue(query)
	if err != nil {
		logger.Errorf("Scan failed: %v", err)
		return
	}

	logger.Debugf("Scan completed: found %d documents in %v",
		len(result.Documents), result.ScanLatency)

	// Check metrics
	metrics := scanner.GetMetrics()
	logger.Debugf("Scanner metrics: %d total scans, %.2f%% cache hit rate",
		metrics.TotalScans, metrics.CacheHitRate*100)

	// Get optimization recommendations
	recommendations := integration.GetMetricsManager().GetOptimizationRecommendations()
	for _, rec := range recommendations {
		logger.Debugf("Optimization recommendation: %s for %s (Priority: %s)",
			rec.Type, rec.BundleName, rec.Priority)
	}
}

// Suggested Bundle Model Extensions
// These methods could be added to the Bundle model to support scanner integration
// Each method is commented to indicate it would be a new addition

/*
// GetDocumentIDs returns all document IDs in the bundle
// SCANNER INTEGRATION: New method to support document scanner interface
func (b *Bundle) GetDocumentIDs() []string {
	if b.Documents == nil {
		return []string{}
	}

	documentIDs := make([]string, 0, len(*b.Documents))
	for docID := range *b.Documents {
		documentIDs = append(documentIDs, docID)
	}

	return documentIDs
}

// GetDocument retrieves a document by its ID
// SCANNER INTEGRATION: New method to support document scanner interface
func (b *Bundle) GetDocument(docID string) *Document {
	if b.Documents == nil {
		return nil
	}

	if doc, exists := (*b.Documents)[docID]; exists {
		return &doc
	}
	return nil
}

// GetAllDocuments returns all documents in the bundle as a map
// SCANNER INTEGRATION: New method to support document scanner interface
func (b *Bundle) GetAllDocuments() map[string]*Document {
	if b.Documents == nil {
		return make(map[string]*Document)
	}

	// Return a copy to prevent external modification
	docs := make(map[string]*Document)
	for docID, doc := range *b.Documents {
		docCopy := doc // Create a copy
		docs[docID] = &docCopy
	}

	return docs
}

// GetName returns the bundle name for logging and metrics
// SCANNER INTEGRATION: This method already exists (b.Name field)
func (b *Bundle) GetName() string {
	return b.Name
}

// GetTotalDocuments returns the total number of documents in the bundle
// SCANNER INTEGRATION: Enhanced to use new TotalDocuments field when available
func (b *Bundle) GetTotalDocuments() int {
	// Use the new TotalDocuments field if available (for paginated documents)
	if b.TotalDocuments > 0 {
		return int(b.TotalDocuments)
	}

	// Fall back to counting documents in memory (legacy support)
	if b.Documents == nil {
		return 0
	}

	return len(*b.Documents)
}

// CreateScanner creates a document scanner for this bundle
// SCANNER INTEGRATION: New convenience method for easy scanner creation
func (b *Bundle) CreateScanner(logger *zap.SugaredLogger, config *ScannerConfig) (DocumentScannerInterface, error) {
	factory := NewScannerFactory(logger, config)
	adapter := NewBundleAdapter(b)
	return factory.CreateScanner(adapter, config)
}
*/
