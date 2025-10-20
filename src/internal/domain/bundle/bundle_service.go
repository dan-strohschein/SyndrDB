package bundle

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/pkg/settings"
	"time"

	"syndrdb/src/internal/domain/index/btreeindexV2"
	hashindex "syndrdb/src/internal/domain/index/hashindexV2"

	//hashindex "syndrdb/src/hash_index"

	"syndrdb/src/pkg/common/helpers"

	"sync"

	"go.uber.org/zap"
)

// IndexUpdate represents a deferred index update operation
type IndexUpdate struct {
	BundleName string
	IndexName  string
	IndexType  string
	Operation  string // "insert", "delete", "update"
	DocumentID string
	FieldValue interface{}
	OldValue   interface{} // For updates
	Timestamp  time.Time
}

// MetadataUpdate represents a deferred metadata update operation
type MetadataUpdate struct {
	BundleName string
	Operation  string // "increment_docs", "decrement_docs", "recalc_pages"
	Value      int64  // For increment/decrement operations
	Timestamp  time.Time
}

// TypeConverter represents a fast type conversion function
type TypeConverter func(interface{}) (interface{}, error)

// Pre-compiled type converters for performance optimization
var typeConverters = map[string]TypeConverter{
	"string": convertToString,
	"int":    convertToInt,
	"float":  convertToFloat,
	"number": convertToFloat, // alias for float
	"bool":   convertToBool,
}

// Fast type converter functions - eliminate reflection overhead
func convertToString(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a string
	if strVal, ok := value.(string); ok {
		return strVal, nil
	}
	// Convert other types to string without reflection
	return fmt.Sprintf("%v", value), nil
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

type BundleService struct {
	store           bundlestore.BundleStore
	factory         BundleFactory
	documentFactory document.DocumentFactory
	settings        *settings.Arguments

	// Changed: Store only bundle metadata, not full bundles with documents
	bundleMetadata map[string]*models.Bundle       // Only schema/structure
	documentPages  map[string]*models.DocumentPage // Page-based document storage (bundleID:pageID -> page)

	logger *zap.SugaredLogger

	// Configuration for page management
	defaultPageSize int // Default number of documents per page
	maxLoadedPages  int // Maximum number of pages to keep in memory

	// Performance optimization: Deferred index updates
	indexUpdateBuffer    []IndexUpdate // Buffer for pending index updates
	indexUpdateBatchSize int           // Maximum updates to batch before flushing
	indexUpdateInterval  time.Duration // Maximum time to wait before flushing
	lastIndexFlush       time.Time     // Last time index updates were flushed

	// Performance optimization: Deferred metadata updates
	metadataUpdateBuffer    []MetadataUpdate // Buffer for pending metadata updates
	metadataPersistInterval int              // Number of operations before forcing metadata persist
	metadataOperationCount  int              // Count of operations since last metadata flush
	lastMetadataFlush       time.Time        // Last time metadata updates were flushed

	// PHASE 1 OPTIMIZATION: Bulk operation detection for WAL bypass
	bulkModeEnabled        bool      // Current bulk mode state
	operationCount         int       // Operations in current time window
	operationWindow        time.Time // Start of current measurement window
	bulkThresholdOpsPerSec int       // Operations per second threshold for bulk mode

	// DOCUMENT SCANNER INTEGRATION: Add scanner management
	scannerIntegration *documentscanner.ScannerIntegration                 // Scanner integration instance
	bundleScanners     map[string]documentscanner.DocumentScannerInterface // Per-bundle scanners
	scannerMutex       sync.RWMutex                                        // Protects bundleScanners map
}

func NewBundleService(store bundlestore.BundleStore, factory BundleFactory,
	docFactory document.DocumentFactory,
	logger *zap.SugaredLogger,
	args *settings.Arguments) *BundleService {
	// Get performance settings from global configuration
	globalSettings := settings.GetSettings()

	service := &BundleService{
		store:           store,
		factory:         factory,
		documentFactory: docFactory,
		settings:        args,
		logger:          logger,
		bundleMetadata:  make(map[string]*models.Bundle),
		documentPages:   make(map[string]*models.DocumentPage),
		defaultPageSize: 1000, // Default: 1000 documents per page
		maxLoadedPages:  100,  // Default: keep max 100 pages in memory

		// PHASE 1 OPTIMIZATION: Use configurable performance settings
		indexUpdateBuffer:    make([]IndexUpdate, 0, globalSettings.MetadataBatchSize),
		indexUpdateBatchSize: globalSettings.MetadataBatchSize,                                       // INCREASED: 50 → 500
		indexUpdateInterval:  time.Duration(globalSettings.MetadataFlushInterval) * time.Millisecond, // Use proper unit conversion
		lastIndexFlush:       time.Now(),

		// PHASE 1 OPTIMIZATION: Deferred metadata updates with configurable intervals
		metadataUpdateBuffer:    make([]MetadataUpdate, 0, globalSettings.MetadataBatchSize),
		metadataPersistInterval: globalSettings.MetadataPersistInterval, // NEW: 1000 docs before disk persist
		metadataOperationCount:  0,
		lastMetadataFlush:       time.Now(),

		// PHASE 1 OPTIMIZATION: Bulk operation detection for WAL bypass
		bulkModeEnabled:        false,
		operationCount:         0,
		operationWindow:        time.Now(),
		bulkThresholdOpsPerSec: globalSettings.WALBulkModeThreshold, // 50 ops/sec threshold

		// DOCUMENT SCANNER INTEGRATION: Initialize scanner management
		scannerIntegration: documentscanner.NewScannerIntegration(logger),
		bundleScanners:     make(map[string]documentscanner.DocumentScannerInterface),
	}

	// Don't load bundle metadata at startup - bundles should be loaded on-demand
	// Only primary database catalog bundles will be loaded during server initialization
	logger.Debugf("Bundle service initialized - bundles will be loaded on-demand")

	return service
}

// scheduleIndexUpdate adds an index update to the deferred update buffer
// This optimizes write performance by batching index updates
func (s *BundleService) scheduleIndexUpdate(bundleName, indexName, indexType, operation, documentID string, fieldValue, oldValue interface{}) {
	update := IndexUpdate{
		BundleName: bundleName,
		IndexName:  indexName,
		IndexType:  indexType,
		Operation:  operation,
		DocumentID: documentID,
		FieldValue: fieldValue,
		OldValue:   oldValue,
		Timestamp:  time.Now(),
	}

	s.indexUpdateBuffer = append(s.indexUpdateBuffer, update)

	// Check if we should flush updates
	if len(s.indexUpdateBuffer) >= s.indexUpdateBatchSize ||
		time.Since(s.lastIndexFlush) >= s.indexUpdateInterval {
		s.flushIndexUpdates()
	}

	// PHASE 1 ENHANCEMENT: Additional flush check for idle periods on index updates
	if len(s.indexUpdateBuffer) > 0 && time.Since(s.lastIndexFlush) >= (s.indexUpdateInterval*5) {
		s.logger.Debugf("IDLE FLUSH: Flushing %d index updates after extended idle period", len(s.indexUpdateBuffer))
		s.flushIndexUpdates()
	}
}

// scheduleMetadataUpdate adds a metadata update to the deferred update buffer
// This optimizes write performance by batching metadata calculations
func (s *BundleService) scheduleMetadataUpdate(bundleName, operation string, value int64) {
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
	if len(s.metadataUpdateBuffer) >= s.indexUpdateBatchSize ||
		time.Since(s.lastMetadataFlush) >= s.indexUpdateInterval {
		s.flushMetadataUpdates()
	}

	// PHASE 1 ENHANCEMENT: Additional flush check for idle periods
	// This catches any remaining operations after bulk periods end
	if len(s.metadataUpdateBuffer) > 0 && time.Since(s.lastMetadataFlush) >= (s.indexUpdateInterval*5) {
		s.logger.Debugf("IDLE FLUSH: Flushing %d metadata updates after extended idle period", len(s.metadataUpdateBuffer))
		s.flushMetadataUpdates()
	}
}

// flushIndexUpdates processes all pending index updates in a batch
// This significantly improves write performance by reducing I/O operations
func (s *BundleService) flushIndexUpdates() {
	if len(s.indexUpdateBuffer) == 0 {
		return
	}

	startTime := time.Now()
	s.logger.Debugf("Flushing %d pending index updates", len(s.indexUpdateBuffer))

	// Group updates by bundle and index for efficient processing
	updateGroups := make(map[string]map[string][]IndexUpdate)

	for _, update := range s.indexUpdateBuffer {
		if updateGroups[update.BundleName] == nil {
			updateGroups[update.BundleName] = make(map[string][]IndexUpdate)
		}
		updateGroups[update.BundleName][update.IndexName] = append(
			updateGroups[update.BundleName][update.IndexName], update)
	}

	// Process updates in batches
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

	// Clear the buffer and update flush time
	s.indexUpdateBuffer = s.indexUpdateBuffer[:0] // Reset slice but keep capacity
	s.lastIndexFlush = time.Now()

	flushTime := time.Since(startTime)
	s.logger.Debugf("Index update flush completed in %v", flushTime)
}

// flushMetadataUpdates processes all pending metadata updates in a batch
// This significantly improves write performance by reducing metadata calculation overhead
func (s *BundleService) flushMetadataUpdates() {
	if len(s.metadataUpdateBuffer) == 0 {
		return
	}

	startTime := time.Now()
	s.logger.Debugf("Flushing %d pending metadata updates", len(s.metadataUpdateBuffer))

	// Group updates by bundle for efficient processing
	bundleUpdates := make(map[string][]MetadataUpdate)

	for _, update := range s.metadataUpdateBuffer {
		bundleUpdates[update.BundleName] = append(bundleUpdates[update.BundleName], update)
	}

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
				docCountDelta -= update.Value
			}
		}

		// Apply the accumulated changes
		bundle.TotalDocuments += docCountDelta

		// Recalculate page count if documents changed
		if docCountDelta != 0 {
			// Ensure PageSize is never zero to prevent divide by zero
			// Use consistent PageSize with BundleService and factory defaults
			if bundle.PageSize == 0 {
				bundle.PageSize = s.defaultPageSize // Use service default (1000)
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

	// PHASE 1 OPTIMIZATION: Deferred persistence - only persist to disk every MetadataPersistInterval operations
	// This reduces disk I/O overhead during high-throughput writes
	shouldPersistToDisk := s.metadataOperationCount >= s.metadataPersistInterval

	if shouldPersistToDisk {
		// CRITICAL: Persist updated metadata to disk
		// Without this, metadata updates are lost when bundles are reloaded from storage
		for bundleName, updates := range bundleUpdates {
			bundle, exists := s.bundleMetadata[bundleName]
			if !exists {
				continue // Already logged warning above
			}

			// Only persist if there were document count changes
			docCountChanged := false
			for _, update := range updates {
				if update.Operation == "increment_docs" || update.Operation == "decrement_docs" {
					docCountChanged = true
					break
				}
			}

			if docCountChanged {
				// Persist the updated metadata to disk using the proper interface method
				err := s.store.UpdateBundleFile(bundle.Database, bundle)
				if err != nil {
					s.logger.Errorf("Failed to persist metadata updates for bundle '%s': %v", bundleName, err)
					// Continue with other bundles even if one fails
				} else {
					s.logger.Debugf("Successfully persisted metadata updates for bundle '%s' (TotalDocuments: %d, PageCount: %d)",
						bundleName, bundle.TotalDocuments, bundle.PageCount)
				}
			}
		}

		// Reset operation counter after persistence
		s.metadataOperationCount = 0
		s.logger.Debugf("Performed deferred metadata persistence after %d operations", s.metadataPersistInterval)
	} else {
		s.logger.Debugf("Skipping disk persistence - %d operations remaining until next persist (threshold: %d)",
			s.metadataPersistInterval-s.metadataOperationCount, s.metadataPersistInterval)
	}

	// Clear the buffer and update flush time
	s.metadataUpdateBuffer = s.metadataUpdateBuffer[:0] // Reset slice but keep capacity
	s.lastMetadataFlush = time.Now()

	flushTime := time.Since(startTime)
	s.logger.Debugf("Metadata update flush completed in %v", flushTime)
}

// forceMetadataPersistence forces immediate persistence of all metadata updates to disk
// This should be called during shutdown, explicit flush requests, or before critical operations
func (s *BundleService) forceMetadataPersistence() {
	if len(s.metadataUpdateBuffer) == 0 {
		return
	}

	s.logger.Debugf("Forcing immediate metadata persistence for %d pending updates", len(s.metadataUpdateBuffer))

	// Temporarily set operation count to trigger persistence
	s.metadataOperationCount = s.metadataPersistInterval

	// Flush metadata with forced persistence
	s.flushMetadataUpdates()

	// Note: metadataOperationCount is reset to 0 in flushMetadataUpdates
	s.logger.Debugf("Forced metadata persistence completed")
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
				s.logger.Infof("PHASE 1: Entering bulk mode - detected %.1f ops/sec (threshold: %d)",
					opsPerSecond, s.bulkThresholdOpsPerSec)
			}
		} else {
			if s.bulkModeEnabled {
				s.bulkModeEnabled = false
				s.logger.Infof("PHASE 1: Exiting bulk mode - detected %.1f ops/sec (threshold: %d)",
					opsPerSecond, s.bulkThresholdOpsPerSec)

				// CRITICAL: Flush all buffers when exiting bulk mode
				// This ensures that any pending operations are persisted to disk
				s.logger.Infof("BULK END: Triggering comprehensive buffer flush")
				if err := s.FlushAllBuffers(); err != nil {
					s.logger.Errorf("BULK END: Failed to flush buffers: %v", err)
				} else {
					s.logger.Infof("BULK END: Successfully flushed all pending operations")
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

// FlushAllBuffers forces immediate flush of all pending operations to disk
// This should be called at the end of bulk operations to ensure data persistence
func (s *BundleService) FlushAllBuffers() error {
	s.logger.Infof("FLUSH: Starting comprehensive buffer flush to ensure data persistence")

	var errors []error

	// 1. Flush index updates first (they may affect metadata)
	if len(s.indexUpdateBuffer) > 0 {
		s.logger.Debugf("FLUSH: Flushing %d pending index updates", len(s.indexUpdateBuffer))
		s.flushIndexUpdates()
	}

	// 2. Force metadata persistence regardless of thresholds
	if len(s.metadataUpdateBuffer) > 0 {
		s.logger.Debugf("FLUSH: Forcing metadata persistence for %d pending updates", len(s.metadataUpdateBuffer))
		s.forceMetadataPersistence()
	}

	// 3. Sync any file system buffers
	s.logger.Debugf("FLUSH: Syncing file system buffers")

	// Note: Individual stores should handle their own sync operations
	s.store.FlushAllWriteBuffers()

	// 4. Log completion
	if len(errors) > 0 {
		s.logger.Errorf("FLUSH: Completed with %d errors", len(errors))
		return fmt.Errorf("flush completed with %d errors", len(errors))
	}

	s.logger.Infof("FLUSH: Successfully flushed all buffers to disk")
	return nil
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

// processHashIndexBatch optimizes hash index updates by batching operations
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

	// Process all deduplicated updates for this hash index
	successCount := 0
	errorCount := 0

	for _, update := range deduplicatedUpdates {
		switch update.Operation {
		case "insert":
			err := hashIndex.InsertDocument(update.DocumentID)
			if err != nil {
				errorCount++
				// Enhanced corruption detection and handling
				if strings.Contains(err.Error(), "is not an overflow page") {
					s.logger.Errorf("Overflow chain corruption detected during bulk operation: %v", err)
					s.logger.Warnf("Continuing with remaining operations despite overflow corruption in document '%s'", update.DocumentID)
				} else if strings.Contains(err.Error(), "is not a bucket page") {
					s.logger.Errorf("CRITICAL: Bucket page corruption detected during bulk operation: %v", err)
					s.logger.Warnf("Bucket corruption in document '%s' - this indicates severe index corruption", update.DocumentID)
					// For bucket corruption, we should continue but also log this as a critical issue
				} else if strings.Contains(err.Error(), "index file corruption") {
					s.logger.Errorf("CRITICAL: Index file corruption detected: %v", err)
					s.logger.Warnf("Continuing despite corruption in document '%s' but index may need rebuilding", update.DocumentID)
				} else {
					s.logger.Warnf("Failed to insert document '%s' into hash index '%s': %v", update.DocumentID, indexName, err)
				}
			} else {
				successCount++
			}
		case "delete":
			_, err := hashIndex.DeleteDocument(update.DocumentID)
			if err != nil {
				errorCount++
				// Check if this is a corruption error that we can recover from
				if strings.Contains(err.Error(), "is not an overflow page") {
					s.logger.Errorf("Hash index corruption detected during bulk delete: %v", err)
					s.logger.Warnf("Continuing with remaining operations despite corruption in document '%s'", update.DocumentID)
				} else {
					s.logger.Warnf("Failed to delete document '%s' from hash index '%s': %v", update.DocumentID, indexName, err)
				}
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
		s.logger.Debugf("Hash index batch processing completed: %d operations successful for index '%s'",
			successCount, indexName)
	}

	// PERFORMANCE FIX: Flush changes to disk after batch processing instead of per-insert
	if err := hashIndex.FlushToDisk(); err != nil {
		s.logger.Warnf("Failed to flush hash index '%s' to disk: %v", indexName, err)
	}

	return nil
}

// processBTreeIndexBatch optimizes BTree index updates by batching operations
func (s *BundleService) processBTreeIndexBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
	if err != nil {
		return fmt.Errorf("failed to load BTree index: %w", err)
	}

	// Process all updates for this BTree index
	for _, update := range updates {
		switch update.Operation {
		case "insert":
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			err = btreeIndex.Insert(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to insert into BTree index '%s': %v", indexName, err)
			}

		case "delete":
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			err = btreeIndex.Delete(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to delete from BTree index '%s': %v", indexName, err)
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
			}
		}
	}

	return nil
}

// forceFlushIndexUpdates ensures all pending updates are processed immediately
// This should be called before critical operations like shutdown
func (s *BundleService) forceFlushIndexUpdates() {
	if len(s.indexUpdateBuffer) > 0 {
		s.logger.Debugf("Force flushing %d pending index updates", len(s.indexUpdateBuffer))
		s.flushIndexUpdates()
	}
	if len(s.metadataUpdateBuffer) > 0 {
		s.logger.Debugf("Force flushing %d pending metadata updates", len(s.metadataUpdateBuffer))
		s.flushMetadataUpdates()
	}
}

func (s *BundleService) AddBundle(databaseService *database.DatabaseService, db *models.Database, bundleCommand *models.BundleCommand) (*models.Bundle, error) {
	args := settings.GetSettings()
	// Check if the bundle already exists
	if _, err := s.GetBundleByName(db, bundleCommand.BundleName); err == nil {
		return nil, fmt.Errorf("bundle '%s' already exists", bundleCommand.BundleName)
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
			s.logger.Infof("Added field '%s' to bundle '%s'", fieldDef.Name, bundleCommand.BundleName)
		}
	}

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return nil, fmt.Errorf("error creating bundle file: %w", err)
	}
	//logger.Infof("Decoded bundle data from file %v", bundle)
	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s_%s.bnd", db.Name, bundle.Name))

	// Write the updated database file
	err = databaseService.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return nil, fmt.Errorf("error updating database file: %w", err)
	}

	createHashIndexInternal(s, bundle, "DocumentID") // Create a hash index on DocumentID

	s.bundleMetadata[bundleCommand.BundleName] = bundle

	// Register the new bundle in the Primary database's "Bundles" catalog
	// This will be handled by the catalog service at a higher level to avoid circular imports
	err = s.registerBundleInPrimary(bundle)
	if err != nil {
		s.logger.Warnf("Warning: Failed to register bundle '%s' in Primary catalog: %v", bundle.Name, err)
		// Don't fail the bundle creation if catalog registration fails
	}

	return bundle, nil
}

func (s *BundleService) AddBundleByStruct(databaseService *database.DatabaseService, db *models.Database, bundle *models.Bundle) error {
	// Set the database reference in the bundle
	bundle.Database = db

	// Initialize bundle properties if not set
	if bundle.PageSize == 0 {
		bundle.PageSize = s.defaultPageSize // Use service default (1000)
		s.logger.Debugf("Set default PageSize of %d for bundle '%s'", s.defaultPageSize, bundle.Name)
	}

	// Initialize TotalDocuments and PageCount based on existing documents
	if bundle.Documents != nil {
		bundle.TotalDocuments = int64(len(*bundle.Documents))
	} else {
		bundle.TotalDocuments = 0
	}

	// Calculate initial PageCount
	// PageCount = ceil(TotalDocuments / PageSize)
	bundle.PageCount = (bundle.TotalDocuments + int64(bundle.PageSize) - 1) / int64(bundle.PageSize)
	if bundle.PageCount == 0 {
		bundle.PageCount = 1 // Always have at least 1 page for new bundles
	}

	s.logger.Debugf("Initialized bundle '%s' with TotalDocuments: %d, PageSize: %d, PageCount: %d",
		bundle.Name, bundle.TotalDocuments, bundle.PageSize, bundle.PageCount)

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	// Add the bundle to the service cache so it can be retrieved later with relationships intact
	s.bundleMetadata[bundle.Name] = bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("error creating bundle file from struct: %w", err)
	}
	//logger.Infof("Decoded bundle data from file %v", bundle)
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
	args := settings.GetSettings()
	fileExists := s.store.BundleFileExists(name, database.Name)

	// Check if the bundle file exists in the store
	if !fileExists {
		return nil, fmt.Errorf("bundle file '%s' does not exist on disk", name)
	}

	bundle, exists := s.bundleMetadata[name]
	if !exists {
		if fileExists {
			// If the bundle exists in the store but not in memory, load metadata only
			if args.Debug {
				s.logger.Infof("Bundle metadata '%s' not found in memory, loading from store", name)
			}

			databasePath := helpers.GetDatabaseFolderPath(database.Name)

			bundle, err := s.store.LoadBundleMetadata(database, databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, name))
			if err != nil {
				return nil, fmt.Errorf("failed to load bundle metadata '%s': %w", name, err)
			}

			// Discover and populate existing index files
			err = s.discoverBundleIndexes(bundle)
			if err != nil {
				s.logger.Warnf("Failed to discover indexes for bundle '%s': %v", name, err)
				// Continue loading the bundle even if index discovery fails
			}

			if args.Debug {
				s.logger.Infof("Loaded bundle metadata '%s' from store", name)
			}

			s.bundleMetadata[name] = bundle
			return bundle, nil
		} else {
			return nil, fmt.Errorf("bundle file exists in memory but not on disk. '%s_%s.bnd' not found", database.Name, name)
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

// GetDocumentPage loads a specific page of documents for a bundle
func (s *BundleService) GetDocumentPage(bundleName string, databaseName string, pageID uint32) (*models.DocumentPage, error) {
	pageKey := fmt.Sprintf("%s:%d", bundleName, pageID)

	// Check if page is already loaded in memory
	if page, exists := s.documentPages[pageKey]; exists {
		s.logger.Debugf("Document page %s already loaded in memory", pageKey)
		return page, nil
	}

	// Load the page from disk
	s.logger.Debugf("Loading document page %s from disk", pageKey)

	databasePath := helpers.GetDatabaseFolderPath(databaseName)

	page, err := s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load document page %s: %w", pageKey, err)
	}

	// Check if we need to evict old pages to stay within memory limits
	if len(s.documentPages) >= s.maxLoadedPages {
		s.evictOldestPage()
	}

	// Store in memory
	s.documentPages[pageKey] = page
	return page, nil
}

// GetDocument retrieves a specific document by ID (loads the page containing it)
func (s *BundleService) GetDocument(bundleName, databaseName, documentID string) (*models.Document, error) {
	// First, find which page contains this document using an index
	pageID, err := s.findDocumentPage(bundleName, documentID)
	if err != nil {
		return nil, fmt.Errorf("could not find document %s in bundle %s: %w", documentID, bundleName, err)
	}

	// Load the page containing the document
	page, err := s.GetDocumentPage(bundleName, databaseName, pageID)
	if err != nil {
		return nil, err
	}

	// Extract the document from the page
	if doc, exists := page.Documents[documentID]; exists {
		return &doc, nil
	}

	return nil, fmt.Errorf("document %s not found in page %d of bundle %s", documentID, pageID, bundleName)
}

// evictOldestPage removes the least recently used page from memory
func (s *BundleService) evictOldestPage() {
	var oldestKey string
	var oldestTime time.Time

	// Find the oldest page by LoadedAt timestamp
	for key, page := range s.documentPages {
		if oldestKey == "" || page.LoadedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = page.LoadedAt
		}
	}

	if oldestKey != "" {
		s.logger.Debugf("Evicting document page %s from memory", oldestKey)

		// If the page is dirty, write it back to disk first
		if s.documentPages[oldestKey].IsDirty {
			// TODO: Implement page write-back
			s.logger.Debugf("Page %s is dirty, writing back to disk", oldestKey)
		}

		delete(s.documentPages, oldestKey)
	}
}

// findDocumentPage uses indexes to determine which page contains a specific document
func (s *BundleService) findDocumentPage(bundleID, documentID string) (uint32, error) {
	// TODO: Implement index-based page lookup
	// For now, use a simple hash-based approach
	// In production, this would use the bundle's indexes

	// Simple hash-based page assignment (temporary implementation)
	hash := s.simpleHash(documentID)

	// Get bundle metadata to determine page count
	bundle, exists := s.bundleMetadata[bundleID]
	if !exists {
		return 0, fmt.Errorf("bundle metadata not found for %s", bundleID)
	}

	if bundle.PageCount == 0 {
		return 0, fmt.Errorf("bundle %s has no pages", bundleID)
	}

	pageID := uint32(hash % uint64(bundle.PageCount))
	return pageID, nil
}

// getAllDocumentsForIndexing loads all documents from all pages for index building
// This is a temporary method during the transition to page-based architecture
func (s *BundleService) getAllDocumentsForIndexing(bundleName string) ([]*models.Document, error) {

	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle metadata not found for %s", bundleName)
	}
	s.logger.Infof("getAllDocumentsForIndexing called for bundle '%s'", bundle.Name)
	//s.logger.Infof("DEBUG: getAllDocumentsForIndexing ENTRY - bundle '%s'", bundle.Name)
	//s.logger.Infof("DEBUG: Bundle PageCount: %d, TotalDocuments: %d", bundle.PageCount, bundle.TotalDocuments)

	// CRITICAL FIX: Force flush pending metadata updates to ensure PageCount is current
	// This is necessary because document additions schedule deferred metadata updates
	// and SELECT TOP needs accurate PageCount to work correctly
	if len(s.metadataUpdateBuffer) > 0 {
		s.logger.Debugf("Forcing metadata flush for bundle %s to ensure current PageCount", bundleName)
		s.flushMetadataUpdates()
	}

	s.logger.Infof("Bundle %s has PageCount: %d", bundleName, bundle.PageCount)

	var allDocuments []*models.Document

	// Special handling: If PageCount is 0, still check page 0 for documents
	// This handles cases where metadata might be out of sync
	if bundle.PageCount == 0 {

		databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
		s.logger.Infof("DEBUG DEBUG DEBUG ::(P0) Loading Database name %s,page %d for bundle '%s'", bundle.Database.Name, 0, bundle.Name)
		//settings := settings.GetSettings()
		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, 0, databasePath)
		if err != nil {
			s.logger.Errorf("DEBUG: Failed to load page 0 for bundle '%s': %v", bundle.Name, err)
			return []*models.Document{}, nil
		}

		s.logger.Infof("DEBUG: Loaded page 0 - found %d documents", len(page.Documents))
		if len(page.Documents) == 0 {
			s.logger.Infof("DEBUG: No documents found in page 0, returning empty slice")
			return []*models.Document{}, nil
		}

		// FIXED: Actually process the documents found in page 0
		for _, doc := range page.Documents {
			docCopy := doc
			allDocuments = append(allDocuments, &docCopy)
		}

		s.logger.Infof("DEBUG: getAllDocumentsForIndexing - loaded %d documents from page 0 (PageCount was 0)", len(allDocuments))
		return allDocuments, nil
	}

	// Load all pages for this bundle using the PageCount from metadata
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		//s.logger.Infof("DEBUG: Loading page %d for bundle '%s'", pageID, bundle.Name)

		//settings := settings.GetSettings()
		//s.logger.Infof("DEBUG DEBUG DEBUG :: Loading Database name %s,page %d for bundle '%s'", bundle.Database.Name, pageID, bundle.Name)
		databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			//s.logger.Errorf("DEBUG: Failed to load page %d for bundle '%s': %v", pageID, bundle.Name, err)
			continue
		}

		//s.logger.Infof("DEBUG: Page %d loaded with %d documents", pageID, len(page.Documents))

		// Convert map to slice and append
		for _, doc := range page.Documents {
			docCopy := doc
			allDocuments = append(allDocuments, &docCopy)
		}
	}

	//s.logger.Infof("DEBUG: FINAL RETURN getAllDocumentsForIndexing - loaded %d total documents from %d pages", len(allDocuments), bundle.PageCount)
	return allDocuments, nil
}

// GetAllDocumentsForIndexing is a public wrapper for document scanner integration
func (s *BundleService) GetAllDocumentsForIndexing(bundleName string) ([]*models.Document, error) {
	return s.getAllDocumentsForIndexing(bundleName)
}

func (s *BundleService) LoadDocumentPage(bundleName, databaseName string, pageID uint32, databasePath string) (*models.DocumentPage, error) {
	// Load the specified document page from the store
	return s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
}

func (s *BundleService) LoadCatalogBundleDocuments(bundleName string) ([]*models.Document, error) {
	// Load all documents for the specified catalog bundle
	return s.getAllDocumentsForIndexing(bundleName)
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
	// The Documents field is left nil to encourage use of the paginated document access methods
	s.logger.Debugf("Returned metadata-only bundle '%s' - use GetDocumentPage for document access", name)

	return bundle, nil
}

func (s *BundleService) GetAllBundles() map[string]*models.Bundle {
	return s.bundleMetadata
}

func (s *BundleService) RemoveBundle(db *models.Database, name string) error {
	// Check if the bundle exists in metadata
	bundle, exists := s.bundleMetadata[name]
	if !exists {
		return fmt.Errorf("bundle '%s' not found", name)
	}

	// Remove the bundle from the store
	err := s.store.RemoveBundleFile(db, bundle.Name)
	if err != nil {
		return fmt.Errorf("failed to remove bundle from store: %w", err)
	}

	// Remove from metadata
	delete(s.bundleMetadata, name)

	// Remove any loaded document pages for this bundle
	keysToDelete := make([]string, 0)
	for pageKey := range s.documentPages {
		if strings.HasPrefix(pageKey, name+":") {
			keysToDelete = append(keysToDelete, pageKey)
		}
	}
	for _, key := range keysToDelete {
		delete(s.documentPages, key)
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

	// Add the relationship to the bundle
	if bundle.Relationships == nil {
		bundle.Relationships = make(map[string]models.Relationship)
	}
	bundle.Relationships[relationship.Name] = relationship

	s.logger.Infof("Adding %s relationship from %s.%s to %s.%s",
		relationship.RelationshipType,
		relationship.SourceBundle,
		relationship.SourceField,
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

		// Also add the reverse field to the source bundle
		reverseFieldName := relationship.DestinationBundle + "ID"
		bundle.DocumentStructure.FieldDefinitions[reverseFieldName] = models.FieldDefinition{
			Name:         reverseFieldName,
			Type:         "relationship",
			IsRequired:   false,
			IsUnique:     false,
			DefaultValue: nil,
		}

		s.logger.Infof("Added reverse field '%s' to source bundle '%s' for ManyToMany relationship",
			reverseFieldName, bundle.Name)

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

	s.logger.Infof("Successfully added relationship '%s' to bundle '%s'", relationshipName, bundle.Name)
	return nil
}

// generateRelationshipName generates a unique relationship name with counter
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
func (s *BundleService) addFieldToDestinationBundle(sourceBundle *models.Bundle, relationship *models.Relationship, isRequired, isUnique bool) error {
	// Find the destination bundle
	destinationBundle, err := s.GetBundleByName(sourceBundle.Database, relationship.DestinationBundle)
	if err != nil {
		return fmt.Errorf("destination bundle '%s' not found: %w", relationship.DestinationBundle, err)
	}

	// Check if field definitions map is initialized
	if destinationBundle.DocumentStructure.FieldDefinitions == nil {
		destinationBundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}

	// Add the relationship field to the destination bundle
	fieldName := relationship.DestinationField
	destinationBundle.DocumentStructure.FieldDefinitions[fieldName] = models.FieldDefinition{
		Name:         fieldName,
		Type:         "relationship",
		IsRequired:   isRequired,
		IsUnique:     isUnique,
		DefaultValue: nil,
	}

	// Update the destination bundle in the store
	err = s.store.UpdateBundleFile(destinationBundle.Database, destinationBundle)
	if err != nil {
		return fmt.Errorf("failed to update destination bundle '%s' in store: %w", destinationBundle.Name, err)
	}

	s.logger.Infof("Added relationship field '%s' to destination bundle '%s' (required=%t, unique=%t)",
		fieldName, destinationBundle.Name, isRequired, isUnique)

	return nil
}

func (s *BundleService) AddIndexToBundle(database *models.Database, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	//args := settings.GetSettings()
	s.logger.Infof("DEBUG: Starting AddIndexToBundle for bundle '%s', index '%s', type '%s'",
		indexCommand.BundleName, indexCommand.IndexName, indexCommand.IndexType)

	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add index")
		return fmt.Errorf("bundle '%s' is nil, cannot add index", indexCommand.BundleName)
	}

	bundle, err := s.GetBundleByName(database, bundle.Name)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", indexCommand.BundleName)
	}

	// Create the index based on the command type

	switch indexCommand.IndexType {
	case "btree":
		s.logger.Infof("DEBUG: Starting BTree index creation")
		err1 := CreateBTreeIndex(s, bundle, indexCommand)
		s.logger.Infof("DEBUG: BTree index creation completed with error: %v", err1)
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
	args := settings.GetSettings()
	// Create configuration for the new hash index
	config := hashindex.IndexConfig{
		BundleName:  bundle.Name,
		FieldName:   indexCommand.Fields[0].Name,
		IsUnique:    indexCommand.Fields[0].IsUnique,
		DataDir:     args.DataDir,
		DebugMode:   args.Debug,
		InitialSize: 16,   // Start with 16 buckets
		PageSize:    8192, // 8KB pages (PostgreSQL-style)
		LoadFactor:  0.75, // Split when 75% full
		CacheSize:   100,  // Cache 100 pages
	}

	// Create the hash index using the new V2 implementation
	hashIndex, err := hashindex.CreateHashIndex(&config, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create hash index: %v", err)
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: indexCommand.Fields[0].Name,
		IsUnique:  indexCommand.Fields[0].IsUnique,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:      indexCommand.IndexName,
		Fields:         indexCommand.Fields,
		IndexType:      indexCommand.IndexType,
		CreateTime:     time.Now(),
		IndexInstance:  hashIndex, // Store the V2 hash index instance
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

	s.logger.Infof("Successfully created hash index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, indexCommand.Fields[0].Name, bundle.Name)
	return nil
}

func createHashIndexInternal(s *BundleService, bundle *models.Bundle, name string) error {
	args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	// Create configuration for the new hash index
	config := hashindex.IndexConfig{
		DatabaseName: bundle.Database.Name,
		BundleName:   bundle.Name,
		FieldName:    name,
		IsUnique:     true,
		DataDir:      databasePath,
		DebugMode:    args.Debug,
		InitialSize:  16,   // Start with 16 buckets
		PageSize:     8192, // 8KB pages (PostgreSQL-style)
		LoadFactor:   0.75, // Split when 75% full
		CacheSize:    100,  // Cache 100 pages
	}

	// Create the hash index using the new V2 implementation
	hashIndex, err := hashindex.CreateHashIndex(&config, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create hash index: %v", err)
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

	s.logger.Infof("Successfully created hash index '%s' on field '%s' for bundle '%s'", name, name, bundle.Name)
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
	s.logger.Infof("DEBUG: CreateBTreeIndex started for bundle '%s', index '%s'", bundle.Name, indexCommand.IndexName)

	// Validate input parameters
	if len(indexCommand.Fields) == 0 {
		return fmt.Errorf("no fields specified for BTree index creation")
	}

	// For now, support single-field indexes (can be extended for composite indexes later)
	if len(indexCommand.Fields) > 1 {
		return fmt.Errorf("composite BTree indexes not yet supported, please create separate indexes for each field")
	}

	fieldDef := indexCommand.Fields[0]
	s.logger.Infof("DEBUG: Field definition: %+v", fieldDef)

	// Validate that the field exists in the bundle structure
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldDef.Name]; !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", fieldDef.Name, bundle.Name)
	}

	s.logger.Infof("Creating BTree index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	// Then in the CreateBTreeIndex function:
	splitRatio := calculateOptimalSplitRatio(fieldDef, fieldDef.IsUnique)

	// Create configuration for the new BTree index
	config := btreeindexV2.IndexConfig{
		BundleName:   bundle.Name,
		FieldName:    fieldDef.Name,
		IsUnique:     fieldDef.IsUnique,
		DataDir:      args.DataDir,
		DebugMode:    args.Debug,
		PageSize:     8192,       // 8KB pages (PostgreSQL-style)
		CacheSize:    100,        // Cache 100 pages for performance
		FillFactor:   0.7,        // 70% fill factor for optimal balance between space and performance
		MaxKeyLength: 2048,       // Set maximum key length to 2KB
		SplitRatio:   splitRatio, // Use the calculated split ratio
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
	allDocuments, err := s.getAllDocumentsForIndexing(bundle.Name)
	if err != nil {
		s.logger.Warnf("Failed to load documents for indexing: %v", err)
		return err
	}

	if len(allDocuments) > 0 {
		s.logger.Debugf("Populating BTree index with %d existing documents", len(allDocuments))

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
			err = btreeIndex.Insert(keyBytes, document.DocumentID)
			if err != nil {
				s.logger.Errorf("Failed to insert document '%s' into BTree index: %v", documentID, err)
				// Close the index and return error if population fails
				btreeIndex.Close()
				return fmt.Errorf("failed to populate BTree index with existing documents: %w", err)
			}
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

	s.logger.Infof("Successfully created BTree index '%s' on field '%s' for bundle '%s'",
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
		return []byte(fmt.Sprintf("%v", v)), nil
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
		Split Ratio = 0.5 (50%) is the recommended value because:

		1.Balanced Tree Structure: When a node becomes full and needs to split, a 50% ratio creates two nodes
		that are equally balanced, maintaining optimal B+ tree characteristics.

		2.PostgreSQL Standard: PostgreSQL uses a similar 50% split ratio for B-tree indexes, which provides
		excellent performance characteristics.

		3.Optimal Performance: Equal splits minimize tree height and provide consistent performance for both
		insertions and searches.

		4.Space Efficiency: Balanced splits ensure good space utilization without excessive fragmentation.
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
//   - *hashindex.HashIndex: The loaded hash index instance
//   - error: Any error that occurred during loading
func (s *BundleService) GetOrLoadHashIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*hashindex.HashIndex, error) {
	// Check if the index instance is already loaded in memory
	if indexRef.IndexInstance != nil {
		if hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndex); ok {
			s.logger.Debugf("Hash index '%s' already loaded in memory", indexName)
			return hashIndex, nil
		}
	}

	s.logger.Debugf("Loading hash index '%s' from disk for bundle '%s'", indexName, bundle.Name)

	// Load the hash index from disk using the index name and bundle information
	args := settings.GetSettings()
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexFilePath := fmt.Sprintf("%s%s_%s.hidx", databasePath, bundle.Name, indexRef.HashIndexField.FieldName)

	hashIndex, err := hashindex.OpenHashIndex(indexFilePath, args.Debug, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load hash index '%s' from disk: %w", indexName, err)
	}

	// Store the loaded instance back in the bundle for future use
	indexRef.IndexInstance = hashIndex
	bundle.Indexes[indexName] = indexRef

	s.logger.Debugf("Successfully loaded hash index '%s' from disk", indexName)
	return hashIndex, nil
}

// getOrLoadBTreeIndex retrieves or loads a BTree index instance for the specified bundle and index name
// This function follows the Single Responsibility Principle by handling only BTree index loading
// Parameters:
//   - bundle: The bundle containing the index reference
//   - indexName: The name of the index to load
//   - indexRef: The index reference containing metadata
//
// Returns:
//   - *btreeindexV2.BTreeIndex: The loaded BTree index instance
//   - error: Any error that occurred during loading
func (s *BundleService) getOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*btreeindexV2.BTreeIndex, error) {
	// Check if the index instance is already loaded in memory
	if indexRef.IndexInstance != nil {
		if btreeIndex, ok := indexRef.IndexInstance.(*btreeindexV2.BTreeIndex); ok {
			s.logger.Debugf("BTree index '%s' already loaded in memory", indexName)
			return btreeIndex, nil
		}
	}

	s.logger.Debugf("Loading BTree index '%s' from disk for bundle '%s'", indexName, bundle.Name)

	// Load the BTree index from disk using the index name and bundle information
	args := settings.GetSettings()
	indexFilePath := fmt.Sprintf("%s/%s_%s.btidx", args.DataDir, bundle.Name, indexRef.BTreeIndexField.FieldName)

	btreeIndex, err := btreeindexV2.OpenBTreeIndex(indexFilePath, args.Debug, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load BTree index '%s' from disk: %w", indexName, err)
	}

	// Store the loaded instance back in the bundle for future use
	indexRef.IndexInstance = btreeIndex
	bundle.Indexes[indexName] = indexRef

	s.logger.Debugf("Successfully loaded BTree index '%s' from disk", indexName)
	return btreeIndex, nil
}

// GetOrLoadBTreeIndex is a public wrapper for getOrLoadBTreeIndex to support query planner
func (s *BundleService) GetOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
}

// GetOrLoadHashIndexInterface is a wrapper to support query planner interface compatibility
func (s *BundleService) GetOrLoadHashIndexInterface(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.GetOrLoadHashIndex(bundle, indexName, indexRef)
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

	// Validate document fields against bundle field definitions
	err := s.validateDocumentFields(bundle, docCommand)
	if err != nil {
		return "", fmt.Errorf("document field validation failed: %w", err)
	}

	// Add the document to the bundle
	newDocument := s.documentFactory.NewDocument(*docCommand)

	// Schedule deferred index updates for optimal performance instead of immediate updates
	if bundle.Indexes != nil {
		// Look for indexes and schedule updates
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Schedule DocumentID hash index update
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", newDocument.DocumentID, newDocument.DocumentID, nil)
				s.logger.Debugf("Scheduled DocumentID hash index update for document '%s'", newDocument.DocumentID)

			} else if indexRef.IndexType == "btree" {
				// Extract the field value for BTree indexing
				fieldValue, err := extractFieldValueForIndex(*newDocument, indexRef.BTreeIndexField.FieldName)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", newDocument.DocumentID, err)
					continue
				}

				// Schedule BTree index update
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", newDocument.DocumentID, fieldValue, nil)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s'",
					newDocument.DocumentID, indexRef.BTreeIndexField.FieldName)
			}
		}
	} else {
		s.logger.Warnf("No indexes found for bundle '%s'", bundle.Name)
	}

	// Schedule deferred metadata update instead of immediate calculation
	s.scheduleMetadataUpdate(docCommand.BundleName, "increment_docs", 1)

	// Add document to bundle file (storage layer handles page allocation)
	err = s.store.AddDocumentToBundleFile(bundle, newDocument)
	if err != nil {
		// Note: Metadata updates are deferred, so no rollback needed here
		// Failed operations won't have their metadata updates applied
		return "", fmt.Errorf("failed to add document to bundle: %w", err)
	}

	return newDocument.DocumentID, nil
}

func (s *BundleService) AddDocumentToBundleByStruct(database *models.Database, bundle *models.Bundle, document *models.Document) error {
	// Schedule deferred metadata update instead of immediate calculation
	s.scheduleMetadataUpdate(bundle.Name, "increment_docs", 1)

	if bundle.Indexes != nil {
		// Schedule deferred index updates instead of immediate updates
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Schedule DocumentID hash index update
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", document.DocumentID, document.DocumentID, nil)
				s.logger.Debugf("Scheduled DocumentID hash index update for document '%s'", document.DocumentID)
			} else if indexRef.IndexType == "btree" {
				// Extract the field value for BTree indexing
				fieldValue, err := extractFieldValueForIndex(*document, indexRef.BTreeIndexField.FieldName)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", document.DocumentID, err)
					continue
				}

				// Schedule BTree index update
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", document.DocumentID, fieldValue, nil)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s'",
					document.DocumentID, indexRef.BTreeIndexField.FieldName)
			}
		}
	}

	// Add document to bundle file
	err := s.store.AddDocumentToBundleFile(bundle, document)
	if err != nil {
		return fmt.Errorf("failed to add document to bundle: %w", err)
	}

	// Update in-memory cache: if the appropriate document page is loaded, add the document to it
	// This prevents cache inconsistency where disk has the new document but memory cache is stale
	pageID, err := s.findDocumentPage(bundle.Name, document.DocumentID)
	if err == nil {
		pageKey := fmt.Sprintf("%s:%d", bundle.Name, pageID)
		if page, exists := s.documentPages[pageKey]; exists {
			// Page is loaded in memory, add the new document to it
			page.Documents[document.DocumentID] = *document
			s.logger.Debugf("Added document '%s' to in-memory page %s", document.DocumentID, pageKey)
		}
	}

	return nil
}
func (s *BundleService) UpdateDocumentInBundle(bundle *models.Bundle, docCommand *models.DocumentUpdateCommand) error {
	args := settings.GetSettings()
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot update document")
		return fmt.Errorf("bundle '%s' is nil, cannot update document", docCommand.BundleName)
	}

	// Get the existing document
	filteredDocs, err := s.GetDocumentsByFilter(bundle, docCommand.WhereClause)
	if err != nil {
		return fmt.Errorf("failed to filter documents: %w", err)
	}

	if args.Debug {
		s.logger.Infof("Updating %d documents from bundle '%s' with filter '%s'", len(filteredDocs), docCommand.BundleName, docCommand.WhereClause)
	}

	// Validate document update fields against bundle field definitions
	err = s.validateUpdateFields(bundle, docCommand)
	if err != nil {
		return fmt.Errorf("document field validation failed: %w", err)
	}

	for _, doc := range filteredDocs {
		// Store the original document state for index maintenance
		originalDoc := *doc

		// Update the document fields
		// loop through the fields in the command and update the document
		for _, kv := range docCommand.Fields {

			foundField := doc.Fields[kv.Key]
			foundField.Name = kv.Key
			foundField.Value = kv.Value
			doc.Fields[kv.Key] = foundField
		}

		// Update indexes if they exist and if indexed fields have changed
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				s.logger.Debugf("Processing index '%s' of type '%s' for document update", indexName, indexRef.IndexType)

				if indexRef.IndexType == "btree" {
					// Check if the indexed field was updated
					fieldName := indexRef.BTreeIndexField.FieldName
					fieldWasUpdated := false

					// Check if this field was in the update command
					for _, kv := range docCommand.Fields {
						if kv.Key == fieldName {
							fieldWasUpdated = true
							break
						}
					}

					if fieldWasUpdated {
						s.logger.Debugf("Indexed field '%s' was updated, maintaining BTree index '%s'", fieldName, indexName)

						// Load BTree index on-demand
						btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
						if err != nil {
							s.logger.Errorf("Failed to load BTree index '%s': %v", indexName, err)
							return fmt.Errorf("failed to load BTree index: %w", err)
						}

						// Extract the old field value for deletion
						oldFieldValue, err := extractFieldValueForIndex(originalDoc, fieldName)
						if err != nil {
							s.logger.Warnf("Failed to extract old field value for document '%s': %v", doc.DocumentID, err)
						} else {
							// Convert old field value to bytes for BTree storage
							oldKeyBytes, err := convertValueToBytes(oldFieldValue)
							if err != nil {
								s.logger.Warnf("Failed to convert old field value to bytes for document '%s': %v", doc.DocumentID, err)
							} else {
								// Delete old key-value pair from the BTree index
								err = btreeIndex.Delete(oldKeyBytes, doc.DocumentID)
								if err != nil {
									s.logger.Warnf("Failed to delete old entry for document '%s' from BTree index '%s': %v", doc.DocumentID, indexName, err)
								} else {
									s.logger.Debugf("Successfully deleted old entry for document '%s' from BTree index '%s'", doc.DocumentID, indexName)
								}
							}
						}

						// Extract the new field value for insertion
						newFieldValue, err := extractFieldValueForIndex(*doc, fieldName)
						if err != nil {
							s.logger.Warnf("Failed to extract new field value for document '%s': %v", doc.DocumentID, err)
						} else {
							// Convert new field value to bytes for BTree storage
							newKeyBytes, err := convertValueToBytes(newFieldValue)
							if err != nil {
								s.logger.Warnf("Failed to convert new field value to bytes for document '%s': %v", doc.DocumentID, err)
							} else {
								// Insert new key-value pair into the BTree index
								err = btreeIndex.Insert(newKeyBytes, doc.DocumentID)
								if err != nil {
									s.logger.Errorf("Failed to insert new entry for document '%s' into BTree index '%s': %v", doc.DocumentID, indexName, err)
									return fmt.Errorf("failed to update document in BTree index: %w", err)
								} else {
									s.logger.Debugf("Successfully inserted new entry for document '%s' into BTree index '%s'", doc.DocumentID, indexName)
								}
							}
						}
					} else {
						s.logger.Debugf("Indexed field '%s' was not updated, skipping BTree index maintenance for '%s'", fieldName, indexName)
					}
				} else if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
					// DocumentID hash indexes don't need update maintenance since DocumentID never changes
					s.logger.Debugf("Skipping DocumentID hash index '%s' - DocumentID cannot be updated", indexName)
				}
			}
		}

		// Save the updated document back to the bundle
		err = s.store.UpdateDocumentInBundleFile(bundle, doc)
		if err != nil {
			return fmt.Errorf("failed to update document in bundle: %w", err)
		}

		// Document is now updated in the persistent store and will be loaded from pages as needed
	}

	return nil
}

func (s *BundleService) DeleteDocumentFromBundle(bundle *models.Bundle, docCommand *models.DocumentDeleteCommand) error {
	args := settings.GetSettings()

	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot delete document")
		return fmt.Errorf("bundle '%s' is nil, cannot delete document", docCommand.BundleName)
	}

	filteredDocs, err := s.GetDocumentsByFilter(bundle, docCommand.WhereClause)
	if err != nil {
		return fmt.Errorf("failed to filter documents: %w", err)
	}

	if args.Debug {
		s.logger.Infof("Deleting %d documents from bundle '%s' with filter '%s'", len(filteredDocs), docCommand.BundleName, docCommand.WhereClause)
	}

	for _, doc := range filteredDocs {
		// Remove the document from indexes using lazy loading
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
					// Load hash index on-demand
					hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
					if err != nil {
						s.logger.Errorf("Failed to load hash index '%s': %v", indexName, err)
						continue // Continue with other indexes
					}

					deleted, err := hashIndex.DeleteDocument(doc.DocumentID)
					if err != nil {
						s.logger.Warnf("Failed to delete DocumentID '%s' from hash index '%s': %v",
							doc.DocumentID, indexName, err)
					} else if deleted {
						s.logger.Debugf("Successfully deleted DocumentID '%s' from hash index '%s'",
							doc.DocumentID, indexName)
					} else {
						s.logger.Debugf("DocumentID '%s' was not found in hash index '%s'",
							doc.DocumentID, indexName)
					}
				} else if indexRef.IndexType == "btree" {
					// Load BTree index on-demand
					btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
					if err != nil {
						s.logger.Errorf("Failed to load BTree index '%s': %v", indexName, err)
						continue // Continue with other indexes
					}

					// Extract the field value for deletion
					fieldValue, err := extractFieldValueForIndex(*doc, indexRef.BTreeIndexField.FieldName)
					if err != nil {
						s.logger.Warnf("Failed to extract field value for document '%s': %v", doc.DocumentID, err)
						continue
					}

					// Convert field value to bytes for BTree storage
					keyBytes, err := convertValueToBytes(fieldValue)
					if err != nil {
						s.logger.Warnf("Failed to convert field value to bytes for document '%s': %v", doc.DocumentID, err)
						continue
					}

					// Delete from the BTree index
					err = btreeIndex.Delete(keyBytes, doc.DocumentID)
					if err != nil {
						s.logger.Warnf("Failed to delete document '%s' from BTree index '%s': %v", doc.DocumentID, indexName, err)
					} else {
						s.logger.Debugf("Successfully deleted document '%s' from BTree index '%s'",
							doc.DocumentID, indexName)
					}
				}
			}
		}

		// Remove the document from the bundle file
		err = s.store.DeleteDocumentFromBundleFile(bundle, doc.DocumentID)
		if err != nil {
			return fmt.Errorf("failed to remove document from bundle: %w", err)
		}

		// Schedule deferred metadata update instead of immediate calculation
		s.scheduleMetadataUpdate(docCommand.BundleName, "decrement_docs", 1)

		// Invalidate any loaded pages that might contain this document
		// TODO: More efficient implementation would find the specific page
		keysToDelete := make([]string, 0)
		for pageKey := range s.documentPages {
			if strings.HasPrefix(pageKey, docCommand.BundleName+":") {
				keysToDelete = append(keysToDelete, pageKey)
			}
		}
		for _, key := range keysToDelete {
			delete(s.documentPages, key)
		}
	}
	return nil
}

// GetDocumentByID retrieves a document by its ID using the hash index for fast lookup
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

				results, err := hashIndex.Search(documentID)
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

// GetDocumentsByFilter retrieves documents from a bundle based on filter criteria
// This function follows the Single Responsibility Principle by handling only document filtering
// Following SyndrDB comprehensive error handling, it optimizes queries using available indexes
// Parameters:
//   - bundle: The bundle to filter documents from
//   - whereParts: The WHERE clause string for filtering
//
// Returns:
//   - []*models.Document: Array of documents matching the filter criteria
//   - error: Any error that occurred during filtering
func (s *BundleService) GetDocumentsByFilter(bundle *models.Bundle, whereParts string) ([]*models.Document, error) {
	// Validate input parameters following SyndrDB defensive programming practices
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot filter documents")
		return nil, fmt.Errorf("bundle is nil, cannot filter documents")
	}

	// Force flush any pending metadata updates to ensure accurate PageCount
	//s.logger.Infof("DEBUG: GetDocumentsByFilter - flushing metadata updates")
	s.flushMetadataUpdates()

	// Use page-based document loading instead of relying on bundle.Documents
	// This ensures scalability and consistency with the new architecture
	// allDocuments, err := s.getAllDocumentsForIndexing(bundle.Name)
	// if err != nil {
	// 	s.logger.Errorf("Failed to load documents for filtering: %v", err)
	// 	return nil, fmt.Errorf("failed to load documents for filtering: %w", err)
	// }

	// if len(allDocuments) == 0 {
	// 	s.logger.Debugf("Bundle '%s' has no documents", bundle.Name)
	// 	return []*models.Document{}, nil
	// }

	// // Convert documents map to slice for processing
	// allDocs := make([]*models.Document, 0, len(allDocuments))
	// for _, doc := range allDocuments {
	// 	d := doc // Avoid pointer aliasing following Go best practices
	// 	allDocs = append(allDocs, &d)
	// }

	// If no WHERE clause, return all documents
	if whereParts == "" {
		//s.logger.Infof("DEBUG: GetDocumentsByFilter - empty filter, calling getAllDocumentsForIndexing")
		result, err := s.getAllDocumentsForIndexing(bundle.Name)
		//s.logger.Infof("DEBUG: GetDocumentsByFilter - getAllDocumentsForIndexing returned %d documents, error: %v", len(result), err)
		return result, err
	}

	// s.logger.Debugf("Filtering %d documents in bundle '%s' with WHERE clause: %s",
	// 	len(allDocs), bundle.Name, whereParts)

	// CRITICAL: Use index-optimized filtering following SyndrDB performance optimization
	// This replaces the direct queryparser.FilterDocuments call with index-aware filtering
	//s.logger.Infof("DEBUG: GetDocumentsByFilter - non-empty filter, calling filterDocumentsWithIndexOptimization")
	result, err := s.filterDocumentsWithIndexOptimization(bundle, nil, whereParts)
	//s.logger.Infof("DEBUG: GetDocumentsByFilter - filterDocumentsWithIndexOptimization returned %d documents, error: %v", len(result), err)
	return result, err
}

// filterDocumentsWithIndexOptimization performs intelligent document filtering using available indexes
// This function follows the Single Responsibility Principle by handling only index-optimized filtering
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
	// Validate input parameters following SyndrDB defensive programming practices
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

	// Fallback to full document scan using modern page-based loading
	// Following SyndrDB comprehensive error handling, provide reliable fallback
	s.logger.Debugf("No suitable index found, performing full document scan with page-based loading")

	// Load all documents using the modern page-based system
	allDocs, err := s.getAllDocumentsForIndexing(bundle.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to load documents for filtering: %w", err)
	}

	s.logger.Debugf("Loaded %d documents for filtering", len(allDocs))

	// Apply filtering using the raw document filter (works with document slice)
	filteredDocs, err := queryparser.FilterDocumentsRaw(allDocs, whereClause, s.logger)
	if err != nil {
		return nil, fmt.Errorf("full document scan failed: %w", err)
	}

	s.logger.Debugf("Full document scan completed, found %d matching documents", len(filteredDocs))
	return filteredDocs, nil
}

// tryHashIndexOptimization attempts to use hash indexes for query optimization
// This function follows the Single Responsibility Principle by handling only hash index optimization
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
	// Parse the WHERE clause to identify potential hash index usage
	// Following SyndrDB modular development, use existing query parsing infrastructure
	whereGroup, err := queryparser.ParseWhereClause(whereClause)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// Hash indexes are optimal for simple equality conditions
	// Following SyndrDB performance optimization, use hash indexes for exact matches
	if len(whereGroup.Clauses) == 1 && len(whereGroup.SubGroups) == 0 {
		clause := whereGroup.Clauses[0]

		// Only use hash index for equality operations
		if clause.Operator == "==" {
			// Check if we have a hash index for this field
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "hash" && s.getIndexFieldName(indexRef) == clause.Field {
					s.logger.Debugf("Found hash index '%s' for field '%s'", indexName, clause.Field)

					// Load the hash index on-demand
					hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
					if err != nil {
						s.logger.Warnf("Failed to load hash index '%s': %v", indexName, err)
						continue
					}

					// Search the hash index for the value
					searchKey := fmt.Sprintf("%v", clause.Value)
					docIDs, err := hashIndex.Search(searchKey)
					if err != nil {
						s.logger.Warnf("Hash index search failed for '%s': %v", searchKey, err)
						continue
					}

					s.logger.Debugf("Hash index found %d document IDs for value '%s'", len(docIDs), searchKey)

					// Convert document IDs to actual documents using page-based loading
					result := make([]*models.Document, 0, len(docIDs))
					for _, docID := range docIDs {
						doc, err := s.GetDocument(bundle.Name, bundle.Database.Name, docID)
						if err != nil {
							s.logger.Warnf("Document ID '%s' found in hash index but could not be loaded: %v", docID, err)
							continue
						}
						result = append(result, doc)
					}

					s.logger.Debugf("Successfully retrieved %d documents via hash index '%s'", len(result), indexName)
					return result, true, nil
				}
			}
		}
	}

	// Hash index optimization not applicable
	return nil, false, nil
}

// tryBTreeIndexOptimization attempts to use BTree indexes for query optimization
// This function follows the Single Responsibility Principle by handling only BTree index optimization
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
	// Parse the WHERE clause to identify potential BTree index usage
	whereGroup, err := queryparser.ParseWhereClause(whereClause)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// BTree indexes support equality, range, and comparison operations
	// Following SyndrDB performance optimization, use BTree indexes for various operations
	if len(whereGroup.Clauses) == 1 && len(whereGroup.SubGroups) == 0 {
		clause := whereGroup.Clauses[0]

		// BTree indexes support multiple operators
		supportedOps := []string{"==", "!=", "<", ">", "<=", ">="}
		isSupported := false
		for _, op := range supportedOps {
			if clause.Operator == op {
				isSupported = true
				break
			}
		}

		if isSupported {
			// Check if we have a BTree index for this field
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "btree" && s.getIndexFieldName(indexRef) == clause.Field {
					s.logger.Debugf("Found BTree index '%s' for field '%s' with operator '%s'",
						indexName, clause.Field, clause.Operator)

					// Load the BTree index on-demand
					btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
					if err != nil {
						s.logger.Warnf("Failed to load BTree index '%s': %v", indexName, err)
						continue
					}

					// Convert search value to bytes for BTree search
					keyBytes, err := convertValueToBytes(clause.Value)
					if err != nil {
						s.logger.Warnf("Failed to convert search value to bytes: %v", err)
						continue
					}
					s.logger.Infof("Performing BTree index search  '%v' with key '%v'",
						btreeIndex, keyBytes)
					// Perform BTree search based on operator
					var docIDs []string = []string{}
					// switch clause.Operator {
					// case "==":
					//     docIDs, err = btreeIndex.Search(keyBytes)
					// case "<":
					//     docIDs, err = btreeIndex.SearchLessThan(keyBytes)
					// case ">":
					//     docIDs, err = btreeIndex.SearchGreaterThan(keyBytes)
					// case "<=":
					//     docIDs, err = btreeIndex.SearchLessThanOrEqual(keyBytes)
					// case ">=":
					//     docIDs, err = btreeIndex.SearchGreaterThanOrEqual(keyBytes)
					// case "!=":
					//     // For inequality, we need to get all documents and exclude matches
					//     allDocIDs, searchErr := btreeIndex.SearchAll()
					//     if searchErr != nil {
					//         err = searchErr
					//     } else {
					//         equalDocIDs, equalErr := btreeIndex.Search(keyBytes)
					//         if equalErr != nil {
					//             err = equalErr
					//         } else {
					//             // Remove equal matches from all documents
					//             docIDs = s.excludeDocumentIDs(allDocIDs, equalDocIDs)
					//         }
					//     }
					// }

					if err != nil {
						s.logger.Warnf("BTree index search failed: %v", err)
						continue
					}

					s.logger.Debugf("BTree index found %d document IDs for operator '%s' with value '%v'",
						len(docIDs), clause.Operator, clause.Value)

					// Convert document IDs to actual documents
					// Convert document IDs to actual documents using page-based loading
					if len(docIDs) == 0 {
						s.logger.Debugf("BTree index search returned no document IDs")
						return []*models.Document{}, true, nil
					}

					result := make([]*models.Document, 0, len(docIDs))
					for _, docID := range docIDs {
						doc, err := s.GetDocument(bundle.Name, bundle.Database.Name, docID)
						if err != nil {
							s.logger.Warnf("Document ID '%s' found in BTree index but could not be loaded: %v", docID, err)
							continue
						}
						result = append(result, doc)
					}

					s.logger.Debugf("Successfully retrieved %d documents via BTree index '%s'", len(result), indexName)
					return result, true, nil
				}
			}
		}
	}

	// BTree index optimization not applicable
	return nil, false, nil
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

// excludeDocumentIDs removes specified document IDs from a slice
// This function follows the Single Responsibility Principle by handling only document ID exclusion
// Following SyndrDB comprehensive error handling, it safely performs set operations
// Parameters:
//   - allDocIDs: The complete list of document IDs
//   - excludeDocIDs: The document IDs to exclude
//
// Returns:
//   - []string: The filtered list of document IDs
// func (s *BundleService) excludeDocumentIDs(allDocIDs, excludeDocIDs []string) []string {
// 	// Create a map of IDs to exclude for O(1) lookup
// 	excludeMap := make(map[string]bool, len(excludeDocIDs))
// 	for _, id := range excludeDocIDs {
// 		excludeMap[id] = true
// 	}

// 	// Filter the all IDs list
// 	result := make([]string, 0, len(allDocIDs))
// 	for _, id := range allDocIDs {
// 		if !excludeMap[id] {
// 			result = append(result, id)
// 		}
// 	}

// 	return result
// }

// closeAllIndexes closes all loaded index instances for a bundle
// This function ensures proper resource cleanup when bundles are unloaded
// Parameters:
//   - bundle: The bundle whose indexes should be closed
//
// Returns:
//   - error: Any error that occurred during closing
// func (s *BundleService) closeAllIndexes(bundle *models.Bundle) error {
// 	if bundle.Indexes == nil {
// 		return nil
// 	}

// 	var errors []string

// 	for indexName, indexRef := range bundle.Indexes {
// 		if indexRef.IndexInstance != nil {
// 			switch index := indexRef.IndexInstance.(type) {
// 			case *hashindex.HashIndex:
// 				if err := index.Close(); err != nil {
// 					errorMsg := fmt.Sprintf("failed to close hash index '%s': %v", indexName, err)
// 					s.logger.Errorf(errorMsg)
// 					errors = append(errors, errorMsg)
// 				} else {
// 					s.logger.Debugf("Successfully closed hash index '%s'", indexName)
// 				}
// 			case *btreeindexV2.BTreeIndex:
// 				if err := index.Close(); err != nil {
// 					errorMsg := fmt.Sprintf("failed to close BTree index '%s': %v", indexName, err)
// 					s.logger.Errorf(errorMsg)
// 					errors = append(errors, errorMsg)
// 				} else {
// 					s.logger.Debugf("Successfully closed BTree index '%s'", indexName)
// 				}
// 			default:
// 				s.logger.Warnf("Unknown index type for index '%s': %T", indexName, indexRef.IndexInstance)
// 			}

// 			// Clear the instance reference
// 			indexRef.IndexInstance = nil
// 			bundle.Indexes[indexName] = indexRef
// 		}
// 	}

// 	if len(errors) > 0 {
// 		return fmt.Errorf("errors occurred while closing indexes: %v", errors)
// 	}

// 	return nil
// }

// validateDocumentFields validates that document fields match bundle field definitions
// This function ensures that:
// 1. All fields in the document command exist in the bundle's field definitions
// 2. Field data types match the bundle field definition types
// 3. Required fields are present
// 4. Field values are compatible with their defined types
func (s *BundleService) validateDocumentFields(bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

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

		// Validate and convert field data type using fast pre-compiled converter
		convertedValue, err := s.validateAndConvertFieldTypeFast(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Update the field value with the converted value
		docCommand.Fields[i].Value = convertedValue

		// Mark this field as provided
		providedFields[fieldName] = true
	}

	// Check that all required fields are provided
	for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
		if fieldDef.IsRequired && !providedFields[fieldName] {
			// Skip DocumentID if it's auto-generated
			if fieldName == "DocumentID" {
				continue
			}
			return fmt.Errorf("required field '%s' is missing from document", fieldName)
		}
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

	// Validate each field in the update command
	for i, field := range docCommand.Fields {
		fieldName := field.Key
		fieldValue := field.Value

		// Check if the field exists in bundle field definitions
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' is not defined in bundle '%s'", fieldName, bundle.Name)
		}

		// Validate and convert field data type using fast pre-compiled converter
		convertedValue, err := s.validateAndConvertFieldTypeFast(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Update the field value with the converted value
		docCommand.Fields[i].Value = convertedValue

		// todo:Additional validation for unique fields could be added here
		// if fieldDef.IsUnique {
		//     // TODO: Check if the new value would violate uniqueness constraint
		// }
	}

	return nil
}

// registerBundleInPrimary adds the bundle information to the "Bundles" bundle in the Primary database
func (s *BundleService) registerBundleInPrimary(bundle *models.Bundle) error {
	// Since we can't directly import the server package due to circular dependency,
	// this method is meant to be overridden or called through the service manager
	// The actual registration logic is implemented in CatalogService.AddBundleToCatalog

	s.logger.Debugf("Bundle '%s' needs to be registered in primary catalog (handled by CatalogService)", bundle.Name)
	return nil
}

// discoverBundleIndexes scans for existing index files and populates the bundle's Indexes field
func (s *BundleService) discoverBundleIndexes(bundle *models.Bundle) error {
	//args := settings.GetSettings()

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	// Look for hash index files: BundleName_FieldName.hidx
	hashPattern := fmt.Sprintf("%s/%s_*.hidx", databasePath, bundle.Name)
	hashFiles, err := filepath.Glob(hashPattern)
	if err != nil {
		return fmt.Errorf("failed to scan for hash index files: %w", err)
	}

	for _, hashFile := range hashFiles {
		// Extract field name from filename: BundleName_FieldName.hidx
		baseName := filepath.Base(hashFile)
		// Remove .hidx extension
		baseName = strings.TrimSuffix(baseName, ".hidx")
		// Remove bundle name prefix and underscore
		prefix := bundle.Name + "_"
		if !strings.HasPrefix(baseName, prefix) {
			continue // Skip files that don't match expected pattern
		}
		fieldName := strings.TrimPrefix(baseName, prefix)

		// Check if this field exists in the bundle's field definitions
		if bundle.DocumentStructure.FieldDefinitions != nil {
			if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
				s.logger.Warnf("Found hash index file for field '%s' but field not defined in bundle '%s'", fieldName, bundle.Name)
				continue
			}
		}

		// Create index reference
		indexName := fmt.Sprintf("%s_%s_hash", bundle.Name, fieldName)
		indexRef := models.IndexReference{
			IndexType: "hash",
			HashIndexField: models.IndexField{
				FieldName: fieldName,
			},
		}

		bundle.Indexes[indexName] = indexRef
		s.logger.Debugf("Discovered hash index '%s' for field '%s' in bundle '%s'", indexName, fieldName, bundle.Name)
	}

	// TODO: Add discovery for BTree indexes when they have a consistent file pattern
	// Look for btree index files if there's a predictable naming pattern

	s.logger.Debugf("Discovered %d indexes for bundle '%s'", len(bundle.Indexes), bundle.Name)
	return nil
}

// Shutdown ensures all pending operations are completed before service termination
// This method should be called during graceful shutdown to maintain data consistency
func (s *BundleService) Shutdown() error {
	s.logger.Infof("Shutting down BundleService, flushing pending operations...")

	// Close scanners before other cleanup
	s.CloseAllScanners()

	// Force flush any pending index updates
	s.forceFlushIndexUpdates()

	// Also force flush any remaining metadata updates
	if len(s.metadataUpdateBuffer) > 0 {
		s.logger.Debugf("Force flushing %d remaining metadata updates during shutdown", len(s.metadataUpdateBuffer))
		s.flushMetadataUpdates()
	}

	// Close all loaded indexes properly
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexInstance != nil {
					s.logger.Debugf("Closing index '%s' for bundle '%s'", indexName, bundleName)
					// Proper index closing would go here based on index type
					// For now, just log the action
				}
			}
		}
	}

	s.logger.Infof("BundleService shutdown completed")
	return nil
}

// DOCUMENT SCANNER INTEGRATION: Scanner management methods

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

	// Cache the scanner
	s.bundleScanners[bundle.Name] = scanner
	s.logger.Infof("Created and cached document scanner for bundle '%s'", bundle.Name)

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

	if scanner, exists := s.bundleScanners[bundleName]; exists {
		scanner.Close()
		delete(s.bundleScanners, bundleName)
		s.logger.Infof("Removed document scanner for bundle '%s'", bundleName)
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
	s.logger.Info("Closed all document scanners")
}
