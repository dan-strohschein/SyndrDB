package bundle

import (
	"fmt"
	"path/filepath"
	"time"

	"syndrdb/src/internal/domain/index/hashindexV3"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
)

/*
HASH INDEX MAINTENANCE

This file implements maintenance and rebuild operations for hash indexes,
providing automatic index rebuilding when staleness thresholds are exceeded.

DUAL-INSTANCE SWAP PATTERN:
1. Keep old index instance running (serves queries)
2. Create new index instance in parallel
3. Re-insert all live entries with fresh pageIDs
4. Atomic pointer swap in bundle.Indexes map
5. Close old instance and cleanup files

This ensures zero downtime during index rebuilds.
*/

// RebuildHashIndexFromBundle rebuilds a hash index with fresh pageIDs from the bundle's SortedIndex
// Uses dual-instance swap pattern to allow queries during rebuild
//
// Parameters:
//   - bundle: The bundle containing the index
//   - indexName: Name of the hash index to rebuild
//   - databaseName: Database name (for file paths)
//
// Returns:
//   - *HashIndexMaintenanceResult: Detailed rebuild results
//   - error: Any error encountered during rebuild
func (s *BundleService) RebuildHashIndexFromBundle(
	bundle *models.Bundle,
	indexName string,
	databaseName string,
) (*HashIndexMaintenanceResult, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	s.logger.Debugf("Rebuilding hash index '%s' for bundle '%s'", indexName, bundle.Name)

	startTime := time.Now()
	result := &HashIndexMaintenanceResult{
		IndexName:  indexName,
		BundleName: bundle.Name,
		Success:    false,
		StartTime:  startTime,
	}

	// Initialize maintenance metadata if needed
	indexRef, exists := bundle.Indexes[indexName]
	if !exists {
		result.ErrorMessage = fmt.Sprintf("index '%s' does not exist in bundle '%s'", indexName, bundle.Name)
		return result, fmt.Errorf(result.ErrorMessage)
	}

	if indexRef.IndexType != "hash" {
		result.ErrorMessage = fmt.Sprintf("index '%s' is not a hash index (type: %s)", indexName, indexRef.IndexType)
		return result, fmt.Errorf(result.ErrorMessage)
	}

	// Initialize maintenance metadata if needed
	if indexRef.Maintenance == nil {
		indexRef.Maintenance = &models.IndexMaintenanceMetadata{
			IsHealthy:     true,
			LastQueryTime: time.Now(),
		}
	}

	// STEP 1: Load the old index
	oldIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("failed to load old index: %v", err)
		indexRef.Maintenance.IsHealthy = false
		indexRef.Maintenance.LastFailureReason = result.ErrorMessage
		indexRef.Maintenance.LastFailureTime = time.Now()
		return result, fmt.Errorf(result.ErrorMessage)
	}

	// STEP 2: Extract all entries from old index
	s.logger.Debugf("Extracting entries from old index '%s'", indexName)
	docIDsByPage, err := oldIndex.GetAllDocumentIDs()
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("failed to extract entries: %v", err)
		indexRef.Maintenance.IsHealthy = false
		indexRef.Maintenance.LastFailureReason = result.ErrorMessage
		indexRef.Maintenance.LastFailureTime = time.Now()
		return result, fmt.Errorf(result.ErrorMessage)
	}

	// Flatten to get total count
	var allDocIDs []string
	for _, docIDs := range docIDsByPage {
		allDocIDs = append(allDocIDs, docIDs...)
	}
	result.EntriesProcessed = len(allDocIDs)

	s.logger.Debugf("Extracted %d entries from old index", result.EntriesProcessed)

	// STEP 3: Create new index instance with temporary name
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")
	tempIndexName := fmt.Sprintf("%s_rebuild_%d", indexName, time.Now().Unix())

	newIndexConfig := hashindexV3.IndexConfig{
		IndexName:          tempIndexName,
		DataDir:            indexesPath,
		BundleName:         bundle.Name,
		FieldName:          indexRef.HashIndexField.FieldName,
		IsUnique:           indexRef.HashIndexField.IsUnique,
		MemTableMaxSize:    100000, // Same as original
		CompactionMaxFiles: 10,
		Logger:             s.logger,
	}

	s.logger.Debugf("Creating new index instance: %s", tempIndexName)
	newIndex, err := hashindexV3.NewHashIndexV3(newIndexConfig)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("failed to create new index: %v", err)
		indexRef.Maintenance.IsHealthy = false
		indexRef.Maintenance.LastFailureReason = result.ErrorMessage
		indexRef.Maintenance.LastFailureTime = time.Now()
		return result, fmt.Errorf(result.ErrorMessage)
	}

	// STEP 4: Re-insert all entries with fresh pageIDs from SortedIndex
	s.logger.Debugf("Re-inserting %d entries with fresh pageIDs", len(allDocIDs))
	failedInserts := 0
	pageSize := uint32(bundle.PageSize)
	if pageSize == 0 {
		pageSize = 4096 // Default page size
	}

	for _, docID := range allDocIDs {
		// Get fresh pageID from SortedIndex using GetGlobalPosition
		pageID, found := bundle.SortedIndex.GetGlobalPosition(docID, pageSize)
		if !found {
			s.logger.Warnf("Document %s not found in SortedIndex", docID)
			failedInserts++
			continue
		}

		// Get the document to extract the field value
		doc, err := s.GetDocumentByID(bundle, docID)
		if err != nil {
			s.logger.Warnf("Document %s not found during rebuild: %v", docID, err)
			failedInserts++
			continue
		}

		// Extract field value as string
		var keyValue string
		if indexRef.HashIndexField.FieldName == "DocumentID" {
			keyValue = docID
		} else {
			field, exists := doc.Fields[indexRef.HashIndexField.FieldName]
			if !exists {
				// Skip documents that don't have the indexed field
				continue
			}
			keyValue = field.Value.String()
		}

		// Insert into new index (commitSequence=0, versionSequence=0 for committed docs)
		if err := newIndex.Put(keyValue, docID, pageID, 0, 0); err != nil {
			s.logger.Warnf("Failed to insert entry for document %s: %v", docID, err)
			failedInserts++
		}
	}

	if failedInserts > 0 {
		s.logger.Warnf("Failed to insert %d entries during rebuild", failedInserts)
	}

	// STEP 5: Flush new index to disk
	s.logger.Debugf("Flushing new index to disk")
	if err := newIndex.Flush(); err != nil {
		result.ErrorMessage = fmt.Sprintf("failed to flush new index: %v", err)
		indexRef.Maintenance.IsHealthy = false
		indexRef.Maintenance.LastFailureReason = result.ErrorMessage
		indexRef.Maintenance.LastFailureTime = time.Now()
		newIndex.Close()
		return result, fmt.Errorf(result.ErrorMessage)
	}

	// STEP 6: Atomic pointer swap - update the cache and bundle reference
	s.logger.Debugf("Performing atomic index swap")

	// Update the in-memory cache
	s.loadedIndexes.Set(bundle.Name, indexName, newIndex)

	// Update the bundle's index reference (for query router)
	indexRef.IndexInstance = newIndex
	bundle.Indexes[indexName] = indexRef

	// STEP 7: Close old index (queries now use new one)
	s.logger.Debugf("Closing old index instance")
	if err := oldIndex.Close(); err != nil {
		s.logger.Warnf("Failed to close old index (non-fatal): %v", err)
	}

	// STEP 8: Cleanup old index files (optional - could keep as backup)
	// For now, we keep old files as they'll be cleaned up by next compaction

	// STEP 9: Update maintenance metadata
	indexRef.Maintenance.LastRebuildTime = time.Now()
	indexRef.Maintenance.TotalRebuilds++
	indexRef.Maintenance.IsHealthy = true
	indexRef.Maintenance.LastFailureReason = ""
	bundle.Indexes[indexName] = indexRef

	// Update result
	result.Success = true
	result.Duration = time.Since(startTime)
	result.EntriesInserted = result.EntriesProcessed - failedInserts

	s.logger.Debugf("Successfully rebuilt hash index '%s': %d entries processed, %d inserted, %d failed in %v",
		indexName, result.EntriesProcessed, result.EntriesInserted, failedInserts, result.Duration)

	return result, nil
}

// HashIndexMaintenanceResult contains the results of a hash index rebuild operation
type HashIndexMaintenanceResult struct {
	IndexName        string
	BundleName       string
	Success          bool
	StartTime        time.Time
	Duration         time.Duration
	EntriesProcessed int
	EntriesInserted  int
	ErrorMessage     string
}
