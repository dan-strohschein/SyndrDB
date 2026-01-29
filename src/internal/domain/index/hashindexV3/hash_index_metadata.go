package hashindexV3

/*
DEPRECATED: This file contains the old metadata system using separate index.meta files.
This has been replaced by the header-based metadata system where each index file
contains its own metadata in a header at the beginning of the file.

NEW SYSTEM:
- Headers embedded in index files (.hidx files)
- See: header.go, header_manager.go, header_serializer.go
- File naming: FieldName-fk.N.hidx or FieldName.N.hidx

MIGRATION PATH:
- New indexes automatically use the header system
- Old indexes will continue to work with this legacy system
- Future: Add migration utility to convert old indexes to new format

TODO: Remove this file after migration is complete (Phase 12+)
*/

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// IndexMetadata represents persisted index metadata
// DEPRECATED: Use IndexHeader in header.go instead
// This file enables fast recovery without scanning all entries
type IndexMetadata struct {
	Version     int    `json:"version"`     // Metadata format version
	IndexName   string `json:"indexName"`   // Index name for validation
	BundleName  string `json:"bundleName"`  // Bundle name for validation
	MaxSequence uint64 `json:"maxSequence"` // Highest sequence number assigned
	Checksum    uint32 `json:"checksum"`    // Simple integrity check
}

// metadataFilePath returns the path to the metadata file
func (idx *HashIndexV3) metadataFilePath() string {
	return filepath.Join(idx.config.DataDir, "index.meta")
}

// SaveMetadata persists index metadata to disk
// DEPRECATED: This function is a no-op. Metadata is now stored in file headers.
// See: HeaderManager.UpdateStatistics() for the new approach
// Following Single Responsibility: only handles metadata I/O
func (idx *HashIndexV3) SaveMetadata() error {
	// TODO (Phase 12+): Remove this function entirely after confirming no callers
	// Metadata is now automatically written to headers during file operations

	// For backward compatibility, we simply return success
	// The header system handles persistence automatically via EntryStorage
	idx.logger.Debugw("SaveMetadata called (deprecated, now no-op - using headers)",
		"indexName", idx.config.IndexName)

	return nil

	/* ORIGINAL CODE - REMOVED, REPLACED BY HEADER SYSTEM
	if !idx.isDirty {
		// No changes to persist
		return nil
	}

	idx.statsMutex.RLock()
	maxSeq := idx.stats.MaxSequence
	idx.statsMutex.RUnlock()

	metadata := IndexMetadata{
		Version:     1,
		IndexName:   idx.config.IndexName,
		BundleName:  idx.config.BundleName,
		MaxSequence: maxSeq,
		Checksum:    computeMetadataChecksum(maxSeq),
	}

	// Atomic write: temp file + rename
	tempPath := idx.metadataFilePath() + ".tmp"
	finalPath := idx.metadataFilePath()

	// Marshal to JSON
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write to temp file
	err = os.WriteFile(tempPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write metadata temp file: %w", err)
	}

	// Atomic rename
	err = os.Rename(tempPath, finalPath)
	if err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	// Mark as clean
	idx.isDirty = false

	idx.logger.Debugw("Saved index metadata",
		"indexName", idx.config.IndexName,
		"maxSequence", maxSeq)

	return nil
	*/
}

// LoadMetadata reads index metadata from disk
// DEPRECATED: This function now reads from file headers instead of separate metadata files
// See: HeaderManager.ReadHeader() for the new approach
// Returns metadata and error (nil error if file doesn't exist = new index)
func (idx *HashIndexV3) LoadMetadata() (*IndexMetadata, error) {
	// TODO (Phase 12+): Remove this function entirely after confirming no callers
	// Metadata is now automatically read from headers during file opening

	// For backward compatibility, check if old metadata file exists
	metaPath := idx.metadataFilePath()

	// Check if file exists
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		// New index, no old metadata - this is expected with new header system
		idx.logger.Debugw("LoadMetadata: No legacy metadata file (expected with header system)",
			"indexName", idx.config.IndexName)
		return nil, nil
	}

	// Old metadata file exists - read it for backward compatibility
	idx.logger.Warnw("LoadMetadata: Found legacy metadata file (should migrate to header system)",
		"indexName", idx.config.IndexName,
		"metaPath", metaPath)

	// Read file
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	// Unmarshal
	var metadata IndexMetadata
	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Validate checksum
	expectedChecksum := computeMetadataChecksum(metadata.MaxSequence)
	if metadata.Checksum != expectedChecksum {
		return nil, fmt.Errorf("metadata checksum mismatch: expected %d, got %d",
			expectedChecksum, metadata.Checksum)
	}

	// Validate index/bundle names
	// MIGRATION FIX: Handle old naming convention where FK indexes had "_fk" suffix duplicated
	// Old: "BundleName_FieldName_fk" → New: "FieldName_fk"
	expectedIndexName := idx.config.IndexName
	actualIndexName := metadata.IndexName

	// Check for exact match first
	if actualIndexName != expectedIndexName {
		// MIGRATION: Check if this is an old-style FK index name
		// Old format: "{BundleName}_{FieldName}_fk" → New format: "{FieldName}_fk"
		if strings.HasSuffix(expectedIndexName, "_fk") {
			// Try to match old naming pattern: BundleName_FieldName_fk
			oldStyleName := fmt.Sprintf("%s_%s", idx.config.BundleName, expectedIndexName)
			if actualIndexName == oldStyleName {
				idx.logger.Warnw("Found old-style FK index metadata, will migrate on next save",
					"oldName", actualIndexName,
					"newName", expectedIndexName)
				// Accept the old name for now - metadata will be updated on next save
			} else {
				return nil, fmt.Errorf("metadata index name mismatch: expected %s, got %s (not old format either: %s)",
					expectedIndexName, actualIndexName, oldStyleName)
			}
		} else {
			return nil, fmt.Errorf("metadata index name mismatch: expected %s, got %s",
				expectedIndexName, actualIndexName)
		}
	}

	if metadata.BundleName != idx.config.BundleName {
		return nil, fmt.Errorf("metadata bundle name mismatch: expected %s, got %s",
			idx.config.BundleName, metadata.BundleName)
	}

	idx.logger.Debugw("Loaded index metadata",
		"indexName", idx.config.IndexName,
		"maxSequence", metadata.MaxSequence)

	return &metadata, nil
}

// RestoreGlobalSequence recovers the global sequence counter
// DEPRECATED: Sequence numbers are now stored in file headers
// This function now reads from the latest file header instead of scanning entries
// Option B: Hybrid approach with safety validation
// 1. Load metadata for fast recovery
// 2. Scan most recent entry file for safety check
// 3. Use max(metadata, actualMax) + safetyMargin
func (idx *HashIndexV3) RestoreGlobalSequence() error {
	// TODO (Phase 12+): Simplify this to only read from headers
	// For now, keep backward compatibility but log deprecation

	idx.logger.Debugw("RestoreGlobalSequence called (now using header system)",
		"indexName", idx.config.IndexName)

	// Try to read from file header first (new system)
	var headerMax uint64 = 0
	files, err := idx.storage.GetEntryFiles()
	if err == nil && len(files) > 0 {
		// Get the latest file
		latestFile := files[len(files)-1]
		latestFilePath := filepath.Join(idx.config.DataDir, latestFile)

		// Read header from latest file
		header, _, err := idx.storage.headerManager.ReadHeader(latestFilePath)
		if err != nil {
			idx.logger.Warnw("Failed to read header from latest file",
				"file", latestFile,
				"error", err)
		} else if header != nil {
			headerMax = header.GlobalSequence
			idx.logger.Debugw("Read max sequence from header",
				"file", latestFile,
				"headerSequence", headerMax)
		}
	}

	// Load old metadata (deprecated path) for backward compatibility
	metadata, err := idx.LoadMetadata()
	if err != nil {
		idx.logger.Warnw("Failed to load metadata, will scan entries",
			"error", err)
		metadata = nil
	}

	var metadataMax uint64 = 0
	if metadata != nil {
		metadataMax = metadata.MaxSequence
	}

	// Take the maximum of header and old metadata
	maxFromPersistence := headerMax
	if metadataMax > maxFromPersistence {
		maxFromPersistence = metadataMax
	}

	// Scan most recent entry file for safety check
	actualMax, err := idx.scanLatestFileForMaxSequence()
	if err != nil {
		idx.logger.Warnw("Failed to scan latest file, using header/metadata only",
			"error", err)
		actualMax = 0
	}

	// Take the maximum of all sources
	safeMax := maxFromPersistence
	if actualMax > safeMax {
		safeMax = actualMax
		// Only warn if the discrepancy is significant (header not updated recently)
		// Don't warn if it's within our update interval (100 entries)
		discrepancy := actualMax - maxFromPersistence
		if discrepancy > 100 {
			idx.logger.Warnw("Actual max sequence exceeds header/metadata",
				"headerMax", headerMax,
				"metadataMax", metadataMax,
				"actualMax", actualMax,
				"discrepancy", discrepancy)
		} else {
			idx.logger.Debugw("Actual max sequence slightly exceeds header (expected)",
				"headerMax", headerMax,
				"actualMax", actualMax,
				"discrepancy", discrepancy)
		}
	}

	// Apply safety margin
	safetyMargin := uint64(idx.config.SequenceSafetyMargin)
	if safetyMargin == 0 {
		safetyMargin = 100 // Default safety margin
	}

	recoveredSequence := safeMax + safetyMargin

	// Set global sequence
	atomic.StoreUint64(&idx.GlobalSequence, recoveredSequence)

	// Update stats
	idx.statsMutex.Lock()
	idx.stats.MaxSequence = safeMax
	idx.statsMutex.Unlock()

	idx.logger.Infow("Restored global sequence",
		"indexName", idx.config.IndexName,
		"headerMax", headerMax,
		"metadataMax", metadataMax,
		"actualMax", actualMax,
		"safeMax", safeMax,
		"safetyMargin", safetyMargin,
		"recoveredSequence", recoveredSequence)

	return nil
}

// scanLatestFileForMaxSequence scans file headers to find max sequence
// Optimized to read headers only (not full entry data) for fast startup
func (idx *HashIndexV3) scanLatestFileForMaxSequence() (uint64, error) {
	// Get list of entry files
	files, err := idx.storage.GetEntryFiles()
	if err != nil {
		return 0, fmt.Errorf("failed to get entry files: %w", err)
	}

	if len(files) == 0 {
		// No files yet, new index
		return 0, nil
	}

	// For bucket-based storage, entries are distributed across multiple bucket files
	// Read headers from ALL files to find the true maximum sequence number
	// This is fast because we only read small headers, not full entry data
	var maxSeq uint64
	successCount := 0
	for _, filename := range files {
		filePath := filepath.Join(idx.config.DataDir, filename)
		
		// Read just the header (much faster than scanning entries)
		header, _, err := idx.storage.headerManager.ReadHeader(filePath)
		if err != nil {
			// If header read fails, fall back to entry scanning for this file
			idx.logger.Debugw("Header read failed, falling back to entry scan",
				"file", filename,
				"error", err)
			fileMaxSeq, scanErr := idx.storage.ScanFileForMaxSequence(filename)
			if scanErr != nil {
				idx.logger.Warnw("Failed to scan file for max sequence, skipping",
					"file", filename,
					"error", scanErr)
				continue
			}
			if fileMaxSeq > maxSeq {
				maxSeq = fileMaxSeq
			}
			successCount++
		} else if header != nil {
			// Fast path: use header's GlobalSequence
			if header.GlobalSequence > maxSeq {
				maxSeq = header.GlobalSequence
			}
			successCount++
		}
	}

	idx.logger.Debugw("Read headers from index files for max sequence",
		"totalFiles", len(files),
		"successfulReads", successCount,
		"maxSequence", maxSeq)

	return maxSeq, nil
}

// computeMetadataChecksum computes a simple checksum for integrity
// Using FNV-1a hash for consistency with entry checksums
func computeMetadataChecksum(maxSequence uint64) uint32 {
	// Simple checksum: hash the max sequence
	return ComputeHash(fmt.Sprintf("meta:%d", maxSequence))
}
