package hashindexV3

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// IndexMetadata represents persisted index metadata
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
// Following Single Responsibility: only handles metadata I/O
func (idx *HashIndexV3) SaveMetadata() error {
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
}

// LoadMetadata reads index metadata from disk
// Returns metadata and error (nil error if file doesn't exist = new index)
func (idx *HashIndexV3) LoadMetadata() (*IndexMetadata, error) {
	metaPath := idx.metadataFilePath()

	// Check if file exists
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		// New index, no metadata yet
		return nil, nil
	}

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
	if metadata.IndexName != idx.config.IndexName {
		return nil, fmt.Errorf("metadata index name mismatch: expected %s, got %s",
			idx.config.IndexName, metadata.IndexName)
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
// Option B: Hybrid approach with safety validation
// 1. Load metadata for fast recovery
// 2. Scan most recent entry file for safety check
// 3. Use max(metadata, actualMax) + safetyMargin
func (idx *HashIndexV3) RestoreGlobalSequence() error {
	// Load metadata
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

	// Scan most recent entry file for safety check
	actualMax, err := idx.scanLatestFileForMaxSequence()
	if err != nil {
		idx.logger.Warnw("Failed to scan latest file, using metadata only",
			"error", err)
		actualMax = 0
	}

	// Take the maximum of both
	safeMax := metadataMax
	if actualMax > safeMax {
		safeMax = actualMax
		idx.logger.Warnw("Actual max sequence exceeds metadata",
			"metadataMax", metadataMax,
			"actualMax", actualMax)
	}

	// Apply safety margin
	safetyMargin := uint64(idx.config.SequenceSafetyMargin)
	if safetyMargin == 0 {
		safetyMargin = 100 // Default safety margin
	}

	recoveredSequence := safeMax + safetyMargin

	// Set global sequence
	atomic.StoreUint64(&idx.globalSequence, recoveredSequence)

	// Update stats
	idx.statsMutex.Lock()
	idx.stats.MaxSequence = safeMax
	idx.statsMutex.Unlock()

	idx.logger.Infow("Restored global sequence",
		"indexName", idx.config.IndexName,
		"metadataMax", metadataMax,
		"actualMax", actualMax,
		"safeMax", safeMax,
		"safetyMargin", safetyMargin,
		"recoveredSequence", recoveredSequence)

	return nil
}

// scanLatestFileForMaxSequence scans only the most recent entry file
// This is much faster than scanning all files (O(1 file) vs O(N files))
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

	// Get the most recent file (last in sorted list)
	latestFile := files[len(files)-1]

	// Scan it for max sequence
	maxSeq, err := idx.storage.ScanFileForMaxSequence(latestFile)
	if err != nil {
		return 0, fmt.Errorf("failed to scan file %s: %w", latestFile, err)
	}

	return maxSeq, nil
}

// computeMetadataChecksum computes a simple checksum for integrity
// Using FNV-1a hash for consistency with entry checksums
func computeMetadataChecksum(maxSequence uint64) uint32 {
	// Simple checksum: hash the max sequence
	return ComputeHash(fmt.Sprintf("meta:%d", maxSequence))
}
