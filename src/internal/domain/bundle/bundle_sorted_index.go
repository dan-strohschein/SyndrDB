/*
BUNDLE SORTED INDEX - Persistence and Initialization

This file provides persistence and initialization logic for the ShardedSortedIndex
that is defined in models.go. The ShardedSortedIndex maintains sorted DocumentIDs
across 64 shards for fast alphabetical position lookup during INSERT operations.

PURPOSE:
Eliminates stale pageID issues by ensuring INSERT calculates pageIDs the same way
LOAD does (alphabetical order). Previously, INSERT used insertion order while LOAD
used alphabetical order, causing 90%+ staleness rates in hash index pageID lookups.

PERSISTENCE FORMAT (binary):
  [4 bytes] Magic number: 0x53494458 ("SIDX" - Sorted InDeX)
  [4 bytes] Version: 1
  [4 bytes] Shard count: 64
  [4 bytes] Total document count (for validation)
  For each shard:
    [4 bytes] Document count in this shard
    For each document:
      [2 bytes] DocumentID length
      [N bytes] DocumentID bytes (UTF-8)

FILE LOCATION:
  {bundle_dir}/sorted_index.sidx

INITIALIZATION:
On bundle load, if sorted_index.sidx exists, load it. Otherwise, the index starts
empty and is populated as documents are inserted. For existing databases, users
should delete and recreate them, or run a manual rebuild.

This implements the persistence portion of the PageID Architecture Alignment Fix.
*/

package bundle

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/storage/bundlestore"
)

const (
	// SortedIndexMagic identifies the file format
	SortedIndexMagic uint32 = 0x53494458 // "SIDX"
	// SortedIndexVersion is the current format version
	SortedIndexVersion uint32 = 1
	// SortedIndexFileName is the filename for the persisted index
	SortedIndexFileName = "sorted_index.sidx"
)

// GetSortedIndexPath returns the full path for a bundle's sorted index file
func GetSortedIndexPath(databaseName, bundleName string) string {
	bundleDir := bundlestore.GetBundleDirectory(databaseName, bundleName)
	return filepath.Join(bundleDir, SortedIndexFileName)
}

// SaveSortedIndex persists a ShardedSortedIndex to disk in binary format.
// Thread-safe: acquires read locks on each shard during save.
func SaveSortedIndex(index *models.ShardedSortedIndex, databaseName, bundleName string) error {
	if index == nil {
		return fmt.Errorf("cannot save nil sorted index")
	}

	filePath := GetSortedIndexPath(databaseName, bundleName)

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for sorted index: %w", err)
	}

	// Write to temp file first, then atomic rename
	tempPath := filePath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create sorted index temp file: %w", err)
	}
	
	// Track whether we've successfully completed (to avoid cleanup on success)
	var success bool
	defer func() {
		if !success {
			file.Close()
			os.Remove(tempPath) // Clean up on error
		}
	}()

	writer := bufio.NewWriter(file)

	// Write header
	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header[0:4], SortedIndexMagic)
	binary.LittleEndian.PutUint32(header[4:8], SortedIndexVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(models.SortedIndexShards))
	binary.LittleEndian.PutUint32(header[12:16], index.TotalDocuments())
	if _, err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write sorted index header: %w", err)
	}

	// Write each shard
	for i := 0; i < models.SortedIndexShards; i++ {
		shard := &index.Shards[i]
		shard.Mu.RLock()

		// Write shard document count
		countBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(countBuf, uint32(len(shard.DocIDs)))
		if _, err := writer.Write(countBuf); err != nil {
			shard.Mu.RUnlock()
			return fmt.Errorf("failed to write shard %d count: %w", i, err)
		}

		// Write each DocumentID
		lenBuf := make([]byte, 2)
		for _, docID := range shard.DocIDs {
			binary.LittleEndian.PutUint16(lenBuf, uint16(len(docID)))
			if _, err := writer.Write(lenBuf); err != nil {
				shard.Mu.RUnlock()
				return fmt.Errorf("failed to write docID length in shard %d: %w", i, err)
			}
			if _, err := writer.WriteString(docID); err != nil {
				shard.Mu.RUnlock()
				return fmt.Errorf("failed to write docID in shard %d: %w", i, err)
			}
		}
		shard.Mu.RUnlock()
	}

	// Flush buffer
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush sorted index: %w", err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync sorted index: %w", err)
	}

	// Close file and check for errors
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close sorted index file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, filePath); err != nil {
		return fmt.Errorf("failed to rename sorted index temp file: %w", err)
	}

	// Mark success to prevent defer cleanup
	success = true
	return nil
}

// LoadSortedIndex loads a ShardedSortedIndex from disk.
// Returns a new empty index if the file doesn't exist.
// Returns an error if the file exists but is corrupt.
func LoadSortedIndex(databaseName, bundleName string) (*models.ShardedSortedIndex, error) {
	filePath := GetSortedIndexPath(databaseName, bundleName)

	file, err := os.Open(filePath)
	if os.IsNotExist(err) {
		// No persisted index - return new empty one
		return models.NewShardedSortedIndex(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open sorted index file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Read header
	header := make([]byte, 16)
	if _, err := reader.Read(header); err != nil {
		return nil, fmt.Errorf("failed to read sorted index header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != SortedIndexMagic {
		return nil, fmt.Errorf("invalid sorted index magic: got 0x%X, expected 0x%X", magic, SortedIndexMagic)
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version != SortedIndexVersion {
		return nil, fmt.Errorf("unsupported sorted index version: %d", version)
	}

	shardCount := binary.LittleEndian.Uint32(header[8:12])
	if shardCount != uint32(models.SortedIndexShards) {
		return nil, fmt.Errorf("shard count mismatch: file has %d, expected %d", shardCount, models.SortedIndexShards)
	}

	expectedTotal := binary.LittleEndian.Uint32(header[12:16])

	// Read each shard
	index := models.NewShardedSortedIndex()
	var actualTotal uint32 = 0

	countBuf := make([]byte, 4)
	lenBuf := make([]byte, 2)

	for i := 0; i < models.SortedIndexShards; i++ {
		// Read shard document count
		if _, err := reader.Read(countBuf); err != nil {
			return nil, fmt.Errorf("failed to read shard %d count: %w", i, err)
		}
		docCount := binary.LittleEndian.Uint32(countBuf)

		// Pre-allocate slice
		shard := &index.Shards[i]
		shard.DocIDs = make([]string, docCount)

		// Read each DocumentID
		for j := uint32(0); j < docCount; j++ {
			if _, err := reader.Read(lenBuf); err != nil {
				return nil, fmt.Errorf("failed to read docID length in shard %d: %w", i, err)
			}
			docIDLen := binary.LittleEndian.Uint16(lenBuf)

			docIDBytes := make([]byte, docIDLen)
			if _, err := reader.Read(docIDBytes); err != nil {
				return nil, fmt.Errorf("failed to read docID in shard %d: %w", i, err)
			}
			shard.DocIDs[j] = string(docIDBytes)
		}

		// Update atomic count
		index.ShardCounts[i].Store(docCount)
		actualTotal += docCount
	}

	// Validate total count
	if actualTotal != expectedTotal {
		return nil, fmt.Errorf("document count mismatch: read %d, expected %d", actualTotal, expectedTotal)
	}

	return index, nil
}

// DeleteSortedIndex removes the sorted index file for a bundle.
// Used when dropping a bundle.
func DeleteSortedIndex(databaseName, bundleName string) error {
	filePath := GetSortedIndexPath(databaseName, bundleName)
	err := os.Remove(filePath)
	if os.IsNotExist(err) {
		return nil // Already deleted
	}
	return err
}

// InitializeBundleSortedIndex ensures a bundle has a SortedIndex.
// Loads from disk if available, otherwise creates empty index.
// This should be called when a bundle is loaded or created.
func InitializeBundleSortedIndex(bundle *models.Bundle) error {
	if bundle == nil {
		return fmt.Errorf("cannot initialize sorted index for nil bundle")
	}
	if bundle.Database == nil {
		// Set fallback empty index and return error
		bundle.SortedIndex = models.NewShardedSortedIndex()
		return fmt.Errorf("cannot initialize sorted index: bundle has no database reference")
	}

	index, err := LoadSortedIndex(bundle.Database.Name, bundle.Name)
	if err != nil {
		// Set fallback empty index and return error
		bundle.SortedIndex = models.NewShardedSortedIndex()
		return fmt.Errorf("failed to load sorted index for bundle %s: %w", bundle.Name, err)
	}

	bundle.SortedIndex = index
	return nil
}

// PersistBundleSortedIndex saves a bundle's SortedIndex to disk.
// Should be called periodically and on graceful shutdown.
func PersistBundleSortedIndex(bundle *models.Bundle) error {
	if bundle == nil || bundle.SortedIndex == nil {
		return nil // Nothing to persist
	}
	if bundle.Database == nil {
		return fmt.Errorf("cannot persist sorted index: bundle has no database reference")
	}

	return SaveSortedIndex(bundle.SortedIndex, bundle.Database.Name, bundle.Name)
}
