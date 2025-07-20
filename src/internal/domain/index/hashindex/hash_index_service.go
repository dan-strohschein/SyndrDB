package hashindex

import (
	"fmt"
	"os"
	"path/filepath"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/storage/hash"

	"go.uber.org/zap"
)

// TODO Note that a lot of these functions feel like they should bne in the hash index file object, not the hash index object.

// NewHashService creates a new hash indexing service
func NewHashService(dataDir string, maxMemorySize int64, logger *zap.SugaredLogger) *HashService {
	return &HashService{
		DataDir:       dataDir,
		MaxMemorySize: maxMemorySize,
		Logger:        logger,
	}
}

// SearchHashIndex searches the hash index for a document with the given key
func (hs *HashService) SearchHashIndex(indexName string, key interface{}, indexField hash.IndexField) (string, error) {
	// Open the index file
	indexPath := filepath.Join(hs.DataDir, indexName+".hidx")
	index, indexFile, err := OpenHashIndex(indexPath, 100, hs.Logger) // Cache up to 100 pages
	if err != nil {
		return "", fmt.Errorf("failed to open hash index: %w", err)
	}
	defer indexFile.Close()

	// Encode key in the same format used for indexing
	encodedKey, _, err := encodeFieldValue(key, indexField)
	if err != nil {
		return "", fmt.Errorf("failed to encode key: %w", err)
	}

	// Search for the key
	result, err := index.Find(indexFile, encodedKey)
	if err != nil {
		return "", fmt.Errorf("hash index search failed: %w", err)
	}

	if result == nil {
		return "", nil // Not found
	}

	return result.DocID, nil
}

// ListHashIndexes lists all hash indexes for a bundle
func (hs *HashService) ListHashIndexes(bundleID string) ([]string, error) {
	pattern := filepath.Join(hs.DataDir, bundleID+"_*_hidx.hidx")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list hash indexes: %w", err)
	}

	indexNames := make([]string, 0, len(matches))
	for _, path := range matches {
		name := filepath.Base(path)
		name = name[:len(name)-5] // Remove ".hidx" extension
		indexNames = append(indexNames, name)
	}

	return indexNames, nil
}

// DropHashIndex removes a hash index
func (hs *HashService) DropHashIndex(indexName string) error {
	indexPath := filepath.Join(hs.DataDir, indexName+".hidx")
	return os.Remove(indexPath)
}

// CreateHashIndex creates a new hash index for the specified field
func (hs *HashService) CreateHashIndex(bundle *models.Bundle, indexField hash.IndexField) (string, error) {
	// Generate a unique index name
	indexName := fmt.Sprintf("%s_%s_hidx", bundle.BundleID, indexField.FieldName)
	indexName = CleanFileName(indexName)

	hs.Logger.Infof("Creating hash index %s on field %s", indexName, indexField.FieldName)

	// Create the index file
	indexPath := filepath.Join(hs.DataDir, indexName+".hidx")
	index, hif, err := CreateEmptyHashIndex(indexPath, indexField, DefaultFillFactor, hs.Logger)
	if err != nil {
		return "", fmt.Errorf("failed to create hash index file: %w", err)
	}

	// Scan the bundle and extract values to index
	tuples, err := hs.ScanBundleForHashIndex(bundle, indexField)
	if err != nil {
		hif.Close()
		os.Remove(indexPath)
		return "", fmt.Errorf("failed to scan bundle: %w", err)
	}

	// Insert all tuples into the hash index
	for _, tuple := range tuples {
		if err := index.Insert(hif, tuple.Key, tuple.DocID, tuple.TID); err != nil {
			hif.Close()
			return "", fmt.Errorf("failed to insert tuple: %w", err)
		}
	}

	// Close and finalize the index
	if err := hif.Close(); err != nil {
		return "", fmt.Errorf("failed to close index: %w", err)
	}

	hs.Logger.Infof("Successfully created hash index %s with %d entries",
		indexName, len(tuples))

	return indexName, nil
}

/*
Improvements to be made later:

WAL logging - For crash recovery
Better cache eviction - Full LRU implementation
Concurrency control - Fine-grained locking
Bitmap pages - For space management
Better overflow handling - Currently, overflow chains could grow long

*/
