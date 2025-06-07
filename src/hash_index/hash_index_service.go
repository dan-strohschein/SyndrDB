package hashindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syndrdb/src/engine"
	"time"

	"go.uber.org/zap"
)

// NewHashService creates a new hash indexing service
func NewHashService(dataDir string, maxMemorySize int64, logger *zap.SugaredLogger) *HashService {
	return &HashService{
		dataDir:       dataDir,
		maxMemorySize: maxMemorySize,
		logger:        logger,
	}
}

// CreateHashIndex creates a new hash index for the specified field
func (hs *HashService) CreateHashIndex(bundle *engine.Bundle, indexField IndexField) (string, error) {
	// Generate a unique index name
	indexName := fmt.Sprintf("%s_%s_hidx", bundle.BundleID, indexField.FieldName)
	indexName = cleanFileName(indexName)

	hs.logger.Infof("Creating hash index %s on field %s", indexName, indexField.FieldName)

	// Create the index file
	indexPath := filepath.Join(hs.dataDir, indexName+".hidx")
	index, err := createEmptyHashIndex(indexPath, indexField, DefaultFillFactor, hs.logger)
	if err != nil {
		return "", fmt.Errorf("failed to create hash index file: %w", err)
	}

	// Scan the bundle and extract values to index
	tuples, err := hs.scanBundleForHashIndex(bundle, indexField)
	if err != nil {
		index.Close()
		os.Remove(indexPath)
		return "", fmt.Errorf("failed to scan bundle: %w", err)
	}

	// Insert all tuples into the hash index
	for _, tuple := range tuples {
		if err := index.Insert(tuple.Key, tuple.DocID, tuple.TID); err != nil {
			index.Close()
			return "", fmt.Errorf("failed to insert tuple: %w", err)
		}
	}

	// Close and finalize the index
	if err := index.Close(); err != nil {
		return "", fmt.Errorf("failed to close index: %w", err)
	}

	hs.logger.Infof("Successfully created hash index %s with %d entries",
		indexName, len(tuples))

	return indexName, nil
}

// SearchHashIndex searches the hash index for a document with the given key
func (hs *HashService) SearchHashIndex(indexName string, key interface{}, indexField IndexField) (string, error) {
	// Open the index file
	indexPath := filepath.Join(hs.dataDir, indexName+".hidx")
	index, err := openHashIndex(indexPath, 100, hs.logger) // Cache up to 100 pages
	if err != nil {
		return "", fmt.Errorf("failed to open hash index: %w", err)
	}
	defer index.Close()

	// Encode key in the same format used for indexing
	encodedKey, _, err := encodeFieldValue(key, indexField)
	if err != nil {
		return "", fmt.Errorf("failed to encode key: %w", err)
	}

	// Search for the key
	result, err := index.Find(encodedKey)
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
	pattern := filepath.Join(hs.dataDir, bundleID+"_*_hidx.hidx")
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
	indexPath := filepath.Join(hs.dataDir, indexName+".hidx")
	return os.Remove(indexPath)
}

// openHashIndex opens an existing hash index
func openHashIndex(path string, cacheSize int, logger *zap.SugaredLogger) (*HashIndex, error) {
	// Open the file
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open hash index file: %w", err)
	}

	// Create the index object
	index := &HashIndex{
		filePath:     path,
		file:         file,
		pageCache:    make(map[uint32]*HashIndexPage),
		cacheSize:    0,
		maxCacheSize: cacheSize,
		logger:       logger,
	}

	// Read the meta page
	metaPage, err := index.readPage(0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read meta page: %w", err)
	}

	// The meta page should have an entry with our metadata
	if len(metaPage.Items) < 1 || !bytes.Equal(metaPage.Items[0].Key, []byte("metadata")) {
		file.Close()
		return nil, fmt.Errorf("invalid meta page format")
	}

	// Read metadata marker
	offset := int64(16) // Skip page header

	// Read timestamp length and skip it
	var timeLen uint32
	timeData := make([]byte, 4)
	file.ReadAt(timeData, offset)
	timeLen = binary.LittleEndian.Uint32(timeData)
	offset += 4 + int64(timeLen)

	// Read marker
	markerLenData := make([]byte, 4)
	file.ReadAt(markerLenData, offset)
	markerLen := binary.LittleEndian.Uint32(markerLenData)
	offset += 4

	markerData := make([]byte, markerLen)
	file.ReadAt(markerData, offset)
	if string(markerData) != "METADATA" {
		file.Close()
		return nil, fmt.Errorf("invalid metadata marker")
	}
	offset += int64(markerLen)

	// Read metadata length
	metaLenData := make([]byte, 4)
	file.ReadAt(metaLenData, offset)
	metaLen := binary.LittleEndian.Uint32(metaLenData)
	offset += 4

	// Read metadata
	metaData := make([]byte, metaLen)
	file.ReadAt(metaData, offset)

	// metadata, err := deserializeHashMetadata(metaData)
	// if err != nil {
	// 	file.Close()
	// 	return nil, fmt.Errorf("failed to deserialize metadata: %w", err)
	// }

	metadata, err := deserializeHashMetadata(metaPage.Items[0].Value)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to deserialize metadata: %w", err)
	}

	index.metadata = *metadata

	return index, nil
}

// scanBundleForHashIndex scans a bundle and extracts values for hash indexing
func (hs *HashService) scanBundleForHashIndex(bundle *engine.Bundle, indexField IndexField) ([]IndexTuple, error) {
	var tuples []IndexTuple
	var tid uint64 = 1 // Start TIDs at 1

	// Check if field definition exists
	_, fieldExists := bundle.DocumentStructure.FieldDefinitions[indexField.FieldName]
	if !fieldExists {
		return nil, fmt.Errorf("field %s not defined in bundle structure", indexField.FieldName)
	}

	// Scan each document in the bundle
	for docID, doc := range bundle.Documents {
		// Get the field from the document
		field, exists := doc.Fields[indexField.FieldName]
		if !exists {
			// Skip documents that don't have this field
			continue
		}

		// Extract and encode the field value
		key, keyString, err := encodeFieldValue(field.Value, indexField)
		if err != nil {
			hs.logger.Warnf("Failed to encode field %s for document %s: %v",
				indexField.FieldName, docID, err)
			continue
		}

		// Create index tuple
		tuple := IndexTuple{
			Key:       key,
			DocID:     docID,
			BundleID:  bundle.BundleID,
			TID:       tid,
			KeyString: keyString,
		}

		tuples = append(tuples, tuple)
		tid++
	}

	return tuples, nil
}

// createEmptyHashIndex creates a new empty hash index file
func createEmptyHashIndex(filePath string, indexField IndexField, fillFactor uint32,
	logger *zap.SugaredLogger) (*HashIndex, error) {

	// Create the file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Initialize hash index
	index := &HashIndex{
		filePath:     filePath,
		file:         file,
		pageCache:    make(map[uint32]*HashIndexPage),
		cacheSize:    0,
		maxCacheSize: 100, // Cache up to 100 pages
		logger:       logger,
		dirty:        true,
	}

	// Initialize metadata
	index.metadata = HashIndexMetadata{
		MaxBucket:     InitialBucketCount - 1, // 0-based
		HighMask:      0x3,                    // 11 in binary (for 4 buckets)
		LowMask:       0,                      // Starts at 0
		FillFactor:    fillFactor,
		NumTuples:     0,
		BitmapPages:   0,
		OverflowPages: 0,
		IndexField:    indexField.FieldName,
		IsUnique:      indexField.IsUnique,
		Created:       time.Now(),
	}

	// Create meta page
	metaPage := &HashIndexPage{
		PageType:  HashMetaPage,
		PageNum:   0,
		ItemCount: 0,
	}

	// Write metadata to the meta page
	if err := index.writeMetaPage(metaPage); err != nil {
		file.Close()
		return nil, err
	}

	// Create initial bucket pages
	for i := uint32(0); i < InitialBucketCount; i++ {
		bucketPage := &HashIndexPage{
			PageType:  HashBucketPage,
			PageNum:   i + 1, // Page numbers start at 1 (0 is meta)
			ItemCount: 0,
			FreeSpace: HashPageSize - 32, // Approximate header size
			Items:     make([]HashIndexItem, 0),
		}

		if err := index.writePage(i+1, bucketPage); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to write bucket page: %w", err)
		}
	}

	logger.Infof("Created empty hash index with %d initial buckets", InitialBucketCount)

	return index, nil
}

// cleanFileName sanitizes a string for use as a filename
func cleanFileName(name string) string {
	// Replace characters that might be problematic in filenames
	return strings.ReplaceAll(name, "-", "_")
}

/*
Improvements to be made later:

WAL logging - For crash recovery
Better cache eviction - Full LRU implementation
Concurrency control - Fine-grained locking
Bitmap pages - For space management
Better overflow handling - Currently, overflow chains could grow long

*/
