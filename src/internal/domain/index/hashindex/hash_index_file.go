package hashindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"syndrdb/src/internal/storage/hash"
	"time"

	"go.uber.org/zap"
)

// createEmptyHashIndex creates a new empty hash index file
func CreateEmptyHashIndex(filePath string, indexField hash.IndexField, fillFactor uint32,
	logger *zap.SugaredLogger) (*HashIndex, *hash.HashIndexFile, error) {

	// Create the file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Initialize hash indexFile
	indexFile := &hash.HashIndexFile{
		FilePath:     filePath,
		File:         file,
		PageCache:    make(map[uint32]*hash.HashIndexPage),
		CacheSize:    0,
		MaxCacheSize: 100, // Cache up to 100 pages
		Logger:       logger,
		Dirty:        true,
	}

	// Initialize metadata
	indexFile.Metadata = hash.HashIndexMetadata{
		MaxBucket:     InitialBucketCount - 1, // 0-based
		HighMask:      0x3,                    // 11 in binary (for 4 buckets)
		LowMask:       0,                      // Starts at 0
		FillFactor:    fillFactor,
		NumTuples:     0,
		BitmapPages:   0,
		OverflowPages: 0,
		IndexField:    indexField.FieldName,
		IsUnique:      indexField.IsUnique,
		Seed:          generateSeed(), //Use cryptographic random seed
		Created:       time.Now(),
	}

	// Create meta page
	metaPage := &hash.HashIndexPage{
		PageType:  HashMetaPage,
		PageNum:   0,
		ItemCount: 0,
	}

	// Write metadata to the meta page
	if err := indexFile.WriteMetaPage(metaPage); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to write meta page: %w", err)
	}

	// Create initial bucket pages
	for i := uint32(0); i < InitialBucketCount; i++ {
		bucketPage := &hash.HashIndexPage{
			PageType:  HashBucketPage,
			PageNum:   i + 1, // Page numbers start at 1 (0 is meta)
			ItemCount: 0,
			FreeSpace: HashPageSize - 32, // Approximate header size
			Items:     make([]hash.HashIndexItem, 0),
		}

		if err := indexFile.WritePage(i+1, bucketPage); err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("failed to write bucket page: %w", err)
		}
	}

	logger.Infof("Created empty hash index with %d initial buckets", InitialBucketCount)

	index := CopyIndexFileToIndex(indexFile)

	return index, indexFile, nil
}

// openHashIndex opens an existing hash index
func OpenHashIndex(path string, cacheSize int, logger *zap.SugaredLogger) (*HashIndex, *hash.HashIndexFile, error) {
	// Open the file
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open hash index file: %w", err)
	}

	// Create the indexFile object
	indexFile := &hash.HashIndexFile{
		FilePath:     path,
		File:         file,
		PageCache:    make(map[uint32]*hash.HashIndexPage),
		CacheSize:    0,
		MaxCacheSize: cacheSize,
		Logger:       logger,
	}

	// Read the meta page
	metaPage, err := indexFile.ReadPage(0)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to read meta page: %w", err)
	}

	// The meta page should have an entry with our metadata
	if len(metaPage.Items) < 1 || !bytes.Equal(metaPage.Items[0].Key, []byte("metadata")) {
		file.Close()
		return nil, nil, fmt.Errorf("invalid meta page format")
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
		return nil, nil, fmt.Errorf("invalid metadata marker")
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

	metadata, err := hash.DeserializeHashMetadata(metaPage.Items[0].Value)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to deserialize metadata: %w", err)
	}

	indexFile.Metadata = *metadata

	index := CopyIndexFileToIndex(indexFile)

	return index, indexFile, nil
}

func CopyIndexFileToIndex(source *hash.HashIndexFile) *HashIndex {
	index := &HashIndex{
		FilePath:     source.FilePath,
		File:         source.File,
		PageCache:    source.PageCache, //make(map[uint32]*hash.HashIndexPage),
		CacheSize:    source.CacheSize,
		MaxCacheSize: source.MaxCacheSize,
		Logger:       source.Logger,
		Dirty:        source.Dirty,
	}

	return index
}
