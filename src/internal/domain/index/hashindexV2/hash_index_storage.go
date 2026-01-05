/*
HASH INDEX STORAGE SYSTEM

This file implements the file-based storage for the hash index with support
for both binary and ASCII formats. The ASCII format is used when debug mode
is enabled to allow human inspection of the index contents.

FILE FORMAT:
The index file consists of multiple sections:
1. Header: Contains file format version and basic info
2. Metadata: Index configuration and statistics
3. Buckets: Primary hash buckets containing items
4. Overflow Pages: Additional pages for bucket overflow

ASCII FORMAT (Debug Mode):
When debug mode is enabled, the file is written in a human-readable format:
- Comments start with '#'
- Sections are clearly labeled
- Items are formatted as key=value pairs
- Metadata is written as name=value pairs

BINARY FORMAT (Production Mode):
In production, data is stored in a compact binary format for efficiency:
- Fixed-size headers with version information
- Little-endian encoding for cross-platform compatibility
- Efficient packing of hash items and metadata

CONCURRENCY:
Storage operations are designed to be thread-safe when used with proper
locking at the higher level (implemented in hash_index.go).
*/

package hashindexV2

// DEPRECATED: This file is part of the old hashindexV2 (non-LSM) implementation.
// Use hashindexV3 (LSM-style) instead. See hash_index_api.go for details.

import (
	"fmt"
	"os"
	"syndrdb/src/pkg/common"

	"go.uber.org/zap"
)

// HashIndexStorage provides storage operations for the hash index
type HashIndexStorage struct {
	fileManager *FileManager
	logger      *zap.SugaredLogger
}

// HashItem represents a single key-value pair in the hash index
// type HashItem struct {
// 	Key       string
// 	Value     string
// 	HashValue uint32
// }

// NewHashIndexStorage creates a new storage instance
// Parameters:
//   - filePath: Path to the index file
//   - debugMode: Whether to use ASCII format for debugging
//   - logger: Logger for debug/error messages
//
// Returns:
//   - *HashIndexStorage: The storage instance
//   - error: Any error that occurred during creation
//
// NewHashIndexStorage creates a new storage instance
func NewHashIndexStorage(fileManager *FileManager, logger *zap.SugaredLogger) *HashIndexStorage {
	return &HashIndexStorage{
		fileManager: fileManager,
		logger:      logger,
	}
}

// SaveMetadata writes the index metadata to the file
// Parameters:
//   - metadata: The metadata to save
//
// Returns:
//   - error: Any error that occurred during saving
func (s *HashIndexStorage) SaveMetadata(metadata *HashIndexMetadata) error {
	return s.fileManager.WriteMetadata(metadata)
}

// saveMetadataASCII saves metadata in human-readable ASCII format
// Parameters:
//   - metadata: The metadata to save
//
// Returns:
//   - error: Any error that occurred during saving
// func (s *HashIndexStorage) saveMetadataASCII(metadata *HashIndexMetadata, file *os.File) error {
// 	writer := bufio.NewWriter(file)

// 	// Write header comment
// 	fmt.Fprintf(writer, "# SyndrDB Hash Index File\n")
// 	fmt.Fprintf(writer, "# Created: %s\n", metadata.Created.Format(time.RFC3339))
// 	fmt.Fprintf(writer, "# Last Modified: %s\n", metadata.LastModified.Format(time.RFC3339))
// 	fmt.Fprintf(writer, "\n")

// 	// Write metadata section
// 	fmt.Fprintf(writer, "[METADATA]\n")
// 	fmt.Fprintf(writer, "Version=%d\n", metadata.Version)
// 	fmt.Fprintf(writer, "PageSize=%d\n", metadata.PageSize)
// 	fmt.Fprintf(writer, "MaxBucket=%d\n", metadata.MaxBucket)
// 	fmt.Fprintf(writer, "HighMask=%d\n", metadata.HighMask)
// 	fmt.Fprintf(writer, "LowMask=%d\n", metadata.LowMask)
// 	fmt.Fprintf(writer, "FillFactor=%d\n", metadata.FillFactor)
// 	fmt.Fprintf(writer, "NumTuples=%d\n", metadata.TotalRecords)
// 	fmt.Fprintf(writer, "NumBuckets=%d\n", metadata.BucketCount)
// 	fmt.Fprintf(writer, "NumOverflows=%d\n", metadata.NumOverflows)
// 	fmt.Fprintf(writer, "IndexField=%s\n", metadata.IndexField)
// 	fmt.Fprintf(writer, "IsUnique=%t\n", metadata.IsUnique)
// 	fmt.Fprintf(writer, "HashSeed=%d\n", metadata.HashSeed)
// 	fmt.Fprintf(writer, "Created=%s\n", metadata.Created.Format(time.RFC3339))
// 	fmt.Fprintf(writer, "LastModified=%s\n", metadata.LastModified.Format(time.RFC3339))
// 	fmt.Fprintf(writer, "\n")

// 	return writer.Flush()
// }

// saveMetadataBinary saves metadata in compact binary format
// Parameters:
//   - metadata: The metadata to save
//
// Returns:
//   - error: Any error that occurred during saving
// func (s *HashIndexStorage) saveMetadataBinary(metadata *HashIndexMetadata, file *os.File) error {
// 	// Implementation would write binary data
// 	// For now, using ASCII format even in "binary" mode for simplicity
// 	return s.saveMetadataASCII(metadata, file)
// }

// LoadMetadata reads the index metadata from the file
// Returns:
//   - *HashIndexMetadata: The loaded metadata
//   - error: Any error that occurred during loading
func (s *HashIndexStorage) LoadMetadata() (*HashIndexMetadata, error) {
	return s.fileManager.ReadMetadata()
}

// loadMetadataASCII loads metadata from ASCII format
// Returns:
//   - *HashIndexMetadata: The loaded metadata
//   - error: Any error that occurred during loading
// func (s *HashIndexStorage) loadMetadataASCII(file *os.File) (*HashIndexMetadata, error) {
// 	scanner := bufio.NewScanner(file)
// 	metadata := &HashIndexMetadata{}

// 	inMetadataSection := false
// 	for scanner.Scan() {
// 		line := strings.TrimSpace(scanner.Text())

// 		// Skip comments and empty lines
// 		if strings.HasPrefix(line, "#") || line == "" {
// 			continue
// 		}

// 		// Check for section header
// 		if line == "[METADATA]" {
// 			inMetadataSection = true
// 			continue
// 		}

// 		// Parse metadata fields
// 		if inMetadataSection && strings.Contains(line, "=") {
// 			parts := strings.SplitN(line, "=", 2)
// 			if len(parts) != 2 {
// 				continue
// 			}

// 			key := strings.TrimSpace(parts[0])
// 			value := strings.TrimSpace(parts[1])

// 			err := s.parseMetadataField(metadata, key, value)
// 			if err != nil {
// 				return nil, fmt.Errorf("failed to parse metadata field %s: %w", key, err)
// 			}
// 		}

// 		// Stop after metadata section
// 		if inMetadataSection && strings.HasPrefix(line, "[") && line != "[METADATA]" {
// 			break
// 		}
// 	}

// 	if err := scanner.Err(); err != nil {
// 		return nil, fmt.Errorf("failed to read metadata: %w", err)
// 	}

// 	return metadata, nil
// }

// parseMetadataField parses a single metadata field from string
// Parameters:
//   - metadata: The metadata object to populate
//   - key: The field name
//   - value: The field value as string
//
// Returns:
//   - error: Any error that occurred during parsing
// func (s *HashIndexStorage) parseMetadataField(metadata *HashIndexMetadata, key, value string) error {
// 	switch key {
// 	case "Version":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.Version = uint32(val)
// 	case "PageSize":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.PageSize = uint32(val)
// 	case "MaxBucket":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.MaxBucket = uint32(val)
// 	case "HighMask":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.HighMask = uint32(val)
// 	case "LowMask":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.LowMask = uint32(val)
// 	case "FillFactor":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.FillFactor = uint32(val)
// 	case "NumTuples":
// 		val, err := strconv.ParseUint(value, 10, 64)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.TotalRecords = val
// 	case "BucketCount":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.BucketCount = uint32(val)
// 	case "NumOverflows":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.NumOverflows = uint32(val)
// 	case "IndexField":
// 		metadata.IndexField = value
// 	case "IsUnique":
// 		val, err := strconv.ParseBool(value)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.IsUnique = val
// 	case "HashSeed":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.HashSeed = uint32(val)
// 	case "Created":
// 		val, err := time.Parse(time.RFC3339, value)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.Created = val
// 	case "LastModified":
// 		val, err := time.Parse(time.RFC3339, value)
// 		if err != nil {
// 			return err
// 		}
// 		metadata.LastModified = val
// 	}

// 	return nil
// }

// loadMetadataBinary loads metadata from binary format
// Returns:
//   - *HashIndexMetadata: The loaded metadata
//   - error: Any error that occurred during loading
// func (s *HashIndexStorage) loadMetadataBinary(file *os.File) (*HashIndexMetadata, error) {
// 	// For now, using ASCII format even in "binary" mode
// 	return s.loadMetadataASCII(file)
// }

// SaveBucket writes a bucket page to the file
// Parameters:
//   - bucket: The bucket to save
//
// Returns:
//   - error: Any error that occurred during saving
func (s *HashIndexStorage) SaveBucket(bucket *BucketPage) error {
	pageNum := bucketNumberToPageNumber(bucket.BucketNumber)
	return s.fileManager.WritePage(pageNum, bucket)
}

// saveBucketASCII saves a bucket in ASCII format
// Parameters:
//   - bucket: The bucket to save
//
// Returns:
//   - error: Any error that occurred during saving
// func (s *HashIndexStorage) saveBucketASCII(bucket *BucketPage, file *os.File) error {
// 	// Seek to end of file to append bucket data
// 	_, err := file.Seek(0, 2)
// 	if err != nil {
// 		return fmt.Errorf("failed to seek to end: %w", err)
// 	}

// 	writer := bufio.NewWriter(file)

// 	fmt.Fprintf(writer, "[BUCKET_%d]\n", bucket.BucketNum)
// 	fmt.Fprintf(writer, "ItemCount=%d\n", bucket.ItemCount)
// 	fmt.Fprintf(writer, "OverflowPage=%d\n", bucket.OverflowPage)

// 	for i, item := range bucket.Items {
// 		fmt.Fprintf(writer, "Item_%d_Key=%s\n", i, item.Key)
// 		fmt.Fprintf(writer, "Item_%d_Value=%s\n", i, item.Value)
// 		fmt.Fprintf(writer, "Item_%d_Hash=%d\n", i, item.HashValue)
// 	}

// 	fmt.Fprintf(writer, "\n")
// 	return writer.Flush()
// }

// saveBucketBinary saves a bucket in binary format
// Parameters:
//   - bucket: The bucket to save
//
// Returns:
//   - error: Any error that occurred during saving
// func (s *HashIndexStorage) saveBucketBinary(bucket *BucketPage, file *os.File) error {
// 	// For now, using ASCII format even in "binary" mode
// 	return s.saveBucketASCII(bucket, file)
// }

// LoadBucket reads a bucket page from the file
// Parameters:
//   - bucketNum: The bucket number to load
//
// Returns:
//   - *BucketPage: The loaded bucket
//   - error: Any error that occurred during loading
func (s *HashIndexStorage) LoadBucket(bucketNum uint32) (*BucketPage, error) {
	pageNum := bucketNumberToPageNumber(bucketNum)
	pageData, err := s.fileManager.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}

	bucket, ok := pageData.(*BucketPage)
	if !ok {
		return nil, fmt.Errorf("page is not a bucket page")
	}

	return bucket, nil
}

// loadBucketASCII loads a bucket from ASCII format
// Parameters:
//   - bucketNum: The bucket number to load
//
// Returns:
//   - *BucketPage: The loaded bucket
//   - error: Any error that occurred during loading
// func (s *HashIndexStorage) loadBucketASCII(bucketNum uint32, file *os.File) (*BucketPage, error) {
// 	// Seek to beginning of file
// 	_, err := file.Seek(0, 0)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to seek to beginning: %w", err)
// 	}

// 	scanner := bufio.NewScanner(file)
// 	bucket := &BucketPage{
// 		BucketNum: bucketNum,
// 		Items:     make([]*IndexRecord, 0),
// 	}

// 	sectionName := fmt.Sprintf("[BUCKET_%d]", bucketNum)
// 	inBucketSection := false
// 	currentItem := &IndexRecord{}
// 	itemIndex := -1

// 	for scanner.Scan() {
// 		line := strings.TrimSpace(scanner.Text())

// 		// Skip comments and empty lines
// 		if strings.HasPrefix(line, "#") || line == "" {
// 			continue
// 		}

// 		// Check for our bucket section
// 		if line == sectionName {
// 			inBucketSection = true
// 			continue
// 		}

// 		// Stop if we hit another section
// 		if inBucketSection && strings.HasPrefix(line, "[") && line != sectionName {
// 			break
// 		}

// 		// Parse bucket fields
// 		if inBucketSection && strings.Contains(line, "=") {
// 			parts := strings.SplitN(line, "=", 2)
// 			if len(parts) != 2 {
// 				continue
// 			}

// 			key := strings.TrimSpace(parts[0])
// 			value := strings.TrimSpace(parts[1])

// 			if key == "ItemCount" {
// 				val, err := strconv.ParseUint(value, 10, 32)
// 				if err != nil {
// 					return nil, err
// 				}
// 				bucket.ItemCount = uint32(val)
// 			} else if key == "OverflowPage" {
// 				val, err := strconv.ParseUint(value, 10, 32)
// 				if err != nil {
// 					return nil, err
// 				}
// 				bucket.OverflowPage = uint32(val)
// 			} else if strings.HasPrefix(key, "Item_") {
// 				// Parse item fields
// 				err := s.parseItemField(bucket, key, value, &currentItem, &itemIndex)
// 				if err != nil {
// 					return nil, err
// 				}
// 			}
// 		}
// 	}

// 	if err := scanner.Err(); err != nil {
// 		return nil, fmt.Errorf("failed to read bucket: %w", err)
// 	}

// 	// Add the last item if it exists
// 	if itemIndex >= 0 && currentItem.Key != "" {
// 		bucket.Items = append(bucket.Items, currentItem)
// 	}

// 	return bucket, nil
// }

// parseItemField parses item fields from ASCII format
// Parameters:
//   - bucket: The bucket being populated
//   - key: The field key
//   - value: The field value
//   - currentItem: The current item being built
//   - itemIndex: The current item index
//
// Returns:
//   - error: Any error that occurred during parsing
// func (s *HashIndexStorage) parseItemField(bucket *BucketPage, key, value string, currentItem **IndexRecord, itemIndex *int) error {
// 	// Extract item index from key (e.g., "Item_0_Key" -> 0)
// 	parts := strings.Split(key, "_")
// 	if len(parts) < 3 {
// 		return nil
// 	}

// 	idx, err := strconv.Atoi(parts[1])
// 	if err != nil {
// 		return err
// 	}

// 	// If this is a new item, save the previous one
// 	if idx != *itemIndex {
// 		if *itemIndex >= 0 && (*currentItem).Key != "" {
// 			bucket.Items = append(bucket.Items, *currentItem)
// 		}
// 		*currentItem = &IndexRecord{}
// 		*itemIndex = idx
// 	}

// 	// Parse the field
// 	fieldType := parts[2]
// 	switch fieldType {
// 	case "Key":
// 		(*currentItem).Key = value
// 	case "Value":
// 		(*currentItem).Value = value
// 	case "Hash":
// 		val, err := strconv.ParseUint(value, 10, 32)
// 		if err != nil {
// 			return err
// 		}
// 		(*currentItem).HashValue = uint32(val)
// 	}

// 	return nil
// }

// loadBucketBinary loads a bucket from binary format
// Parameters:
//   - bucketNum: The bucket number to load
//
// Returns:
//   - *BucketPage: The loaded bucket
//   - error: Any error that occurred during loading
// func (s *HashIndexStorage) loadBucketBinary(bucketNum uint32, file *os.File) (*BucketPage, error) {
// 	// For now, using ASCII format even in "binary" mode
// 	return s.loadBucketASCII(bucketNum, file)
// }

// SaveOverflowPage writes an overflow page to the file
// Parameters:
//   - overflow: The overflow page to save
//
// Returns:
//   - error: Any error that occurred during saving
func (s *HashIndexStorage) SaveOverflowPage(overflow *OverflowPage) error {
	// Implementation similar to SaveBucket but for overflow pages
	return nil // Placeholder
}

// LoadOverflowPage reads an overflow page from the file
// Parameters:
//   - pageNum: The page number to load
//
// Returns:
//   - *OverflowPage: The loaded overflow page
//   - error: Any error that occurred during loading
func (s *HashIndexStorage) LoadOverflowPage(pageNum uint32) (*OverflowPage, error) {
	// Implementation similar to LoadBucket but for overflow pages
	return nil, nil // Placeholder
}

// DeleteOverflowPage removes an overflow page from the file
// Parameters:
//   - pageNum: The page number to delete
//
// Returns:
//   - error: Any error that occurred during deletion
func (s *HashIndexStorage) DeleteOverflowPage(pageNum uint32) error {
	// Implementation to mark overflow page as deleted
	return nil // Placeholder
}

// Close closes the storage and flushes any pending writes
// Returns:
//   - error: Any error that occurred during closing
func (s *HashIndexStorage) Close(file *os.File) error {
	if file != nil {
		err := common.Fdatasync(file)
		if err != nil {
			s.logger.Errorf("Failed to sync file: %v", err)
		}
		return file.Close()
	}
	return nil
}
