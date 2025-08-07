/*
HASH INDEX FILE MANAGER

This file implements file I/O operations for the hash index system.
It handles reading and writing pages to disk, managing the file format,
and providing ASCII debugging output when enabled.

ALGORITHM OVERVIEW:
The file manager abstracts all disk I/O operations and provides a clean
interface for page-based storage. It supports both binary and ASCII
formats for storage based on debug mode settings.

FILE FORMAT:
- Page 0: Index metadata
- Pages 1-N: Bucket pages
- Pages N+1+: Overflow pages
- Each page has a fixed size (typically 4KB or 8KB)

ASCII FORMAT:
When debug mode is enabled, all data is stored in human-readable ASCII
format with clear delimiters and structure for easy debugging.

CONCURRENCY:
File operations are synchronized through the page manager's locking
mechanism to ensure thread safety.
*/

package hashindexV2

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// FileManager handles all file I/O operations for the hash index
type FileManager struct {
	filePath  string
	file      *os.File
	pageSize  uint32
	debugMode bool
	logger    *zap.SugaredLogger
}

// NewFileManager creates a new file manager instance
// Parameters:
//   - filePath: Path to the index file
//   - pageSize: Size of each page in bytes
//   - debugMode: Whether to use ASCII format
//   - logger: Logger for debug/error messages
//
// Returns:
//   - *FileManager: The file manager instance
//   - error: Any error that occurred during creation
func NewFileManager(filePath string, pageSize uint32, debugMode bool, logger *zap.SugaredLogger) (*FileManager, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	if filePath == "" {
		return nil, fmt.Errorf("file path cannot be empty")
	}

	logger.Debugf("Creating file manager for %s (pageSize: %d, debug: %t)", filePath, pageSize, debugMode)

	// Check if file exists
	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
		if pageSize == 0 {
			return nil, fmt.Errorf("cannot read page size from non-existent file: %s", filePath)
		}

		logger.Debugf("File %s does not exist, will create new file", filePath)

	}

	// Open or create the file
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	// If pageSize is 0, read it directly from the file
	if pageSize == 0 && fileExists {
		actualPageSize, err := readPageSizeFromFile(file, debugMode, logger)
		if err != nil {
			logger.Warnf("Failed to read page size from file, using default: %v", err)
			pageSize = 4096 // Default fallback
		} else {
			pageSize = actualPageSize
		}
		logger.Debugf("Determined page size from existing file: %d", pageSize)
	}

	// Set default if still zero
	if pageSize == 0 {
		pageSize = 4096
		logger.Debugf("Using default page size: %d", pageSize)
	}

	fm := &FileManager{
		filePath:  filePath,
		file:      file,
		pageSize:  pageSize,
		debugMode: debugMode,
		logger:    logger,
	}

	logger.Debugf("Successfully created file manager for %s (pageSize: %d, debug: %t)",
		filePath, pageSize, debugMode)

	return fm, nil

}

// readPageSizeFromFile reads the page size directly from file metadata
// This function follows the Single Responsibility Principle for page size extraction
// Parameters:
//   - file: The open file handle
//   - debugMode: Whether the file is in ASCII or binary format
//   - logger: Logger for debug messages
//
// Returns:
//   - uint32: The page size read from the file
//   - error: Any error that occurred during reading
func readPageSizeFromFile(file *os.File, debugMode bool, logger *zap.SugaredLogger) (uint32, error) {
	// Save current position
	currentPos, err := file.Seek(0, 1)
	if err != nil {
		return 0, fmt.Errorf("failed to get current file position: %w", err)
	}

	// Seek to beginning
	_, err = file.Seek(0, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to beginning: %w", err)
	}

	var pageSize uint32

	if debugMode {
		// Read ASCII format to find page size
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "PageSize:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					if size, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 32); err == nil {
						pageSize = uint32(size)
						break
					}
				}
			}
			// Stop after reading metadata section
			if strings.Contains(line, "=== END PAGE ===") {
				break
			}
		}
	} else {
		// Read binary format
		// Assuming metadata structure starts with page type (4 bytes) then page size (4 bytes)
		header := make([]byte, 8)
		n, err := file.Read(header)
		if err != nil || n < 8 {
			return 0, fmt.Errorf("failed to read file header: %w", err)
		}

		// Skip page type, read page size
		pageSize = binary.LittleEndian.Uint32(header[4:8])
	}

	// Restore file position
	_, err = file.Seek(currentPos, 0)
	if err != nil {
		logger.Warnf("Failed to restore file position: %v", err)
	}

	if pageSize == 0 {
		return 0, fmt.Errorf("invalid page size read from file: %d", pageSize)
	}

	return pageSize, nil
}

// ReadPage reads a page from the file
// Parameters:
//   - pageNum: The page number to read
//
// Returns:
//   - interface{}: The page data (BucketPage, OverflowPage, etc.)
//   - error: Any error that occurred during reading
func (fm *FileManager) ReadPage(pageNum uint32) (interface{}, error) {
	fm.logger.Debugf("Reading page %d", pageNum)

	if fm.debugMode {
		return fm.readPageASCII(pageNum)
	}
	return fm.readPageBinary(pageNum)
}

// WritePage writes a page to the file
// Parameters:
//   - pageNum: The page number to write
//   - pageData: The page data to write
//
// Returns:
//   - error: Any error that occurred during writing
func (fm *FileManager) WritePage(pageNum uint32, pageData interface{}) error {
	fm.logger.Debugf("Writing page %d", pageNum)

	if fm.debugMode {
		return fm.writePageASCII(pageNum, pageData)
	}
	return fm.writePageBinary(pageNum, pageData)
}

// ReadMetadata reads the metadata from page 0
// Returns:
//   - *IndexMetadata: The metadata
//   - error: Any error that occurred during reading
func (fm *FileManager) ReadMetadata() (*HashIndexMetadata, error) {
	// Defensive programming to prevent nil pointer dereference
	if fm == nil {
		return nil, fmt.Errorf("file manager is nil")
	}

	if fm.logger == nil {
		return nil, fmt.Errorf("file manager logger is nil")
	}

	if fm.file == nil {
		return nil, fmt.Errorf("file manager file handle is nil")
	}

	fm.logger.Debugf("Reading metadata from page 0")

	pageData, err := fm.ReadPage(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata page: %w", err)
	}

	metadata, ok := pageData.(*HashIndexMetadata)
	if !ok {
		return nil, fmt.Errorf("page 0 does not contain metadata, got type: %T", pageData)
	}

	if metadata == nil {
		return nil, fmt.Errorf("metadata is nil after successful type assertion")
	}

	fm.logger.Debugf("Successfully read metadata: buckets=%d, pageSize=%d",
		metadata.BucketCount, metadata.PageSize)

	return metadata, nil
}

// WriteMetadata writes the metadata to page 0
// Parameters:
//   - metadata: The metadata to write
//
// Returns:
//   - error: Any error that occurred during writing
func (fm *FileManager) WriteMetadata(metadata *HashIndexMetadata) error {
	fm.logger.Debugf("Writing metadata to page 0")
	return fm.WritePage(0, metadata)
}

// Close closes the file
// Returns:
//   - error: Any error that occurred during closing
func (fm *FileManager) Close() error {
	if fm.file != nil {
		err := fm.file.Close()
		fm.file = nil
		return err
	}
	return nil
}

// Sync forces any buffered data to be written to disk
// This function ensures data persistence following database ACID principles
// Returns:
//   - error: Any error that occurred during sync
func (fm *FileManager) Sync() error {
	if fm.file == nil {
		return fmt.Errorf("file handle is nil")
	}

	if err := fm.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file to disk: %w", err)
	}

	fm.logger.Debugf("Successfully synced file to disk")
	return nil
}

// readPageBinary reads a page in binary format
func (fm *FileManager) readPageBinary(pageNum uint32) (interface{}, error) {
	offset := int64(pageNum) * int64(fm.pageSize)

	// Seek to page position
	_, err := fm.file.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to page %d: %w", pageNum, err)
	}

	// Read page data
	pageData := make([]byte, fm.pageSize)
	n, err := fm.file.Read(pageData)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageNum, err)
	}

	if uint32(n) < fm.pageSize {
		// Pad with zeros if file is shorter
		for i := n; i < int(fm.pageSize); i++ {
			pageData[i] = 0
		}
	}

	// Parse based on page type
	return fm.parsePageData(pageData, pageNum)
}

// writePageBinary writes a page in binary format
func (fm *FileManager) writePageBinary(pageNum uint32, pageData interface{}) error {
	offset := int64(pageNum) * int64(fm.pageSize)

	// Seek to page position
	_, err := fm.file.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to page %d: %w", pageNum, err)
	}

	// Serialize page data
	serializedData, err := fm.serializePageData(pageData)
	if err != nil {
		return fmt.Errorf("failed to serialize page data: %w", err)
	}

	// Pad to page size
	if len(serializedData) < int(fm.pageSize) {
		padding := make([]byte, int(fm.pageSize)-len(serializedData))
		serializedData = append(serializedData, padding...)
	}

	// Write to file
	_, err = fm.file.Write(serializedData)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageNum, err)
	}

	return fm.file.Sync()
}

// readPageASCII reads a page in ASCII format
func (fm *FileManager) readPageASCII(pageNum uint32) (interface{}, error) {
	if fm == nil {
		return nil, fmt.Errorf("file manager is nil")
	}

	if fm.file == nil {
		return nil, fmt.Errorf("file handle is nil")
	}

	fm.logger.Debugf("Reading page %d in ASCII format", pageNum)

	// Calculate page offset and seek to it
	offset := int64(pageNum) * int64(fm.pageSize)
	if _, err := fm.file.Seek(offset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to page %d offset %d: %w", pageNum, offset, err)
	}

	// Read the entire page
	pageData := make([]byte, fm.pageSize)
	if _, err := fm.file.Read(pageData); err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageNum, err)
	}

	// Parse the ASCII data
	scanner := bufio.NewScanner(bytes.NewReader(pageData))
	var pageType string

	// Find the page type first
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and padding
		if line == "" || line == " " {
			continue
		}

		// Parse page header to get type
		if strings.HasPrefix(line, "TYPE: ") {
			pageType = strings.TrimPrefix(line, "TYPE: ")
			break
		}
	}

	if pageType == "" {
		return nil, fmt.Errorf("failed to determine page type for page %d", pageNum)
	}

	// Reset scanner to beginning
	scanner = bufio.NewScanner(bytes.NewReader(pageData))

	// Parse data fields based on page type
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines, padding, and header lines
		if line == "" || line == " " || strings.HasPrefix(line, "===") ||
			strings.HasPrefix(line, "TYPE:") || strings.HasPrefix(line, "TIMESTAMP:") {
			continue
		}

		// Parse first data field to determine parsing strategy
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])

				switch pageType {
				case "*hashindexV2.HashIndexMetadata":
					return fm.parseMetadataFromASCII(scanner, key, value)
				case "*hashindexV2.BucketPage":

					bp, err := fm.parseBucketPageFromASCII(scanner, key, value)

					if err != nil {
						return nil, fmt.Errorf("failed to parse bucket page %w", err)
					}
					return bp, nil
				case "*hashindexV2.OverflowPage":
					return fm.parseOverflowPageFromASCII(scanner, key, value)
				default:
					return nil, fmt.Errorf("unknown page type: %s", pageType)
				}
			}
		}
	}

	return nil, fmt.Errorf("failed to parse page %d: no valid data found", pageNum)

}

// writePageASCII writes a page in ASCII format with complete hash data
// This function follows the Single Responsibility Principle by handling only ASCII serialization
// Parameters:
//   - pageNum: The page number to write
//   - pageData: The page data to serialize
//
// Returns:
//   - error: Any error that occurred during writing
func (fm *FileManager) writePageASCII(pageNum uint32, pageData interface{}) error {
	if fm == nil {
		return fmt.Errorf("file manager is nil")
	}

	if fm.file == nil {
		return fmt.Errorf("file handle is nil")
	}

	fm.logger.Debugf("Writing page %d in ASCII format", pageNum)

	var buffer bytes.Buffer

	// Write page header
	buffer.WriteString(fmt.Sprintf("=== PAGE %d ===\n", pageNum))
	buffer.WriteString(fmt.Sprintf("TYPE: %T\n", pageData))
	buffer.WriteString(fmt.Sprintf("TIMESTAMP: %s\n", time.Now().Format(time.RFC3339)))

	// Serialize the actual page data based on its type
	switch data := pageData.(type) {
	case *HashIndexMetadata:
		buffer.WriteString(fmt.Sprintf("HashSeed: %d\n", data.HashSeed))
		buffer.WriteString(fmt.Sprintf("BucketCount: %d\n", data.BucketCount))
		buffer.WriteString(fmt.Sprintf("PageSize: %d\n", data.PageSize))
		buffer.WriteString(fmt.Sprintf("LoadFactor: %.2f\n", data.LoadFactor))
		buffer.WriteString(fmt.Sprintf("DocumentCount: %d\n", data.DocumentCount))
		buffer.WriteString(fmt.Sprintf("NextPageNum: %d\n", data.NextPageNum))

		// Serialize free page list
		buffer.WriteString("FreePageList: [")
		for i, pageNum := range data.FreePageList {
			if i > 0 {
				buffer.WriteString(", ")
			}
			buffer.WriteString(fmt.Sprintf("%d", pageNum))
		}
		buffer.WriteString("]\n")

	case *BucketPage:
		buffer.WriteString(fmt.Sprintf("PageNumber: %d\n", data.PageNumber))
		buffer.WriteString(fmt.Sprintf("BucketNumber: %d\n", data.BucketNumber))
		buffer.WriteString(fmt.Sprintf("RecordCount: %d\n", data.RecordCount))
		buffer.WriteString(fmt.Sprintf("OverflowPage: %d\n", data.OverflowPage))

		// Serialize hash entries with both hash values and DocumentIDs
		buffer.WriteString("Records: [\n")
		for i, entry := range data.Records {
			buffer.WriteString(fmt.Sprintf("  Entry %d:\n", i))
			buffer.WriteString(fmt.Sprintf("    HashValue: %d\n", entry.HashValue))
			buffer.WriteString(fmt.Sprintf("    DocumentID: %s\n", entry.DocumentID))
		}
		buffer.WriteString("]\n")

	case *OverflowPage:
		buffer.WriteString(fmt.Sprintf("PageNumber: %d\n", data.PageNumber))
		buffer.WriteString(fmt.Sprintf("ParentBucket: %d\n", data.ParentBucket))
		buffer.WriteString(fmt.Sprintf("NextOverflowPage: %d\n", data.NextOverflowPage))
		buffer.WriteString(fmt.Sprintf("RecordCount: %d\n", data.RecordCount))

		// Serialize overflow hash entries
		buffer.WriteString("Records: [\n")
		for i, entry := range data.Records {
			buffer.WriteString(fmt.Sprintf("  Entry %d:\n", i))
			buffer.WriteString(fmt.Sprintf("    HashValue: %d\n", entry.HashValue))
			buffer.WriteString(fmt.Sprintf("    DocumentID: %s\n", entry.DocumentID))
		}
		buffer.WriteString("]\n")

	default:
		return fmt.Errorf("unsupported page type for ASCII serialization: %T", pageData)
	}

	// Write page footer
	buffer.WriteString("=== END PAGE ===\n\n")

	// Calculate page offset and write to file
	offset := int64(pageNum) * int64(fm.pageSize)
	if _, err := fm.file.Seek(offset, 0); err != nil {
		return fmt.Errorf("failed to seek to page %d offset %d: %w", pageNum, offset, err)
	}

	// Pad or truncate to exact page size
	data := buffer.Bytes()
	if len(data) > int(fm.pageSize) {
		return fmt.Errorf("page data exceeds page size: %d > %d", len(data), fm.pageSize)
	}

	// Pad with spaces to fill the page
	for len(data) < int(fm.pageSize) {
		data = append(data, ' ')
	}

	// Write the page data
	if _, err := fm.file.Write(data); err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageNum, err)
	}

	fm.logger.Debugf("Successfully wrote page %d in ASCII format (%d bytes)", pageNum, len(data))
	return nil
}

// parseBucketPageFromASCII parses bucket page data from ASCII format
// This function handles the parsing of bucket-specific fields following SRP
// Parameters:
//   - scanner: The scanner for reading additional lines
//   - firstKey: The first key that was already read
//   - firstValue: The first value that was already read
//
// Returns:
//   - *BucketPage: The parsed bucket page
//   - error: Any error that occurred during parsing
func (fm *FileManager) parseBucketPageFromASCII(scanner *bufio.Scanner, firstKey, firstValue string) (*BucketPage, error) {
	// Initialize bucket page with proper defaults following SyndrDB standards
	pageHeader := &PageHeader{
		PageType:     PageTypeBucket,
		PageNumber:   0,
		LastModified: time.Now()}

	bucketPage := &BucketPage{
		PageHeader:   pageHeader,
		BucketNumber: 0,
		RecordCount:  0,
		OverflowPage: 0,
		Records:      make([]*IndexRecord, 0),
	}

	// Parse the first field
	if err := fm.setBucketPageField(bucketPage, firstKey, firstValue); err != nil {
		return nil, err
	}

	// Parse remaining fields
	var currentEntry *IndexRecord
	var inHashEntries bool
	var inRecordsSection bool

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "=== END PAGE ===" {
			break
		}

		// Handle records section
		if line == "Records: [" {
			inHashEntries = true
			inRecordsSection = true
			continue
		}

		if line == "]" && inHashEntries {
			inHashEntries = false
			inRecordsSection = false
			// Add the last entry if it exists
			if currentEntry != nil {
				bucketPage.Records = append(bucketPage.Records, currentEntry)
				currentEntry = nil
			}
			continue
		}

		if inRecordsSection {
			// Parse individual hash entries
			if strings.Contains(line, "Entry ") {
				// Finalize previous entry if exists
				if currentEntry != nil {
					bucketPage.Records = append(bucketPage.Records, currentEntry)
				}
				// Start new entry
				currentEntry = &IndexRecord{}
				continue
			}

			if currentEntry != nil && strings.Contains(line, ":") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])

					switch key {
					case "HashValue":
						if val, err := strconv.ParseUint(value, 10, 32); err != nil {
							return nil, fmt.Errorf("invalid HashValue: %w", err)
						} else {
							currentEntry.HashValue = uint32(val)
						}
					case "DocumentID":
						currentEntry.DocumentID = value
					}
				}
			}
			continue
		}

		// Parse regular fields
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])

				if err := fm.setBucketPageField(bucketPage, key, value); err != nil {
					return nil, err
				}
			}
		}
	}

	// Add the last entry if it exists
	if currentEntry != nil {
		bucketPage.Records = append(bucketPage.Records, currentEntry)
	}

	// Update RecordCount to match actual entries following SyndrDB data integrity principles
	bucketPage.RecordCount = uint32(len(bucketPage.Records))

	fm.logger.Debugf("Successfully parsed bucket page: PageNum=%d, BucketIndex=%d, RecordCount=%d",
		bucketPage.PageNumber, bucketPage.BucketNumber, bucketPage.RecordCount)

	return bucketPage, nil
}

// setBucketPageField sets a field in the bucket page struct
// This function follows the Single Responsibility Principle for field setting
// Parameters:
//   - bucketPage: The bucket page struct to populate
//   - key: The field name
//   - value: The field value as string
//
// Returns:
//   - error: Any error that occurred during field setting
func (fm *FileManager) setBucketPageField(bucketPage *BucketPage, key, value string) error {
	switch key {
	case "PageNumber":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid PageNumber: %w", err)
		} else {
			bucketPage.PageNumber = uint32(val)
		}
	case "BucketNumber":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid BucketNumber: %w", err)
		} else {
			bucketPage.BucketNumber = uint32(val)
		}
	case "RecordCount":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid RecordCount: %w", err)
		} else {
			bucketPage.RecordCount = uint32(val)
		}
	case "OverflowPage":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid OverflowPage: %w", err)
		} else {
			bucketPage.OverflowPage = uint32(val)
		}
	}

	return nil
}

// Helper methods for parsing and serialization
func (fm *FileManager) parsePageData(data []byte, pageNum uint32) (interface{}, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("page data too short")
	}

	// Read page type from first 4 bytes
	pageType := binary.LittleEndian.Uint32(data[0:4])

	switch pageType {
	case PageTypeMetadata:
		return fm.parseMetadata(data)
	case PageTypeBucket:
		return fm.parseBucketPage(data, pageNum)
	case PageTypeOverflow:
		return fm.parseOverflowPage(data, pageNum)
	default:
		// Default to bucket page for compatibility
		return NewBucketPage(pageNum-1, fm.pageSize), nil
	}
}

// parseMetadataFromASCII parses metadata from ASCII format
// This function handles the parsing of metadata-specific fields following SRP
// Parameters:
//   - scanner: The scanner for reading additional lines
//   - firstKey: The first key that was already read
//   - firstValue: The first value that was already read
//
// Returns:
//   - *HashIndexMetadata: The parsed metadata
//   - error: Any error that occurred during parsing
func (fm *FileManager) parseMetadataFromASCII(scanner *bufio.Scanner, firstKey, firstValue string) (*HashIndexMetadata, error) {
	if fm == nil {
		return nil, fmt.Errorf("file manager is nil")
	}

	// Initialize metadata with proper defaults following SyndrDB standards
	metadata := &HashIndexMetadata{
		HashSeed:      GenerateHashSeed(), // Default seed
		BucketCount:   0,
		PageSize:      4096, // Default page size
		LoadFactor:    0.0,
		DocumentCount: 0,
		NextPageNum:   1,
		FreePageList:  make([]uint32, 0),
	}

	// Parse the first field that was already read
	if err := fm.setMetadataField(metadata, firstKey, firstValue); err != nil {
		return nil, fmt.Errorf("failed to set first metadata field %s=%s: %w", firstKey, firstValue, err)
	}

	// Parse remaining fields
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and padding
		if line == "" || line == " " {
			continue
		}

		// Check for end of page
		if line == "=== END PAGE ===" {
			break
		}

		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])

				if err := fm.setMetadataField(metadata, key, value); err != nil {
					return nil, fmt.Errorf("failed to set metadata field %s=%s: %w", key, value, err)
				}
			}
		}
	}

	fm.logger.Debugf("Successfully parsed metadata: BucketCount=%d, PageSize=%d, DocumentCount=%d",
		metadata.BucketCount, metadata.PageSize, metadata.DocumentCount)

	return metadata, nil
}

// setMetadataField sets a field in the metadata struct
// This function follows the Single Responsibility Principle for field setting
// Parameters:
//   - metadata: The metadata struct to populate
//   - key: The field name
//   - value: The field value as string
//
// Returns:
//   - error: Any error that occurred during field setting
func (fm *FileManager) setMetadataField(metadata *HashIndexMetadata, key, value string) error {
	// Defensive programming to prevent nil pointer dereference following SyndrDB standards
	if metadata == nil {
		return fmt.Errorf("metadata is nil, cannot set field %s", key)
	}

	switch key {
	case "HashSeed":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid HashSeed '%s': %w", value, err)
		} else {
			metadata.HashSeed = uint32(val)
		}
	case "BucketCount":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid BucketCount '%s': %w", value, err)
		} else {
			metadata.BucketCount = uint32(val)
		}
	case "PageSize":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid PageSize '%s': %w", value, err)
		} else {
			metadata.PageSize = uint32(val)
		}
	case "LoadFactor":
		if val, err := strconv.ParseFloat(value, 64); err != nil {
			return fmt.Errorf("invalid LoadFactor '%s': %w", value, err)
		} else {
			metadata.LoadFactor = val
		}
	case "DocumentCount":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid DocumentCount '%s': %w", value, err)
		} else {
			metadata.DocumentCount = val
		}
	case "NextPageNum":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid NextPageNum '%s': %w", value, err)
		} else {
			metadata.NextPageNum = uint32(val)
		}
	case "FreePageList":
		// Parse the array format: [1, 2, 3] or []
		value = strings.Trim(value, "[]")
		if value == "" {
			metadata.FreePageList = make([]uint32, 0)
		} else {
			parts := strings.Split(value, ",")
			metadata.FreePageList = make([]uint32, len(parts))
			for i, part := range parts {
				if val, err := strconv.ParseUint(strings.TrimSpace(part), 10, 32); err != nil {
					return fmt.Errorf("invalid FreePageList entry '%s': %w", part, err)
				} else {
					metadata.FreePageList[i] = uint32(val)
				}
			}
		}
	default:
		// Log unknown fields but don't fail - this provides forward compatibility
		if fm.logger != nil {
			fm.logger.Debugf("Unknown metadata field '%s' with value '%s' - ignoring", key, value)
		}
	}

	return nil
}

// parseOverflowPageFromASCII parses overflow page data from ASCII format
// This function handles the parsing of overflow-specific fields following SRP
// Parameters:
//   - scanner: The scanner for reading additional lines
//   - firstKey: The first key that was already read
//   - firstValue: The first value that was already read
//
// Returns:
//   - *OverflowPage: The parsed overflow page
//   - error: Any error that occurred during parsing
func (fm *FileManager) parseOverflowPageFromASCII(scanner *bufio.Scanner, firstKey, firstValue string) (*OverflowPage, error) {
	if fm == nil {
		return nil, fmt.Errorf("file manager is nil")
	}

	pageHeader := &PageHeader{
		PageType:     PageTypeBucket,
		PageNumber:   0,
		LastModified: time.Now(),
	}

	// Initialize overflow page with proper defaults following SyndrDB standards
	overflowPage := &OverflowPage{
		PageHeader:       pageHeader,
		ParentBucket:     0,
		NextOverflowPage: 0,
		RecordCount:      0,
		Records:          make([]*IndexRecord, 0),
	}

	// Parse the first field that was already read
	if err := fm.setOverflowPageField(overflowPage, firstKey, firstValue); err != nil {
		return nil, fmt.Errorf("failed to set first overflow field %s=%s: %w", firstKey, firstValue, err)
	}

	// Parse remaining fields (similar logic to bucket page parsing)
	var currentEntry *IndexRecord
	var inHashEntries bool

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and padding
		if line == "" || line == " " {
			continue
		}

		// Check for end of page
		if line == "=== END PAGE ===" {
			break
		}

		// Handle records section
		if line == "Records: [" {
			inHashEntries = true
			continue
		}

		// Handle end of array sections
		if line == "]" && inHashEntries {
			inHashEntries = false
			if currentEntry != nil {
				overflowPage.Records = append(overflowPage.Records, currentEntry)
				currentEntry = nil
			}
			continue
		}

		// Parse hash entries
		if inHashEntries {
			if strings.Contains(line, "Entry ") && strings.Contains(line, ":") {
				if currentEntry != nil {
					overflowPage.Records = append(overflowPage.Records, currentEntry)
				}
				currentEntry = &IndexRecord{}
				continue
			}

			if currentEntry != nil && strings.Contains(line, ":") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])

					switch key {
					case "HashValue":
						if val, err := strconv.ParseUint(value, 10, 32); err != nil {
							return nil, fmt.Errorf("invalid HashValue '%s': %w", value, err)
						} else {
							currentEntry.HashValue = uint32(val)
						}
					case "DocumentID":
						currentEntry.DocumentID = value
					}
				}
			}
			continue
		}

		// Parse regular fields
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])

				if err := fm.setOverflowPageField(overflowPage, key, value); err != nil {
					return nil, fmt.Errorf("failed to set overflow field %s=%s: %w", key, value, err)
				}
			}
		}
	}

	// Add the last entry if it exists
	if currentEntry != nil {
		overflowPage.Records = append(overflowPage.Records, currentEntry)
	}

	// Update EntryCount to match actual entries
	overflowPage.RecordCount = uint32(len(overflowPage.Records))

	fm.logger.Debugf("Successfully parsed overflow page: PageNum=%d, ParentBucket=%d, EntryCount=%d",
		overflowPage.PageNumber, overflowPage.ParentBucket, overflowPage.RecordCount)

	return overflowPage, nil
}

// setOverflowPageField sets a field in the overflow page struct
// This function follows the Single Responsibility Principle for field setting
// Parameters:
//   - overflowPage: The overflow page struct to populate
//   - key: The field name
//   - value: The field value as string
//
// Returns:
//   - error: Any error that occurred during field setting
func (fm *FileManager) setOverflowPageField(overflowPage *OverflowPage, key, value string) error {
	// Defensive programming to prevent nil pointer dereference
	if overflowPage == nil {
		return fmt.Errorf("overflow page is nil, cannot set field %s", key)
	}

	switch key {
	case "PageNumber":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid PageNumber '%s': %w", value, err)
		} else {
			overflowPage.PageNumber = uint32(val)
		}
	case "ParentBucket":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid ParentBucket '%s': %w", value, err)
		} else {
			overflowPage.ParentBucket = uint32(val)
		}
	case "NextOverflowPage":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid NextOverflowPage '%s': %w", value, err)
		} else {
			overflowPage.NextOverflowPage = uint32(val)
		}
	case "RecordCount":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid RecordCount '%s': %w", value, err)
		} else {
			overflowPage.RecordCount = uint32(val)
		}
	default:
		// Log unknown fields but don't fail - this provides forward compatibility
		if fm.logger != nil {
			fm.logger.Debugf("Unknown overflow page field '%s' with value '%s' - ignoring", key, value)
		}
	}

	return nil
}

func (fm *FileManager) serializePageData(pageData interface{}) ([]byte, error) {
	// Implementation for binary serialization
	switch page := pageData.(type) {
	case *HashIndexMetadata:
		return fm.serializeMetadata(page)
	case *BucketPage:
		return fm.serializeBucketPage(page)
	case *OverflowPage:
		return fm.serializeOverflowPage(page)
	default:
		return nil, fmt.Errorf("unknown page type: %T", pageData)
	}
}

// Placeholder implementations - these would need to be completed
func (fm *FileManager) parseMetadata(data []byte) (*HashIndexMetadata, error) {
	// TODO: Implement binary parsing of metadata
	return NewIndexMetadata(16, fm.pageSize, 0.75, fm.debugMode), nil
}

func (fm *FileManager) parseBucketPage(data []byte, pageNum uint32) (*BucketPage, error) {
	// TODO: Implement binary parsing of bucket page
	return NewBucketPage(pageNum-1, fm.pageSize), nil
}

func (fm *FileManager) parseOverflowPage(data []byte, pageNum uint32) (*OverflowPage, error) {
	// TODO: Implement binary parsing of overflow page
	return NewOverflowPage(pageNum, fm.pageSize), nil
}

func (fm *FileManager) serializeMetadata(metadata *HashIndexMetadata) ([]byte, error) {
	// TODO: Implement binary serialization of metadata
	return make([]byte, fm.pageSize), nil
}

func (fm *FileManager) serializeBucketPage(page *BucketPage) ([]byte, error) {
	// TODO: Implement binary serialization of bucket page
	return make([]byte, fm.pageSize), nil
}

func (fm *FileManager) serializeOverflowPage(page *OverflowPage) ([]byte, error) {
	// TODO: Implement binary serialization of overflow page
	return make([]byte, fm.pageSize), nil
}

func (fm *FileManager) parsePageASCII(scanner *bufio.Scanner, pageNum uint32) (interface{}, error) {
	// TODO: Implement ASCII parsing
	if pageNum == 0 {
		return NewIndexMetadata(16, fm.pageSize, 0.75, fm.debugMode), nil
	}
	return NewBucketPage(pageNum-1, fm.pageSize), nil
}

func (fm *FileManager) writePageDataASCII(file *os.File, pageNum uint32, pageData interface{}) error {
	// TODO: Implement ASCII writing
	fmt.Fprintf(file, "=== PAGE %d ===\n", pageNum)
	fmt.Fprintf(file, "TYPE: %T\n", pageData)
	fmt.Fprintf(file, "TIMESTAMP: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(file, "=== END PAGE ===\n\n")
	return nil
}
