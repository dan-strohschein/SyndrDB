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
	// Open or create the file
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	fm := &FileManager{
		filePath:  filePath,
		file:      file,
		pageSize:  pageSize,
		debugMode: debugMode,
		logger:    logger,
	}

	logger.Debugf("Created file manager for %s (pageSize: %d, debug: %t)", filePath, pageSize, debugMode)
	return fm, nil
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
	fm.logger.Debugf("Reading metadata from page 0")

	pageData, err := fm.ReadPage(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata page: %w", err)
	}

	metadata, ok := pageData.(*HashIndexMetadata)
	if !ok {
		return nil, fmt.Errorf("page 0 does not contain metadata")
	}

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
	// For ASCII format, we'll use a simple text-based format
	// Each page starts with a delimiter and contains key-value pairs

	fm.file.Seek(0, 0)
	scanner := bufio.NewScanner(fm.file)

	// Look for page marker
	pageMarker := fmt.Sprintf("=== PAGE %d ===", pageNum)
	found := false

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == pageMarker {
			found = true
			break
		}
	}

	if !found {
		// Page doesn't exist, return empty page based on page number
		if pageNum == 0 {
			return NewIndexMetadata(16, fm.pageSize, 0.75, fm.debugMode), nil
		}
		return NewBucketPage(pageNum-1, fm.pageSize), nil
	}

	// Parse page content
	return fm.parsePageASCII(scanner, pageNum)
}

// writePageASCII writes a page in ASCII format
func (fm *FileManager) writePageASCII(pageNum uint32, pageData interface{}) error {
	// For simplicity, we'll rewrite the entire file
	// In production, we will do more sophisticated ASCII file management

	tempPath := fm.filePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	// Copy existing pages except the one we're updating
	fm.file.Seek(0, 0)
	scanner := bufio.NewScanner(fm.file)
	var currentPageNum uint32 = 0
	skipPage := false

	for scanner.Scan() {
		line := scanner.Text()

		// Check for page markers
		if strings.HasPrefix(line, "=== PAGE ") && strings.HasSuffix(line, " ===") {
			pageStr := strings.TrimPrefix(line, "=== PAGE ")
			pageStr = strings.TrimSuffix(pageStr, " ===")
			pageNumParsed, _ := strconv.ParseUint(strings.TrimSpace(pageStr), 10, 32)
			currentPageNum = uint32(pageNumParsed)
			skipPage = uint32(currentPageNum) == pageNum
		}

		if !skipPage {
			fmt.Fprintln(tempFile, line)
		}
	}

	// Write the new page
	err = fm.writePageDataASCII(tempFile, pageNum, pageData)
	if err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	// Replace original file
	tempFile.Close()
	fm.file.Close()

	err = os.Rename(tempPath, fm.filePath)
	if err != nil {
		return fmt.Errorf("failed to replace file: %w", err)
	}

	// Reopen file
	fm.file, err = os.OpenFile(fm.filePath, os.O_RDWR|os.O_CREATE, 0644)
	return err
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

// TODO: Add helper methods here for parsing specific page types
// ... (implementation details for parseMetadata, parseBucketPage, etc.)

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
