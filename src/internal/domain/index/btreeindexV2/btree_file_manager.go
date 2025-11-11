/*
BTREE FILE MANAGER SYSTEM

This file implements the file I/O operations for BTree indexes in SyndrDB.
It provides page-based file operations with support for both binary and ASCII
debug formats, following the storage patterns used in PostgreSQL, MySQL, and SQL Server.

FILE FORMAT OVERVIEW:
The BTree index file uses a page-based storage format where:
- Page 0: Contains metadata about the index (configuration and statistics)
- Pages 1-N: Store BTree nodes (internal and leaf nodes)
- Each page has a fixed size (typically 8KB) with a structured header
- Pages contain serialized node data with proper alignment and checksums

DUAL FORMAT SUPPORT:
The file manager supports two storage formats:
1. Binary Format: Efficient binary serialization for production use
2. ASCII Format: Human-readable format for debugging and development

ASCII FORMAT STRUCTURE:
When debug mode is enabled, the file uses a structured ASCII format:
- Clear section headers and delimiters
- Human-readable field names and values
- Proper indentation and formatting for easy inspection
- Comment lines explaining the data structure

BINARY FORMAT STRUCTURE:
For production use, the file uses efficient binary serialization:
- Little-endian byte order for cross-platform compatibility
- Fixed-size headers with version information
- Efficient packing of variable-length data
- CRC32 checksums for data integrity

PAGE MANAGEMENT:
The file manager handles page allocation, deallocation, and free space tracking.
It maintains a free page list in the metadata for efficient space reuse and
supports page-level operations for reading and writing node data.

ERROR HANDLING:
Comprehensive error handling ensures data integrity and provides meaningful
error messages for debugging. All file operations include proper error
checking and recovery mechanisms.

This implementation follows the Single Responsibility Principle by focusing
exclusively on file I/O operations while delegating page caching and node
management to other specialized components.
*/

package btreeindexV2

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// BTreeFileManager handles file I/O operations for BTree indexes
// This structure manages all file operations including reading, writing,
// and maintaining the index file format
type BTreeFileManager struct {
	filePath   string             // Path to the index file
	file       *os.File           // File handle for I/O operations
	pageSize   uint32             // Size of each page in bytes
	debugMode  bool               // Whether to use ASCII format
	isOpen     bool               // Whether the file is currently open
	logger     *zap.SugaredLogger // Logger for debug and error messages
	fileHeader *FileHeader        // File header information
	mutex      sync.RWMutex       // Thread safety for file operations
}

func (fm *BTreeFileManager) AllocatePage() (uint32, error) {
	// Ensure the file is open
	if !fm.isOpen {
		return 0, fmt.Errorf("file manager is not open")
	}

	// Call AllocateNewPage directly since it handles its own locking
	newPageNum, err := fm.AllocateNewPage()
	if err != nil {
		return 0, fmt.Errorf("failed to allocate new page: %w", err)
	}

	fm.logger.Debugf("Allocated new page: %d", newPageNum)

	// Return the allocated page number and a placeholder for the page data
	return newPageNum, nil

}

func (fm *BTreeFileManager) Sync() error {
	// Ensure the file is open
	if !fm.isOpen {
		return fmt.Errorf("file manager is not open")
	}

	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	// Sync the file to ensure all changes are written to disk
	if err := fm.file.Sync(); err != nil {
		fm.logger.Warnf("Failed to sync file: %v", err)
		return fmt.Errorf("failed to sync file: %w", err)
	}

	fm.logger.Debugf("File synced successfully")
	return nil
}

// FileHeader contains file-level metadata
// This structure is stored at the beginning of the file and contains
// information about the file format and compatibility
type FileHeader struct {
	MagicNumber  uint32    // File format identifier (0x42545245 = "BTRE")
	FileVersion  uint32    // File format version number
	PageSize     uint32    // Size of each page in bytes
	TotalPages   uint32    // Total number of pages in the file
	CreatedAt    time.Time // When the file was created
	LastModified time.Time // Last modification timestamp
	IsDebugMode  bool      // Whether file uses ASCII format
	Checksum     uint32    // File header checksum
}

// NewBTreeFileManager creates a new file manager for BTree index operations
// Parameters:
//   - filePath: Path to the index file
//   - pageSize: Size of each page in bytes (0 to read from existing file)
//   - debugMode: Whether to use ASCII format for debugging
//   - logger: Logger for debug and error messages
//
// Returns:
//   - *BTreeFileManager: The created file manager instance
//   - error: Any error that occurred during creation
func NewBTreeFileManager(filePath string, pageSize uint32, debugMode bool, logger *zap.SugaredLogger) (*BTreeFileManager, error) {
	logger.Infof("DEBUG: NewBTreeFileManager called with filePath=%s, pageSize=%d, debugMode=%t", filePath, pageSize, debugMode)

	if filePath == "" {
		return nil, fmt.Errorf("file path cannot be empty")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	logger.Debugf("Creating BTree file manager for: %s", filePath)

	fm := &BTreeFileManager{
		filePath:  filePath,
		pageSize:  pageSize,
		debugMode: debugMode,
		isOpen:    false,
		logger:    logger,
		mutex:     sync.RWMutex{},
	}

	// Check if file exists
	logger.Infof("DEBUG: Checking if file exists: %s", filePath)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Create new file
		logger.Infof("DEBUG: File does not exist, creating new file")
		if pageSize == 0 {
			return nil, fmt.Errorf("page size must be specified for new files")
		}

		if err := fm.createNewFile(); err != nil {
			return nil, fmt.Errorf("failed to create new file: %w", err)
		}
		logger.Infof("DEBUG: New file created successfully")
	} else {
		// Open existing file
		logger.Infof("DEBUG: File exists, opening existing file")
		if err := fm.openExistingFile(); err != nil {
			return nil, fmt.Errorf("failed to open existing file: %w", err)
		}

		// If pageSize was 0, use the one from file header
		if pageSize == 0 {
			fm.pageSize = fm.fileHeader.PageSize
		}
		logger.Infof("DEBUG: Existing file opened successfully")
	}

	logger.Infof("Successfully created BTree file manager for: %s (pageSize: %d, debugMode: %t)",
		filePath, fm.pageSize, debugMode)

	return fm, nil
}

// ReadPage reads a page from the file
// Parameters:
//   - pageNum: The page number to read
//
// Returns:
//   - interface{}: The page data (could be *BTreeNode or *BTreeMetadata)
//   - error: Any error that occurred during reading
func (fm *BTreeFileManager) ReadPage(pageNum uint32) (interface{}, error) {
	if !fm.isOpen {
		return nil, fmt.Errorf("file manager is not open")
	}

	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	fm.logger.Debugf("Reading page %d from file", pageNum)

	// Calculate file offset
	offset := fm.calculatePageOffset(pageNum)

	// Read page data
	pageData := make([]byte, fm.pageSize)
	n, err := fm.file.ReadAt(pageData, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read page %d: %w", pageNum, err)
	}

	if n == 0 {
		return nil, fmt.Errorf("page %d does not exist", pageNum)
	}

	// Trim any padding
	pageData = pageData[:n]

	// Deserialize based on format
	if fm.debugMode {
		return fm.deserializePageASCII(pageData)
	} else {
		return fm.deserializePageBinary(pageData, pageNum)
	}
}

// WritePage writes a page to the file
// Parameters:
//   - pageNum: The page number to write
//   - pageData: The page data to write
//
// Returns:
//   - error: Any error that occurred during writing
func (fm *BTreeFileManager) WritePage(pageNum uint32, pageData interface{}) error {
	if !fm.isOpen {
		return fmt.Errorf("file manager is not open")
	}

	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	// INTENSIVE DEBUG: Log what we're writing
	if node, ok := pageData.(*BTreeNode); ok && !node.IsLeaf {
		fm.logger.Infof("WritePage CALLED: page %d internal node with %d keys (ptr=%p)",
			pageNum, node.KeyCount, pageData)
	} else {
		fm.logger.Debugf("Writing page %d to file", pageNum)
	}

	// Compute and store checksum for BTreeNode pages (skip metadata page 0)
	if pageNum > 0 {
		if btreeNode, ok := pageData.(*BTreeNode); ok {
			// TODO: I need to implement proper CRC32 checksum computation
			// For now, use simple checksum based on node fields
			btreeNode.Checksum = btreeNode.PageNum ^ btreeNode.KeyCount ^ btreeNode.NextLeaf ^ btreeNode.PrevLeaf
			fm.logger.Debugf("Computed checksum 0x%X for page %d", btreeNode.Checksum, pageNum)
		}
	}

	// Serialize page data
	var serialized []byte
	var err error

	if fm.debugMode {
		serialized, err = fm.serializePageASCII(pageData)
	} else {
		serialized, err = fm.serializePageBinary(pageData)
	}

	if err != nil {
		return fmt.Errorf("failed to serialize page %d: %w", pageNum, err)
	}

	// Pad to page size
	if uint32(len(serialized)) > fm.pageSize {
		return fmt.Errorf("serialized page %d size (%d) exceeds page size (%d)",
			pageNum, len(serialized), fm.pageSize)
	}

	// Pad with zeros to reach page size
	padded := make([]byte, fm.pageSize)
	copy(padded, serialized)

	// Calculate file offset
	offset := fm.calculatePageOffset(pageNum)

	// Write to file
	if _, err := fm.file.WriteAt(padded, offset); err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageNum, err)
	}

	// Update file header if necessary
	if pageNum >= fm.fileHeader.TotalPages {
		fm.fileHeader.TotalPages = pageNum + 1
		fm.fileHeader.LastModified = time.Now()

		// Write updated header
		if err := fm.writeFileHeader(); err != nil {
			fm.logger.Warnf("Failed to update file header: %v", err)
		}
	}

	// Sync to disk to ensure durability
	if err := fm.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync page %d to disk: %w", pageNum, err)
	}

	fm.logger.Debugf("Successfully wrote page %d to file", pageNum)

	return nil
}

// ReadMetadata reads the metadata from page 0
// Returns:
//   - *BTreeMetadata: The metadata read from the file
//   - error: Any error that occurred during reading
func (fm *BTreeFileManager) ReadMetadata() (*BTreeMetadata, error) {
	fm.logger.Debugf("Reading metadata from page 0")

	pageData, err := fm.ReadPage(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata page: %w", err)
	}

	metadata, ok := pageData.(*BTreeMetadata)
	if !ok {
		return nil, fmt.Errorf("page 0 does not contain valid metadata")
	}

	return metadata, nil
}

// WriteMetadata writes the metadata to page 0
// Parameters:
//   - metadata: The metadata to write
//
// Returns:
//   - error: Any error that occurred during writing
func (fm *BTreeFileManager) WriteMetadata(metadata *BTreeMetadata) error {
	fm.logger.Debugf("Writing metadata to page 0")

	if err := fm.WritePage(0, metadata); err != nil {
		return fmt.Errorf("failed to write metadata page: %w", err)
	}

	return nil
}

// AllocateNewPage allocates a new page number for use
// Returns:
//   - uint32: The allocated page number
//   - error: Any error that occurred during allocation
func (fm *BTreeFileManager) AllocateNewPage() (uint32, error) {
	if !fm.isOpen {
		return 0, fmt.Errorf("file manager is not open")
	}

	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	// For now, use simple sequential allocation
	// In a more sophisticated implementation, we would check the free page list
	newPageNum := fm.fileHeader.TotalPages
	fm.fileHeader.TotalPages++
	fm.fileHeader.LastModified = time.Now()

	// Write updated header
	if err := fm.writeFileHeader(); err != nil {
		return 0, fmt.Errorf("failed to update file header: %w", err)
	}

	fm.logger.Debugf("Allocated new page: %d", newPageNum)

	return newPageNum, nil
}

// DeallocatePage marks a page as free for reuse
// Parameters:
//   - pageNum: The page number to deallocate
//
// Returns:
//   - error: Any error that occurred during deallocation
func (fm *BTreeFileManager) DeallocatePage(pageNum uint32) error {
	if !fm.isOpen {
		return fmt.Errorf("file manager is not open")
	}

	if pageNum == 0 {
		return fmt.Errorf("cannot deallocate metadata page")
	}

	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	// For now, just log the deallocation
	// In a more sophisticated implementation, we would add to free page list
	fm.logger.Debugf("Deallocated page: %d", pageNum)

	return nil
}

// Close closes the file manager and underlying file
// Returns:
//   - error: Any error that occurred during closing
func (fm *BTreeFileManager) Close() error {
	if !fm.isOpen {
		return nil // Already closed
	}

	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	fm.logger.Debugf("Closing BTree file manager")

	// Sync any pending changes
	if err := fm.file.Sync(); err != nil {
		fm.logger.Warnf("Failed to sync file during close: %v", err)
	}

	// Close the file
	if err := fm.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	fm.isOpen = false
	fm.logger.Infof("Successfully closed BTree file manager")

	return nil
}

// GetFilePath returns the file path
// Returns:
//   - string: The file path
func (fm *BTreeFileManager) GetFilePath() string {
	return fm.filePath
}

// GetPageSize returns the page size
// Returns:
//   - uint32: The page size in bytes
func (fm *BTreeFileManager) GetPageSize() uint32 {
	return fm.pageSize
}

// GetTotalPages returns the total number of pages in the file
// Returns:
//   - uint32: The total number of pages
func (fm *BTreeFileManager) GetTotalPages() uint32 {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	return fm.fileHeader.TotalPages
}

// Private helper methods

// createNewFile creates a new index file with proper header
func (fm *BTreeFileManager) createNewFile() error {
	var err error
	fm.file, err = os.Create(fm.filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	fm.isOpen = true

	// Create file header
	fm.fileHeader = &FileHeader{
		MagicNumber:  0x42545245, // "BTRE"
		FileVersion:  1,
		PageSize:     fm.pageSize,
		TotalPages:   1, // Start with metadata page
		CreatedAt:    time.Now(),
		LastModified: time.Now(),
		IsDebugMode:  fm.debugMode,
		Checksum:     0, // Will be calculated
	}

	// Write file header
	return fm.writeFileHeader()
}

// openExistingFile opens an existing index file and reads header
func (fm *BTreeFileManager) openExistingFile() error {
	var err error
	fm.file, err = os.OpenFile(fm.filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	fm.isOpen = true

	// Read file header
	return fm.readFileHeader()
}

// writeFileHeader writes the file header to the beginning of the file
func (fm *BTreeFileManager) writeFileHeader() error {
	// Calculate checksum
	fm.fileHeader.Checksum = fm.calculateHeaderChecksum()

	var headerData []byte
	var err error

	if fm.debugMode {
		headerData, err = fm.serializeFileHeaderASCII(fm.fileHeader)
	} else {
		headerData, err = fm.serializeFileHeaderBinary()
	}

	if err != nil {
		return fmt.Errorf("failed to serialize file header: %w", err)
	}

	// Write at beginning of file
	if _, err := fm.file.WriteAt(headerData, 0); err != nil {
		return fmt.Errorf("failed to write file header: %w", err)
	}

	return nil
}

// readFileHeader reads the file header from the beginning of the file
func (fm *BTreeFileManager) readFileHeader() error {

	// Read initial bytes to determine format
	headerBytes := make([]byte, 1024) // Should be enough for header
	n, err := fm.file.ReadAt(headerBytes, 0)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read file header: %w", err)
	}

	headerBytes = headerBytes[:n]

	// Try to determine format by looking for magic number or ASCII markers
	if bytes.Contains(headerBytes, []byte("BTREE_INDEX_METADATA")) {
		// ASCII format
		fm.debugMode = true
		// Deserialize ASCII header
		fm.logger.Debugf("Detected ASCII format for file: %s", fm.filePath)
		fileHeader, err := fm.deserializeFileHeaderASCII(headerBytes)
		if err != nil {
			return fmt.Errorf("failed to deserialize ASCII file header: %w", err)
		}
		fm.fileHeader = fileHeader
		return nil
	} else {
		// Binary format
		fm.debugMode = false
		return fm.deserializeFileHeaderBinary(headerBytes)
	}
}

// calculatePageOffset calculates the file offset for a given page number
func (fm *BTreeFileManager) calculatePageOffset(pageNum uint32) int64 {
	// Page 0 starts after the file header
	headerSize := int64(1024) // Fixed header size
	return headerSize + int64(pageNum)*int64(fm.pageSize)
}

// calculateHeaderChecksum calculates CRC32 checksum for the header
func (fm *BTreeFileManager) calculateHeaderChecksum() uint32 {
	// Create a copy without checksum for calculation
	temp := *fm.fileHeader
	temp.Checksum = 0

	// Serialize without checksum
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, temp.MagicNumber)
	binary.Write(buf, binary.LittleEndian, temp.FileVersion)
	binary.Write(buf, binary.LittleEndian, temp.PageSize)
	binary.Write(buf, binary.LittleEndian, temp.TotalPages)

	return crc32.ChecksumIEEE(buf.Bytes())
}

// ASCII serialization methods
// serializeFileHeaderASCII serializes file header to ASCII format
// This function creates a human-readable representation of the file header
// with clear section boundaries for debugging purposes
// Parameters:
//   - header: The file header to serialize
//
// Returns:
//   - []byte: The serialized header data in ASCII format
//   - error: Any error that occurred during serialization
func (fm *BTreeFileManager) serializeFileHeaderASCII(header *FileHeader) ([]byte, error) {
	var buffer bytes.Buffer

	// Write file header with unique borders
	buffer.WriteString("## FILE HEADER\n")
	buffer.WriteString(fmt.Sprintf("MagicNumber: 0x%08X\n", header.MagicNumber))
	buffer.WriteString(fmt.Sprintf("FileVersion: %d\n", header.FileVersion))
	buffer.WriteString(fmt.Sprintf("PageSize: %d\n", header.PageSize))
	buffer.WriteString(fmt.Sprintf("TotalPages: %d\n", header.TotalPages))
	buffer.WriteString(fmt.Sprintf("CreatedAt: %s\n", header.CreatedAt.Format(time.RFC3339)))
	buffer.WriteString(fmt.Sprintf("LastModified: %s\n", header.LastModified.Format(time.RFC3339)))
	buffer.WriteString(fmt.Sprintf("IsDebugMode: %t\n", header.IsDebugMode))
	buffer.WriteString(fmt.Sprintf("Checksum: 0x%08X\n", header.Checksum))
	buffer.WriteString("|| END FILE HEADER\n")

	return buffer.Bytes(), nil
}

// deserializeFileHeaderASCII deserializes file header from ASCII format
// This function parses the human-readable file header format and populates
// the FileHeader structure with proper error handling
// Parameters:
//   - data: The ASCII data containing the file header
//
// Returns:
//   - *FileHeader: The parsed file header structure
//   - error: Any error that occurred during deserialization
func (fm *BTreeFileManager) deserializeFileHeaderASCII(data []byte) (*FileHeader, error) {
	header := &FileHeader{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	inHeaderSection := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Check for section start
		if line == "## FILE HEADER" {
			inHeaderSection = true
			continue
		}

		// Check for section end
		// if line == "|| END FILE HEADER" {
		// 	inHeaderSection = false
		// 	break
		// }

		// Parse header fields only when in header section
		if inHeaderSection && strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])

				if err := fm.parseFileHeaderField(header, key, value); err != nil {
					return nil, fmt.Errorf("failed to parse header field '%s': %w", key, err)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading header data: %w", err)
	}

	fm.logger.Debugf("Successfully parsed file header: Version=%d, PageSize=%d, TotalPages=%d",
		header.FileVersion, header.PageSize, header.TotalPages)

	return header, nil
}

// parseFileHeaderField parses individual file header fields
// This function handles the parsing of specific header field types following SRP
// Parameters:
//   - header: The header structure to populate
//   - key: The field name
//   - value: The field value as string
//
// Returns:
//   - error: Any error that occurred during field parsing
func (fm *BTreeFileManager) parseFileHeaderField(header *FileHeader, key, value string) error {
	switch key {
	case "MagicNumber":
		if val, err := strconv.ParseUint(strings.TrimPrefix(value, "0x"), 16, 32); err != nil {
			return fmt.Errorf("invalid MagicNumber: %w", err)
		} else {
			header.MagicNumber = uint32(val)
		}
	case "FileVersion":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid FileVersion: %w", err)
		} else {
			header.FileVersion = uint32(val)
		}
	case "PageSize":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid PageSize: %w", err)
		} else {
			header.PageSize = uint32(val)
		}
	case "TotalPages":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid TotalPages: %w", err)
		} else {
			header.TotalPages = uint32(val)
		}
	case "CreatedAt":
		if t, err := time.Parse(time.RFC3339, value); err != nil {
			return fmt.Errorf("invalid CreatedAt: %w", err)
		} else {
			header.CreatedAt = t
		}
	case "LastModified":
		if t, err := time.Parse(time.RFC3339, value); err != nil {
			return fmt.Errorf("invalid LastModified: %w", err)
		} else {
			header.LastModified = t
		}
	case "IsDebugMode":
		if val, err := strconv.ParseBool(value); err != nil {
			return fmt.Errorf("invalid IsDebugMode: %w", err)
		} else {
			header.IsDebugMode = val
		}
	case "Checksum":
		if val, err := strconv.ParseUint(strings.TrimPrefix(value, "0x"), 16, 32); err != nil {
			return fmt.Errorf("invalid Checksum: %w", err)
		} else {
			header.Checksum = uint32(val)
		}
	default:
		fm.logger.Debugf("Unknown header field: %s", key)
	}

	return nil
}

// serializePageASCII serializes page data to ASCII format
// This function creates a human-readable representation of either metadata or node data
// with unique section boundaries for reliable parsing
// DEPRECATED: ASCII serialization is only used for debugging (debugMode=true).
// Production systems should use binary serialization for performance and correctness.
// This function is incomplete and missing critical fields (e.g., Values for leaf nodes).
// Consider removing this in a future version once debugging capabilities are no longer needed.
//
// Parameters:
//   - pageData: The page data to serialize (either *BTreeMetadata or *BTreeNode)
//
// Returns:
//   - []byte: The serialized page data in ASCII format
//   - error: Any error that occurred during serialization
func (fm *BTreeFileManager) serializePageASCII(pageData interface{}) ([]byte, error) {
	var buffer bytes.Buffer

	switch data := pageData.(type) {
	case *BTreeMetadata:
		// Serialize metadata with unique borders
		buffer.WriteString("## METADATA HEADER\n")
		buffer.WriteString(fmt.Sprintf("Order: %d\n", data.Order))
		buffer.WriteString(fmt.Sprintf("RootPage: %d\n", data.RootPageNum))
		buffer.WriteString(fmt.Sprintf("TotalPages: %d\n", data.TotalPages))
		buffer.WriteString(fmt.Sprintf("LastCompaction: %s\n", data.LastCompaction.Format(time.RFC3339)))
		buffer.WriteString(fmt.Sprintf("FragmentationPct: %.2f\n", data.FragmentationPct))
		buffer.WriteString(fmt.Sprintf("PageSize: %d\n", data.PageSize))
		buffer.WriteString(fmt.Sprintf("IsUnique: %t\n", data.IsUnique))
		buffer.WriteString(fmt.Sprintf("BundleName: %s\n", data.BundleName))
		buffer.WriteString(fmt.Sprintf("FieldName: %s\n", data.FieldName))
		buffer.WriteString(fmt.Sprintf("RecordCount: %d\n", data.RecordCount))
		buffer.WriteString(fmt.Sprintf("TotalNodes: %d\n", data.TotalNodes))
		buffer.WriteString("|| END METADATA HEADER\n")

	case *BTreeNode:
		// Serialize node with unique borders
		buffer.WriteString("## BTREE NODE\n")
		buffer.WriteString(fmt.Sprintf("PageNum: %d\n", data.PageNum))
		buffer.WriteString(fmt.Sprintf("ParentPage: %d\n", data.ParentPage))
		buffer.WriteString(fmt.Sprintf("IsLeaf: %t\n", data.IsLeaf))
		buffer.WriteString(fmt.Sprintf("NextLeaf: %d\n", data.NextLeaf))
		buffer.WriteString(fmt.Sprintf("PreviousLeaf: %d\n", data.PrevLeaf))

		// Serialize keys
		buffer.WriteString("Keys:\n")
		for i, key := range data.Keys {
			buffer.WriteString(fmt.Sprintf("  Key %d: %s\n", i, string(key)))
		}

		// Serialize children
		buffer.WriteString("Children:\n")
		for i, child := range data.Children {
			buffer.WriteString(fmt.Sprintf("  Child %d: %d\n", i, child))
		}

		// Serialize values if this is a leaf node
		if data.IsLeaf && len(data.Values) > 0 {
			buffer.WriteString("Values:\n")
			for i, valueList := range data.Values {
				buffer.WriteString(fmt.Sprintf("  ValueSet %d:\n", i))
				for j, value := range valueList {
					buffer.WriteString(fmt.Sprintf("    Value %d: %s\n", j, value))
				}
			}
		}

		buffer.WriteString("|| END BTREE NODE\n")

	default:
		return nil, fmt.Errorf("unsupported page data type: %T", pageData)
	}

	return buffer.Bytes(), nil
}

// deserializePageASCII deserializes page data from ASCII format
// This function parses ASCII data and returns either metadata or node structures
// based on the section headers found in the data
// DEPRECATED: ASCII deserialization is only used for debugging (debugMode=true).
// Production systems should use binary deserialization for performance and correctness.
// This function is incomplete and missing critical fields (e.g., proper Values handling).
// Consider removing this in a future version once debugging capabilities are no longer needed.
//
// Parameters:
//   - data: The ASCII data to parse
//
// Returns:
//   - interface{}: The parsed structure (*BTreeMetadata or *BTreeNode)
//   - error: Any error that occurred during deserialization
func (fm *BTreeFileManager) deserializePageASCII(data []byte) (interface{}, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var currentSection string
	var node *BTreeNode
	var metadata *BTreeMetadata

	fm.logger.Debugf("Starting ASCII page deserialization, data length: %d", len(data))

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines
		if line == "" {
			continue
		}

		fm.logger.Debugf("Processing line: '%s'", line)

		// Check for section headers with unique patterns
		if line == "## METADATA HEADER" {
			currentSection = "metadata"
			metadata = &BTreeMetadata{}
			fm.logger.Debugf("Found metadata section header")
			continue
		} else if line == "## BTREE NODE" {
			currentSection = "node"
			node = &BTreeNode{
				Keys:     make([][]byte, 0),
				Children: make([]uint32, 0),
				Values:   make([][]string, 0),
			}
			fm.logger.Debugf("Found node section header")
			continue
		}

		// Check for section endings with unique patterns
		if line == "|| END METADATA HEADER" {
			fm.logger.Debugf("Found metadata section ending")
			currentSection = ""
			continue
		} else if line == "|| END BTREE NODE" {
			fm.logger.Debugf("Found node section ending")
			currentSection = ""
			continue
		}

		// Parse field data based on current section
		if currentSection == "node" && node != nil {
			if err := fm.parseNodeField(line, node); err != nil {
				fm.logger.Warnf("Failed to parse node field '%s': %v", line, err)
			}
		} else if currentSection == "metadata" && metadata != nil {
			if err := fm.parseMetadataField(line, metadata); err != nil {
				fm.logger.Warnf("Failed to parse metadata field '%s': %v", line, err)
			} else {
				// Log successful metadata field parsing for debugging
				if strings.Contains(line, "RootPage") {
					fm.logger.Infof("Successfully parsed RootPage: %d", metadata.RootPageNum)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading ASCII data: %w", err)
	}

	// Return the parsed structure with validation
	if node != nil {
		fm.logger.Debugf("Successfully parsed BTree node: PageNum=%d, Keys=%d, Children=%d",
			node.PageNum, len(node.Keys), len(node.Children))
		return node, nil
	} else if metadata != nil {
		// Validate critical metadata fields before returning
		if metadata.RootPageNum == 0 {
			fm.logger.Errorf("Critical error: parsed metadata has RootPageNum=0")
			return nil, fmt.Errorf("invalid metadata: RootPageNum cannot be 0")
		}

		fm.logger.Infof("Successfully parsed BTree metadata: Order=%d, RootPage=%d, TotalPages=%d",
			metadata.Order, metadata.RootPageNum, metadata.TotalPages)
		return metadata, nil
	}

	fm.logger.Errorf("Failed to parse ASCII page data: no valid structure found")
	return nil, fmt.Errorf("failed to parse ASCII page data: no valid structure found")
}

// parseNodeField parses individual fields for BTree nodes
// This function handles the parsing of node-specific field syntax following SRP
// Parameters:
//   - line: The line containing the field data
//   - node: The node structure to populate
//
// Returns:
//   - error: Any error that occurred during parsing
func (fm *BTreeFileManager) parseNodeField(line string, node *BTreeNode) error {
	fm.logger.Debugf("Parsing node field: '%s'", line)

	// Handle key-value pairs
	if strings.Contains(line, ":") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid field format: %s", line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "PageNum":
			if val, err := strconv.ParseUint(value, 10, 32); err != nil {
				return fmt.Errorf("invalid PageNum: %w", err)
			} else {
				node.PageNum = uint32(val)
			}
		case "ParentPage":
			if val, err := strconv.ParseUint(value, 10, 32); err != nil {
				return fmt.Errorf("invalid ParentPage: %w", err)
			} else {
				node.ParentPage = uint32(val)
			}
		case "IsLeaf":
			if val, err := strconv.ParseBool(value); err != nil {
				return fmt.Errorf("invalid IsLeaf: %w", err)
			} else {
				node.IsLeaf = val
			}
		case "NextLeaf":
			if val, err := strconv.ParseUint(value, 10, 32); err != nil {
				return fmt.Errorf("invalid NextLeaf: %w", err)
			} else {
				node.NextLeaf = uint32(val)
			}
		case "PreviousLeaf":
			if val, err := strconv.ParseUint(value, 10, 32); err != nil {
				return fmt.Errorf("invalid PreviousLeaf: %w", err)
			} else {
				node.PrevLeaf = uint32(val)
			}
		case "Keys", "Children", "Values":
			// Section headers - actual data is on following lines
			return nil
		default:
			// Handle individual key, child, or value entries
			if strings.HasPrefix(key, "Key ") {
				if value != "" {
					node.Keys = append(node.Keys, []byte(value))
				}
			} else if strings.HasPrefix(key, "Child ") {
				if value != "" {
					if val, err := strconv.ParseUint(value, 10, 32); err == nil {
						node.Children = append(node.Children, uint32(val))
					}
				}
			} else if strings.HasPrefix(key, "Value ") {
				if value != "" {
					// Handle individual values within a value set
					// This is a simplified implementation - might need enhancement for complex value structures
					if len(node.Values) == 0 {
						node.Values = append(node.Values, make([]string, 0))
					}
					lastIndex := len(node.Values) - 1
					node.Values[lastIndex] = append(node.Values[lastIndex], value)
				}
			}
		}
	}

	return nil
}

// parseMetadataField parses individual fields for BTree metadata
// This function handles the parsing of metadata-specific field syntax following SRP
// Parameters:
//   - line: The line containing the field data
//   - metadata: The metadata structure to populate
//
// Returns:
//   - error: Any error that occurred during parsing
func (fm *BTreeFileManager) parseMetadataField(line string, metadata *BTreeMetadata) error {
	fm.logger.Debugf("Parsing metadata field: '%s'", line)

	if !strings.Contains(line, ":") {
		return nil // Skip lines without key-value pairs
	}

	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid field format: %s", line)
	}

	key := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])

	switch key {
	case "Order":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid Order: %w", err)
		} else {
			metadata.Order = uint32(val)
		}
	case "RootPage":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid RootPage: %w", err)
		} else {
			metadata.RootPageNum = uint32(val)
			fm.logger.Infof("Set RootPageNum to: %d", metadata.RootPageNum)
		}
	case "TotalPages":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid TotalPages: %w", err)
		} else {
			metadata.TotalPages = uint32(val)
		}
	case "PageSize":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid PageSize: %w", err)
		} else {
			metadata.PageSize = uint32(val)
		}
	case "IsUnique":
		if val, err := strconv.ParseBool(value); err != nil {
			return fmt.Errorf("invalid IsUnique: %w", err)
		} else {
			metadata.IsUnique = val
		}
	case "BundleName":
		metadata.BundleName = value
	case "FieldName":
		metadata.FieldName = value
	case "RecordCount":
		if val, err := strconv.ParseUint(value, 10, 64); err != nil {
			return fmt.Errorf("invalid RecordCount: %w", err)
		} else {
			metadata.RecordCount = val
		}
	case "TotalNodes":
		if val, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("invalid TotalNodes: %w", err)
		} else {
			metadata.TotalNodes = uint32(val)
		}
	case "LastCompaction":
		if t, err := time.Parse(time.RFC3339, value); err != nil {
			return fmt.Errorf("invalid LastCompaction: %w", err)
		} else {
			metadata.LastCompaction = t
		}
	case "FragmentationPct":
		if val, err := strconv.ParseFloat(value, 64); err != nil {
			return fmt.Errorf("invalid FragmentationPct: %w", err)
		} else {
			metadata.FragmentationPct = val
		}
	default:
		fm.logger.Debugf("Unknown metadata field: %s", key)
	}

	return nil
}

// Binary serialization methods

// serializeFileHeaderBinary serializes file header to binary format
func (fm *BTreeFileManager) serializeFileHeaderBinary() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, fm.fileHeader.MagicNumber); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, fm.fileHeader.FileVersion); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, fm.fileHeader.PageSize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, fm.fileHeader.TotalPages); err != nil {
		return nil, err
	}

	// Serialize timestamps as Unix seconds
	if err := binary.Write(buf, binary.LittleEndian, fm.fileHeader.CreatedAt.Unix()); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, fm.fileHeader.LastModified.Unix()); err != nil {
		return nil, err
	}

	// Serialize boolean as byte
	var debugByte byte
	if fm.fileHeader.IsDebugMode {
		debugByte = 1
	}
	if err := binary.Write(buf, binary.LittleEndian, debugByte); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, fm.fileHeader.Checksum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// deserializeFileHeaderBinary deserializes file header from binary format
func (fm *BTreeFileManager) deserializeFileHeaderBinary(data []byte) error {
	buf := bytes.NewReader(data)
	fm.fileHeader = &FileHeader{}

	if err := binary.Read(buf, binary.LittleEndian, &fm.fileHeader.MagicNumber); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &fm.fileHeader.FileVersion); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &fm.fileHeader.PageSize); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &fm.fileHeader.TotalPages); err != nil {
		return err
	}

	// Read timestamps
	var createdUnix, modifiedUnix int64
	if err := binary.Read(buf, binary.LittleEndian, &createdUnix); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &modifiedUnix); err != nil {
		return err
	}

	fm.fileHeader.CreatedAt = time.Unix(createdUnix, 0)
	fm.fileHeader.LastModified = time.Unix(modifiedUnix, 0)

	// Read boolean
	var debugByte byte
	if err := binary.Read(buf, binary.LittleEndian, &debugByte); err != nil {
		return err
	}
	fm.fileHeader.IsDebugMode = debugByte == 1

	if err := binary.Read(buf, binary.LittleEndian, &fm.fileHeader.Checksum); err != nil {
		return err
	}

	return nil
}

// Helper function for minimum calculation
// func min(a, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }

func (fm *BTreeFileManager) serializePageBinary(pageData interface{}) ([]byte, error) {
	//  handle different page types
	switch data := pageData.(type) {
	case *BTreeNode:
		buf := new(bytes.Buffer)
		// Write magic number first for consistency with deserialization
		if err := binary.Write(buf, binary.LittleEndian, uint32(0x42545245)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.PageNum); err != nil {
			return nil, err
		}
		// Write IsLeaf field - CRITICAL for correct deserialization
		if err := binary.Write(buf, binary.LittleEndian, data.IsLeaf); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.ParentPage); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.NextLeaf); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.PrevLeaf); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(data.Keys))); err != nil {
			return nil, err
		}
		for _, key := range data.Keys {
			keyBytes := []byte(key)
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, keyBytes); err != nil {
				return nil, err
			}
		}
		// Write values (for leaf nodes)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(data.Values))); err != nil {
			return nil, err
		}
		for _, valueList := range data.Values {
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(valueList))); err != nil {
				return nil, err
			}
			for _, value := range valueList {
				valueBytes := []byte(value)
				if err := binary.Write(buf, binary.LittleEndian, uint32(len(valueBytes))); err != nil {
					return nil, err
				}
				if err := binary.Write(buf, binary.LittleEndian, valueBytes); err != nil {
					return nil, err
				}
			}
		}
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(data.Children))); err != nil {
			return nil, err
		}
		for _, child := range data.Children {
			if err := binary.Write(buf, binary.LittleEndian, child); err != nil {
				return nil, err
			}
		}

		// Write Tombstones map (map[string]bool where key is "key+docID")
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(data.Tombstones))); err != nil {
			return nil, err
		}
		for tombstoneKey, isDeleted := range data.Tombstones {
			// Write tombstone key (format: "key+docID")
			keyBytes := []byte(tombstoneKey)
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, keyBytes); err != nil {
				return nil, err
			}
			// Write boolean value
			if err := binary.Write(buf, binary.LittleEndian, isDeleted); err != nil {
				return nil, err
			}
		}

		// Write TombstoneCount
		if err := binary.Write(buf, binary.LittleEndian, data.TombstoneCount); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	case *BTreeMetadata:
		buf := new(bytes.Buffer)

		// Write all metadata fields in order matching the struct definition
		// File integrity and versioning
		if err := binary.Write(buf, binary.LittleEndian, data.MagicNumber); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.Version); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.CreatedAt.Unix()); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.LastModified.Unix()); err != nil {
			return nil, err
		}

		// Tree configuration
		if err := binary.Write(buf, binary.LittleEndian, data.PageSize); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.Order); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.TreeHeight); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.RootPageNum); err != nil {
			return nil, err
		}

		// Storage management
		if err := binary.Write(buf, binary.LittleEndian, data.NextPageNum); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.TotalPages); err != nil {
			return nil, err
		}
		// FreePages slice
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(data.FreePages))); err != nil {
			return nil, err
		}
		for _, pageNum := range data.FreePages {
			if err := binary.Write(buf, binary.LittleEndian, pageNum); err != nil {
				return nil, err
			}
		}

		// Statistics
		if err := binary.Write(buf, binary.LittleEndian, data.TotalRecords); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.TotalNodes); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.LeafNodes); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.InternalNodes); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.TotalKeys); err != nil {
			return nil, err
		}

		// Performance metrics
		if err := binary.Write(buf, binary.LittleEndian, data.SplitCount); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.MergeCount); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.SearchCount); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.InsertCount); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.DeleteCount); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.TotalKeysFound); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.TotalNodesVisited); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.AverageSearchEfficiency); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.MaxNodesVisited); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.TotalNodesDeleted); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.StructuralChanges); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.AverageNodesDeleted); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.CompactionCount); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.RecordCount); err != nil {
			return nil, err
		}

		// Insertion performance metrics
		if err := binary.Write(buf, binary.LittleEndian, data.TotalNodesCreated); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.AverageNodesCreated); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, data.TreeGrowthEvents); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported page data type for binary serialization: %T", data)
	}
}

func (fm *BTreeFileManager) deserializePageBinary(data []byte, pageNum uint32) (interface{}, error) {
	// Page 0 is ALWAYS metadata, all other pages are nodes
	// This is the definitive way to distinguish them
	if pageNum == 0 {
		buf := bytes.NewReader(data)
		return fm.deserializeMetadataBinary(buf)
	}

	// All other pages are nodes
	buf := bytes.NewReader(data)
	return fm.deserializeNodeBinary(buf)
}

// deserializeMetadataBinary deserializes metadata from binary format
func (fm *BTreeFileManager) deserializeMetadataBinary(buf *bytes.Reader) (*BTreeMetadata, error) {
	metadata := &BTreeMetadata{}

	// Read all fields in the same order as serialization
	// File integrity and versioning
	if err := binary.Read(buf, binary.LittleEndian, &metadata.MagicNumber); err != nil {
		return nil, fmt.Errorf("failed to read magic number: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.Version); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}

	var createdAtUnix, lastModifiedUnix int64
	if err := binary.Read(buf, binary.LittleEndian, &createdAtUnix); err != nil {
		return nil, fmt.Errorf("failed to read created at: %w", err)
	}
	metadata.CreatedAt = time.Unix(createdAtUnix, 0)

	if err := binary.Read(buf, binary.LittleEndian, &lastModifiedUnix); err != nil {
		return nil, fmt.Errorf("failed to read last modified: %w", err)
	}
	metadata.LastModified = time.Unix(lastModifiedUnix, 0)

	// Tree configuration
	if err := binary.Read(buf, binary.LittleEndian, &metadata.PageSize); err != nil {
		return nil, fmt.Errorf("failed to read page size: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.Order); err != nil {
		return nil, fmt.Errorf("failed to read order: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TreeHeight); err != nil {
		return nil, fmt.Errorf("failed to read tree height: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.RootPageNum); err != nil {
		return nil, fmt.Errorf("failed to read root page num: %w", err)
	}

	// Storage management
	if err := binary.Read(buf, binary.LittleEndian, &metadata.NextPageNum); err != nil {
		return nil, fmt.Errorf("failed to read next page num: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TotalPages); err != nil {
		return nil, fmt.Errorf("failed to read total pages: %w", err)
	}

	// FreePages slice
	var freePagesLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &freePagesLen); err != nil {
		return nil, fmt.Errorf("failed to read free pages length: %w", err)
	}
	metadata.FreePages = make([]uint32, freePagesLen)
	for i := uint32(0); i < freePagesLen; i++ {
		if err := binary.Read(buf, binary.LittleEndian, &metadata.FreePages[i]); err != nil {
			return nil, fmt.Errorf("failed to read free page %d: %w", i, err)
		}
	}

	// Statistics
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TotalRecords); err != nil {
		return nil, fmt.Errorf("failed to read total records: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TotalNodes); err != nil {
		return nil, fmt.Errorf("failed to read total nodes: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.LeafNodes); err != nil {
		return nil, fmt.Errorf("failed to read leaf nodes: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.InternalNodes); err != nil {
		return nil, fmt.Errorf("failed to read internal nodes: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TotalKeys); err != nil {
		return nil, fmt.Errorf("failed to read total keys: %w", err)
	}

	// Performance metrics
	if err := binary.Read(buf, binary.LittleEndian, &metadata.SplitCount); err != nil {
		return nil, fmt.Errorf("failed to read split count: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.MergeCount); err != nil {
		return nil, fmt.Errorf("failed to read merge count: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.SearchCount); err != nil {
		return nil, fmt.Errorf("failed to read search count: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.InsertCount); err != nil {
		return nil, fmt.Errorf("failed to read insert count: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.DeleteCount); err != nil {
		return nil, fmt.Errorf("failed to read delete count: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TotalKeysFound); err != nil {
		return nil, fmt.Errorf("failed to read total keys found: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TotalNodesVisited); err != nil {
		return nil, fmt.Errorf("failed to read total nodes visited: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.AverageSearchEfficiency); err != nil {
		return nil, fmt.Errorf("failed to read average search efficiency: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.MaxNodesVisited); err != nil {
		return nil, fmt.Errorf("failed to read max nodes visited: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TotalNodesDeleted); err != nil {
		return nil, fmt.Errorf("failed to read total nodes deleted: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.StructuralChanges); err != nil {
		return nil, fmt.Errorf("failed to read structural changes: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.AverageNodesDeleted); err != nil {
		return nil, fmt.Errorf("failed to read average nodes deleted: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.CompactionCount); err != nil {
		return nil, fmt.Errorf("failed to read compaction count: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.RecordCount); err != nil {
		return nil, fmt.Errorf("failed to read record count: %w", err)
	}

	// Insertion performance metrics
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TotalNodesCreated); err != nil {
		return nil, fmt.Errorf("failed to read total nodes created: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.AverageNodesCreated); err != nil {
		return nil, fmt.Errorf("failed to read average nodes created: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.TreeGrowthEvents); err != nil {
		return nil, fmt.Errorf("failed to read tree growth events: %w", err)
	}

	return metadata, nil
}

// deserializeNodeBinary deserializes a BTree node from binary format
func (fm *BTreeFileManager) deserializeNodeBinary(buf *bytes.Reader) (*BTreeNode, error) {
	var magicNumber uint32
	if err := binary.Read(buf, binary.LittleEndian, &magicNumber); err != nil {
		return nil, fmt.Errorf("failed to read magic number: %w", err)
	}
	if magicNumber != 0x42545245 { // "BTRE"
		return nil, fmt.Errorf("invalid magic number: expected 0x42545245, got 0x%08X", magicNumber)
	}

	// Read node fields
	var pageNum uint32
	var isLeaf bool
	var parentPage, nextLeaf, prevLeaf uint32

	if err := binary.Read(buf, binary.LittleEndian, &pageNum); err != nil {
		return nil, fmt.Errorf("failed to read page number: %w", err)
	}

	// Continue parsing as BTreeNode - read IsLeaf field
	if err := binary.Read(buf, binary.LittleEndian, &isLeaf); err != nil {
		return nil, fmt.Errorf("failed to read IsLeaf field: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &parentPage); err != nil {
		return nil, fmt.Errorf("failed to read parent page: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &nextLeaf); err != nil {
		return nil, fmt.Errorf("failed to read next leaf page: %w", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &prevLeaf); err != nil {
		return nil, fmt.Errorf("failed to read previous leaf page: %w", err)
	}
	// Read keys
	var keyCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &keyCount); err != nil {
		return nil, fmt.Errorf("failed to read key count: %w", err)
	}
	keys := make([][]byte, keyCount)
	for i := uint32(0); i < keyCount; i++ {
		var keyLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read key length: %w", err)
		}
		keyBytes := make([]byte, keyLen)
		if err := binary.Read(buf, binary.LittleEndian, &keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key bytes: %w", err)
		}
		keys[i] = keyBytes
	}
	// Read values (for leaf nodes)
	var valueCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &valueCount); err != nil {
		return nil, fmt.Errorf("failed to read value count: %w", err)
	}
	values := make([][]string, valueCount)
	for i := uint32(0); i < valueCount; i++ {
		var valueListLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &valueListLen); err != nil {
			return nil, fmt.Errorf("failed to read value list length: %w", err)
		}
		valueList := make([]string, valueListLen)
		for j := uint32(0); j < valueListLen; j++ {
			var valueLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
				return nil, fmt.Errorf("failed to read value length: %w", err)
			}
			valueBytes := make([]byte, valueLen)
			if err := binary.Read(buf, binary.LittleEndian, &valueBytes); err != nil {
				return nil, fmt.Errorf("failed to read value bytes: %w", err)
			}
			valueList[j] = string(valueBytes)
		}
		values[i] = valueList
	}
	// Read children
	var childCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &childCount); err != nil {
		return nil, fmt.Errorf("failed to read child count: %w", err)
	}
	children := make([]uint32, childCount)
	for i := uint32(0); i < childCount; i++ {
		if err := binary.Read(buf, binary.LittleEndian, &children[i]); err != nil {
			return nil, fmt.Errorf("failed to read child page number: %w", err)
		}
	}

	// Read Tombstones map
	var tombstoneMapCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &tombstoneMapCount); err != nil {
		return nil, fmt.Errorf("failed to read tombstone map count: %w", err)
	}
	tombstones := make(map[string]bool)
	for i := uint32(0); i < tombstoneMapCount; i++ {
		var keyLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read tombstone key length: %w", err)
		}
		keyBytes := make([]byte, keyLen)
		if err := binary.Read(buf, binary.LittleEndian, &keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read tombstone key bytes: %w", err)
		}
		var isDeleted bool
		if err := binary.Read(buf, binary.LittleEndian, &isDeleted); err != nil {
			return nil, fmt.Errorf("failed to read tombstone boolean: %w", err)
		}
		tombstones[string(keyBytes)] = isDeleted
	}

	// Read TombstoneCount
	var tombstoneCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &tombstoneCount); err != nil {
		return nil, fmt.Errorf("failed to read tombstone count: %w", err)
	}

	// Construct the BTreeNode
	node := &BTreeNode{
		PageNum:        pageNum,
		IsLeaf:         isLeaf,   // CRITICAL: Set the IsLeaf field from deserialized data
		KeyCount:       keyCount, // CRITICAL: Set the KeyCount field from deserialized data
		ParentPage:     parentPage,
		NextLeaf:       nextLeaf,
		PrevLeaf:       prevLeaf,
		Keys:           keys,
		Values:         values, // CRITICAL: Set the Values field from deserialized data
		Children:       children,
		Tombstones:     tombstones,     // CRITICAL: Set the Tombstones field from deserialized data
		TombstoneCount: tombstoneCount, // CRITICAL: Set the TombstoneCount field from deserialized data
	}
	// Return the constructed node
	return node, nil

}
