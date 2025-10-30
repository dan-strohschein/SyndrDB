package hashindexV3

/*
HEADER MANAGER - HIGH-LEVEL HEADER OPERATIONS

This file provides high-level operations for managing index headers,
including creation, reading, updating, and validation. It coordinates
between the serializer and file system operations.

KEY RESPONSIBILITIES:
- Create new headers for index files
- Read headers from existing files
- Update headers (statistics, metadata)
- Validate header consistency
- Provide header caching (future)

DESIGN PRINCIPLES:
- Single Responsibility: Only coordinates header operations
- Open/Closed: Extensible for future header features
- Dependency Inversion: Uses HeaderSerializer abstraction
- DRY: Centralizes all header management logic

USAGE PATTERNS:
1. Creating new index file:
   - CreateHeader() → generates header with metadata
   - WriteHeaderToFile() → writes to new file

2. Opening existing index file:
   - ReadHeaderFromFile() → loads and validates header
   - Cache header for quick access

3. Updating statistics:
   - UpdateStatistics() → modifies header fields
   - WriteHeaderToFile() → persists changes

4. File rotation:
   - CreateHeader() with incremented file number
   - Copy relevant metadata from old header

HEADER LIFECYCLE:
┌──────────────────────────────────────────────────────┐
│ 1. CREATE: New index file creation                  │
│    → CreateHeader() → WriteHeader()                 │
├──────────────────────────────────────────────────────┤
│ 2. OPEN: Index file opening                         │
│    → ReadHeader() → Validate() → Cache              │
├──────────────────────────────────────────────────────┤
│ 3. UPDATE: Periodic statistics updates              │
│    → UpdateStatistics() → WriteHeader()             │
├──────────────────────────────────────────────────────┤
│ 4. ROTATE: File rotation                            │
│    → CreateHeader(fileNum+1) → WriteHeader()        │
└──────────────────────────────────────────────────────┘

TODO: Future extensions
- Header caching layer to avoid repeated file reads
- Background header statistics updates
- Header versioning for migrations
- Header repair utilities
*/

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
)

// HeaderManager coordinates high-level header operations
// Follows Single Responsibility Principle: Manages header lifecycle
type HeaderManager struct {
	serializer *HeaderSerializer
	logger     *zap.SugaredLogger
}

// NewHeaderManager creates a new header manager
func NewHeaderManager(logger *zap.SugaredLogger) *HeaderManager {
	return &HeaderManager{
		serializer: NewHeaderSerializer(),
		logger:     logger,
	}
}

// CreateHeader creates a new IndexHeader for a new index file
// Parameters:
//   - indexName: Full index name (e.g., "UserID_fk")
//   - fieldName: Field being indexed (e.g., "UserID")
//   - bundleName: Bundle this index belongs to
//   - fileNumber: Sequence number for this file (0, 1, 2, ...)
//   - isForeignKey: True if this is a foreign key index
//   - isUnique: True if index enforces uniqueness
//   - isPrimaryKey: True if this is the primary key index
//
// Returns: *IndexHeader ready to be written to file
func (hm *HeaderManager) CreateHeader(
	indexName, fieldName, bundleName string,
	fileNumber int,
	isForeignKey, isUnique, isPrimaryKey bool,
) *IndexHeader {
	header := NewIndexHeader(indexName, fieldName, bundleName, isForeignKey, isUnique, isPrimaryKey)
	header.FileNumber = fileNumber

	hm.logger.Debugf("Created header for index %s, field %s, file %d (FK=%v, Unique=%v, PK=%v)",
		indexName, fieldName, fileNumber, isForeignKey, isUnique, isPrimaryKey)

	return header
}

// CreateHeaderWithRelationship creates a header for a foreign key index with relationship info
// Parameters:
//   - Same as CreateHeader, plus:
//   - referencedBundle: Target bundle name for foreign key
//   - referencedField: Target field name for foreign key
func (hm *HeaderManager) CreateHeaderWithRelationship(
	indexName, fieldName, bundleName string,
	fileNumber int,
	isUnique, isPrimaryKey bool,
	referencedBundle, referencedField string,
) *IndexHeader {
	header := hm.CreateHeader(indexName, fieldName, bundleName, fileNumber, true, isUnique, isPrimaryKey)
	header.ReferencedBundle = referencedBundle
	header.ReferencedField = referencedField

	hm.logger.Debugf("Created FK header for index %s → %s.%s",
		indexName, referencedBundle, referencedField)

	return header
}

// ReadHeader reads and validates a header from an index file
// Parameters:
//   - filePath: Full path to the index file
//
// Returns:
//   - *IndexHeader: Deserialized and validated header
//   - int64: Offset where entry data begins (after header)
//   - error: If reading or validation fails
func (hm *HeaderManager) ReadHeader(filePath string) (*IndexHeader, int64, error) {
	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Read header using serializer
	header, bytesRead, err := hm.serializer.ReadHeaderFromFile(file)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read header from %s: %w", filePath, err)
	}

	// Additional validation
	if err := hm.ValidateHeader(header); err != nil {
		return nil, 0, fmt.Errorf("header validation failed for %s: %w", filePath, err)
	}

	hm.logger.Debugf("Read header from %s: index=%s, field=%s, entries=%d, size=%d bytes",
		filePath, header.IndexName, header.FieldName, header.TotalEntries, bytesRead)

	return header, bytesRead, nil
}

// WriteHeader writes a header to an index file
// Parameters:
//   - filePath: Full path to the index file
//   - header: Header to write
//
// Returns:
//   - int64: Number of bytes written
//   - error: If writing fails
func (hm *HeaderManager) WriteHeader(filePath string, header *IndexHeader) (int64, error) {
	// Calculate header size before validation (required for validation to pass)
	headerSize, err := hm.serializer.CalculateHeaderSize(header)
	if err != nil {
		return 0, fmt.Errorf("failed to calculate header size: %w", err)
	}
	header.HeaderSize = headerSize

	// Validate before writing
	if err := hm.ValidateHeader(header); err != nil {
		return 0, fmt.Errorf("invalid header: %w", err)
	}

	// Open file for writing (create if doesn't exist)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Write header using serializer
	bytesWritten, err := hm.serializer.WriteHeaderToFile(file, header)
	if err != nil {
		return 0, fmt.Errorf("failed to write header to %s: %w", filePath, err)
	}

	hm.logger.Debugf("Wrote header to %s: index=%s, field=%s, %d bytes",
		filePath, header.IndexName, header.FieldName, bytesWritten)

	return bytesWritten, nil
}

// UpdateStatistics updates header statistics and writes to file
// Parameters:
//   - filePath: Full path to the index file
//   - totalEntries: New total entry count
//   - deletedEntries: New deleted entry count
//   - fileSize: Current file size
//   - globalSequence: Latest sequence number
//
// Returns: error if update fails
func (hm *HeaderManager) UpdateStatistics(
	filePath string,
	totalEntries, deletedEntries uint64,
	fileSize int64,
	globalSequence uint64,
) error {
	// Read current header
	header, _, err := hm.ReadHeader(filePath)
	if err != nil {
		return fmt.Errorf("failed to read header for update: %w", err)
	}

	// Update statistics
	header.TotalEntries = totalEntries
	header.DeletedEntries = deletedEntries
	header.FileSize = fileSize
	header.GlobalSequence = globalSequence

	// Write updated header
	if _, err := hm.WriteHeader(filePath, header); err != nil {
		return fmt.Errorf("failed to write updated header: %w", err)
	}

	hm.logger.Debugf("Updated statistics in %s: entries=%d, deleted=%d, size=%d, seq=%d",
		filePath, totalEntries, deletedEntries, fileSize, globalSequence)

	return nil
}

// UpdateCompactionTime updates the LastCompactedAt timestamp
func (hm *HeaderManager) UpdateCompactionTime(filePath string, compactionTime time.Time) error {
	header, _, err := hm.ReadHeader(filePath)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	header.LastCompactedAt = compactionTime

	if _, err := hm.WriteHeader(filePath, header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	hm.logger.Debugf("Updated compaction time in %s to %s", filePath, compactionTime)
	return nil
}

// ValidateHeader performs comprehensive validation on a header
func (hm *HeaderManager) ValidateHeader(header *IndexHeader) error {
	// Use header's built-in validation
	if err := header.Validate(); err != nil {
		return err
	}

	// Additional business logic validation
	if header.TotalEntries < header.DeletedEntries {
		return fmt.Errorf("deleted entries (%d) cannot exceed total entries (%d)",
			header.DeletedEntries, header.TotalEntries)
	}

	if header.FileSize < 0 {
		return fmt.Errorf("file size cannot be negative: %d", header.FileSize)
	}

	return nil
}

// ValidateFileHeader validates that a file's header matches expected metadata
// Useful for detecting corruption or mismatched files
func (hm *HeaderManager) ValidateFileHeader(filePath string, expectedFieldName string, expectedIsForeignKey bool) error {
	header, _, err := hm.ReadHeader(filePath)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	if header.FieldName != expectedFieldName {
		return fmt.Errorf("field name mismatch: expected %s, got %s", expectedFieldName, header.FieldName)
	}

	if header.IsForeignKey != expectedIsForeignKey {
		return fmt.Errorf("foreign key flag mismatch: expected %v, got %v", expectedIsForeignKey, header.IsForeignKey)
	}

	return nil
}

// GetHeaderInfo returns a human-readable summary of a header
// Useful for debugging and logging
func (hm *HeaderManager) GetHeaderInfo(filePath string) (string, error) {
	header, _, err := hm.ReadHeader(filePath)
	if err != nil {
		return "", err
	}

	info := fmt.Sprintf(
		"Index: %s | Field: %s | Bundle: %s | FK: %v | Unique: %v | PK: %v | "+
			"File: %d | Entries: %d | Deleted: %d | Size: %d bytes | Seq: %d | "+
			"Created: %s",
		header.IndexName, header.FieldName, header.BundleName,
		header.IsForeignKey, header.IsUnique, header.IsPrimaryKey,
		header.FileNumber, header.TotalEntries, header.DeletedEntries,
		header.FileSize, header.GlobalSequence, header.CreatedAt.Format(time.RFC3339),
	)

	if header.IsForeignKey && header.ReferencedBundle != "" {
		info += fmt.Sprintf(" | References: %s.%s", header.ReferencedBundle, header.ReferencedField)
	}

	return info, nil
}

// CloneHeaderForRotation creates a new header for file rotation
// Copies relevant metadata but resets file-specific fields
func (hm *HeaderManager) CloneHeaderForRotation(oldHeader *IndexHeader, newFileNumber int) *IndexHeader {
	newHeader := &IndexHeader{
		Magic:              IndexFileMagic,
		Version:            IndexFileVersion,
		IndexName:          oldHeader.IndexName,
		FieldName:          oldHeader.FieldName,
		BundleName:         oldHeader.BundleName,
		IsForeignKey:       oldHeader.IsForeignKey,
		IsUnique:           oldHeader.IsUnique,
		IsPrimaryKey:       oldHeader.IsPrimaryKey,
		IndexType:          oldHeader.IndexType,
		FileNumber:         newFileNumber,
		CreatedAt:          time.Now(),               // New creation time
		LastCompactedAt:    time.Time{},              // Reset
		TotalEntries:       0,                        // Reset for new file
		DeletedEntries:     0,                        // Reset
		FileSize:           0,                        // Reset
		GlobalSequence:     oldHeader.GlobalSequence, // Continue sequence
		ReferencedBundle:   oldHeader.ReferencedBundle,
		ReferencedField:    oldHeader.ReferencedField,
		HeaderSize:         0, // Will be calculated during serialization
		CompressionEnabled: oldHeader.CompressionEnabled,
		BloomFilterEnabled: oldHeader.BloomFilterEnabled,
		ChecksumAlgorithm:  oldHeader.ChecksumAlgorithm,
	}

	hm.logger.Debugf("Cloned header for rotation: %s file %d → file %d",
		oldHeader.IndexName, oldHeader.FileNumber, newFileNumber)

	return newHeader
}

// GetHeaderSize reads only the header size from a file without deserializing the full header
// This is more efficient than ReadHeader() when only the size is needed (e.g., for seeking)
// Parameters:
//   - filePath: Full path to the index file
//
// Returns:
//   - int64: Header size in bytes (offset where entry data begins)
//   - error: If reading fails or header is invalid
func (hm *HeaderManager) GetHeaderSize(filePath string) (int64, error) {
	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Use serializer to read just the header size
	headerSize, err := hm.serializer.ReadHeaderSize(file)
	if err != nil {
		return 0, fmt.Errorf("failed to read header size from %s: %w", filePath, err)
	}

	return headerSize, nil
}
