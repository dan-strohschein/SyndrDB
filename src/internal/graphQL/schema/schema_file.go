package schema

// schema_file.go
//
// This file implements GraphQL schema file operations for SyndrDB.
// It provides the core I/O layer for reading and writing schema files,
// managing the file format defined in file_header.go and schema_record.go.
//
// Key capabilities:
// - Create new schema files with proper headers
// - Open and validate existing schema files
// - Append new schema records (versioning)
// - Read all active schemas
// - Validate file integrity
//
// Design Principles:
// - Single Responsibility: Handles only file I/O operations
// - Open/Closed: Extensible through record payload without breaking format
// - DRY: Reuses header and record serialization

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SchemaFile manages a GraphQL schema file for a database
type SchemaFile struct {
	// File metadata
	FilePath string      // Full path to the schema file
	Header   *FileHeader // File header

	// File handle
	file *os.File
	mu   sync.RWMutex // Protects concurrent access
}

// CreateSchemaFile creates a new schema file with the given header
func CreateSchemaFile(filePath string, databaseName, databaseID string) (*SchemaFile, error) {
	// Check if file already exists
	if _, err := os.Stat(filePath); err == nil {
		return nil, fmt.Errorf("schema file already exists: %s", filePath)
	}

	// Create directory if needed
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create the file
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Create header
	header := NewFileHeader(databaseName, databaseID)

	// Serialize and write header
	headerBytes, err := header.Serialize()
	if err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, fmt.Errorf("failed to serialize header: %w", err)
	}

	if _, err := file.Write(headerBytes[:]); err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(filePath)
		return nil, fmt.Errorf("failed to sync file: %w", err)
	}

	return &SchemaFile{
		FilePath: filePath,
		Header:   header,
		file:     file,
	}, nil
}

// OpenSchemaFile opens an existing schema file
func OpenSchemaFile(filePath string) (*SchemaFile, error) {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("schema file does not exist: %s", filePath)
	}

	// Open the file
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Read header
	headerBytes := make([]byte, FileHeaderSize)
	n, err := file.Read(headerBytes)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	if n != FileHeaderSize {
		file.Close()
		return nil, fmt.Errorf("incomplete header: expected %d bytes, got %d", FileHeaderSize, n)
	}

	// Deserialize header
	header := &FileHeader{}
	var headerArray [FileHeaderSize]byte
	copy(headerArray[:], headerBytes)
	if err := header.Deserialize(headerArray[:]); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	// Validate header
	if err := header.Validate(); err != nil {
		file.Close()
		return nil, fmt.Errorf("invalid header: %w", err)
	}

	return &SchemaFile{
		FilePath: filePath,
		Header:   header,
		file:     file,
	}, nil
}

// AppendSchema adds a new schema record to the file
func (sf *SchemaFile) AppendSchema(record *SchemaRecord) error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Validate record
	if err := record.Validate(); err != nil {
		return fmt.Errorf("invalid record: %w", err)
	}

	// Serialize record
	recordBytes, err := record.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize record: %w", err)
	}

	// Seek to end of file
	if _, err := sf.file.Seek(0, 2); err != nil {
		return fmt.Errorf("failed to seek to end: %w", err)
	}

	// Write record
	if _, err := sf.file.Write(recordBytes); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	// Update header
	sf.Header.TotalRecords++
	if record.IsActive() {
		sf.Header.ActiveRecords++
	} else if record.IsTombstone() {
		sf.Header.TombstoneCount++
	}
	sf.Header.UpdatedAt = record.UpdatedAt

	// Write updated header
	if err := sf.writeHeader(); err != nil {
		return fmt.Errorf("failed to update header: %w", err)
	}

	// Sync to disk
	if err := sf.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// ReadAllSchemas reads all records from the file
func (sf *SchemaFile) ReadAllSchemas() ([]*SchemaRecord, error) {
	sf.mu.RLock()
	defer sf.mu.RUnlock()

	// Seek past header
	if _, err := sf.file.Seek(FileHeaderSize, 0); err != nil {
		return nil, fmt.Errorf("failed to seek past header: %w", err)
	}

	records := make([]*SchemaRecord, 0, sf.Header.TotalRecords)

	// Read records until EOF
	for {
		// Read record size first (4 bytes)
		sizeBytes := make([]byte, 4)
		n, err := sf.file.Read(sizeBytes)
		if err != nil {
			if err.Error() == "EOF" {
				break // Normal end of file
			}
			return nil, fmt.Errorf("failed to read record size: %w", err)
		}
		if n == 0 {
			break // End of file
		}
		if n != 4 {
			return nil, fmt.Errorf("incomplete record size: expected 4 bytes, got %d", n)
		}

		// Parse record size
		recordSize := binary.LittleEndian.Uint32(sizeBytes)
		if recordSize < FixedRecordHeaderSize {
			return nil, fmt.Errorf("invalid record size: %d (minimum: %d)", recordSize, FixedRecordHeaderSize)
		}

		// Read full record
		recordBytes := make([]byte, recordSize)
		copy(recordBytes[0:4], sizeBytes) // Copy size we already read
		n, err = sf.file.Read(recordBytes[4:])
		if err != nil {
			return nil, fmt.Errorf("failed to read record: %w", err)
		}
		if n != int(recordSize)-4 {
			return nil, fmt.Errorf("incomplete record: expected %d bytes, got %d", recordSize-4, n)
		}

		// Deserialize record
		record := &SchemaRecord{}
		if err := record.Deserialize(recordBytes); err != nil {
			return nil, fmt.Errorf("failed to deserialize record: %w", err)
		}

		records = append(records, record)
	}

	return records, nil
}

// ReadActiveSchemas reads only active (non-tombstoned) records
// A schema is considered active only if there's an active record and NO tombstone for that bundle+version
func (sf *SchemaFile) ReadActiveSchemas() ([]*SchemaRecord, error) {
	allRecords, err := sf.ReadAllSchemas()
	if err != nil {
		return nil, err
	}

	// Build a map of tombstoned bundle+version combinations
	tombstoned := make(map[string]bool)
	for _, record := range allRecords {
		if record.IsTombstone() {
			key := record.GetBundleName() + ":" + fmt.Sprintf("%d", record.SchemaVersion)
			tombstoned[key] = true
		}
	}

	// Filter active records, excluding any that have been tombstoned
	activeRecords := make([]*SchemaRecord, 0, sf.Header.ActiveRecords)
	for _, record := range allRecords {
		if record.IsActive() {
			key := record.GetBundleName() + ":" + fmt.Sprintf("%d", record.SchemaVersion)
			if !tombstoned[key] {
				activeRecords = append(activeRecords, record)
			}
		}
	}

	return activeRecords, nil
}

// GetLatestSchema returns the most recent active schema
func (sf *SchemaFile) GetLatestSchema() (*SchemaRecord, error) {
	activeRecords, err := sf.ReadActiveSchemas()
	if err != nil {
		return nil, err
	}

	if len(activeRecords) == 0 {
		return nil, fmt.Errorf("no active schemas found")
	}

	// Find record with highest version
	latest := activeRecords[0]
	for _, record := range activeRecords[1:] {
		if record.SchemaVersion > latest.SchemaVersion {
			latest = record
		}
	}

	return latest, nil
}

// GetSchemaByVersion returns a specific version of the schema
func (sf *SchemaFile) GetSchemaByVersion(version int64) (*SchemaRecord, error) {
	allRecords, err := sf.ReadAllSchemas()
	if err != nil {
		return nil, err
	}

	for _, record := range allRecords {
		if record.SchemaVersion == version && record.IsActive() {
			return record, nil
		}
	}

	return nil, fmt.Errorf("schema version %d not found", version)
}

// ValidateIntegrity checks file integrity
func (sf *SchemaFile) ValidateIntegrity() error {
	sf.mu.RLock()
	defer sf.mu.RUnlock()

	// Validate header
	if err := sf.Header.Validate(); err != nil {
		return fmt.Errorf("invalid header: %w", err)
	}

	// Read all records to validate checksums
	records, err := sf.ReadAllSchemas()
	if err != nil {
		return fmt.Errorf("failed to read records: %w", err)
	}

	// Validate record counts match header
	activeCount := uint32(0)
	tombstoneCount := uint32(0)
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return fmt.Errorf("invalid record (version %d): %w", record.SchemaVersion, err)
		}
		if record.IsActive() {
			activeCount++
		} else if record.IsTombstone() {
			tombstoneCount++
		}
	}

	if activeCount != uint32(sf.Header.ActiveRecords) {
		return fmt.Errorf("active schema count mismatch: header=%d, actual=%d", sf.Header.ActiveRecords, activeCount)
	}

	if tombstoneCount != uint32(sf.Header.TombstoneCount) {
		return fmt.Errorf("tombstone count mismatch: header=%d, actual=%d", sf.Header.TombstoneCount, tombstoneCount)
	}

	return nil
}

// Close closes the file
func (sf *SchemaFile) Close() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if sf.file == nil {
		return nil
	}

	// Sync before closing
	if err := sf.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	if err := sf.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	sf.file = nil
	return nil
}

// writeHeader writes the current header to the file (internal helper)
func (sf *SchemaFile) writeHeader() error {
	// Serialize header
	headerBytes, err := sf.Header.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize header: %w", err)
	}

	// Seek to beginning
	if _, err := sf.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to beginning: %w", err)
	}

	// Write header
	if _, err := sf.file.Write(headerBytes[:]); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	return nil
}

// NeedsCompaction returns true if compaction is recommended
func (sf *SchemaFile) NeedsCompaction() bool {
	return sf.Header.ShouldCompact()
}

// GetStats returns file statistics
func (sf *SchemaFile) GetStats() FileStats {
	sf.mu.RLock()
	defer sf.mu.RUnlock()

	return FileStats{
		FilePath:         sf.FilePath,
		DatabaseName:     sf.Header.GetDatabaseName(),
		TotalRecords:     uint32(sf.Header.TotalRecords),
		ActiveSchemas:    uint32(sf.Header.ActiveRecords),
		TombstoneRecords: uint32(sf.Header.TombstoneCount),
		TombstoneRatio:   sf.Header.GetTombstoneRatio(),
		NeedsCompaction:  sf.Header.ShouldCompact(),
		CreatedAt:        sf.Header.CreatedAt,
		UpdatedAt:        sf.Header.UpdatedAt,
		LastCompactedAt:  sf.Header.LastCompactedAt,
	}
}

// FileStats represents file statistics
type FileStats struct {
	FilePath         string
	DatabaseName     string
	TotalRecords     uint32
	ActiveSchemas    uint32
	TombstoneRecords uint32
	TombstoneRatio   float64
	NeedsCompaction  bool
	CreatedAt        int64
	UpdatedAt        int64
	LastCompactedAt  int64
}
