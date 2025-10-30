package hashindexV3

/*
FILE NAMING UTILITIES - HASH INDEX FILE NAME MANAGEMENT

This file provides utilities for generating and parsing hash index file names
following the new naming convention that embeds metadata in the filename itself.

KEY RESPONSIBILITIES:
- Generate properly formatted index file names
- Parse file names to extract metadata
- Identify foreign key indexes by suffix
- Support file rotation with sequence numbers

DESIGN PRINCIPLES:
- Single Responsibility: Only handles file naming operations
- Open/Closed: Extensible for future index types
- DRY: Centralized naming logic used throughout codebase
- Dependency Inversion: Used by higher-level components

NAMING CONVENTION:
Regular Index:    FieldName.N.hidx
Foreign Key:      FieldName_fk.N.hidx
Primary Key:      DocumentID.N.hidx (special case of regular)

Examples:
- DocumentID.0.hidx        → Primary key index, file 0
- UserID_fk.0.hidx         → Foreign key to User bundle, file 0
- Email.0.hidx             → Regular unique index, file 0
- OrderID_fk.1.hidx        → Foreign key, file 1 (after rotation)

PARSING:
From filename "UserID_fk.0.hidx" we extract:
- FieldName: "UserID"
- IsForeignKey: true
- FileNumber: 0
- Extension: ".hidx"

FILE DISCOVERY:
Pattern matching: *_fk.*.hidx OR *.*.hidx
This allows bundle discovery to find all index files by:
1. Listing all .hidx files
2. Parsing each filename
3. Grouping by FieldName

TODO: Future extensions
- Support for composite indexes (e.g., "FirstName-LastName.0.hidx")
- Version markers in filename (e.g., "UserID-fk.v2.0.hidx")
- Temporary file naming during compaction
*/

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// FileNamingHelper provides utilities for index file naming
// Follows Single Responsibility Principle: Only handles file name generation and parsing
type FileNamingHelper struct {
	// Configuration
	dataDir    string // Base directory for index files
	bundleName string // Bundle name for path construction
}

// NewFileNamingHelper creates a new file naming helper
func NewFileNamingHelper(dataDir, bundleName string) *FileNamingHelper {
	return &FileNamingHelper{
		dataDir:    dataDir,
		bundleName: bundleName,
	}
}

// GenerateIndexFileName generates a file name for a hash index file
// Parameters:
//   - fieldName: Name of the field being indexed (e.g., "UserID")
//   - isForeignKey: True if this is a foreign key index
//   - fileNumber: Sequence number for file rotation (0, 1, 2, ...)
//
// Returns: filename in format "FieldName_fk.N.hidx" or "FieldName.N.hidx"
func (fnh *FileNamingHelper) GenerateIndexFileName(fieldName string, isForeignKey bool, fileNumber int) string {
	if isForeignKey {
		// Format: FieldName_fk.N.hidx
		return fmt.Sprintf("%s%s.%d%s", fieldName, ForeignKeySuffix, fileNumber, IndexFileExtension)
	}
	// Format: FieldName.N.hidx
	return fmt.Sprintf("%s.%d%s", fieldName, fileNumber, IndexFileExtension)
}

// GenerateIndexFilePath generates a full path for a hash index file
// Returns: full path like "/data/indexes/FieldName_fk.0.hidx"
func (fnh *FileNamingHelper) GenerateIndexFilePath(fieldName string, isForeignKey bool, fileNumber int) string {
	fileName := fnh.GenerateIndexFileName(fieldName, isForeignKey, fileNumber)
	return filepath.Join(fnh.dataDir, fileName)
}

// ParsedIndexFileName contains metadata extracted from an index file name
type ParsedIndexFileName struct {
	FieldName    string // Name of the indexed field
	IsForeignKey bool   // Whether this is a foreign key index
	FileNumber   int    // Sequence number
	FullPath     string // Full file path (if provided)
	FileName     string // Just the file name
	IsValid      bool   // Whether parsing succeeded
}

// ParseIndexFileName extracts metadata from an index file name
// Handles both "FieldName-fk.N.hidx" and "FieldName.N.hidx" formats
// Parameters:
//   - fileName: Just the file name (e.g., "UserID-fk.0.hidx") or full path
//
// Returns: ParsedIndexFileName with extracted metadata
func (fnh *FileNamingHelper) ParseIndexFileName(fileName string) ParsedIndexFileName {
	result := ParsedIndexFileName{
		FullPath: fileName,
		FileName: filepath.Base(fileName),
		IsValid:  false,
	}

	// Check if file has correct extension
	if !strings.HasSuffix(result.FileName, IndexFileExtension) {
		return result
	}

	// Remove extension
	nameWithoutExt := strings.TrimSuffix(result.FileName, IndexFileExtension)

	// Pattern 1: Try foreign key format "FieldName-fk.N"
	if strings.Contains(nameWithoutExt, ForeignKeySuffix) {
		parts := strings.Split(nameWithoutExt, ".")
		if len(parts) != 2 {
			return result
		}

		// Extract field name (everything before .N)
		fieldPart := parts[0]
		if !strings.HasSuffix(fieldPart, ForeignKeySuffix) {
			return result
		}
		result.FieldName = strings.TrimSuffix(fieldPart, ForeignKeySuffix)
		result.IsForeignKey = true

		// Extract file number
		fileNum, err := strconv.Atoi(parts[1])
		if err != nil {
			return result
		}
		result.FileNumber = fileNum
		result.IsValid = true
		return result
	}

	// Pattern 2: Try regular format "FieldName.N"
	parts := strings.Split(nameWithoutExt, ".")
	if len(parts) != 2 {
		return result
	}

	result.FieldName = parts[0]
	result.IsForeignKey = false

	// Extract file number
	fileNum, err := strconv.Atoi(parts[1])
	if err != nil {
		return result
	}
	result.FileNumber = fileNum
	result.IsValid = true
	return result
}

// IsIndexFile checks if a file name matches the index file naming pattern
// Returns true for both "FieldName-fk.N.hidx" and "FieldName.N.hidx"
func (fnh *FileNamingHelper) IsIndexFile(fileName string) bool {
	if !strings.HasSuffix(fileName, IndexFileExtension) {
		return false
	}
	parsed := fnh.ParseIndexFileName(fileName)
	return parsed.IsValid
}

// GetIndexFilePattern returns a regex pattern for matching index files
// Returns pattern that matches both foreign key and regular index files
func (fnh *FileNamingHelper) GetIndexFilePattern() *regexp.Regexp {
	// Pattern: (FieldName-fk OR FieldName).N.hidx
	// Matches: UserID-fk.0.hidx OR UserID.0.hidx
	pattern := `^([a-zA-Z0-9_]+)(-fk)?\.([0-9]+)\.hidx$`
	return regexp.MustCompile(pattern)
}

// ListIndexFilesForField finds all index files for a specific field
// Parameters:
//   - fieldName: Name of the field to search for
//   - isForeignKey: Whether to search for foreign key or regular index
//
// Returns: List of file numbers found (e.g., [0, 1, 2] for rotated files)
func (fnh *FileNamingHelper) ListIndexFilesForField(fieldName string, isForeignKey bool) ([]int, error) {
	// List all files in indexes directory
	entries, err := filepath.Glob(filepath.Join(fnh.dataDir, "*"+IndexFileExtension))
	if err != nil {
		return nil, fmt.Errorf("failed to list index files: %w", err)
	}

	var fileNumbers []int
	for _, entry := range entries {
		parsed := fnh.ParseIndexFileName(entry)
		if !parsed.IsValid {
			continue
		}

		// Check if this file matches the requested field and type
		if parsed.FieldName == fieldName && parsed.IsForeignKey == isForeignKey {
			fileNumbers = append(fileNumbers, parsed.FileNumber)
		}
	}

	return fileNumbers, nil
}

// GetLatestFileNumber returns the highest file number for a given field
// Used to determine the next file number during rotation
// Returns -1 if no files exist yet
func (fnh *FileNamingHelper) GetLatestFileNumber(fieldName string, isForeignKey bool) (int, error) {
	fileNumbers, err := fnh.ListIndexFilesForField(fieldName, isForeignKey)
	if err != nil {
		return -1, err
	}

	if len(fileNumbers) == 0 {
		return -1, nil // No files yet
	}

	// Find maximum
	maxNum := fileNumbers[0]
	for _, num := range fileNumbers[1:] {
		if num > maxNum {
			maxNum = num
		}
	}

	return maxNum, nil
}

// GenerateNextFileName generates the next file name for rotation
// Automatically determines the next sequence number
func (fnh *FileNamingHelper) GenerateNextFileName(fieldName string, isForeignKey bool) (string, int, error) {
	latestNum, err := fnh.GetLatestFileNumber(fieldName, isForeignKey)
	if err != nil {
		return "", -1, fmt.Errorf("failed to get latest file number: %w", err)
	}

	nextNum := 0
	if latestNum >= 0 {
		nextNum = latestNum + 1
	}

	fileName := fnh.GenerateIndexFileName(fieldName, isForeignKey, nextNum)
	return fileName, nextNum, nil
}

// GetBundleIndexDirectory returns the directory path where index files are stored
func (fnh *FileNamingHelper) GetBundleIndexDirectory() string {
	return fnh.dataDir
}

// ValidateFileName checks if a file name is valid and follows conventions
// Returns error if the name doesn't follow the expected format
func (fnh *FileNamingHelper) ValidateFileName(fileName string) error {
	parsed := fnh.ParseIndexFileName(fileName)
	if !parsed.IsValid {
		return fmt.Errorf("invalid index file name format: %s", fileName)
	}

	if parsed.FieldName == "" {
		return fmt.Errorf("field name cannot be empty in: %s", fileName)
	}

	if parsed.FileNumber < 0 {
		return fmt.Errorf("file number must be non-negative in: %s", fileName)
	}

	return nil
}
