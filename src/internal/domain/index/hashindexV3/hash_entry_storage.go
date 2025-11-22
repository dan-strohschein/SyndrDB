package hashindexV3

/*
HASH ENTRY STORAGE - APPEND-ONLY FILE MANAGEMENT

This file implements the storage layer for LSM-style hash index entries.
It manages append-only entry files on disk, following the same patterns
as bundle storage for consistency.

KEY RESPONSIBILITIES:
- Append entries to disk files (never modify in place)
- Read entries using backward scanning (latest wins)
- Manage file rotation when files grow too large
- Handle crash recovery and integrity checks

DESIGN PRINCIPLES:
- Single Responsibility: Only handles entry file I/O
- Follows bundle storage patterns (magic numbers, headers)
- Thread-safe operations with mutex protection
- Write buffering for performance

LSM INTEGRATION:
- Writes: Append entry immediately, update MemTable
- Reads: Scan backward from end of file for latest entry
- Compaction: Merge old files, remove tombstones

FILE FORMAT:
Each file contains sequential entries:
[Entry1][Entry2][Entry3]...[EntryN]

Each entry is serialized using HashIndexEntry.Serialize():
[Magic:4][Header:44][KeyLen:4][Key:N][DocIDLen:4][DocID:M]

TODO: Future extensions
- Background file rotation
- Bloom filters per file for fast negative lookups
- Block-based compression
- Parallel reads across multiple files
- Write-Ahead Log (WAL) integration
*/

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	// DefaultMaxFileSize is the maximum size of an entry file before rotation (128 MB)
	DefaultMaxFileSize = 128 * 1024 * 1024

	// DefaultWriteBufferSize is the size of the write buffer (64 KB)
	DefaultWriteBufferSize = 64 * 1024

	// EntryFileExtension is the file extension for entry files
	EntryFileExtension = ".idx"

	// EntryFileSuffix is the suffix pattern for entry files: bundleName_indexName_000001.idx
	EntryFileSuffix = "_%s_%06d" + EntryFileExtension
)

// EntryStorage manages append-only entry files for hash indexes
// Follows Single Responsibility Principle: Only handles entry persistence
type EntryStorage struct {
	// Configuration
	indexName       string // Name of the index (e.g., "DocumentID_idx")
	fieldName       string // Name of the field being indexed
	bundleName      string // Name of the bundle this index belongs to
	dataDir         string // Directory where index files are stored
	maxFileSize     int64  // Maximum file size before rotation
	writeBufferSize int    // Size of write buffer

	// Index metadata flags
	isForeignKey bool // True if this is a foreign key index
	isUnique     bool // True if index enforces uniqueness
	isPrimaryKey bool // True if this is the primary key index

	// Foreign key relationship (only populated if isForeignKey == true)
	referencedBundle string // Target bundle name
	referencedField  string // Target field name

	// Current active file
	currentFile     *os.File      // Currently active file for writes
	currentFileNum  int           // Current file number (0, 1, 2, ...)
	currentFileSize int64         // Current size of active file
	writeBuffer     *bufio.Writer // Buffered writer for performance
	currentHeader   *IndexHeader  // Header of current file (cached)

	// File management
	allFiles  []string     // List of all entry files (ordered oldest to newest)
	fileMutex sync.RWMutex // Protects file operations

	// Header management (new)
	headerManager *HeaderManager
	namingHelper  *FileNamingHelper

	// Statistics
	totalEntries uint64    // Total entries written
	totalBytes   uint64    // Total bytes written
	lastRotation time.Time // Last file rotation time

	// Header update tracking
	entriesSinceHeaderUpdate uint64 // Entries written since last header update
	headerUpdateInterval     uint64 // Update header every N entries

	// Logging
	logger *zap.SugaredLogger
}

// EntryStorageConfig holds configuration for EntryStorage
type EntryStorageConfig struct {
	IndexName       string // Name of the index
	FieldName       string // Name of the field being indexed
	BundleName      string // Name of the bundle
	DataDir         string // Directory for index files
	MaxFileSize     int64  // Maximum file size before rotation
	WriteBufferSize int    // Write buffer size

	// Index metadata flags
	IsForeignKey bool // True if this is a foreign key index
	IsUnique     bool // True if index enforces uniqueness
	IsPrimaryKey bool // True if this is the primary key index

	// Foreign key relationship (only if IsForeignKey == true)
	ReferencedBundle string // Target bundle name
	ReferencedField  string // Target field name

	Logger *zap.SugaredLogger
}

// NewEntryStorage creates a new entry storage manager
// Parameters:
//   - config: Configuration for the storage
//
// Returns initialized EntryStorage ready for use
func NewEntryStorage(config EntryStorageConfig) (*EntryStorage, error) {
	// Validate configuration
	if config.IndexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}
	if config.FieldName == "" {
		return nil, fmt.Errorf("field name cannot be empty")
	}
	if config.BundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty")
	}
	if config.DataDir == "" {
		return nil, fmt.Errorf("data directory cannot be empty")
	}

	// Validate foreign key relationship if applicable
	if config.IsForeignKey {
		if config.ReferencedBundle == "" {
			return nil, fmt.Errorf("referenced bundle required for foreign key index")
		}
		if config.ReferencedField == "" {
			return nil, fmt.Errorf("referenced field required for foreign key index")
		}
	}

	// Set defaults
	if config.MaxFileSize <= 0 {
		config.MaxFileSize = DefaultMaxFileSize
	}
	if config.WriteBufferSize <= 0 {
		config.WriteBufferSize = DefaultWriteBufferSize
	}

	// Create data directory if it doesn't exist
	// config.DataDir already contains: data_files/<Database>/<Bundle>/indexes
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create index directory: %w", err)
	}

	storage := &EntryStorage{
		indexName:                config.IndexName,
		fieldName:                config.FieldName,
		bundleName:               config.BundleName,
		dataDir:                  config.DataDir,
		maxFileSize:              config.MaxFileSize,
		writeBufferSize:          config.WriteBufferSize,
		isForeignKey:             config.IsForeignKey,
		isUnique:                 config.IsUnique,
		isPrimaryKey:             config.IsPrimaryKey,
		referencedBundle:         config.ReferencedBundle,
		referencedField:          config.ReferencedField,
		allFiles:                 make([]string, 0),
		lastRotation:             time.Now(),
		logger:                   config.Logger,
		headerManager:            NewHeaderManager(config.Logger),
		namingHelper:             NewFileNamingHelper(config.DataDir, config.BundleName),
		entriesSinceHeaderUpdate: 0,
		headerUpdateInterval:     100, // Update header every 100 entries
	}

	// Discover existing files
	if err := storage.discoverFiles(); err != nil {
		return nil, fmt.Errorf("failed to discover existing files: %w", err)
	}

	// Open or create the current active file
	if err := storage.openCurrentFile(); err != nil {
		return nil, fmt.Errorf("failed to open current file: %w", err)
	}

	if storage.logger != nil {
		storage.logger.Infow("Entry storage initialized",
			"indexName", config.IndexName,
			"fieldName", config.FieldName,
			"bundleName", config.BundleName,
			"dataDir", config.DataDir,
			"isForeignKey", config.IsForeignKey,
			"existingFiles", len(storage.allFiles))
	}

	return storage, nil
}

// AppendEntry appends a new entry to the current file
// This is the main write path for LSM index entries
//
// Parameters:
//   - entry: The entry to append
//
// Returns error if append fails
func (es *EntryStorage) AppendEntry(entry *HashIndexEntry) error {
	if entry == nil {
		return fmt.Errorf("cannot append nil entry")
	}

	es.fileMutex.Lock()
	defer es.fileMutex.Unlock()

	// Serialize the entry
	data, err := entry.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize entry: %w", err)
	}

	// Check if we need to rotate the file
	if es.currentFileSize+int64(len(data)) > es.maxFileSize {
		if err := es.rotateFile(); err != nil {
			return fmt.Errorf("failed to rotate file: %w", err)
		}
	}

	// Write to buffer
	n, err := es.writeBuffer.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	// Verify all data was written
	if n != len(data) {
		return fmt.Errorf("partial write: wrote %d bytes, expected %d bytes", n, len(data))
	}

	// Flush immediately to ensure entry is written to disk
	// This prevents partial entries from being written
	if err := es.writeBuffer.Flush(); err != nil {
		return fmt.Errorf("failed to flush entry: %w", err)
	}

	// Update statistics
	es.currentFileSize += int64(n)
	es.totalEntries++
	es.totalBytes += uint64(n)

	return nil
}

// AppendEntries appends multiple entries in a batch
// More efficient than multiple AppendEntry calls
//
// Parameters:
//   - entries: Slice of entries to append
//
// Returns error if any append fails
func (es *EntryStorage) AppendEntries(entries []*HashIndexEntry) error {
	if len(entries) == 0 {
		return nil
	}

	es.fileMutex.Lock()
	defer es.fileMutex.Unlock()

	for _, entry := range entries {
		if entry == nil {
			continue
		}

		// Serialize the entry
		data, err := entry.Serialize()
		if err != nil {
			return fmt.Errorf("failed to serialize entry: %w", err)
		}

		// Check if we need to rotate the file
		if es.currentFileSize+int64(len(data)) > es.maxFileSize {
			if err := es.rotateFile(); err != nil {
				return fmt.Errorf("failed to rotate file: %w", err)
			}
		}

		// Write to buffer
		n, err := es.writeBuffer.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}

		// Update statistics
		es.currentFileSize += int64(n)
		es.totalEntries++
		es.totalBytes += uint64(n)
	}

	return nil
}

// Flush forces all buffered data to disk
// Call this to ensure durability
func (es *EntryStorage) Flush() error {
	es.fileMutex.Lock()
	defer es.fileMutex.Unlock()

	if es.writeBuffer != nil {
		if err := es.writeBuffer.Flush(); err != nil {
			return fmt.Errorf("failed to flush write buffer: %w", err)
		}
	}

	if es.currentFile != nil {
		if err := es.currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	}

	return nil
}

// UpdateHeaderStatistics updates the header of the current file with latest statistics
// This should be called periodically to keep header metadata in sync with actual data
// Note: This is a relatively expensive operation (rewrites header), so call judiciously
func (es *EntryStorage) UpdateHeaderStatistics(globalSequence uint64) error {
	es.fileMutex.Lock()
	defer es.fileMutex.Unlock()

	if es.currentHeader == nil {
		return fmt.Errorf("no current header to update")
	}

	// Update header statistics in memory
	es.currentHeader.TotalEntries = es.totalEntries
	// DeletedEntries would need to be tracked separately - for now keep existing value
	es.currentHeader.FileSize = es.currentFileSize
	es.currentHeader.GlobalSequence = globalSequence

	// Write updated header to file
	currentPath := es.namingHelper.GenerateIndexFilePath(es.fieldName, es.isForeignKey, es.currentFileNum)

	if err := es.headerManager.UpdateStatistics(
		currentPath,
		es.currentHeader.TotalEntries,
		es.currentHeader.DeletedEntries,
		es.currentFileSize,
		globalSequence,
	); err != nil {
		return fmt.Errorf("failed to update header statistics: %w", err)
	}

	if es.logger != nil {
		es.logger.Debugf("Updated header statistics: entries=%d, size=%d, seq=%d",
			es.totalEntries, es.currentFileSize, globalSequence)
	}

	return nil
}

// FlushWithHeaderUpdate flushes data and updates header statistics atomically
// This is the recommended way to ensure header stays in sync with data
func (es *EntryStorage) FlushWithHeaderUpdate(globalSequence uint64) error {
	// First flush data
	if err := es.Flush(); err != nil {
		return fmt.Errorf("failed to flush data: %w", err)
	}

	// Then update header
	if err := es.UpdateHeaderStatistics(globalSequence); err != nil {
		// Log warning but don't fail - data is already flushed
		if es.logger != nil {
			es.logger.Warnf("Failed to update header after flush: %v", err)
		}
	}

	// Reset the counter since we just updated the header
	atomic.StoreUint64(&es.entriesSinceHeaderUpdate, 0)

	return nil
}

// ScanBackward scans entries backward from the end of files
// This is the primary read path for LSM indexes (latest entry wins)
//
// Parameters:
//   - visitor: Function called for each entry (return false to stop)
//
// Returns error if scan fails
func (es *EntryStorage) ScanBackward(visitor func(*HashIndexEntry) bool) error {
	es.fileMutex.RLock()
	defer es.fileMutex.RUnlock()

	// Flush current buffer to ensure we read latest data
	if err := es.writeBuffer.Flush(); err != nil {
		return fmt.Errorf("failed to flush before scan: %w", err)
	}

	// Scan files from newest to oldest
	for i := len(es.allFiles) - 1; i >= 0; i-- {
		filename := es.allFiles[i]
		filePath := filepath.Join(es.dataDir, filename)
		if err := es.scanFileBackward(filePath, visitor); err != nil {
			return fmt.Errorf("failed to scan file %s: %w", filePath, err)
		}
	}

	return nil
}

// ScanForward scans entries forward from the beginning
// Used for compaction and rebuilding MemTable
//
// Parameters:
//   - visitor: Function called for each entry (return false to stop)
//
// Returns error if scan fails
func (es *EntryStorage) ScanForward(visitor func(*HashIndexEntry) bool) error {
	es.fileMutex.RLock()
	defer es.fileMutex.RUnlock()

	// Flush current buffer to ensure we read latest data
	if err := es.writeBuffer.Flush(); err != nil {
		return fmt.Errorf("failed to flush before scan: %w", err)
	}

	// Scan files from oldest to newest
	for _, filename := range es.allFiles {
		filePath := filepath.Join(es.dataDir, filename)
		if err := es.scanFileForward(filePath, visitor); err != nil {
			return fmt.Errorf("failed to scan file %s: %w", filePath, err)
		}
	}

	return nil
}

// GetLatestEntry finds the latest entry for a given key
// Scans backward until finding the first (most recent) match
//
// Parameters:
//   - key: The key to search for
//
// Returns the latest entry or nil if not found
func (es *EntryStorage) GetLatestEntry(key string) (*HashIndexEntry, error) {
	var foundEntry *HashIndexEntry

	err := es.ScanBackward(func(entry *HashIndexEntry) bool {
		if entry.KeyValue == key {
			foundEntry = entry
			return false // Stop scanning - we found the latest
		}
		return true // Continue scanning
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan for key %s: %w", key, err)
	}

	return foundEntry, nil
}

// Close closes the storage and flushes all data
func (es *EntryStorage) Close() error {
	es.fileMutex.Lock()
	defer es.fileMutex.Unlock()

	// Flush buffer
	if es.writeBuffer != nil {
		if err := es.writeBuffer.Flush(); err != nil {
			return fmt.Errorf("failed to flush on close: %w", err)
		}
	}

	// Close current file
	if es.currentFile != nil {
		if err := es.currentFile.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}
		es.currentFile = nil
	}

	if es.logger != nil {
		es.logger.Infow("Entry storage closed",
			"indexName", es.indexName,
			"bundleName", es.bundleName,
			"totalEntries", es.totalEntries,
			"totalBytes", es.totalBytes)
	}

	return nil
}

// DeleteFile removes a file from tracking and deletes it from disk
// This is used during compaction to remove old files
//
// Parameters:
//   - filePath: Absolute path to file to delete
//
// Returns error if file cannot be deleted
func (es *EntryStorage) DeleteFile(filePath string) error {
	es.fileMutex.Lock()
	defer es.fileMutex.Unlock()

	// Remove from allFiles slice
	for i, f := range es.allFiles {
		if f == filePath {
			es.allFiles = append(es.allFiles[:i], es.allFiles[i+1:]...)
			break
		}
	}

	// Delete physical file
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file %s: %w", filePath, err)
	}

	es.logger.Debugw("Deleted index file",
		"file", filePath,
		"remainingFiles", len(es.allFiles))

	return nil
}

// DeleteFiles removes multiple files atomically
// If any deletion fails, previous deletions are NOT rolled back
//
// Parameters:
//   - filePaths: List of absolute file paths to delete
//
// Returns error if any file cannot be deleted
func (es *EntryStorage) DeleteFiles(filePaths []string) error {
	var errors []error

	for _, filePath := range filePaths {
		if err := es.DeleteFile(filePath); err != nil {
			errors = append(errors, err)
			// Continue deleting other files even if one fails
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to delete %d files: %v", len(errors), errors)
	}

	return nil
}

// RegisterFile adds an externally-created file to the storage's file list
// This is used after compaction creates a new merged file
//
// Parameters:
//   - filePath: Absolute path to file to register
//
// Returns error if file doesn't exist
func (es *EntryStorage) RegisterFile(filePath string) error {
	es.fileMutex.Lock()
	defer es.fileMutex.Unlock()

	// Verify file exists
	if _, err := os.Stat(filePath); err != nil {
		return fmt.Errorf("cannot register non-existent file %s: %w", filePath, err)
	}

	// Add to allFiles and maintain sorted order
	es.allFiles = append(es.allFiles, filePath)
	sort.Strings(es.allFiles)

	es.logger.Debugw("Registered new index file",
		"file", filePath,
		"totalFiles", len(es.allFiles))

	return nil
}

// ReplaceFiles atomically replaces old files with a new compacted file
// This is the critical method for compaction integration
//
// Steps:
//  1. Verify new file exists
//  2. Register new file (adds to tracking)
//  3. Delete old files (removes from tracking and disk)
//
// Parameters:
//   - oldFiles: List of old file paths to remove
//   - newFile: Path to new compacted file
//
// Returns error if replacement fails
func (es *EntryStorage) ReplaceFiles(oldFiles []string, newFile string) error {
	es.logger.Infow("Replacing files with compacted version",
		"oldFileCount", len(oldFiles),
		"newFile", newFile)

	// Step 1: Register new file first (makes it available for reads)
	if err := es.RegisterFile(newFile); err != nil {
		return fmt.Errorf("failed to register new file: %w", err)
	}

	// Step 2: Delete old files
	// Note: If deletion fails, new file is still registered
	// This is safe because reads will use latest data
	if err := es.DeleteFiles(oldFiles); err != nil {
		es.logger.Warnw("Failed to delete some old files after compaction",
			"error", err,
			"oldFiles", oldFiles)
		// Don't return error - compaction was successful, cleanup can be retried
	}

	es.logger.Infow("File replacement completed",
		"newFile", newFile,
		"deletedCount", len(oldFiles))

	return nil
}

// GetStats returns storage statistics
type EntryStorageStats struct {
	IndexName       string    // Index name
	BundleName      string    // Bundle name
	FileCount       int       // Number of files
	TotalEntries    uint64    // Total entries written
	TotalBytes      uint64    // Total bytes written
	CurrentFileSize int64     // Size of current active file
	LastRotation    time.Time // Last file rotation time
}

// GetStats retrieves current storage statistics
func (es *EntryStorage) GetStats() EntryStorageStats {
	es.fileMutex.RLock()
	defer es.fileMutex.RUnlock()

	return EntryStorageStats{
		IndexName:       es.indexName,
		BundleName:      es.bundleName,
		FileCount:       len(es.allFiles),
		TotalEntries:    es.totalEntries,
		TotalBytes:      es.totalBytes,
		CurrentFileSize: es.currentFileSize,
		LastRotation:    es.lastRotation,
	}
}

// Private helper methods

// discoverFiles finds all existing entry files for this index
func (es *EntryStorage) discoverFiles() error {
	pattern := filepath.Join(es.dataDir, fmt.Sprintf("%s_%s_*%s",
		es.bundleName, es.indexName, EntryFileExtension))

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to glob files: %w", err)
	}

	// Extract basenames only (not full paths) for storage
	baseNames := make([]string, len(matches))
	for i, fullPath := range matches {
		baseNames[i] = filepath.Base(fullPath)
	}

	// Sort files by number (oldest to newest)
	es.allFiles = baseNames

	// Determine next file number
	if len(es.allFiles) > 0 {
		// Parse the last file number
		lastFile := es.allFiles[len(es.allFiles)-1]
		var fileNum int
		_, err := fmt.Sscanf(filepath.Base(lastFile),
			fmt.Sprintf("%s_%s_%%d%s", es.bundleName, es.indexName, EntryFileExtension),
			&fileNum)
		if err == nil {
			es.currentFileNum = fileNum
		}
	}

	return nil
}

// openCurrentFile opens or creates the current active file
// Uses new naming convention: FieldName-fk.N.hidx OR FieldName.N.hidx
// Writes header at the beginning of new files
func (es *EntryStorage) openCurrentFile() error {
	// Generate filename using new naming convention
	filename := es.namingHelper.GenerateIndexFileName(es.fieldName, es.isForeignKey, es.currentFileNum)
	filePath := filepath.Join(es.dataDir, filename)

	// Check if file exists
	fileExists := false
	if _, err := os.Stat(filePath); err == nil {
		fileExists = true
	}

	// Open file in read-write mode (create if doesn't exist)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	// If file is new, write header first
	if !fileExists {
		// Create header for this file
		var header *IndexHeader
		if es.isForeignKey {
			header = es.headerManager.CreateHeaderWithRelationship(
				es.indexName, es.fieldName, es.bundleName,
				es.currentFileNum, es.isUnique, es.isPrimaryKey,
				es.referencedBundle, es.referencedField,
			)
		} else {
			header = es.headerManager.CreateHeader(
				es.indexName, es.fieldName, es.bundleName,
				es.currentFileNum, es.isForeignKey, es.isUnique, es.isPrimaryKey,
			)
		}

		// Write header to file
		bytesWritten, err := es.headerManager.WriteHeader(filePath, header)
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to write header: %w", err)
		}

		es.currentHeader = header
		es.currentFileSize = bytesWritten

		if es.logger != nil {
			es.logger.Debugf("Wrote header to new file %s: %d bytes", filename, bytesWritten)
		}

		// Seek to end for appending entries
		if _, err := file.Seek(0, io.SeekEnd); err != nil {
			file.Close()
			return fmt.Errorf("failed to seek to end: %w", err)
		}
	} else {
		// File exists - read header and seek past it
		header, headerSize, err := es.headerManager.ReadHeader(filePath)
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to read header from existing file: %w", err)
		}

		es.currentHeader = header

		// Get current file size
		fileInfo, err := file.Stat()
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to stat file: %w", err)
		}
		es.currentFileSize = fileInfo.Size()

		// Seek to end for appending entries
		if _, err := file.Seek(0, io.SeekEnd); err != nil {
			file.Close()
			return fmt.Errorf("failed to seek to end: %w", err)
		}

		if es.logger != nil {
			es.logger.Debugf("Opened existing file %s: header size=%d, file size=%d",
				filename, headerSize, es.currentFileSize)
		}
	}

	es.currentFile = file
	es.writeBuffer = bufio.NewWriterSize(file, es.writeBufferSize)

	// Add to file list if not already there (store only basename)
	fileAlreadyListed := false
	for _, f := range es.allFiles {
		if f == filename {
			fileAlreadyListed = true
			break
		}
	}
	if !fileAlreadyListed {
		es.allFiles = append(es.allFiles, filename)
	}

	return nil
}

// rotateFile closes the current file and opens a new one
// Called automatically when file size exceeds maxFileSize
// Creates new header for the rotated file
func (es *EntryStorage) rotateFile() error {
	// Update statistics in current header before closing
	if es.currentHeader != nil && es.currentFile != nil {
		currentPath := es.namingHelper.GenerateIndexFilePath(es.fieldName, es.isForeignKey, es.currentFileNum)

		// Update header with final statistics
		if err := es.headerManager.UpdateStatistics(
			currentPath,
			es.currentHeader.TotalEntries,
			es.currentHeader.DeletedEntries,
			es.currentFileSize,
			es.currentHeader.GlobalSequence,
		); err != nil {
			es.logger.Warnf("Failed to update header statistics before rotation: %v", err)
			// Continue with rotation even if header update fails
		}
	}

	// Flush and close current file
	if es.writeBuffer != nil {
		if err := es.writeBuffer.Flush(); err != nil {
			return fmt.Errorf("failed to flush before rotation: %w", err)
		}
	}

	if es.currentFile != nil {
		if err := es.currentFile.Close(); err != nil {
			return fmt.Errorf("failed to close file before rotation: %w", err)
		}
	}

	// Increment file number and open new file
	es.currentFileNum++
	es.lastRotation = time.Now()

	// Reset header update counter since we're creating a new file with a fresh header
	atomic.StoreUint64(&es.entriesSinceHeaderUpdate, 0)

	if err := es.openCurrentFile(); err != nil {
		return fmt.Errorf("failed to open new file: %w", err)
	}

	if es.logger != nil {
		es.logger.Infow("File rotated",
			"indexName", es.indexName,
			"fieldName", es.fieldName,
			"bundleName", es.bundleName,
			"newFileNum", es.currentFileNum,
			"totalFiles", len(es.allFiles))
	}

	return nil
}

// scanFileBackward scans a single file backward
// UPDATED: Now skips header for new .hidx files
func (es *EntryStorage) scanFileBackward(filePath string, visitor func(*HashIndexEntry) bool) error {
	// Read entire file (TODO: optimize with memory-mapped I/O)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Determine starting offset based on file format
	offset := 0

	// Check if this is a new format file with header (.hidx extension)
	if strings.HasSuffix(filePath, ".hidx") {
		// New format - read header to get offset
		if len(data) < HeaderMetadataPosition {
			return fmt.Errorf("file too short to contain header")
		}

		// Read header size from fixed position
		headerSizeBytes := data[8:12]
		headerSize := int32(binary.LittleEndian.Uint32(headerSizeBytes))

		if headerSize <= 0 || int(headerSize) > len(data) {
			return fmt.Errorf("invalid header size: %d", headerSize)
		}

		// Start reading entries after header
		offset = int(headerSize)

		if es.logger != nil {
			es.logger.Debugf("Scanning new format file, skipping header: %d bytes", headerSize)
		}
	}
	// else: Legacy format (.idx) - no header, start at offset 0

	// Parse all entries first (we need to reverse them)
	entries := make([]*HashIndexEntry, 0, 1000)

	for offset < len(data) {
		entry, bytesRead, err := DeserializeEntry(data[offset:])
		if err != nil {
			// Log warning but continue (corrupted entry)
			if es.logger != nil {
				es.logger.Warnw("Failed to deserialize entry",
					"file", filePath,
					"offset", offset,
					"error", err)
			}
			break
		}

		entries = append(entries, entry)
		offset += bytesRead
	}

	// Visit entries in reverse order (newest first)
	for i := len(entries) - 1; i >= 0; i-- {
		if !visitor(entries[i]) {
			return nil // Visitor requested stop
		}
	}

	return nil
}

// scanFileForward scans a single file forward
// UPDATED: Now skips header for new .hidx files
func (es *EntryStorage) scanFileForward(filePath string, visitor func(*HashIndexEntry) bool) error {
	// Read entire file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Determine starting offset based on file format
	offset := 0

	// Check if this is a new format file with header (.hidx extension)
	if strings.HasSuffix(filePath, ".hidx") {
		// New format - read header to get offset
		if len(data) < HeaderMetadataPosition {
			return fmt.Errorf("file too short to contain header")
		}

		// Read header size from fixed position
		headerSizeBytes := data[8:12]
		headerSize := int32(binary.LittleEndian.Uint32(headerSizeBytes))

		if headerSize <= 0 || int(headerSize) > len(data) {
			return fmt.Errorf("invalid header size: %d", headerSize)
		}

		// Start reading entries after header
		offset = int(headerSize)

		if es.logger != nil {
			es.logger.Debugf("Scanning new format file forward, skipping header: %d bytes", headerSize)
		}
	}
	// else: Legacy format (.idx) - no header, start at offset 0

	// Parse and visit entries in order
	for offset < len(data) {
		entry, bytesRead, err := DeserializeEntry(data[offset:])
		if err != nil {
			// Log warning but continue
			if es.logger != nil {
				es.logger.Warnw("Failed to deserialize entry",
					"file", filePath,
					"offset", offset,
					"error", err)
			}
			break
		}

		if !visitor(entry) {
			return nil // Visitor requested stop
		}

		offset += bytesRead
	}

	return nil
}

// GetEntryFiles returns a sorted list of all entry files
// Files are sorted by creation timestamp (oldest to newest)
func (es *EntryStorage) GetEntryFiles() ([]string, error) {
	es.fileMutex.RLock()
	defer es.fileMutex.RUnlock()

	// Return a copy to avoid concurrent modification
	files := make([]string, len(es.allFiles))
	copy(files, es.allFiles)

	return files, nil
}

// ScanFileForMaxSequence scans a single entry file and returns the maximum sequence number
// This is used for crash recovery to validate metadata
func (es *EntryStorage) ScanFileForMaxSequence(filename string) (uint64, error) {

	fullPath := filepath.Join(es.dataDir, filename)

	// Open file for reading
	file, err := os.Open(fullPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	var maxSeq uint64 = 0

	// Read file info to get total size
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	// Get header size to know where entry data begins
	// This is efficient - only reads first 12 bytes to get header size
	headerSize, err := es.headerManager.GetHeaderSize(fullPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read header size from %s: %w", filename, err)
	}

	// Calculate entry data size (total file size - header size)
	entryDataSize := fileInfo.Size() - headerSize
	if entryDataSize <= 0 {
		// File only contains header, no entries yet
		es.logger.Debugf("File %s contains only header, no entries to scan", filename)
		return 0, nil
	}

	// Seek past the header to where entry data begins
	if _, err := file.Seek(headerSize, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek past header: %w", err)
	}

	// Read entry data (everything after header)
	data := make([]byte, entryDataSize)
	_, err = file.Read(data)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("failed to read entry data: %w", err)
	}

	// Deserialize entries one by one
	offset := 0
	for offset < len(data) {
		entry, bytesRead, err := DeserializeEntry(data[offset:])
		if err != nil {
			// Skip corrupted entries but continue scanning
			es.logger.Warnw("Skipping corrupted entry during max sequence scan",
				"file", filename,
				"offset", offset,
				"error", err)
			break
		}

		if entry.Sequence > maxSeq {
			maxSeq = entry.Sequence
		}

		offset += bytesRead
	}

	es.logger.Debugf("Scanned %s: found max sequence %d from %d bytes of entry data",
		filename, maxSeq, entryDataSize)

	return maxSeq, nil
}

// TODO: Future extensions
// - Implement memory-mapped I/O for large files
// - Add Bloom filter per file for fast negative lookups
// - Implement block-based compression
// - Support parallel reads across multiple files
// - Add Write-Ahead Log (WAL) for crash recovery
// - Implement background file rotation
// - Add file-level statistics (min/max keys, entry counts)
