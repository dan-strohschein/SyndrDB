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
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	bundleName      string // Name of the bundle this index belongs to
	dataDir         string // Directory where index files are stored
	maxFileSize     int64  // Maximum file size before rotation
	writeBufferSize int    // Size of write buffer

	// Current active file
	currentFile     *os.File      // Currently active file for writes
	currentFileNum  int           // Current file number (0, 1, 2, ...)
	currentFileSize int64         // Current size of active file
	writeBuffer     *bufio.Writer // Buffered writer for performance

	// File management
	allFiles  []string     // List of all entry files (ordered oldest to newest)
	fileMutex sync.RWMutex // Protects file operations

	// Statistics
	totalEntries uint64    // Total entries written
	totalBytes   uint64    // Total bytes written
	lastRotation time.Time // Last file rotation time

	// Logging
	logger *zap.SugaredLogger
}

// EntryStorageConfig holds configuration for EntryStorage
type EntryStorageConfig struct {
	IndexName       string // Name of the index
	BundleName      string // Name of the bundle
	DataDir         string // Directory for index files
	MaxFileSize     int64  // Maximum file size before rotation
	WriteBufferSize int    // Write buffer size
	Logger          *zap.SugaredLogger
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
	if config.BundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty")
	}
	if config.DataDir == "" {
		return nil, fmt.Errorf("data directory cannot be empty")
	}

	// Set defaults
	if config.MaxFileSize <= 0 {
		config.MaxFileSize = DefaultMaxFileSize
	}
	if config.WriteBufferSize <= 0 {
		config.WriteBufferSize = DefaultWriteBufferSize
	}

	// Create data directory if it doesn't exist
	// config.DataDir already contains: data_files/<Database>/indexes/<Bundle>
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create index directory: %w", err)
	}

	storage := &EntryStorage{
		indexName:       config.IndexName,
		bundleName:      config.BundleName,
		dataDir:         config.DataDir,
		maxFileSize:     config.MaxFileSize,
		writeBufferSize: config.WriteBufferSize,
		allFiles:        make([]string, 0),
		lastRotation:    time.Now(),
		logger:          config.Logger,
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
			"bundleName", config.BundleName,
			"dataDir", config.DataDir,
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
		filePath := es.allFiles[i]

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
	for _, filePath := range es.allFiles {
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

	// Sort files by number (oldest to newest)
	es.allFiles = matches

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
func (es *EntryStorage) openCurrentFile() error {
	// Generate filename
	filename := fmt.Sprintf("%s_%s_%06d%s",
		es.bundleName, es.indexName, es.currentFileNum, EntryFileExtension)
	filePath := filepath.Join(es.dataDir, filename)

	// Open file in append mode (create if doesn't exist)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to stat file: %w", err)
	}

	es.currentFile = file
	es.currentFileSize = fileInfo.Size()
	es.writeBuffer = bufio.NewWriterSize(file, es.writeBufferSize)

	// Add to file list if not already there
	fileAlreadyListed := false
	for _, f := range es.allFiles {
		if f == filePath {
			fileAlreadyListed = true
			break
		}
	}
	if !fileAlreadyListed {
		es.allFiles = append(es.allFiles, filePath)
	}

	return nil
}

// rotateFile closes the current file and opens a new one
// Called automatically when file size exceeds maxFileSize
func (es *EntryStorage) rotateFile() error {
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

	if err := es.openCurrentFile(); err != nil {
		return fmt.Errorf("failed to open new file: %w", err)
	}

	if es.logger != nil {
		es.logger.Infow("File rotated",
			"indexName", es.indexName,
			"bundleName", es.bundleName,
			"newFileNum", es.currentFileNum,
			"totalFiles", len(es.allFiles))
	}

	return nil
}

// scanFileBackward scans a single file backward
func (es *EntryStorage) scanFileBackward(filePath string, visitor func(*HashIndexEntry) bool) error {
	// Read entire file (TODO: optimize with memory-mapped I/O)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Parse all entries first (we need to reverse them)
	entries := make([]*HashIndexEntry, 0, 1000)
	offset := 0

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
func (es *EntryStorage) scanFileForward(filePath string, visitor func(*HashIndexEntry) bool) error {
	// Read entire file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Parse and visit entries in order
	offset := 0

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

// TODO: Future extensions
// - Implement memory-mapped I/O for large files
// - Add Bloom filter per file for fast negative lookups
// - Implement block-based compression
// - Support parallel reads across multiple files
// - Add Write-Ahead Log (WAL) for crash recovery
// - Implement background file rotation
// - Add file-level statistics (min/max keys, entry counts)
