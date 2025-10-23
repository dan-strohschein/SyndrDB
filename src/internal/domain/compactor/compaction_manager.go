package compactor

/*
COMPACTION MANAGER - LSM-STYLE MERGE AND CLEANUP

This file implements the compaction manager for LSM-style storage systems.
It handles merging multiple entry files, removing tombstones, and maintaining
optimal storage efficiency for both hash index files and bundle files.

KEY RESPONSIBILITIES:
- Merge multiple entry files into a single compacted file
- Remove tombstones (deleted entries) during merge
- Maintain temporal ordering (latest entry wins)
- Atomic file replacement to prevent data loss
- Track compaction statistics and performance

DESIGN PRINCIPLES:
- Single Responsibility: Only handles compaction logic
- Thread-safe operations with mutex protection
- Configurable compaction strategies (size, count, tombstone ratio)
- Minimal disruption to read/write operations

COMPACTION PROCESS:
1. Select files for compaction based on strategy
2. Scan files forward to merge entries
3. Keep only latest version of each key
4. Remove tombstones from final output
5. Write merged entries to new file
6. Atomically replace old files with new file

LSM INTEGRATION:
- Works with hash index entry storage
- Future: Bundle file compaction (removing deleted documents)
- Maintains append-only semantics
- Preserves temporal ordering

FILE NAMING:
- Hash Index: {bundle}_{index}_{NNNNNN}.idx
- Bundle Files: {database}_{bundle}.bnd (future compaction support)

TODO: Future extensions
- Leveled compaction (L0, L1, L2, ...)
- Size-tiered compaction strategies
- Parallel compaction across multiple files
- Background compaction threads
- Incremental compaction (pause/resume)
- Compaction statistics dashboard
*/

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

// CompactionManager manages compaction operations for LSM-style storage
// Follows Single Responsibility Principle: Only handles compaction
type CompactionManager struct {
	// Configuration
	dataDir  string             // Directory containing files to compact
	strategy CompactionStrategy // Strategy for triggering compaction
	enabled  bool               // Whether compaction is enabled

	// State management
	compacting     bool         // True if compaction is in progress
	lastCompaction time.Time    // Time of last compaction
	mutex          sync.RWMutex // Protects compaction state

	// Statistics
	stats CompactionStats // Compaction statistics

	// Logging
	logger *zap.SugaredLogger
}

// CompactionStats tracks compaction performance metrics
type CompactionStats struct {
	TotalCompactions    uint64        // Total number of compactions performed
	TotalFilesCompacted uint64        // Total files compacted
	TotalBytesWritten   uint64        // Total bytes written during compaction
	TotalEntriesKept    uint64        // Total entries kept after compaction
	TotalEntriesRemoved uint64        // Total entries removed (tombstones + old versions)
	LastDuration        time.Duration // Duration of last compaction
	AverageDuration     time.Duration // Average compaction duration
	LastError           error         // Last compaction error

	mutex sync.RWMutex // Protects statistics
}

// CompactionConfig holds configuration for CompactionManager
type CompactionConfig struct {
	DataDir  string             // Directory containing files
	Strategy CompactionStrategy // Compaction strategy
	Enabled  bool               // Enable/disable compaction
	Logger   *zap.SugaredLogger // Logger instance
}

// NewCompactionManager creates a new compaction manager
//
// Parameters:
//   - config: Configuration for the compaction manager
//
// Returns the CompactionManager instance and any error
func NewCompactionManager(config CompactionConfig) (*CompactionManager, error) {
	if config.DataDir == "" {
		return nil, fmt.Errorf("data directory cannot be empty")
	}

	if config.Strategy == nil {
		// Use default strategy if none provided
		config.Strategy = NewDefaultCompactionStrategy()
	}

	if config.Logger == nil {
		// Create default logger if none provided
		logger, _ := zap.NewProduction()
		config.Logger = logger.Sugar()
	}

	cm := &CompactionManager{
		dataDir:    config.DataDir,
		strategy:   config.Strategy,
		enabled:    config.Enabled,
		compacting: false,
		stats:      CompactionStats{},
		logger:     config.Logger,
	}

	return cm, nil
}

// CompactHashIndexFiles compacts hash index entry files
// This is the main entry point for hash index compaction
//
// Parameters:
//   - bundleName: Name of the bundle
//   - indexName: Name of the index
//   - entryFiles: List of entry files to compact (absolute paths)
//
// Returns the path to the compacted file and any error
func (cm *CompactionManager) CompactHashIndexFiles(bundleName, indexName string, entryFiles []string) (string, error) {
	// Validate inputs
	if bundleName == "" || indexName == "" {
		return "", fmt.Errorf("bundle name and index name cannot be empty")
	}

	if len(entryFiles) == 0 {
		return "", fmt.Errorf("no files to compact")
	}

	// Check if already compacting
	cm.mutex.Lock()
	if cm.compacting {
		cm.mutex.Unlock()
		return "", fmt.Errorf("compaction already in progress")
	}
	cm.compacting = true
	cm.mutex.Unlock()

	defer func() {
		cm.mutex.Lock()
		cm.compacting = false
		cm.mutex.Unlock()
	}()

	startTime := time.Now()

	cm.logger.Infow("Starting hash index compaction",
		"bundle", bundleName,
		"index", indexName,
		"fileCount", len(entryFiles))

	// Perform compaction
	compactedFile, stats, err := cm.compactHashIndexFilesInternal(bundleName, indexName, entryFiles)

	duration := time.Since(startTime)

	// Update statistics
	cm.updateStats(stats, duration, err)

	if err != nil {
		cm.logger.Errorw("Hash index compaction failed",
			"bundle", bundleName,
			"index", indexName,
			"duration", duration,
			"error", err)
		return "", err
	}

	cm.logger.Infow("Hash index compaction completed",
		"bundle", bundleName,
		"index", indexName,
		"duration", duration,
		"inputFiles", len(entryFiles),
		"outputFile", compactedFile,
		"entriesKept", stats.TotalEntriesKept,
		"entriesRemoved", stats.TotalEntriesRemoved)

	return compactedFile, nil
}

// compactHashIndexFilesInternal performs the actual compaction logic
// Separated from main function for cleaner error handling
func (cm *CompactionManager) compactHashIndexFilesInternal(bundleName, indexName string, entryFiles []string) (string, CompactionStats, error) {
	stats := CompactionStats{}

	// Sort files by name (ensures temporal ordering)
	sortedFiles := make([]string, len(entryFiles))
	copy(sortedFiles, entryFiles)
	sort.Strings(sortedFiles)

	// Create temporary output file
	outputFileName := fmt.Sprintf("%s_%s_compacted_%d.idx.tmp", bundleName, indexName, time.Now().UnixNano())
	outputFilePath := filepath.Join(cm.dataDir, outputFileName)

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return "", stats, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	// Track unique keys and their latest entries
	// Map: key -> latest entry
	latestEntries := make(map[string]*hashIndexEntryWrapper)

	// Read all entries from all files
	// Latest file wins for duplicate keys
	for _, filePath := range sortedFiles {
		entries, err := cm.readEntriesFromFile(filePath)
		if err != nil {
			return "", stats, fmt.Errorf("failed to read entries from %s: %w", filePath, err)
		}

		stats.TotalFilesCompacted++

		// Process entries - latest wins
		for _, entry := range entries {
			wrapper := &hashIndexEntryWrapper{
				entry:    entry,
				filePath: filePath,
			}

			existing, exists := latestEntries[entry.KeyValue]
			if !exists {
				// First time seeing this key
				latestEntries[entry.KeyValue] = wrapper
			} else {
				// Compare timestamps/sequences to determine latest
				if cm.isNewer(entry, existing.entry) {
					latestEntries[entry.KeyValue] = wrapper
					stats.TotalEntriesRemoved++ // Old version removed
				} else {
					stats.TotalEntriesRemoved++ // Current entry is old, discard
				}
			}
		}
	}

	// Write non-tombstone entries to output file
	var entriesToWrite []*hashIndexEntry
	for _, wrapper := range latestEntries {
		if wrapper.entry.Deleted {
			// Skip tombstones - they can be removed during compaction
			stats.TotalEntriesRemoved++
		} else {
			entriesToWrite = append(entriesToWrite, wrapper.entry)
			stats.TotalEntriesKept++
		}
	}

	// Sort entries by key for better locality
	sort.Slice(entriesToWrite, func(i, j int) bool {
		return entriesToWrite[i].KeyValue < entriesToWrite[j].KeyValue
	})

	// Write entries to output file
	for _, entry := range entriesToWrite {
		data, err := cm.serializeEntry(entry)
		if err != nil {
			return "", stats, fmt.Errorf("failed to serialize entry: %w", err)
		}

		n, err := outputFile.Write(data)
		if err != nil {
			return "", stats, fmt.Errorf("failed to write entry: %w", err)
		}

		stats.TotalBytesWritten += uint64(n)
	}

	// Sync to disk
	if err := outputFile.Sync(); err != nil {
		return "", stats, fmt.Errorf("failed to sync output file: %w", err)
	}

	// Close output file before rename
	outputFile.Close()

	// Generate final file name
	finalFileName := fmt.Sprintf("%s_%s_%06d.idx", bundleName, indexName, time.Now().Unix())
	finalFilePath := filepath.Join(cm.dataDir, finalFileName)

	// Rename temp file to final name (atomic on most systems)
	if err := os.Rename(outputFilePath, finalFilePath); err != nil {
		// Clean up temp file
		os.Remove(outputFilePath)
		return "", stats, fmt.Errorf("failed to rename output file: %w", err)
	}

	// TODO: Delete old files after successful compaction
	// For now, we keep them for safety - implement in production version
	// This requires coordination with the storage layer to ensure no active readers

	cm.logger.Infow("Compacted file created",
		"outputFile", finalFilePath,
		"entriesKept", stats.TotalEntriesKept,
		"entriesRemoved", stats.TotalEntriesRemoved,
		"bytesWritten", stats.TotalBytesWritten)

	return finalFilePath, stats, nil
}

// CompactBundleFile compacts a bundle file to remove tombstones
// This is a placeholder for future bundle file compaction
//
// TODO: Implement bundle file compaction in later sprint
// This will work similar to hash index compaction:
// 1. Read all documents from bundle file
// 2. Skip documents with tombstones (0xDEADDEAD markers)
// 3. Write remaining documents to new file
// 4. Atomically replace old file with new file
//
// Parameters:
//   - bundleName: Name of the bundle
//   - databaseName: Name of the database
//   - bundleFilePath: Path to the bundle file
//
// Returns the path to the compacted file and any error
func (cm *CompactionManager) CompactBundleFile(bundleName, databaseName, bundleFilePath string) (string, error) {
	// TODO: Sprint X - Bundle File Compaction
	//
	// Implementation steps:
	// 1. Parse bundle file using parseAppendedDocuments pattern
	// 2. Identify documents with tombstones (0xDEADDEAD markers)
	// 3. Create new bundle file with only active documents
	// 4. Use same magic numbers: 0xDEADBEEF for documents
	// 5. Maintain temporal ordering
	// 6. Update bundle metadata (document count, file size)
	// 7. Atomically replace old file
	// 8. Update indexes to point to new file
	//
	// Integration points:
	// - Use bundlestore.parseAppendedDocuments() for reading
	// - Use bundlestore.AppendDocumentToBundleFile() pattern for writing
	// - Coordinate with BundleStorageEngine for file locking
	// - Update all indexes after compaction
	//
	// Performance considerations:
	// - Use buffered I/O (64KB buffer like current implementation)
	// - Process in batches to avoid memory pressure
	// - Pause compaction if system is under load
	// - Track compaction statistics (documents kept/removed, bytes saved)

	return "", fmt.Errorf("bundle file compaction not yet implemented - coming in later sprint")
}

// ShouldCompact checks if compaction should be triggered based on strategy
//
// Parameters:
//   - files: List of files to check
//
// Returns true if compaction should be triggered
func (cm *CompactionManager) ShouldCompact(files []string) bool {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	if !cm.enabled {
		return false
	}

	if cm.compacting {
		return false
	}

	return cm.strategy.ShouldCompact(files)
}

// GetStats returns current compaction statistics
func (cm *CompactionManager) GetStats() CompactionStats {
	cm.stats.mutex.RLock()
	defer cm.stats.mutex.RUnlock()

	return cm.stats
}

// Enable enables compaction
func (cm *CompactionManager) Enable() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.enabled = true
}

// Disable disables compaction
func (cm *CompactionManager) Disable() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.enabled = false
}

// IsCompacting returns true if compaction is in progress
func (cm *CompactionManager) IsCompacting() bool {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.compacting
}

// updateStats updates compaction statistics
func (cm *CompactionManager) updateStats(stats CompactionStats, duration time.Duration, err error) {
	cm.stats.mutex.Lock()
	defer cm.stats.mutex.Unlock()

	cm.stats.TotalCompactions++
	cm.stats.TotalFilesCompacted += stats.TotalFilesCompacted
	cm.stats.TotalBytesWritten += stats.TotalBytesWritten
	cm.stats.TotalEntriesKept += stats.TotalEntriesKept
	cm.stats.TotalEntriesRemoved += stats.TotalEntriesRemoved
	cm.stats.LastDuration = duration
	cm.stats.LastError = err

	// Update average duration
	if cm.stats.TotalCompactions > 0 {
		totalDuration := cm.stats.AverageDuration*time.Duration(cm.stats.TotalCompactions-1) + duration
		cm.stats.AverageDuration = totalDuration / time.Duration(cm.stats.TotalCompactions)
	}

	cm.lastCompaction = time.Now()
}

// readEntriesFromFile reads all entries from a file
// This is a placeholder - will be replaced with actual storage integration
func (cm *CompactionManager) readEntriesFromFile(filePath string) ([]*hashIndexEntry, error) {
	// TODO: Sprint 3 - Integration with EntryStorage
	// Replace this with actual EntryStorage.ScanForward() call
	//
	// Example integration:
	// storage := hashindexV3.NewEntryStorage(...)
	// var entries []*hashIndexEntry
	// err := storage.ScanForward(func(entry *HashIndexEntry) bool {
	//     entries = append(entries, convertToInternalEntry(entry))
	//     return true
	// })

	return nil, fmt.Errorf("readEntriesFromFile not yet implemented - needs EntryStorage integration")
}

// serializeEntry serializes an entry to bytes
// This is a placeholder - will be replaced with actual entry serialization
func (cm *CompactionManager) serializeEntry(entry *hashIndexEntry) ([]byte, error) {
	// TODO: Sprint 3 - Integration with HashIndexEntry
	// Replace this with actual HashIndexEntry.Serialize() call
	//
	// Example integration:
	// realEntry := convertToHashIndexEntry(entry)
	// return realEntry.Serialize()

	return nil, fmt.Errorf("serializeEntry not yet implemented - needs HashIndexEntry integration")
}

// isNewer determines if entry1 is newer than entry2
// Uses sequence number and timestamp for comparison
func (cm *CompactionManager) isNewer(entry1, entry2 *hashIndexEntry) bool {
	// First compare sequence numbers (if available)
	if entry1.Sequence != entry2.Sequence {
		return entry1.Sequence > entry2.Sequence
	}

	// Fall back to timestamp comparison
	return entry1.Timestamp.After(entry2.Timestamp)
}

// hashIndexEntry is an internal representation for compaction
// This will be replaced with actual HashIndexEntry from hashindexV3
type hashIndexEntry struct {
	KeyValue   string
	DocumentID string
	Timestamp  time.Time
	Sequence   uint64
	Deleted    bool
	HashValue  uint32
}

// hashIndexEntryWrapper wraps an entry with metadata
type hashIndexEntryWrapper struct {
	entry    *hashIndexEntry
	filePath string // Source file for debugging
}
