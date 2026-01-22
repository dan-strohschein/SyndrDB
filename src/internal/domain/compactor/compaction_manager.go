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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syndrdb/src/internal/domain/index/hashindexV3"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common"
	"syndrdb/src/pkg/common/helpers"
	"time"

	"go.uber.org/zap"
)

// MetricsReporter is a callback function for reporting metrics without import cycles
// This allows CompactionManager to report metrics to the server package without importing it
type MetricsReporter func(metricName string, value uint64)

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

	// Per-bundle/index locking for concurrent compaction
	// Key format: "bundle:{bundleName}" or "index:{bundleName}_{indexName}"
	// Value: *sync.Mutex for that resource
	bundleLocks sync.Map // Fine-grained locks to prevent concurrent compaction of same bundle/index

	// Statistics
	stats CompactionStats // Compaction statistics

	// Metrics reporting (optional callback to avoid import cycles)
	metricsReporter MetricsReporter

	// PHASE 5: MVCC - Callback to get oldest active snapshot sequence
	// Used to determine which versions to preserve during compaction
	getOldestSnapshot func() uint64

	// Logging
	logger *zap.SugaredLogger

	Compacting bool
}

// CompactionStats tracks compaction performance metrics
type CompactionStats struct {
	TotalCompactions    uint64 // Total number of compactions performed
	TotalFilesCompacted uint64 // Total files compacted
	TotalBytesWritten   uint64 // Total bytes written during compaction
	TotalEntriesKept    uint64 // Total entries kept after compaction
	TotalEntriesRemoved uint64 // Total entries removed (tombstones + old versions)

	// Bundle-specific statistics
	TotalBundlesCompacted uint64 // Total bundle files compacted
	TotalDocumentsKept    uint64 // Total documents kept in bundle compaction
	TotalDocumentsRemoved uint64 // Total documents removed in bundle compaction

	LastDuration    time.Duration // Duration of last compaction
	AverageDuration time.Duration // Average compaction duration
	LastError       error         // Last compaction error

	mutex sync.RWMutex // Protects statistics
}

// CompactionConfig holds configuration for CompactionManager
type CompactionConfig struct {
	DataDir         string             // Directory containing files
	Strategy        CompactionStrategy // Compaction strategy
	Enabled         bool               // Enable/disable compaction
	Logger          *zap.SugaredLogger // Logger instance
	MetricsReporter MetricsReporter    // Optional callback for reporting metrics
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
		dataDir:     config.DataDir,
		strategy:    config.Strategy,
		enabled:     config.Enabled,
		compacting:  false,
		bundleLocks: sync.Map{}, // Initialize per-bundle lock map
		stats:       CompactionStats{},
		logger:      config.Logger,
		// PHASE 5: MVCC - Initialize getOldestSnapshot callback (can be set later)
		getOldestSnapshot: nil,
	}

	return cm, nil
}

// SetOldestSnapshotGetter sets the callback function to get oldest active snapshot
// PHASE 5: MVCC - Allows CompactionManager to query oldest snapshot without import cycles
func (cm *CompactionManager) SetOldestSnapshotGetter(getter func() uint64) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.getOldestSnapshot = getter
}

func (cm *CompactionManager) GetCompacting() bool {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.compacting
}

func (cm *CompactionManager) SetCompacting(value bool) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	cm.compacting = true
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

	// Acquire per-index lock to prevent concurrent compaction
	// This ensures only one compaction happens per index at a time
	indexLock := cm.acquireIndexLock(bundleName, indexName)
	defer indexLock.Unlock()

	// Check if already compacting globally
	cm.mutex.Lock()
	if cm.compacting {
		cm.mutex.Unlock()
		if cm.metricsReporter != nil {
			cm.metricsReporter("CompactionBlockedByLock", 1)
		}
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
func (cm *CompactionManager) compactHashIndexFilesInternal(bundleName, indexName string, entryFiles []string) (string, *CompactionStats, error) {
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
		return "", &stats, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	// PHASE 5: MVCC - Get oldest snapshot sequence to determine which entries to preserve
	var oldestSnapshotSeq uint64
	if cm.getOldestSnapshot != nil {
		oldestSnapshotSeq = cm.getOldestSnapshot()
		cm.logger.Debugw("MVCC index compaction filtering",
			"oldestSnapshotSeq", oldestSnapshotSeq)
	} else {
		// No snapshot getter - preserve all entries (safe default)
		oldestSnapshotSeq = 0 // 0 means preserve all
	}

	// Track unique keys and their latest entries
	// Map: key -> latest entry
	latestEntries := make(map[string]*hashIndexEntryWrapper)

	// Read all entries from all files
	// Latest file wins for duplicate keys
	for _, filePath := range sortedFiles {
		entries, err := cm.readEntriesFromFile(filePath)
		if err != nil {
			return "", &stats, fmt.Errorf("failed to read entries from %s: %w", filePath, err)
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

	// PHASE 5: MVCC - Filter entries: preserve those with CommitSequence >= oldestSnapshot
	// Write non-tombstone entries to output file
	var entriesToWrite []*hashindexV3.HashIndexEntry
	preservedCount := 0
	removedCount := 0

	for _, wrapper := range latestEntries {
		entry := wrapper.entry
		shouldPreserve := false

		if oldestSnapshotSeq == 0 {
			// No snapshot filtering - preserve all entries (safe default)
			shouldPreserve = true
		} else {
			// Preserve if:
			// 1. Uncommitted (CommitSequence == 0) - may be visible to active transactions
			// 2. Committed and visible to oldest snapshot (CommitSequence >= oldestSnapshotSeq)
			if entry.CommitSequence == 0 {
				// Uncommitted - preserve to be safe
				shouldPreserve = true
			} else if entry.CommitSequence >= oldestSnapshotSeq {
				// Committed and visible to oldest snapshot - preserve
				shouldPreserve = true
			} else {
				// Committed before oldest snapshot - safe to remove
				shouldPreserve = false
			}
		}

		if shouldPreserve {
			if entry.Deleted {
				// PHASE 5: MVCC - Preserve tombstones if they're visible to active snapshots
				// Tombstones are needed for MVCC visibility filtering
				entriesToWrite = append(entriesToWrite, entry)
				preservedCount++
				stats.TotalEntriesKept++
			} else {
				entriesToWrite = append(entriesToWrite, entry)
				preservedCount++
				stats.TotalEntriesKept++
			}
		} else {
			// Entry not visible to any active snapshot - safe to remove
			removedCount++
			stats.TotalEntriesRemoved++
			cm.logger.Debugw("Removing index entry (not visible to any active snapshot)",
				"key", entry.KeyValue,
				"commitSequence", entry.CommitSequence,
				"oldestSnapshotSeq", oldestSnapshotSeq)
		}
	}

	cm.logger.Debugw("Index compaction filtering complete",
		"preservedEntries", preservedCount,
		"removedEntries", removedCount,
		"oldestSnapshotSeq", oldestSnapshotSeq)

	// Sort entries by key for better locality
	sort.Slice(entriesToWrite, func(i, j int) bool {
		return entriesToWrite[i].KeyValue < entriesToWrite[j].KeyValue
	})

	// Write entries to output file
	// TODO FIXME: the stats is returning the lock. We should change this to return a pointer
	// to the stats struct instead of a copy so the state is correctly updated. Passing by value
	// will create an independent copy of the stats struct and the mutex will not be shared.
	for _, entry := range entriesToWrite {
		data, err := cm.serializeEntry(entry)
		if err != nil {
			return "", &stats, fmt.Errorf("failed to serialize entry: %w", err)
		}

		n, err := outputFile.Write(data)
		if err != nil {
			return "", &stats, fmt.Errorf("failed to write entry: %w", err)
		}

		stats.TotalBytesWritten += uint64(n)
	}

	// Sync to disk
	if err := outputFile.Sync(); err != nil {
		return "", &stats, fmt.Errorf("failed to sync output file: %w", err)
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
		return "", &stats, fmt.Errorf("failed to rename output file: %w", err)
	}

	// TODO: Delete old files after successful compaction
	// For now, we keep them for safety - implement in production version
	// This requires coordination with the storage layer to ensure no active readers

	cm.logger.Infow("Compacted file created",
		"outputFile", finalFilePath,
		"entriesKept", stats.TotalEntriesKept,
		"entriesRemoved", stats.TotalEntriesRemoved,
		"bytesWritten", stats.TotalBytesWritten)

	return finalFilePath, &stats, nil
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
	// Acquire per-bundle lock to prevent concurrent compaction
	// This ensures ghost cleanup worker and manual triggers don't conflict
	bundleLock := cm.acquireBundleLock(bundleName)
	defer bundleLock.Unlock()

	cm.logger.Infow("Starting bundle file compaction",
		"bundle", bundleName,
		"database", databaseName,
		"filePath", bundleFilePath)

	startTime := time.Now()

	// Step 1: Parse bundle file and collect all documents
	cm.logger.Infow("Parsing bundle file", "filePath", bundleFilePath)
	documentWrappers, err := cm.parseBundleDocuments(bundleFilePath)
	if err != nil {
		cm.logger.Errorw("Failed to parse bundle documents",
			"filePath", bundleFilePath,
			"error", err)
		return "", fmt.Errorf("failed to parse bundle file: %w", err)
	}

	// Step 2: PHASE 5: MVCC - Filter versions based on oldest active snapshot
	// Get oldest snapshot sequence to determine which versions to preserve
	var oldestSnapshotSeq uint64
	if cm.getOldestSnapshot != nil {
		oldestSnapshotSeq = cm.getOldestSnapshot()
		cm.logger.Debugw("MVCC compaction filtering",
			"oldestSnapshotSeq", oldestSnapshotSeq)
	} else {
		// No snapshot getter - preserve all versions (safe default)
		cm.logger.Debugw("No snapshot getter available, preserving all versions")
		oldestSnapshotSeq = 0 // 0 means preserve all
	}

	activeDocuments := make([]*models.Document, 0)
	tombstoneCount := 0
	totalVersions := 0
	preservedVersions := 0
	removedVersions := 0

	for documentID, versions := range documentWrappers {
		totalVersions += len(versions)
		// Sort versions by VersionSequence (descending - newest first)
		// This ensures we write versions in correct order
		sort.Slice(versions, func(i, j int) bool {
			return versions[i].document.VersionSequence > versions[j].document.VersionSequence
		})

		// PHASE 5: MVCC - Filter versions: preserve those with CommitSequence >= oldestSnapshot
		// Also preserve uncommitted versions (CommitSequence == 0) as they may be visible to active transactions
		for _, wrapper := range versions {
			doc := &wrapper.document
			shouldPreserve := false

			if oldestSnapshotSeq == 0 {
				// No snapshot filtering - preserve all versions (safe default)
				shouldPreserve = true
			} else {
				// Preserve if:
				// 1. Uncommitted (CommitSequence == 0) - may be visible to active transactions
				// 2. Committed and visible to oldest snapshot (CommitSequence >= oldestSnapshotSeq)
				if doc.CommitSequence == 0 {
					// Uncommitted - preserve to be safe (active transactions may see it)
					shouldPreserve = true
				} else if doc.CommitSequence >= oldestSnapshotSeq {
					// Committed and visible to oldest snapshot - preserve
					shouldPreserve = true
				} else {
					// Committed before oldest snapshot and no active snapshots can see it - safe to remove
					shouldPreserve = false
				}
			}

			if shouldPreserve {
				preservedVersions++
				if cm.isTombstoneDocument(wrapper) {
					tombstoneCount++
					cm.logger.Debugw("Preserving tombstone version",
						"documentID", documentID,
						"versionSequence", doc.VersionSequence,
						"commitSequence", doc.CommitSequence)
				}
				activeDocuments = append(activeDocuments, doc)
			} else {
				removedVersions++
				cm.logger.Debugw("Removing version (not visible to any active snapshot)",
					"documentID", documentID,
					"versionSequence", doc.VersionSequence,
					"commitSequence", doc.CommitSequence,
					"oldestSnapshotSeq", oldestSnapshotSeq)
			}
		}
	}

	cm.logger.Infow("Document analysis complete",
		"uniqueDocuments", len(documentWrappers),
		"totalVersions", totalVersions,
		"preservedVersions", preservedVersions,
		"removedVersions", removedVersions,
		"activeDocuments", len(activeDocuments),
		"tombstones", tombstoneCount,
		"oldestSnapshotSeq", oldestSnapshotSeq)

	// If no tombstones, no compaction needed
	if tombstoneCount == 0 {
		cm.logger.Infow("No tombstones found, skipping compaction",
			"filePath", bundleFilePath)
		return bundleFilePath, nil
	}

	// Step 3: Create temporary compacted file
	tempFilePath := bundleFilePath + ".compact.tmp"
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		cm.logger.Errorw("Failed to create temporary file",
			"tempFilePath", tempFilePath,
			"error", err)
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer func() {
		tempFile.Close()
		// Clean up temp file if we return with error
		if err != nil {
			os.Remove(tempFilePath)
		}
	}()

	// Step 4: Write active documents to new file
	// TODO: Batch writes for large bundles (>10K documents)
	bytesWritten := int64(0)
	documentsWritten := 0

	for _, doc := range activeDocuments {
		n, writeErr := cm.writeBundleDocument(tempFile, doc)
		if writeErr != nil {
			err = fmt.Errorf("failed to write document %s: %w", doc.DocumentID, writeErr)
			cm.logger.Errorw("Failed to write document",
				"documentID", doc.DocumentID,
				"error", writeErr)
			return "", err
		}
		bytesWritten += int64(n)
		documentsWritten++
	}

	// Ensure all data is written to disk before replacing
	if syncErr := common.Fdatasync(tempFile); syncErr != nil {
		err = fmt.Errorf("failed to sync temporary file: %w", syncErr)
		cm.logger.Errorw("Failed to sync temporary file", "error", syncErr)
		return "", err
	}

	tempFile.Close() // Close before rename

	// Step 5: Get original file info for stats
	originalInfo, err := os.Stat(bundleFilePath)
	if err != nil {
		cm.logger.Errorw("Failed to stat original file",
			"filePath", bundleFilePath,
			"error", err)
		return "", fmt.Errorf("failed to stat original file: %w", err)
	}
	originalSize := originalInfo.Size()

	// Step 6: Atomically replace original file with compacted version
	if err := os.Rename(tempFilePath, bundleFilePath); err != nil {
		cm.logger.Errorw("Failed to replace original file",
			"originalPath", bundleFilePath,
			"tempPath", tempFilePath,
			"error", err)
		return "", fmt.Errorf("failed to replace original file: %w", err)
	}

	// Step 7: Update compaction statistics
	duration := time.Since(startTime)
	spaceSaved := originalSize - bytesWritten

	// Create stats struct (without mutex - that's managed by updateStats)
	stats := CompactionStats{
		TotalBundlesCompacted: 1,
		TotalDocumentsKept:    uint64(documentsWritten),
		TotalDocumentsRemoved: uint64(tombstoneCount),
		TotalBytesWritten:     uint64(bytesWritten),
		TotalFilesCompacted:   1,
		// Note: mutex field is zero-valued and not used in transfer
	}

	cm.updateStats(&stats, duration, nil)

	cm.logger.Infow("Bundle file compaction complete",
		"bundle", bundleName,
		"database", databaseName,
		"originalSize", originalSize,
		"compactedSize", bytesWritten,
		"spaceSaved", spaceSaved,
		"documentsKept", documentsWritten,
		"documentsRemoved", tombstoneCount,
		"duration", duration)

	// TODO: Step 8 - Notify indexes to update references to compacted file
	// This will be implemented when index update coordination is added

	return bundleFilePath, nil
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
func (cm *CompactionManager) GetStats() *CompactionStats {
	cm.stats.mutex.RLock()
	defer cm.stats.mutex.RUnlock()

	return &cm.stats
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

func (cm *CompactionManager) GetStrategy() CompactionStrategy {
	return cm.strategy
}

func (cm *CompactionManager) GetLogger() *zap.SugaredLogger {
	return cm.logger
}

// acquireBundleLock acquires an exclusive lock for a bundle file
// Returns the lock that must be released after compaction completes
//
// This prevents concurrent compaction of the same bundle from ghost cleanup
// worker and manual triggers, which would corrupt the file.
//
// Parameters:
//   - bundleName: Name of bundle to lock
//
// Returns the mutex that was acquired (caller must defer Unlock())
//
// TODO: MVCC Evolution Path - Replace per-bundle locks with MVCC copy-on-write
// When transaction support (BEGIN/COMMIT/ROLLBACK) is implemented:
// 1. Remove this lock acquisition entirely
// 2. Ghost cleanup creates new bundle version with suffix .v{TransactionID}.bnd
// 3. Write all non-tombstone documents to new version file
// 4. Update bundle metadata to point to new version (atomic pointer swap)
// 5. Old version file remains readable by in-progress queries (check their TransactionID)
// 6. Version cleanup thread removes old versions when no active transaction references them
// 7. This enables true lock-free reads during compaction (queries never block)
//
// MVCC Implementation Notes:
// - Bundle metadata needs LatestVersionID atomic.Uint64 field
// - Each bundle version file: {database}_{bundle}.v{versionID}.bnd
// - Reader queries snapshot bundle versions at query start (bundleVersionMap map[string]uint64)
// - Compaction increments version ID, writes new file, atomically updates metadata
// - Version cleanup scans all active queries' snapshots, deletes unreferenced versions
// - This matches SQL Server's snapshot isolation (queries see consistent point-in-time data)
func (cm *CompactionManager) acquireBundleLock(bundleName string) *sync.Mutex {
	lockKey := fmt.Sprintf("bundle:%s", bundleName)

	// Get or create mutex for this bundle
	lockInterface, _ := cm.bundleLocks.LoadOrStore(lockKey, &sync.Mutex{})
	lock := lockInterface.(*sync.Mutex)

	// Acquire the lock (blocks if another compaction is in progress)
	lock.Lock()

	return lock
}

// acquireIndexLock acquires an exclusive lock for a hash index
// Returns the lock that must be released after compaction completes
//
// This prevents concurrent compaction of the same index from periodic triggers
// and manual compaction calls.
//
// Parameters:
//   - bundleName: Name of bundle containing the index
//   - indexName: Name of index to lock
//
// Returns the mutex that was acquired (caller must defer Unlock())
//
// TODO: MVCC Evolution Path - Replace per-index locks with versioned index files
// When MVCC transactions are implemented:
// 1. Hash index files use versioning: {bundle}_{index}_{NNNNNN}.v{TransactionID}.idx
// 2. Compaction creates new version file with merged entries
// 3. Index metadata tracks latest version ID (atomic swap on completion)
// 4. Queries snapshot index version IDs at query start
// 5. Old index versions cleaned up when no queries reference them
// 6. Lock-free compaction: readers use old version while compaction builds new version
//
// This requires coordination between:
// - HashIndex.triggerCompaction() - snapshot current version before compaction
// - CompactionManager.CompactHashIndexFiles() - write to .v{newVersionID}.idx file
// - HashIndex metadata update - atomic version pointer swap after compaction
// - Version cleanup - remove unreferenced .idx files based on active query snapshots
func (cm *CompactionManager) acquireIndexLock(bundleName, indexName string) *sync.Mutex {
	lockKey := fmt.Sprintf("index:%s_%s", bundleName, indexName)

	// Get or create mutex for this index
	lockInterface, _ := cm.bundleLocks.LoadOrStore(lockKey, &sync.Mutex{})
	lock := lockInterface.(*sync.Mutex)

	// Acquire the lock (blocks if another compaction is in progress)
	lock.Lock()

	return lock
}

func (cm *CompactionManager) UpdateStats(stats *CompactionStats, duration time.Duration, err error) {
	cm.UpdateStats(stats, duration, err)
}

// updateStats updates compaction statistics
func (cm *CompactionManager) updateStats(stats *CompactionStats, duration time.Duration, err error) {
	cm.stats.mutex.Lock()
	defer cm.stats.mutex.Unlock()

	cm.stats.TotalCompactions++
	cm.stats.TotalFilesCompacted += stats.TotalFilesCompacted
	cm.stats.TotalBytesWritten += stats.TotalBytesWritten
	cm.stats.TotalEntriesKept += stats.TotalEntriesKept
	cm.stats.TotalEntriesRemoved += stats.TotalEntriesRemoved

	// Update bundle-specific statistics
	cm.stats.TotalBundlesCompacted += stats.TotalBundlesCompacted
	cm.stats.TotalDocumentsKept += stats.TotalDocumentsKept
	cm.stats.TotalDocumentsRemoved += stats.TotalDocumentsRemoved

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
// Uses hashindexV3's deserialization to read entries
func (cm *CompactionManager) readEntriesFromFile(filePath string) ([]*hashindexV3.HashIndexEntry, error) {
	var entries []*hashindexV3.HashIndexEntry

	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Read all entries from file
	for {
		entry, bytesRead, err := hashindexV3.DeserializeEntryFromReader(file)
		if err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			// Log error but continue - partial corruption shouldn't stop compaction
			cm.logger.Warnw("Failed to deserialize entry, skipping",
				"file", filePath,
				"error", err)
			continue
		}

		if bytesRead == 0 {
			break // No more entries
		}

		entries = append(entries, entry)
	}

	cm.logger.Debugw("Read entries from file",
		"file", filePath,
		"entryCount", len(entries))

	return entries, nil
}

// serializeEntry serializes an entry to bytes
// Uses hashindexV3's built-in serialization
func (cm *CompactionManager) serializeEntry(entry *hashindexV3.HashIndexEntry) ([]byte, error) {
	// Use hashindexV3's built-in serialization
	data, err := entry.Serialize()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize entry: %w", err)
	}
	return data, nil
}

func (cm *CompactionManager) IsNewer(entry1, entry2 *hashindexV3.HashIndexEntry) bool {
	return cm.isNewer(entry1, entry2)
}

// isNewer determines if entry1 is newer than entry2
// Uses sequence number and timestamp for comparison
func (cm *CompactionManager) isNewer(entry1, entry2 *hashindexV3.HashIndexEntry) bool {
	// First compare sequence numbers (if available)
	if entry1.Sequence != entry2.Sequence {
		return entry1.Sequence > entry2.Sequence
	}

	// Fall back to timestamp comparison
	return entry1.Timestamp.After(entry2.Timestamp)
}

// hashIndexEntryWrapper wraps an entry with metadata
type hashIndexEntryWrapper struct {
	entry    *hashindexV3.HashIndexEntry
	filePath string // Source file for debugging
}

// bundleDocumentWrapper wraps a document with compaction metadata
// Used during bundle file compaction to track document state
type bundleDocumentWrapper struct {
	document    models.Document
	isTombstone bool      // True if this is a deletion marker (0xDEADDEAD)
	offset      int64     // File offset where document was found
	timestamp   time.Time // When document was written/deleted
}

// parseBundleDocuments reads and parses all documents from a bundle file
// This function follows the parseAppendedDocuments pattern from BundleStorageEngine
// PHASE 0: MVCC - Returns a map of documentID -> slice of document wrappers (all versions)
// Phase 5 will add MVCC filtering to only keep visible versions
//
// TODO: Add support for compressed bundle formats in future
func (cm *CompactionManager) parseBundleDocuments(filePath string) (map[string][]*bundleDocumentWrapper, error) {
	documents := make(map[string][]*bundleDocumentWrapper)

	// Read entire file into memory
	// TODO: Implement streaming for very large files (>1GB)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bundle file: %w", err)
	}

	offset := int64(0)

	for offset < int64(len(data)) {
		// Need at least 8 bytes for header (magic + size)
		if offset+8 > int64(len(data)) {
			break
		}

		// Read magic number and size
		magic := binary.LittleEndian.Uint32(data[offset : offset+4])
		size := binary.LittleEndian.Uint32(data[offset+4 : offset+8])

		// Validate size to prevent out-of-bounds access
		if offset+8+int64(size) > int64(len(data)) {
			cm.logger.Warnw("Invalid document size at offset, stopping parse",
				"offset", offset,
				"size", size,
				"fileSize", len(data))
			break
		}

		// Extract document/tombstone data
		docData := data[offset+8 : offset+8+int64(size)]

		if magic == 0xDEADBEEF {
			// Active document
			docMap, err := helpers.DecodeFastBinary(docData)
			if err != nil {
				cm.logger.Warnw("Failed to decode document, skipping",
					"offset", offset,
					"error", err)
				offset += 8 + int64(size)
				continue
			}

			// Convert to Document struct
			doc := models.Document{}
			if docID, ok := docMap["DocumentID"].(string); ok {
				doc.DocumentID = docID
			}
			if fields, ok := docMap["Fields"].(map[string]models.Field); ok {
				doc.Fields = fields
			}
			if createdAt, ok := docMap["CreatedAt"].(time.Time); ok {
				doc.CreatedAt = createdAt
			}
			if updatedAt, ok := docMap["UpdatedAt"].(time.Time); ok {
				doc.UpdatedAt = updatedAt
			}
			// PHASE 0: MVCC - Extract version metadata fields
			if createdByTxID, ok := docMap["CreatedByTxID"].(uint64); ok {
				doc.CreatedByTxID = createdByTxID
			}
			if deletedByTxID, ok := docMap["DeletedByTxID"].(uint64); ok {
				doc.DeletedByTxID = deletedByTxID
			}
			if commitSequence, ok := docMap["CommitSequence"].(uint64); ok {
				doc.CommitSequence = commitSequence
			}
			if versionSequence, ok := docMap["VersionSequence"].(uint64); ok {
				doc.VersionSequence = versionSequence
			}

			// PHASE 0: MVCC - Store all versions (temporary, Phase 5 will add MVCC filtering)
			wrapper := &bundleDocumentWrapper{
				document:    doc,
				isTombstone: false,
				offset:      offset,
				timestamp:   doc.UpdatedAt,
			}

			// Append to versions list (keep all versions)
			documents[doc.DocumentID] = append(documents[doc.DocumentID], wrapper)

		} else if magic == 0xDEADDEAD {
			// Deletion marker (tombstone)
			deletionMap, err := helpers.DecodeFastBinary(docData)
			if err != nil {
				cm.logger.Warnw("Failed to decode deletion marker, skipping",
					"offset", offset,
					"error", err)
				offset += 8 + int64(size)
				continue
			}

			if documentID, ok := deletionMap["DocumentID"].(string); ok && documentID != "" {
				// Mark document as deleted
				wrapper := &bundleDocumentWrapper{
					document: models.Document{
						DocumentID: documentID,
					},
					isTombstone: true,
					offset:      offset,
					timestamp:   time.Now(), // TODO: Extract actual deletion timestamp if available
				}

				// PHASE 0: MVCC - Append tombstone to versions list (keep all versions)
				documents[documentID] = append(documents[documentID], wrapper)

				cm.logger.Debugw("Found deletion marker",
					"documentID", documentID,
					"offset", offset)
			}
		} else {
			// Unknown magic number - corrupted data
			cm.logger.Warnw("Unknown magic number, stopping parse",
				"offset", offset,
				"magic", fmt.Sprintf("0x%X", magic))
			break
		}

		offset += 8 + int64(size)
	}

	cm.logger.Infow("Parsed bundle documents",
		"filePath", filePath,
		"totalDocuments", len(documents))

	return documents, nil
}

// isTombstoneDocument checks if a document wrapper represents a deletion marker
// Simple helper to improve code readability
func (cm *CompactionManager) isTombstoneDocument(wrapper *bundleDocumentWrapper) bool {
	return wrapper.isTombstone
}

// writeBundleDocument writes a single document to the output file with proper formatting
// Follows the AppendDocumentToBundleFile pattern from BundleStorageEngine
// Returns bytes written and any error
//
// TODO: Add document compression option for large payloads
func (cm *CompactionManager) writeBundleDocument(file *os.File, doc *models.Document) (int, error) {
	// Serialize document using fast binary format
	docMap := map[string]interface{}{
		"DocumentID": doc.DocumentID,
		"Fields":     doc.Fields,
		"CreatedAt":  doc.CreatedAt,
		"UpdatedAt":  doc.UpdatedAt,
	}

	documentBytes, err := helpers.EncodeFastBinary(docMap)
	if err != nil {
		return 0, fmt.Errorf("failed to encode document %s: %w", doc.DocumentID, err)
	}

	// Create header with magic number and size
	headerSize := uint32(len(documentBytes))
	headerBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADBEEF) // Magic number for active documents
	binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

	// Write header
	n1, err := file.Write(headerBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to write header: %w", err)
	}

	// Write document data
	n2, err := file.Write(documentBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to write document data: %w", err)
	}

	return n1 + n2, nil
}

// ----- BUCKET-AWARE COMPACTION (Phase 4: xxHash Optimization) -----

// CompactBucketFiles compacts all files for a specific bucket
// This enables parallel compaction across buckets for better performance
//
// Parameters:
//   - fieldName: Name of the indexed field
//   - isForeignKey: Whether this is a foreign key index
//   - bucketNum: Bucket number to compact
//   - bucketFiles: List of bucket files to compact (absolute paths)
//
// Returns the path to the compacted file and any error
func (cm *CompactionManager) CompactBucketFiles(fieldName string, isForeignKey bool, bucketNum uint32, bucketFiles []string) (string, error) {
	if len(bucketFiles) == 0 {
		return "", fmt.Errorf("no files to compact for bucket %d", bucketNum)
	}

	// Check if already compacting (single-threaded safety check)
	// Note: For parallel compaction, each bucket gets its own goroutine
	cm.mutex.Lock()
	if cm.compacting {
		cm.mutex.Unlock()
		return "", fmt.Errorf("compaction already in progress for bucket %d", bucketNum)
	}
	cm.compacting = true
	cm.mutex.Unlock()

	defer func() {
		cm.mutex.Lock()
		cm.compacting = false
		cm.mutex.Unlock()
	}()

	startTime := time.Now()

	cm.logger.Infow("Starting bucket compaction",
		"fieldName", fieldName,
		"isForeignKey", isForeignKey,
		"bucketNum", bucketNum,
		"fileCount", len(bucketFiles))

	// Perform compaction
	compactedFile, stats, err := cm.compactBucketFilesInternal(fieldName, isForeignKey, bucketNum, bucketFiles)

	duration := time.Since(startTime)

	// Update statistics
	cm.updateStats(stats, duration, err)

	if err != nil {
		cm.logger.Errorw("Bucket compaction failed",
			"fieldName", fieldName,
			"bucketNum", bucketNum,
			"duration", duration,
			"error", err)
		return "", err
	}

	cm.logger.Infow("Bucket compaction completed",
		"fieldName", fieldName,
		"bucketNum", bucketNum,
		"duration", duration,
		"inputFiles", len(bucketFiles),
		"outputFile", compactedFile,
		"entriesKept", stats.TotalEntriesKept,
		"entriesRemoved", stats.TotalEntriesRemoved)

	return compactedFile, nil
}

// compactBucketFilesInternal performs the actual bucket compaction logic
// Separated from main function for cleaner error handling
func (cm *CompactionManager) compactBucketFilesInternal(fieldName string, isForeignKey bool, bucketNum uint32, bucketFiles []string) (string, *CompactionStats, error) {
	stats := CompactionStats{}

	// Sort files by entry number (ensures temporal ordering within bucket)
	sortedFiles := make([]string, len(bucketFiles))
	copy(sortedFiles, bucketFiles)
	sort.Strings(sortedFiles)

	// Create temporary output file
	fkSuffix := ""
	if isForeignKey {
		fkSuffix = "_fk"
	}
	outputFileName := fmt.Sprintf("%s%s_bucket_%06d_compacted_%d.hidx.tmp", fieldName, fkSuffix, bucketNum, time.Now().UnixNano())
	outputFilePath := filepath.Join(cm.dataDir, outputFileName)

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return "", &stats, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	// PHASE 5: MVCC - Get oldest snapshot sequence to determine which entries to preserve
	var oldestSnapshotSeq uint64
	if cm.getOldestSnapshot != nil {
		oldestSnapshotSeq = cm.getOldestSnapshot()
		cm.logger.Debugw("MVCC bucket compaction filtering",
			"bucketNum", bucketNum,
			"oldestSnapshotSeq", oldestSnapshotSeq)
	} else {
		// No snapshot getter - preserve all entries (safe default)
		oldestSnapshotSeq = 0 // 0 means preserve all
	}

	// Track unique keys and their latest entries
	// Map: key -> latest entry
	latestEntries := make(map[string]*hashIndexEntryWrapper)

	// Read all entries from all files
	// Latest file wins for duplicate keys
	for _, filePath := range sortedFiles {
		entries, err := cm.readEntriesFromFile(filePath)
		if err != nil {
			return "", &stats, fmt.Errorf("failed to read entries from %s: %w", filePath, err)
		}

		stats.TotalFilesCompacted++

		// Process entries - latest wins
		for _, entry := range entries {
			// Verify entry belongs to this bucket (sanity check)
			if entry.BucketNum != bucketNum {
				cm.logger.Warnw("Entry has wrong bucket number, skipping",
					"expectedBucket", bucketNum,
					"actualBucket", entry.BucketNum,
					"key", entry.KeyValue,
					"file", filePath)
				continue
			}

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

	// PHASE 5: MVCC - Filter entries: preserve those with CommitSequence >= oldestSnapshot
	// Write non-tombstone entries to output file
	var entriesToWrite []*hashindexV3.HashIndexEntry
	preservedCount := 0
	removedCount := 0

	for _, wrapper := range latestEntries {
		entry := wrapper.entry
		shouldPreserve := false

		if oldestSnapshotSeq == 0 {
			// No snapshot filtering - preserve all entries (safe default)
			shouldPreserve = true
		} else {
			// Preserve if:
			// 1. Uncommitted (CommitSequence == 0) - may be visible to active transactions
			// 2. Committed and visible to oldest snapshot (CommitSequence >= oldestSnapshotSeq)
			if entry.CommitSequence == 0 {
				// Uncommitted - preserve to be safe
				shouldPreserve = true
			} else if entry.CommitSequence >= oldestSnapshotSeq {
				// Committed and visible to oldest snapshot - preserve
				shouldPreserve = true
			} else {
				// Committed before oldest snapshot - safe to remove
				shouldPreserve = false
			}
		}

		if shouldPreserve {
			// PHASE 5: MVCC - Preserve tombstones if they're visible to active snapshots
			// Tombstones are needed for MVCC visibility filtering
			entriesToWrite = append(entriesToWrite, entry)
			preservedCount++
			stats.TotalEntriesKept++
		} else {
			// Entry not visible to any active snapshot - safe to remove
			removedCount++
			stats.TotalEntriesRemoved++
			cm.logger.Debugw("Removing bucket index entry (not visible to any active snapshot)",
				"key", entry.KeyValue,
				"bucketNum", bucketNum,
				"commitSequence", entry.CommitSequence,
				"oldestSnapshotSeq", oldestSnapshotSeq)
		}
	}

	cm.logger.Debugw("Bucket compaction filtering complete",
		"bucketNum", bucketNum,
		"preservedEntries", preservedCount,
		"removedEntries", removedCount,
		"oldestSnapshotSeq", oldestSnapshotSeq)

	// Sort entries by key for better locality
	sort.Slice(entriesToWrite, func(i, j int) bool {
		return entriesToWrite[i].KeyValue < entriesToWrite[j].KeyValue
	})

	// Write entries to output file
	for _, entry := range entriesToWrite {
		data, err := cm.serializeEntry(entry)
		if err != nil {
			return "", &stats, fmt.Errorf("failed to serialize entry: %w", err)
		}

		n, err := outputFile.Write(data)
		if err != nil {
			return "", &stats, fmt.Errorf("failed to write entry: %w", err)
		}

		stats.TotalBytesWritten += uint64(n)
	}

	// Sync to disk
	if err := outputFile.Sync(); err != nil {
		return "", &stats, fmt.Errorf("failed to sync output file: %w", err)
	}

	// Close output file before rename
	outputFile.Close()

	// Generate final file name with bucket-aware format
	// Format: FieldName(_fk)?_bucket_NNNNNN_entry_000000.hidx
	finalFileName := fmt.Sprintf("%s%s_bucket_%06d_entry_000000.hidx", fieldName, fkSuffix, bucketNum)
	finalFilePath := filepath.Join(cm.dataDir, finalFileName)

	// Rename temp file to final name (atomic on most systems)
	if err := os.Rename(outputFilePath, finalFilePath); err != nil {
		// Clean up temp file
		os.Remove(outputFilePath)
		return "", &stats, fmt.Errorf("failed to rename output file: %w", err)
	}

	cm.logger.Infow("Compacted bucket file created",
		"outputFile", finalFilePath,
		"bucketNum", bucketNum,
		"entriesKept", stats.TotalEntriesKept,
		"entriesRemoved", stats.TotalEntriesRemoved,
		"bytesWritten", stats.TotalBytesWritten)

	return finalFilePath, &stats, nil
}

// CompactAllBucketsParallel compacts all buckets in parallel
// This provides significant performance improvement for large indexes
//
// Parameters:
//   - fieldName: Name of the indexed field
//   - isForeignKey: Whether this is a foreign key index
//   - bucketFileMap: Map of bucketNum -> list of files for that bucket
//   - maxConcurrency: Maximum number of parallel compactions (e.g., 256 for all buckets)
//
// Returns:
//   - compactedFiles: Map of bucketNum -> compacted file path
//   - errors: Map of bucketNum -> error (if any)
func (cm *CompactionManager) CompactAllBucketsParallel(fieldName string, isForeignKey bool, bucketFileMap map[uint32][]string, maxConcurrency int) (map[uint32]string, map[uint32]error) {
	if maxConcurrency <= 0 {
		maxConcurrency = 4 // Default concurrency
	}

	compactedFiles := make(map[uint32]string)
	errors := make(map[uint32]error)

	// Use channel to limit concurrency
	semaphore := make(chan struct{}, maxConcurrency)

	// Use wait group to wait for all compactions
	var wg sync.WaitGroup
	var resultMutex sync.Mutex

	cm.logger.Infow("Starting parallel bucket compaction",
		"fieldName", fieldName,
		"totalBuckets", len(bucketFileMap),
		"maxConcurrency", maxConcurrency)

	startTime := time.Now()

	for bucketNum, files := range bucketFileMap {
		if len(files) == 0 {
			continue // Skip empty buckets
		}

		wg.Add(1)
		go func(bNum uint32, bFiles []string) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Compact this bucket
			// Note: We temporarily disable the compacting flag check for parallel compaction
			cm.mutex.Lock()
			wasCompacting := cm.compacting
			cm.compacting = false // Allow parallel operations
			cm.mutex.Unlock()

			compactedFile, err := cm.CompactBucketFiles(fieldName, isForeignKey, bNum, bFiles)

			// Restore compacting state
			cm.mutex.Lock()
			cm.compacting = wasCompacting
			cm.mutex.Unlock()

			// Store results
			resultMutex.Lock()
			if err != nil {
				errors[bNum] = err
			} else {
				compactedFiles[bNum] = compactedFile
			}
			resultMutex.Unlock()
		}(bucketNum, files)
	}

	// Wait for all compactions to complete
	wg.Wait()

	duration := time.Since(startTime)

	cm.logger.Infow("Parallel bucket compaction completed",
		"fieldName", fieldName,
		"totalBuckets", len(bucketFileMap),
		"successfulBuckets", len(compactedFiles),
		"failedBuckets", len(errors),
		"duration", duration)

	return compactedFiles, errors
}
