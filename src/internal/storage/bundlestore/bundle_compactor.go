package bundlestore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common"
	"time"

	"go.uber.org/zap"
)

/*
BUNDLE FILE COMPACTOR

Implements file-level compaction for multi-file bundle storage with PostgreSQL-inspired triggers.
Merges multiple segment files, removes tombstones, and creates optimized output files.

COMPACTION TRIGGERS (4 conditions):
1. Tombstone Ratio > 20%: Too many deleted documents
2. File Count > 10: Too many small files causing read amplification
3. Small File Merge: Files < 32MB and older than 1 hour
4. Large Bundle Fragmentation > 30%: Large bundles (> 1GB) with high fragmentation

POSTGRESQL INSPIRATION:
- Similar to autovacuum but at file level (not tuple level)
- Uses cost-based I/O throttling like autovacuum_cost_delay
- Tracks file age like autovacuum_naptime
- Multi-worker architecture like autovacuum_max_workers
*/

// CompactionTrigger identifies why compaction was initiated
type CompactionTrigger int

const (
	TriggerTombstoneRatio CompactionTrigger = iota // Trigger A: Tombstone ratio exceeds threshold
	TriggerFileCount                               // Trigger B: Too many files
	TriggerSmallFileMerge                          // Trigger C: Small old files need merging
	TriggerFragmentation                           // Trigger D: Large bundle fragmentation
)

func (t CompactionTrigger) String() string {
	switch t {
	case TriggerTombstoneRatio:
		return "tombstone_ratio"
	case TriggerFileCount:
		return "file_count"
	case TriggerSmallFileMerge:
		return "small_file_merge"
	case TriggerFragmentation:
		return "fragmentation"
	default:
		return "unknown"
	}
}

// CompactionRequest represents a compaction job
type CompactionRequest struct {
	DatabaseName string
	BundleName   string
	Trigger      CompactionTrigger
	Priority     int // Higher = more urgent (based on tombstone ratio)
	RequestedAt  time.Time
}

// BundleCompactor handles file-level compaction for bundle storage
type BundleCompactor struct {
	dataDir           string
	logger            *zap.SugaredLogger
	storageEngine     *BundleStorageEngine
	compactionMutex   sync.Mutex      // Protects active compactions map
	activeCompactions map[string]bool // bundleName -> is compacting

	// I/O throttling for rate-limited compaction
	throttler *IOThrottler

	// Configuration thresholds
	tombstoneRatioThreshold float64
	fileCountThreshold      int
	smallFileAgeHours       int
	smallFileSizeMB         int
	largeBundleSizeGB       int
	fragmentationThreshold  float64
}

// NewBundleCompactor creates a new bundle compactor
func NewBundleCompactor(dataDir string, storageEngine *BundleStorageEngine, logger *zap.SugaredLogger) *BundleCompactor {
	// TODO: I will load these from settings.GetSettings().Compaction when CompactionConfig is added
	// Initialize I/O throttler (50 MB/s normal, 10 MB/s degraded)
	throttler := NewIOThrottler(50.0, 10.0, logger)

	return &BundleCompactor{
		dataDir:                 dataDir,
		logger:                  logger,
		storageEngine:           storageEngine,
		activeCompactions:       make(map[string]bool),
		throttler:               throttler,
		tombstoneRatioThreshold: 0.20, // 20%
		fileCountThreshold:      10,
		smallFileAgeHours:       1,
		smallFileSizeMB:         32,
		largeBundleSizeGB:       1,
		fragmentationThreshold:  0.30, // 30%
	}
}

// ShouldCompact evaluates all 4 trigger conditions and returns the trigger reason
func (bc *BundleCompactor) ShouldCompact(databaseName, bundleName string) (bool, CompactionTrigger, error) {
	// Check if already compacting
	bc.compactionMutex.Lock()
	bundleKey := databaseName + ":" + bundleName
	if bc.activeCompactions[bundleKey] {
		bc.compactionMutex.Unlock()
		return false, 0, nil // Already compacting
	}
	bc.compactionMutex.Unlock()

	// Load manifest to get file statistics
	manifestMgr := bc.storageEngine.getOrCreateManifestManager(databaseName, bundleName)
	manifest, err := manifestMgr.LoadOrCreate(databaseName, bundleName)
	if err != nil {
		return false, 0, fmt.Errorf("failed to load manifest: %w", err)
	}

	// No files = nothing to compact
	if len(manifest.Files) <= 1 {
		return false, 0, nil
	}

	// TRIGGER A: Tombstone ratio exceeds threshold (20%)
	if manifest.TotalDocuments+manifest.TotalTombstones > 0 {
		tombstoneRatio := float64(manifest.TotalTombstones) / float64(manifest.TotalDocuments+manifest.TotalTombstones)
		if tombstoneRatio > bc.tombstoneRatioThreshold {
			bc.logger.Infof("Compaction triggered for %s:%s - tombstone ratio %.2f%% exceeds threshold %.2f%%",
				databaseName, bundleName, tombstoneRatio*100, bc.tombstoneRatioThreshold*100)
			return true, TriggerTombstoneRatio, nil
		}
	}

	// TRIGGER B: File count exceeds threshold (10 files)
	if len(manifest.Files) > bc.fileCountThreshold {
		bc.logger.Infof("Compaction triggered for %s:%s - file count %d exceeds threshold %d",
			databaseName, bundleName, len(manifest.Files), bc.fileCountThreshold)
		return true, TriggerFileCount, nil
	}

	// TRIGGER C: Small file merge - files < 32MB and older than 1 hour
	smallFileSizeBytes := int64(bc.smallFileSizeMB) * 1024 * 1024
	smallFileAgeThreshold := time.Now().Add(-time.Duration(bc.smallFileAgeHours) * time.Hour)
	smallFileCount := 0
	for _, file := range manifest.Files {
		if file.FileSize < smallFileSizeBytes && file.CreatedAt.Before(smallFileAgeThreshold) {
			smallFileCount++
		}
	}
	if smallFileCount >= 3 { // Merge if 3+ small old files
		bc.logger.Infof("Compaction triggered for %s:%s - %d small old files need merging",
			databaseName, bundleName, smallFileCount)
		return true, TriggerSmallFileMerge, nil
	}

	// TRIGGER D: Large bundle fragmentation (> 1GB bundle with > 30% fragmentation)
	totalSize := int64(0)
	for _, file := range manifest.Files {
		totalSize += file.FileSize
	}
	largeBundleSizeBytes := int64(bc.largeBundleSizeGB) * 1024 * 1024 * 1024
	if totalSize > largeBundleSizeBytes {
		// Calculate fragmentation as (tombstones + small files) / total documents
		fragmentation := 0.0
		if manifest.TotalDocuments > 0 {
			fragmentation = float64(manifest.TotalTombstones+int64(smallFileCount*1000)) / float64(manifest.TotalDocuments)
		}
		if fragmentation > bc.fragmentationThreshold {
			bc.logger.Infof("Compaction triggered for %s:%s - large bundle fragmentation %.2f%% exceeds threshold %.2f%%",
				databaseName, bundleName, fragmentation*100, bc.fragmentationThreshold*100)
			return true, TriggerFragmentation, nil
		}
	}

	return false, 0, nil
}

// Compact performs file-level compaction on a bundle
// Merges multiple files, removes tombstones, and creates optimized output files
func (bc *BundleCompactor) Compact(databaseName, bundleName string, trigger CompactionTrigger) error {
	// Mark as compacting
	bc.compactionMutex.Lock()
	bundleKey := databaseName + ":" + bundleName
	if bc.activeCompactions[bundleKey] {
		bc.compactionMutex.Unlock()
		return fmt.Errorf("bundle %s:%s is already being compacted", databaseName, bundleName)
	}
	bc.activeCompactions[bundleKey] = true
	bc.compactionMutex.Unlock()

	// Ensure cleanup on exit
	defer func() {
		bc.compactionMutex.Lock()
		delete(bc.activeCompactions, bundleKey)
		bc.compactionMutex.Unlock()
	}()

	startTime := time.Now()
	bc.logger.Infow("Starting compaction",
		"database", databaseName,
		"bundle", bundleName,
		"trigger", trigger.String())

	// Load manifest
	manifestMgr := bc.storageEngine.getOrCreateManifestManager(databaseName, bundleName)
	manifest, err := manifestMgr.LoadOrCreate(databaseName, bundleName)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	// Flush write buffers before compaction to ensure all data is on disk
	if err := bc.storageEngine.FlushWriteBuffers(bundleName); err != nil {
		bc.logger.Warnf("Failed to flush write buffers before compaction: %v", err)
	}

	// STEP 1: Read and merge all files with last-write-wins
	mergedDocuments := make(map[string]models.Document)
	var totalBytesRead int64

	for _, fileInfo := range manifest.Files {
		// Skip immutable files if trigger is small file merge (only merge small mutable files)
		if trigger == TriggerSmallFileMerge && fileInfo.IsImmutable {
			continue
		}

		bundleDir := GetBundleDirectory(databaseName, bundleName)
		filePath := filepath.Join(bundleDir, fileInfo.FileName)

		// Check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			bc.logger.Debugf("Skipping non-existent file %s (likely already compacted)", filePath)
			continue
		}

		// Read file
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			bc.logger.Warnf("Failed to read file %s: %v", filePath, err)
			continue
		}
		totalBytesRead += int64(len(fileData))

		// Throttle read I/O (PostgreSQL autovacuum_cost_delay equivalent)
		bc.throttler.Throttle(int64(len(fileData)))

		// Parse all documents from this file (no range filtering)
		// PROJECTION PUSHDOWN: Pass nil projection fields (full deserialization) for compaction
		fileDocuments, _, err := bc.storageEngine.parseAppendedDocumentsRange(&fileData, 0, ^uint32(0), nil)
		if err != nil {
			bc.logger.Warnf("Failed to parse documents from file %s: %v", filePath, err)
			continue
		}

		// Merge with last-write-wins (later files overwrite earlier files)
		for docID, doc := range fileDocuments {
			mergedDocuments[docID] = doc
		}
	}

	// STEP 2: Filter out tombstones (documents with empty DocumentID)
	liveDocuments := make(map[string]models.Document)
	tombstoneCount := 0
	for docID, doc := range mergedDocuments {
		if doc.DocumentID == "" {
			tombstoneCount++
			continue
		}
		liveDocuments[docID] = doc
	}

	bc.logger.Infof("Compaction merge complete: %d live documents, %d tombstones removed",
		len(liveDocuments), tombstoneCount)

	// STEP 3: Build bloom filter for the compacted documents
	// This allows fast document ID lookups without reading the file
	documentIDs := make([]string, 0, len(liveDocuments))
	for docID := range liveDocuments {
		documentIDs = append(documentIDs, docID)
	}
	bloomFilter := BuildBloomFilterForDocuments(documentIDs, 0.01) // 1% false positive rate

	// STEP 4: Write compacted output file(s)
	// TODO: Split into multiple files if needed (32MB each)
	// maxFileSizeBytes := int64(settings.GetSettings().Storage.BundleFileMaxSizeMB) * 1024 * 1024
	outputFileID := 1
	currentFileSize := int64(0)

	// Get next available file ID
	maxExistingFileID := 0
	for _, fileInfo := range manifest.Files {
		if fileInfo.FileID > maxExistingFileID {
			maxExistingFileID = fileInfo.FileID
		}
	}
	outputFileID = maxExistingFileID + 1

	bundleDir := GetBundleDirectory(databaseName, bundleName)
	tempFilePath := filepath.Join(bundleDir, fmt.Sprintf("%06d.compact.tmp", outputFileID))

	// Create temp file for compaction output
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("failed to create temp compaction file: %w", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFilePath) // Clean up temp file on error
	}()

	// Write documents to output file
	documentsWritten := 0
	for _, doc := range liveDocuments {
		// Serialize document
		docBytes, err := bc.storageEngine.serializeDocumentDirect(&doc)
		if err != nil {
			bc.logger.Warnf("Failed to serialize document %s: %v", doc.DocumentID, err)
			continue
		}

		// Write header (magic + size)
		headerBytes := make([]byte, 8)
		binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADBEEF)
		binary.LittleEndian.PutUint32(headerBytes[4:8], uint32(len(docBytes)))

		if _, err := tempFile.Write(headerBytes); err != nil {
			return fmt.Errorf("failed to write document header: %w", err)
		}
		if _, err := tempFile.Write(docBytes); err != nil {
			return fmt.Errorf("failed to write document data: %w", err)
		}

		bytesWritten := int64(len(headerBytes) + len(docBytes))
		currentFileSize += bytesWritten
		documentsWritten++

		// Throttle write I/O (PostgreSQL autovacuum cost-based delay)
		bc.throttler.Throttle(bytesWritten)

		// TODO: File splitting will be implemented when needed (if currentFileSize > maxFileSizeBytes)
	}

	// Flush and sync to disk
	if err := common.Fdatasync(tempFile); err != nil {
		return fmt.Errorf("failed to sync compacted file: %w", err)
	}
	tempFile.Close()

	// STEP 5: Atomically replace old files with compacted file
	// Use PostgreSQL-style atomic rename: write → fsync → rename
	finalFilePath := filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", outputFileID))
	if err := os.Rename(tempFilePath, finalFilePath); err != nil {
		return fmt.Errorf("failed to rename compacted file: %w", err)
	}

	// STEP 6: Update manifest - add new file, remove old files
	fileName := fmt.Sprintf("%06d.bnd", outputFileID)
	if err := manifestMgr.AddFile(outputFileID, fileName); err != nil {
		bc.logger.Warnf("Failed to add compacted file to manifest: %v", err)
	}

	// Update file stats
	if err := manifestMgr.UpdateFileStats(outputFileID, int64(documentsWritten), 0, 0, 0, currentFileSize); err != nil {
		bc.logger.Warnf("Failed to update compacted file stats: %v", err)
	}

	// Freeze the compacted file (make it immutable)
	if err := manifestMgr.FreezeFile(outputFileID); err != nil {
		bc.logger.Warnf("Failed to freeze compacted file: %v", err)
	}

	// Store bloom filter in manifest for read path optimization
	if bloomFilter != nil {
		bloomData, bloomSize, bloomHashes, err := SerializeBloomFilter(bloomFilter)
		if err != nil {
			bc.logger.Warnf("Failed to serialize bloom filter: %v", err)
		} else {
			if err := manifestMgr.UpdateBloomFilter(outputFileID, bloomData, bloomSize, bloomHashes); err != nil {
				bc.logger.Warnf("Failed to update bloom filter in manifest: %v", err)
			} else {
				bc.logger.Debugf("Bloom filter added to manifest for file %d (size: %d bits, %d hashes)",
					outputFileID, bloomSize, bloomHashes)
			}
		}
	}

	// Remove old files from manifest
	oldFileIDs := make([]int, 0, len(manifest.Files))
	for _, fileInfo := range manifest.Files {
		// Skip the newly created compacted file
		if fileInfo.FileID == outputFileID {
			continue
		}
		// Skip files that shouldn't be compacted (e.g., still being written to)
		if !fileInfo.IsImmutable && trigger != TriggerFragmentation {
			continue
		}
		oldFileIDs = append(oldFileIDs, fileInfo.FileID)
	}

	if err := manifestMgr.RemoveFiles(oldFileIDs); err != nil {
		bc.logger.Warnf("Failed to remove old files from manifest: %v", err)
	}

	// STEP 7: Delete old physical files
	for _, fileID := range oldFileIDs {
		oldFilePath := filepath.Join(bundleDir, fmt.Sprintf("%06d.bnd", fileID))
		if err := os.Remove(oldFilePath); err != nil {
			bc.logger.Warnf("Failed to delete old file %s: %v", oldFilePath, err)
		}
	}

	// Invalidate file-read cache for this bundle so subsequent reads see the new compacted segment
	bc.storageEngine.InvalidateFileReadCacheForBundle(databaseName, bundleName)

	duration := time.Since(startTime)

	// Get throttling statistics
	throttleStats := bc.throttler.GetStatistics()

	bc.logger.Infow("Compaction completed",
		"database", databaseName,
		"bundle", bundleName,
		"trigger", trigger.String(),
		"duration", duration,
		"documentsWritten", documentsWritten,
		"tombstonesRemoved", tombstoneCount,
		"oldFiles", len(oldFileIDs),
		"bytesRead", totalBytesRead,
		"bytesWritten", currentFileSize,
		"compressionRatio", float64(totalBytesRead)/float64(currentFileSize),
		"throttleMode", throttleStats.CurrentMode,
		"throttleSleep", throttleStats.TotalSleepTime,
		"throttleCount", throttleStats.ThrottleCount)

	return nil
}

// CalculatePriority determines compaction priority based on urgency
// Higher priority = more urgent (based on tombstone ratio)
func (bc *BundleCompactor) CalculatePriority(databaseName, bundleName string) int {
	manifestMgr := bc.storageEngine.getOrCreateManifestManager(databaseName, bundleName)
	manifest, err := manifestMgr.LoadOrCreate(databaseName, bundleName)
	if err != nil {
		return 0
	}

	if manifest.TotalDocuments+manifest.TotalTombstones == 0 {
		return 0
	}

	// Priority = tombstone ratio * 100 (0-100 scale)
	tombstoneRatio := float64(manifest.TotalTombstones) / float64(manifest.TotalDocuments+manifest.TotalTombstones)
	return int(tombstoneRatio * 100)
}

// SetDegradedMode switches I/O throttling to degraded mode (lower rate)
// Use this when system load is high or during peak hours
func (bc *BundleCompactor) SetDegradedMode(degraded bool) {
	bc.throttler.SetDegradedMode(degraded)
}

// GetThrottleStatistics returns current I/O throttling statistics
func (bc *BundleCompactor) GetThrottleStatistics() ThrottleStats {
	return bc.throttler.GetStatistics()
}
