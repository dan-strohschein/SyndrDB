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
	"time"

	"go.uber.org/zap"

	"syndrdb/src/internal/domain/index/hashindexV3"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
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
	var entriesToWrite []*hashindexV3.HashIndexEntry
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

	// Step 2: Separate active documents from tombstones
	activeDocuments := make([]*models.Document, 0)
	tombstoneCount := 0

	for _, wrapper := range documentWrappers {
		if cm.isTombstoneDocument(wrapper) {
			tombstoneCount++
			cm.logger.Debugw("Skipping tombstone document",
				"documentID", wrapper.document.DocumentID)
		} else {
			activeDocuments = append(activeDocuments, &wrapper.document)
		}
	}

	cm.logger.Infow("Document analysis complete",
		"totalDocuments", len(documentWrappers),
		"activeDocuments", len(activeDocuments),
		"tombstones", tombstoneCount)

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
	if syncErr := tempFile.Sync(); syncErr != nil {
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

	cm.updateStats(stats, duration, nil)

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

func (cm *CompactionManager) UpdateStats(stats CompactionStats, duration time.Duration, err error) {
	cm.UpdateStats(stats, duration, err)
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
// Returns a map of documentID -> document wrapper
//
// TODO: Add support for compressed bundle formats in future
func (cm *CompactionManager) parseBundleDocuments(filePath string) (map[string]*bundleDocumentWrapper, error) {
	documents := make(map[string]*bundleDocumentWrapper)

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

			// Store or update document (latest wins)
			wrapper := &bundleDocumentWrapper{
				document:    doc,
				isTombstone: false,
				offset:      offset,
				timestamp:   doc.UpdatedAt,
			}

			// If document already exists, keep the newer one
			if existing, exists := documents[doc.DocumentID]; exists {
				if wrapper.timestamp.After(existing.timestamp) {
					documents[doc.DocumentID] = wrapper
				}
			} else {
				documents[doc.DocumentID] = wrapper
			}

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

				// Tombstone always wins (marks document for deletion)
				if existing, exists := documents[documentID]; exists {
					if wrapper.timestamp.After(existing.timestamp) {
						documents[documentID] = wrapper
					}
				} else {
					documents[documentID] = wrapper
				}

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
func (cm *CompactionManager) compactBucketFilesInternal(fieldName string, isForeignKey bool, bucketNum uint32, bucketFiles []string) (string, CompactionStats, error) {
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

	// Write non-tombstone entries to output file
	var entriesToWrite []*hashindexV3.HashIndexEntry
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

	// Generate final file name with bucket-aware format
	// Format: FieldName(_fk)?_bucket_NNNNNN_entry_000000.hidx
	finalFileName := fmt.Sprintf("%s%s_bucket_%06d_entry_000000.hidx", fieldName, fkSuffix, bucketNum)
	finalFilePath := filepath.Join(cm.dataDir, finalFileName)

	// Rename temp file to final name (atomic on most systems)
	if err := os.Rename(outputFilePath, finalFilePath); err != nil {
		// Clean up temp file
		os.Remove(outputFilePath)
		return "", stats, fmt.Errorf("failed to rename output file: %w", err)
	}

	cm.logger.Infow("Compacted bucket file created",
		"outputFile", finalFilePath,
		"bucketNum", bucketNum,
		"entriesKept", stats.TotalEntriesKept,
		"entriesRemoved", stats.TotalEntriesRemoved,
		"bytesWritten", stats.TotalBytesWritten)

	return finalFilePath, stats, nil
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
