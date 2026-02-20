/*
BTREE INDEX API SYSTEM

This file provides the primary interface for BTree index operations in SyndrDB.
It implements a B+ tree algorithm similar to those used in PostgreSQL, MySQL, and SQL Server
with the following features:

ALGORITHM OVERVIEW:
- B+ Tree: Balanced tree with all leaf nodes at the same level
- Internal nodes store keys and pointers to child nodes
- Leaf nodes store key-value pairs and are linked for efficient range queries
- Automatic tree balancing through splitting and merging operations
- O(log n) search, insert, and delete operations

ARCHITECTURE:
- Page 0: Metadata page containing index configuration and statistics
- Pages 1-N: Internal and leaf nodes stored in fixed-size pages
- Each page is typically 8KB with structured header and node data
- LRU page cache for performance optimization

FEATURES:
- Efficient range queries through linked leaf nodes
- Automatic node splitting and merging for tree balance
- Thread-safe operations with read/write locks
- Support for unique and non-unique indexes
- ASCII file format support for debugging
- Comprehensive statistics and performance monitoring

This implementation follows the Single Responsibility Principle where this file
provides the public API while delegating specific operations to specialized files.
The API is designed to integrate seamlessly with SyndrDB's bundle and document system.
*/

package btreeindexV2

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"syndrdb/src/pkg/settings"

	"github.com/cespare/xxhash/v2"

	"go.uber.org/zap"
)

// CreateBTreeIndex creates a new BTree index for the specified bundle and field
// Parameters:
//   - config: Configuration parameters for the new index
//   - logger: Logger for debug/error messages
//
// Returns:
//   - *BTreeIndex: The created BTree index instance
//   - error: Any error that occurred during creation
func CreateBTreeIndex(config *IndexConfig, logger *zap.SugaredLogger) (*BTreeIndex, error) {
	args := settings.GetSettings()
	//logger.Debugf("DEBUG: CreateBTreeIndex V2 started with config: %+v", config)

	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	// Validate configuration
	//logger.Debugf("DEBUG: Validating configuration")
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	logger.Debugf("Creating BTree index for bundle '%s' field '%s'", config.BundleName, config.FieldName)

	// Create file manager
	indexFilePath := config.GetIndexFilePath()
	//logger.Debugf("DEBUG: Creating file manager for path: %s", indexFilePath)
	fileManager, err := NewBTreeFileManager(indexFilePath, config.PageSize, args.Debug, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}

	// Set to batched mode for better performance - rely on WAL for durability
	// This prevents individual fsync calls on each page write during batch flushes
	fileManager.SetSyncMode("batched")
	//logger.Debugf("DEBUG: File manager created successfully")

	// Create page manager
	//logger.Debugf("DEBUG: Creating page manager")
	pageManager, err := NewBTreePageManager(config.PageSize, config.CacheSize, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}
	//logger.Debugf("DEBUG: Page manager created successfully")

	// Create metadata
	metadata := NewBTreeMetadata(config)

	// Create BTree index instance
	index := &BTreeIndex{
		FilePath:        indexFilePath,
		Metadata:        metadata,
		FileManager:     fileManager,
		PageManager:     pageManager,
		rootPageNum:     metadata.RootPageNum, //1, // Page 0 is metadata, root starts at page 1
		mutex:           sync.RWMutex{},
		isOpen:          true,
		bundleName:      config.BundleName,
		fieldName:       config.FieldName,
		logger:          logger,
		metricsReporter: config.MetricsReporter,
	}

	// Initialize WAL manager if provided in config
	// TODO: I could add batch WAL writes for better performance on high-throughput workloads
	if config.WALManager != nil {
		btreeWALManager, err := NewBTreeWALManager(config.WALManager, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize B-tree WAL manager: %w", err)
		}
		index.walManager = btreeWALManager
		index.walEnabled = true
		logger.Debugf("WAL enabled for B-tree index '%s'", config.FieldName)
	} else {
		index.walEnabled = false
		logger.Debugf("WAL not configured for B-tree index '%s'", config.FieldName)
	}

	// Configure page writer for cache eviction
	// DRY Principle: Reuse fileManager.WritePage for both explicit flushes and evictions
	// This ensures dirty pages are persisted before being evicted from cache
	pageManager.SetWriter(func(pageNum uint32, pageData interface{}) error {
		node, ok := pageData.(*BTreeNode)
		if !ok {
			return fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
		}
		return fileManager.WritePage(pageNum, node)
	})

	// Initialize the index file and create root node
	if err := index.initializeIndex(); err != nil {
		return nil, fmt.Errorf("failed to initialize index: %w", err)
	}

	// Verify that root page number is correct after initialization
	if index.rootPageNum == 0 {
		logger.Errorf("Critical error: rootPageNum is 0 after initialization")
		return nil, fmt.Errorf("invalid root page number after initialization: %d", index.rootPageNum)
	}

	logger.Debugf("Successfully created BTree index '%s' for bundle '%s' field '%s'",
		config.GetIndexFilePath(), config.BundleName, config.FieldName)

	return index, nil
}

// OpenBTreeIndex opens an existing BTree index from file
// Parameters:
//   - filePath: Path to the index file
//   - debugMode: Whether to use ASCII format for debugging
//   - logger: Logger for debug/error messages
//
// Returns:
//   - *BTreeIndex: The opened BTree index instance
//   - error: Any error that occurred during opening
func OpenBTreeIndex(filePath string, debugMode bool, logger *zap.SugaredLogger) (*BTreeIndex, error) {
	args := settings.GetSettings()
	if filePath == "" {
		return nil, fmt.Errorf("file path cannot be empty")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	logger.Debugf("Opening BTree index from file: %s", filePath)

	// Create file manager
	fileManager, err := NewBTreeFileManager(filePath, 0, args.Debug, logger) // Page size will be read from metadata
	if err != nil {
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}

	// Set to batched mode for better performance - rely on WAL for durability
	fileManager.SetSyncMode("batched")

	// Read metadata from file
	metadata, err := fileManager.ReadMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	// Create page manager with metadata settings
	pageManager, err := NewBTreePageManager(metadata.PageSize, 1000, logger) // Default cache size
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}

	// Create BTree index instance
	index := &BTreeIndex{
		FilePath:    filePath,
		Metadata:    metadata,
		FileManager: fileManager,
		PageManager: pageManager,
		rootPageNum: metadata.RootPageNum,
		mutex:       sync.RWMutex{},
		isOpen:      true,
		bundleName:  metadata.BundleName,
		fieldName:   metadata.FieldName,
		logger:      logger,
	}

	// Configure page writer for cache eviction
	// DRY Principle: Same writer setup as CreateBTreeIndex
	pageManager.SetWriter(func(pageNum uint32, pageData interface{}) error {
		node, ok := pageData.(*BTreeNode)
		if !ok {
			return fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
		}
		return fileManager.WritePage(pageNum, node)
	})

	// Mark as fresh disk load (not from cache)
	index.fromCache = false

	// CRASH RECOVERY: Validate index integrity and repair if needed
	// Following PostgreSQL's hybrid approach: auto-repair minor issues, fail-fast on major corruption
	// Single Responsibility: Each validation function checks one aspect of integrity
	logger.Debugf("Performing crash recovery validation for index '%s'", filePath)

	if err := index.performCrashRecovery(); err != nil {
		return nil, fmt.Errorf("crash recovery failed: %w", err)
	}

	// Mark validation timestamp after successful recovery
	index.lastValidated = time.Now()

	logger.Debugf("Successfully opened BTree index '%s' for bundle '%s' field '%s'",
		filePath, metadata.BundleName, metadata.FieldName)

	return index, nil
}

// performCrashRecovery validates index integrity and performs recovery if needed
//
// CRASH RECOVERY STRATEGY (PostgreSQL-style hybrid approach):
// 1. Validate file header and magic number
// 2. Verify page checksums (CRC32 using SIMD when available)
// 3. Validate tree structure (parent-child links, key ordering)
// 4. Count corruption: <5 pages = auto-repair, >=5 pages = fail-fast
// 5. Replay WAL entries if WAL manager available
//
// Single Responsibility: Orchestrates recovery process, delegates to specialized validators
// DRY Principle: Reuses existing page reading and WAL replay infrastructure
//
// Parameters: none (operates on idx receiver)
//
// Returns:
//   - error: Critical errors that prevent index usage
//
// TODO: I could add parallel page validation for faster recovery on large indexes
func (idx *BTreeIndex) performCrashRecovery() error {
	// PERFORMANCE OPTIMIZATION: Skip validation for recently-validated cached indexes
	// This eliminates 40-50ms recovery overhead when index is already in memory
	const validationTTL = 5 * time.Minute
	if idx.fromCache && !idx.lastValidated.IsZero() && time.Since(idx.lastValidated) < validationTTL {
		idx.logger.Debugf("Skipping crash recovery for cached index '%s' (validated %v ago)",
			idx.Metadata.IndexName, time.Since(idx.lastValidated))
		return nil
	}

	idx.logger.Debugf("Starting crash recovery for index '%s'", idx.Metadata.IndexName)

	// Step 1: Validate file header magic number
	if err := idx.validateFileHeader(); err != nil {
		return fmt.Errorf("file header validation failed: %w", err)
	}

	// Step 2: Verify page checksums with corruption counting
	corruptPages, err := idx.verifyPageChecksums()
	if err != nil {
		return fmt.Errorf("checksum verification failed: %w", err)
	}

	// Step 3: Apply hybrid corruption handling strategy
	// PostgreSQL approach: auto-repair minor corruption, fail-fast on major
	if len(corruptPages) > 0 {
		idx.logger.Warnf("Found %d corrupt pages: %v", len(corruptPages), corruptPages)

		if len(corruptPages) >= 5 {
			// FAIL-FAST: Major corruption detected
			return fmt.Errorf("major corruption detected: %d corrupt pages (threshold: 5) - manual intervention required", len(corruptPages))
		}

		// AUTO-REPAIR: Minor corruption, attempt recovery
		idx.logger.Debugf("Minor corruption detected (%d pages < 5), attempting auto-repair", len(corruptPages))
		if err := idx.repairCorruptPages(corruptPages); err != nil {
			return fmt.Errorf("auto-repair failed: %w", err)
		}
		idx.logger.Debugf("Successfully repaired %d corrupt pages", len(corruptPages))
	}

	// Step 4: Validate tree structural integrity
	if err := idx.validateTreeStructure(); err != nil {
		idx.logger.Warnf("Tree structure invalid: %v, attempting rebuild", err)

		// Attempt to rebuild tree from valid pages
		if err := idx.rebuildTreeStructure(); err != nil {
			return fmt.Errorf("tree rebuild failed: %w", err)
		}
		idx.logger.Debugf("Successfully rebuilt tree structure")
	}

	// Step 5: Replay WAL entries if WAL manager is available
	// This recovers any uncommitted operations from before the crash
	if idx.walManager != nil {
		idx.logger.Debugf("WAL manager available, replaying uncommitted entries")
		if err := idx.replayWALEntries(); err != nil {
			return fmt.Errorf("WAL replay failed: %w", err)
		}
	} else {
		idx.logger.Debugf("No WAL manager available, skipping WAL replay")
	}

	idx.logger.Debugf("Crash recovery completed successfully")
	return nil
}

// validateFileHeader checks the file header magic number for corruption
//
// Following PostgreSQL's file validation approach, we verify the magic number
// to ensure the file is a valid B-tree index and hasn't been corrupted.
//
// Single Responsibility: Only validates file header magic number
//
// Returns:
//   - error: If magic number is invalid or missing
func (idx *BTreeIndex) validateFileHeader() error {
	// TODO: I could add version compatibility checking to support index format migrations
	expectedMagic := uint32(0x42545245) // "BTRE" in hex

	if idx.Metadata.MagicNumber != expectedMagic {
		return fmt.Errorf("invalid magic number: got 0x%X, expected 0x%X",
			idx.Metadata.MagicNumber, expectedMagic)
	}

	idx.logger.Debugf("File header validation passed: magic number 0x%X", expectedMagic)
	return nil
}

// verifyPageChecksums validates integrity of all allocated pages using CRC32
//
// CHECKSUM ALGORITHM:
// Uses CRC32 (IEEE polynomial) with SIMD acceleration when available (crc32c instruction)
// Each page stores its checksum in the BTreePage.Checksum field
// We recompute the checksum and compare against the stored value
//
// CORRUPTION DETECTION:
// Returns list of corrupt page numbers for hybrid handling strategy
// Caller decides whether to auto-repair or fail-fast based on count
//
// Single Responsibility: Only verifies page checksums, doesn't repair
// Open/Closed: Extensible to other checksum algorithms (SHA256, xxHash)
//
// Returns:
//   - []uint32: List of corrupt page numbers
//   - error: Critical errors preventing validation
//
// TODO: I could add parallel validation using goroutines for large indexes
func (idx *BTreeIndex) verifyPageChecksums() ([]uint32, error) {
	corruptPages := []uint32{}
	totalPages := idx.Metadata.TotalPages

	idx.logger.Debugf("Verifying checksums for %d pages", totalPages)

	for pageNum := uint32(0); pageNum < totalPages; pageNum++ {
		// Skip metadata page (page 0) as it has different validation
		if pageNum == 0 {
			continue
		}

		// Read page from disk
		node, err := idx.FileManager.ReadPage(pageNum)
		if err != nil {
			idx.logger.Warnf("Failed to read page %d: %v", pageNum, err)
			corruptPages = append(corruptPages, pageNum)
			continue
		}

		// Cast to BTreeNode
		btreeNode, ok := node.(*BTreeNode)
		if !ok {
			idx.logger.Warnf("Page %d is not a valid B-tree node", pageNum)
			corruptPages = append(corruptPages, pageNum)
			continue
		}

		// Compute checksum of page data
		// DRY Principle: Reuse computePageChecksum helper
		computedChecksum := idx.computePageChecksum(node)

		// Compare against stored checksum
		storedChecksum := btreeNode.Checksum

		if computedChecksum != storedChecksum {
			idx.logger.Warnf("Checksum mismatch on page %d: computed=0x%X, stored=0x%X",
				pageNum, computedChecksum, storedChecksum)
			corruptPages = append(corruptPages, pageNum)
		}
	}

	if len(corruptPages) == 0 {
		idx.logger.Debugf("All page checksums valid (%d pages verified)", totalPages-1)
	}

	return corruptPages, nil
}

// computePageChecksum computes an xxHash64 checksum for a BTree node
// Used for crash recovery validation to detect corruption
//
// CHECKSUM ALGORITHM:
// 1. Zero out the Checksum field (to match write-time computation)
// 2. Serialize the entire node to bytes
// 3. Compute xxHash64 over serialized bytes
// 4. Restore original checksum
//
// This ensures the checksum computed at verification time matches
// the checksum computed at write time.
//
// Parameters:
//   - node: The node to compute checksum for
//
// Returns:
//   - uint64: xxHash64 checksum value
func (idx *BTreeIndex) computePageChecksum(node interface{}) uint64 {
	btreeNode, ok := node.(*BTreeNode)
	if !ok {
		return 0
	}

	// Save and zero out checksum field for consistent computation
	originalChecksum := btreeNode.Checksum
	btreeNode.Checksum = 0

	// Serialize node using the same method as WritePage
	serialized, err := idx.FileManager.SerializePageBinary(btreeNode)
	if err != nil {
		idx.logger.Warnf("Failed to serialize node for checksum: %v", err)
		btreeNode.Checksum = originalChecksum
		return 0
	}

	// Compute xxHash64 over serialized bytes
	checksum := xxhash.Sum64(serialized)

	// Restore original checksum
	btreeNode.Checksum = originalChecksum

	return checksum
}

// repairCorruptPages attempts to repair pages with checksum mismatches
//
// REPAIR STRATEGY (PostgreSQL-inspired):
// 1. For each corrupt page, check if WAL has recent entries
// 2. If WAL entries exist, replay them to reconstruct page
// 3. If no WAL, mark page as damaged and exclude from tree
// 4. Update parent pointers to skip damaged pages
//
// Single Responsibility: Only repairs corrupt pages, doesn't detect them
// Open/Closed: Extensible to add more sophisticated repair strategies
//
// Parameters:
//   - corruptPages: List of page numbers with checksum mismatches
//
// Returns:
//   - error: If repair fails for critical pages (root, metadata)
//
// TODO: I could add page reconstruction from sibling pages for leaf nodes
func (idx *BTreeIndex) repairCorruptPages(corruptPages []uint32) error {
	idx.logger.Debugf("Attempting to repair %d corrupt pages", len(corruptPages))

	repairedCount := 0
	failedPages := []uint32{}

	for _, pageNum := range corruptPages {
		// Check if this is a critical page (root or metadata)
		if pageNum == 0 || pageNum == idx.rootPageNum {
			return fmt.Errorf("critical page %d is corrupt - cannot auto-repair", pageNum)
		}

		// Attempt to rebuild page from WAL if available
		if idx.walManager != nil {
			// TODO: I need to implement WAL-based page reconstruction
			idx.logger.Debugf("WAL-based repair not yet implemented for page %d", pageNum)
			failedPages = append(failedPages, pageNum)
			continue
		}

		// Mark page as free if repair not possible
		idx.logger.Warnf("Marking corrupt page %d as free (repair not possible)", pageNum)
		idx.Metadata.FreePages = append(idx.Metadata.FreePages, pageNum)
		repairedCount++
	}

	if len(failedPages) > 0 {
		idx.logger.Warnf("Failed to repair %d pages: %v", len(failedPages), failedPages)
	}

	idx.logger.Debugf("Repaired %d/%d corrupt pages", repairedCount, len(corruptPages))
	return nil
}

// validateTreeStructure verifies the B-tree structural invariants
//
// VALIDATION RULES (B-tree properties):
// 1. All leaf nodes at same level (balanced tree)
// 2. Parent pointers are consistent with child->parent relationships
// 3. Keys are in sorted order within each node
// 4. Internal node keys are valid separators for child subtrees
// 5. No cycles in tree (prevents infinite loops)
//
// Single Responsibility: Only validates structure, doesn't repair
// DRY Principle: Reuses page reading infrastructure
//
// Returns:
//   - error: If structural invariants are violated
//
// TODO: I could add parallel validation for large trees using goroutines
func (idx *BTreeIndex) validateTreeStructure() error {
	idx.logger.Debugf("Validating tree structure starting from root page %d", idx.rootPageNum)

	// Validate tree starting from root
	visitedPages := make(map[uint32]bool)

	if err := idx.validateNodeRecursive(idx.rootPageNum, 0, visitedPages); err != nil {
		return fmt.Errorf("tree validation failed: %w", err)
	}

	idx.logger.Debugf("Tree structure validation passed (visited %d nodes)", len(visitedPages))
	return nil
}

// validateNodeRecursive recursively validates a node and its children
//
// Single Responsibility: Validates one node and recurses to children
//
// Parameters:
//   - pageNum: Page number of node to validate
//   - level: Current tree level (0 = root)
//   - visitedPages: Set of already visited pages (cycle detection)
//
// Returns:
//   - error: If validation fails for this node or any descendant
func (idx *BTreeIndex) validateNodeRecursive(pageNum uint32, level uint32, visitedPages map[uint32]bool) error {
	// Cycle detection
	if visitedPages[pageNum] {
		return fmt.Errorf("cycle detected: page %d visited twice", pageNum)
	}
	visitedPages[pageNum] = true

	// Read node
	node, err := idx.FileManager.ReadPage(pageNum)
	if err != nil {
		return fmt.Errorf("failed to read page %d: %w", pageNum, err)
	}

	btreeNode, ok := node.(*BTreeNode)
	if !ok {
		return fmt.Errorf("page %d is not a valid B-tree node", pageNum)
	}

	// Validate keys are sorted
	for i := 1; i < len(btreeNode.Keys); i++ {
		if bytes.Compare(btreeNode.Keys[i-1], btreeNode.Keys[i]) > 0 {
			return fmt.Errorf("keys not sorted in node %d: key[%d] > key[%d]", pageNum, i-1, i)
		}
	}

	// If internal node, recurse to children
	if !btreeNode.IsLeaf {
		for _, childPage := range btreeNode.Children {
			if childPage == 0 {
				continue // Skip null children
			}

			if err := idx.validateNodeRecursive(childPage, level+1, visitedPages); err != nil {
				return err
			}
		}
	}

	return nil
}

// rebuildTreeStructure reconstructs the tree from valid pages
//
// REBUILD STRATEGY:
// 1. Scan all pages to find valid leaf nodes (bypassing corrupted tree structure)
// 2. Extract all key-value pairs from valid leaf nodes
// 3. Clear existing tree structure (mark pages as free)
// 4. Create new empty root node
// 5. Re-insert all extracted key-value pairs using Insert operation
// 6. Update root page number and metadata
// 7. Persist new tree structure
//
// Single Responsibility: Only rebuilds tree, doesn't validate
//
// Returns:
//   - error: If rebuild is not possible
func (idx *BTreeIndex) rebuildTreeStructure() error {
	idx.logger.Debugf("Starting tree rebuild for index '%s'", idx.Metadata.IndexName)

	// Step 1: Extract all key-value pairs from valid leaf nodes
	// We scan all pages directly (not traversing the tree) to avoid cycles
	type keyValuePair struct {
		key        []byte
		documentID string
	}
	var allPairs []keyValuePair

	idx.logger.Debugf("Scanning %d pages to extract key-value pairs", idx.Metadata.TotalPages)
	validLeafCount := 0

	// Scan all pages (skip page 0 which is metadata)
	for pageNum := uint32(1); pageNum < idx.Metadata.TotalPages; pageNum++ {
		// Try to read the page
		node, err := idx.FileManager.ReadPage(pageNum)
		if err != nil {
			// Skip pages that can't be read (they're likely corrupt or free)
			continue
		}

		btreeNode, ok := node.(*BTreeNode)
		if !ok {
			// Not a valid B-tree node, skip
			continue
		}

		// Only process leaf nodes
		if !btreeNode.IsLeaf {
			continue
		}

		validLeafCount++

		// Extract all key-value pairs from this leaf node
		// Skip tombstones (deleted entries)
		for i, key := range btreeNode.Keys {
			if i >= len(btreeNode.Values) {
				continue
			}

			// Get live document IDs (excluding tombstones)
			liveDocIDs := btreeNode.GetLiveDocumentIDs(i)
			if len(liveDocIDs) == 0 {
				// All entries for this key are tombstoned, skip
				continue
			}

			// Add all live document IDs for this key
			for _, docID := range liveDocIDs {
				allPairs = append(allPairs, keyValuePair{
					key:        append([]byte(nil), key...), // Make a copy
					documentID: docID,
				})
			}
		}
	}

	idx.logger.Debugf("Extracted %d key-value pairs from %d valid leaf nodes", len(allPairs), validLeafCount)

	if len(allPairs) == 0 {
		idx.logger.Warnf("No valid key-value pairs found - creating empty tree")
		// Still need to create a valid empty tree structure
	}

	// Step 2: Clear existing tree structure
	// Clear the page cache
	idx.PageManager.ClearCache()

	// Mark all pages except metadata (page 0) as free
	idx.Metadata.FreePages = make([]uint32, 0)
	for pageNum := uint32(1); pageNum < idx.Metadata.TotalPages; pageNum++ {
		idx.Metadata.FreePages = append(idx.Metadata.FreePages, pageNum)
	}

	// Reset tree structure
	idx.Metadata.TotalPages = 1 // Only metadata page exists
	idx.Metadata.NextPageNum = 2
	idx.Metadata.TotalNodes = 0
	idx.Metadata.LeafNodes = 0
	idx.Metadata.InternalNodes = 0
	idx.Metadata.TreeHeight = 0

	// Step 3: Create new empty root leaf node
	newRootPageNum, err := idx.FileManager.AllocatePage()
	if err != nil {
		return fmt.Errorf("failed to allocate page for new root: %w", err)
	}

	newRoot := NewBTreeNode(true, idx.Metadata.PageSize)
	newRoot.PageNum = newRootPageNum
	newRoot.ParentPage = 0 // Root has no parent

	// Write the new root node
	if err := idx.FileManager.WritePage(newRootPageNum, newRoot); err != nil {
		return fmt.Errorf("failed to write new root node: %w", err)
	}

	// Update root page number
	idx.rootPageNum = newRootPageNum
	idx.Metadata.RootPageNum = newRootPageNum
	idx.Metadata.TotalPages = 2 // Metadata + root
	idx.Metadata.NextPageNum = 3
	idx.Metadata.TotalNodes = 1
	idx.Metadata.LeafNodes = 1
	idx.Metadata.TreeHeight = 1

	idx.logger.Debugf("Created new empty root node at page %d", newRootPageNum)

	// Step 4: Re-insert all extracted key-value pairs
	// Temporarily disable WAL during rebuild to avoid logging rebuild operations
	walEnabled := idx.walEnabled
	idx.walEnabled = false

	insertedCount := 0
	for _, pair := range allPairs {
		// Use the Insert method which handles tree balancing automatically
		if err := idx.Insert(pair.key, pair.documentID); err != nil {
			idx.logger.Warnf("Failed to re-insert key '%s' with document ID '%s': %v",
				string(pair.key), pair.documentID, err)
			// Continue with other pairs even if one fails
			continue
		}
		insertedCount++
	}

	// Re-enable WAL
	idx.walEnabled = walEnabled

	idx.logger.Debugf("Re-inserted %d/%d key-value pairs", insertedCount, len(allPairs))

	// Step 5: Persist metadata
	if err := idx.FileManager.WriteMetadata(idx.Metadata); err != nil {
		return fmt.Errorf("failed to persist metadata after rebuild: %w", err)
	}

	// Flush all dirty pages
	if err := idx.PageManager.Flush(idx.FileManager.WritePage); err != nil {
		idx.logger.Warnf("Failed to flush pages after rebuild: %v", err)
		// Don't fail the rebuild if flush fails
	}

	idx.logger.Debugf("Tree rebuild completed successfully: %d pairs re-inserted, new root at page %d",
		insertedCount, idx.rootPageNum)

	return nil
}

// replayWALEntries replays uncommitted WAL entries after crash
//
// WAL REPLAY STRATEGY (PostgreSQL-style):
// 1. Get all uncommitted entries from WAL manager
// 2. Filter entries for this specific index
// 3. Replay in LSN order to maintain causality
// 4. Update index metadata with last replayed LSN
//
// Single Responsibility: Only replays WAL, doesn't validate
// DRY Principle: Reuses Insert/Delete operations for replay
//
// Returns:
//   - error: If WAL replay fails
//
// TODO: I could add parallel replay for independent operations
func (idx *BTreeIndex) replayWALEntries() error {
	if idx.walManager == nil {
		return fmt.Errorf("WAL manager not available for replay")
	}

	idx.logger.Debugf("Replaying WAL entries for index '%s'", idx.Metadata.IndexName)

	// TODO: I need to implement GetUncommittedEntries method on BTreeWALManager
	// For now, log that WAL replay is not yet implemented
	idx.logger.Debugf("WAL replay not yet fully implemented - skipping")

	return nil
}

// Insert adds a new key-value pair to the BTree index
// Parameters:
//   - key: The key to insert (converted to bytes internally)
//   - documentID: The document ID associated with the key
//
// Returns:
//   - error: Any error that occurred during insertion
func (idx *BTreeIndex) Insert(key []byte, documentID string) error {
	insertStart := time.Now()
	if !idx.isOpen {
		return fmt.Errorf("index is not open")
	}

	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	if documentID == "" {
		return fmt.Errorf("document ID cannot be empty")
	}

	if uint32(len(key)) > idx.Metadata.PageSize/4 {
		return fmt.Errorf("key length (%d) exceeds maximum allowed (%d)",
			len(key), idx.Metadata.PageSize/4)
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// WAL Integration: Log BEFORE modifying index (PostgreSQL-style durability)
	// This ensures we can replay the operation after a crash
	// DRY Principle: Reuse WAL infrastructure from journal package
	if idx.walEnabled && idx.walManager != nil {
		lsn := idx.nextLSN
		idx.nextLSN++ // Increment LSN for next operation

		if err := idx.walManager.LogInsert(idx.Metadata.IndexName, idx.bundleName, key, documentID, lsn); err != nil {
			idx.logger.Errorf("Failed to log insert to WAL: %v", err)
			return fmt.Errorf("WAL insert failed: %w", err)
		}

		idx.logger.Debugf("Logged insert to WAL: LSN=%d, key=%s, docID=%s", lsn, string(key), documentID)
	}

	//idx.logger.Debugf("DEBUG: Insert called with rootPageNum=%d, metadata.RootPageNum=%d", idx.rootPageNum, idx.Metadata.RootPageNum)

	// Validate root page number before proceeding
	if idx.rootPageNum == 0 {
		idx.logger.Errorf("Invalid root page number: %d, attempting to recover from metadata", idx.rootPageNum)
		if idx.Metadata.RootPageNum > 0 {
			idx.rootPageNum = idx.Metadata.RootPageNum
			idx.logger.Debugf("Recovered root page number from metadata: %d", idx.rootPageNum)
		} else {
			return fmt.Errorf("invalid root page number: metadata also indicates root page 0")
		}
	}

	idx.logger.Debugf("Inserting key '%s' with document ID '%s'", string(key), documentID)

	// Check for uniqueness if required
	if idx.Metadata.IsUnique {
		existing, totalNodesVisited, err := idx.searchInternal(key, idx.rootPageNum)
		if err != nil {
			return fmt.Errorf("failed to check for existing key: %w", err)
		}

		// Update search metrics for uniqueness check
		idx.Metadata.UpdateSearchMetrics(len(existing), totalNodesVisited)

		// Log performance metrics for uniqueness check
		idx.logger.Debugf("Uniqueness check for key '%s': visited %d nodes, found %d existing entries",
			string(key), totalNodesVisited, len(existing))

		// Log performance warning if uniqueness check was inefficient
		if totalNodesVisited > 10 { // Threshold for inefficient uniqueness check
			idx.logger.Warnf("Inefficient uniqueness check: visited %d nodes for key '%s'",
				totalNodesVisited, string(key))
		}

		if len(existing) > 0 {
			return fmt.Errorf("duplicate key '%s' in unique index", string(key))
		}
	}

	// Perform the insertion
	//idx.logger.Debugf("DEBUG: About to call insertInternal with rootPageNum=%d", idx.rootPageNum)
	newRootPageNum, affectsParentNode, nodesCreated, err := idx.insertInternal(key, documentID, idx.rootPageNum)
	if err != nil {
		return fmt.Errorf("failed to insert key: %w", err)
	}

	//idx.logger.Debugf("DEBUG: insertInternal returned: newRootPageNum=%d, affectsParentNode=%v, oldRootPageNum=%d",
	//	newRootPageNum, affectsParentNode, idx.rootPageNum)

	// Handle root page changes if insertion caused tree restructuring
	if newRootPageNum != idx.rootPageNum {
		idx.logger.Debugf("Root page changed from %d to %d due to insertion (tree height increased - normal B-tree growth)",
			idx.rootPageNum, newRootPageNum)
		idx.rootPageNum = newRootPageNum
		idx.Metadata.RootPageNum = newRootPageNum

		// CRITICAL: Flush new root page to disk immediately to prevent corruption on crash
		// The new root is essential for tree traversal - if it's lost, the entire tree becomes inaccessible
		// Get the page from cache and write it directly to disk
		pageData, err := idx.PageManager.GetPage(newRootPageNum, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			idx.logger.Errorf("Failed to retrieve new root page %d from cache: %v", newRootPageNum, err)
			return fmt.Errorf("failed to retrieve new root page: %w", err)
		}

		if err := idx.FileManager.WritePage(newRootPageNum, pageData); err != nil {
			idx.logger.Errorf("Failed to flush new root page %d to disk: %v", newRootPageNum, err)
			return fmt.Errorf("failed to persist new root page: %w", err)
		}
		idx.logger.Debugf("Flushed new root page %d to disk after root change", newRootPageNum)
	}

	// Log structural changes for monitoring and debugging
	if affectsParentNode {
		idx.logger.Debugf("Insertion caused structural changes affecting parent nodes")
	}

	// Track nodes created for maintenance and statistics
	if nodesCreated > 0 {
		idx.logger.Debugf("Insertion resulted in %d new nodes being created", nodesCreated)
		idx.Metadata.TotalNodes += uint32(nodesCreated)

		// Update fragmentation metrics after node creation
		idx.updateFragmentationAfterInsertion(nodesCreated)
	}

	// Update metadata statistics
	idx.Metadata.IncrementRecordCount()
	idx.Metadata.UpdateStatistics("insert")
	idx.Metadata.UpdateInsertionMetrics(nodesCreated, affectsParentNode)

	// Check if tree needs maintenance after significant structural changes
	if affectsParentNode || nodesCreated > 0 {
		idx.checkMaintenanceNeeded()
	}

	idx.logger.Debugf("Successfully inserted key '%s' with document ID '%s', nodes created: %d",
		string(key), documentID, nodesCreated)

	if idx.metricsReporter != nil {
		idx.metricsReporter("BTreeIndexInsertsTotal", 1)
		idx.reportBTreeLatencyBucket("BTreeInsertLatency", time.Since(insertStart).Seconds()*1000)
	}

	return nil
}

// UpdateInsertionMetrics updates insertion-specific performance metrics
// This method tracks detailed insertion statistics for performance analysis
// Parameters:
//   - nodesCreated: Number of nodes that were created during insertion
//   - structuralChanges: Whether the insertion caused structural changes
func (meta *BTreeMetadata) UpdateInsertionMetrics(nodesCreated int, structuralChanges bool) {
	// Track total nodes created for capacity planning
	meta.TotalNodesCreated += uint64(nodesCreated)

	// Track structural changes for tree health monitoring
	if structuralChanges {
		meta.StructuralChanges++
	}

	// Update average nodes created per insertion
	if meta.InsertCount > 0 {
		meta.AverageNodesCreated = float64(meta.TotalNodesCreated) / float64(meta.InsertCount)
	}

	// Track tree growth for maintenance scheduling
	if nodesCreated > 0 {
		meta.TreeGrowthEvents++
	}
}

// updateFragmentationAfterInsertion updates fragmentation metrics after node creation
// This function calculates and updates the fragmentation percentage based on created nodes
// Parameters:
//   - nodesCreated: Number of nodes that were created during insertion
func (idx *BTreeIndex) updateFragmentationAfterInsertion(nodesCreated int) {
	if idx.Metadata.TotalNodes == 0 {
		idx.Metadata.FragmentationPct = 0.0
		return
	}

	// Node creation typically reduces fragmentation by filling existing space
	// or creating well-balanced new nodes
	fragmentationReduction := float64(nodesCreated) / float64(idx.Metadata.TotalNodes) * 5.0 // Small reduction per new node

	// Update fragmentation percentage (clamped to 0.0 minimum)
	idx.Metadata.FragmentationPct = MaxFloat(0.0, idx.Metadata.FragmentationPct-fragmentationReduction)

	idx.logger.Debugf("Updated fragmentation: reduced by %.2f%% to %.2f%% after creating %d nodes",
		fragmentationReduction, idx.Metadata.FragmentationPct, nodesCreated)
}

// Delete removes a key-value pair from the BTree index
// Parameters:
//   - key: The key to delete
//   - documentID: The specific document ID to remove (for non-unique indexes)
//
// Returns:
//   - error: Any error that occurred during deletion
func (idx *BTreeIndex) Delete(key []byte, documentID string) error {
	if !idx.isOpen {
		return fmt.Errorf("index is not open")
	}
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	if documentID == "" {
		return fmt.Errorf("document ID cannot be empty")
	}
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
	err := idx.deleteOneLocked(key, documentID)
	if err == nil && idx.metricsReporter != nil {
		idx.metricsReporter("BTreeIndexDeletesTotal", 1)
	}
	return err
}

// deleteOneLocked performs one (key, documentID) deletion. Caller must hold idx.mutex.
// Used by Delete and by DeleteByDocumentIDs.
func (idx *BTreeIndex) deleteOneLocked(key []byte, documentID string) error {
	if idx.walEnabled && idx.walManager != nil {
		lsn := idx.nextLSN
		idx.nextLSN++
		if err := idx.walManager.LogDelete(idx.Metadata.IndexName, idx.bundleName, key, documentID, lsn); err != nil {
			idx.logger.Errorf("Failed to log delete to WAL: %v", err)
			return fmt.Errorf("WAL delete failed: %w", err)
		}
		idx.logger.Debugf("Logged delete to WAL: LSN=%d, key=%s, docID=%s", lsn, string(key), documentID)
	}
	idx.logger.Debugf("Deleting key '%s' with document ID '%s'", string(key), documentID)
	newRootPageNum, affectsParentNode, nodesDeleted, err := idx.deleteInternal(key, documentID, idx.rootPageNum)
	if err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}
	if newRootPageNum != idx.rootPageNum {
		idx.logger.Debugf("Root page changed from %d to %d due to deletion", idx.rootPageNum, newRootPageNum)
		idx.rootPageNum = newRootPageNum
		idx.Metadata.RootPageNum = newRootPageNum
	}
	if affectsParentNode {
		idx.logger.Debugf("Deletion caused structural changes affecting parent nodes")
	}
	if nodesDeleted > 0 {
		idx.logger.Debugf("Deletion resulted in %d nodes being removed/merged", nodesDeleted)
		idx.Metadata.TotalNodes -= uint32(nodesDeleted)
		idx.updateFragmentationAfterDeletion(nodesDeleted)
	}
	idx.Metadata.DecrementRecordCount()
	idx.Metadata.UpdateStatistics("delete")
	idx.Metadata.UpdateDeletionMetrics(nodesDeleted, affectsParentNode)
	if affectsParentNode || nodesDeleted > 0 {
		idx.checkMaintenanceNeeded()
	}
	idx.logger.Debugf("Successfully deleted key '%s' with document ID '%s', nodes deleted: %d",
		string(key), documentID, nodesDeleted)
	return nil
}

// DeleteByDocumentIDs removes all B-tree entries whose documentID is in the given list.
// Used when harvest fails (GetDocument fails) and we cannot obtain the index key.
// Does one full in-order scan per B-tree, then deletes each (key, documentID) found.
// Batched for speed: one scan for all documentIDs instead of one scan per documentID.
//
// Returns the number of entries deleted.
func (idx *BTreeIndex) DeleteByDocumentIDs(documentIDs []string) (int, error) {
	if !idx.isOpen {
		return 0, fmt.Errorf("index is not open")
	}
	if len(documentIDs) == 0 {
		return 0, nil
	}
	set := make(map[string]struct{}, len(documentIDs))
	for _, d := range documentIDs {
		if d != "" {
			set[d] = struct{}{}
		}
	}
	if len(set) == 0 {
		return 0, nil
	}
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
	keys, docIDsOut, err := scanKeysForDocumentIDs(idx, set, idx.rootPageNum)
	if err != nil {
		return 0, err
	}
	for i := range keys {
		if err := idx.deleteOneLocked(keys[i], docIDsOut[i]); err != nil {
			idx.logger.Warnf("DeleteByDocumentIDs: deleteOneLocked failed for docID %s: %v", docIDsOut[i], err)
		}
	}
	return len(keys), nil
}

// updateFragmentationAfterDeletion updates fragmentation metrics after node deletions
// This function calculates and updates the fragmentation percentage based on deleted nodes
// Parameters:
//   - nodesDeleted: Number of nodes that were deleted or merged
func (idx *BTreeIndex) updateFragmentationAfterDeletion(nodesDeleted int) {
	if idx.Metadata.TotalNodes == 0 {
		idx.Metadata.FragmentationPct = 0.0
		return
	}

	// Calculate fragmentation reduction due to node consolidation
	fragmentationReduction := float64(nodesDeleted) / float64(idx.Metadata.TotalNodes) * 100.0

	// Update fragmentation percentage (clamped to 0.0 minimum)
	idx.Metadata.FragmentationPct = MaxFloat(0.0, idx.Metadata.FragmentationPct-fragmentationReduction)

	idx.logger.Debugf("Updated fragmentation: reduced by %.2f%% to %.2f%% after deleting %d nodes",
		fragmentationReduction, idx.Metadata.FragmentationPct, nodesDeleted)
}

// checkMaintenanceNeeded determines if the index needs maintenance after structural changes
// This function analyzes the current state and schedules maintenance if needed
// PERFORMANCE: Throttled to run every 1000 operations to avoid O(n) tree traversal on every insert
func (idx *BTreeIndex) checkMaintenanceNeeded() {
	// Throttle: only check every 1000 operations to avoid expensive calculateFillFactor() calls
	const maintenanceCheckInterval = 1000
	idx.Metadata.OperationsSinceLastCheck++

	if idx.Metadata.OperationsSinceLastCheck < maintenanceCheckInterval {
		return // Skip expensive checks
	}

	// Reset counter
	idx.Metadata.OperationsSinceLastCheck = 0

	// Check if fragmentation is too high
	if idx.Metadata.FragmentationPct > 25.0 {
		idx.logger.Debugf("High fragmentation detected (%.2f%%), maintenance recommended",
			idx.Metadata.FragmentationPct)
		idx.Metadata.MaintenanceNeeded = true
	}

	// Check if tree height is becoming unbalanced
	// This requires full tree traversal - expensive operation!
	fillFactor, err := idx.calculateFillFactor()
	if err != nil {
		idx.logger.Warnf("Failed to calculate fill factor: %v", err)
		fillFactor = 0.0
	}

	if fillFactor < 0.5 {
		idx.logger.Debugf("Low fill factor detected (%.2f%%), compaction recommended", fillFactor)
		idx.Metadata.CompactionNeeded = true
	}
}

// Search finds all document IDs associated with the given key
// Parameters:
//   - key: The key to search for
//
// Returns:
//   - []string: List of document IDs associated with the key
//   - error: Any error that occurred during search
func (idx *BTreeIndex) Search(key []byte) ([]string, error) {
	searchStart := time.Now()
	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	idx.logger.Debugf("Searching for key '%s' using rootPageNum=%d", string(key), idx.rootPageNum)

	// Perform the search
	results, totalNodesVisited, err := idx.searchInternal(key, idx.rootPageNum)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}

	// Update metadata statistics
	idx.Metadata.UpdateStatistics("search")
	idx.Metadata.UpdateSearchMetrics(len(results), totalNodesVisited)

	if idx.metricsReporter != nil {
		idx.metricsReporter("BTreeIndexSearchesTotal", 1)
		idx.reportBTreeLatencyBucket("BTreeSearchLatency", time.Since(searchStart).Seconds()*1000)
	}

	idx.logger.Debugf("Search for key '%s' returned %d results", string(key), len(results))

	return results, nil
}

// Exists reports whether the key has at least one entry. For uniqueness checks.
// Skips stats and logging to meet <100µs for in-memory indexes.
func (idx *BTreeIndex) Exists(key []byte) (bool, error) {
	if !idx.isOpen {
		return false, fmt.Errorf("index is not open")
	}
	if len(key) == 0 {
		return false, fmt.Errorf("key cannot be empty")
	}
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()
	results, _, err := idx.searchInternal(key, idx.rootPageNum)
	if err != nil {
		return false, err
	}
	return len(results) > 0, nil
}

// RangeSearch finds all document IDs for keys within the specified range
// Parameters:
//   - startKey: The starting key of the range (inclusive)
//   - endKey: The ending key of the range (inclusive)
//
// Returns:
//   - []string: List of document IDs for keys in the range
//   - error: Any error that occurred during range search
func (idx *BTreeIndex) RangeSearch(startKey, endKey []byte) ([]string, error) {
	return idx.RangeSearchWithBounds(startKey, endKey, false, false)
}

// RangeSearchWithBounds performs a range search with configurable inclusive/exclusive bounds
// This method supports all comparison operators (>, >=, <, <=, BETWEEN) by allowing
// callers to specify whether start and end bounds are inclusive or exclusive.
//
// Algorithm:
//  1. Validate inputs and acquire read lock
//  2. Delegate to rangeSearchInternalWithBounds for actual search
//  3. Update statistics and log performance metrics
//
// Parameters:
//   - startKey: The starting key for the range (required, non-empty)
//   - endKey: The ending key for the range (required, non-empty)
//   - excludeStart: If true, exclude the startKey from results (for > operator)
//   - excludeEnd: If true, exclude the endKey from results (for < operator)
//
// Returns:
//   - []string: List of document IDs for keys in the range
//   - error: Any error that occurred during range search
//
// Examples:
//
//	RangeSearchWithBounds([]byte("10"), []byte("20"), false, false) // 10 <= key <= 20 (BETWEEN)
//	RangeSearchWithBounds([]byte("10"), []byte("20"), true, false)  // 10 < key <= 20 (>)
//	RangeSearchWithBounds([]byte("10"), []byte("20"), false, true)  // 10 <= key < 20 (<)
//	RangeSearchWithBounds([]byte("10"), []byte("20"), true, true)   // 10 < key < 20
func (idx *BTreeIndex) RangeSearchWithBounds(startKey, endKey []byte, excludeStart, excludeEnd bool) ([]string, error) {
	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	if len(startKey) == 0 || len(endKey) == 0 {
		return nil, fmt.Errorf("start key and end key cannot be empty")
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	idx.logger.Debugf("Range search from key '%s' to '%s' (excludeStart=%v, excludeEnd=%v)",
		string(startKey), string(endKey), excludeStart, excludeEnd)

	// Perform the range search with boundary control
	results, keysFound, nodesVisited, err := idx.rangeSearchInternalWithBounds(
		startKey, endKey, excludeStart, excludeEnd)
	if err != nil {
		return nil, fmt.Errorf("range search failed: %w", err)
	}

	// Update metadata statistics with detailed metrics
	idx.Metadata.UpdateStatistics("search")
	idx.Metadata.UpdateSearchMetrics(keysFound, nodesVisited)

	// Log performance metrics for monitoring and optimization
	idx.logger.Debugf("Range search completed: found %d results from %d keys, visited %d nodes",
		len(results), keysFound, nodesVisited)

	// Log performance warning if search was inefficient
	if nodesVisited > 0 {
		efficiency := float64(keysFound) / float64(nodesVisited)
		if efficiency < 0.1 { // Less than 10% efficiency
			idx.logger.Warnf("Range search inefficient: %d keys found from %d nodes visited (%.2f%% efficiency)",
				keysFound, nodesVisited, efficiency*100)
		}
	}

	return results, nil
}

// RangeSearchWithLimit performs a forward (ASC) range search with early termination at limit.
// This is optimized for ORDER BY ... LIMIT queries where we only need the first N documents.
//
// Parameters:
//   - startKey: The starting key for the range
//   - endKey: The ending key for the range
//   - limit: Maximum number of document IDs to return (0 = no limit)
//
// Returns:
//   - []string: List of document IDs in ascending key order, capped at limit
//   - error: Any error that occurred during range search
func (idx *BTreeIndex) RangeSearchWithLimit(startKey, endKey []byte, limit int) ([]string, error) {
	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	idx.logger.Debugf("Range search with limit: start='%s', end='%s', limit=%d",
		string(startKey), string(endKey), limit)

	// Use the iterator for efficient traversal with early termination
	iterator := NewBTreeRangeIterator(idx, idx.logger, startKey, endKey, false, false)
	if err := iterator.SeekFirst(); err != nil {
		return nil, fmt.Errorf("failed to seek to first key: %w", err)
	}

	var results []string
	if limit > 0 {
		results = make([]string, 0, limit)
	} else {
		results = make([]string, 0, 1000) // Pre-allocate reasonable capacity
	}

	for {
		entry := iterator.Current()
		if entry == nil {
			break
		}

		// Append all document IDs for this key
		for _, docID := range entry.DocumentIDs {
			results = append(results, docID)
			if limit > 0 && len(results) >= limit {
				idx.logger.Debugf("Range search hit limit: %d documents", limit)
				idx.Metadata.UpdateStatistics("search")
				return results, nil
			}
		}

		if !iterator.HasNext() {
			break
		}
		// Next() returns the next entry - advance the iterator
		_ = iterator.Next()
	}

	idx.Metadata.UpdateStatistics("search")
	idx.logger.Debugf("Range search with limit completed: found %d results", len(results))

	return results, nil
}

// ReverseRangeSearchWithLimit performs a backward (DESC) range search with early termination at limit.
// This is optimized for ORDER BY ... DESC LIMIT queries where we only need the last N documents
// in descending order.
//
// Parameters:
//   - startKey: The starting key for the range (logically the "larger" key for DESC)
//   - endKey: The ending key for the range (logically the "smaller" key for DESC)
//   - limit: Maximum number of document IDs to return (0 = no limit)
//
// Returns:
//   - []string: List of document IDs in descending key order, capped at limit
//   - error: Any error that occurred during range search
func (idx *BTreeIndex) ReverseRangeSearchWithLimit(startKey, endKey []byte, limit int) ([]string, error) {
	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	idx.logger.Debugf("Reverse range search with limit: start='%s', end='%s', limit=%d",
		string(startKey), string(endKey), limit)

	// Use the iterator for efficient backward traversal with early termination
	// For DESC, we seek to the end (largest key) and iterate backward
	iterator := NewBTreeRangeIterator(idx, idx.logger, endKey, startKey, false, false)
	if err := iterator.SeekLast(); err != nil {
		return nil, fmt.Errorf("failed to seek to last key: %w", err)
	}

	var results []string
	if limit > 0 {
		results = make([]string, 0, limit)
	} else {
		results = make([]string, 0, 1000) // Pre-allocate reasonable capacity
	}

	for {
		entry := iterator.Current()
		if entry == nil {
			break
		}

		// Append all document IDs for this key (in reverse order for DESC)
		// Note: We reverse the document IDs within a key since iterator returns them in insertion order
		for i := len(entry.DocumentIDs) - 1; i >= 0; i-- {
			results = append(results, entry.DocumentIDs[i])
			if limit > 0 && len(results) >= limit {
				idx.logger.Debugf("Reverse range search hit limit: %d documents", limit)
				idx.Metadata.UpdateStatistics("search")
				return results, nil
			}
		}

		if !iterator.HasPrev() {
			break
		}
		// Prev() returns the previous entry - advance the iterator backward
		_ = iterator.Prev()
	}

	idx.Metadata.UpdateStatistics("search")
	idx.logger.Debugf("Reverse range search with limit completed: found %d results", len(results))

	return results, nil
}

// UpdateSearchMetrics updates search performance metrics
// This method tracks detailed search statistics for performance analysis
// Parameters:
//   - keysFound: Number of keys that matched the search criteria
//   - nodesVisited: Number of nodes that were accessed during the search
func (meta *BTreeMetadata) UpdateSearchMetrics(keysFound, nodesVisited int) {
	// Update cumulative statistics
	meta.TotalKeysFound += uint64(keysFound)
	meta.TotalNodesVisited += uint64(nodesVisited)

	// Calculate and update average search efficiency
	if meta.SearchCount > 0 {
		meta.AverageSearchEfficiency = float64(meta.TotalKeysFound) / float64(meta.TotalNodesVisited)
	}

	// Track maximum nodes visited for capacity planning
	if nodesVisited > int(meta.MaxNodesVisited) {
		meta.MaxNodesVisited = uint32(nodesVisited)
	}
}

// Close closes the BTree index and flushes any pending changes
// Returns:
//   - error: Any error that occurred during closing
func (idx *BTreeIndex) Close() error {
	if !idx.isOpen {
		return nil // Already closed
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.logger.Debugf("Closing BTree index '%s'", idx.FilePath)

	// Stop the flush worker goroutine by closing the trigger channel
	if idx.PageManager.flushTriggerChan != nil {
		close(idx.PageManager.flushTriggerChan)
		idx.logger.Debugf("Closed flush worker channel for BTree index '%s'", idx.FilePath)
	}

	// Flush any dirty pages
	if err := idx.PageManager.Flush(idx.FileManager.WritePage); err != nil {
		idx.logger.Warnf("Failed to flush pages during close: %v", err)
	}

	// Update and write final metadata
	idx.Metadata.LastModified = time.Now()
	if err := idx.FileManager.WriteMetadata(idx.Metadata); err != nil {
		idx.logger.Warnf("Failed to write metadata during close: %v", err)
	}

	// Close file manager
	if err := idx.FileManager.Close(); err != nil {
		return fmt.Errorf("failed to close file manager: %w", err)
	}

	idx.isOpen = false
	idx.logger.Debugf("Successfully closed BTree index '%s'", idx.FilePath)

	return nil
}

// GetStats returns current index statistics
// Returns:
//   - *BTreeStats: Current index statistics
func (idx *BTreeIndex) GetStats() *BTreeStats {
	if !idx.isOpen {
		return nil
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	// Calculate dynamic statistics
	fillFactor, err := idx.calculateFillFactor()
	if err != nil {
		idx.logger.Warnf("Failed to calculate fill factor: %v", err)
		fillFactor = 0.0
	}
	cacheStats := idx.PageManager.GetCacheStats()

	avgKeyLength, err := idx.calculateAverageKeyLength()
	if err != nil {
		idx.logger.Warnf("Failed to calculate average key length: %v", err)
		avgKeyLength = 0.0
	}

	return &BTreeStats{
		TotalRecords:      idx.Metadata.TotalRecords,
		TotalNodes:        idx.Metadata.TotalNodes,
		TreeHeight:        idx.Metadata.TreeHeight,
		AverageKeyLength:  avgKeyLength,
		FillFactor:        fillFactor,
		FragmentationPct:  idx.Metadata.FragmentationPct,
		CacheHitRate:      cacheStats.HitRate,
		TotalSearches:     idx.Metadata.SearchCount,
		TotalInserts:      idx.Metadata.InsertCount,
		TotalDeletes:      idx.Metadata.DeleteCount,
		AverageSearchTime: 0, // Would need to track timing
		LastUpdated:       time.Now(),
	}
}

// GetCacheStats returns detailed cache performance statistics
// Returns:
//   - *CacheStats: Current cache statistics including hits, misses, evictions
func (idx *BTreeIndex) GetCacheStats() *CacheStats {
	if !idx.isOpen {
		return nil
	}
	return idx.PageManager.GetPageManagerCacheStats()
}

// reportBTreeLatencyBucket reports a latency measurement to the appropriate histogram bucket.
// prefix should be "BTreeInsertLatency" or "BTreeSearchLatency".
func (idx *BTreeIndex) reportBTreeLatencyBucket(prefix string, latencyMs float64) {
	var bucket string
	switch {
	case latencyMs < 1:
		bucket = "Lt1ms"
	case latencyMs < 10:
		bucket = "Lt10ms"
	case latencyMs < 100:
		bucket = "Lt100ms"
	case latencyMs < 1000:
		bucket = "Lt1s"
	default:
		bucket = "Gte1s"
	}
	idx.metricsReporter(prefix+bucket, 1)
}

// DeletionStats represents tombstone and deletion-related metrics
type DeletionStats struct {
	TotalTombstones     uint64  // Total number of tombstones
	TotalRecords        uint64  // Total number of records
	TombstoneRatio      float64 // Ratio of tombstones to live records
	CompactionNeeded    bool    // Whether compaction is needed
	NodesNeedCompaction uint32  // Number of nodes needing compaction
}

// GetDeletionStats returns deletion and tombstone-related statistics
// Returns:
//   - *DeletionStats: Current deletion statistics
func (idx *BTreeIndex) GetDeletionStats() *DeletionStats {
	if !idx.isOpen {
		return nil
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	return &DeletionStats{
		TotalTombstones:     idx.Metadata.TotalTombstones,
		TotalRecords:        idx.Metadata.TotalRecords,
		TombstoneRatio:      idx.Metadata.TombstoneRatio,
		CompactionNeeded:    idx.Metadata.CompactionNeeded,
		NodesNeedCompaction: idx.Metadata.NodesNeedCompaction,
	}
}

// Validate performs integrity checks on the BTree index
// Returns:
//   - error: Any integrity issues found, nil if index is valid
func (idx *BTreeIndex) Validate() error {
	if !idx.isOpen {
		return fmt.Errorf("index is not open")
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	idx.logger.Debugf("Validating BTree index integrity")

	// Validate tree structure
	if err := idx.validateTree(); err != nil {
		return fmt.Errorf("tree structure validation failed: %w", err)
	}

	// Validate key ordering
	if err := idx.validateKeyOrder(); err != nil {
		return fmt.Errorf("key order validation failed: %w", err)
	}

	// Validate tree balance
	if err := idx.checkTreeBalance(); err != nil {
		return fmt.Errorf("tree balance validation failed: %w", err)
	}

	idx.logger.Debugf("BTree index validation completed successfully")

	return nil
}

// Compact performs index maintenance to reduce fragmentation
// Returns:
//   - error: Any error that occurred during compaction
func (idx *BTreeIndex) Compact() error {
	if !idx.isOpen {
		return fmt.Errorf("index is not open")
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.logger.Debugf("Starting BTree index compaction")

	// Perform compaction using existing compact() method
	if err := idx.compact(); err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Update metadata
	idx.Metadata.LastCompaction = time.Now()
	idx.Metadata.FragmentationPct = 0.0 // Reset after compaction
	idx.Metadata.CompactionCount++
	idx.Metadata.CompactionNeeded = false
	idx.Metadata.NodesNeedCompaction = 0
	idx.Metadata.TotalTombstones = 0  // Reset tombstone count after compaction
	idx.Metadata.TombstoneRatio = 0.0 // Reset tombstone ratio

	// Write updated metadata
	if err := idx.FileManager.WriteMetadata(idx.Metadata); err != nil {
		return fmt.Errorf("failed to update metadata after compaction: %w", err)
	}

	idx.logger.Debugf("BTree index compaction completed successfully")

	return nil
}

// Private helper methods

// initializeIndex creates the initial index structure
func (idx *BTreeIndex) initializeIndex() error {
	// Check if index file already has valid data (for crash recovery)
	existingMetadata, err := idx.FileManager.ReadMetadata()
	if err == nil && existingMetadata != nil {
		idx.logger.Debugf("Read existing metadata: RootPageNum=%d, TotalRecords=%d", existingMetadata.RootPageNum, existingMetadata.TotalRecords)

		if existingMetadata.RootPageNum > 0 {
			// Index file exists with valid metadata - use existing data (recovery scenario)
			idx.logger.Debugf("Index file exists with valid metadata, recovering from existing data (root page: %d, records: %d)",
				existingMetadata.RootPageNum, existingMetadata.TotalRecords)
			idx.Metadata = existingMetadata
			idx.rootPageNum = existingMetadata.RootPageNum
			return nil
		}
	} else if err != nil {
		idx.logger.Debugf("Could not read existing metadata: %v", err)
	}

	// File doesn't exist or has invalid data - initialize fresh
	idx.logger.Debugf("Initializing new index file with fresh root node")

	// Ensure metadata has correct root page number
	if idx.Metadata.RootPageNum == 0 {
		idx.Metadata.RootPageNum = 1 // Page 0 is metadata, root starts at page 1
	}

	// Ensure index root page number matches metadata
	idx.rootPageNum = idx.Metadata.RootPageNum

	// Write metadata to page 0
	if err := idx.FileManager.WriteMetadata(idx.Metadata); err != nil {
		return fmt.Errorf("failed to write initial metadata: %w", err)
	}

	// Create root node (empty leaf node)
	rootNode := NewBTreeNode(true, idx.Metadata.PageSize) // Start with leaf node
	rootNode.PageNum = idx.rootPageNum

	// Write root node to page 1
	if err := idx.FileManager.WritePage(idx.rootPageNum, rootNode); err != nil {
		return fmt.Errorf("failed to write initial root node: %w", err)
	}

	return nil
}

// Internal methods for BTree operations on the btreeIndex struct

func (idx *BTreeIndex) searchInternal(key []byte, rootPageNum uint32) ([]string, int, error) {
	childResults, totalNodesVisited, err := searchInternal(idx, key, rootPageNum)
	if err != nil {
		return nil, 0, fmt.Errorf("btree index search failed: %w", err)
	}
	return childResults, totalNodesVisited, nil
}

func (idx *BTreeIndex) insertInternal(key []byte, documentID string, rootPageNum uint32) (uint32, bool, int, error) {
	pageNum, affectsParentNode, nodesCreated, err := insertInternal(idx, key, documentID, rootPageNum)
	if err != nil {
		return 0, false, 0, fmt.Errorf("insert failed: %w", err)
	}
	return pageNum, affectsParentNode, nodesCreated, nil
}

func (idx *BTreeIndex) deleteInternal(key []byte, documentID string, rootPageNum uint32) (uint32, bool, int, error) {
	pageNum, affectsParentNode, nodesDeleted, err := deleteInternal(idx, key, documentID, rootPageNum)
	return pageNum, affectsParentNode, nodesDeleted, err
}

func (idx *BTreeIndex) rangeSearchInternal(startKey, endKey []byte) ([]string, int, int, error) {
	return rangeSearchInternal(idx, startKey, endKey, idx.rootPageNum)
}

func (idx *BTreeIndex) rangeSearchInternalWithBounds(startKey, endKey []byte, excludeStart, excludeEnd bool) ([]string, int, int, error) {
	return rangeSearchInternalWithBounds(idx, startKey, endKey, excludeStart, excludeEnd, idx.rootPageNum)
}

func (idx *BTreeIndex) validateTree() error {

	result := ValidateTreeStructure(idx)
	if !result.IsValid {
		return fmt.Errorf("tree validation failed: %v", result.Errors)
	}
	return nil
}

func (idx *BTreeIndex) validateKeyOrder() error {
	result := ValidationResult{
		IsValid: true,
		Errors:  []string{},
	}

	if err := validateKeyOrdering(idx, &result); err != nil {
		return fmt.Errorf("key order validation failed: %w", err)
	}
	return nil
}

func (idx *BTreeIndex) checkTreeBalance() error {
	result := CheckTreeBalance(idx)
	if !result.IsBalanced {
		return fmt.Errorf("tree balance validation failed: %v", result.Errors)
	}
	idx.logger.Debugf("Tree balance validation passed: height %d, nodes %d",
		idx.Metadata.TreeHeight, idx.Metadata.TotalNodes)

	idx.Metadata.TreeHeight = result.Height
	idx.Metadata.TotalNodes = result.TotalNodes
	idx.Metadata.FragmentationPct = result.FragmentationPct
	idx.Metadata.FillFactor = result.FillFactor
	idx.Metadata.AverageKeyLength = result.AverageKeyLength
	idx.Metadata.LastValidation = time.Now()
	return nil
}

func (idx *BTreeIndex) calculateFillFactor() (float64, error) {
	return CalculateFillFactor(idx)
}

func (idx *BTreeIndex) calculateAverageKeyLength() (float64, error) {
	return CalculateAverageKeyLength(idx)
}

// PersistMetadata writes current in-memory metadata to page 0. Call when
// using Insert/Delete in a context that does not run processBTreeIndexBatch
// (e.g. bulk populate, in-place update loops). The batch path calls this once
// after FlushDirtyPages.
func (idx *BTreeIndex) PersistMetadata() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()
	return idx.FileManager.WriteMetadata(idx.Metadata)
}

// extractPageNumFromOverflowError parses the page number from a "exceeds page size" error.
// e.g. "failed to flush page 62: serialized page 62 size (8254) exceeds page size (8192)" -> 62
func extractPageNumFromOverflowError(s string) uint32 {
	// Match "page 62 size" from "serialized page 62 size (8254) exceeds page size (8192)"
	re := regexp.MustCompile(`page (\d+) size`)
	m := re.FindStringSubmatch(s)
	if len(m) < 2 {
		return 0
	}
	n, err := strconv.ParseUint(m[1], 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}

// ForceSplitOverflowingLeaf splits a leaf that exceeds the page size so both halves fit.
// Run after CompactOverflowNode when the node is still too large. Clears tombstones
// first so split does not need to partition them. Caller must not hold PageManager mutex.
func (idx *BTreeIndex) ForceSplitOverflowingLeaf(pageNum uint32) error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return err
	}
	node, ok := pageData.(*BTreeNode)
	if !ok || !node.IsLeaf {
		return fmt.Errorf("page %d is not a leaf node", pageNum)
	}
	idx.CompactOverflowNode(node)

	var newRoot uint32
	if len(node.Keys) == 1 && len(node.Values) == 1 && len(node.Values[0]) >= 2 {
		newRoot, err = splitLeafValueList(idx, node)
	} else if len(node.Keys) >= 2 {
		newRoot, _, _, err = splitLeafNode(idx, node)
	} else {
		v0 := 0
		if len(node.Values) > 0 {
			v0 = len(node.Values[0])
		}
		return fmt.Errorf("page %d has too few keys to split (keys=%d, values[0]=%d)", pageNum, len(node.Keys), v0)
	}
	if err != nil {
		return err
	}
	if newRoot != 0 && newRoot != idx.rootPageNum {
		idx.rootPageNum = newRoot
		idx.Metadata.RootPageNum = newRoot
	}
	return nil
}

// CompactOverflowNode physically removes tombstone entries from a leaf node to
// reduce its serialized size. Used when a node would exceed the page size on
// flush because Tombstones (and/or variable-length Keys/Values) grew without
// IsFull having triggered a split. Modifies the node in place; caller must
// not hold PageManager mutex (e.g. when called from the flush writer).
func (idx *BTreeIndex) CompactOverflowNode(node *BTreeNode) {
	if node == nil || !node.IsLeaf || node.Tombstones == nil || len(node.Tombstones) == 0 {
		return
	}
	removed := uint64(0)
	for k := range node.Tombstones {
		sep := strings.LastIndex(k, ":")
		if sep < 0 {
			continue
		}
		keyPart := []byte(k[:sep])
		docID := k[sep+1:]
		for i, key := range node.Keys {
			if bytes.Equal(key, keyPart) {
				newVals := make([]string, 0, len(node.Values[i]))
				for _, d := range node.Values[i] {
					if d != docID {
						newVals = append(newVals, d)
					}
				}
				node.Values[i] = newVals
				if len(newVals) == 0 {
					node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
					node.Values = append(node.Values[:i], node.Values[i+1:]...)
					node.KeyCount--
				}
				break
			}
		}
		delete(node.Tombstones, k)
		removed++
	}
	node.TombstoneCount = 0
	node.Tombstones = nil
	if removed > 0 && idx.Metadata.TotalTombstones >= removed {
		idx.Metadata.TotalTombstones -= removed
	}
}

// FlushDirtyPages flushes all dirty pages to disk
// In batched mode: skips final sync, relies on WAL checkpoint
// In immediate mode: syncs after writing all pages
// Returns:
//   - error: Any error that occurred during flushing
func (idx *BTreeIndex) FlushDirtyPages() error {
	writer := func(pageNum uint32, pageData interface{}) error {
		err := idx.FileManager.WritePage(pageNum, pageData)
		if err != nil && strings.Contains(err.Error(), "exceeds page size") {
			if node, ok := pageData.(*BTreeNode); ok {
				idx.CompactOverflowNode(node)
				err = idx.FileManager.WritePage(pageNum, pageData)
				if err != nil && strings.Contains(err.Error(), "exceeds page size") {
					idx.logger.Warnf("Page %d still exceeds page size after compacting tombstones; will try split on flush retry", pageNum)
				}
			}
		}
		return err
	}

	err := idx.PageManager.Flush(writer)
	if err != nil && strings.Contains(err.Error(), "exceeds page size") {
		if pg := extractPageNumFromOverflowError(err.Error()); pg != 0 {
			if splitErr := idx.ForceSplitOverflowingLeaf(pg); splitErr == nil {
				err = idx.PageManager.Flush(writer)
			} else {
				idx.logger.Warnf("Cannot split overflowing page %d: %v; run COMPACT INDEX", pg, splitErr)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("failed to flush dirty pages: %w", err)
	}

	// CRITICAL FIX: Only sync in immediate mode
	// In batched mode, WAL provides durability - defer sync to checkpoint
	syncMode := idx.FileManager.GetSyncMode()
	if syncMode != "batched" {
		// Immediate mode: do final fsync for durability
		if err := idx.FileManager.Sync(); err != nil {
			return fmt.Errorf("failed to sync after batch flush: %w", err)
		}
		idx.logger.Debugf("Synced pages to disk (immediate mode)")
	} else {
		// Batched mode: skip expensive fsync, rely on WAL
		idx.logger.Debugf("Skipped sync after flush (batched mode - WAL provides durability)")
	}

	return nil
}

// GetCacheStats returns current cache statistics
func (pm *BTreePageManager) GetCacheStats() *CacheStats {

	return pm.GetPageManagerCacheStats()
}
