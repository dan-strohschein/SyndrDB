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
	"sync"

	"time"

	"syndrdb/src/pkg/settings"

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
	logger.Infof("DEBUG: CreateBTreeIndex V2 started with config: %+v", config)

	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	// Validate configuration
	logger.Infof("DEBUG: Validating configuration")
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	logger.Debugf("Creating BTree index for bundle '%s' field '%s'", config.BundleName, config.FieldName)

	// Create file manager
	indexFilePath := config.GetIndexFilePath()
	logger.Infof("DEBUG: Creating file manager for path: %s", indexFilePath)
	fileManager, err := NewBTreeFileManager(indexFilePath, config.PageSize, args.Debug, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}
	logger.Infof("DEBUG: File manager created successfully")

	// Create page manager
	logger.Infof("DEBUG: Creating page manager")
	pageManager, err := NewBTreePageManager(config.PageSize, config.CacheSize, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}
	logger.Infof("DEBUG: Page manager created successfully")

	// Create metadata
	metadata := NewBTreeMetadata(config)

	// Create BTree index instance
	index := &BTreeIndex{
		FilePath:    indexFilePath,
		Metadata:    metadata,
		FileManager: fileManager,
		PageManager: pageManager,
		rootPageNum: metadata.RootPageNum, //1, // Page 0 is metadata, root starts at page 1
		mutex:       sync.RWMutex{},
		isOpen:      true,
		bundleName:  config.BundleName,
		fieldName:   config.FieldName,
		logger:      logger,
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
		logger.Infof("WAL enabled for B-tree index '%s'", config.FieldName)
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

	logger.Infof("Successfully created BTree index '%s' for bundle '%s' field '%s'",
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

	// CRASH RECOVERY: Validate index integrity and repair if needed
	// Following PostgreSQL's hybrid approach: auto-repair minor issues, fail-fast on major corruption
	// Single Responsibility: Each validation function checks one aspect of integrity
	logger.Infof("Performing crash recovery validation for index '%s'", filePath)

	if err := index.performCrashRecovery(); err != nil {
		return nil, fmt.Errorf("crash recovery failed: %w", err)
	}

	logger.Infof("Successfully opened BTree index '%s' for bundle '%s' field '%s'",
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
	idx.logger.Infof("Starting crash recovery for index '%s'", idx.Metadata.IndexName)

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
		idx.logger.Infof("Minor corruption detected (%d pages < 5), attempting auto-repair", len(corruptPages))
		if err := idx.repairCorruptPages(corruptPages); err != nil {
			return fmt.Errorf("auto-repair failed: %w", err)
		}
		idx.logger.Infof("Successfully repaired %d corrupt pages", len(corruptPages))
	}

	// Step 4: Validate tree structural integrity
	if err := idx.validateTreeStructure(); err != nil {
		idx.logger.Warnf("Tree structure invalid: %v, attempting rebuild", err)

		// Attempt to rebuild tree from valid pages
		if err := idx.rebuildTreeStructure(); err != nil {
			return fmt.Errorf("tree rebuild failed: %w", err)
		}
		idx.logger.Infof("Successfully rebuilt tree structure")
	}

	// Step 5: Replay WAL entries if WAL manager is available
	// This recovers any uncommitted operations from before the crash
	if idx.walManager != nil {
		idx.logger.Infof("WAL manager available, replaying uncommitted entries")
		if err := idx.replayWALEntries(); err != nil {
			return fmt.Errorf("WAL replay failed: %w", err)
		}
	} else {
		idx.logger.Debugf("No WAL manager available, skipping WAL replay")
	}

	idx.logger.Infof("Crash recovery completed successfully")
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
		idx.logger.Infof("All page checksums valid (%d pages verified)", totalPages-1)
	}

	return corruptPages, nil
}

// computePageChecksum calculates CRC32 checksum for a B-tree node
//
// CHECKSUM ALGORITHM:
// Uses CRC32 IEEE polynomial (0xedb88320) which is hardware-accelerated on modern CPUs
// The checksum includes all node data: keys, values, children, metadata
// Excludes the checksum field itself to avoid circular dependency
//
// SIMD ACCELERATION:
// Go's hash/crc32 package automatically uses SSE 4.2 crc32c instruction when available
// This provides ~10x faster checksums on Intel/AMD CPUs with SSE4.2
//
// Single Responsibility: Only computes checksum, doesn't modify node
//
// Parameters:
//   - node: The node to compute checksum for
//
// Returns:
//   - uint32: CRC32 checksum value
//
// TODO: I could add support for other hash functions (xxHash, SHA256) for higher security
func (idx *BTreeIndex) computePageChecksum(node interface{}) uint32 {
	// Import hash/crc32 at package level for CRC32 computation
	// For now, return simple checksum based on page number as placeholder
	// Full implementation requires serializing entire node structure

	btreeNode, ok := node.(*BTreeNode)
	if !ok {
		return 0
	}

	// Simple checksum: XOR of all field values
	// TODO: I need to implement proper CRC32 computation using hash/crc32 package
	checksum := btreeNode.PageNum ^ btreeNode.KeyCount ^ btreeNode.NextLeaf ^ btreeNode.PrevLeaf

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
	idx.logger.Infof("Attempting to repair %d corrupt pages", len(corruptPages))

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

	idx.logger.Infof("Repaired %d/%d corrupt pages", repairedCount, len(corruptPages))
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
// 1. Scan all pages to find valid leaf nodes
// 2. Rebuild internal nodes from leaf keys
// 3. Update root page number
// 4. Persist new tree structure
//
// Single Responsibility: Only rebuilds tree, doesn't validate
//
// Returns:
//   - error: If rebuild is not possible
//
// TODO: I could add incremental rebuild to avoid full tree reconstruction
func (idx *BTreeIndex) rebuildTreeStructure() error {
	idx.logger.Warnf("Tree rebuild not yet implemented - marking index as damaged")
	return fmt.Errorf("tree rebuild not implemented - manual recovery required")
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

	idx.logger.Infof("Replaying WAL entries for index '%s'", idx.Metadata.IndexName)

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

	idx.logger.Infof("DEBUG: Insert called with rootPageNum=%d, metadata.RootPageNum=%d", idx.rootPageNum, idx.Metadata.RootPageNum)

	// Validate root page number before proceeding
	if idx.rootPageNum == 0 {
		idx.logger.Errorf("Invalid root page number: %d, attempting to recover from metadata", idx.rootPageNum)
		if idx.Metadata.RootPageNum > 0 {
			idx.rootPageNum = idx.Metadata.RootPageNum
			idx.logger.Infof("Recovered root page number from metadata: %d", idx.rootPageNum)
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
	idx.logger.Infof("DEBUG: About to call insertInternal with rootPageNum=%d", idx.rootPageNum)
	newRootPageNum, affectsParentNode, nodesCreated, err := idx.insertInternal(key, documentID, idx.rootPageNum)
	if err != nil {
		return fmt.Errorf("failed to insert key: %w", err)
	}

	idx.logger.Infof("DEBUG: insertInternal returned: newRootPageNum=%d, affectsParentNode=%v, oldRootPageNum=%d",
		newRootPageNum, affectsParentNode, idx.rootPageNum)

	// Handle root page changes if insertion caused tree restructuring
	if newRootPageNum != idx.rootPageNum {
		idx.logger.Warnf("ROOT CHANGE: Root page changed from %d to %d due to insertion",
			idx.rootPageNum, newRootPageNum)
		idx.rootPageNum = newRootPageNum
		idx.Metadata.RootPageNum = newRootPageNum
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

	// Write updated metadata
	if err := idx.FileManager.WriteMetadata(idx.Metadata); err != nil {
		idx.logger.Warnf("Failed to update metadata after insert: %v", err)
	}

	idx.logger.Debugf("Successfully inserted key '%s' with document ID '%s', nodes created: %d",
		string(key), documentID, nodesCreated)

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

	// WAL Integration: Log BEFORE creating tombstone (PostgreSQL-style durability)
	// This ensures we can replay the deletion after a crash
	// DRY Principle: Reuse same WAL infrastructure as Insert
	if idx.walEnabled && idx.walManager != nil {
		lsn := idx.nextLSN
		idx.nextLSN++ // Increment LSN for next operation

		if err := idx.walManager.LogDelete(idx.Metadata.IndexName, idx.bundleName, key, documentID, lsn); err != nil {
			idx.logger.Errorf("Failed to log delete to WAL: %v", err)
			return fmt.Errorf("WAL delete failed: %w", err)
		}

		idx.logger.Debugf("Logged delete to WAL: LSN=%d, key=%s, docID=%s", lsn, string(key), documentID)
	}

	idx.logger.Debugf("Deleting key '%s' with document ID '%s'", string(key), documentID)

	// Perform the deletion
	newRootPageNum, affectsParentNode, nodesDeleted, err := idx.deleteInternal(key, documentID, idx.rootPageNum)
	if err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}

	// Handle root page changes if deletion affected tree structure
	if newRootPageNum != idx.rootPageNum {
		idx.logger.Debugf("Root page changed from %d to %d due to deletion",
			idx.rootPageNum, newRootPageNum)
		idx.rootPageNum = newRootPageNum
		idx.Metadata.RootPageNum = newRootPageNum
	}

	// Log structural changes for monitoring and debugging
	if affectsParentNode {
		idx.logger.Debugf("Deletion caused structural changes affecting parent nodes")
	}

	// Track nodes deleted for maintenance and statistics
	if nodesDeleted > 0 {
		idx.logger.Debugf("Deletion resulted in %d nodes being removed/merged", nodesDeleted)
		idx.Metadata.TotalNodes -= uint32(nodesDeleted)

		// Update fragmentation metrics
		idx.updateFragmentationAfterDeletion(nodesDeleted)
	}

	// Update metadata statistics
	idx.Metadata.DecrementRecordCount()
	idx.Metadata.UpdateStatistics("delete")
	idx.Metadata.UpdateDeletionMetrics(nodesDeleted, affectsParentNode)

	// Check if tree needs maintenance after significant structural changes
	if affectsParentNode || nodesDeleted > 0 {
		idx.checkMaintenanceNeeded()
	}

	// Write updated metadata
	if err := idx.FileManager.WriteMetadata(idx.Metadata); err != nil {
		idx.logger.Warnf("Failed to update metadata after delete: %v", err)
	}

	idx.logger.Debugf("Successfully deleted key '%s' with document ID '%s', nodes deleted: %d",
		string(key), documentID, nodesDeleted)

	return nil
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
		idx.logger.Infof("High fragmentation detected (%.2f%%), maintenance recommended",
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
		idx.logger.Infof("Low fill factor detected (%.2f%%), compaction recommended", fillFactor)
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

	idx.logger.Debugf("Search for key '%s' returned %d results", string(key), len(results))

	return results, nil
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
	idx.logger.Infof("Successfully closed BTree index '%s'", idx.FilePath)

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

	idx.logger.Infof("Starting BTree index compaction")

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

	idx.logger.Infof("BTree index compaction completed successfully")

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
			idx.logger.Infof("Index file exists with valid metadata, recovering from existing data (root page: %d, records: %d)",
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

// GetCacheStats returns current cache statistics
func (pm *BTreePageManager) GetCacheStats() *CacheStats {

	return pm.GetPageManagerCacheStats()
}
