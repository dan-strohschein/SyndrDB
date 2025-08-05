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
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	logger.Debugf("Creating BTree index for bundle '%s' field '%s'", config.BundleName, config.FieldName)

	// Create file manager
	indexFilePath := config.GetIndexFilePath()
	fileManager, err := NewBTreeFileManager(indexFilePath, config.PageSize, args.Debug, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}

	// Create page manager
	pageManager, err := NewBTreePageManager(config.PageSize, config.CacheSize, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}

	// Create metadata
	metadata := NewBTreeMetadata(config)

	// Create BTree index instance
	index := &BTreeIndex{
		FilePath:    indexFilePath,
		metadata:    metadata,
		fileManager: fileManager,
		pageManager: pageManager,
		rootPageNum: metadata.RootPageNum, //1, // Page 0 is metadata, root starts at page 1
		mutex:       sync.RWMutex{},
		isOpen:      true,
		bundleName:  config.BundleName,
		fieldName:   config.FieldName,
		logger:      logger,
	}

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
		metadata:    metadata,
		fileManager: fileManager,
		pageManager: pageManager,
		rootPageNum: metadata.RootPageNum,
		mutex:       sync.RWMutex{},
		isOpen:      true,
		bundleName:  metadata.BundleName,
		fieldName:   metadata.FieldName,
		logger:      logger,
	}

	logger.Infof("Successfully opened BTree index '%s' for bundle '%s' field '%s'",
		filePath, metadata.BundleName, metadata.FieldName)

	return index, nil
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

	if uint32(len(key)) > idx.metadata.PageSize/4 {
		return fmt.Errorf("key length (%d) exceeds maximum allowed (%d)",
			len(key), idx.metadata.PageSize/4)
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	// Validate root page number before proceeding
	if idx.rootPageNum == 0 {
		idx.logger.Errorf("Invalid root page number: %d, attempting to recover from metadata", idx.rootPageNum)
		if idx.metadata.RootPageNum > 0 {
			idx.rootPageNum = idx.metadata.RootPageNum
			idx.logger.Infof("Recovered root page number from metadata: %d", idx.rootPageNum)
		} else {
			return fmt.Errorf("invalid root page number: metadata also indicates root page 0")
		}
	}

	idx.logger.Debugf("Inserting key '%s' with document ID '%s'", string(key), documentID)

	// Check for uniqueness if required
	if idx.metadata.IsUnique {
		existing, totalNodesVisited, err := idx.searchInternal(key, idx.rootPageNum)
		if err != nil {
			return fmt.Errorf("failed to check for existing key: %w", err)
		}

		// Update search metrics for uniqueness check
		idx.metadata.UpdateSearchMetrics(len(existing), totalNodesVisited)

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
	newRootPageNum, affectsParentNode, nodesCreated, err := idx.insertInternal(key, documentID, idx.rootPageNum)
	if err != nil {
		return fmt.Errorf("failed to insert key: %w", err)
	}

	// Handle root page changes if insertion caused tree restructuring
	if newRootPageNum != idx.rootPageNum {
		idx.logger.Debugf("Root page changed from %d to %d due to insertion",
			idx.rootPageNum, newRootPageNum)
		idx.rootPageNum = newRootPageNum
		idx.metadata.RootPageNum = newRootPageNum
	}

	// Log structural changes for monitoring and debugging
	if affectsParentNode {
		idx.logger.Debugf("Insertion caused structural changes affecting parent nodes")
	}

	// Track nodes created for maintenance and statistics
	if nodesCreated > 0 {
		idx.logger.Debugf("Insertion resulted in %d new nodes being created", nodesCreated)
		idx.metadata.TotalNodes += uint32(nodesCreated)

		// Update fragmentation metrics after node creation
		idx.updateFragmentationAfterInsertion(nodesCreated)
	}

	// Update metadata statistics
	idx.metadata.IncrementRecordCount()
	idx.metadata.UpdateStatistics("insert")
	idx.metadata.UpdateInsertionMetrics(nodesCreated, affectsParentNode)

	// Check if tree needs maintenance after significant structural changes
	if affectsParentNode || nodesCreated > 0 {
		idx.checkMaintenanceNeeded()
	}

	// Write updated metadata
	if err := idx.fileManager.WriteMetadata(idx.metadata); err != nil {
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
	if idx.metadata.TotalNodes == 0 {
		idx.metadata.FragmentationPct = 0.0
		return
	}

	// Node creation typically reduces fragmentation by filling existing space
	// or creating well-balanced new nodes
	fragmentationReduction := float64(nodesCreated) / float64(idx.metadata.TotalNodes) * 5.0 // Small reduction per new node

	// Update fragmentation percentage (clamped to 0.0 minimum)
	idx.metadata.FragmentationPct = MaxFloat(0.0, idx.metadata.FragmentationPct-fragmentationReduction)

	idx.logger.Debugf("Updated fragmentation: reduced by %.2f%% to %.2f%% after creating %d nodes",
		fragmentationReduction, idx.metadata.FragmentationPct, nodesCreated)
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
		idx.metadata.RootPageNum = newRootPageNum
	}

	// Log structural changes for monitoring and debugging
	if affectsParentNode {
		idx.logger.Debugf("Deletion caused structural changes affecting parent nodes")
	}

	// Track nodes deleted for maintenance and statistics
	if nodesDeleted > 0 {
		idx.logger.Debugf("Deletion resulted in %d nodes being removed/merged", nodesDeleted)
		idx.metadata.TotalNodes -= uint32(nodesDeleted)

		// Update fragmentation metrics
		idx.updateFragmentationAfterDeletion(nodesDeleted)
	}

	// Update metadata statistics
	idx.metadata.DecrementRecordCount()
	idx.metadata.UpdateStatistics("delete")
	idx.metadata.UpdateDeletionMetrics(nodesDeleted, affectsParentNode)

	// Check if tree needs maintenance after significant structural changes
	if affectsParentNode || nodesDeleted > 0 {
		idx.checkMaintenanceNeeded()
	}

	// Write updated metadata
	if err := idx.fileManager.WriteMetadata(idx.metadata); err != nil {
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
	if idx.metadata.TotalNodes == 0 {
		idx.metadata.FragmentationPct = 0.0
		return
	}

	// Calculate fragmentation reduction due to node consolidation
	fragmentationReduction := float64(nodesDeleted) / float64(idx.metadata.TotalNodes) * 100.0

	// Update fragmentation percentage (clamped to 0.0 minimum)
	idx.metadata.FragmentationPct = MaxFloat(0.0, idx.metadata.FragmentationPct-fragmentationReduction)

	idx.logger.Debugf("Updated fragmentation: reduced by %.2f%% to %.2f%% after deleting %d nodes",
		fragmentationReduction, idx.metadata.FragmentationPct, nodesDeleted)
}

// checkMaintenanceNeeded determines if the index needs maintenance after structural changes
// This function analyzes the current state and schedules maintenance if needed
func (idx *BTreeIndex) checkMaintenanceNeeded() {
	// Check if fragmentation is too high
	if idx.metadata.FragmentationPct > 25.0 {
		idx.logger.Infof("High fragmentation detected (%.2f%%), maintenance recommended",
			idx.metadata.FragmentationPct)
		idx.metadata.MaintenanceNeeded = true
	}

	// Check if tree height is becoming unbalanced
	fillFactor, err := idx.calculateFillFactor()
	if err != nil {
		idx.logger.Warnf("Failed to calculate fill factor: %v", err)
		fillFactor = 0.0
	}

	if fillFactor < 0.5 {
		idx.logger.Infof("Low fill factor detected (%.2f%%), compaction recommended", fillFactor)
		idx.metadata.CompactionNeeded = true
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

	idx.logger.Debugf("Searching for key '%s'", string(key))

	// Perform the search
	results, totalNodesVisited, err := idx.searchInternal(key, idx.rootPageNum)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}

	// Update metadata statistics
	idx.metadata.UpdateStatistics("search")
	idx.metadata.UpdateSearchMetrics(len(results), totalNodesVisited)

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
	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	if len(startKey) == 0 || len(endKey) == 0 {
		return nil, fmt.Errorf("start key and end key cannot be empty")
	}

	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	idx.logger.Debugf("Range search from key '%s' to '%s'", string(startKey), string(endKey))

	// Perform the range search
	// allDocuments, keysFound, nodesVisited
	results, keysFound, nodesVisited, err := idx.rangeSearchInternal(startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("range search failed: %w", err)
	}

	// Update metadata statistics with detailed metrics
	idx.metadata.UpdateStatistics("search")
	idx.metadata.UpdateSearchMetrics(keysFound, nodesVisited)

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
	if err := idx.pageManager.Flush(idx.fileManager.WritePage); err != nil {
		idx.logger.Warnf("Failed to flush pages during close: %v", err)
	}

	// Update and write final metadata
	idx.metadata.LastModified = time.Now()
	if err := idx.fileManager.WriteMetadata(idx.metadata); err != nil {
		idx.logger.Warnf("Failed to write metadata during close: %v", err)
	}

	// Close file manager
	if err := idx.fileManager.Close(); err != nil {
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
	cacheStats := idx.pageManager.GetCacheStats()

	avgKeyLength, err := idx.calculateAverageKeyLength()
	if err != nil {
		idx.logger.Warnf("Failed to calculate average key length: %v", err)
		avgKeyLength = 0.0
	}

	return &BTreeStats{
		TotalRecords:      idx.metadata.TotalRecords,
		TotalNodes:        idx.metadata.TotalNodes,
		TreeHeight:        idx.metadata.TreeHeight,
		AverageKeyLength:  avgKeyLength,
		FillFactor:        fillFactor,
		FragmentationPct:  idx.metadata.FragmentationPct,
		CacheHitRate:      cacheStats.HitRate,
		TotalSearches:     idx.metadata.SearchCount,
		TotalInserts:      idx.metadata.InsertCount,
		TotalDeletes:      idx.metadata.DeleteCount,
		AverageSearchTime: 0, // Would need to track timing
		LastUpdated:       time.Now(),
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

	// Perform compaction
	if err := idx.compact(); err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Update metadata
	idx.metadata.LastCompaction = time.Now()
	idx.metadata.FragmentationPct = 0.0 // Reset after compaction

	// Write updated metadata
	if err := idx.fileManager.WriteMetadata(idx.metadata); err != nil {
		return fmt.Errorf("failed to update metadata after compaction: %w", err)
	}

	idx.logger.Infof("BTree index compaction completed successfully")

	return nil
}

// Private helper methods

// initializeIndex creates the initial index structure
func (idx *BTreeIndex) initializeIndex() error {
	// Ensure metadata has correct root page number
	if idx.metadata.RootPageNum == 0 {
		idx.metadata.RootPageNum = 1 // Page 0 is metadata, root starts at page 1
	}

	// Ensure index root page number matches metadata
	idx.rootPageNum = idx.metadata.RootPageNum

	// Write metadata to page 0
	if err := idx.fileManager.WriteMetadata(idx.metadata); err != nil {
		return fmt.Errorf("failed to write initial metadata: %w", err)
	}

	// Create root node (empty leaf node)
	rootNode := NewBTreeNode(true, idx.metadata.PageSize) // Start with leaf node
	rootNode.PageNum = idx.rootPageNum

	// Write root node to page 1
	if err := idx.fileManager.WritePage(idx.rootPageNum, rootNode); err != nil {
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
	pageNum, affectsParentNode, nodesCreated, nil := insertInternal(idx, key, documentID, rootPageNum)
	if nil != nil {
		return 0, false, 0, fmt.Errorf("insert failed: %w", nil)
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
		idx.metadata.TreeHeight, idx.metadata.TotalNodes)

	idx.metadata.TreeHeight = result.Height
	idx.metadata.TotalNodes = result.TotalNodes
	idx.metadata.FragmentationPct = result.FragmentationPct
	idx.metadata.FillFactor = result.FillFactor
	idx.metadata.AverageKeyLength = result.AverageKeyLength
	idx.metadata.LastValidation = time.Now()
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
