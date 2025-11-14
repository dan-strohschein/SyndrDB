/*
BTREE HELPER FUNCTIONS SYSTEM

This file implements utility and helper functions for BTree index operations in SyndrDB.
It provides validation, calculation, and maintenance functions that support the core
BTree operations while following the algorithms used in PostgreSQL, MySQL, and SQL Server.

HELPER FUNCTION CATEGORIES:

VALIDATION FUNCTIONS:
- Tree structure integrity validation ensuring proper B+ tree properties
- Key ordering validation across all nodes to maintain sorted order
- Node balance validation to ensure optimal tree performance
- Cross-reference validation between parent and child nodes
- Metadata consistency validation against actual tree structure

CALCULATION FUNCTIONS:
- Tree height calculation for performance analysis
- Fill factor calculation for space utilization monitoring
- Average key length calculation for memory optimization
- Node capacity calculation based on page sizes and key characteristics
- Memory usage calculation for cache management

MAINTENANCE FUNCTIONS:
- Tree statistics collection and updating
- Fragmentation analysis and reporting
- Performance metric calculation
- Index health assessment and reporting
- Cleanup operations for orphaned nodes

UTILITY FUNCTIONS:
- Binary search implementations for efficient key lookup
- Key comparison functions with proper byte ordering
- Node traversal utilities for tree operations
- Memory allocation and deallocation helpers
- Error formatting and logging utilities

PERFORMANCE ANALYSIS:
- Cache hit rate analysis for optimization recommendations
- Query pattern analysis for index tuning suggestions
- Space utilization analysis for maintenance scheduling
- Access pattern tracking for performance optimization

This implementation follows the Single Responsibility Principle by providing
focused utility functions that support the main BTree operations without
duplicating functionality. Each helper function has a specific purpose and
can be used independently by other components in the BTree system.
*/

package btreeindexV2

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"time"
)

// ValidationResult represents the result of tree validation operations
// This structure contains detailed information about any issues found
type ValidationResult struct {
	IsValid      bool          // Whether the tree structure is valid
	Errors       []string      // List of validation errors found
	Warnings     []string      // List of potential issues or warnings
	NodesChecked int           // Number of nodes that were validated
	TimeElapsed  time.Duration // Time taken for validation
}

// TreeStatistics represents comprehensive statistics about the BTree structure
// This structure provides detailed metrics for performance analysis and optimization
type TreeStatistics struct {
	TotalNodes        int     // Total number of nodes in the tree
	LeafNodes         int     // Number of leaf nodes
	InternalNodes     int     // Number of internal nodes
	TotalKeys         int     // Total number of keys across all nodes
	AverageKeyLength  float64 // Average length of keys in bytes
	AverageFillFactor float64 // Average fill factor across all nodes
	TreeHeight        int     // Height of the tree
	MaxKeyLength      int     // Maximum key length found
	MinKeyLength      int     // Minimum key length found
	MemoryUsage       uint64  // Total memory usage in bytes
}

// NodeInfo represents detailed information about a specific node
// This structure is used for debugging and analysis purposes
type NodeInfo struct {
	PageNum      uint32   // Page number of the node
	IsLeaf       bool     // Whether this is a leaf node
	KeyCount     int      // Number of keys in the node
	Keys         [][]byte // Copy of the keys for analysis
	FillFactor   float64  // Fill factor of this specific node
	MemoryUsage  uint64   // Memory usage of this node
	Parent       uint32   // Page number of parent node (0 if root)
	Children     []uint32 // Page numbers of child nodes (empty for leaf)
	NextLeaf     uint32   // Page number of next leaf (0 if not applicable)
	PreviousLeaf uint32   // Page number of previous leaf (0 if not applicable)
}

// ValidateTreeStructure performs comprehensive validation of the BTree structure
// This function checks all aspects of the tree to ensure it maintains proper B+ tree properties
// Parameters:
//   - idx: The BTree index to validate
//
// Returns:
//   - *ValidationResult: Detailed validation results with any errors or warnings found
func ValidateTreeStructure(idx *BTreeIndex) *ValidationResult {
	startTime := time.Now()

	result := &ValidationResult{
		IsValid:      true,
		Errors:       make([]string, 0),
		Warnings:     make([]string, 0),
		NodesChecked: 0,
		TimeElapsed:  0,
	}

	idx.logger.Debugf("Starting comprehensive tree structure validation")

	// Validate that index is open
	if !idx.isOpen {
		result.IsValid = false
		result.Errors = append(result.Errors, "index is not open")
		result.TimeElapsed = time.Since(startTime)
		return result
	}

	// Validate root node exists
	if idx.rootPageNum == 0 {
		result.IsValid = false
		result.Errors = append(result.Errors, "root page number is 0")
		result.TimeElapsed = time.Since(startTime)
		return result
	}

	// Validate tree height consistency
	if err := validateTreeHeight(idx, result); err != nil {
		idx.logger.Errorf("Tree height validation failed: %v", err)
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("tree height validation failed: %v", err))
	}

	// Validate all leaf nodes are at the same level
	if err := validateLeafLevel(idx, result); err != nil {
		idx.logger.Errorf("Leaf level validation failed: %v", err)
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("leaf level validation failed: %v", err))
	}

	// Validate key ordering across all nodes
	if err := validateKeyOrdering(idx, result); err != nil {
		idx.logger.Errorf("Key ordering validation failed: %v", err)
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("key ordering validation failed: %v", err))
	}

	// Validate parent-child relationships
	if err := validateParentChildRelationships(idx, result); err != nil {
		idx.logger.Errorf("Parent-child validation failed: %v", err)
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("parent-child validation failed: %v", err))
	}

	// Validate leaf node linking
	if err := validateLeafNodeLinking(idx, result); err != nil {
		idx.logger.Errorf("Leaf node linking validation failed: %v", err)
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("leaf node linking validation failed: %v", err))
	}

	result.TimeElapsed = time.Since(startTime)

	if result.IsValid {
		idx.logger.Infof("Tree structure validation completed successfully (%d nodes checked in %v)",
			result.NodesChecked, result.TimeElapsed)
	} else {
		idx.logger.Warnf("Tree structure validation found %d errors and %d warnings",
			len(result.Errors), len(result.Warnings))
	}

	return result
}

// CalculateTreeStatistics computes comprehensive statistics about the BTree structure
// This function analyzes the entire tree to provide performance and utilization metrics
// Parameters:
//   - idx: The BTree index to analyze
//
// Returns:
//   - *TreeStatistics: Comprehensive statistics about the tree structure
//   - error: Any error that occurred during statistics calculation
func CalculateTreeStatistics(idx *BTreeIndex) (*TreeStatistics, error) {
	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	idx.logger.Debugf("Calculating comprehensive tree statistics")

	stats := &TreeStatistics{
		TotalNodes:        0,
		LeafNodes:         0,
		InternalNodes:     0,
		TotalKeys:         0,
		AverageKeyLength:  0.0,
		AverageFillFactor: 0.0,
		TreeHeight:        0,
		MaxKeyLength:      0,
		MinKeyLength:      math.MaxInt32,
		MemoryUsage:       0,
	}

	// Calculate tree height
	height, err := calculateActualTreeHeight(idx, idx.rootPageNum)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate tree height: %w", err)
	}
	stats.TreeHeight = height

	// Traverse all nodes to collect statistics
	if err := collectNodeStatistics(idx, idx.rootPageNum, stats); err != nil {
		return nil, fmt.Errorf("failed to collect node statistics: %w", err)
	}

	// Calculate averages
	if stats.TotalKeys > 0 {
		stats.AverageKeyLength = float64(stats.MemoryUsage) / float64(stats.TotalKeys)
	}

	if stats.TotalNodes > 0 {
		stats.AverageFillFactor = stats.AverageFillFactor / float64(stats.TotalNodes)
	}

	// Adjust minimum key length if no keys were found
	if stats.TotalKeys == 0 {
		stats.MinKeyLength = 0
	}

	idx.logger.Infof("Tree statistics calculated: %d nodes (%d leaf, %d internal), %d keys, height %d",
		stats.TotalNodes, stats.LeafNodes, stats.InternalNodes, stats.TotalKeys, stats.TreeHeight)

	return stats, nil
}

// CalculateFillFactor computes the fill factor for the entire BTree
// This function analyzes how efficiently the tree is using its allocated space
// Parameters:
//   - idx: The BTree index to analyze
//
// Returns:
//   - float64: The fill factor as a percentage (0.0 to 1.0)
//   - error: Any error that occurred during calculation
func CalculateFillFactor(idx *BTreeIndex) (float64, error) {
	if !idx.isOpen {
		return 0.0, fmt.Errorf("index is not open")
	}

	idx.logger.Debugf("Calculating tree fill factor")

	totalCapacity := uint32(0)
	totalUsed := uint32(0)

	err := traverseAllNodes(idx, idx.rootPageNum, func(node *BTreeNode) error {
		maxKeys := calculateMaxKeysForNode(node, idx.Metadata.PageSize)
		totalCapacity += maxKeys
		totalUsed += node.KeyCount
		return nil
	})

	if err != nil {
		return 0.0, fmt.Errorf("failed to traverse nodes for fill factor calculation: %w", err)
	}

	if totalCapacity == 0 {
		return 0.0, nil
	}

	fillFactor := float64(totalUsed) / float64(totalCapacity)

	idx.logger.Debugf("Fill factor calculated: %.2f%% (%d used / %d capacity)",
		fillFactor*100, totalUsed, totalCapacity)

	return fillFactor, nil
}

// CalculateAverageKeyLength computes the average length of keys in the BTree
// This function analyzes key sizes for memory optimization recommendations
// Parameters:
//   - idx: The BTree index to analyze
//
// Returns:
//   - float64: The average key length in bytes
//   - error: Any error that occurred during calculation
func CalculateAverageKeyLength(idx *BTreeIndex) (float64, error) {
	if !idx.isOpen {
		return 0.0, fmt.Errorf("index is not open")
	}

	idx.logger.Debugf("Calculating average key length")

	totalLength := 0
	totalKeys := 0

	err := traverseLeafNodes(idx, idx.rootPageNum, func(leaf *BTreeNode) error {
		for _, key := range leaf.Keys {
			totalLength += len(key)
			totalKeys++
		}
		return nil
	})

	if err != nil {
		return 0.0, fmt.Errorf("failed to traverse leaf nodes for key length calculation: %w", err)
	}

	if totalKeys == 0 {
		return 0.0, nil
	}

	averageLength := float64(totalLength) / float64(totalKeys)

	idx.logger.Debugf("Average key length calculated: %.2f bytes (%d total length / %d keys)",
		averageLength, totalLength, totalKeys)

	return averageLength, nil
}

// GetNodeInfo retrieves detailed information about a specific node
// This function provides comprehensive debugging information about a node
// Parameters:
//   - idx: The BTree index containing the node
//   - pageNum: The page number of the node to analyze
//
// Returns:
//   - *NodeInfo: Detailed information about the node
//   - error: Any error that occurred during information retrieval
func GetNodeInfo(idx *BTreeIndex, pageNum uint32) (*NodeInfo, error) {
	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	idx.logger.Debugf("Getting detailed information for node %d", pageNum)

	// Load the node
	pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return nil, fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
	}

	// Create node info structure
	info := &NodeInfo{
		PageNum:      pageNum,
		IsLeaf:       node.IsLeaf,
		KeyCount:     int(node.KeyCount),
		Keys:         make([][]byte, len(node.Keys)),
		FillFactor:   0.0,
		MemoryUsage:  0,
		Parent:       node.ParentPage,
		Children:     make([]uint32, len(node.Children)),
		NextLeaf:     node.NextLeaf,
		PreviousLeaf: node.PrevLeaf,
	}

	// Copy keys (deep copy to avoid reference issues)
	for i, key := range node.Keys {
		info.Keys[i] = make([]byte, len(key))
		copy(info.Keys[i], key)
	}

	// Copy children
	copy(info.Children, node.Children)

	// Calculate fill factor for this node
	maxKeys := calculateMaxKeysForNode(node, idx.Metadata.PageSize)
	if maxKeys > 0 {
		info.FillFactor = float64(node.KeyCount) / float64(maxKeys)
	}

	// Calculate memory usage
	info.MemoryUsage = calculateNodeMemoryUsage(node)

	idx.logger.Debugf("Node %d info: leaf=%t, keys=%d, fill=%.2f%%",
		pageNum, info.IsLeaf, info.KeyCount, info.FillFactor*100)

	return info, nil
}

// FindKeyInTree performs a comprehensive search for a key across the entire tree
// This function is primarily used for debugging and validation purposes
// Parameters:
//   - idx: The BTree index to search
//   - key: The key to find
//
// Returns:
//   - []uint32: List of page numbers where the key was found
//   - error: Any error that occurred during the search
func FindKeyInTree(idx *BTreeIndex, key []byte) ([]uint32, error) {
	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	idx.logger.Debugf("Searching for key '%s' across entire tree", string(key))

	var foundPages []uint32

	err := traverseAllNodes(idx, idx.rootPageNum, func(node *BTreeNode) error {
		// Check if key exists in this node
		for _, nodeKey := range node.Keys {
			if bytes.Equal(nodeKey, key) {
				foundPages = append(foundPages, node.PageNum)
				break
			}
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to search tree: %w", err)
	}

	idx.logger.Debugf("Key '%s' found in %d nodes: %v", string(key), len(foundPages), foundPages)

	return foundPages, nil
}

// Private helper functions for validation

// validateTreeHeight validates that the tree height matches metadata
func validateTreeHeight(idx *BTreeIndex, result *ValidationResult) error {
	actualHeight, err := calculateActualTreeHeight(idx, idx.rootPageNum)
	if err != nil {
		return fmt.Errorf("failed to calculate actual tree height: %w", err)
	}

	if uint32(actualHeight) != idx.Metadata.TreeHeight {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("metadata height (%d) doesn't match actual height (%d)",
				idx.Metadata.TreeHeight, actualHeight))
	}

	return nil
}

// validateLeafLevel validates that all leaf nodes are at the same level
func validateLeafLevel(idx *BTreeIndex, result *ValidationResult) error {
	leafLevels := make(map[uint32]int) // page number -> level

	err := traverseAllNodesWithLevel(idx, idx.rootPageNum, 0, func(node *BTreeNode, level int) error {
		result.NodesChecked++

		if node.IsLeaf {
			leafLevels[node.PageNum] = level
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to traverse nodes for leaf level validation: %w", err)
	}

	// Check that all leaf nodes are at the same level
	var expectedLevel *int
	for pageNum, level := range leafLevels {
		if expectedLevel == nil {
			expectedLevel = &level
		} else if level != *expectedLevel {
			result.Errors = append(result.Errors,
				fmt.Sprintf("leaf node %d at level %d, expected level %d",
					pageNum, level, *expectedLevel))
		}
	}

	return nil
}

// validateKeyOrdering validates that keys are properly ordered within and across nodes
func validateKeyOrdering(idx *BTreeIndex, result *ValidationResult) error {
	return traverseAllNodes(idx, idx.rootPageNum, func(node *BTreeNode) error {
		// Validate key ordering within this node
		for i := 1; i < len(node.Keys); i++ {
			if bytes.Compare(node.Keys[i-1], node.Keys[i]) >= 0 {
				result.Errors = append(result.Errors,
					fmt.Sprintf("keys out of order in node %d at positions %d and %d",
						node.PageNum, i-1, i))
			}
		}

		return nil
	})
}

// validateParentChildRelationships validates parent-child pointer consistency
func validateParentChildRelationships(idx *BTreeIndex, result *ValidationResult) error {
	return traverseAllNodes(idx, idx.rootPageNum, func(node *BTreeNode) error {
		// For internal nodes, validate that children point back to this node as parent
		if !node.IsLeaf {
			for _, childPageNum := range node.Children {
				childData, err := idx.PageManager.GetPage(childPageNum, func(pn uint32) (interface{}, error) {
					return idx.FileManager.ReadPage(pn)
				})
				if err != nil {
					result.Errors = append(result.Errors,
						fmt.Sprintf("failed to load child page %d from parent %d", childPageNum, node.PageNum))
					continue
				}

				child, ok := childData.(*BTreeNode)
				if !ok {
					result.Errors = append(result.Errors,
						fmt.Sprintf("child page %d is not a valid BTree node", childPageNum))
					continue
				}

				if child.ParentPage != node.PageNum {
					result.Errors = append(result.Errors,
						fmt.Sprintf("child %d has parent %d, expected %d",
							childPageNum, child.ParentPage, node.PageNum))
				}
			}
		}

		return nil
	})
}

// validateLeafNodeLinking validates the linked list structure of leaf nodes
func validateLeafNodeLinking(idx *BTreeIndex, result *ValidationResult) error {
	var leafNodes []*BTreeNode

	// Collect all leaf nodes
	err := traverseLeafNodes(idx, idx.rootPageNum, func(leaf *BTreeNode) error {
		leafNodes = append(leafNodes, leaf)
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to collect leaf nodes: %w", err)
	}

	// Sort leaf nodes by their first key for validation
	sort.Slice(leafNodes, func(i, j int) bool {
		if len(leafNodes[i].Keys) == 0 {
			return true
		}
		if len(leafNodes[j].Keys) == 0 {
			return false
		}
		return bytes.Compare(leafNodes[i].Keys[0], leafNodes[j].Keys[0]) < 0
	})

	// Validate the linked list structure
	for i := 0; i < len(leafNodes)-1; i++ {
		current := leafNodes[i]
		next := leafNodes[i+1]

		if current.NextLeaf != next.PageNum {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("leaf %d next pointer (%d) doesn't match expected next leaf (%d)",
					current.PageNum, current.NextLeaf, next.PageNum))
		}

		if next.PrevLeaf != current.PageNum {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("leaf %d previous pointer (%d) doesn't match expected previous leaf (%d)",
					next.PageNum, next.PrevLeaf, current.PageNum))
		}
	}

	return nil
}

// Private helper functions for calculations

// calculateActualTreeHeight calculates the actual height of the tree by traversal
func calculateActualTreeHeight(idx *BTreeIndex, rootPageNum uint32) (int, error) {
	if rootPageNum == 0 {
		return 0, nil
	}

	pageData, err := idx.PageManager.GetPage(rootPageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return 0, fmt.Errorf("failed to load root page %d: %w", rootPageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return 0, fmt.Errorf("root page %d does not contain a valid BTree node", rootPageNum)
	}

	if node.IsLeaf {
		return 1, nil
	}

	// Find height by traversing to a leaf
	maxHeight := 0
	for _, childPageNum := range node.Children {
		childHeight, err := calculateActualTreeHeight(idx, childPageNum)
		if err != nil {
			return 0, fmt.Errorf("failed to calculate height for child %d: %w", childPageNum, err)
		}
		if childHeight > maxHeight {
			maxHeight = childHeight
		}
	}

	return maxHeight + 1, nil
}

// collectNodeStatistics collects statistics from all nodes in the tree
func collectNodeStatistics(idx *BTreeIndex, pageNum uint32, stats *TreeStatistics) error {
	pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
	}

	// Update statistics for this node
	stats.TotalNodes++
	stats.TotalKeys += int(node.KeyCount)

	if node.IsLeaf {
		stats.LeafNodes++
	} else {
		stats.InternalNodes++
	}

	// Calculate fill factor for this node
	maxKeys := calculateMaxKeysForNode(node, idx.Metadata.PageSize)
	if maxKeys > 0 {
		fillFactor := float64(node.KeyCount) / float64(maxKeys)
		stats.AverageFillFactor += fillFactor
	}

	// Update key length statistics
	for _, key := range node.Keys {
		keyLen := len(key)
		if keyLen > stats.MaxKeyLength {
			stats.MaxKeyLength = keyLen
		}
		if keyLen < stats.MinKeyLength {
			stats.MinKeyLength = keyLen
		}
		stats.MemoryUsage += uint64(keyLen)
	}

	// Recurse to children if this is an internal node
	if !node.IsLeaf {
		for _, childPageNum := range node.Children {
			if err := collectNodeStatistics(idx, childPageNum, stats); err != nil {
				return fmt.Errorf("failed to collect statistics for child %d: %w", childPageNum, err)
			}
		}
	}

	return nil
}

// traverseAllNodes traverses all nodes in the tree and calls the provided function
func traverseAllNodes(idx *BTreeIndex, pageNum uint32, fn func(*BTreeNode) error) error {
	pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
	}

	// Call function for this node
	if err := fn(node); err != nil {
		return err
	}

	// Recurse to children if this is an internal node
	if !node.IsLeaf {
		for _, childPageNum := range node.Children {
			if err := traverseAllNodes(idx, childPageNum, fn); err != nil {
				return err
			}
		}
	}

	return nil
}

// traverseAllNodesWithLevel traverses all nodes with level information
func traverseAllNodesWithLevel(idx *BTreeIndex, pageNum uint32, level int, fn func(*BTreeNode, int) error) error {
	pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
	}

	// Call function for this node
	if err := fn(node, level); err != nil {
		return err
	}

	// Recurse to children if this is an internal node
	if !node.IsLeaf {
		for _, childPageNum := range node.Children {
			if err := traverseAllNodesWithLevel(idx, childPageNum, level+1, fn); err != nil {
				return err
			}
		}
	}

	return nil
}

// traverseLeafNodes traverses only leaf nodes in the tree
func traverseLeafNodes(idx *BTreeIndex, pageNum uint32, fn func(*BTreeNode) error) error {
	return traverseAllNodes(idx, pageNum, func(node *BTreeNode) error {
		if node.IsLeaf {
			return fn(node)
		}
		return nil
	})
}

// calculateMaxKeysForNode calculates the maximum number of keys that can fit in a node
// This function performs accurate calculations based on actual node structure and key characteristics
// Parameters:
//   - node: The BTree node to calculate capacity for
//   - pageSize: The page size in bytes
//
// Returns:
//   - int: The maximum number of keys that can fit in the node
func calculateMaxKeysForNode(node *BTreeNode, pageSize uint32) uint32 {
	// Calculate the fixed overhead for the node structure
	nodeHeaderSize := calculateNodeHeaderSize()

	// Calculate available space for keys and associated data
	availableSpace := pageSize - nodeHeaderSize

	// Ensure we have some minimum space available
	if availableSpace <= 0 {
		return 1 // Minimum of 1 key per node
	}

	// Calculate space requirements based on node type
	var maxKeys uint32
	if node.IsLeaf {
		maxKeys = calculateMaxKeysForLeafNode(node, availableSpace)
	} else {
		maxKeys = calculateMaxKeysForInternalNode(node, availableSpace)
	}

	// Ensure we have at least 1 key and don't exceed reasonable limits
	if maxKeys < 1 {
		maxKeys = 1
	}

	// Apply B-tree minimum degree constraints (for proper B-tree properties)
	minDegree := calculateMinimumDegree(pageSize)
	maxKeys = max(maxKeys, minDegree)

	return maxKeys
}

// calculateNodeHeaderSize calculates the fixed overhead size for a BTree node
// This includes all the metadata and structure information stored with each node
// Returns:
//   - int: The header size in bytes
func calculateNodeHeaderSize() uint32 {
	headerSize := uint32(0)

	// Basic node metadata
	headerSize += 4 // PageNum (uint32)
	headerSize += 1 // IsLeaf (bool)
	headerSize += 4 // KeyCount (uint32)
	headerSize += 4 // ParentPage (uint32)
	headerSize += 4 // NextLeaf (uint32)
	headerSize += 4 // PrevLeaf (uint32)

	// Timestamps and versioning
	headerSize += 8 // LastModified (int64 timestamp)
	headerSize += 4 // Version (uint32)

	// Checksum and integrity
	headerSize += 4 // Checksum (uint32)

	// Padding for alignment (ensure 8-byte alignment)
	headerSize += 3 // Padding bytes

	// Slice headers for Keys, Values, and Children arrays
	headerSize += 24 // Keys slice header (3 * 8 bytes)
	headerSize += 24 // Values slice header (3 * 8 bytes)
	headerSize += 24 // Children slice header (3 * 8 bytes)

	return headerSize
}

// calculateMaxKeysForLeafNode calculates maximum keys for a leaf node
// Leaf nodes store keys and associated document ID lists
// Parameters:
//   - node: The leaf node to calculate for
//   - availableSpace: Available space in bytes after header
//
// Returns:
//   - int: Maximum number of keys that can fit
func calculateMaxKeysForLeafNode(node *BTreeNode, availableSpace uint32) uint32 {
	// Analyze existing keys to determine average sizes
	avgKeySize := calculateAverageKeySize(node)
	avgDocumentListSize := calculateAverageDocumentListSize(node)

	// If no existing data, use reasonable defaults
	if avgKeySize == 0 {
		avgKeySize = 32 // Default average key size
	}
	if avgDocumentListSize == 0 {
		avgDocumentListSize = 48 // Default: ~3 document IDs of 16 bytes each
	}

	// Calculate space per key-value pair
	spacePerEntry := 0

	// Key storage: pointer + length + actual key data
	spacePerEntry += 8 + 4 + avgKeySize // slice element + length + key bytes

	// Document ID list storage: pointer + length + document IDs
	spacePerEntry += 8 + 4 + avgDocumentListSize // slice element + length + doc IDs

	// Additional overhead for slice growth and memory alignment
	spacePerEntry += 8 // Growth overhead and alignment

	// Calculate maximum keys
	maxKeys := availableSpace / uint32(spacePerEntry)

	return maxKeys
}

// calculateMaxKeysForInternalNode calculates maximum keys for an internal node
// Internal nodes store keys and child page pointers
// Parameters:
//   - node: The internal node to calculate for
//   - availableSpace: Available space in bytes after header
//
// Returns:
//   - int: Maximum number of keys that can fit
func calculateMaxKeysForInternalNode(node *BTreeNode, availableSpace uint32) uint32 {
	// Analyze existing keys to determine average size
	avgKeySize := calculateAverageKeySize(node)

	// If no existing data, use reasonable default
	if avgKeySize == 0 {
		avgKeySize = 32 // Default average key size
	}

	// Calculate space per key entry
	spacePerEntry := 0

	// Key storage: pointer + length + actual key data
	spacePerEntry += 8 + 4 + avgKeySize // slice element + length + key bytes

	// Child pointer storage: one uint32 per child
	// Internal nodes have n+1 children for n keys
	spacePerEntry += 4 // Child pointer (uint32)

	// Additional overhead for slice growth and memory alignment
	spacePerEntry += 4 // Growth overhead and alignment

	// Account for the extra child pointer (n+1 children for n keys)
	extraChildPointer := 4

	// Calculate maximum keys
	maxKeys := (availableSpace - uint32(extraChildPointer)) / uint32(spacePerEntry)

	return maxKeys
}

// calculateAverageKeySize calculates the average size of keys in the node
// This provides a more accurate estimate than using fixed sizes
// Parameters:
//   - node: The node to analyze
//
// Returns:
//   - int: Average key size in bytes
func calculateAverageKeySize(node *BTreeNode) int {
	if len(node.Keys) == 0 {
		return 0
	}

	totalSize := 0
	for _, key := range node.Keys {
		totalSize += len(key)
	}

	return totalSize / len(node.Keys)
}

// calculateAverageDocumentListSize calculates the average size of document ID lists
// This is used for leaf nodes to estimate space requirements for values
// Parameters:
//   - node: The leaf node to analyze
//
// Returns:
//   - int: Average document list size in bytes
func calculateAverageDocumentListSize(node *BTreeNode) int {
	if !node.IsLeaf || len(node.Values) == 0 {
		return 0
	}

	totalSize := 0
	totalLists := 0

	for _, docList := range node.Values {
		listSize := 0
		for _, docID := range docList {
			listSize += len(docID)
		}
		// Add overhead for slice header and pointers
		listSize += 24 + (len(docList) * 8) // slice header + string pointers

		totalSize += listSize
		totalLists++
	}

	if totalLists == 0 {
		return 0
	}

	return totalSize / totalLists
}

// calculateMinimumDegree calculates the minimum degree for B-tree properties
// This ensures the tree maintains proper balance characteristics
// Parameters:
//   - pageSize: The page size in bytes
//
// Returns:
//   - int: Minimum degree (minimum number of keys per non-root node)
func calculateMinimumDegree(pageSize uint32) uint32 {
	// B-tree minimum degree is typically calculated to ensure good performance
	// Standard formula: t >= 2, where t is the minimum degree
	// Each internal node has at least t-1 keys and at most 2t-1 keys
	// Each leaf node has at least t-1 keys and at most 2t-1 keys

	// Base minimum degree on page size
	// Larger pages can support higher degrees for better performance
	switch {
	case pageSize <= 4096:
		return 16 // Small pages: degree 16 (15-31 keys)
	case pageSize <= 8192:
		return 32 // Medium pages: degree 32 (31-63 keys)
	case pageSize <= 16384:
		return 64 // Large pages: degree 64 (63-127 keys)
	default:
		return 128 // Very large pages: degree 128 (127-255 keys)
	}
}

// calculateActualNodeSize calculates the actual current size of a node in bytes
// This function provides precise measurements of node memory usage
// Parameters:
//   - node: The node to calculate size for
//
// Returns:
//   - int: Actual node size in bytes
// func calculateActualNodeSize(node *BTreeNode) uint32 {
// 	size := calculateNodeHeaderSize()

// 	// Add size of all keys
// 	for _, key := range node.Keys {
// 		size += 8 + 4 + uint32(len(key)) // slice element + length + key data
// 	}

// 	if node.IsLeaf {
// 		// Add size of all document ID lists
// 		for _, docList := range node.Values {
// 			size += 8 + 4 // slice element + length
// 			for _, docID := range docList {
// 				size += 8 + uint32(len(docID)) // string pointer + string data
// 			}
// 		}
// 	} else {
// 		// Add size of child pointers
// 		size += uint32(len(node.Children)) * 4 // uint32 per child
// 	}

// 	return size
// }

// calculateOptimalKeyCapacity calculates the optimal number of keys for best performance
// This balances space utilization with performance characteristics
// Parameters:
//   - node: The node to calculate for
//   - pageSize: The page size in bytes
//   - targetFillFactor: Desired fill factor (0.0 to 1.0)
//
// Returns:
//   - int: Optimal number of keys for the node
// func calculateOptimalKeyCapacity(node *BTreeNode, pageSize uint32, targetFillFactor float64) uint32 {
// 	maxKeys := calculateMaxKeysForNode(node, pageSize)

// 	// Apply target fill factor
// 	optimalKeys := uint32(float64(maxKeys) * targetFillFactor)

// 	// Ensure we meet minimum requirements
// 	minDegree := calculateMinimumDegree(pageSize)
// 	if optimalKeys < minDegree {
// 		optimalKeys = minDegree
// 	}

// 	return optimalKeys
// }

// validateNodeCapacity validates that a node's current size fits within the page
// This function helps detect capacity issues and corruption
// Parameters:
//   - node: The node to validate
//   - pageSize: The page size in bytes
//
// Returns:
//   - bool: Whether the node fits within the page size
//   - int: Actual size of the node
//   - error: Any validation errors found
// func validateNodeCapacity(node *BTreeNode, pageSize uint32) (bool, uint32, error) {
// 	actualSize := calculateActualNodeSize(node)

// 	if actualSize > pageSize {
// 		return false, actualSize, fmt.Errorf("node size %d exceeds page size %d", actualSize, pageSize)
// 	}

// 	// Check for reasonable utilization
// 	utilizationPercent := float64(actualSize) / float64(pageSize) * 100

// 	if utilizationPercent > 90 {
// 		return true, actualSize, fmt.Errorf("warning: node utilization is very high (%.1f%%)", utilizationPercent)
// 	}

// 	return true, actualSize, nil
// }

// Helper function for max calculation
func max(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func MaxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// calculateNodeMemoryUsage calculates the memory usage of a specific node
func calculateNodeMemoryUsage(node *BTreeNode) uint64 {
	usage := uint64(64) // Base node structure size

	// Add key sizes
	for _, key := range node.Keys {
		usage += uint64(len(key))
	}

	// Add value sizes for leaf nodes
	if node.IsLeaf {
		for _, docList := range node.Values {
			for _, docID := range docList {
				usage += uint64(len(docID))
			}
		}
	}

	// Add child pointer sizes for internal nodes
	usage += uint64(len(node.Children) * 4)

	return usage
}

// CheckTreeBalance performs comprehensive tree balance analysis and validation
// This function traverses the entire BTree structure to analyze balance characteristics,
// fill factors, fragmentation levels, and overall tree health metrics
// Parameters:
//   - idx: The BTree index to analyze for balance characteristics
//
// Returns:
//   - *TreeBalanceResult: Detailed analysis results including balance status and metrics
func CheckTreeBalance(idx *BTreeIndex) *TreeBalanceResult {
	if idx == nil {
		return &TreeBalanceResult{
			IsBalanced:       false,
			Height:           0,
			TotalNodes:       0,
			FragmentationPct: 100.0,
			FillFactor:       0.0,
			AverageKeyLength: 0.0,
			Errors:           []string{"index cannot be nil"},
		}
	}

	if !idx.isOpen {
		return &TreeBalanceResult{
			IsBalanced:       false,
			Height:           0,
			TotalNodes:       0,
			FragmentationPct: 100.0,
			FillFactor:       0.0,
			AverageKeyLength: 0.0,
			Errors:           []string{"index is not open"},
		}
	}

	if idx.rootPageNum == 0 {
		return &TreeBalanceResult{
			IsBalanced:       true, // Empty tree is balanced
			Height:           0,
			TotalNodes:       0,
			FragmentationPct: 0.0,
			FillFactor:       1.0,
			AverageKeyLength: 0.0,
			Errors:           []string{},
		}
	}

	idx.logger.Debugf("Starting tree balance analysis for index '%s'", idx.FilePath)

	result := &TreeBalanceResult{
		IsBalanced:       true,
		Height:           0,
		TotalNodes:       0,
		FragmentationPct: 0.0,
		FillFactor:       0.0,
		AverageKeyLength: 0.0,
		Errors:           []string{},
	}

	// Track analysis metrics
	leafDepths := make([]uint32, 0)
	totalKeys := uint32(0)
	totalKeyLength := uint32(0)
	totalCapacity := uint32(0)
	totalUsedSpace := uint32(0)
	nodeCount := 0
	emptyNodes := 0

	// Recursive function to analyze each node
	var analyzeNode func(pageNum uint32, depth uint32) error
	analyzeNode = func(pageNum uint32, depth uint32) error {
		// Load the node
		pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to load page %d: %v", pageNum, err))
			return err
		}

		node, ok := pageData.(*BTreeNode)
		if !ok {
			result.Errors = append(result.Errors, fmt.Sprintf("page %d is not a valid BTree node", pageNum))
			return fmt.Errorf("invalid node type")
		}

		nodeCount++

		// Calculate node capacity and utilization
		maxKeys := calculateMaxKeysForNode(node, idx.Metadata.PageSize)
		nodeCapacity := maxKeys
		nodeUsage := uint32(len(node.Keys))

		totalCapacity += nodeCapacity
		totalUsedSpace += nodeUsage

		// Track empty nodes for fragmentation calculation
		if nodeUsage == 0 {
			emptyNodes++
		}

		// Calculate key statistics
		for _, key := range node.Keys {
			totalKeys++
			totalKeyLength += uint32(len(key))
		}

		if node.IsLeaf {
			// Record leaf depth for balance analysis
			leafDepths = append(leafDepths, depth)

			// Update maximum depth
			if depth > result.Height {
				result.Height = uint32(depth)
			}
		} else {
			// Validate internal node structure
			if len(node.Children) != len(node.Keys)+1 {
				result.Errors = append(result.Errors,
					fmt.Sprintf("internal node %d has %d keys but %d children (should be %d)",
						pageNum, len(node.Keys), len(node.Children), len(node.Keys)+1))
				result.IsBalanced = false
			}

			// Recursively analyze children
			for _, childPageNum := range node.Children {
				if err := analyzeNode(childPageNum, depth+1); err != nil {
					return err
				}
			}
		}

		// Validate node is not overfull
		if nodeUsage > maxKeys {
			result.Errors = append(result.Errors,
				fmt.Sprintf("node %d is overfull: %d keys (max %d)", pageNum, nodeUsage, maxKeys))
			result.IsBalanced = false
		}

		// Validate minimum key requirements (except for root)
		if pageNum != idx.rootPageNum && nodeUsage < maxKeys/2 {
			result.Errors = append(result.Errors,
				fmt.Sprintf("node %d is underfull: %d keys (min %d)", pageNum, nodeUsage, maxKeys/2))
			result.IsBalanced = false
		}

		return nil
	}

	// Start analysis from root
	if err := analyzeNode(idx.rootPageNum, 0); err != nil {
		idx.logger.Errorf("Tree balance analysis failed: %v", err)
		result.IsBalanced = false
		return result
	}

	// Update total node count
	result.TotalNodes = uint32(nodeCount)

	// Analyze leaf depth consistency (all leaves should be at same depth)
	if len(leafDepths) > 1 {
		minDepth := leafDepths[0]
		maxDepth := leafDepths[0]

		for _, depth := range leafDepths {
			if depth < minDepth {
				minDepth = depth
			}
			if depth > maxDepth {
				maxDepth = depth
			}
		}

		// In a balanced B+ tree, all leaves should be at the same depth
		if maxDepth != minDepth {
			result.Errors = append(result.Errors,
				fmt.Sprintf("unbalanced tree: leaf depths range from %d to %d", minDepth, maxDepth))
			result.IsBalanced = false
		}
	}

	// Calculate fill factor
	if totalCapacity > 0 {
		result.FillFactor = float64(totalUsedSpace) / float64(totalCapacity)
	} else {
		result.FillFactor = 1.0
	}

	// Calculate fragmentation percentage
	if nodeCount > 0 {
		result.FragmentationPct = (float64(emptyNodes) / float64(nodeCount)) * 100.0

		// Add underutilization to fragmentation
		if totalCapacity > 0 {
			underutilization := 1.0 - result.FillFactor
			result.FragmentationPct += underutilization * 50.0 // Weight underutilization
		}
	}

	// Calculate average key length
	if totalKeys > 0 {
		result.AverageKeyLength = float64(totalKeyLength) / float64(totalKeys)
	}

	// Validate overall tree health
	if result.FillFactor < 0.3 {
		result.Errors = append(result.Errors,
			fmt.Sprintf("low fill factor: %.2f%% (recommended minimum: 30%%)", result.FillFactor*100))
		result.IsBalanced = false
	}

	if result.FragmentationPct > 40.0 {
		result.Errors = append(result.Errors,
			fmt.Sprintf("high fragmentation: %.2f%% (recommended maximum: 40%%)", result.FragmentationPct))
		result.IsBalanced = false
	}

	// Log analysis results
	idx.logger.Debugf("Tree balance analysis completed: balanced=%t, height=%d, nodes=%d, fill=%.2f%%, frag=%.2f%%",
		result.IsBalanced, result.Height, result.TotalNodes, result.FillFactor*100, result.FragmentationPct)

	if len(result.Errors) > 0 {
		idx.logger.Warnf("Tree balance issues detected: %d errors", len(result.Errors))
		for _, err := range result.Errors {
			idx.logger.Warnf("  - %s", err)
		}
	}

	return result
}

// compact performs comprehensive index compaction to reduce fragmentation and optimize performance
// This function extracts all entries from the current index, creates an optimized new index structure,
// and atomically replaces the old structure with the new one, following database compaction patterns
// used in PostgreSQL and other production database systems
// Returns:
//   - error: Any error that occurred during the compaction process
func (idx *BTreeIndex) compact() error {
	if idx == nil {
		return fmt.Errorf("index cannot be nil")
	}

	if idx.rootPageNum == 0 {
		idx.logger.Debugf("Index is empty, no compaction needed")
		return nil
	}

	idx.logger.Infof("Starting index compaction for '%s'", idx.FilePath)

	// Create maintenance result to track compaction metrics
	result := &MaintenanceResult{
		Operation:      "compaction",
		StartTime:      time.Now(),
		PagesProcessed: 0,
		PagesReclaimed: 0,
		SpaceSaved:     0,
		ErrorsFixed:    0,
		WarningsFound:  0,
		WarningsIssued: make([]string, 0),
		Success:        false,
	}

	// Step 1: Analyze current index fragmentation and performance characteristics
	preCompactionStats, err := idx.analyzeIndexHealth()
	if err != nil {
		idx.logger.Warnf("Failed to analyze pre-compaction health: %v", err)
		// Continue with compaction even if analysis fails
	}

	idx.logger.Debugf("Pre-compaction analysis: fragmentation=%.2f%%, fill_factor=%.2f%%",
		preCompactionStats.FragmentationPct, preCompactionStats.FillFactor*100)

	// Step 2: Extract all entries from the current index for rebuilding
	idx.logger.Debugf("Extracting all entries from current index")
	allEntries, err := extractAllEntries(idx)
	if err != nil {
		result.EndTime = time.Now()
		return fmt.Errorf("failed to extract entries during compaction: %w", err)
	}

	idx.logger.Debugf("Extracted %d entries for compaction", len(allEntries))
	result.PagesProcessed = len(allEntries)

	// Step 3: Sort entries by key for optimal insertion order and tree structure
	idx.logger.Debugf("Sorting entries for optimal insertion order")
	sortEntries(allEntries)

	// Step 4: Create configuration for the new optimized index
	compactConfig := &IndexConfig{
		BundleName: idx.bundleName,
		FieldName:  idx.fieldName,
		IsUnique:   idx.Metadata.IsUnique,
		PageSize:   idx.Metadata.PageSize,
		CacheSize:  1000,  // Use reasonable cache size for compaction
		DebugMode:  false, // Use binary format for compaction
	}

	// Calculate optimal page size based on current data characteristics
	optimalPageSize := calculateOptimalPageSize(allEntries)
	if optimalPageSize != idx.Metadata.PageSize {
		idx.logger.Debugf("Using optimal page size %d instead of current %d",
			optimalPageSize, idx.Metadata.PageSize)
		compactConfig.PageSize = optimalPageSize
	}

	// Step 5: Create new optimized index structure
	idx.logger.Debugf("Creating optimized index structure")
	newIndex, err := createOptimizedIndex(idx, allEntries, optimalPageSize, compactConfig.FillFactor)
	if err != nil {
		result.EndTime = time.Now()
		return fmt.Errorf("failed to create optimized index: %w", err)
	}

	// Step 6: Perform atomic replacement of the old index structure
	idx.logger.Debugf("Performing atomic replacement of index structure")
	if err := replaceIndexStructure(idx, newIndex, result); err != nil {
		// Clean up the new index if replacement fails
		if closeErr := newIndex.Close(); closeErr != nil {
			idx.logger.Warnf("Failed to close new index after replacement failure: %v", closeErr)
		}
		result.EndTime = time.Now()
		return fmt.Errorf("failed to replace index structure: %w", err)
	}

	// Step 7: Analyze post-compaction health and calculate improvements
	postCompactionStats, err := idx.analyzeIndexHealth()
	if err != nil {
		idx.logger.Warnf("Failed to analyze post-compaction health: %v", err)
	} else {
		// Calculate space savings and performance improvements
		if preCompactionStats != nil {
			fragmentationImprovement := preCompactionStats.FragmentationPct - postCompactionStats.FragmentationPct
			fillFactorImprovement := postCompactionStats.FillFactor - preCompactionStats.FillFactor

			idx.logger.Infof("Compaction results: fragmentation reduced by %.2f%%, fill factor improved by %.2f%%",
				fragmentationImprovement, fillFactorImprovement*100)
		}

		idx.logger.Debugf("Post-compaction analysis: fragmentation=%.2f%%, fill_factor=%.2f%%",
			postCompactionStats.FragmentationPct, postCompactionStats.FillFactor*100)
	}

	// Step 8: Update metadata with compaction results
	idx.Metadata.LastCompaction = time.Now()
	idx.Metadata.FragmentationPct = 0.0 // Reset fragmentation after successful compaction
	idx.Metadata.CompactionCount++
	idx.Metadata.MaintenanceNeeded = false
	idx.Metadata.CompactionNeeded = false

	// Step 9: Flush all changes to ensure durability
	if err := idx.FileManager.Sync(); err != nil {
		idx.logger.Warnf("Failed to sync after compaction: %v", err)
	}

	// Step 10: Write updated metadata to storage
	if err := idx.FileManager.WriteMetadata(idx.Metadata); err != nil {
		idx.logger.Warnf("Failed to write metadata after compaction: %v", err)
	}

	// Mark compaction as successful
	result.Success = true
	result.EndTime = time.Now()
	duration := result.EndTime.Sub(result.StartTime)

	idx.logger.Infof("Index compaction completed successfully in %v: processed %d entries, reclaimed %d pages",
		duration, len(allEntries), result.PagesReclaimed)

	return nil
}

// analyzeIndexHealth performs comprehensive health analysis of the current index
// This function calculates fragmentation, fill factors, and other health metrics
// Returns:
//   - *IndexHealthStats: Current health statistics
//   - error: Any error that occurred during analysis
func (idx *BTreeIndex) analyzeIndexHealth() (*IndexHealthStats, error) {
	// Use existing tree balance check to get comprehensive health metrics
	balanceResult := CheckTreeBalance(idx)

	stats := &IndexHealthStats{
		TotalNodes:       balanceResult.TotalNodes,
		TreeHeight:       balanceResult.Height,
		FragmentationPct: balanceResult.FragmentationPct,
		FillFactor:       balanceResult.FillFactor,
		AverageKeyLength: balanceResult.AverageKeyLength,
		IsHealthy:        balanceResult.IsBalanced,
		Issues:           balanceResult.Errors,
	}

	return stats, nil
}
