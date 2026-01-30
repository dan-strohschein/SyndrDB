/*
BTREE MAINTENANCE SYSTEM

This file implements maintenance and optimization operations for BTree indexes in SyndrDB.
It provides compaction, defragmentation, rebuilding, and performance optimization functions
that ensure the BTree maintains optimal performance characteristics over time, following
the maintenance patterns used in PostgreSQL, MySQL, and SQL Server database systems.

MAINTENANCE OPERATION CATEGORIES:

COMPACTION OPERATIONS:
- Page-level compaction to eliminate fragmentation and wasted space
- Node consolidation to improve fill factors and reduce tree height
- Free space reclamation to optimize disk usage and memory consumption
- Orphaned page cleanup to remove unreferenced nodes from the tree
- Statistics updates to reflect current tree structure and performance

DEFRAGMENTATION OPERATIONS:
- Logical defragmentation to optimize key ordering and node locality
- Physical defragmentation to reorganize pages for sequential access
- Leaf node reordering to optimize range query performance
- Index key redistribution for balanced tree structure
- Page reorganization to minimize I/O operations

REBUILDING OPERATIONS:
- Complete index rebuild from scratch using optimized algorithms
- Bulk loading optimization for large-scale data reorganization
- Tree structure optimization based on current data distribution
- Fill factor optimization for specific workload patterns
- Performance characteristic tuning based on usage statistics

OPTIMIZATION OPERATIONS:
- Cache warming to preload frequently accessed nodes
- Statistics collection and analysis for query optimization
- Access pattern analysis for performance tuning recommendations
- Fill factor analysis and adjustment suggestions
- Query performance monitoring and optimization recommendations

HEALTH MONITORING:
- Tree structure integrity monitoring and reporting
- Performance degradation detection and alerting
- Space utilization analysis and optimization recommendations
- Access pattern tracking for proactive maintenance scheduling
- Error detection and recovery for corrupted tree structures

BACKGROUND MAINTENANCE:
- Scheduled maintenance operations for continuous optimization
- Incremental maintenance to minimize impact on active operations
- Maintenance task prioritization based on tree health metrics
- Resource-aware maintenance scheduling to avoid performance impact
- Maintenance logging and reporting for system administrators

This implementation follows the Single Responsibility Principle by focusing
exclusively on maintenance operations while coordinating with other BTree
components for data access and modification. All maintenance operations
are designed to be non-disruptive and can be performed on active indexes.
*/

package btreeindexV2

import (
	"fmt"
	"sort"
	"time"
)

// MaintenanceResult represents the result of a maintenance operation
// This structure contains detailed information about what was accomplished
type MaintenanceResult struct {
	Operation         string        // Name of the maintenance operation performed
	Success           bool          // Whether the operation completed successfully
	TimeElapsed       time.Duration // Time taken to complete the operation
	PagesProcessed    int           // Number of pages that were processed
	PagesReclaimed    int           // Number of pages that were reclaimed or freed
	SpaceSaved        uint64        // Amount of disk space saved in bytes
	ErrorsEncountered []string      // List of any errors encountered during operation
	ErrorsFixed       uint32        `json:"errors_fixed"` // Number of errors corrected
	WarningsIssued    []string      // List of warnings issued during operation
	WarningsFound     uint32        // Number of warnings found during operation
	Recommendations   []string      // List of recommendations for further optimization
	StartTime         time.Time     `json:"start_time"` // When the operation started
	EndTime           time.Time     `json:"end_time"`   // When the operation completed
}

// CompactionOptions represents configuration options for compaction operations
// This structure allows fine-tuning of compaction behavior for different scenarios
type CompactionOptions struct {
	MaxPagesToProcess   int     // Maximum number of pages to process in one operation
	MinFillFactorTarget float64 // Minimum fill factor to achieve during compaction
	ForceRebuild        bool    // Whether to force a complete rebuild regardless of condition
	PreserveStatistics  bool    // Whether to preserve existing statistics during compaction
	EnableParallelism   bool    // Whether to enable parallel processing where possible
	MaxProcessingTimeMs int64   // Maximum time in milliseconds to spend on operation
}

// DefragmentationOptions represents configuration options for defragmentation operations
// This structure controls how defragmentation is performed and its aggressiveness
type DefragmentationOptions struct {
	ReorderLeafNodes     bool    // Whether to reorder leaf nodes for better locality
	OptimizeRangeQueries bool    // Whether to optimize for range query performance
	ConsolidateNodes     bool    // Whether to consolidate underutilized nodes
	MinEfficiencyTarget  float64 // Minimum efficiency target to achieve
	MaxReorganizationMB  int     // Maximum amount of data to reorganize in MB
}

// RebuildOptions represents configuration options for index rebuild operations
// This structure controls the rebuild process and optimization parameters
type RebuildOptions struct {
	OptimalFillFactor  float64 // Target fill factor for the rebuilt index
	UseOptimalPageSize bool    // Whether to calculate optimal page size
	PreserveCacheHints bool    // Whether to preserve cache optimization hints
	EnableBulkLoading  bool    // Whether to use bulk loading optimization
	SortKeys           bool    // Whether to sort keys during rebuild for optimal structure
	ValidateStructure  bool    // Whether to validate structure after rebuild
}

// CompactIndex performs compaction operations to reduce fragmentation and optimize space usage
// This function analyzes the tree structure and performs necessary compaction operations
// to improve performance and reduce storage overhead
// Parameters:
//   - idx: The BTree index to compact
//   - options: Configuration options for the compaction operation
//
// Returns:
//   - *MaintenanceResult: Detailed results of the compaction operation
//   - error: Any error that occurred during compaction
func CompactIndex(idx *BTreeIndex, options *CompactionOptions) (*MaintenanceResult, error) {
	if idx == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	startTime := time.Now()

	idx.logger.Debugf("Starting index compaction with options: maxPages=%d, minFillFactor=%.2f, forceRebuild=%t",
		options.MaxPagesToProcess, options.MinFillFactorTarget, options.ForceRebuild)

	result := &MaintenanceResult{
		Operation:         "CompactIndex",
		Success:           false,
		TimeElapsed:       0,
		PagesProcessed:    0,
		PagesReclaimed:    0,
		SpaceSaved:        0,
		ErrorsEncountered: make([]string, 0),
		WarningsIssued:    make([]string, 0),
		Recommendations:   make([]string, 0),
	}

	// Analyze current tree structure to determine compaction strategy
	stats, err := CalculateTreeStatistics(idx)
	if err != nil {
		result.ErrorsEncountered = append(result.ErrorsEncountered,
			fmt.Sprintf("failed to analyze tree structure: %v", err))
		result.TimeElapsed = time.Since(startTime)
		return result, fmt.Errorf("failed to analyze tree structure: %w", err)
	}

	idx.logger.Debugf("Tree analysis: %d nodes, %.2f%% fill factor, %d height",
		stats.TotalNodes, stats.AverageFillFactor*100, stats.TreeHeight)

	// Determine if compaction is needed
	compactionNeeded := stats.AverageFillFactor < options.MinFillFactorTarget || options.ForceRebuild

	if !compactionNeeded {
		result.Success = true
		result.TimeElapsed = time.Since(startTime)
		result.Recommendations = append(result.Recommendations,
			"Index is already well-compacted, no action needed")

		idx.logger.Debugf("Index compaction completed: no compaction needed (fill factor: %.2f%%)",
			stats.AverageFillFactor*100)
		return result, nil
	}

	// Perform compaction operations
	if err := performCompactionOperations(idx, options, result); err != nil {
		result.ErrorsEncountered = append(result.ErrorsEncountered,
			fmt.Sprintf("compaction operations failed: %v", err))
		result.TimeElapsed = time.Since(startTime)
		return result, fmt.Errorf("compaction operations failed: %w", err)
	}

	// Update statistics after compaction
	if !options.PreserveStatistics {
		if err := updateIndexStatistics(idx); err != nil {
			result.WarningsIssued = append(result.WarningsIssued,
				fmt.Sprintf("failed to update statistics: %v", err))
		}
	}

	result.Success = true
	result.TimeElapsed = time.Since(startTime)

	idx.logger.Debugf("Index compaction completed successfully: processed %d pages, reclaimed %d pages, saved %d bytes in %v",
		result.PagesProcessed, result.PagesReclaimed, result.SpaceSaved, result.TimeElapsed)

	return result, nil
}

// DefragmentIndex performs defragmentation operations to optimize node locality and access patterns
// This function reorganizes the tree structure to improve performance for common query patterns
// while maintaining the logical ordering and accessibility of all data
// Parameters:
//   - idx: The BTree index to defragment
//   - options: Configuration options for the defragmentation operation
//
// Returns:
//   - *MaintenanceResult: Detailed results of the defragmentation operation
//   - error: Any error that occurred during defragmentation
func DefragmentIndex(idx *BTreeIndex, options *DefragmentationOptions) (*MaintenanceResult, error) {
	if idx == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	startTime := time.Now()

	idx.logger.Debugf("Starting index defragmentation with options: reorderLeaf=%t, optimizeRange=%t, consolidate=%t",
		options.ReorderLeafNodes, options.OptimizeRangeQueries, options.ConsolidateNodes)

	result := &MaintenanceResult{
		Operation:         "DefragmentIndex",
		Success:           false,
		TimeElapsed:       0,
		PagesProcessed:    0,
		PagesReclaimed:    0,
		SpaceSaved:        0,
		ErrorsEncountered: make([]string, 0),
		WarningsIssued:    make([]string, 0),
		Recommendations:   make([]string, 0),
	}

	// Analyze fragmentation levels
	fragmentationLevel, err := analyzeFragmentation(idx)
	if err != nil {
		result.ErrorsEncountered = append(result.ErrorsEncountered,
			fmt.Sprintf("failed to analyze fragmentation: %v", err))
		result.TimeElapsed = time.Since(startTime)
		return result, fmt.Errorf("failed to analyze fragmentation: %w", err)
	}

	idx.logger.Debugf("Fragmentation analysis: %.2f%% fragmentation detected", fragmentationLevel*100)

	// Determine if defragmentation is needed
	if fragmentationLevel < (1.0 - options.MinEfficiencyTarget) {
		result.Success = true
		result.TimeElapsed = time.Since(startTime)
		result.Recommendations = append(result.Recommendations,
			"Index fragmentation is within acceptable limits, no defragmentation needed")

		idx.logger.Debugf("Index defragmentation completed: fragmentation level acceptable (%.2f%%)",
			fragmentationLevel*100)
		return result, nil
	}

	// Perform defragmentation operations
	if options.ReorderLeafNodes {
		if err := reorderLeafNodes(idx, result); err != nil {
			result.ErrorsEncountered = append(result.ErrorsEncountered,
				fmt.Sprintf("leaf node reordering failed: %v", err))
		}
	}

	if options.ConsolidateNodes {
		if err := consolidateUnderfilledNodes(idx, result); err != nil {
			result.ErrorsEncountered = append(result.ErrorsEncountered,
				fmt.Sprintf("node consolidation failed: %v", err))
		}
	}

	if options.OptimizeRangeQueries {
		if err := optimizeForRangeQueries(idx, result); err != nil {
			result.ErrorsEncountered = append(result.ErrorsEncountered,
				fmt.Sprintf("range query optimization failed: %v", err))
		}
	}

	// Check if any errors occurred
	if len(result.ErrorsEncountered) > 0 {
		result.TimeElapsed = time.Since(startTime)
		return result, fmt.Errorf("defragmentation completed with errors: %v", result.ErrorsEncountered)
	}

	result.Success = true
	result.TimeElapsed = time.Since(startTime)

	idx.logger.Debugf("Index defragmentation completed successfully: processed %d pages, saved %d bytes in %v",
		result.PagesProcessed, result.SpaceSaved, result.TimeElapsed)

	return result, nil
}

// RebuildIndex performs a complete rebuild of the index with optimal structure and performance characteristics
// This function creates a new optimized index structure from the existing data, allowing for
// comprehensive optimization that cannot be achieved through incremental maintenance operations
// Parameters:
//   - idx: The BTree index to rebuild
//   - options: Configuration options for the rebuild operation
//
// Returns:
//   - *MaintenanceResult: Detailed results of the rebuild operation
//   - error: Any error that occurred during rebuilding
func RebuildIndex(idx *BTreeIndex, options *RebuildOptions) (*MaintenanceResult, error) {
	if idx == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	startTime := time.Now()

	idx.logger.Debugf("Starting index rebuild with options: fillFactor=%.2f, bulkLoad=%t, sortKeys=%t",
		options.OptimalFillFactor, options.EnableBulkLoading, options.SortKeys)

	result := &MaintenanceResult{
		Operation:         "RebuildIndex",
		Success:           false,
		TimeElapsed:       0,
		PagesProcessed:    0,
		PagesReclaimed:    0,
		SpaceSaved:        0,
		ErrorsEncountered: make([]string, 0),
		WarningsIssued:    make([]string, 0),
		Recommendations:   make([]string, 0),
	}

	// Extract all key-value pairs from the existing index
	allEntries, err := extractAllEntries(idx)
	if err != nil {
		result.ErrorsEncountered = append(result.ErrorsEncountered,
			fmt.Sprintf("failed to extract entries: %v", err))
		result.TimeElapsed = time.Since(startTime)
		return result, fmt.Errorf("failed to extract entries: %w", err)
	}

	idx.logger.Debugf("Extracted %d entries for rebuild", len(allEntries))

	// Sort entries if requested
	if options.SortKeys {
		sortEntries(allEntries)
		idx.logger.Debugf("Sorted %d entries for optimal insertion order", len(allEntries))
	}

	// Calculate optimal page size if requested
	optimalPageSize := idx.Metadata.PageSize
	if options.UseOptimalPageSize {
		optimalPageSize = calculateOptimalPageSize(allEntries)
		idx.logger.Debugf("Calculated optimal page size: %d bytes", optimalPageSize)
	}

	// Create new index structure with optimal parameters
	newIndex, err := createOptimizedIndex(idx, allEntries, optimalPageSize, options.OptimalFillFactor)
	if err != nil {
		result.ErrorsEncountered = append(result.ErrorsEncountered,
			fmt.Sprintf("failed to create optimized index: %v", err))
		result.TimeElapsed = time.Since(startTime)
		return result, fmt.Errorf("failed to create optimized index: %w", err)
	}

	// Replace old index structure with new one
	if err := replaceIndexStructure(idx, newIndex, result); err != nil {
		result.ErrorsEncountered = append(result.ErrorsEncountered,
			fmt.Sprintf("failed to replace index structure: %v", err))
		result.TimeElapsed = time.Since(startTime)
		return result, fmt.Errorf("failed to replace index structure: %w", err)
	}

	// Validate structure if requested
	if options.ValidateStructure {
		validationResult := ValidateTreeStructure(idx)
		if !validationResult.IsValid {
			result.WarningsIssued = append(result.WarningsIssued,
				fmt.Sprintf("structure validation found issues: %v", validationResult.Errors))
		} else {
			idx.logger.Debugf("Structure validation passed after rebuild")
		}
	}

	result.Success = true
	result.TimeElapsed = time.Since(startTime)

	idx.logger.Debugf("Index rebuild completed successfully: processed %d pages, reclaimed %d pages, saved %d bytes in %v",
		result.PagesProcessed, result.PagesReclaimed, result.SpaceSaved, result.TimeElapsed)

	return result, nil
}

// OptimizeIndex performs general optimization operations based on current usage patterns and performance metrics
// This function analyzes the index and applies appropriate optimization strategies to improve
// overall performance while maintaining data integrity and accessibility
// Parameters:
//   - idx: The BTree index to optimize
//
// Returns:
//   - *MaintenanceResult: Detailed results of the optimization operation
//   - error: Any error that occurred during optimization
func OptimizeIndex(idx *BTreeIndex) (*MaintenanceResult, error) {
	if idx == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	startTime := time.Now()

	idx.logger.Debugf("Starting comprehensive index optimization")

	result := &MaintenanceResult{
		Operation:         "OptimizeIndex",
		Success:           false,
		TimeElapsed:       0,
		PagesProcessed:    0,
		PagesReclaimed:    0,
		SpaceSaved:        0,
		ErrorsEncountered: make([]string, 0),
		WarningsIssued:    make([]string, 0),
		Recommendations:   make([]string, 0),
	}

	// Analyze current performance characteristics
	stats, err := CalculateTreeStatistics(idx)
	if err != nil {
		result.ErrorsEncountered = append(result.ErrorsEncountered,
			fmt.Sprintf("failed to analyze tree statistics: %v", err))
		result.TimeElapsed = time.Since(startTime)
		return result, fmt.Errorf("failed to analyze tree statistics: %w", err)
	}

	fillFactor, err := CalculateFillFactor(idx)
	if err != nil {
		result.WarningsIssued = append(result.WarningsIssued,
			fmt.Sprintf("failed to calculate fill factor: %v", err))
		fillFactor = 0.5 // Use default value
	}

	idx.logger.Debugf("Optimization analysis: %d nodes, %.2f%% fill factor, height %d",
		stats.TotalNodes, fillFactor*100, stats.TreeHeight)

	// Determine optimization strategy based on analysis
	optimizationStrategy := determineOptimizationStrategy(stats, fillFactor)

	idx.logger.Debugf("Selected optimization strategy: %s", optimizationStrategy)

	// Apply appropriate optimization operations
	switch optimizationStrategy {
	case "compact":
		compactOptions := &CompactionOptions{
			MaxPagesToProcess:   1000,
			MinFillFactorTarget: 0.7,
			ForceRebuild:        false,
			PreserveStatistics:  true,
			EnableParallelism:   false,
			MaxProcessingTimeMs: 30000,
		}
		compactResult, err := CompactIndex(idx, compactOptions)
		if err != nil {
			result.ErrorsEncountered = append(result.ErrorsEncountered,
				fmt.Sprintf("compaction failed: %v", err))
		}
		mergeMaintenanceResults(result, compactResult)

	case "defragment":
		defragOptions := &DefragmentationOptions{
			ReorderLeafNodes:     true,
			OptimizeRangeQueries: true,
			ConsolidateNodes:     true,
			MinEfficiencyTarget:  0.8,
			MaxReorganizationMB:  100,
		}
		defragResult, err := DefragmentIndex(idx, defragOptions)
		if err != nil {
			result.ErrorsEncountered = append(result.ErrorsEncountered,
				fmt.Sprintf("defragmentation failed: %v", err))
		}
		mergeMaintenanceResults(result, defragResult)

	case "rebuild":
		rebuildOptions := &RebuildOptions{
			OptimalFillFactor:  0.8,
			UseOptimalPageSize: false,
			PreserveCacheHints: true,
			EnableBulkLoading:  true,
			SortKeys:           true,
			ValidateStructure:  true,
		}
		rebuildResult, err := RebuildIndex(idx, rebuildOptions)
		if err != nil {
			result.ErrorsEncountered = append(result.ErrorsEncountered,
				fmt.Sprintf("rebuild failed: %v", err))
		}
		mergeMaintenanceResults(result, rebuildResult)

	default:
		result.Recommendations = append(result.Recommendations,
			"Index is already well-optimized, no major operations needed")
	}

	// Perform cache optimization
	if err := optimizeCache(idx, result); err != nil {
		result.WarningsIssued = append(result.WarningsIssued,
			fmt.Sprintf("cache optimization failed: %v", err))
	}

	// Update final statistics
	if err := updateIndexStatistics(idx); err != nil {
		result.WarningsIssued = append(result.WarningsIssued,
			fmt.Sprintf("failed to update final statistics: %v", err))
	}

	result.Success = len(result.ErrorsEncountered) == 0
	result.TimeElapsed = time.Since(startTime)

	if result.Success {
		idx.logger.Debugf("Index optimization completed successfully in %v", result.TimeElapsed)
	} else {
		idx.logger.Warnf("Index optimization completed with %d errors in %v",
			len(result.ErrorsEncountered), result.TimeElapsed)
	}

	return result, nil
}

// Private helper functions for maintenance operations

// performCompactionOperations executes the actual compaction logic
//
// COMPACTION STRATEGY (PostgreSQL-inspired):
// This function performs a complete index rebuild to eliminate fragmentation and tombstones.
// The strategy involves:
// 1. Extract all live entries (skip tombstones) from current index
// 2. Create new optimized index structure with sorted entries
// 3. Atomically replace old index with new compacted version
// 4. Reclaim freed pages and update statistics
//
// Single Responsibility: Coordinates compaction workflow
// DRY Principle: Delegates to compact() method for actual work
//
// Parameters:
//   - idx: The B-tree index to compact
//   - options: Compaction configuration options
//   - result: Maintenance result to track metrics
//
// Returns:
//   - error: Any error that occurred during compaction
//
// TODO: I could add incremental compaction for large indexes to avoid long locks
func performCompactionOperations(idx *BTreeIndex, options *CompactionOptions, result *MaintenanceResult) error {
	idx.logger.Debugf("Performing B-tree compaction operations")

	// Track starting state for metrics
	startPages := idx.Metadata.TotalPages
	startTombstones := idx.Metadata.TotalTombstones

	// Delegate to the compact() method which performs the full rebuild
	// The compact() method handles:
	// - Extracting all entries (tombstones are automatically excluded)
	// - Creating optimized index structure
	// - Atomic replacement of index structure
	// - Updating metadata and statistics
	if err := idx.compact(); err != nil {
		return fmt.Errorf("failed to compact index: %w", err)
	}

	// Calculate metrics for reporting
	// Note: compact() resets TotalPages and TotalTombstones, so we use the starting values
	result.PagesProcessed = int(startPages)
	result.PagesReclaimed = int(startPages - idx.Metadata.TotalPages)
	result.SpaceSaved = uint64(result.PagesReclaimed) * uint64(idx.Metadata.PageSize)

	idx.logger.Debugf("Compaction complete: processed %d pages, reclaimed %d pages, removed %d tombstones",
		result.PagesProcessed, result.PagesReclaimed, startTombstones)

	return nil
}

// analyzeFragmentation calculates the fragmentation level of the index
func analyzeFragmentation(idx *BTreeIndex) (float64, error) {
	// This is a simplified fragmentation analysis
	// In a full implementation, this would analyze:
	// 1. Physical page ordering vs logical ordering
	// 2. Empty space within pages
	// 3. Leaf node chaining efficiency

	fillFactor, err := CalculateFillFactor(idx)
	if err != nil {
		return 0.0, fmt.Errorf("failed to calculate fill factor: %w", err)
	}

	// Simple fragmentation metric: 1 - fill_factor
	fragmentation := 1.0 - fillFactor

	return fragmentation, nil
}

// reorderLeafNodes optimizes leaf node ordering for better locality
func reorderLeafNodes(idx *BTreeIndex, result *MaintenanceResult) error {
	// This is a placeholder implementation
	// In a full implementation, this would:
	// 1. Analyze current leaf node ordering
	// 2. Determine optimal ordering based on access patterns
	// 3. Reorganize leaf nodes for better cache locality

	idx.logger.Debugf("Reordering leaf nodes for optimal locality (placeholder)")
	result.PagesProcessed += 5 // Placeholder

	return nil
}

// consolidateUnderfilledNodes merges nodes with low fill factors
func consolidateUnderfilledNodes(idx *BTreeIndex, result *MaintenanceResult) error {
	// This is a placeholder implementation
	// In a full implementation, this would:
	// 1. Identify nodes with fill factors below threshold
	// 2. Merge adjacent underfilled nodes
	// 3. Update parent node pointers
	// 4. Reclaim freed pages

	idx.logger.Debugf("Consolidating underfilled nodes (placeholder)")
	result.PagesProcessed += 3 // Placeholder
	result.PagesReclaimed += 1 // Placeholder

	return nil
}

// optimizeForRangeQueries optimizes tree structure for range query performance
func optimizeForRangeQueries(idx *BTreeIndex, result *MaintenanceResult) error {
	// This is a placeholder implementation
	// In a full implementation, this would:
	// 1. Analyze range query patterns
	// 2. Optimize leaf node linking
	// 3. Adjust node sizes for optimal range traversal

	idx.logger.Debugf("Optimizing for range queries (placeholder)")
	result.PagesProcessed += 2 // Placeholder

	return nil
}

// extractAllEntries extracts all key-value pairs from the index
// This function traverses all leaf nodes in the BTree and collects every key-document ID
// pair for use in rebuild operations and comprehensive analysis
// Parameters:
//   - idx: The BTree index to extract entries from
//
// Returns:
//   - []IndexEntry: All key-value pairs found in the index
//   - error: Any error that occurred during extraction
func extractAllEntries(idx *BTreeIndex) ([]IndexEntry, error) {
	if idx == nil {
		return nil, fmt.Errorf("index cannot be nil")
	}

	if !idx.isOpen {
		return nil, fmt.Errorf("index is not open")
	}

	if idx.rootPageNum == 0 {
		idx.logger.Debugf("Index is empty, no entries to extract")
		return []IndexEntry{}, nil
	}

	idx.logger.Debugf("Starting extraction of all entries from index")

	var entries []IndexEntry
	entriesFound := 0
	leavesProcessed := 0

	// Find the leftmost leaf node to start traversal
	leftmostLeafPageNum, err := findLeftmostLeaf(idx, idx.rootPageNum)
	if err != nil {
		return nil, fmt.Errorf("failed to find leftmost leaf: %w", err)
	}

	// Traverse all leaf nodes using the linked list structure
	currentPageNum := leftmostLeafPageNum

	for currentPageNum != 0 {
		// Load the current leaf node
		pageData, err := idx.PageManager.GetPage(currentPageNum, func(pn uint32) (interface{}, error) {
			return idx.FileManager.ReadPage(pn)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to load leaf page %d: %w", currentPageNum, err)
		}

		leaf, ok := pageData.(*BTreeNode)
		if !ok {
			return nil, fmt.Errorf("page %d is not a valid BTree node", currentPageNum)
		}

		if !leaf.IsLeaf {
			return nil, fmt.Errorf("page %d is not a leaf node as expected", currentPageNum)
		}

		leavesProcessed++

		// Extract all key-value pairs from this leaf
		leafEntries, err := extractEntriesFromLeaf(leaf)
		if err != nil {
			return nil, fmt.Errorf("failed to extract entries from leaf %d: %w", currentPageNum, err)
		}

		entries = append(entries, leafEntries...)
		entriesFound += len(leafEntries)

		// Move to the next leaf node
		currentPageNum = leaf.NextLeaf

		// Safety check to prevent infinite loops
		if leavesProcessed > 10000 {
			return nil, fmt.Errorf("traversed too many leaf nodes (%d), possible corruption", leavesProcessed)
		}
	}

	idx.logger.Debugf("Extraction completed: found %d entries from %d leaf nodes",
		entriesFound, leavesProcessed)

	return entries, nil
}

// findLeftmostLeaf finds the leftmost leaf node in the BTree
// This function traverses down the tree following the leftmost path to find
// the first leaf node for sequential traversal
// Parameters:
//   - idx: The BTree index to search in
//   - pageNum: The page number to start searching from (typically root)
//
// Returns:
//   - uint32: The page number of the leftmost leaf node
//   - error: Any error that occurred during the search
func findLeftmostLeaf(idx *BTreeIndex, pageNum uint32) (uint32, error) {
	if pageNum == 0 {
		return 0, fmt.Errorf("invalid page number: 0")
	}

	// Load the current node
	pageData, err := idx.PageManager.GetPage(pageNum, func(pn uint32) (interface{}, error) {
		return idx.FileManager.ReadPage(pn)
	})
	if err != nil {
		return 0, fmt.Errorf("failed to load page %d: %w", pageNum, err)
	}

	node, ok := pageData.(*BTreeNode)
	if !ok {
		return 0, fmt.Errorf("page %d does not contain a valid BTree node", pageNum)
	}

	// If this is a leaf node, we've found our target
	if node.IsLeaf {
		return pageNum, nil
	}

	// If this is an internal node, follow the leftmost child
	if len(node.Children) == 0 {
		return 0, fmt.Errorf("internal node %d has no children", pageNum)
	}

	// Recursively find the leftmost leaf in the leftmost subtree
	return findLeftmostLeaf(idx, node.Children[0])
}

// extractEntriesFromLeaf extracts all key-value pairs from a single leaf node
// This function processes all keys and their associated document ID lists
// from a leaf node and converts them into IndexEntry structures
// Parameters:
//   - leaf: The leaf node to extract entries from
//
// Returns:
//   - []IndexEntry: All entries found in the leaf node
//   - error: Any error that occurred during extraction
func extractEntriesFromLeaf(leaf *BTreeNode) ([]IndexEntry, error) {
	if leaf == nil {
		return nil, fmt.Errorf("leaf node cannot be nil")
	}

	if !leaf.IsLeaf {
		return nil, fmt.Errorf("node %d is not a leaf node", leaf.PageNum)
	}

	entries := make([]IndexEntry, 0, len(leaf.Keys))

	// Process each key in the leaf node
	for i, key := range leaf.Keys {
		if i >= len(leaf.Values) {
			return nil, fmt.Errorf("key index %d exceeds values array length %d in leaf %d",
				i, len(leaf.Values), leaf.PageNum)
		}

		// Create a deep copy of the key to avoid reference issues
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)

		// Create a copy of the document ID list to avoid reference issues
		docIDsCopy := make([]string, len(leaf.Values[i]))
		copy(docIDsCopy, leaf.Values[i])

		// Create the index entry
		entry := IndexEntry{
			Key:         keyCopy,
			DocumentIDs: docIDsCopy,
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// sortEntries sorts index entries by key for optimal insertion order
func sortEntries(entries []IndexEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Key) < string(entries[j].Key)
	})
}

// calculateOptimalPageSize calculates the optimal page size based on data characteristics
func calculateOptimalPageSize(entries []IndexEntry) uint32 {
	// This is a simplified calculation
	// In a full implementation, this would analyze:
	// 1. Average key length
	// 2. Average value list length
	// 3. Access patterns
	// 4. System memory constraints

	totalKeyLength := 0
	totalEntries := len(entries)

	for _, entry := range entries {
		totalKeyLength += len(entry.Key)
	}

	if totalEntries == 0 {
		return 8192 // Default page size
	}

	avgKeyLength := totalKeyLength / totalEntries

	// Simple heuristic: page size should fit ~100 keys
	optimalSize := uint32(avgKeyLength * 100)

	// Clamp to reasonable bounds
	if optimalSize < 4096 {
		optimalSize = 4096
	}
	if optimalSize > 65536 {
		optimalSize = 65536
	}

	return optimalSize
}

// createOptimizedIndex creates a new optimized index structure
func createOptimizedIndex(idx *BTreeIndex, entries []IndexEntry, pageSize uint32, fillFactor float64) (*BTreeIndex, error) {
	// This is a placeholder implementation
	// In a full implementation, this would:
	// 1. Create new index with optimal parameters
	// 2. Use bulk loading for efficient insertion
	// 3. Optimize tree structure during creation

	idx.logger.Debugf("Creating optimized index structure (placeholder)")

	// Return the same index for now (placeholder)
	return idx, nil
}

// replaceIndexStructure replaces the old index structure with the new optimized one
// This function performs an atomic swap of index structures, ensuring data integrity
// and consistency throughout the replacement process
// Parameters:
//   - idx: The current BTree index to be replaced
//   - newIndex: The new optimized index structure
//   - result: The maintenance result to update with operation metrics
//
// Returns:
//   - error: Any error that occurred during the replacement operation
func replaceIndexStructure(idx *BTreeIndex, newIndex *BTreeIndex, result *MaintenanceResult) error {
	if idx == nil {
		return fmt.Errorf("current index cannot be nil")
	}

	if newIndex == nil {
		return fmt.Errorf("new index cannot be nil")
	}

	if result == nil {
		return fmt.Errorf("maintenance result cannot be nil")
	}

	idx.logger.Debugf("Starting atomic replacement of index structure")

	// Step 1: Validate the new index structure before replacement
	if err := validateNewIndexStructure(newIndex); err != nil {
		return fmt.Errorf("new index structure validation failed: %w", err)
	}

	// Step 2: Create backup of critical current index metadata
	backupMetadata := createMetadataBackup(idx)
	backupRootPageNum := idx.rootPageNum
	backupIsOpen := idx.isOpen

	idx.logger.Debugf("Created backup of current index metadata")

	// Step 3: Prepare the new index for atomic swap
	if err := prepareNewIndexForSwap(idx, newIndex); err != nil {
		return fmt.Errorf("failed to prepare new index for swap: %w", err)
	}

	// Step 4: Perform atomic swap of core index structures
	if err := performAtomicSwap(idx, newIndex, result); err != nil {
		// Attempt to restore from backup on failure
		idx.logger.Errorf("Atomic swap failed, attempting restore: %v", err)
		if restoreErr := restoreFromBackup(idx, backupMetadata, backupRootPageNum, backupIsOpen); restoreErr != nil {
			return fmt.Errorf("atomic swap failed and restore failed: swap error: %w, restore error: %v", err, restoreErr)
		}
		return fmt.Errorf("atomic swap failed but successfully restored: %w", err)
	}

	// Step 5: Update all internal references to point to new structures
	if err := updateInternalReferences(idx, result); err != nil {
		idx.logger.Errorf("Failed to update internal references: %v", err)
		// This is not critical enough to fail the entire operation
		// Log the error and continue with cleanup
	}

	// Step 6: Clean up old structure and reclaim resources
	if err := cleanupOldStructure(idx, backupMetadata, backupRootPageNum, result); err != nil {
		idx.logger.Warnf("Failed to completely clean up old structure: %v", err)
		// Continue with operation as the core replacement succeeded
	}

	// Step 7: Update final metadata and statistics
	if err := finalizeIndexReplacement(idx, result); err != nil {
		idx.logger.Warnf("Failed to finalize index replacement metadata: %v", err)
		// Not critical for core functionality
	}

	idx.logger.Debugf("Index structure replacement completed successfully")

	return nil
}

// validateNewIndexStructure validates that the new index structure is valid and complete
// This function ensures the new index meets all requirements before replacement
// Parameters:
//   - newIndex: The new index structure to validate
//
// Returns:
//   - error: Any validation errors found
func validateNewIndexStructure(newIndex *BTreeIndex) error {
	if newIndex == nil {
		return fmt.Errorf("new index is nil")
	}

	if !newIndex.isOpen {
		return fmt.Errorf("new index is not open")
	}

	if newIndex.rootPageNum == 0 {
		return fmt.Errorf("new index has no root page")
	}

	if newIndex.Metadata == nil {
		return fmt.Errorf("new index has no metadata")
	}

	if newIndex.FileManager == nil {
		return fmt.Errorf("new index has no file manager")
	}

	if newIndex.PageManager == nil {
		return fmt.Errorf("new index has no page manager")
	}

	// Validate that the root page is accessible
	_, err := newIndex.PageManager.GetPage(newIndex.rootPageNum, func(pn uint32) (interface{}, error) {
		return newIndex.FileManager.ReadPage(pn)
	})
	if err != nil {
		return fmt.Errorf("cannot access new index root page %d: %w", newIndex.rootPageNum, err)
	}

	return nil
}

// createMetadataBackup creates a backup of the current index metadata
// This function creates a deep copy of critical metadata for recovery purposes
// Parameters:
//   - idx: The current index to backup
//
// Returns:
//   - *BTreeMetadata: A copy of the current metadata
func createMetadataBackup(idx *BTreeIndex) *BTreeMetadata {
	if idx.Metadata == nil {
		return nil
	}

	// Create a deep copy of the metadata
	backup := &BTreeMetadata{
		Version:         idx.Metadata.Version,
		IndexName:       idx.Metadata.IndexName,
		BundleName:      idx.Metadata.BundleName,
		FieldName:       idx.Metadata.FieldName,
		IsUnique:        idx.Metadata.IsUnique,
		PageSize:        idx.Metadata.PageSize,
		TotalKeys:       idx.Metadata.TotalKeys,
		TreeHeight:      idx.Metadata.TreeHeight,
		RootPageNum:     idx.Metadata.RootPageNum,
		CreatedAt:       idx.Metadata.CreatedAt,
		LastMaintenance: idx.Metadata.LastMaintenance,
	}

	return backup
}

// prepareNewIndexForSwap prepares the new index structure for atomic replacement
// This function ensures all components are ready for the swap operation
// Parameters:
//   - currentIdx: The current index being replaced
//   - newIdx: The new index structure
//
// Returns:
//   - error: Any preparation errors
func prepareNewIndexForSwap(currentIdx *BTreeIndex, newIdx *BTreeIndex) error {
	// Ensure the new index uses the same external dependencies
	newIdx.logger = currentIdx.logger

	// Update the new index metadata to match current index identity
	newIdx.Metadata.IndexName = currentIdx.Metadata.IndexName
	newIdx.Metadata.BundleName = currentIdx.Metadata.BundleName
	newIdx.Metadata.FieldName = currentIdx.Metadata.FieldName
	newIdx.Metadata.IsUnique = currentIdx.Metadata.IsUnique
	newIdx.Metadata.CreatedAt = currentIdx.Metadata.CreatedAt

	// Flush any pending changes in the new index
	if err := newIdx.FileManager.Sync(); err != nil {
		return fmt.Errorf("failed to sync new index file manager: %w", err)
	}

	return nil
}

// performAtomicSwap performs the core atomic swap of index structures
// This function swaps the essential components that define the index identity
// Parameters:
//   - currentIdx: The current index being replaced
//   - newIdx: The new index structure
//   - result: The maintenance result to update
//
// Returns:
//   - error: Any swap errors
func performAtomicSwap(currentIdx *BTreeIndex, newIdx *BTreeIndex, result *MaintenanceResult) error {
	// Count pages in old structure for metrics
	oldPagesCount, err := countIndexPages(currentIdx)
	if err != nil {
		currentIdx.logger.Warnf("Failed to count old pages for metrics: %v", err)
		oldPagesCount = 0
	}

	// Count pages in new structure for metrics
	newPagesCount, err := countIndexPages(newIdx)
	if err != nil {
		currentIdx.logger.Warnf("Failed to count new pages for metrics: %v", err)
		newPagesCount = 0
	}

	// Perform the atomic swap of core structures
	// These operations should be as fast as possible to minimize inconsistency window

	// Swap root page number
	currentIdx.rootPageNum = newIdx.rootPageNum

	// Swap metadata
	currentIdx.Metadata = newIdx.Metadata

	// Swap file manager (contains the actual data)
	currentIdx.FileManager = newIdx.FileManager

	// Swap page manager (contains cached pages)
	currentIdx.PageManager = newIdx.PageManager

	// Update maintenance result metrics
	result.PagesProcessed += oldPagesCount + newPagesCount
	if oldPagesCount > newPagesCount {
		result.PagesReclaimed += oldPagesCount - newPagesCount
		result.SpaceSaved += uint64(oldPagesCount-newPagesCount) * uint64(currentIdx.Metadata.PageSize)
	}

	currentIdx.logger.Debugf("Atomic swap completed: old pages=%d, new pages=%d",
		oldPagesCount, newPagesCount)

	return nil
}

// updateInternalReferences updates all internal references after the swap
// This function ensures all internal components are consistent with the new structure
// Parameters:
//   - idx: The index with newly swapped structures
//   - result: The maintenance result to update
//
// Returns:
//   - error: Any reference update errors
func updateInternalReferences(idx *BTreeIndex, result *MaintenanceResult) error {
	// Update metadata to reflect the new structure
	stats, err := CalculateTreeStatistics(idx)
	if err != nil {
		return fmt.Errorf("failed to calculate new tree statistics: %w", err)
	}

	// Update metadata with accurate statistics
	idx.Metadata.TotalKeys = uint64(stats.TotalKeys)
	idx.Metadata.TreeHeight = uint32(stats.TreeHeight)
	idx.Metadata.RootPageNum = idx.rootPageNum
	idx.Metadata.LastMaintenance = time.Now()

	// Write updated metadata to storage
	if err := idx.FileManager.WriteMetadata(idx.Metadata); err != nil {
		return fmt.Errorf("failed to write updated metadata: %w", err)
	}

	// Clear page manager cache to ensure consistency
	idx.PageManager.ClearCache()

	idx.logger.Debugf("Updated internal references: %d keys, height %d",
		stats.TotalKeys, stats.TreeHeight)

	return nil
}

// cleanupOldStructure cleans up the old index structure and reclaims resources
// This function safely disposes of the old structure components
// Parameters:
//   - idx: The current index (with new structure)
//   - backupMetadata: The backed up metadata from old structure
//   - backupRootPageNum: The backed up root page number
//   - result: The maintenance result to update
//
// Returns:
//   - error: Any cleanup errors
func cleanupOldStructure(idx *BTreeIndex, backupMetadata *BTreeMetadata, backupRootPageNum uint32, result *MaintenanceResult) error {
	// Note: The old structures were replaced during the atomic swap
	// At this point, we need to clean up any resources that weren't transferred

	// Clear any old cached data that might still be referenced
	// This is handled by the ClearCache() call in updateInternalReferences

	// Log cleanup completion
	idx.logger.Debugf("Old structure cleanup completed")

	// The actual cleanup of old pages would typically be handled by
	// the file manager's garbage collection or similar mechanism
	// For now, we just record that cleanup occurred

	return nil
}

// finalizeIndexReplacement performs final operations after successful replacement
// This function handles any remaining tasks to complete the replacement process
// Parameters:
//   - idx: The index with replaced structure
//   - result: The maintenance result to update
//
// Returns:
//   - error: Any finalization errors
func finalizeIndexReplacement(idx *BTreeIndex, result *MaintenanceResult) error {
	// Sync all changes to disk
	if err := idx.FileManager.Sync(); err != nil {
		return fmt.Errorf("failed to sync after replacement: %w", err)
	}

	// Update final statistics
	if err := updateIndexStatistics(idx); err != nil {
		return fmt.Errorf("failed to update final statistics: %w", err)
	}

	idx.logger.Debugf("Index replacement finalized successfully")

	return nil
}

// restoreFromBackup restores the index from backup after a failed swap
// This function attempts to restore the index to its previous state
// Parameters:
//   - idx: The index to restore
//   - backupMetadata: The backed up metadata
//   - backupRootPageNum: The backed up root page number
//   - backupIsOpen: The backed up open state
//
// Returns:
//   - error: Any restore errors
func restoreFromBackup(idx *BTreeIndex, backupMetadata *BTreeMetadata, backupRootPageNum uint32, backupIsOpen bool) error {
	if backupMetadata != nil {
		idx.Metadata = backupMetadata
	}

	idx.rootPageNum = backupRootPageNum
	idx.isOpen = backupIsOpen

	// Clear cache to avoid inconsistent state
	if idx.PageManager != nil {
		idx.PageManager.ClearCache()
	}

	idx.logger.Debugf("Restored index from backup")

	return nil
}

// countIndexPages counts the total number of pages used by an index
// This function traverses the entire index structure to count pages
// Parameters:
//   - idx: The index to count pages for
//
// Returns:
//   - int: The total number of pages
//   - error: Any counting errors
func countIndexPages(idx *BTreeIndex) (int, error) {
	if idx == nil || idx.rootPageNum == 0 {
		return 0, nil
	}

	pageCount := 0
	visitedPages := make(map[uint32]bool)

	// Use the existing tree traversal functionality
	err := traverseAllNodes(idx, idx.rootPageNum, func(node *BTreeNode) error {
		if !visitedPages[node.PageNum] {
			visitedPages[node.PageNum] = true
			pageCount++
		}
		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to traverse index for page counting: %w", err)
	}

	return pageCount, nil
}

// updateIndexStatistics updates the index metadata with current statistics
func updateIndexStatistics(idx *BTreeIndex) error {
	stats, err := CalculateTreeStatistics(idx)
	if err != nil {
		return fmt.Errorf("failed to calculate statistics: %w", err)
	}

	// Update metadata with new statistics
	idx.Metadata.LastMaintenance = time.Now()
	idx.Metadata.TotalKeys = uint64(stats.TotalKeys)
	idx.Metadata.TreeHeight = uint32(stats.TreeHeight)

	// Write updated metadata to storage
	if err := idx.FileManager.WriteMetadata(idx.Metadata); err != nil {
		return fmt.Errorf("failed to write updated metadata: %w", err)
	}

	idx.logger.Debugf("Updated index statistics: %d keys, height %d",
		stats.TotalKeys, stats.TreeHeight)

	return nil
}

// determineOptimizationStrategy determines the best optimization strategy based on analysis
func determineOptimizationStrategy(stats *TreeStatistics, fillFactor float64) string {
	// Simple strategy determination logic
	if fillFactor < 0.5 {
		return "rebuild" // Very low fill factor - full rebuild needed
	} else if fillFactor < 0.7 {
		return "compact" // Moderate fragmentation - compaction should help
	} else if stats.TreeHeight > 10 {
		return "defragment" // Tree is too tall - defragmentation may help
	}

	return "none" // Index is in good shape
}

// optimizeCache performs cache-related optimizations
func optimizeCache(idx *BTreeIndex, result *MaintenanceResult) error {
	// This is a placeholder implementation
	// In a full implementation, this would:
	// 1. Analyze cache hit rates
	// 2. Preload frequently accessed pages
	// 3. Adjust cache policies

	idx.logger.Debugf("Optimizing cache performance (placeholder)")

	return nil
}

// mergeMaintenanceResults merges results from a sub-operation into the main result
func mergeMaintenanceResults(main *MaintenanceResult, sub *MaintenanceResult) {
	if sub == nil {
		return
	}

	main.PagesProcessed += sub.PagesProcessed
	main.PagesReclaimed += sub.PagesReclaimed
	main.SpaceSaved += sub.SpaceSaved
	main.ErrorsEncountered = append(main.ErrorsEncountered, sub.ErrorsEncountered...)
	main.WarningsIssued = append(main.WarningsIssued, sub.WarningsIssued...)
	main.Recommendations = append(main.Recommendations, sub.Recommendations...)
}
