/*
SORT EXECUTION NODE - PHASE 2

This file implements the SortNode execution node for the unified query system.
It provides ORDER BY functionality by wrapping the existing DocumentSorter component.

ARCHITECTURE:
The SortNode follows the Adapter pattern, delegating all sorting logic to the
well-tested DocumentSorter component while implementing the ExecutionNode interface.

DESIGN PRINCIPLES:
- Single Responsibility: Only responsible for coordinating sort execution in the query plan
- Open/Closed: Extends ExecutionNode without modifying existing code
- Dependency Inversion: Depends on ExecutionNode abstraction and DocumentSorter

EXECUTION MODEL:
1. Pull documents from child node
2. Convert document map to sortable slice
3. Delegate to DocumentSorter for actual sorting
4. Return sorted documents as map (preserving compatibility)

PERFORMANCE:
- Time Complexity: O(n log n) where n is number of documents
- Space Complexity: O(n) for temporary slice during sorting
- Uses Go's stable sort algorithm for consistent results

COST ESTIMATION:
Cost = ChildCost + (n * log(n) * 0.001)
Where n is estimated number of rows from child node

This node is part of Phase 2 of the unified query system implementation.
*/

package planner

import (
	"context"
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner/sorting"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// SortNode implements ORDER BY clause execution
// PHASE 2: Execution Nodes - Sort Operation
type SortNode struct {
	// Child node providing input documents
	Child ExecutionNode

	// OrderBy clause specifying sort fields and directions
	OrderBy *queryparser.OrderByClause

	// Cost is the estimated execution cost
	Cost float64

	// EstimatedRows is the expected number of output rows
	EstimatedRows int

	// Logger for debugging and monitoring
	Logger *zap.SugaredLogger

	// sorter delegates to existing DocumentSorter implementation (fallback)
	sorter *queryparser.DocumentSorter

	// sortedDocuments stores the sorted document slice to preserve order for downstream nodes
	sortedDocuments []*models.Document

	// config holds all sorting optimization parameters (PHASE 4: Configuration System)
	config *sorting.SortingConfig
}

// NewSortNode creates a new sort execution node
// PHASE 2: Factory function for SortNode creation
//
// Parameters:
//   - child: ExecutionNode providing input documents
//   - orderBy: ORDER BY clause specification
//   - logger: Logger for debugging
//
// Returns:
//   - *SortNode: Configured sort execution node
func NewSortNode(child ExecutionNode, orderBy *queryparser.OrderByClause, logger *zap.SugaredLogger) *SortNode {
	if orderBy == nil {
		logger.Warn("Creating SortNode with nil OrderBy clause")
	}

	// Create document sorter instance (reuses existing component as fallback)
	sorter := queryparser.NewDocumentSorter(orderBy, logger)

	// Load sorting configuration from CLI flags
	// Single Responsibility: Config loading delegated to settings package
	args := settings.GetSettings()
	config := &sorting.SortingConfig{
		TopNThreshold:       args.SortTopNThreshold,
		TopNMinSize:         args.SortTopNMinSize,
		RadixMinSize:        args.SortRadixMinSize,
		RadixLimitRatio:     args.SortRadixLimitRatio,
		SIMDEnabled:         args.SortSIMDEnabled,
		SIMDAbbrevBytes:     args.SortSIMDAbbrevBytes,
		SIMDMinSize:         args.SortSIMDMinSize,
		HeapInitialCapacity: args.SortHeapInitialCapacity,
		RadixMaxPasses:      args.SortRadixMaxPasses,
		EnableParallelSort:  args.SortEnableParallel,
		ParallelThreshold:   args.SortParallelThreshold,
		ParallelEnabled:     args.SortParallelEnabled, // Phase 5 parallel support
		ParallelMinSize:     args.SortParallelMinSize, // Phase 5 parallel support
		MaxSortMemoryMB:     args.SortMaxMemoryMB,
	}

	// Validate configuration (Open/Closed Principle: Validation in config itself)
	if err := config.Validate(); err != nil {
		logger.Warnf("Invalid sorting configuration, using defaults: %v", err)
		config = sorting.DefaultSortingConfig()
	}

	node := &SortNode{
		Child:         child,
		OrderBy:       orderBy,
		Logger:        logger,
		EstimatedRows: child.GetEstimatedRows(),
		sorter:        sorter,
		config:        config,
	}

	// Calculate cost: child cost + sorting cost
	// Sorting cost = n * log(n) * 0.001 (small constant for comparison operations)
	n := float64(child.GetEstimatedRows())
	sortCost := 0.0
	if n > 0 {
		sortCost = n * logBase2(n) * 0.001
	}
	node.Cost = child.GetCost() + sortCost

	logger.Debugf("Created SortNode: EstimatedRows=%d, Cost=%.4f (child=%.4f, sort=%.4f)",
		node.EstimatedRows, node.Cost, child.GetCost(), sortCost)

	return node
}

// Execute performs the sort operation
// PHASE 2: Main execution method for SortNode
//
// Execution flow:
//  1. Execute child node to get input documents
//  2. Choose optimal sorting strategy:
//     a. Top-N Heapsort if LIMIT present and small
//     b. Full sort otherwise
//  3. Return sorted documents
//
// Returns:
//   - map[string]*models.Document: Sorted documents
//   - error: Any error during execution
func (n *SortNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	n.Logger.Infof("Executing SortNode with %d ORDER BY fields", len(n.OrderBy.Fields))

	// Execute child node to get input documents
	documents, err := n.Child.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("SortNode: child execution failed: %w", err)
	}

	n.Logger.Debugf("SortNode received %d documents from child", len(documents))

	// Handle empty result set
	if len(documents) == 0 {
		n.Logger.Debug("SortNode: no documents to sort, returning empty result")
		return documents, nil
	}

	// Handle nil or empty ORDER BY clause (pass-through)
	if n.OrderBy == nil || len(n.OrderBy.Fields) == 0 {
		n.Logger.Debug("SortNode: no ORDER BY fields specified, returning unsorted documents")
		return documents, nil
	}

	// Memory tracking: Sample documents before sorting (sorting can be memory-intensive)
	// We sample here to catch large result sets before allocating sort buffers
	memoryTracker := GetMemoryTrackerFromContext(ctx)
	if memoryTracker != nil {
		docCount := 0
		for _, doc := range documents {
			docCount++

			// Sample every 100th document
			if docCount%100 == 0 {
				docSize := models.EstimateDocumentSize(doc)
				memoryTracker.Sample(docSize, docCount)

				if memoryTracker.WillExceedLimit(len(documents)) {
					return nil, ErrMemoryLimitExceeded
				}
			}
		}
	}

	// Execute the sort
	var sortedDocs []*models.Document
	sortedDocs, err = n.sorter.SortDocumentMap(documents)
	if err != nil {
		return nil, fmt.Errorf("SortNode: sorting failed: %w", err)
	}

	// Store sorted documents for downstream nodes (like LimitNode) to preserve order
	n.sortedDocuments = sortedDocs

	// Convert sorted slice back to map (preserves ExecutionNode interface compatibility)
	sortedMap := make(map[string]*models.Document, len(sortedDocs))
	for _, doc := range sortedDocs {
		sortedMap[doc.DocumentID] = doc
	}

	n.Logger.Infof("SortNode completed: sorted %d documents by %d fields",
		len(sortedMap), len(n.OrderBy.Fields))

	return sortedMap, nil
}

// ExecuteWithLimit performs Top-N heapsort when LIMIT is known
// This is called by LimitNode to enable the Top-N optimization
//
// Parameters:
//   - ctx: Context for cancellation support
//   - limit: The LIMIT value from downstream LimitNode
//
// Returns:
//   - []*models.Document: Top N documents in sorted order
//   - error: Any error during execution
func (n *SortNode) ExecuteWithLimit(ctx context.Context, limit int) ([]*models.Document, error) {
	n.Logger.Infof("Executing SortNode with LIMIT %d optimization", limit)

	// Execute child node to get input documents
	documents, err := n.Child.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("SortNode: child execution failed: %w", err)
	}

	n.Logger.Debugf("SortNode received %d documents from child", len(documents))

	// Handle empty result set
	if len(documents) == 0 {
		n.Logger.Debug("SortNode: empty input, returning empty result")
		return []*models.Document{}, nil
	}

	// Handle nil or empty ORDER BY clause
	if n.OrderBy == nil || len(n.OrderBy.Fields) == 0 {
		n.Logger.Debug("SortNode: no ORDER BY fields, returning unsorted documents")
		// Convert map to slice
		result := make([]*models.Document, 0, len(documents))
		for _, doc := range documents {
			result = append(result, doc)
		}
		return result, nil
	}

	// Decide whether to use Top-N heap or full sort
	if sorting.ShouldUseTopNHeap(len(documents), limit, n.config.TopNThreshold) {
		n.Logger.Debugf("Using Top-N heapsort: %d documents, LIMIT %d (%.1f%%)",
			len(documents), limit, float64(limit)/float64(len(documents))*100)

		// Choose appropriate Top-N algorithm based on field type
		// Single Responsibility: Delegate type detection and algorithm selection to sorting package
		sortedDocs, err := n.selectTopNAlgorithm(documents, limit)
		if err != nil {
			return nil, fmt.Errorf("SortNode: Top-N heapsort failed: %w", err)
		}

		n.sortedDocuments = sortedDocs
		return sortedDocs, nil
	}

	// Dataset too large or LIMIT too high - decide between radix sort or full sort
	// For numeric fields on large datasets, radix sort is O(n) vs O(n log n)
	n.Logger.Debugf("Top-N threshold not met: %d documents, LIMIT %d - checking radix sort eligibility",
		len(documents), limit)

	// Try radix sort for integer fields if beneficial
	if radixSorted, err := n.tryRadixSort(documents, limit); err == nil && radixSorted != nil {
		n.sortedDocuments = radixSorted
		return radixSorted, nil
	}

	// Fall back to standard full sort
	n.Logger.Debugf("Using standard full sort: %d documents", len(documents))
	sortedDocs, err := n.sorter.SortDocumentMap(documents)
	if err != nil {
		return nil, fmt.Errorf("SortNode: sorting failed: %w", err)
	}

	n.sortedDocuments = sortedDocs
	return sortedDocs, nil
}

// GetCost returns the estimated execution cost
// PHASE 2: Cost accessor for query planning
func (n *SortNode) GetCost() float64 {
	return n.Cost
}

// GetEstimatedRows returns the estimated number of output rows
// PHASE 2: Cardinality accessor for query planning
func (n *SortNode) GetEstimatedRows() int {
	return n.EstimatedRows
}

// GetSortedDocuments returns the documents in sorted order
// This allows downstream nodes (like LimitNode) to preserve the sort order
func (n *SortNode) GetSortedDocuments() []*models.Document {
	return n.sortedDocuments
}

// tryRadixSort attempts to use radix sort for integer fields on large datasets.
//
// Radix sort advantages:
// - O(n * 8) = O(n) linear time for int64 (vs O(n log n) for comparison sorts)
// - Cache-friendly sequential access
// - 6.7x faster than quicksort for large datasets
//
// Strategy:
// 1. Check if primary field is integer type
// 2. Verify dataset size meets threshold (default: 1000 docs)
// 3. Call RadixSort for eligible queries
//
// Returns:
// - Sorted documents if radix sort was used
// - nil if radix sort not applicable (caller should fall back to standard sort)
//
// TODO: I could extend this to support multi-field radix sort by sorting
// secondary fields within groups of equal primary field values.
func (n *SortNode) tryRadixSort(documents map[string]*models.Document, limit int) ([]*models.Document, error) {
	// Need at least one ORDER BY field
	if len(n.OrderBy.Fields) == 0 {
		return nil, nil
	}

	primaryField := n.OrderBy.Fields[0]

	// Sample document to detect field type
	var sampleDoc *models.Document
	for _, doc := range documents {
		sampleDoc = doc
		break
	}

	if sampleDoc == nil {
		return nil, nil
	}

	// Check if field exists and is integer type
	field, exists := sampleDoc.Fields[primaryField.FieldName]
	if !exists {
		return nil, nil
	}

	// Only use radix sort for integer types
	switch field.Value.AsInterface().(type) {
	case int, int32, int64:
		// Integer field - check if radix sort is beneficial
		if !sorting.ShouldUseRadixSort(len(documents), limit, n.config.RadixMinSize, n.config.RadixLimitRatio) {
			n.Logger.Debugf("Radix sort threshold not met: %d documents, LIMIT %d",
				len(documents), limit)
			return nil, nil
		}

		ascending := primaryField.Direction == queryparser.SortAsc

		// Check if parallel radix sort should be used
		// Parallel benefits: 3.5-7x speedup on 8 cores for datasets >= ParallelMinSize
		if sorting.ShouldUseParallelRadix(len(documents), limit, n.config) {
			// Use all available CPU cores for parallelism
			numWorkers := 0 // 0 means use runtime.NumCPU() in parallel algorithm

			n.Logger.Infof("Using PARALLEL radix sort for integer field '%s': %d documents, LIMIT %d (ASC: %v)",
				primaryField.FieldName, len(documents), limit, ascending)

			sortedDocs, err := sorting.ParallelRadixSort(documents, primaryField.FieldName, ascending, numWorkers, n.Logger)
			if err != nil {
				n.Logger.Warnf("Parallel radix sort failed, falling back to sequential radix: %v", err)
				// Fall through to sequential radix sort
			} else {
				n.Logger.Debugf("Parallel radix sort completed: %d documents sorted", len(sortedDocs))
				return sortedDocs, nil
			}
		}

		// Use sequential radix sort
		n.Logger.Infof("Using radix sort for integer field '%s': %d documents, LIMIT %d (ASC: %v)",
			primaryField.FieldName, len(documents), limit, ascending)

		sortedDocs, err := sorting.RadixSort(documents, primaryField.FieldName, ascending, n.Logger)
		if err != nil {
			n.Logger.Warnf("Radix sort failed, falling back to standard sort: %v", err)
			return nil, nil
		}

		n.Logger.Debugf("Radix sort completed: %d documents sorted", len(sortedDocs))
		return sortedDocs, nil

	default:
		// Not an integer field, radix sort not applicable
		return nil, nil
	}
}

// selectTopNAlgorithm chooses the optimal Top-N sorting algorithm based on field type.
//
// Strategy (Open/Closed Principle):
// - Detects first ORDER BY field type
// - Delegates to specialized algorithm:
//   - String fields: StringHeapSort (abbreviated keys + SIMD)
//   - Numeric fields: TopNHeapSort (integer comparison)
//   - Other types: Fallback to TopNHeapSort
//
// DRY Principle: Each algorithm handles its own type-specific optimizations
//
// TODO: I could extend this to support multi-field sorting with mixed types
// by building a composite comparison function that handles each field's type.
func (n *SortNode) selectTopNAlgorithm(documents map[string]*models.Document, limit int) ([]*models.Document, error) {
	// Determine field type from first ORDER BY field
	if len(n.OrderBy.Fields) == 0 {
		return nil, fmt.Errorf("no ORDER BY fields specified")
	}

	primaryField := n.OrderBy.Fields[0]

	// Sample a document to detect field type
	// TODO: I could cache field type metadata to avoid this sampling
	// or use schema information if available
	var sampleDoc *models.Document
	for _, doc := range documents {
		sampleDoc = doc
		break
	}

	if sampleDoc == nil {
		return []*models.Document{}, nil
	}

	// Extract field and detect type
	field, exists := sampleDoc.Fields[primaryField.FieldName]
	if !exists {
		// Field doesn't exist, use generic algorithm
		n.Logger.Debugf("Field '%s' not found in sample document, using generic Top-N heap", primaryField.FieldName)
		return sorting.TopNHeapSort(documents, limit, n.OrderBy, n.Logger)
	}

	// Determine ascending/descending
	ascending := primaryField.Direction == queryparser.SortAsc

	// Determine NULLS handling
	var nullsFirst bool
	switch primaryField.NullsPosition {
	case queryparser.NullsFirst:
		nullsFirst = true
	case queryparser.NullsLast:
		nullsFirst = false
	case queryparser.NullsDefault:
		// PostgreSQL semantics: ASC = NULLS LAST, DESC = NULLS FIRST
		nullsFirst = !ascending
	}

	// Select algorithm based on field value type
	switch field.Value.AsInterface().(type) {
	case string, []byte:
		// Check if parallel string sort should be used
		// Parallel benefits: 2-6x speedup with SIMD acceleration
		if sorting.ShouldUseParallelString(len(documents), n.config) {
			numWorkers := 0 // 0 means use runtime.NumCPU()

			n.Logger.Debugf("Using PARALLEL StringSort for field '%s' (SIMD: %v, limit: %d)",
				primaryField.FieldName, n.config.SIMDEnabled, limit)

			sortedDocs, err := sorting.ParallelStringSort(
				documents,
				primaryField.FieldName,
				ascending,
				n.config.SIMDEnabled,
				numWorkers,
				n.Logger,
			)
			if err != nil {
				n.Logger.Warnf("Parallel string sort failed, falling back to sequential: %v", err)
				// Fall through to sequential string sort
			} else {
				// Parallel string sort does full sort, limit the result to requested size
				if len(sortedDocs) > limit {
					sortedDocs = sortedDocs[:limit]
				}
				return sortedDocs, nil
			}
		}

		// Use SIMD-accelerated sequential string sorting
		n.Logger.Debugf("Using StringHeapSort for field '%s' (SIMD: %v)", primaryField.FieldName, n.config.SIMDEnabled)
		return sorting.StringHeapSort(
			documents,
			limit,
			primaryField.FieldName,
			ascending,
			n.config.SIMDEnabled,
			nullsFirst,
			n.Logger,
		)

	case int, int32, int64, float32, float64:
		// Check if parallel Top-N heap should be used
		// Parallel benefits: 3-4x speedup on 4 cores for large datasets
		if sorting.ShouldUseParallelTopN(len(documents), limit, n.config) {
			numWorkers := 0 // 0 means use runtime.NumCPU()

			n.Logger.Debugf("Using PARALLEL TopNHeapSort for numeric field '%s' (limit: %d)",
				primaryField.FieldName, limit)

			sortedDocs, err := sorting.ParallelTopNHeapSort(
				documents,
				limit,
				n.OrderBy,
				numWorkers,
				n.Logger,
			)
			if err != nil {
				n.Logger.Warnf("Parallel Top-N heap sort failed, falling back to sequential: %v", err)
				// Fall through to sequential Top-N heap
			} else {
				return sortedDocs, nil
			}
		}

		// Use sequential integer/numeric Top-N heap
		n.Logger.Debugf("Using TopNHeapSort for numeric field '%s'", primaryField.FieldName)
		return sorting.TopNHeapSort(documents, limit, n.OrderBy, n.Logger)

	default:
		// Fallback to generic Top-N heap
		n.Logger.Debugf("Using generic TopNHeapSort for field '%s' (type: %T)", primaryField.FieldName, field.Value)
		return sorting.TopNHeapSort(documents, limit, n.OrderBy, n.Logger)
	}
}

// logBase2 calculates log base 2 of n
// PHASE 2: Helper function for sorting cost estimation
func logBase2(n float64) float64 {
	if n <= 1 {
		return 0
	}
	// log2(n) = log(n) / log(2)
	return 3.321928094887362 * log10(n) // log(2) ≈ 0.30103, 1/0.30103 ≈ 3.32193
}

// log10 calculates log base 10 of n using natural log
// PHASE 2: Helper function for logarithm calculation
func log10(n float64) float64 {
	if n <= 0 {
		return 0
	}
	// Approximate log10 using natural log
	// log10(n) = ln(n) / ln(10)
	// We'll use a simple approximation for small values
	result := 0.0
	temp := n
	for temp >= 10 {
		temp /= 10
		result++
	}
	// Add fractional part (rough approximation)
	if temp > 1 {
		result += (temp - 1) / 9
	}
	return result
}
