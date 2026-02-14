/*
MERGE JOIN STRATEGY - POSTGRESQL-STYLE SORT-MERGE JOIN

This file implements a merge join algorithm that leverages sorted data access
for O(n + m) join performance when both inputs are sorted on the join key.

KEY FEATURES:
1. O(n + m) time complexity when inputs are sorted (vs O(n * m) for nested loop)
2. Uses B-tree iterators for efficient sorted traversal when indexes exist
3. Falls back to external sort when no suitable indexes exist
4. Memory-efficient streaming merge without building full in-memory copies

POSTGRESQL ALIGNMENT:
This implementation follows PostgreSQL's merge join design:
- Uses sorted iterator access via index scans when available
- Supports mark/restore for many-to-many joins (duplicate handling)
- Integrates with the cost estimator for join algorithm selection

WHEN MERGE JOIN IS PREFERRED:
1. Both sides have B-tree indexes on the join key (zero sort cost)
2. One side is already sorted and the other is small enough to sort
3. The join is an equi-join with expected high selectivity
4. Memory is limited and hash join would spill excessively

IMPLEMENTATION PHASES:
Phase 1 (current): Basic merge join with in-memory sorting
Phase 2: External sort for large datasets
Phase 3: Parallel merge with partitioning
*/

package joinexecutor

import (
	"fmt"
	"sort"
	"time"

	"syndrdb/src/internal/domain/index/btreeindexV2"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/pkg/common/conversion"

	"go.uber.org/zap"
)

// MergeJoinStrategy implements a sort-merge join algorithm
// This strategy is optimal when both inputs are sorted on the join key
type MergeJoinStrategy struct {
	logger      *zap.SugaredLogger
	memoryLimit int64 // Maximum memory for sorting before external sort

	// Configuration
	sortBufferSize   int  // Buffer size for in-memory sorting
	useSIMD          bool // Enable SIMD acceleration for comparisons
	preferIndexScans bool // Prefer index scans over sorting when possible

	// Cost model parameters
	sortCostPerRecord float64 // Cost multiplier for sorting a record
	mergeCostPerPair  float64 // Cost multiplier for merging a pair
}

// NewMergeJoinStrategy creates a new merge join strategy
// logger: Logger for debugging and monitoring
// memoryLimit: Maximum memory to use for sorting (bytes)
// useSIMD: Enable SIMD acceleration for key comparisons
func NewMergeJoinStrategy(logger *zap.SugaredLogger, memoryLimit int64, useSIMD bool) *MergeJoinStrategy {
	return &MergeJoinStrategy{
		logger:            logger,
		memoryLimit:       memoryLimit,
		sortBufferSize:    10000, // Default 10k records in-memory sort buffer
		useSIMD:           useSIMD,
		preferIndexScans:  true, // Default to preferring index scans
		sortCostPerRecord: 1.5,  // Sorting adds ~50% overhead per record
		mergeCostPerPair:  0.5,  // Merge comparison is cheaper than hash lookup
	}
}

// GetName returns the name of this join strategy
func (mjs *MergeJoinStrategy) GetName() string {
	return "MergeJoin"
}

// EstimateCost estimates the cost of executing a join with the merge join strategy
// Cost model: O(n log n + m log m) for sorting + O(n + m) for merge
// If indexes exist, sorting cost is zero
func (mjs *MergeJoinStrategy) EstimateCost(request *JoinRequest) (cost float64, canHandle bool) {
	// Merge join can only handle equi-joins (equality conditions)
	for _, condition := range request.Conditions {
		if condition.Operator != "=" && condition.Operator != "==" {
			return 0, false // Cannot handle non-equality joins
		}
	}

	leftSize := float64(request.LeftBundle.GetTotalDocuments())
	rightSize := float64(request.RightBundle.GetTotalDocuments())

	// Check for B-tree index availability on join keys
	leftIndexed := mjs.hasBTreeIndex(request.LeftBundle, request.Conditions[0].LeftKey)
	rightIndexed := mjs.hasBTreeIndex(request.RightBundle, request.Conditions[0].RightKey)

	// Calculate sort costs (zero if indexed)
	var sortCost float64
	if !leftIndexed && leftSize > 1 {
		sortCost += leftSize * mjs.sortCostPerRecord * logN(leftSize)
	}
	if !rightIndexed && rightSize > 1 {
		sortCost += rightSize * mjs.sortCostPerRecord * logN(rightSize)
	}

	// Merge cost is O(n + m)
	mergeCost := (leftSize + rightSize) * mjs.mergeCostPerPair

	// Total cost
	baseCost := sortCost + mergeCost

	// Apply bonuses for ideal merge join scenarios
	if leftIndexed && rightIndexed {
		// Both sides have indexes - ideal case for merge join
		baseCost *= 0.6 // 40% bonus
		mjs.logger.Debugf("Merge join: both sides indexed, applying 40%% bonus")
	} else if leftIndexed || rightIndexed {
		// One side indexed
		baseCost *= 0.8 // 20% bonus
		mjs.logger.Debugf("Merge join: one side indexed, applying 20%% bonus")
	}

	// Penalty if sorting would exceed memory limit
	estimatedSortMemory := int64(leftSize+rightSize) * 500 // ~500 bytes per doc
	if estimatedSortMemory > request.MemoryLimit {
		// Would need external sort
		baseCost *= 1.5 // 50% penalty for external sort
		mjs.logger.Debugf("Merge join: external sort needed, applying 50%% penalty")
	}

	// Merge join is less efficient for small datasets due to sort overhead
	if leftSize < 100 && rightSize < 100 {
		baseCost *= 1.3 // 30% penalty for small datasets
	}

	mjs.logger.Debugf("Merge join cost estimate: %.2f (left: %.0f, right: %.0f, leftIdx: %v, rightIdx: %v)",
		baseCost, leftSize, rightSize, leftIndexed, rightIndexed)

	return baseCost, true
}

// Execute performs the merge join operation
func (mjs *MergeJoinStrategy) Execute(request *JoinRequest) (*JoinResult, error) {
	startTime := time.Now()

	mjs.logger.Debugf("Executing merge join: %s ⋈ %s",
		request.LeftBundle.GetName(), request.RightBundle.GetName())

	// Extract join keys
	leftKey := request.Conditions[0].LeftKey
	rightKey := request.Conditions[0].RightKey

	// Get sorted iterators for both sides
	leftIter, leftSorted, err := mjs.getSortedIterator(request.LeftBundle, leftKey, request)
	if err != nil {
		return nil, fmt.Errorf("failed to get left sorted iterator: %w", err)
	}

	rightIter, rightSorted, err := mjs.getSortedIterator(request.RightBundle, rightKey, request)
	if err != nil {
		return nil, fmt.Errorf("failed to get right sorted iterator: %w", err)
	}

	mjs.logger.Debugf("Merge join: leftSorted=%v (size=%d), rightSorted=%v (size=%d)",
		leftSorted != nil, len(leftIter), rightSorted != nil, len(rightIter))

	// Execute merge
	var joinedDocs []*JoinedDocument
	var stats *MergeJoinStats

	if leftSorted != nil && rightSorted != nil {
		// Both sides have index iterators - use iterator-based merge
		joinedDocs, stats, err = mjs.mergeWithIterators(leftSorted, rightSorted, leftKey, rightKey, request)
	} else {
		// At least one side needed sorting - use slice-based merge
		joinedDocs, stats, err = mjs.mergeSlices(leftIter, rightIter, leftKey, rightKey, request)
	}

	if err != nil {
		return nil, fmt.Errorf("merge execution failed: %w", err)
	}

	result := &JoinResult{
		Documents:     joinedDocs,
		ExecutionTime: time.Since(startTime),
		MemoryUsed:    stats.MemoryUsed,
		DiskSpilled:   stats.ExternalSortUsed,
		Algorithm:     mjs.GetName(),
		LeftScanned:   stats.LeftScanned,
		RightScanned:  stats.RightScanned,
		Comparisons:   stats.Comparisons,
	}

	mjs.logger.Debugf("Merge join completed: %d results in %v (comparisons: %d)",
		len(joinedDocs), result.ExecutionTime, stats.Comparisons)

	return result, nil
}

// SupportsJoinType returns whether merge join supports the given join type
func (mjs *MergeJoinStrategy) SupportsJoinType(joinType JoinType) bool {
	switch joinType {
	case InnerJoin, LeftJoin, RightJoin, FullOuterJoin:
		return true
	default:
		return false
	}
}

// MergeJoinStats holds statistics for a merge join operation
type MergeJoinStats struct {
	LeftScanned      int64 // Documents scanned from left
	RightScanned     int64 // Documents scanned from right
	Comparisons      int64 // Total key comparisons
	MemoryUsed       int64 // Peak memory usage
	ExternalSortUsed bool  // Whether external sort was needed
}

// SortedDocEntry represents a document with its extracted and typed key for sorting
type SortedDocEntry struct {
	Doc    *models.Document
	KeyStr string      // String representation for comparison
	KeyVal interface{} // Original typed value for accurate comparison
}

// getSortedIterator returns a sorted iterator for the given bundle and key
// If a B-tree index exists, uses the index iterator
// Otherwise, loads documents and sorts them in memory
func (mjs *MergeJoinStrategy) getSortedIterator(
	bundle documentscanner.BundleInterface,
	keyField string,
	request *JoinRequest,
) ([]SortedDocEntry, *btreeindexV2.BTreeIterator, error) {

	// Check for B-tree index on the join key
	if btreeIdx := mjs.getBTreeIndex(bundle, keyField); btreeIdx != nil {
		mjs.logger.Debugf("Using B-tree index for sorted access on %s.%s",
			bundle.GetName(), keyField)

		iter := btreeindexV2.NewBTreeIterator(btreeIdx, mjs.logger)
		if err := iter.SeekFirst(); err != nil {
			mjs.logger.Warnf("Failed to seek B-tree iterator: %v, falling back to sort", err)
			// Fall through to sorting
		} else {
			return nil, iter, nil
		}
	}

	// No index available - load and sort documents
	mjs.logger.Debugf("No B-tree index on %s.%s, loading and sorting %d documents",
		bundle.GetName(), keyField, bundle.GetTotalDocuments())

	docs := bundle.GetAllDocuments()
	sorted := make([]SortedDocEntry, 0, len(docs))
	schema := bundle.FieldSchema()

	for _, doc := range docs {
		keyVal, err := extractFieldValue(doc, keyField, schema)
		if err != nil {
			// Skip documents without the join key
			continue
		}
		keyStr := conversion.ValueToString(keyVal)
		sorted = append(sorted, SortedDocEntry{
			Doc:    doc,
			KeyStr: keyStr,
			KeyVal: keyVal,
		})
	}

	// Sort by key
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].KeyStr < sorted[j].KeyStr
	})

	return sorted, nil, nil
}

// mergeSlices performs merge join on two pre-sorted slices
// This is used when at least one side required in-memory sorting
func (mjs *MergeJoinStrategy) mergeSlices(
	leftSorted, rightSorted []SortedDocEntry,
	leftKey, rightKey string,
	request *JoinRequest,
) ([]*JoinedDocument, *MergeJoinStats, error) {

	stats := &MergeJoinStats{}

	// Estimate result size for pre-allocation
	estimatedSize := min(len(leftSorted), len(rightSorted))
	if estimatedSize > maxJoinedDocsPrealloc {
		estimatedSize = maxJoinedDocsPrealloc
	}
	joinedDocs := make([]*JoinedDocument, 0, estimatedSize)

	// Merge phase - standard merge join algorithm with mark/restore for duplicates
	leftIdx := 0
	rightIdx := 0

	for leftIdx < len(leftSorted) && rightIdx < len(rightSorted) {
		stats.Comparisons++

		leftEntry := leftSorted[leftIdx]
		rightEntry := rightSorted[rightIdx]

		cmp := compareKeys(leftEntry.KeyStr, rightEntry.KeyStr)

		if cmp < 0 {
			// Left key is smaller - advance left
			// For LEFT/FULL OUTER join, would emit left with null right here
			if request.JoinType == LeftJoin || request.JoinType == FullOuterJoin {
				joinedDocs = append(joinedDocs, &JoinedDocument{
					LeftDocument:  leftEntry.Doc,
					RightDocument: nil,
					JoinKey:       leftEntry.KeyStr,
				})
			}
			leftIdx++
			stats.LeftScanned++
		} else if cmp > 0 {
			// Right key is smaller - advance right
			// For RIGHT/FULL OUTER join, would emit right with null left here
			if request.JoinType == RightJoin || request.JoinType == FullOuterJoin {
				joinedDocs = append(joinedDocs, &JoinedDocument{
					LeftDocument:  nil,
					RightDocument: rightEntry.Doc,
					JoinKey:       rightEntry.KeyStr,
				})
			}
			rightIdx++
			stats.RightScanned++
		} else {
			// Keys match - need to handle potential duplicates on both sides
			// Mark the starting position for both sides
			leftMark := leftIdx
			rightMark := rightIdx
			matchKey := leftEntry.KeyStr

			// Find all left entries with this key
			for leftIdx < len(leftSorted) && leftSorted[leftIdx].KeyStr == matchKey {
				leftIdx++
				stats.LeftScanned++
			}

			// Find all right entries with this key
			for rightIdx < len(rightSorted) && rightSorted[rightIdx].KeyStr == matchKey {
				rightIdx++
				stats.RightScanned++
			}

			// Cross-product of all matches (Cartesian product for this key)
			for li := leftMark; li < leftIdx; li++ {
				for ri := rightMark; ri < rightIdx; ri++ {
					stats.Comparisons++
					joinedDocs = append(joinedDocs, &JoinedDocument{
						LeftDocument:  leftSorted[li].Doc,
						RightDocument: rightSorted[ri].Doc,
						JoinKey:       matchKey,
					})
				}
			}
		}
	}

	// Handle remaining left entries for LEFT/FULL OUTER join
	if request.JoinType == LeftJoin || request.JoinType == FullOuterJoin {
		for leftIdx < len(leftSorted) {
			joinedDocs = append(joinedDocs, &JoinedDocument{
				LeftDocument:  leftSorted[leftIdx].Doc,
				RightDocument: nil,
				JoinKey:       leftSorted[leftIdx].KeyStr,
			})
			leftIdx++
			stats.LeftScanned++
		}
	}

	// Handle remaining right entries for RIGHT/FULL OUTER join
	if request.JoinType == RightJoin || request.JoinType == FullOuterJoin {
		for rightIdx < len(rightSorted) {
			joinedDocs = append(joinedDocs, &JoinedDocument{
				LeftDocument:  nil,
				RightDocument: rightSorted[rightIdx].Doc,
				JoinKey:       rightSorted[rightIdx].KeyStr,
			})
			rightIdx++
			stats.RightScanned++
		}
	}

	// Estimate memory usage
	stats.MemoryUsed = int64(len(leftSorted)+len(rightSorted)) * 500

	return joinedDocs, stats, nil
}

// mergeWithIterators performs merge join using B-tree iterators
// This is the most efficient path when both sides have sorted index access
func (mjs *MergeJoinStrategy) mergeWithIterators(
	leftIter, rightIter *btreeindexV2.BTreeIterator,
	leftKey, rightKey string,
	request *JoinRequest,
) ([]*JoinedDocument, *MergeJoinStats, error) {

	defer leftIter.Close()
	defer rightIter.Close()

	stats := &MergeJoinStats{}
	joinedDocs := make([]*JoinedDocument, 0, 1000)

	// Get first entries from both iterators
	leftEntry, leftHasMore := mjs.nextIteratorEntry(leftIter)
	rightEntry, rightHasMore := mjs.nextIteratorEntry(rightIter)

	for leftHasMore && rightHasMore {
		stats.Comparisons++

		leftKeyStr := string(leftEntry.Key)
		rightKeyStr := string(rightEntry.Key)

		cmp := compareKeys(leftKeyStr, rightKeyStr)

		if cmp < 0 {
			// Left key is smaller - handle outer joins and advance
			if request.JoinType == LeftJoin || request.JoinType == FullOuterJoin {
				// Need to fetch actual documents for outer join emission
				for _, docID := range leftEntry.DocumentIDs {
					leftDoc := mjs.fetchDocument(request.LeftBundle, docID)
					if leftDoc != nil {
						joinedDocs = append(joinedDocs, &JoinedDocument{
							LeftDocument:  leftDoc,
							RightDocument: nil,
							JoinKey:       leftKeyStr,
						})
					}
				}
			}
			leftEntry, leftHasMore = mjs.nextIteratorEntry(leftIter)
			stats.LeftScanned++
		} else if cmp > 0 {
			// Right key is smaller - handle outer joins and advance
			if request.JoinType == RightJoin || request.JoinType == FullOuterJoin {
				for _, docID := range rightEntry.DocumentIDs {
					rightDoc := mjs.fetchDocument(request.RightBundle, docID)
					if rightDoc != nil {
						joinedDocs = append(joinedDocs, &JoinedDocument{
							LeftDocument:  nil,
							RightDocument: rightDoc,
							JoinKey:       rightKeyStr,
						})
					}
				}
			}
			rightEntry, rightHasMore = mjs.nextIteratorEntry(rightIter)
			stats.RightScanned++
		} else {
			// Keys match - need to handle duplicates
			matchKey := leftKeyStr

			// Collect all matching left document IDs
			leftDocIDs := leftEntry.DocumentIDs
			leftEntry, leftHasMore = mjs.nextIteratorEntry(leftIter)
			stats.LeftScanned++

			// Keep collecting while keys match
			for leftHasMore && string(leftEntry.Key) == matchKey {
				leftDocIDs = append(leftDocIDs, leftEntry.DocumentIDs...)
				leftEntry, leftHasMore = mjs.nextIteratorEntry(leftIter)
				stats.LeftScanned++
			}

			// Collect all matching right document IDs
			rightDocIDs := rightEntry.DocumentIDs
			rightEntry, rightHasMore = mjs.nextIteratorEntry(rightIter)
			stats.RightScanned++

			for rightHasMore && string(rightEntry.Key) == matchKey {
				rightDocIDs = append(rightDocIDs, rightEntry.DocumentIDs...)
				rightEntry, rightHasMore = mjs.nextIteratorEntry(rightIter)
				stats.RightScanned++
			}

			// Batch fetch documents for efficiency
			leftDocs := request.LeftBundle.GetDocumentsByIDs(leftDocIDs)
			rightDocs := request.RightBundle.GetDocumentsByIDs(rightDocIDs)

			// Cross-product of all matches
			for _, leftDocID := range leftDocIDs {
				leftDoc := leftDocs[leftDocID]
				if leftDoc == nil {
					continue
				}
				for _, rightDocID := range rightDocIDs {
					rightDoc := rightDocs[rightDocID]
					if rightDoc == nil {
						continue
					}
					stats.Comparisons++
					joinedDocs = append(joinedDocs, &JoinedDocument{
						LeftDocument:  leftDoc,
						RightDocument: rightDoc,
						JoinKey:       matchKey,
					})
				}
			}
		}
	}

	// Handle remaining entries for outer joins
	if request.JoinType == LeftJoin || request.JoinType == FullOuterJoin {
		for leftHasMore {
			for _, docID := range leftEntry.DocumentIDs {
				leftDoc := mjs.fetchDocument(request.LeftBundle, docID)
				if leftDoc != nil {
					joinedDocs = append(joinedDocs, &JoinedDocument{
						LeftDocument:  leftDoc,
						RightDocument: nil,
						JoinKey:       string(leftEntry.Key),
					})
				}
			}
			leftEntry, leftHasMore = mjs.nextIteratorEntry(leftIter)
			stats.LeftScanned++
		}
	}

	if request.JoinType == RightJoin || request.JoinType == FullOuterJoin {
		for rightHasMore {
			for _, docID := range rightEntry.DocumentIDs {
				rightDoc := mjs.fetchDocument(request.RightBundle, docID)
				if rightDoc != nil {
					joinedDocs = append(joinedDocs, &JoinedDocument{
						LeftDocument:  nil,
						RightDocument: rightDoc,
						JoinKey:       string(rightEntry.Key),
					})
				}
			}
			rightEntry, rightHasMore = mjs.nextIteratorEntry(rightIter)
			stats.RightScanned++
		}
	}

	// Iterator-based merge uses less memory (no full copies)
	stats.MemoryUsed = int64(len(joinedDocs)) * 100

	return joinedDocs, stats, nil
}

// nextIteratorEntry gets the next entry from a B-tree iterator
func (mjs *MergeJoinStrategy) nextIteratorEntry(iter *btreeindexV2.BTreeIterator) (*btreeindexV2.IteratorEntry, bool) {
	if !iter.HasNext() {
		return nil, false
	}

	entry := iter.Next()
	if entry == nil {
		return nil, false
	}

	return entry, true
}

// fetchDocument retrieves a single document by ID
func (mjs *MergeJoinStrategy) fetchDocument(bundle documentscanner.BundleInterface, docID string) *models.Document {
	return bundle.GetDocument(docID)
}

// hasBTreeIndex checks if a bundle has a B-tree index on the specified field
func (mjs *MergeJoinStrategy) hasBTreeIndex(bundle documentscanner.BundleInterface, fieldName string) bool {
	return mjs.getBTreeIndex(bundle, fieldName) != nil
}

// getBTreeIndex retrieves the B-tree index for a field, if it exists
func (mjs *MergeJoinStrategy) getBTreeIndex(bundle documentscanner.BundleInterface, fieldName string) *btreeindexV2.BTreeIndex {
	// Try to get the index through the bundle's interface
	// This requires the bundle to expose its index manager
	if !bundle.HasIndexOnField(fieldName) {
		return nil
	}

	// The bundle's GetHashIndexForField is generic - check if it's actually a B-tree
	// TODO: Add GetBTreeIndexForField to BundleInterface for cleaner access
	idx := bundle.GetHashIndexForField(fieldName)
	if btreeIdx, ok := idx.(*btreeindexV2.BTreeIndex); ok {
		return btreeIdx
	}

	return nil
}

// compareKeys compares two key strings
// Returns negative if a < b, positive if a > b, zero if equal
func compareKeys(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// logN returns the natural log of n (used for sort cost estimation)
func logN(n float64) float64 {
	if n <= 1 {
		return 1
	}
	// Use base-2 log for more intuitive sort complexity
	result := 0.0
	for n > 1 {
		n /= 2
		result++
	}
	return result
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
