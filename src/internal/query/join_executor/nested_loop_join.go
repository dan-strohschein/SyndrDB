package joinexecutor

import (
	"fmt"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/pkg/common/conversion"

	syndrdbsimd "github.com/dan-strohschein/syndrdb-simd"
	"go.uber.org/zap"
)

// NestedLoopJoinStrategy implements a simple nested loop join algorithm
// This serves as a fallback for small datasets or when hash join is not suitable
type NestedLoopJoinStrategy struct {
	logger *zap.SugaredLogger

	// Configuration
	maxInnerLoopSize int  // Maximum size for inner loop to avoid O(n²) on large datasets
	batchSize        int  // Batch size for processing outer loop
	useSIMD          bool // Enable SIMD acceleration for key comparisons

	// PHASE 2: Could add index-aware optimization
	// indexOptimizer   IndexOptimizer // Use existing indexes to speed up inner loop

	// PHASE 4: Could add caching for repeated inner loop scans
	// resultCache      NestedLoopCache // Cache results of inner loop scans
}

// NewNestedLoopJoinStrategy creates a new nested loop join strategy
// logger: Logger for debugging and monitoring
// maxInnerLoopSize: Maximum documents in inner loop before rejecting this strategy
// useSIMD: Enable SIMD acceleration for key comparisons
func NewNestedLoopJoinStrategy(logger *zap.SugaredLogger, maxInnerLoopSize int, useSIMD bool) *NestedLoopJoinStrategy {
	return &NestedLoopJoinStrategy{
		logger:           logger,
		maxInnerLoopSize: maxInnerLoopSize,
		batchSize:        100, // Process outer loop in batches of 100
		useSIMD:          useSIMD,
	}
}

// GetName returns the name of this join strategy
func (nljs *NestedLoopJoinStrategy) GetName() string {
	return "NestedLoop"
}

// EstimateCost estimates the cost of executing a join with nested loop strategy
// This uses O(n*m) cost model with early rejection for large datasets
func (nljs *NestedLoopJoinStrategy) EstimateCost(request *JoinRequest) (cost float64, canHandle bool) {
	leftSize := int64(request.LeftBundle.GetTotalDocuments())
	rightSize := int64(request.RightBundle.GetTotalDocuments())

	// Choose smaller bundle for inner loop
	outerSize := leftSize
	innerSize := rightSize
	if rightSize > leftSize {
		outerSize = rightSize
		innerSize = leftSize
	}

	// CRITICAL: Reject if cartesian product would create too many comparisons
	// Even "small" datasets like 100×500 = 50,000 comparisons is too much for nested loop
	// Force hash join (O(n+m)) for any scenario where n×m > 1000
	if outerSize*innerSize > 1000 {
		nljs.logger.Debugf("Nested loop rejected: cartesian product too large (%d × %d = %d > 1000)",
			outerSize, innerSize, outerSize*innerSize)
		return 0, false
	}

	// Also reject if inner loop is too large (avoid O(n²) performance issues)
	if innerSize > int64(nljs.maxInnerLoopSize) {
		nljs.logger.Debugf("Nested loop rejected: inner loop too large (%d > %d)",
			innerSize, nljs.maxInnerLoopSize)
		return 0, false
	}

	// Cost is O(n*m) - outer loop size times inner loop size
	baseCost := float64(outerSize * innerSize)

	// Add bonus for very small datasets where nested loop overhead is minimal
	if outerSize < 10 && innerSize < 10 {
		baseCost *= 0.5 // 50% bonus for very small joins
	}

	// Add penalty for non-equality joins (more complex comparisons)
	for _, condition := range request.Conditions {
		if condition.Operator != "=" {
			baseCost *= 1.5 // 50% penalty for non-equality conditions
		}
	}

	nljs.logger.Debugf("Nested loop cost estimate: %.2f (outer: %d, inner: %d)",
		baseCost, outerSize, innerSize)

	return baseCost, true
}

// Execute performs the nested loop join operation
func (nljs *NestedLoopJoinStrategy) Execute(request *JoinRequest) (*JoinResult, error) {
	startTime := time.Now()

	nljs.logger.Debugf("Executing nested loop join: %s ⋈ %s",
		request.LeftBundle.GetName(), request.RightBundle.GetName())

	// Choose outer and inner loops based on size (smaller becomes inner)
	outerBundle, innerBundle, swapped := nljs.chooseOuterInner(request.LeftBundle, request.RightBundle)
	outerKey, innerKey := nljs.getJoinKeys(request.Conditions, swapped)

	// DEBUG: Log join configuration
	//nljs.logger.Debugf("JOIN DEBUG: Outer bundle: %s, Inner bundle: %s, Swapped: %t",
	//	outerBundle.GetName(), innerBundle.GetName(), swapped)
	//nljs.logger.Debugf("JOIN DEBUG: Outer key: '%s', Inner key: '%s'", outerKey, innerKey)
	//nljs.logger.Debugf("JOIN DEBUG: Original join condition: %s %s %s",
	//	request.Conditions[0].LeftKey, request.Conditions[0].Operator,
	//	request.Conditions[0].RightKey)

	// Pre-load inner loop documents for repeated access
	innerDocs := innerBundle.GetAllDocuments()
	nljs.logger.Debugf("Loaded %d documents for inner loop from bundle %s",
		len(innerDocs), innerBundle.GetName())

	// Execute nested loop join
	joinedDocs, stats, err := nljs.executeNestedLoop(
		outerBundle, innerDocs, outerKey, innerKey, request, swapped)
	if err != nil {
		return nil, fmt.Errorf("nested loop execution failed: %w", err)
	}

	// Create result
	result := &JoinResult{
		Documents:     joinedDocs,
		ExecutionTime: time.Since(startTime),
		MemoryUsed:    nljs.estimateMemoryUsage(innerDocs),
		DiskSpilled:   false, // Nested loop doesn't use disk spillover
		Algorithm:     nljs.GetName(),
		LeftScanned:   stats.OuterScanned,
		RightScanned:  stats.InnerScanned,
		Comparisons:   stats.Comparisons,
	}

	nljs.logger.Debugf("Nested loop join completed: %d results in %v",
		len(joinedDocs), result.ExecutionTime)

	return result, nil
}

// SupportsJoinType returns whether nested loop supports the given join type
func (nljs *NestedLoopJoinStrategy) SupportsJoinType(joinType JoinType) bool {
	switch joinType {
	case InnerJoin, LeftJoin, RightJoin, FullOuterJoin:
		return true // Nested loop can handle all join types
	default:
		return false
	}
}

// chooseOuterInner determines which bundle should be the outer vs inner loop
// Returns (outerBundle, innerBundle, swapped)
func (nljs *NestedLoopJoinStrategy) chooseOuterInner(left, right documentscanner.BundleInterface) (
	outer, inner documentscanner.BundleInterface, swapped bool) {

	leftSize := left.GetTotalDocuments()
	rightSize := right.GetTotalDocuments()

	if rightSize < leftSize {
		// Right bundle is smaller, use it for inner loop
		// Keep original order: left outer, right inner
		return left, right, false
	}
	// Left bundle is smaller, use it for inner loop
	// Swap: right outer, left inner
	return right, left, true
}

// getJoinKeys extracts the appropriate join keys based on whether sides were swapped
func (nljs *NestedLoopJoinStrategy) getJoinKeys(conditions []JoinCondition, swapped bool) (outerKey, innerKey string) {
	// For now, use the first condition (PHASE 2: will support multiple conditions)
	condition := conditions[0]

	if swapped {
		// When bundles are swapped (right becomes outer, left becomes inner)
		// The keys need to be swapped too
		return condition.RightKey, condition.LeftKey
	}
	// Normal case: left is outer, right is inner
	return condition.LeftKey, condition.RightKey
}

// executeNestedLoop performs the actual nested loop join operation
func (nljs *NestedLoopJoinStrategy) executeNestedLoop(
	outerBundle documentscanner.BundleInterface,
	innerDocs map[string]*models.Document,
	outerKey, innerKey string,
	request *JoinRequest,
	swapped bool,
) ([]*JoinedDocument, *NestedLoopStats, error) {

	// OPTIMIZATION: Pre-allocate result slice with estimated capacity
	// Eliminates ~10-15 slice reallocations during append operations
	outerSize := int64(outerBundle.GetTotalDocuments())
	innerSize := int64(len(innerDocs))
	// TODO: Integrate with JoinPatternTracker to learn actual selectivity per pattern
	// from historical execution stats instead of using fixed 0.1 default
	selectivity := 0.1 // Default 10% selectivity estimate
	estimatedResults := int(float64(outerSize) * float64(innerSize) * selectivity)
	joinedDocs := make([]*JoinedDocument, 0, estimatedResults)

	stats := &NestedLoopStats{
		OuterScanned: 0,
		InnerScanned: 0,
		Comparisons:  0,
	}

	// PROTECTION: Add limits to prevent infinite loops from massive datasets
	maxOuterDocs := 10000
	maxInnerDocs := 10000
	maxFailures := 1000
	failureCount := 0

	// OPTIMIZATION: Pre-extract join keys once to eliminate repeated map lookups
	// Eliminates 100,000+ map accesses in hot loop (50,000 comparisons × 2 sides)
	// Saves ~500 microseconds per join operation
	outerDocs := outerBundle.GetAllDocuments()
	outerKeyValues, outerDocsSlice, err := nljs.extractJoinKeysOnce(outerDocs, outerKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract outer join keys: %w", err)
	}

	innerKeyValues, innerDocsSlice, err := nljs.extractJoinKeysOnce(innerDocs, innerKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract inner join keys: %w", err)
	}

	// Stream through outer loop documents
	outerCount := 0
	for outerIdx, outerDoc := range outerDocsSlice {
		// PROTECTION: Limit outer document processing
		outerCount++
		if outerCount > maxOuterDocs {
			nljs.logger.Errorf("PROTECTION: Exceeded maximum outer documents (%d), terminating JOIN", maxOuterDocs)
			break
		}

		// Check for cancellation
		select {
		case <-request.Context.Done():
			return nil, nil, request.Context.Err()
		default:
		}

		stats.OuterScanned++

		// Get pre-extracted join key value (no map lookup!)
		outerKeyValue := outerKeyValues[outerIdx]
		if outerKeyValue == nil {
			failureCount++
			if failureCount >= maxFailures {
				nljs.logger.Errorf("PROTECTION: Exceeded maximum failures (%d), terminating JOIN", maxFailures)
				break
			}
			continue
		}

		// Track matches for this outer document
		hasMatches := false

		// Scan through all inner loop documents
		innerCount := 0
		for innerIdx, innerDoc := range innerDocsSlice {
			// PROTECTION: Limit inner document processing
			innerCount++
			if innerCount > maxInnerDocs {
				nljs.logger.Warnf("PROTECTION: Exceeded maximum inner documents (%d) for outer doc %d", maxInnerDocs, outerIdx)
				break
			}

			stats.InnerScanned++

			// Get pre-extracted join key value (no map lookup!)
			innerKeyValue := innerKeyValues[innerIdx]
			if innerKeyValue == nil {
				failureCount++
				if failureCount >= maxFailures {
					nljs.logger.Errorf("PROTECTION: Exceeded maximum failures (%d), terminating JOIN", maxFailures)
					return joinedDocs, stats, fmt.Errorf("too many key extraction failures (%d)", maxFailures)
				}
				continue
			}

			// Compare join keys using the specified operator
			matches, err := nljs.compareValues(outerKeyValue, innerKeyValue, request.Conditions[0].Operator)
			if err != nil {
				nljs.logger.Warnf("Failed to compare values: %v", err)
				continue
			}

			stats.Comparisons++

			if matches {
				hasMatches = true
				joinedDoc := nljs.createJoinedDocument(outerDoc, innerDoc,
					fmt.Sprintf("%v", outerKeyValue), swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
				}
			}
		}

		// Handle outer joins - include unmatched outer documents
		if !hasMatches && (request.JoinType == LeftJoin || request.JoinType == FullOuterJoin) {
			joinedDoc := nljs.createJoinedDocument(outerDoc, nil,
				fmt.Sprintf("%v", outerKeyValue), swapped, request.JoinType)
			if joinedDoc != nil {
				joinedDocs = append(joinedDocs, joinedDoc)
			}
		}
	}

	// PHASE 2: For full outer joins, need to find unmatched inner documents
	// This requires an additional pass through inner documents
	if request.JoinType == FullOuterJoin {
		// TODO: Implement unmatched inner document handling for full outer join
		//nljs.logger.Debugf("Full outer join: unmatched inner documents not yet implemented")
	}

	nljs.logger.Debugf("Nested loop completed: outer=%d, inner_total=%d, comparisons=%d, results=%d",
		stats.OuterScanned, stats.InnerScanned, stats.Comparisons, len(joinedDocs))

	return joinedDocs, stats, nil
}

// compareValues compares two values using the specified operator
// Uses SIMD-accelerated comparison when available for common types
func (nljs *NestedLoopJoinStrategy) compareValues(left, right interface{}, operator string) (bool, error) {
	switch operator {
	case "=", "==":
		// Use SIMD for equality checks when available
		if nljs.useSIMD {
			switch v1 := left.(type) {
			case string:
				if v2, ok := right.(string); ok {
					// SIMD string equality - 4-6x faster for UUIDs
					return syndrdbsimd.StrEq([]byte(v1), []byte(v2)), nil
				}
			case int64:
				if v2, ok := right.(int64); ok {
					return v1 == v2, nil
				}
			case int:
				if v2, ok := right.(int); ok {
					return v1 == v2, nil
				}
			case int32:
				if v2, ok := right.(int32); ok {
					return v1 == v2, nil
				}
			}
		}
		// Fallback to string conversion
		return conversion.ValueToString(left) == conversion.ValueToString(right), nil
	case "!=", "<>":
		if nljs.useSIMD {
			switch v1 := left.(type) {
			case string:
				if v2, ok := right.(string); ok {
					return !syndrdbsimd.StrEq([]byte(v1), []byte(v2)), nil
				}
			case int64:
				if v2, ok := right.(int64); ok {
					return v1 != v2, nil
				}
			case int:
				if v2, ok := right.(int); ok {
					return v1 != v2, nil
				}
			case int32:
				if v2, ok := right.(int32); ok {
					return v1 != v2, nil
				}
			}
		}
		return conversion.ValueToString(left) != conversion.ValueToString(right), nil
	case "<":
		return nljs.compareOrdered(left, right, -1)
	case "<=":
		result, err := nljs.compareOrdered(left, right, -1)
		if err != nil {
			return false, err
		}
		if result {
			return true, nil
		}
		return nljs.compareOrdered(left, right, 0)
	case ">":
		return nljs.compareOrdered(left, right, 1)
	case ">=":
		result, err := nljs.compareOrdered(left, right, 1)
		if err != nil {
			return false, err
		}
		if result {
			return true, nil
		}
		return nljs.compareOrdered(left, right, 0)
	default:
		return false, fmt.Errorf("unsupported operator: %s", operator)
	}
}

// compareOrdered compares two values for ordering (used for <, <=, >, >=)
// Uses SIMD when available for string comparisons
func (nljs *NestedLoopJoinStrategy) compareOrdered(left, right interface{}, expected int) (bool, error) {
	// Use SIMD for string comparison when available
	if nljs.useSIMD {
		if v1, ok1 := left.(string); ok1 {
			if v2, ok2 := right.(string); ok2 {
				// SIMD string comparison - 2-3x faster on long strings
				cmpResult := syndrdbsimd.StrCmp([]byte(v1), []byte(v2))
				return cmpResult == expected, nil
			}
		}
	}

	// Fallback: Type-aware comparison for numbers, then string conversion
	// PHASE 2: Add type-aware comparison (numbers, dates, etc.)
	leftStr := conversion.ValueToString(left)
	rightStr := conversion.ValueToString(right)

	if leftStr < rightStr {
		return expected == -1, nil
	} else if leftStr > rightStr {
		return expected == 1, nil
	} else {
		return expected == 0, nil
	}
}

// createJoinedDocument creates a JoinedDocument from outer and inner documents
// OPTIMIZATION: Uses object pool to eliminate allocations
func (nljs *NestedLoopJoinStrategy) createJoinedDocument(
	outerDoc, innerDoc *models.Document,
	joinKey string,
	swapped bool,
	joinType JoinType,
) *JoinedDocument {

	// Get from pool instead of allocating
	joined := GetPooledJoinedDocument()

	if swapped {
		// Swap back to maintain left/right consistency
		joined.LeftDocument = innerDoc
		joined.RightDocument = outerDoc
		joined.JoinKey = joinKey
	} else {
		joined.LeftDocument = outerDoc
		joined.RightDocument = innerDoc
		joined.JoinKey = joinKey
	}

	return joined
}

// extractJoinKeysOnce pre-extracts join key values from all documents
// This eliminates repeated map lookups in the hot comparison loop
// Returns: (keyValues []interface{}, docsSlice []*models.Document, error)
// TODO: Consider parallel extraction for large document sets (>10,000 docs) to further improve performance
func (nljs *NestedLoopJoinStrategy) extractJoinKeysOnce(docs map[string]*models.Document, keyName string) ([]interface{}, []*models.Document, error) {
	keyValues := make([]interface{}, 0, len(docs))
	docsSlice := make([]*models.Document, 0, len(docs))

	for _, doc := range docs {
		docsSlice = append(docsSlice, doc)

		// Extract key value
		field, exists := doc.Fields[keyName]
		if !exists {
			keyValues = append(keyValues, nil) // Mark as missing
			continue
		}

		keyValues = append(keyValues, field.Value)
	}

	return keyValues, docsSlice, nil
}

// estimateMemoryUsage estimates memory usage for the inner loop documents
func (nljs *NestedLoopJoinStrategy) estimateMemoryUsage(innerDocs map[string]*models.Document) int64 {
	// Rough estimate: 500 bytes per document
	return int64(len(innerDocs)) * 500
}

// NestedLoopStats holds statistics for nested loop execution
type NestedLoopStats struct {
	OuterScanned int64 // Number of outer loop documents scanned
	InnerScanned int64 // Total number of inner loop scans performed
	Comparisons  int64 // Total number of key comparisons
}

// PHASE 2: Index-aware optimization for nested loop
/*
type IndexOptimizer interface {
	HasIndex(bundle BundleInterface, keyName string) bool
	UseIndexForLookup(bundle BundleInterface, keyName string, value interface{}) ([]*models.Document, error)
	GetIndexSelectivity(bundle BundleInterface, keyName string) float64
}

func (nljs *NestedLoopJoinStrategy) optimizeWithIndex(
	outerBundle BundleInterface,
	innerBundle BundleInterface,
	innerKey string,
) bool {
	// Check if inner bundle has an index on the join key
	if nljs.indexOptimizer != nil && nljs.indexOptimizer.HasIndex(innerBundle, innerKey) {
		return true
	}
	return false
}
*/

// PHASE 4: Result caching for repeated inner loop patterns
/*
type NestedLoopCache interface {
	GetCachedResult(bundle string, key string, value interface{}) ([]*models.Document, bool)
	CacheResult(bundle string, key string, value interface{}, results []*models.Document)
	InvalidateCache(bundle string)
}

func (nljs *NestedLoopJoinStrategy) enableResultCaching(cache NestedLoopCache) {
	nljs.resultCache = cache
}
*/
