package joinexecutor

import (
	"fmt"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"

	"go.uber.org/zap"
)

// NestedLoopJoinStrategy implements a simple nested loop join algorithm
// This serves as a fallback for small datasets or when hash join is not suitable
type NestedLoopJoinStrategy struct {
	logger *zap.SugaredLogger

	// Configuration
	maxInnerLoopSize int // Maximum size for inner loop to avoid O(n²) on large datasets
	batchSize        int // Batch size for processing outer loop

	// PHASE 2: Could add index-aware optimization
	// indexOptimizer   IndexOptimizer // Use existing indexes to speed up inner loop

	// PHASE 4: Could add caching for repeated inner loop scans
	// resultCache      NestedLoopCache // Cache results of inner loop scans
}

// NewNestedLoopJoinStrategy creates a new nested loop join strategy
// logger: Logger for debugging and monitoring
// maxInnerLoopSize: Maximum documents in inner loop before rejecting this strategy
func NewNestedLoopJoinStrategy(logger *zap.SugaredLogger, maxInnerLoopSize int) *NestedLoopJoinStrategy {
	return &NestedLoopJoinStrategy{
		logger:           logger,
		maxInnerLoopSize: maxInnerLoopSize,
		batchSize:        100, // Process outer loop in batches of 100
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

	// Reject if inner loop is too large (avoid O(n²) performance issues)
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

	nljs.logger.Infof("Executing nested loop join: %s ⋈ %s",
		request.LeftBundle.GetName(), request.RightBundle.GetName())

	// Choose outer and inner loops based on size (smaller becomes inner)
	outerBundle, innerBundle, swapped := nljs.chooseOuterInner(request.LeftBundle, request.RightBundle)
	outerKey, innerKey := nljs.getJoinKeys(request.Conditions, swapped)

	// DEBUG: Log join configuration
	nljs.logger.Infof("JOIN DEBUG: Outer bundle: %s, Inner bundle: %s, Swapped: %t",
		outerBundle.GetName(), innerBundle.GetName(), swapped)
	nljs.logger.Infof("JOIN DEBUG: Outer key: '%s', Inner key: '%s'", outerKey, innerKey)
	nljs.logger.Infof("JOIN DEBUG: Original join condition: %s %s %s",
		request.Conditions[0].LeftKey, request.Conditions[0].Operator,
		request.Conditions[0].RightKey)

	// Pre-load inner loop documents for repeated access
	innerDocs := innerBundle.GetAllDocuments()
	nljs.logger.Infof("Loaded %d documents for inner loop from bundle %s",
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

	nljs.logger.Infof("Nested loop join completed: %d results in %v",
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

	var joinedDocs []*JoinedDocument
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

	// Stream through outer loop documents
	outerDocs := outerBundle.GetAllDocuments()
	outerCount := 0
	for outerDocID, outerDoc := range outerDocs {
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

		// Extract join key from outer document
		outerKeyValue, err := nljs.extractKeyValue(outerDoc, outerKey)
		if err != nil {
			failureCount++
			if failureCount <= 10 { // Only log first 10 failures to prevent spam
				nljs.logger.Warnf("Failed to extract key %s from outer document %s: %v",
					outerKey, outerDocID, err)
			}
			if failureCount >= maxFailures {
				nljs.logger.Errorf("PROTECTION: Exceeded maximum failures (%d), terminating JOIN", maxFailures)
				break
			}
			continue
		}

		// DEBUG: Log first few outer key values
		if stats.OuterScanned <= 5 {
			//nljs.logger.Infof("JOIN DEBUG: Outer doc %s has key '%s' = '%v'", outerDocID, outerKey, outerKeyValue)
		}

		// Track matches for this outer document
		hasMatches := false

		// Scan through all inner loop documents
		innerCount := 0
		for innerDocID, innerDoc := range innerDocs {
			// PROTECTION: Limit inner document processing
			innerCount++
			if innerCount > maxInnerDocs {
				nljs.logger.Warnf("PROTECTION: Exceeded maximum inner documents (%d) for outer doc %s", maxInnerDocs, outerDocID)
				break
			}

			stats.InnerScanned++

			// Extract join key from inner document
			innerKeyValue, err := nljs.extractKeyValue(innerDoc, innerKey)
			if err != nil {
				failureCount++
				if failureCount <= 10 { // Only log first 10 failures to prevent spam
					nljs.logger.Warnf("Failed to extract key %s from inner document %s: %v",
						innerKey, innerDocID, err)
				}
				if failureCount >= maxFailures {
					nljs.logger.Errorf("PROTECTION: Exceeded maximum failures (%d), terminating JOIN", maxFailures)
					return joinedDocs, stats, fmt.Errorf("too many key extraction failures (%d)", maxFailures)
				}
				continue
			}

			// DEBUG: Log first few inner key values for comparison
			if stats.OuterScanned == 1 && stats.InnerScanned <= 5 {
				//nljs.logger.Infof("JOIN DEBUG: Inner doc %s has key '%s' = '%v'", innerDocID, innerKey, innerKeyValue)
			}

			// Compare join keys using the specified operator
			matches, err := nljs.compareValues(outerKeyValue, innerKeyValue, request.Conditions[0].Operator)
			if err != nil {
				nljs.logger.Warnf("Failed to compare values: %v", err)
				continue
			}

			stats.Comparisons++

			// DEBUG: Log first successful match
			if matches && len(joinedDocs) == 0 {
				//nljs.logger.Infof("JOIN DEBUG: FIRST MATCH FOUND! Outer '%v' %s Inner '%v' = %t",
				//	outerKeyValue, request.Conditions[0].Operator, innerKeyValue, matches)
			}

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
func (nljs *NestedLoopJoinStrategy) compareValues(left, right interface{}, operator string) (bool, error) {
	switch operator {
	case "=", "==":
		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right), nil
	case "!=", "<>":
		return fmt.Sprintf("%v", left) != fmt.Sprintf("%v", right), nil
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
func (nljs *NestedLoopJoinStrategy) compareOrdered(left, right interface{}, expected int) (bool, error) {
	// Simple string comparison for now
	// PHASE 2: Add type-aware comparison (numbers, dates, etc.)
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	if leftStr < rightStr {
		return expected == -1, nil
	} else if leftStr > rightStr {
		return expected == 1, nil
	} else {
		return expected == 0, nil
	}
}

// createJoinedDocument creates a JoinedDocument from outer and inner documents
func (nljs *NestedLoopJoinStrategy) createJoinedDocument(
	outerDoc, innerDoc *models.Document,
	joinKey string,
	swapped bool,
	joinType JoinType,
) *JoinedDocument {

	if swapped {
		// Swap back to maintain left/right consistency
		return &JoinedDocument{
			LeftDocument:  innerDoc,
			RightDocument: outerDoc,
			JoinKey:       joinKey,
		}
	}

	return &JoinedDocument{
		LeftDocument:  outerDoc,
		RightDocument: innerDoc,
		JoinKey:       joinKey,
	}
}

// extractKeyValue extracts the value of a specific key from a document
func (nljs *NestedLoopJoinStrategy) extractKeyValue(doc *models.Document, keyName string) (interface{}, error) {
	// Check if the field exists in the document
	field, exists := doc.Fields[keyName]
	if !exists {
		// DEBUG: Log available fields to help diagnose the issue
		availableFields := make([]string, 0, len(doc.Fields))
		for fieldName := range doc.Fields {
			availableFields = append(availableFields, fieldName)
		}
		nljs.logger.Debugf("Field '%s' not found in document %s. Available fields: %v",
			keyName, doc.DocumentID, availableFields)
		return nil, fmt.Errorf("field %s not found in document %s", keyName, doc.DocumentID)
	}

	return field.Value, nil
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
