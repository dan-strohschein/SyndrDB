/*
COST ESTIMATOR FOR JOIN OPERATIONS

This file implements a statistics-based cost estimator for join operations,
following PostgreSQL's approach of using table statistics to estimate
join costs and selectivity.

KEY FEATURES:
1. Integrates with existing BundleStatistics system
2. Uses histograms and MCV (Most Common Values) for selectivity estimation
3. Estimates result cardinality for join operations
4. Provides cost estimates for different join algorithms

POSTGRESQL ALIGNMENT:
- Uses table statistics (row counts, distinct values, histograms)
- Implements selectivity estimation using histograms
- Supports cost-based join algorithm selection

COST MODEL:
- Hash Join: O(n + m) where n is build size, m is probe size
- Nested Loop: O(n × m) but can be O(n × log m) with index
- Merge Join: O(n + m) when both sides are sorted

This estimator enables the query planner to make informed decisions about:
- Which join algorithm to use
- Which table should be the build side vs probe side
- Whether to use indexes
*/

package joinexecutor

import (
	"math"
	"syndrdb/src/internal/domain/statistics"
	"syndrdb/src/internal/query/documentscanner"

	"go.uber.org/zap"
)

// JoinCostEstimator estimates costs for join operations using bundle statistics
type JoinCostEstimator struct {
	logger      *zap.SugaredLogger
	statsCache  map[string]*statistics.BundleStatistics // Cache of bundle statistics
	memoryLimit int64                                   // Memory limit for join operations
}

// CostEstimate represents the estimated cost of a join operation
type CostEstimate struct {
	Algorithm            string  // Name of the join algorithm
	TotalCost            float64 // Total estimated cost
	StartupCost          float64 // Cost before first row can be returned
	RunCost              float64 // Cost to process all rows
	EstimatedRows        int64   // Estimated number of result rows
	MemoryRequired       int64   // Estimated memory required (bytes)
	NeedsDiskSpill       bool    // Whether disk spillover is likely needed
	IndexBenefit         float64 // Cost reduction from index usage (0.0 to 1.0)
	CanUseProbeIndex     bool    // Whether probe-side index can be used
	CanUseBuildIndex     bool    // Whether build-side index can be used
	RecommendedBuildSide string  // Recommended bundle for build side
}

// NewJoinCostEstimator creates a new cost estimator
func NewJoinCostEstimator(logger *zap.SugaredLogger, memoryLimit int64) *JoinCostEstimator {
	return &JoinCostEstimator{
		logger:      logger,
		statsCache:  make(map[string]*statistics.BundleStatistics),
		memoryLimit: memoryLimit,
	}
}

// EstimateJoinCost estimates the cost of a join operation
// Returns cost estimates for all applicable join algorithms
func (jce *JoinCostEstimator) EstimateJoinCost(request *JoinRequest) []CostEstimate {
	leftSize := int64(request.LeftBundle.GetTotalDocuments())
	rightSize := int64(request.RightBundle.GetTotalDocuments())
	leftName := request.LeftBundle.GetName()
	rightName := request.RightBundle.GetName()

	// Estimate join selectivity (what fraction of cross product will match)
	selectivity := jce.estimateJoinSelectivity(request)

	// Estimate result cardinality
	estimatedRows := int64(float64(leftSize*rightSize) * selectivity)
	if estimatedRows < 1 {
		estimatedRows = 1
	}

	var estimates []CostEstimate

	// Estimate Hash Join cost
	hashEstimate := jce.estimateHashJoinCost(leftSize, rightSize, estimatedRows, leftName, rightName)
	estimates = append(estimates, hashEstimate)

	// Estimate Nested Loop Join cost
	nestedLoopEstimate := jce.estimateNestedLoopCost(leftSize, rightSize, estimatedRows, request)
	estimates = append(estimates, nestedLoopEstimate)

	// Estimate Merge Join cost (if applicable)
	if jce.canUseMergeJoin(request) {
		mergeEstimate := jce.estimateMergeJoinCost(leftSize, rightSize, estimatedRows, request)
		estimates = append(estimates, mergeEstimate)
	}

	return estimates
}

// GetBestAlgorithm returns the lowest-cost join algorithm
func (jce *JoinCostEstimator) GetBestAlgorithm(request *JoinRequest) CostEstimate {
	estimates := jce.EstimateJoinCost(request)

	if len(estimates) == 0 {
		// Fallback to hash join
		return CostEstimate{
			Algorithm: "HashJoin",
			TotalCost: math.MaxFloat64,
		}
	}

	// Find minimum cost
	best := estimates[0]
	for _, e := range estimates[1:] {
		if e.TotalCost < best.TotalCost {
			best = e
		}
	}

	jce.logger.Debugf("Best join algorithm for %s ⋈ %s: %s (cost: %.2f, rows: %d)",
		request.LeftBundle.GetName(), request.RightBundle.GetName(),
		best.Algorithm, best.TotalCost, best.EstimatedRows)

	return best
}

// estimateJoinSelectivity estimates what fraction of the cross product will match
// Uses statistics when available, otherwise uses heuristics
func (jce *JoinCostEstimator) estimateJoinSelectivity(request *JoinRequest) float64 {
	if len(request.Conditions) == 0 {
		return 1.0 // Cross join - all rows match
	}

	// For each condition, estimate selectivity and multiply
	selectivity := 1.0

	for _, cond := range request.Conditions {
		condSelectivity := jce.estimateConditionSelectivity(
			request.LeftBundle, cond.LeftKey,
			request.RightBundle, cond.RightKey,
			cond.Operator,
		)
		selectivity *= condSelectivity
	}

	return selectivity
}

// estimateConditionSelectivity estimates selectivity for a single join condition
func (jce *JoinCostEstimator) estimateConditionSelectivity(
	leftBundle documentscanner.BundleInterface, leftKey string,
	rightBundle documentscanner.BundleInterface, rightKey string,
	operator string,
) float64 {
	leftSize := int64(leftBundle.GetTotalDocuments())
	rightSize := int64(rightBundle.GetTotalDocuments())

	// Get statistics for both sides
	leftStats := jce.getFieldStats(leftBundle.GetName(), leftKey)
	rightStats := jce.getFieldStats(rightBundle.GetName(), rightKey)

	// Equality join selectivity estimation
	if operator == "=" || operator == "==" {
		return jce.estimateEqualitySelectivity(leftStats, rightStats, leftSize, rightSize)
	}

	// Range join selectivity (less precise)
	// TODO: I could implement more sophisticated range selectivity using histograms
	switch operator {
	case "<", "<=", ">", ">=":
		return 0.33 // Assume 1/3 of rows match range conditions
	case "!=", "<>":
		return 0.9 // Most rows are different
	default:
		return 0.1 // Unknown operator - conservative estimate
	}
}

// estimateEqualitySelectivity estimates selectivity for equality joins
// Uses PostgreSQL's formula: 1/max(distinct_A, distinct_B) when no MCV overlap
func (jce *JoinCostEstimator) estimateEqualitySelectivity(
	leftStats, rightStats *statistics.FieldStatistics,
	leftSize, rightSize int64,
) float64 {
	// Default selectivity when no statistics available
	defaultSelectivity := 0.1

	if leftStats == nil && rightStats == nil {
		return defaultSelectivity
	}

	// Get distinct counts (use heuristics if not available)
	leftDistinct := int64(10)
	rightDistinct := int64(10)

	if leftStats != nil && leftStats.DistinctCount > 0 {
		leftDistinct = leftStats.DistinctCount
	} else if leftSize > 0 {
		// Heuristic: assume sqrt(n) distinct values
		leftDistinct = int64(math.Sqrt(float64(leftSize)))
	}

	if rightStats != nil && rightStats.DistinctCount > 0 {
		rightDistinct = rightStats.DistinctCount
	} else if rightSize > 0 {
		rightDistinct = int64(math.Sqrt(float64(rightSize)))
	}

	// PostgreSQL formula: selectivity = 1 / max(distinct_A, distinct_B)
	maxDistinct := leftDistinct
	if rightDistinct > maxDistinct {
		maxDistinct = rightDistinct
	}

	if maxDistinct <= 0 {
		return defaultSelectivity
	}

	selectivity := 1.0 / float64(maxDistinct)

	// Check for MCV overlap (if both sides have MCVs)
	if leftStats != nil && rightStats != nil {
		mcvSelectivity := jce.estimateMCVOverlap(leftStats, rightStats, leftSize, rightSize)
		if mcvSelectivity > selectivity {
			selectivity = mcvSelectivity
		}
	}

	return selectivity
}

// estimateMCVOverlap calculates selectivity contribution from most common values
func (jce *JoinCostEstimator) estimateMCVOverlap(
	leftStats, rightStats *statistics.FieldStatistics,
	leftSize, rightSize int64,
) float64 {
	if len(leftStats.MostCommonValues) == 0 || len(rightStats.MostCommonValues) == 0 {
		return 0.0
	}

	// Build map of right MCVs for quick lookup
	rightMCV := make(map[interface{}]float64)
	for _, vf := range rightStats.MostCommonValues {
		rightMCV[vf.Value] = vf.Frequency
	}

	// Calculate overlap selectivity
	var totalSelectivity float64
	for _, leftVF := range leftStats.MostCommonValues {
		if rightFreq, exists := rightMCV[leftVF.Value]; exists {
			// This value appears frequently in both sides
			// Selectivity contribution = leftFreq × rightFreq
			totalSelectivity += leftVF.Frequency * rightFreq
		}
	}

	return totalSelectivity
}

// estimateHashJoinCost estimates the cost of a hash join
func (jce *JoinCostEstimator) estimateHashJoinCost(
	leftSize, rightSize, estimatedRows int64,
	leftName, rightName string,
) CostEstimate {
	// Choose smaller side as build side
	buildSize := leftSize
	probeSize := rightSize
	buildSide := leftName
	if rightSize < leftSize {
		buildSize = rightSize
		probeSize = leftSize
		buildSide = rightName
	}

	// Cost model: O(n + m)
	// - Startup cost: building hash table from smaller side
	// - Run cost: probing hash table with larger side

	// Estimate bytes per document (rough average)
	bytesPerDoc := int64(500)

	// Memory required for hash table
	memoryRequired := buildSize * bytesPerDoc

	// Check if disk spill is needed
	needsDiskSpill := memoryRequired > jce.memoryLimit

	// Base costs
	startupCost := float64(buildSize) * 1.0    // Build hash table
	runCost := float64(probeSize) * 0.5        // Probe (faster than build)
	outputCost := float64(estimatedRows) * 0.1 // Output cost

	totalCost := startupCost + runCost + outputCost

	// Add penalty for disk spill (25% overhead)
	if needsDiskSpill {
		spillPenalty := float64(memoryRequired-jce.memoryLimit) / float64(jce.memoryLimit)
		totalCost *= 1.0 + spillPenalty*0.25
	}

	return CostEstimate{
		Algorithm:            "HashJoin",
		TotalCost:            totalCost,
		StartupCost:          startupCost,
		RunCost:              runCost + outputCost,
		EstimatedRows:        estimatedRows,
		MemoryRequired:       memoryRequired,
		NeedsDiskSpill:       needsDiskSpill,
		RecommendedBuildSide: buildSide,
	}
}

// estimateNestedLoopCost estimates the cost of a nested loop join
func (jce *JoinCostEstimator) estimateNestedLoopCost(
	leftSize, rightSize, estimatedRows int64,
	request *JoinRequest,
) CostEstimate {
	// Base nested loop cost: O(n × m)
	baseCost := float64(leftSize * rightSize)

	// Check if we can use an index on the inner side
	canUseIndex := false
	indexBenefit := 0.0

	if request.IndexStrategy != nil && request.IndexStrategy.IsApplicable() {
		canUseIndex = true
		// With index, cost becomes O(n × log m)
		if rightSize > 1 {
			indexBenefit = 1.0 - (math.Log2(float64(rightSize)) / float64(rightSize))
			baseCost = float64(leftSize) * math.Log2(float64(rightSize))
		}
	}

	// Nested loop has no startup cost but high run cost
	startupCost := 0.0
	runCost := baseCost
	outputCost := float64(estimatedRows) * 0.1

	// Memory is minimal for nested loop
	memoryRequired := int64(1024) // Just iteration state

	return CostEstimate{
		Algorithm:        "NestedLoop",
		TotalCost:        runCost + outputCost,
		StartupCost:      startupCost,
		RunCost:          runCost + outputCost,
		EstimatedRows:    estimatedRows,
		MemoryRequired:   memoryRequired,
		NeedsDiskSpill:   false, // Nested loop doesn't spill
		IndexBenefit:     indexBenefit,
		CanUseProbeIndex: canUseIndex,
	}
}

// canUseMergeJoin checks if merge join is applicable
func (jce *JoinCostEstimator) canUseMergeJoin(request *JoinRequest) bool {
	// Merge join requires equality conditions
	for _, cond := range request.Conditions {
		if cond.Operator != "=" && cond.Operator != "==" {
			return false
		}
	}

	// TODO: Check if either side has a sorted B-tree index on join key
	// For now, return false until B-tree iterator is implemented
	return false
}

// estimateMergeJoinCost estimates the cost of a merge join
func (jce *JoinCostEstimator) estimateMergeJoinCost(
	leftSize, rightSize, estimatedRows int64,
	request *JoinRequest,
) CostEstimate {
	// Merge join cost: O(n + m) if both sides sorted
	// If not sorted, add O(n log n) + O(m log m) for sorting

	// Check if sorting is needed
	leftNeedsSort := true
	rightNeedsSort := true

	// TODO: Check for existing B-tree indexes that provide sorted order
	// For now, assume sorting is needed

	sortCost := 0.0
	if leftNeedsSort {
		sortCost += float64(leftSize) * math.Log2(float64(leftSize)+1)
	}
	if rightNeedsSort {
		sortCost += float64(rightSize) * math.Log2(float64(rightSize)+1)
	}

	mergeCost := float64(leftSize + rightSize)
	outputCost := float64(estimatedRows) * 0.1

	// Memory for sorting
	memoryRequired := (leftSize + rightSize) * 100 // Smaller footprint than hash

	return CostEstimate{
		Algorithm:      "MergeJoin",
		TotalCost:      sortCost + mergeCost + outputCost,
		StartupCost:    sortCost,
		RunCost:        mergeCost + outputCost,
		EstimatedRows:  estimatedRows,
		MemoryRequired: memoryRequired,
		NeedsDiskSpill: memoryRequired > jce.memoryLimit,
	}
}

// RegisterBundleStats registers statistics for a bundle
func (jce *JoinCostEstimator) RegisterBundleStats(bundleName string, stats *statistics.BundleStatistics) {
	if stats != nil {
		jce.statsCache[bundleName] = stats
	}
}

// getFieldStats retrieves field statistics from cache
func (jce *JoinCostEstimator) getFieldStats(bundleName, fieldName string) *statistics.FieldStatistics {
	bundleStats, exists := jce.statsCache[bundleName]
	if !exists || bundleStats == nil {
		return nil
	}

	fieldStats, exists := bundleStats.FieldStats[fieldName]
	if !exists {
		return nil
	}

	return fieldStats
}

// EstimateResultCardinality estimates the number of result rows for a join
func (jce *JoinCostEstimator) EstimateResultCardinality(request *JoinRequest) int64 {
	leftSize := int64(request.LeftBundle.GetTotalDocuments())
	rightSize := int64(request.RightBundle.GetTotalDocuments())

	selectivity := jce.estimateJoinSelectivity(request)

	estimatedRows := int64(float64(leftSize*rightSize) * selectivity)
	if estimatedRows < 1 {
		estimatedRows = 1
	}

	return estimatedRows
}

// EstimateBuildSide determines which bundle should be the build side for hash join
// Returns the name of the recommended build-side bundle
func (jce *JoinCostEstimator) EstimateBuildSide(leftBundle, rightBundle documentscanner.BundleInterface) string {
	leftSize := leftBundle.GetTotalDocuments()
	rightSize := rightBundle.GetTotalDocuments()

	// Choose smaller side as build side
	if leftSize <= rightSize {
		return leftBundle.GetName()
	}
	return rightBundle.GetName()
}
