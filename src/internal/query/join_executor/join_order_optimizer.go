/*
JOIN ORDER OPTIMIZER - POSTGRESQL-STYLE DYNAMIC PROGRAMMING

This file implements join order optimization using dynamic programming for
multi-way joins. It determines the optimal order of joining tables to
minimize total execution cost.

KEY FEATURES:
1. Dynamic programming for optimal join ordering (up to ~10-12 tables)
2. Cardinality estimation using statistics when available
3. Join selectivity estimation based on index and column statistics
4. Consideration of join conditions and available indexes

POSTGRESQL ALIGNMENT:
This follows PostgreSQL's approach:
- Uses DP for small numbers of relations
- Falls back to heuristics (greedy) for larger queries
- Considers join predicates when building join paths

ALGORITHM OVERVIEW:
1. Build base relation costs (single table scans)
2. Generate 2-way joins and cost each combination
3. Iteratively build N-way joins from (N-1)-way joins
4. Select the lowest cost complete join order

COMPLEXITY:
- Time: O(n! / (n-k)!) for exhaustive search, O(n^2 * 2^n) for DP
- For n > 12, switches to greedy heuristics (GEQO-style)
*/

package joinexecutor

import (
	"fmt"
	"math"
	"sort"

	"syndrdb/src/internal/domain/statistics"
	"syndrdb/src/internal/query/documentscanner"

	"go.uber.org/zap"
)

// JoinOrderOptimizer determines the optimal order for executing multi-way joins
type JoinOrderOptimizer struct {
	logger        *zap.SugaredLogger
	costEstimator *JoinCostEstimator

	// Configuration
	maxDPRelations int     // Maximum relations for DP (beyond this, use greedy)
	greedyFactor   float64 // Randomization factor for greedy search
}

// JoinRelation represents a single relation (table/bundle) in a join
type JoinRelation struct {
	Name        string                          // Bundle name
	Bundle      documentscanner.BundleInterface // Bundle interface
	Cardinality int64                           // Estimated row count
	Statistics  *statistics.BundleStatistics    // Statistics if available

	// Index information
	IndexedFields []string // Fields with indexes available
}

// JoinEdge represents a join condition between two relations
type JoinEdge struct {
	LeftRelation  string // Name of left relation
	RightRelation string // Name of right relation
	LeftKey       string // Join key on left side
	RightKey      string // Join key on right side
	Operator      string // Join operator (=, <, >, etc.)

	// Cost estimation hints
	Selectivity float64 // Estimated selectivity (0.0 to 1.0)
}

// JoinPath represents a (partial) join order
type JoinPath struct {
	Relations   []string    // Relations in this path (in join order)
	Edges       []*JoinEdge // Join edges used
	Cost        float64     // Estimated total cost
	Cardinality int64       // Estimated result cardinality

	// For DP memoization
	RelationSet uint64 // Bitmask of included relations (for fast lookup)
}

// JoinOrderPlan represents the final optimized join order
type JoinOrderPlan struct {
	Order       []string    // Relations in optimal join order
	JoinMethods []string    // Suggested join method for each step (Hash, Merge, NestedLoop)
	TotalCost   float64     // Estimated total cost
	Steps       []*JoinStep // Detailed join steps
}

// JoinStep represents a single join operation in the plan
type JoinStep struct {
	LeftInput     string  // Left input (relation name or intermediate result)
	RightInput    string  // Right input (relation name)
	JoinKey       string  // Join key
	JoinMethod    string  // Suggested join method
	EstimatedCost float64 // Cost for this step
	EstimatedRows int64   // Output cardinality
}

// NewJoinOrderOptimizer creates a new join order optimizer
func NewJoinOrderOptimizer(logger *zap.SugaredLogger, costEstimator *JoinCostEstimator) *JoinOrderOptimizer {
	return &JoinOrderOptimizer{
		logger:         logger,
		costEstimator:  costEstimator,
		maxDPRelations: 12, // PostgreSQL uses ~12 as threshold for GEQO
		greedyFactor:   0.2,
	}
}

// OptimizeJoinOrder determines the optimal join order for the given relations and edges
// Returns an optimized join plan
func (opt *JoinOrderOptimizer) OptimizeJoinOrder(
	relations map[string]*JoinRelation,
	edges []*JoinEdge,
) (*JoinOrderPlan, error) {

	if len(relations) == 0 {
		return nil, fmt.Errorf("no relations to optimize")
	}

	if len(relations) == 1 {
		// Single relation - no join ordering needed
		for name := range relations {
			return &JoinOrderPlan{
				Order:     []string{name},
				TotalCost: 1.0,
				Steps:     nil,
			}, nil
		}
	}

	opt.logger.Debugf("Optimizing join order for %d relations, %d join edges",
		len(relations), len(edges))

	// Choose algorithm based on relation count
	if len(relations) <= opt.maxDPRelations {
		return opt.optimizeWithDP(relations, edges)
	}
	return opt.optimizeWithGreedy(relations, edges)
}

// optimizeWithDP uses dynamic programming for optimal join ordering
// Time complexity: O(n^2 * 2^n) where n is the number of relations
func (opt *JoinOrderOptimizer) optimizeWithDP(
	relations map[string]*JoinRelation,
	edges []*JoinEdge,
) (*JoinOrderPlan, error) {

	// Create relation index for bitmask operations
	relIndex := make(map[string]int)
	relNames := make([]string, 0, len(relations))
	for name := range relations {
		relIndex[name] = len(relNames)
		relNames = append(relNames, name)
	}

	// Build edge lookup for efficient access
	edgeMap := opt.buildEdgeMap(edges)

	// Initialize DP table: pathTable[bitmask] = best path for that set of relations
	n := len(relations)
	pathTable := make(map[uint64]*JoinPath)

	// Base case: single relations
	for name, rel := range relations {
		mask := uint64(1) << relIndex[name]
		pathTable[mask] = &JoinPath{
			Relations:   []string{name},
			Edges:       nil,
			Cost:        float64(rel.Cardinality) * 0.1, // Base scan cost
			Cardinality: rel.Cardinality,
			RelationSet: mask,
		}
	}

	// Build up paths of increasing size
	for size := 2; size <= n; size++ {
		opt.logger.Debugf("DP iteration: building %d-way joins", size)

		// Generate all subsets of size 'size'
		for mask := uint64(1); mask < uint64(1<<n); mask++ {
			if popcount(mask) != size {
				continue
			}

			// Try all ways to split this subset into two non-empty parts
			bestPath := opt.findBestSplit(mask, pathTable, relIndex, relNames, relations, edgeMap)
			if bestPath != nil {
				pathTable[mask] = bestPath
			}
		}
	}

	// Get the complete join (all relations)
	fullMask := uint64((1 << n) - 1)
	bestPath := pathTable[fullMask]

	if bestPath == nil {
		// No valid join path found - relations may not be connected
		opt.logger.Warn("No valid join path found via DP, falling back to greedy")
		return opt.optimizeWithGreedy(relations, edges)
	}

	// Convert path to plan
	plan := opt.pathToPlan(bestPath, relations, edgeMap)

	opt.logger.Debugf("DP optimization complete: order=%v, cost=%.2f",
		plan.Order, plan.TotalCost)

	return plan, nil
}

// findBestSplit finds the best way to join two subsets to form the given set
func (opt *JoinOrderOptimizer) findBestSplit(
	mask uint64,
	pathTable map[uint64]*JoinPath,
	relIndex map[string]int,
	relNames []string,
	relations map[string]*JoinRelation,
	edgeMap map[string]map[string]*JoinEdge,
) *JoinPath {

	var bestPath *JoinPath

	// Try all non-empty proper subsets
	for leftMask := mask & (mask - 1); leftMask > 0; leftMask = mask & (leftMask - 1) {
		rightMask := mask ^ leftMask

		if leftMask == 0 || rightMask == 0 {
			continue
		}

		leftPath := pathTable[leftMask]
		rightPath := pathTable[rightMask]

		if leftPath == nil || rightPath == nil {
			continue
		}

		// Check if there's a join edge between these subsets
		edge := opt.findJoinEdge(leftPath, rightPath, edgeMap)
		if edge == nil {
			// No join condition - would be Cartesian product
			// Allow only if explicitly needed (very high cost)
			continue
		}

		// Calculate join cost
		joinCost := opt.calculateJoinCost(leftPath, rightPath, edge, relations)

		totalCost := leftPath.Cost + rightPath.Cost + joinCost.Cost

		if bestPath == nil || totalCost < bestPath.Cost {
			// Merge relation lists
			newRelations := make([]string, 0, len(leftPath.Relations)+len(rightPath.Relations))
			newRelations = append(newRelations, leftPath.Relations...)
			newRelations = append(newRelations, rightPath.Relations...)

			// Merge edges
			newEdges := make([]*JoinEdge, 0, len(leftPath.Edges)+len(rightPath.Edges)+1)
			newEdges = append(newEdges, leftPath.Edges...)
			newEdges = append(newEdges, rightPath.Edges...)
			newEdges = append(newEdges, edge)

			bestPath = &JoinPath{
				Relations:   newRelations,
				Edges:       newEdges,
				Cost:        totalCost,
				Cardinality: joinCost.Cardinality,
				RelationSet: mask,
			}
		}
	}

	return bestPath
}

// findJoinEdge finds a join edge connecting two partial join results
func (opt *JoinOrderOptimizer) findJoinEdge(
	leftPath, rightPath *JoinPath,
	edgeMap map[string]map[string]*JoinEdge,
) *JoinEdge {

	for _, leftRel := range leftPath.Relations {
		if rightEdges, ok := edgeMap[leftRel]; ok {
			for _, rightRel := range rightPath.Relations {
				if edge, ok := rightEdges[rightRel]; ok {
					return edge
				}
			}
		}
	}
	return nil
}

// JoinCostResult holds the result of a join cost calculation
type JoinCostResult struct {
	Cost        float64
	Cardinality int64
	JoinMethod  string
}

// calculateJoinCost estimates the cost of joining two paths
func (opt *JoinOrderOptimizer) calculateJoinCost(
	leftPath, rightPath *JoinPath,
	edge *JoinEdge,
	relations map[string]*JoinRelation,
) *JoinCostResult {

	leftCard := leftPath.Cardinality
	rightCard := rightPath.Cardinality

	// Estimate selectivity (default 10% if unknown)
	selectivity := edge.Selectivity
	if selectivity <= 0 {
		selectivity = 0.1
	}

	// Output cardinality
	outputCard := int64(float64(leftCard*rightCard) * selectivity)
	if outputCard < 1 {
		outputCard = 1
	}

	// Determine best join method and cost
	var joinMethod string
	var joinCost float64

	// Hash join cost: O(n + m)
	hashCost := float64(leftCard+rightCard) * 1.2

	// Nested loop cost: O(n * m) - only viable for small inputs
	nestedLoopCost := float64(leftCard * rightCard)
	if leftCard <= 100 && rightCard <= 100 {
		nestedLoopCost *= 0.5 // Bonus for small datasets
	}

	// Merge join cost: O(n log n + m log m) for sort + O(n + m) for merge
	// Cheaper if inputs are already sorted (have B-tree index)
	mergeCost := float64(leftCard+rightCard) * 1.5
	leftHasIndex := opt.hasIndexOnKey(relations, leftPath.Relations, edge.LeftKey)
	rightHasIndex := opt.hasIndexOnKey(relations, rightPath.Relations, edge.RightKey)
	if leftHasIndex && rightHasIndex {
		mergeCost *= 0.5 // 50% discount for indexed merge
	}

	// Choose best method
	if hashCost <= nestedLoopCost && hashCost <= mergeCost {
		joinMethod = "HashJoin"
		joinCost = hashCost
	} else if mergeCost <= nestedLoopCost {
		joinMethod = "MergeJoin"
		joinCost = mergeCost
	} else {
		joinMethod = "NestedLoop"
		joinCost = nestedLoopCost
	}

	return &JoinCostResult{
		Cost:        joinCost,
		Cardinality: outputCard,
		JoinMethod:  joinMethod,
	}
}

// hasIndexOnKey checks if any relation in the list has an index on the given key
func (opt *JoinOrderOptimizer) hasIndexOnKey(
	relations map[string]*JoinRelation,
	relNames []string,
	key string,
) bool {
	for _, name := range relNames {
		if rel, ok := relations[name]; ok {
			for _, indexedField := range rel.IndexedFields {
				if indexedField == key {
					return true
				}
			}
		}
	}
	return false
}

// optimizeWithGreedy uses a greedy heuristic for large join ordering
// This is similar to PostgreSQL's GEQO but deterministic
func (opt *JoinOrderOptimizer) optimizeWithGreedy(
	relations map[string]*JoinRelation,
	edges []*JoinEdge,
) (*JoinOrderPlan, error) {

	opt.logger.Info("Using greedy heuristic for join ordering (>12 relations)")

	// Build edge lookup
	edgeMap := opt.buildEdgeMap(edges)

	// Start with the smallest relation
	remaining := make(map[string]*JoinRelation)
	for name, rel := range relations {
		remaining[name] = rel
	}

	// Find starting relation (smallest cardinality)
	var startName string
	var startCard int64 = math.MaxInt64
	for name, rel := range remaining {
		if rel.Cardinality < startCard {
			startCard = rel.Cardinality
			startName = name
		}
	}

	order := []string{startName}
	steps := make([]*JoinStep, 0)
	delete(remaining, startName)
	currentCard := startCard
	totalCost := float64(startCard) * 0.1

	// Greedily add relations that minimize incremental cost
	for len(remaining) > 0 {
		var bestNext string
		var bestCost float64 = math.MaxFloat64
		var bestEdge *JoinEdge
		var bestCard int64

		for name, rel := range remaining {
			// Check if this relation can join with current result
			edge := opt.findEdgeToJoined(order, name, edgeMap)
			if edge == nil {
				continue // No join path available yet
			}

			// Calculate join cost
			selectivity := edge.Selectivity
			if selectivity <= 0 {
				selectivity = 0.1
			}

			joinCost := float64(currentCard+rel.Cardinality) * 1.2
			outputCard := int64(float64(currentCard*rel.Cardinality) * selectivity)

			if joinCost < bestCost {
				bestCost = joinCost
				bestNext = name
				bestEdge = edge
				bestCard = outputCard
			}
		}

		if bestNext == "" {
			// No connectable relation found - add first remaining as Cartesian
			for name := range remaining {
				bestNext = name
				bestCost = float64(currentCard * remaining[name].Cardinality)
				bestCard = currentCard * remaining[name].Cardinality
				break
			}
		}

		// Add to order
		order = append(order, bestNext)
		if bestEdge != nil {
			steps = append(steps, &JoinStep{
				LeftInput:     fmt.Sprintf("join_result_%d", len(steps)),
				RightInput:    bestNext,
				JoinKey:       bestEdge.LeftKey,
				JoinMethod:    "HashJoin",
				EstimatedCost: bestCost,
				EstimatedRows: bestCard,
			})
		}
		totalCost += bestCost
		currentCard = bestCard
		delete(remaining, bestNext)
	}

	plan := &JoinOrderPlan{
		Order:     order,
		TotalCost: totalCost,
		Steps:     steps,
	}

	opt.logger.Debugf("Greedy optimization complete: order=%v, cost=%.2f",
		plan.Order, plan.TotalCost)

	return plan, nil
}

// findEdgeToJoined finds an edge connecting any joined relation to the target
func (opt *JoinOrderOptimizer) findEdgeToJoined(
	joined []string,
	target string,
	edgeMap map[string]map[string]*JoinEdge,
) *JoinEdge {
	for _, name := range joined {
		if rightEdges, ok := edgeMap[name]; ok {
			if edge, ok := rightEdges[target]; ok {
				return edge
			}
		}
		// Check reverse direction
		if rightEdges, ok := edgeMap[target]; ok {
			if edge, ok := rightEdges[name]; ok {
				// Return reversed edge
				return &JoinEdge{
					LeftRelation:  edge.RightRelation,
					RightRelation: edge.LeftRelation,
					LeftKey:       edge.RightKey,
					RightKey:      edge.LeftKey,
					Operator:      edge.Operator,
					Selectivity:   edge.Selectivity,
				}
			}
		}
	}
	return nil
}

// buildEdgeMap creates a lookup map from edges
func (opt *JoinOrderOptimizer) buildEdgeMap(edges []*JoinEdge) map[string]map[string]*JoinEdge {
	edgeMap := make(map[string]map[string]*JoinEdge)

	for _, edge := range edges {
		if edgeMap[edge.LeftRelation] == nil {
			edgeMap[edge.LeftRelation] = make(map[string]*JoinEdge)
		}
		edgeMap[edge.LeftRelation][edge.RightRelation] = edge

		// Add reverse direction
		if edgeMap[edge.RightRelation] == nil {
			edgeMap[edge.RightRelation] = make(map[string]*JoinEdge)
		}
		edgeMap[edge.RightRelation][edge.LeftRelation] = &JoinEdge{
			LeftRelation:  edge.RightRelation,
			RightRelation: edge.LeftRelation,
			LeftKey:       edge.RightKey,
			RightKey:      edge.LeftKey,
			Operator:      edge.Operator,
			Selectivity:   edge.Selectivity,
		}
	}

	return edgeMap
}

// pathToPlan converts a JoinPath to a JoinOrderPlan with detailed steps
func (opt *JoinOrderOptimizer) pathToPlan(
	path *JoinPath,
	relations map[string]*JoinRelation,
	edgeMap map[string]map[string]*JoinEdge,
) *JoinOrderPlan {

	plan := &JoinOrderPlan{
		Order:     path.Relations,
		TotalCost: path.Cost,
		Steps:     make([]*JoinStep, 0, len(path.Relations)-1),
	}

	// Determine join methods for each step
	joinMethods := make([]string, 0, len(path.Edges))
	for _, edge := range path.Edges {
		leftRel := relations[edge.LeftRelation]
		rightRel := relations[edge.RightRelation]

		if leftRel == nil || rightRel == nil {
			joinMethods = append(joinMethods, "HashJoin")
			continue
		}

		// Simple heuristic for join method selection
		leftCard := leftRel.Cardinality
		rightCard := rightRel.Cardinality

		if leftCard*rightCard < 10000 {
			joinMethods = append(joinMethods, "NestedLoop")
		} else {
			joinMethods = append(joinMethods, "HashJoin")
		}
	}
	plan.JoinMethods = joinMethods

	// Build detailed steps
	if len(path.Relations) > 1 {
		currentResult := path.Relations[0]
		for i := 1; i < len(path.Relations); i++ {
			rightRel := path.Relations[i]
			edge := path.Edges[i-1]

			joinMethod := "HashJoin"
			if i-1 < len(joinMethods) {
				joinMethod = joinMethods[i-1]
			}

			step := &JoinStep{
				LeftInput:     currentResult,
				RightInput:    rightRel,
				JoinKey:       edge.LeftKey,
				JoinMethod:    joinMethod,
				EstimatedCost: 0, // Could calculate per-step cost
				EstimatedRows: 0, // Could estimate per-step cardinality
			}
			plan.Steps = append(plan.Steps, step)

			currentResult = fmt.Sprintf("join_%d", i)
		}
	}

	return plan
}

// EstimateJoinSelectivity estimates the selectivity of a join condition
// Returns a value between 0 and 1 indicating the fraction of rows that will match
func (opt *JoinOrderOptimizer) EstimateJoinSelectivity(
	leftRel, rightRel *JoinRelation,
	leftKey, rightKey string,
) float64 {
	// If we have statistics, use them
	if opt.costEstimator != nil {
		if leftRel.Statistics != nil && rightRel.Statistics != nil {
			// Use n_distinct values if available
			leftNDistinct := opt.getDistinctCount(leftRel.Statistics, leftKey)
			rightNDistinct := opt.getDistinctCount(rightRel.Statistics, rightKey)

			if leftNDistinct > 0 && rightNDistinct > 0 {
				// Selectivity = 1 / max(n_distinct_left, n_distinct_right)
				maxDistinct := leftNDistinct
				if rightNDistinct > maxDistinct {
					maxDistinct = rightNDistinct
				}
				return 1.0 / float64(maxDistinct)
			}
		}
	}

	// Default selectivity assumptions based on PostgreSQL
	// Primary key join: 1/max(n_left, n_right)
	// Foreign key join: 1/n_parent
	// General: 0.1 (10%)

	leftCard := leftRel.Cardinality
	rightCard := rightRel.Cardinality

	if leftCard > 0 && rightCard > 0 {
		maxCard := leftCard
		if rightCard > maxCard {
			maxCard = rightCard
		}
		return 1.0 / float64(maxCard)
	}

	return 0.1 // Default 10% selectivity
}

// getDistinctCount gets the distinct count for a field from statistics
func (opt *JoinOrderOptimizer) getDistinctCount(stats *statistics.BundleStatistics, fieldName string) int64 {
	// TODO: Extract n_distinct from BundleStatistics
	// For now, return 0 to indicate unknown
	return 0
}

// popcount counts the number of set bits in a uint64 (population count)
func popcount(x uint64) int {
	count := 0
	for x > 0 {
		count += int(x & 1)
		x >>= 1
	}
	return count
}

// CreateJoinRelation creates a JoinRelation from a bundle interface
func CreateJoinRelation(
	name string,
	bundle documentscanner.BundleInterface,
	stats *statistics.BundleStatistics,
) *JoinRelation {

	rel := &JoinRelation{
		Name:        name,
		Bundle:      bundle,
		Cardinality: int64(bundle.GetTotalDocuments()),
		Statistics:  stats,
	}

	// Collect indexed fields
	// TODO: Enhance BundleInterface to expose available indexes
	rel.IndexedFields = []string{}

	return rel
}

// CreateJoinEdge creates a JoinEdge from join condition information
func CreateJoinEdge(
	leftRel, rightRel string,
	leftKey, rightKey string,
	operator string,
) *JoinEdge {
	return &JoinEdge{
		LeftRelation:  leftRel,
		RightRelation: rightRel,
		LeftKey:       leftKey,
		RightKey:      rightKey,
		Operator:      operator,
		Selectivity:   0.1, // Default, can be refined with statistics
	}
}

// SortRelationsByCardinality returns relation names sorted by cardinality (ascending)
func SortRelationsByCardinality(relations map[string]*JoinRelation) []string {
	names := make([]string, 0, len(relations))
	for name := range relations {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool {
		return relations[names[i]].Cardinality < relations[names[j]].Cardinality
	})

	return names
}
