package joinexecutor

/*
INDEX STRATEGY SELECTOR FOR JOIN OPTIMIZATION

This file implements strategy pattern for selecting index-assisted join approaches.
It evaluates available indexes and recommends the optimal index usage strategy.

KEY RESPONSIBILITIES:
- Define IndexStrategy interface for extensibility (Open/Closed Principle)
- Implement ProbeIndexStrategy for using probe-side indexes
- Implement BuildIndexStrategy for using build-side indexes
- Select best index strategy based on cost evaluation
- Handle cases where no index strategy is beneficial

DESIGN PRINCIPLES:
- Single Responsibility: Only handles index strategy selection logic
- Open/Closed: New index strategies can be added without modifying existing code
- Liskov Substitution: All IndexStrategy implementations are interchangeable
- Interface Segregation: Minimal interface with only necessary methods

STRATEGY PATTERN:
IndexStrategy (interface)
├── ProbeIndexStrategy: Use probe-side index to filter documents
├── BuildIndexStrategy: Use build-side index to construct hash table
└── (Future) MergeIndexStrategy: Use both indexes for merge join

SELECTION ALGORITHM:
1. Check if probe bundle has index on join key
2. Check if build bundle has index on join key
3. Evaluate cost of each available strategy
4. Choose strategy with lowest cost
5. Break ties by preferring probe-side (larger bundle, more benefit)
6. Return nil if no index strategy is beneficial

USAGE EXAMPLE:
	strategy := SelectIndexStrategy(
	    buildBundle, probeBundle,
	    "DocumentID", "author_id",
	    estimatedHashTableSize)

	if strategy != nil {
	    logger.Infof("Using %s strategy", strategy.GetName())
	    request.IndexStrategy = strategy
	} else {
	    logger.Info("No beneficial index found, using full scan")
	}

TODO: Add MergeIndexStrategy for sorted indexes (when BTree indexes are implemented)
TODO: Add CompositeIndexStrategy for multi-column join conditions
TODO: Add CostBasedStrategyCache to remember optimal strategies for query patterns
*/

import (
	"fmt"

	"syndrdb/src/internal/domain/index/hashindexV3"
	"syndrdb/src/internal/query/documentscanner"
)

// IndexExecutionStrategy defines the interface for index-assisted join strategies
// This enables the Open/Closed Principle - new strategies can be added without modifying existing code
// Note: This is different from IndexStrategy (int enum) which is for internal classification
type IndexExecutionStrategy interface {
	// GetName returns the strategy name for logging and metrics
	GetName() string

	// EstimateCost estimates the cost of using this strategy
	// Lower cost means better performance
	// Parameters:
	//   - hashTableSize: Estimated number of keys in hash table
	//   - buildSize: Number of rows in build side
	//   - probeSize: Number of rows in probe side
	// Returns: Estimated cost in abstract units
	EstimateCost(hashTableSize int, buildSize int, probeSize int) float64

	// IsApplicable checks if this strategy can be used for the given join
	// Returns false if required indexes don't exist or join type is incompatible
	IsApplicable() bool

	// GetIndex returns the index to use for this strategy
	// Returns nil if strategy doesn't use an index
	GetIndex() *hashindexV3.HashIndexV3
}

// ProbeIndexStrategy uses an index on the probe side (larger bundle) to filter documents
// This is typically the most beneficial approach because it avoids scanning the large table
//
// Algorithm:
//  1. Build hash table from smaller bundle (normal hash join build phase)
//  2. Extract all unique keys from hash table
//  3. Use probe bundle's index to BatchGet only documents matching those keys
//  4. Probe hash table with filtered documents (much smaller set)
//
// Cost: O(build) + O(hashTableKeys * indexLookup) + O(matchedDocs * probe)
type ProbeIndexStrategy struct {
	probeIndex *hashindexV3.HashIndexV3 // Index on probe bundle's join key
	indexStats *hashindexV3.QueryOptimizationStats
}

// GetName returns the strategy identifier
func (s *ProbeIndexStrategy) GetName() string {
	return "index_assisted_probe"
}

// EstimateCost calculates the cost of probe index strategy
func (s *ProbeIndexStrategy) EstimateCost(hashTableSize int, buildSize int, probeSize int) float64 {
	// Use the cost evaluator to get accurate estimate
	estimate := EvaluateIndexUsage(hashTableSize, probeSize, s.indexStats)
	return estimate.IndexCost
}

// IsApplicable checks if probe index strategy can be used
func (s *ProbeIndexStrategy) IsApplicable() bool {
	return s.probeIndex != nil
}

// GetIndex returns the probe-side index
func (s *ProbeIndexStrategy) GetIndex() *hashindexV3.HashIndexV3 {
	return s.probeIndex
}

// BuildIndexStrategy uses an index on the build side (smaller bundle) to construct hash table
// This is less common but can be beneficial for highly selective build-side predicates
//
// Algorithm:
//  1. Use build bundle's index to get filtered document set
//  2. Build hash table from indexed documents only
//  3. Scan probe bundle normally
//  4. Probe hash table with each document
//
// Cost: O(buildKeys * indexLookup) + O(filteredBuild * hash) + O(probe * lookup)
//
// TODO: Implement this strategy when use cases emerge
// For now, we focus on ProbeIndexStrategy which provides more benefit
type BuildIndexStrategy struct {
	buildIndex *hashindexV3.HashIndexV3 // Index on build bundle's join key
	indexStats *hashindexV3.QueryOptimizationStats
}

// GetName returns the strategy identifier
func (s *BuildIndexStrategy) GetName() string {
	return "index_assisted_build"
}

// EstimateCost calculates the cost of build index strategy
// TODO: Implement proper cost model when strategy is fully implemented
func (s *BuildIndexStrategy) EstimateCost(hashTableSize int, buildSize int, probeSize int) float64 {
	// For now, estimate as slightly more expensive than regular hash join
	// This discourages selection until fully implemented
	return float64(buildSize+probeSize) * 1.1
}

// IsApplicable checks if build index strategy can be used
func (s *BuildIndexStrategy) IsApplicable() bool {
	// TODO: Return true when BuildIndexStrategy is fully implemented
	return false // Not yet implemented
}

// GetIndex returns the build-side index
func (s *BuildIndexStrategy) GetIndex() *hashindexV3.HashIndexV3 {
	return s.buildIndex
}

// SelectIndexStrategy evaluates available indexes and selects the optimal strategy
// This is the main entry point for index-based join optimization
//
// Parameters:
//   - buildBundle: The smaller bundle used to build the hash table
//   - probeBundle: The larger bundle that will be probed
//   - buildKey: Field name for join key on build side
//   - probeKey: Field name for join key on probe side
//   - estimatedHashTableSize: Expected number of unique keys in hash table
//
// Returns: Selected IndexExecutionStrategy or nil if no beneficial index exists
//
// Algorithm:
//  1. Check for probe-side index (most beneficial)
//  2. Check for build-side index (less common)
//  3. Evaluate cost of each available strategy
//  4. Compare with full scan cost
//  5. Return cheapest strategy that beats full scan
//  6. Break ties by preferring probe-side (default rule)
func SelectIndexStrategy(
	buildBundle documentscanner.BundleInterface,
	probeBundle documentscanner.BundleInterface,
	buildKey string,
	probeKey string,
	estimatedHashTableSize int,
) IndexExecutionStrategy {

	buildSize := buildBundle.GetTotalDocuments()
	probeSize := probeBundle.GetTotalDocuments()

	// TODO Codesmells with these deeply nested ifs - refactor later
	// Check for probe-side index (preferred)
	var probeStrategy *ProbeIndexStrategy
	hasProbe := probeBundle.HasIndexOnField(probeKey)
	if hasProbe {
		indexRef := probeBundle.GetHashIndexForField(probeKey)
		if indexRef != nil {
			// Type assert to HashIndexV3
			if hashIndex, ok := indexRef.(*hashindexV3.HashIndexV3); ok {
				stats := hashIndex.GetQueryOptimizationStats()
				probeStrategy = &ProbeIndexStrategy{
					probeIndex: hashIndex,
					indexStats: &stats,
				}
			}
		}
	}

	// Check for build-side index (less common, not yet fully implemented)
	var buildStrategy *BuildIndexStrategy
	if buildBundle.HasIndexOnField(buildKey) {
		indexRef := buildBundle.GetHashIndexForField(buildKey)
		if indexRef != nil {
			if hashIndex, ok := indexRef.(*hashindexV3.HashIndexV3); ok {
				stats := hashIndex.GetQueryOptimizationStats()
				buildStrategy = &BuildIndexStrategy{
					buildIndex: hashIndex,
					indexStats: &stats,
				}
			}
		}
	}

	// No indexes available
	if probeStrategy == nil && buildStrategy == nil {
		return nil
	}

	// Collect applicable strategies with their costs
	type strategyCost struct {
		strategy IndexExecutionStrategy
		cost     float64
	}
	var candidates []strategyCost

	if probeStrategy != nil && probeStrategy.IsApplicable() {
		cost := probeStrategy.EstimateCost(estimatedHashTableSize, buildSize, probeSize)
		candidates = append(candidates, strategyCost{probeStrategy, cost})
	}

	if buildStrategy != nil && buildStrategy.IsApplicable() {
		cost := buildStrategy.EstimateCost(estimatedHashTableSize, buildSize, probeSize)
		candidates = append(candidates, strategyCost{buildStrategy, cost})
	}

	// No applicable strategies
	if len(candidates) == 0 {
		return nil
	}

	// Calculate full scan cost for comparison
	scanCost := float64(probeSize) * scanCostPerDocument

	// Find cheapest strategy that beats full scan
	var bestStrategy IndexExecutionStrategy
	bestCost := scanCost // Start with scan cost as baseline

	for _, candidate := range candidates {
		if candidate.cost < bestCost {
			bestStrategy = candidate.strategy
			bestCost = candidate.cost
		} else if candidate.cost == bestCost {
			// Break tie by preferring probe-side (default rule)
			if candidate.strategy.GetName() == "index_assisted_probe" {
				bestStrategy = candidate.strategy
				bestCost = candidate.cost
			}
		}
	}

	// Return best strategy (nil if no strategy beats full scan)
	return bestStrategy
}

// FormatStrategyExplanation creates a human-readable explanation of strategy selection
// This is useful for query explain output and debugging
func FormatStrategyExplanation(
	strategy IndexExecutionStrategy,
	hashTableSize int,
	buildSize int,
	probeSize int,
) string {
	if strategy == nil {
		return "No beneficial index found - using full table scan"
	}

	cost := strategy.EstimateCost(hashTableSize, buildSize, probeSize)
	scanCost := float64(probeSize) * scanCostPerDocument
	speedup := scanCost / cost

	return fmt.Sprintf(
		"Using %s strategy: estimated cost %.2f vs scan cost %.2f (%.2fx speedup expected)",
		strategy.GetName(), cost, scanCost, speedup,
	)
}
