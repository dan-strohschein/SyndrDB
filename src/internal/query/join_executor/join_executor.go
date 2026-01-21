package joinexecutor

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

var (
	joinConcurrencyMu   sync.Mutex
	joinConcurrencySem  chan struct{}
	joinConcurrencyCap  int
)

// DefaultJoinExecutor implements the JoinExecutor interface
// It coordinates strategy selection and execution with cost-based optimization
type DefaultJoinExecutor struct {
	strategies     []JoinStrategy     // Registered join strategies
	patternTracker JoinMetrics        // Tracks join patterns and performance
	logger         *zap.SugaredLogger // Logger for debugging

	// Configuration
	defaultMemoryLimit    int64 // Default memory limit for joins
	enablePatternTracking bool  // Whether to track join patterns
	useSIMD               bool  // Enable SIMD acceleration for hash/compare operations

	// PHASE 2: Parallel execution coordination
	// maxConcurrentJoins int           // Maximum concurrent joins
	// joinQueue         JoinQueue     // Queue for managing concurrent joins

	// PHASE 3: Relationship-aware optimization
	// relationshipMetadata RelationshipMetadata // Metadata for relationship optimization

	// PHASE 4: Advanced features
	// resultCache       JoinCache     // Cache for join results
	// cachingEnabled    bool          // Whether result caching is enabled
}

// NewDefaultJoinExecutor creates a new join executor with default configuration
// logger: Logger for debugging and monitoring
// defaultMemoryLimit: Default memory limit for join operations (bytes)
// useSIMD: Enable SIMD acceleration (AVX2/NEON) for hash/compare operations
func NewDefaultJoinExecutor(logger *zap.SugaredLogger, defaultMemoryLimit int64, useSIMD bool) JoinExecutor {
	executor := &DefaultJoinExecutor{
		strategies:            make([]JoinStrategy, 0),
		patternTracker:        NewJoinPatternTracker(logger, 5, 10), // Hot after 5, optimize after 10
		logger:                logger,
		defaultMemoryLimit:    defaultMemoryLimit,
		enablePatternTracking: true,
		useSIMD:               useSIMD,
	}

	// Register default strategies (pass SIMD flag to strategies)
	executor.RegisterStrategy(NewHashJoinStrategy(logger, defaultMemoryLimit, useSIMD))
	executor.RegisterStrategy(NewNestedLoopJoinStrategy(logger, 1000, useSIMD)) // Max 1000 docs in inner loop

	logger.Infof("Created join executor with %d strategies, memory limit: %d bytes, SIMD: %v",
		len(executor.strategies), defaultMemoryLimit, useSIMD)

	return executor
}

// RegisterStrategy registers a new join strategy
func (dje *DefaultJoinExecutor) RegisterStrategy(strategy JoinStrategy) {
	dje.strategies = append(dje.strategies, strategy)
	dje.logger.Debugf("Registered join strategy: %s", strategy.GetName())
}

// Execute selects the best strategy and executes the join
func (dje *DefaultJoinExecutor) Execute(request *JoinRequest) (*JoinResult, error) {
	// Validate request
	if err := dje.validateRequest(request); err != nil {
		return nil, fmt.Errorf("invalid join request: %w", err)
	}

	// Optional concurrency cap: limit how many joins run full build+probe at once (0 = disabled)
	if cap := settings.GetSettings().JoinConcurrencyLimit; cap > 0 {
		joinConcurrencyMu.Lock()
		if joinConcurrencySem == nil || joinConcurrencyCap != cap {
			joinConcurrencySem = make(chan struct{}, cap)
			joinConcurrencyCap = cap
		}
		sem := joinConcurrencySem
		joinConcurrencyMu.Unlock()
		sem <- struct{}{}
		defer func() { <-sem }()
	}

	dje.logger.Infof("Executing join: %s ⋈ %s (type: %v, conditions: %d)",
		request.LeftBundle.GetName(), request.RightBundle.GetName(),
		request.JoinType, len(request.Conditions))

	// Apply defaults to request
	dje.applyDefaults(request)

	// Select the best strategy for this join
	strategy, err := dje.selectBestStrategy(request)
	if err != nil {
		return nil, fmt.Errorf("strategy selection failed: %w", err)
	}

	dje.logger.Debugf("Selected strategy: %s", strategy.GetName())

	// Execute the join using the selected strategy
	result, err := strategy.Execute(request)
	if err != nil {
		dje.logger.Errorf("Join execution failed with strategy %s: %v", strategy.GetName(), err)

		// PHASE 2: Try fallback strategy if primary fails
		fallbackResult, fallbackErr := dje.tryFallbackStrategy(request, strategy)
		if fallbackErr != nil {
			return nil, fmt.Errorf("join failed with %s and fallback failed: %w, fallback: %w",
				strategy.GetName(), err, fallbackErr)
		}
		result = fallbackResult
	}

	// Record performance metrics for learning
	if dje.enablePatternTracking {
		dje.patternTracker.RecordJoin(
			request.LeftBundle.GetName(),
			request.RightBundle.GetName(),
			result,
		)
	}

	dje.logger.Infof("Join completed: %d results in %v using %s",
		len(result.Documents), result.ExecutionTime, result.Algorithm)

	return result, nil
}

// GetAvailableStrategies returns all registered strategies
func (dje *DefaultJoinExecutor) GetAvailableStrategies() []JoinStrategy {
	strategies := make([]JoinStrategy, len(dje.strategies))
	copy(strategies, dje.strategies)
	return strategies
}

// validateRequest validates the join request parameters
func (dje *DefaultJoinExecutor) validateRequest(request *JoinRequest) error {
	if request == nil {
		return fmt.Errorf("request cannot be nil")
	}

	if request.LeftBundle == nil {
		return fmt.Errorf("left bundle cannot be nil")
	}

	if request.RightBundle == nil {
		return fmt.Errorf("right bundle cannot be nil")
	}

	if len(request.Conditions) == 0 {
		return fmt.Errorf("at least one join condition is required")
	}

	if request.Context == nil {
		request.Context = context.Background()
	}

	// Validate join conditions
	for i, condition := range request.Conditions {
		if condition.LeftKey == "" {
			return fmt.Errorf("condition %d: left key cannot be empty", i)
		}
		if condition.RightKey == "" {
			return fmt.Errorf("condition %d: right key cannot be empty", i)
		}
		if condition.Operator == "" {
			return fmt.Errorf("condition %d: operator cannot be empty", i)
		}
	}

	return nil
}

// applyDefaults applies default values to the request where not specified
func (dje *DefaultJoinExecutor) applyDefaults(request *JoinRequest) {
	if request.MemoryLimit <= 0 {
		request.MemoryLimit = dje.defaultMemoryLimit
	}

	if request.ExpectedResultSize <= 0 {
		// Estimate based on bundle sizes
		leftSize := int64(request.LeftBundle.GetTotalDocuments())
		rightSize := int64(request.RightBundle.GetTotalDocuments())
		request.ExpectedResultSize = leftSize * rightSize / 10 // Conservative estimate
	}

	// Default to allowing disk spillover
	if !request.AllowDiskSpillover {
		request.AllowDiskSpillover = true
	}
}

// selectBestStrategy selects the optimal strategy for the given join request
func (dje *DefaultJoinExecutor) selectBestStrategy(request *JoinRequest) (JoinStrategy, error) {
	// Check if we have learned a preferred strategy for this pattern
	if dje.enablePatternTracking {
		if preferredStrategy := dje.getPreferredStrategy(request); preferredStrategy != nil {
			dje.logger.Debugf("Using learned preferred strategy: %s", preferredStrategy.GetName())
			return preferredStrategy, nil
		}
	}

	// PHASE 1: Check for index-assisted optimization opportunities
	// This must happen before standard strategy evaluation
	if len(request.Conditions) > 0 {
		// Extract join keys from first condition (Phase 1 only supports single-column)
		firstCondition := request.Conditions[0]
		leftKey := firstCondition.LeftKey
		rightKey := firstCondition.RightKey

		// Determine build/probe bundles (hash join uses smaller as build side)
		leftSize := request.LeftBundle.GetTotalDocuments()
		rightSize := request.RightBundle.GetTotalDocuments()
		var buildBundle, probeBundle documentscanner.BundleInterface
		var buildKey, probeKey string

		if leftSize <= rightSize {
			buildBundle = request.LeftBundle
			probeBundle = request.RightBundle
			buildKey = leftKey
			probeKey = rightKey
		} else {
			buildBundle = request.RightBundle
			probeBundle = request.LeftBundle
			buildKey = rightKey
			probeKey = leftKey
		}

		// Estimate hash table size (used for cost calculation)
		estimatedHashTableSize := buildBundle.GetTotalDocuments()

		// Select index strategy if beneficial
		indexStrategy := SelectIndexStrategy(
			buildBundle, probeBundle,
			buildKey, probeKey,
			estimatedHashTableSize)

		if indexStrategy != nil {
			// Store selected strategy in request for use by join strategies
			request.IndexStrategy = indexStrategy

			// Log strategy explanation for debugging
			explanation := FormatStrategyExplanation(
				indexStrategy,
				estimatedHashTableSize,
				buildBundle.GetTotalDocuments(),
				probeBundle.GetTotalDocuments())
			dje.logger.Infof("Index optimization: %s", explanation)
		} else {
			dje.logger.Debugf("No beneficial index found for join keys %s/%s", buildKey, probeKey)
		}
	}

	// Evaluate all strategies and select the best one
	type strategyScore struct {
		strategy JoinStrategy
		cost     float64
	}

	var candidates []strategyScore

	for _, strategy := range dje.strategies {
		// Check if strategy supports the join type
		if !strategy.SupportsJoinType(request.JoinType) {
			dje.logger.Debugf("Strategy %s does not support join type %v",
				strategy.GetName(), request.JoinType)
			continue
		}

		// Get cost estimate
		cost, canHandle := strategy.EstimateCost(request)
		if !canHandle {
			dje.logger.Debugf("Strategy %s cannot handle this join",
				strategy.GetName())
			continue
		}

		candidates = append(candidates, strategyScore{
			strategy: strategy,
			cost:     cost,
		})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no strategy can handle this join request")
	}

	// Sort by cost (lower is better)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].cost < candidates[j].cost
	})

	bestStrategy := candidates[0].strategy

	// OPTIMIZATION: Bias toward hash join when costs are within 10% of each other
	// Hash join typically has better scalability and memory characteristics
	// OPTIMIZATION: Increased bias threshold from 10%→25% to more aggressively prefer hash join
	// Hash join's O(n+m) algorithmic advantage justifies higher tolerance vs nested loop O(n×m)
	// TODO: Make this threshold configurable via JoinRequest for query-specific tuning
	if len(candidates) > 1 {
		for i := 1; i < len(candidates); i++ {
			if candidates[i].strategy.GetName() == "HashJoin" {
				costDiff := (candidates[i].cost - candidates[0].cost) / candidates[0].cost
				if costDiff <= 0.25 { // Within 25% (was 10%)
					bestStrategy = candidates[i].strategy
					dje.logger.Debugf("Biasing toward HashJoin (cost diff: %.1f%%)", costDiff*100)
					break
				}
			}
		}
	}

	dje.logger.Debugf("Selected strategy %s with cost %.2f",
		bestStrategy.GetName(), candidates[0].cost)

	return bestStrategy, nil
}

// getPreferredStrategy returns the learned preferred strategy for this join pattern
func (dje *DefaultJoinExecutor) getPreferredStrategy(request *JoinRequest) JoinStrategy {
	stats := dje.patternTracker.GetJoinStats(
		request.LeftBundle.GetName(),
		request.RightBundle.GetName(),
	)

	if stats == nil || stats.PreferredAlgorithm == "" {
		return nil
	}

	// Find the strategy with the preferred algorithm name
	for _, strategy := range dje.strategies {
		if strategy.GetName() == stats.PreferredAlgorithm {
			return strategy
		}
	}

	return nil
}

// tryFallbackStrategy attempts to execute the join with a fallback strategy
func (dje *DefaultJoinExecutor) tryFallbackStrategy(request *JoinRequest, failedStrategy JoinStrategy) (*JoinResult, error) {
	dje.logger.Debugf("Attempting fallback strategy after %s failed", failedStrategy.GetName())

	// Try nested loop as fallback (usually the most reliable)
	for _, strategy := range dje.strategies {
		if strategy.GetName() == "NestedLoop" && strategy != failedStrategy {
			if strategy.SupportsJoinType(request.JoinType) {
				_, canHandle := strategy.EstimateCost(request)
				if canHandle {
					dje.logger.Infof("Using fallback strategy: %s", strategy.GetName())
					return strategy.Execute(request)
				}
			}
		}
	}

	return nil, fmt.Errorf("no suitable fallback strategy found")
}

// GetJoinStatistics returns comprehensive statistics about join performance
func (dje *DefaultJoinExecutor) GetJoinStatistics() JoinExecutorStatistics {
	hotPatterns := dje.patternTracker.GetHotJoinPatterns()
	recommendations := dje.patternTracker.GetOptimizationRecommendations()

	return JoinExecutorStatistics{
		RegisteredStrategies: len(dje.strategies),
		HotPatterns:          len(hotPatterns),
		Recommendations:      len(recommendations),
		PatternTracker:       dje.patternTracker,
	}
}

// JoinExecutorStatistics provides comprehensive statistics about the join executor
type JoinExecutorStatistics struct {
	RegisteredStrategies int         `json:"registered_strategies"` // Number of registered strategies
	HotPatterns          int         `json:"hot_patterns"`          // Number of hot join patterns
	Recommendations      int         `json:"recommendations"`       // Number of optimization recommendations
	PatternTracker       JoinMetrics `json:"-"`                     // Pattern tracker (not serialized)
}

// PHASE 2: Parallel execution coordination methods
/*
func (dje *DefaultJoinExecutor) SetMaxConcurrentJoins(max int) {
	dje.maxConcurrentJoins = max
	dje.logger.Infof("Set maximum concurrent joins to %d", max)
}

func (dje *DefaultJoinExecutor) GetJoinQueue() JoinQueue {
	return dje.joinQueue
}

type JoinQueue interface {
	Enqueue(request *JoinRequest) (string, error)
	GetStatus(jobID string) JoinJobStatus
	Cancel(jobID string) error
	GetQueueSize() int
}

type JoinJobStatus struct {
	ID       string
	Status   string // "queued", "running", "completed", "failed"
	Progress float64
	Result   *JoinResult
	Error    error
}
*/

// PHASE 3: Relationship-aware optimization methods
/*
func (dje *DefaultJoinExecutor) UpdateRelationshipMetadata(metadata RelationshipMetadata) {
	dje.relationshipMetadata = metadata
	dje.logger.Infof("Updated relationship metadata")
}

func (dje *DefaultJoinExecutor) GetJoinRecommendations(bundles []string) []JoinRecommendation {
	var recommendations []JoinRecommendation

	// Analyze bundle relationships and suggest optimal join orders
	for i := 0; i < len(bundles); i++ {
		for j := i+1; j < len(bundles); j++ {
			if dje.relationshipMetadata != nil {
				relType := dje.relationshipMetadata.GetRelationshipType(bundles[i], bundles[j])
				// Generate recommendations based on relationship type
			}
		}
	}

	return recommendations
}

type JoinRecommendation struct {
	LeftBundle       string
	RightBundle      string
	RecommendedOrder int
	Algorithm        string
	Reasoning        string
}
*/

// PHASE 4: Advanced caching and optimization methods
/*
func (dje *DefaultJoinExecutor) EnableResultCaching(enabled bool) {
	dje.cachingEnabled = enabled
	if enabled && dje.resultCache == nil {
		dje.resultCache = NewJoinCache(dje.logger)
	}
	dje.logger.Infof("Result caching %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

func (dje *DefaultJoinExecutor) ClearJoinCache() {
	if dje.resultCache != nil {
		dje.resultCache.Clear()
		dje.logger.Infof("Join result cache cleared")
	}
}

func (dje *DefaultJoinExecutor) executeWithCaching(request *JoinRequest) (*JoinResult, error) {
	if dje.cachingEnabled && dje.resultCache != nil {
		if result, found := dje.resultCache.Get(request); found {
			dje.logger.Debugf("Cache hit for join: %s ⋈ %s",
				request.LeftBundle.GetName(), request.RightBundle.GetName())
			return result, nil
		}
	}

	// Execute normally and cache result
	result, err := dje.executeNormally(request)
	if err == nil && dje.cachingEnabled && dje.resultCache != nil {
		dje.resultCache.Put(request, result)
	}

	return result, err
}
*/
