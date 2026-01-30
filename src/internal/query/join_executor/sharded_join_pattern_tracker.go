/*
SHARDED JOIN PATTERN TRACKER - High-Concurrency Join Pattern Learning

This file provides a sharded implementation of JoinPatternTracker that distributes
lock contention across 128 shards, eliminating the bottleneck caused by a single
global mutex on every join operation.

PROBLEM SOLVED:
The original JoinPatternTracker used a single mutex that ALL joins hit during:
- RecordJoin() - called after EVERY JOIN completion (150 concurrent joins)
- GetJoinStats() - called for cost estimation on hot path
This created severe convoy effects under high-concurrency workloads.

DESIGN:
- 128 shards (baseline, can scale to 256 if needed)
- Each shard has its own RWMutex protecting a map[string]*JoinPatternStats
- Shard selection via xxhash(patternKey) & 127 for O(1) distribution
- Fast path: RLock for reads (GetJoinStats), Lock for writes (RecordJoin)
- Atomic counters for global stats (totalJoins)
- Maintains JoinMetrics interface for drop-in replacement

PERFORMANCE CHARACTERISTICS:
- 150 join operations → ~1.17 joins per shard average
- Reads (GetJoinStats): concurrent within same shard
- Writes (RecordJoin): exclusive but only blocks same-shard operations
- Expected ~128x reduction in lock contention vs non-sharded version

USAGE:
  tracker := NewShardedJoinPatternTracker(logger, hotThreshold, optimizeAfter)
  tracker.RecordJoin("users", "orders", joinResult)
  stats := tracker.GetJoinStats("users", "orders")
*/

package joinexecutor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

const (
	// ShardedJoinPatternTrackerShards is the number of shards for distributing join pattern tracking.
	// 128 shards as baseline (can scale to 256 if contention persists).
	ShardedJoinPatternTrackerShards = 128
)

// joinPatternShard holds a partition of join pattern statistics
type joinPatternShard struct {
	mu       sync.RWMutex
	patterns map[string]*JoinPatternStats
}

// ShardedJoinPatternTracker learns join patterns and identifies frequently executed joins
// with sharding to support high concurrency (150+ connections).
type ShardedJoinPatternTracker struct {
	shards [ShardedJoinPatternTrackerShards]joinPatternShard

	// Configuration parameters (immutable after init)
	logger            *zap.SugaredLogger
	hotThreshold      int
	optimizeAfter     int
	maxPatternHistory int

	// Cleanup parameters
	cleanupInterval time.Duration
	maxAge          time.Duration
	lastCleanup     atomic.Value // stores time.Time

	// Global statistics (atomic for lock-free updates)
	totalJoins    atomic.Int64
	startTime     time.Time
	lastOptimized atomic.Value // stores time.Time
}

// NewShardedJoinPatternTracker creates a new sharded join pattern tracker.
// This is a drop-in replacement for NewJoinPatternTracker with identical method signatures.
func NewShardedJoinPatternTracker(logger *zap.SugaredLogger, hotThreshold, optimizeAfter int) *ShardedJoinPatternTracker {
	tracker := &ShardedJoinPatternTracker{
		logger:            logger,
		hotThreshold:      hotThreshold,
		optimizeAfter:     optimizeAfter,
		maxPatternHistory: 500,            // Track up to 500 different patterns
		cleanupInterval:   time.Hour * 2,  // Clean up every 2 hours
		maxAge:            time.Hour * 48, // Keep data for 48 hours
		startTime:         time.Now(),
	}

	// Initialize all shards
	for i := range tracker.shards {
		tracker.shards[i].patterns = make(map[string]*JoinPatternStats)
	}

	// Initialize atomic time values
	now := time.Now()
	tracker.lastCleanup.Store(now)
	tracker.lastOptimized.Store(now)

	return tracker
}

// createPatternKey creates a unique key for a join pattern
func (jpt *ShardedJoinPatternTracker) createPatternKey(leftBundle, rightBundle string) string {
	// Use consistent ordering to ensure same pattern regardless of join direction
	if leftBundle < rightBundle {
		return leftBundle + "⋈" + rightBundle
	}
	return rightBundle + "⋈" + leftBundle
}

// shardIndex returns the shard index for a given pattern key
func (jpt *ShardedJoinPatternTracker) shardIndex(patternKey string) uint64 {
	return xxhash.Sum64String(patternKey) & (ShardedJoinPatternTrackerShards - 1)
}

// RecordJoin records metrics for a completed join operation.
// Only locks the target shard, allowing concurrent join tracking on different patterns.
func (jpt *ShardedJoinPatternTracker) RecordJoin(leftBundle, rightBundle string, result *JoinResult) {
	// Create pattern key
	patternKey := jpt.createPatternKey(leftBundle, rightBundle)

	idx := jpt.shardIndex(patternKey)
	shard := &jpt.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Get or create pattern stats
	stats, exists := shard.patterns[patternKey]
	if !exists {
		stats = &JoinPatternStats{
			LeftBundle:         leftBundle,
			RightBundle:        rightBundle,
			AlgorithmStats:     make(map[string]*AlgorithmStats),
			JoinConditions:     make(map[string]int64),
			FirstSeen:          time.Now(),
			PreferredAlgorithm: result.Algorithm,
		}
		shard.patterns[patternKey] = stats
		jpt.logger.Debugf("Started tracking new join pattern: %s", patternKey)
	}

	// Update execution statistics
	stats.ExecutionCount++
	stats.TotalExecutionTime += result.ExecutionTime
	stats.AverageTime = stats.TotalExecutionTime / time.Duration(stats.ExecutionCount)
	stats.LastExecuted = time.Now()

	// Update memory usage statistics
	stats.TotalMemoryUsed += result.MemoryUsed
	stats.AverageMemoryUsage = stats.TotalMemoryUsed / stats.ExecutionCount

	// Update disk spill frequency
	if result.DiskSpilled {
		stats.DiskSpillFrequency = (stats.DiskSpillFrequency*float64(stats.ExecutionCount-1) + 1.0) / float64(stats.ExecutionCount)
	} else {
		stats.DiskSpillFrequency = stats.DiskSpillFrequency * float64(stats.ExecutionCount-1) / float64(stats.ExecutionCount)
	}

	// Update algorithm-specific statistics
	algStats, algExists := stats.AlgorithmStats[result.Algorithm]
	if !algExists {
		algStats = &AlgorithmStats{
			Name:        result.Algorithm,
			SuccessRate: 1.0,
		}
		stats.AlgorithmStats[result.Algorithm] = algStats
	}

	algStats.ExecutionCount++
	algStats.TotalTime += result.ExecutionTime
	algStats.AverageTime = algStats.TotalTime / time.Duration(algStats.ExecutionCount)
	algStats.MemoryUsage = (algStats.MemoryUsage*int64(algStats.ExecutionCount-1) + result.MemoryUsed) / algStats.ExecutionCount

	if result.DiskSpilled {
		algStats.DiskSpillRate = (algStats.DiskSpillRate*float64(algStats.ExecutionCount-1) + 1.0) / float64(algStats.ExecutionCount)
	} else {
		algStats.DiskSpillRate = algStats.DiskSpillRate * float64(algStats.ExecutionCount-1) / float64(algStats.ExecutionCount)
	}

	// Update preferred algorithm based on performance
	jpt.updatePreferredAlgorithm(stats)

	// Calculate selectivity
	if result.LeftScanned > 0 && result.RightScanned > 0 {
		selectivity := float64(len(result.Documents)) / float64(result.LeftScanned+result.RightScanned)
		stats.AverageSelectivity = (stats.AverageSelectivity*float64(stats.ExecutionCount-1) + selectivity) / float64(stats.ExecutionCount)
	}

	// Increment global counter atomically
	jpt.totalJoins.Add(1)

	// Log hot patterns
	if stats.ExecutionCount == int64(jpt.hotThreshold) {
		jpt.logger.Infof("Join pattern is now HOT: %s (executed %d times, avg time: %v)",
			patternKey, stats.ExecutionCount, stats.AverageTime)
	}

	// Periodic cleanup (check without blocking)
	lastCleanup := jpt.lastCleanup.Load().(time.Time)
	if time.Since(lastCleanup) > jpt.cleanupInterval {
		go jpt.cleanup()
	}

	jpt.logger.Debugf("Recorded join: %s -> %s (exec: %d, avg: %v, mem: %d)",
		leftBundle, rightBundle, stats.ExecutionCount, stats.AverageTime, stats.AverageMemoryUsage)
}

// GetJoinStats returns statistics for joins between specific bundles.
// Only locks the target shard for reading.
func (jpt *ShardedJoinPatternTracker) GetJoinStats(leftBundle, rightBundle string) *JoinStats {
	patternKey := jpt.createPatternKey(leftBundle, rightBundle)
	idx := jpt.shardIndex(patternKey)
	shard := &jpt.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	stats, exists := shard.patterns[patternKey]
	if !exists {
		return nil
	}

	return &JoinStats{
		ExecutionCount:     stats.ExecutionCount,
		AverageTime:        stats.AverageTime,
		TotalTime:          stats.TotalExecutionTime,
		LastExecuted:       stats.LastExecuted,
		PreferredAlgorithm: stats.PreferredAlgorithm,
		AverageMemoryUsage: stats.AverageMemoryUsage,
		DiskSpillFrequency: stats.DiskSpillFrequency,
	}
}

// GetHotJoinPatterns returns frequently executed join patterns.
// Aggregates across all shards.
func (jpt *ShardedJoinPatternTracker) GetHotJoinPatterns() []JoinPattern {
	var hotPatterns []JoinPattern

	// Collect hot patterns from all shards
	for i := range jpt.shards {
		shard := &jpt.shards[i]
		shard.mu.RLock()

		for _, stats := range shard.patterns {
			if stats.ExecutionCount >= int64(jpt.hotThreshold) {
				pattern := JoinPattern{
					LeftBundle:  stats.LeftBundle,
					RightBundle: stats.RightBundle,
					JoinKeys:    stats.JoinKeys,
					Frequency:   stats.ExecutionCount,
					Performance: stats.AverageTime,
				}
				hotPatterns = append(hotPatterns, pattern)
			}
		}

		shard.mu.RUnlock()
	}

	jpt.logger.Debugf("Found %d hot join patterns", len(hotPatterns))
	return hotPatterns
}

// GetOptimizationRecommendations suggests optimizations for join patterns.
// Aggregates across all shards.
func (jpt *ShardedJoinPatternTracker) GetOptimizationRecommendations() []OptimizationRecommendation {
	var recommendations []OptimizationRecommendation

	// Collect recommendations from all shards
	for i := range jpt.shards {
		shard := &jpt.shards[i]
		shard.mu.RLock()

		for patternKey, stats := range shard.patterns {
			if stats.ExecutionCount >= int64(jpt.optimizeAfter) {
				// Recommend optimization based on pattern characteristics
				if stats.DiskSpillFrequency > 0.5 {
					recommendations = append(recommendations, OptimizationRecommendation{
						PatternKey:    patternKey,
						Type:          "MemoryOptimization",
						Description:   "High disk spill frequency - consider increasing memory limit",
						Priority:      "High",
						EstimatedGain: stats.DiskSpillFrequency * 0.3,
					})
				}

				if stats.AverageTime > time.Second*5 {
					recommendations = append(recommendations, OptimizationRecommendation{
						PatternKey:    patternKey,
						Type:          "AlgorithmOptimization",
						Description:   "Slow join performance - consider different algorithm",
						Priority:      "Medium",
						EstimatedGain: 0.2,
					})
				}
			}
		}

		shard.mu.RUnlock()
	}

	jpt.logger.Debugf("Generated %d optimization recommendations", len(recommendations))
	return recommendations
}

// updatePreferredAlgorithm updates the preferred algorithm based on performance.
// Caller must hold shard lock.
func (jpt *ShardedJoinPatternTracker) updatePreferredAlgorithm(stats *JoinPatternStats) {
	var bestAlgorithm string
	var bestScore float64

	for _, algStats := range stats.AlgorithmStats {
		// Calculate score based on speed and success rate
		score := algStats.SuccessRate / float64(algStats.AverageTime.Milliseconds())

		if score > bestScore {
			bestScore = score
			bestAlgorithm = algStats.Name
		}
	}

	if bestAlgorithm != "" && bestAlgorithm != stats.PreferredAlgorithm {
		jpt.logger.Infof("Preferred algorithm updated for pattern %s⋈%s: %s -> %s",
			stats.LeftBundle, stats.RightBundle, stats.PreferredAlgorithm, bestAlgorithm)
		stats.PreferredAlgorithm = bestAlgorithm
	}
}

// cleanup removes old patterns and performs maintenance.
// This method locks each shard sequentially (runs in background goroutine).
func (jpt *ShardedJoinPatternTracker) cleanup() {
	// Update last cleanup time atomically (acts as a lightweight lock)
	now := time.Now()
	oldLastCleanup := jpt.lastCleanup.Load().(time.Time)
	if !jpt.lastCleanup.CompareAndSwap(oldLastCleanup, now) {
		// Another goroutine is already running cleanup
		return
	}

	totalRemoved := 0

	// Clean each shard sequentially
	for i := range jpt.shards {
		shard := &jpt.shards[i]
		shard.mu.Lock()

		removed := 0

		// Remove old patterns
		for key, stats := range shard.patterns {
			if now.Sub(stats.LastExecuted) > jpt.maxAge {
				delete(shard.patterns, key)
				removed++
			}
		}

		// If this shard still has too many patterns, remove least frequently used
		maxPatternsPerShard := jpt.maxPatternHistory / ShardedJoinPatternTrackerShards
		if len(shard.patterns) > maxPatternsPerShard {
			type patternEntry struct {
				key   string
				count int64
			}
			entries := make([]patternEntry, 0, len(shard.patterns))
			for key, stats := range shard.patterns {
				entries = append(entries, patternEntry{key: key, count: stats.ExecutionCount})
			}

			// Sort by execution count (ascending) - least used first
			for i := 0; i < len(entries)-1; i++ {
				for j := i + 1; j < len(entries); j++ {
					if entries[j].count < entries[i].count {
						entries[i], entries[j] = entries[j], entries[i]
					}
				}
			}

			// Remove excess patterns
			excess := len(shard.patterns) - maxPatternsPerShard
			for i := 0; i < excess && i < len(entries); i++ {
				delete(shard.patterns, entries[i].key)
				removed++
			}
		}

		totalRemoved += removed
		shard.mu.Unlock()
	}

	if totalRemoved > 0 {
		jpt.logger.Debugf("Cleaned up %d old join patterns", totalRemoved)
	}
}

// IntegrateWithHotKeyTracker connects this JOIN tracker with existing hot key trackers.
// Aggregates across all shards.
func (jpt *ShardedJoinPatternTracker) IntegrateWithHotKeyTracker(leftTracker, rightTracker HotKeyTrackerInterface) {
	jpt.logger.Infof("Integrating JOIN pattern tracker with hot key trackers for enhanced optimization")

	// For each shard, analyze hot key overlap
	for i := range jpt.shards {
		shard := &jpt.shards[i]
		shard.mu.Lock()

		// Get hot keys from both bundles
		leftHotKeys := leftTracker.GetTopKeys(10)
		rightHotKeys := rightTracker.GetTopKeys(10)

		// Update recommendations for each pattern in this shard
		for patternKey, stats := range shard.patterns {
			jpt.updateJoinRecommendationsWithHotKeys(patternKey, stats, leftHotKeys, rightHotKeys)
		}

		shard.mu.Unlock()
	}

	totalPatterns := int64(0)
	for i := range jpt.shards {
		shard := &jpt.shards[i]
		shard.mu.RLock()
		totalPatterns += int64(len(shard.patterns))
		shard.mu.RUnlock()
	}

	jpt.logger.Debugf("Hot key integration complete for %d JOIN patterns", totalPatterns)
}

// updateJoinRecommendationsWithHotKeys updates join recommendations based on hot key information.
// Caller must hold shard lock.
func (jpt *ShardedJoinPatternTracker) updateJoinRecommendationsWithHotKeys(patternKey string, stats *JoinPatternStats, leftHotKeys, rightHotKeys []string) {
	hasLeftHotKeys := len(leftHotKeys) > 0
	hasRightHotKeys := len(rightHotKeys) > 0

	var recommendations []string

	if hasLeftHotKeys && hasRightHotKeys {
		recommendations = append(recommendations, "Consider hash join with hot key optimization")
		recommendations = append(recommendations, "Hot keys detected on both sides - consider partitioned join")
	} else if hasLeftHotKeys || hasRightHotKeys {
		hotSide := "left"
		if hasRightHotKeys {
			hotSide = "right"
		}
		recommendations = append(recommendations, fmt.Sprintf("Hot keys on %s side - consider using as build side", hotSide))
	}

	if len(recommendations) > 0 {
		jpt.logger.Debugf("Updated JOIN pattern %s with hot key recommendations: %v", patternKey, recommendations)
	}
}

// GetHotKeyAwareRecommendations provides JOIN optimization recommendations considering hot key patterns.
func (jpt *ShardedJoinPatternTracker) GetHotKeyAwareRecommendations(leftBundle, rightBundle string, leftTracker, rightTracker HotKeyTrackerInterface) []OptimizationRecommendation {
	patternKey := jpt.createPatternKey(leftBundle, rightBundle)
	var recommendations []OptimizationRecommendation

	// Add hot key aware recommendations
	if leftTracker != nil && rightTracker != nil {
		leftHotKeys := leftTracker.GetTopKeys(5)
		rightHotKeys := rightTracker.GetTopKeys(5)

		if len(leftHotKeys) > 0 || len(rightHotKeys) > 0 {
			var description string
			var estimatedGain float64

			if len(leftHotKeys) > len(rightHotKeys) {
				description = "Use right bundle as build side due to left hot key concentration (15-25% improvement)"
				estimatedGain = 0.20
			} else if len(rightHotKeys) > len(leftHotKeys) {
				description = "Use left bundle as build side due to right hot key concentration (15-25% improvement)"
				estimatedGain = 0.20
			} else {
				description = "Consider partitioned hash join to leverage hot key locality (10-20% improvement)"
				estimatedGain = 0.15
			}

			hotKeyRec := OptimizationRecommendation{
				PatternKey:    patternKey,
				Type:          "hot_key_optimization",
				Priority:      "medium",
				Description:   fmt.Sprintf("Hot key integration available: left=%d keys, right=%d keys. %s", len(leftHotKeys), len(rightHotKeys), description),
				EstimatedGain: estimatedGain,
			}

			recommendations = append(recommendations, hotKeyRec)
		}
	}

	return recommendations
}

// IsJoinPatternHot determines if a JOIN pattern should be considered "hot" based on usage.
// Only locks the target shard for reading.
func (jpt *ShardedJoinPatternTracker) IsJoinPatternHot(leftBundle, rightBundle string) bool {
	patternKey := jpt.createPatternKey(leftBundle, rightBundle)
	idx := jpt.shardIndex(patternKey)
	shard := &jpt.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	stats, exists := shard.patterns[patternKey]
	if !exists {
		return false
	}

	return stats.ExecutionCount >= int64(jpt.hotThreshold)
}

// GetJoinHotness returns a score (0.0-1.0) indicating how "hot" a JOIN pattern is.
// Only locks the target shard for reading.
func (jpt *ShardedJoinPatternTracker) GetJoinHotness(leftBundle, rightBundle string) float64 {
	patternKey := jpt.createPatternKey(leftBundle, rightBundle)
	idx := jpt.shardIndex(patternKey)
	shard := &jpt.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	stats, exists := shard.patterns[patternKey]
	if !exists {
		return 0.0
	}

	// Calculate hotness based on execution frequency and recency
	frequencyScore := float64(stats.ExecutionCount) / float64(jpt.hotThreshold)
	if frequencyScore > 1.0 {
		frequencyScore = 1.0
	}

	// Recent executions get higher scores
	timeSinceLastExecution := time.Since(stats.LastExecuted)
	recencyScore := 1.0 - (float64(timeSinceLastExecution) / float64(jpt.maxAge))
	if recencyScore < 0.0 {
		recencyScore = 0.0
	}

	// Combine frequency and recency (weight frequency more heavily)
	hotness := (0.7 * frequencyScore) + (0.3 * recencyScore)
	return hotness
}
