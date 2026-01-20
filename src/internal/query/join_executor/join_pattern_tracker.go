package joinexecutor

import (
	"fmt" // NEW: For string formatting in hot key integration
	"sync"
	"time"

	"go.uber.org/zap"
)

// JoinPatternTracker extends the HotKeyTracker concept to learn and optimize join patterns
// This tracks which bundle pairs are joined frequently and learns optimal strategies
type JoinPatternTracker struct {
	patterns map[string]*JoinPatternStats // Pattern statistics keyed by bundle pair
	mutex    sync.RWMutex                 // Protects concurrent access
	logger   *zap.SugaredLogger           // Logger for debugging

	// Configuration parameters
	hotThreshold      int           // Executions needed to consider pattern "hot"
	optimizeAfter     int           // When to suggest optimization
	maxPatternHistory int           // Maximum patterns to track
	cleanupInterval   time.Duration // How often to clean old data
	maxAge            time.Duration // Maximum age for tracked patterns
	lastCleanup       time.Time     // When we last cleaned up

	// Global statistics
	totalJoins    int64     // Total joins processed
	startTime     time.Time // When tracking started
	lastOptimized time.Time // When we last ran optimization
}

// JoinPatternStats tracks statistics for joins between two specific bundles
type JoinPatternStats struct {
	LeftBundle  string `json:"left_bundle"`  // Left bundle name
	RightBundle string `json:"right_bundle"` // Right bundle name

	// Execution statistics
	ExecutionCount     int64         `json:"execution_count"`      // Number of times executed
	TotalExecutionTime time.Duration `json:"total_execution_time"` // Total time spent
	AverageTime        time.Duration `json:"average_time"`         // Average execution time
	LastExecuted       time.Time     `json:"last_executed"`        // When last executed
	FirstSeen          time.Time     `json:"first_seen"`           // When first seen

	// Algorithm performance tracking
	AlgorithmStats     map[string]*AlgorithmStats `json:"algorithm_stats"`     // Performance by algorithm
	PreferredAlgorithm string                     `json:"preferred_algorithm"` // Best performing algorithm

	// Resource usage patterns
	AverageMemoryUsage int64   `json:"average_memory_usage"` // Average memory usage
	DiskSpillFrequency float64 `json:"disk_spill_frequency"` // How often disk spillover occurs
	TotalMemoryUsed    int64   `json:"total_memory_used"`    // Total memory used across all executions

	// Join characteristics
	JoinKeys           []string         `json:"join_keys"`            // Keys used for joining
	JoinConditions     map[string]int64 `json:"join_conditions"`      // Condition frequency
	AverageSelectivity float64          `json:"average_selectivity"`  // Average result selectivity
	ResultSizeVariance float64          `json:"result_size_variance"` // Variance in result sizes

	// PHASE 3: Relationship metadata (to be implemented)
	// DetectedRelationshipType string  `json:"detected_relationship_type"` // 1:1, 1:many, etc.
	// CardinalityEstimate     int64    `json:"cardinality_estimate"`       // Estimated cardinality
	// RelationshipConfidence  float64  `json:"relationship_confidence"`    // Confidence in detection
}

// AlgorithmStats tracks performance statistics for a specific algorithm
type AlgorithmStats struct {
	Name           string        `json:"name"`            // Algorithm name
	ExecutionCount int64         `json:"execution_count"` // Number of executions
	TotalTime      time.Duration `json:"total_time"`      // Total execution time
	AverageTime    time.Duration `json:"average_time"`    // Average execution time
	SuccessRate    float64       `json:"success_rate"`    // Success rate (0.0 - 1.0)
	MemoryUsage    int64         `json:"memory_usage"`    // Average memory usage
	DiskSpillRate  float64       `json:"disk_spill_rate"` // How often disk spillover occurs
}

// NewJoinPatternTracker creates a new join pattern tracker
// logger: Logger for debugging and monitoring
// hotThreshold: Executions needed to consider a pattern "hot"
// optimizeAfter: Number of executions before suggesting optimization
func NewJoinPatternTracker(logger *zap.SugaredLogger, hotThreshold, optimizeAfter int) *JoinPatternTracker {
	return &JoinPatternTracker{
		patterns:          make(map[string]*JoinPatternStats),
		logger:            logger,
		hotThreshold:      hotThreshold,
		optimizeAfter:     optimizeAfter,
		maxPatternHistory: 500,            // Track up to 500 different patterns
		cleanupInterval:   time.Hour * 2,  // Clean up every 2 hours
		maxAge:            time.Hour * 48, // Keep data for 48 hours
		startTime:         time.Now(),
		lastCleanup:       time.Now(),
		lastOptimized:     time.Now(),
	}
}

// RecordJoin records metrics for a completed join operation
func (jpt *JoinPatternTracker) RecordJoin(leftBundle, rightBundle string, result *JoinResult) {
	jpt.mutex.Lock()
	defer jpt.mutex.Unlock()

	// Create pattern key
	patternKey := jpt.createPatternKey(leftBundle, rightBundle)

	// Get or create pattern stats
	stats, exists := jpt.patterns[patternKey]
	if !exists {
		stats = &JoinPatternStats{
			LeftBundle:         leftBundle,
			RightBundle:        rightBundle,
			AlgorithmStats:     make(map[string]*AlgorithmStats),
			JoinConditions:     make(map[string]int64),
			FirstSeen:          time.Now(),
			PreferredAlgorithm: result.Algorithm,
		}
		jpt.patterns[patternKey] = stats
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

	// Increment global counter
	jpt.totalJoins++

	// Log hot patterns
	if stats.ExecutionCount == int64(jpt.hotThreshold) {
		jpt.logger.Infof("Join pattern is now HOT: %s (executed %d times, avg time: %v)",
			patternKey, stats.ExecutionCount, stats.AverageTime)
	}

	// Periodic cleanup
	if time.Since(jpt.lastCleanup) > jpt.cleanupInterval {
		jpt.cleanup()
	}

	jpt.logger.Debugf("Recorded join: %s -> %s (exec: %d, avg: %v, mem: %d)",
		leftBundle, rightBundle, stats.ExecutionCount, stats.AverageTime, stats.AverageMemoryUsage)
}

// GetJoinStats returns statistics for joins between specific bundles
func (jpt *JoinPatternTracker) GetJoinStats(leftBundle, rightBundle string) *JoinStats {
	jpt.mutex.RLock()
	defer jpt.mutex.RUnlock()

	patternKey := jpt.createPatternKey(leftBundle, rightBundle)
	stats, exists := jpt.patterns[patternKey]
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

// GetHotJoinPatterns returns frequently executed join patterns
func (jpt *JoinPatternTracker) GetHotJoinPatterns() []JoinPattern {
	jpt.mutex.RLock()
	defer jpt.mutex.RUnlock()

	var hotPatterns []JoinPattern

	for _, stats := range jpt.patterns {
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

	jpt.logger.Debugf("Found %d hot join patterns", len(hotPatterns))
	return hotPatterns
}

// GetOptimizationRecommendations suggests optimizations for join patterns
func (jpt *JoinPatternTracker) GetOptimizationRecommendations() []OptimizationRecommendation {
	jpt.mutex.RLock()
	defer jpt.mutex.RUnlock()

	var recommendations []OptimizationRecommendation

	for patternKey, stats := range jpt.patterns {
		if stats.ExecutionCount >= int64(jpt.optimizeAfter) {
			// Recommend optimization based on pattern characteristics
			if stats.DiskSpillFrequency > 0.5 {
				recommendations = append(recommendations, OptimizationRecommendation{
					PatternKey:    patternKey,
					Type:          "MemoryOptimization",
					Description:   "High disk spill frequency - consider increasing memory limit",
					Priority:      "High",
					EstimatedGain: stats.DiskSpillFrequency * 0.3, // Estimated 30% improvement
				})
			}

			if stats.AverageTime > time.Second*5 {
				recommendations = append(recommendations, OptimizationRecommendation{
					PatternKey:    patternKey,
					Type:          "AlgorithmOptimization",
					Description:   "Slow join performance - consider different algorithm",
					Priority:      "Medium",
					EstimatedGain: 0.2, // Estimated 20% improvement
				})
			}

			// PHASE 3: Add relationship-aware recommendations
			// if stats.DetectedRelationshipType == "1:1" {
			//     recommendations = append(recommendations, OptimizationRecommendation{
			//         Type: "RelationshipOptimization",
			//         Description: "1:1 relationship detected - consider specialized algorithm",
			//     })
			// }
		}
	}

	jpt.logger.Debugf("Generated %d optimization recommendations", len(recommendations))
	return recommendations
}

// createPatternKey creates a unique key for a join pattern
func (jpt *JoinPatternTracker) createPatternKey(leftBundle, rightBundle string) string {
	// Use consistent ordering to ensure same pattern regardless of join direction
	if leftBundle < rightBundle {
		return leftBundle + "⋈" + rightBundle
	}
	return rightBundle + "⋈" + leftBundle
}

// updatePreferredAlgorithm updates the preferred algorithm based on performance
func (jpt *JoinPatternTracker) updatePreferredAlgorithm(stats *JoinPatternStats) {
	var bestAlgorithm string
	var bestScore float64

	for _, algStats := range stats.AlgorithmStats {
		// Calculate score based on speed and success rate
		// Lower average time and higher success rate are better
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

// cleanup removes old patterns and performs maintenance
func (jpt *JoinPatternTracker) cleanup() {
	now := time.Now()
	removed := 0

	for key, stats := range jpt.patterns {
		if now.Sub(stats.LastExecuted) > jpt.maxAge {
			delete(jpt.patterns, key)
			removed++
		}
	}

	// If we still have too many patterns, remove the least frequently used (LFU eviction)
	if len(jpt.patterns) > jpt.maxPatternHistory {
		// Build a slice of patterns sorted by execution count (ascending)
		type patternEntry struct {
			key   string
			count int64
		}
		entries := make([]patternEntry, 0, len(jpt.patterns))
		for key, stats := range jpt.patterns {
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

		// Remove excess patterns (least frequently used)
		excess := len(jpt.patterns) - jpt.maxPatternHistory
		for i := 0; i < excess && i < len(entries); i++ {
			delete(jpt.patterns, entries[i].key)
			removed++
		}
		jpt.logger.Debugf("LFU eviction: removed %d least-used join patterns (limit: %d)",
			excess, jpt.maxPatternHistory)
	}

	jpt.lastCleanup = now
	if removed > 0 {
		jpt.logger.Debugf("Cleaned up %d old join patterns", removed)
	}
}

// OptimizationRecommendation suggests specific optimizations for join patterns
type OptimizationRecommendation struct {
	PatternKey    string  `json:"pattern_key"`    // Join pattern identifier
	Type          string  `json:"type"`           // Type of optimization
	Description   string  `json:"description"`    // Human-readable description
	Priority      string  `json:"priority"`       // Priority level (High, Medium, Low)
	EstimatedGain float64 `json:"estimated_gain"` // Estimated performance improvement (0.0-1.0)

	// PHASE 3: Add more detailed recommendations
	// Actions       []string `json:"actions"`        // Specific actions to take
	// Dependencies  []string `json:"dependencies"`   // Dependencies for this optimization
	// Risk          string   `json:"risk"`           // Risk level of the optimization
}

// PHASE 2: Interfaces for advanced pattern analysis
/*
type PatternAnalyzer interface {
	AnalyzeJoinPattern(stats *JoinPatternStats) PatternAnalysis
	PredictOptimalAlgorithm(leftBundle, rightBundle string) string
	EstimateJoinCost(leftBundle, rightBundle string, algorithm string) float64
}

type PatternAnalysis struct {
	OptimalAlgorithm   string
	MemoryRequirement  int64
	ExpectedPerformance time.Duration
	RiskFactors        []string
}
*/

// PHASE 3: Relationship detection and optimization
/*
type RelationshipDetector interface {
	DetectRelationshipType(leftBundle, rightBundle string, joinKey string) RelationshipType
	EstimateCardinality(leftBundle, rightBundle string, joinKey string) (int64, int64)
	GetRelationshipStrength(leftBundle, rightBundle string) float64
}

func (jpt *JoinPatternTracker) EnableRelationshipDetection(detector RelationshipDetector) {
	jpt.relationshipDetector = detector
}
*/

// PHASE 4: Machine learning for pattern prediction
/*
type JoinPredictor interface {
	TrainModel(patterns []*JoinPatternStats)
	PredictPerformance(request *JoinRequest) PerformancePrediction
	RecommendOptimizations(pattern *JoinPatternStats) []OptimizationRecommendation
}

type PerformancePrediction struct {
	EstimatedTime   time.Duration
	EstimatedMemory int64
	RecommendedAlgorithm string
	Confidence      float64
}
*/

// NEW HOT KEY INTEGRATION: Methods to integrate with existing HotKeyTracker system
// This allows JOIN patterns to leverage hot key information for optimization

// HotKeyTrackerInterface defines the interface needed from the existing hot key tracker
// This avoids direct dependencies while allowing integration
type HotKeyTrackerInterface interface {
	GetKeyStats(key string) interface{} // Returns key statistics (typed as interface{} to avoid circular imports)
	IsHotKey(key string) bool           // Check if a key is considered "hot"
	GetTopKeys(n int) []string          // Get top N most queried keys
}

// IntegrateWithHotKeyTracker connects this JOIN tracker with existing hot key trackers
// This enables the JOIN system to leverage hot key information for better optimization
func (jpt *JoinPatternTracker) IntegrateWithHotKeyTracker(leftTracker, rightTracker HotKeyTrackerInterface) {
	jpt.mutex.Lock()
	defer jpt.mutex.Unlock()

	jpt.logger.Infof("Integrating JOIN pattern tracker with hot key trackers for enhanced optimization")

	// For each tracked join pattern, analyze hot key overlap
	for patternKey, stats := range jpt.patterns {
		// Get hot keys from both bundles
		leftHotKeys := leftTracker.GetTopKeys(10)   // Top 10 hot keys from left bundle
		rightHotKeys := rightTracker.GetTopKeys(10) // Top 10 hot keys from right bundle

		// Update recommendations based on hot key patterns
		jpt.updateJoinRecommendationsWithHotKeys(patternKey, stats, leftHotKeys, rightHotKeys)
	}

	jpt.logger.Debugf("Hot key integration complete for %d JOIN patterns", len(jpt.patterns))
}

// updateJoinRecommendationsWithHotKeys updates join recommendations based on hot key information
func (jpt *JoinPatternTracker) updateJoinRecommendationsWithHotKeys(patternKey string, stats *JoinPatternStats, leftHotKeys, rightHotKeys []string) {
	// If there are hot keys involved, prefer algorithms that benefit from locality
	hasLeftHotKeys := len(leftHotKeys) > 0
	hasRightHotKeys := len(rightHotKeys) > 0

	var recommendations []string

	if hasLeftHotKeys && hasRightHotKeys {
		// Both sides have hot keys - hash join might benefit from key locality
		recommendations = append(recommendations, "Consider hash join with hot key optimization")
		recommendations = append(recommendations, "Hot keys detected on both sides - consider partitioned join")
	} else if hasLeftHotKeys || hasRightHotKeys {
		// One side has hot keys - nested loop might benefit if small side has hot keys
		hotSide := "left"
		if hasRightHotKeys {
			hotSide = "right"
		}
		recommendations = append(recommendations, fmt.Sprintf("Hot keys on %s side - consider using as build side", hotSide))
	}

	// Update the pattern stats with hot key aware recommendations
	if len(recommendations) > 0 {
		jpt.logger.Debugf("Updated JOIN pattern %s with hot key recommendations: %v", patternKey, recommendations)
		// Note: Would store recommendations in stats structure if we extend JoinPatternStats
	}
}

// GetHotKeyAwareRecommendations provides JOIN optimization recommendations that consider hot key patterns
func (jpt *JoinPatternTracker) GetHotKeyAwareRecommendations(leftBundle, rightBundle string, leftTracker, rightTracker HotKeyTrackerInterface) []OptimizationRecommendation {
	jpt.mutex.RLock()
	defer jpt.mutex.RUnlock()

	patternKey := jpt.createPatternKey(leftBundle, rightBundle)

	var recommendations []OptimizationRecommendation

	// Add hot key aware recommendations
	if leftTracker != nil && rightTracker != nil {
		leftHotKeys := leftTracker.GetTopKeys(5)
		rightHotKeys := rightTracker.GetTopKeys(5)

		if len(leftHotKeys) > 0 || len(rightHotKeys) > 0 {
			var description string
			var estimatedGain float64

			// Determine specific recommendations based on hot key patterns
			if len(leftHotKeys) > len(rightHotKeys) {
				description = "Use right bundle as build side due to left hot key concentration (15-25% improvement)"
				estimatedGain = 0.20 // 20% improvement estimate
			} else if len(rightHotKeys) > len(leftHotKeys) {
				description = "Use left bundle as build side due to right hot key concentration (15-25% improvement)"
				estimatedGain = 0.20 // 20% improvement estimate
			} else {
				description = "Consider partitioned hash join to leverage hot key locality (10-20% improvement)"
				estimatedGain = 0.15 // 15% improvement estimate
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

// IsJoinPatternHot determines if a JOIN pattern should be considered "hot" based on usage
func (jpt *JoinPatternTracker) IsJoinPatternHot(leftBundle, rightBundle string) bool {
	jpt.mutex.RLock()
	defer jpt.mutex.RUnlock()

	patternKey := jpt.createPatternKey(leftBundle, rightBundle)
	stats, exists := jpt.patterns[patternKey]
	if !exists {
		return false
	}

	return stats.ExecutionCount >= int64(jpt.hotThreshold)
}

// GetJoinHotness returns a score (0.0-1.0) indicating how "hot" a JOIN pattern is
func (jpt *JoinPatternTracker) GetJoinHotness(leftBundle, rightBundle string) float64 {
	jpt.mutex.RLock()
	defer jpt.mutex.RUnlock()

	patternKey := jpt.createPatternKey(leftBundle, rightBundle)
	stats, exists := jpt.patterns[patternKey]
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
