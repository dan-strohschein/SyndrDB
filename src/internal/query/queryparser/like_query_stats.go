package queryparser

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

/*
like_query_stats.go

This file implements statistics collection and monitoring for LIKE and NOT LIKE query operators.
It tracks query patterns, execution metrics, and pattern type distribution to help optimize performance
and identify frequently-used LIKE queries for potential pattern caching.

Main Functions:
- RecordLikeQuery: Records statistics for a LIKE query execution
- GetLikeQueryStats: Retrieves all LIKE query statistics (Public API for monitoring)
- GetLikeQueryStatsJSON: Returns statistics as JSON (Public API for admin monitoring)
- ResetLikeQueryStats: Clears all statistics (Public API for admin management)
- pruneLikeStatsLRU: Removes least recently used stats when limit is exceeded

Key Implementation Details:
- Thread-safe statistics collection using sync.RWMutex
- LRU (Least Recently Used) eviction with hardcoded 10,000 entry limit
- Statistics aggregated by field name + pattern type combination
- Pattern normalization for consistent aggregation (consecutive % collapsed)
- Tracks execution time separately for case-sensitive and case-insensitive queries
- Warning deduplication tracking to log performance warnings once per unique pattern
- Ephemeral statistics (not persisted across server restarts)
- Monitors pattern types: prefix, suffix, contains, exact, match_all
- Tracks index usage potential (prefix patterns can use B-tree indexes)

Pattern Type Performance Characteristics:
- prefix: Fast with B-tree index (if available and case-sensitive)
- suffix: Requires full table scan
- contains: Requires full table scan (warn user)
- exact: Same as equality operator (==)
- match_all: No filtering needed (matches all records)

TODO: Integrate with full monitoring system via pub-sub when available
TODO: Add hot pattern detection for automatic query result caching
TODO: Export statistics in additional formats (CSV, Prometheus metrics)
TODO: Add configurable retention policies and pruning thresholds
TODO: Implement pattern caching for frequently used LIKE queries to avoid re-parsing
TODO: Implement query hint system for explicit index control
TODO: When full-text search is implemented, recommend SEARCH() for word-based matching
TODO: Implement trigram index optimization for contains/suffix patterns
*/

const (
	// Maximum number of unique LIKE query patterns to track
	maxLikeStatsEntries = 10000
)

// LikeQueryStats tracks statistics for a specific LIKE query pattern
// Aggregated by field name + pattern type combination
type LikeQueryStats struct {
	FieldName       string    `json:"field_name"`
	PatternType     string    `json:"pattern_type"`      // "prefix", "suffix", "contains", "exact", "match_all"
	PatternLength   int       `json:"pattern_length"`    // Average pattern length
	WildcardCount   int       `json:"wildcard_count"`    // Total wildcards across all executions
	ExecutionTime   int64     `json:"execution_time_ns"` // Total execution time in nanoseconds
	HitCount        int64     `json:"hit_count"`         // Number of successful matches
	MissCount       int64     `json:"miss_count"`        // Number of non-matches
	CaseInsensitive bool      `json:"case_insensitive"`  // Whether case-insensitive matching was used
	IndexUsed       bool      `json:"index_used"`        // Whether B-tree index was used (prefix patterns only)
	QueryCount      int64     `json:"query_count"`       // Number of times this pattern was executed
	LastAccessed    time.Time `json:"last_accessed"`
}

// LikeQueryStatsManager manages collection and access to LIKE query statistics
type LikeQueryStatsManager struct {
	stats          map[string]*LikeQueryStats // Key: hash of (fieldName + patternType + caseInsensitive)
	warnedPatterns map[string]bool            // Track patterns we've already warned about
	mutex          sync.RWMutex
	logger         *zap.SugaredLogger
}

var (
	// Global stats manager instance
	globalLikeStatsManager *LikeQueryStatsManager
	likeStatsOnce          sync.Once
)

// getLikeStatsManager returns the global stats manager instance (singleton)
func getLikeStatsManager() *LikeQueryStatsManager {
	likeStatsOnce.Do(func() {
		globalLikeStatsManager = &LikeQueryStatsManager{
			stats:          make(map[string]*LikeQueryStats),
			warnedPatterns: make(map[string]bool),
			logger:         zap.NewNop().Sugar(),
		}
	})
	return globalLikeStatsManager
}

// InitLikeStatsManager initializes the stats manager with a logger
func InitLikeStatsManager(logger *zap.SugaredLogger) {
	manager := getLikeStatsManager()
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.logger = logger
}

// RecordLikeQuery records statistics for a LIKE query execution
// Parameters:
//   - fieldName: The field being queried
//   - patternType: The pattern type (prefix/suffix/contains/exact/match_all)
//   - patternLength: Length of the pattern string
//   - executionTimeNs: Execution time in nanoseconds
//   - caseInsensitive: Whether case-insensitive matching was used
//   - matched: Whether the query found a match
//   - indexUsed: Whether a B-tree index was used (prefix patterns only)
//
// Statistics are aggregated by field name + pattern type + case sensitivity
// LRU pruning occurs when maxLikeStatsEntries is exceeded
func RecordLikeQuery(fieldName string, patternType string, patternLength int, executionTimeNs int64,
	caseInsensitive bool, matched bool, indexUsed bool) {

	manager := getLikeStatsManager()
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// Create key for aggregation: fieldName + patternType + caseInsensitive
	caseStr := "case_sensitive"
	if caseInsensitive {
		caseStr = "case_insensitive"
	}
	key := fmt.Sprintf("%s:%s:%s", fieldName, patternType, caseStr)

	// Get or create stats entry
	stats, exists := manager.stats[key]
	if !exists {
		stats = &LikeQueryStats{
			FieldName:       fieldName,
			PatternType:     patternType,
			PatternLength:   0,
			WildcardCount:   0,
			ExecutionTime:   0,
			HitCount:        0,
			MissCount:       0,
			CaseInsensitive: caseInsensitive,
			IndexUsed:       indexUsed,
			QueryCount:      0,
			LastAccessed:    time.Now(),
		}
		manager.stats[key] = stats

		// Prune LRU if needed
		if len(manager.stats) > maxLikeStatsEntries {
			manager.pruneLikeStatsLRU()
		}
	}

	// Update statistics
	stats.QueryCount++
	stats.ExecutionTime += executionTimeNs
	stats.LastAccessed = time.Now()

	// Update average pattern length
	totalLength := (stats.PatternLength * int(stats.QueryCount-1)) + patternLength
	stats.PatternLength = totalLength / int(stats.QueryCount)

	// Update hit/miss counts
	if matched {
		stats.HitCount++
	} else {
		stats.MissCount++
	}

	// Update index usage (true if any query used index)
	if indexUsed {
		stats.IndexUsed = true
	}
}

// pruneLikeStatsLRU removes the least recently used stats entry
// Called when maxLikeStatsEntries is exceeded
// Note: Caller must hold mutex lock
func (m *LikeQueryStatsManager) pruneLikeStatsLRU() {
	if len(m.stats) == 0 {
		return
	}

	// Find oldest entry
	var oldestKey string
	var oldestTime time.Time
	first := true

	for key, stats := range m.stats {
		if first || stats.LastAccessed.Before(oldestTime) {
			oldestKey = key
			oldestTime = stats.LastAccessed
			first = false
		}
	}

	// Remove oldest entry
	if oldestKey != "" {
		delete(m.stats, oldestKey)
		m.logger.Debugf("Pruned LRU LIKE query stats entry: %s (last accessed: %s)",
			oldestKey, oldestTime.Format(time.RFC3339))
	}
}

// GetLikeQueryStats retrieves all LIKE query statistics
// Returns a deep copy of the statistics map for thread-safety
// This is a public exported function for monitoring via future pub-sub system
//
// Returns:
//   - map[string]*LikeQueryStats: Deep copy of statistics keyed by field:patternType:caseSensitivity
func GetLikeQueryStats() map[string]*LikeQueryStats {
	manager := getLikeStatsManager()
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	// Create deep copy
	statsCopy := make(map[string]*LikeQueryStats, len(manager.stats))
	for key, stats := range manager.stats {
		statsCopy[key] = &LikeQueryStats{
			FieldName:       stats.FieldName,
			PatternType:     stats.PatternType,
			PatternLength:   stats.PatternLength,
			WildcardCount:   stats.WildcardCount,
			ExecutionTime:   stats.ExecutionTime,
			HitCount:        stats.HitCount,
			MissCount:       stats.MissCount,
			CaseInsensitive: stats.CaseInsensitive,
			IndexUsed:       stats.IndexUsed,
			QueryCount:      stats.QueryCount,
			LastAccessed:    stats.LastAccessed,
		}
	}

	return statsCopy
}

// GetLikeQueryStatsJSON returns LIKE query statistics as JSON
// This is a public exported function for admin monitoring via future pub-sub system
//
// Returns:
//   - string: JSON representation of statistics
//   - error: JSON marshaling error if any
//
// Note: In production, this should be restricted to admin users only via pub-sub permissions
func GetLikeQueryStatsJSON() (string, error) {
	stats := GetLikeQueryStats()

	jsonBytes, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal LIKE query stats to JSON: %w", err)
	}

	return string(jsonBytes), nil
}

// ResetLikeQueryStats clears all LIKE query statistics
// This is a public exported function for admin management via future pub-sub system
//
// Note: In production, this should be restricted to admin users only via pub-sub permissions
func ResetLikeQueryStats() {
	manager := getLikeStatsManager()
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// Clear all stats
	manager.stats = make(map[string]*LikeQueryStats)
	manager.warnedPatterns = make(map[string]bool)

	manager.logger.Info("LIKE query statistics reset")
}

// ShouldWarnAboutPattern checks if we should warn about a performance issue with this pattern
// Returns true if this is the first time we're seeing this pattern type
// Used to deduplicate performance warnings in query planner
//
// Parameters:
//   - fieldName: The field being queried
//   - patternType: The pattern type (prefix/suffix/contains/exact/match_all)
//   - caseInsensitive: Whether case-insensitive matching was used
//
// Returns:
//   - bool: true if warning should be logged, false if already warned
func ShouldWarnAboutPattern(fieldName string, patternType string, caseInsensitive bool) bool {
	manager := getLikeStatsManager()
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	caseStr := "case_sensitive"
	if caseInsensitive {
		caseStr = "case_insensitive"
	}
	key := fmt.Sprintf("%s:%s:%s", fieldName, patternType, caseStr)

	// Check if we've already warned
	if manager.warnedPatterns[key] {
		return false
	}

	// Mark as warned
	manager.warnedPatterns[key] = true
	return true
}
