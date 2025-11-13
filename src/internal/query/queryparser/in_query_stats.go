package queryparser

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

/*
in_query_stats.go

This file implements statistics collection and monitoring for IN and NOT IN query operators.
It tracks query patterns, execution metrics, and memory usage to help optimize performance
and identify frequently-used IN queries for potential caching.

Main Functions:
- RecordInQuery: Records statistics for an IN query execution
- GetInQueryStats: Retrieves all IN query statistics
- GetInQueryStatsJSON: Returns statistics as JSON (Admin-only, requires permission check)
- pruneLRU: Removes least recently used stats when limit is exceeded

Key Implementation Details:
- Thread-safe statistics collection using sync.RWMutex
- LRU (Least Recently Used) eviction with hardcoded 10,000 entry limit
- Memory usage tracking for hash set creation
- Ephemeral statistics (not persisted across server restarts)
- Tracks original vs deduplicated list sizes
- Records case-insensitive usage patterns
- Monitors index vs scan strategy selection

TODO: Integrate with full monitoring system when available
TODO: Add hot pattern detection for automatic query result caching
TODO: Export statistics in additional formats (CSV, Prometheus metrics)
TODO: Add configurable retention policies and pruning thresholds
*/

const (
	// Maximum number of unique IN query patterns to track
	maxStatsEntries = 10000
)

// InQueryStats tracks statistics for a specific IN query pattern
type InQueryStats struct {
	FieldName            string    `json:"field_name"`
	ListSizeOriginal     int       `json:"list_size_original"`     // Original list size before deduplication
	ListSizeDeduplicated int       `json:"list_size_deduplicated"` // List size after deduplication
	ExecutionTime        int64     `json:"execution_time_ns"`      // Execution time in nanoseconds
	MemoryUsageBytes     uint64    `json:"memory_usage_bytes"`     // Memory used for hash set
	HitCount             int64     `json:"hit_count"`              // Number of times this pattern was executed
	MissCount            int64     `json:"miss_count"`             // Number of non-matches
	CaseInsensitive      bool      `json:"case_insensitive"`       // Whether case-insensitive matching was used
	SingleValueOptimized bool      `json:"single_value_optimized"` // Whether single-value optimization was applied
	Strategy             string    `json:"strategy"`               // "index" or "scan"
	LastAccessed         time.Time `json:"last_accessed"`
}

// InQueryStatsManager manages collection and access to IN query statistics
type InQueryStatsManager struct {
	stats  map[string]*InQueryStats // Key: hash of (fieldName + list size + case sensitivity)
	mutex  sync.RWMutex
	logger *zap.SugaredLogger
}

var (
	// Global stats manager instance
	globalStatsManager *InQueryStatsManager
	statsOnce          sync.Once
)

// getStatsManager returns the global stats manager instance (singleton)
func getStatsManager() *InQueryStatsManager {
	statsOnce.Do(func() {
		globalStatsManager = &InQueryStatsManager{
			stats:  make(map[string]*InQueryStats),
			logger: zap.NewNop().Sugar(),
		}
	})
	return globalStatsManager
}

// InitStatsManager initializes the stats manager with a logger
func InitStatsManager(logger *zap.SugaredLogger) {
	manager := getStatsManager()
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.logger = logger
}

// RecordInQuery records statistics for an IN query execution
// Parameters:
//   - fieldName: The field being queried
//   - originalSize: Original list size before deduplication
//   - deduplicatedSize: List size after deduplication
//   - executionTimeNs: Execution time in nanoseconds
//   - memoryBytes: Memory used for hash set creation
//   - caseInsensitive: Whether case-insensitive matching was used
//   - singleValueOptimized: Whether single-value optimization was applied
//   - strategy: "index" or "scan"
//   - matched: Whether the query found a match
//
// TODO: Add integration with real-time monitoring system
// TODO: Implement hot pattern detection for caching
func RecordInQuery(fieldName string, originalSize, deduplicatedSize int, executionTimeNs int64,
	memoryBytes uint64, caseInsensitive, singleValueOptimized bool, strategy string, matched bool) {

	manager := getStatsManager()
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// Generate key for this query pattern
	key := generateStatsKey(fieldName, deduplicatedSize, caseInsensitive)

	// Get or create stats entry
	stats, exists := manager.stats[key]
	if !exists {
		stats = &InQueryStats{
			FieldName:            fieldName,
			ListSizeOriginal:     originalSize,
			ListSizeDeduplicated: deduplicatedSize,
			CaseInsensitive:      caseInsensitive,
			SingleValueOptimized: singleValueOptimized,
			Strategy:             strategy,
		}
		manager.stats[key] = stats
	}

	// Update statistics
	stats.ExecutionTime = executionTimeNs // Keep most recent execution time
	stats.MemoryUsageBytes = memoryBytes
	stats.LastAccessed = time.Now()

	if matched {
		stats.HitCount++
	} else {
		stats.MissCount++
	}

	// Prune if we've exceeded the limit
	if len(manager.stats) > maxStatsEntries {
		manager.pruneLRU()
	}
}

// pruneLRU removes the least recently used stats entries to stay under the limit
// Removes 10% of entries (1000) when limit is exceeded
func (m *InQueryStatsManager) pruneLRU() {
	// TODO: Implement more sophisticated pruning based on access patterns
	// TODO: Consider keeping hot queries and pruning cold queries first

	// Find oldest entries
	type statsEntry struct {
		key        string
		lastAccess time.Time
	}

	entries := make([]statsEntry, 0, len(m.stats))
	for key, stats := range m.stats {
		entries = append(entries, statsEntry{
			key:        key,
			lastAccess: stats.LastAccessed,
		})
	}

	// Sort by last accessed time
	for i := 0; i < len(entries)-1; i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].lastAccess.After(entries[j].lastAccess) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	// Remove oldest 10% (1000 entries)
	pruneCount := maxStatsEntries / 10
	for i := 0; i < pruneCount && i < len(entries); i++ {
		delete(m.stats, entries[i].key)
	}

	if m.logger != nil {
		m.logger.Debugf("Pruned %d IN query stats entries (LRU eviction)", pruneCount)
	}
}

// generateStatsKey generates a unique key for a query pattern
func generateStatsKey(fieldName string, listSize int, caseInsensitive bool) string {
	caseFlag := "cs" // case-sensitive
	if caseInsensitive {
		caseFlag = "ci" // case-insensitive
	}
	return fmt.Sprintf("%s_%d_%s", fieldName, listSize, caseFlag)
}

// GetInQueryStats returns all IN query statistics
// Returns a copy to prevent external modification
func GetInQueryStats() map[string]*InQueryStats {
	manager := getStatsManager()
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	// Return a copy
	statsCopy := make(map[string]*InQueryStats, len(manager.stats))
	for key, stats := range manager.stats {
		// Deep copy
		statsCopy[key] = &InQueryStats{
			FieldName:            stats.FieldName,
			ListSizeOriginal:     stats.ListSizeOriginal,
			ListSizeDeduplicated: stats.ListSizeDeduplicated,
			ExecutionTime:        stats.ExecutionTime,
			MemoryUsageBytes:     stats.MemoryUsageBytes,
			HitCount:             stats.HitCount,
			MissCount:            stats.MissCount,
			CaseInsensitive:      stats.CaseInsensitive,
			SingleValueOptimized: stats.SingleValueOptimized,
			Strategy:             stats.Strategy,
			LastAccessed:         stats.LastAccessed,
		}
	}

	return statsCopy
}

// GetInQueryStatsJSON returns IN query statistics as JSON
// This function requires Admin permission and should be called after checking:
//
//	permissionService.UserHasPermission(username, "Admin")
//
// Parameters:
//   - username: The username requesting the stats (used for logging)
//
// Returns:
//   - JSON string containing all IN query statistics
//   - error if JSON marshaling fails
//
// TODO: Add authentication/authorization check integration when admin API exists
// TODO: Support additional export formats (CSV, Prometheus metrics)
func GetInQueryStatsJSON(username string) (string, error) {
	// Note: Permission check should be done by the caller using:
	// hasPermission, err := permissionService.UserHasPermission(username, "Admin")

	stats := GetInQueryStats()

	// Marshal to JSON
	jsonBytes, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal IN query stats to JSON: %v", err)
	}

	manager := getStatsManager()
	if manager.logger != nil {
		manager.logger.Infof("User '%s' retrieved IN query statistics (%d patterns tracked)",
			username, len(stats))
	}

	return string(jsonBytes), nil
}

// ResetInQueryStats clears all statistics (Admin-only operation)
// Should only be called after verifying Admin permission
func ResetInQueryStats(username string) {
	manager := getStatsManager()
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	entryCount := len(manager.stats)
	manager.stats = make(map[string]*InQueryStats)

	if manager.logger != nil {
		manager.logger.Infof("User '%s' reset IN query statistics (%d entries cleared)",
			username, entryCount)
	}
}
