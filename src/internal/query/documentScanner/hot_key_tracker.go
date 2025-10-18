package documentscanner

import (
	"reflect"
	"sync"
	"time"

	"go.uber.org/zap"
)

// KeyQueryStats tracks statistics for queries on a specific key
// This helps identify hot keys and understand query patterns
type KeyQueryStats struct {
	QueryCount     int64                `json:"query_count"`     // Total number of queries for this key
	LastQueried    time.Time            `json:"last_queried"`    // When this key was last queried
	ValueTypes     map[reflect.Type]int `json:"value_types"`     // Types of values queried for this key
	UniqueValues   int                  `json:"unique_values"`   // Estimated number of unique values seen
	AverageLatency time.Duration        `json:"average_latency"` // Average query latency for this key
	FirstSeen      time.Time            `json:"first_seen"`      // When we first saw queries for this key

	// Performance indicators
	TotalLatency time.Duration `json:"total_latency"` // Sum of all query latencies
	CacheHits    int64         `json:"cache_hits"`    // How often this key benefits from caching

	// Value distribution for cardinality estimation
	valueFrequency map[interface{}]int `json:"-"` // Frequency of each value (not exported)
}

// HotKeyTracker learns query patterns and identifies frequently accessed keys
// This component is thread-safe and designed for high-concurrency usage
type HotKeyTracker struct {
	keyStats map[string]*KeyQueryStats // Statistics per key
	mu       sync.RWMutex              // Protects keyStats map
	logger   *zap.SugaredLogger        // Logger for debugging

	// Configuration parameters
	hotThreshold  int // Queries needed to consider key "hot"
	optimizeAfter int // When to suggest optimization
	maxKeyHistory int // Maximum keys to track

	// Cleanup parameters to prevent unbounded growth
	cleanupInterval time.Duration // How often to clean old data
	maxAge          time.Duration // Maximum age for tracked keys
	lastCleanup     time.Time     // When we last cleaned up

	// Performance metrics
	totalQueries int64     // Total queries processed
	startTime    time.Time // When tracking started
}

// NewHotKeyTracker creates a new hot key tracker with the specified configuration
// logger: Logger for debugging and monitoring
// hotThreshold: Number of queries needed to consider a key "hot"
// optimizeAfter: Number of queries before suggesting optimization
func NewHotKeyTracker(logger *zap.SugaredLogger, hotThreshold, optimizeAfter int) *HotKeyTracker {
	return &HotKeyTracker{
		keyStats:        make(map[string]*KeyQueryStats),
		logger:          logger,
		hotThreshold:    hotThreshold,
		optimizeAfter:   optimizeAfter,
		maxKeyHistory:   1000,           // Track up to 1000 different keys
		cleanupInterval: time.Hour,      // Clean up every hour
		maxAge:          time.Hour * 24, // Keep data for 24 hours
		startTime:       time.Now(),
		lastCleanup:     time.Now(),
	}
}

// RecordQuery records a query for the specified key and value
// This method is thread-safe and designed for high-frequency calls
// keyName: The key being queried
// value: The value being searched for
// latency: How long the query took
func (hkt *HotKeyTracker) RecordQuery(keyName string, value interface{}, latency time.Duration) {
	hkt.mu.Lock()
	defer hkt.mu.Unlock()

	// Get or create stats for this key
	stats, exists := hkt.keyStats[keyName]
	if !exists {
		stats = &KeyQueryStats{
			ValueTypes:     make(map[reflect.Type]int),
			valueFrequency: make(map[interface{}]int),
			FirstSeen:      time.Now(),
		}
		hkt.keyStats[keyName] = stats

		// Log new key discovery for monitoring
		hkt.logger.Debugf("New key discovered for tracking: %s", keyName)
	}

	// Update query statistics
	stats.QueryCount++
	stats.LastQueried = time.Now()
	stats.TotalLatency += latency
	stats.AverageLatency = time.Duration(int64(stats.TotalLatency) / stats.QueryCount)

	// Track value type distribution
	valueType := reflect.TypeOf(value)
	stats.ValueTypes[valueType]++

	// Track value frequency for cardinality estimation
	// Limit frequency map size to prevent memory explosion
	if len(stats.valueFrequency) < 10000 {
		stats.valueFrequency[value]++
		stats.UniqueValues = len(stats.valueFrequency)
	}

	// Update global metrics
	hkt.totalQueries++

	// Log hot key promotion
	if stats.QueryCount == int64(hkt.hotThreshold) {
		hkt.logger.Infof("Key '%s' promoted to hot key after %d queries", keyName, hkt.hotThreshold)
	}

	// Suggest optimization if threshold reached
	if stats.QueryCount == int64(hkt.optimizeAfter) {
		hkt.logger.Warnf("Key '%s' has %d queries - consider creating an index", keyName, hkt.optimizeAfter)
	}

	// Periodic cleanup to prevent memory leaks
	if time.Since(hkt.lastCleanup) > hkt.cleanupInterval {
		go hkt.cleanup()
	}
}

// RecordCacheHit records that a query for the specified key benefited from caching
// This helps measure the effectiveness of caching strategies
func (hkt *HotKeyTracker) RecordCacheHit(keyName string) {
	hkt.mu.Lock()
	defer hkt.mu.Unlock()

	if stats, exists := hkt.keyStats[keyName]; exists {
		stats.CacheHits++
	}
}

// IsHotKey determines if a key should be considered "hot" based on query frequency
// Returns true if the key has been queried more than the hot threshold
func (hkt *HotKeyTracker) IsHotKey(keyName string) bool {
	hkt.mu.RLock()
	defer hkt.mu.RUnlock()

	stats, exists := hkt.keyStats[keyName]
	return exists && stats.QueryCount >= int64(hkt.hotThreshold)
}

// GetHotKeys returns a list of all currently identified hot keys
// Keys are sorted by query frequency (most queried first)
func (hkt *HotKeyTracker) GetHotKeys() []string {
	hkt.mu.RLock()
	defer hkt.mu.RUnlock()

	type keyFreq struct {
		name  string
		count int64
	}

	var hotKeys []keyFreq
	for keyName, stats := range hkt.keyStats {
		if stats.QueryCount >= int64(hkt.hotThreshold) {
			hotKeys = append(hotKeys, keyFreq{name: keyName, count: stats.QueryCount})
		}
	}

	// Sort by query count (descending)
	for i := 0; i < len(hotKeys)-1; i++ {
		for j := i + 1; j < len(hotKeys); j++ {
			if hotKeys[j].count > hotKeys[i].count {
				hotKeys[i], hotKeys[j] = hotKeys[j], hotKeys[i]
			}
		}
	}

	// Extract just the key names
	result := make([]string, len(hotKeys))
	for i, kf := range hotKeys {
		result[i] = kf.name
	}

	return result
}

// GetKeyStats returns detailed statistics for a specific key
// Returns nil if the key is not being tracked
func (hkt *HotKeyTracker) GetKeyStats(keyName string) *KeyQueryStats {
	hkt.mu.RLock()
	defer hkt.mu.RUnlock()

	if stats, exists := hkt.keyStats[keyName]; exists {
		// Return a copy to prevent external modification
		statsCopy := *stats
		statsCopy.ValueTypes = make(map[reflect.Type]int)
		for k, v := range stats.ValueTypes {
			statsCopy.ValueTypes[k] = v
		}
		return &statsCopy
	}

	return nil
}

// GetOverallStats returns overall statistics about the hot key tracker
func (hkt *HotKeyTracker) GetOverallStats() map[string]interface{} {
	hkt.mu.RLock()
	defer hkt.mu.RUnlock()

	hotKeyCount := 0
	totalKeys := len(hkt.keyStats)

	for _, stats := range hkt.keyStats {
		if stats.QueryCount >= int64(hkt.hotThreshold) {
			hotKeyCount++
		}
	}

	uptime := time.Since(hkt.startTime)
	queriesPerSecond := float64(hkt.totalQueries) / uptime.Seconds()

	return map[string]interface{}{
		"total_queries":       hkt.totalQueries,
		"total_keys_tracked":  totalKeys,
		"hot_keys_identified": hotKeyCount,
		"uptime_seconds":      uptime.Seconds(),
		"queries_per_second":  queriesPerSecond,
		"last_cleanup":        hkt.lastCleanup,
	}
}

// ShouldOptimize suggests whether a key should be optimized (e.g., indexed)
// This considers both query frequency and the potential benefit of optimization
func (hkt *HotKeyTracker) ShouldOptimize(keyName string) bool {
	stats := hkt.GetKeyStats(keyName)
	if stats == nil {
		return false
	}

	// Suggest optimization if:
	// 1. Query count exceeds optimization threshold
	// 2. Average latency is significant (> 10ms)
	// 3. Cache hit rate is low (< 50%)

	exceedsThreshold := stats.QueryCount >= int64(hkt.optimizeAfter)
	slowQueries := stats.AverageLatency > time.Millisecond*10
	lowCacheHitRate := float64(stats.CacheHits)/float64(stats.QueryCount) < 0.5

	return exceedsThreshold && (slowQueries || lowCacheHitRate)
}

// cleanup removes old key statistics to prevent unbounded memory growth
// This method is called periodically in a separate goroutine
func (hkt *HotKeyTracker) cleanup() {
	hkt.mu.Lock()
	defer hkt.mu.Unlock()

	now := time.Now()
	keysRemoved := 0

	// Remove keys that haven't been queried recently
	for keyName, stats := range hkt.keyStats {
		if now.Sub(stats.LastQueried) > hkt.maxAge {
			delete(hkt.keyStats, keyName)
			keysRemoved++
		}
	}

	// If we still have too many keys, remove the least frequently used ones
	if len(hkt.keyStats) > hkt.maxKeyHistory {
		// Convert to slice for sorting
		type keyStats struct {
			name  string
			stats *KeyQueryStats
		}

		var allKeys []keyStats
		for name, stats := range hkt.keyStats {
			allKeys = append(allKeys, keyStats{name: name, stats: stats})
		}

		// Sort by query count (ascending) to remove least used first
		for i := 0; i < len(allKeys)-1; i++ {
			for j := i + 1; j < len(allKeys); j++ {
				if allKeys[j].stats.QueryCount < allKeys[i].stats.QueryCount {
					allKeys[i], allKeys[j] = allKeys[j], allKeys[i]
				}
			}
		}

		// Remove excess keys
		keysToRemove := len(allKeys) - hkt.maxKeyHistory
		for i := 0; i < keysToRemove; i++ {
			delete(hkt.keyStats, allKeys[i].name)
			keysRemoved++
		}
	}

	hkt.lastCleanup = now

	if keysRemoved > 0 {
		hkt.logger.Debugf("Cleaned up %d old key statistics", keysRemoved)
	}
}
