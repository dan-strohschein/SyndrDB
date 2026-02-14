/*
SHARDED HOT KEY TRACKER - High-Concurrency Query Pattern Learning

This file provides a sharded implementation of HotKeyTracker that distributes
lock contention across 128 shards, eliminating the severe bottleneck caused by
a single global mutex on every query operation.

PROBLEM SOLVED:
The original HotKeyTracker used a single mutex that ALL queries hit during:
- RecordQuery() - called on EVERY document scan (2x per query)
- IsHotKey() - called on every cache check
This created extreme convoy effects with 150+ concurrent connections.

DESIGN:
- 128 shards (baseline, can scale to 256 if needed)
- Each shard has its own RWMutex protecting a map[string]*KeyQueryStats
- Shard selection via xxhash(keyName) & 127 for O(1) distribution
- Fast path: RLock for reads (IsHotKey), Lock for writes (RecordQuery)
- Atomic counters for global stats (totalQueries)

PERFORMANCE CHARACTERISTICS:
- 150 connections → ~1.17 queries per shard average
- Reads (IsHotKey): concurrent within same shard
- Writes (RecordQuery): exclusive but only blocks same-shard operations
- Expected ~128x reduction in lock contention vs non-sharded version

USAGE:
  tracker := NewShardedHotKeyTracker(logger, hotThreshold, optimizeAfter)
  tracker.RecordQuery("user_id", "12345", time.Millisecond)
  if tracker.IsHotKey("user_id") {
      // Handle hot key...
  }
*/

package documentscanner

import (
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

const (
	// ShardedHotKeyTrackerShards is the number of shards for distributing hot key tracking.
	// 128 shards as baseline (can scale to 256 if contention persists).
	ShardedHotKeyTrackerShards = 128
)

// hotKeyTrackerShard holds a partition of the hot key statistics
type hotKeyTrackerShard struct {
	mu       sync.RWMutex
	keyStats map[string]*KeyQueryStats
}

// ShardedHotKeyTracker learns query patterns and identifies frequently accessed keys
// with sharding to support high concurrency (150+ connections).
type ShardedHotKeyTracker struct {
	shards [ShardedHotKeyTrackerShards]hotKeyTrackerShard

	// Configuration parameters (immutable after init)
	logger        *zap.SugaredLogger
	hotThreshold  int
	optimizeAfter int
	maxKeyHistory int

	// Cleanup parameters
	cleanupInterval time.Duration
	maxAge          time.Duration
	lastCleanup     atomic.Value // stores time.Time

	// Performance metrics (atomic for lock-free updates)
	totalQueries atomic.Int64
	startTime    time.Time
}

// NewShardedHotKeyTracker creates a new sharded hot key tracker with the specified configuration.
// This is a drop-in replacement for NewHotKeyTracker with identical method signatures.
func NewShardedHotKeyTracker(logger *zap.SugaredLogger, hotThreshold, optimizeAfter int) *ShardedHotKeyTracker {
	tracker := &ShardedHotKeyTracker{
		logger:          logger,
		hotThreshold:    hotThreshold,
		optimizeAfter:   optimizeAfter,
		maxKeyHistory:   1000,           // Track up to 1000 different keys
		cleanupInterval: time.Hour,      // Clean up every hour
		maxAge:          time.Hour * 24, // Keep data for 24 hours
		startTime:       time.Now(),
	}

	// Initialize all shards
	for i := range tracker.shards {
		tracker.shards[i].keyStats = make(map[string]*KeyQueryStats)
	}

	// Initialize lastCleanup
	tracker.lastCleanup.Store(time.Now())

	return tracker
}

// shardIndex returns the shard index for a given key name
func (hkt *ShardedHotKeyTracker) shardIndex(keyName string) uint64 {
	return xxhash.Sum64String(keyName) & (ShardedHotKeyTrackerShards - 1)
}

// RecordQuery records a query for the specified key and value.
// This method is thread-safe and designed for high-frequency calls.
// Only locks the target shard, allowing concurrent operations on different keys.
func (hkt *ShardedHotKeyTracker) RecordQuery(keyName string, value interface{}, latency time.Duration) {
	idx := hkt.shardIndex(keyName)
	shard := &hkt.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Get or create stats for this key
	stats, exists := shard.keyStats[keyName]
	if !exists {
		stats = &KeyQueryStats{
			ValueTypes:     make(map[reflect.Type]int),
			valueFrequency: make(map[interface{}]int),
			FirstSeen:      time.Now(),
		}
		shard.keyStats[keyName] = stats

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
	if len(stats.valueFrequency) < 1000 {
		stats.valueFrequency[value]++
		stats.UniqueValues = len(stats.valueFrequency)
	}

	// Update global metrics atomically
	hkt.totalQueries.Add(1)

	// Log hot key promotion
	if stats.QueryCount == int64(hkt.hotThreshold) {
		hkt.logger.Infof("Key '%s' promoted to hot key after %d queries", keyName, hkt.hotThreshold)
	}

	// Suggest optimization if threshold reached
	if stats.QueryCount == int64(hkt.optimizeAfter) {
		hkt.logger.Warnf("Key '%s' has %d queries - consider creating an index", keyName, hkt.optimizeAfter)
	}

	// Periodic cleanup to prevent memory leaks
	lastCleanup := hkt.lastCleanup.Load().(time.Time)
	if time.Since(lastCleanup) > hkt.cleanupInterval {
		go hkt.cleanup()
	}
}

// RecordCacheHit records that a query for the specified key benefited from caching.
// Only locks the target shard.
func (hkt *ShardedHotKeyTracker) RecordCacheHit(keyName string) {
	idx := hkt.shardIndex(keyName)
	shard := &hkt.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if stats, exists := shard.keyStats[keyName]; exists {
		stats.CacheHits++
	}
}

// IsHotKey determines if a key should be considered "hot" based on query frequency.
// Only locks the target shard for reading (concurrent with other reads).
func (hkt *ShardedHotKeyTracker) IsHotKey(keyName string) bool {
	idx := hkt.shardIndex(keyName)
	shard := &hkt.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	stats, exists := shard.keyStats[keyName]
	return exists && stats.QueryCount >= int64(hkt.hotThreshold)
}

// GetHotKeys returns a list of all currently identified hot keys.
// Keys are sorted by query frequency (most queried first).
// Aggregates across all shards.
func (hkt *ShardedHotKeyTracker) GetHotKeys() []string {
	type keyFreq struct {
		name  string
		count int64
	}

	var hotKeys []keyFreq

	// Collect hot keys from all shards
	for i := range hkt.shards {
		shard := &hkt.shards[i]
		shard.mu.RLock()

		for keyName, stats := range shard.keyStats {
			if stats.QueryCount >= int64(hkt.hotThreshold) {
				hotKeys = append(hotKeys, keyFreq{name: keyName, count: stats.QueryCount})
			}
		}

		shard.mu.RUnlock()
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

// GetKeyStats returns detailed statistics for a specific key.
// Returns nil if the key is not being tracked.
// Only locks the target shard for reading.
func (hkt *ShardedHotKeyTracker) GetKeyStats(keyName string) *KeyQueryStats {
	idx := hkt.shardIndex(keyName)
	shard := &hkt.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	if stats, exists := shard.keyStats[keyName]; exists {
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

// GetOverallStats returns overall statistics about the hot key tracker.
// Aggregates across all shards.
func (hkt *ShardedHotKeyTracker) GetOverallStats() map[string]interface{} {
	hotKeyCount := 0
	totalKeys := 0

	// Count across all shards
	for i := range hkt.shards {
		shard := &hkt.shards[i]
		shard.mu.RLock()

		totalKeys += len(shard.keyStats)
		for _, stats := range shard.keyStats {
			if stats.QueryCount >= int64(hkt.hotThreshold) {
				hotKeyCount++
			}
		}

		shard.mu.RUnlock()
	}

	totalQueriesCount := hkt.totalQueries.Load()
	uptime := time.Since(hkt.startTime)
	queriesPerSecond := float64(totalQueriesCount) / uptime.Seconds()
	lastCleanup := hkt.lastCleanup.Load().(time.Time)

	return map[string]interface{}{
		"total_queries":       totalQueriesCount,
		"total_keys_tracked":  totalKeys,
		"hot_keys_identified": hotKeyCount,
		"uptime_seconds":      uptime.Seconds(),
		"queries_per_second":  queriesPerSecond,
		"last_cleanup":        lastCleanup,
		"shard_count":         ShardedHotKeyTrackerShards,
	}
}

// ShouldOptimize suggests whether a key should be optimized (e.g., indexed).
// This considers both query frequency and the potential benefit of optimization.
func (hkt *ShardedHotKeyTracker) ShouldOptimize(keyName string) bool {
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

// cleanup removes old key statistics to prevent unbounded memory growth.
// This method locks each shard sequentially (runs in background goroutine).
func (hkt *ShardedHotKeyTracker) cleanup() {
	// Update last cleanup time atomically (acts as a lightweight lock)
	now := time.Now()
	oldLastCleanup := hkt.lastCleanup.Load().(time.Time)
	if !hkt.lastCleanup.CompareAndSwap(oldLastCleanup, now) {
		// Another goroutine is already running cleanup
		return
	}

	totalKeysRemoved := 0
	totalValueFrequencyCleared := 0

	halfMaxAge := hkt.maxAge / 2

	// Clean each shard sequentially
	for i := range hkt.shards {
		shard := &hkt.shards[i]
		shard.mu.Lock()

		keysRemoved := 0
		valueFrequencyCleared := 0

		// Remove keys that haven't been queried recently
		for keyName, stats := range shard.keyStats {
			age := now.Sub(stats.LastQueried)
			if age > hkt.maxAge {
				delete(shard.keyStats, keyName)
				keysRemoved++
			} else if age > halfMaxAge && len(stats.valueFrequency) > 0 {
				// Key is stale but retained - clear valueFrequency to free memory
				stats.valueFrequency = make(map[interface{}]int)
				stats.UniqueValues = 0
				valueFrequencyCleared++
			}
		}

		// If this shard still has too many keys, remove the least frequently used ones
		maxKeysPerShard := hkt.maxKeyHistory / ShardedHotKeyTrackerShards
		if len(shard.keyStats) > maxKeysPerShard {
			type keyStats struct {
				name  string
				stats *KeyQueryStats
			}

			var allKeys []keyStats
			for name, stats := range shard.keyStats {
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
			keysToRemove := len(allKeys) - maxKeysPerShard
			for i := 0; i < keysToRemove; i++ {
				delete(shard.keyStats, allKeys[i].name)
				keysRemoved++
			}
		}

		totalKeysRemoved += keysRemoved
		totalValueFrequencyCleared += valueFrequencyCleared

		shard.mu.Unlock()
	}

	if totalKeysRemoved > 0 || totalValueFrequencyCleared > 0 {
		hkt.logger.Debugf("Cleaned up %d old key statistics, cleared valueFrequency for %d stale keys",
			totalKeysRemoved, totalValueFrequencyCleared)
	}
}
