package planner

import (
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// StatsStore manages column statistics for all bundles.
// Thread-safe via a single RWMutex (shard by bundle if contention appears).
type StatsStore struct {
	mu    sync.RWMutex
	stats map[string]map[string]*ColumnStats // bundleName -> fieldName -> *ColumnStats

	globalVersion atomic.Uint64
	logger        *zap.SugaredLogger
}

// NewStatsStore creates a new statistics store.
func NewStatsStore(logger *zap.SugaredLogger) *StatsStore {
	return &StatsStore{
		stats:  make(map[string]map[string]*ColumnStats),
		logger: logger,
	}
}

// GetColumnStats returns statistics for a specific bundle field.
// Returns nil if no stats are available.
func (ss *StatsStore) GetColumnStats(bundleName, fieldName string) *ColumnStats {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	bundleStats, ok := ss.stats[bundleName]
	if !ok {
		return nil
	}
	return bundleStats[fieldName]
}

// GetBundleStats returns all column stats for a bundle (shallow copy).
func (ss *StatsStore) GetBundleStats(bundleName string) map[string]*ColumnStats {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	bundleStats, ok := ss.stats[bundleName]
	if !ok {
		return nil
	}
	result := make(map[string]*ColumnStats, len(bundleStats))
	for k, v := range bundleStats {
		result[k] = v
	}
	return result
}

// GetStatsVersion returns the current global stats version.
func (ss *StatsStore) GetStatsVersion() uint64 {
	return ss.globalVersion.Load()
}

// GetBundleStatsVersion returns the maximum StatsVersion across all fields in a bundle.
func (ss *StatsStore) GetBundleStatsVersion(bundleName string) uint64 {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	bundleStats, ok := ss.stats[bundleName]
	if !ok {
		return 0
	}
	var maxVersion uint64
	for _, cs := range bundleStats {
		if cs.StatsVersion > maxVersion {
			maxVersion = cs.StatsVersion
		}
	}
	return maxVersion
}

// UpdateColumnStats sets or replaces statistics for a bundle field.
func (ss *StatsStore) UpdateColumnStats(stats *ColumnStats) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	bundleStats, ok := ss.stats[stats.BundleName]
	if !ok {
		bundleStats = make(map[string]*ColumnStats)
		ss.stats[stats.BundleName] = bundleStats
	}

	stats.StatsVersion = ss.globalVersion.Add(1)
	bundleStats[stats.FieldName] = stats
}

// RemoveBundle removes all statistics for a dropped bundle.
func (ss *StatsStore) RemoveBundle(bundleName string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.stats, bundleName)
}

// IncrementalUpdate updates statistics incrementally for a single field after a write.
// Called from the write path (INSERT/UPDATE/DELETE).
func (ss *StatsStore) IncrementalUpdate(
	bundleName, fieldName string,
	oldValue, newValue interface{},
	totalDocs int64,
) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	bundleStats, ok := ss.stats[bundleName]
	if !ok {
		bundleStats = make(map[string]*ColumnStats)
		ss.stats[bundleName] = bundleStats
	}

	cs, ok := bundleStats[fieldName]
	if !ok {
		cs = &ColumnStats{
			BundleName:    bundleName,
			FieldName:     fieldName,
			HLL:           NewHyperLogLog(),
			IsIncremental: true,
		}
		bundleStats[fieldName] = cs
	}

	cs.RowCount = totalDocs

	// Track nulls
	if newValue == nil && oldValue != nil {
		// Field became null (UPDATE to null or DELETE)
		cs.NullCount++
	} else if newValue != nil && oldValue == nil {
		// Field became non-null (INSERT or UPDATE from null)
		if cs.NullCount > 0 {
			cs.NullCount--
		}
	}

	// Add new value to HLL (incremental distinct tracking)
	if newValue != nil && cs.HLL != nil {
		cs.HLL.AddString(fmt.Sprintf("%v", newValue))
		cs.DistinctCount = int64(cs.HLL.Count())
	}

	// Update min/max
	cs.updateMinMax(newValue)

	cs.StatsVersion = ss.globalVersion.Add(1)
}
