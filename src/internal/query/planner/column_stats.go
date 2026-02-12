package planner

import (
	"math"
	"time"
)

// ColumnStats holds per-field statistics for a single bundle field.
// Maintained incrementally on write paths and periodically reconciled via ANALYZE.
type ColumnStats struct {
	BundleName string
	FieldName  string

	// Core statistics (always maintained)
	RowCount      int64 // Total rows in bundle (mirrors Bundle.TotalDocuments)
	NullCount     int64 // Number of rows where this field is null/missing
	DistinctCount int64 // Approximate number of distinct non-null values

	// Min/Max for range selectivity (numeric/date/string fields)
	MinValue interface{} // Smallest observed value (nil if no stats)
	MaxValue interface{} // Largest observed value (nil if no stats)

	// HyperLogLog sketch for approximate distinct count
	HLL *HyperLogLog

	// Optional equi-depth histogram (populated by ANALYZE, nil otherwise)
	Histogram *EquiDepthHistogram

	// Metadata
	StatsVersion  uint64    // Monotonically increasing; bumped on every stats update
	LastAnalyzeAt time.Time // When full ANALYZE was last run
	SampleSize    int64     // Number of rows sampled during last ANALYZE
	IsIncremental bool      // True if stats were built incrementally (not full ANALYZE)
}

// NullFrac returns the null fraction.
func (cs *ColumnStats) NullFrac() float64 {
	if cs.RowCount == 0 {
		return 0.0
	}
	return float64(cs.NullCount) / float64(cs.RowCount)
}

// NDV returns the approximate number of distinct values (non-null).
func (cs *ColumnStats) NDV() int64 {
	if cs.HLL != nil {
		return int64(cs.HLL.Count())
	}
	return cs.DistinctCount
}

// updateMinMax updates the min/max bounds for numeric values.
func (cs *ColumnStats) updateMinMax(value interface{}) {
	if value == nil {
		return
	}
	fval, ok := toFloat64ForStats(value)
	if !ok {
		return
	}
	if cs.MinValue == nil {
		cs.MinValue = fval
		cs.MaxValue = fval
		return
	}
	if currentMin, ok := cs.MinValue.(float64); ok && fval < currentMin {
		cs.MinValue = fval
	}
	if currentMax, ok := cs.MaxValue.(float64); ok && fval > currentMax {
		cs.MaxValue = fval
	}
}

// toFloat64ForStats converts a value to float64 for min/max tracking.
func toFloat64ForStats(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// EquiDepthHistogram stores value distribution in equal-depth buckets.
type EquiDepthHistogram struct {
	Boundaries    []float64 // Upper bounds of each bucket (sorted)
	NumBuckets    int
	RowsPerBucket float64 // Approximate rows per bucket
}

// FractionInRange returns the estimated fraction of rows in [low, high].
func (h *EquiDepthHistogram) FractionInRange(low, high float64) float64 {
	if h == nil || h.NumBuckets == 0 {
		return 0.5
	}
	totalBuckets := float64(h.NumBuckets)
	lowBucket := h.findBucket(low)
	highBucket := h.findBucket(high)

	fullBuckets := float64(highBucket - lowBucket)
	if fullBuckets < 0 {
		fullBuckets = 0
	}
	return math.Min(1.0, math.Max(0.0, fullBuckets/totalBuckets))
}

// findBucket returns the 0-based bucket index for a value using binary search.
func (h *EquiDepthHistogram) findBucket(val float64) int {
	lo, hi := 0, len(h.Boundaries)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if h.Boundaries[mid] < val {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if lo >= len(h.Boundaries) {
		return len(h.Boundaries) - 1
	}
	return lo
}
