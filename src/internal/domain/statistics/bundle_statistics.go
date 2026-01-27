/*
bundle_statistics.go

This file implements the core statistics collection system for SyndrDB bundles.
It provides PostgreSQL-style statistics gathering with adaptive sampling, most common
values (MCV) tracking, and histogram-based selectivity estimation.

DESIGN PRINCIPLES:
- Single Responsibility: Each type handles a specific aspect of statistics
- Adaptive Sampling: Tiered strategy based on bundle size for efficiency
- Open/Closed: New statistic types can be added without modifying existing code

ADAPTIVE SAMPLING STRATEGY:
- Small bundles (<1000 documents): Full scan for 100% accuracy
- Medium bundles (1k-100k documents): 10% random sample
- Large bundles (>100k documents): Fixed 1000-document sample

This implements PostgreSQL's approach to statistics collection with SyndrDB-specific
optimizations for document-based storage.
*/

package statistics

import (
	"fmt"
	"math"
	"sort"
	"syndrdb/src/internal/domain/models"
	"time"

	"go.uber.org/zap"
)

// STATS_VERSION is the current statistics format version
// Increment when statistics schema changes to trigger automatic re-analysis
const STATS_VERSION = 1

// Default configuration constants
const (
	DEFAULT_HISTOGRAM_BUCKETS = 20    // PostgreSQL uses 100, we use 20 for initial implementation
	DEFAULT_MCV_COUNT         = 10    // Track top 10 most common values
	MIN_MCV_FREQUENCY         = 0.01  // 1% minimum frequency threshold
	RESERVOIR_SAMPLE_SIZE     = 10000 // Reservoir sampling buffer size
)

// BundleStatistics holds comprehensive statistics for a bundle
type BundleStatistics struct {
	BundleName       string                      `json:"bundle_name"`
	TotalDocuments   int64                       `json:"total_documents"`
	LastAnalyzed     time.Time                   `json:"last_analyzed"`
	StatsVersion     int                         `json:"stats_version"`
	FieldStats       map[string]*FieldStatistics `json:"field_stats"`
	CompressedSize   int64                       `json:"compressed_size,omitempty"`
	UncompressedSize int64                       `json:"uncompressed_size,omitempty"`
}

// FieldStatistics holds statistics for a single field
type FieldStatistics struct {
	FieldName        string            `json:"field_name"`
	DistinctCount    int64             `json:"distinct_count"`
	NullCount        int64             `json:"null_count"`
	AvgDocumentSize  int64             `json:"avg_document_size"` // Average size in bytes
	MostCommonValues []ValueFrequency  `json:"most_common_values"`
	Histogram        []HistogramBucket `json:"histogram"`
}

// ValueFrequency represents a value and its frequency in the dataset
type ValueFrequency struct {
	Value     interface{} `json:"value"`
	Frequency float64     `json:"frequency"` // Percentage of documents (0.0 to 1.0)
}

// HistogramBucket represents a range of values with document count
type HistogramBucket struct {
	LowerBound    interface{} `json:"lower_bound"`
	UpperBound    interface{} `json:"upper_bound"`
	DocumentCount int64       `json:"document_count"`
	DistinctCount int64       `json:"distinct_count"`
}

// StatisticsCollector collects statistics from bundles using adaptive sampling
type StatisticsCollector struct {
	logger *zap.SugaredLogger
}

// NewStatisticsCollector creates a new statistics collector
func NewStatisticsCollector(logger *zap.SugaredLogger) *StatisticsCollector {
	return &StatisticsCollector{
		logger: logger,
	}
}

// AnalyzeBundle collects comprehensive statistics for a bundle
func (sc *StatisticsCollector) AnalyzeBundle(bundle *models.Bundle) (*BundleStatistics, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	totalDocs := bundle.TotalDocuments
	if totalDocs == 0 {
		sc.logger.Infof("Bundle %s has no documents, returning empty statistics", bundle.Name)
		return &BundleStatistics{
			BundleName:     bundle.Name,
			TotalDocuments: 0,
			LastAnalyzed:   time.Now(),
			StatsVersion:   STATS_VERSION,
			FieldStats:     make(map[string]*FieldStatistics),
		}, nil
	}

	// Determine sampling strategy based on bundle size
	sampleRate := sc.determineSampleStrategy(totalDocs)
	sc.logger.Infof("Analyzing bundle %s: %d documents, sample rate: %.2f%%",
		bundle.Name, totalDocs, sampleRate*100)

	// Sample documents
	sampledDocs, err := sc.sampleDocuments(bundle, sampleRate)
	if err != nil {
		return nil, fmt.Errorf("failed to sample documents: %w", err)
	}

	// Collect field-level statistics
	fieldStats := make(map[string]*FieldStatistics)
	for fieldName := range bundle.DocumentStructure.FieldDefinitions {
		stats, err := sc.analyzeField(fieldName, sampledDocs, int(totalDocs))
		if err != nil {
			sc.logger.Warnf("Failed to analyze field %s: %v", fieldName, err)
			continue
		}
		fieldStats[fieldName] = stats
	}

	return &BundleStatistics{
		BundleName:     bundle.Name,
		TotalDocuments: totalDocs,
		LastAnalyzed:   time.Now(),
		StatsVersion:   STATS_VERSION,
		FieldStats:     fieldStats,
	}, nil
}

// determineSampleStrategy calculates the sampling rate based on bundle size
// Implements adaptive sampling:
// - <1000 documents: 1.0 (full scan)
// - 1k-100k documents: 0.10 (10% sample)
// - >100k documents: min(1000/totalDocs, 1.0) (fixed 1000-document sample)
func (sc *StatisticsCollector) determineSampleStrategy(totalDocuments int64) float64 {
	if totalDocuments < 1000 {
		return 1.0 // Full scan for small bundles
	} else if totalDocuments <= 100000 {
		return 0.10 // 10% sample for medium bundles
	} else {
		// Fixed 1000-document sample for large bundles
		return math.Min(1000.0/float64(totalDocuments), 1.0)
	}
}

// sampleDocuments selects documents using reservoir sampling
// TODO: This function needs to be refactored to use the page cache instead of bundle.Documents
func (sc *StatisticsCollector) sampleDocuments(bundle *models.Bundle, sampleRate float64) ([]*models.Document, error) {
	// Documents are now accessed via page cache - this function is deprecated
	// For now, return an error until this is refactored to use page-based access
	return nil, fmt.Errorf("sampleDocuments needs refactoring: bundle.Documents memtable removed, use page cache")
}

// analyzeField collects statistics for a single field
func (sc *StatisticsCollector) analyzeField(fieldName string, documents []*models.Document, totalDocs int) (*FieldStatistics, error) {
	stats := &FieldStatistics{
		FieldName:        fieldName,
		MostCommonValues: make([]ValueFrequency, 0, DEFAULT_MCV_COUNT),
		Histogram:        make([]HistogramBucket, 0, DEFAULT_HISTOGRAM_BUCKETS),
	}

	// Count values and track nulls
	valueCount := make(map[interface{}]int64)
	var nullCount int64
	var totalSize int64

	for _, doc := range documents {
		field, exists := doc.Fields[fieldName]
		if !exists || field.Value.IsNil() {
			nullCount++
			continue
		}

		value := field.Value.AsInterface()
		valueCount[value]++

		// Estimate size (rough approximation)
		totalSize += sc.estimateValueSize(value)
	}

	stats.NullCount = nullCount
	stats.DistinctCount = int64(len(valueCount))

	if len(documents) > 0 {
		stats.AvgDocumentSize = totalSize / int64(len(documents))
	}

	// Build most common values with adaptive threshold
	stats.MostCommonValues = sc.findMostCommonValues(valueCount, len(documents), stats.DistinctCount)

	// Build histogram
	stats.Histogram = sc.buildHistogram(valueCount, totalDocs)

	return stats, nil
}

// findMostCommonValues identifies the most frequent values
// Implements adaptive threshold: max(0.01, 1.0/distinctCount) to ensure at least one MCV
func (sc *StatisticsCollector) findMostCommonValues(valueCount map[interface{}]int64, totalSampled int, distinctCount int64) []ValueFrequency {
	if len(valueCount) == 0 {
		return nil
	}

	// Calculate adaptive frequency threshold
	threshold := math.Max(MIN_MCV_FREQUENCY, 1.0/float64(distinctCount))

	// Convert to slice for sorting
	type valuePair struct {
		value interface{}
		count int64
		freq  float64
	}

	pairs := make([]valuePair, 0, len(valueCount))
	for value, count := range valueCount {
		freq := float64(count) / float64(totalSampled)
		if freq >= threshold {
			pairs = append(pairs, valuePair{value, count, freq})
		}
	}

	// Sort by frequency descending
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].count > pairs[j].count
	})

	// Take top 10
	limit := DEFAULT_MCV_COUNT
	if len(pairs) < limit {
		limit = len(pairs)
	}

	result := make([]ValueFrequency, limit)
	for i := 0; i < limit; i++ {
		result[i] = ValueFrequency{
			Value:     pairs[i].value,
			Frequency: pairs[i].freq,
		}
	}

	return result
}

// buildHistogram creates an equi-depth histogram with 20 buckets
// TODO: I will make histogram bucket count adaptive based on bundle size when precision requirements vary
func (sc *StatisticsCollector) buildHistogram(valueCount map[interface{}]int64, totalDocs int) []HistogramBucket {
	if len(valueCount) == 0 {
		return nil
	}

	// Sort values
	type valuePair struct {
		value interface{}
		count int64
	}

	pairs := make([]valuePair, 0, len(valueCount))
	for value, count := range valueCount {
		pairs = append(pairs, valuePair{value, count})
	}

	// Sort by value (for comparable types)
	sort.Slice(pairs, func(i, j int) bool {
		return sc.compareValues(pairs[i].value, pairs[j].value) < 0
	})

	// Create equi-depth buckets
	buckets := make([]HistogramBucket, 0, DEFAULT_HISTOGRAM_BUCKETS)
	docsPerBucket := totalDocs / DEFAULT_HISTOGRAM_BUCKETS
	if docsPerBucket == 0 {
		docsPerBucket = 1
	}

	var currentBucket HistogramBucket
	var currentCount int64
	var distinctInBucket int64

	for i, pair := range pairs {
		if currentBucket.LowerBound == nil {
			currentBucket.LowerBound = pair.value
		}

		currentCount += pair.count
		distinctInBucket++
		currentBucket.UpperBound = pair.value

		// Close bucket when reaching target size or end
		if currentCount >= int64(docsPerBucket) || i == len(pairs)-1 {
			currentBucket.DocumentCount = currentCount
			currentBucket.DistinctCount = distinctInBucket
			buckets = append(buckets, currentBucket)

			// Reset for next bucket
			currentBucket = HistogramBucket{}
			currentCount = 0
			distinctInBucket = 0

			if len(buckets) >= DEFAULT_HISTOGRAM_BUCKETS {
				break
			}
		}
	}

	return buckets
}

// EstimateSelectivity estimates the selectivity of a predicate using statistics
func (sc *StatisticsCollector) EstimateSelectivity(fieldStats *FieldStatistics, operator string, value interface{}) float64 {
	if fieldStats == nil {
		return 0.1 // Default 10% selectivity
	}

	// Check MCV for exact match
	if operator == "=" || operator == "==" {
		for _, mcv := range fieldStats.MostCommonValues {
			if sc.compareValues(mcv.Value, value) == 0 {
				return mcv.Frequency
			}
		}
	}

	// Use histogram for range queries
	// TODO: I will implement histogram-based selectivity estimation for range predicates

	// Default heuristics
	switch operator {
	case "=", "==":
		if fieldStats.DistinctCount > 0 {
			return 1.0 / float64(fieldStats.DistinctCount)
		}
		return 0.01
	case ">", "<", ">=", "<=":
		return 0.33 // Approximate 1/3 of data
	default:
		return 0.1
	}
}

// Helper functions

func (sc *StatisticsCollector) estimateValueSize(value interface{}) int64 {
	switch v := value.(type) {
	case string:
		return int64(len(v))
	case int, int32, int64, float32, float64, bool:
		return 8
	default:
		return 16 // Default estimate
	}
}

func (sc *StatisticsCollector) compareValues(a, b interface{}) int {
	// Simple comparison for basic types
	switch va := a.(type) {
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int:
		if vb, ok := b.(int); ok {
			return va - vb
		}
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}
	return 0
}
