package planner

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

const (
	// DefaultAnalyzeSampleSize is the default sample size for ANALYZE.
	// PostgreSQL uses 30000 * default_statistics_target (100) = 30000 rows.
	DefaultAnalyzeSampleSize = 30000

	// DefaultHistogramBuckets is the default number of histogram buckets.
	DefaultHistogramBuckets = 100
)

// AnalyzeBundle performs full statistics collection for a bundle.
// Uses reservoir sampling for bounded memory usage.
func AnalyzeBundle(
	ctx context.Context,
	bundleName string,
	bundleService BundleServiceInterface,
	statsStore *StatsStore,
	logger *zap.SugaredLogger,
	sampleSize ...int,
) error {
	targetSample := DefaultAnalyzeSampleSize
	if len(sampleSize) > 0 && sampleSize[0] > 0 {
		targetSample = sampleSize[0]
	}

	logger.Infof("ANALYZE: Starting statistics collection for bundle '%s' (sample=%d)", bundleName, targetSample)

	// Use GetDocumentChunksForIndexing to stream documents without loading entire bundle
	// Reservoir sampling: keep exactly targetSample random documents
	reservoir := make([]*models.Document, 0, targetSample)
	totalSeen := int64(0)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	err := bundleService.GetDocumentChunksForIndexing(ctx, bundleName, 1000, func(chunk []*models.Document) bool {
		for _, doc := range chunk {
			totalSeen++
			if len(reservoir) < targetSample {
				reservoir = append(reservoir, doc)
			} else {
				// Reservoir sampling: replace with probability targetSample/totalSeen
				j := rng.Int63n(totalSeen)
				if j < int64(targetSample) {
					reservoir[j] = doc
				}
			}
		}
		select {
		case <-ctx.Done():
			return true // stop
		default:
			return false // continue
		}
	})
	if err != nil {
		return fmt.Errorf("ANALYZE failed for bundle '%s': %w", bundleName, err)
	}

	logger.Infof("ANALYZE: Sampled %d/%d documents from bundle '%s'", len(reservoir), totalSeen, bundleName)

	// Build per-field stats from the sample
	fieldStats := make(map[string]*ColumnStats)

	for _, doc := range reservoir {
		if doc == nil || doc.Fields == nil {
			continue
		}
		for fieldName, field := range doc.Fields {
			cs, ok := fieldStats[fieldName]
			if !ok {
				cs = &ColumnStats{
					BundleName: bundleName,
					FieldName:  fieldName,
					RowCount:   totalSeen,
					HLL:        NewHyperLogLog(),
					SampleSize: int64(len(reservoir)),
				}
				fieldStats[fieldName] = cs
			}

			val := field.Value.AsInterface()
			if val == nil {
				cs.NullCount++
			} else {
				cs.HLL.AddString(fmt.Sprintf("%v", val))
				cs.updateMinMax(val)
			}
		}
	}

	// Finalize and store each field's stats
	now := time.Now()
	for _, cs := range fieldStats {
		cs.DistinctCount = int64(cs.HLL.Count())
		// Scale null count from sample to full bundle
		if int64(len(reservoir)) > 0 && totalSeen > 0 {
			scaleFactor := float64(totalSeen) / float64(len(reservoir))
			cs.NullCount = int64(float64(cs.NullCount) * scaleFactor)
		}
		cs.RowCount = totalSeen
		cs.LastAnalyzeAt = now
		cs.IsIncremental = false
		statsStore.UpdateColumnStats(cs)
	}

	logger.Infof("ANALYZE: Completed for bundle '%s': %d fields analyzed, %d documents sampled",
		bundleName, len(fieldStats), len(reservoir))

	return nil
}
