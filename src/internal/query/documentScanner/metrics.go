package documentscanner

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// MetricsManager aggregates and exposes metrics from multiple scanners
// This component helps the query planner make informed decisions about optimization
type MetricsManager struct {
	scanners map[string]DocumentScannerInterface // Scanner instances by bundle name
	mu       sync.RWMutex                        // Protects scanners map
	logger   *zap.SugaredLogger                  // Logger for debugging

	// Aggregated metrics
	globalMetrics *GlobalScanMetrics
	startTime     time.Time

	// Background metrics collection
	metricsInterval time.Duration
	stopChan        chan bool
	running         bool
}

// GlobalScanMetrics aggregates metrics across all scanners
// These metrics provide insights for query optimization and system monitoring
type GlobalScanMetrics struct {
	// Cross-scanner metrics
	TotalScanners      int           `json:"total_scanners"`
	TotalScans         int64         `json:"total_scans"`
	AverageLatency     time.Duration `json:"average_latency"`
	GlobalCacheHitRate float64       `json:"global_cache_hit_rate"`

	// Hot key insights
	GlobalHotKeys   []string            `json:"global_hot_keys"`
	HotKeysByBundle map[string][]string `json:"hot_keys_by_bundle"`

	// Performance insights
	SlowestBundles     []BundlePerformance    `json:"slowest_bundles"`
	MostQueriedBundles []BundleQueryFrequency `json:"most_queried_bundles"`

	// System health
	TotalMemoryPressureGCs int64 `json:"total_memory_pressure_gcs"`
	TotalErrors            int64 `json:"total_errors"`

	// Query planner recommendations
	RecommendedOptimizations []OptimizationRecommendation `json:"recommended_optimizations"`

	// Last updated timestamp
	LastUpdated time.Time `json:"last_updated"`
}

// BundlePerformance tracks performance metrics for a specific bundle
type BundlePerformance struct {
	BundleName     string        `json:"bundle_name"`
	AverageLatency time.Duration `json:"average_latency"`
	ScanCount      int64         `json:"scan_count"`
	CacheHitRate   float64       `json:"cache_hit_rate"`
	ErrorRate      float64       `json:"error_rate"`
}

// BundleQueryFrequency tracks query frequency for a specific bundle
type BundleQueryFrequency struct {
	BundleName     string  `json:"bundle_name"`
	QueryCount     int64   `json:"query_count"`
	QueriesPerHour float64 `json:"queries_per_hour"`
	UniqueHotKeys  int     `json:"unique_hot_keys"`
}

// OptimizationRecommendation suggests performance improvements
type OptimizationRecommendation struct {
	Type           string    `json:"type"` // "index", "cache", "partition", etc.
	BundleName     string    `json:"bundle_name"`
	KeyName        string    `json:"key_name,omitempty"`
	Priority       string    `json:"priority"` // "high", "medium", "low"
	Reason         string    `json:"reason"`
	EstimatedGain  string    `json:"estimated_gain"`
	Implementation string    `json:"implementation"`
	CreatedAt      time.Time `json:"created_at"`
}

// NewMetricsManager creates a new metrics manager
// logger: Logger for debugging and monitoring
// metricsInterval: How often to collect and aggregate metrics
func NewMetricsManager(logger *zap.SugaredLogger, metricsInterval time.Duration) *MetricsManager {
	return &MetricsManager{
		scanners:        make(map[string]DocumentScannerInterface),
		logger:          logger,
		globalMetrics:   &GlobalScanMetrics{HotKeysByBundle: make(map[string][]string)},
		startTime:       time.Now(),
		metricsInterval: metricsInterval,
		stopChan:        make(chan bool),
		running:         false,
	}
}

// RegisterScanner registers a scanner for metrics collection
// bundleName: Name of the bundle this scanner handles
// scanner: The scanner instance to monitor
func (mm *MetricsManager) RegisterScanner(bundleName string, scanner DocumentScannerInterface) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.scanners[bundleName] = scanner
	mm.logger.Infof("Registered scanner for bundle '%s'", bundleName)

	// Start metrics collection if not already running
	if !mm.running {
		go mm.startMetricsCollection()
	}
}

// UnregisterScanner removes a scanner from metrics collection
func (mm *MetricsManager) UnregisterScanner(bundleName string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	delete(mm.scanners, bundleName)
	delete(mm.globalMetrics.HotKeysByBundle, bundleName)
	mm.logger.Infof("Unregistered scanner for bundle '%s'", bundleName)
}

// GetGlobalMetrics returns aggregated metrics across all scanners
// This provides a comprehensive view for query optimization decisions
func (mm *MetricsManager) GetGlobalMetrics() *GlobalScanMetrics {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	mm.updateGlobalMetrics()

	// Return a copy to prevent external modification
	metricsCopy := *mm.globalMetrics
	metricsCopy.HotKeysByBundle = make(map[string][]string)
	for bundle, keys := range mm.globalMetrics.HotKeysByBundle {
		metricsCopy.HotKeysByBundle[bundle] = make([]string, len(keys))
		copy(metricsCopy.HotKeysByBundle[bundle], keys)
	}

	return &metricsCopy
}

// GetBundleMetrics returns metrics for a specific bundle
func (mm *MetricsManager) GetBundleMetrics(bundleName string) *ScanMetrics {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if scanner, exists := mm.scanners[bundleName]; exists {
		return scanner.GetMetrics()
	}

	return nil
}

// GetHotKeysAcrossAllBundles returns hot keys across all registered scanners
// This helps identify system-wide query patterns
func (mm *MetricsManager) GetHotKeysAcrossAllBundles() map[string][]string {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	hotKeys := make(map[string][]string)

	for bundleName, scanner := range mm.scanners {
		hotKeys[bundleName] = scanner.GetHotKeys()
	}

	return hotKeys
}

// GetOptimizationRecommendations returns current optimization recommendations
// This helps the query planner and database administrators improve performance
func (mm *MetricsManager) GetOptimizationRecommendations() []OptimizationRecommendation {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	mm.generateOptimizationRecommendations()

	// Return a copy
	recommendations := make([]OptimizationRecommendation, len(mm.globalMetrics.RecommendedOptimizations))
	copy(recommendations, mm.globalMetrics.RecommendedOptimizations)

	return recommendations
}

// Stop stops the metrics collection background process
func (mm *MetricsManager) Stop() {
	if mm.running {
		mm.stopChan <- true
		mm.running = false
		mm.logger.Info("Stopped metrics collection")
	}
}

// startMetricsCollection runs the background metrics collection process
func (mm *MetricsManager) startMetricsCollection() {
	mm.mu.Lock()
	mm.running = true
	mm.mu.Unlock()

	mm.logger.Infof("Started metrics collection with interval %v", mm.metricsInterval)

	ticker := time.NewTicker(mm.metricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mm.collectAndAggregateMetrics()
		case <-mm.stopChan:
			mm.logger.Info("Metrics collection stopped")
			return
		}
	}
}

// collectAndAggregateMetrics collects metrics from all scanners and aggregates them
func (mm *MetricsManager) collectAndAggregateMetrics() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.updateGlobalMetrics()
	mm.generateOptimizationRecommendations()

	mm.logger.Debugf("Collected metrics from %d scanners", len(mm.scanners))
}

// updateGlobalMetrics updates the global metrics based on current scanner data
func (mm *MetricsManager) updateGlobalMetrics() {
	if len(mm.scanners) == 0 {
		return
	}

	mm.globalMetrics.TotalScanners = len(mm.scanners)
	mm.globalMetrics.LastUpdated = time.Now()

	var totalScans int64
	var totalLatency time.Duration
	var totalCacheHits int64
	var totalMemoryGCs int64
	var totalErrors int64

	// Collect bundle performance data
	var bundlePerformances []BundlePerformance
	var bundleFrequencies []BundleQueryFrequency
	globalHotKeyMap := make(map[string]bool)

	uptime := time.Since(mm.startTime)
	hoursRunning := uptime.Hours()

	for bundleName, scanner := range mm.scanners {
		metrics := scanner.GetMetrics()
		if metrics == nil {
			continue
		}

		// Aggregate totals
		totalScans += metrics.TotalScans
		totalCacheHits += metrics.CacheHits
		totalMemoryGCs += metrics.MemoryPressureGCs
		totalErrors += metrics.ScanErrors

		// Calculate weighted average latency
		if metrics.TotalScans > 0 {
			totalLatency += time.Duration(int64(metrics.AverageLatency) * metrics.TotalScans)
		}

		// Collect hot keys
		hotKeys := scanner.GetHotKeys()
		mm.globalMetrics.HotKeysByBundle[bundleName] = hotKeys
		for _, key := range hotKeys {
			globalHotKeyMap[key] = true
		}

		// Bundle performance
		cacheHitRate := 0.0
		if metrics.TotalScans > 0 {
			cacheHitRate = float64(metrics.CacheHits) / float64(metrics.TotalScans)
		}

		errorRate := 0.0
		if metrics.TotalScans > 0 {
			errorRate = float64(metrics.ScanErrors) / float64(metrics.TotalScans)
		}

		bundlePerformances = append(bundlePerformances, BundlePerformance{
			BundleName:     bundleName,
			AverageLatency: metrics.AverageLatency,
			ScanCount:      metrics.TotalScans,
			CacheHitRate:   cacheHitRate,
			ErrorRate:      errorRate,
		})

		// Bundle query frequency
		queriesPerHour := 0.0
		if hoursRunning > 0 {
			queriesPerHour = float64(metrics.TotalScans) / hoursRunning
		}

		bundleFrequencies = append(bundleFrequencies, BundleQueryFrequency{
			BundleName:     bundleName,
			QueryCount:     metrics.TotalScans,
			QueriesPerHour: queriesPerHour,
			UniqueHotKeys:  len(hotKeys),
		})
	}

	// Update global aggregates
	mm.globalMetrics.TotalScans = totalScans
	mm.globalMetrics.TotalMemoryPressureGCs = totalMemoryGCs
	mm.globalMetrics.TotalErrors = totalErrors

	if totalScans > 0 {
		mm.globalMetrics.AverageLatency = totalLatency / time.Duration(totalScans)
		mm.globalMetrics.GlobalCacheHitRate = float64(totalCacheHits) / float64(totalScans)
	}

	// Convert global hot keys map to slice
	globalHotKeys := make([]string, 0, len(globalHotKeyMap))
	for key := range globalHotKeyMap {
		globalHotKeys = append(globalHotKeys, key)
	}
	mm.globalMetrics.GlobalHotKeys = globalHotKeys

	// Sort and store top performers
	mm.sortBundlePerformances(bundlePerformances)
	mm.sortBundleFrequencies(bundleFrequencies)

	mm.globalMetrics.SlowestBundles = bundlePerformances
	mm.globalMetrics.MostQueriedBundles = bundleFrequencies
}

// generateOptimizationRecommendations creates optimization recommendations based on current metrics
func (mm *MetricsManager) generateOptimizationRecommendations() {
	var recommendations []OptimizationRecommendation
	now := time.Now()

	for bundleName, scanner := range mm.scanners {
		metrics := scanner.GetMetrics()
		if metrics == nil {
			continue
		}

		// Recommend indexing for hot keys
		hotKeys := scanner.GetHotKeys()
		for _, keyName := range hotKeys {
			if len(hotKeys) > 3 { // Only recommend if bundle has multiple hot keys
				recommendations = append(recommendations, OptimizationRecommendation{
					Type:           "index",
					BundleName:     bundleName,
					KeyName:        keyName,
					Priority:       "high",
					Reason:         "Key is accessed frequently and would benefit from indexing",
					EstimatedGain:  "50-90% query time reduction",
					Implementation: "CREATE INDEX ON " + bundleName + " (" + keyName + ")",
					CreatedAt:      now,
				})
			}
		}

		// Recommend caching for high-latency bundles
		if metrics.AverageLatency > time.Millisecond*100 && metrics.CacheHitRate < 0.3 {
			recommendations = append(recommendations, OptimizationRecommendation{
				Type:           "cache",
				BundleName:     bundleName,
				Priority:       "medium",
				Reason:         "High average latency with low cache hit rate",
				EstimatedGain:  "30-70% latency reduction",
				Implementation: "Increase cache size or adjust cache strategy",
				CreatedAt:      now,
			})
		}

		// Recommend memory optimization for high GC pressure
		if metrics.MemoryPressureGCs > 10 {
			recommendations = append(recommendations, OptimizationRecommendation{
				Type:           "memory",
				BundleName:     bundleName,
				Priority:       "medium",
				Reason:         "High memory pressure causing frequent garbage collection",
				EstimatedGain:  "10-30% performance improvement",
				Implementation: "Reduce batch size or increase memory threshold",
				CreatedAt:      now,
			})
		}
	}

	mm.globalMetrics.RecommendedOptimizations = recommendations
}

// Helper methods for sorting

func (mm *MetricsManager) sortBundlePerformances(performances []BundlePerformance) {
	// Simple bubble sort by average latency (descending)
	for i := 0; i < len(performances)-1; i++ {
		for j := i + 1; j < len(performances); j++ {
			if performances[j].AverageLatency > performances[i].AverageLatency {
				performances[i], performances[j] = performances[j], performances[i]
			}
		}
	}
}

func (mm *MetricsManager) sortBundleFrequencies(frequencies []BundleQueryFrequency) {
	// Simple bubble sort by query count (descending)
	for i := 0; i < len(frequencies)-1; i++ {
		for j := i + 1; j < len(frequencies); j++ {
			if frequencies[j].QueryCount > frequencies[i].QueryCount {
				frequencies[i], frequencies[j] = frequencies[j], frequencies[i]
			}
		}
	}
}
