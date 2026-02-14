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
	mm.logger.Debugf("Registered scanner for bundle '%s'", bundleName)

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

	//mm.logger.Infof("Started metrics collection with interval %v", mm.metricsInterval)

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

// JoinIndexMetrics tracks metrics for index-assisted join operations
// These metrics provide insights into join optimization effectiveness
type JoinIndexMetrics struct {
	// Overall index usage statistics
	TotalJoins         int64   `json:"total_joins"`          // Total join operations executed
	IndexAssistedJoins int64   `json:"index_assisted_joins"` // Joins that used index optimization
	IndexUsageRate     float64 `json:"index_usage_rate"`     // Percentage of joins using indexes
	IndexFallbackCount int64   `json:"index_fallback_count"` // Times index probe failed and fell back

	// Performance metrics
	AverageIndexLookupTime time.Duration `json:"average_index_lookup_time"` // Average time for index lookups
	TotalIndexLookupTime   time.Duration `json:"total_index_lookup_time"`   // Total time spent in index lookups
	AverageScanReduction   float64       `json:"average_scan_reduction"`    // Average % of scans avoided (0-100)
	AverageSpeedup         float64       `json:"average_speedup"`           // Average speedup multiplier (e.g., 500x)

	// Index selectivity tracking
	AverageSelectivity float64 `json:"average_selectivity"` // Average rows returned per index lookup
	BestSelectivity    float64 `json:"best_selectivity"`    // Best (lowest) selectivity observed
	WorstSelectivity   float64 `json:"worst_selectivity"`   // Worst (highest) selectivity observed

	// Strategy selection tracking
	StrategySelection map[string]int64 `json:"strategy_selection"` // Count by strategy name
	ProbeIndexUsed    int64            `json:"probe_index_used"`   // Times probe-side index was used
	BuildIndexUsed    int64            `json:"build_index_used"`   // Times build-side index was used

	// Accuracy and estimation
	EstimationAccuracy float64 `json:"estimation_accuracy"` // Average % accuracy of speedup estimates
	Underperformances  int64   `json:"underperformances"`   // Times actual < 80% of estimated
	Outperformances    int64   `json:"outperformances"`     // Times actual > 120% of estimated

	// Bundle-specific tracking
	IndexUsageByBundle map[string]int64 `json:"index_usage_by_bundle"` // Index usage count per bundle

	// Time tracking
	LastUpdated time.Time    `json:"last_updated"`
	mu          sync.RWMutex `json:"-"` // Protects concurrent access
}

// JoinIndexMetricsSnapshot is a snapshot of JoinIndexMetrics without the mutex
// This is safe to return by value and can be serialized
type JoinIndexMetricsSnapshot struct {
	TotalJoins             int64            `json:"total_joins"`
	IndexAssistedJoins     int64            `json:"index_assisted_joins"`
	IndexUsageRate         float64          `json:"index_usage_rate"`
	IndexFallbackCount     int64            `json:"index_fallback_count"`
	AverageIndexLookupTime time.Duration    `json:"average_index_lookup_time"`
	TotalIndexLookupTime   time.Duration    `json:"total_index_lookup_time"`
	AverageScanReduction   float64          `json:"average_scan_reduction"`
	AverageSpeedup         float64          `json:"average_speedup"`
	AverageSelectivity     float64          `json:"average_selectivity"`
	BestSelectivity        float64          `json:"best_selectivity"`
	WorstSelectivity       float64          `json:"worst_selectivity"`
	StrategySelection      map[string]int64 `json:"strategy_selection"`
	ProbeIndexUsed         int64            `json:"probe_index_used"`
	BuildIndexUsed         int64            `json:"build_index_used"`
	EstimationAccuracy     float64          `json:"estimation_accuracy"`
	Underperformances      int64            `json:"underperformances"`
	Outperformances        int64            `json:"outperformances"`
	IndexUsageByBundle     map[string]int64 `json:"index_usage_by_bundle"`
	LastUpdated            time.Time        `json:"last_updated"`
}

// NewJoinIndexMetrics creates a new join index metrics tracker
func NewJoinIndexMetrics() *JoinIndexMetrics {
	return &JoinIndexMetrics{
		StrategySelection:  make(map[string]int64),
		IndexUsageByBundle: make(map[string]int64),
		LastUpdated:        time.Now(),
	}
}

// RecordJoinExecution records metrics for a join operation
// bundleName: Name of the probe bundle where index was used (if applicable)
// usedIndex: Whether index was used for this join
// strategy: Name of the index strategy used (e.g., "index_assisted_probe")
func (m *JoinIndexMetrics) RecordJoinExecution(bundleName string, usedIndex bool, strategy string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalJoins++

	if usedIndex {
		m.IndexAssistedJoins++
		if bundleName != "" {
			m.IndexUsageByBundle[bundleName]++
		}

		if strategy != "" {
			m.StrategySelection[strategy]++

			// Track specific strategy usage
			if strategy == "index_assisted_probe" {
				m.ProbeIndexUsed++
			} else if strategy == "index_assisted_build" {
				m.BuildIndexUsed++
			}
		}
	}

	// Update index usage rate
	if m.TotalJoins > 0 {
		m.IndexUsageRate = float64(m.IndexAssistedJoins) / float64(m.TotalJoins) * 100
	}

	m.LastUpdated = time.Now()
}

// RecordIndexLookup records metrics for an index lookup operation
// lookupTime: Duration of the index lookup
// selectivity: Number of rows returned per lookup
func (m *JoinIndexMetrics) RecordIndexLookup(lookupTime time.Duration, selectivity float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalIndexLookupTime += lookupTime

	// Update average lookup time (running average)
	if m.IndexAssistedJoins > 0 {
		m.AverageIndexLookupTime = m.TotalIndexLookupTime / time.Duration(m.IndexAssistedJoins)
	}

	// Update selectivity tracking
	// Use running average for average selectivity
	if m.AverageSelectivity == 0 {
		m.AverageSelectivity = selectivity
		m.BestSelectivity = selectivity
		m.WorstSelectivity = selectivity
	} else {
		// Weighted running average
		weight := 0.1 // 10% weight for new value
		m.AverageSelectivity = m.AverageSelectivity*(1-weight) + selectivity*weight

		if selectivity < m.BestSelectivity {
			m.BestSelectivity = selectivity
		}
		if selectivity > m.WorstSelectivity {
			m.WorstSelectivity = selectivity
		}
	}

	m.LastUpdated = time.Now()
}

// RecordPerformance records actual vs estimated performance
// scanReduction: Percentage of scans avoided (0-100)
// actualSpeedup: Actual speedup multiplier achieved
// estimatedSpeedup: Estimated speedup multiplier
func (m *JoinIndexMetrics) RecordPerformance(scanReduction float64, actualSpeedup float64, estimatedSpeedup float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update average scan reduction (running average)
	if m.AverageScanReduction == 0 {
		m.AverageScanReduction = scanReduction
	} else {
		weight := 0.1 // 10% weight for new value
		m.AverageScanReduction = m.AverageScanReduction*(1-weight) + scanReduction*weight
	}

	// Update average speedup (running average)
	if m.AverageSpeedup == 0 {
		m.AverageSpeedup = actualSpeedup
	} else {
		weight := 0.1
		m.AverageSpeedup = m.AverageSpeedup*(1-weight) + actualSpeedup*weight
	}

	// Track estimation accuracy
	if estimatedSpeedup > 0 {
		accuracy := (1.0 - ((actualSpeedup - estimatedSpeedup) / estimatedSpeedup)) * 100
		if accuracy < 0 {
			accuracy = -accuracy // Make it absolute
		}

		// Update running average of accuracy
		if m.EstimationAccuracy == 0 {
			m.EstimationAccuracy = accuracy
		} else {
			weight := 0.1
			m.EstimationAccuracy = m.EstimationAccuracy*(1-weight) + accuracy*weight
		}

		// Track underperformance and outperformance
		if actualSpeedup < estimatedSpeedup*0.8 {
			m.Underperformances++
		} else if actualSpeedup > estimatedSpeedup*1.2 {
			m.Outperformances++
		}
	}

	m.LastUpdated = time.Now()
}

// RecordFallback records when index-assisted join falls back to full scan
func (m *JoinIndexMetrics) RecordFallback() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.IndexFallbackCount++
	m.LastUpdated = time.Now()
}

// GetSnapshot returns a snapshot of current metrics (thread-safe)
func (m *JoinIndexMetrics) GetSnapshot() JoinIndexMetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a snapshot without the mutex
	snapshot := JoinIndexMetricsSnapshot{
		TotalJoins:             m.TotalJoins,
		IndexAssistedJoins:     m.IndexAssistedJoins,
		IndexUsageRate:         m.IndexUsageRate,
		IndexFallbackCount:     m.IndexFallbackCount,
		AverageIndexLookupTime: m.AverageIndexLookupTime,
		TotalIndexLookupTime:   m.TotalIndexLookupTime,
		AverageScanReduction:   m.AverageScanReduction,
		AverageSpeedup:         m.AverageSpeedup,
		AverageSelectivity:     m.AverageSelectivity,
		BestSelectivity:        m.BestSelectivity,
		WorstSelectivity:       m.WorstSelectivity,
		ProbeIndexUsed:         m.ProbeIndexUsed,
		BuildIndexUsed:         m.BuildIndexUsed,
		EstimationAccuracy:     m.EstimationAccuracy,
		Underperformances:      m.Underperformances,
		Outperformances:        m.Outperformances,
		LastUpdated:            m.LastUpdated,
		StrategySelection:      make(map[string]int64),
		IndexUsageByBundle:     make(map[string]int64),
	}

	// Deep copy the maps
	for k, v := range m.StrategySelection {
		snapshot.StrategySelection[k] = v
	}
	for k, v := range m.IndexUsageByBundle {
		snapshot.IndexUsageByBundle[k] = v
	}

	return snapshot
}

// Reset resets all metrics (useful for testing)
func (m *JoinIndexMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalJoins = 0
	m.IndexAssistedJoins = 0
	m.IndexUsageRate = 0
	m.IndexFallbackCount = 0
	m.AverageIndexLookupTime = 0
	m.TotalIndexLookupTime = 0
	m.AverageScanReduction = 0
	m.AverageSpeedup = 0
	m.AverageSelectivity = 0
	m.BestSelectivity = 0
	m.WorstSelectivity = 0
	m.ProbeIndexUsed = 0
	m.BuildIndexUsed = 0
	m.EstimationAccuracy = 0
	m.Underperformances = 0
	m.Outperformances = 0
	m.StrategySelection = make(map[string]int64)
	m.IndexUsageByBundle = make(map[string]int64)
	m.LastUpdated = time.Now()
}
