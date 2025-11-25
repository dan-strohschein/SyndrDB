package graphQL

import (
	"runtime"
	"sort"
	"sync"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// QueryMetric represents metrics for a single GraphQL query execution
type QueryMetric struct {
	QueryID       string
	Username      string
	ClientIP      string
	Role          string
	IsAdmin       bool
	Query         string
	OperationType string // "query" or "mutation"
	IsDDL         bool

	// Timing metrics
	StartTime time.Time
	Duration  time.Duration

	// Complexity metrics
	Complexity int
	Depth      int

	// Rate limiting metrics
	TokensCost      int
	TokensAvailable float64

	// Result metrics
	Success         bool
	TimeoutOccurred bool // Whether query timed out
	ErrorCode       string
	ErrorMessage    string

	// Metadata
	Timestamp time.Time
}

// QueryMonitor tracks GraphQL query metrics for monitoring and alerting
type QueryMonitor struct {
	config        *settings.GraphQLSecurityConfig
	logger        *zap.SugaredLogger
	metrics       []*QueryMetric
	mu            sync.RWMutex
	stopPurge     chan struct{}
	purgeTicker   *time.Ticker
	totalMetrics  int64 // Total metrics recorded (for stats)
	purgedMetrics int64 // Total metrics purged (for stats)
}

// NewQueryMonitor creates a new query monitor with automatic purging
func NewQueryMonitor(config *settings.GraphQLSecurityConfig, logger *zap.SugaredLogger) *QueryMonitor {
	qm := &QueryMonitor{
		config:        config,
		logger:        logger,
		metrics:       make([]*QueryMetric, 0, 1000),
		stopPurge:     make(chan struct{}),
		totalMetrics:  0,
		purgedMetrics: 0,
	}

	// Start automatic purge routine
	qm.startPurgeRoutine()

	return qm
}

// startPurgeRoutine starts a background goroutine to purge old metrics
func (qm *QueryMonitor) startPurgeRoutine() {
	qm.purgeTicker = time.NewTicker(qm.config.MetricsPurgeInterval)

	go func() {
		for {
			select {
			case <-qm.purgeTicker.C:
				qm.purgeMetrics()
			case <-qm.stopPurge:
				qm.purgeTicker.Stop()
				return
			}
		}
	}()
}

// Stop stops the purge routine
func (qm *QueryMonitor) Stop() {
	close(qm.stopPurge)
}

// RecordQuery records a query metric
func (qm *QueryMonitor) RecordQuery(metric *QueryMetric) {
	if !qm.config.EnableQueryMonitoring {
		return
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()

	metric.Timestamp = time.Now()
	qm.metrics = append(qm.metrics, metric)
	qm.totalMetrics++

	// Check if we need immediate memory-based purge
	qm.checkMemoryPurge()

	// Log expensive queries
	if metric.Duration >= qm.config.ExpensiveQueryDuration {
		qm.logger.Warnw("Expensive GraphQL query detected",
			"queryID", metric.QueryID,
			"username", metric.Username,
			"duration", metric.Duration,
			"complexity", metric.Complexity,
			"operationType", metric.OperationType,
		)
	}

	// Log timeout events
	if metric.TimeoutOccurred {
		qm.logger.Warnw("Query timeout occurred",
			"queryID", metric.QueryID,
			"username", metric.Username,
			"duration", metric.Duration,
			"query", metric.Query,
		)
	}

	// Log abuse patterns (repeated failures)
	qm.checkAbusePatterns(metric)
}

// checkMemoryPurge checks if we've exceeded memory limits and purges if needed
func (qm *QueryMonitor) checkMemoryPurge() {
	// Get current memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	currentMemoryMB := m.Alloc / 1024 / 1024

	// Check if we've exceeded the memory cap
	if currentMemoryMB >= uint64(qm.config.MaxMemoryMB) {
		qm.logger.Warnw("Query monitor memory limit exceeded, triggering purge",
			"currentMemoryMB", currentMemoryMB,
			"limitMB", qm.config.MaxMemoryMB,
			"metricsCount", len(qm.metrics),
		)

		// Purge oldest 30% of metrics
		purgeCount := int(float64(len(qm.metrics)) * 0.30)
		if purgeCount > 0 {
			// Sort by timestamp (oldest first)
			sort.Slice(qm.metrics, func(i, j int) bool {
				return qm.metrics[i].Timestamp.Before(qm.metrics[j].Timestamp)
			})

			// Remove oldest 30%
			qm.metrics = qm.metrics[purgeCount:]
			qm.purgedMetrics += int64(purgeCount)

			qm.logger.Infow("Memory-based purge completed",
				"purgedCount", purgeCount,
				"remainingCount", len(qm.metrics),
			)
		}
	}
}

// purgeMetrics removes metrics older than the retention duration
func (qm *QueryMonitor) purgeMetrics() {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	now := time.Now()
	cutoffTime := now.Add(-qm.config.MetricsRetentionDuration)

	// Count metrics to purge
	purgeCount := 0
	for _, metric := range qm.metrics {
		if metric.Timestamp.Before(cutoffTime) {
			purgeCount++
		} else {
			break // Assuming metrics are roughly ordered by time
		}
	}

	if purgeCount > 0 {
		// Remove old metrics
		qm.metrics = qm.metrics[purgeCount:]
		qm.purgedMetrics += int64(purgeCount)

		qm.logger.Debugw("Age-based purge completed",
			"purgedCount", purgeCount,
			"remainingCount", len(qm.metrics),
			"cutoffTime", cutoffTime,
		)
	}

	// Also check memory limit during regular purge
	qm.checkMemoryPurge()
}

// checkAbusePatterns checks for abuse patterns (repeated failures, high rate)
func (qm *QueryMonitor) checkAbusePatterns(metric *QueryMetric) {
	if metric.Success {
		return
	}

	// Count recent failures for this user
	now := time.Now()
	recentWindow := now.Add(-5 * time.Minute)
	failureCount := 0

	for i := len(qm.metrics) - 1; i >= 0; i-- {
		m := qm.metrics[i]
		if m.Timestamp.Before(recentWindow) {
			break
		}
		if m.Username == metric.Username && !m.Success {
			failureCount++
		}
	}

	if failureCount >= qm.config.AbuseErrorThreshold {
		qm.logger.Errorw("Potential abuse detected - excessive failures",
			"username", metric.Username,
			"clientIP", metric.ClientIP,
			"failureCount", failureCount,
			"window", "5 minutes",
		)
	} else if failureCount >= qm.config.AbuseWarningThreshold {
		qm.logger.Warnw("High failure rate detected",
			"username", metric.Username,
			"clientIP", metric.ClientIP,
			"failureCount", failureCount,
			"window", "5 minutes",
		)
	}
}

// GetMetrics returns current metrics (for admin endpoints)
func (qm *QueryMonitor) GetMetrics(limit int) []*QueryMetric {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	if limit <= 0 || limit > len(qm.metrics) {
		limit = len(qm.metrics)
	}

	// Return most recent metrics
	start := len(qm.metrics) - limit
	if start < 0 {
		start = 0
	}

	result := make([]*QueryMetric, limit)
	copy(result, qm.metrics[start:])
	return result
}

// GetMetricsForUser returns metrics for a specific user
func (qm *QueryMonitor) GetMetricsForUser(username string, limit int) []*QueryMetric {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := make([]*QueryMetric, 0, limit)

	// Iterate backwards (most recent first)
	for i := len(qm.metrics) - 1; i >= 0 && len(result) < limit; i-- {
		if qm.metrics[i].Username == username {
			result = append(result, qm.metrics[i])
		}
	}

	return result
}

// GetStats returns global statistics about the query monitor
func (qm *QueryMonitor) GetStats() map[string]interface{} {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	// Calculate memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Count success/failure/timeout rates
	successCount := 0
	failureCount := 0
	timeoutCount := 0
	totalDuration := time.Duration(0)

	for _, metric := range qm.metrics {
		if metric.Success {
			successCount++
		} else {
			failureCount++
		}
		if metric.TimeoutOccurred {
			timeoutCount++
		}
		totalDuration += metric.Duration
	}

	avgDuration := time.Duration(0)
	if len(qm.metrics) > 0 {
		avgDuration = totalDuration / time.Duration(len(qm.metrics))
	}

	return map[string]interface{}{
		"totalMetricsRecorded": qm.totalMetrics,
		"totalMetricsPurged":   qm.purgedMetrics,
		"currentMetricsCount":  len(qm.metrics),
		"maxMetricsRetained":   qm.config.MaxMetricsRetained,
		"memoryUsageMB":        m.Alloc / 1024 / 1024,
		"memoryLimitMB":        qm.config.MaxMemoryMB,
		"successCount":         successCount,
		"failureCount":         failureCount,
		"timeoutCount":         timeoutCount,
		"successRate":          float64(successCount) / float64(len(qm.metrics)),
		"timeoutRate":          float64(timeoutCount) / float64(len(qm.metrics)),
		"avgDuration":          avgDuration.String(),
	}
}

// GetExpensiveQueries returns queries that exceeded the expensive query threshold
func (qm *QueryMonitor) GetExpensiveQueries(limit int) []*QueryMetric {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := make([]*QueryMetric, 0, limit)

	for i := len(qm.metrics) - 1; i >= 0 && len(result) < limit; i-- {
		metric := qm.metrics[i]
		if metric.Duration >= qm.config.ExpensiveQueryDuration {
			result = append(result, metric)
		}
	}

	return result
}

// GetFailedQueries returns queries that failed
func (qm *QueryMonitor) GetFailedQueries(limit int) []*QueryMetric {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	result := make([]*QueryMetric, 0, limit)

	for i := len(qm.metrics) - 1; i >= 0 && len(result) < limit; i-- {
		if !qm.metrics[i].Success {
			result = append(result, qm.metrics[i])
		}
	}

	return result
}
