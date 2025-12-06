/*
auto_analyze_tracker.go

This file implements automatic statistics analysis triggering based on document
modifications and version detection.

DESIGN PRINCIPLES:
- Threshold-Based Triggering: Automatic analysis after significant data changes
- Whichever-Larger Logic: max(1000 changes, 10% of bundle size) for robustness
- Version Detection: Automatic re-analysis on statistics format mismatch

TRIGGERING STRATEGY:
- Track document changes (inserts, updates, deletes) per bundle
- Trigger when changes >= max(1000, bundle_size * 0.10)
- Immediately trigger on version mismatch detection
- Reset counter after successful analysis

This implements PostgreSQL's autovacuum-style statistics maintenance with
SyndrDB-specific adaptations for document-based storage.
*/

package statistics

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	DEFAULT_AUTO_ANALYZE_THRESHOLD = 1000 // Minimum 1000 document changes
	DEFAULT_AUTO_ANALYZE_RATIO     = 0.10 // Or 10% of bundle size
)

// BundleChangeTracker tracks document changes for a single bundle
type BundleChangeTracker struct {
	BundleName          string
	TotalDocuments      int64
	ChangesSinceAnalyze int64
	LastAnalyzed        time.Time
	NeedsReAnalyze      bool // Force flag for version mismatch
	mu                  sync.RWMutex
}

// AutoAnalyzeTracker manages auto-analyze triggers across all bundles
type AutoAnalyzeTracker struct {
	trackers          map[string]*BundleChangeTracker
	absoluteThreshold int
	ratioThreshold    float64
	statsStorage      *StatisticsStorage
	logger            *zap.SugaredLogger
	mu                sync.RWMutex
	analysisQueue     chan string // Bundle names pending analysis
	stopCh            chan struct{}
	wg                sync.WaitGroup
}

// AutoAnalyzeConfig holds configuration for auto-analyze
type AutoAnalyzeConfig struct {
	Enabled           bool
	AbsoluteThreshold int     // Minimum number of changes
	RatioThreshold    float64 // Ratio of changes to bundle size
	WorkerCount       int     // Number of background workers
}

// NewAutoAnalyzeTracker creates a new auto-analyze tracker
func NewAutoAnalyzeTracker(config AutoAnalyzeConfig, statsStorage *StatisticsStorage, logger *zap.SugaredLogger) *AutoAnalyzeTracker {
	if config.AbsoluteThreshold == 0 {
		config.AbsoluteThreshold = DEFAULT_AUTO_ANALYZE_THRESHOLD
	}
	if config.RatioThreshold == 0.0 {
		config.RatioThreshold = DEFAULT_AUTO_ANALYZE_RATIO
	}
	if config.WorkerCount == 0 {
		config.WorkerCount = 2 // Default 2 background workers
	}

	tracker := &AutoAnalyzeTracker{
		trackers:          make(map[string]*BundleChangeTracker),
		absoluteThreshold: config.AbsoluteThreshold,
		ratioThreshold:    config.RatioThreshold,
		statsStorage:      statsStorage,
		logger:            logger,
		analysisQueue:     make(chan string, 100), // Buffer up to 100 bundles
		stopCh:            make(chan struct{}),
	}

	// Start background workers if enabled
	if config.Enabled {
		for i := 0; i < config.WorkerCount; i++ {
			tracker.wg.Add(1)
			go tracker.analysisWorker(i)
		}
		logger.Infof("Auto-analyze tracker started with %d workers (threshold: %d changes or %.1f%% ratio)",
			config.WorkerCount, config.AbsoluteThreshold, config.RatioThreshold*100)
	} else {
		logger.Info("Auto-analyze tracker created but disabled")
	}

	return tracker
}

// RegisterBundle registers a bundle for tracking
func (aat *AutoAnalyzeTracker) RegisterBundle(bundleName string, totalDocuments int64) {
	aat.mu.Lock()
	defer aat.mu.Unlock()

	if _, exists := aat.trackers[bundleName]; exists {
		// Update total documents count
		tracker := aat.trackers[bundleName]
		tracker.mu.Lock()
		tracker.TotalDocuments = totalDocuments
		tracker.mu.Unlock()
		return
	}

	// Check if statistics exist and are current version
	stats, err := aat.statsStorage.LoadStatistics(bundleName)
	var lastAnalyzed time.Time
	var needsReAnalyze bool

	if err != nil {
		aat.logger.Debugf("No existing statistics for bundle %s: %v", bundleName, err)
		needsReAnalyze = true
	} else if stats.StatsVersion != STATS_VERSION {
		aat.logger.Warnf("Statistics version mismatch for %s: found %d, expected %d (forcing re-analysis)",
			bundleName, stats.StatsVersion, STATS_VERSION)
		needsReAnalyze = true
	} else {
		lastAnalyzed = stats.LastAnalyzed
	}

	aat.trackers[bundleName] = &BundleChangeTracker{
		BundleName:          bundleName,
		TotalDocuments:      totalDocuments,
		ChangesSinceAnalyze: 0,
		LastAnalyzed:        lastAnalyzed,
		NeedsReAnalyze:      needsReAnalyze,
	}

	// Queue immediate analysis if version mismatch or no stats
	if needsReAnalyze {
		aat.logger.Infof("Queueing immediate analysis for %s (version mismatch or missing stats)", bundleName)
		select {
		case aat.analysisQueue <- bundleName:
		default:
			aat.logger.Warnf("Analysis queue full, skipping immediate analysis for %s", bundleName)
		}
	}
}

// TrackInserts tracks document insertions
func (aat *AutoAnalyzeTracker) TrackInserts(bundleName string, count int64) {
	aat.trackChanges(bundleName, count, count, 0)
}

// TrackUpdates tracks document updates
func (aat *AutoAnalyzeTracker) TrackUpdates(bundleName string, count int64) {
	aat.trackChanges(bundleName, 0, 0, count)
}

// TrackDeletes tracks document deletions
func (aat *AutoAnalyzeTracker) TrackDeletes(bundleName string, count int64) {
	aat.trackChanges(bundleName, -count, 0, count)
}

// trackChanges is the internal method that tracks all changes
func (aat *AutoAnalyzeTracker) trackChanges(bundleName string, netDocumentChange, insertCount, modifyCount int64) {
	aat.mu.RLock()
	tracker, exists := aat.trackers[bundleName]
	aat.mu.RUnlock()

	if !exists {
		aat.logger.Warnf("Bundle %s not registered with auto-analyze tracker", bundleName)
		return
	}

	tracker.mu.Lock()
	tracker.TotalDocuments += netDocumentChange
	tracker.ChangesSinceAnalyze += insertCount + modifyCount
	totalDocs := tracker.TotalDocuments
	changesSince := tracker.ChangesSinceAnalyze
	needsReAnalyze := tracker.NeedsReAnalyze
	tracker.mu.Unlock()

	// Check if analysis threshold reached
	if needsReAnalyze || aat.shouldAnalyze(totalDocs, changesSince) {
		aat.logger.Infof("Auto-analyze threshold reached for %s: %d changes (%d total docs, threshold: %d)",
			bundleName, changesSince, totalDocs, aat.calculateThreshold(totalDocs))

		// Queue for analysis
		select {
		case aat.analysisQueue <- bundleName:
			aat.logger.Debugf("Queued %s for auto-analysis", bundleName)
		default:
			aat.logger.Warnf("Analysis queue full, bundle %s will be analyzed later", bundleName)
		}
	}
}

// shouldAnalyze determines if a bundle should be analyzed
func (aat *AutoAnalyzeTracker) shouldAnalyze(totalDocuments, changesSinceAnalyze int64) bool {
	threshold := aat.calculateThreshold(totalDocuments)
	return changesSinceAnalyze >= threshold
}

// calculateThreshold implements whichever-larger logic: max(1000, 10% of bundle size)
func (aat *AutoAnalyzeTracker) calculateThreshold(totalDocuments int64) int64 {
	ratioThreshold := int64(float64(totalDocuments) * aat.ratioThreshold)
	if ratioThreshold > int64(aat.absoluteThreshold) {
		return ratioThreshold
	}
	return int64(aat.absoluteThreshold)
}

// MarkAnalyzed resets the change counter after successful analysis
func (aat *AutoAnalyzeTracker) MarkAnalyzed(bundleName string) {
	aat.mu.RLock()
	tracker, exists := aat.trackers[bundleName]
	aat.mu.RUnlock()

	if !exists {
		return
	}

	tracker.mu.Lock()
	tracker.ChangesSinceAnalyze = 0
	tracker.LastAnalyzed = time.Now()
	tracker.NeedsReAnalyze = false
	tracker.mu.Unlock()

	aat.logger.Infof("Reset change counter for %s after successful analysis", bundleName)
}

// GetTrackerStats returns current tracking statistics
func (aat *AutoAnalyzeTracker) GetTrackerStats(bundleName string) (totalDocs, changes int64, lastAnalyzed time.Time, needsAnalysis bool) {
	aat.mu.RLock()
	tracker, exists := aat.trackers[bundleName]
	aat.mu.RUnlock()

	if !exists {
		return 0, 0, time.Time{}, false
	}

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	return tracker.TotalDocuments, tracker.ChangesSinceAnalyze, tracker.LastAnalyzed,
		tracker.NeedsReAnalyze || aat.shouldAnalyze(tracker.TotalDocuments, tracker.ChangesSinceAnalyze)
}

// ForceAnalysis forces immediate analysis of a bundle
func (aat *AutoAnalyzeTracker) ForceAnalysis(bundleName string) error {
	aat.mu.RLock()
	tracker, exists := aat.trackers[bundleName]
	aat.mu.RUnlock()

	if !exists {
		return fmt.Errorf("bundle %s not registered", bundleName)
	}

	tracker.mu.Lock()
	tracker.NeedsReAnalyze = true
	tracker.mu.Unlock()

	// Queue for immediate analysis
	select {
	case aat.analysisQueue <- bundleName:
		aat.logger.Infof("Forced analysis queued for %s", bundleName)
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout queuing analysis for %s", bundleName)
	}
}

// analysisWorker processes analysis queue
func (aat *AutoAnalyzeTracker) analysisWorker(workerID int) {
	defer aat.wg.Done()
	aat.logger.Infof("Auto-analyze worker %d started", workerID)

	for {
		select {
		case bundleName := <-aat.analysisQueue:
			aat.logger.Infof("Worker %d: analyzing bundle %s", workerID, bundleName)

			// TODO: I will integrate with actual bundle loading and StatisticsCollector
			// For now, just log the analysis request
			aat.logger.Infof("Worker %d: analysis placeholder for %s (integration pending)", workerID, bundleName)

			// Mark as analyzed (in real implementation, this would be after successful analysis)
			// aat.MarkAnalyzed(bundleName)

		case <-aat.stopCh:
			aat.logger.Infof("Auto-analyze worker %d stopping", workerID)
			return
		}
	}
}

// Stop gracefully shuts down the tracker
func (aat *AutoAnalyzeTracker) Stop() {
	aat.logger.Info("Stopping auto-analyze tracker...")
	close(aat.stopCh)
	aat.wg.Wait()
	aat.logger.Info("Auto-analyze tracker stopped")
}

// GetPendingAnalysisCount returns the number of bundles waiting for analysis
func (aat *AutoAnalyzeTracker) GetPendingAnalysisCount() int {
	return len(aat.analysisQueue)
}

// ListTrackedBundles returns all tracked bundle names
func (aat *AutoAnalyzeTracker) ListTrackedBundles() []string {
	aat.mu.RLock()
	defer aat.mu.RUnlock()

	bundles := make([]string, 0, len(aat.trackers))
	for name := range aat.trackers {
		bundles = append(bundles, name)
	}
	return bundles
}
