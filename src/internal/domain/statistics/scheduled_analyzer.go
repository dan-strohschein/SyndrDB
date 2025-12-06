/*
scheduled_analyzer.go

This file implements scheduled background statistics analysis for large bundles.

DESIGN PRINCIPLES:
- Time-Based Scheduling: Cron-like execution at configurable time (default 02:00 local)
- Size Filtering: Only analyze bundles >= 100k documents
- Coordination: Avoid duplicate work with AutoAnalyzeTracker
- Server Local Time: Use server's local timezone for scheduling

SCHEDULING STRATEGY:
- Parse HH:MM time configuration
- Calculate next execution time in server local timezone
- Sleep until next scheduled time
- Analyze eligible bundles (>=100k docs, not recently analyzed)
- Skip bundles already queued by auto-analyze

This implements PostgreSQL's autovacuum scheduler pattern with SyndrDB-specific
adaptations for document-based storage.
*/

package statistics

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	DEFAULT_SCHEDULED_TIME    = "02:00"         // 2 AM server local time
	DEFAULT_MIN_DOCS          = 100000          // 100k documents minimum
	SCHEDULE_CHECK_INTERVAL   = 1 * time.Minute // Check every minute
	MIN_TIME_BETWEEN_ANALYSES = 20 * time.Hour  // Don't re-analyze within 20 hours
)

// ScheduledAnalyzer performs scheduled background statistics analysis
type ScheduledAnalyzer struct {
	scheduledTime      time.Time // Daily scheduled time (hour:minute only)
	minDocuments       int64
	autoAnalyzeTracker *AutoAnalyzeTracker
	statsStorage       *StatisticsStorage
	logger             *zap.SugaredLogger
	stopCh             chan struct{}
	wg                 sync.WaitGroup
	mu                 sync.RWMutex
}

// ScheduledAnalyzerConfig holds configuration for scheduled analysis
type ScheduledAnalyzerConfig struct {
	Enabled       bool
	ScheduledTime string // HH:MM format
	MinDocuments  int
}

// NewScheduledAnalyzer creates a new scheduled analyzer
func NewScheduledAnalyzer(
	config ScheduledAnalyzerConfig,
	autoAnalyzeTracker *AutoAnalyzeTracker,
	statsStorage *StatisticsStorage,
	logger *zap.SugaredLogger,
) (*ScheduledAnalyzer, error) {
	if config.MinDocuments == 0 {
		config.MinDocuments = DEFAULT_MIN_DOCS
	}
	if config.ScheduledTime == "" {
		config.ScheduledTime = DEFAULT_SCHEDULED_TIME
	}

	// Parse scheduled time
	scheduledTime, err := parseScheduledTime(config.ScheduledTime)
	if err != nil {
		return nil, fmt.Errorf("invalid scheduled time format: %w", err)
	}

	analyzer := &ScheduledAnalyzer{
		scheduledTime:      scheduledTime,
		minDocuments:       int64(config.MinDocuments),
		autoAnalyzeTracker: autoAnalyzeTracker,
		statsStorage:       statsStorage,
		logger:             logger,
		stopCh:             make(chan struct{}),
	}

	if config.Enabled {
		analyzer.wg.Add(1)
		go analyzer.schedulerLoop()
		logger.Infof("Scheduled analyzer started (time: %s server local, min docs: %d)",
			config.ScheduledTime, config.MinDocuments)
	} else {
		logger.Info("Scheduled analyzer created but disabled")
	}

	return analyzer, nil
}

// schedulerLoop is the main scheduler loop
func (sa *ScheduledAnalyzer) schedulerLoop() {
	defer sa.wg.Done()
	sa.logger.Info("Scheduled analyzer loop started")

	ticker := time.NewTicker(SCHEDULE_CHECK_INTERVAL)
	defer ticker.Stop()

	var lastRun time.Time

	for {
		select {
		case now := <-ticker.C:
			if sa.shouldRunNow(now, lastRun) {
				sa.logger.Info("Starting scheduled analysis run...")
				if err := sa.runScheduledAnalysis(); err != nil {
					sa.logger.Errorf("Scheduled analysis failed: %v", err)
				} else {
					lastRun = now
					sa.logger.Info("Scheduled analysis completed successfully")
				}
			}

		case <-sa.stopCh:
			sa.logger.Info("Scheduled analyzer loop stopping")
			return
		}
	}
}

// shouldRunNow determines if scheduled analysis should run now
func (sa *ScheduledAnalyzer) shouldRunNow(now, lastRun time.Time) bool {
	// Check if we're within the scheduled time window
	scheduledHour := sa.scheduledTime.Hour()
	scheduledMinute := sa.scheduledTime.Minute()

	currentHour := now.Hour()
	currentMinute := now.Minute()

	// Must match scheduled hour and be within same minute
	if currentHour != scheduledHour || currentMinute != scheduledMinute {
		return false
	}

	// Prevent running multiple times within the same day
	if !lastRun.IsZero() {
		timeSinceLastRun := now.Sub(lastRun)
		if timeSinceLastRun < MIN_TIME_BETWEEN_ANALYSES {
			sa.logger.Debugf("Skipping scheduled run (last run was %v ago, minimum %v)",
				timeSinceLastRun, MIN_TIME_BETWEEN_ANALYSES)
			return false
		}
	}

	return true
}

// runScheduledAnalysis executes the scheduled analysis
func (sa *ScheduledAnalyzer) runScheduledAnalysis() error {
	// Get all tracked bundles
	trackedBundles := sa.autoAnalyzeTracker.ListTrackedBundles()

	analyzedCount := 0
	skippedCount := 0

	for _, bundleName := range trackedBundles {
		totalDocs, changes, lastAnalyzed, needsAnalysis := sa.autoAnalyzeTracker.GetTrackerStats(bundleName)

		// Skip if below minimum document threshold
		if totalDocs < sa.minDocuments {
			sa.logger.Debugf("Skipping %s: only %d documents (minimum: %d)",
				bundleName, totalDocs, sa.minDocuments)
			skippedCount++
			continue
		}

		// Skip if already flagged for auto-analysis (avoid duplicate work)
		if needsAnalysis {
			sa.logger.Debugf("Skipping %s: already queued for auto-analysis", bundleName)
			skippedCount++
			continue
		}

		// Skip if recently analyzed (within last 20 hours)
		if !lastAnalyzed.IsZero() {
			timeSinceAnalysis := time.Since(lastAnalyzed)
			if timeSinceAnalysis < MIN_TIME_BETWEEN_ANALYSES {
				sa.logger.Debugf("Skipping %s: analyzed %v ago (minimum %v)",
					bundleName, timeSinceAnalysis, MIN_TIME_BETWEEN_ANALYSES)
				skippedCount++
				continue
			}
		}

		// Queue for analysis
		sa.logger.Infof("Scheduling analysis for %s (%d documents, %d changes, last analyzed: %v)",
			bundleName, totalDocs, changes, lastAnalyzed)

		if err := sa.autoAnalyzeTracker.ForceAnalysis(bundleName); err != nil {
			sa.logger.Errorf("Failed to queue %s for analysis: %v", bundleName, err)
			continue
		}

		analyzedCount++
	}

	sa.logger.Infof("Scheduled analysis completed: %d bundles queued, %d skipped",
		analyzedCount, skippedCount)
	return nil
}

// UpdateScheduledTime updates the scheduled time (HH:MM format)
func (sa *ScheduledAnalyzer) UpdateScheduledTime(timeStr string) error {
	scheduledTime, err := parseScheduledTime(timeStr)
	if err != nil {
		return fmt.Errorf("invalid time format: %w", err)
	}

	sa.mu.Lock()
	defer sa.mu.Unlock()

	sa.scheduledTime = scheduledTime
	sa.logger.Infof("Updated scheduled time to %s (server local)", timeStr)
	return nil
}

// GetScheduledTime returns the current scheduled time
func (sa *ScheduledAnalyzer) GetScheduledTime() string {
	sa.mu.RLock()
	defer sa.mu.RUnlock()

	return fmt.Sprintf("%02d:%02d", sa.scheduledTime.Hour(), sa.scheduledTime.Minute())
}

// GetNextScheduledRun returns the next scheduled run time
func (sa *ScheduledAnalyzer) GetNextScheduledRun() time.Time {
	sa.mu.RLock()
	defer sa.mu.RUnlock()

	now := time.Now()
	scheduledHour := sa.scheduledTime.Hour()
	scheduledMinute := sa.scheduledTime.Minute()

	// Calculate next run time
	nextRun := time.Date(now.Year(), now.Month(), now.Day(),
		scheduledHour, scheduledMinute, 0, 0, now.Location())

	// If scheduled time has passed today, move to tomorrow
	if nextRun.Before(now) {
		nextRun = nextRun.Add(24 * time.Hour)
	}

	return nextRun
}

// Stop gracefully shuts down the analyzer
func (sa *ScheduledAnalyzer) Stop() {
	sa.logger.Info("Stopping scheduled analyzer...")
	close(sa.stopCh)
	sa.wg.Wait()
	sa.logger.Info("Scheduled analyzer stopped")
}

// Helper functions

// parseScheduledTime parses HH:MM time string to time.Time (only hour/minute used)
func parseScheduledTime(timeStr string) (time.Time, error) {
	// Parse as HH:MM
	t, err := time.Parse("15:04", timeStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected HH:MM format: %w", err)
	}
	return t, nil
}
