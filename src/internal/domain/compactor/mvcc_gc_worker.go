package compactor

/*
MVCC GC WORKER - Background Garbage Collection for MVCC Versions

This file implements a background worker that automatically performs MVCC-aware
garbage collection. The worker scans databases/bundles/indexes, identifies version
accumulation, and triggers MVCC-aware compaction to remove obsolete versions while
preserving versions needed by active transactions.

KEY RESPONSIBILITIES:
- Scan all databases/bundles every N seconds (configurable, default 30s)
- Identify documents/index entries with excessive version accumulation
- Trigger MVCC-aware compaction when version count or age thresholds are exceeded
- Pause during high query load to prioritize user queries
- Run at low priority using runtime.Gosched() to yield CPU
- Trigger immediate GC on server startup and shutdown

DESIGN PRINCIPLES:
- Single Responsibility: Only handles MVCC version cleanup
- Non-blocking: Uses goroutines and channels for lifecycle management
- Load-aware: Pauses when server is busy (default: 500+ concurrent queries)
- MVCC-aware: Always checks oldest snapshot before removing versions
- Safe: Uses per-bundle locks to prevent conflicts with user operations

PATTERN ALIGNMENT:
- Follows GhostCleanupWorker pattern exactly
- Background goroutine with periodic ticker
- Load-aware pausing
- CPU yielding with runtime.Gosched()
- Graceful lifecycle management (Start/Stop with channels)
- Immediate trigger support for startup/shutdown
*/

import (
	"context"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/journal"
	"syndrdb/src/pkg/fatal"

	"go.uber.org/zap"
)

// MVCCGCWorker manages automatic background MVCC garbage collection
// Follows SQL Server's ghost cleanup pattern for removing obsolete versions
type MVCCGCWorker struct {
	// Configuration
	interval             time.Duration  // How often to run GC cycle (default: 30s)
	batchSize            int            // Max bundles/indexes to process per cycle (default: 10)
	pauseThreshold       int            // Pause if active queries >= this (default: 500)
	maxVersionsThreshold int            // Trigger compaction if document has >N versions (default: 5)
	minVersionAge        time.Duration  // Minimum age before considering version for GC (default: 1h)
	dataDir              string         // Data directory for bundle file paths
	activeQueryCount     *atomic.Uint64 // Server's query counter for load detection

	// Vacuum configuration (dead version reclamation)
	vacuumDeadRatioThreshold float64 // Trigger full compaction at this dead ratio (default: 0.3)
	vacuumMaxPagesPerCycle   int     // Max pages to scan per GC cycle (default: 100)
	vacuumEnabled            bool    // Enable page-level dead version reclamation (default: true)

	// Service access (using interface to avoid import cycles)
	serviceManagerGetter  func() interface{} // Returns ServiceManager to avoid direct import
	snapshotManagerGetter func() interface{} // Returns SnapshotManager to check oldest snapshot

	// Lifecycle management
	stopChan chan struct{}  // Signal to stop the worker
	doneChan chan struct{}  // Signal that worker has stopped
	wg       sync.WaitGroup // Wait for goroutine to exit

	// Compaction manager for triggering compactions
	compactor *CompactionManager

	// Metrics reporting (callback to avoid import cycles)
	metricsReporter MetricsReporter

	// Logging
	logger *zap.SugaredLogger

	// Immediate GC trigger support
	immediateGCChan  chan string // Channel for triggering immediate GC ("startup" or "shutdown")
	immediateGCMutex sync.Mutex  // Protects immediate GC execution
}

// MVCCGCConfig holds configuration for the worker
type MVCCGCConfig struct {
	Interval              time.Duration
	BatchSize             int
	PauseThreshold        int
	MaxVersionsThreshold  int
	MinVersionAge         time.Duration
	DataDir               string             // Data directory for bundle file paths
	ServiceManagerGetter  func() interface{} // Function to get ServiceManager (avoids import cycle)
	SnapshotManagerGetter func() interface{} // Function to get SnapshotManager (avoids import cycle)
	ActiveQueryCount      *atomic.Uint64
	Compactor             *CompactionManager
	MetricsReporter       MetricsReporter // Callback for reporting metrics
	Logger                *zap.SugaredLogger

	// Vacuum configuration (dead version reclamation)
	VacuumDeadRatioThreshold float64 // Trigger full compaction at this dead ratio (default: 0.3)
	VacuumMaxPagesPerCycle   int     // Max pages to scan per GC cycle (default: 100)
	VacuumEnabled            bool    // Enable page-level dead version reclamation (default: true)
}

// NewMVCCGCWorker creates a new MVCC GC worker
func NewMVCCGCWorker(config MVCCGCConfig) *MVCCGCWorker {
	// Validate configuration
	if config.Interval < 5*time.Second {
		config.Logger.Warnw("MVCC GC interval too low, using 30 seconds",
			"configured", config.Interval,
			"minimum", 5*time.Second)
		config.Interval = 30 * time.Second
	}

	if config.BatchSize <= 0 {
		config.Logger.Warnw("Invalid batch size, using default",
			"configured", config.BatchSize,
			"default", 10)
		config.BatchSize = 10
	}

	if config.PauseThreshold < 0 {
		config.Logger.Warnw("Invalid pause threshold, using default",
			"configured", config.PauseThreshold,
			"default", 500)
		config.PauseThreshold = 500
	}

	if config.MaxVersionsThreshold <= 0 {
		config.Logger.Warnw("Invalid max versions threshold, using default",
			"configured", config.MaxVersionsThreshold,
			"default", 5)
		config.MaxVersionsThreshold = 5
	}

	if config.MinVersionAge <= 0 {
		config.Logger.Warnw("Invalid min version age, using default",
			"configured", config.MinVersionAge,
			"default", 1*time.Hour)
		config.MinVersionAge = 1 * time.Hour
	}

	// Default vacuum settings
	vacuumDeadRatio := config.VacuumDeadRatioThreshold
	if vacuumDeadRatio <= 0 {
		vacuumDeadRatio = 0.3
	}
	vacuumMaxPages := config.VacuumMaxPagesPerCycle
	if vacuumMaxPages <= 0 {
		vacuumMaxPages = 100
	}
	vacuumEnabled := config.VacuumEnabled
	// Default to true if not explicitly set (zero value = false, but we want default true)
	// Callers that want to disable must explicitly set VacuumEnabled = false
	// Since Go zero value for bool is false, we use the config directly; server.go sets it

	return &MVCCGCWorker{
		interval:                 config.Interval,
		batchSize:                config.BatchSize,
		pauseThreshold:           config.PauseThreshold,
		maxVersionsThreshold:     config.MaxVersionsThreshold,
		minVersionAge:            config.MinVersionAge,
		dataDir:                  config.DataDir,
		serviceManagerGetter:     config.ServiceManagerGetter,
		snapshotManagerGetter:    config.SnapshotManagerGetter,
		activeQueryCount:         config.ActiveQueryCount,
		compactor:                config.Compactor,
		metricsReporter:          config.MetricsReporter,
		vacuumDeadRatioThreshold: vacuumDeadRatio,
		vacuumMaxPagesPerCycle:   vacuumMaxPages,
		vacuumEnabled:            vacuumEnabled,
		stopChan:                 make(chan struct{}),
		doneChan:                 make(chan struct{}),
		immediateGCChan:          make(chan string, 1), // Buffered to allow non-blocking trigger
		logger:                   config.Logger,
	}
}

// Start begins the background GC worker
func (w *MVCCGCWorker) Start() {
	w.logger.Infow("Starting MVCC GC worker",
		"interval", w.interval,
		"batchSize", w.batchSize,
		"pauseThreshold", w.pauseThreshold,
		"maxVersionsThreshold", w.maxVersionsThreshold,
		"minVersionAge", w.minVersionAge)

	// Start background goroutine
	w.wg.Add(1)
	go w.run()
}

// Stop gracefully stops the GC worker
func (w *MVCCGCWorker) Stop() {
	w.logger.Info("Stopping MVCC GC worker...")

	// Signal the worker to stop
	close(w.stopChan)

	// Wait for worker to finish with timeout
	done := make(chan struct{})
	go func() {
		<-w.doneChan
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("MVCC GC worker stopped gracefully")
	case <-time.After(30 * time.Second):
		w.logger.Warn("MVCC GC worker stop timed out after 30 seconds")
	}

	// Wait for goroutine to exit
	w.wg.Wait()
}

// TriggerImmediateGC triggers an immediate GC cycle (for startup/shutdown)
// triggerType should be "startup" or "shutdown" for logging/metrics
func (w *MVCCGCWorker) TriggerImmediateGC(triggerType string) {
	w.immediateGCMutex.Lock()
	defer w.immediateGCMutex.Unlock()

	w.logger.Infow("Triggering immediate MVCC GC",
		"triggerType", triggerType)

	// Use context with timeout to prevent hanging during shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Execute GC cycle synchronously
	startTime := time.Now()
	w.performGCCycleWithContext(ctx, triggerType)
	duration := time.Since(startTime)

	// Track metrics
	if w.metricsReporter != nil {
		if triggerType == "startup" {
			w.metricsReporter("MVCCGCStartupTriggers", 1)
		} else if triggerType == "shutdown" {
			w.metricsReporter("MVCCGCShutdownTriggers", 1)
		}
	}

	w.logger.Infow("Immediate MVCC GC completed",
		"triggerType", triggerType,
		"duration", duration)
}

// run is the main worker loop
func (w *MVCCGCWorker) run() {
	defer func() {
		if r := recover(); r != nil {
			fatal.LogFatalAndExit(r)
		}
	}()
	defer w.wg.Done()
	defer close(w.doneChan)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.logger.Info("MVCC GC worker running")

	for {
		select {
		case <-ticker.C:
			// Run periodic GC cycle
			w.performGCCycle()

		case triggerType := <-w.immediateGCChan:
			// Run immediate GC cycle (startup/shutdown)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			w.performGCCycleWithContext(ctx, triggerType)
			cancel()

		case <-w.stopChan:
			w.logger.Info("MVCC GC worker received stop signal")
			return
		}
	}
}

// performGCCycle executes one GC cycle (periodic)
func (w *MVCCGCWorker) performGCCycle() {
	ctx := context.Background()
	w.performGCCycleWithContext(ctx, "periodic")
}

// performGCCycleWithContext executes one GC cycle with context support
func (w *MVCCGCWorker) performGCCycleWithContext(ctx context.Context, triggerType string) {
	startTime := time.Now()

	// Check if we should pause due to high load (skip for immediate triggers)
	if triggerType == "periodic" && w.shouldPauseForLoad() {
		activeQueries := w.activeQueryCount.Load()
		w.logger.Debugw("MVCC GC paused due to high query load",
			"activeQueries", activeQueries,
			"threshold", w.pauseThreshold)

		// Increment metric
		if w.metricsReporter != nil {
			w.metricsReporter("MVCCGCPausedForLoad", 1)
		}
		return
	}

	// Track metrics
	if w.metricsReporter != nil {
		w.metricsReporter("MVCCGCCyclesTotal", 1)
	}

	// Get oldest active snapshot from SnapshotManager
	oldestSnapshotSeq := w.getOldestSnapshotSequence()
	if oldestSnapshotSeq == 0 {
		// No active transactions, use current global sequence as boundary
		// This allows more aggressive cleanup when no transactions are active
		if snapshotMgr := w.getSnapshotManager(); snapshotMgr != nil {
			oldestSnapshotSeq = snapshotMgr.GetCurrentSequence()
		}
	}

	w.logger.Debugw("MVCC GC cycle starting",
		"triggerType", triggerType,
		"oldestSnapshotSeq", oldestSnapshotSeq)

	// Scan databases and bundles
	bundlesScanned := 0
	compactionsTriggered := 0
	versionsRemoved := uint64(0)
	versionsPreserved := uint64(0)

	// Get service manager to access database service
	serviceMgr := w.getServiceManager()
	if serviceMgr == nil {
		w.logger.Warn("Service manager not available, skipping GC cycle")
		return
	}

	// Access database and bundle services using reflection to avoid import cycles
	// ServiceManager is in server package, so we use reflection to access fields
	serviceMgrValue := reflect.ValueOf(serviceMgr)
	if serviceMgrValue.Kind() == reflect.Ptr {
		serviceMgrValue = serviceMgrValue.Elem()
	}

	// Get DatabaseService field
	dbServiceField := serviceMgrValue.FieldByName("DatabaseService")
	if !dbServiceField.IsValid() || dbServiceField.IsNil() {
		w.logger.Warn("DatabaseService not available, skipping GC cycle")
		return
	}
	databaseService, ok := dbServiceField.Interface().(*database.DatabaseService)
	if !ok || databaseService == nil {
		w.logger.Warn("DatabaseService type assertion failed, skipping GC cycle")
		return
	}

	// Get BundleService field
	bundleServiceField := serviceMgrValue.FieldByName("BundleService")
	if !bundleServiceField.IsValid() || bundleServiceField.IsNil() {
		w.logger.Warn("BundleService not available, skipping GC cycle")
		return
	}
	bundleService, ok := bundleServiceField.Interface().(*bundle.BundleService)
	if !ok || bundleService == nil {
		w.logger.Warn("BundleService type assertion failed, skipping GC cycle")
		return
	}

	// Get list of databases
	databases := databaseService.ListDatabases()
	if len(databases) == 0 {
		w.logger.Debug("No databases found, skipping GC cycle")
		return
	}

	// Process databases (limit by batch size)
	processedCount := 0
	for _, db := range databases {
		if processedCount >= w.batchSize {
			break
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			w.logger.Info("MVCC GC cycle cancelled by context")
			return
		default:
		}

		// Process bundles in this database
		for bundleName := range db.Bundles {
			if processedCount >= w.batchSize {
				break
			}

			// Check context cancellation
			select {
			case <-ctx.Done():
				w.logger.Info("MVCC GC cycle cancelled by context")
				return
			default:
			}

			shouldCompact, versionCount, err := w.analyzeBundleVersions(ctx, bundleName, db.Name, bundleService)
			if err != nil {
				w.logger.Warnw("Failed to analyze bundle versions",
					"bundle", bundleName,
					"database", db.Name,
					"error", err)
				continue
			}

			bundlesScanned++

			if shouldCompact {
				// Phase 2a: Lightweight page-level dead version reclamation
				if w.vacuumEnabled {
					removed, preserved, err := w.reclaimDeadVersionsInBundle(ctx, bundleName, db.Name, bundleService, oldestSnapshotSeq)
					if err != nil {
						w.logger.Warnw("Failed to reclaim dead versions",
							"bundle", bundleName,
							"database", db.Name,
							"error", err)
					} else {
						versionsRemoved += uint64(removed)
						versionsPreserved += uint64(preserved)

						// Phase 2b: Trigger full segment compaction if dead ratio is high
						if removed > 0 {
							deadRatio := float64(removed) / float64(removed+preserved)
							if deadRatio > w.vacuumDeadRatioThreshold {
								err := w.triggerMVCCCompaction(bundleName, db.Name)
								if err != nil {
									w.logger.Warnw("Failed to trigger MVCC compaction after vacuum",
										"bundle", bundleName,
										"database", db.Name,
										"error", err)
								} else {
									compactionsTriggered++
								}
							}
						}
					}
				} else {
					// Vacuum disabled — fall back to old compaction-only path
					versions, err := w.sampleDocumentVersions(ctx, bundleName, db.Name, bundleService)
					if err != nil {
						w.logger.Warnw("Failed to sample document versions for age analysis",
							"bundle", bundleName,
							"database", db.Name,
							"error", err)
						continue
					}

					if w.shouldCompactByAge(oldestSnapshotSeq, versions) {
						err := w.triggerMVCCCompaction(bundleName, db.Name)
						if err != nil {
							w.logger.Warnw("Failed to trigger MVCC compaction",
								"bundle", bundleName,
								"database", db.Name,
								"error", err)
						} else {
							compactionsTriggered++
							versionsRemoved += uint64(versionCount - 1)
							versionsPreserved += 1
						}
					} else {
						versionsPreserved += uint64(versionCount)
					}
				}
			} else {
				versionsPreserved += uint64(versionCount)
			}

			processedCount++

			// Yield CPU after processing each bundle
			runtime.Gosched()
		}
	}

	// Scan hash indexes for version accumulation (simplified - can be enhanced later)
	// TODO: Implement analyzeIndexVersions for hash indexes

	duration := time.Since(startTime)
	if w.metricsReporter != nil {
		w.metricsReporter("MVCCGCDurationMs", uint64(duration.Milliseconds()))
		w.metricsReporter("MVCCGCBundlesScanned", uint64(bundlesScanned))
		w.metricsReporter("MVCCGCCompactionsTriggered", uint64(compactionsTriggered))
		w.metricsReporter("MVCCGCVersionsRemoved", versionsRemoved)
		w.metricsReporter("MVCCGCVersionsPreserved", versionsPreserved)
	}

	w.logger.Debugw("MVCC GC cycle complete",
		"triggerType", triggerType,
		"duration", duration,
		"bundlesScanned", bundlesScanned,
		"compactionsTriggered", compactionsTriggered,
		"versionsRemoved", versionsRemoved,
		"versionsPreserved", versionsPreserved)
}

// shouldPauseForLoad checks if GC should pause due to high query load
func (w *MVCCGCWorker) shouldPauseForLoad() bool {
	if w.pauseThreshold == 0 {
		return false // Never pause
	}

	activeQueries := w.activeQueryCount.Load()
	return activeQueries >= uint64(w.pauseThreshold)
}

// getOldestSnapshotSequence retrieves the oldest active snapshot sequence
func (w *MVCCGCWorker) getOldestSnapshotSequence() uint64 {
	snapshotMgr := w.getSnapshotManager()
	if snapshotMgr == nil {
		return 0
	}
	return snapshotMgr.GetOldestActiveSnapshot()
}

// getSnapshotManager retrieves the SnapshotManager from the getter
func (w *MVCCGCWorker) getSnapshotManager() *journal.SnapshotManager {
	if w.snapshotManagerGetter == nil {
		return nil
	}

	result := w.snapshotManagerGetter()
	if snapshotMgr, ok := result.(*journal.SnapshotManager); ok {
		return snapshotMgr
	}

	return nil
}

// getServiceManager retrieves the ServiceManager from the getter
func (w *MVCCGCWorker) getServiceManager() interface{} {
	if w.serviceManagerGetter == nil {
		return nil
	}
	return w.serviceManagerGetter()
}

// analyzeBundleVersions analyzes a bundle to detect version accumulation.
// Uses GetDocumentChunksForIndexing to sample doc IDs, then GetDocumentVersions per doc.
// Returns: shouldCompact, versionCount (avg), error.
//
// PERFORMANCE FIX: Now properly stops after collecting sampleSize documents.
// Previously the callback always returned false (continue), causing it to scan ALL documents
// in the bundle even though only 100 samples were needed. This caused MVCC GC to take 30+ seconds
// on bundles with many documents.
func (w *MVCCGCWorker) analyzeBundleVersions(ctx context.Context, bundleName, databaseName string, bundleService *bundle.BundleService) (bool, int, error) {
	const sampleSize = 100
	maxVersionsFound := 0
	totalVersions := 0
	samplesChecked := 0
	var docIDs []string

	err := bundleService.GetDocumentChunksForIndexing(ctx, bundleName, sampleSize, func(chunk []*models.Document) (stop bool) {
		for _, d := range chunk {
			docIDs = append(docIDs, d.DocumentID)
			// PERFORMANCE FIX: Stop once we have enough samples
			if len(docIDs) >= sampleSize {
				return true // stop = true means "stop iterating"
			}
		}
		// Continue if we don't have enough samples yet
		return len(docIDs) >= sampleSize
	})
	if err != nil {
		return false, 0, err
	}

	// Limit to sampleSize in case we got slightly more
	if len(docIDs) > sampleSize {
		docIDs = docIDs[:sampleSize]
	}

	for _, docID := range docIDs {
		versions, err := bundleService.GetDocumentVersions(bundleName, databaseName, docID)
		if err != nil {
			w.logger.Debugw("GetDocumentVersions failed for sample", "docID", docID, "error", err)
			continue
		}
		n := len(versions)
		if n > maxVersionsFound {
			maxVersionsFound = n
		}
		totalVersions += n
		samplesChecked++
	}

	avgVersions := 0
	if samplesChecked > 0 {
		avgVersions = totalVersions / samplesChecked
	}
	shouldCompact := maxVersionsFound > w.maxVersionsThreshold || (samplesChecked > 0 && avgVersions > 2)
	return shouldCompact, avgVersions, nil
}

// sampleDocumentVersions samples document versions for age analysis.
// Uses GetDocumentChunksForIndexing to get a chunk of docs, then GetDocumentVersions for each.
//
// PERFORMANCE FIX: Now properly stops after collecting chunkSize documents.
// Previously the callback always returned false (continue), causing it to scan ALL documents
// in the bundle even though only 50 samples were needed.
func (w *MVCCGCWorker) sampleDocumentVersions(ctx context.Context, bundleName, databaseName string, bundleService *bundle.BundleService) ([]*models.Document, error) {
	const chunkSize = 50
	var docIDs []string
	err := bundleService.GetDocumentChunksForIndexing(ctx, bundleName, chunkSize, func(chunk []*models.Document) (stop bool) {
		for _, d := range chunk {
			docIDs = append(docIDs, d.DocumentID)
			// PERFORMANCE FIX: Stop once we have enough samples
			if len(docIDs) >= chunkSize {
				return true // stop = true means "stop iterating"
			}
		}
		// Continue if we don't have enough samples yet
		return len(docIDs) >= chunkSize
	})
	if err != nil {
		return nil, err
	}

	// Limit to chunkSize in case we got slightly more
	if len(docIDs) > chunkSize {
		docIDs = docIDs[:chunkSize]
	}

	var all []*models.Document
	for _, docID := range docIDs {
		versions, err := bundleService.GetDocumentVersions(bundleName, databaseName, docID)
		if err != nil {
			continue
		}
		all = append(all, versions...)
	}
	return all, nil
}

// shouldCompactByAge checks if versions are old enough for GC
// RCU Model: Uses grace period (100ms) to ensure in-flight reads complete
func (w *MVCCGCWorker) shouldCompactByAge(oldestSnapshotSeq uint64, versions []*models.Document) bool {
	if len(versions) == 0 {
		return false
	}

	// Count versions that are safe to cleanup using RCU grace period
	safeToCleanup := 0
	for _, version := range versions {
		// RCU check: Use IsSafeToCleanup which checks grace period (100ms)
		if version.IsSafeToCleanup() {
			safeToCleanup++
			continue
		}

		// Legacy MVCC check for documents without SupersededAt set
		if version.CommitSequence > 0 && oldestSnapshotSeq > 0 && version.CommitSequence < oldestSnapshotSeq {
			safeToCleanup++
		}
	}

	// Trigger compaction if we have versions safe to cleanup
	// Require at least 2 versions (1 current + 1 superseded) to avoid unnecessary work
	return safeToCleanup > 0 && len(versions) >= 2
}

// canCleanupDocument checks if a specific document version can be removed
// RCU Model: Respects grace period for in-flight reads
func (w *MVCCGCWorker) canCleanupDocument(doc *models.Document, oldestSnapshotSeq uint64) bool {
	if doc == nil {
		return false
	}

	// RCU check: Current versions are never cleaned up
	if doc.IsCurrentVersion() {
		return false
	}

	// RCU check: Grace period must have passed
	if doc.IsSafeToCleanup() {
		return true
	}

	// Legacy MVCC check for backward compatibility
	if doc.CommitSequence > 0 && oldestSnapshotSeq > 0 && doc.CommitSequence < oldestSnapshotSeq {
		return true
	}

	return false
}

// triggerMVCCCompaction triggers MVCC-aware compaction for a bundle
func (w *MVCCGCWorker) triggerMVCCCompaction(bundleName, databaseName string) error {
	if w.compactor == nil {
		return nil // Compactor not available, skip
	}

	// Build bundle file path
	// Format: {dataDir}/{database}/{bundle}.bnd
	if w.dataDir == "" {
		w.logger.Warnw("Data directory not available, cannot trigger compaction",
			"bundle", bundleName,
			"database", databaseName)
		return nil
	}

	bundleFilePath := filepath.Join(w.dataDir, databaseName, bundleName+".bnd")

	// Check if compaction is already in progress (via CompactionManager state)
	// The CompactionManager uses per-bundle locks, so we can safely trigger compaction

	// Trigger compaction (MVCC filtering is already enabled via getOldestSnapshot callback)
	_, err := w.compactor.CompactBundleFile(bundleName, databaseName, bundleFilePath)
	if err != nil {
		return err
	}

	w.logger.Debugw("Triggered MVCC compaction",
		"bundle", bundleName,
		"database", databaseName)

	return nil
}

// reclaimDeadVersionsInBundle performs lightweight page-level dead version reclamation.
// Iterates pages in the bundle and removes dead document versions from the in-memory page cache.
// This is the equivalent of PostgreSQL's VACUUM for individual heap tuples.
//
// Dead versions are removed from the page cache immediately. On-disk cleanup happens
// when full segment compaction is triggered (when dead ratio exceeds threshold).
//
// Returns: (removed, preserved, error)
func (w *MVCCGCWorker) reclaimDeadVersionsInBundle(
	ctx context.Context,
	bundleName, databaseName string,
	bundleService *bundle.BundleService,
	safeCutoff uint64,
) (int, int, error) {
	if safeCutoff == 0 {
		return 0, 0, nil // No safe cutoff — can't reclaim anything
	}

	// Get bundle metadata to find page count
	bundleMeta := w.getBundleMetadata(bundleService, bundleName)
	if bundleMeta == nil {
		return 0, 0, nil
	}

	pageCount := int(bundleMeta.PageCount)
	if pageCount == 0 {
		return 0, 0, nil
	}

	totalRemoved := 0
	totalPreserved := 0
	pagesScanned := 0

	for pageID := 0; pageID < pageCount && pagesScanned < w.vacuumMaxPagesPerCycle; pageID++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return totalRemoved, totalPreserved, ctx.Err()
		default:
		}

		removedDocIDs, remaining, err := bundleService.RemoveDeadVersionsFromPage(bundleName, uint32(pageID), safeCutoff)
		if err != nil {
			w.logger.Debugw("Failed to reclaim dead versions from page",
				"bundle", bundleName,
				"pageID", pageID,
				"error", err)
			continue
		}

		removed := len(removedDocIDs)
		totalRemoved += removed
		totalPreserved += remaining
		pagesScanned++

		// Clean up index entries for reclaimed documents
		if removed > 0 {
			w.cleanupIndexEntriesForBundle(bundleName, databaseName, bundleService, bundleMeta, removedDocIDs)

			w.logger.Debugw("Reclaimed dead versions from page",
				"bundle", bundleName,
				"pageID", pageID,
				"removed", removed,
				"remaining", remaining)
		}

		// Yield CPU periodically
		if pagesScanned%10 == 0 {
			runtime.Gosched()
		}
	}

	if totalRemoved > 0 {
		w.logger.Infow("Dead version reclamation complete",
			"bundle", bundleName,
			"database", databaseName,
			"pagesScanned", pagesScanned,
			"totalRemoved", totalRemoved,
			"totalPreserved", totalPreserved)

		// Report metrics
		if w.metricsReporter != nil {
			w.metricsReporter("MVCCVacuumPagesScanned", uint64(pagesScanned))
			w.metricsReporter("MVCCVacuumVersionsRemoved", uint64(totalRemoved))
		}
	}

	return totalRemoved, totalPreserved, nil
}

// cleanupIndexEntriesForBundle removes index entries pointing to reclaimed document versions.
// For hash indexes: schedules delete operations via scheduleIndexUpdate.
// For B-tree indexes: schedules delete operations for the reclaimed document IDs.
func (w *MVCCGCWorker) cleanupIndexEntriesForBundle(
	bundleName, databaseName string,
	bundleService *bundle.BundleService,
	bundleMeta *models.Bundle,
	reclaimedDocIDs []string,
) {
	if bundleMeta.Indexes == nil || len(reclaimedDocIDs) == 0 {
		return
	}

	for indexName, indexRef := range bundleMeta.Indexes {
		switch indexRef.IndexType {
		case "hash":
			// Hash indexes: schedule delete for each reclaimed document
			for _, docID := range reclaimedDocIDs {
				bundleService.ScheduleIndexUpdateForVacuum(bundleName, indexName, "hash", "delete", docID, docID, 0)
			}
		case "btree":
			// B-tree indexes: schedule delete for each reclaimed document
			// The field value is unknown at this point, but the index can handle deletion by docID
			for _, docID := range reclaimedDocIDs {
				bundleService.ScheduleIndexUpdateForVacuum(bundleName, indexName, "btree", "delete", docID, nil, 0)
			}
		case "brin":
			// BRIN indexes don't store per-document entries — no cleanup needed
		}
	}
}

// getBundleMetadata retrieves bundle metadata from the bundle service.
func (w *MVCCGCWorker) getBundleMetadata(bundleService *bundle.BundleService, bundleName string) *models.Bundle {
	allBundles := bundleService.GetAllBundles()
	if allBundles == nil {
		return nil
	}
	return allBundles[bundleName]
}
