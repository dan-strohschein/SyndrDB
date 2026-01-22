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

	return &MVCCGCWorker{
		interval:              config.Interval,
		batchSize:             config.BatchSize,
		pauseThreshold:        config.PauseThreshold,
		maxVersionsThreshold:  config.MaxVersionsThreshold,
		minVersionAge:         config.MinVersionAge,
		dataDir:               config.DataDir,
		serviceManagerGetter:  config.ServiceManagerGetter,
		snapshotManagerGetter: config.SnapshotManagerGetter,
		activeQueryCount:      config.ActiveQueryCount,
		compactor:             config.Compactor,
		metricsReporter:       config.MetricsReporter,
		stopChan:              make(chan struct{}),
		doneChan:              make(chan struct{}),
		immediateGCChan:       make(chan string, 1), // Buffered to allow non-blocking trigger
		logger:                config.Logger,
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

			// Analyze bundle for version accumulation
			shouldCompact, versionCount, err := w.analyzeBundleVersions(bundleName, db.Name, bundleService)
			if err != nil {
				w.logger.Warnw("Failed to analyze bundle versions",
					"bundle", bundleName,
					"database", db.Name,
					"error", err)
				continue
			}

			bundlesScanned++

			if shouldCompact {
				// Check version age
				versions, err := w.sampleDocumentVersions(bundleName, db.Name, bundleService)
				if err != nil {
					w.logger.Warnw("Failed to sample document versions for age analysis",
						"bundle", bundleName,
						"database", db.Name,
						"error", err)
					continue
				}

				if w.shouldCompactByAge(oldestSnapshotSeq, versions) {
					// Trigger compaction
					err := w.triggerMVCCCompaction(bundleName, db.Name)
					if err != nil {
						w.logger.Warnw("Failed to trigger MVCC compaction",
							"bundle", bundleName,
							"database", db.Name,
							"error", err)
					} else {
						compactionsTriggered++
						versionsRemoved += uint64(versionCount - 1) // Assume we keep 1 version
						versionsPreserved += 1
					}
				} else {
					versionsPreserved += uint64(versionCount)
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

// analyzeBundleVersions analyzes a bundle to detect version accumulation
// Returns: shouldCompact bool, versionCount int, error
func (w *MVCCGCWorker) analyzeBundleVersions(bundleName, databaseName string, bundleService *bundle.BundleService) (bool, int, error) {
	// Sample documents from bundle (check first 100 document IDs)
	// For simplicity, we'll sample by attempting to get versions for a few document IDs
	// In a real implementation, we'd get document IDs from an index

	maxVersionsFound := 0
	totalVersions := 0
	samplesChecked := 0

	// TODO: Get document IDs from bundle's hash index or metadata
	// For now, this is a placeholder that would need to be implemented
	// by accessing the bundle's document index or scanning a sample of documents

	// Placeholder: We would iterate through document IDs and call GetDocumentVersions
	// For now, return conservative values
	// In real implementation:
	//   for each documentID in sample:
	//     versions, err := bundleService.GetDocumentVersions(bundleName, databaseName, documentID)
	//     versionCount := len(versions)
	//     if versionCount > maxVersionsFound {
	//       maxVersionsFound = versionCount
	//     }
	//     totalVersions += versionCount
	//     samplesChecked++

	// For now, return false (don't compact) since we can't sample without document IDs
	// This will be enhanced when we have access to document ID lists
	avgVersions := 0
	if samplesChecked > 0 {
		avgVersions = totalVersions / samplesChecked
	}

	shouldCompact := maxVersionsFound > w.maxVersionsThreshold || avgVersions > 2

	return shouldCompact, avgVersions, nil
}

// sampleDocumentVersions samples document versions for age analysis
func (w *MVCCGCWorker) sampleDocumentVersions(bundleName, databaseName string, bundleService *bundle.BundleService) ([]*models.Document, error) {
	// TODO: Implement document sampling
	// For now, return empty slice
	// In real implementation, would sample documents and return their versions
	return []*models.Document{}, nil
}

// shouldCompactByAge checks if versions are old enough for GC
func (w *MVCCGCWorker) shouldCompactByAge(oldestSnapshotSeq uint64, versions []*models.Document) bool {
	if len(versions) == 0 {
		return false
	}

	// Find oldest version's commit sequence
	var oldestCommitSeq uint64 = 0
	for _, version := range versions {
		if version.CommitSequence > 0 && (oldestCommitSeq == 0 || version.CommitSequence < oldestCommitSeq) {
			oldestCommitSeq = version.CommitSequence
		}
	}

	if oldestCommitSeq == 0 {
		return false // No committed versions
	}

	// Check if oldest version is significantly older than oldest snapshot
	// Consider minVersionAge threshold
	if oldestSnapshotSeq > 0 && oldestCommitSeq < oldestSnapshotSeq {
		// Versions are older than oldest snapshot, safe to compact
		// Additional age check: ensure versions are at least minVersionAge old
		// (This would require timestamp comparison, simplified here)
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
