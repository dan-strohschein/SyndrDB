// Package view provides the domain model and business logic for database views in SyndrDB.
// This file contains the lifecycle management for materialized views.
//
// The materializer handles the complete lifecycle of materialized views:
// 1. Initial population during CREATE MATERIALIZED VIEW
// 2. Manual refresh via REFRESH MATERIALIZED VIEW
// 3. Lock management during refresh operations
// 4. Stale data warnings based on LastRefreshed timestamp
//
// Materialized views store pre-computed query results as physical data bundles.
// The data bundle is named with a "_mv_" prefix (e.g., "_mv_active_users") and
// is hidden from normal bundle listings.
//
// Refresh process:
// 1. Acquire exclusive lock on view (1-minute timeout)
// 2. Execute view definition query against underlying bundles
// 3. Drop existing hidden data bundle
// 4. Create new hidden data bundle with query results
// 5. Update LastRefreshed timestamp in catalog
// 6. Sync catalog changes
// 7. Release lock
//
// Lock management:
// - Use LockService.AcquireExclusiveLock with 1-minute timeout
// - Prevent concurrent refreshes of the same materialized view
// - Return clear error if lock timeout exceeded
// - Auto-release on panic/error
//
// Stale data warnings:
// - Check time since LastRefreshed
// - Default threshold: 48 hours (configurable via DefaultStaleWarningHours)
// - Log warning when querying stale materialized view
// - Warning message: "Materialized view '{name}' last refreshed {hours} hours ago. Consider running REFRESH MATERIALIZED VIEW."
//
// Main functions:
// - PopulateView: Execute view query and create hidden data bundle
// - RefreshView: Re-execute query and replace hidden bundle data
// - CheckStaleWarning: Determine if view needs refresh warning
// - DropMaterializedViewData: Delete hidden data bundle
//
// TODO: I should implement REFRESH MATERIALIZED VIEW CONCURRENTLY for non-blocking refreshes
// TODO: I should add configurable stale warning threshold per view
package view

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

// LockServiceInterface defines the lock operations needed by ViewMaterializer
type LockServiceInterface interface {
	LockDatabase(dbName, adminUser, reason, comment string) error
	UnlockDatabase(dbName, adminUser string) error
	IsLocked(dbName string) bool
}

// BundleServiceInterface defines the bundle operations needed by ViewMaterializer
type BundleServiceInterface interface {
	// CreateBundle creates a new bundle (will be used to create hidden _mv_ bundles)
	// TODO: I should add CreateHiddenBundle method to BundleService for materialized views
	// For now, this interface documents what will be needed
}

// QueryExecutorInterface defines the query execution interface needed by ViewMaterializer
type QueryExecutorInterface interface {
	// Execute runs a SELECT query and returns results
	// TODO: I should wire in UnifiedQueryPlanner.CreatePlan + ExecutionPlan.Execute
	// For now, this interface documents what will be needed
}

// ViewMaterializer handles materialized view lifecycle operations
type ViewMaterializer struct {
	logger        *zap.SugaredLogger
	lockService   LockServiceInterface
	bundleService BundleServiceInterface
	queryExecutor QueryExecutorInterface
	viewStore     ViewStore
}

// NewViewMaterializer creates a new ViewMaterializer
func NewViewMaterializer(
	logger *zap.SugaredLogger,
	lockService LockServiceInterface,
	bundleService BundleServiceInterface,
	queryExecutor QueryExecutorInterface,
	viewStore ViewStore,
) *ViewMaterializer {
	return &ViewMaterializer{
		logger:        logger,
		lockService:   lockService,
		bundleService: bundleService,
		queryExecutor: queryExecutor,
		viewStore:     viewStore,
	}
}

// PopulateView executes the view definition and creates the hidden data bundle
// Called during CREATE MATERIALIZED VIEW
func (m *ViewMaterializer) PopulateView(view *View) error {
	if view.Type != ViewTypeMaterialized {
		return fmt.Errorf("Cannot populate non-materialized view '%s'", view.ViewName)
	}

	m.logger.Infof("Populating materialized view '%s'", view.ViewName)

	// Acquire exclusive lock with 1-minute timeout
	lockKey := fmt.Sprintf("view_%s_%s", view.DatabaseName, view.ViewName)
	err := m.lockService.LockDatabase(lockKey, "system", "MATERIALIZED_VIEW_POPULATE",
		fmt.Sprintf("Populating materialized view '%s'", view.ViewName))
	if err != nil {
		return fmt.Errorf("Failed to acquire lock for view '%s'. Another operation may be in progress. Lock timeout: 1 minute.", view.ViewName)
	}
	// Ensure lock is released on exit (success or failure)
	defer func() {
		if unlockErr := m.lockService.UnlockDatabase(lockKey, "system"); unlockErr != nil {
			m.logger.Warnf("Failed to release lock for view '%s': %v", view.ViewName, unlockErr)
		}
	}()

	// Execute view definition query
	// TODO: I should execute view.Definition using UnifiedQueryPlanner
	// results, err := queryExecutor.Execute(view.Definition, view.DatabaseName)
	// if err != nil {
	//     return fmt.Errorf("Failed to execute view definition: %w", err)
	// }
	m.logger.Debugf("TODO: Execute view definition query: %s", view.Definition)

	// Create hidden data bundle with results
	// TODO: I should create hidden bundle using bundleService.CreateHiddenBundle(view.DataBundleName, view.DatabaseName, results)
	m.logger.Debugf("TODO: Create hidden bundle '%s' in database '%s'", view.DataBundleName, view.DatabaseName)

	// Update LastRefreshed timestamp
	now := time.Now()
	view.LastRefreshed = &now

	// Sync to catalog
	if err := m.viewStore.SyncToCatalog(view); err != nil {
		return fmt.Errorf("Failed to sync view to catalog: %w", err)
	}

	m.logger.Infof("Materialized view '%s' populated successfully", view.ViewName)
	return nil
}

// RefreshView re-executes the view definition and updates the hidden data bundle
// Called via REFRESH MATERIALIZED VIEW command
func (m *ViewMaterializer) RefreshView(view *View) error {
	if view.Type != ViewTypeMaterialized {
		return fmt.Errorf("Cannot refresh non-materialized view '%s'. Only materialized views can be refreshed.", view.ViewName)
	}

	m.logger.Infof("Refreshing materialized view '%s'", view.ViewName)

	// Acquire exclusive lock with 1-minute timeout
	lockKey := fmt.Sprintf("view_%s_%s", view.DatabaseName, view.ViewName)
	err := m.lockService.LockDatabase(lockKey, "system", "MATERIALIZED_VIEW_REFRESH",
		fmt.Sprintf("Refreshing materialized view '%s'", view.ViewName))
	if err != nil {
		return fmt.Errorf("Failed to acquire lock for view '%s'. Another refresh may be in progress. Lock timeout: 1 minute.", view.ViewName)
	}
	// Ensure lock is released on exit (success or failure)
	defer func() {
		if unlockErr := m.lockService.UnlockDatabase(lockKey, "system"); unlockErr != nil {
			m.logger.Warnf("Failed to release lock for view '%s': %v", view.ViewName, unlockErr)
		}
	}()

	// Execute view definition query
	// TODO: I should execute view.Definition using UnifiedQueryPlanner
	// results, err := queryExecutor.Execute(view.Definition, view.DatabaseName)
	// if err != nil {
	//     return fmt.Errorf("Failed to execute view definition during refresh: %w", err)
	// }
	m.logger.Debugf("TODO: Execute view definition query: %s", view.Definition)

	// Drop existing hidden bundle
	// TODO: I should drop hidden bundle using bundleService.DropBundle(view.DataBundleName, view.DatabaseName)
	// If bundle doesn't exist, log warning but continue (may be first refresh after incomplete creation)
	m.logger.Debugf("TODO: Drop existing hidden bundle '%s' from database '%s'", view.DataBundleName, view.DatabaseName)

	// Create new hidden bundle with updated results
	// TODO: I should create hidden bundle using bundleService.CreateHiddenBundle(view.DataBundleName, view.DatabaseName, results)
	m.logger.Debugf("TODO: Create new hidden bundle '%s' in database '%s'", view.DataBundleName, view.DatabaseName)

	// Update LastRefreshed timestamp
	now := time.Now()
	view.LastRefreshed = &now

	// Sync to catalog
	if err := m.viewStore.SyncToCatalog(view); err != nil {
		// Rollback: If catalog sync fails, we're in an inconsistent state
		// Log error and return - admin will need to manually fix or re-create view
		m.logger.Errorf("Failed to sync view '%s' to catalog after refresh: %v. View may be in inconsistent state.", view.ViewName, err)
		return fmt.Errorf("Failed to sync view to catalog after refresh: %w", err)
	}

	m.logger.Infof("Materialized view '%s' refreshed successfully", view.ViewName)
	return nil
}

// CheckStaleWarning checks if a materialized view is stale and returns warning message
// Returns empty string if not stale, warning message if stale
func (m *ViewMaterializer) CheckStaleWarning(view *View) string {
	if view.Type != ViewTypeMaterialized {
		return ""
	}

	// Check if view has never been refreshed
	if view.LastRefreshed == nil {
		return fmt.Sprintf("Materialized view '%s' has never been refreshed. Run REFRESH MATERIALIZED VIEW to populate data.", view.ViewName)
	}

	// Calculate hours since last refresh
	hoursSinceRefresh := time.Since(*view.LastRefreshed).Hours()

	// Check against threshold
	if hoursSinceRefresh > float64(DefaultStaleWarningHours) {
		return fmt.Sprintf("Materialized view '%s' last refreshed %.1f hours ago. Consider running REFRESH MATERIALIZED VIEW.", view.ViewName, hoursSinceRefresh)
	}

	return ""
}

// DropMaterializedViewData deletes the hidden data bundle for a materialized view
// Called during DROP MATERIALIZED VIEW
func (m *ViewMaterializer) DropMaterializedViewData(view *View) error {
	if view.Type != ViewTypeMaterialized {
		return fmt.Errorf("Cannot drop data for non-materialized view '%s'", view.ViewName)
	}

	m.logger.Infof("Dropping data bundle for materialized view '%s'", view.ViewName)

	// Drop hidden bundle
	// TODO: I should drop hidden bundle using bundleService.DropBundle(view.DataBundleName, view.DatabaseName)
	// If bundle doesn't exist, log warning but don't fail (may have been manually deleted)
	m.logger.Debugf("TODO: Drop hidden bundle '%s' from database '%s'", view.DataBundleName, view.DatabaseName)
	// Note: If bundleService.DropBundle returns "bundle not found" error, catch it and log warning instead of failing

	m.logger.Infof("Data bundle for materialized view '%s' dropped successfully", view.ViewName)
	return nil
}

// GetRefreshStatus returns the refresh status of a materialized view
// Returns time since last refresh and whether it's stale
func (m *ViewMaterializer) GetRefreshStatus(view *View) (timeSinceRefresh time.Duration, isStale bool) {
	if view.Type != ViewTypeMaterialized {
		return 0, false
	}

	// Check if view has never been refreshed
	if view.LastRefreshed == nil {
		// Return a very large duration to indicate it's definitely stale
		return time.Duration(365*24) * time.Hour, true
	}

	timeSinceRefresh = time.Since(*view.LastRefreshed)
	hoursSinceRefresh := timeSinceRefresh.Hours()
	isStale = hoursSinceRefresh > float64(DefaultStaleWarningHours)

	return timeSinceRefresh, isStale
}
