// Package view provides the domain model and business logic for database views in SyndrDB.
// This file contains the ViewRegistry which maintains an in-memory cache of views.
//
// The registry provides fast lookups for view existence checks and metadata retrieval,
// avoiding disk I/O for common operations. It also manages permission caching to
// optimize authorization checks for view access.
//
// Key responsibilities:
// - In-memory view cache by database and view name
// - Fast view existence checks (case-sensitive)
// - Permission cache with expiration
// - Background cleanup of expired cache entries
// - Orphaned bundle detection on startup
//
// The registry follows the Singleton pattern per ViewService instance and uses
// read-write locks for thread-safe concurrent access.
//
// Main functions:
// - IsView: Fast check if a name refers to a view
// - GetView: Retrieve view from cache
// - RegisterView: Add view to cache
// - UnregisterView: Remove view from cache
// - ListViews: Get all views for a database
// - CheckPermission: Check cached permission
// - CachePermission: Store permission check result
// - StartCleanupRoutine: Begin background cache cleanup
//
// Permission caching reduces authorization overhead by storing permission check
// results for 5 minutes. A background goroutine runs every 10 minutes to purge
// expired entries, preventing memory growth.
//
// Orphaned bundle detection scans for _mv_* bundles without corresponding views
// on startup, logging warnings for manual DBA cleanup.
package view

import (
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// CachedPermission represents a cached permission check result
type CachedPermission struct {
	Username  string    // User who was checked
	ViewName  string    // View that was accessed
	Timestamp time.Time // When permission was checked
	HasAccess bool      // Whether user has access
}

// ViewRegistry maintains an in-memory cache of views for fast access
type ViewRegistry struct {
	// views maps database name -> view name -> View
	views map[string]map[string]*View

	// permissionCache stores permission check results
	// Key format: "username:database:viewname"
	permissionCache map[string]*CachedPermission

	// mu protects concurrent access to the registry
	mu sync.RWMutex

	// logger for recording events
	logger *zap.SugaredLogger
}

// NewViewRegistry creates a new ViewRegistry instance
func NewViewRegistry(logger *zap.SugaredLogger) *ViewRegistry {
	registry := &ViewRegistry{
		views:           make(map[string]map[string]*View),
		permissionCache: make(map[string]*CachedPermission),
		logger:          logger,
	}

	// Start background cleanup routine
	go registry.startCleanupRoutine()

	return registry
}

// IsView checks if a name refers to a view in the given database (case-sensitive)
func (r *ViewRegistry) IsView(databaseName, name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dbViews, exists := r.views[databaseName]
	if !exists {
		return false
	}

	_, exists = dbViews[name]
	return exists
}

// GetView retrieves a view from the cache (case-sensitive)
// Returns nil if view not found
func (r *ViewRegistry) GetView(databaseName, viewName string) *View {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dbViews, exists := r.views[databaseName]
	if !exists {
		return nil
	}

	return dbViews[viewName]
}

// RegisterView adds a view to the cache
func (r *ViewRegistry) RegisterView(view *View) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Ensure database map exists
	if r.views[view.DatabaseName] == nil {
		r.views[view.DatabaseName] = make(map[string]*View)
	}

	// Add view to cache
	r.views[view.DatabaseName][view.ViewName] = view

	r.logger.Debugf("Registered view '%s' in database '%s'", view.ViewName, view.DatabaseName)
}

// UnregisterView removes a view from the cache
func (r *ViewRegistry) UnregisterView(databaseName, viewName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	dbViews, exists := r.views[databaseName]
	if !exists {
		return
	}

	delete(dbViews, viewName)

	// Clean up database map if empty
	if len(dbViews) == 0 {
		delete(r.views, databaseName)
	}

	r.logger.Debugf("Unregistered view '%s' from database '%s'", viewName, databaseName)
}

// ListViews returns all views for a database
func (r *ViewRegistry) ListViews(databaseName string) []*View {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dbViews, exists := r.views[databaseName]
	if !exists {
		return []*View{}
	}

	// Collect views into slice
	views := make([]*View, 0, len(dbViews))
	for _, view := range dbViews {
		views = append(views, view)
	}

	return views
}

// CheckPermission checks if a cached permission exists and is still valid
// Returns (hasAccess, isCached)
func (r *ViewRegistry) CheckPermission(username, databaseName, viewName string) (bool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := r.permissionKey(username, databaseName, viewName)
	cached, exists := r.permissionCache[key]
	if !exists {
		return false, false
	}

	// Check if cache entry is still valid (5 minutes)
	if time.Since(cached.Timestamp) > PermissionCacheDuration {
		return false, false
	}

	return cached.HasAccess, true
}

// CachePermission stores a permission check result
func (r *ViewRegistry) CachePermission(username, databaseName, viewName string, hasAccess bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.permissionKey(username, databaseName, viewName)
	r.permissionCache[key] = &CachedPermission{
		Username:  username,
		ViewName:  viewName,
		Timestamp: time.Now(),
		HasAccess: hasAccess,
	}
}

// InvalidatePermissionCache removes all cached permissions for a view
// Called when view is modified or dropped
func (r *ViewRegistry) InvalidatePermissionCache(databaseName, viewName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove all cache entries for this view
	for key := range r.permissionCache {
		if strings.HasSuffix(key, ":"+databaseName+":"+viewName) {
			delete(r.permissionCache, key)
		}
	}

	r.logger.Debugf("Invalidated permission cache for view '%s.%s'", databaseName, viewName)
}

// permissionKey generates a cache key for permission checks
func (r *ViewRegistry) permissionKey(username, databaseName, viewName string) string {
	return username + ":" + databaseName + ":" + viewName
}

// startCleanupRoutine runs a background goroutine to clean up expired cache entries
func (r *ViewRegistry) startCleanupRoutine() {
	ticker := time.NewTicker(PermissionCacheCleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		r.cleanupExpiredPermissions()
	}
}

// cleanupExpiredPermissions removes expired permission cache entries
func (r *ViewRegistry) cleanupExpiredPermissions() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	expiredCount := 0

	for key, cached := range r.permissionCache {
		if now.Sub(cached.Timestamp) > PermissionCacheDuration {
			delete(r.permissionCache, key)
			expiredCount++
		}
	}

	if expiredCount > 0 {
		r.logger.Debugf("Cleaned up %d expired permission cache entries", expiredCount)
	}
}

// CheckCaseConflict checks if a view with the same name but different casing exists
// Returns (conflicting name, exists)
func (r *ViewRegistry) CheckCaseConflict(databaseName, viewName string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dbViews, exists := r.views[databaseName]
	if !exists {
		return "", false
	}

	// Check for case-insensitive match with different exact name
	viewNameLower := strings.ToLower(viewName)
	for existingName := range dbViews {
		if strings.ToLower(existingName) == viewNameLower && existingName != viewName {
			return existingName, true
		}
	}

	return "", false
}

// TODO: I should implement orphaned hidden bundle detection on server startup
// DetectOrphanedBundles scans for _mv_* bundles without corresponding views
// This should be called on server startup to log warnings for manual cleanup
func (r *ViewRegistry) DetectOrphanedBundles(bundleNames []string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, bundleName := range bundleNames {
		if strings.HasPrefix(bundleName, "_mv_") {
			// Extract view name from bundle name
			viewName := strings.TrimPrefix(bundleName, "_mv_")

			// Check all databases for this view
			found := false
			for _, dbViews := range r.views {
				if view, exists := dbViews[viewName]; exists {
					if view.Type == ViewTypeMaterialized && view.DataBundleName == bundleName {
						found = true
						break
					}
				}
			}

			if !found {
				r.logger.Warnf("Found orphaned hidden bundle '%s' with no corresponding view. Manual cleanup recommended. TODO: I should implement orphaned hidden bundle detection and cleanup on server startup.", bundleName)
			}
		}
	}
}
