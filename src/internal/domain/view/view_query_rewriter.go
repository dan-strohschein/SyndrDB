// Package view provides the domain model and business logic for database views in SyndrDB.
// This file contains the query rewriting logic for views.
//
// The query rewriter transforms queries against views into equivalent queries against
// the underlying bundles. This implements PostgreSQL-style "query rewrite" for regular views.
//
// Query rewriting process:
// 1. Parse incoming query to identify view references
// 2. Validate user has permissions on all referenced bundles
// 3. Replace view references with view definition (subquery)
// 4. Preserve original query structure (WHERE, ORDER BY, LIMIT, etc.)
// 5. Cache rewritten query in query plan cache
//
// For example:
//
//	Original: SELECT * FROM active_users WHERE age > 25
//	View def: SELECT id, name, email FROM users WHERE active = true
//	Rewritten: SELECT * FROM (SELECT id, name, email FROM users WHERE active = true) AS active_users WHERE age > 25
//
// Permission validation:
// - Check cached permissions (5-minute TTL)
// - If not cached, query all referenced bundles from view definition
// - Verify user has SELECT permission on each bundle
// - Cache result for 5 minutes
// - Return error if any permission check fails
//
// Query plan caching:
// - Cache rewritten queries in UnifiedQueryPlanner cache
// - Invalidate cache on view definition changes
// - Invalidate cache on permission changes
// - Invalidate cache on bundle schema changes
//
// Main functions:
// - RewriteQuery: Rewrite query against view to query against bundles
// - ValidatePermissions: Check user has access to all referenced bundles
// - ExtractViewReferences: Find all view names in query
// - InvalidateCache: Remove cached plans for a view
//
// TODO: I should implement query rewriting for nested views (views referencing other views)
// TODO: I should optimize rewritten queries to push down predicates into view definition
package view

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// ViewQueryRewriter handles query rewriting for views
type ViewQueryRewriter struct {
	logger   *zap.SugaredLogger
	registry *ViewRegistry
	// TODO: Add query plan cache reference
	// TODO: Add permission service reference
}

// NewViewQueryRewriter creates a new ViewQueryRewriter
func NewViewQueryRewriter(logger *zap.SugaredLogger, registry *ViewRegistry) *ViewQueryRewriter {
	return &ViewQueryRewriter{
		logger:   logger,
		registry: registry,
	}
}

// RewriteQuery rewrites a query against a view into a query against underlying bundles
// Returns the rewritten query string and any errors
func (r *ViewQueryRewriter) RewriteQuery(originalQuery, username, databaseName string) (string, error) {
	// Extract view references from query
	viewRefs := r.ExtractViewReferences(originalQuery, databaseName)
	if len(viewRefs) == 0 {
		// No views referenced, return original query
		return originalQuery, nil
	}

	// Validate permissions for all referenced views
	for _, viewName := range viewRefs {
		if err := r.ValidatePermissions(username, databaseName, viewName); err != nil {
			return "", err
		}
	}

	// Rewrite query
	rewrittenQuery := originalQuery
	for _, viewName := range viewRefs {
		view := r.registry.GetView(databaseName, viewName)
		if view == nil {
			return "", fmt.Errorf("View '%s' not found in database '%s'", viewName, databaseName)
		}

		// Replace view reference with subquery
		// TODO: This is a simple string replacement - I should use proper AST rewriting
		viewToken := viewName
		subquery := fmt.Sprintf("(%s) AS %s", view.Definition, viewName)
		rewrittenQuery = strings.ReplaceAll(rewrittenQuery, viewToken, subquery)
	}

	r.logger.Debugf("Rewritten query for views %v: %s", viewRefs, rewrittenQuery)

	// TODO: Cache rewritten query in query plan cache

	return rewrittenQuery, nil
}

// ValidatePermissions checks if user has SELECT permission on all bundles referenced by a view
// Uses cached permissions with 5-minute TTL, queries bundle permissions if not cached
func (r *ViewQueryRewriter) ValidatePermissions(username, databaseName, viewName string) error {
	// Check cached permission
	hasAccess, isCached := r.registry.CheckPermission(username, databaseName, viewName)
	if isCached {
		if !hasAccess {
			return fmt.Errorf("User '%s' does not have SELECT permission on view '%s' or its underlying bundles", username, viewName)
		}
		return nil
	}

	// Get view
	view := r.registry.GetView(databaseName, viewName)
	if view == nil {
		return fmt.Errorf("View '%s' not found in database '%s'", viewName, databaseName)
	}

	// Check permissions on all referenced bundles
	// TODO: Query permission service for each bundle
	// For now, assume we have a method like permissionService.CheckBundlePermission(username, databaseName, bundleName)
	for _, bundleName := range view.ReferencedBundles {
		// Placeholder - actual implementation will query permission service
		_ = bundleName
		// if !permissionService.CheckBundlePermission(username, databaseName, bundleName) {
		//     r.registry.CachePermission(username, databaseName, viewName, false)
		//     return fmt.Errorf("User '%s' does not have SELECT permission on bundle '%s' referenced by view '%s'", username, bundleName, viewName)
		// }
	}

	// All permissions granted
	r.registry.CachePermission(username, databaseName, viewName, true)
	return nil
}

// ExtractViewReferences finds all view references in a query
// Returns a slice of view names found in the query
func (r *ViewQueryRewriter) ExtractViewReferences(query, databaseName string) []string {
	// TODO: This is a simple implementation - I should use proper query parsing
	// For now, scan for known view names
	viewRefs := []string{}
	allViews := r.registry.ListViews(databaseName)

	for _, view := range allViews {
		// Simple case-sensitive search for view name
		if strings.Contains(query, view.ViewName) {
			viewRefs = append(viewRefs, view.ViewName)
		}
	}

	return viewRefs
}

// InvalidateCache removes cached query plans for a view
// Called when view is modified or dropped
func (r *ViewQueryRewriter) InvalidateCache(databaseName, viewName string) {
	r.logger.Debugf("Invalidating query plan cache for view '%s'", viewName)
	// TODO: Call query plan cache to invalidate all plans referencing this view
}
