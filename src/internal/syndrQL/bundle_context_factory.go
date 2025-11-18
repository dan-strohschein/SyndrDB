package syndrQL

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
)

/*
bundle_context_factory.go

This file provides factory functions for creating BundleContext instances from queries.
It handles loading all required bundles (primary + joined) and constructing the context
with proper field resolver and caching.

Key responsibilities:
- Loading bundles referenced in queries
- Creating BundleContext with all required bundles
- Handling single-bundle and multi-bundle (JOIN) scenarios
- Error handling for missing bundles

This factory is used throughout the query execution pipeline to provide bundle-aware
field resolution and qualified field name validation.
*/

// BundleServiceInterface defines the methods needed to load bundles
// This interface allows the factory to work with different bundle service implementations
type BundleServiceInterface interface {
	GetBundleByName(database *models.Database, bundleName string) (*models.Bundle, error)
}

// NewBundleContextFromQuery creates a BundleContext from a UnifiedSelectQuery
// It loads all bundles referenced in the query (FROM + JOINs) and constructs
// a BundleContext with field resolver and field map caching
//
// Parameters:
//   - fromBundle: Name of the primary bundle (FROM clause)
//   - joinBundles: Names of joined bundles (from JOIN clauses)
//   - database: Database containing the bundles
//   - bundleService: Service to load bundle metadata
//
// Returns:
//   - *BundleContext: Context with all bundles, field resolver, and caching
//   - error: If any bundle cannot be loaded
func NewBundleContextFromQuery(
	fromBundle string,
	joinBundles []string,
	database *models.Database,
	bundleService BundleServiceInterface,
) (*BundleContext, error) {
	bundles := make(map[string]*models.Bundle)

	// Load primary bundle
	primaryBundle, err := bundleService.GetBundleByName(database, fromBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to load primary bundle '%s': %w", fromBundle, err)
	}
	bundles[fromBundle] = primaryBundle

	// Load joined bundles
	for _, joinBundleName := range joinBundles {
		joinBundle, err := bundleService.GetBundleByName(database, joinBundleName)
		if err != nil {
			return nil, fmt.Errorf("failed to load join bundle '%s': %w", joinBundleName, err)
		}
		bundles[joinBundleName] = joinBundle
	}

	// Create BundleContext with field resolver and caching
	return NewBundleContext(bundles, fromBundle, joinBundles), nil
}

// NewBundleContextForSingleBundle creates a BundleContext for a single-bundle query
// This is a convenience method for simple SELECT queries without JOINs
//
// Parameters:
//   - bundle: The bundle to create context for
//
// Returns:
//   - *BundleContext: Context with single bundle
func NewBundleContextForSingleBundle(bundle *models.Bundle) *BundleContext {
	bundles := map[string]*models.Bundle{
		bundle.Name: bundle,
	}
	return NewBundleContext(bundles, bundle.Name, []string{})
}
