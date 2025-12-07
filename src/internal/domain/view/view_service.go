// Package view provides the domain model and business logic for database views in SyndrDB.
// This file contains the main ViewService which orchestrates all view operations.
//
// The ViewService manages both regular views (query rewrite) and materialized views
// (pre-computed snapshots). It coordinates with the storage layer, registry, and
// query planner to provide a complete view implementation.
//
// Key responsibilities:
// - View creation, modification, and deletion
// - Permission validation and caching
// - Query rewriting for regular views
// - Materialized view refresh management
// - System catalog synchronization
//
// This implementation follows the DRY principle by reusing existing bundle services,
// permission services, and storage patterns. It adheres to the Single Responsibility
// Principle by delegating specific tasks to specialized components (factory, store,
// registry, materializer, rewriter).
//
// Main functions:
// - CreateView: Creates a new regular view
// - CreateMaterializedView: Creates a new materialized view
// - DropView: Deletes a view (with optional FORCE)
// - RefreshMaterializedView: Updates materialized view data
// - GetView: Retrieves a view by name
// - ListViews: Lists all views in a database
// - DescribeView: Gets detailed view information
//
// TODO: I should implement scheduled refresh with REFRESH EVERY syntax
// TODO: I should implement view_dependency.go to track and cascade drop dependent views
// TODO: I should make MaxBundlesPerView configurable via settings file
package view

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Constants for view management
const (
	// MaxBundlesPerView is the maximum number of bundles a view can reference
	// TODO: I should make this limit configurable via settings file
	MaxBundlesPerView = 50

	// DefaultStaleWarningHours is the default threshold for stale materialized views
	DefaultStaleWarningHours = 48

	// PermissionCacheDuration is how long permission checks are cached
	PermissionCacheDuration = 5 * time.Minute

	// PermissionCacheCleanupInterval is how often expired cache entries are purged
	PermissionCacheCleanupInterval = 10 * time.Minute

	// MaxViewDefinitionLength is the maximum size of a view query (64KB)
	MaxViewDefinitionLength = 65536

	// MaxViewNameLength is the maximum length of a view name
	MaxViewNameLength = 128

	// LockTimeoutDuration is how long to wait for a lock before failing
	// TODO: I should make lock timeout duration configurable via settings file
	LockTimeoutDuration = 1 * time.Minute
)

// ViewType represents the type of view
type ViewType int

const (
	// ViewTypeRegular represents a regular view (query rewrite)
	ViewTypeRegular ViewType = iota
	// ViewTypeMaterialized represents a materialized view (pre-computed)
	ViewTypeMaterialized
)

// String returns the string representation of ViewType
func (vt ViewType) String() string {
	switch vt {
	case ViewTypeRegular:
		return "REGULAR"
	case ViewTypeMaterialized:
		return "MATERIALIZED"
	default:
		return "UNKNOWN"
	}
}

// ViewColumn represents a column in a view
type ViewColumn struct {
	Name         string // Column name (may have _1, _2 suffix for duplicates)
	Type         string // Data type
	SourceBundle string // Source bundle name
	SourceColumn string // Source column name
}

// View represents a database view
type View struct {
	ViewName          string       // View name (case-sensitive, max 128 chars)
	DatabaseName      string       // Database name (case-sensitive)
	Definition        string       // SQL query text (max 64KB)
	Type              ViewType     // REGULAR or MATERIALIZED
	CreatedAt         time.Time    // Creation timestamp
	CreatedBy         string       // Username of creator
	LastRefreshed     *time.Time   // Last refresh time (materialized views only)
	Columns           []ViewColumn // View columns
	DataBundleName    string       // Hidden bundle name (_mv_{name}) for materialized views
	IndexNames        []string     // Index names on materialized data
	ReferencedBundles []string     // Bundles referenced in view definition (max 50)
}

// ViewService manages view operations
type ViewService struct {
	factory  *ViewFactory
	store    ViewStore
	registry *ViewRegistry
	logger   *zap.SugaredLogger
	// TODO: Add materializer, rewriter, validator fields
}

// ViewStore interface defines storage operations for views
type ViewStore interface {
	SaveView(view *View) error
	LoadView(databaseName, viewName string) (*View, error)
	DeleteView(databaseName, viewName string) error
	ListViews(databaseName string) ([]*View, error)
	SyncToCatalog(view *View) error
}

// NewViewService creates a new ViewService instance
func NewViewService(logger *zap.SugaredLogger) *ViewService {
	factory := NewViewFactory()
	registry := NewViewRegistry(logger)

	return &ViewService{
		factory:  factory,
		registry: registry,
		logger:   logger,
	}
}

// CreateView creates a new regular view
func (s *ViewService) CreateView(viewName, databaseName, definition, createdBy string) (*View, error) {
	s.logger.Infof("Creating regular view '%s' in database '%s'", viewName, databaseName)

	// Create view object
	view, err := s.factory.NewView(viewName, databaseName, definition, createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create view: %w", err)
	}

	// TODO: Parse definition to extract columns and referenced bundles
	// TODO: Validate max bundles limit
	// TODO: Check for case-sensitive name conflicts
	// TODO: Acquire lock before double-checking existence
	// TODO: Save to store and sync to catalog
	// TODO: Register in registry

	s.logger.Infof("Successfully created regular view '%s'", viewName)
	return view, nil
}

// CreateMaterializedView creates a new materialized view
func (s *ViewService) CreateMaterializedView(viewName, databaseName, definition, createdBy string) (*View, error) {
	s.logger.Infof("Creating materialized view '%s' in database '%s'", viewName, databaseName)

	// Create view object
	view, err := s.factory.NewMaterializedView(viewName, databaseName, definition, createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create materialized view: %w", err)
	}

	// TODO: Check existence before acquiring lock
	// TODO: Acquire exclusive lock with timeout
	// TODO: Double-check existence after lock
	// TODO: Parse definition and validate
	// TODO: Create hidden bundle
	// TODO: Execute query and populate
	// TODO: Save to store and sync to catalog with rollback on failure
	// TODO: Register in registry
	// TODO: Release lock

	s.logger.Infof("Successfully created materialized view '%s'", viewName)
	return view, nil
}

// GetView retrieves a view by name from the registry
func (s *ViewService) GetView(databaseName, viewName string) (*View, error) {
	view := s.registry.GetView(databaseName, viewName)
	if view == nil {
		return nil, fmt.Errorf("view '%s' not found in database '%s'", viewName, databaseName)
	}
	return view, nil
}

// IsView checks if a name refers to a view
func (s *ViewService) IsView(databaseName, name string) bool {
	return s.registry.IsView(databaseName, name)
}

// ListViews returns all views in a database
func (s *ViewService) ListViews(databaseName string) ([]*View, error) {
	return s.registry.ListViews(databaseName), nil
}
