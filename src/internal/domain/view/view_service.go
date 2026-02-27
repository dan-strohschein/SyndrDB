package view

import (
	"fmt"
	"regexp"
	"time"

	"go.uber.org/zap"
)

// Constants for view management
const (
	MaxBundlesPerView             = 50
	DefaultStaleWarningHours      = 48
	PermissionCacheDuration       = 5 * time.Minute
	PermissionCacheCleanupInterval = 10 * time.Minute
	MaxViewDefinitionLength       = 65536
	MaxViewNameLength             = 128
	LockTimeoutDuration           = 1 * time.Minute
)

// ViewType represents the type of view
type ViewType int

const (
	ViewTypeRegular      ViewType = iota
	ViewTypeMaterialized
)

// String returns the string representation of ViewType
func (vt ViewType) String() string {
	switch vt {
	case ViewTypeRegular:
		return "VIEW"
	case ViewTypeMaterialized:
		return "MATERIALIZED_VIEW"
	default:
		return "UNKNOWN"
	}
}

// ViewColumn represents a column in a view
type ViewColumn struct {
	Name         string
	Type         string
	SourceBundle string
	SourceColumn string
}

// View represents a database view
type View struct {
	ViewName          string       `json:"view_name"`
	DatabaseName      string       `json:"database_name"`
	Definition        string       `json:"definition"`
	Type              ViewType     `json:"type"`
	CreatedAt         time.Time    `json:"created_at"`
	CreatedBy         string       `json:"created_by"`
	LastRefreshed     *time.Time   `json:"last_refreshed,omitempty"`
	Columns           []ViewColumn `json:"columns"`
	DataBundleName    string       `json:"data_bundle_name,omitempty"`
	IndexNames        []string     `json:"index_names,omitempty"`
	ReferencedBundles []string     `json:"referenced_bundles"`
}

// BundleOpsInterface defines operations the view system needs from the bundle/planner layer.
// An adapter in the server package implements this to avoid import cycles.
type BundleOpsInterface interface {
	CreateInternalBundle(dbName, bundleName string, fields interface{}) error
	DeleteInternalBundle(dbName, bundleName string) error
	ExecuteViewQuery(definition, dbName string) (map[string]interface{}, error)
	InsertDocumentsIntoBundle(dbName, bundleName string, docs map[string]interface{}) error
	BundleExists(dbName, bundleName string) bool
}

// ViewStore interface defines storage operations for views
type ViewStore interface {
	SaveView(view *View) error
	LoadView(databaseName, viewName string) (*View, error)
	DeleteView(databaseName, viewName string) error
	ListViews(databaseName string) ([]*View, error)
	SyncToCatalog(view *View) error
}

// ViewService manages view operations
type ViewService struct {
	factory   *ViewFactory
	store     ViewStore
	registry  *ViewRegistry
	validator *ViewValidator
	bundleOps BundleOpsInterface
	logger    *zap.SugaredLogger
}

// NewViewService creates a new ViewService with its own registry
func NewViewService(logger *zap.SugaredLogger) *ViewService {
	factory := NewViewFactory()
	registry := NewViewRegistry(logger)
	store := NewViewFileStore(logger)
	validator := NewViewValidator(logger)

	return &ViewService{
		factory:   factory,
		store:     store,
		registry:  registry,
		validator: validator,
		logger:    logger,
	}
}

// NewViewServiceWithRegistry creates a ViewService that shares an external registry
func NewViewServiceWithRegistry(logger *zap.SugaredLogger, registry *ViewRegistry, store ViewStore) *ViewService {
	factory := NewViewFactory()
	validator := NewViewValidator(logger)

	return &ViewService{
		factory:   factory,
		store:     store,
		registry:  registry,
		validator: validator,
		logger:    logger,
	}
}

// SetBundleOps wires in the bundle operations adapter for materialized view support
func (s *ViewService) SetBundleOps(ops BundleOpsInterface) {
	s.bundleOps = ops
}

// GetRegistry returns the view registry (for external access such as planner sharing)
func (s *ViewService) GetRegistry() *ViewRegistry {
	return s.registry
}

// regex to extract bundle names from FROM and JOIN clauses
var bundleRefPattern = regexp.MustCompile(`(?i)(?:FROM|JOIN)\s+"([^"]+)"`)

// extractReferencedBundles parses bundle names from a view definition SQL string
func extractReferencedBundles(definition string) []string {
	matches := bundleRefPattern.FindAllStringSubmatch(definition, -1)
	seen := make(map[string]struct{}, len(matches))
	bundles := make([]string, 0, len(matches))
	for _, m := range matches {
		name := m[1]
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			bundles = append(bundles, name)
		}
	}
	return bundles
}

// CreateView creates a new regular view
func (s *ViewService) CreateView(viewName, databaseName, definition, createdBy string) (*View, error) {
	s.logger.Infof("Creating regular view '%s' in database '%s'", viewName, databaseName)

	// Check duplicate
	if s.registry.IsView(databaseName, viewName) {
		return nil, fmt.Errorf("view '%s' already exists in database '%s'", viewName, databaseName)
	}

	// Check case conflict
	if conflicting, exists := s.registry.CheckCaseConflict(databaseName, viewName); exists {
		return nil, fmt.Errorf("view name '%s' conflicts with existing view '%s' (case-insensitive match)", viewName, conflicting)
	}

	// Validate definition for regular views (reject aggregates/DISTINCT)
	if err := s.validator.ValidateViewDefinition(definition, ViewTypeRegular); err != nil {
		return nil, err
	}

	// Extract referenced bundles
	refBundles := extractReferencedBundles(definition)
	if len(refBundles) > MaxBundlesPerView {
		return nil, fmt.Errorf("view references %d bundles, maximum is %d", len(refBundles), MaxBundlesPerView)
	}

	// Validate referenced bundles exist
	if s.bundleOps != nil {
		for _, bundleName := range refBundles {
			if !s.bundleOps.BundleExists(databaseName, bundleName) {
				return nil, fmt.Errorf("bundle '%s' does not exist in database '%s'", bundleName, databaseName)
			}
		}
	}

	// Create view object
	view, err := s.factory.NewView(viewName, databaseName, definition, createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create view: %w", err)
	}
	view.ReferencedBundles = refBundles

	// Persist to store
	if err := s.store.SaveView(view); err != nil {
		return nil, fmt.Errorf("failed to save view: %w", err)
	}

	// Register in registry
	s.registry.RegisterView(view)

	s.logger.Infof("Successfully created regular view '%s' referencing bundles %v", viewName, refBundles)
	return view, nil
}

// CreateMaterializedView creates a new materialized view with pre-computed data
func (s *ViewService) CreateMaterializedView(viewName, databaseName, definition, createdBy string) (*View, error) {
	s.logger.Infof("Creating materialized view '%s' in database '%s'", viewName, databaseName)

	// Check duplicate
	if s.registry.IsView(databaseName, viewName) {
		return nil, fmt.Errorf("view '%s' already exists in database '%s'", viewName, databaseName)
	}

	// Check case conflict
	if conflicting, exists := s.registry.CheckCaseConflict(databaseName, viewName); exists {
		return nil, fmt.Errorf("view name '%s' conflicts with existing view '%s' (case-insensitive match)", viewName, conflicting)
	}

	// Extract referenced bundles
	refBundles := extractReferencedBundles(definition)
	if len(refBundles) > MaxBundlesPerView {
		return nil, fmt.Errorf("view references %d bundles, maximum is %d", len(refBundles), MaxBundlesPerView)
	}

	// Create view object
	view, err := s.factory.NewMaterializedView(viewName, databaseName, definition, createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create materialized view: %w", err)
	}
	view.ReferencedBundles = refBundles

	// Create hidden _mv_ bundle and populate if bundleOps is wired
	if s.bundleOps != nil {
		// Create the hidden bundle
		if err := s.bundleOps.CreateInternalBundle(databaseName, view.DataBundleName, nil); err != nil {
			return nil, fmt.Errorf("failed to create internal bundle for materialized view: %w", err)
		}

		// Execute the view definition query to get data
		results, err := s.bundleOps.ExecuteViewQuery(definition, databaseName)
		if err != nil {
			// Rollback: delete the created bundle
			_ = s.bundleOps.DeleteInternalBundle(databaseName, view.DataBundleName)
			return nil, fmt.Errorf("failed to execute view definition: %w", err)
		}

		// Insert results into the hidden bundle
		if results != nil {
			if err := s.bundleOps.InsertDocumentsIntoBundle(databaseName, view.DataBundleName, results); err != nil {
				_ = s.bundleOps.DeleteInternalBundle(databaseName, view.DataBundleName)
				return nil, fmt.Errorf("failed to populate materialized view: %w", err)
			}
		}

		now := time.Now()
		view.LastRefreshed = &now
	}

	// Persist to store
	if err := s.store.SaveView(view); err != nil {
		// Rollback hidden bundle if it was created
		if s.bundleOps != nil {
			_ = s.bundleOps.DeleteInternalBundle(databaseName, view.DataBundleName)
		}
		return nil, fmt.Errorf("failed to save materialized view: %w", err)
	}

	// Register in registry
	s.registry.RegisterView(view)

	s.logger.Infof("Successfully created materialized view '%s'", viewName)
	return view, nil
}

// DropView drops a regular or materialized view
func (s *ViewService) DropView(viewName, databaseName string, isMaterialized bool) error {
	s.logger.Infof("Dropping view '%s' from database '%s' (materialized=%v)", viewName, databaseName, isMaterialized)

	// Check view exists
	view := s.registry.GetView(databaseName, viewName)
	if view == nil {
		return fmt.Errorf("view '%s' not found in database '%s'", viewName, databaseName)
	}

	// Verify type matches
	if isMaterialized && view.Type != ViewTypeMaterialized {
		return fmt.Errorf("'%s' is a regular view, not a materialized view. Use DROP VIEW instead of DROP MATERIALIZED VIEW", viewName)
	}
	if !isMaterialized && view.Type == ViewTypeMaterialized {
		return fmt.Errorf("'%s' is a materialized view. Use DROP MATERIALIZED VIEW instead of DROP VIEW", viewName)
	}

	// If materialized, delete the hidden bundle
	if view.Type == ViewTypeMaterialized && s.bundleOps != nil {
		if err := s.bundleOps.DeleteInternalBundle(databaseName, view.DataBundleName); err != nil {
			s.logger.Warnf("Failed to delete internal bundle '%s' for materialized view '%s': %v",
				view.DataBundleName, viewName, err)
		}
	}

	// Delete from store
	if err := s.store.DeleteView(databaseName, viewName); err != nil {
		s.logger.Warnf("Failed to delete view file for '%s': %v", viewName, err)
	}

	// Unregister from registry and invalidate permission cache
	s.registry.UnregisterView(databaseName, viewName)
	s.registry.InvalidatePermissionCache(databaseName, viewName)

	s.logger.Infof("Successfully dropped view '%s'", viewName)
	return nil
}

// RefreshMaterializedView re-executes the view query and repopulates the hidden bundle
func (s *ViewService) RefreshMaterializedView(viewName, databaseName string) (int, error) {
	s.logger.Infof("Refreshing materialized view '%s' in database '%s'", viewName, databaseName)

	view := s.registry.GetView(databaseName, viewName)
	if view == nil {
		return 0, fmt.Errorf("view '%s' not found in database '%s'", viewName, databaseName)
	}
	if view.Type != ViewTypeMaterialized {
		return 0, fmt.Errorf("'%s' is not a materialized view", viewName)
	}

	if s.bundleOps == nil {
		return 0, fmt.Errorf("materialized view operations not available (bundleOps not configured)")
	}

	// Execute view definition query
	results, err := s.bundleOps.ExecuteViewQuery(view.Definition, databaseName)
	if err != nil {
		return 0, fmt.Errorf("failed to execute view definition during refresh: %w", err)
	}

	// Delete and recreate the hidden bundle
	_ = s.bundleOps.DeleteInternalBundle(databaseName, view.DataBundleName)

	if err := s.bundleOps.CreateInternalBundle(databaseName, view.DataBundleName, nil); err != nil {
		return 0, fmt.Errorf("failed to recreate internal bundle during refresh: %w", err)
	}

	rowCount := 0
	if results != nil {
		if err := s.bundleOps.InsertDocumentsIntoBundle(databaseName, view.DataBundleName, results); err != nil {
			return 0, fmt.Errorf("failed to populate materialized view during refresh: %w", err)
		}
		// Count documents in results
		if docs, ok := results["Documents"]; ok {
			if docMap, ok := docs.(map[string]interface{}); ok {
				rowCount = len(docMap)
			}
		}
	}

	// Update LastRefreshed
	now := time.Now()
	view.LastRefreshed = &now

	// Persist updated view
	if err := s.store.SaveView(view); err != nil {
		s.logger.Warnf("Failed to persist refreshed view metadata: %v", err)
	}

	s.logger.Infof("Successfully refreshed materialized view '%s' (%d rows)", viewName, rowCount)
	return rowCount, nil
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

// LoadAllViews loads views from disk for the given databases and registers them in the registry
func (s *ViewService) LoadAllViews(databaseNames []string) {
	for _, dbName := range databaseNames {
		views, err := s.store.ListViews(dbName)
		if err != nil {
			s.logger.Warnf("Failed to load views for database '%s': %v", dbName, err)
			continue
		}
		for _, v := range views {
			s.registry.RegisterView(v)
			s.logger.Debugf("Loaded view '%s' from database '%s'", v.ViewName, dbName)
		}
	}
}
