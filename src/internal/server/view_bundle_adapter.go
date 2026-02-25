package server

import (
	"context"
	"fmt"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/pkg/common/helpers"
	"time"

	"go.uber.org/zap"
)

// ViewBundleAdapter bridges the view.BundleOpsInterface to real BundleService and planner.
// It lives in the server package to avoid import cycles between view and bundle packages.
type ViewBundleAdapter struct {
	bundleService   *bundle.BundleService
	databaseService *database.DatabaseService
	unifiedPlanner  *planner.UnifiedQueryPlanner
	logger          *zap.SugaredLogger
}

// NewViewBundleAdapter creates a new adapter
func NewViewBundleAdapter(
	bundleService *bundle.BundleService,
	databaseService *database.DatabaseService,
	unifiedPlanner *planner.UnifiedQueryPlanner,
	logger *zap.SugaredLogger,
) *ViewBundleAdapter {
	return &ViewBundleAdapter{
		bundleService:   bundleService,
		databaseService: databaseService,
		unifiedPlanner:  unifiedPlanner,
		logger:          logger,
	}
}

// BundleExists checks if a bundle exists in the given database
func (a *ViewBundleAdapter) BundleExists(dbName, bundleName string) bool {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return false
	}
	_, exists := db.Bundles[bundleName]
	return exists
}

// CreateInternalBundle creates a hidden _mv_ bundle for materialized views
func (a *ViewBundleAdapter) CreateInternalBundle(dbName, bundleName string, fields interface{}) error {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return fmt.Errorf("database '%s' not found: %w", dbName, err)
	}

	var fieldDefs []models.FieldDefinition
	if fields != nil {
		if defs, ok := fields.([]models.FieldDefinition); ok {
			fieldDefs = defs
		}
	}

	_, err = a.bundleService.AddInternalBundle(a.databaseService, db, bundleName, fieldDefs)
	return err
}

// DeleteInternalBundle deletes a hidden _mv_ bundle
func (a *ViewBundleAdapter) DeleteInternalBundle(dbName, bundleName string) error {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return fmt.Errorf("database '%s' not found: %w", dbName, err)
	}

	bundleCmd := &models.BundleCommand{
		BundleName:     bundleName,
		HasForceSwitch: true, // Skip referential integrity for internal bundles
	}

	return a.bundleService.DeleteBundle(db, bundleCmd)
}

// ExecuteViewQuery parses and executes a view definition SELECT, returning results as map
func (a *ViewBundleAdapter) ExecuteViewQuery(definition, dbName string) (map[string]interface{}, error) {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found: %w", dbName, err)
	}

	// Use the new V3 parser (same as normal SELECT path) to properly parse
	// WHERE/GROUP BY/aggregate expressions as syndrQL.Expression types
	query, err := ParseQuery(definition, a.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse view definition: %w", err)
	}

	plan, err := a.unifiedPlanner.CreatePlan(query, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create plan for view definition: %w", err)
	}

	ctx := context.Background()
	result, err := plan.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute view definition: %w", err)
	}

	// Convert result to map[string]interface{} - the planner returns map[string]*Document
	if docMap, ok := result.(map[string]*models.Document); ok {
		resultMap := make(map[string]interface{})
		docs := make(map[string]interface{}, len(docMap))
		for id, doc := range docMap {
			docs[id] = doc
		}
		resultMap["Documents"] = docs
		return resultMap, nil
	}

	return nil, nil
}

// InsertDocumentsIntoBundle inserts documents from ExecuteViewQuery results into a bundle
func (a *ViewBundleAdapter) InsertDocumentsIntoBundle(dbName, bundleName string, docs map[string]interface{}) error {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return fmt.Errorf("database '%s' not found: %w", dbName, err)
	}

	bndl, err := a.bundleService.GetBundleByName(db, bundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found: %w", bundleName, err)
	}

	// Extract the Documents sub-map if present
	docsToInsert := docs
	if innerDocs, ok := docs["Documents"]; ok {
		if innerMap, ok := innerDocs.(map[string]interface{}); ok {
			docsToInsert = innerMap
		}
	}

	for _, docVal := range docsToInsert {
		switch doc := docVal.(type) {
		case *models.Document:
			// Create a copy with a new ID for the MV bundle
			mvDoc := *doc
			mvDoc.DocumentID = helpers.GenerateFastUUID()
			mvDoc.CreatedAt = time.Now()
			mvDoc.UpdatedAt = time.Now()
			mvDoc.CommitSequence = 0
			mvDoc.VersionSequence = 1
			if err := a.bundleService.AddDocumentToBundleByStruct(db, bndl, &mvDoc); err != nil {
				a.logger.Warnf("Failed to insert document into MV bundle '%s': %v", bundleName, err)
			}
		default:
			a.logger.Debugf("Skipping non-document value in MV population")
		}
	}

	return nil
}
