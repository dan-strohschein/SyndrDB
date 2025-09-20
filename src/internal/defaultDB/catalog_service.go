package defaultdb

import (
	"fmt"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
	"time"

	"go.uber.org/zap"
)

// CatalogService provides operations for maintaining the system catalog
type CatalogService struct {
	databaseService *database.DatabaseService
	bundleService   *bundle.BundleService
	logger          *zap.SugaredLogger
}

// NewCatalogService creates a new catalog service instance
func NewCatalogService(databaseService *database.DatabaseService, bundleService *bundle.BundleService, logger *zap.SugaredLogger) *CatalogService {
	return &CatalogService{
		databaseService: databaseService,
		bundleService:   bundleService,
		logger:          logger,
	}
}

// AddDatabaseToCatalog adds a new database to the primary.Databases bundle to maintain the system catalog
func (cs *CatalogService) AddDatabaseToCatalog(db *models.Database) error {
	// Get the primary database
	primaryDB, err := cs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return fmt.Errorf("failed to get primary database: %w", err)
	}

	// Get the Databases bundle from primary database
	databasesBundle, err := cs.bundleService.GetBundleByName(primaryDB, "Databases")
	if err != nil {
		return fmt.Errorf("failed to get primary.Databases bundle: %w", err)
	}

	// Generate UUID for the document
	docID := helpers.GenerateUUID()

	// Create a document for this database in the Databases catalog
	catalogDoc := models.Document{
		DocumentID: docID,
		Fields: map[string]models.Field{
			"DocumentID": {Name: "DocumentID", Value: docID},
			"DatabaseID": {Name: "DatabaseID", Value: db.DatabaseID},
			"Name":       {Name: "Name", Value: db.Name},
			"FilePath":   {Name: "FilePath", Value: db.DataDirectory},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Add the document to the Databases bundle using the service method
	err = cs.bundleService.AddDocumentToBundleByStruct(primaryDB, databasesBundle, &catalogDoc)
	if err != nil {
		return fmt.Errorf("failed to add database to catalog: %w", err)
	}

	cs.logger.Infof("Added database '%s' (ID: %s) to system catalog", db.Name, db.DatabaseID)
	return nil
}

// RemoveDatabaseFromCatalog removes a database document from the primary.Databases bundle by DatabaseID
func (cs *CatalogService) RemoveDatabaseFromCatalog(databaseID string) error {
	// Get the primary database
	primaryDB, err := cs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return fmt.Errorf("failed to get primary database: %w", err)
	}

	// Get the Databases bundle from primary database
	databasesBundle, err := cs.bundleService.GetBundleByName(primaryDB, "Databases")
	if err != nil {
		return fmt.Errorf("failed to get primary.Databases bundle: %w", err)
	}

	// Find the document for this database
	var docIDToRemove string
	var databaseName string
	if databasesBundle.Documents != nil {
		for docID, doc := range *databasesBundle.Documents {
			// Access the DatabaseID field from the Document struct
			if dbIDField, exists := doc.Fields["DatabaseID"]; exists && dbIDField.Value == databaseID {
				docIDToRemove = docID
				if nameField, exists := doc.Fields["Name"]; exists {
					if name, ok := nameField.Value.(string); ok {
						databaseName = name
					}
				}
				break
			}
		}
	}

	if docIDToRemove != "" {
		// Create a delete command for the document using DocumentID as the where clause
		deleteCommand := &models.DocumentDeleteCommand{
			BundleName:  "Databases",
			WhereClause: fmt.Sprintf("DocumentID='%s'", docIDToRemove),
		}

		// Delete the document from the bundle
		err = cs.bundleService.DeleteDocumentFromBundle(databasesBundle, deleteCommand)
		if err != nil {
			return fmt.Errorf("failed to delete database from catalog: %w", err)
		}

		cs.logger.Infof("Removed database '%s' (ID: %s) from system catalog", databaseName, databaseID)
	} else {
		cs.logger.Warnf("Database with ID '%s' not found in system catalog", databaseID)
	}

	return nil
}

// AddBundleToCatalog adds a new bundle to the primary.Bundles bundle to maintain the system catalog
func (cs *CatalogService) AddBundleToCatalog(bundle *models.Bundle) error {
	// Get the primary database
	primaryDB, err := cs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return fmt.Errorf("failed to get primary database: %w", err)
	}

	// Get the Bundles bundle from primary database
	bundlesBundle, err := cs.bundleService.GetBundleByName(primaryDB, "Bundles")
	if err != nil {
		return fmt.Errorf("failed to get primary.Bundles bundle: %w", err)
	}

	// Generate UUID for the document
	docID := helpers.GenerateUUID()

	// Create a document for this bundle in the Bundles catalog
	catalogDoc := models.Document{
		DocumentID: docID,
		Fields: map[string]models.Field{
			"DocumentID":   {Name: "DocumentID", Value: docID},
			"BundleID":     {Name: "BundleID", Value: bundle.BundleID},
			"Name":         {Name: "Name", Value: bundle.Name},
			"DatabaseID":   {Name: "DatabaseID", Value: bundle.Database.DatabaseID},
			"DatabaseName": {Name: "DatabaseName", Value: bundle.Database.Name},
			"FieldCount":   {Name: "FieldCount", Value: len(bundle.DocumentStructure.FieldDefinitions)},
			"FilePath":     {Name: "FilePath", Value: fmt.Sprintf("%s.bnd", bundle.Name)},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Add the document to the Bundles bundle using the service method
	err = cs.bundleService.AddDocumentToBundleByStruct(primaryDB, bundlesBundle, &catalogDoc)
	if err != nil {
		return fmt.Errorf("failed to add bundle to catalog: %w", err)
	}

	cs.logger.Infof("Added bundle '%s' (ID: %s) to system catalog", bundle.Name, bundle.BundleID)
	return nil
}

// GetDatabaseFromCatalog retrieves a database document from the catalog by DatabaseID
func (cs *CatalogService) GetDatabaseFromCatalog(databaseID string) (map[string]interface{}, error) {
	// Get the primary database
	primaryDB, err := cs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary database: %w", err)
	}

	// Get the Databases bundle from primary database
	databasesBundle, err := cs.bundleService.GetBundleByName(primaryDB, "Databases")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary.Databases bundle: %w", err)
	}

	// Find the document for this database
	if databasesBundle.Documents != nil {
		for _, doc := range *databasesBundle.Documents {
			// Access the DatabaseID field from the Document struct
			if dbIDField, exists := doc.Fields["DatabaseID"]; exists && dbIDField.Value == databaseID {
				// Convert document to map for return
				result := make(map[string]interface{})
				result["DocumentID"] = doc.DocumentID
				result["CreatedAt"] = doc.CreatedAt
				result["UpdatedAt"] = doc.UpdatedAt

				// Add all fields to the result
				for fieldName, field := range doc.Fields {
					result[fieldName] = field.Value
				}

				return result, nil
			}
		}
	}

	return nil, fmt.Errorf("database with ID '%s' not found in catalog", databaseID)
}

// ListAllDatabasesInCatalog retrieves all database documents from the catalog
func (cs *CatalogService) ListAllDatabasesInCatalog() ([]map[string]interface{}, error) {
	// Get the primary database
	primaryDB, err := cs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary database: %w", err)
	}

	// Get the Databases bundle from primary database
	databasesBundle, err := cs.bundleService.GetBundleByName(primaryDB, "Databases")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary.Databases bundle: %w", err)
	}

	var databases []map[string]interface{}
	if databasesBundle.Documents != nil {
		for _, doc := range *databasesBundle.Documents {
			// Convert document to map
			docMap := make(map[string]interface{})
			docMap["DocumentID"] = doc.DocumentID
			docMap["CreatedAt"] = doc.CreatedAt
			docMap["UpdatedAt"] = doc.UpdatedAt

			// Add all fields to the result
			for fieldName, field := range doc.Fields {
				docMap[fieldName] = field.Value
			}

			databases = append(databases, docMap)
		}
	}

	return databases, nil
}

// AddDatabaseToCatalogByParams is a convenience method to add a database to catalog with basic parameters
func (cs *CatalogService) AddDatabaseToCatalogByParams(databaseID, name, filePath string) error {
	// Create a temporary Database struct for the catalog operation
	tempDB := &models.Database{
		DatabaseID:    databaseID,
		Name:          name,
		DataDirectory: filePath,
	}

	return cs.AddDatabaseToCatalog(tempDB)
}

// RemoveDatabaseFromCatalogByID is a convenience method to remove a database from catalog by ID
func (cs *CatalogService) RemoveDatabaseFromCatalogByID(databaseID string) error {
	return cs.RemoveDatabaseFromCatalog(databaseID)
}

// GetDatabaseFromCatalog retrieves a database document from the catalog by DatabaseID

// RemoveBundleFromCatalog removes a bundle document from the primary.Bundles bundle by BundleID
func (cs *CatalogService) RemoveBundleFromCatalog(bundleID string) error {
	// Get the primary database
	primaryDB, err := cs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return fmt.Errorf("failed to get primary database: %w", err)
	}

	// Get the Bundles bundle from primary database
	bundlesBundle, err := cs.bundleService.GetBundleByName(primaryDB, "Bundles")
	if err != nil {
		return fmt.Errorf("failed to get primary.Bundles bundle: %w", err)
	}

	// Find the document for this bundle
	var docIDToRemove string
	var bundleName string
	if bundlesBundle.Documents != nil {
		for docID, doc := range *bundlesBundle.Documents {
			// Access the BundleID field from the Document struct
			if bundleIDField, exists := doc.Fields["BundleID"]; exists && bundleIDField.Value == bundleID {
				docIDToRemove = docID
				if nameField, exists := doc.Fields["Name"]; exists {
					if name, ok := nameField.Value.(string); ok {
						bundleName = name
					}
				}
				break
			}
		}
	}

	if docIDToRemove != "" {
		// Create a delete command for the document using DocumentID as the where clause
		deleteCommand := &models.DocumentDeleteCommand{
			BundleName:  "Bundles",
			WhereClause: fmt.Sprintf("DocumentID='%s'", docIDToRemove),
		}

		// Delete the document from the bundle
		err = cs.bundleService.DeleteDocumentFromBundle(bundlesBundle, deleteCommand)
		if err != nil {
			return fmt.Errorf("failed to delete bundle from catalog: %w", err)
		}

		cs.logger.Infof("Removed bundle '%s' (ID: %s) from system catalog", bundleName, bundleID)
	} else {
		cs.logger.Warnf("Bundle with ID '%s' not found in system catalog", bundleID)
	}

	return nil
}

// GetBundleFromCatalog retrieves a bundle document from the catalog by BundleID
func (cs *CatalogService) GetBundleFromCatalog(bundleID string) (map[string]interface{}, error) {
	// Get the primary database
	primaryDB, err := cs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary database: %w", err)
	}

	// Get the Bundles bundle from primary database
	bundlesBundle, err := cs.bundleService.GetBundleByName(primaryDB, "Bundles")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary.Bundles bundle: %w", err)
	}

	// Find the document for this bundle
	if bundlesBundle.Documents != nil {
		for _, doc := range *bundlesBundle.Documents {
			// Access the BundleID field from the Document struct
			if bundleIDField, exists := doc.Fields["BundleID"]; exists && bundleIDField.Value == bundleID {
				// Convert document to map for return
				result := make(map[string]interface{})
				result["DocumentID"] = doc.DocumentID
				result["CreatedAt"] = doc.CreatedAt
				result["UpdatedAt"] = doc.UpdatedAt

				// Add all fields to the result
				for fieldName, field := range doc.Fields {
					result[fieldName] = field.Value
				}

				return result, nil
			}
		}
	}

	return nil, fmt.Errorf("bundle with ID '%s' not found in catalog", bundleID)
}

// ListAllBundlesInCatalog retrieves all bundle documents from the catalog
func (cs *CatalogService) ListAllBundlesInCatalog() ([]map[string]interface{}, error) {
	// Get the primary database
	primaryDB, err := cs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary database: %w", err)
	}

	// Get the Bundles bundle from primary database
	bundlesBundle, err := cs.bundleService.GetBundleByName(primaryDB, "Bundles")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary.Bundles bundle: %w", err)
	}

	var bundles []map[string]interface{}
	if bundlesBundle.Documents != nil {
		for _, doc := range *bundlesBundle.Documents {
			// Convert document to map
			docMap := make(map[string]interface{})
			docMap["DocumentID"] = doc.DocumentID
			docMap["CreatedAt"] = doc.CreatedAt
			docMap["UpdatedAt"] = doc.UpdatedAt

			// Add all fields to the result
			for fieldName, field := range doc.Fields {
				docMap[fieldName] = field.Value
			}

			bundles = append(bundles, docMap)
		}
	}

	return bundles, nil
}

// AddBundleToCatalogByParams is a convenience method to add a bundle to catalog with basic parameters
func (cs *CatalogService) AddBundleToCatalogByParams(bundleID, name, databaseID string) error {
	// Get the database first
	db, err := cs.databaseService.GetDatabaseByID(databaseID)
	if err != nil {
		return fmt.Errorf("failed to get database with ID '%s': %w", databaseID, err)
	}

	// Create a temporary Bundle struct for the catalog operation
	tempBundle := &models.Bundle{
		BundleID: bundleID,
		Name:     name,
		Database: db,
	}

	return cs.AddBundleToCatalog(tempBundle)
}

// RemoveBundleFromCatalogByID is a convenience method to remove a bundle from catalog by ID
func (cs *CatalogService) RemoveBundleFromCatalogByID(bundleID string) error {
	return cs.RemoveBundleFromCatalog(bundleID)
}
