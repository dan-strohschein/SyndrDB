package defaultdb

import (
	"fmt"
	"path/filepath"
	"strings"
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

	// Generate document ID using fast UUID
	docID := helpers.GenerateFastUUID()

	// Create a document for this database in the Databases catalog
	catalogDoc := models.Document{
		DocumentID: docID,
		Fields: map[string]models.Field{
			"DocumentID": {Name: "DocumentID", Value: models.NewStringValue(docID)},
			"DatabaseID": {Name: "DatabaseID", Value: models.NewStringValue(db.DatabaseID)},
			"Name":       {Name: "Name", Value: models.NewStringValue(db.Name)},
			"FilePath":   {Name: "FilePath", Value: models.NewStringValue(db.DataDirectory)},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Add the document to the Databases bundle using the service method
	err = cs.bundleService.AddDocumentToBundleByStruct(primaryDB, databasesBundle, &catalogDoc)
	if err != nil {
		return fmt.Errorf("failed to add database to catalog: %w", err)
	}

	cs.bundleService.FlushAllBuffers()

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

	// TODO Use the LoadCatalogBundleDocuments function to ensure we have the latest documents
	// Find the document for this database
	var docIDToRemove string
	var databaseName string
	if databasesBundle.Documents != nil {
		for docID, doc := range *databasesBundle.Documents {
			// Access the DatabaseID field from the Document struct
			if dbIDField, exists := doc.Fields["DatabaseID"]; exists {
				if dbID, ok := dbIDField.Value.AsString(); ok && dbID == databaseID {
					docIDToRemove = docID
					if nameField, exists := doc.Fields["Name"]; exists {
						if name, ok := nameField.Value.AsString(); ok {
							databaseName = name
						}
					}
					break
				}
			}
		}
	}

	if docIDToRemove != "" {
		// Create a delete command for the document using DocumentID as the where clause
		deleteCommand := &models.DocumentDeleteCommand{
			BundleName:  "Databases",
			WhereClause: fmt.Sprintf("DocumentID='%s'", docIDToRemove),
		}
		//TODO Make this use the same technique as the CommandDirector.DeleteDocument function
		// Delete the document from the bundle
		err = cs.bundleService.DeleteDocumentFromBundle(databasesBundle, deleteCommand, make([]string, 0))
		if err != nil {
			return fmt.Errorf("failed to delete database from catalog: %w", err)
		}

		// TODO We may need to flush the buffers here to ensure data is written

		cs.logger.Infof("Removed database '%s' (ID: %s) from system catalog", databaseName, databaseID)
	} else {
		cs.logger.Warnf("Database with ID '%s' not found in system catalog", databaseID)
	}

	return nil
}

// RegisterBundleInCatalog registers a new bundle to the primary.Bundles bundle to maintain the system catalog
func (cs *CatalogService) RegisterBundleInCatalog(bundle *models.Bundle) error {
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

	// Generate document ID using fast UUID
	docID := helpers.GenerateFastUUID()

	// Create a document for this bundle in the Bundles catalog
	catalogDoc := models.Document{
		DocumentID: docID,
		Fields: map[string]models.Field{
			"DocumentID":   {Name: "DocumentID", Value: models.NewStringValue(docID)},
			"BundleID":     {Name: "BundleID", Value: models.NewStringValue(bundle.BundleID)},
			"Name":         {Name: "Name", Value: models.NewStringValue(bundle.Name)},
			"DatabaseID":   {Name: "DatabaseID", Value: models.NewStringValue(bundle.Database.DatabaseID)},
			"DatabaseName": {Name: "DatabaseName", Value: models.NewStringValue(bundle.Database.Name)}, // This doesn't actually exist on the bundle struct but we can get it from the Database reference
			"FieldCount":   {Name: "FieldCount", Value: models.NewIntValue(int64(len(bundle.DocumentStructure.FieldDefinitions)))},
			"FilePath":     {Name: "FilePath", Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Add the document to the Bundles bundle using the service method
	err = cs.bundleService.AddDocumentToBundleByStruct(primaryDB, bundlesBundle, &catalogDoc)
	if err != nil {
		return fmt.Errorf("failed to add bundle to catalog: %w", err)
	}

	// Flush buffers to ensure data is written
	cs.bundleService.FlushAllBuffers()

	cs.logger.Infof("Added bundle '%s' (ID: %s) to system catalog", bundle.Name, bundle.BundleID)
	return nil
}

// UpdateBundleNameInCatalog updates the Name and FilePath fields in the primary.Bundles
// catalog when a bundle is renamed. This ensures the system catalog remains consistent
// with the actual bundle state on disk.
//
// This method is called by BundleService.RenameBundle() after the bundle directory
// and metadata file have been successfully renamed on disk.
//
// Parameters:
//   - bundleID: The unique identifier of the bundle (doesn't change during rename)
//   - databaseName: The name of the database containing the bundle
//   - oldName: The previous bundle name (for logging/validation)
//   - newName: The new bundle name to update in the catalog
//
// Returns an error if the catalog cannot be loaded or updated.
func (cs *CatalogService) UpdateBundleNameInCatalog(bundleID, databaseName, oldName, newName string) error {
	cs.logger.Infof("Updating catalog entry for bundle rename: '%s' -> '%s' (ID: %s)", oldName, newName, bundleID)

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

	// Load all documents from the catalog using page-based loading
	docs, err := cs.bundleService.LoadCatalogBundleDocuments(bundlesBundle.Name)
	if err != nil {
		return fmt.Errorf("failed to load catalog documents: %w", err)
	}

	// Find the document for this bundle by BundleID
	var targetDoc *models.Document
	for _, doc := range docs {
		if bundleIDField, exists := doc.Fields["BundleID"]; exists {
			if bID, ok := bundleIDField.Value.AsString(); ok && bID == bundleID {
				targetDoc = doc
				break
			}
		}
	}

	if targetDoc == nil {
		return fmt.Errorf("bundle with ID '%s' not found in catalog", bundleID)
	}

	// Verify the old name matches (validation check)
	if nameField, exists := targetDoc.Fields["Name"]; exists {
		if currentName, ok := nameField.Value.AsString(); ok && currentName != oldName {
			cs.logger.Warnf("Catalog name mismatch: expected '%s' but found '%s' (proceeding with update)", oldName, currentName)
		}
	}

	// Update the Name field
	targetDoc.Fields["Name"] = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(newName),
	}

	// Update the FilePath field (format: <databaseName>_<bundleName>.bnd)
	newFilePath := fmt.Sprintf("%s_%s.bnd", databaseName, newName)
	targetDoc.Fields["FilePath"] = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(newFilePath),
	}

	// Update the timestamp
	targetDoc.UpdatedAt = time.Now()

	// Persist the updated document back to the catalog
	// Create an update command for the document
	updateCommand := &models.DocumentUpdateCommand{
		BundleName:  bundlesBundle.Name,
		WhereClause: fmt.Sprintf("\"DocumentID\" == \"%s\"", targetDoc.DocumentID),
		Fields: []models.KeyValue{
			{Key: "Name", Value: newName},
			{Key: "FilePath", Value: newFilePath},
		},
	}

	// Use UpdateDocumentInBundle to persist changes
	if err := cs.bundleService.UpdateDocumentInBundle(primaryDB, bundlesBundle, updateCommand); err != nil {
		return fmt.Errorf("failed to update bundle in catalog: %w", err)
	}

	// Flush buffers to ensure data is written immediately
	cs.bundleService.FlushAllBuffers()

	cs.logger.Infof("Successfully updated catalog entry for bundle '%s' -> '%s'", oldName, newName)
	return nil
}

// UpdateDatabaseNameInCatalog updates the Name and FilePath fields in the primary.Databases
// catalog when a database is renamed. This ensures the system catalog remains consistent
// with the actual database state on disk.
//
// This method is called by DatabaseService.RenameDatabase() after the database directory
// and files have been successfully renamed on disk.
//
// Parameters:
//   - databaseID: The unique identifier of the database (doesn't change during rename)
//   - oldName: The previous database name (for logging/validation)
//   - newName: The new database name to update in the catalog
//
// Returns an error if the catalog cannot be loaded or updated.
func (cs *CatalogService) UpdateDatabaseNameInCatalog(databaseID, oldName, newName string) error {
	cs.logger.Infof("Updating catalog entry for database rename: '%s' -> '%s' (ID: %s)", oldName, newName, databaseID)

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

	// Load all documents from the catalog using page-based loading
	docs, err := cs.bundleService.LoadCatalogBundleDocuments(databasesBundle.Name)
	if err != nil {
		return fmt.Errorf("failed to load catalog documents: %w", err)
	}

	// Find the document for this database by DatabaseID
	var targetDoc *models.Document
	for _, doc := range docs {
		if dbIDField, exists := doc.Fields["DatabaseID"]; exists {
			if dbID, ok := dbIDField.Value.AsString(); ok && dbID == databaseID {
				targetDoc = doc
				break
			}
		}
	}

	if targetDoc == nil {
		return fmt.Errorf("database with ID '%s' not found in catalog", databaseID)
	}

	// Verify the old name matches (validation check)
	if nameField, exists := targetDoc.Fields["Name"]; exists {
		if currentName, ok := nameField.Value.AsString(); ok && !strings.EqualFold(currentName, oldName) {
			cs.logger.Warnf("Catalog name mismatch: expected '%s' but found '%s' (proceeding with update)", oldName, currentName)
		}
	}

	// Update the Name field
	targetDoc.Fields["Name"] = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(newName),
	}

	// Update the FilePath field (now points to new database directory)
	newFilePath := filepath.Join(cs.databaseService.Settings.DataDir, newName)
	targetDoc.Fields["FilePath"] = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(newFilePath),
	}

	// Update the timestamp
	targetDoc.UpdatedAt = time.Now()

	// Persist the updated document back to the catalog
	updateCommand := &models.DocumentUpdateCommand{
		BundleName:  databasesBundle.Name,
		WhereClause: fmt.Sprintf("\"DocumentID\" == \"%s\"", targetDoc.DocumentID),
		Fields: []models.KeyValue{
			{Key: "Name", Value: newName},
			{Key: "FilePath", Value: newFilePath},
		},
	}

	// Use UpdateDocumentInBundle to persist changes
	if err := cs.bundleService.UpdateDocumentInBundle(primaryDB, databasesBundle, updateCommand); err != nil {
		return fmt.Errorf("failed to update database in catalog: %w", err)
	}

	// Flush buffers to ensure data is written immediately
	cs.bundleService.FlushAllBuffers()

	cs.logger.Infof("Successfully updated catalog entry for database '%s' -> '%s'", oldName, newName)
	return nil
}

// Removes a bundle registered in the primary db
func (cs *CatalogService) UnRegisterBundleInCatalog(bundleID, bundleName, databaseID, databaseName string) error {
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

	// Load documents using page-based loading (consistent with other catalog methods)
	docs, err := cs.bundleService.LoadCatalogBundleDocuments(bundlesBundle.Name)
	if err != nil {
		return fmt.Errorf("failed to load documents for bundle '%s': %w", bundlesBundle.Name, err)
	}
	docIDs := make([]string, 0)
	for _, doc := range docs {
		if name, okN := doc.Fields["Name"].Value.AsString(); okN {
			if dbID, okD := doc.Fields["DatabaseID"].Value.AsString(); okD && name == bundleName && dbID == databaseID {
				// Create a delete command for the bundle using BundleID as the where clause
				docIDs = append(docIDs, doc.DocumentID)
			}
		}
	}

	deleteCommand := &models.DocumentDeleteCommand{
		BundleName:  "Bundles",
		WhereClause: fmt.Sprintf("\"DocumentID\" ==\"%s\"", docIDs[0]),
	}

	// Delete the bundle from the bundle
	err = cs.bundleService.DeleteDocumentFromBundle(bundlesBundle, deleteCommand, docIDs)
	if err != nil {
		return fmt.Errorf("failed to delete bundle from catalog: %w", err)
	}

	// Flush buffers to ensure data is written
	cs.bundleService.FlushAllBuffers()

	cs.logger.Infof("Removed bundle '%s' (ID: %s) from system catalog", bundleName, bundleID)
	return nil
}

// GetDatabaseFromCatalog retrieves a database document from the catalog by DatabaseID
func (cs *CatalogService) GetDatabaseFromCatalog(databaseID string) (*models.Document, error) {
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

	docs, err := cs.bundleService.LoadCatalogBundleDocuments(databasesBundle.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to load documents for bundle '%s': %w", databasesBundle.Name, err)
	}

	// Find the document for this database

	for _, doc := range docs {
		// Access the DatabaseID field from the Document struct
		if dbIDField, exists := doc.Fields["DatabaseID"]; exists {
			if dbID, ok := dbIDField.Value.AsString(); ok && dbID == databaseID {
				// Convert document to map for return

				return doc, nil
			}
		}
	}

	return nil, fmt.Errorf("database with ID '%s' not found in catalog", databaseID)
}

// GetDatabaseFromCatalog retrieves a database document from the catalog by DatabaseID
func (cs *CatalogService) GetDatabaseFromCatalogByName(databaseName string) (*models.Document, error) {
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

	docs, err := cs.bundleService.LoadCatalogBundleDocuments(databasesBundle.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to load documents for bundle '%s': %w", databasesBundle.Name, err)
	}

	// Find the document for this database

	for _, doc := range docs {
		// Access the DatabaseID field from the Document struct
		if dbIDField, exists := doc.Fields["Name"]; exists {
			if dbName, ok := dbIDField.Value.AsString(); ok && strings.EqualFold(dbName, databaseName) {
				// Found it!

				return doc, nil
			}
		}
	}

	return nil, fmt.Errorf("database with Name '%s' not found in catalog", databaseName)
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

	// Load documents using page-based loading (same as other catalog methods)
	docs, err := cs.bundleService.LoadCatalogBundleDocuments(databasesBundle.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to load documents for bundle '%s': %w", databasesBundle.Name, err)
	}

	var databases []map[string]interface{}
	for _, doc := range docs {
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

	// Load documents using page-based loading (consistent with other catalog methods)
	docs, err := cs.bundleService.LoadCatalogBundleDocuments(bundlesBundle.Name)
	if err != nil {
		return fmt.Errorf("failed to load documents for bundle '%s': %w", bundlesBundle.Name, err)
	}

	// Find the document for this bundle
	var docIDToRemove string
	var bundleName string
	for _, doc := range docs {
		// Access the BundleID field from the Document struct
		if bundleIDField, exists := doc.Fields["BundleID"]; exists {
			if bID, ok := bundleIDField.Value.AsString(); ok && bID == bundleID {
				docIDToRemove = doc.DocumentID
				if nameField, exists := doc.Fields["Name"]; exists {
					if name, ok := nameField.Value.AsString(); ok {
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

		// TODO FIX THIS TO USE THE SAME TECHNIQUE AS THE CommandDirector.DeleteDocument function
		// Delete the document from the bundle
		err = cs.bundleService.DeleteDocumentFromBundle(bundlesBundle, deleteCommand, make([]string, 0))
		if err != nil {
			return fmt.Errorf("failed to delete bundle from catalog: %w", err)
		}

		// TODO We may need to flush the buffers here to ensure data is written

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

	docs, err := cs.bundleService.LoadCatalogBundleDocuments(bundlesBundle.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to load documents for bundle '%s': %w", bundlesBundle.Name, err)
	}

	// Find the document for this bundle

	for _, doc := range docs {
		// Access the BundleID field from the Document struct
		if bundleIDField, exists := doc.Fields["BundleID"]; exists {
			if bID, ok := bundleIDField.Value.AsString(); ok && bID == bundleID {
				name, _ := doc.Fields["Name"].Value.AsString()
				actualBundle, err1 := cs.bundleService.GetBundleByName(primaryDB, name)
				if err1 != nil {
					return nil, fmt.Errorf("failed to get bundle Metadata from Catalog by name '%s': %w", name, err1)
				}
				// Convert document to map for return
				result := make(map[string]interface{})
				result["BundleMetadata"] = actualBundle
				result["CreatedAt"] = doc.CreatedAt
				result["UpdatedAt"] = doc.UpdatedAt
				result["DatabaseName"] = doc.Fields["DatabaseName"].Value
				result["DatabaseID"] = doc.Fields["DatabaseID"].Value
				result["FieldCount"] = doc.Fields["FieldCount"].Value
				result["FilePath"] = doc.Fields["FilePath"].Value

				// // Add all fields to the result
				// for fieldName, field := range doc.Fields {
				// 	result[fieldName] = field.Value
				// }

				return result, nil
			}
		}
	}

	return nil, fmt.Errorf("bundle with ID '%s' not found in catalog", bundleID)
}

// GetBundleFromCatalog retrieves a bundle document from the catalog by BundleID
func (cs *CatalogService) GetBundlesFromCatalogByDatabaseName(databaseName string) (*[]map[string]interface{}, error) {
	// Get the primary database
	primaryDB, err := cs.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary database: %w", err)
	}

	// Get the database in the query
	targetDB, err := cs.databaseService.GetDatabaseByName(databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to get target database by name '%s': %w", databaseName, err)
	}

	// Get the Bundles bundle metadata from primary database. THIS CALL IS DEPRECATED AND SHOULD BE REPLACED
	bundlesBundle, err := cs.bundleService.GetBundleByName(primaryDB, "Bundles")
	if err != nil {
		return nil, fmt.Errorf("failed to get primary.Bundles bundle: %w", err)
	}

	docs, err := cs.bundleService.LoadCatalogBundleDocuments(bundlesBundle.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to load documents for bundle '%s': %w", bundlesBundle.Name, err)
	}

	results := make([]map[string]interface{}, 0)
	// Find the document for this bundle

	for _, doc := range docs {

		// Access the DatabaseName field from the Document struct
		if databaseNameField, exists := doc.Fields["DatabaseName"]; exists {
			if dbName, ok := databaseNameField.Value.AsString(); ok && dbName == databaseName {
				// Add doc to slice for return
				name, _ := doc.Fields["Name"].Value.AsString()
				actualBundle, err1 := cs.bundleService.GetBundleByName(targetDB, name)
				if err1 != nil {
					return nil, fmt.Errorf("failed to get bundle Metadata from Catalog by name '%s': %w", name, err1)
				}
				// Convert document to map for return
				result := make(map[string]interface{})
				result["BundleMetadata"] = actualBundle
				result["CreatedAt"] = doc.CreatedAt
				result["UpdatedAt"] = doc.UpdatedAt
				result["DatabaseName"] = doc.Fields["DatabaseName"].Value
				result["DatabaseID"] = doc.Fields["DatabaseID"].Value
				result["FieldCount"] = doc.Fields["FieldCount"].Value
				result["FilePath"] = doc.Fields["FilePath"].Value

				// // Add all fields to the result
				// for fieldName, field := range doc.Fields {
				// 	result[fieldName] = field.Value
				// }

				results = append(results, result)
			}
		}
	}

	return &results, nil
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

	// Load documents using page-based loading (same as other catalog methods)
	docs, err := cs.bundleService.LoadCatalogBundleDocuments(bundlesBundle.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to load documents for bundle '%s': %w", bundlesBundle.Name, err)
	}

	var bundles []map[string]interface{}
	for _, doc := range docs {
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

	return cs.RegisterBundleInCatalog(tempBundle)
}

// RemoveBundleFromCatalogByID is a convenience method to remove a bundle from catalog by ID
func (cs *CatalogService) RemoveBundleFromCatalogByID(bundleID string) error {
	return cs.RemoveBundleFromCatalog(bundleID)
}
