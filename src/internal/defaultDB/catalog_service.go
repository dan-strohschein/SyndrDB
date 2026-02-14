package defaultdb

import (
	"context"
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

	docID := helpers.GenerateFastUUID()
	schema := databasesBundle.DocumentStructure.FieldSchema()
	catalogDoc := models.Document{
		DocumentID: docID,
		Values:     make([]models.FieldValue, len(schema.Names)),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	for name, idx := range schema.NameToIndex {
		switch name {
		case "DocumentID":
			catalogDoc.Values[idx] = models.NewStringValue(docID)
		case "DatabaseID":
			catalogDoc.Values[idx] = models.NewStringValue(db.DatabaseID)
		case "Name":
			catalogDoc.Values[idx] = models.NewStringValue(db.Name)
		case "FilePath":
			catalogDoc.Values[idx] = models.NewStringValue(db.DataDirectory)
		}
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

	// TODO I should use the LoadCatalogBundleDocuments function to ensure we have the latest documents
	// Find the document for this database using page cache
	var docIDToRemove string
	var databaseName string
	// TODO: I need to iterate through documents using page cache via BundleService.GetDocumentPage()
	// For now, we scan through all document IDs and load each document
	dbSchema := databasesBundle.DocumentStructure.FieldSchema()
	dbDocIDs := databasesBundle.SortedIndex.GetAllDocumentIDs()
	for _, docID := range dbDocIDs {
		doc, err := cs.bundleService.GetDocument(databasesBundle.Name, cs.databaseService.Databases["primary"].Name, docID)
		if err != nil {
			continue
		}
		fv, exists := doc.GetFieldValue(dbSchema, "DatabaseID")
		if exists {
			if dbID, ok := fv.AsString(); ok && dbID == databaseID {
				docIDToRemove = docID
				if nv, ex := doc.GetFieldValue(dbSchema, "Name"); ex {
					if name, ok := nv.AsString(); ok {
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
		//TODO Make this use the same technique as the CommandDirector.DeleteDocument function
		// Delete the document from the bundle
		err = cs.bundleService.DeleteDocumentFromBundle(databasesBundle, deleteCommand, make([]string, 0), nil)
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

	docID := helpers.GenerateFastUUID()
	bundlesSchema := bundlesBundle.DocumentStructure.FieldSchema()
	catalogDoc := models.Document{
		DocumentID: docID,
		Values:     make([]models.FieldValue, len(bundlesSchema.Names)),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	for name, idx := range bundlesSchema.NameToIndex {
		switch name {
		case "DocumentID":
			catalogDoc.Values[idx] = models.NewStringValue(docID)
		case "BundleID":
			catalogDoc.Values[idx] = models.NewStringValue(bundle.BundleID)
		case "Name":
			catalogDoc.Values[idx] = models.NewStringValue(bundle.Name)
		case "DatabaseID":
			catalogDoc.Values[idx] = models.NewStringValue(bundle.Database.DatabaseID)
		case "DatabaseName":
			catalogDoc.Values[idx] = models.NewStringValue(bundle.Database.Name)
		case "FieldCount":
			catalogDoc.Values[idx] = models.NewIntValue(int64(len(bundle.DocumentStructure.FieldDefinitions)))
		case "FilePath":
			catalogDoc.Values[idx] = models.NewStringValue(fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))
		}
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

	docs, err := cs.bundleService.LoadCatalogBundleDocuments(bundlesBundle.Name)
	if err != nil {
		return fmt.Errorf("failed to load catalog documents: %w", err)
	}

	bundlesSchema := bundlesBundle.DocumentStructure.FieldSchema()
	var targetIdx int
	found := false
	for i, doc := range docs {
		fv, exists := doc.GetFieldValue(bundlesSchema, "BundleID")
		if exists {
			if bID, ok := fv.AsString(); ok && bID == bundleID {
				targetIdx = i
				found = true
				break
			}
		}
	}
	var targetDoc *models.Document
	if found {
		targetDoc = docs[targetIdx]
	}

	if targetDoc == nil {
		return fmt.Errorf("bundle with ID '%s' not found in catalog", bundleID)
	}

	if nv, exists := targetDoc.GetFieldValue(bundlesSchema, "Name"); exists {
		if currentName, ok := nv.AsString(); ok && currentName != oldName {
			cs.logger.Warnf("Catalog name mismatch: expected '%s' but found '%s' (proceeding with update)", oldName, currentName)
		}
	}

	// Update Name and FilePath by index (copy Values so we don't mutate shared slice)
	newValues := make([]models.FieldValue, len(targetDoc.Values))
	copy(newValues, targetDoc.Values)
	if i, ok := bundlesSchema.NameToIndex["Name"]; ok && i < len(newValues) {
		newValues[i] = models.NewStringValue(newName)
	}
	newFilePath := fmt.Sprintf("%s_%s.bnd", databaseName, newName)
	if i, ok := bundlesSchema.NameToIndex["FilePath"]; ok && i < len(newValues) {
		newValues[i] = models.NewStringValue(newFilePath)
	}
	targetDoc.Values = newValues

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

	if err := cs.bundleService.UpdateDocumentInBundle(context.Background(), primaryDB, bundlesBundle, updateCommand); err != nil {
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

	docs, err := cs.bundleService.LoadCatalogBundleDocuments(databasesBundle.Name)
	if err != nil {
		return fmt.Errorf("failed to load catalog documents: %w", err)
	}

	dbSchema := databasesBundle.DocumentStructure.FieldSchema()
	var targetDoc *models.Document
	for i := range docs {
		fv, exists := docs[i].GetFieldValue(dbSchema, "DatabaseID")
		if exists {
			if dbID, ok := fv.AsString(); ok && dbID == databaseID {
				targetDoc = docs[i]
				break
			}
		}
	}

	if targetDoc == nil {
		return fmt.Errorf("database with ID '%s' not found in catalog", databaseID)
	}

	if nv, exists := targetDoc.GetFieldValue(dbSchema, "Name"); exists {
		if currentName, ok := nv.AsString(); ok && !strings.EqualFold(currentName, oldName) {
			cs.logger.Warnf("Catalog name mismatch: expected '%s' but found '%s' (proceeding with update)", oldName, currentName)
		}
	}

	newFilePath := filepath.Join(cs.databaseService.Settings.DataDir, newName)
	newValues := make([]models.FieldValue, len(targetDoc.Values))
	copy(newValues, targetDoc.Values)
	if i, ok := dbSchema.NameToIndex["Name"]; ok && i < len(newValues) {
		newValues[i] = models.NewStringValue(newName)
	}
	if i, ok := dbSchema.NameToIndex["FilePath"]; ok && i < len(newValues) {
		newValues[i] = models.NewStringValue(newFilePath)
	}
	targetDoc.Values = newValues

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

	if err := cs.bundleService.UpdateDocumentInBundle(context.Background(), primaryDB, databasesBundle, updateCommand); err != nil {
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
	bundlesSchema := bundlesBundle.DocumentStructure.FieldSchema()
	docIDs := make([]string, 0)
	for _, doc := range docs {
		nv, _ := doc.GetFieldValue(bundlesSchema, "Name")
		dv, _ := doc.GetFieldValue(bundlesSchema, "DatabaseID")
		if name, okN := nv.AsString(); okN {
			if dbID, okD := dv.AsString(); okD && name == bundleName && dbID == databaseID {
				docIDs = append(docIDs, doc.DocumentID)
			}
		}
	}

	deleteCommand := &models.DocumentDeleteCommand{
		BundleName:  "Bundles",
		WhereClause: fmt.Sprintf("\"DocumentID\" ==\"%s\"", docIDs[0]),
	}

	// Delete the bundle from the bundle
	err = cs.bundleService.DeleteDocumentFromBundle(bundlesBundle, deleteCommand, docIDs, nil)
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

	dbSchema := databasesBundle.DocumentStructure.FieldSchema()
	for i := range docs {
		fv, exists := docs[i].GetFieldValue(dbSchema, "DatabaseID")
		if exists {
			if dbID, ok := fv.AsString(); ok && dbID == databaseID {
				return docs[i], nil
			}
		}
	}

	return nil, fmt.Errorf("database with ID '%s' not found in catalog", databaseID)
}

// GetDatabaseFromCatalogByName retrieves a database document from the catalog by name
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

	dbSchema := databasesBundle.DocumentStructure.FieldSchema()
	for i := range docs {
		fv, exists := docs[i].GetFieldValue(dbSchema, "Name")
		if exists {
			if dbName, ok := fv.AsString(); ok && strings.EqualFold(dbName, databaseName) {
				return docs[i], nil
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

	dbSchema := databasesBundle.DocumentStructure.FieldSchema()
	var databases []map[string]interface{}
	for _, doc := range docs {
		docMap := make(map[string]interface{})
		docMap["DocumentID"] = doc.DocumentID
		docMap["CreatedAt"] = doc.CreatedAt
		docMap["UpdatedAt"] = doc.UpdatedAt
		for i, name := range dbSchema.Names {
			if i < len(doc.Values) {
				docMap[name] = doc.Values[i].AsInterface()
			}
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

	bundlesSchema := bundlesBundle.DocumentStructure.FieldSchema()
	var docIDToRemove string
	var bundleName string
	for _, doc := range docs {
		fv, exists := doc.GetFieldValue(bundlesSchema, "BundleID")
		if exists {
			if bID, ok := fv.AsString(); ok && bID == bundleID {
				docIDToRemove = doc.DocumentID
				if nv, ex := doc.GetFieldValue(bundlesSchema, "Name"); ex {
					if name, ok := nv.AsString(); ok {
						bundleName = name
					}
				}
				break
			}
		}
	}

	if docIDToRemove != "" {
		deleteCommand := &models.DocumentDeleteCommand{
			BundleName:  "Bundles",
			WhereClause: fmt.Sprintf("DocumentID='%s'", docIDToRemove),
		}

		// TODO FIX THIS TO USE THE SAME TECHNIQUE AS THE CommandDirector.DeleteDocument function
		// Delete the document from the bundle
		err = cs.bundleService.DeleteDocumentFromBundle(bundlesBundle, deleteCommand, make([]string, 0), nil)
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

	bundlesSchema := bundlesBundle.DocumentStructure.FieldSchema()
	for _, doc := range docs {
		fv, exists := doc.GetFieldValue(bundlesSchema, "BundleID")
		if exists {
			if bID, ok := fv.AsString(); ok && bID == bundleID {
				nv, _ := doc.GetFieldValue(bundlesSchema, "Name")
				name, _ := nv.AsString()
				actualBundle, err1 := cs.bundleService.GetBundleByName(primaryDB, name)
				if err1 != nil {
					return nil, fmt.Errorf("failed to get bundle Metadata from Catalog by name '%s': %w", name, err1)
				}
				result := make(map[string]interface{})
				result["BundleMetadata"] = actualBundle
				result["CreatedAt"] = doc.CreatedAt
				result["UpdatedAt"] = doc.UpdatedAt
				if v, ok := doc.GetFieldValue(bundlesSchema, "DatabaseName"); ok {
					result["DatabaseName"] = v.AsInterface()
				}
				if v, ok := doc.GetFieldValue(bundlesSchema, "DatabaseID"); ok {
					result["DatabaseID"] = v.AsInterface()
				}
				if v, ok := doc.GetFieldValue(bundlesSchema, "FieldCount"); ok {
					result["FieldCount"] = v.AsInterface()
				}
				if v, ok := doc.GetFieldValue(bundlesSchema, "FilePath"); ok {
					result["FilePath"] = v.AsInterface()
				}
				return result, nil
			}
		}
	}

	return nil, fmt.Errorf("bundle with ID '%s' not found in catalog", bundleID)
}

// GetBundlesFromCatalogByDatabaseName returns all bundles in the catalog for a given database name
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

	bundlesSchema := bundlesBundle.DocumentStructure.FieldSchema()
	for _, doc := range docs {
		dv, exists := doc.GetFieldValue(bundlesSchema, "DatabaseName")
		if exists {
			if dbName, ok := dv.AsString(); ok && dbName == databaseName {
				nv, _ := doc.GetFieldValue(bundlesSchema, "Name")
				name, _ := nv.AsString()
				actualBundle, err1 := cs.bundleService.GetBundleByName(targetDB, name)
				if err1 != nil {
					return nil, fmt.Errorf("failed to get bundle Metadata from Catalog by name '%s': %w", name, err1)
				}
				// Convert document to map for return - include only metadata fields, NOT documents
				result := make(map[string]interface{})
				result["BundleID"] = actualBundle.BundleID
				result["Name"] = actualBundle.Name
				result["Description"] = actualBundle.Description
				result["TotalDocuments"] = actualBundle.TotalDocuments
				result["PageCount"] = actualBundle.PageCount
				result["PageSize"] = actualBundle.PageSize
				result["CreatedAt"] = actualBundle.CreatedAt
				result["UpdatedAt"] = actualBundle.UpdatedAt
				result["CreatedBy"] = actualBundle.CreatedBy

				// // Add all fields to the result
				// for fieldName, field := range doc.Fields {
				// 	result[fieldName] = field.Value
				// }

				// Always include document structure information (even if empty)
				result["DocumentStructure"] = actualBundle.DocumentStructure

				// Include indexes information if available
				if len(actualBundle.Indexes) > 0 {
					result["Indexes"] = actualBundle.Indexes
				}

				// Include index names if available
				if len(actualBundle.IndexNames) > 0 {
					result["IndexNames"] = actualBundle.IndexNames
				}

				// Include relationships information if available
				if len(actualBundle.Relationships) > 0 {
					result["Relationships"] = actualBundle.Relationships
				}

				// Include constraints information if available
				if len(actualBundle.Constraints) > 0 {
					result["Constraints"] = actualBundle.Constraints
				}

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

	bundlesSchema := bundlesBundle.DocumentStructure.FieldSchema()
	var bundles []map[string]interface{}
	for _, doc := range docs {
		docMap := make(map[string]interface{})
		docMap["DocumentID"] = doc.DocumentID
		docMap["CreatedAt"] = doc.CreatedAt
		docMap["UpdatedAt"] = doc.UpdatedAt
		for i, name := range bundlesSchema.Names {
			if i < len(doc.Values) {
				docMap[name] = doc.Values[i].AsInterface()
			}
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
