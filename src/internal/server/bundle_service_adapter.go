package server

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/journal"

	"go.uber.org/zap"
)

/*
bundle_service_adapter.go

This adapter bridges the gap between what the migration service expects (simple map-based interface)
and what BundleService provides (complex domain model with Database/Bundle objects).

Design Principles:
  - Single Responsibility: Only responsible for type/interface conversion
  - DRY: Centralizes database/bundle lookup logic used by all methods
  - Open/Closed: Extends BundleService functionality without modifying it

TODO: I will add caching for frequently accessed databases/bundles to improve performance
TODO: I will implement retry logic for transient failures in production environments
TODO: I will add metrics tracking for migration operations to monitor system health
*/

// BundleServiceAdapter wraps BundleService to provide the interface expected by MigrationService
type BundleServiceAdapter struct {
	bundleService   *bundle.BundleService
	databaseService *database.DatabaseService
	catalogService  *defaultdb.CatalogService
	walManager      *journal.WALManager
	logger          *zap.SugaredLogger
}

// NewBundleServiceAdapter creates a new adapter instance
func NewBundleServiceAdapter(
	bundleService *bundle.BundleService,
	databaseService *database.DatabaseService,
	catalogService *defaultdb.CatalogService,
	walManager *journal.WALManager,
	logger *zap.SugaredLogger,
) *BundleServiceAdapter {
	return &BundleServiceAdapter{
		bundleService:   bundleService,
		databaseService: databaseService,
		catalogService:  catalogService,
		walManager:      walManager,
		logger:          logger,
	}
}

// resolveDatabaseAndBundle is a helper method that centralizes database and bundle lookup
// This follows the DRY principle by avoiding repeated lookup code in each method
func (a *BundleServiceAdapter) resolveDatabaseAndBundle(dbName, bundleName string) (*models.Database, *models.Bundle, error) {
	// Get database by name
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return nil, nil, err
	}

	// Get bundle by name
	bndl, err := a.bundleService.GetBundleByName(db, bundleName)
	if err != nil {
		return nil, nil, fmt.Errorf("bundle '%s' not found in database '%s': %w", bundleName, dbName, err)
	}

	return db, bndl, nil
}

// InsertDocument adapts the simple map-based interface to BundleService's domain model
func (a *BundleServiceAdapter) InsertDocument(dbName, bundleName string, doc map[string]interface{}) error {
	db, bndl, err := a.resolveDatabaseAndBundle(dbName, bundleName)
	if err != nil {
		return err
	}

	// Convert map to DocumentCommand
	// DocumentCommand uses []KeyValue for Fields, not map
	keyValues := make([]models.KeyValue, 0, len(doc))
	for key, value := range doc {
		// Skip DocumentID as it's not a regular field
		if key == "DocumentID" {
			continue
		}

		// DEBUG: Log array fields before storage
		// if key == "UpCommands" || key == "DownCommands" {
		// 	//a.logger.Infof("[STORAGE DEBUG] Storing %s: type=%T, value=%v", key, value, value)
		// 	if arr, ok := value.([]string); ok {
		// 	//	a.logger.Infof("[STORAGE DEBUG] %s is []string with %d elements", key, len(arr))
		// 		for i, cmd := range arr {
		// 			a.logger.Infof("[STORAGE DEBUG] %s[%d]: length=%d, preview=%s", key, i, len(cmd), truncate(cmd, 100))
		// 		}
		// 	}
		// }

		keyValues = append(keyValues, models.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	docCommand := &models.DocumentCommand{
		CommandType: "ADD_DOCUMENT",
		BundleName:  bundleName,
		Fields:      keyValues,
	}

	// Call BundleService.AddDocumentToBundle
	_, err = a.bundleService.AddDocumentToBundle(db, bndl, docCommand)
	if err != nil {
		return fmt.Errorf("failed to insert document into bundle '%s': %w", bundleName, err)
	}

	return nil
}

// UpdateDocument adapts the simple interface to BundleService's update mechanism
func (a *BundleServiceAdapter) UpdateDocument(dbName, bundleName, docID string, updates map[string]interface{}) error {
	db, bndl, err := a.resolveDatabaseAndBundle(dbName, bundleName)
	if err != nil {
		return err
	}

	// Convert to DocumentUpdateCommand
	// DocumentUpdateCommand uses []KeyValue for Fields and has WhereClause
	keyValues := make([]models.KeyValue, 0, len(updates))
	for key, value := range updates {
		keyValues = append(keyValues, models.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	// Use WHERE clause to target specific document by ID
	whereClause := fmt.Sprintf("DocumentID == '%s'", docID)

	updateCommand := &models.DocumentUpdateCommand{
		BundleName:  bundleName,
		Fields:      keyValues,
		WhereClause: whereClause,
		Confirmed:   true,
	}

	// Call BundleService.UpdateDocumentInBundle
	err = a.bundleService.UpdateDocumentInBundle(db, bndl, updateCommand)
	if err != nil {
		return fmt.Errorf("failed to update document '%s' in bundle '%s': %w", docID, bundleName, err)
	}

	return nil
}

// UpdateDocumentByField updates documents using a WHERE clause on any field (not just DocumentID)
// This is useful for updating by unique fields like MigrationID when DocumentID is auto-generated
func (a *BundleServiceAdapter) UpdateDocumentByField(dbName, bundleName, fieldName, fieldValue string, updates map[string]interface{}) error {
	db, bndl, err := a.resolveDatabaseAndBundle(dbName, bundleName)
	if err != nil {
		return err
	}

	// Convert to DocumentUpdateCommand
	keyValues := make([]models.KeyValue, 0, len(updates))
	for key, value := range updates {
		keyValues = append(keyValues, models.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	// Use WHERE clause with the specified field
	// CRITICAL: Use double quotes for string values (parseValue expects double quotes)
	whereClause := fmt.Sprintf("%s == \"%s\"", fieldName, fieldValue)

	updateCommand := &models.DocumentUpdateCommand{
		BundleName:  bundleName,
		Fields:      keyValues,
		WhereClause: whereClause,
		Confirmed:   true,
	}

	a.logger.Debugf("UpdateDocumentByField: bundle=%s, field=%s, value=%s, updates=%+v", bundleName, fieldName, fieldValue, updates)

	// Call BundleService.UpdateDocumentInBundle
	err = a.bundleService.UpdateDocumentInBundle(db, bndl, updateCommand)
	if err != nil {
		return fmt.Errorf("failed to update document in bundle '%s' where %s='%s': %w", bundleName, fieldName, fieldValue, err)
	}

	return nil
}

// DeleteDocument adapts the simple interface to BundleService's delete mechanism
func (a *BundleServiceAdapter) DeleteDocument(dbName, bundleName, docID string) error {
	_, bndl, err := a.resolveDatabaseAndBundle(dbName, bundleName)
	if err != nil {
		return err
	}

	// Convert to DocumentDeleteCommand
	deleteCommand := &models.DocumentDeleteCommand{
		BundleName: bundleName,
	}

	// Call BundleService.DeleteDocumentFromBundle
	err = a.bundleService.DeleteDocumentFromBundle(bndl, deleteCommand, []string{docID})
	if err != nil {
		return fmt.Errorf("failed to delete document '%s' from bundle '%s': %w", docID, bundleName, err)
	}

	return nil
}

// QueryDocuments retrieves documents matching the filter and converts them to maps
// TODO: I will implement more sophisticated filter-to-WHERE-clause conversion for complex queries
func (a *BundleServiceAdapter) QueryDocuments(dbName, bundleName string, filter map[string]interface{}) ([]map[string]interface{}, error) {
	_, bndl, err := a.resolveDatabaseAndBundle(dbName, bundleName)
	if err != nil {
		return nil, err
	}

	// Convert filter map to WHERE clause string
	// For now, we support simple equality filters
	// Example: {"status": "PENDING"} => "status == 'PENDING'"
	whereClause := a.buildWhereClause(filter)

	// Call BundleService.GetDocumentsByFilter
	documents, err := a.bundleService.GetDocumentsByFilter(bndl, whereClause)
	if err != nil {
		return nil, fmt.Errorf("failed to query documents from bundle '%s': %w", bundleName, err)
	}

	a.logger.Debugf("QueryDocuments: bundle=%s, filter=%+v, found %d documents", bundleName, filter, len(documents))

	// Convert []*models.Document to []map[string]interface{}
	result := make([]map[string]interface{}, 0, len(documents))
	for _, doc := range documents {
		docMap := make(map[string]interface{})
		docMap["DocumentID"] = doc.DocumentID

		// Copy all fields from the Fields map
		// CRITICAL: Extract actual value from FieldValue union using AsInterface()
		for fieldName, field := range doc.Fields {
			docMap[fieldName] = field.Value.AsInterface()
		}

		result = append(result, docMap)
	}

	return result, nil
}

// buildWhereClause converts a simple filter map to a WHERE clause string
// This is a basic implementation that handles equality comparisons
func (a *BundleServiceAdapter) buildWhereClause(filter map[string]interface{}) string {
	if len(filter) == 0 {
		return ""
	}

	conditions := make([]string, 0, len(filter))
	for key, value := range filter {
		// Skip DocumentID as it's handled separately
		if key == "DocumentID" {
			continue
		}

		// Build condition based on value type
		// CRITICAL: Use double quotes for strings, not single quotes
		// The WHERE clause parser (parseValue) only strips double quotes
		switch v := value.(type) {
		case string:
			conditions = append(conditions, fmt.Sprintf("%s == \"%s\"", key, v))
		case int, int64:
			conditions = append(conditions, fmt.Sprintf("%s == %v", key, v))
		case float64:
			conditions = append(conditions, fmt.Sprintf("%s == %v", key, v))
		case bool:
			conditions = append(conditions, fmt.Sprintf("%s == %v", key, v))
		default:
			// For complex types, convert to string
			conditions = append(conditions, fmt.Sprintf("%s == \"%v\"", key, v))
		}
	}

	return strings.Join(conditions, " AND ")
}

// GetDocumentCount returns the number of documents in a bundle
func (a *BundleServiceAdapter) GetDocumentCount(dbName, bundleName string) (int, error) {
	_, bndl, err := a.resolveDatabaseAndBundle(dbName, bundleName)
	if err != nil {
		return 0, err
	}

	// Use bundle metadata to get document count
	// TotalDocuments includes both active and deleted documents
	// For migration purposes, we want the total count
	count := int(bndl.TotalDocuments)

	return count, nil
}

// BeginTransaction starts a new WAL transaction
// TODO: I will add database-specific transaction context when WALManager supports per-database transactions
func (a *BundleServiceAdapter) BeginTransaction(dbName string) (string, error) {
	if a.walManager == nil {
		return "", fmt.Errorf("WAL manager not initialized")
	}

	txID, err := a.walManager.BeginTransaction()
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction for database '%s': %w", dbName, err)
	}

	a.logger.Debugf("Started transaction %s for database '%s'", txID, dbName)
	return txID, nil
}

// CommitTransaction commits a WAL transaction
func (a *BundleServiceAdapter) CommitTransaction(txID string) error {
	if a.walManager == nil {
		return fmt.Errorf("WAL manager not initialized")
	}

	err := a.walManager.CommitTransaction(txID)
	if err != nil {
		return fmt.Errorf("failed to commit transaction %s: %w", txID, err)
	}

	a.logger.Debugf("Committed transaction %s", txID)
	return nil
}

// RollbackTransaction rolls back a WAL transaction
func (a *BundleServiceAdapter) RollbackTransaction(txID string) error {
	if a.walManager == nil {
		return fmt.Errorf("WAL manager not initialized")
	}

	err := a.walManager.RollbackTransaction(txID)
	if err != nil {
		return fmt.Errorf("failed to rollback transaction %s: %w", txID, err)
	}

	a.logger.Debugf("Rolled back transaction %s", txID)
	return nil
}

// ========================================================================
// DDL Operations (Bundle Schema Management)
// These methods support CREATE/ALTER/DROP BUNDLE operations in migrations
// ========================================================================

// CreateBundle creates a new bundle with the specified schema
// TODO: I will add field validation and constraint checking before bundle creation
func (a *BundleServiceAdapter) CreateBundle(dbName, bundleName string, fields []models.FieldDefinition) error {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	// Create BundleCommand for the AddBundle operation
	bundleCmd := &models.BundleCommand{
		CommandType: "CREATE",
		BundleName:  bundleName,
		Fields:      fields,
	}

	// Use AddBundle to create the new bundle
	newBundle, err := a.bundleService.AddBundle(a.databaseService, db, bundleCmd)
	if err != nil {
		return fmt.Errorf("failed to create bundle '%s': %w", bundleName, err)
	}

	// Register the bundle in the system catalog (critical for SHOW BUNDLES to work)
	if a.catalogService != nil {
		err = a.catalogService.RegisterBundleInCatalog(newBundle)
		if err != nil {
			a.logger.Warnf("Warning: Failed to register bundle '%s' in catalog: %v", bundleName, err)
			// Don't fail the migration if catalog registration fails - log and continue
		}
	} else {
		a.logger.Warnf("Warning: CatalogService is nil, bundle '%s' not registered in catalog", bundleName)
	}

	a.logger.Infof("Created bundle '%s' in database '%s'", bundleName, dbName)
	return nil
}

// DeleteBundle removes a bundle from the database
// TODO: I will add cascade delete option for bundles with relationships
func (a *BundleServiceAdapter) DeleteBundle(dbName, bundleName string, force bool) error {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	// Check if bundle has documents (unless force)
	if !force {
		count, err := a.GetDocumentCount(dbName, bundleName)
		if err != nil {
			return fmt.Errorf("failed to check document count: %w", err)
		}
		if count > 0 {
			return fmt.Errorf("cannot delete bundle '%s': contains %d documents (use force=true to override)", bundleName, count)
		}
	}

	// Create BundleCommand for deletion
	bundleCmd := &models.BundleCommand{
		CommandType:    "DELETE",
		BundleName:     bundleName,
		HasForceSwitch: force,
	}

	// Use DeleteBundle to remove the bundle
	err = a.bundleService.DeleteBundle(db, bundleCmd)
	if err != nil {
		return fmt.Errorf("failed to delete bundle '%s': %w", bundleName, err)
	}

	a.logger.Infof("Deleted bundle '%s' from database '%s'", bundleName, dbName)
	return nil
}

// RenameBundle changes a bundle's name
// TODO: I will add validation to prevent renaming to existing bundle names
func (a *BundleServiceAdapter) RenameBundle(dbName, oldName, newName string) error {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	// Get the bundle to rename
	bndl, err := a.bundleService.GetBundleByName(db, oldName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found: %w", oldName, err)
	}

	// Use RenameBundle service method
	err = a.bundleService.RenameBundle(db, bndl, newName)
	if err != nil {
		return fmt.Errorf("failed to rename bundle from '%s' to '%s': %w", oldName, newName, err)
	}

	a.logger.Infof("Renamed bundle from '%s' to '%s' in database '%s'", oldName, newName, dbName)
	return nil
}

// AddField adds a new field to a bundle's schema
// TODO: I will implement field addition with default value propagation to existing documents
func (a *BundleServiceAdapter) AddField(dbName, bundleName string, field models.FieldDefinition) error {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	bndl, err := a.bundleService.GetBundleByName(db, bundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found: %w", bundleName, err)
	}

	// Create field change for ADD operation
	change := models.FieldChange{
		ChangeType: "ADD",
		NewField:   field,
	}

	// Apply the field change directly (no BundleCommand wrapper needed)
	err = a.bundleService.ApplyFieldChanges(db, bndl, []models.FieldChange{change})
	if err != nil {
		return fmt.Errorf("failed to add field '%s' to bundle '%s': %w", field.Name, bundleName, err)
	}

	a.logger.Infof("Added field '%s' to bundle '%s' in database '%s'", field.Name, bundleName, dbName)
	return nil
}

// DropField removes a field from a bundle's schema
// TODO: I will add safety checks to prevent dropping required/indexed fields
func (a *BundleServiceAdapter) DropField(dbName, bundleName, fieldName string) error {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	bndl, err := a.bundleService.GetBundleByName(db, bundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found: %w", bundleName, err)
	}

	// Create field change for REMOVE operation
	change := models.FieldChange{
		ChangeType:   "REMOVE",
		OldFieldName: fieldName,
	}

	// Apply the field change directly (no BundleCommand wrapper needed)
	err = a.bundleService.ApplyFieldChanges(db, bndl, []models.FieldChange{change})
	if err != nil {
		return fmt.Errorf("failed to drop field '%s' from bundle '%s': %w", fieldName, bundleName, err)
	}

	a.logger.Infof("Dropped field '%s' from bundle '%s' in database '%s'", fieldName, bundleName, dbName)
	return nil
}

// RenameField changes a field's name in a bundle's schema
// TODO: I will add index rebuilding after field rename to maintain query performance
func (a *BundleServiceAdapter) RenameField(dbName, bundleName, oldFieldName, newFieldName string) error {
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	bndl, err := a.bundleService.GetBundleByName(db, bundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found: %w", bundleName, err)
	}

	// Get the existing field definition from DocumentStructure
	existingField, exists := bndl.DocumentStructure.FieldDefinitions[oldFieldName]
	if !exists {
		return fmt.Errorf("field '%s' not found in bundle '%s'", oldFieldName, bundleName)
	}

	// Create new field definition with updated name
	newField := existingField
	newField.Name = newFieldName

	// Create field change for MODIFY operation (rename uses MODIFY, not CHANGE)
	change := models.FieldChange{
		ChangeType:   "MODIFY",
		OldFieldName: oldFieldName,
		NewField:     newField,
	}

	// Apply the field change directly (no BundleCommand wrapper needed)
	err = a.bundleService.ApplyFieldChanges(db, bndl, []models.FieldChange{change})
	if err != nil {
		return fmt.Errorf("failed to rename field from '%s' to '%s' in bundle '%s': %w", oldFieldName, newFieldName, bundleName, err)
	}

	a.logger.Infof("Renamed field from '%s' to '%s' in bundle '%s' (database '%s')", oldFieldName, newFieldName, bundleName, dbName)
	return nil
}

// CreateHashIndex creates a hash index on a bundle using the command string
func (a *BundleServiceAdapter) CreateHashIndex(dbName, command string) error {
	// Get database
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return fmt.Errorf("database '%s' not found: %w", dbName, err)
	}

	// Use the existing CreateHashIndex function from index_operations.go
	// Create a ServiceManager to pass to the function
	sm := ServiceManager{
		BundleService:   a.bundleService,
		DatabaseService: a.databaseService,
	}

	_, err, _ = CreateHashIndex(command, a.logger, sm, db)
	if err != nil {
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	return nil
}

// CreateBTreeIndex creates a B-Tree index on a bundle using the command string
func (a *BundleServiceAdapter) CreateBTreeIndex(dbName, command string) error {
	// Get database
	db, err := a.databaseService.GetDatabaseByName(dbName)
	if err != nil {
		return fmt.Errorf("database '%s' not found: %w", dbName, err)
	}

	// Use the existing CreateBTreeIndex function from index_operations.go
	// Create a ServiceManager to pass to the function
	sm := ServiceManager{
		BundleService:   a.bundleService,
		DatabaseService: a.databaseService,
	}

	_, err = CreateBTreeIndex(command, a.logger, sm, db)
	if err != nil {
		return fmt.Errorf("failed to create B-Tree index: %w", err)
	}

	return nil
}

// truncate returns first n chars of s with "..." if truncated
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
