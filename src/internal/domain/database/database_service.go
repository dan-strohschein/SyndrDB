package database

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

type DatabaseServiceInterface interface {
	AddDatabase(databaseCommand models.DatabaseCommand) error
	UpdateDatabase(databaseCommand models.DatabaseCommand) error
	DeleteDatabase(databaseName string) error
	GetDatabaseByID(id string) (*models.Database, error)
	GetDatabaseByName(name string) (*models.Database, error)
	ListDatabases() []*models.Database
	AddBundleToDatabase(dbName string, bundle models.Bundle, bundleStore bundlestore.BundleStore) error
	// Introspection methods
	GetDatabaseBundles(databaseName string) ([]string, error)
	GetBundleInfo(databaseName, bundleName string) (*models.Bundle, error)
}

// DatabaseService manages operations on databases
type DatabaseService struct {
	Store     databasestore.DatabaseStore
	Factory   DatabaseFactory
	Settings  *settings.Arguments
	Databases map[string]*models.Database
	Logger    *zap.SugaredLogger
}

// NewDatabaseService creates a new DatabaseService
func NewDatabaseService(store databasestore.DatabaseStore, factory DatabaseFactory,
	settings *settings.Arguments,
	logger *zap.SugaredLogger) *DatabaseService {
	service := &DatabaseService{
		Store:     store,
		Factory:   factory,
		Settings:  settings,
		Logger:    logger,
		Databases: make(map[string]*models.Database),
	}

	// Load existing databases
	databases, err := store.LoadAllDatabaseDataFiles(settings.DataDir, logger)
	if err != nil {
		logger.Warnf("Warning: Error loading databases: %v", err)
	} else {
		service.Databases = databases
		logger.Debugf("Database service loaded %d databases", len(databases))

		// Deprecated - I don't think this is necessary anymore
		// Register non-primary databases in the system catalog
		//service.syncDatabaseCatalog(logger)
	}

	return service
}

func (s *DatabaseService) AddDatabase(databaseCommand models.DatabaseCommand) (*models.Database, error) {

	// Check if the database already exists
	if _, err := s.GetDatabaseByName(databaseCommand.DatabaseName); err == nil {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT, fmt.Sprintf("Database '%s' already exists", databaseCommand.DatabaseName), errors.LayerDomain).WithContext("database_name", databaseCommand.DatabaseName)
	}

	db := s.Factory.NewDatabase(databaseCommand.DatabaseName, "")
	db.DataDirectory = s.Settings.DataDir

	// Add to in-memory map
	s.Databases[db.Name] = db

	// Create the database data file
	err := s.Store.CreateDatabaseDataFile(db)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Failed to create database data file for '%s'", db.Name), errors.LayerStorage).WithContext("database_name", db.Name)
	}

	// Register the new database in the Primary database's "Databases" bundle
	// (Skip this step if we're creating the primary database itself)
	if strings.ToLower(databaseCommand.DatabaseName) != "primary" {
		// Remove this as it is already done in the catalog_service.AddDatabaseToCatalog func
		// err = s.registerDatabaseInPrimary(db)
		// if err != nil {
		// 	s.Logger.Warnf("Warning: Failed to register database '%s' in Primary database: %v", db.Name, err)
		// 	// Don't fail the database creation if registration fails
		// }
	}

	return db, nil
}

// registerDatabaseInPrimary adds the new database to the "Databases" bundle in the Primary database
func (s *DatabaseService) registerDatabaseInPrimary(newDB *models.Database) error {
	// Get the Primary database
	primaryDB, err := s.GetDatabaseByName("primary")
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, "Primary database not found", errors.LayerDomain)
	}

	// Find the "Databases" bundle in the Primary database
	databasesBundle, exists := primaryDB.Bundles["Databases"]
	if !exists {
		return errors.New(errors.ERR_NOT_FOUND_BUNDLE, "Databases bundle not found in Primary database", errors.LayerDomain)
	}

	// Create a document representing the new database
	databaseDoc := models.Document{
		DocumentID: fmt.Sprintf("db_%s", newDB.DatabaseID),
		Fields: map[string]models.Field{
			"DocumentID": {Name: "DocumentID", Value: models.NewStringValue(fmt.Sprintf("db_%s", newDB.DatabaseID))},
			"DatabaseID": {Name: "DatabaseID", Value: models.NewStringValue(newDB.DatabaseID)},
			"Name":       {Name: "Name", Value: models.NewStringValue(newDB.Name)},
			"FilePath":   {Name: "FilePath", Value: models.NewStringValue(fmt.Sprintf("%s/%s/%s.db", newDB.DataDirectory, newDB.Name, newDB.Name))},
		},
	}

	// Add the document to the Databases bundle
	if databasesBundle.Documents == nil {
		documentsMap := make(map[string]models.Document)
		databasesBundle.Documents = &documentsMap
	}

	(*databasesBundle.Documents)[databaseDoc.DocumentID] = databaseDoc

	// Update the bundle back in the database
	primaryDB.Bundles["Databases"] = databasesBundle

	//Persiste the updated bundle to disk.
	// First get the serviceManager, then get the BundleService, then call UpdateBundleFile

	// Persist the updated primary database to disk
	err = s.Store.UpdateDatabaseDataFile(primaryDB)
	if err != nil {
		return fmt.Errorf("failed to persist primary database after registering new database: %w", err)
	}

	s.Logger.Infof("Registered database '%s' (ID: %s) in Primary database", newDB.Name, newDB.DatabaseID)
	return nil
}

// syncDatabaseCatalog ensures all loaded databases are registered in the primary.Databases catalog
func (s *DatabaseService) syncDatabaseCatalog(logger *zap.SugaredLogger) {
	// Wait for primary database to be available
	primaryDB, err := s.GetDatabaseByName("primary")
	if err != nil {
		logger.Debugf("Primary database not available yet, skipping catalog sync")
		return
	}

	// Check if Databases bundle exists
	databasesBundle, exists := primaryDB.Bundles["Databases"]
	if !exists {
		logger.Debugf("Databases bundle not found in primary database, skipping catalog sync")
		return
	}

	// Ensure Documents map exists
	if databasesBundle.Documents == nil {
		documentsMap := make(map[string]models.Document)
		databasesBundle.Documents = &documentsMap
	}

	// Get existing database IDs from catalog to avoid duplicates
	existingDatabaseIDs := make(map[string]bool)
	for _, doc := range *databasesBundle.Documents {
		if dbIDField, exists := doc.Fields["DatabaseID"]; exists {
			if dbID, ok := dbIDField.Value.AsString(); ok { // ✅ Use AsString()
				existingDatabaseIDs[dbID] = true
			}
		}
	}

	// Register any non-primary databases that aren't already in the catalog
	for _, db := range s.Databases {
		if strings.ToLower(db.Name) != "primary" && !existingDatabaseIDs[db.DatabaseID] {
			logger.Debugf("Registering database '%s' in system catalog during startup", db.Name)
			err := s.registerDatabaseInPrimary(db)
			if err != nil {
				logger.Warnf("Warning: Failed to register database '%s' in catalog during startup: %v", db.Name, err)
			}
		}
	}
}

func (s *DatabaseService) UpdateDatabase(databaseCommand models.DatabaseCommand) error {
	// Check if database exists
	db, err := s.GetDatabaseByName(databaseCommand.DatabaseName)
	if db == nil {
		return errors.New(errors.ERR_NOT_FOUND_DATABASE, fmt.Sprintf("Database '%s' not found", databaseCommand.DatabaseName), errors.LayerDomain).WithContext("database_name", databaseCommand.DatabaseName)
	}
	if err != nil {
		return errors.ConvertError(err, errors.LayerDomain).WithContext("database_name", databaseCommand.DatabaseName)
	}

	// Update in-memory database (use Name as key for consistency)
	s.Databases[db.Name] = db

	// Update on disk
	err = s.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, fmt.Sprintf("Failed to update database file for '%s'", db.Name), errors.LayerStorage).WithContext("database_name", db.Name)
	}

	log.Printf("Updated database %s (ID: %s)", db.Name, db.DatabaseID)
	return nil
}

func GetDatabase(databases *map[string]*models.Database, databaseName string) (*models.Database, error) {
	// Check if the database exists in the system.
	for dbName, db := range *databases {
		if strings.EqualFold(dbName, databaseName) {
			return db, nil
		}
	}

	return nil, errors.New(errors.ERR_NOT_FOUND_DATABASE, fmt.Sprintf("Database '%s' not found", databaseName), errors.LayerDomain).WithContext("database_name", databaseName)
}

// DeleteDatabase removes a database from the server
func (s *DatabaseService) DeleteDatabase(databaseName string) error {
	// Find database by name
	db, err := s.GetDatabaseByName(databaseName)
	if err != nil {
		return err
	}

	// Remove from memory
	delete(s.Databases, db.DatabaseID)

	// Could add actual file deletion here if needed
	log.Printf("Deleted database %s (ID: %s)", db.Name, db.DatabaseID)
	return nil
}

// RenameDatabase renames a database by updating its name in the system catalog,
// renaming the database directory on disk, and updating all in-memory references.
//
// This operation:
// 1. Validates the new database name
// 2. Checks that the new name isn't already in use
// 3. Renames the database directory on disk (atomic operation)
// 4. Updates the in-memory database map
// 5. Updates the database data file with the new name
//
// Parameters:
//   - oldName: Current database name
//   - newName: Desired new database name
//
// Returns the renamed database and an error if validation fails or if the rename
// operation cannot be completed. If the directory rename succeeds but data file
// update fails, a warning is logged but the operation is considered successful
// (the data file update is just metadata and can be manually corrected).
func (s *DatabaseService) RenameDatabase(oldName, newName string) (*models.Database, error) {
	s.Logger.Infof("Renaming database '%s' to '%s'", oldName, newName)

	// Validate new database name
	if !IsValidDatabaseName(newName) {
		return nil, fmt.Errorf("invalid new database name: %s. Database names must start with a letter, can be alphanumeric, with underscores and hyphens", newName)
	}

	if oldName == newName {
		return nil, fmt.Errorf("new database name is the same as the current name")
	}

	// Get the existing database
	database, err := s.GetDatabaseByName(oldName)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, fmt.Sprintf("Database '%s' not found", oldName), errors.LayerDomain).WithContext("database_name", oldName)
	}

	// Check that new name doesn't already exist (case-insensitive)
	_, err = s.GetDatabaseByName(newName)
	if err == nil {
		return nil, fmt.Errorf("database with name '%s' already exists", newName)
	}

	// Get old and new directory paths
	oldDirPath := filepath.Join(s.Settings.DataDir, oldName)
	newDirPath := filepath.Join(s.Settings.DataDir, newName)

	// Verify old directory exists
	if _, err := os.Stat(oldDirPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database directory does not exist: %s", oldDirPath)
	}

	// Verify new directory doesn't exist
	if _, err := os.Stat(newDirPath); err == nil {
		return nil, fmt.Errorf("directory for new database name already exists: %s", newDirPath)
	}

	// Rename the database directory (atomic operation on most filesystems)
	// This renames all database files and bundles in one operation
	if err := os.Rename(oldDirPath, newDirPath); err != nil {
		return nil, fmt.Errorf("failed to rename database directory: %w", err)
	}

	// Update database metadata in memory
	database.Name = newName
	database.DataDirectory = newDirPath

	// Update in-memory database map (remove old key, add new key)
	delete(s.Databases, oldName)
	s.Databases[newName] = database

	// Update the database data file with new name
	if err := s.Store.UpdateDatabaseDataFile(database); err != nil {
		s.Logger.Warnf("Failed to update database data file after rename: %v", err)
		// Don't fail - directory is already renamed, this is just metadata
	}

	s.Logger.Infof("Successfully renamed database '%s' to '%s'", oldName, newName)
	return database, nil
}

// GetDatabaseByID retrieves a database by its ID
func (s *DatabaseService) GetDatabaseByID(id string) (*models.Database, error) {
	if db, exists := s.Databases[id]; exists {
		return db, nil
	}
	return nil, errors.New(errors.ERR_NOT_FOUND_DATABASE, fmt.Sprintf("Database with ID '%s' not found", id), errors.LayerDomain).WithContext("database_id", id)
}

// GetDatabaseByName retrieves a database by name (case insensitive)
func (s *DatabaseService) GetDatabaseByName(name string) (*models.Database, error) {
	nameLower := strings.ToLower(name)
	for _, db := range s.Databases {
		if strings.ToLower(db.Name) == nameLower {
			return db, nil
		}
	}
	return nil, errors.New(errors.ERR_NOT_FOUND_DATABASE, fmt.Sprintf("Database '%s' not found", name), errors.LayerDomain).WithContext("database_name", name)
}

// ListDatabases returns all databases
func (s *DatabaseService) ListDatabases() []*models.Database {
	databases := make([]*models.Database, 0, len(s.Databases))
	for _, db := range s.Databases {
		databases = append(databases, db)
	}
	return databases
}

// In DatabaseService
func (s *DatabaseService) AddBundleToDatabase(dbName string, bundle models.Bundle, bundleStore bundlestore.BundleStore) error {
	db, err := s.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	db.Bundles[bundle.Name] = bundle

	//This needs to be added to a bundle file
	err = bundleStore.CreateBundleFile(db, &bundle)
	if err != nil {
		return fmt.Errorf("error creating bundle file: %w", err)
	}
	//logger.Infof("Decoded bundle data from file %v", bundle)
	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s_%s.bnd", db.Name, bundle.Name))

	// Write the updated database file
	err = s.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("error updating database file: %w", err)
	}

	return err
}

// GetDatabaseBundles returns a list of bundle names for a given database
// Note: This method needs access to BundleService to get loaded bundles from buffer
// The current implementation returns bundle names from the database's BundleFiles list
func (s *DatabaseService) GetDatabaseBundles(databaseName string) ([]string, error) {
	db, err := s.GetDatabaseByName(databaseName)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found: %w", databaseName, err)
	}

	// For now, return the bundle file names from the database metadata
	// This lists the bundles that should be available for the database
	bundleNames := make([]string, 0, len(db.BundleFiles))
	for _, bundleFileName := range db.BundleFiles {
		// Remove .bnd extension to get bundle name
		bundleName := strings.TrimSuffix(bundleFileName, ".bnd")
		bundleName = strings.TrimPrefix(bundleName, db.Name+"_") // Remove database name prefix if present
		bundleNames = append(bundleNames, bundleName)
	}

	return bundleNames, nil
}

// GetBundleInfo returns detailed information about a specific bundle
func (s *DatabaseService) GetBundleInfo(databaseName, bundleName string) (*models.Bundle, error) {
	db, err := s.GetDatabaseByName(databaseName)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found: %w", databaseName, err)
	}

	bundle, exists := db.Bundles[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle '%s' not found in database '%s'", bundleName, databaseName)
	}

	return &bundle, nil
}
