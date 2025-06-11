package directors

import (
	"fmt"
	"log"
	"strings"
	"syndrdb/src/engine"
	"syndrdb/src/models"

	"syndrdb/src/settings"

	"go.uber.org/zap"
)

// func (s *Server) LoadAllDatabases() error {
// 	// Load all databases from the data directory
// 	args := settings.GetSettings()
// 	data, database, err := engine.LoadDBMetadataIntoMemory(args.DataDir)
// 	if err != nil {
// 		return fmt.Errorf("failed to load databases: %w", err)
// 	}

// 	// Add each database to the server
// 	for _, db := range databases {
// 		s.Databases[db.Name] = db
// 	}

//		return nil
//	}

// DatabaseService manages operations on databases
type DatabaseService struct {
	store     engine.DatabaseStore
	factory   engine.DatabaseFactory
	settings  *settings.Arguments
	databases map[string]*models.Database
	logger    *zap.SugaredLogger
}

// NewDatabaseService creates a new DatabaseService
func NewDatabaseService(store engine.DatabaseStore, factory engine.DatabaseFactory,
	settings *settings.Arguments,
	logger *zap.SugaredLogger) *DatabaseService {
	service := &DatabaseService{
		store:     store,
		factory:   factory,
		settings:  settings,
		logger:    logger,
		databases: make(map[string]*models.Database),
	}

	// Load existing databases
	databases, err := store.LoadAllDatabaseDataFiles(settings.DataDir)
	if err != nil {
		log.Printf("Warning: Error loading databases: %v", err)
	} else {
		service.databases = databases
		log.Printf("Database service loaded %d databases", len(databases))
	}

	return service
}

func (s *DatabaseService) AddDatabase(databaseCommand engine.DatabaseCommand) error {

	// Check if the database already exists
	if _, err := s.GetDatabaseByName(databaseCommand.DatabaseName); err == nil {
		return fmt.Errorf("database '%s' already exists", databaseCommand.DatabaseName)
	}

	db := s.factory.NewDatabase(databaseCommand.DatabaseName, "")
	db.DataDirectory = s.settings.DataDir

	// Add to in-memory map
	s.databases[db.DatabaseID] = db

	return s.store.CreateDatabaseDataFile(db)

}

func (s *DatabaseService) UpdateDatabase(databaseCommand engine.DatabaseCommand) error {
	// Check if database exists
	db, err := s.GetDatabaseByName(databaseCommand.DatabaseName)
	if db == nil {
		return fmt.Errorf("database '%s' not found", databaseCommand.DatabaseName)
	}
	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}

	// Update in-memory database
	s.databases[db.DatabaseID] = db

	// Update on disk
	err = s.store.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("failed to update database file: %w", err)
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

	return nil, fmt.Errorf("database '%s' not found", databaseName)
}

// DeleteDatabase removes a database from the server
func (s *DatabaseService) DeleteDatabase(databaseName string) error {
	// Find database by name
	db, err := s.GetDatabaseByName(databaseName)
	if err != nil {
		return err
	}

	// Remove from memory
	delete(s.databases, db.DatabaseID)

	// Could add actual file deletion here if needed
	log.Printf("Deleted database %s (ID: %s)", db.Name, db.DatabaseID)
	return nil
}

// GetDatabaseByID retrieves a database by its ID
func (s *DatabaseService) GetDatabaseByID(id string) (*models.Database, error) {
	if db, exists := s.databases[id]; exists {
		return db, nil
	}
	return nil, fmt.Errorf("database with ID %s not found", id)
}

// GetDatabaseByName retrieves a database by name (case insensitive)
func (s *DatabaseService) GetDatabaseByName(name string) (*models.Database, error) {
	nameLower := strings.ToLower(name)
	for _, db := range s.databases {
		if strings.ToLower(db.Name) == nameLower {
			return db, nil
		}
	}
	return nil, fmt.Errorf("database '%s' not found", name)
}

// ListDatabases returns all databases
func (s *DatabaseService) ListDatabases() []*models.Database {
	databases := make([]*models.Database, 0, len(s.databases))
	for _, db := range s.databases {
		databases = append(databases, db)
	}
	return databases
}

// In DatabaseService
func (s *DatabaseService) AddBundleToDatabase(dbName string, bundle models.Bundle, bundleStore engine.BundleStore) error {
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
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s.bnd", bundle.Name))

	// Write the updated database file
	err = s.store.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("error updating database file: %w", err)
	}

	return err
}
