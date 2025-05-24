package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syndrdb/src/helpers"
	"syscall"

	"golang.org/x/sys/unix"
)

// DatabaseStore defines the interface for database storage operations
type DatabaseStore interface {
	LoadAllDatabaseDataFiles(dataRootDir string) (map[string]*Database, error)

	LoadDatabaseDataFile(dataRootDir, fileName string) (*Database, error)

	LoadDatabaseIntoMemory(database *Database, databaseName string) (*[]byte, *Database, error)

	CreateDatabaseDataFile(database *Database) error

	UpdateDatabaseDataFile(database *Database) error

	// GetByID(id string) (*Database, bool)
	// GetByName(name string) (*Database, bool)
	// Add(db *Database) error
	// Update(db *Database) error
	// Remove(id string) error
	// List() []*Database
}

type DatabaseStorageEngine struct {
	DataDirectory string
}

// DatabaseFactory creates new Database instances
type DatabaseFactory interface {
	NewDatabase(name, description string) *Database
}

func NewDatabaseStore(dataDir string) (*DatabaseStorageEngine, error) {
	// Create a new database store
	store := &DatabaseStorageEngine{
		DataDirectory: dataDir,
	}

	// Ensure the data directory exists
	if err := os.MkdirAll(store.DataDirectory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", store.DataDirectory, err)
	}

	return store, nil
}

// LoadAllDatabaseDataFiles scans the data directory and loads all database metadata files
func (d *DatabaseStorageEngine) LoadAllDatabaseDataFiles(dataRootDir string) (map[string]*Database, error) {
	// Create map to hold databases
	databases := make(map[string]*Database)

	// Ensure the data directory exists
	if err := os.MkdirAll(dataRootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", dataRootDir, err)
	}

	// Get all files in the directory
	files, err := os.ReadDir(dataRootDir)
	if err != nil {
		return nil, fmt.Errorf("error reading data directory %s: %w", dataRootDir, err)
	}

	// Process each file that could be a database metadata file
	for _, file := range files {
		// Skip directories and hidden files
		if file.IsDir() || strings.HasPrefix(file.Name(), ".") {
			continue
		}

		// Check file extension if you have a specific extension for database files
		// if !strings.HasSuffix(file.Name(), ".db") {
		//     continue
		// }

		// Load the database
		db, err := d.LoadDatabaseDataFile(dataRootDir, file.Name())
		if err != nil {
			log.Printf("Warning: Failed to load database from %s: %v", file.Name(), err)
			continue
		}

		// Add to map using the database ID as key
		databases[db.DatabaseID] = db

		// Also create a name-based lookup if needed
		// This is useful for case-insensitive lookups later
		// databases[strings.ToLower(db.Name)] = db

		log.Printf("Loaded database: %s (ID: %s)", db.Name, db.DatabaseID)
	}

	return databases, nil
}

// LoadDatabaseDataFile loads a single database metadata file
func (d *DatabaseStorageEngine) LoadDatabaseDataFile(dataRootDir, fileName string) (*Database, error) {
	fullPath := filepath.Join(dataRootDir, fileName)

	// Open the file
	dbFile, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("error opening database file %s: %w", fileName, err)
	}
	defer dbFile.Close()

	// Get the file size
	stat, err := dbFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}
	fileSize := int(stat.Size())

	// Skip empty files
	if fileSize == 0 {
		return nil, fmt.Errorf("database file is empty")
	}

	// Memory map the file
	data, err := unix.Mmap(int(dbFile.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to memory map file: %w", err)
	}
	defer unix.Munmap(data)

	// Decode BSON data
	dbMap, err := helpers.DecodeBSON(data)
	if err != nil {
		return nil, fmt.Errorf("error decoding database data: %w", err)
	}

	// Convert map to Database object
	db, err := MapToDB(dbMap)
	if err != nil {
		return nil, fmt.Errorf("error converting map to Database: %w", err)
	}

	// Set the data directory if it's not already set
	if db.DataDirectory == "" {
		db.DataDirectory = dataRootDir
	}

	return db, nil
}

func (d *DatabaseStorageEngine) LoadDatabaseIntoMemory(database *Database, databaseName string) (*[]byte, *Database, error) {
	dbFile, err := helpers.OpenDataFile(database.DataDirectory, databaseName)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening database file %s: %w", databaseName, err)
	}
	defer dbFile.Close()

	// Get the file size
	stat, err := dbFile.Stat()
	if err != nil {
		log.Printf("Failed to get file stats: %v\n", err)
		return nil, nil, err
	}
	fileSize := int(stat.Size())

	// Memory map the file
	data, err := unix.Mmap(int(dbFile.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		fmt.Printf("Failed to memory map file: %v\n", err)
		return nil, nil, err
	}
	defer unix.Munmap(data)

	dbMetadata, err := helpers.DecodeBSON(data)
	if err != nil {
		return nil, nil, fmt.Errorf("error decoding bundle data: %w", err)
	}

	// Assert that the decoded data is of type Bundle
	db, ok := dbMetadata.(Database)
	if !ok {
		return nil, nil, fmt.Errorf("decoded data is not of type Bundle")
	}

	return &data, &db, nil
}

func (d *DatabaseStorageEngine) CreateDatabaseDataFile(database *Database) error {
	// Create a new data file
	filePath := filepath.Join(database.DataDirectory, database.Name)

	// Check if the file already exists
	if helpers.FileExists(filePath) {
		return fmt.Errorf("Database %s already exists", database.Name)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating database file %s: %w", database.Name, err)
	}

	// Ensure the file is closed when the function exits
	defer file.Close()

	//convert the db to a map
	convertedDatabase := DBToMap(database)

	// Encode the db to BSON
	encodedDB, err := helpers.EncodeBSON(convertedDatabase)
	if err != nil {
		return fmt.Errorf("error encoding bundle data: %w", err)
	}

	// Write the encoded db to the file
	fileLen, err := file.Write(encodedDB)
	if err != nil {
		return fmt.Errorf("error writing to bundle data file %s: %w", database.Name, err)
	}

	if fileLen != len(encodedDB) {
		return fmt.Errorf("error writing to bundle data file %s: wrote %d bytes, expected %d", database.Name, fileLen, len(encodedDB))
	}

	return nil
}

func (d *DatabaseStorageEngine) UpdateDatabaseDataFile(database *Database) error {
	// Create a new data file
	filePath := filepath.Join(database.DataDirectory, database.Name)

	// Check if the file already exists
	if !helpers.FileExists(filePath) {
		return fmt.Errorf("Database %s does not exist", database.Name)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error opening database file %s: %w", database.Name, err)
	}
	defer file.Close()

	//convert the db to a map
	convertedDatabase := DBToMap(database)

	// Encode the db to BSON
	encodedDB, err := helpers.EncodeBSON(convertedDatabase)
	if err != nil {
		return fmt.Errorf("error encoding bundle data: %w", err)
	}

	// Write the encoded db to the file
	fileLen, err := file.Write(encodedDB)
	if err != nil {
		return fmt.Errorf("error writing to bundle data file %s: %w", database.Name, err)
	}

	if fileLen != len(encodedDB) {
		return fmt.Errorf("error writing to bundle data file %s: wrote %d bytes, expected %d", database.Name, fileLen, len(encodedDB))
	}

	return nil
}

func DBToMap(database *Database) map[string]interface{} {
	// Convert the database object to a map
	return map[string]interface{}{
		"DatabaseID":    database.DatabaseID,
		"Name":          database.Name,
		"Description":   database.Description,
		"BundleFiles":   database.BundleFiles,
		"Bundles":       database.Bundles,
		"DataDirectory": database.DataDirectory,
	}
}

// MapToDB converts a map to a Database object
func MapToDB(data interface{}) (*Database, error) {
	// Check if data is a map
	dbMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("decoded data is not a map")
	}

	// Create a new database
	db := &Database{
		Bundles:     make(map[string]Bundle),
		BundleFiles: []string{},
	}

	// Extract fields from the map
	if id, ok := dbMap["DatabaseID"].(string); ok {
		db.DatabaseID = id
	} else {
		return nil, fmt.Errorf("missing or invalid DatabaseID field")
	}

	if name, ok := dbMap["Name"].(string); ok {
		db.Name = name
	} else {
		return nil, fmt.Errorf("missing or invalid Name field")
	}

	// Optional fields
	if desc, ok := dbMap["Description"].(string); ok {
		db.Description = desc
	}

	if dir, ok := dbMap["DataDirectory"].(string); ok {
		db.DataDirectory = dir
	}

	// Extract bundle files map
	if bundleFilesInterface, ok := dbMap["BundleFiles"].([]interface{}); ok {
		for _, pathInterface := range bundleFilesInterface {
			if pathStr, ok := pathInterface.(string); ok {
				db.BundleFiles = append(db.BundleFiles, pathStr)
			}
		}
	}

	// For each bundle file, we might want to load the actual bundle data
	// but that's better done on demand to avoid loading everything at startup

	return db, nil
}
