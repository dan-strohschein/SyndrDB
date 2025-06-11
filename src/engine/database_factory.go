package engine

import (
	"syndrdb/src/helpers"
	"syndrdb/src/models"
)

// DatabaseFactoryImpl is a concrete implementation of DatabaseFactory
type DatabaseFactoryImpl struct {
	// You can add configuration fields here if needed
	// For example:
	defaultDataDir string
}

// NewDatabaseFactory creates a new instance of DatabaseFactory
func NewDatabaseFactory() DatabaseFactory {
	return &DatabaseFactoryImpl{
		// Initialize with default values if needed
	}
}

// NewDatabase creates a new Database instance
func (f *DatabaseFactoryImpl) NewDatabase(name, description string) *models.Database {
	return &models.Database{
		DatabaseID:    helpers.GenerateUUID(),
		Name:          name,
		Description:   description,
		DataDirectory: f.defaultDataDir, // This will be populated later
		Bundles:       make(map[string]models.Bundle),
		BundleFiles:   []string{},
	}
}

// WithDefaultDataDirectory sets the default data directory
func (f *DatabaseFactoryImpl) WithDefaultDataDirectory(dataDir string) *DatabaseFactoryImpl {
	f.defaultDataDir = dataDir
	return f
}
