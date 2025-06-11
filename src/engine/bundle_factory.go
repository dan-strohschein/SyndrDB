package engine

import (
	"syndrdb/src/helpers"
	"syndrdb/src/models"
)

type BundleFactoryImpl struct {
	// TODO Add configuration fields here if needed
	// For example:
	defaultDataDir string
}

func NewBundleFactory() BundleFactory {
	return &BundleFactoryImpl{
		// Initialize with default values if needed
	}
}

func (f *BundleFactoryImpl) NewBundle(name string, description string) *models.Bundle {
	return &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              name,
		DocumentStructure: models.DocumentStructure{FieldDefinitions: make(map[string]models.FieldDefinition)},
		Documents:         make(map[string]models.Document),
		Relationships:     make(map[string]models.Relationship),
		Constraints:       make(map[string]models.Constraint),
	}
}

// WithDefaultDataDirectory sets the default data directory
func (f *BundleFactoryImpl) WithDefaultDataDirectory(dataDir string) *BundleFactoryImpl {
	f.defaultDataDir = dataDir
	return f
}
