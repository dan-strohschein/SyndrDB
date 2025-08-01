package bundle

import (
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
)

//"syndrdb/src/helpers"
//"syndrdb/src/models"

type BundleFactoryImpl struct {
	// TODO Add configuration fields here if needed
	// For example:
	defaultDataDir string
}

type BundleFactory interface {
	NewBundle(name, description string) *models.Bundle
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
		Documents:         func() *map[string]models.Document { m := make(map[string]models.Document); return &m }(),
		Relationships:     make(map[string]models.Relationship),
		Indexes:           make(map[string]models.IndexReference),
		IndexNames:        []string{},
		Constraints:       make(map[string]models.Constraint),
	}
}

// WithDefaultDataDirectory sets the default data directory
func (f *BundleFactoryImpl) WithDefaultDataDirectory(dataDir string) *BundleFactoryImpl {
	f.defaultDataDir = dataDir
	return f
}
