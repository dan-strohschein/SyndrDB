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
		Description:       description,
		DocumentStructure: models.DocumentStructure{FieldDefinitions: make(map[string]models.FieldDefinition)},
		// WRITE-THROUGH CACHE: Documents memtable removed - all storage goes through page cache
		Relationships: make(map[string]models.Relationship),
		Indexes:       make(map[string]models.IndexReference),
		IndexNames:    []string{},
		Constraints:   make(map[string]models.Constraint),
		// Initialize pagination metadata to prevent divide by zero
		// Use consistent PageSize with BundleService default (4096)
		TotalDocuments: 0,
		PageSize:       4096, // Match BundleService defaultPageSize (power of 2)
		PageCount:      0,
		// Initialize the sorted index for proper pageID alignment
		SortedIndex: models.NewShardedSortedIndex(),
	}
}

// WithDefaultDataDirectory sets the default data directory
func (f *BundleFactoryImpl) WithDefaultDataDirectory(dataDir string) *BundleFactoryImpl {
	f.defaultDataDir = dataDir
	return f
}
