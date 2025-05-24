package engine

import "syndrdb/src/helpers"

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

func (f *BundleFactoryImpl) NewBundle(name string, description string) *Bundle {
	return &Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              name,
		DocumentStructure: make(map[string]Field),
		Documents:         make(map[string]Document),
		Relationships:     make(map[string]Relationship),
		Constraints:       make(map[string]Constraint),
	}
}

// WithDefaultDataDirectory sets the default data directory
func (f *BundleFactoryImpl) WithDefaultDataDirectory(dataDir string) *BundleFactoryImpl {
	f.defaultDataDir = dataDir
	return f
}
