package engine

import "fmt"

type Bundle struct {
	// BundleID is the unique identifier for the bundle.
	BundleID string

	// Name is the name of the bundle.
	Name string

	// A description of the document structure, similar to a schema/table definition.
	DocumentStructure map[string]Field

	// A list of documents in the bundle, similar to rows in a table.
	Documents map[string]Document

	Relationships map[string]Relationship
	Constraints   map[string]Constraint
}

func (db *Database) GetBundle(bundleID string) (*Bundle, error) {
	bundle, exists := db.Bundles[bundleID]
	if exists {
		return &bundle, nil
	}
	return nil, fmt.Errorf("Bundle with ID %s not found", bundle.BundleID)
}

func (db *Database) AddBundle(bundle Bundle) error {
	if _, exists := db.Bundles[bundle.BundleID]; exists {
		return fmt.Errorf("Bundle with ID %s already exists", bundle.BundleID)
	}
	db.Bundles[bundle.BundleID] = bundle
	return nil
}

func (db *Database) RemoveBundle(bundleID string) error {
	if _, exists := db.Bundles[bundleID]; !exists {
		return fmt.Errorf("Bundle with ID %s not found", bundleID)
	}
	delete(db.Bundles, bundleID)
	return nil
}

func (db *Database) UpdateBundle(bundleID string, bundle Bundle) error {
	if _, exists := db.Bundles[bundleID]; !exists {
		return fmt.Errorf("Bundle with ID %s not found", bundleID)
	}
	db.Bundles[bundleID] = bundle
	return nil
}

func (db *Database) ListBundles() []Bundle {
	var bundleList []Bundle
	for _, bundle := range db.Bundles {
		bundleList = append(bundleList, bundle)
	}
	return bundleList
}

/*

Each bundle is stored in a file
Within the file, we have a list of Documents
Each document is a json object with a list of Fields and their values



*/
