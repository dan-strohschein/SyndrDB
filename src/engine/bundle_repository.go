package engine

import (
	"fmt"
	"os"
)

// TODO this whole file needs to be redone and the functions placed in the right areas/files

func (db *Database) GetBundle(bundleName string) (*Bundle, error) {
	bundle, exists := db.Bundles[bundleName]
	if exists {
		return &bundle, nil
	}
	return nil, fmt.Errorf("Bundle %s was not found", bundle.Name)
}

func (db *Database) AddBundle(bundle Bundle, dbStore DatabaseStore) error {
	if _, exists := db.Bundles[bundle.Name]; exists {
		return fmt.Errorf("Bundle %s already exists", bundle.Name)
	}
	db.Bundles[bundle.Name] = bundle

	//This needs to be added to a bundle file
	err := CreateBundleFile(db, &bundle)
	if err != nil {
		return fmt.Errorf("error creating bundle file: %w", err)
	}

	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, bundle.Name)

	// Write the updated database file
	err = dbStore.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("error updating database file: %w", err)
	}

	return nil
}

func (db *Database) RemoveBundle(bundleName string, dbStore DatabaseStore) error {
	if _, exists := db.Bundles[bundleName]; !exists {
		return fmt.Errorf("Bundle with ID %s not found to remove", bundleName)
	}
	delete(db.Bundles, bundleName)

	// Remove the bundle file from the file system
	bundleFilePath := fmt.Sprintf("%s/%s.bundle", db.DataDirectory, bundleName)
	if err := os.Remove(bundleFilePath); err != nil {
		return fmt.Errorf("error removing bundle file %s: %w", bundleFilePath, err)
	}

	// Remove the bundle file name from the database
	for i, fileName := range db.BundleFiles {
		if fileName == bundleName {
			db.BundleFiles = append(db.BundleFiles[:i], db.BundleFiles[i+1:]...)
			break
		}
	}

	// Write the updated database file
	err := dbStore.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("error updating database file: %w", err)
	}
	// Remove the bundle from the in-memory map
	delete(db.Bundles, bundleName)

	return nil
}

func (db *Database) UpdateBundle(bundleName string, bundle Bundle) error {
	if _, exists := db.Bundles[bundleName]; !exists {
		return fmt.Errorf("Bundle %s not found to update", bundleName)
	}

	// Update the bundle file
	bundleFilePath := fmt.Sprintf("%s/%s.bundle", db.DataDirectory, bundleName)
	if err := os.Remove(bundleFilePath); err != nil {
		return fmt.Errorf("error removing old bundle file %s: %w", bundleFilePath, err)
	}

	err := UpdateBundleFile(db, &bundle)
	if err != nil {
		return fmt.Errorf("error updating bundle file %s: %w", bundleFilePath, err)
	}

	db.Bundles[bundleName] = bundle
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
