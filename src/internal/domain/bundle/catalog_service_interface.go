// catalog_service_interface.go
//
// This file defines the interface for the CatalogService, allowing the BundleService
// to update the system catalog without creating a circular dependency. The interface
// follows the Dependency Inversion Principle, where high-level modules (BundleService)
// depend on abstractions (interfaces) rather than concrete implementations.
//
// This enables loose coupling and makes testing easier through mock implementations.

package bundle

import "syndrdb/src/internal/domain/models"

// CatalogServiceInterface defines the contract for catalog operations that
// the BundleService needs to update the system catalog (primary.Databases and
// primary.Bundles). This interface is implemented by defaultdb.CatalogService.
type CatalogServiceInterface interface {
	// UpdateBundleNameInCatalog updates the Name and FilePath fields in the
	// primary.Bundles catalog when a bundle is renamed.
	//
	// Parameters:
	//   bundleID: The unique identifier of the bundle
	//   databaseName: The name of the database containing the bundle
	//   oldName: The previous bundle name (for logging/validation)
	//   newName: The new bundle name
	//
	// Returns an error if the catalog update fails
	UpdateBundleNameInCatalog(bundleID, databaseName, oldName, newName string) error

	// RegisterBundleInCatalog adds a new bundle entry to the primary.Bundles catalog
	// This is called when a new bundle is created
	RegisterBundleInCatalog(bundle *models.Bundle) error

	// UnRegisterBundleInCatalog removes a bundle entry from the primary.Bundles catalog
	// This is called when a bundle is deleted
	UnRegisterBundleInCatalog(bundleID, bundleName, databaseID, databaseName string) error

	// AddDatabaseToCatalog registers a database in the primary.Databases catalog
	AddDatabaseToCatalog(db *models.Database) error
}
