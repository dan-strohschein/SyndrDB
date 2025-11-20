package server

import (
	"fmt"
	"path/filepath"

	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

// AddRelationshipToBundle adds a relationship to a bundle
func AddRelationshipToBundle(serviceManager ServiceManager, database *models.Database, bundleName string, relationshipCommand *models.RelationshipCommand) (*CommandResponse, error) {
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleName, err)
	}

	err = serviceManager.BundleService.AddRelationshipToBundle(bundle, relationshipCommand)
	if err != nil {
		return nil, fmt.Errorf("error adding relationship to bundle '%s': %v", bundleName, err)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Relationship successfully added to bundle '" + bundleName + "'.",
	}
	return cmdResponse, nil
}

// CreateBundleCommand handles the CREATE BUNDLE command with WAL logging
func CreateBundleCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, result string) (*CommandResponse, error) {
	//args := settings.GetSettings()
	// Use new parser if feature flag is enabled, fallback to legacy on error
	bundleCmd, err := parseCreateBundle(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing bundle command: %v", err)
	}

	//Check if the bundle already exists
	databasePath := helpers.GetDatabaseFolderPath(database.Name)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundleCmd.BundleName))
	existingBundle := helpers.FileExists(filePath, *logger)
	if existingBundle {
		return nil, fmt.Errorf("bundle '%s' already exists", bundleCmd.BundleName)
	}

	// Validate the bundle name with a regex
	if !bndle.IsValidBundleName(bundleCmd.BundleName) {
		return nil, fmt.Errorf("invalid bundle name: %s. Bundle names must start with a letter, can be alphanumeric, with underscores and hyphens", bundleCmd.BundleName)
	}

	// Get database object by name
	// database, err1 := serviceManager.DatabaseService.GetDatabaseByName(database.Name)
	// if err1 != nil {
	// 	return nil, fmt.Errorf("error retrieving database '%s': %v", database.Name, err)
	// }
	logger.Infof("Creating bundle '%s' in database '%s'", bundleCmd.BundleName, database.Name)

	// Execute with WAL logging if available
	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Log the bundle creation before execution
			err := serviceManager.WALManager.LogBundleCreate(txID, bundleCmd.BundleName, bundleCmd)
			if err != nil {
				return fmt.Errorf("failed to log bundle create: %w", err)
			}

			// Add the bundle to the database
			bundle, err := serviceManager.BundleService.AddBundle(serviceManager.DatabaseService, database, bundleCmd)
			if err != nil {
				return fmt.Errorf("error creating bundle: %v", err)
			}

			// CRITICAL FIX: Check for errors when adding bundle to catalog
			// Without this, bundles may be created but not registered in the system catalog
			err = serviceManager.InternalCatalogService.RegisterBundleInCatalog(bundle)
			if err != nil {
				// Bundle was created but catalog registration failed
				logger.Errorf("Bundle '%s' created but failed to register in catalog: %v", bundle.Name, err)
				return fmt.Errorf("bundle created but catalog registration failed: %v", err)
			}

			return err
		})
	} else {
		// Fallback to direct execution if WAL is not available
		logger.Warn("WAL Manager not available, executing without transaction logging")
		bundle, err := serviceManager.BundleService.AddBundle(serviceManager.DatabaseService, database, bundleCmd)

		if err != nil {
			return nil, fmt.Errorf("error creating bundle: %v", err)
		}

		// CRITICAL FIX: Check for errors when adding bundle to catalog
		// Without this, bundles may be created but not registered in the system catalog
		err = serviceManager.InternalCatalogService.RegisterBundleInCatalog(bundle)
		if err != nil {
			// Bundle was created but catalog registration failed
			logger.Errorf("Bundle '%s' created but failed to register in catalog: %v", bundle.Name, err)
			return nil, fmt.Errorf("bundle created but catalog registration failed: %v", err)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("error creating bundle: %v", err)
	}

	// Return the response
	result = fmt.Sprintf("Bundle '%s' created successfully in database '%s'.", bundleCmd.BundleName, database.Name)
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	return cmdResponse, nil
}

func DeleteBundleCommand(bundleCmd *models.BundleCommand, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error) {

	// We need to check some things:
	// 1. If the force command was not present
	// 2. If there are documents in the bundle
	// 3. if there are relationships associated with the bundle
	bundleMetadata, err := serviceManager.BundleService.GetBundleMetadata(database, bundleCmd.BundleName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bundle '%s': %v", bundleCmd.BundleName, err)
	}

	hasDocuments := bundleMetadata.TotalDocuments > 0
	hasRelationships := len(bundleMetadata.Relationships) > 0

	if (hasDocuments || hasRelationships) && !bundleCmd.HasForceSwitch {
		return nil, fmt.Errorf("cannot drop bundle '%s' because it contains documents or has relationships. Use FORCE to override", bundleCmd.BundleName)
	}

	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Log the bundle creation before execution
			err := serviceManager.WALManager.LogBundleDelete(txID, bundleCmd.BundleName, bundleCmd)
			if err != nil {
				return fmt.Errorf("failed to log bundle create: %w", err)
			}

			//Delete the bundle from the database
			err = serviceManager.BundleService.DeleteBundle(database, bundleCmd)
			if err != nil {
				return fmt.Errorf("error deleting bundle: %v", err)
			}

			// CRITICAL FIX: Check for errors when adding bundle to catalog
			// Without this, bundles may be deleted but not removed from the system catalog
			err = serviceManager.InternalCatalogService.UnRegisterBundleInCatalog(bundleMetadata.BundleID,
				bundleCmd.BundleName, database.DatabaseID, database.Name)
			if err != nil {
				// Bundle was created but catalog registration failed
				logger.Errorf("Bundle '%s' created but failed to register in catalog: %v", bundleMetadata.Name, err)
				return fmt.Errorf("bundle created but catalog registration failed: %v", err)
			}

			return err
		})
	} else {
		// Fallback to direct execution if WAL is not available
		logger.Warn("WAL Manager not available, executing without transaction logging")

		err = serviceManager.BundleService.DeleteBundle(database, bundleCmd)
		if err != nil {
			return nil, fmt.Errorf("error deleting bundle: %v", err)
		}

		// CRITICAL FIX: Check for errors when adding bundle to catalog
		// Without this, bundles may be deleted but not removed from the system catalog
		err = serviceManager.InternalCatalogService.UnRegisterBundleInCatalog(bundleMetadata.BundleID,
			bundleCmd.BundleName, database.DatabaseID, database.Name)
		if err != nil {
			// Bundle was created but catalog registration failed
			logger.Errorf("Bundle '%s' created but failed to register in catalog: %v", bundleMetadata.Name, err)
			return nil, fmt.Errorf("bundle created but catalog registration failed: %v", err)
		}
	}

	result := fmt.Sprintf("Bundle '%s' deleted successfully from database '%s'.", bundleCmd.BundleName, database.Name)
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	return cmdResponse, nil
}
