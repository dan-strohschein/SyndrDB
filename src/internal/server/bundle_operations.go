package server

import (
	"fmt"
	"path/filepath"

	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

// AddRelationshipToBundle adds a relationship to a bundle
func AddRelationshipToBundle(serviceManager ServiceManager, database *models.Database, bundleName string, relationshipCommand *models.RelationshipCommand) (*CommandResponse, error) {
	bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
	}

	err = serviceManager.BundleService.AddRelationshipToBundle(bundle, relationshipCommand)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
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
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("command", command)
	}

	//Check if the bundle already exists
	databasePath := helpers.GetDatabaseFolderPath(database.Name)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundleCmd.BundleName))
	existingBundle := helpers.FileExists(filePath, *logger)
	if existingBundle {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
			fmt.Sprintf("bundle '%s' already exists", bundleCmd.BundleName),
			errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
	}

	// Validate the bundle name with a regex
	if !bndle.IsValidBundleName(bundleCmd.BundleName) {
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("invalid bundle name: %s. Bundle names must start with a letter, can be alphanumeric, with underscores and hyphens", bundleCmd.BundleName),
			errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
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
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
					"failed to log bundle create", errors.LayerWAL).WithContext("bundle", bundleCmd.BundleName)
			}

			// Add the bundle to the database
			bundle, err := serviceManager.BundleService.AddBundle(serviceManager.DatabaseService, database, bundleCmd)
			if err != nil {
				return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
			}

			// CRITICAL FIX: Check for errors when adding bundle to catalog
			// Without this, bundles may be created but not registered in the system catalog
			err = serviceManager.InternalCatalogService.RegisterBundleInCatalog(bundle)
			if err != nil {
				// Bundle was created but catalog registration failed
				logger.Errorf("Bundle '%s' created but failed to register in catalog: %v", bundle.Name, err)
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL,
					"bundle created but catalog registration failed", errors.LayerCommand).WithContext("bundle", bundle.Name)
			}

			return err
		})
	} else {
		// Fallback to direct execution if WAL is not available
		logger.Warn("WAL Manager not available, executing without transaction logging")
		bundle, err := serviceManager.BundleService.AddBundle(serviceManager.DatabaseService, database, bundleCmd)

		if err != nil {
			return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
		}

		// CRITICAL FIX: Check for errors when adding bundle to catalog
		// Without this, bundles may be created but not registered in the system catalog
		err = serviceManager.InternalCatalogService.RegisterBundleInCatalog(bundle)
		if err != nil {
			// Bundle was created but catalog registration failed
			logger.Errorf("Bundle '%s' created but failed to register in catalog: %v", bundle.Name, err)
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
				"bundle created but catalog registration failed", errors.LayerCommand).WithContext("bundle", bundle.Name)
		}
	}

	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
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
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
	}

	hasDocuments := bundleMetadata.TotalDocuments > 0
	hasRelationships := len(bundleMetadata.Relationships) > 0

	if hasRelationships  {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
			fmt.Sprintf("cannot drop bundle '%s' because it has relationships", bundleCmd.BundleName),
			errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
	}


	if hasDocuments && !bundleCmd.HasForceSwitch {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
			fmt.Sprintf("cannot drop bundle '%s' because it contains documents. Use WITH FORCE to override", bundleCmd.BundleName),
			errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
	}

	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Log the bundle creation before execution
			err := serviceManager.WALManager.LogBundleDelete(txID, bundleCmd.BundleName, bundleCmd)
			if err != nil {
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
					"failed to log bundle delete", errors.LayerWAL).WithContext("bundle", bundleCmd.BundleName)
			}

			//Delete the bundle from the database
			err = serviceManager.BundleService.DeleteBundle(database, bundleCmd)
			if err != nil {
				return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
			}

			// CRITICAL FIX: Check for errors when adding bundle to catalog
			// Without this, bundles may be deleted but not removed from the system catalog
			err = serviceManager.InternalCatalogService.UnRegisterBundleInCatalog(bundleMetadata.BundleID,
				bundleCmd.BundleName, database.DatabaseID, database.Name)
			if err != nil {
				// Bundle was deleted but catalog unregistration failed
				logger.Errorf("Bundle '%s' deleted but failed to unregister from catalog: %v", bundleMetadata.Name, err)
				return errors.WrapWithMessage(err, errors.ERR_INTERNAL,
					"bundle deleted but catalog unregistration failed", errors.LayerCommand).WithContext("bundle", bundleMetadata.Name)
			}

			return err
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Fallback to direct execution if WAL is not available
		logger.Warn("WAL Manager not available, executing without transaction logging")

		err = serviceManager.BundleService.DeleteBundle(database, bundleCmd)
		if err != nil {
			return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleCmd.BundleName)
		}

		// CRITICAL FIX: Check for errors when adding bundle to catalog
		// Without this, bundles may be deleted but not removed from the system catalog
		err = serviceManager.InternalCatalogService.UnRegisterBundleInCatalog(bundleMetadata.BundleID,
			bundleCmd.BundleName, database.DatabaseID, database.Name)
		if err != nil {
			// Bundle was deleted but catalog unregistration failed
			logger.Errorf("Bundle '%s' deleted but failed to unregister from catalog: %v", bundleMetadata.Name, err)
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
				"bundle deleted but catalog unregistration failed", errors.LayerCommand).WithContext("bundle", bundleMetadata.Name)
		}
	}

	result := fmt.Sprintf("Bundle '%s' deleted successfully from database '%s'.", bundleCmd.BundleName, database.Name)
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	return cmdResponse, nil
}
