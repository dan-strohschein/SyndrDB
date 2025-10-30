package server

import (
	"fmt"

	"syndrdb/src/internal/domain/index"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// CreateHashIndex handles the CREATE HASH INDEX command
func CreateHashIndex(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error, bool) {
	hashIndexCommand, err := index.ParseCreateHashIndexCommand(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing hash index command: %v", err), false
	}
	logger.Infof("Parsed Hash index command: %+v", hashIndexCommand)

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, hashIndexCommand.BundleName)
	if err == nil {
		return nil, fmt.Errorf("bundle '%s' cannot be found", bundle.Name), false
	}

	err = serviceManager.BundleService.AddIndexToBundle(database, bundle, hashIndexCommand)
	if err != nil {
		return nil, fmt.Errorf("error adding hash index to bundle '%s': %v", hashIndexCommand.BundleName, err), false
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Index created successfully",
	}
	return cmdResponse, nil, true
}

// CreateBTreeIndex handles the CREATE BTREE INDEX command
func CreateBTreeIndex(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error) {
	btreeIndexCommand, err := index.ParseCreateBTreeIndexCommand(command, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing B-Tree index command: %v", err)
	}
	logger.Infof("Parsed B-Tree index command: %+v", btreeIndexCommand)

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, btreeIndexCommand.BundleName)
	if err == nil && bundle == nil {
		return nil, fmt.Errorf("bundle '%s' cannot be found", btreeIndexCommand.BundleName)
	}

	// TODO Validate the index name
	err = serviceManager.BundleService.AddIndexToBundle(database, bundle, btreeIndexCommand)
	if err != nil {
		return nil, fmt.Errorf("error adding B-Tree index to bundle '%s': %v", btreeIndexCommand.BundleName, err)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Index created successfully",
	}
	return cmdResponse, nil
}
