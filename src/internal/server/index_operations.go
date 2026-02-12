package server

import (
	"syndrdb/src/internal/domain/index"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

// CreateHashIndex handles the CREATE HASH INDEX command
func CreateHashIndex(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error, bool) {
	hashIndexCommand, err := index.ParseCreateHashIndexCommand(command, logger)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command), false
	}
	logger.Infof("Parsed Hash index command: %+v", hashIndexCommand)

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, hashIndexCommand.BundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", hashIndexCommand.BundleName), false
	}

	err = serviceManager.BundleService.AddIndexToBundle(database, bundle, hashIndexCommand)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", hashIndexCommand.BundleName).WithContext("index_name", hashIndexCommand.IndexName), false
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
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
	}
	logger.Infof("Parsed B-Tree index command: %+v", btreeIndexCommand)

	// Get the bundle by name
	bundle, err := serviceManager.BundleService.GetBundleByName(database, btreeIndexCommand.BundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", btreeIndexCommand.BundleName)
	}

	// TODO Validate the index name
	err = serviceManager.BundleService.AddIndexToBundle(database, bundle, btreeIndexCommand)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", btreeIndexCommand.BundleName).WithContext("index_name", btreeIndexCommand.IndexName)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "Index created successfully",
	}
	return cmdResponse, nil
}

// CreateBRINIndex handles the CREATE BRIN INDEX command
func CreateBRINIndex(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error) {
	brinIndexCommand, err := index.ParseCreateBRINIndexCommand(command, logger)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
	}
	logger.Infof("Parsed BRIN index command: %+v", brinIndexCommand)

	bundle, err := serviceManager.BundleService.GetBundleByName(database, brinIndexCommand.BundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", brinIndexCommand.BundleName)
	}

	err = serviceManager.BundleService.AddIndexToBundle(database, bundle, brinIndexCommand)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", brinIndexCommand.BundleName).WithContext("index_name", brinIndexCommand.IndexName)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      "BRIN index created successfully",
	}
	return cmdResponse, nil
}
