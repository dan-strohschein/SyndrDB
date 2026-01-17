package server

import (
	"fmt"
	"syndrdb/src/internal/domain/migration"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

/*
migration_service_adapter.go

This adapter wraps the migration.MigrationService to implement the MigrationServiceInterface
used by command handlers. It converts between interface{} types (used to avoid circular dependencies)
and concrete migration types.

Design Principles:
  - Single Responsibility: Only responsible for type marshaling between interface{} and concrete types
  - Open/Closed: Extends MigrationService without modifying its core logic

TODO: I will add request/response logging for audit trail and compliance
TODO: I will implement caching for frequently accessed migration metadata
TODO: I will add metrics tracking for migration command execution times
*/

// MigrationServiceAdapter wraps migration.MigrationService to match MigrationServiceInterface
type MigrationServiceAdapter struct {
	service *migration.MigrationService
	logger  *zap.SugaredLogger
}

// NewMigrationServiceAdapter creates a new adapter instance
func NewMigrationServiceAdapter(service *migration.MigrationService, logger *zap.SugaredLogger) *MigrationServiceAdapter {
	return &MigrationServiceAdapter{
		service: service,
		logger:  logger,
	}
}

	// CreateMigration adapts the interface{} parameter to MigrationCommand
func (a *MigrationServiceAdapter) CreateMigration(cmd interface{}) (interface{}, error) {
	migCmd, ok := cmd.(migration.MigrationCommand)
	if !ok {
		return nil, errors.New(errors.ERR_VALIDATION_TYPE,
			fmt.Sprintf("invalid migration command type: expected MigrationCommand, got %T", cmd),
			errors.LayerCommand).WithContext("command_type", fmt.Sprintf("%T", cmd))
	}

	migration, err := a.service.CreateMigration(migCmd)
	if err != nil {
		a.logger.Errorf("Failed to create migration: %v", err)
		return nil, err
	}

	a.logger.Infof("Created migration version %d for database '%s'", migration.Version, migration.DatabaseName)
	return migration, nil
}

// ApplyMigration delegates to the underlying service
func (a *MigrationServiceAdapter) ApplyMigration(databaseName string, version int, force bool) error {
	a.logger.Infof("Applying migration version %d to database '%s' (force=%v)", version, databaseName, force)

	err := a.service.ApplyMigration(databaseName, version, force)
	if err != nil {
		a.logger.Errorf("Failed to apply migration: %v", err)
		return err
	}

	a.logger.Infof("Successfully applied migration version %d to database '%s'", version, databaseName)
	return nil
}

// RollbackToVersion delegates to the underlying service
func (a *MigrationServiceAdapter) RollbackToVersion(databaseName string, targetVersion int) error {
	a.logger.Infof("Rolling back database '%s' to version %d", databaseName, targetVersion)

	err := a.service.RollbackToVersion(databaseName, targetVersion)
	if err != nil {
		a.logger.Errorf("Failed to rollback: %v", err)
		return err
	}

	a.logger.Infof("Successfully rolled back database '%s' to version %d", databaseName, targetVersion)
	return nil
}

// ValidateMigration delegates to the underlying service and returns the validation report
func (a *MigrationServiceAdapter) ValidateMigration(databaseName string, version int, validatedBy string) (interface{}, error) {
	a.logger.Infof("Validating migration version %d for database '%s'", version, databaseName)

	report, err := a.service.ValidateMigration(databaseName, version, validatedBy)
	if err != nil {
		a.logger.Errorf("Failed to validate migration: %v", err)
		return nil, err
	}

	a.logger.Infof("Validation complete for migration version %d: %s", version, report.Status)
	return report, nil
}

// ValidateRollback delegates to the underlying service and returns the validation report
func (a *MigrationServiceAdapter) ValidateRollback(databaseName string, targetVersion int, validatedBy string) (interface{}, error) {
	a.logger.Infof("Validating rollback to version %d for database '%s'", targetVersion, databaseName)

	report, err := a.service.ValidateRollback(databaseName, targetVersion, validatedBy)
	if err != nil {
		a.logger.Errorf("Failed to validate rollback: %v", err)
		return nil, err
	}

	a.logger.Infof("Rollback validation complete: %s", report.Status)
	return report, nil
}

// ListMigrations delegates to the underlying service and returns the migration list
func (a *MigrationServiceAdapter) ListMigrations(databaseName string, filters map[string]interface{}) (interface{}, error) {
	a.logger.Debugf("Listing migrations for database '%s' with filters: %v", databaseName, filters)

	migrations, err := a.service.ListMigrations(databaseName, filters)
	if err != nil {
		a.logger.Errorf("Failed to list migrations: %v", err)
		return nil, err
	}

	a.logger.Debugf("Found %d migrations for database '%s'", len(migrations), databaseName)
	return migrations, nil
}

// GetCurrentVersion delegates to the underlying service
func (a *MigrationServiceAdapter) GetCurrentVersion(databaseName string) (int, error) {
	version, err := a.service.GetCurrentVersion(databaseName)
	if err != nil {
		a.logger.Errorf("Failed to get current version for database '%s': %v", databaseName, err)
		return 0, err
	}

	a.logger.Debugf("Current version for database '%s': %d", databaseName, version)
	return version, nil
}
