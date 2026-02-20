/*
Package server provides restore command operations for the SyndrDB database server.

This file implements the RESTORE DATABASE command, which restores a database from a backup
archive created by the BACKUP DATABASE command. The restore process:

1. Validates the backup file exists and is readable
2. Extracts the backup archive to a temporary directory
3. Verifies all file CRC checksums for integrity
4. Checks backup compatibility with current server version
5. Creates the database directory structure
6. Copies all files from the backup
7. Creates the database in the system (LOCKED state)
8. Restores Primary database metadata (Databases and Bundles documents)
9. Validates the restored database
10. Rolls back entire operation if any step fails

Main Functions:
- RestoreDatabase: Parses RESTORE DATABASE command and restores from backup
- parseRestoreCommand: Extracts backup path and target database name from command tokens

After restoration, the database is LOCKED and must be manually unlocked before use.
This prevents accidental writes to a freshly restored database.
*/

package server

import (
	"fmt"
	"path/filepath"
	"strings"
	"syndrdb/src/internal/backup"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// RestoreDatabase handles the RESTORE DATABASE command
// Command syntax: RESTORE DATABASE FROM "path/to/backup.sdb" AS "dbname"
// Options can be specified: RESTORE DATABASE FROM "backup.sdb" AS "dbname" WITH FORCE = true
//
// Returns a CommandResponse with restore statistics and database state
func RestoreDatabase(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager) (*CommandResponse, error) {
	// Parse the command to extract backup path and database name
	backupPath, dbName, options, err := parseRestoreCommand(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse RESTORE command", errors.LayerCommand)
	}

	// Wire the parsed AS name into the restore options
	options.TargetDBName = dbName

	logger.Infof("Starting database restore: backupPath=%s, database=%s", backupPath, dbName)

	// Create restore service
	restoreService := backup.NewRestoreService(
		serviceManager.DatabaseService,
		serviceManager.BundleService,
		serviceManager.LockService,
		logger,
	)

	// Execute the restore
	startTime := time.Now()
	if err := restoreService.RestoreBackup(backupPath, options); err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("backup_path", backupPath).WithContext("database", dbName)
	}
	duration := time.Since(startTime)

	logger.Infof("Database restore completed: backupPath=%s, database=%s, duration=%s",
		backupPath, dbName, duration.String())

	// Return success response
	response := &CommandResponse{
		ResultCount:     1,
		Result:          fmt.Sprintf("Database '%s' restored successfully from '%s' in %s. Database is LOCKED - use UNLOCK DATABASE to enable writes.", dbName, backupPath, duration.String()),
		ExecutionTimeMS: float64(duration.Milliseconds()),
	}

	return response, nil
}

// parseRestoreCommand extracts backup path, target database name, and options from command tokens
// Expected format: RESTORE DATABASE FROM "path/to/backup.sdb" AS "dbname" [WITH FORCE = true]
func parseRestoreCommand(command string) (string, string, backup.RestoreOptions, error) {
	tokens := tokenizeQuoted(command)

	// Initialize default options
	options := backup.RestoreOptions{
		Force: false, // Don't overwrite existing databases by default
	}

	// Minimum tokens: RESTORE DATABASE FROM "path" AS "name" = 6 tokens
	if len(tokens) < 6 {
		return "", "", options, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid RESTORE syntax: expected RESTORE DATABASE FROM \"path\" AS \"name\"",
			errors.LayerCommand)
	}

	// Validate command structure
	if strings.ToUpper(tokens[0]) != "RESTORE" || strings.ToUpper(tokens[1]) != "DATABASE" {
		return "", "", options, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid RESTORE syntax: expected RESTORE DATABASE",
			errors.LayerCommand)
	}

	// Validate FROM keyword (token 2)
	if strings.ToUpper(tokens[2]) != "FROM" {
		return "", "", options, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid RESTORE syntax: expected FROM keyword",
			errors.LayerCommand)
	}

	// Extract backup path (token 3)
	backupPath := strings.Trim(tokens[3], "\"'")
	if backupPath == "" {
		return "", "", options, errors.New(errors.ERR_VALIDATION_REQUIRED,
			"backup path cannot be empty", errors.LayerCommand)
	}

	// Resolve relative paths against the configured BackupDir
	if !filepath.IsAbs(backupPath) {
		backupPath = filepath.Join(settings.GetSettings().BackupDir, backupPath)
	}

	// Validate AS keyword (token 4)
	if strings.ToUpper(tokens[4]) != "AS" {
		return "", "", options, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid RESTORE syntax: expected AS keyword",
			errors.LayerCommand)
	}

	// Extract database name (token 5)
	dbName := strings.Trim(tokens[5], "\"'")
	if dbName == "" {
		return "", "", options, errors.New(errors.ERR_VALIDATION_REQUIRED,
			"database name cannot be empty", errors.LayerCommand)
	}

	// Parse optional WITH clause for options (tokens 6+)
	if len(tokens) > 6 {
		if err := parseRestoreOptions(tokens[6:], &options); err != nil {
			return "", "", options, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
				"failed to parse restore options", errors.LayerCommand)
		}
	}

	return backupPath, dbName, options, nil
}

// parseRestoreOptions parses the WITH clause for restore options
// Supported options:
// - FORCE = true | false (overwrite existing database)
//
// TODO: I will add support for additional options:
// - SELECTIVE = 'bundle1,bundle2' to restore only specific bundles
// - POINT_IN_TIME = '2024-01-01T00:00:00Z' to restore to specific timestamp
// - RENAME_BUNDLES = 'old1:new1,old2:new2' to rename bundles during restore
// - DRY_RUN = true to validate without actually restoring
func parseRestoreOptions(tokens []string, options *backup.RestoreOptions) error {
	if len(tokens) == 0 {
		return nil
	}

	// First token should be WITH
	if strings.ToUpper(tokens[0]) != "WITH" {
		return errors.New(errors.ERR_VALIDATION_SYNTAX,
			"expected WITH keyword for options", errors.LayerCommand)
	}

	// Parse key=value pairs
	i := 1
	for i < len(tokens) {
		if i+2 >= len(tokens) {
			return errors.New(errors.ERR_VALIDATION_SYNTAX,
				"incomplete option specification", errors.LayerCommand)
		}

		key := strings.ToUpper(tokens[i])
		equals := tokens[i+1]
		value := strings.Trim(tokens[i+2], "\"'")

		if equals != "=" {
			return errors.New(errors.ERR_VALIDATION_SYNTAX,
				"expected '=' after option key", errors.LayerCommand).WithContext("option_key", key)
		}

		switch key {
		case "FORCE":
			// Parse boolean value
			value = strings.ToLower(value)
			if value == "true" || value == "1" {
				options.Force = true
			} else if value == "false" || value == "0" {
				options.Force = false
			} else {
				return errors.New(errors.ERR_VALIDATION_TYPE,
					fmt.Sprintf("invalid boolean value for FORCE: %s", value),
					errors.LayerCommand).WithContext("value", value)
			}

		default:
			return errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown restore option: %s", key),
				errors.LayerCommand).WithContext("option", key)
		}

		// Move to next option (skip key, =, value)
		i += 3

		// If there's a comma, skip it
		if i < len(tokens) && tokens[i] == "," {
			i++
		}
	}

	return nil
}
