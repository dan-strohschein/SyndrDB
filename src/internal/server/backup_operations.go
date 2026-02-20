/*
Package server provides backup command operations for the SyndrDB database server.

This file implements the BACKUP DATABASE command, which creates a complete backup of a database
including all bundles, indexes, documents, and Primary database metadata. The backup process:

1. Validates the database exists
2. Executes a CHECKPOINT to ensure data consistency
3. Collects all database files (bundles, indexes, documents)
4. Calculates CRC checksums for integrity validation
5. Copies Primary database metadata (Databases and Bundles documents)
6. Creates a compressed tar archive (.sdb file)
7. Validates the backup after creation

Main Functions:
- BackupDatabase: Parses BACKUP DATABASE command and creates backup archive
- parseBackupCommand: Extracts database name and backup path from command tokens

The backup format is a tar archive with optional compression (gzip, zstd, or none).
Each backup includes a manifest.json file with metadata and file checksums.
*/

package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syndrdb/src/internal/backup"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// BackupDatabase handles the BACKUP DATABASE command
// Command syntax: BACKUP DATABASE "dbname" TO "path/to/backup.sdb"
// Options can be specified: BACKUP DATABASE "dbname" TO "path/to/backup.sdb" WITH COMPRESSION = 'gzip'
//
// Returns a CommandResponse with backup file path and statistics
func BackupDatabase(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager) (*CommandResponse, error) {
	// Parse the command to extract database name and backup path
	dbName, backupPath, options, err := parseBackupCommand(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse BACKUP command", errors.LayerCommand)
	}

	logger.Infof("Starting database backup: database=%s, backupPath=%s", dbName, backupPath)

	// Create backup service
	backupService := backup.NewBackupService(
		serviceManager.DatabaseService,
		serviceManager.BundleService,
		serviceManager.LockService,
		serviceManager.WALManager,
		logger,
	)

	// Execute the backup
	startTime := time.Now()
	backupFilePath, err := backupService.CreateBackup(dbName, options)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", dbName).WithContext("backup_path", backupPath)
	}
	duration := time.Since(startTime)

	logger.Infof("Database backup completed: database=%s, file=%s, duration=%s",
		dbName, backupFilePath, duration.String())

	// Return success response
	response := &CommandResponse{
		ResultCount:     1,
		Result:          fmt.Sprintf("Database '%s' backed up successfully to '%s' in %s", dbName, backupFilePath, duration.String()),
		ExecutionTimeMS: float64(duration.Milliseconds()),
	}

	return response, nil
}

// parseBackupCommand extracts database name, backup path, and options from command tokens
// Expected format: BACKUP DATABASE "dbname" TO "path/to/backup.sdb" [WITH COMPRESSION = 'gzip']
func parseBackupCommand(command string) (string, string, backup.BackupOptions, error) {
	tokens := tokenizeQuoted(command)

	// Initialize default options from settings
	args := settings.GetSettings()
	options := backup.BackupOptions{
		Compression:    args.BackupCompression,
		IncludeIndexes: args.BackupIncludeIndexes,
	}

	// Minimum tokens: BACKUP DATABASE "name" TO "path" = 5 tokens
	if len(tokens) < 5 {
		return "", "", options, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid BACKUP syntax: expected BACKUP DATABASE \"name\" TO \"path\"",
			errors.LayerCommand)
	}

	// Validate command structure
	if strings.ToUpper(tokens[0]) != "BACKUP" || strings.ToUpper(tokens[1]) != "DATABASE" {
		return "", "", options, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid BACKUP syntax: expected BACKUP DATABASE",
			errors.LayerCommand)
	}

	// Extract database name (token 2)
	dbName := strings.Trim(tokens[2], "\"'")
	if dbName == "" {
		return "", "", options, errors.New(errors.ERR_VALIDATION_REQUIRED,
			"database name cannot be empty", errors.LayerCommand)
	}

	// Validate TO keyword (token 3)
	if strings.ToUpper(tokens[3]) != "TO" {
		return "", "", options, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid BACKUP syntax: expected TO keyword",
			errors.LayerCommand)
	}

	// Extract backup path (token 4)
	backupPath := strings.Trim(tokens[4], "\"'")
	if backupPath == "" {
		return "", "", options, errors.New(errors.ERR_VALIDATION_REQUIRED,
			"backup path cannot be empty", errors.LayerCommand)
	}

	// TODO: I will add support for absolute paths vs relative paths
	// For now, if the path is not absolute, make it relative to the BackupDir setting
	if !filepath.IsAbs(backupPath) {
		backupPath = filepath.Join(args.BackupDir, backupPath)
	}

	// Ensure the backup file has .sdb extension
	if filepath.Ext(backupPath) == "" {
		backupPath += ".sdb"
	}

	// Parse optional WITH clause for options (tokens 5+)
	if len(tokens) > 5 {
		if err := parseBackupOptions(tokens[5:], &options); err != nil {
			return "", "", options, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
				"failed to parse backup options", errors.LayerCommand)
		}
	}

	options.OutputPath = backupPath

	return dbName, backupPath, options, nil
}

// ShowBackups handles the SHOW BACKUPS command.
// It reads the configured BackupDir and lists all .sdb backup files.
func ShowBackups(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager, startTime time.Time) (*CommandResponse, error) {
	backupDir := settings.GetSettings().BackupDir

	// Check if the backup directory exists
	info, err := os.Stat(backupDir)
	if err != nil {
		if os.IsNotExist(err) {
			return &CommandResponse{
				ResultCount:     0,
				Result:          []map[string]interface{}{},
				ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
			}, nil
		}
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to access backup directory", errors.LayerCommand).WithContext("backup_dir", backupDir)
	}
	if !info.IsDir() {
		return nil, errors.New(errors.ERR_INTERNAL_STORAGE,
			fmt.Sprintf("backup path is not a directory: %s", backupDir),
			errors.LayerCommand).WithContext("backup_dir", backupDir)
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to read backup directory", errors.LayerCommand).WithContext("backup_dir", backupDir)
	}

	results := make([]map[string]interface{}, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sdb" {
			continue
		}
		fileInfo, err := entry.Info()
		if err != nil {
			logger.Warnf("Failed to stat backup file %s: %v", entry.Name(), err)
			continue
		}
		results = append(results, map[string]interface{}{
			"file_name":   entry.Name(),
			"size_bytes":  fileInfo.Size(),
			"modified_at": fileInfo.ModTime().UTC().Format(time.RFC3339),
		})
	}

	return &CommandResponse{
		ResultCount:     len(results),
		Result:          results,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}, nil
}

// parseBackupOptions parses the WITH clause for backup options
// Supported options:
// - COMPRESSION = 'gzip' | 'zstd' | 'none'
// - INCLUDE_INDEXES = true | false
//
// TODO: I will add support for additional options:
// - ENCRYPT = true with optional KEY = 'password'
// - INCREMENTAL = true to create incremental backup
// - ASYNC = true to run backup in background
func parseBackupOptions(tokens []string, options *backup.BackupOptions) error {
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
		case "COMPRESSION":
			// Validate compression type
			value = strings.ToLower(value)
			if value != "gzip" && value != "zstd" && value != "none" {
				return errors.New(errors.ERR_VALIDATION_FIELD,
					fmt.Sprintf("invalid compression type: %s (supported: gzip, zstd, none)", value),
					errors.LayerCommand).WithContext("compression_type", value)
			}
			options.Compression = value

		case "INCLUDE_INDEXES":
			// Parse boolean value
			value = strings.ToLower(value)
			if value == "true" || value == "1" {
				options.IncludeIndexes = true
			} else if value == "false" || value == "0" {
				options.IncludeIndexes = false
			} else {
				return errors.New(errors.ERR_VALIDATION_TYPE,
					fmt.Sprintf("invalid boolean value for INCLUDE_INDEXES: %s", value),
					errors.LayerCommand).WithContext("value", value)
			}

		default:
			return errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown backup option: %s", key),
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
