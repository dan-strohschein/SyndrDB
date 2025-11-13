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
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"syndrdb/src/internal/backup"
	"syndrdb/src/pkg/settings"
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
		return nil, fmt.Errorf("failed to parse BACKUP command: %w", err)
	}

	logger.Infof("Starting database backup: database=%s, backupPath=%s", dbName, backupPath)

	// TODO: Check user permissions - only admins can create backups
	// This requires session/authentication context to be passed through

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
		return nil, fmt.Errorf("backup failed: %w", err)
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
	tokens := strings.Fields(command)

	// Initialize default options from settings
	args := settings.GetSettings()
	options := backup.BackupOptions{
		Compression:    args.BackupCompression,
		IncludeIndexes: args.BackupIncludeIndexes,
	}

	// Minimum tokens: BACKUP DATABASE "name" TO "path" = 5 tokens
	if len(tokens) < 5 {
		return "", "", options, fmt.Errorf("invalid BACKUP syntax: expected BACKUP DATABASE \"name\" TO \"path\"")
	}

	// Validate command structure
	if strings.ToUpper(tokens[0]) != "BACKUP" || strings.ToUpper(tokens[1]) != "DATABASE" {
		return "", "", options, fmt.Errorf("invalid BACKUP syntax: expected BACKUP DATABASE")
	}

	// Extract database name (token 2)
	dbName := strings.Trim(tokens[2], "\"'")
	if dbName == "" {
		return "", "", options, fmt.Errorf("database name cannot be empty")
	}

	// Validate TO keyword (token 3)
	if strings.ToUpper(tokens[3]) != "TO" {
		return "", "", options, fmt.Errorf("invalid BACKUP syntax: expected TO keyword")
	}

	// Extract backup path (token 4)
	backupPath := strings.Trim(tokens[4], "\"'")
	if backupPath == "" {
		return "", "", options, fmt.Errorf("backup path cannot be empty")
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
			return "", "", options, fmt.Errorf("failed to parse backup options: %w", err)
		}
	}

	return dbName, backupPath, options, nil
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
		return fmt.Errorf("expected WITH keyword for options")
	}

	// Parse key=value pairs
	i := 1
	for i < len(tokens) {
		if i+2 >= len(tokens) {
			return fmt.Errorf("incomplete option specification")
		}

		key := strings.ToUpper(tokens[i])
		equals := tokens[i+1]
		value := strings.Trim(tokens[i+2], "\"'")

		if equals != "=" {
			return fmt.Errorf("expected '=' after option key")
		}

		switch key {
		case "COMPRESSION":
			// Validate compression type
			value = strings.ToLower(value)
			if value != "gzip" && value != "zstd" && value != "none" {
				return fmt.Errorf("invalid compression type: %s (supported: gzip, zstd, none)", value)
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
				return fmt.Errorf("invalid boolean value for INCLUDE_INDEXES: %s", value)
			}

		default:
			return fmt.Errorf("unknown backup option: %s", key)
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
