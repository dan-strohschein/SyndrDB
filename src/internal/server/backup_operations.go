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

	// Load catalog for enrichment (optional — gracefully degrade if missing)
	catalogPath := filepath.Join(backupDir, "backup_catalog.json")
	catalog, _ := backup.LoadCatalog(catalogPath)

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
		row := map[string]interface{}{
			"file_name":   entry.Name(),
			"size_bytes":  fileInfo.Size(),
			"modified_at": fileInfo.ModTime().UTC().Format(time.RFC3339),
		}

		// Enrich with catalog info if available
		if catalog != nil {
			fullPath := filepath.Join(backupDir, entry.Name())
			if ce := catalog.FindEntryByPath(fullPath); ce != nil {
				row["backup_type"] = ce.BackupType
				row["chain_depth"] = ce.ChainDepth
				row["database_name"] = ce.DatabaseName
				row["backup_id"] = ce.BackupID
			}
		}

		results = append(results, row)
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

		case "INCREMENTAL":
			value = strings.ToLower(value)
			if value == "true" || value == "1" {
				options.Incremental = true
			} else if value == "false" || value == "0" {
				options.Incremental = false
			} else {
				return errors.New(errors.ERR_VALIDATION_TYPE,
					fmt.Sprintf("invalid boolean value for INCREMENTAL: %s", value),
					errors.LayerCommand).WithContext("value", value)
			}

		case "TYPE":
			value = strings.ToLower(value)
			if value == "incremental" {
				options.Incremental = true
			} else if value == "full" {
				options.Incremental = false
			} else {
				return errors.New(errors.ERR_VALIDATION_FIELD,
					fmt.Sprintf("invalid backup type: %s (supported: full, incremental)", value),
					errors.LayerCommand).WithContext("type", value)
			}

		case "PARENT":
			options.ParentBackupPath = value

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

// ShowBackupChain handles the SHOW BACKUP CHAIN FOR "path.sdb" command.
// It builds and displays the full backup chain for a given backup file.
func ShowBackupChain(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager, startTime time.Time) (*CommandResponse, error) {
	// Parse: SHOW BACKUP CHAIN FOR "path"
	tokens := tokenizeQuoted(command)

	// Find the FOR keyword
	var backupPath string
	for i, t := range tokens {
		if strings.ToUpper(t) == "FOR" && i+1 < len(tokens) {
			backupPath = strings.Trim(tokens[i+1], "\"'")
			break
		}
	}

	if backupPath == "" {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid syntax: expected SHOW BACKUP CHAIN FOR \"path\"",
			errors.LayerCommand)
	}

	args := settings.GetSettings()
	if !filepath.IsAbs(backupPath) {
		backupPath = filepath.Join(args.BackupDir, backupPath)
	}

	// Try catalog-based chain first
	catalogPath := filepath.Join(args.BackupDir, "backup_catalog.json")
	catalog, _ := backup.LoadCatalog(catalogPath)

	var chain []backup.BackupCatalogEntry
	var chainErr error

	if catalog != nil {
		if ce := catalog.FindEntryByPath(backupPath); ce != nil {
			chain, chainErr = catalog.BuildChain(ce.BackupID)
		}
	}

	// Fallback to path-based discovery
	if chain == nil || chainErr != nil {
		chain, chainErr = backup.BuildChainFromPath(backupPath)
	}

	if chainErr != nil {
		return nil, errors.WrapWithMessage(chainErr, errors.ERR_INTERNAL_STORAGE,
			"failed to build backup chain", errors.LayerCommand).WithContext("backup_path", backupPath)
	}

	results := make([]map[string]interface{}, 0, len(chain))
	for _, entry := range chain {
		results = append(results, map[string]interface{}{
			"backup_id":        entry.BackupID,
			"backup_type":      entry.BackupType,
			"chain_depth":      entry.ChainDepth,
			"database_name":    entry.DatabaseName,
			"timestamp":        entry.Timestamp.UTC().Format(time.RFC3339),
			"start_commit_seq": entry.StartCommitSeq,
			"end_commit_seq":   entry.EndCommitSeq,
			"file_path":        entry.FilePath,
			"size_bytes":       entry.SizeBytes,
		})
	}

	return &CommandResponse{
		ResultCount:     len(results),
		Result:          results,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}, nil
}
