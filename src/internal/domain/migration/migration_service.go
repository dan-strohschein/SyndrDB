package migration

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

/*
migration_service.go

This file implements the core business logic for SyndrDB's native migration system.
The MigrationService orchestrates the complete migration lifecycle: creation, validation,
application, rollback, and cleanup of database schema migrations.

Key Responsibilities:
  - CreateMigration: Parse commands, auto-generate descriptions/down commands, assign versions
  - ApplyMigration: Execute migration with fail-fast locking and WAL transaction wrapping
  - RollbackToVersion: Revert database to specific version with strict ordering enforcement
  - ValidateMigration: Run 5-phase validation pipeline without executing changes
  - ValidateRollback: Simulate rollback execution to verify safety
  - ArchiveExpiredReports: Soft-delete validation reports past retention period
  - ListMigrations: Query migrations with per-database isolation
  - GetCurrentVersion: Retrieve active version for target database

Design Patterns:
  - Dependency Injection: BundleService injected for storage operations
  - Fail-Fast Locking: Single migration per database enforced via MigrationLocks bundle
  - Atomic Transactions: WAL wrapping ensures all-or-nothing migration execution
  - Per-Database Isolation: All queries filtered by DatabaseName for multi-tenant security

Concurrency Strategy:
  - Lock acquisition before migration execution (fail-fast if lock exists)
  - Lock release in defer block to handle panics gracefully
  - TODO: Replace fail-fast locks with advisory lock queue for concurrent request handling
  - TODO: Implement migration approval token system for VALIDATE ROLLBACK → APPLY flow

Performance Characteristics:
  - Version lookup: O(1) with hash index on DatabaseVersions.DatabaseName
  - Migration query: O(log n) with hash index on Migrations.DatabaseName + Version
  - Lock check: O(1) with hash index on MigrationLocks.DatabaseName
  - Command execution: O(m) where m = number of commands in migration

Error Handling:
  - Syntax errors: Fail migration immediately with detailed error message
  - Lock conflicts: Return "migration in progress" error without blocking
  - WAL failures: Automatic rollback via transaction abort
  - Checksum mismatches: Fail validation with tamper detection warning

TODO: I will add support for migration branching when team collaboration features are required
TODO: I will implement migration squashing to combine sequential migrations for performance
TODO: I will add migration approval workflow with token-based authorization for production safety
*/

// BundleServiceInterface defines the required operations from BundleService
// Allows for dependency injection and testing with mock implementations
type BundleServiceInterface interface {
	// InsertDocument creates a new document in the specified bundle
	InsertDocument(dbName, bundleName string, doc map[string]interface{}) error

	// UpdateDocument modifies an existing document by ID
	UpdateDocument(dbName, bundleName, docID string, updates map[string]interface{}) error

	// DeleteDocument removes a document by ID
	DeleteDocument(dbName, bundleName, docID string) error

	// QueryDocuments retrieves documents matching the provided filter
	QueryDocuments(dbName, bundleName string, filter map[string]interface{}) ([]map[string]interface{}, error)

	// GetDocumentCount returns the number of documents in a bundle
	GetDocumentCount(dbName, bundleName string) (int, error)

	// BeginTransaction starts a new WAL transaction
	BeginTransaction(dbName string) (string, error)

	// CommitTransaction persists changes to disk
	CommitTransaction(txID string) error

	// RollbackTransaction aborts changes
	RollbackTransaction(txID string) error
}

// MigrationService handles all migration operations
type MigrationService struct {
	bundleService BundleServiceInterface
	validator     *MigrationValidator
	reverser      *MigrationReverser
	config        MigrationConfig
	logger        *zap.Logger

	// Lock for in-memory state (version counters, etc.)
	mu sync.RWMutex
}

// NewMigrationService creates a new migration service instance
func NewMigrationService(
	bundleService BundleServiceInterface,
	config MigrationConfig,
	logger *zap.Logger,
) *MigrationService {
	return &MigrationService{
		bundleService: bundleService,
		validator:     NewMigrationValidator(bundleService, config, logger),
		reverser:      NewMigrationReverser(logger),
		config:        config,
		logger:        logger,
	}
}

// CreateMigration creates a new migration in PENDING status
// Automatically assigns sequential version number per database
// Generates description and down commands if not provided
// Returns the created Migration or error
func (s *MigrationService) CreateMigration(cmd MigrationCommand) (*Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Creating migration",
		zap.String("database", cmd.DatabaseName),
		zap.Int("commandCount", len(cmd.Commands)),
		zap.String("description", cmd.Description),
	)

	// Validate command count
	if len(cmd.Commands) > s.config.MaxCommandsPerMigration {
		return nil, fmt.Errorf("migration exceeds maximum command limit: %d > %d",
			len(cmd.Commands), s.config.MaxCommandsPerMigration)
	}

	// Validate description length if provided
	if cmd.Description != "" && len(cmd.Description) > 500 {
		return nil, fmt.Errorf("description exceeds 500 character limit: %d chars", len(cmd.Description))
	}

	// Get next version number for this database
	currentVersion, err := s.GetCurrentVersion(cmd.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to get current version: %w", err)
	}
	nextVersion := currentVersion + 1

	// Auto-generate description if not provided
	description := cmd.Description
	if description == "" {
		description = GenerateDescription(cmd.Commands)
	}

	// Auto-generate down commands if not provided and auto-reverse is enabled
	downCommands := cmd.DownCommands
	if len(downCommands) == 0 && s.config.EnableAutoReverse {
		downCommands, err = s.reverser.GenerateDownCommands(cmd.Commands)
		if err != nil {
			if s.config.RequireExplicitDownCommands {
				return nil, fmt.Errorf("failed to auto-generate down commands: %w", err)
			}
			s.logger.Warn("Could not auto-generate down commands, manual down required",
				zap.Error(err),
				zap.String("database", cmd.DatabaseName),
			)
			downCommands = []string{} // Empty down commands
		}
	}

	// Generate checksum
	upCommandsStr := strings.Join(cmd.Commands, ";")
	downCommandsStr := strings.Join(downCommands, ";")
	checksum := GenerateChecksum(upCommandsStr, downCommandsStr, description)

	// Create migration document
	migration := &Migration{
		MigrationID:         generateUUID(),
		Version:             nextVersion,
		DatabaseName:        cmd.DatabaseName,
		Description:         description,
		UpCommands:          cmd.Commands,
		DownCommands:        downCommands,
		Status:              PENDING,
		Checksum:            checksum,
		AppliedBy:           cmd.CreatedBy,
		CreatedAt:           time.Now(),
		AppliedAt:           nil,
		RolledBackAt:        nil,
		ExecutionTimeMs:     0,
		ErrorMessage:        "",
		PerformanceWarnings: make([]string, 0),
	}

	// Store in primary.Migrations bundle
	migrationDoc := s.migrationToDocument(migration)
	err = s.bundleService.InsertDocument("primary", "Migrations", migrationDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to store migration: %w", err)
	}

	s.logger.Info("Migration created successfully",
		zap.String("migrationID", migration.MigrationID),
		zap.Int("version", migration.Version),
		zap.String("database", migration.DatabaseName),
	)

	return migration, nil
}

// ApplyMigration executes a pending migration with fail-fast locking
// Wraps execution in WAL transaction for atomicity
// Updates migration status to APPLIED or FAILED
// Returns error if lock exists or execution fails
func (s *MigrationService) ApplyMigration(databaseName string, version int, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Applying migration",
		zap.String("database", databaseName),
		zap.Int("version", version),
		zap.Bool("force", force),
	)

	// Acquire migration lock (fail-fast)
	lockAcquired, err := s.acquireLock(databaseName, version)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !lockAcquired {
		return fmt.Errorf("migration already in progress for database %s", databaseName)
	}
	defer s.releaseLock(databaseName)

	// Retrieve migration
	migration, err := s.getMigrationByVersion(databaseName, version)
	if err != nil {
		return fmt.Errorf("failed to retrieve migration: %w", err)
	}

	// Validate migration status
	if migration.Status == APPLIED {
		return fmt.Errorf("migration version %d already applied", version)
	}

	// Update status to IN_PROGRESS
	err = s.updateMigrationStatus(migration.MigrationID, IN_PROGRESS, "")
	if err != nil {
		return fmt.Errorf("failed to update migration status: %w", err)
	}

	// Execute migration in WAL transaction
	startTime := time.Now()
	txID, err := s.bundleService.BeginTransaction(databaseName)
	if err != nil {
		s.updateMigrationStatus(migration.MigrationID, FAILED, fmt.Sprintf("failed to begin transaction: %v", err))
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute each command sequentially
	commands := migration.UpCommands
	for i, cmd := range commands {
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}

		s.logger.Debug("Executing migration command",
			zap.String("database", databaseName),
			zap.Int("version", version),
			zap.Int("commandIndex", i),
			zap.String("command", cmd),
		)

		// TODO: Execute command through command director
		// For now, this is a placeholder - actual command execution
		// will be integrated in command_director.go
		err = s.executeCommand(databaseName, cmd, txID)
		if err != nil {
			s.bundleService.RollbackTransaction(txID)
			s.updateMigrationStatus(migration.MigrationID, FAILED, fmt.Sprintf("command %d failed: %v", i, err))
			return fmt.Errorf("command %d failed: %w", i, err)
		}
	}

	// Commit transaction
	err = s.bundleService.CommitTransaction(txID)
	if err != nil {
		s.bundleService.RollbackTransaction(txID)
		s.updateMigrationStatus(migration.MigrationID, FAILED, fmt.Sprintf("failed to commit transaction: %v", err))
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update migration status to APPLIED
	executionTime := time.Since(startTime).Milliseconds()
	appliedAt := time.Now()
	err = s.updateMigrationComplete(migration.MigrationID, APPLIED, &appliedAt, int(executionTime))
	if err != nil {
		return fmt.Errorf("failed to update migration status: %w", err)
	}

	// Update database version
	err = s.updateDatabaseVersion(databaseName, version, migration.MigrationID)
	if err != nil {
		return fmt.Errorf("failed to update database version: %w", err)
	}

	s.logger.Info("Migration applied successfully",
		zap.String("migrationID", migration.MigrationID),
		zap.Int("version", version),
		zap.String("database", databaseName),
		zap.Int64("executionTimeMs", executionTime),
	)

	return nil
}

// RollbackToVersion reverts database to specific version with strict ordering
// Executes down commands for all migrations > targetVersion in reverse order
// Updates migration status to ROLLED_BACK
// Returns error if any rollback fails (strict enforcement)
func (s *MigrationService) RollbackToVersion(databaseName string, targetVersion int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Rolling back to version",
		zap.String("database", databaseName),
		zap.Int("targetVersion", targetVersion),
	)

	// Get current version
	currentVersion, err := s.GetCurrentVersion(databaseName)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	if targetVersion >= currentVersion {
		return fmt.Errorf("target version %d must be less than current version %d", targetVersion, currentVersion)
	}

	// Acquire migration lock
	lockAcquired, err := s.acquireLock(databaseName, targetVersion)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !lockAcquired {
		return fmt.Errorf("migration already in progress for database %s", databaseName)
	}
	defer s.releaseLock(databaseName)

	// Get all migrations between current and target (strict reverse order)
	migrationsToRollback := []Migration{}
	for v := currentVersion; v > targetVersion; v-- {
		migration, err := s.getMigrationByVersion(databaseName, v)
		if err != nil {
			return fmt.Errorf("failed to retrieve migration version %d: %w", v, err)
		}

		// Validate migration is in APPLIED status
		if migration.Status != APPLIED {
			return fmt.Errorf("migration version %d is not in APPLIED status (current: %s)", v, migration.Status)
		}

		// Validate down commands exist
		if len(migration.DownCommands) == 0 {
			return fmt.Errorf("migration version %d has no down commands, cannot rollback", v)
		}

		migrationsToRollback = append(migrationsToRollback, *migration)
	}

	// Execute rollbacks in strict reverse order
	for _, migration := range migrationsToRollback {
		s.logger.Info("Rolling back migration",
			zap.String("migrationID", migration.MigrationID),
			zap.Int("version", migration.Version),
			zap.String("database", databaseName),
		)

		// Update status to IN_PROGRESS
		err = s.updateMigrationStatus(migration.MigrationID, IN_PROGRESS, "")
		if err != nil {
			return fmt.Errorf("failed to update migration status: %w", err)
		}

		// Execute rollback in WAL transaction
		startTime := time.Now()
		txID, err := s.bundleService.BeginTransaction(databaseName)
		if err != nil {
			s.updateMigrationStatus(migration.MigrationID, FAILED, fmt.Sprintf("failed to begin transaction: %v", err))
			return fmt.Errorf("failed to begin transaction for rollback: %w", err)
		}

		// Execute down commands sequentially
		commands := migration.DownCommands
		for i, cmd := range commands {
			cmd = strings.TrimSpace(cmd)
			if cmd == "" {
				continue
			}

			s.logger.Debug("Executing rollback command",
				zap.String("database", databaseName),
				zap.Int("version", migration.Version),
				zap.Int("commandIndex", i),
				zap.String("command", cmd),
			)

			err = s.executeCommand(databaseName, cmd, txID)
			if err != nil {
				s.bundleService.RollbackTransaction(txID)
				s.updateMigrationStatus(migration.MigrationID, FAILED, fmt.Sprintf("rollback command %d failed: %v", i, err))
				return fmt.Errorf("rollback command %d failed: %w", i, err)
			}
		}

		// Commit transaction
		err = s.bundleService.CommitTransaction(txID)
		if err != nil {
			s.bundleService.RollbackTransaction(txID)
			s.updateMigrationStatus(migration.MigrationID, FAILED, fmt.Sprintf("failed to commit rollback transaction: %v", err))
			return fmt.Errorf("failed to commit rollback transaction: %w", err)
		}

		// Update migration status to ROLLED_BACK
		executionTime := time.Since(startTime).Milliseconds()
		rolledBackAt := time.Now()
		err = s.updateMigrationRollback(migration.MigrationID, &rolledBackAt, int(executionTime))
		if err != nil {
			return fmt.Errorf("failed to update migration status: %w", err)
		}
	}

	// Update database version
	err = s.updateDatabaseVersion(databaseName, targetVersion, "")
	if err != nil {
		return fmt.Errorf("failed to update database version: %w", err)
	}

	s.logger.Info("Rollback completed successfully",
		zap.String("database", databaseName),
		zap.Int("targetVersion", targetVersion),
		zap.Int("migrationsRolledBack", len(migrationsToRollback)),
	)

	return nil
}

// ValidateMigration runs 5-phase validation pipeline without executing
// Stores validation report in MigrationValidationReports bundle
// Returns validation report or error
func (s *MigrationService) ValidateMigration(databaseName string, version int, validatedBy string) (*ValidationReport, error) {
	s.logger.Info("Validating migration",
		zap.String("database", databaseName),
		zap.Int("version", version),
	)

	// Retrieve migration
	migration, err := s.getMigrationByVersion(databaseName, version)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve migration: %w", err)
	}

	// Run validation pipeline
	commands := migration.UpCommands
	result, err := s.validator.ValidateMigration(databaseName, commands)
	if err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Serialize validation results to JSON
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize validation results: %w", err)
	}

	// Check report size limit
	reportSize := len(resultJSON)
	if reportSize > s.config.MaxValidationReportSize {
		return nil, fmt.Errorf("validation report exceeds size limit: %d bytes > %d bytes",
			reportSize, s.config.MaxValidationReportSize)
	}

	// Create validation report
	expiresAt := time.Now().AddDate(0, 0, s.config.ValidationReportRetentionDays)
	report := &ValidationReport{
		ReportID:          generateUUID(),
		MigrationVersion:  version,
		TargetVersion:     0,
		DatabaseName:      databaseName,
		ReportType:        MIGRATION_VALIDATION,
		GeneratedAt:       time.Now(),
		GeneratedBy:       validatedBy,
		ValidationResults: []string{string(resultJSON)},
		ReportSizeBytes:   int64(reportSize),
		Status:            ACTIVE,
		ArchivedAt:        nil,
		ExpiresAt:         &expiresAt,
	}

	// Store report in MigrationValidationReports bundle
	reportDoc := s.validationReportToDocument(report)
	err = s.bundleService.InsertDocument("primary", "MigrationValidationReports", reportDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to store validation report: %w", err)
	}

	s.logger.Info("Validation completed",
		zap.String("reportID", report.ReportID),
		zap.Int("reportSize", reportSize),
		zap.Bool("valid", result.IsValid()),
	)

	return report, nil
}

// ValidateRollback simulates rollback execution without applying changes
// Validates down commands exist and can be executed in strict reverse order
// Returns validation report or error
func (s *MigrationService) ValidateRollback(databaseName string, targetVersion int, validatedBy string) (*ValidationReport, error) {
	s.logger.Info("Validating rollback",
		zap.String("database", databaseName),
		zap.Int("targetVersion", targetVersion),
	)

	// Get current version
	currentVersion, err := s.GetCurrentVersion(databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to get current version: %w", err)
	}

	if targetVersion >= currentVersion {
		return nil, fmt.Errorf("target version %d must be less than current version %d", targetVersion, currentVersion)
	}

	// Simulate rollback validation
	rollbackPlan := []map[string]interface{}{}
	for v := currentVersion; v > targetVersion; v-- {
		migration, err := s.getMigrationByVersion(databaseName, v)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve migration version %d: %w", v, err)
		}

		// Check down commands exist
		if len(migration.DownCommands) == 0 {
			rollbackPlan = append(rollbackPlan, map[string]interface{}{
				"version":     v,
				"migrationID": migration.MigrationID,
				"canRollback": false,
				"error":       "no down commands available",
			})
		} else {
			// Validate down commands syntax
			commands := migration.DownCommands
			result, _ := s.validator.ValidateMigration(databaseName, commands)

			rollbackPlan = append(rollbackPlan, map[string]interface{}{
				"version":      v,
				"migrationID":  migration.MigrationID,
				"canRollback":  result.IsValid(),
				"downCommands": len(commands),
				"syntaxValid":  result.SyntaxValid,
			})
		}
	}

	// Serialize rollback plan to JSON
	resultJSON, err := json.Marshal(map[string]interface{}{
		"currentVersion": currentVersion,
		"targetVersion":  targetVersion,
		"rollbackPlan":   rollbackPlan,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to serialize rollback plan: %w", err)
	}

	// Check report size limit
	reportSize := len(resultJSON)
	if reportSize > s.config.MaxValidationReportSize {
		return nil, fmt.Errorf("validation report exceeds size limit: %d bytes > %d bytes",
			reportSize, s.config.MaxValidationReportSize)
	}

	// Create validation report
	expiresAt := time.Now().AddDate(0, 0, s.config.ValidationReportRetentionDays)
	report := &ValidationReport{
		ReportID:          generateUUID(),
		MigrationVersion:  0,
		TargetVersion:     targetVersion,
		DatabaseName:      databaseName,
		ReportType:        ROLLBACK_VALIDATION,
		GeneratedAt:       time.Now(),
		GeneratedBy:       validatedBy,
		ValidationResults: []string{string(resultJSON)},
		ReportSizeBytes:   int64(reportSize),
		Status:            ACTIVE,
		ArchivedAt:        nil,
		ExpiresAt:         &expiresAt,
	}

	// Store report in MigrationValidationReports bundle
	reportDoc := s.validationReportToDocument(report)
	err = s.bundleService.InsertDocument("primary", "MigrationValidationReports", reportDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to store validation report: %w", err)
	}

	s.logger.Info("Rollback validation completed",
		zap.String("reportID", report.ReportID),
		zap.Int("reportSize", reportSize),
	)

	return report, nil
}

// ArchiveExpiredReports soft-deletes validation reports past retention period
// Updates Status to ARCHIVED for reports where ExpiresAt < now
// External cleanup application can hard-delete ARCHIVED reports
func (s *MigrationService) ArchiveExpiredReports() error {
	s.logger.Info("Archiving expired validation reports")

	// Query for expired reports with Status = ACTIVE
	now := time.Now()
	filter := map[string]interface{}{
		"Status":    string(ACTIVE),
		"ExpiresAt": map[string]interface{}{"$lt": now},
	}

	reports, err := s.bundleService.QueryDocuments("primary", "MigrationValidationReports", filter)
	if err != nil {
		return fmt.Errorf("failed to query expired reports: %w", err)
	}

	archivedCount := 0
	for _, reportDoc := range reports {
		reportID := reportDoc["ReportID"].(string)
		archivedAt := time.Now()

		updates := map[string]interface{}{
			"Status":     string(ARCHIVED),
			"ArchivedAt": archivedAt,
		}
		err = s.bundleService.UpdateDocument("primary", "MigrationValidationReports", reportID, updates)
		if err != nil {
			s.logger.Error("Failed to archive report",
				zap.String("reportID", reportID),
				zap.Error(err),
			)
			continue
		}

		archivedCount++
	}

	s.logger.Info("Archived expired reports",
		zap.Int("count", archivedCount),
	)

	return nil
}

// ListMigrations retrieves migrations with per-database isolation
// Supports optional filters (status, version range, etc.)
func (s *MigrationService) ListMigrations(databaseName string, filters map[string]interface{}) ([]Migration, error) {
	// Enforce per-database isolation
	filters["DatabaseName"] = databaseName

	docs, err := s.bundleService.QueryDocuments("primary", "Migrations", filters)
	if err != nil {
		return nil, fmt.Errorf("failed to query migrations: %w", err)
	}

	migrations := make([]Migration, 0, len(docs))
	for _, doc := range docs {
		migration := s.documentToMigration(doc)
		migrations = append(migrations, *migration)
	}

	return migrations, nil
}

// GetCurrentVersion retrieves the active version for target database
// Returns 0 if no migrations have been applied yet
func (s *MigrationService) GetCurrentVersion(databaseName string) (int, error) {
	filter := map[string]interface{}{
		"DatabaseName": databaseName,
	}

	docs, err := s.bundleService.QueryDocuments("primary", "DatabaseVersions", filter)
	if err != nil {
		return 0, fmt.Errorf("failed to query database version: %w", err)
	}

	if len(docs) == 0 {
		return 0, nil // No migrations applied yet
	}

	currentVersion := int(docs[0]["CurrentVersion"].(float64))
	return currentVersion, nil
}

// Helper functions

// acquireLock attempts to acquire a migration lock for the database
// Returns true if lock acquired, false if lock already exists
func (s *MigrationService) acquireLock(databaseName string, version int) (bool, error) {
	// Check if lock already exists
	filter := map[string]interface{}{
		"DatabaseName": databaseName,
		"Status":       "ACTIVE",
	}

	locks, err := s.bundleService.QueryDocuments("primary", "MigrationLocks", filter)
	if err != nil {
		return false, fmt.Errorf("failed to query locks: %w", err)
	}

	if len(locks) > 0 {
		return false, nil // Lock already exists
	}

	// Create new lock
	lock := map[string]interface{}{
		"DatabaseName":     databaseName,
		"LockedAt":         time.Now(),
		"LockedBy":         "migration_service", // TODO: Replace with actual user/session
		"MigrationVersion": version,
		"MigrationID":      "", // Will be updated during execution
		"Status":           "ACTIVE",
	}

	err = s.bundleService.InsertDocument("primary", "MigrationLocks", lock)
	if err != nil {
		return false, fmt.Errorf("failed to create lock: %w", err)
	}

	return true, nil
}

// releaseLock removes the migration lock for the database
func (s *MigrationService) releaseLock(databaseName string) error {
	filter := map[string]interface{}{
		"DatabaseName": databaseName,
		"Status":       "ACTIVE",
	}

	locks, err := s.bundleService.QueryDocuments("primary", "MigrationLocks", filter)
	if err != nil {
		return fmt.Errorf("failed to query locks: %w", err)
	}

	for _, lock := range locks {
		lockID := lock["DocumentID"].(string)
		updates := map[string]interface{}{
			"Status": "RELEASED",
		}

		err = s.bundleService.UpdateDocument("primary", "MigrationLocks", lockID, updates)
		if err != nil {
			return fmt.Errorf("failed to release lock: %w", err)
		}
	}

	return nil
}

// getMigrationByVersion retrieves a migration by version and database
func (s *MigrationService) getMigrationByVersion(databaseName string, version int) (*Migration, error) {
	filter := map[string]interface{}{
		"DatabaseName": databaseName,
		"Version":      version,
	}

	docs, err := s.bundleService.QueryDocuments("primary", "Migrations", filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query migration: %w", err)
	}

	if len(docs) == 0 {
		return nil, fmt.Errorf("migration version %d not found for database %s", version, databaseName)
	}

	return s.documentToMigration(docs[0]), nil
}

// updateMigrationStatus updates the status and error message of a migration
func (s *MigrationService) updateMigrationStatus(migrationID string, status MigrationStatus, errorMsg string) error {
	updates := map[string]interface{}{
		"Status":       string(status),
		"ErrorMessage": errorMsg,
	}

	return s.bundleService.UpdateDocument("primary", "Migrations", migrationID, updates)
}

// updateMigrationComplete updates migration to APPLIED status with timing info
func (s *MigrationService) updateMigrationComplete(migrationID string, status MigrationStatus, appliedAt *time.Time, executionTimeMs int) error {
	updates := map[string]interface{}{
		"Status":          string(status),
		"AppliedAt":       appliedAt,
		"ExecutionTimeMs": executionTimeMs,
	}

	return s.bundleService.UpdateDocument("primary", "Migrations", migrationID, updates)
}

// updateMigrationRollback updates migration to ROLLED_BACK status
func (s *MigrationService) updateMigrationRollback(migrationID string, rolledBackAt *time.Time, executionTimeMs int) error {
	updates := map[string]interface{}{
		"Status":          string(ROLLED_BACK),
		"RolledBackAt":    rolledBackAt,
		"ExecutionTimeMs": executionTimeMs,
	}

	return s.bundleService.UpdateDocument("primary", "Migrations", migrationID, updates)
}

// updateDatabaseVersion updates the current version for a database
func (s *MigrationService) updateDatabaseVersion(databaseName string, version int, migrationID string) error {
	filter := map[string]interface{}{
		"DatabaseName": databaseName,
	}

	docs, err := s.bundleService.QueryDocuments("primary", "DatabaseVersions", filter)
	if err != nil {
		return fmt.Errorf("failed to query database version: %w", err)
	}

	versionDoc := map[string]interface{}{
		"DatabaseName":    databaseName,
		"CurrentVersion":  version,
		"LastUpdated":     time.Now(),
		"LastMigrationID": migrationID,
	}

	if len(docs) == 0 {
		// Create new version entry
		return s.bundleService.InsertDocument("primary", "DatabaseVersions", versionDoc)
	}

	// Update existing version entry
	docID := docs[0]["DocumentID"].(string)
	return s.bundleService.UpdateDocument("primary", "DatabaseVersions", docID, versionDoc)
}

// executeCommand executes a single SyndrQL command within a transaction
// TODO: Integrate with command_director.go for actual command execution
func (s *MigrationService) executeCommand(databaseName, command, txID string) error {
	// Placeholder for command execution
	// This will be replaced with actual command director integration
	s.logger.Debug("Executing command (placeholder)",
		zap.String("database", databaseName),
		zap.String("command", command),
		zap.String("txID", txID),
	)

	// TODO: Call command director to execute command
	return nil
}

// Conversion functions

func (s *MigrationService) migrationToDocument(m *Migration) map[string]interface{} {
	doc := map[string]interface{}{
		"MigrationID":         m.MigrationID,
		"Version":             m.Version,
		"DatabaseName":        m.DatabaseName,
		"Description":         m.Description,
		"UpCommands":          m.UpCommands,
		"DownCommands":        m.DownCommands,
		"Status":              string(m.Status),
		"Checksum":            m.Checksum,
		"AppliedBy":           m.AppliedBy,
		"CreatedAt":           m.CreatedAt,
		"ExecutionTimeMs":     m.ExecutionTimeMs,
		"ErrorMessage":        m.ErrorMessage,
		"PerformanceWarnings": m.PerformanceWarnings,
	}

	if m.AppliedAt != nil {
		doc["AppliedAt"] = *m.AppliedAt
	}

	if m.RolledBackAt != nil {
		doc["RolledBackAt"] = *m.RolledBackAt
	}

	return doc
}

func (s *MigrationService) documentToMigration(doc map[string]interface{}) *Migration {
	// Convert command arrays from interface{} to []string
	upCommandsInterface := doc["UpCommands"].([]interface{})
	upCommands := make([]string, len(upCommandsInterface))
	for i, v := range upCommandsInterface {
		upCommands[i] = v.(string)
	}

	downCommandsInterface := doc["DownCommands"].([]interface{})
	downCommands := make([]string, len(downCommandsInterface))
	for i, v := range downCommandsInterface {
		downCommands[i] = v.(string)
	}

	// Convert performance warnings from interface{} to []string
	perfWarningsInterface := doc["PerformanceWarnings"].([]interface{})
	perfWarnings := make([]string, len(perfWarningsInterface))
	for i, v := range perfWarningsInterface {
		perfWarnings[i] = v.(string)
	}

	m := &Migration{
		MigrationID:         doc["MigrationID"].(string),
		Version:             int(doc["Version"].(float64)),
		DatabaseName:        doc["DatabaseName"].(string),
		Description:         doc["Description"].(string),
		UpCommands:          upCommands,
		DownCommands:        downCommands,
		Status:              MigrationStatus(doc["Status"].(string)),
		Checksum:            doc["Checksum"].(string),
		AppliedBy:           doc["AppliedBy"].(string),
		CreatedAt:           doc["CreatedAt"].(time.Time),
		ExecutionTimeMs:     int64(doc["ExecutionTimeMs"].(float64)),
		ErrorMessage:        doc["ErrorMessage"].(string),
		PerformanceWarnings: perfWarnings,
	}

	if appliedAt, ok := doc["AppliedAt"].(time.Time); ok {
		m.AppliedAt = &appliedAt
	}

	if rolledBackAt, ok := doc["RolledBackAt"].(time.Time); ok {
		m.RolledBackAt = &rolledBackAt
	}

	return m
}

func (s *MigrationService) validationReportToDocument(r *ValidationReport) map[string]interface{} {
	doc := map[string]interface{}{
		"ReportID":          r.ReportID,
		"DatabaseName":      r.DatabaseName,
		"ReportType":        string(r.ReportType),
		"GeneratedAt":       r.GeneratedAt,
		"GeneratedBy":       r.GeneratedBy,
		"ValidationResults": r.ValidationResults,
		"ReportSizeBytes":   r.ReportSizeBytes,
		"Status":            string(r.Status),
		"ExpiresAt":         r.ExpiresAt,
		"MigrationVersion":  r.MigrationVersion,
		"TargetVersion":     r.TargetVersion,
	}

	if r.ArchivedAt != nil {
		doc["ArchivedAt"] = *r.ArchivedAt
	}

	return doc
}

// Placeholder utility functions

func generateUUID() string {
	// TODO: Implement proper UUID generation using github.com/google/uuid
	return fmt.Sprintf("migration-%d", time.Now().UnixNano())
}
