package server

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/migration"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

/*
migration_commands.go

This file implements command handlers for SyndrDB's migration system.
Handlers parse migration commands and delegate to MigrationService for execution.

Supported Commands:
  1. START MIGRATION [WITH DESCRIPTION "..."] <commands> COMMIT
  2. SHOW MIGRATIONS FOR "<database>"
  3. APPLY MIGRATION WITH VERSION <number> [FORCE]
  4. APPLY ROLLBACK TO VERSION <number>
  5. VALIDATE MIGRATION WITH VERSION <number>
  6. VALIDATE ROLLBACK TO VERSION <number>

Design Patterns:
  - Parse-Validate-Execute: Commands are parsed, validated, then delegated to service
  - Error Handling: Detailed error messages for user feedback
  - Per-Database Isolation: All commands enforce DatabaseName filtering

Integration Points:
  - ServiceManager.MigrationService: Core migration logic
  - Database Context: Active database from session
  - Logger: Audit trail for migration operations

TODO: Integrate with full SyndrQL parser when migration_parser.go is complete
TODO: Add support for WHERE clause in SHOW MIGRATIONS
TODO: Add support for ORDER BY in SHOW MIGRATIONS
TODO: Implement approval token validation for APPLY ROLLBACK
*/

// StartMigrationCommand handles START MIGRATION ... COMMIT command
// Parses migration definition, creates migration record in PENDING status
func StartMigrationCommand(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager) (interface{}, error) {
	if serviceManager.MigrationService == nil {
		return nil, fmt.Errorf("migration service not initialized")
	}

	if database == nil {
		return nil, fmt.Errorf("no active database selected. Use 'USE DATABASE \"name\"' first")
	}

	logger.Infof("Executing START MIGRATION command for database '%s'", database.Name)

	// Parse START MIGRATION command
	// Format: START MIGRATION [WITH DESCRIPTION "description"] <commands> COMMIT

	// Extract description if provided
	description := ""
	descPattern := regexp.MustCompile(`(?i)WITH\s+DESCRIPTION\s+"([^"]+)"`)
	if matches := descPattern.FindStringSubmatch(command); len(matches) > 1 {
		description = matches[1]

		// Validate description length (500 char limit)
		if len(description) > 500 {
			return nil, fmt.Errorf("description exceeds 500 character limit: %d chars", len(description))
		}
	}

	// Extract commands between START MIGRATION and COMMIT
	// Remove START MIGRATION ... WITH DESCRIPTION "..." part
	commandBody := command
	if description != "" {
		commandBody = descPattern.ReplaceAllString(commandBody, "")
	}

	// Remove START MIGRATION prefix and COMMIT suffix
	commandBody = regexp.MustCompile(`(?i)START\s+MIGRATION\s*`).ReplaceAllString(commandBody, "")
	commandBody = regexp.MustCompile(`(?i)\s*COMMIT\s*$`).ReplaceAllString(commandBody, "")
	commandBody = strings.TrimSpace(commandBody)

	if commandBody == "" {
		return nil, fmt.Errorf("migration must contain at least one command")
	}

	// Split commands by semicolon, respecting quoted strings
	commands := splitBySemicolon(commandBody)
	cleanCommands := []string{}
	for _, cmd := range commands {
		cmd = strings.TrimSpace(cmd)
		if cmd != "" {
			cleanCommands = append(cleanCommands, cmd)
		}
	}

	if len(cleanCommands) == 0 {
		return nil, fmt.Errorf("migration must contain at least one command")
	}

	// Create MigrationCommand struct
	migrationCmd := migration.MigrationCommand{
		DatabaseName: database.Name,
		Description:  description,
		Commands:     cleanCommands,
		DownCommands: []string{}, // Will be auto-generated if possible
		CreatedBy:    "system",   // TODO: Get from session/auth context
	}

	// Delegate to migration service
	migrationResult, err := serviceManager.MigrationService.CreateMigration(migrationCmd)
	if err != nil {
		logger.Errorf("Failed to create migration: %v", err)
		return nil, fmt.Errorf("failed to create migration: %w", err)
	}

	// Type assert to get the migration details
	mig, ok := migrationResult.(*migration.Migration)
	if !ok {
		logger.Errorf("Unexpected migration result type: %T", migrationResult)
		return nil, fmt.Errorf("unexpected migration result type")
	}

	logger.Infof("Migration created successfully for database '%s'", database.Name)
	return map[string]interface{}{
		"migration_id":     mig.MigrationID,
		"version":          mig.Version,
		"migration_status": string(mig.Status),
		"status":           "success",
		"message":          fmt.Sprintf("Migration created successfully for database '%s'", database.Name),
	}, nil
}

// ApplyMigrationCommand handles APPLY MIGRATION WITH VERSION <number> [FORCE]
// Executes pending migration with optional FORCE flag to override warnings
func ApplyMigrationCommand(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager) (interface{}, error) {
	if serviceManager.MigrationService == nil {
		return nil, fmt.Errorf("migration service not initialized")
	}

	if database == nil {
		return nil, fmt.Errorf("no active database selected. Use 'USE DATABASE \"name\"' first")
	}

	logger.Infof("Executing APPLY MIGRATION command for database '%s'", database.Name)

	// Parse version number
	// Format: APPLY MIGRATION WITH VERSION <number> [FORCE]
	versionPattern := regexp.MustCompile(`(?i)WITH\s+VERSION\s+(\d+)`)
	matches := versionPattern.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid APPLY MIGRATION syntax. Expected: APPLY MIGRATION WITH VERSION <number> [FORCE]")
	}

	version, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid version number: %v", err)
	}

	// Check for FORCE flag
	force := strings.Contains(strings.ToUpper(command), "FORCE")

	// Delegate to migration service
	err = serviceManager.MigrationService.ApplyMigration(database.Name, version, force)
	if err != nil {
		logger.Errorf("Failed to apply migration: %v", err)
		return nil, fmt.Errorf("failed to apply migration: %w", err)
	}

	logger.Infof("Migration version %d applied successfully for database '%s'", version, database.Name)
	return map[string]interface{}{
		"status":  "success",
		"message": fmt.Sprintf("Migration version %d applied successfully", version),
		"version": version,
		"force":   force,
	}, nil
}

// ApplyRollbackCommand handles APPLY ROLLBACK TO VERSION <number>
// Reverts database to specified version with strict ordering enforcement
func ApplyRollbackCommand(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager) (interface{}, error) {
	if serviceManager.MigrationService == nil {
		return nil, fmt.Errorf("migration service not initialized")
	}

	if database == nil {
		return nil, fmt.Errorf("no active database selected. Use 'USE DATABASE \"name\"' first")
	}

	logger.Infof("Executing APPLY ROLLBACK command for database '%s'", database.Name)

	// Parse target version
	// Format: APPLY ROLLBACK TO VERSION <number>
	versionPattern := regexp.MustCompile(`(?i)TO\s+VERSION\s+(\d+)`)
	matches := versionPattern.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid APPLY ROLLBACK syntax. Expected: APPLY ROLLBACK TO VERSION <number>")
	}

	targetVersion, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid version number: %v", err)
	}

	// TODO: Check for approval token when approval workflow is implemented
	// For now, proceed with rollback directly

	// Delegate to migration service
	err = serviceManager.MigrationService.RollbackToVersion(database.Name, targetVersion)
	if err != nil {
		logger.Errorf("Failed to rollback to version %d: %v", targetVersion, err)
		return nil, fmt.Errorf("failed to rollback: %w", err)
	}

	logger.Infof("Database '%s' rolled back to version %d successfully", database.Name, targetVersion)
	return map[string]interface{}{
		"status":        "success",
		"message":       fmt.Sprintf("Database rolled back to version %d successfully", targetVersion),
		"targetVersion": targetVersion,
	}, nil
}

// ValidateMigrationCommand handles VALIDATE MIGRATION WITH VERSION <number>
// Runs 5-phase validation pipeline without executing migration
func ValidateMigrationCommand(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager) (interface{}, error) {
	if serviceManager.MigrationService == nil {
		return nil, fmt.Errorf("migration service not initialized")
	}

	if database == nil {
		return nil, fmt.Errorf("no active database selected. Use 'USE DATABASE \"name\"' first")
	}

	logger.Infof("Executing VALIDATE MIGRATION command for database '%s'", database.Name)

	// Parse version number
	// Format: VALIDATE MIGRATION WITH VERSION <number>
	versionPattern := regexp.MustCompile(`(?i)WITH\s+VERSION\s+(\d+)`)
	matches := versionPattern.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid VALIDATE MIGRATION syntax. Expected: VALIDATE MIGRATION WITH VERSION <number>")
	}

	version, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid version number: %v", err)
	}

	// Delegate to migration service
	report, err := serviceManager.MigrationService.ValidateMigration(database.Name, version, "system") // TODO: Get user from session
	if err != nil {
		logger.Errorf("Failed to validate migration: %v", err)
		return nil, fmt.Errorf("failed to validate migration: %w", err)
	}

	logger.Infof("Migration version %d validated for database '%s'", version, database.Name)
	return map[string]interface{}{
		"status":  "success",
		"message": fmt.Sprintf("Migration version %d validation complete", version),
		"report":  report,
	}, nil
}

// ValidateRollbackCommand handles VALIDATE ROLLBACK TO VERSION <number>
// Simulates rollback execution to verify safety without applying changes
func ValidateRollbackCommand(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager) (interface{}, error) {
	if serviceManager.MigrationService == nil {
		return nil, fmt.Errorf("migration service not initialized")
	}

	if database == nil {
		return nil, fmt.Errorf("no active database selected. Use 'USE DATABASE \"name\"' first")
	}

	logger.Infof("Executing VALIDATE ROLLBACK command for database '%s'", database.Name)

	// Parse target version
	// Format: VALIDATE ROLLBACK TO VERSION <number>
	versionPattern := regexp.MustCompile(`(?i)TO\s+VERSION\s+(\d+)`)
	matches := versionPattern.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid VALIDATE ROLLBACK syntax. Expected: VALIDATE ROLLBACK TO VERSION <number>")
	}

	targetVersion, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid version number: %v", err)
	}

	// Delegate to migration service
	report, err := serviceManager.MigrationService.ValidateRollback(database.Name, targetVersion, "system") // TODO: Get user from session
	if err != nil {
		logger.Errorf("Failed to validate rollback: %v", err)
		return nil, fmt.Errorf("failed to validate rollback: %w", err)
	}

	logger.Infof("Rollback to version %d validated for database '%s'", targetVersion, database.Name)
	return map[string]interface{}{
		"status":        "success",
		"message":       fmt.Sprintf("Rollback to version %d validation complete", targetVersion),
		"targetVersion": targetVersion,
		"report":        report,
	}, nil
}

// ShowMigrationsCommand handles SHOW MIGRATIONS FOR "<database>"
// Lists migrations with per-database isolation
func ShowMigrationsCommand(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager) (interface{}, error) {
	if serviceManager.MigrationService == nil {
		return nil, fmt.Errorf("migration service not initialized")
	}

	if database == nil {
		return nil, fmt.Errorf("no active database selected. Use 'USE DATABASE \"name\"' first")
	}

	parser, err := syndrQL.NewShowMigrationsParser(command)
	if err != nil {
		return nil, fmt.Errorf("Failed to create SHOW MIGRATIONS parser: %w", err)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("Failed to parse SHOW MIGRATIONS command: %w", err)
	}

	dbName := stmt.DatabaseName
	if dbName == "" {
		dbName = database.Name
	}

	logger.Infof("Executing SHOW MIGRATIONS command for database '%s'", dbName)

	// Parse optional filters
	// Format: SHOW MIGRATIONS FOR "database" [WHERE Status = "PENDING"] [ORDER BY Version DESC]
	filters := make(map[string]interface{})

	// Check for WHERE clause <- This isn't really legal. The right way would be for the developer
	// to execute a  SELECT query against a migrations view/table. But for now, this is a quick way to add filtering.
	wherePattern := regexp.MustCompile(`(?i)WHERE\s+(\w+)\s*=\s*"([^"]+)"`)
	if matches := wherePattern.FindStringSubmatch(command); len(matches) > 2 {
		fieldName := matches[1]
		fieldValue := matches[2]
		filters[fieldName] = fieldValue
		logger.Infof("Applying filter: %s = %s", fieldName, fieldValue)
	}

	// Delegate to migration service
	migrations, err := serviceManager.MigrationService.ListMigrations(dbName, filters)
	if err != nil {
		logger.Errorf("Failed to list migrations: %v", err)
		return nil, fmt.Errorf("failed to list migrations: %w", err)
	}

	// Get current version for context
	currentVersion, err := serviceManager.MigrationService.GetCurrentVersion(dbName)
	if err != nil {
		logger.Warnf("Could not get current version: %v", err)
		currentVersion = 0
	}

	logger.Infof("Retrieved migrations for database '%s'", dbName)
	return map[string]interface{}{
		"status":         "success",
		"database":       dbName,
		"currentVersion": currentVersion,
		"migrations":     migrations,
	}, nil
}

// splitBySemicolon splits a command string by semicolons while respecting quoted strings
// This prevents breaking commands that contain semicolons inside string literals
func splitBySemicolon(input string) []string {
	var commands []string
	var currentCommand strings.Builder
	inString := false
	var stringChar byte // Track whether we're in " or '

	for i := 0; i < len(input); i++ {
		ch := input[i]

		switch {
		case ch == '"' || ch == '\'':
			// Check if this is an escaped quote
			if i > 0 && input[i-1] == '\\' {
				currentCommand.WriteByte(ch)
			} else if inString && ch == stringChar {
				// Closing quote
				inString = false
				currentCommand.WriteByte(ch)
			} else if !inString {
				// Opening quote
				inString = true
				stringChar = ch
				currentCommand.WriteByte(ch)
			} else {
				// Different quote type inside string
				currentCommand.WriteByte(ch)
			}

		case ch == ';' && !inString:
			// End of command - save it and start new one
			cmd := strings.TrimSpace(currentCommand.String())
			if cmd != "" {
				commands = append(commands, cmd)
			}
			currentCommand.Reset()

		default:
			currentCommand.WriteByte(ch)
		}
	}

	// Add final command if any
	cmd := strings.TrimSpace(currentCommand.String())
	if cmd != "" {
		commands = append(commands, cmd)
	}

	return commands
}
