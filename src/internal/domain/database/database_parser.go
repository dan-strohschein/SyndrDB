/*
CREATE BUNDLE "BUNDLE_NAME"
WITH FIELDS ({"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}, {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>})

To update a bundle:
UPDATE BUNDLE "BUNDLE_NAME"
CHANGE FIELD "<OLDFIELDNAME"> TO {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}
ADD FIELD {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}
REMOVE FIELD "<FIELDNAME>"

To Drop a bundle:
DELETE BUNDLE "BUNDLE_NAME"

// ------------------------------------------- db structure SQL-------------------------------------------

To Setup a relationship between two bundles:
UPDATE BUNDLE "BUNDLE_NAME"
CREATE RELATIONSHIP "RELATIONSHIP_NAME"
FROM BUNDLE "BUNDLE_NAME"
TO BUNDLE "BUNDLE_NAME"
WITH FIELDS ({"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}, {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>})

To update a relationship:
UPDATE RELATIONSHIP "RELATIONSHIP_NAME"
FROM BUNDLE "BUNDLE_NAME"
TO BUNDLE "BUNDLE_NAME"
WITH FIELDS ({"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}, {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>})

To Drop a relationship:
UPDATE BUNDLE "BUNDLE_NAME"
DELETE RELATIONSHIP "RELATIONSHIP_NAME"
*/
package database

import (
	"fmt"
	"regexp"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// type DatabaseCommand struct {
// 	ID                 string
// 	CommandType        string // CREATE, UPDATE, DELETE
// 	DatabaseName       string
// 	DBMetadataFilePath string
// }

// extractQuotedOrUnquotedValue extracts a value from the command that may be quoted or unquoted.
// This function preserves the original case of the value by extracting it from the original command.
// Parameters:
//   - original: The original command with original case preserved
//   - upperCmd: The uppercase version of the command for case-insensitive keyword matching
//   - keyword: The keyword to search after (e.g., "DATABASE", "TO")
//
// Returns the extracted value with original case preserved, or an error if extraction fails.
// TODO: I will add support for escaped quotes within quoted values (e.g., "name with \"quotes\"")
func extractQuotedOrUnquotedValue(original, upperCmd, keyword string) (string, error) {
	// Find the position of the keyword in the uppercase version
	keywordPos := strings.Index(upperCmd, keyword)
	if keywordPos == -1 {
		return "", fmt.Errorf("keyword '%s' not found", keyword)
	}

	// Move past the keyword and any whitespace
	startPos := keywordPos + len(keyword)
	remaining := strings.TrimSpace(original[startPos:])

	if len(remaining) == 0 {
		return "", fmt.Errorf("no value found after '%s'", keyword)
	}

	// Check if the value is quoted
	if remaining[0] == '"' {
		// Find the closing quote
		endQuote := strings.Index(remaining[1:], "\"")
		if endQuote == -1 {
			return "", fmt.Errorf("unclosed quote in value after '%s'", keyword)
		}
		// Extract the quoted value (without the quotes)
		return remaining[1 : endQuote+1], nil
	}

	// Unquoted value - extract until whitespace, semicolon, or end of string
	value := ""
	for i, char := range remaining {
		if char == ' ' || char == '\t' || char == '\n' || char == '\r' || char == ';' {
			value = remaining[:i]
			break
		}
	}

	// If we didn't find a delimiter, use the entire remaining string
	if value == "" {
		value = remaining
	}

	return strings.TrimSpace(value), nil
}

// ParseDatabaseCommand is the new case-insensitive parser for CREATE DATABASE commands.
// This function routes CREATE DATABASE commands through the SyndrQL parser for case-insensitive keyword matching.
// Supports both quoted and unquoted database names:
//   - CREATE DATABASE "MyDatabase"
//   - CREATE DATABASE MyDatabase
//   - create database "MyDatabase" (case-insensitive keywords)
//
// Returns a DatabaseCommand with CommandType="CREATE" and the database name with original case preserved.
// TODO: I will add support for additional CREATE DATABASE options (e.g., WITH ENCODING, TABLESPACE)
func ParseDatabaseCommand(command string, logger *zap.SugaredLogger) (*models.DatabaseCommand, error) {
	args := settings.GetSettings()

	// Preserve original command for extracting values
	original := strings.TrimSpace(command)
	original = strings.TrimSuffix(original, ";")

	// Create uppercase version for case-insensitive keyword matching
	upperCmd := strings.ToUpper(original)

	// Validate command starts with CREATE DATABASE
	if !strings.HasPrefix(upperCmd, "CREATE DATABASE") {
		logger.Infof("Invalid CREATE DATABASE command syntax: %s", command)
		return nil, fmt.Errorf("invalid CREATE DATABASE command syntax")
	}

	// Extract database name using helper function (preserves original case)
	databaseName, err := extractQuotedOrUnquotedValue(original, upperCmd, "CREATE DATABASE")
	if err != nil {
		logger.Infof("Failed to extract database name from command: %s, error: %v", command, err)
		return nil, fmt.Errorf("invalid CREATE DATABASE syntax: %w", err)
	}

	if databaseName == "" {
		logger.Infof("Empty database name in CREATE DATABASE command: %s", command)
		return nil, fmt.Errorf("database name cannot be empty")
	}

	logger.Debugf("Parsed CREATE DATABASE command: database='%s'", databaseName)

	return &models.DatabaseCommand{
		ID:                 helpers.GenerateUUID(),
		DatabaseName:       databaseName,
		DBMetadataFilePath: args.DataDir,
		CommandType:        "CREATE",
	}, nil
}

// DEPRECATED: Use ParseDatabaseCommand instead for case-insensitive parsing
// ParseCreateDatabaseCommand is the legacy case-sensitive parser.
// Kept for backward compatibility but will be removed in a future version.
// TODO: I will remove this function once all callers are migrated to ParseDatabaseCommand
func ParseCreateDatabaseCommand(command string, logger *zap.SugaredLogger) (*models.DatabaseCommand, error) {
	args := settings.GetSettings()
	// Regular expression to extract database name
	databaseNameRegex := regexp.MustCompile(`CREATE DATABASE\s+(?:"([^"]+)"|([^\s;]+))`)
	matches := databaseNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		logger.Infof("Invalid CREATE DATABASE command syntax: %s", command)
		logger.Infof("Matches found: %v", matches)

		return nil, fmt.Errorf("invalid CREATE DATABASE command syntax")
	}

	// The database name could be in group 1 (with quotes) or group 2 (without quotes)
	var databaseName string
	if matches[1] != "" {
		databaseName = matches[1] // Quoted name
	} else {
		databaseName = matches[2] // Unquoted name
	}

	return &models.DatabaseCommand{
		ID:                 helpers.GenerateUUID(), // Generate a unique ID for the command
		DatabaseName:       databaseName,
		DBMetadataFilePath: args.DataDir, // Placeholder for actual metadata file path
		CommandType:        "CREATE",
	}, nil
}

// ParseDropDatabaseCommand is the new case-insensitive parser for DROP DATABASE commands.
// This function routes DROP DATABASE commands through the SyndrQL parser for case-insensitive keyword matching.
// Supports both quoted and unquoted database names, with optional FORCE keyword:
//   - DROP DATABASE "MyDatabase"
//   - DROP DATABASE MyDatabase
//   - drop database "MyDatabase" FORCE (case-insensitive keywords)
//   - DROP DATABASE MyDatabase FORCE
//
// Returns a DatabaseCommand with CommandType="DROP", database name with original case preserved,
// and Force flag set if FORCE keyword is present.
// TODO: I will add support for CASCADE/RESTRICT options for more granular control
func ParseDropDatabaseCommand(command string) (*models.DatabaseCommand, error) {
	// Preserve original command for extracting values
	original := strings.TrimSpace(command)
	original = strings.TrimSuffix(original, ";")

	// Create uppercase version for case-insensitive keyword matching
	upperCmd := strings.ToUpper(original)

	// Validate command starts with DROP DATABASE (or just DROP for legacy support)
	hasDatabaseKeyword := strings.Contains(upperCmd, "DROP DATABASE")
	if !strings.HasPrefix(upperCmd, "DROP") {
		return nil, fmt.Errorf("invalid DROP DATABASE command syntax")
	}

	// Determine the keyword to search for
	keyword := "DROP DATABASE"
	if !hasDatabaseKeyword {
		keyword = "DROP"
	}

	// Extract database name using helper function (preserves original case)
	databaseName, err := extractQuotedOrUnquotedValue(original, upperCmd, keyword)
	if err != nil {
		return nil, fmt.Errorf("invalid DROP DATABASE syntax: %w", err)
	}

	if databaseName == "" {
		return nil, fmt.Errorf("database name cannot be empty")
	}

	// Check for WITH FORCE clause (case-insensitive, consistent with DROP BUNDLE syntax)
	force := strings.Contains(upperCmd, "WITH FORCE")

	// Validate database name
	if !IsValidDatabaseName(databaseName) {
		return nil, fmt.Errorf("invalid database name: %s. Database names must start with a letter, can be alphanumeric, with underscores and hyphens", databaseName)
	}

	return &models.DatabaseCommand{
		ID:           helpers.GenerateUUID(),
		CommandType:  "DROP",
		DatabaseName: databaseName,
		Force:        force,
	}, nil
}

func ParseUpdateDatabaseCommand(command string) (*models.DatabaseCommand, error) {
	// Regular expression to extract database name
	databaseNameRegex := regexp.MustCompile(`UPDATE DATABASE\s+"([^"]+)"`)
	matches := databaseNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid UPDATE DATABASE command syntax")
	}
	databaseName := matches[1]

	return &models.DatabaseCommand{

		DatabaseName:       databaseName,
		DBMetadataFilePath: "path/to/metadata/file", // Placeholder for actual metadata file path
		CommandType:        "UPDATE",
	}, nil
}

func ParseDeleteDatabaseCommand(command string) (*models.DatabaseCommand, error) {
	// Regular expression to extract database name
	databaseNameRegex := regexp.MustCompile(`DELETE DATABASE\s+"([^"]+)"`)
	matches := databaseNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid DELETE DATABASE command syntax")
	}
	databaseName := matches[1]

	return &models.DatabaseCommand{
		DatabaseName: databaseName,
		CommandType:  "DELETE",
	}, nil
}

// ParseRenameDatabaseCommand is the new case-insensitive parser for RENAME DATABASE commands.
// This function routes RENAME DATABASE commands through the SyndrQL parser for case-insensitive keyword matching.
// Supports both quoted and unquoted database names, with optional FORCE keyword:
//   - RENAME DATABASE "OldName" TO "NewName"
//   - RENAME DATABASE OldName TO NewName
//   - rename database "OldName" to "NewName" FORCE (case-insensitive keywords)
//   - RENAME DATABASE OldName TO NewName FORCE
//
// Returns a DatabaseCommand with CommandType="RENAME", old database name, new database name,
// and Force flag set if FORCE keyword is present.
// TODO: I will add validation to prevent renaming to an existing database name
// TODO: I will add support for atomic rename operations across clustered nodes
func ParseRenameDatabaseCommand(command string) (*models.DatabaseCommand, error) {
	// Preserve original command for extracting values
	original := strings.TrimSpace(command)
	original = strings.TrimSuffix(original, ";")

	// Create uppercase version for case-insensitive keyword matching
	upperCmd := strings.ToUpper(original)

	// Validate command starts with RENAME DATABASE
	if !strings.HasPrefix(upperCmd, "RENAME DATABASE") {
		return nil, fmt.Errorf("invalid RENAME DATABASE command syntax")
	}

	// Extract old database name
	oldName, err := extractQuotedOrUnquotedValue(original, upperCmd, "RENAME DATABASE")
	if err != nil {
		return nil, fmt.Errorf("invalid RENAME DATABASE syntax: failed to extract old database name: %w", err)
	}

	if oldName == "" {
		return nil, fmt.Errorf("old database name cannot be empty")
	}

	// Find the TO keyword position in uppercase command
	toPos := strings.Index(upperCmd, " TO ")
	if toPos == -1 {
		return nil, fmt.Errorf("invalid RENAME DATABASE syntax: TO keyword not found. Expected: RENAME DATABASE \"old_name\" TO \"new_name\"")
	}

	// Extract new database name (everything after TO)
	newName, err := extractQuotedOrUnquotedValue(original, upperCmd, " TO ")
	if err != nil {
		return nil, fmt.Errorf("invalid RENAME DATABASE syntax: failed to extract new database name: %w", err)
	}

	if newName == "" {
		return nil, fmt.Errorf("new database name cannot be empty")
	}

	// Check if FORCE keyword is present (case-insensitive)
	force := strings.Contains(upperCmd, "FORCE")

	// Validate database names
	if !IsValidDatabaseName(oldName) {
		return nil, fmt.Errorf("invalid old database name: %s. Database names must start with a letter, can be alphanumeric, with underscores and hyphens", oldName)
	}

	if !IsValidDatabaseName(newName) {
		return nil, fmt.Errorf("invalid new database name: %s. Database names must start with a letter, can be alphanumeric, with underscores and hyphens", newName)
	}

	if oldName == newName {
		return nil, fmt.Errorf("new database name cannot be the same as the current name")
	}

	return &models.DatabaseCommand{
		ID:              helpers.GenerateUUID(),
		CommandType:     "RENAME",
		DatabaseName:    oldName,
		NewDatabaseName: newName,
		Force:           force,
	}, nil
}

func ParseSelectDatabaseCommand(command string) (*models.DatabaseCommand, error) {
	// Regular expression to extract database name
	databaseNameRegex := regexp.MustCompile(`SELECT DATABASES\s+"([^"]+)"`)
	matches := databaseNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid SELECT DATABASES command syntax")
	}
	databaseName := matches[1]

	return &models.DatabaseCommand{
		DatabaseName: databaseName,
		CommandType:  "SELECT",
	}, nil
}

// parseBool converts string "true"/"false" to bool
// func parseBool(value string) bool {
// 	value = strings.ToLower(strings.TrimSpace(value))
// 	return value == "true"
// }

func IsValidDatabaseName(name string) bool {
	// Regular expression to validate database name
	// Must start with a letter, can contain letters, numbers, underscores, and hyphens
	validNameRegex := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)
	return validNameRegex.MatchString(name)
}
func IsValidBundleName(name string) bool {
	// Regular expression to validate bundle name
	// Must start with a letter, can contain letters, numbers, underscores, and hyphens
	validNameRegex := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)
	return validNameRegex.MatchString(name)
}
func IsValidFieldName(name string) bool {
	// Regular expression to validate field name
	// Must start with a letter, can contain letters, numbers, underscores, and hyphens
	validNameRegex := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)
	return validNameRegex.MatchString(name)
}
func IsValidRelationshipName(name string) bool {
	// Regular expression to validate relationship name
	// Must start with a letter, can contain letters, numbers, underscores, and hyphens
	validNameRegex := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)
	return validNameRegex.MatchString(name)
}
