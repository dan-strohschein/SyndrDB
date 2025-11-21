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

// ParseRenameDatabaseCommand parses a RENAME DATABASE command
// Expected formats:
//   - RENAME DATABASE "old_name" TO "new_name"
//   - RENAME DATABASE "old_name" TO "new_name" FORCE
//
// Returns a DatabaseCommand with:
//   - CommandType: "RENAME"
//   - DatabaseName: old database name
//   - NewDatabaseName: new database name
//   - Force: true if FORCE keyword is present
func ParseRenameDatabaseCommand(command string) (*models.DatabaseCommand, error) {
	// Regular expression to extract old name, new name, and optional FORCE keyword
	// Pattern: RENAME DATABASE "old_name" TO "new_name" [FORCE]
	renameRegex := regexp.MustCompile(`RENAME\s+DATABASE\s+"([^"]+)"\s+TO\s+"([^"]+)"`)
	matches := renameRegex.FindStringSubmatch(command)

	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid RENAME DATABASE command syntax. Expected: RENAME DATABASE \"old_name\" TO \"new_name\" [FORCE]")
	}

	oldName := matches[1]
	newName := matches[2]

	// Check if FORCE keyword is present (case-insensitive)
	force := strings.Contains(strings.ToUpper(command), "FORCE")

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
