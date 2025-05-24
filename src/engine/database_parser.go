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
package engine

import (
	"fmt"
	"regexp"
	"strings"
	"syndrdb/src/helpers"
)

type DatabaseCommand struct {
	ID                 string
	CommandType        string // CREATE, UPDATE, DELETE
	DatabaseName       string
	DBMetadataFilePath string
}

func ParseCreateDatabaseCommand(command string) (*DatabaseCommand, error) {
	// Regular expression to extract database name
	databaseNameRegex := regexp.MustCompile(`CREATE DATABASE\s+"([^"]+)"`)
	matches := databaseNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid CREATE DATABASE command syntax")
	}
	databaseName := matches[1]

	return &DatabaseCommand{
		ID:                 helpers.GenerateUUID(), // Generate a unique ID for the command
		DatabaseName:       databaseName,
		DBMetadataFilePath: "path/to/metadata/file", // Placeholder for actual metadata file path
		CommandType:        "CREATE",
	}, nil
}

func ParseUpdateDatabaseCommand(command string) (*DatabaseCommand, error) {
	// Regular expression to extract database name
	databaseNameRegex := regexp.MustCompile(`UPDATE DATABASE\s+"([^"]+)"`)
	matches := databaseNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid UPDATE DATABASE command syntax")
	}
	databaseName := matches[1]

	return &DatabaseCommand{

		DatabaseName:       databaseName,
		DBMetadataFilePath: "path/to/metadata/file", // Placeholder for actual metadata file path
		CommandType:        "UPDATE",
	}, nil
}

func ParseDeleteDatabaseCommand(command string) (*DatabaseCommand, error) {
	// Regular expression to extract database name
	databaseNameRegex := regexp.MustCompile(`DELETE DATABASE\s+"([^"]+)"`)
	matches := databaseNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid DELETE DATABASE command syntax")
	}
	databaseName := matches[1]

	return &DatabaseCommand{
		DatabaseName: databaseName,
		CommandType:  "DELETE",
	}, nil
}

func ParseSelectDatabaseCommand(command string) (*DatabaseCommand, error) {
	// Regular expression to extract database name
	databaseNameRegex := regexp.MustCompile(`SELECT DATABASES\s+"([^"]+)"`)
	matches := databaseNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid SELECT DATABASES command syntax")
	}
	databaseName := matches[1]

	return &DatabaseCommand{
		DatabaseName: databaseName,
		CommandType:  "SELECT",
	}, nil
}

// parseBool converts string "true"/"false" to bool
func parseBool(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == "true"
}
