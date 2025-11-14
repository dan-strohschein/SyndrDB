package syndrQL

/*
migration_parser.go

This file implements the SyndrQL parser for migration commands.
Parses 5 command types for database versioning and schema migration tracking.

Supported Commands:
  1. START MIGRATION [WITH DESCRIPTION "..."] <commands> COMMIT
  2. SHOW MIGRATIONS FOR "<database>"
  3. APPLY MIGRATION WITH VERSION <number> [FORCE]
  4. VALIDATE MIGRATION WITH VERSION <number>
  5. VALIDATE ROLLBACK TO VERSION <number>

Parser Integration:
  - Uses existing tokenizer from tokenizer.go
  - Returns AST nodes for command director routing
  - Validates syntax only (semantic validation in migration_validator.go)

TODO: Implement full parser once command_director integration is ready
TODO: Add support for WHERE clause in SHOW MIGRATIONS
TODO: Add support for ORDER BY in SHOW MIGRATIONS
*/

// MigrationNode represents a parsed migration command
type MigrationNode struct {
	Type         string // "START", "SHOW", "APPLY", "VALIDATE_MIGRATION", "VALIDATE_ROLLBACK"
	DatabaseName string
	Version      int
	Description  string
	Commands     []string
	Force        bool
}

// Placeholder - will be fully implemented in next iteration
func ParseMigrationCommand(tokens []Token) (*MigrationNode, error) {
	// TODO: Implement parser
	return nil, nil
}
