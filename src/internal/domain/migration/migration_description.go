package migration

import (
	"fmt"
	"strings"
	"time"
)

/*
migration_description.go

This file implements automatic migration description generation for SyndrDB.
When users create migrations without manual descriptions, this module analyzes
the first command to extract operation type and target entity, then formats
a standardized description with timestamp.

Description Format:
  "{OPERATION} {ENTITY} {NAME} - {TIMESTAMP}"

Examples:
  - "CREATE BUNDLE Authors - 2024-01-15 14:30:45"
  - "ADD FIELD email TO Users - 2024-01-15 14:31:12"
  - "CREATE H-INDEX idx_email ON Users - 2024-01-15 14:32:00"
  - "DROP BUNDLE OldData - 2024-01-15 14:33:22"

Design Principles:
  - Fixed non-configurable format (user decision during planning)
  - Parse first command only (representative of migration intent)
  - Include timestamp for uniqueness and chronological tracking
  - Fallback to generic description if parsing fails

Parsing Strategy:
  - Tokenize command by whitespace
  - Extract operation verb (CREATE, ADD, DROP, REMOVE, RENAME, etc.)
  - Extract entity type (BUNDLE, FIELD, H-INDEX, B-INDEX, RELATIONSHIP)
  - Extract entity name (identifier after entity type)
  - Handle complex commands (ADD FIELD, CREATE INDEX, etc.)

Limitations:
  - Only analyzes first command (multi-command migrations may be ambiguous)
  - Basic keyword extraction (not full syntax parsing)
  - Non-customizable format (future enhancement if needed)

Performance Characteristics:
  - Parsing: O(1) - only first command analyzed
  - String operations: Negligible overhead (< 1 microsecond)
  - No external dependencies or I/O

TODO: I will add configurable description templates if users request customization
TODO: I will support multi-command summary generation for complex migrations
*/

// GenerateDescription creates an auto-generated description from migration commands
// Analyzes first command to extract operation, entity type, and name
// Returns formatted description: "{OPERATION} {ENTITY} {NAME} - {TIMESTAMP}"
// Falls back to generic description if parsing fails
func GenerateDescription(commands []string) string {
	// Handle empty command list
	if len(commands) == 0 {
		return fmt.Sprintf("Empty migration - %s", formatTimestamp(time.Now()))
	}

	// Get first command and normalize
	firstCommand := strings.TrimSpace(commands[0])
	if firstCommand == "" {
		return fmt.Sprintf("Empty migration - %s", formatTimestamp(time.Now()))
	}

	// Tokenize command
	tokens := tokenizeCommand(firstCommand)
	if len(tokens) == 0 {
		return fmt.Sprintf("Migration - %s", formatTimestamp(time.Now()))
	}

	// Parse operation and entity
	operation, entity, name := parseCommandTokens(tokens)

	// Format description
	if operation != "" && entity != "" && name != "" {
		return fmt.Sprintf("%s %s %s - %s", operation, entity, name, formatTimestamp(time.Now()))
	} else if operation != "" && entity != "" {
		return fmt.Sprintf("%s %s - %s", operation, entity, formatTimestamp(time.Now()))
	} else if operation != "" {
		return fmt.Sprintf("%s - %s", operation, formatTimestamp(time.Now()))
	}

	// Fallback to generic description
	return fmt.Sprintf("Migration - %s", formatTimestamp(time.Now()))
}

// tokenizeCommand splits command into tokens by whitespace
// Handles quoted strings as single tokens
func tokenizeCommand(command string) []string {
	tokens := []string{}
	inQuotes := false
	currentToken := strings.Builder{}

	for _, char := range command {
		if char == '"' || char == '\'' {
			inQuotes = !inQuotes
		} else if char == ' ' || char == '\t' {
			if inQuotes {
				currentToken.WriteRune(char)
			} else if currentToken.Len() > 0 {
				tokens = append(tokens, strings.ToUpper(currentToken.String()))
				currentToken.Reset()
			}
		} else {
			currentToken.WriteRune(char)
		}
	}

	// Add last token
	if currentToken.Len() > 0 {
		tokens = append(tokens, strings.ToUpper(currentToken.String()))
	}

	return tokens
}

// parseCommandTokens extracts operation, entity type, and name from tokens
// Returns (operation, entity, name) tuple
func parseCommandTokens(tokens []string) (string, string, string) {
	if len(tokens) == 0 {
		return "", "", ""
	}

	operation := tokens[0]
	entity := ""
	name := ""

	// Handle compound operations (e.g., "ADD FIELD", "CREATE H-INDEX")
	switch operation {
	case "CREATE":
		if len(tokens) >= 3 {
			// CREATE BUNDLE Authors
			// CREATE H-INDEX idx_email
			// CREATE B-INDEX idx_age
			entity = tokens[1]
			name = tokens[2]
		}

	case "DROP":
		if len(tokens) >= 3 {
			// DROP BUNDLE Authors
			// DROP INDEX idx_email
			entity = tokens[1]
			name = tokens[2]
		}

	case "ADD":
		if len(tokens) >= 3 {
			// ADD FIELD email
			entity = tokens[1]
			name = tokens[2]
		}

	case "REMOVE":
		if len(tokens) >= 3 {
			// REMOVE FIELD email
			entity = tokens[1]
			name = tokens[2]
		}

	case "RENAME":
		if len(tokens) >= 4 {
			// RENAME FIELD old_name TO new_name
			// RENAME BUNDLE OldName TO NewName
			entity = tokens[1]
			name = tokens[2] // Old name
		}

	case "ALTER":
		if len(tokens) >= 3 {
			// ALTER BUNDLE Authors
			entity = tokens[1]
			name = tokens[2]
		}

	case "UPDATE":
		if len(tokens) >= 2 {
			// UPDATE Authors
			entity = "BUNDLE"
			name = tokens[1]
		}

	case "DELETE":
		if len(tokens) >= 3 {
			// DELETE FROM Authors
			entity = "BUNDLE"
			if tokens[1] == "FROM" && len(tokens) >= 3 {
				name = tokens[2]
			}
		}

	case "INSERT":
		if len(tokens) >= 3 {
			// INSERT INTO Authors
			entity = "BUNDLE"
			if tokens[1] == "INTO" && len(tokens) >= 3 {
				name = tokens[2]
			}
		}

	default:
		// Unknown operation
		if len(tokens) >= 2 {
			entity = tokens[1]
		}
		if len(tokens) >= 3 {
			name = tokens[2]
		}
	}

	return operation, entity, name
}

// formatTimestamp formats time in consistent format for descriptions
// Format: "2006-01-02 15:04:05"
func formatTimestamp(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}
