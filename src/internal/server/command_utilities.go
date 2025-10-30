package server

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// parseBundleNameFromCommand extracts the bundle name from UPDATE/DELETE commands
// This function follows the Single Responsibility Principle by handling only bundle name extraction
// Following SyndrDB comprehensive error handling, it properly handles quoted strings and multi-line commands
// Parameters:
//   - command: The full command string to parse
//   - keyword: The keyword to look for ("IN" for UPDATE, "FROM" for DELETE)
//
// Returns:
//   - string: The extracted bundle name without quotes
//   - error: Any error that occurred during parsing
func parseBundleNameFromCommand(command, keyword string) (string, error) {
	// Normalize the command by removing extra whitespace and newlines
	// Following SyndrDB data integrity requirements, ensure consistent parsing
	normalizedCommand := strings.ReplaceAll(command, "\n", " ")
	normalizedCommand = strings.ReplaceAll(normalizedCommand, "\r", " ")
	normalizedCommand = strings.ReplaceAll(normalizedCommand, "\t", " ")

	// Collapse multiple spaces into single spaces
	for strings.Contains(normalizedCommand, "  ") {
		normalizedCommand = strings.ReplaceAll(normalizedCommand, "  ", " ")
	}
	normalizedCommand = strings.TrimSpace(normalizedCommand)

	// Find the position of the keyword (case-insensitive)
	keywordUpper := strings.ToUpper(keyword)
	commandUpper := strings.ToUpper(normalizedCommand)
	keywordPos := strings.Index(commandUpper, keywordUpper)

	if keywordPos == -1 {
		return "", fmt.Errorf("keyword '%s' not found in command", keyword)
	}

	// Extract the part after the keyword
	afterKeyword := normalizedCommand[keywordPos+len(keyword):]
	afterKeyword = strings.TrimSpace(afterKeyword)

	// Look for "BUNDLE" keyword after the main keyword
	bundleUpper := "BUNDLE"
	bundlePos := strings.Index(strings.ToUpper(afterKeyword), bundleUpper)

	if bundlePos == -1 {
		return "", fmt.Errorf("'BUNDLE' keyword not found after '%s'", keyword)
	}

	// Extract the part after "BUNDLE"
	afterBundle := afterKeyword[bundlePos+len(bundleUpper):]
	afterBundle = strings.TrimSpace(afterBundle)

	// Find the quoted bundle name
	bundleName, err := extractQuotedString(afterBundle)
	if err != nil {
		return "", fmt.Errorf("failed to extract bundle name: %w", err)
	}

	return bundleName, nil
}

// extractQuotedString extracts a quoted string from the beginning of a text
// This function follows the Single Responsibility Principle by handling only quoted string extraction
// Following SyndrDB comprehensive error handling, it supports multiple quote types
// Parameters:
//   - text: The text to extract the quoted string from
//
// Returns:
//   - string: The extracted string without quotes
//   - error: Any error that occurred during extraction
func extractQuotedString(text string) (string, error) {
	text = strings.TrimSpace(text)

	if len(text) == 0 {
		return "", fmt.Errorf("empty text provided for quote extraction")
	}

	// Check for different quote types
	quoteChars := []rune{'"', '\'', '"', '"'} // Regular quotes and smart quotes

	for _, quoteChar := range quoteChars {
		if rune(text[0]) == quoteChar {
			// Find the closing quote
			for i := 1; i < len(text); i++ {
				if rune(text[i]) == quoteChar {
					// Found closing quote
					return text[1:i], nil
				}
			}
			return "", fmt.Errorf("unterminated quoted string starting with %c", quoteChar)
		}
	}

	// If no quotes found, look for the first word (until space or special character)
	words := strings.Fields(text)
	if len(words) > 0 {
		// Find where the first word ends (before parentheses, WHERE, etc.)
		firstWord := words[0]
		stopChars := []string{"(", "WHERE", "SET"}

		for _, stopChar := range stopChars {
			if idx := strings.Index(strings.ToUpper(text), stopChar); idx != -1 {
				beforeStop := strings.TrimSpace(text[:idx])
				if beforeStop != "" {
					return beforeStop, nil
				}
			}
		}

		return firstWord, nil
	}

	return "", fmt.Errorf("no quoted string or valid identifier found in text: %s", text)
}

// parseBundleNameFromShowCommand extracts the bundle name from SHOW BUNDLE "<NAME>" command
func parseBundleNameFromShowCommand(command string) (string, error) {
	// Expected format: SHOW BUNDLE "<BUNDLE_NAME>";
	// Find the quoted bundle name
	re := regexp.MustCompile(`(?i)show\s+bundle\s+"([^"]+)"`)
	matches := re.FindStringSubmatch(command)

	if len(matches) < 2 {
		return "", fmt.Errorf("invalid SHOW BUNDLE command format. Expected: SHOW BUNDLE \"<bundle_name>\";")
	}

	return matches[1], nil
}

// parseDatabaseNameFromShowBundlesFor extracts the database name from SHOW BUNDLES FOR "<NAME>" command
func parseDatabaseNameFromShowBundlesFor(command string) (string, error) {
	// Expected format: SHOW BUNDLES FOR "<DATABASE_NAME>";
	// Find the quoted database name
	re := regexp.MustCompile(`(?i)show\s+bundles\s+for\s+"([^"]+)"`)
	matches := re.FindStringSubmatch(command)

	if len(matches) < 2 {
		return "", fmt.Errorf("invalid SHOW BUNDLES FOR command format. Expected: SHOW BUNDLES FOR \"<database_name>\";")
	}

	return matches[1], nil
}

// parseDatabaseNameFromUse extracts the database name from USE "<DATABASE_NAME>"; command
func parseDatabaseNameFromUse(command string) (string, error) {
	// Expected format: USE "<DATABASE_NAME>";
	// Find the quoted database name
	re := regexp.MustCompile(`(?i)use\s+"([^"]+)"`)
	matches := re.FindStringSubmatch(command)

	if len(matches) < 2 {
		return "", fmt.Errorf("invalid USE command format. Expected: USE \"<database_name>\";")
	}

	return matches[1], nil
}

// parseAttachDatabaseCommand parses the ATTACH DATABASE command to extract file path and database name
func parseAttachDatabaseCommand(command string) (string, string, error) {
	// Expected format: ATTACH DATABASE "<file_path>" "<database_name>";
	// Use regex to extract both quoted strings after DATABASE keyword
	re := regexp.MustCompile(`(?i)attach\s+database\s+"([^"]+)"\s+"([^"]+)"`)
	matches := re.FindStringSubmatch(command)

	if len(matches) < 3 {
		return "", "", fmt.Errorf("invalid ATTACH DATABASE command format. Expected: ATTACH DATABASE \"<file_path>\" \"<database_name>\";")
	}

	filePath := matches[1]
	databaseName := matches[2]

	return filePath, databaseName, nil
}

// generateDatabaseID generates a unique database ID
func generateDatabaseID() string {
	return fmt.Sprintf("db_%d", time.Now().UnixNano())
}

// generateBundleID generates a unique bundle ID
func generateBundleID() string {
	return fmt.Sprintf("bnd_%d", time.Now().UnixNano())
}
