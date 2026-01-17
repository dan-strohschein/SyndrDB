package server

import (
	"fmt"
	"regexp"
	"strings"

	"syndrdb/src/pkg/errors"
)

// convertParserError converts a parser error to a detailed SyndrDBError
// Extracts location information, suggestions, and provides actionable guidance
func convertParserError(parseErr error, command string) errors.SyndrDBError {
	if parseErr == nil {
		return nil
	}

	errMsg := parseErr.Error()

	// Extract line/column information from error message if available
	// Pattern: "at line X, column Y" or "line X, column Y"
	location := extractLocation(errMsg)

	// Extract unexpected token/keyword if mentioned
	unexpected := extractUnexpected(errMsg)

	// Extract expected token/keyword if mentioned
	expected := extractExpected(errMsg)

	// Build suggestions
	suggestions := buildSuggestions(errMsg, unexpected, expected)

	// Build correct example (try to suggest a fix)
	correctExample := buildCorrectExample(command, unexpected, expected)

	// Build submitted input snippet (show context around error)
	submittedInput := command
	if len(command) > 150 {
		// Truncate very long commands, but try to show relevant part
		submittedInput = command[:75] + "..." + command[len(command)-75:]
	}

	// Build detailed user message
	userMessage := errMsg
	if unexpected != "" && expected != "" {
		userMessage = fmt.Sprintf("Syntax error in command: unexpected '%s'", unexpected)
		if expected != "" {
			userMessage += fmt.Sprintf(", expected '%s'", expected)
		}
	} else if strings.Contains(strings.ToLower(errMsg), "syntax") {
		// Keep original message if it mentions syntax
		userMessage = errMsg
	} else {
		userMessage = "Syntax error: " + errMsg
	}

	details := &errors.ValidationErrorDetails{
		SubmittedInput: submittedInput,
		ExpectedFormat: expected,
		Location:       location,
		CorrectExample: correctExample,
		Suggestions:    suggestions,
	}

	return errors.NewValidationError(
		errors.ERR_VALIDATION_SYNTAX,
		userMessage,
		errors.LayerParser,
		details,
	)
}

// extractLocation extracts line/column information from error message
// Looks for patterns like "at line 1, column 10" or "line 1, column 10"
func extractLocation(errMsg string) string {
	// Pattern 1: "at line X, column Y"
	pattern1 := regexp.MustCompile(`(?i)at\s+line\s+(\d+),\s*column\s+(\d+)`)
	matches := pattern1.FindStringSubmatch(errMsg)
	if len(matches) == 3 {
		return fmt.Sprintf("line %s, column %s", matches[1], matches[2])
	}

	// Pattern 2: "line X, column Y" (without "at")
	pattern2 := regexp.MustCompile(`(?i)line\s+(\d+),\s*column\s+(\d+)`)
	matches = pattern2.FindStringSubmatch(errMsg)
	if len(matches) == 3 {
		return fmt.Sprintf("line %s, column %s", matches[1], matches[2])
	}

	// Pattern 3: Just "line X"
	pattern3 := regexp.MustCompile(`(?i)line\s+(\d+)`)
	matches = pattern3.FindStringSubmatch(errMsg)
	if len(matches) == 2 {
		return fmt.Sprintf("line %s", matches[1])
	}

	return ""
}

// extractUnexpected extracts the unexpected token/keyword from error message
// Patterns: "got X", "unexpected X", "unexpected token X"
func extractUnexpected(errMsg string) string {
	// Pattern 1: "got X"
	pattern1 := regexp.MustCompile(`(?i)got\s+['"]?([^'"\s,;:]+)['"]?`)
	matches := pattern1.FindStringSubmatch(errMsg)
	if len(matches) >= 2 {
		return matches[1]
	}

	// Pattern 2: "unexpected X"
	pattern2 := regexp.MustCompile(`(?i)unexpected\s+(?:token\s+)?['"]?([^'"\s,;:]+)['"]?`)
	matches = pattern2.FindStringSubmatch(errMsg)
	if len(matches) >= 2 {
		return matches[1]
	}

	return ""
}

// extractExpected extracts the expected token/keyword from error message
// Patterns: "expected X", "expecting X"
func extractExpected(errMsg string) string {
	// Pattern 1: "expected X"
	pattern1 := regexp.MustCompile(`(?i)expected\s+['"]?([^'"\s,;:]+)['"]?`)
	matches := pattern1.FindStringSubmatch(errMsg)
	if len(matches) >= 2 {
		return matches[1]
	}

	// Pattern 2: "expecting X"
	pattern2 := regexp.MustCompile(`(?i)expecting\s+['"]?([^'"\s,;:]+)['"]?`)
	matches = pattern2.FindStringSubmatch(errMsg)
	if len(matches) >= 2 {
		return matches[1]
	}

	return ""
}

// buildSuggestions builds helpful suggestions based on the error
func buildSuggestions(errMsg, unexpected, expected string) []string {
	var suggestions []string

	// Common typos/misspellings
	if unexpected != "" && expected != "" {
		suggestions = append(suggestions, fmt.Sprintf("Did you mean '%s'?", expected))
	}

	// Check for common SQL typos
	if strings.ToUpper(unexpected) == "FORM" {
		suggestions = append(suggestions, "Did you mean 'FROM'?")
	}
	if strings.ToUpper(unexpected) == "WHER" {
		suggestions = append(suggestions, "Did you mean 'WHERE'?")
	}
	if strings.ToUpper(unexpected) == "SELET" || strings.ToUpper(unexpected) == "SELCT" {
		suggestions = append(suggestions, "Did you mean 'SELECT'?")
	}

	// Add command-specific help if we can detect the command type
	if strings.Contains(strings.ToUpper(errMsg), "SELECT") {
		suggestions = append(suggestions, "For SELECT syntax: SELECT [fields] FROM BUNDLE \"bundle_name\" [WHERE conditions];")
	}
	if strings.Contains(strings.ToUpper(errMsg), "INSERT") {
		suggestions = append(suggestions, "For INSERT syntax: INSERT INTO BUNDLE \"bundle_name\" VALUES {...};")
	}

	return suggestions
}

// buildCorrectExample attempts to build a correct example by fixing common errors
func buildCorrectExample(command, unexpected, expected string) string {
	if unexpected == "" || expected == "" {
		return ""
	}

	// Try to replace the unexpected token with the expected one
	corrected := strings.Replace(command, unexpected, expected, 1)
	if corrected == command {
		// Case-insensitive replacement
		corrected = replaceCaseInsensitive(command, unexpected, expected)
	}

	// Don't return if nothing changed or if the result is too different
	if corrected == command || len(corrected) != len(command) {
		return ""
	}

	return corrected
}

// replaceCaseInsensitive replaces a substring case-insensitively
func replaceCaseInsensitive(s, old, new string) string {
	// Simple approach: find first occurrence and replace
	lowerS := strings.ToLower(s)
	lowerOld := strings.ToLower(old)

	idx := strings.Index(lowerS, lowerOld)
	if idx == -1 {
		return s
	}

	return s[:idx] + new + s[idx+len(old):]
}
