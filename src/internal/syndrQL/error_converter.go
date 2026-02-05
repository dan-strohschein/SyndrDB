package syndrQL

import (
	"fmt"
	"strings"

	"syndrdb/src/pkg/errors"
)

// ConvertParserError converts a parser error to a detailed SyndrDBError.
// Uses token information (line/column) if available for detailed validation messages.
// command should be the original user input when available; otherwise use
// reconstructCommandFromTokens(tokens) (approximate).
func ConvertParserError(parseErr error, tokens []Token, currentTokenIndex int, command string) errors.SyndrDBError {
	if parseErr == nil {
		return nil
	}

	errMsg := parseErr.Error()

	// Extract location from current token if available
	var location string
	var unexpected, expected string
	var suggestions []string

	if currentTokenIndex >= 0 && currentTokenIndex < len(tokens) {
		token := tokens[currentTokenIndex]
		if token.Line > 0 || token.Column > 0 {
			if token.Column > 0 {
				location = fmt.Sprintf("line %d, column %d", token.Line, token.Column)
			} else {
				location = fmt.Sprintf("line %d", token.Line)
			}
		}
		unexpected = token.Value
	}

	// Extract expected token/keyword from error message
	// Patterns: "expected X, got Y" or "expected X"
	if strings.Contains(errMsg, "expected") {
		parts := strings.Split(errMsg, "expected")
		if len(parts) > 1 {
			expectedPart := strings.TrimSpace(parts[1])
			// Remove "got" part if present
			if gotIdx := strings.Index(expectedPart, ", got"); gotIdx > 0 {
				expected = strings.TrimSpace(expectedPart[:gotIdx])
			} else {
				expected = strings.TrimSpace(expectedPart)
			}
		}
	}

	// Build suggestions for common typos
	if unexpected != "" && expected != "" {
		suggestions = buildParserSuggestions(unexpected, expected)
	}

	// Build correct example (try to suggest a fix)
	correctExample := buildCorrectExampleFromCommand(command, unexpected, expected)

	// Build submitted input snippet
	submittedInput := command
	if len(command) > 150 {
		submittedInput = command[:75] + "..." + command[len(command)-75:]
	}

	// Build detailed user message
	userMessage := errMsg
	if unexpected != "" && expected != "" {
		userMessage = fmt.Sprintf("Syntax error: unexpected '%s', expected '%s'", unexpected, expected)
	} else if strings.Contains(strings.ToLower(errMsg), "syntax") || strings.Contains(strings.ToLower(errMsg), "expected") {
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

// buildParserSuggestions builds helpful suggestions based on unexpected/expected tokens
func buildParserSuggestions(unexpected, expected string) []string {
	var suggestions []string

	// Common SQL typos
	unexpectedUpper := strings.ToUpper(unexpected)
	expectedUpper := strings.ToUpper(expected)

	if unexpectedUpper == "FORM" && expectedUpper == "FROM" {
		suggestions = append(suggestions, "Did you mean 'FROM'?")
	}
	if unexpectedUpper == "WHER" && expectedUpper == "WHERE" {
		suggestions = append(suggestions, "Did you mean 'WHERE'?")
	}
	if unexpectedUpper == "SELET" || unexpectedUpper == "SELCT" {
		if expectedUpper == "SELECT" {
			suggestions = append(suggestions, "Did you mean 'SELECT'?")
		}
	}

	// General suggestion if different
	if unexpectedUpper != expectedUpper && expected != "" {
		suggestions = append(suggestions, fmt.Sprintf("Did you mean '%s'?", expected))
	}

	// Add command-specific help based on expected token
	if expectedUpper == "FROM" {
		suggestions = append(suggestions, "Syntax: SELECT [fields] FROM BUNDLE \"bundle_name\" [WHERE conditions];")
	}
	if expectedUpper == "WHERE" {
		suggestions = append(suggestions, "Syntax: SELECT * FROM BUNDLE \"name\" WHERE field = value;")
	}

	return suggestions
}

// buildCorrectExampleFromCommand attempts to build a correct example
func buildCorrectExampleFromCommand(command, unexpected, expected string) string {
	if unexpected == "" || expected == "" {
		return ""
	}

	// Try to replace the unexpected token with the expected one (case-insensitive)
	corrected := replaceCaseInsensitive(command, unexpected, expected)
	if corrected == command {
		return "" // Nothing changed
	}

	return corrected
}

// replaceCaseInsensitive replaces a substring case-insensitively
func replaceCaseInsensitive(s, old, new string) string {
	lowerS := strings.ToLower(s)
	lowerOld := strings.ToLower(old)

	idx := strings.Index(lowerS, lowerOld)
	if idx == -1 {
		return s
	}

	// Replace at the found index with the new value
	return s[:idx] + new + s[idx+len(old):]
}

// reconstructCommandFromTokens reconstructs the command string from tokens for error reporting
func reconstructCommandFromTokens(tokens []Token) string {
	// Simple reconstruction - just join token values with spaces
	parts := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token.Type != TOKEN_EOF {
			parts = append(parts, token.Value)
		}
	}
	return strings.Join(parts, " ")
}

// CreateParserErrorWithToken creates a detailed parser error with token context.
// This is the preferred way to create parser errors with line/column information.
// command should be the original user input when available; otherwise use
// reconstructCommandFromTokens (approximate).
func CreateParserErrorWithToken(
	msg string,
	token Token,
	expected string,
	command string,
) errors.SyndrDBError {
	location := ""
	if token.Line > 0 || token.Column > 0 {
		if token.Column > 0 {
			location = fmt.Sprintf("line %d, column %d", token.Line, token.Column)
		} else {
			location = fmt.Sprintf("line %d", token.Line)
		}
	}

	suggestions := buildParserSuggestions(token.Value, expected)

	details := &errors.ValidationErrorDetails{
		SubmittedInput: command,
		ExpectedFormat: expected,
		Location:       location,
		Suggestions:    suggestions,
	}

	return errors.NewValidationError(
		errors.ERR_VALIDATION_SYNTAX,
		msg,
		errors.LayerParser,
		details,
	)
}
