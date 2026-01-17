package errors

import (
	"fmt"
	"strings"
)

// New creates a new SyndrDBError with the specified code, user message, and layer
// This is the primary constructor for creating errors
func New(code ErrorCode, userMessage string, layer ErrorLayer) SyndrDBError {
	return NewWithSeverity(code, userMessage, layer, SeverityError)
}

// NewWithSeverity creates a new SyndrDBError with severity specified
func NewWithSeverity(code ErrorCode, userMessage string, layer ErrorLayer, severity ErrorSeverity) SyndrDBError {
	internalMessage := userMessage // Default: same as user message

	// For internal errors, enhance the internal message
	if code.IsInternalError() {
		internalMessage = fmt.Sprintf("[%s] %s", code, userMessage)
	}

	return &syndrDBError{
		code:            code,
		userMessage:     userMessage,
		internalMessage: internalMessage,
		severity:        severity,
		layer:           layer,
		stackTrace:      captureStackTrace(3), // Skip: captureStackTrace, NewWithSeverity, caller
		context:         make(map[string]string),
	}
}

// NewValidationError creates a detailed validation error with full context
// This constructor enforces the requirement that validation errors MUST be detailed and actionable
func NewValidationError(
	code ErrorCode,
	userMessage string,
	layer ErrorLayer,
	details *ValidationErrorDetails,
) SyndrDBError {
	// Build a detailed user message from validation details if provided
	detailedMessage := userMessage
	if details != nil {
		detailedMessage = buildDetailedValidationMessage(userMessage, details)
	}

	// Build internal message with full context
	internalMessage := fmt.Sprintf("[%s] %s", code, userMessage)
	if details != nil {
		internalParts := []string{internalMessage}
		if details.Location != "" {
			internalParts = append(internalParts, fmt.Sprintf("Location: %s", details.Location))
		}
		if details.SubmittedInput != "" {
			internalParts = append(internalParts, fmt.Sprintf("Submitted: %s", details.SubmittedInput))
		}
		if details.ExpectedFormat != "" {
			internalParts = append(internalParts, fmt.Sprintf("Expected: %s", details.ExpectedFormat))
		}
		internalMessage = strings.Join(internalParts, " | ")
	}

	err := &syndrDBError{
		code:              code,
		userMessage:       detailedMessage,
		internalMessage:   internalMessage,
		severity:          SeverityError,
		layer:             layer,
		stackTrace:        captureStackTrace(3), // Skip: captureStackTrace, NewValidationError, caller
		context:           make(map[string]string),
		validationDetails: details,
	}

	return err
}

// buildDetailedValidationMessage constructs a detailed, actionable validation error message
// This ensures validation errors always provide helpful guidance to developers
func buildDetailedValidationMessage(baseMessage string, details *ValidationErrorDetails) string {
	var parts []string

	// Start with the base message
	parts = append(parts, baseMessage)

	// Add location if available
	if details.Location != "" {
		parts = append(parts, fmt.Sprintf("at %s", details.Location))
	}

	// Add submitted input snippet (show context around error)
	if details.SubmittedInput != "" {
		parts = append(parts, fmt.Sprintf("\nYour command: %s", details.SubmittedInput))
	}

	// Add suggestions
	if len(details.Suggestions) > 0 {
		parts = append(parts, "\n"+strings.Join(details.Suggestions, "\n"))
	}

	// Add correct example
	if details.CorrectExample != "" {
		parts = append(parts, fmt.Sprintf("Correct syntax: %s", details.CorrectExample))
	}

	// Add available options if provided
	if len(details.AvailableOptions) > 0 {
		parts = append(parts, fmt.Sprintf("Available options: %s", strings.Join(details.AvailableOptions, ", ")))
	}

	return strings.Join(parts, "\n")
}

// Wrap wraps a standard error into a SyndrDBError
// If err is already a SyndrDBError, returns it as-is
// Otherwise, creates a new SyndrDBError with the error message
func Wrap(err error, code ErrorCode, layer ErrorLayer) SyndrDBError {
	if err == nil {
		return nil
	}

	// If it's already a SyndrDBError, return it
	if sdbErr, ok := err.(SyndrDBError); ok {
		return sdbErr
	}

	// Extract user-friendly message from error
	userMessage := err.Error()
	internalMessage := fmt.Sprintf("[%s] %s", code, userMessage)

	return &syndrDBError{
		code:            code,
		userMessage:     userMessage,
		internalMessage: internalMessage,
		severity:        SeverityError,
		layer:           layer,
		cause:           err,
		stackTrace:      captureStackTrace(3), // Skip: captureStackTrace, Wrap, caller
		context:         make(map[string]string),
	}
}

// WrapWithMessage wraps a standard error with a custom user message
func WrapWithMessage(err error, code ErrorCode, userMessage string, layer ErrorLayer) SyndrDBError {
	if err == nil {
		return nil
	}

	// If it's already a SyndrDBError, enhance it
	if sdbErr, ok := err.(SyndrDBError); ok {
		// Create new error with provided message but preserve other details
		return NewWithCause(code, userMessage, layer, sdbErr)
	}

	internalMessage := fmt.Sprintf("[%s] %s (caused by: %v)", code, userMessage, err)

	return &syndrDBError{
		code:            code,
		userMessage:     userMessage,
		internalMessage: internalMessage,
		severity:        SeverityError,
		layer:           layer,
		cause:           err,
		stackTrace:      captureStackTrace(3), // Skip: captureStackTrace, WrapWithMessage, caller
		context:         make(map[string]string),
	}
}

// NewWithCause creates a new error with a cause
func NewWithCause(code ErrorCode, userMessage string, layer ErrorLayer, cause error) SyndrDBError {
	internalMessage := fmt.Sprintf("[%s] %s (caused by: %v)", code, userMessage, cause)
	if code.IsInternalError() {
		internalMessage = fmt.Sprintf("[%s] %s", code, userMessage)
	}

	return &syndrDBError{
		code:            code,
		userMessage:     userMessage,
		internalMessage: internalMessage,
		severity:        SeverityError,
		layer:           layer,
		cause:           cause,
		stackTrace:      captureStackTrace(3), // Skip: captureStackTrace, NewWithCause, caller
		context:         make(map[string]string),
	}
}

// ConvertError converts a standard error to SyndrDBError
// If err is nil, returns nil
// If err is already a SyndrDBError, returns it
// Otherwise, wraps it with ERR_INTERNAL
func ConvertError(err error, layer ErrorLayer) SyndrDBError {
	if err == nil {
		return nil
	}

	if sdbErr, ok := err.(SyndrDBError); ok {
		return sdbErr
	}

	return Wrap(err, ERR_INTERNAL, layer)
}

// NewSyntaxError creates a detailed syntax validation error
// This is a convenience function for parser errors that should include full context
func NewSyntaxError(
	command string,
	location string, // "line 1, column 10"
	unexpected string,
	expected string,
	suggestions []string,
) SyndrDBError {
	// Build submitted input snippet (show context around error)
	submittedInput := command
	if len(command) > 100 {
		// Truncate long commands, show first and last parts
		submittedInput = command[:50] + "..." + command[len(command)-50:]
	}

	// Build suggestions if not provided
	if len(suggestions) == 0 && unexpected != "" && expected != "" {
		suggestions = []string{fmt.Sprintf("Did you mean '%s'?", expected)}
	}

	// Build correct example (try to replace the unexpected part)
	correctExample := command
	if unexpected != "" && expected != "" {
		correctExample = strings.Replace(command, unexpected, expected, 1)
	}

	details := &ValidationErrorDetails{
		SubmittedInput: submittedInput,
		ExpectedFormat: expected,
		Location:       location,
		CorrectExample: correctExample,
		Suggestions:    suggestions,
	}

	userMessage := fmt.Sprintf("Syntax error in command: unexpected '%s'", unexpected)
	if expected != "" {
		userMessage += fmt.Sprintf(", expected '%s'", expected)
	}

	return NewValidationError(ERR_VALIDATION_SYNTAX, userMessage, LayerParser, details)
}
