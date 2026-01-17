package errors

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// SanitizeError sanitizes an error for user-facing output
// Removes file paths, internal IDs, stack traces, and other sensitive information
// Returns a sanitized SyndrDBError
func SanitizeError(err error) SyndrDBError {
	if err == nil {
		return nil
	}

	// If it's already a SyndrDBError, sanitize its user message
	if sdbErr, ok := err.(SyndrDBError); ok {
		return sanitizeSyndrDBError(sdbErr)
	}

	// For standard errors, wrap them
	userMessage := sanitizeErrorMessage(err.Error())
	return New(ERR_INTERNAL, userMessage, LayerAPI)
}

// sanitizeSyndrDBError sanitizes a SyndrDBError's user message
func sanitizeSyndrDBError(err SyndrDBError) SyndrDBError {
	// User message should already be sanitized, but ensure it is
	userMsg := sanitizeErrorMessage(err.UserMessage())

	// Create a new error with sanitized message
	sanitized := &syndrDBError{
		code:              err.Code(),
		userMessage:       userMsg,
		internalMessage:   err.InternalMessage(), // Keep internal message as-is
		severity:          err.Severity(),
		layer:             err.Layer(),
		cause:             err.Cause(),
		stackTrace:        err.StackTrace(), // Keep stack trace for internal logging
		context:           make(map[string]string),
		validationDetails: err.ValidationDetails(), // Keep validation details
	}

	// Sanitize context (remove sensitive keys)
	for k, v := range err.GetContext() {
		if !isSensitiveContextKey(k) {
			sanitized.context[k] = sanitizeErrorMessage(v)
		}
	}

	return sanitized
}

// sanitizeErrorMessage removes sensitive information from error messages
func sanitizeErrorMessage(msg string) string {
	if msg == "" {
		return msg
	}

	// Remove absolute file paths
	msg = removeFilePaths(msg)

	// Remove internal IDs (UUIDs, document IDs, etc.)
	msg = removeInternalIDs(msg)

	// Remove stack traces (already handled separately, but double-check)
	msg = removeStackTraceReferences(msg)

	return msg
}

// removeFilePaths removes file paths from error messages
func removeFilePaths(msg string) string {
	// Match common file path patterns
	// Unix/Windows absolute paths
	pathPattern := regexp.MustCompile(`(/[^\s:]+|\\[A-Z]:\\[^\s:]+)`)
	msg = pathPattern.ReplaceAllStringFunc(msg, func(match string) string {
		// Replace with just the filename
		return filepath.Base(match)
	})

	// Also remove paths that look like data_dir structures
	dataDirPattern := regexp.MustCompile(`(\.\/)?(data_files|data_dir|log_files|temp_files)[^\s]*`)
	msg = dataDirPattern.ReplaceAllString(msg, "[data directory]")

	return msg
}

// removeInternalIDs removes internal IDs from error messages
func removeInternalIDs(msg string) string {
	// Remove UUIDs
	uuidPattern := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	msg = uuidPattern.ReplaceAllString(msg, "[id]")

	// Remove document IDs (doc_ followed by numbers)
	docIDPattern := regexp.MustCompile(`doc_\d+`)
	msg = docIDPattern.ReplaceAllString(msg, "[document_id]")

	// Remove session IDs
	sessionIDPattern := regexp.MustCompile(`session_[a-zA-Z0-9]+`)
	msg = sessionIDPattern.ReplaceAllString(msg, "[session_id]")

	return msg
}

// removeStackTraceReferences removes references to stack traces in messages
func removeStackTraceReferences(msg string) string {
	// Remove "stack trace:" or similar patterns
	stackRefPattern := regexp.MustCompile(`(?i)stack\s+trace:?\s*`)
	msg = stackRefPattern.ReplaceAllString(msg, "")

	// Remove goroutine references
	goroutinePattern := regexp.MustCompile(`goroutine\s+\d+`)
	msg = goroutinePattern.ReplaceAllString(msg, "[goroutine]")

	return msg
}

// isSensitiveContextKey returns true if a context key contains sensitive information
func isSensitiveContextKey(key string) bool {
	sensitiveKeys := []string{
		"file_path", "filepath", "path",
		"password", "password_hash",
		"session_id", "token",
		"internal_id", "document_id",
		"stack_trace",
	}

	keyLower := strings.ToLower(key)
	for _, sensitive := range sensitiveKeys {
		if strings.Contains(keyLower, sensitive) {
			return true
		}
	}
	return false
}

// FormatUserResponse formats a SyndrDBError for client response
// Returns a map suitable for JSON serialization
func FormatUserResponse(err SyndrDBError) map[string]interface{} {
	if err == nil {
		return nil
	}

	response := map[string]interface{}{
		"error_code": err.Code().String(),
		"message":    err.UserMessage(),
		"layer":      err.Layer().String(),
	}

	// Add validation details if available
	if details := err.ValidationDetails(); details != nil {
		validationInfo := map[string]interface{}{}
		if details.Location != "" {
			validationInfo["location"] = details.Location
		}
		if details.CorrectExample != "" {
			validationInfo["correct_example"] = details.CorrectExample
		}
		if len(details.Suggestions) > 0 {
			validationInfo["suggestions"] = details.Suggestions
		}
		if len(details.AvailableOptions) > 0 {
			validationInfo["available_options"] = details.AvailableOptions
		}
		if len(validationInfo) > 0 {
			response["validation"] = validationInfo
		}
	}

	return response
}

// FormatInternalLog formats a SyndrDBError for internal logging
// Includes full details, stack traces, and context
func FormatInternalLog(err SyndrDBError) map[string]interface{} {
	if err == nil {
		return nil
	}

	logData := map[string]interface{}{
		"error_code":       err.Code().String(),
		"user_message":     err.UserMessage(),
		"internal_message": err.InternalMessage(),
		"severity":         err.Severity().String(),
		"layer":            err.Layer().String(),
	}

	if err.Cause() != nil {
		logData["cause"] = err.Cause().Error()
	}

	if err.StackTrace() != "" {
		logData["stack_trace"] = err.StackTrace()
	}

	// Add context
	if context := err.GetContext(); len(context) > 0 {
		logData["context"] = context
	}

	// Add validation details if available
	if details := err.ValidationDetails(); details != nil {
		validationInfo := map[string]interface{}{
			"submitted_input": details.SubmittedInput,
			"expected_format": details.ExpectedFormat,
			"location":        details.Location,
			"correct_example": details.CorrectExample,
		}
		if len(details.Suggestions) > 0 {
			validationInfo["suggestions"] = details.Suggestions
		}
		if len(details.AvailableOptions) > 0 {
			validationInfo["available_options"] = details.AvailableOptions
		}
		logData["validation_details"] = validationInfo
	}

	return logData
}

// FormatConsoleOutput formats a SyndrDBError for console output in debug mode
// Includes full details with formatted output for readability
func FormatConsoleOutput(err SyndrDBError) string {
	if err == nil {
		return ""
	}

	var parts []string

	// Header
	parts = append(parts, fmt.Sprintf("=== ERROR [%s] ===", err.Code()))
	parts = append(parts, fmt.Sprintf("Layer: %s | Severity: %s", err.Layer(), err.Severity()))
	parts = append(parts, "")

	// User message
	parts = append(parts, "User Message:")
	parts = append(parts, "  "+err.UserMessage())
	parts = append(parts, "")

	// Internal message (if different)
	if err.InternalMessage() != err.UserMessage() {
		parts = append(parts, "Internal Details:")
		parts = append(parts, "  "+err.InternalMessage())
		parts = append(parts, "")
	}

	// Validation details
	if details := err.ValidationDetails(); details != nil {
		parts = append(parts, "Validation Details:")
		if details.Location != "" {
			parts = append(parts, fmt.Sprintf("  Location: %s", details.Location))
		}
		if details.SubmittedInput != "" {
			parts = append(parts, fmt.Sprintf("  Submitted: %s", details.SubmittedInput))
		}
		if details.ExpectedFormat != "" {
			parts = append(parts, fmt.Sprintf("  Expected: %s", details.ExpectedFormat))
		}
		if details.CorrectExample != "" {
			parts = append(parts, fmt.Sprintf("  Correct: %s", details.CorrectExample))
		}
		if len(details.Suggestions) > 0 {
			parts = append(parts, "  Suggestions:")
			for _, s := range details.Suggestions {
				parts = append(parts, fmt.Sprintf("    - %s", s))
			}
		}
		parts = append(parts, "")
	}

	// Context
	if context := err.GetContext(); len(context) > 0 {
		parts = append(parts, "Context:")
		for k, v := range context {
			parts = append(parts, fmt.Sprintf("  %s: %s", k, v))
		}
		parts = append(parts, "")
	}

	// Stack trace
	if err.StackTrace() != "" {
		parts = append(parts, "Stack Trace:")
		parts = append(parts, err.StackTrace())
	}

	// Cause
	if err.Cause() != nil {
		parts = append(parts, "")
		parts = append(parts, "Caused by:")
		parts = append(parts, "  "+err.Cause().Error())
	}

	return strings.Join(parts, "\n")
}
