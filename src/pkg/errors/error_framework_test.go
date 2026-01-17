package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorCreation(t *testing.T) {
	t.Run("New creates error with correct code and message", func(t *testing.T) {
		err := New(ERR_NOT_FOUND_BUNDLE, "Bundle not found", LayerDomain)
		
		assert.NotNil(t, err)
		assert.Equal(t, ERR_NOT_FOUND_BUNDLE, err.Code())
		assert.Equal(t, "Bundle not found", err.UserMessage())
		assert.Equal(t, LayerDomain, err.Layer())
		assert.Equal(t, SeverityError, err.Severity())
	})

	t.Run("NewWithSeverity creates error with custom severity", func(t *testing.T) {
		err := NewWithSeverity(ERR_RESOURCE_EXHAUSTED, "Memory limit exceeded", LayerQuery, SeverityCritical)
		
		assert.Equal(t, SeverityCritical, err.Severity())
	})

	t.Run("Wrap converts standard error to SyndrDBError", func(t *testing.T) {
		originalErr := assert.AnError
		wrapped := Wrap(originalErr, ERR_INTERNAL, LayerStorage)
		
		assert.NotNil(t, wrapped)
		assert.Equal(t, ERR_INTERNAL, wrapped.Code())
		assert.Equal(t, LayerStorage, wrapped.Layer())
		assert.Equal(t, originalErr, wrapped.Cause())
	})

	t.Run("Wrap returns SyndrDBError as-is", func(t *testing.T) {
		originalErr := New(ERR_NOT_FOUND_BUNDLE, "Bundle not found", LayerDomain)
		wrapped := Wrap(originalErr, ERR_INTERNAL, LayerStorage)
		
		// Should return the same error, not wrap it
		assert.Equal(t, originalErr, wrapped)
	})

	t.Run("ConvertError converts nil to nil", func(t *testing.T) {
		result := ConvertError(nil, LayerAPI)
		assert.Nil(t, result)
	})
}

func TestValidationErrorCreation(t *testing.T) {
	t.Run("NewValidationError creates detailed validation error", func(t *testing.T) {
		details := &ValidationErrorDetails{
			SubmittedInput:   "SELECT * FORM users",
			ExpectedFormat:   "FROM",
			Location:         "line 1, column 10",
			CorrectExample:   "SELECT * FROM users",
			Suggestions:      []string{"Did you mean 'FROM'?"},
			AvailableOptions: []string{"FROM", "INTO"},
		}

		err := NewValidationError(ERR_VALIDATION_SYNTAX, "Syntax error", LayerParser, details)
		
		assert.NotNil(t, err)
		assert.Equal(t, ERR_VALIDATION_SYNTAX, err.Code())
		assert.NotEmpty(t, err.UserMessage())
		assert.Contains(t, err.UserMessage(), "Your command:")
		assert.Contains(t, err.UserMessage(), "Did you mean 'FROM'?")
		assert.Contains(t, err.UserMessage(), "Correct syntax:")
		assert.Equal(t, details, err.ValidationDetails())
	})

	t.Run("NewSyntaxError creates detailed syntax error", func(t *testing.T) {
		err := NewSyntaxError(
			"SELECT * FORM users",
			"line 1, column 10",
			"FORM",
			"FROM",
			[]string{"Did you mean 'FROM'?"},
		)
		
		assert.NotNil(t, err)
		assert.Equal(t, ERR_VALIDATION_SYNTAX, err.Code())
		assert.Equal(t, LayerParser, err.Layer())
		assert.NotNil(t, err.ValidationDetails())
		assert.Equal(t, "FROM", err.ValidationDetails().ExpectedFormat) // ExpectedFormat is the expected token
	})
}

func TestErrorContext(t *testing.T) {
	t.Run("WithContext adds context to error", func(t *testing.T) {
		err := New(ERR_NOT_FOUND_BUNDLE, "Bundle not found", LayerDomain)
		err = err.WithContext("bundle_name", "users")
		
		context := err.GetContext()
		assert.Equal(t, "users", context["bundle_name"])
	})

	t.Run("WithLayer sets error layer", func(t *testing.T) {
		err := New(ERR_NOT_FOUND_BUNDLE, "Bundle not found", LayerDomain)
		err = err.WithLayer(LayerStorage)
		
		assert.Equal(t, LayerStorage, err.Layer())
	})
}

func TestErrorCodes(t *testing.T) {
	t.Run("Error code helper methods work correctly", func(t *testing.T) {
		assert.True(t, ERR_VALIDATION_SYNTAX.IsValidationError())
		assert.False(t, ERR_VALIDATION_SYNTAX.IsNotFoundError())
		assert.False(t, ERR_VALIDATION_SYNTAX.IsInternalError())

		assert.True(t, ERR_NOT_FOUND_BUNDLE.IsNotFoundError())
		assert.False(t, ERR_NOT_FOUND_BUNDLE.IsValidationError())

		assert.True(t, ERR_INTERNAL_STORAGE.IsInternalError())
		assert.False(t, ERR_INTERNAL_STORAGE.IsValidationError())
	})

	t.Run("Error code string representation", func(t *testing.T) {
		assert.Equal(t, "ERR_NOT_FOUND_BUNDLE", ERR_NOT_FOUND_BUNDLE.String())
		assert.Equal(t, "ERR_VALIDATION_SYNTAX", ERR_VALIDATION_SYNTAX.String())
	})
}

func TestErrorSanitization(t *testing.T) {
	t.Run("SanitizeError removes file paths", func(t *testing.T) {
		err := New(ERR_INTERNAL, "Error reading file /path/to/sensitive/file.db", LayerStorage)
		sanitized := SanitizeError(err)
		
		msg := sanitized.UserMessage()
		assert.NotContains(t, msg, "/path/to/sensitive/file.db")
		assert.Contains(t, msg, "file.db") // Should still show filename
	})

	t.Run("SanitizeError removes UUIDs", func(t *testing.T) {
		err := New(ERR_INTERNAL, "Session 123e4567-e89b-12d3-a456-426614174000 expired", LayerAuth)
		sanitized := SanitizeError(err)
		
		msg := sanitized.UserMessage()
		assert.NotContains(t, msg, "123e4567-e89b-12d3-a456-426614174000")
		assert.Contains(t, msg, "[id]")
	})

	t.Run("SanitizeError handles nil", func(t *testing.T) {
		result := SanitizeError(nil)
		assert.Nil(t, result)
	})

	t.Run("SanitizeError handles standard errors", func(t *testing.T) {
		stdErr := assert.AnError
		result := SanitizeError(stdErr)
		
		assert.NotNil(t, result)
		assert.Equal(t, ERR_INTERNAL, result.Code())
		assert.Equal(t, LayerAPI, result.Layer())
	})
}

func TestErrorFormatting(t *testing.T) {
	t.Run("FormatUserResponse creates proper JSON structure", func(t *testing.T) {
		details := &ValidationErrorDetails{
			Location:       "line 1, column 10",
			CorrectExample: "SELECT * FROM users",
			Suggestions:    []string{"Did you mean 'FROM'?"},
		}
		err := NewValidationError(ERR_VALIDATION_SYNTAX, "Syntax error", LayerParser, details)
		
		response := FormatUserResponse(err)
		
		assert.Equal(t, err.Code().String(), response["error_code"])
		assert.Equal(t, err.UserMessage(), response["message"])
		assert.Equal(t, err.Layer().String(), response["layer"])
		assert.NotNil(t, response["validation"])
	})

	t.Run("FormatInternalLog includes all details", func(t *testing.T) {
		err := New(ERR_NOT_FOUND_BUNDLE, "Bundle not found", LayerDomain)
		err = err.WithContext("bundle_name", "users")
		
		logData := FormatInternalLog(err)
		
		assert.Equal(t, err.Code().String(), logData["error_code"])
		assert.Equal(t, err.UserMessage(), logData["user_message"])
		assert.Equal(t, err.InternalMessage(), logData["internal_message"])
		assert.Equal(t, err.Layer().String(), logData["layer"])
		assert.NotNil(t, logData["context"])
	})

	t.Run("FormatConsoleOutput includes formatted details", func(t *testing.T) {
		details := &ValidationErrorDetails{
			Location:       "line 1, column 10",
			CorrectExample: "SELECT * FROM users",
		}
		err := NewValidationError(ERR_VALIDATION_SYNTAX, "Syntax error", LayerParser, details)
		
		output := FormatConsoleOutput(err)
		
		assert.Contains(t, output, "ERROR")
		assert.Contains(t, output, "ERR_VALIDATION_SYNTAX")
		assert.Contains(t, output, "Syntax error")
		assert.Contains(t, output, "Validation Details")
		assert.Contains(t, output, "line 1, column 10")
	})
}

func TestParserErrorConverter(t *testing.T) {
	t.Run("Parser error extraction", func(t *testing.T) {
		// Test that parser errors are correctly converted
		// This would be tested in the server package where convertParserError is defined
		// Just verify the error types work correctly here
		
		err := NewSyntaxError(
			"SELECT * FORM users",
			"line 1, column 10",
			"FORM",
			"FROM",
			[]string{"Did you mean 'FROM'?"},
		)
		
		assert.NotNil(t, err.ValidationDetails())
		assert.Equal(t, "FROM", err.ValidationDetails().ExpectedFormat) // ExpectedFormat is the expected token
		assert.Contains(t, err.UserMessage(), "FORM") // User message should mention the unexpected token
	})
}
