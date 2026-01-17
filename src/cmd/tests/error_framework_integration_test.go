package main

import (
	"os"
	"path/filepath"
	"testing"

	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// TestErrorFramework_ErrorPropagation tests that errors propagate correctly through layers
func TestErrorFramework_ErrorPropagation(t *testing.T) {
	tempDir := t.TempDir()
	logger := zap.NewNop().Sugar()

	// Create database service
	dbStore, err := databasestore.NewDatabaseStore(tempDir, logger)
	assert.NoError(t, err)
	dbFactory := database.NewDatabaseFactory()

	// Create minimal settings
	config := &settings.Arguments{
		DataDir: tempDir,
	}
	dbService := database.NewDatabaseService(dbStore, dbFactory, config, logger)

	// Skip bundle service setup - we'll only test database errors

	t.Run("Database not found error uses SyndrDBError", func(t *testing.T) {
		_, err := dbService.GetDatabaseByName("nonexistent")

		assert.Error(t, err)
		sdbErr, ok := err.(errors.SyndrDBError)
		if !ok {
			t.Fatalf("Expected SyndrDBError, got: %T", err)
		}

		assert.Equal(t, errors.ERR_NOT_FOUND_DATABASE, sdbErr.Code())
		assert.Equal(t, errors.LayerDomain, sdbErr.Layer())
		assert.Contains(t, sdbErr.UserMessage(), "not found")
		assert.NotNil(t, sdbErr.GetContext())
	})

	// Bundle tests skipped - require bundle store setup with buffer pool
	// These are covered in domain layer tests

	// Bundle creation tests skipped - require bundle store setup
	// These are covered in bundle service unit tests
}

// TestErrorFramework_ErrorLogger tests the error logging infrastructure
func TestErrorFramework_ErrorLogger(t *testing.T) {
	tempDir := t.TempDir()
	logDir := filepath.Join(tempDir, "logs")

	config := &errors.ErrorLoggerConfig{
		InternalLogFile: "errors_internal.log",
		ExternalLogFile: "errors_external.log",
		LogDir:          logDir,
		DebugMode:       false, // Don't test console output in automated tests
		IncludeStack:    true,
	}

	logger, err := errors.NewErrorLogger(config)
	assert.NoError(t, err)
	defer logger.Sync()

	t.Run("Error logger logs validation errors", func(t *testing.T) {
		details := &errors.ValidationErrorDetails{
			SubmittedInput: "SELECT * FORM users",
			CorrectExample: "SELECT * FROM users",
			Location:       "line 1, column 10",
		}
		err := errors.NewValidationError(
			errors.ERR_VALIDATION_SYNTAX,
			"Syntax error",
			errors.LayerParser,
			details,
		)

		logger.LogError(err) // Should not panic or error

		// Verify log files were created
		internalLog := filepath.Join(logDir, "errors_internal.log")
		externalLog := filepath.Join(logDir, "errors_external.log")

		// Files should exist after logging
		_, statErr := os.Stat(internalLog)
		assert.NoError(t, statErr, "Internal log file should exist")

		_, statErr = os.Stat(externalLog)
		assert.NoError(t, statErr, "External log file should exist")
	})

	t.Run("Error logger handles nil gracefully", func(t *testing.T) {
		logger.LogError(nil) // Should not panic
	})

	t.Run("Error logger sync succeeds", func(t *testing.T) {
		err := logger.Sync()
		assert.NoError(t, err)
	})
}
