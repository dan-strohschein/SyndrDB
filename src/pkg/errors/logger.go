package errors

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ErrorLogger manages error logging with separate internal and external log files
type ErrorLogger struct {
	internalLogger *zap.Logger // Full details, stack traces, sensitive info
	externalLogger *zap.Logger // Sanitized, user-facing messages only
	consoleLogger  *zap.SugaredLogger // For debug mode console output
	config         *ErrorLoggerConfig
	mu             sync.RWMutex
}

// ErrorLoggerConfig holds configuration for error logging
type ErrorLoggerConfig struct {
	InternalLogFile string // Path to internal error log file
	ExternalLogFile string // Path to external error log file
	LogDir          string // Base directory for log files
	DebugMode       bool   // If true, also output to console
	IncludeStack    bool   // Include stack traces in internal logs
}

// NewErrorLogger creates a new error logger with separate internal/external log files
func NewErrorLogger(config *ErrorLoggerConfig) (*ErrorLogger, error) {
	if config == nil {
		return nil, fmt.Errorf("error logger config cannot be nil")
	}

	el := &ErrorLogger{
		config: config,
	}

	// Build full paths for log files
	internalPath := config.InternalLogFile
	externalPath := config.ExternalLogFile

	if config.LogDir != "" {
		internalPath = filepath.Join(config.LogDir, config.InternalLogFile)
		externalPath = filepath.Join(config.LogDir, config.ExternalLogFile)

		// Ensure log directory exists
		if err := os.MkdirAll(config.LogDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory %s: %w", config.LogDir, err)
		}
	}

	// Create internal logger (full details, including stack traces)
	internalCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(getLogWriter(internalPath)),
		zapcore.ErrorLevel, // Only log errors (not info/debug)
	)
	el.internalLogger = zap.New(internalCore, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// Create external logger (sanitized, user-facing messages only)
	externalCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(getLogWriter(externalPath)),
		zapcore.ErrorLevel,
	)
	el.externalLogger = zap.New(externalCore, zap.AddCaller())

	// Create console logger for debug mode (if enabled)
	if config.DebugMode {
		// Use development encoder for better readability in console
		consoleConfig := zap.NewDevelopmentConfig()
		consoleConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		consoleConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
		consoleLogger, err := consoleConfig.Build()
		if err != nil {
			return nil, fmt.Errorf("failed to create console logger: %w", err)
		}
		el.consoleLogger = consoleLogger.Sugar()
	}

	return el, nil
}

// getLogWriter returns a WriteSyncer for the log file
// Creates the file if it doesn't exist
func getLogWriter(path string) zapcore.WriteSyncer {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		// If we can't create the log file, fall back to stderr
		return zapcore.AddSync(os.Stderr)
	}
	return zapcore.AddSync(file)
}

// LogError logs an error to both internal and external logs
// If debug mode is enabled, also outputs to console
// User validation errors are logged at Warn level (no stack trace) while
// internal/system errors are logged at Error level (with stack trace)
func (el *ErrorLogger) LogError(err SyndrDBError) {
	if err == nil {
		return
	}

	el.mu.RLock()
	defer el.mu.RUnlock()

	// Check if this is a user validation error (no stack trace needed)
	isUserValidationError := isUserError(err.Code())

	// Log to internal log (full details for system errors, reduced for user errors)
	internalFields := formatInternalLogFields(err, isUserValidationError)
	if isUserValidationError {
		// User validation errors logged at Warn level (no stack trace)
		el.internalLogger.Warn(err.InternalMessage(), internalFields...)
	} else {
		// System/internal errors logged at Error level (with stack trace)
		el.internalLogger.Error(err.InternalMessage(), internalFields...)
	}

	// Log to external log (sanitized) - always at Error level for external visibility
	externalFields := formatExternalLogFields(err)
	el.externalLogger.Error(err.UserMessage(), externalFields...)

	// Output to console in debug mode
	if el.consoleLogger != nil {
		if isUserValidationError {
			// User validation errors logged at Warn level (no stack trace from zap)
			el.consoleLogger.Warn("\n" + FormatConsoleOutput(err))
		} else {
			// System errors logged at Error level (includes stack trace from zap)
			el.consoleLogger.Error("\n" + FormatConsoleOutput(err))
		}
	}
}

// formatInternalLogFields formats error fields for internal logging
// skipStackTrace should be true for user validation errors where stack traces are not helpful
func formatInternalLogFields(err SyndrDBError, skipStackTrace bool) []zap.Field {
	fields := []zap.Field{
		zap.String("error_code", err.Code().String()),
		zap.String("layer", err.Layer().String()),
		zap.String("severity", err.Severity().String()),
		zap.String("user_message", err.UserMessage()),
		zap.String("internal_message", err.InternalMessage()),
	}

	// Add stack trace only for internal/system errors, skip for user validation errors
	if err.StackTrace() != "" && err.Severity() >= SeverityError && !skipStackTrace {
		fields = append(fields, zap.String("stack_trace", err.StackTrace()))
	}

	// Add cause if available
	if err.Cause() != nil {
		fields = append(fields, zap.Error(err.Cause()))
	}

	// Add context
	if context := err.GetContext(); len(context) > 0 {
		contextFields := make([]zap.Field, 0, len(context))
		for k, v := range context {
			contextFields = append(contextFields, zap.String(k, v))
		}
		fields = append(fields, zap.Namespace("context"))
		fields = append(fields, contextFields...)
	}

	// Add validation details if available
	if details := err.ValidationDetails(); details != nil {
		validationFields := []zap.Field{}
		if details.Location != "" {
			validationFields = append(validationFields, zap.String("location", details.Location))
		}
		if details.SubmittedInput != "" {
			validationFields = append(validationFields, zap.String("submitted_input", details.SubmittedInput))
		}
		if details.ExpectedFormat != "" {
			validationFields = append(validationFields, zap.String("expected_format", details.ExpectedFormat))
		}
		if details.CorrectExample != "" {
			validationFields = append(validationFields, zap.String("correct_example", details.CorrectExample))
		}
		if len(details.Suggestions) > 0 {
			validationFields = append(validationFields, zap.Strings("suggestions", details.Suggestions))
		}
		if len(details.AvailableOptions) > 0 {
			validationFields = append(validationFields, zap.Strings("available_options", details.AvailableOptions))
		}
		if len(validationFields) > 0 {
			fields = append(fields, zap.Namespace("validation"))
			fields = append(fields, validationFields...)
		}
	}

	return fields
}

// formatExternalLogFields formats error fields for external logging (sanitized)
func formatExternalLogFields(err SyndrDBError) []zap.Field {
	fields := []zap.Field{
		zap.String("error_code", err.Code().String()),
		zap.String("layer", err.Layer().String()),
		zap.String("severity", err.Severity().String()),
	}

	// Add sanitized validation details (no submitted input with sensitive data)
	if details := err.ValidationDetails(); details != nil {
		validationFields := []zap.Field{}
		if details.Location != "" {
			validationFields = append(validationFields, zap.String("location", details.Location))
		}
		if details.ExpectedFormat != "" {
			validationFields = append(validationFields, zap.String("expected_format", details.ExpectedFormat))
		}
		if details.CorrectExample != "" {
			validationFields = append(validationFields, zap.String("correct_example", details.CorrectExample))
		}
		if len(details.Suggestions) > 0 {
			validationFields = append(validationFields, zap.Strings("suggestions", details.Suggestions))
		}
		if len(validationFields) > 0 {
			fields = append(fields, zap.Namespace("validation"))
			fields = append(fields, validationFields...)
		}
	}

	return fields
}

// Sync flushes all log writers
func (el *ErrorLogger) Sync() error {
	el.mu.RLock()
	defer el.mu.RUnlock()

	var errors []error
	if el.internalLogger != nil {
		if err := el.internalLogger.Sync(); err != nil {
			errors = append(errors, fmt.Errorf("internal logger sync failed: %w", err))
		}
	}
	if el.externalLogger != nil {
		if err := el.externalLogger.Sync(); err != nil {
			errors = append(errors, fmt.Errorf("external logger sync failed: %w", err))
		}
	}
	if el.consoleLogger != nil {
		if err := el.consoleLogger.Sync(); err != nil {
			errors = append(errors, fmt.Errorf("console logger sync failed: %w", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("logger sync errors: %v", errors)
	}
	return nil
}
