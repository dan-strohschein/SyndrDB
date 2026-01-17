package errors

import (
	"fmt"
	"runtime"
	"strings"
)

// ErrorSeverity represents the severity level of an error
type ErrorSeverity int

const (
	SeverityInfo ErrorSeverity = iota
	SeverityWarning
	SeverityError
	SeverityCritical
)

// String returns the string representation of the error severity
func (es ErrorSeverity) String() string {
	switch es {
	case SeverityInfo:
		return "INFO"
	case SeverityWarning:
		return "WARNING"
	case SeverityError:
		return "ERROR"
	case SeverityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// ValidationErrorDetails provides structured details for validation errors
// Used internally to build detailed, actionable error messages
type ValidationErrorDetails struct {
	SubmittedInput   string   // What the user submitted (sanitized, show context around error)
	ExpectedFormat   string   // What was expected
	Location         string   // Line/column if available: "line 1, column 10"
	CorrectExample   string   // Example of correct syntax
	Suggestions      []string // Alternative suggestions (e.g., "Did you mean 'email'?")
	AvailableOptions []string // Available options (e.g., valid field names, command keywords)
}

// SyndrDBError is the core error interface for the error framework
// All errors in the system should implement this interface
type SyndrDBError interface {
	error
	Code() ErrorCode
	UserMessage() string      // Detailed, actionable message for developers (sanitized)
	InternalMessage() string  // Full technical details for logging (may contain sensitive info)
	Severity() ErrorSeverity
	Layer() ErrorLayer
	Cause() error
	StackTrace() string
	WithContext(key, value string) SyndrDBError
	WithLayer(layer ErrorLayer) SyndrDBError
	ValidationDetails() *ValidationErrorDetails // Optional validation details (nil for non-validation errors)
	GetContext() map[string]string              // Get context map (for internal logging)
}

// syndrDBError is the concrete implementation of SyndrDBError
type syndrDBError struct {
	code              ErrorCode
	userMessage       string
	internalMessage   string
	severity          ErrorSeverity
	layer             ErrorLayer
	cause             error
	stackTrace        string
	context           map[string]string
	validationDetails *ValidationErrorDetails
}

// Error implements the error interface - returns the user message
func (e *syndrDBError) Error() string {
	return e.userMessage
}

// Code returns the error code
func (e *syndrDBError) Code() ErrorCode {
	return e.code
}

// UserMessage returns the user-facing error message (sanitized)
func (e *syndrDBError) UserMessage() string {
	return e.userMessage
}

// InternalMessage returns the internal error message (may contain sensitive details)
func (e *syndrDBError) InternalMessage() string {
	return e.internalMessage
}

// Severity returns the error severity
func (e *syndrDBError) Severity() ErrorSeverity {
	return e.severity
}

// Layer returns the error layer
func (e *syndrDBError) Layer() ErrorLayer {
	return e.layer
}

// Cause returns the underlying cause error (if any)
func (e *syndrDBError) Cause() error {
	return e.cause
}

// StackTrace returns the stack trace (if captured)
func (e *syndrDBError) StackTrace() string {
	return e.stackTrace
}

// WithContext adds additional context to the error
func (e *syndrDBError) WithContext(key, value string) SyndrDBError {
	if e.context == nil {
		e.context = make(map[string]string)
	}
	e.context[key] = value
	return e
}

// WithLayer sets the error layer
func (e *syndrDBError) WithLayer(layer ErrorLayer) SyndrDBError {
	e.layer = layer
	return e
}

// ValidationDetails returns validation error details (if this is a validation error)
func (e *syndrDBError) ValidationDetails() *ValidationErrorDetails {
	return e.validationDetails
}

// captureStackTrace captures the current stack trace
func captureStackTrace(skip int) string {
	var buf strings.Builder
	pc := make([]uintptr, 32)
	n := runtime.Callers(skip, pc)
	frames := runtime.CallersFrames(pc[:n])

	buf.WriteString("\n")
	for {
		frame, more := frames.Next()
		buf.WriteString(fmt.Sprintf("%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line))
		if !more {
			break
		}
	}
	return buf.String()
}

// GetContext returns the context map (for internal logging)
func (e *syndrDBError) GetContext() map[string]string {
	if e.context == nil {
		return make(map[string]string)
	}
	return e.context
}
