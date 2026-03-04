package errors

// ErrorCode represents a standardized error code for error categorization
// Error codes are grouped by category using 4-digit codes
type ErrorCode string

// Validation Errors (1xxx)
const (
	ERR_VALIDATION_SYNTAX     ErrorCode = "ERR_VALIDATION_SYNTAX"     // 1001 - Invalid command syntax
	ERR_VALIDATION_FIELD      ErrorCode = "ERR_VALIDATION_FIELD"      // 1002 - Invalid field name/value
	ERR_VALIDATION_TYPE       ErrorCode = "ERR_VALIDATION_TYPE"       // 1003 - Type mismatch
	ERR_VALIDATION_CONSTRAINT ErrorCode = "ERR_VALIDATION_CONSTRAINT" // 1004 - Constraint violation
	ERR_VALIDATION_REQUIRED   ErrorCode = "ERR_VALIDATION_REQUIRED"   // 1005 - Required field missing
)

// Not Found Errors (2xxx)
const (
	ERR_NOT_FOUND_DATABASE ErrorCode = "ERR_NOT_FOUND_DATABASE" // 2001 - Database not found
	ERR_NOT_FOUND_BUNDLE   ErrorCode = "ERR_NOT_FOUND_BUNDLE"   // 2002 - Bundle not found
	ERR_NOT_FOUND_DOCUMENT ErrorCode = "ERR_NOT_FOUND_DOCUMENT" // 2003 - Document not found
	ERR_NOT_FOUND_INDEX    ErrorCode = "ERR_NOT_FOUND_INDEX"    // 2004 - Index not found
	ERR_NOT_FOUND_USER     ErrorCode = "ERR_NOT_FOUND_USER"     // 2005 - User not found
)

// Permission Errors (3xxx)
const (
	ERR_PERMISSION_DENIED    ErrorCode = "ERR_PERMISSION_DENIED"    // 3001 - General permission denied
	ERR_PERMISSION_DATABASE  ErrorCode = "ERR_PERMISSION_DATABASE"  // 3002 - Database access denied
	ERR_PERMISSION_BUNDLE    ErrorCode = "ERR_PERMISSION_BUNDLE"    // 3003 - Bundle access denied
	ERR_PERMISSION_OPERATION ErrorCode = "ERR_PERMISSION_OPERATION" // 3004 - Operation not permitted
	ERR_PERMISSION_ROLE      ErrorCode = "ERR_PERMISSION_ROLE"      // 3005 - Insufficient role privileges
)

// Authentication Errors (4xxx)
const (
	ERR_AUTH_FAILED          ErrorCode = "ERR_AUTH_FAILED"          // 4001 - Authentication failed
	ERR_AUTH_REQUIRED        ErrorCode = "ERR_AUTH_REQUIRED"        // 4002 - Authentication required
	ERR_AUTH_SESSION_EXPIRED ErrorCode = "ERR_AUTH_SESSION_EXPIRED" // 4003 - Session expired
	ERR_AUTH_LOCKOUT         ErrorCode = "ERR_AUTH_LOCKOUT"         // 4004 - Account locked
	ERR_AUTH_RATE_LIMIT      ErrorCode = "ERR_AUTH_RATE_LIMIT"      // 4005 - Too many attempts
)

// Resource Errors (5xxx)
const (
	ERR_RESOURCE_EXHAUSTED   ErrorCode = "ERR_RESOURCE_EXHAUSTED"   // 5001 - Memory limit exceeded
	ERR_RESOURCE_TIMEOUT     ErrorCode = "ERR_RESOURCE_TIMEOUT"     // 5002 - Operation timeout
	ERR_RESOURCE_CONNECTION  ErrorCode = "ERR_RESOURCE_CONNECTION"  // 5003 - Connection limit reached
	ERR_RESOURCE_DISK        ErrorCode = "ERR_RESOURCE_DISK"        // 5004 - Disk space exhausted
	ERR_RESOURCE_QUERY_LIMIT ErrorCode = "ERR_RESOURCE_QUERY_LIMIT" // 5005 - Query complexity limit exceeded
)

// Internal Errors (6xxx)
const (
	ERR_INTERNAL             ErrorCode = "ERR_INTERNAL"             // 6001 - Internal server error (generic)
	ERR_INTERNAL_STORAGE     ErrorCode = "ERR_INTERNAL_STORAGE"     // 6002 - Storage operation failed
	ERR_INTERNAL_INDEX       ErrorCode = "ERR_INTERNAL_INDEX"       // 6003 - Index operation failed
	ERR_INTERNAL_PARSER      ErrorCode = "ERR_INTERNAL_PARSER"      // 6004 - Parser internal error
	ERR_INTERNAL_QUERY       ErrorCode = "ERR_INTERNAL_QUERY"       // 6005 - Query execution error
	ERR_INTERNAL_TRANSACTION ErrorCode = "ERR_INTERNAL_TRANSACTION" // 6006 - Transaction error
	ERR_INTERNAL_WAL         ErrorCode = "ERR_INTERNAL_WAL"         // 6007 - WAL operation failed
	ERR_INTERNAL_LOCK        ErrorCode = "ERR_INTERNAL_LOCK"        // 6008 - Lock operation failed
)

// System Errors (7xxx)
const (
	ERR_SYSTEM_CONFIG      ErrorCode = "ERR_SYSTEM_CONFIG"      // 7001 - Configuration error
	ERR_SYSTEM_INIT        ErrorCode = "ERR_SYSTEM_INIT"        // 7002 - Initialization error
	ERR_SYSTEM_SHUTDOWN    ErrorCode = "ERR_SYSTEM_SHUTDOWN"    // 7003 - Shutdown error
	ERR_SYSTEM_MAINTENANCE ErrorCode = "ERR_SYSTEM_MAINTENANCE" // 7004 - System in maintenance mode
)

// Transaction Errors (8xxx)
const (
	ERR_TRANSACTION_NOT_STARTED      ErrorCode = "ERR_TRANSACTION_NOT_STARTED"      // 8001 - No active transaction
	ERR_TRANSACTION_CONFLICT         ErrorCode = "ERR_TRANSACTION_CONFLICT"         // 8002 - Transaction conflict
	ERR_TRANSACTION_DEADLOCK         ErrorCode = "ERR_TRANSACTION_DEADLOCK"         // 8003 - Deadlock detected
	ERR_TRANSACTION_ROLLBACK         ErrorCode = "ERR_TRANSACTION_ROLLBACK"         // 8004 - Transaction rolled back
	ERR_TRANSACTION_VERSION_CONFLICT ErrorCode = "ERR_TRANSACTION_VERSION_CONFLICT" // 8005 - OCC version conflict (document modified since read)
)

// Trigger Errors (9xxx)
const (
	ERR_TRIGGER_FAILED    ErrorCode = "ERR_TRIGGER_FAILED"    // 9001 - Trigger execution failed
	ERR_TRIGGER_RECURSION ErrorCode = "ERR_TRIGGER_RECURSION" // 9002 - Trigger recursion limit exceeded
	ERR_TRIGGER_TIMEOUT   ErrorCode = "ERR_TRIGGER_TIMEOUT"   // 9003 - Trigger execution timeout
)

// String returns the string representation of the error code
func (ec ErrorCode) String() string {
	return string(ec)
}

// IsValidationError returns true if the error code is a validation error (1xxx)
func (ec ErrorCode) IsValidationError() bool {
	return ec == ERR_VALIDATION_SYNTAX ||
		ec == ERR_VALIDATION_FIELD ||
		ec == ERR_VALIDATION_TYPE ||
		ec == ERR_VALIDATION_CONSTRAINT ||
		ec == ERR_VALIDATION_REQUIRED
}

// IsNotFoundError returns true if the error code is a not found error (2xxx)
func (ec ErrorCode) IsNotFoundError() bool {
	return ec == ERR_NOT_FOUND_DATABASE ||
		ec == ERR_NOT_FOUND_BUNDLE ||
		ec == ERR_NOT_FOUND_DOCUMENT ||
		ec == ERR_NOT_FOUND_INDEX ||
		ec == ERR_NOT_FOUND_USER
}

// IsPermissionError returns true if the error code is a permission error (3xxx)
func (ec ErrorCode) IsPermissionError() bool {
	return ec == ERR_PERMISSION_DENIED ||
		ec == ERR_PERMISSION_DATABASE ||
		ec == ERR_PERMISSION_BUNDLE ||
		ec == ERR_PERMISSION_OPERATION ||
		ec == ERR_PERMISSION_ROLE
}

// IsInternalError returns true if the error code is an internal error (6xxx)
func (ec ErrorCode) IsInternalError() bool {
	return ec == ERR_INTERNAL ||
		ec == ERR_INTERNAL_STORAGE ||
		ec == ERR_INTERNAL_INDEX ||
		ec == ERR_INTERNAL_PARSER ||
		ec == ERR_INTERNAL_QUERY ||
		ec == ERR_INTERNAL_TRANSACTION ||
		ec == ERR_INTERNAL_WAL ||
		ec == ERR_INTERNAL_LOCK
}
