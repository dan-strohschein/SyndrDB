package constants

/*
constants.go

This package provides centralized constants for the SyndrDB codebase.
All magic numbers should be extracted here for maintainability and clarity.

MED-008: Comprehensive constants extraction following best practices:
- Group related constants together
- Document reasoning for each value
- Use descriptive names
- Organize by category

Design Principles:
- Single source of truth for all numeric constants
- Clear documentation explaining why each value was chosen
- Easy to find and modify when requirements change
*/

// File Permissions
// Standard Unix file permission modes used throughout the codebase
const (
	// DirPermissionsDefault is the default directory permission mode (0755 = rwxr-xr-x)
	// Allows owner full access, group and others read/execute access
	// Used for data directories, log directories, temp directories
	DirPermissionsDefault = 0755

	// FilePermissionsDefault is the default file permission mode (0644 = rw-r--r--)
	// Allows owner read/write, group and others read-only
	// Used for log files and configuration files
	FilePermissionsDefault = 0644

	// DirPermissionsSecure is for sensitive directories (0700 = rwx------)
	// Only owner has access - used for user store and other security-sensitive directories
	DirPermissionsSecure = 0700

	// FilePermissionsSecure is for sensitive files (0600 = rw-------)
	// Only owner has read/write access - used for encrypted user store files
	FilePermissionsSecure = 0600
)

// Buffer Sizes
// Pre-allocated buffer sizes for various operations
const (
	// BufferSizeDefault is the default buffer size (4096 bytes = 4KB)
	// Standard page size for many file systems and network operations
	// Used for string builder pooling, file I/O buffers, network reads
	BufferSizeDefault = 4096

	// BufferSizeSmall is for small buffers (128 bytes)
	// Used for token pre-allocation, small string builders
	BufferSizeSmall = 128

	// BufferSizeLarge is for large buffers (65536 bytes = 64KB)
	// Used for bulk operations, large document reads
	BufferSizeLarge = 65536
)

// Network and Port Limits
const (
	// PortMin is the minimum valid TCP/UDP port number (1)
	PortMin = 1

	// PortMax is the maximum valid TCP/UDP port number (65535)
	// Standard 16-bit port range limit
	PortMax = 65535
)

// Timeout Ranges (in seconds)
const (
	// TimeoutMin is the minimum timeout value (1 second)
	// Prevents zero or negative timeouts which could cause issues
	TimeoutMin = 1

	// TimeoutMaxDefault is the maximum default timeout (3600 seconds = 1 hour)
	// Reasonable upper bound for most operations
	TimeoutMaxDefault = 3600

	// TimeoutMaxAdmin is the maximum admin timeout (7200 seconds = 2 hours)
	// Extended timeout for admin operations that may require more time
	TimeoutMaxAdmin = 7200
)

// String and Field Limits
const (
	// FieldNameMaxLength is the maximum length for field names (64 bytes)
	// Balances usability with storage efficiency
	FieldNameMaxLength = 64

	// FieldNameMaxLengthExtended is for extended field names (256 bytes)
	// Used in some contexts where longer names are acceptable
	FieldNameMaxLengthExtended = 256

	// UsernameMaxLength is the maximum username length (64 bytes)
	// Standard limit for usernames across most systems
	UsernameMaxLength = 64

	// PasswordMaxLength is the maximum password length (128 bytes)
	// Allows for passphrases while preventing excessive length attacks
	PasswordMaxLength = 128

	// DescriptionMaxLength is the maximum description length (500 characters)
	// Used for migration descriptions and other metadata
	DescriptionMaxLength = 500

	// UserAgentMaxLength is the maximum user agent string length (100 characters)
	// Truncated to prevent excessive storage
	UserAgentMaxLength = 100

	// UserAgentTruncateLength is the length to truncate user agent for display (50 characters)
	UserAgentTruncateLength = 50
)

// Input Length Limits (MED-003)
const (
	// InputMaxParameterValueLength is the maximum parameter value length (10000 bytes = 10KB)
	// Prevents memory exhaustion DoS attacks via oversized parameter values
	InputMaxParameterValueLength = 10000

	// InputMaxFieldValueLength is the maximum field value length (50000 bytes = 50KB)
	// Allows reasonable JSON payloads while preventing DoS attacks
	InputMaxFieldValueLength = 50000

	// InputMaxFieldNameLength is the maximum field name length (256 bytes)
	// Prevents attacks via excessively long field names
	InputMaxFieldNameLength = 256

	// InputMaxCommandLength is the maximum command length (10000 bytes = 10KB)
	// Prevents command buffer overflow attacks
	InputMaxCommandLength = 10000
)

// Memory and Size Limits
const (
	// MemoryMBToBytes is the conversion factor from MB to bytes (1024 * 1024)
	// Used for converting memory limits from MB (human-readable) to bytes (system)
	MemoryMBToBytes = 1024 * 1024

	// PageSizeDefault is the default page size (4096 bytes = 4KB)
	// Standard page size used in many systems for efficient memory management
	PageSizeDefault = 4096

	// EncryptionKeyMinLength is the minimum encryption key length (32 bytes = 256 bits)
	// Required for strong encryption (AES-256)
	EncryptionKeyMinLength = 32
)

// Retry Configuration
// Retry delays for transient failures
const (
	// RetryDelayShort is the initial retry delay (50 milliseconds)
	// Used for fast retries on transient errors
	RetryDelayShort = 50

	// RetryDelayMedium is the medium retry delay (200 milliseconds)
	// Used for second retry attempt
	RetryDelayMedium = 200

	// RetryDelayLong is the long retry delay (1000 milliseconds = 1 second)
	// Used for third retry attempt
	RetryDelayLong = 1000

	// RetryDelayMinimal is a minimal retry delay (1 millisecond)
	// Used for immediate retries in hot paths
	RetryDelayMinimal = 1

	// RetryMaxAttemptsDefault is the default maximum retry attempts (3)
	// Prevents infinite retry loops while allowing recovery from transient failures
	RetryMaxAttemptsDefault = 3
)

// Unicode and Character Limits
const (
	// UnicodeASCIIMax is the maximum ASCII value (127)
	// Used for ASCII-only validation checks
	UnicodeASCIIMax = 127

	// UnicodeExtendedASCIIMax is the maximum extended ASCII value (255)
	// Used for extended ASCII validation
	UnicodeExtendedASCIIMax = 255
)

// Sample Rate Constants
const (
	// SampleRateDefault is the default sampling rate (100)
	// Used for memory tracking and performance sampling
	SampleRateDefault = 100
)

// Validation Report Limits
const (
	// ValidationReportMaxSizeDefault is the default maximum validation report size (10485760 bytes = 10MB)
	// Prevents excessive report sizes while allowing detailed validation output
	ValidationReportMaxSizeDefault = 10485760 // 10MB
)

// Time Constants (milliseconds)
const (
	// TimeMillisecond is 1 millisecond
	TimeMillisecond = 1

	// TimeSecond is 1 second in milliseconds (1000)
	TimeSecond = 1000

	// TimeMinute is 1 minute in milliseconds (60000)
	TimeMinute = 60000

	// TimeHour is 1 hour in milliseconds (3600000)
	TimeHour = 3600000
)
