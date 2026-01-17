package server

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
	"unicode"
)

// SecurityConfig holds security-related configuration
type SecurityConfig struct {
	MaxCommandLength          int
	MaxUsernameLength         int
	MaxPasswordLength         int
	MinPasswordLength         int
	RequirePasswordComplexity bool
	AllowedSpecialChars       string
	// MED-003: Input length limits for DoS prevention
	MaxParameterValueLength int // Maximum length for individual parameter values
	MaxFieldValueLength     int // Maximum length for individual field values
	MaxFieldNameLength      int // Maximum length for field names
}

// DefaultSecurityConfig returns default security configuration
func DefaultSecurityConfig() *SecurityConfig {
	settings := settings.GetSettings()
	return &SecurityConfig{
		MaxCommandLength:          constants.InputMaxCommandLength,
		MaxUsernameLength:         constants.UsernameMaxLength,
		MaxPasswordLength:         constants.PasswordMaxLength,
		MinPasswordLength:         8,
		RequirePasswordComplexity: true,
		AllowedSpecialChars:       "!@#$%^&*()_+-=[]{}|;:,.<>?",
		// MED-003: Load from settings with defaults
		MaxParameterValueLength: getMaxParameterValueLength(settings),
		MaxFieldValueLength:     getMaxFieldValueLength(settings),
		MaxFieldNameLength:      getMaxFieldNameLength(settings),
	}
}

// getMaxParameterValueLength gets the maximum parameter value length from settings
func getMaxParameterValueLength(settings *settings.Arguments) int {
	if settings.MaxParameterValueLength > 0 {
		return settings.MaxParameterValueLength
	}
	return constants.InputMaxParameterValueLength
}

// getMaxFieldValueLength gets the maximum field value length from settings
func getMaxFieldValueLength(settings *settings.Arguments) int {
	if settings.MaxFieldValueLength > 0 {
		return settings.MaxFieldValueLength
	}
	return constants.InputMaxFieldValueLength
}

// getMaxFieldNameLength gets the maximum field name length from settings
func getMaxFieldNameLength(settings *settings.Arguments) int {
	if settings.MaxFieldNameLength > 0 {
		return settings.MaxFieldNameLength
	}
	return constants.InputMaxFieldNameLength
}

// ValidateInput performs comprehensive input validation
func ValidateInput(input string, inputType string, config *SecurityConfig) error {
	if config == nil {
		config = DefaultSecurityConfig()
	}

	switch inputType {
	case "command":
		return validateCommand(input, config)
	case "username":
		return validateUsername(input, config)
	case "password":
		return validatePassword(input, config)
	case "database_name":
		return validateDatabaseName(input)
	case "bundle_name":
		return validateBundleName(input)
	default:
		return validateGenericInput(input, config)
	}
}

// validateCommand validates SQL-like commands for injection attacks
// This function implements multiple layers of defense:
// 1. Pattern-based validation (blacklist of known attack patterns)
// 2. Unicode normalization detection (lookalike character attacks)
// 3. Encoding attempt detection (URL/hex encoding bypasses)
// 4. Context-aware validation (improved pattern matching)
//
// SECURITY NOTE: While this validation provides defense-in-depth, it is NOT a replacement
// for parameterized queries. All user-provided values should use parameterized queries
// ($1, $2, $3 placeholders) which are the primary defense against injection attacks.
// See: docs/user/parameterized_queries.md for more information.
func validateCommand(command string, config *SecurityConfig) error {
	if len(command) == 0 {
		return errors.New(errors.ERR_VALIDATION_REQUIRED,
			"command cannot be empty", errors.LayerCommand)
	}

	if len(command) > config.MaxCommandLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("command too long: maximum %d characters allowed", config.MaxCommandLength),
			errors.LayerCommand).WithContext("max_length", fmt.Sprintf("%d", config.MaxCommandLength))
	}

	// Layer 1: Unicode normalization detection - detect lookalike character attacks
	// Check for common Unicode lookalikes that could bypass pattern matching
	if err := detectUnicodeLookalikes(command); err != nil {
		return errors.WrapWithMessage(err, errors.ERR_VALIDATION_FIELD,
			"unicode lookalike characters detected", errors.LayerCommand)
	}

	// Layer 2: Encoding attempt detection - detect URL/hex encoding bypasses
	if encodingAttempts := detectEncodingAttempts(command); len(encodingAttempts) > 0 {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("potential encoding bypass detected: %s", strings.Join(encodingAttempts, ", ")),
			errors.LayerCommand).WithContext("encoding_types", strings.Join(encodingAttempts, ", "))
	}

	// Layer 3: Pattern-based validation (existing approach, enhanced)
	// Check for potential injection patterns
	suspiciousPatterns := []string{
		"--",              // SQL comment
		"/*",              // SQL block comment start
		"*/",              // SQL block comment end
		";--",             // Command termination with comment
		"' OR '1'='1",     // Classic SQL injection
		"\" OR \"1\"=\"1", // Quote variant
		"UNION SELECT",    // Union-based injection
		"DROP TABLE",      // Destructive commands
		"TRUNCATE",        // Table truncation
		"<script",         // XSS attempt
		"javascript:",     // JavaScript injection
		"../../",          // Path traversal
		"..\\",            // Windows path traversal
		"\x00",            // Null byte injection
	}

	// Normalize command for pattern matching (after encoding detection)
	commandNormalized := normalizeForPatternMatching(command)
	commandUpper := strings.ToUpper(commandNormalized)

	for _, pattern := range suspiciousPatterns {
		if strings.Contains(commandUpper, strings.ToUpper(pattern)) {
			return errors.New(errors.ERR_VALIDATION_FIELD,
				fmt.Sprintf("potentially malicious pattern detected in command: %s", pattern),
				errors.LayerCommand).WithContext("pattern", pattern)
		}
	}

	// Special check for DELETE FROM (SQL injection) vs DELETE DOCUMENTS FROM (legitimate SyndrDB command)
	// Block "DELETE FROM" only if it's NOT part of "DELETE DOCUMENTS FROM"
	if strings.Contains(commandUpper, "DELETE FROM") && !strings.Contains(commandUpper, "DELETE DOCUMENTS FROM") {
		return errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid DELETE syntax: use 'DELETE DOCUMENTS FROM <bundle>' instead of 'DELETE FROM'",
			errors.LayerCommand)
	}

	// MED-002: Context-aware character validation instead of simple ratio checking
	// This implementation:
	// 1. Checks if special characters are balanced (matching quotes, braces, parentheses)
	// 2. Validates that special characters appear in expected positions
	// 3. Uses a state machine to track quoted strings, JSON objects, and command syntax
	// 4. Applies different validation rules based on context
	// 5. Separates command structure validation from data payload validation

	// Check for parameterized query placeholders - if present, only validate command template
	if hasParameterizedPlaceholders(command) {
		// For parameterized queries, we only validate the command structure (with $1, $2 placeholders)
		// The actual parameter values are validated separately when they're substituted
		return validateCommandStructure(command, commandUpper)
	}

	// For non-parameterized queries, use context-aware validation
	return validateCommandWithContext(command, commandUpper)
}

// detectUnicodeLookalikes detects common Unicode lookalike characters that could bypass pattern matching
// This detects characters that look like ASCII but are actually Unicode (homoglyph attacks)
func detectUnicodeLookalikes(input string) error {
	// Common Unicode lookalikes for ASCII characters used in SQL injection
	// Cyrillic, Greek, and other scripts that have visually similar characters
	lookalikePatterns := map[rune]string{
		// Cyrillic lookalikes
		'\u0430': "Cyrillic 'a' (looks like ASCII 'a')",
		'\u0435': "Cyrillic 'e' (looks like ASCII 'e')",
		'\u043E': "Cyrillic 'o' (looks like ASCII 'o')",
		'\u0440': "Cyrillic 'p' (looks like ASCII 'p')",
		'\u0441': "Cyrillic 'c' (looks like ASCII 'c')",
		'\u0445': "Cyrillic 'x' (looks like ASCII 'x')",
		// Greek lookalikes
		'\u0391': "Greek 'Α' (looks like ASCII 'A')",
		'\u0395': "Greek 'Ε' (looks like ASCII 'E')",
		'\u039F': "Greek 'Ο' (looks like ASCII 'O')",
	}

	for _, char := range input {
		if warning, exists := lookalikePatterns[char]; exists {
			return errors.New(errors.ERR_VALIDATION_FIELD,
				fmt.Sprintf("unicode lookalike character detected: %s", warning),
				errors.LayerCommand).WithContext("warning", warning)
		}
		// Check for characters outside ASCII printable range that aren't expected
		// Allow common whitespace and expected Unicode ranges
		if char > 127 && !unicode.IsSpace(char) && !unicode.IsLetter(char) && !unicode.IsDigit(char) && !unicode.IsPunct(char) {
			// This is a conservative check - flag non-ASCII non-printables that aren't standard punctuation
			// Allow common legitimate Unicode (accented letters, etc) but flag suspicious characters
			if char < constants.UnicodeExtendedASCIIMax+1 && !unicode.IsPrint(char) {
				return errors.New(errors.ERR_VALIDATION_FIELD,
					"non-printable unicode character detected (potential obfuscation)",
					errors.LayerCommand)
			}
		}
	}

	return nil
}

// detectEncodingAttempts detects common encoding-based bypass attempts
// Returns a list of detected encoding types
func detectEncodingAttempts(input string) []string {
	attempts := []string{}

	// Detect URL encoding (%XX patterns)
	// Look for suspicious patterns that might be URL-encoded SQL injection
	urlEncodedPatterns := []string{"%27", "%22", "%2D", "%2D", "%2F", "%2A", "%3B", "%55", "%4E", "%49", "%4F", "%4E"}
	for _, pattern := range urlEncodedPatterns {
		if strings.Contains(strings.ToUpper(input), pattern) {
			// Decode and check if it results in suspicious content
			decoded, err := url.QueryUnescape(input)
			if err == nil && decoded != input {
				// Check if decoded string contains injection patterns
				decodedUpper := strings.ToUpper(decoded)
				suspicious := []string{"' OR", "UNION", "DROP", "TRUNCATE", "DELETE FROM", "--"}
				for _, susp := range suspicious {
					if strings.Contains(decodedUpper, susp) {
						attempts = append(attempts, "URL encoding")
						break
					}
				}
			}
		}
	}

	// Detect hex encoding (0xXX or \xXX patterns)
	hexPattern := regexp.MustCompile(`(?:0x|\\x)[0-9a-fA-F]{2}`)
	matches := hexPattern.FindAllString(input, -1)
	if len(matches) > 5 {
		// If there are many hex-encoded bytes, it might be an attempt
		// Try to decode some common hex patterns
		for _, match := range matches {
			if strings.HasPrefix(match, "\\x") {
				hexStr := match[2:]
				decoded, err := hex.DecodeString(hexStr)
				if err == nil && len(decoded) > 0 {
					// Check if decoded byte is a suspicious character
					char := rune(decoded[0])
					if char == '\'' || char == '"' || char == ';' || char == '-' {
						attempts = append(attempts, "hex encoding")
						break
					}
				}
			}
		}
	}

	return attempts
}

// hasParameterizedPlaceholders checks if the command contains parameterized query placeholders ($1, $2, etc.)
// MED-002: Parameterized queries should only validate command structure, not parameter values
func hasParameterizedPlaceholders(command string) bool {
	// Look for $ followed by digits (e.g., $1, $2, $10, etc.)
	paramPattern := regexp.MustCompile(`\$\d+`)
	return paramPattern.MatchString(command)
}

// validateCommandStructure validates the structure of parameterized query commands
// MED-002: Only validates command syntax with placeholders, not the parameter values
func validateCommandStructure(command, commandUpper string) error {
	// For parameterized queries, we're more lenient since placeholders are normalized
	// Still check for balanced brackets/quotes in the command structure
	if err := checkBalancedDelimiters(command); err != nil {
		return errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"unbalanced delimiters in command structure", errors.LayerCommand)
	}

	// Check for suspicious patterns in command structure (not in data)
	// These are more restrictive since we're only checking the command template
	suspiciousInStructure := []string{
		"; DROP", "; DELETE", "; TRUNCATE", "; DROP TABLE", "; DROP DATABASE",
		"'; DROP", "'; DELETE", "'; TRUNCATE",
	}

	for _, pattern := range suspiciousInStructure {
		if strings.Contains(commandUpper, pattern) {
			return errors.New(errors.ERR_VALIDATION_FIELD,
				fmt.Sprintf("suspicious pattern in command structure: %s", pattern),
				errors.LayerCommand).WithContext("pattern", pattern)
		}
	}

	return nil
}

// validateCommandWithContext performs context-aware validation using a state machine
// MED-002: Tracks quoted strings, JSON objects, and command syntax separately
func validateCommandWithContext(command, commandUpper string) error {
	// First check for balanced delimiters
	if err := checkBalancedDelimiters(command); err != nil {
		return errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"unbalanced delimiters", errors.LayerCommand)
	}

	// Use state machine to separate command structure from data payload
	commandStructChars := 0
	commandStructTotal := 0
	dataPayloadChars := 0
	dataPayloadTotal := 0

	state := parseStateCommand
	stack := []rune{}                          // Track nested brackets/braces
	prevStateBeforeString := parseStateCommand // Track state before entering a string

	for _, char := range command {
		// Skip whitespace for counting
		if unicode.IsSpace(char) {
			continue
		}

		// Update state based on current character
		newState, charCategory := updateParseState(char, state, &stack, &prevStateBeforeString)
		state = newState

		// Count characters based on context
		if charCategory == charCategoryData {
			dataPayloadTotal++
			if !unicode.IsLetter(char) && !unicode.IsDigit(char) {
				dataPayloadChars++
			}
		} else {
			commandStructTotal++
			if !unicode.IsLetter(char) && !unicode.IsDigit(char) {
				commandStructChars++
			}
		}
	}

	// Ensure all brackets/braces were closed
	if len(stack) > 0 {
		return errors.New(errors.ERR_VALIDATION_SYNTAX,
			"unclosed delimiters detected", errors.LayerCommand)
	}

	// Calculate ratio: command structure should have reasonable special char ratio
	// Data payloads can have high ratios (JSON, etc.) - we don't check those
	if commandStructTotal > 0 {
		commandRatio := float64(commandStructChars) / float64(commandStructTotal)

		// Command structure should have lower special char ratio
		// Data payloads are allowed to have high ratios and are not checked
		if commandRatio > 0.4 {
			// Only fail if command structure has excessive special chars
			return errors.New(errors.ERR_VALIDATION_FIELD,
				fmt.Sprintf("command structure contains excessive special characters (ratio: %.2f)", commandRatio),
				errors.LayerCommand).WithContext("ratio", fmt.Sprintf("%.2f", commandRatio))
		}
	}

	return nil
}

// parseState represents the parsing state for context-aware validation
type parseState int

const (
	parseStateCommand   parseState = iota // In command syntax
	parseStateString                      // Inside quoted string
	parseStateJSON                        // Inside JSON object/array
	parseStateParameter                   // Inside parameter placeholder ($1, etc.)
)

// charCategory indicates where a character belongs
type charCategory int

const (
	charCategoryCommand charCategory = iota // Command structure
	charCategoryData                        // Data payload (JSON, strings, etc.)
)

// checkBalancedDelimiters checks if quotes, brackets, and braces are balanced
func checkBalancedDelimiters(command string) error {
	var (
		singleQuotes int
		doubleQuotes int
		parens       int
		brackets     int
		braces       int
		escapeNext   bool
	)

	for i, char := range command {
		if escapeNext {
			escapeNext = false
			continue
		}

		switch char {
		case '\\':
			escapeNext = true
			continue
		case '\'':
			// Skip if inside double-quoted string
			if doubleQuotes%2 == 0 {
				singleQuotes++
			}
		case '"':
			// Skip if inside single-quoted string
			if singleQuotes%2 == 0 {
				doubleQuotes++
			}
		case '(':
			// Skip if inside quoted string
			if singleQuotes%2 == 0 && doubleQuotes%2 == 0 {
				parens++
			}
		case ')':
			if singleQuotes%2 == 0 && doubleQuotes%2 == 0 {
				parens--
				if parens < 0 {
					return errors.New(errors.ERR_VALIDATION_SYNTAX,
						fmt.Sprintf("unmatched ')' at position %d", i),
						errors.LayerCommand).WithContext("position", fmt.Sprintf("%d", i)).WithContext("delimiter", ")")
				}
			}
		case '[':
			if singleQuotes%2 == 0 && doubleQuotes%2 == 0 {
				brackets++
			}
		case ']':
			if singleQuotes%2 == 0 && doubleQuotes%2 == 0 {
				brackets--
				if brackets < 0 {
					return errors.New(errors.ERR_VALIDATION_SYNTAX,
						fmt.Sprintf("unmatched ']' at position %d", i),
						errors.LayerCommand).WithContext("position", fmt.Sprintf("%d", i)).WithContext("delimiter", "]")
				}
			}
		case '{':
			if singleQuotes%2 == 0 && doubleQuotes%2 == 0 {
				braces++
			}
		case '}':
			if singleQuotes%2 == 0 && doubleQuotes%2 == 0 {
				braces--
				if braces < 0 {
					return errors.New(errors.ERR_VALIDATION_SYNTAX,
						fmt.Sprintf("unmatched '}' at position %d", i),
						errors.LayerCommand).WithContext("position", fmt.Sprintf("%d", i)).WithContext("delimiter", "}")
				}
			}
		}
	}

	// Check for unmatched opening delimiters
	if singleQuotes%2 != 0 {
		return errors.New(errors.ERR_VALIDATION_SYNTAX,
			"unmatched single quote", errors.LayerCommand)
	}
	if doubleQuotes%2 != 0 {
		return errors.New(errors.ERR_VALIDATION_SYNTAX,
			"unmatched double quote", errors.LayerCommand)
	}
	if parens > 0 {
		return errors.New(errors.ERR_VALIDATION_SYNTAX,
			"unmatched '('", errors.LayerCommand)
	}
	if brackets > 0 {
		return errors.New(errors.ERR_VALIDATION_SYNTAX,
			"unmatched '['", errors.LayerCommand)
	}
	if braces > 0 {
		return errors.New(errors.ERR_VALIDATION_SYNTAX,
			"unmatched '{'", errors.LayerCommand)
	}

	return nil
}

// updateParseState updates the parsing state based on the current character
// Returns new state and character category (command vs data)
// stack tracks nested JSON structures, and prevStateBeforeString tracks where we were before entering a string
func updateParseState(char rune, currentState parseState, stack *[]rune, prevStateBeforeString *parseState) (parseState, charCategory) {
	switch currentState {
	case parseStateCommand:
		switch char {
		case '\'':
			*prevStateBeforeString = parseStateCommand
			return parseStateString, charCategoryData
		case '"':
			*prevStateBeforeString = parseStateCommand
			return parseStateString, charCategoryData
		case '{', '[':
			*stack = append(*stack, char)
			return parseStateJSON, charCategoryData
		case '$':
			return parseStateParameter, charCategoryCommand
		default:
			return parseStateCommand, charCategoryCommand
		}

	case parseStateString:
		switch char {
		case '\'', '"':
			// Close the string - return to previous state (could be command or JSON)
			return *prevStateBeforeString, charCategoryData
		case '\\':
			// Escape sequence - stay in string
			return parseStateString, charCategoryData
		default:
			return parseStateString, charCategoryData
		}

	case parseStateJSON:
		switch char {
		case '\'', '"':
			// Start of string within JSON - remember we were in JSON
			*prevStateBeforeString = parseStateJSON
			return parseStateString, charCategoryData
		case '{':
			*stack = append(*stack, char)
			return parseStateJSON, charCategoryData
		case '[':
			*stack = append(*stack, char)
			return parseStateJSON, charCategoryData
		case '}':
			if len(*stack) > 0 && (*stack)[len(*stack)-1] == '{' {
				*stack = (*stack)[:len(*stack)-1]
			}
			if len(*stack) == 0 {
				return parseStateCommand, charCategoryData
			}
			return parseStateJSON, charCategoryData
		case ']':
			if len(*stack) > 0 && (*stack)[len(*stack)-1] == '[' {
				*stack = (*stack)[:len(*stack)-1]
			}
			if len(*stack) == 0 {
				return parseStateCommand, charCategoryData
			}
			return parseStateJSON, charCategoryData
		default:
			return parseStateJSON, charCategoryData
		}

	case parseStateParameter:
		if unicode.IsDigit(char) {
			return parseStateParameter, charCategoryCommand
		}
		// End of parameter placeholder - return to command
		return parseStateCommand, charCategoryCommand

	default:
		return parseStateCommand, charCategoryCommand
	}
}

// normalizeForPatternMatching normalizes input for pattern matching by decoding common encodings
// This helps detect encoded injection attempts
func normalizeForPatternMatching(input string) string {
	// Decode URL encoding
	decoded, err := url.QueryUnescape(input)
	if err == nil {
		input = decoded
	}

	// Note: Full Unicode normalization would require golang.org/x/text/unicode/norm
	// This is a simplified version that handles basic cases
	// The detectUnicodeLookalikes function handles Unicode checks separately

	return input
}

// validateUsername validates username format and content
func validateUsername(username string, config *SecurityConfig) error {
	if len(username) == 0 {
		return errors.New(errors.ERR_VALIDATION_REQUIRED,
			"username cannot be empty", errors.LayerCommand)
	}

	if len(username) > config.MaxUsernameLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("username too long: maximum %d characters allowed", config.MaxUsernameLength),
			errors.LayerCommand).WithContext("max_length", fmt.Sprintf("%d", config.MaxUsernameLength))
	}

	// Username should only contain alphanumeric characters, underscores, and hyphens
	validUsername := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	if !validUsername.MatchString(username) {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"username contains invalid characters: only letters, numbers, underscores, and hyphens allowed",
			errors.LayerCommand)
	}

	// Username should start with a letter
	if !unicode.IsLetter(rune(username[0])) {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"username must start with a letter", errors.LayerCommand)
	}

	// Check for reserved usernames
	reservedUsernames := []string{"admin", "root", "system", "guest", "anonymous", "public", "syndr", "syndrdb"}
	for _, reserved := range reservedUsernames {
		if strings.EqualFold(username, reserved) {
			return errors.New(errors.ERR_VALIDATION_CONSTRAINT,
				fmt.Sprintf("username '%s' is reserved and cannot be used", username),
				errors.LayerCommand).WithContext("username", username)
		}
	}

	return nil
}

// validatePassword validates password strength and security
func validatePassword(password string, config *SecurityConfig) error {
	if len(password) < config.MinPasswordLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("password too short: minimum %d characters required", config.MinPasswordLength),
			errors.LayerCommand).WithContext("min_length", fmt.Sprintf("%d", config.MinPasswordLength))
	}

	if len(password) > config.MaxPasswordLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("password too long: maximum %d characters allowed", config.MaxPasswordLength),
			errors.LayerCommand).WithContext("max_length", fmt.Sprintf("%d", config.MaxPasswordLength))
	}

	if !config.RequirePasswordComplexity {
		return nil
	}

	// Check password complexity
	var hasUpper, hasLower, hasDigit, hasSpecial bool
	for _, char := range password {
		switch {
		case unicode.IsUpper(char):
			hasUpper = true
		case unicode.IsLower(char):
			hasLower = true
		case unicode.IsDigit(char):
			hasDigit = true
		case strings.ContainsRune(config.AllowedSpecialChars, char):
			hasSpecial = true
		}
	}

	missing := []string{}
	if !hasUpper {
		missing = append(missing, "uppercase letter")
	}
	if !hasLower {
		missing = append(missing, "lowercase letter")
	}
	if !hasDigit {
		missing = append(missing, "digit")
	}
	if !hasSpecial {
		missing = append(missing, "special character")
	}

	if len(missing) > 0 {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("password must contain at least one: %s", strings.Join(missing, ", ")),
			errors.LayerCommand).WithContext("missing_requirements", strings.Join(missing, ", "))
	}

	// Check for common weak passwords
	weakPasswords := []string{
		"password", "123456", "qwerty", "admin", "letmein", "welcome",
		"password123", "admin123", "qwerty123", "123456789", "password1",
	}
	for _, weak := range weakPasswords {
		if strings.EqualFold(password, weak) {
			return errors.New(errors.ERR_VALIDATION_FIELD,
				"password is too common and easily guessable", errors.LayerCommand)
		}
	}

	return nil
}

// validateDatabaseName validates database name format
func validateDatabaseName(name string) error {
	if len(name) == 0 {
		return errors.New(errors.ERR_VALIDATION_REQUIRED,
			"database name cannot be empty", errors.LayerCommand)
	}

	if len(name) > constants.FieldNameMaxLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("database name too long: maximum %d characters allowed", constants.FieldNameMaxLength),
			errors.LayerCommand).WithContext("max_length", fmt.Sprintf("%d", constants.FieldNameMaxLength))
	}

	// Database name should only contain alphanumeric characters, underscores, and hyphens
	validName := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	if !validName.MatchString(name) {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"database name contains invalid characters", errors.LayerCommand)
	}

	// Should start with a letter
	if !unicode.IsLetter(rune(name[0])) {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"database name must start with a letter", errors.LayerCommand)
	}

	return nil
}

// validateBundleName validates bundle name format
func validateBundleName(name string) error {
	if len(name) == 0 {
		return errors.New(errors.ERR_VALIDATION_REQUIRED,
			"bundle name cannot be empty", errors.LayerCommand)
	}

	if len(name) > constants.FieldNameMaxLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("bundle name too long: maximum %d characters allowed", constants.FieldNameMaxLength),
			errors.LayerCommand).WithContext("max_length", fmt.Sprintf("%d", constants.FieldNameMaxLength))
	}

	// Bundle name should only contain alphanumeric characters, underscores, and hyphens
	validName := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	if !validName.MatchString(name) {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"bundle name contains invalid characters", errors.LayerCommand)
	}

	return nil
}

// validateGenericInput validates general input for basic security
func validateGenericInput(input string, config *SecurityConfig) error {
	if len(input) > config.MaxCommandLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("input too long: maximum %d characters allowed", config.MaxCommandLength),
			errors.LayerCommand).WithContext("max_length", fmt.Sprintf("%d", config.MaxCommandLength))
	}

	// Check for null bytes
	if strings.Contains(input, "\x00") {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"input contains null bytes", errors.LayerCommand)
	}

	// Check for excessive control characters
	controlCharCount := 0
	for _, char := range input {
		if unicode.IsControl(char) && char != '\t' && char != '\n' && char != '\r' {
			controlCharCount++
		}
	}

	if controlCharCount > 5 {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"input contains excessive control characters", errors.LayerCommand).WithContext("count", fmt.Sprintf("%d", controlCharCount))
	}

	return nil
}

// ValidateParameterValue validates an individual parameter value length (MED-003)
func ValidateParameterValue(paramValue string, config *SecurityConfig) error {
	if config == nil {
		config = DefaultSecurityConfig()
	}

	if len(paramValue) > config.MaxParameterValueLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("parameter value too long: maximum %d bytes allowed", config.MaxParameterValueLength),
			errors.LayerCommand).WithContext("max_length", fmt.Sprintf("%d", config.MaxParameterValueLength)).WithContext("actual_length", fmt.Sprintf("%d", len(paramValue)))
	}

	return nil
}

// ValidateFieldValue validates an individual field value length (MED-003)
func ValidateFieldValue(fieldValue interface{}, config *SecurityConfig) error {
	if config == nil {
		config = DefaultSecurityConfig()
	}

	// Convert field value to string for length checking
	var valueStr string
	switch v := fieldValue.(type) {
	case string:
		valueStr = v
	case []byte:
		valueStr = string(v)
	default:
		// For non-string values, serialize to string for length check
		// This covers JSON objects, arrays, etc.
		valueStr = fmt.Sprintf("%v", v)
	}

	if len(valueStr) > config.MaxFieldValueLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("field value too long: maximum %d bytes allowed", config.MaxFieldValueLength),
			errors.LayerCommand).WithContext("max_length", fmt.Sprintf("%d", config.MaxFieldValueLength)).WithContext("actual_length", fmt.Sprintf("%d", len(valueStr)))
	}

	return nil
}

// ValidateFieldName validates a field name length (MED-003)
func ValidateFieldName(fieldName string, config *SecurityConfig) error {
	if config == nil {
		config = DefaultSecurityConfig()
	}

	if len(fieldName) > config.MaxFieldNameLength {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("field name too long: maximum %d bytes allowed", config.MaxFieldNameLength),
			errors.LayerCommand).WithContext("max_length", fmt.Sprintf("%d", config.MaxFieldNameLength)).WithContext("actual_length", fmt.Sprintf("%d", len(fieldName)))
	}

	return nil
}

// SanitizeInput removes or escapes potentially dangerous characters
func SanitizeInput(input string) string {
	// Remove null bytes
	input = strings.ReplaceAll(input, "\x00", "")

	// PHASE 3: Use pooled string builder to reduce allocations
	sanitized := GetStringBuilder()
	defer PutStringBuilder(sanitized)

	// Remove other control characters except tab, newline, carriage return
	for _, char := range input {
		if !unicode.IsControl(char) || char == '\t' || char == '\n' || char == '\r' {
			sanitized.WriteRune(char)
		}
	}

	return sanitized.String()
}

// ValidateFilePath validates file paths to prevent directory traversal
func ValidateFilePath(path string) error {
	if len(path) == 0 {
		return errors.New(errors.ERR_VALIDATION_REQUIRED,
			"file path cannot be empty", errors.LayerCommand)
	}

	// Check for directory traversal patterns
	dangerousPatterns := []string{
		"../", "..\\", "/..", "\\..",
		"//", "\\\\",
		"~", "$",
	}

	for _, pattern := range dangerousPatterns {
		if strings.Contains(path, pattern) {
			return errors.New(errors.ERR_VALIDATION_FIELD,
				fmt.Sprintf("file path contains potentially dangerous pattern: %s", pattern),
				errors.LayerCommand).WithContext("pattern", pattern)
		}
	}

	// Path should not start with / or \ (should be relative)
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "\\") {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"absolute paths are not allowed", errors.LayerCommand)
	}

	return nil
}
