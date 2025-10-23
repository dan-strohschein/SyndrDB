package server

import (
	"fmt"
	"regexp"
	"strings"
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
}

// DefaultSecurityConfig returns default security configuration
func DefaultSecurityConfig() *SecurityConfig {
	return &SecurityConfig{
		MaxCommandLength:          10000, // 10KB max command
		MaxUsernameLength:         64,
		MaxPasswordLength:         128,
		MinPasswordLength:         8,
		RequirePasswordComplexity: true,
		AllowedSpecialChars:       "!@#$%^&*()_+-=[]{}|;:,.<>?",
	}
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
func validateCommand(command string, config *SecurityConfig) error {
	if len(command) == 0 {
		return fmt.Errorf("command cannot be empty")
	}

	if len(command) > config.MaxCommandLength {
		return fmt.Errorf("command too long: maximum %d characters allowed", config.MaxCommandLength)
	}

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

	commandUpper := strings.ToUpper(command)
	for _, pattern := range suspiciousPatterns {
		if strings.Contains(commandUpper, strings.ToUpper(pattern)) {
			return fmt.Errorf("potentially malicious pattern detected in command: %s", pattern)
		}
	}

	// Special check for DELETE FROM (SQL injection) vs DELETE DOCUMENTS FROM (legitimate SyndrDB command)
	// Block "DELETE FROM" only if it's NOT part of "DELETE DOCUMENTS FROM"
	if strings.Contains(commandUpper, "DELETE FROM") && !strings.Contains(commandUpper, "DELETE DOCUMENTS FROM") {
		return fmt.Errorf("invalid DELETE syntax: use 'DELETE DOCUMENTS FROM <bundle>' instead of 'DELETE FROM'")
	}

	// Check for excessive special characters (potential obfuscation)
	specialCharCount := 0
	for _, char := range command {
		if !unicode.IsLetter(char) && !unicode.IsDigit(char) && !unicode.IsSpace(char) {
			specialCharCount++
		}
	}

	if float64(specialCharCount)/float64(len(command)) > 0.3 {
		return fmt.Errorf("command contains excessive special characters")
	}

	return nil
}

// validateUsername validates username format and content
func validateUsername(username string, config *SecurityConfig) error {
	if len(username) == 0 {
		return fmt.Errorf("username cannot be empty")
	}

	if len(username) > config.MaxUsernameLength {
		return fmt.Errorf("username too long: maximum %d characters allowed", config.MaxUsernameLength)
	}

	// Username should only contain alphanumeric characters, underscores, and hyphens
	validUsername := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	if !validUsername.MatchString(username) {
		return fmt.Errorf("username contains invalid characters: only letters, numbers, underscores, and hyphens allowed")
	}

	// Username should start with a letter
	if !unicode.IsLetter(rune(username[0])) {
		return fmt.Errorf("username must start with a letter")
	}

	// Check for reserved usernames
	reservedUsernames := []string{"admin", "root", "system", "guest", "anonymous", "public", "syndr", "syndrdb"}
	for _, reserved := range reservedUsernames {
		if strings.EqualFold(username, reserved) {
			return fmt.Errorf("username '%s' is reserved and cannot be used", username)
		}
	}

	return nil
}

// validatePassword validates password strength and security
func validatePassword(password string, config *SecurityConfig) error {
	if len(password) < config.MinPasswordLength {
		return fmt.Errorf("password too short: minimum %d characters required", config.MinPasswordLength)
	}

	if len(password) > config.MaxPasswordLength {
		return fmt.Errorf("password too long: maximum %d characters allowed", config.MaxPasswordLength)
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
		return fmt.Errorf("password must contain at least one: %s", strings.Join(missing, ", "))
	}

	// Check for common weak passwords
	weakPasswords := []string{
		"password", "123456", "qwerty", "admin", "letmein", "welcome",
		"password123", "admin123", "qwerty123", "123456789", "password1",
	}
	for _, weak := range weakPasswords {
		if strings.EqualFold(password, weak) {
			return fmt.Errorf("password is too common and easily guessable")
		}
	}

	return nil
}

// validateDatabaseName validates database name format
func validateDatabaseName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("database name cannot be empty")
	}

	if len(name) > 64 {
		return fmt.Errorf("database name too long: maximum 64 characters allowed")
	}

	// Database name should only contain alphanumeric characters, underscores, and hyphens
	validName := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	if !validName.MatchString(name) {
		return fmt.Errorf("database name contains invalid characters")
	}

	// Should start with a letter
	if !unicode.IsLetter(rune(name[0])) {
		return fmt.Errorf("database name must start with a letter")
	}

	return nil
}

// validateBundleName validates bundle name format
func validateBundleName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("bundle name cannot be empty")
	}

	if len(name) > 64 {
		return fmt.Errorf("bundle name too long: maximum 64 characters allowed")
	}

	// Bundle name should only contain alphanumeric characters, underscores, and hyphens
	validName := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	if !validName.MatchString(name) {
		return fmt.Errorf("bundle name contains invalid characters")
	}

	return nil
}

// validateGenericInput validates general input for basic security
func validateGenericInput(input string, config *SecurityConfig) error {
	if len(input) > config.MaxCommandLength {
		return fmt.Errorf("input too long: maximum %d characters allowed", config.MaxCommandLength)
	}

	// Check for null bytes
	if strings.Contains(input, "\x00") {
		return fmt.Errorf("input contains null bytes")
	}

	// Check for excessive control characters
	controlCharCount := 0
	for _, char := range input {
		if unicode.IsControl(char) && char != '\t' && char != '\n' && char != '\r' {
			controlCharCount++
		}
	}

	if controlCharCount > 5 {
		return fmt.Errorf("input contains excessive control characters")
	}

	return nil
}

// SanitizeInput removes or escapes potentially dangerous characters
func SanitizeInput(input string) string {
	// Remove null bytes
	input = strings.ReplaceAll(input, "\x00", "")

	// Remove other control characters except tab, newline, carriage return
	var sanitized strings.Builder
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
		return fmt.Errorf("file path cannot be empty")
	}

	// Check for directory traversal patterns
	dangerousPatterns := []string{
		"../", "..\\", "/..", "\\..",
		"//", "\\\\",
		"~", "$",
	}

	for _, pattern := range dangerousPatterns {
		if strings.Contains(path, pattern) {
			return fmt.Errorf("file path contains potentially dangerous pattern: %s", pattern)
		}
	}

	// Path should not start with / or \ (should be relative)
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "\\") {
		return fmt.Errorf("absolute paths are not allowed")
	}

	return nil
}
