package server

import (
	"strconv"
	"strings"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"
)

// ParameterDelimiter matches the client-side delimiter for parameter separation
// Using ASCII ENQ (Enquiry) for parameter separation in prepared statement protocol.
const ParameterDelimiter = "\x05"

// ParseParameterizedCommand parses a command with delimiter-separated parameters
// Format: EXECUTE stmt_name\x05param1\x05param2\x05...\x04
// Returns the command part and the parameter values (with escape sequences unescaped)
// MED-003: Validates individual parameter value lengths to prevent memory exhaustion DoS
func ParseParameterizedCommand(rawCommand string) (command string, params []string, hasParams bool, err error) {
	// Check if command contains parameter delimiter
	if !strings.Contains(rawCommand, ParameterDelimiter) {
		return rawCommand, nil, false, nil
	}

	// Split on parameter delimiter
	parts := strings.Split(rawCommand, ParameterDelimiter)
	if len(parts) < 2 {
		return rawCommand, nil, false, nil
	}

	// First part is the command
	command = parts[0]

	// Get security config for validation
	securityConfig := DefaultSecurityConfig()

	// Remaining parts are parameters (unescape special characters)
	params = make([]string, 0, len(parts)-1)
	for i := 1; i < len(parts); i++ {
		unescaped := unescapeParameterValue(parts[i])
		
		// MED-003: Validate individual parameter value length
		if err := ValidateParameterValue(unescaped, securityConfig); err != nil {
			return "", nil, false, err
		}
		
		params = append(params, unescaped)
	}

	return command, params, true, nil
}

// unescapeParameterValue unescapes special characters in parameter values
// - \x05\x05 -> \x05
// - \x04\x04 -> \x04
func unescapeParameterValue(value string) string {
	// Fast path: no escaped sequences
	if !strings.Contains(value, "\x04\x04") && !strings.Contains(value, "\x05\x05") {
		return value
	}

	var result strings.Builder
	result.Grow(len(value))

	i := 0
	for i < len(value) {
		ch := value[i]

		// Check for escaped sequences
		if ch == '\x04' && i+1 < len(value) && value[i+1] == '\x04' {
			result.WriteByte('\x04')
			i += 2 // Skip both characters
		} else if ch == '\x05' && i+1 < len(value) && value[i+1] == '\x05' {
			result.WriteByte('\x05')
			i += 2 // Skip both characters
		} else {
			result.WriteByte(ch)
			i++
		}
	}

	return result.String()
}

// CreateParameterContext creates a ParameterContext from string parameter values
// This performs runtime type coercion based on the expected types in the query
func CreateParameterContext(params []string) (*syndrQL.ParameterContext, error) {
	// Convert string parameters to interface{} slice
	interfaceParams := make([]interface{}, len(params))
	for i, param := range params {
		// Store as string - the evaluator will perform runtime coercion
		// when comparing against document fields (see evaluator.go type coercion logic)
		interfaceParams[i] = param
	}

	// Create parameter context (validates count against MaxParametersPerQuery)
	paramContext, err := syndrQL.NewParameterContext(interfaceParams)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("parameter_count", strconv.Itoa(len(params)))
	}

	return paramContext, nil
}
