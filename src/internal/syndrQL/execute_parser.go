package syndrQL

import (
	"fmt"

	"go.uber.org/zap"
)

// ExecuteStatement represents a parsed EXECUTE command
// Syntax: EXECUTE statement_name
// Note: Parameters are passed via the delimiter-based protocol, not in the SQL syntax
type ExecuteStatement struct {
	StatementName string // Name of the prepared statement to execute
}

// ParseExecuteStatement parses an EXECUTE command
// Syntax: EXECUTE statement_name
// Example: EXECUTE user_query
// Note: Parameter values are provided separately via the protocol layer (delimiter-separated)
func ParseExecuteStatement(tokens []Token, logger *zap.SugaredLogger, command ...string) (*ExecuteStatement, error) {
	// Reconstruct command if not provided
	originalCommand := ""
	if len(command) > 0 && command[0] != "" {
		originalCommand = command[0]
	} else {
		originalCommand = reconstructCommandFromTokens(tokens)
	}

	if len(tokens) < 2 {
		return nil, CreateParserErrorWithToken(
			"invalid EXECUTE syntax: expected 'EXECUTE statement_name'",
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			"EXECUTE statement_name",
			originalCommand,
		)
	}

	// Token 0: EXECUTE
	if tokens[0].Type != TOKEN_EXECUTE {
		return nil, CreateParserErrorWithToken(
			fmt.Sprintf("expected EXECUTE keyword, got %s", tokens[0].Value),
			tokens[0],
			"EXECUTE",
			originalCommand,
		)
	}

	// Token 1: statement_name (identifier)
	if tokens[1].Type != TOKEN_IDENT {
		return nil, CreateParserErrorWithToken(
			fmt.Sprintf("expected statement name, got %s", tokens[1].Value),
			tokens[1],
			"statement name (identifier)",
			originalCommand,
		)
	}

	statementName := tokens[1].Value

	// Validate statement name
	if err := validateStatementName(statementName); err != nil {
		// Wrap validation error with parser context
		return nil, CreateParserErrorWithToken(
			fmt.Sprintf("invalid statement name: %v", err),
			tokens[1],
			"valid statement name (alphanumeric and underscore, max 64 chars)",
			originalCommand,
		)
	}

	// Check for extra tokens (PostgreSQL-style EXECUTE with inline params is NOT supported)
	if len(tokens) > 2 {
		// Check if it's EOF or semicolon
		if tokens[2].Type != TOKEN_EOF && tokens[2].Type != TOKEN_SEMICOLON {
			return nil, CreateParserErrorWithToken(
				fmt.Sprintf("unexpected token after statement name: %s. Parameters should be passed via protocol layer, not in SQL syntax", tokens[2].Value),
				tokens[2],
				"EOF or semicolon",
				originalCommand,
			)
		}
	}

	logger.Debugf("Parsed EXECUTE statement: name=%s", statementName)

	return &ExecuteStatement{
		StatementName: statementName,
	}, nil
}
