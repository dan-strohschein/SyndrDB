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
func ParseExecuteStatement(tokens []Token, logger *zap.SugaredLogger) (*ExecuteStatement, error) {
	if len(tokens) < 2 {
		return nil, fmt.Errorf("invalid EXECUTE syntax: expected 'EXECUTE statement_name'")
	}

	// Token 0: EXECUTE
	if tokens[0].Type != TOKEN_EXECUTE {
		return nil, fmt.Errorf("expected EXECUTE keyword, got %s", tokens[0].Value)
	}

	// Token 1: statement_name (identifier)
	if tokens[1].Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected statement name, got %s", tokens[1].Value)
	}

	statementName := tokens[1].Value

	// Validate statement name
	if err := validateStatementName(statementName); err != nil {
		return nil, fmt.Errorf("invalid statement name: %w", err)
	}

	// Check for extra tokens (PostgreSQL-style EXECUTE with inline params is NOT supported)
	if len(tokens) > 2 {
		// Check if it's EOF or semicolon
		if tokens[2].Type != TOKEN_EOF && tokens[2].Type != TOKEN_SEMICOLON {
			return nil, fmt.Errorf("unexpected token after statement name: %s. Parameters should be passed via protocol layer, not in SQL syntax", tokens[2].Value)
		}
	}

	logger.Debugf("Parsed EXECUTE statement: name=%s", statementName)

	return &ExecuteStatement{
		StatementName: statementName,
	}, nil
}
