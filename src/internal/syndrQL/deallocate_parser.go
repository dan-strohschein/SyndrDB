package syndrQL

import (
	"fmt"

	"go.uber.org/zap"
)

// DeallocateStatement represents a parsed DEALLOCATE command
// Syntax: DEALLOCATE statement_name
type DeallocateStatement struct {
	StatementName string // Name of the prepared statement to deallocate
}

// ParseDeallocateStatement parses a DEALLOCATE command
// Syntax: DEALLOCATE statement_name
// Example: DEALLOCATE user_query
func ParseDeallocateStatement(tokens []Token, logger *zap.SugaredLogger) (*DeallocateStatement, error) {
	if len(tokens) < 2 {
		return nil, fmt.Errorf("invalid DEALLOCATE syntax: expected 'DEALLOCATE statement_name'")
	}

	// Token 0: DEALLOCATE
	if tokens[0].Type != TOKEN_DEALLOCATE {
		return nil, fmt.Errorf("expected DEALLOCATE keyword, got %s", tokens[0].Value)
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

	// Check for extra tokens
	if len(tokens) > 2 {
		if tokens[2].Type != TOKEN_EOF && tokens[2].Type != TOKEN_SEMICOLON {
			return nil, fmt.Errorf("unexpected token after statement name: %s", tokens[2].Value)
		}
	}

	logger.Debugf("Parsed DEALLOCATE statement: name=%s", statementName)

	return &DeallocateStatement{
		StatementName: statementName,
	}, nil
}
