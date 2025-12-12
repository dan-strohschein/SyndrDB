package syndrQL

import (
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
)

// PrepareStatement represents a parsed PREPARE command
// Syntax: PREPARE statement_name AS query_text
type PrepareStatement struct {
	StatementName string // Name of the prepared statement (alphanumeric + underscore, max 64 chars)
	QueryText     string // The query text with $1, $2, ... placeholders
}

// ParsePrepareStatement parses a PREPARE command
// Syntax: PREPARE statement_name AS query_text
// Example: PREPARE user_query AS SELECT * FROM Users WHERE age > $1 AND status = $2
func ParsePrepareStatement(tokens []Token, logger *zap.SugaredLogger) (*PrepareStatement, error) {
	if len(tokens) < 4 {
		return nil, fmt.Errorf("invalid PREPARE syntax: expected 'PREPARE statement_name AS query_text'")
	}

	// Token 0: PREPARE
	if tokens[0].Type != TOKEN_PREPARE {
		return nil, fmt.Errorf("expected PREPARE keyword, got %s", tokens[0].Value)
	}

	// Token 1: statement_name (identifier)
	if tokens[1].Type != TOKEN_IDENT {
		return nil, fmt.Errorf("expected statement name, got %s", tokens[1].Value)
	}

	statementName := tokens[1].Value

	// Validate statement name: alphanumeric + underscore, max 64 chars
	if err := validateStatementName(statementName); err != nil {
		return nil, fmt.Errorf("invalid statement name: %w", err)
	}

	// Token 2: AS
	if tokens[2].Type != TOKEN_AS {
		return nil, fmt.Errorf("expected AS keyword after statement name, got %s", tokens[2].Value)
	}

	// Remaining tokens: query_text
	// Reconstruct the query text from the remaining tokens
	queryTokens := tokens[3:]
	if len(queryTokens) == 0 {
		return nil, fmt.Errorf("missing query text after AS keyword")
	}

	// Rebuild query text from tokens
	// This preserves the original query structure with parameters
	var queryBuilder strings.Builder
	for i, tok := range queryTokens {
		if i > 0 && needsSpaceBefore(tok) {
			queryBuilder.WriteString(" ")
		}
		queryBuilder.WriteString(tok.Value)
	}

	queryText := queryBuilder.String()

	logger.Debugf("Parsed PREPARE statement: name=%s, query=%s", statementName, queryText)

	return &PrepareStatement{
		StatementName: statementName,
		QueryText:     queryText,
	}, nil
}

// validateStatementName validates the prepared statement name
// Rules: alphanumeric + underscore, max 64 characters
func validateStatementName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("statement name cannot be empty")
	}

	if len(name) > 64 {
		return fmt.Errorf("statement name exceeds maximum length of 64 characters (got %d)", len(name))
	}

	// Must match: ^[a-zA-Z0-9_]+$
	validName := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if !validName.MatchString(name) {
		return fmt.Errorf("statement name must contain only alphanumeric characters and underscores")
	}

	return nil
}

// needsSpaceBefore determines if a space is needed before a token when reconstructing query text
func needsSpaceBefore(tok Token) bool {
	switch tok.Type {
	case TOKEN_COMMA, TOKEN_SEMICOLON, TOKEN_DOT, TOKEN_RPAREN, TOKEN_RBRACKET, TOKEN_RBRACE:
		return false
	default:
		return true
	}
}
