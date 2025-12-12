package syndrQL

import (
	"fmt"
	"regexp"
)

// TransactionNode represents a transaction command
type TransactionNode struct {
	Type          TransactionType // BEGIN, COMMIT, ROLLBACK, SAVEPOINT, ROLLBACK TO SAVEPOINT
	SavepointName string          // For SAVEPOINT and ROLLBACK TO SAVEPOINT commands
}

// TransactionType represents the type of transaction command
type TransactionType string

const (
	TransactionBegin               TransactionType = "BEGIN"
	TransactionCommit              TransactionType = "COMMIT"
	TransactionRollback            TransactionType = "ROLLBACK"
	TransactionSavepoint           TransactionType = "SAVEPOINT"
	TransactionRollbackToSavepoint TransactionType = "ROLLBACK_TO_SAVEPOINT"
)

// savepointNameRegex validates alphanumeric-only savepoint names
var savepointNameRegex = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

// ParseTransactionCommand parses transaction-related commands
// Supports:
//   - BEGIN TRANSACTION
//   - COMMIT
//   - ROLLBACK
//   - SAVEPOINT "name"
//   - ROLLBACK TO SAVEPOINT "name"
func ParseTransactionCommand(tokens []Token) (*TransactionNode, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty token list")
	}

	firstToken := tokens[0].Type

	switch firstToken {
	case TOKEN_BEGIN:
		return parseBeginTransaction(tokens)
	case TOKEN_COMMIT:
		return parseCommit(tokens)
	case TOKEN_ROLLBACK:
		return parseRollback(tokens)
	case TOKEN_SAVEPOINT:
		return parseSavepoint(tokens)
	default:
		return nil, fmt.Errorf("unexpected token type for transaction command: %s", firstToken)
	}
}

// parseBeginTransaction parses BEGIN TRANSACTION command
// Syntax: BEGIN TRANSACTION
func parseBeginTransaction(tokens []Token) (*TransactionNode, error) {
	if len(tokens) < 2 {
		return nil, fmt.Errorf("incomplete BEGIN command, expected: BEGIN TRANSACTION")
	}

	if tokens[0].Type != TOKEN_BEGIN {
		return nil, fmt.Errorf("expected BEGIN, got %s", tokens[0].Type)
	}

	if tokens[1].Type != TOKEN_TRANSACTION {
		return nil, fmt.Errorf("expected TRANSACTION after BEGIN, got %s", tokens[1].Type)
	}

	if len(tokens) > 2 {
		return nil, fmt.Errorf("unexpected tokens after BEGIN TRANSACTION")
	}

	return &TransactionNode{
		Type: TransactionBegin,
	}, nil
}

// parseCommit parses COMMIT command
// Syntax: COMMIT
func parseCommit(tokens []Token) (*TransactionNode, error) {
	if len(tokens) != 1 {
		return nil, fmt.Errorf("unexpected tokens after COMMIT")
	}

	if tokens[0].Type != TOKEN_COMMIT {
		return nil, fmt.Errorf("expected COMMIT, got %s", tokens[0].Type)
	}

	return &TransactionNode{
		Type: TransactionCommit,
	}, nil
}

// parseRollback parses ROLLBACK and ROLLBACK TO SAVEPOINT commands
// Syntax:
//   - ROLLBACK
//   - ROLLBACK TO SAVEPOINT "name"
func parseRollback(tokens []Token) (*TransactionNode, error) {
	if len(tokens) == 1 {
		// Simple ROLLBACK
		if tokens[0].Type != TOKEN_ROLLBACK {
			return nil, fmt.Errorf("expected ROLLBACK, got %s", tokens[0].Type)
		}

		return &TransactionNode{
			Type: TransactionRollback,
		}, nil
	}

	// Check for ROLLBACK TO SAVEPOINT "name"
	if len(tokens) < 4 {
		return nil, fmt.Errorf("incomplete ROLLBACK TO SAVEPOINT command, expected: ROLLBACK TO SAVEPOINT \"name\"")
	}

	if tokens[0].Type != TOKEN_ROLLBACK {
		return nil, fmt.Errorf("expected ROLLBACK, got %s", tokens[0].Type)
	}

	if tokens[1].Type != TOKEN_TO {
		return nil, fmt.Errorf("expected TO after ROLLBACK, got %s", tokens[1].Type)
	}

	if tokens[2].Type != TOKEN_SAVEPOINT {
		return nil, fmt.Errorf("expected SAVEPOINT after ROLLBACK TO, got %s", tokens[2].Type)
	}

	if tokens[3].Type != TOKEN_STRING {
		return nil, fmt.Errorf("expected quoted savepoint name, got %s", tokens[3].Type)
	}

	savepointName := tokens[3].Value

	// Validate savepoint name is alphanumeric only
	if !savepointNameRegex.MatchString(savepointName) {
		return nil, fmt.Errorf("savepoint name must contain only alphanumeric characters (a-z, A-Z, 0-9), got: %s", savepointName)
	}

	if len(tokens) > 4 {
		return nil, fmt.Errorf("unexpected tokens after ROLLBACK TO SAVEPOINT \"name\"")
	}

	return &TransactionNode{
		Type:          TransactionRollbackToSavepoint,
		SavepointName: savepointName,
	}, nil
}

// parseSavepoint parses SAVEPOINT command
// Syntax: SAVEPOINT "name"
func parseSavepoint(tokens []Token) (*TransactionNode, error) {
	if len(tokens) != 2 {
		return nil, fmt.Errorf("incomplete SAVEPOINT command, expected: SAVEPOINT \"name\"")
	}

	if tokens[0].Type != TOKEN_SAVEPOINT {
		return nil, fmt.Errorf("expected SAVEPOINT, got %s", tokens[0].Type)
	}

	if tokens[1].Type != TOKEN_STRING {
		return nil, fmt.Errorf("expected quoted savepoint name, got %s", tokens[1].Type)
	}

	savepointName := tokens[1].Value

	// Validate savepoint name is alphanumeric only
	if !savepointNameRegex.MatchString(savepointName) {
		return nil, fmt.Errorf("savepoint name must contain only alphanumeric characters (a-z, A-Z, 0-9), got: %s", savepointName)
	}

	return &TransactionNode{
		Type:          TransactionSavepoint,
		SavepointName: savepointName,
	}, nil
}
