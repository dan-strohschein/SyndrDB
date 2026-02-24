package syndrQL

import (
	"fmt"
	"regexp"
)

// IsolationLevel represents a transaction isolation level
type IsolationLevel int

const (
	IsolationDefault         IsolationLevel = iota // Use session default
	IsolationReadUncommitted                       // Mapped to ReadCommitted (like PostgreSQL)
	IsolationReadCommitted                         // Each statement sees latest committed data
	IsolationRepeatableRead                        // Snapshot Isolation (current default behavior)
	IsolationSerializable                          // SSI - not yet implemented (Phase 2)
)

// String returns the SQL-standard name for the isolation level
func (il IsolationLevel) String() string {
	switch il {
	case IsolationDefault:
		return "DEFAULT"
	case IsolationReadUncommitted:
		return "READ UNCOMMITTED"
	case IsolationReadCommitted:
		return "READ COMMITTED"
	case IsolationRepeatableRead:
		return "REPEATABLE READ"
	case IsolationSerializable:
		return "SERIALIZABLE"
	default:
		return "UNKNOWN"
	}
}

// ParseIsolationLevelString parses a string like "READ COMMITTED" into an IsolationLevel
func ParseIsolationLevelString(s string) (IsolationLevel, error) {
	switch s {
	case "READ UNCOMMITTED":
		return IsolationReadUncommitted, nil
	case "READ COMMITTED":
		return IsolationReadCommitted, nil
	case "REPEATABLE READ":
		return IsolationRepeatableRead, nil
	case "SERIALIZABLE":
		return IsolationSerializable, nil
	default:
		return IsolationDefault, fmt.Errorf("unknown isolation level: %s", s)
	}
}

// TransactionNode represents a transaction command
type TransactionNode struct {
	Type           TransactionType // BEGIN, COMMIT, ROLLBACK, SAVEPOINT, ROLLBACK TO SAVEPOINT, etc.
	SavepointName  string          // For SAVEPOINT and ROLLBACK TO SAVEPOINT commands
	IsolationLevel IsolationLevel  // For BEGIN TRANSACTION ISOLATION LEVEL and SET TRANSACTION
}

// TransactionType represents the type of transaction command
type TransactionType string

const (
	TransactionBegin               TransactionType = "BEGIN"
	TransactionCommit              TransactionType = "COMMIT"
	TransactionRollback            TransactionType = "ROLLBACK"
	TransactionSavepoint           TransactionType = "SAVEPOINT"
	TransactionRollbackToSavepoint TransactionType = "ROLLBACK_TO_SAVEPOINT"
	TransactionSetIsolation        TransactionType = "SET_ISOLATION"         // SET TRANSACTION ISOLATION LEVEL ...
	TransactionSetSessionIsolation TransactionType = "SET_SESSION_ISOLATION" // SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...
	TransactionShowIsolation       TransactionType = "SHOW_ISOLATION"        // SHOW TRANSACTION ISOLATION LEVEL
)

// savepointNameRegex validates alphanumeric-only savepoint names
var savepointNameRegex = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

// ParseTransactionCommand parses transaction-related commands
// Supports:
//   - BEGIN TRANSACTION [ISOLATION LEVEL ...]
//   - COMMIT
//   - ROLLBACK
//   - SAVEPOINT "name"
//   - ROLLBACK TO SAVEPOINT "name"
//   - SET TRANSACTION ISOLATION LEVEL ...
//   - SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...
//   - SHOW TRANSACTION ISOLATION LEVEL
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
	case TOKEN_SET:
		return parseSetCommand(tokens)
	case TOKEN_SHOW:
		return parseShowTransaction(tokens)
	default:
		return nil, fmt.Errorf("unexpected token type for transaction command: %s", firstToken)
	}
}

// parseBeginTransaction parses BEGIN TRANSACTION command
// Syntax: BEGIN TRANSACTION [ISOLATION LEVEL <level>]
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

	// Simple BEGIN TRANSACTION
	if len(tokens) == 2 {
		return &TransactionNode{
			Type: TransactionBegin,
		}, nil
	}

	// BEGIN TRANSACTION ISOLATION LEVEL <level>
	if tokens[2].Type == TOKEN_ISOLATION {
		level, err := parseIsolationLevel(tokens, 2)
		if err != nil {
			return nil, err
		}
		return &TransactionNode{
			Type:           TransactionBegin,
			IsolationLevel: level,
		}, nil
	}

	return nil, fmt.Errorf("unexpected token after BEGIN TRANSACTION: %s", tokens[2].Type)
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

// isContextualKeyword checks if a token is a contextual keyword (TOKEN_IDENT with a specific value).
// LEVEL, READ, COMMITTED, SESSION are not in the keywords map to avoid conflicts
// with field names, so they tokenize as TOKEN_IDENT. We check them contextually here.
func isContextualKeyword(tok Token, keyword string) bool {
	switch keyword {
	case "LEVEL":
		return tok.Type == TOKEN_LEVEL || (tok.Type == TOKEN_IDENT && tok.Value == "LEVEL")
	case "READ":
		return tok.Type == TOKEN_READ_KW || (tok.Type == TOKEN_IDENT && tok.Value == "READ")
	case "COMMITTED":
		return tok.Type == TOKEN_COMMITTED_KW || (tok.Type == TOKEN_IDENT && tok.Value == "COMMITTED")
	case "SESSION":
		return tok.Type == TOKEN_SESSION_KW || (tok.Type == TOKEN_IDENT && tok.Value == "SESSION")
	default:
		return false
	}
}

// parseSetCommand routes SET TRANSACTION and SET SESSION commands
func parseSetCommand(tokens []Token) (*TransactionNode, error) {
	if len(tokens) < 2 {
		return nil, fmt.Errorf("incomplete SET command")
	}

	if tokens[1].Type == TOKEN_TRANSACTION {
		return parseSetTransaction(tokens)
	}
	if isContextualKeyword(tokens[1], "SESSION") {
		return parseSetSessionIsolation(tokens)
	}
	return nil, fmt.Errorf("expected TRANSACTION or SESSION after SET, got %s", tokens[1].Type)
}

// parseSetTransaction parses SET TRANSACTION ISOLATION LEVEL <level>
func parseSetTransaction(tokens []Token) (*TransactionNode, error) {
	// SET TRANSACTION ISOLATION LEVEL <level>
	// [0]  [1]          [2]       [3]   [4...]
	if len(tokens) < 3 || tokens[2].Type != TOKEN_ISOLATION {
		return nil, fmt.Errorf("expected ISOLATION after SET TRANSACTION")
	}

	level, err := parseIsolationLevel(tokens, 2)
	if err != nil {
		return nil, err
	}

	return &TransactionNode{
		Type:           TransactionSetIsolation,
		IsolationLevel: level,
	}, nil
}

// parseSetSessionIsolation parses SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>
func parseSetSessionIsolation(tokens []Token) (*TransactionNode, error) {
	// SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>
	// [0]  [1]     [2]              [3] [4]          [5]       [6]   [7...]
	if len(tokens) < 8 {
		return nil, fmt.Errorf("incomplete SET SESSION CHARACTERISTICS command")
	}

	if tokens[2].Type != TOKEN_CHARACTERISTICS {
		return nil, fmt.Errorf("expected CHARACTERISTICS after SET SESSION, got %s", tokens[2].Type)
	}
	if tokens[3].Type != TOKEN_AS {
		return nil, fmt.Errorf("expected AS after SET SESSION CHARACTERISTICS, got %s", tokens[3].Type)
	}
	if tokens[4].Type != TOKEN_TRANSACTION {
		return nil, fmt.Errorf("expected TRANSACTION after AS, got %s", tokens[4].Type)
	}
	if tokens[5].Type != TOKEN_ISOLATION {
		return nil, fmt.Errorf("expected ISOLATION after TRANSACTION, got %s", tokens[5].Type)
	}
	// tokens[6] is LEVEL (contextual - checked in parseIsolationLevel)

	level, err := parseIsolationLevel(tokens, 5)
	if err != nil {
		return nil, err
	}

	return &TransactionNode{
		Type:           TransactionSetSessionIsolation,
		IsolationLevel: level,
	}, nil
}

// parseShowTransaction parses SHOW TRANSACTION ISOLATION LEVEL
func parseShowTransaction(tokens []Token) (*TransactionNode, error) {
	// SHOW TRANSACTION ISOLATION LEVEL
	// [0]   [1]         [2]       [3]
	if len(tokens) < 4 {
		return nil, fmt.Errorf("incomplete SHOW command, expected: SHOW TRANSACTION ISOLATION LEVEL")
	}

	if tokens[1].Type != TOKEN_TRANSACTION {
		return nil, fmt.Errorf("expected TRANSACTION after SHOW, got %s", tokens[1].Type)
	}
	if tokens[2].Type != TOKEN_ISOLATION {
		return nil, fmt.Errorf("expected ISOLATION after SHOW TRANSACTION, got %s", tokens[2].Type)
	}
	if !isContextualKeyword(tokens[3], "LEVEL") {
		return nil, fmt.Errorf("expected LEVEL after SHOW TRANSACTION ISOLATION, got %s", tokens[3].Type)
	}
	if len(tokens) > 4 {
		return nil, fmt.Errorf("unexpected tokens after SHOW TRANSACTION ISOLATION LEVEL")
	}

	return &TransactionNode{
		Type: TransactionShowIsolation,
	}, nil
}

// parseIsolationLevel parses ISOLATION LEVEL <level> starting at the given index.
// Valid levels: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
// Note: LEVEL, READ, and COMMITTED are contextual keywords (may be TOKEN_IDENT)
func parseIsolationLevel(tokens []Token, startIdx int) (IsolationLevel, error) {
	// tokens[startIdx] should be ISOLATION
	// tokens[startIdx+1] should be LEVEL (contextual)
	// tokens[startIdx+2...] should be the level keywords

	if startIdx+1 >= len(tokens) {
		return IsolationDefault, fmt.Errorf("expected LEVEL after ISOLATION")
	}
	if tokens[startIdx].Type != TOKEN_ISOLATION {
		return IsolationDefault, fmt.Errorf("expected ISOLATION, got %s", tokens[startIdx].Type)
	}
	if !isContextualKeyword(tokens[startIdx+1], "LEVEL") {
		return IsolationDefault, fmt.Errorf("expected LEVEL after ISOLATION, got %s (%s)", tokens[startIdx+1].Type, tokens[startIdx+1].Value)
	}

	levelIdx := startIdx + 2
	if levelIdx >= len(tokens) {
		return IsolationDefault, fmt.Errorf("expected isolation level after ISOLATION LEVEL")
	}

	// Check for READ (contextual keyword)
	if isContextualKeyword(tokens[levelIdx], "READ") {
		// READ UNCOMMITTED or READ COMMITTED
		if levelIdx+1 >= len(tokens) {
			return IsolationDefault, fmt.Errorf("expected UNCOMMITTED or COMMITTED after READ")
		}
		if tokens[levelIdx+1].Type == TOKEN_UNCOMMITTED {
			if levelIdx+2 < len(tokens) {
				return IsolationDefault, fmt.Errorf("unexpected tokens after READ UNCOMMITTED")
			}
			return IsolationReadUncommitted, nil
		}
		if isContextualKeyword(tokens[levelIdx+1], "COMMITTED") {
			if levelIdx+2 < len(tokens) {
				return IsolationDefault, fmt.Errorf("unexpected tokens after READ COMMITTED")
			}
			return IsolationReadCommitted, nil
		}
		return IsolationDefault, fmt.Errorf("expected UNCOMMITTED or COMMITTED after READ, got %s", tokens[levelIdx+1].Value)
	}

	if tokens[levelIdx].Type == TOKEN_REPEATABLE {
		// REPEATABLE READ
		if levelIdx+1 >= len(tokens) {
			return IsolationDefault, fmt.Errorf("expected READ after REPEATABLE")
		}
		if !isContextualKeyword(tokens[levelIdx+1], "READ") {
			return IsolationDefault, fmt.Errorf("expected READ after REPEATABLE, got %s", tokens[levelIdx+1].Value)
		}
		if levelIdx+2 < len(tokens) {
			return IsolationDefault, fmt.Errorf("unexpected tokens after REPEATABLE READ")
		}
		return IsolationRepeatableRead, nil
	}

	if tokens[levelIdx].Type == TOKEN_SERIALIZABLE {
		// SERIALIZABLE
		if levelIdx+1 < len(tokens) {
			return IsolationDefault, fmt.Errorf("unexpected tokens after SERIALIZABLE")
		}
		return IsolationSerializable, nil
	}

	return IsolationDefault, fmt.Errorf("unknown isolation level: %s", tokens[levelIdx].Value)
}
