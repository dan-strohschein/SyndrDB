package syndrQL

import (
	"testing"
)

func TestParseSetTransactionIsolationLevel(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedType   TransactionType
		expectedLevel  IsolationLevel
		expectError    bool
	}{
		{
			name:          "READ COMMITTED",
			input:         "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
			expectedType:  TransactionSetIsolation,
			expectedLevel: IsolationReadCommitted,
		},
		{
			name:          "READ UNCOMMITTED",
			input:         "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
			expectedType:  TransactionSetIsolation,
			expectedLevel: IsolationReadUncommitted,
		},
		{
			name:          "REPEATABLE READ",
			input:         "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			expectedType:  TransactionSetIsolation,
			expectedLevel: IsolationRepeatableRead,
		},
		{
			name:          "SERIALIZABLE",
			input:         "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
			expectedType:  TransactionSetIsolation,
			expectedLevel: IsolationSerializable,
		},
		{
			name:        "missing ISOLATION keyword",
			input:       "SET TRANSACTION LEVEL READ COMMITTED",
			expectError: true,
		},
		{
			name:        "missing LEVEL keyword",
			input:       "SET TRANSACTION ISOLATION READ COMMITTED",
			expectError: true,
		},
		{
			name:        "incomplete - missing level",
			input:       "SET TRANSACTION ISOLATION LEVEL",
			expectError: true,
		},
		{
			name:        "invalid level keyword",
			input:       "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
			expectError: true,
		},
		{
			name:        "READ without qualifier",
			input:       "SET TRANSACTION ISOLATION LEVEL READ",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.input)
			tokens, err := tokenizer.Tokenize()
			if err != nil {
				t.Fatalf("tokenize error: %v", err)
			}
			// Remove EOF
			if len(tokens) > 0 && tokens[len(tokens)-1].Type == TOKEN_EOF {
				tokens = tokens[:len(tokens)-1]
			}

			node, err := ParseTransactionCommand(tokens)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if node.Type != tt.expectedType {
				t.Errorf("type = %s, want %s", node.Type, tt.expectedType)
			}
			if node.IsolationLevel != tt.expectedLevel {
				t.Errorf("isolation level = %v, want %v", node.IsolationLevel, tt.expectedLevel)
			}
		})
	}
}

func TestParseSetSessionCharacteristics(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedLevel IsolationLevel
		expectError   bool
	}{
		{
			name:          "READ COMMITTED",
			input:         "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
			expectedLevel: IsolationReadCommitted,
		},
		{
			name:          "REPEATABLE READ",
			input:         "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			expectedLevel: IsolationRepeatableRead,
		},
		{
			name:        "missing AS",
			input:       "SET SESSION CHARACTERISTICS TRANSACTION ISOLATION LEVEL READ COMMITTED",
			expectError: true,
		},
		{
			name:        "incomplete",
			input:       "SET SESSION CHARACTERISTICS AS",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.input)
			tokens, err := tokenizer.Tokenize()
			if err != nil {
				t.Fatalf("tokenize error: %v", err)
			}
			if len(tokens) > 0 && tokens[len(tokens)-1].Type == TOKEN_EOF {
				tokens = tokens[:len(tokens)-1]
			}

			node, err := ParseTransactionCommand(tokens)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if node.Type != TransactionSetSessionIsolation {
				t.Errorf("type = %s, want %s", node.Type, TransactionSetSessionIsolation)
			}
			if node.IsolationLevel != tt.expectedLevel {
				t.Errorf("isolation level = %v, want %v", node.IsolationLevel, tt.expectedLevel)
			}
		})
	}
}

func TestParseBeginTransactionIsolationLevel(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedLevel IsolationLevel
		expectError   bool
	}{
		{
			name:          "basic BEGIN TRANSACTION",
			input:         "BEGIN TRANSACTION",
			expectedLevel: IsolationDefault,
		},
		{
			name:          "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
			input:         "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
			expectedLevel: IsolationReadCommitted,
		},
		{
			name:          "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			input:         "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			expectedLevel: IsolationRepeatableRead,
		},
		{
			name:          "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
			input:         "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
			expectedLevel: IsolationSerializable,
		},
		{
			name:        "BEGIN TRANSACTION with invalid trailing token",
			input:       "BEGIN TRANSACTION SOMETHING",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.input)
			tokens, err := tokenizer.Tokenize()
			if err != nil {
				t.Fatalf("tokenize error: %v", err)
			}
			if len(tokens) > 0 && tokens[len(tokens)-1].Type == TOKEN_EOF {
				tokens = tokens[:len(tokens)-1]
			}

			node, err := ParseTransactionCommand(tokens)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if node.Type != TransactionBegin {
				t.Errorf("type = %s, want %s", node.Type, TransactionBegin)
			}
			if node.IsolationLevel != tt.expectedLevel {
				t.Errorf("isolation level = %v, want %v", node.IsolationLevel, tt.expectedLevel)
			}
		})
	}
}

func TestParseShowTransactionIsolationLevel(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:  "valid SHOW",
			input: "SHOW TRANSACTION ISOLATION LEVEL",
		},
		{
			name:        "incomplete SHOW",
			input:       "SHOW TRANSACTION ISOLATION",
			expectError: true,
		},
		{
			name:        "extra tokens",
			input:       "SHOW TRANSACTION ISOLATION LEVEL EXTRA",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.input)
			tokens, err := tokenizer.Tokenize()
			if err != nil {
				t.Fatalf("tokenize error: %v", err)
			}
			if len(tokens) > 0 && tokens[len(tokens)-1].Type == TOKEN_EOF {
				tokens = tokens[:len(tokens)-1]
			}

			node, err := ParseTransactionCommand(tokens)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if node.Type != TransactionShowIsolation {
				t.Errorf("type = %s, want %s", node.Type, TransactionShowIsolation)
			}
		})
	}
}

func TestIsolationLevelString(t *testing.T) {
	tests := []struct {
		level    IsolationLevel
		expected string
	}{
		{IsolationDefault, "DEFAULT"},
		{IsolationReadUncommitted, "READ UNCOMMITTED"},
		{IsolationReadCommitted, "READ COMMITTED"},
		{IsolationRepeatableRead, "REPEATABLE READ"},
		{IsolationSerializable, "SERIALIZABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("String() = %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestParseIsolationLevelString(t *testing.T) {
	tests := []struct {
		input       string
		expected    IsolationLevel
		expectError bool
	}{
		{"READ UNCOMMITTED", IsolationReadUncommitted, false},
		{"READ COMMITTED", IsolationReadCommitted, false},
		{"REPEATABLE READ", IsolationRepeatableRead, false},
		{"SERIALIZABLE", IsolationSerializable, false},
		{"SNAPSHOT", IsolationDefault, true},
		{"", IsolationDefault, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseIsolationLevelString(tt.input)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestExistingTransactionCommandsStillWork(t *testing.T) {
	// Ensure existing commands are not broken by the new tokens
	tests := []struct {
		name         string
		input        string
		expectedType TransactionType
	}{
		{"BEGIN TRANSACTION", "BEGIN TRANSACTION", TransactionBegin},
		{"COMMIT", "COMMIT", TransactionCommit},
		{"ROLLBACK", "ROLLBACK", TransactionRollback},
		{"SAVEPOINT", `SAVEPOINT "sp1"`, TransactionSavepoint},
		{"ROLLBACK TO SAVEPOINT", `ROLLBACK TO SAVEPOINT "sp1"`, TransactionRollbackToSavepoint},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.input)
			tokens, err := tokenizer.Tokenize()
			if err != nil {
				t.Fatalf("tokenize error: %v", err)
			}
			if len(tokens) > 0 && tokens[len(tokens)-1].Type == TOKEN_EOF {
				tokens = tokens[:len(tokens)-1]
			}

			node, err := ParseTransactionCommand(tokens)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if node.Type != tt.expectedType {
				t.Errorf("type = %s, want %s", node.Type, tt.expectedType)
			}
		})
	}
}
