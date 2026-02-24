package server

import (
	"syndrdb/src/internal/syndrQL"
	"testing"
)

func TestSessionDefaultIsolationLevel(t *testing.T) {
	session := &Session{}
	// Default should be REPEATABLE READ
	level := session.GetEffectiveIsolationLevel()
	if level != syndrQL.IsolationRepeatableRead {
		t.Errorf("default isolation level = %v, want REPEATABLE READ", level)
	}
}

func TestSessionSetDefaultIsolationLevel(t *testing.T) {
	session := &Session{}
	session.SetDefaultIsolationLevel(syndrQL.IsolationReadCommitted)

	level := session.GetEffectiveIsolationLevel()
	if level != syndrQL.IsolationReadCommitted {
		t.Errorf("isolation level = %v, want READ COMMITTED", level)
	}
}

func TestSessionTransactionIsolationOverridesDefault(t *testing.T) {
	session := &Session{}
	session.SetDefaultIsolationLevel(syndrQL.IsolationRepeatableRead)

	// Simulate beginning a transaction with a different isolation level
	session.mu.Lock()
	session.TransactionActive = true
	session.TransactionIsolation = syndrQL.IsolationReadCommitted
	session.TransactionStatus = TransactionStatusActive
	session.mu.Unlock()

	level := session.GetEffectiveIsolationLevel()
	if level != syndrQL.IsolationReadCommitted {
		t.Errorf("isolation level = %v, want READ COMMITTED (transaction override)", level)
	}
}

func TestSessionReadUncommittedMapsToReadCommitted(t *testing.T) {
	session := &Session{}
	session.SetDefaultIsolationLevel(syndrQL.IsolationReadUncommitted)

	level := session.GetEffectiveIsolationLevel()
	if level != syndrQL.IsolationReadCommitted {
		t.Errorf("isolation level = %v, want READ COMMITTED (mapped from READ UNCOMMITTED)", level)
	}
}

func TestSessionIsReadCommitted(t *testing.T) {
	session := &Session{}
	if session.IsReadCommitted() {
		t.Error("default session should not be READ COMMITTED")
	}

	session.SetDefaultIsolationLevel(syndrQL.IsolationReadCommitted)
	if !session.IsReadCommitted() {
		t.Error("session with READ COMMITTED default should return true")
	}
}

func TestSessionSetTransactionIsolation(t *testing.T) {
	session := &Session{}
	session.SetTransactionIsolation(syndrQL.IsolationReadCommitted)

	session.mu.RLock()
	pending := session.PendingIsolationLevel
	session.mu.RUnlock()

	if pending != syndrQL.IsolationReadCommitted {
		t.Errorf("pending isolation level = %v, want READ COMMITTED", pending)
	}
}

func TestBeginTransactionWithIsolationLevel(t *testing.T) {
	session := &Session{}
	session.BeginTransaction("tx1", 0, nil, syndrQL.IsolationReadCommitted)

	level := session.GetEffectiveIsolationLevel()
	if level != syndrQL.IsolationReadCommitted {
		t.Errorf("isolation level = %v, want READ COMMITTED", level)
	}
}

func TestBeginTransactionUsesPendingLevel(t *testing.T) {
	session := &Session{}
	session.SetTransactionIsolation(syndrQL.IsolationReadCommitted)
	session.BeginTransaction("tx1", 0, nil)

	level := session.GetEffectiveIsolationLevel()
	if level != syndrQL.IsolationReadCommitted {
		t.Errorf("isolation level = %v, want READ COMMITTED (from pending)", level)
	}

	// Pending should be cleared
	session.mu.RLock()
	pending := session.PendingIsolationLevel
	session.mu.RUnlock()
	if pending != syndrQL.IsolationDefault {
		t.Errorf("pending isolation level = %v, want DEFAULT (should be cleared after BEGIN)", pending)
	}
}

func TestBeginTransactionExplicitOverridesPending(t *testing.T) {
	session := &Session{}
	session.SetTransactionIsolation(syndrQL.IsolationReadCommitted)
	session.BeginTransaction("tx1", 0, nil, syndrQL.IsolationRepeatableRead)

	level := session.GetEffectiveIsolationLevel()
	if level != syndrQL.IsolationRepeatableRead {
		t.Errorf("isolation level = %v, want REPEATABLE READ (explicit overrides pending)", level)
	}
}

func TestClearTransactionStateResetsIsolation(t *testing.T) {
	session := &Session{}
	session.BeginTransaction("tx1", 0, nil, syndrQL.IsolationReadCommitted)

	// Commit clears transaction state
	session.CommitTransaction()

	// After commit, should fall back to session/server default
	level := session.GetEffectiveIsolationLevel()
	if level != syndrQL.IsolationRepeatableRead {
		t.Errorf("isolation level after commit = %v, want REPEATABLE READ (server default)", level)
	}
}

func TestAbortTransactionResetsIsolation(t *testing.T) {
	session := &Session{}
	session.BeginTransaction("tx1", 0, nil, syndrQL.IsolationReadCommitted)

	session.AbortTransaction()

	level := session.GetEffectiveIsolationLevel()
	if level != syndrQL.IsolationRepeatableRead {
		t.Errorf("isolation level after abort = %v, want REPEATABLE READ (server default)", level)
	}
}

func TestSessionDefaultPersistsAcrossTransactions(t *testing.T) {
	session := &Session{}
	session.SetDefaultIsolationLevel(syndrQL.IsolationReadCommitted)

	// First transaction
	session.BeginTransaction("tx1", 0, nil)
	level1 := session.GetEffectiveIsolationLevel()
	session.CommitTransaction()

	// Second transaction
	session.BeginTransaction("tx2", 0, nil)
	level2 := session.GetEffectiveIsolationLevel()
	session.CommitTransaction()

	if level1 != syndrQL.IsolationReadCommitted {
		t.Errorf("first transaction isolation = %v, want READ COMMITTED", level1)
	}
	if level2 != syndrQL.IsolationReadCommitted {
		t.Errorf("second transaction isolation = %v, want READ COMMITTED", level2)
	}
}

func TestIsTransactionCommandExtended(t *testing.T) {
	tests := []struct {
		command  string
		expected bool
	}{
		{"BEGIN TRANSACTION", true},
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"SAVEPOINT \"sp1\"", true},
		{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", true},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED", true},
		{"SHOW TRANSACTION ISOLATION LEVEL", true},
		{"SELECT * FROM \"test\"", false},
		{"SET something_else", false},
		{"SHOW BUNDLES", false},
	}

	for _, tt := range tests {
		t.Run(tt.command, func(t *testing.T) {
			got := IsTransactionCommand(tt.command)
			if got != tt.expected {
				t.Errorf("IsTransactionCommand(%q) = %v, want %v", tt.command, got, tt.expected)
			}
		})
	}
}
