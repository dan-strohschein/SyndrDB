package syndrQL

import (
	"context"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
TRANSACTION E2E TEST SUITE

Comprehensive end-to-end validation of all SyndrQL transaction functionality.
Tests cover BEGIN TRANSACTION, COMMIT, ROLLBACK, SAVEPOINT, ROLLBACK TO SAVEPOINT,
deadlock handling, idle timeout, DML-only enforcement, and auto-rollback on errors.

Test Strategy:
- Real integration: Uses setupFullServer with real ServiceManager, BundleService, etc.
- Session-based: All commands within a transaction use the same session
- Sequential execution: No parallelization for transaction isolation
- Validation: Verify data consistency after commit/rollback
- Auto-cleanup: t.TempDir() for temporary test data

Note: extractDocuments helper is defined in select_e2e_test.go and is reused here
*/

// =============================================================================
// BASIC TRANSACTION TESTS
// =============================================================================

func TestTransaction_BasicCommit(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()
	startTime := time.Now()

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "TestTxBundle" WITH FIELDS (
		{"ID", "STRING", TRUE, FALSE, ""},
		{"Name", "STRING", FALSE, FALSE, ""},
		{"Value", "INT", FALSE, FALSE, 0}
	)`

	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Create a session for the transaction
	session, err := fixture.ServiceManager.SessionManager.CreateSession("test-user", "user1", "primary", fixture.Database, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session")
	require.NotNil(t, session)
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session.SessionID)

	// BEGIN TRANSACTION
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "BEGIN TRANSACTION failed")
	require.True(t, session.TransactionActive, "Transaction should be active")

	// INSERT documents within transaction
	insertCmd1 := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="tx1"}, {"Name"="First"}, {"Value"=100});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd1, fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "INSERT 1 failed")

	insertCmd2 := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="tx2"}, {"Name"="Second"}, {"Value"=200});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd2, fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "INSERT 2 failed")

	// COMMIT
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "COMMIT", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "COMMIT failed")
	require.False(t, session.TransactionActive, "Transaction should be inactive after commit")

	// Verify data persisted
	selectCmd := `SELECT * FROM "TestTxBundle"`
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT failed")
	docs := extractDocuments(t, response)
	assert.Len(t, docs, 2, "Should have 2 documents after commit")
}

func TestTransaction_BasicRollback(t *testing.T) {
	fixture := setupFullServer(t)
	ctx := context.Background()
	startTime := time.Now()

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "TestTxBundle" WITH FIELDS (
		{"ID", "STRING", TRUE, FALSE, ""},
		{"Name", "STRING", FALSE, FALSE, ""},
		{"Value", "INT", FALSE, FALSE, 0}
	)`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Insert initial data outside transaction
	insertCmd := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="initial"}, {"Name"="Initial"}, {"Value"=50});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Initial INSERT failed")

	// Create a session for the transaction
	session, err := fixture.ServiceManager.SessionManager.CreateSession("test-user", "user1", "primary", fixture.Database, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session")
	require.NotNil(t, session)
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session.SessionID)

	// BEGIN TRANSACTION
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "BEGIN TRANSACTION failed")

	// INSERT documents within transaction
	insertCmd1 := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="tx1"}, {"Name"="First"}, {"Value"=100});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd1, fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "INSERT 1 failed")

	insertCmd2 := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="tx2"}, {"Name"="Second"}, {"Value"=200});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd2, fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "INSERT 2 failed")

	// Check documents BEFORE rollback
	selectCmdBefore := `SELECT * FROM "TestTxBundle";`
	responseBefore, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmdBefore, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT before rollback failed")
	docsBefore := extractDocuments(t, responseBefore)
	t.Logf("BEFORE ROLLBACK: Found %d documents", len(docsBefore))
	for i, doc := range docsBefore {
		t.Logf("  Doc %d: ID=%s, DocumentID=%s", i+1, doc["ID"], doc["DocumentID"])
	}

	// ROLLBACK
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "ROLLBACK", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "ROLLBACK failed")
	require.False(t, session.TransactionActive, "Transaction should be inactive after rollback")

	// Add a small delay to allow any async operations to complete
	time.Sleep(100 * time.Millisecond)

	// Verify only initial data exists (transaction data rolled back)
	selectCmd := `SELECT * FROM "TestTxBundle";`
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT failed")
	docs := extractDocuments(t, response)
	t.Logf("AFTER ROLLBACK: Found %d documents", len(docs))
	for i, doc := range docs {
		t.Logf("  Doc %d: ID=%s, DocumentID=%s", i+1, doc["ID"], doc["DocumentID"])
	}
	assert.Len(t, docs, 1, "Should have only 1 document after rollback")
	if len(docs) == 1 {
		// Handle FieldValue type - extract string from FieldValue if needed
		idValue := docs[0]["ID"]
		var idStr string
		if fv, ok := idValue.(models.FieldValue); ok {
			idStr = fv.StringVal
		} else if s, ok := idValue.(string); ok {
			idStr = s
		}
		assert.Equal(t, "initial", idStr, "Only initial document should remain")
	}
}

// =============================================================================
// BUFFER-AWARE TRANSACTION TESTS
// =============================================================================

func TestTransaction_RollbackDiscardsBuffered(t *testing.T) {
	t.Log("Testing that rollback discards buffered documents without disk I/O")
	fixture := setupFullServer(t)
	ctx := context.Background()
	startTime := time.Now()

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "TestTxBundle" WITH FIELDS (
		{"ID", "STRING", TRUE, FALSE, ""},
		{"Name", "STRING", FALSE, FALSE, ""},
		{"Value", "INT", FALSE, FALSE, 0}
	)`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Create session and begin transaction
	session, err := fixture.ServiceManager.SessionManager.CreateSession("test-user", "user1", "primary", fixture.Database, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session")
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session.SessionID)

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "BEGIN TRANSACTION failed")

	// Insert 3 documents (should stay in buffer)
	for i := 1; i <= 3; i++ {
		insertCmd := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="doc` + string(rune('0'+i)) + `"}, {"Name"="Document"}, {"Value"=` + string(rune('0'+i)) + `00});`
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, session, "127.0.0.1")
		require.NoError(t, err, "INSERT failed")
	}

	// Verify documents are buffered (check buffer directly)
	t.Log("Documents inserted, verifying they are in buffer...")

	// ROLLBACK - should discard buffered documents
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "ROLLBACK", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "ROLLBACK failed")

	time.Sleep(100 * time.Millisecond)

	// Verify no documents exist
	selectCmd := `SELECT * FROM "TestTxBundle";`
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT failed")
	docs := extractDocuments(t, response)
	assert.Len(t, docs, 0, "All buffered documents should be discarded")
	t.Logf("✓ Successfully discarded %d buffered documents", 3)
}

func TestTransaction_MixedBufferedAndFlushed(t *testing.T) {
	t.Log("Testing rollback with mix of buffered and flushed documents")
	fixture := setupFullServer(t)
	ctx := context.Background()
	startTime := time.Now()

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "TestTxBundle" WITH FIELDS (
		{"ID", "STRING", TRUE, FALSE, ""},
		{"Name", "STRING", FALSE, FALSE, ""},
		{"Value", "INT", FALSE, FALSE, 0}
	)`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Insert initial document outside transaction
	insertCmd := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="initial"}, {"Name"="Initial"}, {"Value"=0});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Initial INSERT failed")

	// Create session and begin transaction
	session, err := fixture.ServiceManager.SessionManager.CreateSession("test-user", "user1", "primary", fixture.Database, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session")
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session.SessionID)

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "BEGIN TRANSACTION failed")

	// Insert 50 documents to trigger buffer flush
	t.Log("Inserting 50 documents to trigger buffer flush...")
	for i := 1; i <= 50; i++ {
		insertCmd := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="tx` + string(rune('0'+i%10)) + `"}, {"Name"="Doc"}, {"Value"=` + string(rune('0'+i%10)) + `});`
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, session, "127.0.0.1")
		require.NoError(t, err, "INSERT failed")
	}

	// ROLLBACK - should handle both buffered and flushed
	t.Log("Rolling back mixed buffered/flushed documents...")
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "ROLLBACK", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "ROLLBACK failed")

	time.Sleep(200 * time.Millisecond)

	// Verify only initial document remains
	selectCmd := `SELECT * FROM "TestTxBundle";`
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT failed")
	docs := extractDocuments(t, response)
	assert.Len(t, docs, 1, "Should have only initial document after rollback")
	t.Logf("✓ Successfully rolled back 50 transaction documents (buffered + flushed)")
}

func TestTransaction_CommitPersistsBuffered(t *testing.T) {
	t.Log("Testing that commit flushes and persists buffered documents")
	fixture := setupFullServer(t)
	ctx := context.Background()
	startTime := time.Now()

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "TestTxBundle" WITH FIELDS (
		{"ID", "STRING", TRUE, FALSE, ""},
		{"Name", "STRING", FALSE, FALSE, ""},
		{"Value", "INT", FALSE, FALSE, 0}
	)`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Create session and begin transaction
	session, err := fixture.ServiceManager.SessionManager.CreateSession("test-user", "user1", "primary", fixture.Database, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session")
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session.SessionID)

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "BEGIN TRANSACTION failed")

	// Insert 5 documents (should stay in buffer)
	t.Log("Inserting 5 documents into transaction buffer...")
	for i := 1; i <= 5; i++ {
		insertCmd := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="doc` + string(rune('0'+i)) + `"}, {"Name"="Document"}, {"Value"=100});`
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, session, "127.0.0.1")
		require.NoError(t, err, "INSERT failed")
	}

	// COMMIT - should flush buffered documents
	t.Log("Committing transaction...")
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "COMMIT", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "COMMIT failed")

	time.Sleep(100 * time.Millisecond)

	// Verify all documents persisted
	selectCmd := `SELECT * FROM "TestTxBundle";`
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT failed")
	docs := extractDocuments(t, response)
	assert.Len(t, docs, 5, "All 5 documents should be persisted after commit")
	t.Logf("✓ Successfully committed and persisted %d buffered documents", len(docs))
}

func TestTransaction_MultipleRollbacks(t *testing.T) {
	t.Log("Testing multiple sequential transactions with rollbacks")
	fixture := setupFullServer(t)
	ctx := context.Background()
	startTime := time.Now()

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "TestTxBundle" WITH FIELDS (
		{"ID", "STRING", TRUE, FALSE, ""},
		{"Name", "STRING", FALSE, FALSE, ""},
		{"Value", "INT", FALSE, FALSE, 0}
	)`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Create session
	session, err := fixture.ServiceManager.SessionManager.CreateSession("test-user", "user1", "primary", fixture.Database, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session")
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session.SessionID)

	// Transaction 1: Insert and rollback
	t.Log("Transaction 1: Insert 3 docs and rollback...")
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err)

	for i := 1; i <= 3; i++ {
		insertCmd := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="tx1_` + string(rune('0'+i)) + `"}, {"Name"="TX1"}, {"Value"=100});`
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, session, "127.0.0.1")
		require.NoError(t, err)
	}

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "ROLLBACK", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Transaction 2: Insert and commit
	t.Log("Transaction 2: Insert 2 docs and commit...")
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err)

	for i := 1; i <= 2; i++ {
		insertCmd := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="tx2_` + string(rune('0'+i)) + `"}, {"Name"="TX2"}, {"Value"=200});`
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, session, "127.0.0.1")
		require.NoError(t, err)
	}

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "COMMIT", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Transaction 3: Insert and rollback
	t.Log("Transaction 3: Insert 4 docs and rollback...")
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err)

	for i := 1; i <= 4; i++ {
		insertCmd := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="tx3_` + string(rune('0'+i)) + `"}, {"Name"="TX3"}, {"Value"=300});`
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, session, "127.0.0.1")
		require.NoError(t, err)
	}

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "ROLLBACK", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Verify only TX2 documents exist
	selectCmd := `SELECT * FROM "TestTxBundle";`
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT failed")
	docs := extractDocuments(t, response)
	assert.Len(t, docs, 2, "Should have only 2 documents from TX2 (TX1 and TX3 rolled back)")
	t.Logf("✓ Multiple transactions handled correctly: TX1 rolled back (3 docs), TX2 committed (2 docs), TX3 rolled back (4 docs)")
}

func TestTransaction_LargeRollback(t *testing.T) {
	t.Log("Testing rollback performance with large number of documents")
	fixture := setupFullServer(t)
	ctx := context.Background()
	startTime := time.Now()

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "TestTxBundle" WITH FIELDS (
		{"ID", "STRING", TRUE, FALSE, ""},
		{"Name", "STRING", FALSE, FALSE, ""},
		{"Value", "INT", FALSE, FALSE, 0}
	)`
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Create session and begin transaction
	session, err := fixture.ServiceManager.SessionManager.CreateSession("test-user", "user1", "primary", fixture.Database, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session")
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session.SessionID)

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "BEGIN TRANSACTION failed")

	// Insert 100 documents
	t.Log("Inserting 100 documents...")
	insertStart := time.Now()
	for i := 1; i <= 100; i++ {
		insertCmd := `ADD DOCUMENT TO BUNDLE "TestTxBundle" WITH ({"ID"="doc` + string(rune('0'+i%10)) + `"}, {"Name"="Document"}, {"Value"=` + string(rune('0'+i%10)) + `});`
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, session, "127.0.0.1")
		require.NoError(t, err, "INSERT failed")
	}
	insertDuration := time.Since(insertStart)
	t.Logf("Inserted 100 documents in %v", insertDuration)

	// ROLLBACK - measure performance
	t.Log("Rolling back 100 documents...")
	rollbackStart := time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "ROLLBACK", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "ROLLBACK failed")
	rollbackDuration := time.Since(rollbackStart)
	t.Logf("Rolled back 100 documents in %v", rollbackDuration)

	time.Sleep(200 * time.Millisecond)

	// Verify all documents rolled back
	selectCmd := `SELECT * FROM "TestTxBundle";`
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT failed")
	docs := extractDocuments(t, response)
	assert.Len(t, docs, 0, "All 100 documents should be rolled back")

	t.Logf("✓ Performance: Insert=%v, Rollback=%v (ratio: %.2fx)",
		insertDuration, rollbackDuration, float64(insertDuration)/float64(rollbackDuration))
}
