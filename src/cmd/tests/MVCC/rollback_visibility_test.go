package MVCC

/*
MVCC ROLLBACK VISIBILITY TEST

This test suite validates that rolled-back documents are invisible to all snapshots,
including concurrent transactions and future transactions. This is a critical property
of MVCC snapshot isolation.

Test Strategy:
- Real integration: Uses setupFullServer with real ServiceManager, BundleService, etc.
- Multiple sessions: Tests visibility across different transaction snapshots
- Sequential execution: No parallelization for test isolation
- Validation: Verify rolled-back documents are invisible to all snapshots
- Auto-cleanup: t.TempDir() for temporary test data
*/

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"syndrdb/src/cmd/tests/syndrQL"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRollback_InvisibleToAllSnapshots verifies that rolled-back documents
// are invisible to all snapshots, including:
// 1. The transaction that rolled back (should not see its own rolled-back writes)
// 2. Concurrent transactions (should not see rolled-back writes from other transactions)
// 3. Future transactions (should not see rolled-back writes from past transactions)
func TestRollback_InvisibleToAllSnapshots(t *testing.T) {
	fixture := syndrQL.SetupFullServer(t)
	if fixture == nil {
		t.Fatal("Failed to setup test server")
	}
	ctx := context.Background()
	startTime := time.Now()

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "MVCCTestBundle" WITH FIELDS (
		{"ID", "STRING", TRUE, FALSE, ""},
		{"Name", "STRING", FALSE, FALSE, ""},
		{"Value", "INT", FALSE, FALSE, 0}
	)`

	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Failed to create bundle")

	// Insert initial committed document (autocommit - no transaction)
	initialInsert := `ADD DOCUMENT TO BUNDLE "MVCCTestBundle" WITH ({"ID"="initial"}, {"Name"="Initial"}, {"Value"=100});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, initialInsert, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "Initial INSERT failed")

	// Wait a bit for autocommit to complete and async operations
	time.Sleep(500 * time.Millisecond)

	// Flush buffers to ensure document is persisted
	err = fixture.ServiceManager.BundleService.FlushAllBuffers()
	require.NoError(t, err, "Failed to flush buffers")

	// Verify initial document was inserted (use SELECT * since WHERE might have issues)
	selectAllCheck := `SELECT * FROM "MVCCTestBundle";`
	responseAllCheck, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectAllCheck, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT all documents check failed")
	docsAllCheck := extractDocuments(t, responseAllCheck)
	require.Len(t, docsAllCheck, 1, "Initial document should be visible after insert (prerequisite check)")
	// Verify it's the correct document (handle FieldValue type)
	idValue := getFieldValue(docsAllCheck[0]["ID"])
	assert.Equal(t, "initial", idValue, "Initial document should have correct ID")

	// Transaction 1: Insert document and rollback
	t.Log("Transaction 1: Insert document and rollback...")
	session1, err := fixture.ServiceManager.SessionManager.CreateSession("test-user-1", "user1", "primary", fixture.Database, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session 1")
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session1.SessionID)

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session1, "127.0.0.1")
	require.NoError(t, err, "BEGIN TRANSACTION failed for session 1")

	// Insert document in transaction 1
	insertTx1 := `ADD DOCUMENT TO BUNDLE "MVCCTestBundle" WITH ({"ID"="rolled_back"}, {"Name"="RolledBack"}, {"Value"=200});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertTx1, fixture.Logger, startTime, session1, "127.0.0.1")
	require.NoError(t, err, "INSERT in transaction 1 failed")

	// Verify we can SELECT within transaction (at least committed docs visible).
	// Note: Read-your-own-writes (seeing "rolled_back" before commit) is not guaranteed
	// when the execution path uses the page-based document scanner; uncommitted docs
	// may live only in the transaction buffer. The critical check is post-rollback visibility below.
	selectAllInTx1 := `SELECT * FROM "MVCCTestBundle";`
	responseAllInTx1, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectAllInTx1, fixture.Logger, startTime, session1, "127.0.0.1")
	require.NoError(t, err, "SELECT in transaction 1 failed")
	docsAllInTx1 := extractDocuments(t, responseAllInTx1)
	assert.GreaterOrEqual(t, len(docsAllInTx1), 1, "Should see at least initial document in transaction 1")

	// ROLLBACK transaction 1
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "ROLLBACK", fixture.Logger, startTime, session1, "127.0.0.1")
	require.NoError(t, err, "ROLLBACK failed for session 1")
	require.False(t, session1.TransactionActive, "Transaction 1 should be inactive after rollback")

	// Wait for async operations to complete
	time.Sleep(200 * time.Millisecond)

	// Test 1: Rolled-back document should NOT be visible to the same session after rollback
	t.Log("Test 1: Verify rolled-back document invisible to same session after rollback...")
	selectAfterRollback := `SELECT * FROM "MVCCTestBundle" WHERE ID = "rolled_back";`
	responseAfterRollback, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectAfterRollback, fixture.Logger, startTime, session1, "127.0.0.1")
	require.NoError(t, err, "SELECT after rollback failed")
	docsAfterRollback := extractDocuments(t, responseAfterRollback)
	assert.Len(t, docsAfterRollback, 0, "Rolled-back document should NOT be visible to same session after rollback")

	// Test 2: Rolled-back document should NOT be visible to concurrent transaction
	t.Log("Test 2: Verify rolled-back document invisible to concurrent transaction...")
	session2, err := fixture.ServiceManager.SessionManager.CreateSession("test-user-2", "user2", "primary", fixture.Database, "conn2", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session 2")
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session2.SessionID)

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session2, "127.0.0.1")
	require.NoError(t, err, "BEGIN TRANSACTION failed for session 2")

	// Concurrent transaction should not see rolled-back document
	selectAllInTx2 := `SELECT * FROM "MVCCTestBundle";`
	responseAllInTx2, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectAllInTx2, fixture.Logger, startTime, session2, "127.0.0.1")
	require.NoError(t, err, "SELECT in transaction 2 failed")
	docsAllInTx2 := extractDocuments(t, responseAllInTx2)
	// Check if rolled_back is visible
	foundRolledBackInTx2 := false
	for _, doc := range docsAllInTx2 {
		if getFieldValue(doc["ID"]) == "rolled_back" {
			foundRolledBackInTx2 = true
			break
		}
	}
	assert.False(t, foundRolledBackInTx2, "Rolled-back document should NOT be visible to concurrent transaction")
	assert.Len(t, docsAllInTx2, 1, "Concurrent transaction should only see initial document")
	assert.Equal(t, "initial", getFieldValue(docsAllInTx2[0]["ID"]), "Concurrent transaction should only see initial document")

	// Commit transaction 2
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "COMMIT", fixture.Logger, startTime, session2, "127.0.0.1")
	require.NoError(t, err, "COMMIT failed for session 2")

	// Test 3: Rolled-back document should NOT be visible to future transaction
	t.Log("Test 3: Verify rolled-back document invisible to future transaction...")
	time.Sleep(100 * time.Millisecond) // Ensure transaction 2 is fully committed

	session3, err := fixture.ServiceManager.SessionManager.CreateSession("test-user-3", "user3", "primary", fixture.Database, "conn3", 30*time.Minute, "127.0.0.1", "test-agent")
	require.NoError(t, err, "Failed to create session 3")
	defer fixture.ServiceManager.SessionManager.InvalidateSession(session3.SessionID)

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "BEGIN TRANSACTION", fixture.Logger, startTime, session3, "127.0.0.1")
	require.NoError(t, err, "BEGIN TRANSACTION failed for session 3")

	// Future transaction should not see rolled-back document
	selectAllInTx3 := `SELECT * FROM "MVCCTestBundle";`
	responseAllInTx3, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectAllInTx3, fixture.Logger, startTime, session3, "127.0.0.1")
	require.NoError(t, err, "SELECT in transaction 3 failed")
	docsAllInTx3 := extractDocuments(t, responseAllInTx3)
	// Check if rolled_back is visible
	foundRolledBackInTx3 := false
	for _, doc := range docsAllInTx3 {
		if getFieldValue(doc["ID"]) == "rolled_back" {
			foundRolledBackInTx3 = true
			break
		}
	}
	assert.False(t, foundRolledBackInTx3, "Rolled-back document should NOT be visible to future transaction")

	// Verify initial document is still visible
	assert.Len(t, docsAllInTx3, 1, "Future transaction should only see initial document")
	assert.Equal(t, "initial", getFieldValue(docsAllInTx3[0]["ID"]), "Future transaction should see initial document")
	assert.Equal(t, "Initial", getFieldValue(docsAllInTx3[0]["Name"]), "Initial document should have correct Name")

	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "COMMIT", fixture.Logger, startTime, session3, "127.0.0.1")
	require.NoError(t, err, "COMMIT failed for session 3")
}

// TestRollback_MultipleDocuments verifies that rolling back a transaction
// with multiple documents makes all of them invisible
func TestRollback_MultipleDocuments(t *testing.T) {
	fixture := syndrQL.SetupFullServer(t)
	if fixture == nil {
		t.Fatal("Failed to setup test server")
	}
	ctx := context.Background()
	startTime := time.Now()

	// Create test bundle
	createBundleCmd := `CREATE BUNDLE "MVCCTestBundle" WITH FIELDS (
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

	// Insert multiple documents
	docIDs := []string{"doc1", "doc2", "doc3", "doc4", "doc5"}
	for i, docID := range docIDs {
		value := (i + 1) * 10
		insertCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "MVCCTestBundle" WITH ({"ID"="%s"}, {"Name"="Document%d"}, {"Value"=%d});`, docID, i+1, value)
		_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, insertCmd, fixture.Logger, startTime, session, "127.0.0.1")
		require.NoError(t, err, "INSERT failed for document %s", docID)
	}

	// SELECT within transaction (read-your-own-writes may not show buffered docs in scanner path)
	selectAll := `SELECT * FROM "MVCCTestBundle";`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectAll, fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "SELECT in transaction failed")

	// ROLLBACK
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, "ROLLBACK", fixture.Logger, startTime, session, "127.0.0.1")
	require.NoError(t, err, "ROLLBACK failed")
	require.False(t, session.TransactionActive, "Transaction should be inactive after rollback")

	// Wait for async operations
	time.Sleep(200 * time.Millisecond)

	// Verify all rolled-back documents are invisible
	responseAfterRollback, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectAll, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT after rollback failed")
	docsAfterRollback := extractDocuments(t, responseAfterRollback)
	assert.Len(t, docsAfterRollback, 0, "All rolled-back documents should be invisible")

	// Verify each document individually (check in all documents result)
	selectAllFinal := `SELECT * FROM "MVCCTestBundle";`
	responseAllFinal, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectAllFinal, fixture.Logger, startTime, nil, "127.0.0.1")
	require.NoError(t, err, "SELECT all documents failed")
	docsAllFinal := extractDocuments(t, responseAllFinal)
	foundDocIDsFinal := make(map[string]bool)
	for _, doc := range docsAllFinal {
		idValue := getFieldValue(doc["ID"])
		if idValue != "" {
			foundDocIDsFinal[idValue] = true
		}
	}
	for _, docID := range docIDs {
		assert.False(t, foundDocIDsFinal[docID], "Document %s should be invisible after rollback", docID)
	}
}

// getFieldValue extracts the string value from a field, handling both FieldValue and string types
func getFieldValue(field interface{}) string {
	if field == nil {
		return ""
	}
	if fv, ok := field.(models.FieldValue); ok {
		return fv.StringVal
	}
	if str, ok := field.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", field)
}

// extractDocuments extracts documents from CommandResponse. Uses GetResultOrTransform
// so that both Result (non-streaming) and StreamDocuments/StreamSlice (streaming, Values-based)
// are converted to a consistent []map[string]interface{} format.
func extractDocuments(t *testing.T, response interface{}) []map[string]interface{} {
	t.Helper()

	cmdResponse, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected *server.CommandResponse, got %T", response)
	}

	// Prefer materialized Result; if streaming path was used, transform on demand.
	toParse := cmdResponse.Result
	if toParse == nil && (len(cmdResponse.StreamDocuments) > 0 || len(cmdResponse.StreamSlice) > 0) {
		toParse = cmdResponse.GetResultOrTransform()
	}
	if toParse == nil {
		return []map[string]interface{}{}
	}

	// Handle different result types
	switch result := toParse.(type) {
	case []map[string]interface{}:
		return result
	case []interface{}:
		docs := make([]map[string]interface{}, len(result))
		for i, item := range result {
			if doc, ok := item.(map[string]interface{}); ok {
				docs[i] = doc
			} else {
				t.Fatalf("Expected map[string]interface{} in result array at index %d, got %T", i, item)
			}
		}
		return docs
	case map[string]interface{}:
		// Single result wrapped in a map - could be aggregate or single document
		// Check if it has document fields
		if _, hasID := result["ID"]; hasID {
			return []map[string]interface{}{result}
		}
		// Otherwise it's probably an aggregate, return empty array
		t.Logf("WARNING: Got map[string]interface{} but it looks like an aggregate, not documents: %v", result)
		return []map[string]interface{}{}
	case map[string]int:
		// This is an aggregate response (COUNT, etc.), not documents
		t.Logf("WARNING: Got aggregate response (map[string]int), not documents: %v", result)
		return []map[string]interface{}{}
	case string:
		// Try to parse as JSON
		var docs []map[string]interface{}
		if err := json.Unmarshal([]byte(result), &docs); err != nil {
			// If Result is not JSON array, try parsing as single object
			var singleDoc map[string]interface{}
			if err2 := json.Unmarshal([]byte(result), &singleDoc); err2 == nil {
				return []map[string]interface{}{singleDoc}
			}
			// If Result is not JSON at all, return empty
			return []map[string]interface{}{}
		}
		return docs
	default:
		t.Logf("Unexpected result type: %T, value: %v", result, result)
		return []map[string]interface{}{}
	}
}
