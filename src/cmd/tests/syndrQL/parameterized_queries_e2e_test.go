/*
Package syndrQL contains end-to-end tests for parameterized queries.

This file tests the PREPARE/EXECUTE/DEALLOCATE prepared statement functionality,
including parameter binding, SQL injection prevention, and cache behavior.
Tests follow the existing E2E testing patterns and use the delimiter-based protocol
for parameter passing.
*/
package syndrQL

import (
	"context"
	"fmt"
	"strings"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// TestPreparedStatement_BasicPrepare tests basic PREPARE statement creation
func TestPreparedStatement_BasicPrepare(t *testing.T) {
	fixture := setupFullServer(t)
	defer func() {
		// Cleanup handled by t.TempDir()
	}()

	setupBundles(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a session for prepared statements
	session, err := fixture.ServiceManager.SessionManager.CreateSession("testuser", "testuser-id", fixture.Database.Name, fixture.Database, "test-conn-1", 30*time.Minute, "127.0.0.1", "TestClient")
	if err != nil || session == nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	prepareCmd := `PREPARE get_author_by_name AS SELECT * FROM "Authors" WHERE "Name" == $1`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, prepareCmd, fixture.Logger, startTime, session, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		result := fmt.Sprintf("%v", cmdResp.Result)
		if !strings.Contains(result, "Prepared statement") || !strings.Contains(result, "created successfully") {
			t.Errorf("Expected success message, got: %v", result)
		}
	} else {
		t.Errorf("Expected CommandResponse, got: %T", response)
	}

	t.Log("✓ Basic PREPARE statement succeeded")
}

// TestPreparedStatement_ExecuteBasic tests executing a prepared statement with parameters
func TestPreparedStatement_ExecuteBasic(t *testing.T) {
	fixture := setupFullServer(t)
	defer func() {
		// Cleanup handled by t.TempDir()
	}()

	setupBundles(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a session for prepared statements
	session, err := fixture.ServiceManager.SessionManager.CreateSession("testuser", "testuser-id", fixture.Database.Name, fixture.Database, "test-conn-2", 30*time.Minute, "127.0.0.1", "TestClient")
	if err != nil || session == nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Prepare statement
	prepareCmd := `PREPARE get_author AS SELECT * FROM "Authors" WHERE "Name" == $1`
	startTime := time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, prepareCmd, fixture.Logger, startTime, session, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	// Execute with parameter using delimiter protocol
	executeCmd := "EXECUTE get_author"
	params := []string{"Strohschein"}

	startTime = time.Now()
	response, err := server.CommandDirectorWithParams(ctx, fixture.Database, *fixture.ServiceManager, executeCmd, params, fixture.Logger, startTime, session, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to execute prepared statement: %v", err)
	}

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount < 1 {
			t.Errorf("Expected at least 1 result, got %d", cmdResp.ResultCount)
		}
		t.Logf("✓ EXECUTE returned %d results", cmdResp.ResultCount)
	}
}

// TestPreparedStatement_DeallocateBasic tests DEALLOCATE functionality
func TestPreparedStatement_DeallocateBasic(t *testing.T) {
	fixture := setupFullServer(t)
	defer func() {
		// Cleanup handled by t.TempDir()
	}()

	setupBundles(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a session for prepared statements
	session, err := fixture.ServiceManager.SessionManager.CreateSession("testuser", "testuser-id", fixture.Database.Name, fixture.Database, "test-conn-3", 30*time.Minute, "127.0.0.1", "TestClient")
	if err != nil || session == nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Prepare statement
	prepareCmd := `PREPARE my_statement AS SELECT * FROM "Authors" WHERE "Name" == $1`
	startTime := time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, prepareCmd, fixture.Logger, startTime, session, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	// Deallocate it
	deallocateCmd := "DEALLOCATE my_statement"
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, deallocateCmd, fixture.Logger, startTime, session, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to deallocate statement: %v", err)
	}

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		result := fmt.Sprintf("%v", cmdResp.Result)
		if !strings.Contains(result, "deallocated successfully") {
			t.Errorf("Expected success message, got: %v", result)
		}
	}

	// Try to execute after deallocation - should fail
	executeCmd := "EXECUTE my_statement"
	params := []string{"Strohschein"}

	startTime = time.Now()
	response, err = server.CommandDirectorWithParams(ctx, fixture.Database, *fixture.ServiceManager, executeCmd, params, fixture.Logger, startTime, session, "127.0.0.1")

	if err == nil {
		t.Errorf("Expected error executing deallocated statement, got success: %v", response)
	}

	t.Log("✓ DEALLOCATE succeeded and statement no longer executable")
}

// TestPreparedStatement_SQLInjectionPrevention tests that parameters prevent SQL injection
func TestPreparedStatement_SQLInjectionPrevention(t *testing.T) {
	fixture := setupFullServer(t)
	defer func() {
		// Cleanup handled by t.TempDir()
	}()

	setupBundles(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a session for prepared statements
	session, err := fixture.ServiceManager.SessionManager.CreateSession("testuser", "testuser-id", fixture.Database.Name, fixture.Database, "test-conn-4", 30*time.Minute, "127.0.0.1", "TestClient")
	if err != nil || session == nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Prepare statement
	prepareCmd := `PREPARE safe_query AS SELECT * FROM "Authors" WHERE "Name" == $1`
	startTime := time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, prepareCmd, fixture.Logger, startTime, session, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	// Try SQL injection attempts - should all be treated as literal strings
	injectionAttempts := []string{
		"Strohschein' OR '1'=='1",
		"'; DROP BUNDLE Authors; --",
		"' OR 1=1 --",
		"Strohschein' UNION SELECT * FROM Authors --",
	}

	for _, attempt := range injectionAttempts {
		executeCmd := "EXECUTE safe_query"
		params := []string{attempt}

		startTime = time.Now()
		response, err := server.CommandDirectorWithParams(ctx, fixture.Database, *fixture.ServiceManager, executeCmd, params, fixture.Logger, startTime, session, "127.0.0.1")

		if err != nil {
			t.Errorf("Injection attempt caused error (should treat as literal): %v", err)
			continue
		}

		if cmdResp, ok := response.(*server.CommandResponse); ok {
			// Should return 0 results since these strings don't match any actual names
			if cmdResp.ResultCount != 0 {
				t.Errorf("Injection attempt '%s' returned %d results (expected 0)", attempt, cmdResp.ResultCount)
			}
		}
	}

	t.Log("✓ All SQL injection attempts safely prevented")
}

// TestPreparedStatement_InvalidStatementName tests validation of statement names
func TestPreparedStatement_InvalidStatementName(t *testing.T) {
	fixture := setupFullServer(t)
	defer func() {
		// Cleanup handled by t.TempDir()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a session for prepared statements
	session, err := fixture.ServiceManager.SessionManager.CreateSession("testuser", "testuser-id", fixture.Database.Name, fixture.Database, "test-conn-5", 30*time.Minute, "127.0.0.1", "TestClient")
	if err != nil || session == nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Try to prepare with invalid statement name (contains special characters)
	prepareCmd := `PREPARE get-author@123 AS SELECT * FROM "Authors" WHERE "Name" == $1`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, prepareCmd, fixture.Logger, startTime, session, "127.0.0.1")

	if err == nil {
		t.Errorf("Expected error for invalid statement name, got success: %v", response)
	} else if !strings.Contains(err.Error(), "invalid statement name") && !strings.Contains(err.Error(), "illegal token") && !strings.Contains(err.Error(), "failed to tokenize") && !strings.Contains(err.Error(), "statement name") {
		t.Errorf("Expected error about invalid statement name, illegal token, or tokenization failure, got: %v", err)
	}

	t.Log("✓ Invalid statement name correctly rejected")
}

// TestPreparedStatement_ExecuteNotPrepared tests executing a non-existent statement
func TestPreparedStatement_ExecuteNotPrepared(t *testing.T) {
	fixture := setupFullServer(t)
	defer func() {
		// Cleanup handled by t.TempDir()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a session for prepared statements
	session, err := fixture.ServiceManager.SessionManager.CreateSession("testuser", "testuser-id", fixture.Database.Name, fixture.Database, "test-conn-6", 30*time.Minute, "127.0.0.1", "TestClient")
	if err != nil || session == nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Try to execute non-existent prepared statement
	executeCmd := "EXECUTE nonexistent_statement"
	params := []string{"value"}

	startTime := time.Now()
	response, err := server.CommandDirectorWithParams(ctx, fixture.Database, *fixture.ServiceManager, executeCmd, params, fixture.Logger, startTime, session, "127.0.0.1")

	if err == nil {
		t.Errorf("Expected error for non-existent statement, got success: %v", response)
	} else if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected 'not found' error, got: %v", err)
	}

	t.Log("✓ Non-existent prepared statement correctly rejected")
}
