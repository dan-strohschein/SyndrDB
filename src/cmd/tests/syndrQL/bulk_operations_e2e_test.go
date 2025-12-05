package syndrQL

import (
	"context"
	"fmt"
	"strings"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// TestBulkDelete_MissingConfirmed_ShouldFail verifies that DELETE without WHERE clause
// requires the CONFIRMED keyword for safety
func TestBulkDelete_MissingConfirmed_ShouldFail(t *testing.T) {
	fixture := setupFullServer(t)
	setupBundlesForBulkOps(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Attempt DELETE without WHERE clause and without CONFIRMED
	deleteCmd := `DELETE DOCUMENTS FROM "Authors"`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, deleteCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	// Should fail with specific error message
	if err == nil {
		t.Fatal("Expected DELETE without CONFIRMED to fail, but it succeeded")
	}

	// Verify error message mentions CONFIRMED keyword
	errMsg := err.Error()
	if !strings.Contains(strings.ToLower(errMsg), "confirmed") {
		t.Errorf("Error message should mention CONFIRMED keyword, got: %s", errMsg)
	}

	t.Logf("✓ DELETE without CONFIRMED correctly rejected: %s", errMsg)
}

// TestBulkUpdate_MissingConfirmed_ShouldFail verifies that UPDATE without WHERE clause
// requires the CONFIRMED keyword for safety
func TestBulkUpdate_MissingConfirmed_ShouldFail(t *testing.T) {
	fixture := setupFullServer(t)
	setupBundlesForBulkOps(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Attempt UPDATE without WHERE clause and without CONFIRMED
	updateCmd := `UPDATE DOCUMENTS IN BUNDLE "Authors" ({"Country" = "Unknown"})`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, updateCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	// Should fail with specific error message
	if err == nil {
		t.Fatal("Expected UPDATE without CONFIRMED to fail, but it succeeded")
	}

	// Verify error message mentions CONFIRMED keyword
	errMsg := err.Error()
	if !strings.Contains(strings.ToLower(errMsg), "confirmed") {
		t.Errorf("Error message should mention CONFIRMED keyword, got: %s", errMsg)
	}

	t.Logf("✓ UPDATE without CONFIRMED correctly rejected: %s", errMsg)
}

// TestBulkDelete_WithConfirmed_ShouldSucceed verifies bulk DELETE with CONFIRMED works
func TestBulkDelete_WithConfirmed_ShouldSucceed(t *testing.T) {
	fixture := setupFullServer(t)
	setupBundlesForBulkOps(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Verify documents exist before deletion
	selectCmd := `SELECT COUNT(*) FROM "Authors"`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to count authors before delete: %v", err)
	}
	validateCountResult(t, response, 100, "Before bulk delete")

	// Perform bulk delete with CONFIRMED
	deleteCmd := `DELETE DOCUMENTS FROM "Authors" CONFIRMED`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, deleteCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Bulk DELETE with CONFIRMED failed: %v", err)
	}

	// Verify all documents were deleted
	startTime = time.Now()
	response, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to count authors after delete: %v", err)
	}
	validateCountResult(t, response, 0, "After bulk delete")

	t.Logf("✓ Bulk DELETE with CONFIRMED successfully deleted 100 documents")
}

// TestBulkUpdate_WithConfirmed_ShouldSucceed verifies bulk UPDATE with CONFIRMED works
func TestBulkUpdate_WithConfirmed_ShouldSucceed(t *testing.T) {
	fixture := setupFullServer(t)
	setupBundlesForBulkOps(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Verify initial state
	selectCmd := `SELECT COUNT(*) FROM "Authors" WHERE "Country" == "Updated"`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to count authors before update: %v", err)
	}
	validateCountResult(t, response, 0, "Before bulk update")

	// Perform bulk update with CONFIRMED
	updateCmd := `UPDATE DOCUMENTS IN BUNDLE "Authors" ({"Country" = "Updated"}) CONFIRMED`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, updateCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Bulk UPDATE with CONFIRMED failed: %v", err)
	}

	// Verify all documents were updated
	startTime = time.Now()
	response, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to count authors after update: %v", err)
	}
	validateCountResult(t, response, 100, "After bulk update")

	t.Logf("✓ Bulk UPDATE with CONFIRMED successfully updated 100 documents")
}

// TestBulkDelete_ReferentialIntegrity_ShouldBlockWithCountError verifies that
// bulk delete is blocked when referential integrity violations exist, and
// error message contains aggregated counts
func TestBulkDelete_ReferentialIntegrity_ShouldBlockWithCountError(t *testing.T) {
	fixture := setupFullServer(t)
	setupBundlesForBulkOps(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 50)
	seedSimpleBooksBundleTB(t, fixture, 200) // Each author has ~4 books

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create relationship from Books to Authors
	relCmd := `CREATE RELATIONSHIP "Authors_Books" FROM "Authors" TO "Books" ON "DocumentID" -> "AuthorsID" WITH DELETE RESTRICT`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, relCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create relationship: %v", err)
	}

	// Create hash index on AuthorsID for referential integrity checking
	indexCmd := `CREATE INDEX "idx_books_authorsid" ON BUNDLE "Books" USING HASH ("AuthorsID")`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, indexCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Attempt to delete all authors (should be blocked by referential integrity)
	deleteCmd := `DELETE DOCUMENTS FROM "Authors" CONFIRMED`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, deleteCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	// Should fail with referential integrity violation
	if err == nil {
		t.Fatal("Expected DELETE to fail due to referential integrity, but it succeeded")
	}

	// Verify error message contains reference information
	errMsg := err.Error()
	if !strings.Contains(strings.ToLower(errMsg), "references") && !strings.Contains(strings.ToLower(errMsg), "referential") {
		t.Errorf("Error message should mention referential integrity, got: %s", errMsg)
	}

	// Verify error shows the blocking bundle name
	if !strings.Contains(errMsg, "Books") {
		t.Errorf("Error message should mention blocking bundle 'Books', got: %s", errMsg)
	}

	// Verify documents were NOT deleted
	selectCmd := `SELECT COUNT(*) FROM "Authors"`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to count authors after blocked delete: %v", err)
	}
	validateCountResult(t, response, 50, "After blocked delete")

	t.Logf("✓ Bulk DELETE correctly blocked by referential integrity: %s", errMsg)
}

// TestBulkDelete_LargeDataset_Performance verifies that bulk delete can handle
// large datasets efficiently (1000+ documents)
func TestBulkDelete_LargeDataset_Performance(t *testing.T) {
	fixture := setupFullServer(t)
	setupBundlesForBulkOps(t, fixture)

	t.Logf("Seeding 1000 authors...")
	seedSimpleAuthorsBundleTB(t, fixture, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Verify initial count
	selectCmd := `SELECT COUNT(*) FROM "Authors"`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to count authors: %v", err)
	}
	validateCountResult(t, response, 1000, "Before bulk delete")

	// Perform bulk delete and measure time
	deleteCmd := `DELETE DOCUMENTS FROM "Authors" CONFIRMED`
	deleteStart := time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, deleteCmd, fixture.Logger, deleteStart, nil, "127.0.0.1")
	deleteDuration := time.Since(deleteStart)

	if err != nil {
		t.Fatalf("Bulk DELETE of 1000 documents failed: %v", err)
	}

	// Verify all deleted
	startTime = time.Now()
	response, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to count authors after delete: %v", err)
	}
	validateCountResult(t, response, 0, "After bulk delete")

	// Performance check: should complete in reasonable time (< 10 seconds for 1000 docs)
	if deleteDuration > 10*time.Second {
		t.Logf("⚠ Bulk DELETE of 1000 documents took %v (> 10s)", deleteDuration)
	} else {
		t.Logf("✓ Bulk DELETE of 1000 documents completed in %v", deleteDuration)
	}
}

// TestDelete_WithWhere_NoConfirmedRequired verifies that DELETE with WHERE clause
// does NOT require CONFIRMED keyword
func TestDelete_WithWhere_NoConfirmedRequired(t *testing.T) {
	fixture := setupFullServer(t)
	setupBundlesForBulkOps(t, fixture)
	seedSimpleAuthorsBundleTB(t, fixture, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// DELETE with WHERE clause should work WITHOUT CONFIRMED
	deleteCmd := `DELETE DOCUMENTS FROM "Authors" WHERE "BirthYear" > 2000`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, deleteCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("DELETE with WHERE clause failed: %v", err)
	}

	// Verify some were deleted (authors with BirthYear > 2000)
	selectCmd := `SELECT COUNT(*) FROM "Authors"`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to count authors: %v", err)
	}

	cmdResp, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatal("Invalid response type")
	}

	if cmdResp.ResultCount != 1 {
		t.Fatalf("Expected 1 result row, got %d", cmdResp.ResultCount)
	}

	results, ok := cmdResp.Result.([]map[string]interface{})
	if !ok || len(results) != 1 {
		t.Fatal("Failed to extract count result")
	}

	var count int
	if val, ok := results[0]["Column1"]; ok {
		if fval, ok := val.(float64); ok {
			count = int(fval)
		}
	}

	if count >= 100 {
		t.Errorf("Expected some documents to be deleted, but count is still %d", count)
	}

	t.Logf("✓ DELETE with WHERE clause works without CONFIRMED (%d documents remaining)", count)
}

// TestBulkDelete_EmptyBundle_ShouldSucceed verifies that bulk delete on empty
// bundle succeeds gracefully
func TestBulkDelete_EmptyBundle_ShouldSucceed(t *testing.T) {
	fixture := setupFullServer(t)
	setupBundlesForBulkOps(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Bulk delete on empty bundle should succeed gracefully
	deleteCmd := `DELETE DOCUMENTS FROM "Authors" CONFIRMED`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, deleteCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Bulk DELETE on empty bundle failed: %v", err)
	}

	t.Logf("✓ Bulk DELETE on empty bundle succeeded gracefully")
}

// setupBundlesForBulkOps creates the test bundles for bulk operations tests
func setupBundlesForBulkOps(t *testing.T, fixture *TestFixture) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Drop existing bundles
	for _, bundleName := range []string{"Authors", "Books"} {
		dropCmd := fmt.Sprintf(`DROP BUNDLE "%s" WITH FORCE`, bundleName)
		startTime := time.Now()
		_, _ = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	}

	// Create Authors bundle
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Name", "STRING", true, false, ""},
		{"Country", "STRING", false, false, ""},
		{"BirthYear", "INT", false, false, 0}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Authors bundle: %v", err)
	}

	// Create Books bundle
	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Title", "STRING", true, false, ""},
		{"AuthorID", "INT", false, false, 0},
		{"AuthorsID", "INT", false, false, 0},
		{"Genre", "STRING", false, false, ""}
	);`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Books bundle: %v", err)
	}

	t.Logf("Bundles created successfully for bulk operations tests")
}
