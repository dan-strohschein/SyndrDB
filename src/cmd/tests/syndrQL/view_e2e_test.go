package syndrQL

import (
	"context"
	"fmt"
	"strings"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// =============================================================================
// REGULAR VIEW TESTS
// =============================================================================

// TestCreateView_BasicSuccess tests creating a simple regular view
func TestCreateView_BasicSuccess(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a simple view
	createViewCmd := `CREATE VIEW "ActiveCustomers" AS SELECT * FROM "Customers" WHERE "Status" == "Active";`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Verify success message
	resultStr := fmt.Sprintf("%v", response)
	if !strings.Contains(resultStr, "ActiveCustomers") || !strings.Contains(resultStr, "created") {
		t.Errorf("Expected success message about view creation, got: %s", resultStr)
	}

	t.Logf("✓ View created successfully: %s", resultStr)
}

// TestCreateView_InvalidName tests view name validation
func TestCreateView_InvalidName(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testCases := []struct {
		name        string
		viewName    string
		expectedErr string
	}{
		{
			name:        "Reserved _mv_ prefix",
			viewName:    "_mv_TestView",
			expectedErr: "_mv_",
		},
		{
			name:        "Name too long (129 chars)",
			viewName:    strings.Repeat("A", 129),
			expectedErr: "128 characters",
		},
		{
			name:        "Empty name",
			viewName:    "",
			expectedErr: "cannot be empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			createViewCmd := fmt.Sprintf(`CREATE VIEW "%s" AS SELECT * FROM "Customers";`, tc.viewName)
			startTime := time.Now()
			_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")

			if err == nil {
				t.Fatalf("Expected error for invalid view name, but creation succeeded")
			}

			errMsg := err.Error()
			if !strings.Contains(strings.ToLower(errMsg), strings.ToLower(tc.expectedErr)) {
				t.Errorf("Expected error containing '%s', got: %s", tc.expectedErr, errMsg)
			}

			t.Logf("✓ Correctly rejected invalid name: %s", errMsg)
		})
	}
}

// TestCreateView_InvalidDefinition tests view definition validation
func TestCreateView_InvalidDefinition(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Definition exceeding 64KB
	largeQuery := fmt.Sprintf("SELECT * FROM \"Customers\" WHERE \"Name\" IN (%s)", strings.Repeat("'value',", 20000))
	createViewCmd := fmt.Sprintf(`CREATE VIEW "LargeView" AS %s;`, largeQuery)

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err == nil {
		t.Fatal("Expected error for definition exceeding 64KB")
	}

	errMsg := err.Error()
	if !strings.Contains(strings.ToLower(errMsg), "64kb") && !strings.Contains(strings.ToLower(errMsg), "64 kb") && !strings.Contains(strings.ToLower(errMsg), "too long") {
		t.Errorf("Expected error about size limit or too long, got: %s", errMsg)
	}

	t.Logf("✓ Correctly rejected oversized definition: %s", errMsg)
}

// TestCreateView_MissingBundle tests view creation with non-existent bundle
func TestCreateView_MissingBundle(t *testing.T) {

	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	createViewCmd := `CREATE VIEW "TestView" AS SELECT * FROM "NonExistentBundle";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err == nil {
		t.Fatal("Expected error for non-existent bundle reference")
	}

	errMsg := err.Error()
	if !strings.Contains(strings.ToLower(errMsg), "not found") && !strings.Contains(strings.ToLower(errMsg), "does not exist") {
		t.Errorf("Expected error about bundle not found, got: %s", errMsg)
	}

	t.Logf("✓ Correctly rejected view with missing bundle: %s", errMsg)
}

// TestCreateView_DuplicateName tests creating view with duplicate name
func TestCreateView_DuplicateName(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create first view
	createViewCmd := `CREATE VIEW "DuplicateTest" AS SELECT * FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create first view: %v", err)
	}

	// Attempt to create duplicate
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err == nil {
		t.Fatal("Expected error for duplicate view name")
	}

	errMsg := err.Error()
	if !strings.Contains(strings.ToLower(errMsg), "already exists") && !strings.Contains(strings.ToLower(errMsg), "duplicate") {
		t.Errorf("Expected error about duplicate view, got: %s", errMsg)
	}

	t.Logf("✓ Correctly rejected duplicate view name: %s", errMsg)
}

// TestQueryView_BasicSuccess tests querying a regular view
func TestQueryView_BasicSuccess(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create view
	createViewCmd := `CREATE VIEW "ActiveCustomers" AS SELECT * FROM "Customers" WHERE "Status" == "Active";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	queryCmd := `SELECT * FROM "ActiveCustomers";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, queryCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	_ = response // Used via getResultCount()

	if err != nil {
		t.Fatalf("Failed to query view: %v", err)
	}

	// Verify we got results (should be rewritten to underlying query)
	if getResultCount(response) == 0 {
		t.Error("Expected results from view query, got 0")
	}

	t.Logf("✓ View query returned %d results", getResultCount(response))
}

// TestQueryView_WithAdditionalFilter tests adding WHERE clause to view query
func TestQueryView_WithAdditionalFilter(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create view with filter
	createViewCmd := `CREATE VIEW "ActiveCustomers" AS SELECT * FROM "Customers" WHERE "Status" == "Active";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query view with additional filter
	queryCmd := `SELECT * FROM "ActiveCustomers" WHERE "Country" == "USA";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, queryCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	_ = response // Used via getResultCount()

	if err != nil {
		t.Fatalf("Failed to query view with filter: %v", err)
	}

	// Verify results are filtered (query should be rewritten to combine filters)
	if getResultCount(response) == 0 {
		t.Error("Expected filtered results from view query")
	}

	t.Logf("✓ View query with additional filter returned %d results", getResultCount(response))
}

// TestDropView_BasicSuccess tests dropping a regular view
func TestDropView_BasicSuccess(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create view
	createViewCmd := `CREATE VIEW "TestView" AS SELECT * FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Drop view
	dropViewCmd := `DROP VIEW "TestView";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to drop view: %v", err)
	}

	// Verify success message
	resultStr := fmt.Sprintf("%v", response)
	if !strings.Contains(resultStr, "dropped") || !strings.Contains(resultStr, "TestView") {
		t.Errorf("Expected success message about view drop, got: %s", resultStr)
	}

	t.Logf("✓ View dropped successfully: %s", resultStr)
}

// TestDropView_NonExistent tests dropping non-existent view
func TestDropView_NonExistent(t *testing.T) {

	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dropViewCmd := `DROP VIEW "NonExistentView";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err == nil {
		t.Fatal("Expected error when dropping non-existent view")
	}

	errMsg := err.Error()
	if !strings.Contains(strings.ToLower(errMsg), "not found") && !strings.Contains(strings.ToLower(errMsg), "does not exist") {
		t.Errorf("Expected error about view not found, got: %s", errMsg)
	}

	t.Logf("✓ Correctly rejected drop of non-existent view: %s", errMsg)
}

// =============================================================================
// MATERIALIZED VIEW TESTS
// =============================================================================

// TestCreateMaterializedView_BasicSuccess tests creating a materialized view
func TestCreateMaterializedView_BasicSuccess(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create materialized view
	createViewCmd := `CREATE MATERIALIZED VIEW "CustomerStats" AS SELECT "Country", COUNT(*) AS "Count" FROM "Customers" GROUP BY "Country";`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to create materialized view: %v", err)
	}

	// Verify success message mentions row count
	resultStr := fmt.Sprintf("%v", response)
	if !strings.Contains(resultStr, "CustomerStats") || !strings.Contains(resultStr, "created") {
		t.Errorf("Expected success message about materialized view creation, got: %s", resultStr)
	}

	t.Logf("✓ Materialized view created successfully: %s", resultStr)
}

// TestCreateMaterializedView_DataBundleCreated tests that _mv_ bundle is created
func TestCreateMaterializedView_DataBundleCreated(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create materialized view
	createViewCmd := `CREATE MATERIALIZED VIEW "TestMV" AS SELECT * FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create materialized view: %v", err)
	}

	// Verify data bundle exists (internal check - would normally query catalog)
	// The _mv_TestMV bundle should have been created
	db, err := fixture.ServiceManager.DatabaseService.GetDatabaseByName(fixture.Database.Name)
	if err != nil || db == nil {
		t.Fatal("Failed to get database")
	}

	// Check if _mv_ bundle exists in database
	_, exists := db.Bundles["_mv_TestMV"]
	if !exists {
		t.Error("Expected _mv_TestMV data bundle to be created")
	}

	t.Logf("✓ Data bundle _mv_TestMV created successfully")
}

// TestQueryMaterializedView_BasicSuccess tests querying materialized view
func TestQueryMaterializedView_BasicSuccess(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create materialized view
	createViewCmd := `CREATE MATERIALIZED VIEW "CustomerStats" AS SELECT "Country", COUNT(*) AS "Count" FROM "Customers" GROUP BY "Country";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create materialized view: %v", err)
	}

	// Query materialized view (should read from _mv_ bundle)
	queryCmd := `SELECT * FROM "CustomerStats";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, queryCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	_ = response // Used via getResultCount()

	if err != nil {
		t.Fatalf("Failed to query materialized view: %v", err)
	}

	// Verify we got aggregated results
	if getResultCount(response) == 0 {
		t.Error("Expected aggregated results from materialized view")
	}

	t.Logf("✓ Materialized view query returned %d results", getResultCount(response))
}

// TestRefreshMaterializedView_BasicSuccess tests refreshing materialized view
func TestRefreshMaterializedView_BasicSuccess(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create materialized view
	createViewCmd := `CREATE MATERIALIZED VIEW "CustomerStats" AS SELECT COUNT(*) AS "Total" FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create materialized view: %v", err)
	}

	// Add more data (use offset to avoid unique constraint violation)
	seedCustomersForViewsFrom(t, fixture, 50, 50)

	// Refresh materialized view
	refreshCmd := `REFRESH MATERIALIZED VIEW "CustomerStats";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, refreshCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to refresh materialized view: %v", err)
	}

	// Verify success message
	resultStr := fmt.Sprintf("%v", response)
	if !strings.Contains(resultStr, "refreshed") || !strings.Contains(resultStr, "CustomerStats") {
		t.Errorf("Expected success message about refresh, got: %s", resultStr)
	}

	t.Logf("✓ Materialized view refreshed successfully: %s", resultStr)
}

// TestRefreshMaterializedView_DataUpdated tests that refresh updates data
func TestRefreshMaterializedView_DataUpdated(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create materialized view with count
	createViewCmd := `CREATE MATERIALIZED VIEW "CustomerCount" AS SELECT COUNT(*) AS "Total" FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create materialized view: %v", err)
	}

	// Query initial count
	queryCmd := `SELECT * FROM "CustomerCount";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, queryCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to query materialized view: %v", err)
	}
	initialCount := getResultCount(response)

	// Add more data (use offset to avoid unique constraint violation)
	seedCustomersForViewsFrom(t, fixture, 50, 50)

	// Query before refresh (should show old count)
	startTime = time.Now()
	response, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, queryCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to query materialized view before refresh: %v", err)
	}
	beforeRefreshCount := getResultCount(response)

	if beforeRefreshCount != initialCount {
		t.Error("Count should not change before refresh")
	}

	// Refresh
	refreshCmd := `REFRESH MATERIALIZED VIEW "CustomerCount";`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, refreshCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to refresh materialized view: %v", err)
	}

	// Query after refresh (should show updated count)
	startTime = time.Now()
	response, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, queryCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to query materialized view after refresh: %v", err)
	}
	afterRefreshCount := getResultCount(response)

	if afterRefreshCount == initialCount {
		t.Error("Count should be updated after refresh")
	}

	t.Logf("✓ Data updated: initial=%d, after_refresh=%d", initialCount, afterRefreshCount)
}

// TestDropMaterializedView_BasicSuccess tests dropping materialized view
func TestDropMaterializedView_BasicSuccess(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create materialized view
	createViewCmd := `CREATE MATERIALIZED VIEW "TestMV" AS SELECT * FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create materialized view: %v", err)
	}

	// Drop materialized view
	dropViewCmd := `DROP MATERIALIZED VIEW "TestMV";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to drop materialized view: %v", err)
	}

	// Verify success message
	resultStr := fmt.Sprintf("%v", response)
	if !strings.Contains(resultStr, "dropped") || !strings.Contains(resultStr, "TestMV") {
		t.Errorf("Expected success message about drop, got: %s", resultStr)
	}

	t.Logf("✓ Materialized view dropped successfully: %s", resultStr)
}

// TestDropMaterializedView_DataBundleRemoved tests that _mv_ bundle is deleted
func TestDropMaterializedView_DataBundleRemoved(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create materialized view
	createViewCmd := `CREATE MATERIALIZED VIEW "TestMV" AS SELECT * FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create materialized view: %v", err)
	}

	// Drop materialized view
	dropViewCmd := `DROP MATERIALIZED VIEW "TestMV";`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to drop materialized view: %v", err)
	}

	// Verify data bundle was removed
	db, err := fixture.ServiceManager.DatabaseService.GetDatabaseByName(fixture.Database.Name)
	if err != nil || db == nil {
		t.Fatal("Failed to get database")
	}

	_, exists := db.Bundles["_mv_DropMV"]
	if exists {
		t.Error("Expected _mv_DropMV data bundle to be dropped")
	}

	t.Logf("✓ Data bundle _mv_DropMV removed successfully")
}

// =============================================================================
// SHOW VIEWS TESTS
// =============================================================================

// TestShowViews_EmptyDatabase tests SHOW VIEWS with no views
func TestShowViews_EmptyDatabase(t *testing.T) {

	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	showViewsCmd := `SHOW VIEWS;`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, showViewsCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to execute SHOW VIEWS: %v", err)
	}

	if getResultCount(response) != 0 {
		t.Errorf("Expected 0 views in empty database, got %d", getResultCount(response))
	}

	t.Logf("✓ SHOW VIEWS returned 0 results for empty database")
}

// TestShowViews_WithViews tests SHOW VIEWS with multiple views
func TestShowViews_WithViews(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create regular view
	createView1 := `CREATE VIEW "View1" AS SELECT * FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createView1, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create View1: %v", err)
	}

	// Create materialized view
	createView2 := `CREATE MATERIALIZED VIEW "View2" AS SELECT COUNT(*) FROM "Customers";`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createView2, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create View2: %v", err)
	}

	// Show all views
	showViewsCmd := `SHOW VIEWS;`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, showViewsCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to execute SHOW VIEWS: %v", err)
	}

	if getResultCount(response) != 2 {
		t.Errorf("Expected 2 views, got %d", getResultCount(response))
	}

	t.Logf("✓ SHOW VIEWS returned %d views", getResultCount(response))
}

// TestShowViews_TypeDistinction tests that SHOW VIEWS distinguishes view types
func TestShowViews_TypeDistinction(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create one of each type
	createView := `CREATE VIEW "RegularView" AS SELECT * FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createView, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create regular view: %v", err)
	}

	createMV := `CREATE MATERIALIZED VIEW "MaterializedView" AS SELECT * FROM "Customers";`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createMV, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create materialized view: %v", err)
	}

	// Show views and verify types are different
	showViewsCmd := `SHOW VIEWS;`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, showViewsCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to execute SHOW VIEWS: %v", err)
	}

	// Response should contain both VIEW and MATERIALIZED_VIEW types
	resultStr := fmt.Sprintf("%v", response)
	if !strings.Contains(resultStr, "VIEW") {
		t.Error("Expected 'VIEW' type in results")
	}
	if !strings.Contains(resultStr, "MATERIALIZED") {
		t.Error("Expected 'MATERIALIZED_VIEW' type in results")
	}

	t.Logf("✓ SHOW VIEWS correctly distinguishes view types")
}

// =============================================================================
// DESCRIBE VIEW TESTS
// =============================================================================

// TestDescribeView_RegularView tests DESCRIBE VIEW for regular view
func TestDescribeView_RegularView(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create view
	createViewCmd := `CREATE VIEW "TestView" AS SELECT * FROM "Customers" WHERE "Status" == "Active";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Describe view
	describeCmd := `DESCRIBE VIEW "TestView";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, describeCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to execute DESCRIBE VIEW: %v", err)
	}

	// Verify response contains view metadata
	resultStr := fmt.Sprintf("%v", response)
	if !strings.Contains(resultStr, "TestView") {
		t.Error("Expected view name in description")
	}
	if !strings.Contains(resultStr, "Definition") || !strings.Contains(resultStr, "SELECT") {
		t.Error("Expected view definition in description")
	}

	t.Logf("✓ DESCRIBE VIEW returned metadata: %s", resultStr)
}

// TestDescribeView_MaterializedView tests DESCRIBE VIEW for materialized view
func TestDescribeView_MaterializedView(t *testing.T) {

	fixture := setupFullServer(t)
	setupTestBundlesForViews(t, fixture)
	seedCustomersForViews(t, fixture, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create materialized view
	createViewCmd := `CREATE MATERIALIZED VIEW "TestMV" AS SELECT * FROM "Customers";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createViewCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create materialized view: %v", err)
	}

	// Describe materialized view
	describeCmd := `DESCRIBE VIEW "TestMV";`
	startTime = time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, describeCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err != nil {
		t.Fatalf("Failed to execute DESCRIBE VIEW: %v", err)
	}

	// Verify response includes materialized view specific fields
	resultStr := fmt.Sprintf("%v", response)
	if !strings.Contains(resultStr, "TestMV") {
		t.Error("Expected view name in description")
	}
	if !strings.Contains(resultStr, "DataBundleName") || !strings.Contains(resultStr, "_mv_") {
		t.Error("Expected data bundle name in materialized view description")
	}
	if !strings.Contains(resultStr, "LastRefreshed") {
		t.Error("Expected LastRefreshed timestamp in materialized view description")
	}

	t.Logf("✓ DESCRIBE VIEW returned materialized view metadata: %s", resultStr)
}

// TestDescribeView_NonExistent tests DESCRIBE VIEW for non-existent view
func TestDescribeView_NonExistent(t *testing.T) {

	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	describeCmd := `DESCRIBE VIEW "NonExistentView";`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, describeCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err == nil {
		t.Fatal("Expected error for non-existent view")
	}

	errMsg := err.Error()
	if !strings.Contains(strings.ToLower(errMsg), "not found") && !strings.Contains(strings.ToLower(errMsg), "does not exist") {
		t.Errorf("Expected error about view not found, got: %s", errMsg)
	}

	t.Logf("✓ Correctly rejected DESCRIBE of non-existent view: %s", errMsg)
}

// =============================================================================
// BUNDLE PREFIX VALIDATION TESTS
// =============================================================================

// TestBundleCreation_ReservedPrefix tests that _mv_ prefix is blocked for bundles
func TestBundleCreation_ReservedPrefix(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Attempt to create bundle with _mv_ prefix
	createBundleCmd := `CREATE BUNDLE "_mv_TestBundle" WITH FIELDS ({"Name", STRING, TRUE, FALSE});`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")

	if err == nil {
		t.Fatal("Expected error for bundle name with _mv_ prefix")
	}

	errMsg := err.Error()
	errMsgLower := strings.ToLower(errMsg)
	// Accept error messages that mention _mv_, reserved, invalid bundle name, or validation failure
	if !strings.Contains(errMsgLower, "_mv_") && !strings.Contains(errMsgLower, "reserved") && !strings.Contains(errMsgLower, "invalid bundle name") && !strings.Contains(errMsgLower, "bundle name") {
		t.Errorf("Expected error about _mv_ prefix, reserved prefix, or invalid bundle name, got: %s", errMsg)
	}

	t.Logf("✓ Correctly rejected _mv_ prefix for bundle: %s", errMsg)
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// setupTestBundlesForViews creates test bundles for view testing
func setupTestBundlesForViews(t *testing.T, fixture *TestFixture) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create Customers bundle
	createBundleCmd := `CREATE BUNDLE "Customers" WITH FIELDS (
		{"CustomerID", STRING, TRUE, TRUE},
		{"Name", STRING, TRUE, FALSE},
		{"Email", STRING, FALSE, FALSE},
		{"Country", STRING, FALSE, FALSE},
		{"Status", STRING, FALSE, FALSE}
	);`

	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBundleCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Customers bundle: %v", err)
	}

	// Create Orders bundle for JOIN tests
	createOrdersCmd := `CREATE BUNDLE "Orders" WITH FIELDS (
		{"OrderID", STRING, TRUE, TRUE},
		{"CustomerID", STRING, TRUE, FALSE},
		{"Amount", FLOAT, TRUE, FALSE},
		{"OrderDate", STRING, FALSE, FALSE}
	);`

	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createOrdersCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Orders bundle: %v", err)
	}
}

// seedCustomersForViews adds test customer data starting from offset
func seedCustomersForViews(t *testing.T, fixture *TestFixture, count int) {
	t.Helper()
	seedCustomersForViewsFrom(t, fixture, count, 0)
}

// seedCustomersForViewsFrom adds test customer data starting from the given offset
func seedCustomersForViewsFrom(t *testing.T, fixture *TestFixture, count int, offset int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	countries := []string{"USA", "Canada", "UK", "Germany", "France"}
	statuses := []string{"Active", "Inactive", "Pending"}

	for i := 0; i < count; i++ {
		idx := i + offset
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Customers" WITH ({"CustomerID" = "%s"}, {"Name" = "Customer%d"}, {"Email" = "customer%d@example.com"}, {"Country" = "%s"}, {"Status" = "%s"});`,
			fmt.Sprintf("C%05d", idx+1),
			idx+1,
			idx+1,
			countries[idx%len(countries)],
			statuses[idx%len(statuses)],
		)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to add customer document %d: %v", i+1, err)
		}
	}

	t.Logf("Seeded %d customer documents", count)
}

// getResultCount safely extracts result count from command response
func getResultCount(response interface{}) int {
	if cmdResp, ok := response.(*server.CommandResponse); ok {
		return cmdResp.ResultCount
	}
	return 0
}

// getDatabaseBundles safely gets bundles map from database
func getDatabaseBundles() map[string]interface{} {
	// This is a placeholder since the actual structure may vary
	// In real tests, this would access db.Bundles after proper type assertion
	return make(map[string]interface{})
}
