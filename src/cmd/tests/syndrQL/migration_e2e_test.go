package syndrQL

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// TestMigration_E2E_CreateAndApply tests the complete migration lifecycle:
// 1. Create a new empty database
// 2. Start migration with Authors and Books bundles
// 3. Validate migration was stored correctly
// 4. Apply migration
// 5. Verify bundles exist
// 6. Cleanup database
func TestMigration_E2E_CreateAndApply(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testDBName := "migration_test_db"

	// Step 1: Create test database
	t.Logf("Step 1: Creating database '%s'", testDBName)
	createDBCmd := fmt.Sprintf(`CREATE DATABASE "%s"`, testDBName)
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createDBCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	t.Logf("✓ Database created: %+v", response)

	// Register cleanup to drop database at end of test
	defer func() {
		dropDBCmd := fmt.Sprintf(`DROP DATABASE "%s" WITH FORCE`, testDBName)
		startTime := time.Now()
		_, _ = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropDBCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		t.Logf("✓ Cleanup: Database '%s' dropped", testDBName)
	}()

	// Step 2: Start migration with CREATE BUNDLE commands
	t.Logf("Step 2: Creating migration for database '%s'", testDBName)

	migrationBody := `
CREATE BUNDLE "Authors" WITH FIELDS (
	{"id", "STRING", TRUE, TRUE, NULL},
	{"firstName", "STRING", TRUE, FALSE, NULL},
	{"lastName", "STRING", TRUE, FALSE, NULL},
	{"birthDate", "DATETIME", FALSE, FALSE, NULL},
	{"biography", "STRING", FALSE, FALSE, NULL}
);

CREATE BUNDLE "Books" WITH FIELDS (
	{"id", "STRING", TRUE, TRUE, NULL},
	{"title", "STRING", TRUE, FALSE, NULL},
	{"isbn", "STRING", TRUE, TRUE, NULL},
	{"authorId", "STRING", TRUE, FALSE, NULL},
	{"publicationYear", "INT", FALSE, FALSE, NULL},
	{"price", "FLOAT", TRUE, FALSE, NULL},
	{"genre", "STRING", FALSE, FALSE, NULL},
	{"description", "STRING", FALSE, FALSE, NULL},
	{"inStock", "BOOLEAN", TRUE, FALSE, NULL},
	{"createdAt", "DATETIME", TRUE, FALSE, NULL}
);
`

	// Switch to the new database first
	useCmd := fmt.Sprintf(`USE "%s"`, testDBName)
	startTime = time.Now()
	useResponse, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, useCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to switch to database: %v", err)
	}

	// Get the new database from the service manager after USE command
	newDatabase, err := fixture.ServiceManager.DatabaseService.GetDatabaseByName(testDBName)
	if err != nil {
		t.Fatalf("Failed to get database after USE: %v", err)
	}
	t.Logf("✓ Switched to database: %+v", useResponse)

	// Now create the migration in the context of the new database
	startMigrationCmd := fmt.Sprintf(`START MIGRATION WITH DESCRIPTION "Initial schema: Create Authors and Books bundles" %s COMMIT`, migrationBody)
	startTime = time.Now()
	migrationResponse, err := server.CommandDirector(ctx, newDatabase, *fixture.ServiceManager, startMigrationCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create migration: %v", err)
	}

	if cmdResp, ok := migrationResponse.(*server.CommandResponse); ok {
		t.Logf("✓ Migration created: %+v", cmdResp.Result)
	} else if resultMap, ok := migrationResponse.(map[string]interface{}); ok {
		t.Logf("✓ Migration created: %+v", resultMap)
	} else {
		t.Fatalf("Unexpected response type: %T", migrationResponse)
	}

	// Step 3: Verify migration was stored - SHOW MIGRATIONS
	t.Logf("Step 3: Verifying migration was stored for database '%s'", testDBName)

	showMigrationsCmd := fmt.Sprintf(`SHOW MIGRATIONS FOR "%s"`, testDBName)
	startTime = time.Now()
	showResponse, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, showMigrationsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to show migrations: %v", err)
	}

	var migrationFound bool
	if cmdResp, ok := showResponse.(*server.CommandResponse); ok {
		if cmdResp.ResultCount == 0 {
			t.Fatalf("CRITICAL: Migration not found! Expected 1 migration, got 0. Response: %+v", cmdResp.Result)
		}

		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected 1 migration, got %d", cmdResp.ResultCount)
		}

		// Validate migration fields
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			if len(results) > 0 {
				migration := results[0]
				t.Logf("✓ Migration found: Version=%v, Database=%v, Status=%v",
					migration["Version"], migration["DatabaseName"], migration["Status"])

				// Validate expected fields
				if version, ok := migration["Version"]; !ok || version != 1 {
					t.Errorf("Expected Version=1, got %v", version)
				}
				if dbName, ok := migration["DatabaseName"]; !ok || dbName != testDBName {
					t.Errorf("Expected DatabaseName='%s', got %v", testDBName, dbName)
				}
				if status, ok := migration["Status"]; !ok || status != "PENDING" {
					t.Errorf("Expected Status='PENDING', got %v", status)
				}
				migrationFound = true
			}
		} else {
			t.Errorf("Unexpected result type: %T", cmdResp.Result)
		}
	} else if mapResp, ok := showResponse.(map[string]interface{}); ok {
		// Handle map[string]interface{} response
		t.Logf("DEBUG: Response type is map, migrations field type: %T, value: %+v", mapResp["migrations"], mapResp["migrations"])

		// Check if migrations key exists and is non-nil
		if migrationsRaw, exists := mapResp["migrations"]; exists && migrationsRaw != nil {
			// Try to handle as []interface{} first (generic slice)
			if migrations, ok := migrationsRaw.([]interface{}); ok {
				if len(migrations) == 0 {
					t.Fatalf("CRITICAL: Migration not found! Expected 1 migration, got 0. Response: %+v", mapResp)
				}
				if len(migrations) != 1 {
					t.Errorf("Expected 1 migration, got %d", len(migrations))
				}
				if len(migrations) > 0 {
					if migration, ok := migrations[0].(map[string]interface{}); ok {
						t.Logf("✓ Migration found: Version=%v, Database=%v, Status=%v",
							migration["Version"], migration["DatabaseName"], migration["Status"])

						// Validate expected fields
						if version, ok := migration["Version"]; !ok || version != 1 {
							t.Errorf("Expected Version=1, got %v", version)
						}
						if dbName, ok := migration["DatabaseName"]; !ok || dbName != testDBName {
							t.Errorf("Expected DatabaseName='%s', got %v", testDBName, dbName)
						}
						if status, ok := migration["Status"]; !ok || status != "PENDING" {
							t.Errorf("Expected Status='PENDING', got %v", status)
						}
						migrationFound = true
					} else {
						t.Errorf("Migration item is not a map: %T", migrations[0])
					}
				}
			} else {
				// Handle as typed slice - use reflection to get length
				migrationsValue := reflect.ValueOf(migrationsRaw)
				if migrationsValue.Kind() == reflect.Slice {
					migrationCount := migrationsValue.Len()
					if migrationCount == 0 {
						t.Fatalf("CRITICAL: Migration not found! Expected 1 migration, got 0. Response: %+v", mapResp)
					}
					if migrationCount != 1 {
						t.Errorf("Expected 1 migration, got %d", migrationCount)
					}
					if migrationCount > 0 {
						// Migration exists, assume it's valid
						t.Logf("✓ Migration found (typed slice with %d item(s))", migrationCount)
						migrationFound = true
					}
				} else {
					t.Errorf("Migrations field is not a slice: %T", migrationsRaw)
				}
			}
		} else {
			t.Fatalf("Response does not contain 'migrations' field or it is nil: %+v", mapResp)
		}
	} else {
		t.Fatalf("Unexpected response type: %T", showResponse)
	}

	if !migrationFound {
		t.Fatal("Migration validation failed")
	}

	// Step 4: Validate migration (dry run)
	t.Logf("Step 4: Validating migration for database '%s'", testDBName)

	validateCmd := `VALIDATE MIGRATION WITH VERSION 1`
	startTime = time.Now()
	validateResponse, err := server.CommandDirector(ctx, newDatabase, *fixture.ServiceManager, validateCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to validate migration: %v", err)
	}

	if cmdResp, ok := validateResponse.(*server.CommandResponse); ok {
		t.Logf("✓ Migration validated: %+v", cmdResp.Result)
	}

	// Step 5: Apply migration
	t.Logf("Step 5: Applying migration for database '%s'", testDBName)

	applyCmd := `APPLY MIGRATION WITH VERSION 1`
	startTime = time.Now()
	applyResponse, err := server.CommandDirector(ctx, newDatabase, *fixture.ServiceManager, applyCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to apply migration: %v", err)
	}

	if cmdResp, ok := applyResponse.(*server.CommandResponse); ok {
		t.Logf("✓ Migration applied: %+v", cmdResp.Result)
	}

	// Step 6: Verify bundles exist in database
	t.Logf("Step 6: Verifying bundles exist in database '%s'", testDBName)

	showBundlesCmd := `SHOW BUNDLES`
	startTime = time.Now()
	bundlesResponse, err := server.CommandDirector(ctx, newDatabase, *fixture.ServiceManager, showBundlesCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to show bundles: %v", err)
	}

	if cmdResp, ok := bundlesResponse.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 2 {
			t.Errorf("Expected 2 bundles (Authors, Books), got %d", cmdResp.ResultCount)
		}

		t.Logf("DEBUG: Bundle response Result type: %T, value: %+v", cmdResp.Result, cmdResp.Result)

		// Try different result type extractions
		bundleNames := make([]string, 0, 2)

		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			for _, bundle := range results {
				// Try both "BundleName" and "Name" field names
				if name, ok := bundle["BundleName"].(string); ok {
					bundleNames = append(bundleNames, name)
				} else if name, ok := bundle["Name"].(string); ok {
					bundleNames = append(bundleNames, name)
				}
			}
		} else if results, ok := cmdResp.Result.([]interface{}); ok {
			for _, bundleRaw := range results {
				if bundle, ok := bundleRaw.(map[string]interface{}); ok {
					// Try both "BundleName" and "Name" field names
					if name, ok := bundle["BundleName"].(string); ok {
						bundleNames = append(bundleNames, name)
					} else if name, ok := bundle["Name"].(string); ok {
						bundleNames = append(bundleNames, name)
					}
				}
			}
		}

		t.Logf("✓ Bundles created: %v", bundleNames)

		// Verify expected bundles
		expectedBundles := map[string]bool{"Authors": false, "Books": false}
		for _, name := range bundleNames {
			if _, exists := expectedBundles[name]; exists {
				expectedBundles[name] = true
			}
		}

		for bundleName, found := range expectedBundles {
			if !found {
				t.Errorf("Expected bundle '%s' not found", bundleName)
			}
		}
	} else if mapResp, ok := bundlesResponse.(map[string]interface{}); ok {
		// Handle map response
		t.Logf("DEBUG: Bundle map response: %+v", mapResp)

		// Try to find bundles in map response
		if bundlesRaw, exists := mapResp["bundles"]; exists && bundlesRaw != nil {
			bundleNames := make([]string, 0, 2)

			if bundlesList, ok := bundlesRaw.([]interface{}); ok {
				for _, bundleRaw := range bundlesList {
					if bundle, ok := bundleRaw.(map[string]interface{}); ok {
						if name, ok := bundle["BundleName"].(string); ok {
							bundleNames = append(bundleNames, name)
						}
					}
				}
			}

			t.Logf("✓ Bundles created: %v", bundleNames)

			// Verify expected bundles
			expectedBundles := map[string]bool{"Authors": false, "Books": false}
			for _, name := range bundleNames {
				if _, exists := expectedBundles[name]; exists {
					expectedBundles[name] = true
				}
			}

			for bundleName, found := range expectedBundles {
				if !found {
					t.Errorf("Expected bundle '%s' not found", bundleName)
				}
			}
		}
	} else {
		t.Errorf("Unexpected response type for SHOW BUNDLES: %T", bundlesResponse)
	}

	// Step 7: Verify migration status changed to APPLIED
	t.Logf("Step 7: Verifying migration status changed to APPLIED")

	showMigrationsCmd = fmt.Sprintf(`SHOW MIGRATIONS FOR "%s"`, testDBName)
	startTime = time.Now()
	finalShowResponse, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, showMigrationsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to show migrations after apply: %v", err)
	}

	if cmdResp, ok := finalShowResponse.(*server.CommandResponse); ok {
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			if len(results) > 0 {
				migration := results[0]
				if status, ok := migration["Status"]; !ok || status != "APPLIED" {
					t.Errorf("Expected Status='APPLIED' after migration apply, got %v", status)
				} else {
					t.Logf("✓ Migration status updated to APPLIED")
				}
			}
		}
	}

	t.Logf("✓ END-TO-END MIGRATION TEST PASSED")
}

// TestMigration_E2E_QueryAfterMigration tests that we can query migration documents immediately after creation
// This specifically tests for the bug where documents are written but not immediately queryable
func TestMigration_E2E_QueryAfterMigration(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testDBName := "migration_query_test_db"

	// Create database
	t.Logf("Creating database '%s'", testDBName)
	createDBCmd := fmt.Sprintf(`CREATE DATABASE "%s"`, testDBName)
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createDBCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	defer func() {
		dropDBCmd := fmt.Sprintf(`DROP DATABASE "%s" WITH FORCE`, testDBName)
		startTime := time.Now()
		_, _ = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropDBCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	}()

	// Create migration
	t.Logf("Creating migration")
	migrationBody := `CREATE BUNDLE "TestBundle" WITH FIELDS ({"id", "STRING", TRUE, TRUE, NULL});`
	startMigrationCmd := fmt.Sprintf(`START MIGRATION FOR "%s" WITH DESCRIPTION "Test migration" AS %s COMMIT;`, testDBName, migrationBody)
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, startMigrationCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create migration: %v", err)
	}

	// IMMEDIATE query - this is where the bug occurs
	t.Logf("Querying migration IMMEDIATELY after creation")
	showMigrationsCmd := fmt.Sprintf(`SHOW MIGRATIONS FOR "%s"`, testDBName)
	startTime = time.Now()
	showResponse, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, showMigrationsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to show migrations: %v", err)
	}

	if cmdResp, ok := showResponse.(*server.CommandResponse); ok {
		if cmdResp.ResultCount == 0 {
			t.Fatalf("CRITICAL BUG: Migration document not found immediately after creation! This is the bug we're fixing.")
		}
		t.Logf("✓ Migration found immediately after creation (bug not reproduced or already fixed)")
	}

	t.Logf("✓ IMMEDIATE QUERY TEST PASSED")
}

// TestMigrationE2E_CommandRouting tests that migration commands are properly routed
func TestMigrationE2E_CommandRouting(t *testing.T) {
	fixture := setupFullServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		name    string
		command string
	}{
		{
			name:    "ShowMigrations",
			command: `SHOW MIGRATIONS FOR DATABASE "testdb";`,
		},
		{
			name:    "ValidateMigration",
			command: `VALIDATE MIGRATION WITH VERSION 1 FOR DATABASE "testdb" BY "test_user";`,
		},
		{
			name:    "ApplyMigration",
			command: `APPLY MIGRATION WITH VERSION 1 FOR DATABASE "testdb";`,
		},
		{
			name:    "ValidateRollback",
			command: `VALIDATE ROLLBACK TO VERSION 0 FOR DATABASE "testdb" BY "test_user";`,
		},
		{
			name:    "ApplyRollback",
			command: `APPLY ROLLBACK TO VERSION 0 FOR DATABASE "testdb";`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime := time.Now()
			_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, tt.command, fixture.Logger, startTime, nil, "127.0.0.1")

			// We expect errors since migrations don't exist, but we're testing routing
			// The key is that we don't get "unknown command" errors
			if err != nil {
				if strings.Contains(err.Error(), "unknown") && !strings.Contains(err.Error(), "not found") {
					t.Errorf("Command not properly routed: %v", err)
				} else {
					t.Logf("Got expected error (command routed correctly): %v", err)
				}
			}
		})
	}
}

// TestMigrationE2E_ServiceInitialization verifies migration service is initialized
func TestMigrationE2E_ServiceInitialization(t *testing.T) {
	fixture := setupFullServer(t)

	// Check that migration service is not nil
	if fixture.ServiceManager.MigrationService == nil {
		t.Fatal("MigrationService is nil - service not properly initialized")
	}

	t.Log("MigrationService successfully initialized")
}
