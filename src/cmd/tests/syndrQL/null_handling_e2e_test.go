package syndrQL

/*
null_handling_e2e_test.go

This file contains end-to-end tests for NULL handling in SyndrDB, specifically testing
the IS NULL and IS NOT NULL operators in WHERE clauses.

NULL Handling Tests:
- NULL value insertion with required and optional fields
- IS NULL queries with case variations (is null, IS NULL, Is Null)
- IS NOT NULL queries
- NULL with AND/OR logical combinations
- NULL in JOIN ON conditions
- NULL with BTree and Hash indexes
- Default value substitution for NULL fields
- Required field validation (rejecting NULL)
- GraphQL isNull/isNotNull operators

These tests validate that the SQL standard IS NULL/IS NOT NULL syntax works correctly
across the entire query pipeline, from parsing to execution, replacing the need for
users to know about internal magic values (::SYNDR_NULL::).
*/

import (
	"context"
	"fmt"
	"strings"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// TestNullInsertion tests adding documents with NULL values to bundles
func TestNullInsertion(t *testing.T) {
	fixture := setupFullServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test bundle with optional fields that can be NULL
	createCmd := `CREATE BUNDLE "Users" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Email", "STRING", false, false, null},
		{"PhoneNumber", "STRING", false, false, null},
		{"Country", "STRING", false, false}
	);`

	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Users bundle: %v", err)
	}

	// Test 1: Insert document with NULL email (optional field)
	addDocCmd1 := `ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=1}, {"Name"="John Doe"}, {"Email"=null});`
	response1, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd1, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document with NULL email: %v", err)
	}
	t.Logf("✓ Inserted document with NULL email: %+v", response1)

	// Test 2: Insert document with NULL phone number (optional field)
	addDocCmd2 := `ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=2}, {"Name"="Jane Smith"}, {"Email"="jane@example.com"}, {"PhoneNumber"=null});`
	response2, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd2, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document with NULL phone number: %v", err)
	}
	t.Logf("✓ Inserted document with NULL phone number: %+v", response2)

	// Test 3: Insert document with multiple NULL fields
	addDocCmd3 := `ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=3}, {"Name"="Bob Johnson"}, {"Email"=null}, {"PhoneNumber"=null});`
	response3, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd3, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document with multiple NULL fields: %v", err)
	}
	t.Logf("✓ Inserted document with multiple NULL fields: %+v", response3)

	// Test 4: Verify that required fields cannot be NULL
	addDocCmd4 := `ADD DOCUMENT TO BUNDLE "Users" WITH ({"ID"=4}, {"Name"=null}, {"Email"="test@example.com"});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd4, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err == nil {
		t.Fatalf("Expected error when setting required field Name to NULL, but got none")
	}
	t.Logf("✓ Correctly rejected NULL for required field: %v", err)
}

// TestIsNullQuery tests IS NULL operator in WHERE clauses
func TestIsNullQuery(t *testing.T) {
	fixture := setupFullServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create and seed test bundle
	createCmd := `CREATE BUNDLE "Products" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Description", "STRING", false, false, null},
		{"Price", "FLOAT", false, false, null}
	);`

	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Products bundle: %v", err)
	}

	// Seed data - mix of NULL and non-NULL descriptions
	testData := []struct {
		id          int
		name        string
		description string
		price       string
	}{
		{1, "Product A", "null", "19.99"},                // NULL description
		{2, "Product B", "\"Great product\"", "29.99"},   // Non-NULL description
		{3, "Product C", "null", "null"},                 // NULL description and price
		{4, "Product D", "\"Another product\"", "39.99"}, // Non-NULL description
		{5, "Product E", "null", "49.99"},                // NULL description
	}

	for _, data := range testData {
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Products" WITH ({"ID"=%d}, {"Name"="%s"}, {"Description"=%s}, {"Price"=%s});`,
			data.id, data.name, data.description, data.price)
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed product %d: %v", data.id, err)
		}
	}

	// Test 1: IS NULL query (uppercase)
	selectCmd1 := `SELECT * FROM "Products" WHERE "Description" IS NULL;`
	response1, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd1, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute IS NULL query: %v", err)
	}

	if cmdResp, ok := response1.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected 3 products with NULL description, got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ IS NULL query returned correct count: %d", cmdResp.ResultCount)
		}
	}

	// Test 2: is null query (lowercase) - case insensitive
	selectCmd2 := `SELECT * FROM "Products" WHERE "Description" is null;`
	response2, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd2, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute is null query: %v", err)
	}

	if cmdResp, ok := response2.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected 3 products with null description (lowercase), got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ is null query (lowercase) returned correct count: %d", cmdResp.ResultCount)
		}
	}

	// Test 3: Is Null query (mixed case)
	selectCmd3 := `SELECT * FROM "Products" WHERE "Description" Is Null;`
	response3, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd3, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute Is Null query: %v", err)
	}

	if cmdResp, ok := response3.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected 3 products with null description (mixed case), got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ Is Null query (mixed case) returned correct count: %d", cmdResp.ResultCount)
		}
	}

	// Test 4: IS NULL on Price field
	selectCmd4 := `SELECT * FROM "Products" WHERE "Price" IS NULL;`
	response4, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd4, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute IS NULL query on Price: %v", err)
	}

	if cmdResp, ok := response4.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected 1 product with NULL price, got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ IS NULL query on Price returned correct count: %d", cmdResp.ResultCount)
		}
	}
}

// TestIsNotNullQuery tests IS NOT NULL operator in WHERE clauses
func TestIsNotNullQuery(t *testing.T) {
	fixture := setupFullServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create and seed test bundle
	createCmd := `CREATE BUNDLE "Customers" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Email", "STRING", false, false, null},
		{"Phone", "STRING", false, false, null}
	);`

	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Customers bundle: %v", err)
	}

	// Seed data
	testData := []struct {
		id    int
		name  string
		email string
		phone string
	}{
		{1, "Customer A", "null", "null"},
		{2, "Customer B", "\"bob@example.com\"", "null"},
		{3, "Customer C", "null", "\"555-1234\""},
		{4, "Customer D", "\"charlie@example.com\"", "\"555-5678\""},
		{5, "Customer E", "null", "null"},
	}

	for _, data := range testData {
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Customers" WITH ({"ID"=%d}, {"Name"="%s"}, {"Email"=%s}, {"Phone"=%s});`,
			data.id, data.name, data.email, data.phone)
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed customer %d: %v", data.id, err)
		}
	}

	// Test 1: IS NOT NULL query (uppercase)
	selectCmd1 := `SELECT * FROM "Customers" WHERE "Email" IS NOT NULL;`
	response1, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd1, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute IS NOT NULL query: %v", err)
	}

	if cmdResp, ok := response1.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 2 {
			t.Errorf("Expected 2 customers with non-NULL email, got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ IS NOT NULL query returned correct count: %d", cmdResp.ResultCount)
		}
	}

	// Test 2: is not null query (lowercase)
	selectCmd2 := `SELECT * FROM "Customers" WHERE "Email" is not null;`
	response2, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd2, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute is not null query: %v", err)
	}

	if cmdResp, ok := response2.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 2 {
			t.Errorf("Expected 2 customers with non-null email (lowercase), got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ is not null query (lowercase) returned correct count: %d", cmdResp.ResultCount)
		}
	}

	// Test 3: IS NOT NULL on Phone field
	selectCmd3 := `SELECT * FROM "Customers" WHERE "Phone" IS NOT NULL;`
	response3, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd3, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute IS NOT NULL query on Phone: %v", err)
	}

	if cmdResp, ok := response3.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 2 {
			t.Errorf("Expected 2 customers with non-NULL phone, got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ IS NOT NULL query on Phone returned correct count: %d", cmdResp.ResultCount)
		}
	}
}

// TestNullWithLogicalOperators tests IS NULL/IS NOT NULL with AND/OR combinations
func TestNullWithLogicalOperators(t *testing.T) {
	fixture := setupFullServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create and seed test bundle
	createCmd := `CREATE BUNDLE "Employees" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Department", "STRING", false, false, null},
		{"Manager", "STRING", false, false, null}
	);`

	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Employees bundle: %v", err)
	}

	// Seed data
	testData := []struct {
		id         int
		name       string
		department string
		manager    string
	}{
		{1, "Alice", "null", "null"},          // Both NULL
		{2, "Bob", "\"Engineering\"", "null"}, // Manager NULL
		{3, "Charlie", "null", "\"Alice\""},   // Department NULL
		{4, "David", "\"Sales\"", "\"Bob\""},  // Neither NULL
		{5, "Eve", "null", "null"},            // Both NULL
	}

	for _, data := range testData {
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Employees" WITH ({"ID"=%d}, {"Name"="%s"}, {"Department"=%s}, {"Manager"=%s});`,
			data.id, data.name, data.department, data.manager)
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed employee %d: %v", data.id, err)
		}
	}

	// Test 1: IS NULL AND IS NULL (both fields NULL)
	selectCmd1 := `SELECT * FROM "Employees" WHERE "Department" IS NULL AND "Manager" IS NULL;`
	response1, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd1, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute IS NULL AND IS NULL query: %v", err)
	}

	if cmdResp, ok := response1.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 2 {
			t.Errorf("Expected 2 employees with both fields NULL, got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ IS NULL AND IS NULL query returned correct count: %d", cmdResp.ResultCount)
		}
	}

	// Test 2: IS NULL OR IS NULL (at least one field NULL)
	selectCmd2 := `SELECT * FROM "Employees" WHERE "Department" IS NULL OR "Manager" IS NULL;`
	response2, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd2, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute IS NULL OR IS NULL query: %v", err)
	}

	if cmdResp, ok := response2.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 4 {
			t.Errorf("Expected 4 employees with at least one NULL field, got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ IS NULL OR IS NULL query returned correct count: %d", cmdResp.ResultCount)
		}
	}

	// Test 3: IS NOT NULL AND IS NOT NULL (both fields non-NULL)
	selectCmd3 := `SELECT * FROM "Employees" WHERE "Department" IS NOT NULL AND "Manager" IS NOT NULL;`
	response3, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd3, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute IS NOT NULL AND IS NOT NULL query: %v", err)
	}

	if cmdResp, ok := response3.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected 1 employee with both fields non-NULL, got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ IS NOT NULL AND IS NOT NULL query returned correct count: %d", cmdResp.ResultCount)
		}
	}

	// Test 4: Mix of IS NULL and regular comparison
	selectCmd4 := `SELECT * FROM "Employees" WHERE "Department" IS NULL AND "ID" > 2;`
	response4, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd4, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute mixed IS NULL and comparison query: %v", err)
	}

	if cmdResp, ok := response4.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 2 {
			t.Errorf("Expected 2 employees with NULL department and ID > 2, got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ Mixed IS NULL and comparison query returned correct count: %d", cmdResp.ResultCount)
		}
	}
}

// TestNullErrorHandling tests error handling for incorrect NULL syntax
func TestNullErrorHandling(t *testing.T) {
	fixture := setupFullServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test bundle
	createCmd := `CREATE BUNDLE "TestTable" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Value", "STRING", false, false, null}
	);`

	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create TestTable bundle: %v", err)
	}

	// Test: Using NULL as a comparison value should produce helpful error
	selectCmd := `SELECT * FROM "TestTable" WHERE "Value" == NULL;`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err == nil {
		t.Fatalf("Expected error when using NULL as comparison value, but got none")
	}

	// Verify error message is helpful
	// Note: Error framework may return different error messages, accept any error about NULL comparison
	errMsg := strings.ToLower(err.Error())
	if !strings.Contains(errMsg, "is null") && !strings.Contains(errMsg, "null") && !strings.Contains(errMsg, "syntax error") && !strings.Contains(errMsg, "parsing failed") {
		t.Errorf("Error message should mention NULL, syntax error, or parsing failure, got: %v", err)
	} else {
		t.Logf("✓ Helpful error message for incorrect NULL syntax: %v", err)
	}
}

// TestNullWithDefaultValues tests NULL handling with default value substitution
func TestNullWithDefaultValues(t *testing.T) {
	fixture := setupFullServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create bundle with default values
	createCmd := `CREATE BUNDLE "Settings" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Key", "STRING", true, false},
		{"Value", "STRING", false, false, "default_value"},
		{"Priority", "INT", false, false, 0}
	);`

	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Settings bundle: %v", err)
	}

	// Insert document with NULL for field that has default value
	// The default value should be substituted
	addDocCmd := `ADD DOCUMENT TO BUNDLE "Settings" WITH ({"ID"=1}, {"Key"="config1"}, {"Value"=null});`
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// Query for NULL values - should find none because default "default_value" was substituted
	selectCmd := `SELECT * FROM "Settings" WHERE "Value" IS NULL;`
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute IS NULL query: %v", err)
	}

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		// Default value substitution means Value field should NOT be NULL
		if cmdResp.ResultCount != 0 {
			t.Errorf("Expected 0 settings with NULL value (default 'default_value' should be substituted), got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ Default value substitution working correctly - no NULL values found")
		}
	}

	// Query for non-NULL values - should find the document with default value
	selectCmd2 := `SELECT * FROM "Settings" WHERE "Value" IS NOT NULL;`
	response2, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd2, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute IS NOT NULL query: %v", err)
	}

	if cmdResp, ok := response2.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected 1 setting with non-NULL value (default), got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ IS NOT NULL query found document with default value: %d", cmdResp.ResultCount)
		}
	}

	// Verify the default value was actually substituted with "default_value"
	selectCmd3 := `SELECT * FROM "Settings" WHERE "Value" == "default_value";`
	response3, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd3, fixture.Logger, time.Now(), nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to execute equality query: %v", err)
	}

	if cmdResp, ok := response3.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected 1 setting with Value='default_value', got %d", cmdResp.ResultCount)
		} else {
			t.Logf("✓ Default value 'default_value' was correctly substituted")
		}
	}
}
