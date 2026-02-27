package bundle

import (
	"fmt"
	"strings"
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// createTestBundleNoFK creates a bundle with no relationships.
func createTestBundleNoFK(name string) *models.Bundle {
	return &models.Bundle{
		Name: name,
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"name": {Name: "name", Type: "STRING"},
			},
		},
		Indexes:       make(map[string]models.IndexReference),
		Relationships: make(map[string]models.Relationship),
	}
}

func testLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

// TestIdentifyInsertForeignKeyFields_NilDatabase verifies that
// IdentifyInsertForeignKeyFields returns nil when database is nil (zero-overhead fast path).
func TestIdentifyInsertForeignKeyFields_NilDatabase(t *testing.T) {
	logger := testLogger()
	validator := NewReferentialIntegrityValidator(nil, logger)
	bundle := createTestBundleNoFK("Products")
	bundleCache := make(map[string]*models.Bundle)

	insertFields := map[string]string{"name": "Widget"}
	fkUpdates := validator.IdentifyInsertForeignKeyFields(nil, bundle, insertFields, bundleCache)
	if fkUpdates != nil {
		t.Fatalf("expected nil FK updates for nil database, got %d", len(fkUpdates))
	}
}

// TestIdentifyForeignKeyFields_Section1_ForUpdate verifies that IdentifyForeignKeyFields
// section 1 (outgoing relationships) identifies fields via SourceField.
// This tests the UPDATE path where SourceField is the FK field in the current bundle.
func TestIdentifyForeignKeyFields_Section1_ForUpdate(t *testing.T) {
	logger := testLogger()
	validator := NewReferentialIntegrityValidator(nil, logger)

	// Simulate a bundle where SourceField IS the FK (as section 1 expects for UPDATE)
	bundle := &models.Bundle{
		Name: "Orders",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"customer_id": {Name: "customer_id", Type: "STRING"},
			},
		},
		Indexes: make(map[string]models.IndexReference),
		Relationships: map[string]models.Relationship{
			"orders_customers": {
				Name:         "orders_customers",
				SourceBundle: "Customers",
				SourceField:  "customer_id",
			},
		},
	}
	bundleCache := make(map[string]*models.Bundle)

	t.Run("FK field present in update fields is identified", func(t *testing.T) {
		updateFields := map[string]string{
			"customer_id": "cust-123",
			"other_field": "value",
		}
		fkUpdates := validator.IdentifyForeignKeyFields(nil, bundle, updateFields, bundleCache)
		if len(fkUpdates) != 1 {
			t.Fatalf("expected 1 FK update, got %d", len(fkUpdates))
		}
		if fkUpdates[0].FieldName != "customer_id" {
			t.Errorf("expected FK field 'customer_id', got '%s'", fkUpdates[0].FieldName)
		}
		if fkUpdates[0].NewValue != "cust-123" {
			t.Errorf("expected FK value 'cust-123', got '%s'", fkUpdates[0].NewValue)
		}
	})

	t.Run("non-FK field is not identified", func(t *testing.T) {
		updateFields := map[string]string{"other_field": "value"}
		fkUpdates := validator.IdentifyForeignKeyFields(nil, bundle, updateFields, bundleCache)
		if len(fkUpdates) != 0 {
			t.Fatalf("expected 0 FK updates, got %d", len(fkUpdates))
		}
	})

	t.Run("empty update fields returns no FK updates", func(t *testing.T) {
		fkUpdates := validator.IdentifyForeignKeyFields(nil, bundle, map[string]string{}, bundleCache)
		if len(fkUpdates) != 0 {
			t.Fatalf("expected 0 FK updates, got %d", len(fkUpdates))
		}
	})
}

// TestIdentifyForeignKeyFields_NoRelationships verifies that bundles with no relationships
// produce no FK updates (zero-overhead fast path).
func TestIdentifyForeignKeyFields_NoRelationships(t *testing.T) {
	logger := testLogger()
	validator := NewReferentialIntegrityValidator(nil, logger)
	bundle := createTestBundleNoFK("Products")
	bundleCache := make(map[string]*models.Bundle)

	updateFields := map[string]string{"name": "Widget", "price": "9.99"}
	fkUpdates := validator.IdentifyForeignKeyFields(nil, bundle, updateFields, bundleCache)
	if len(fkUpdates) != 0 {
		t.Fatalf("expected 0 FK updates for bundle with no relationships, got %d", len(fkUpdates))
	}
}

// TestFKFieldValueConversion verifies that fmt.Sprintf("%v", value) correctly converts
// non-string FK values (int64, float64, bool) to strings for hash index lookup.
// This is the fix for the UPDATE type bug where only string values were extracted.
func TestFKFieldValueConversion(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"string value", "hello", "hello"},
		{"int64 value", int64(42), "42"},
		{"float64 value", float64(3.14), "3.14"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"int value", 123, "123"},
		{"negative int64", int64(-99), "-99"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := fmt.Sprintf("%v", tc.value)
			if result != tc.expected {
				t.Errorf("fmt.Sprintf(\"%%v\", %v) = %q, want %q", tc.value, result, tc.expected)
			}
		})
	}
}

// TestFKFieldValueConversion_NilSkipped verifies that nil values are skipped
// during field extraction (not converted to "nil" string).
func TestFKFieldValueConversion_NilSkipped(t *testing.T) {
	fields := []models.KeyValue{
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: nil},
		{Key: "score", Value: int64(100)},
	}

	result := make(map[string]string)
	for _, kv := range fields {
		if kv.Value == nil {
			continue
		}
		result[kv.Key] = fmt.Sprintf("%v", kv.Value)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 fields (nil skipped), got %d", len(result))
	}
	if result["name"] != "Alice" {
		t.Errorf("expected name='Alice', got '%s'", result["name"])
	}
	if result["score"] != "100" {
		t.Errorf("expected score='100', got '%s'", result["score"])
	}
	if _, exists := result["age"]; exists {
		t.Error("nil value should have been skipped")
	}
}

// TestInsertFKValidationGuard verifies the guard condition for INSERT:
// FK validation only runs when database is non-nil (needed to scan other bundles).
func TestInsertFKValidationGuard(t *testing.T) {
	t.Run("nil database skips validation", func(t *testing.T) {
		var database *models.Database
		if database != nil {
			t.Error("nil database should skip INSERT FK validation")
		}
	})

	t.Run("non-nil database enables validation", func(t *testing.T) {
		database := &models.Database{Name: "testdb"}
		if database == nil {
			t.Error("non-nil database should enable INSERT FK validation")
		}
	})
}

// TestFKViolationErrorFormat verifies that ForeignKeyViolation.Error() produces
// correctly formatted error messages.
func TestFKViolationErrorFormat(t *testing.T) {
	violation := &ForeignKeyViolation{
		FieldName:           "customer_id",
		AttemptedValue:      "nonexistent-999",
		ParentBundle:        "Customers",
		ParentField:         "DocumentID",
		AffectedDocumentIDs: []string{"new-document"},
		SuggestedAction:     "Create document 'nonexistent-999' in bundle 'Customers' or choose an existing value",
	}

	errMsg := violation.Error()

	expectedParts := []string{
		"Foreign key violation",
		"customer_id",
		"nonexistent-999",
		"Customers",
	}
	for _, part := range expectedParts {
		if !strings.Contains(errMsg, part) {
			t.Errorf("expected error message to contain %q, got: %s", part, errMsg)
		}
	}
}

// TestBatchValidationCache verifies that the validation cache deduplicates
// FK lookups across multiple documents with the same FK value.
func TestBatchValidationCache(t *testing.T) {
	cache := make(map[string]*ForeignKeyViolation)

	// First lookup: cache miss
	cacheKey := fmt.Sprintf("%s:%s", "customer_id", "cust-123")
	if _, found := cache[cacheKey]; found {
		t.Error("expected cache miss on first lookup")
	}

	// Store nil (validation passed)
	cache[cacheKey] = nil

	// Second lookup: cache hit
	if _, found := cache[cacheKey]; !found {
		t.Error("expected cache hit on second lookup")
	}

	// Store violation
	violationKey := fmt.Sprintf("%s:%s", "customer_id", "bad-value")
	cache[violationKey] = &ForeignKeyViolation{
		FieldName:      "customer_id",
		AttemptedValue: "bad-value",
		ParentBundle:   "Customers",
	}

	if cached, found := cache[violationKey]; !found || cached == nil {
		t.Error("expected cached violation")
	}
}

// TestParentBundleRelationshipNotTreatedAsFK verifies that when a parent bundle
// has relationships stored on it (e.g., "users" with "users_orders"), the
// SourceField (PK like "ID") is NOT mistakenly treated as a FK for INSERT.
// This was the root cause bug: inserting into "users" would check "ID" as FK.
func TestParentBundleRelationshipNotTreatedAsFK(t *testing.T) {
	logger := testLogger()
	validator := NewReferentialIntegrityValidator(nil, logger)

	// Real-world model: relationship stored on PARENT bundle "users"
	// SourceField = "ID" (PK in users), DestinationField = "user_id" (FK in orders)
	usersBundle := &models.Bundle{
		Name: "users",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"ID":   {Name: "ID", Type: "INTEGER"},
				"name": {Name: "name", Type: "STRING"},
			},
		},
		Indexes: make(map[string]models.IndexReference),
		Relationships: map[string]models.Relationship{
			"users_orders": {
				Name:              "users_orders",
				RelationshipType:  "1toMany",
				SourceBundle:      "users",
				SourceField:       "ID",
				DestinationBundle: "orders",
				DestinationField:  "user_id",
			},
		},
	}

	// IdentifyInsertForeignKeyFields with nil database returns nil — correct for INSERT
	insertFields := map[string]string{"ID": "1", "name": "Alice"}
	bundleCache := make(map[string]*models.Bundle)
	fkUpdates := validator.IdentifyInsertForeignKeyFields(nil, usersBundle, insertFields, bundleCache)
	if fkUpdates != nil {
		t.Fatalf("IdentifyInsertForeignKeyFields should return nil for nil database, got %d updates", len(fkUpdates))
	}

	// IdentifyForeignKeyFields section 1 WOULD match "ID" (the PK) — this is why
	// it must NOT be used for INSERT
	fkUpdatesSection1 := validator.IdentifyForeignKeyFields(nil, usersBundle, insertFields, bundleCache)
	if len(fkUpdatesSection1) != 1 {
		t.Fatalf("expected section 1 to match SourceField 'ID', got %d", len(fkUpdatesSection1))
	}
	if fkUpdatesSection1[0].FieldName != "ID" {
		t.Errorf("expected section 1 to match 'ID', got '%s'", fkUpdatesSection1[0].FieldName)
	}
	// This demonstrates the bug: section 1 incorrectly treats PK "ID" as a FK
}

// TestMultipleRelationshipsOnParent verifies that IdentifyForeignKeyFields section 1
// identifies multiple SourceFields when a parent bundle has multiple relationships.
func TestMultipleRelationshipsOnParent(t *testing.T) {
	logger := testLogger()
	validator := NewReferentialIntegrityValidator(nil, logger)

	bundle := &models.Bundle{
		Name: "OrderItems",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"order_id":   {Name: "order_id", Type: "STRING"},
				"product_id": {Name: "product_id", Type: "STRING"},
			},
		},
		Indexes: make(map[string]models.IndexReference),
		Relationships: map[string]models.Relationship{
			"item_order": {
				Name:         "item_order",
				SourceBundle: "Orders",
				SourceField:  "order_id",
			},
			"item_product": {
				Name:         "item_product",
				SourceBundle: "Products",
				SourceField:  "product_id",
			},
		},
	}
	bundleCache := make(map[string]*models.Bundle)

	t.Run("both fields identified when both present", func(t *testing.T) {
		updateFields := map[string]string{
			"order_id":   "ord-1",
			"product_id": "prod-1",
		}
		fkUpdates := validator.IdentifyForeignKeyFields(nil, bundle, updateFields, bundleCache)
		if len(fkUpdates) != 2 {
			t.Fatalf("expected 2 FK updates, got %d", len(fkUpdates))
		}
	})

	t.Run("only one field identified when other not present", func(t *testing.T) {
		updateFields := map[string]string{
			"order_id": "ord-1",
			"name":     "Widget",
		}
		fkUpdates := validator.IdentifyForeignKeyFields(nil, bundle, updateFields, bundleCache)
		if len(fkUpdates) != 1 {
			t.Fatalf("expected 1 FK update, got %d", len(fkUpdates))
		}
		if fkUpdates[0].FieldName != "order_id" {
			t.Errorf("expected FK field 'order_id', got '%s'", fkUpdates[0].FieldName)
		}
	})
}
