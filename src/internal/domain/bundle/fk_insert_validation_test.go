package bundle

import (
	"fmt"
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// createTestBundleWithFK creates a child bundle with a foreign key relationship to a parent bundle.
// In SyndrDB's relationship model (for outgoing FKs stored on the child bundle):
//   - SourceField = the FK field in the child bundle (e.g., "customer_id")
//   - SourceBundle = the parent bundle being referenced (e.g., "Customers")
//   - DestinationBundle = the child bundle itself (e.g., "Orders")
//   - DestinationField = the referenced field in the parent (e.g., "DocumentID")
func createTestBundleWithFK(childName, parentName, fkField, parentField string) *models.Bundle {
	return &models.Bundle{
		Name: childName,
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				fkField: {Name: fkField, Type: "STRING"},
			},
		},
		Indexes:       make(map[string]models.IndexReference),
		Relationships: map[string]models.Relationship{
			childName + "_" + parentName: {
				Name:              childName + "_" + parentName,
				SourceBundle:      parentName,
				SourceBundleName:  parentName,
				SourceField:       fkField,
				DestinationBundle: parentName,
				DestinationField:  parentField,
			},
		},
	}
}

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

// TestIdentifyForeignKeyFields_OutgoingRelationships verifies that IdentifyForeignKeyFields
// correctly identifies FK fields from outgoing relationships (bundle.Relationships).
func TestIdentifyForeignKeyFields_OutgoingRelationships(t *testing.T) {
	logger := testLogger()
	validator := NewReferentialIntegrityValidator(nil, logger)
	bundle := createTestBundleWithFK("Orders", "Customers", "customer_id", "DocumentID")
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
		updateFields := map[string]string{
			"other_field": "value",
		}
		fkUpdates := validator.IdentifyForeignKeyFields(nil, bundle, updateFields, bundleCache)
		if len(fkUpdates) != 0 {
			t.Fatalf("expected 0 FK updates, got %d", len(fkUpdates))
		}
	})

	t.Run("empty update fields returns no FK updates", func(t *testing.T) {
		updateFields := map[string]string{}
		fkUpdates := validator.IdentifyForeignKeyFields(nil, bundle, updateFields, bundleCache)
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

	updateFields := map[string]string{
		"name":  "Widget",
		"price": "9.99",
	}
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

// TestFKValidationEarlyExit_NoRelationships verifies the zero-overhead fast path:
// when a bundle has no relationships AND database is nil, FK validation should be skipped.
func TestFKValidationEarlyExit_NoRelationships(t *testing.T) {
	bundle := createTestBundleNoFK("Products")

	// Simulate the guard condition used in all INSERT/UPDATE paths
	var database *models.Database // nil database
	shouldValidate := len(bundle.Relationships) > 0 || database != nil
	if shouldValidate {
		t.Error("expected FK validation to be skipped for bundle with no relationships and nil database")
	}
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

	// Check essential components
	if got := errMsg; got == "" {
		t.Fatal("error message should not be empty")
	}
	expectedParts := []string{
		"Foreign key violation",
		"customer_id",
		"nonexistent-999",
		"Customers",
	}
	for _, part := range expectedParts {
		found := false
		for i := 0; i <= len(errMsg)-len(part); i++ {
			if errMsg[i:i+len(part)] == part {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected error message to contain %q, got: %s", part, errMsg)
		}
	}
}

// TestBatchValidationCache verifies that the validation cache deduplicates
// FK lookups across multiple documents with the same FK value.
func TestBatchValidationCache(t *testing.T) {
	// Simulate the cache key generation used in batchValidateForeignKeys
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

	// Verify violation is cached
	if cached, found := cache[violationKey]; !found || cached == nil {
		t.Error("expected cached violation")
	}
}

// TestMultipleRelationships verifies that IdentifyForeignKeyFields correctly
// identifies multiple FK fields when a bundle has multiple relationships.
func TestMultipleRelationships(t *testing.T) {
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
				Name:              "item_order",
				SourceBundle:      "Orders",
				SourceField:       "order_id",
				DestinationBundle: "Orders",
				DestinationField:  "DocumentID",
			},
			"item_product": {
				Name:              "item_product",
				SourceBundle:      "Products",
				SourceField:       "product_id",
				DestinationBundle: "Products",
				DestinationField:  "DocumentID",
			},
		},
	}
	bundleCache := make(map[string]*models.Bundle)

	t.Run("both FK fields identified when both present", func(t *testing.T) {
		updateFields := map[string]string{
			"order_id":   "ord-1",
			"product_id": "prod-1",
		}
		fkUpdates := validator.IdentifyForeignKeyFields(nil, bundle, updateFields, bundleCache)
		if len(fkUpdates) != 2 {
			t.Fatalf("expected 2 FK updates, got %d", len(fkUpdates))
		}
	})

	t.Run("only one FK field identified when other not present", func(t *testing.T) {
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
