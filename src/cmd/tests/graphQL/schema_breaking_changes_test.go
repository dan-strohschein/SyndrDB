package main

import (
	"testing"

	"syndrdb/src/internal/graphQL/schema"
)

// TestDetectBreakingChanges_FieldRemoval tests detection of removed fields
func TestDetectBreakingChanges_FieldRemoval(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	oldSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"},
			{Name: "email", Type: "String!"},
			{Name: "age", Type: "Int"},
		},
	}

	newSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"},
			// email field removed
			{Name: "age", Type: "Int"},
		},
	}

	changes := generator.DetectBreakingChanges(oldSchema, newSchema)

	if len(changes) != 1 {
		t.Fatalf("Expected 1 breaking change, got %d", len(changes))
	}

	change := changes[0]
	if change.ChangeType != "FIELD_REMOVED" {
		t.Errorf("Expected FIELD_REMOVED, got %s", change.ChangeType)
	}
	if change.FieldName != "email" {
		t.Errorf("Expected field name 'email', got '%s'", change.FieldName)
	}
	if change.Severity != "BREAKING" {
		t.Errorf("Expected severity BREAKING, got %s", change.Severity)
	}

	t.Logf("✓ Field removal detected: %s (field: %s)", change.ChangeType, change.FieldName)
}

// TestDetectBreakingChanges_TypeChange tests detection of field type changes
func TestDetectBreakingChanges_TypeChange(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	oldSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "age", Type: "Int"},
		},
	}

	newSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "age", Type: "String"}, // Changed from Int to String
		},
	}

	changes := generator.DetectBreakingChanges(oldSchema, newSchema)

	if len(changes) != 1 {
		t.Fatalf("Expected 1 breaking change, got %d", len(changes))
	}

	change := changes[0]
	if change.ChangeType != "TYPE_CHANGED" {
		t.Errorf("Expected TYPE_CHANGED, got %s", change.ChangeType)
	}
	if change.FieldName != "age" {
		t.Errorf("Expected field name 'age', got '%s'", change.FieldName)
	}
	if change.OldValue != "Int" {
		t.Errorf("Expected old value 'Int', got '%s'", change.OldValue)
	}
	if change.NewValue != "String" {
		t.Errorf("Expected new value 'String', got '%s'", change.NewValue)
	}

	t.Logf("✓ Type change detected: %s → %s (field: %s)", change.OldValue, change.NewValue, change.FieldName)
}

// TestDetectBreakingChanges_NullabilityChange tests nullable → non-null changes
func TestDetectBreakingChanges_NullabilityChange(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	oldSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"}, // Nullable
		},
	}

	newSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"}, // Made non-nullable
		},
	}

	changes := generator.DetectBreakingChanges(oldSchema, newSchema)

	if len(changes) != 1 {
		t.Fatalf("Expected 1 breaking change, got %d", len(changes))
	}

	change := changes[0]
	if change.ChangeType != "NULLABILITY_CHANGED" {
		t.Errorf("Expected NULLABILITY_CHANGED, got %s", change.ChangeType)
	}
	if change.FieldName != "name" {
		t.Errorf("Expected field name 'name', got '%s'", change.FieldName)
	}

	t.Logf("✓ Nullability change detected: %s → %s (field: %s)", 
		change.OldValue, change.NewValue, change.FieldName)
}

// TestDetectBreakingChanges_NonNullToNull tests non-null → nullable (safe change)
func TestDetectBreakingChanges_NonNullToNull(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	oldSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"}, // Non-nullable
		},
	}

	newSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"}, // Made nullable
		},
	}

	changes := generator.DetectBreakingChanges(oldSchema, newSchema)

	// non-null → nullable is NOT a breaking change (backward compatible)
	if len(changes) != 0 {
		t.Errorf("Expected 0 breaking changes (non-null → nullable is safe), got %d", len(changes))
		for _, change := range changes {
			t.Logf("  Unexpected change: %s on field %s", change.ChangeType, change.FieldName)
		}
	} else {
		t.Logf("✓ Non-null → nullable correctly not flagged as breaking")
	}
}

// TestDetectBreakingChanges_FieldAddition tests that adding fields is safe
func TestDetectBreakingChanges_FieldAddition(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	oldSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"},
		},
	}

	newSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"},
			{Name: "email", Type: "String"}, // New field added
		},
	}

	changes := generator.DetectBreakingChanges(oldSchema, newSchema)

	// Adding fields is NOT a breaking change
	if len(changes) != 0 {
		t.Errorf("Expected 0 breaking changes (field addition is safe), got %d", len(changes))
		for _, change := range changes {
			t.Logf("  Unexpected change: %s on field %s", change.ChangeType, change.FieldName)
		}
	} else {
		t.Logf("✓ Field addition correctly not flagged as breaking")
	}
}

// TestDetectBreakingChanges_MultipleChanges tests detection of multiple breaking changes
func TestDetectBreakingChanges_MultipleChanges(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	oldSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"},
			{Name: "email", Type: "String"},
			{Name: "age", Type: "Int"},
		},
	}

	newSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"}, 
			// email removed (1 breaking change)
			{Name: "age", Type: "String"}, // type changed (2 breaking changes)
		},
	}

	changes := generator.DetectBreakingChanges(oldSchema, newSchema)

	if len(changes) != 2 {
		t.Fatalf("Expected 2 breaking changes, got %d", len(changes))
	}

	// Count change types
	changeTypes := make(map[string]int)
	for _, change := range changes {
		changeTypes[change.ChangeType]++
		t.Logf("  Breaking change: %s on field '%s'", change.ChangeType, change.FieldName)
	}

	if changeTypes["FIELD_REMOVED"] != 1 {
		t.Errorf("Expected 1 FIELD_REMOVED, got %d", changeTypes["FIELD_REMOVED"])
	}
	if changeTypes["TYPE_CHANGED"] != 1 {
		t.Errorf("Expected 1 TYPE_CHANGED, got %d", changeTypes["TYPE_CHANGED"])
	}

	t.Logf("✓ Multiple breaking changes detected correctly: %d total", len(changes))
}

// TestDetectBreakingChanges_NilSchemas tests handling of nil inputs
func TestDetectBreakingChanges_NilSchemas(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	changes := generator.DetectBreakingChanges(nil, nil)
	if changes != nil {
		t.Errorf("Expected nil for nil inputs, got %d changes", len(changes))
	}

	schema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
		},
	}

	changes = generator.DetectBreakingChanges(nil, schema)
	if changes != nil {
		t.Errorf("Expected nil for nil old schema, got %d changes", len(changes))
	}

	changes = generator.DetectBreakingChanges(schema, nil)
	if changes != nil {
		t.Errorf("Expected nil for nil new schema, got %d changes", len(changes))
	}

	t.Logf("✓ Nil schema handling correct")
}

// TestDetectBreakingChanges_NoChanges tests schema with no modifications
func TestDetectBreakingChanges_NoChanges(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	oldSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"},
			{Name: "email", Type: "String"},
		},
	}

	newSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"},
			{Name: "email", Type: "String"},
		},
	}

	changes := generator.DetectBreakingChanges(oldSchema, newSchema)

	if len(changes) != 0 {
		t.Errorf("Expected 0 breaking changes for identical schemas, got %d", len(changes))
	}

	t.Logf("✓ Identical schemas correctly have no breaking changes")
}
