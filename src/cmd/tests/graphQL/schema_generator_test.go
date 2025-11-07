package main

import (
	"os"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/graphQL/schema"
)

// TestSchemaGeneratorBasic tests basic schema generation
func TestSchemaGeneratorBasic(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	// Create test bundle
	bundle := &models.Bundle{
		Name: "users",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"name": {
					Name:       "name",
					Type:       "string",
					IsRequired: true,
				},
				"email": {
					Name:       "email",
					Type:       "string",
					IsRequired: true,
				},
				"age": {
					Name:       "age",
					Type:       "int",
					IsRequired: false,
				},
			},
		},
	}

	// Generate schema
	schemaDef, err := generator.GenerateSchema(bundle)
	if err != nil {
		t.Fatalf("Failed to generate schema: %v", err)
	}

	// Verify type name
	if schemaDef.TypeName != "User" {
		t.Errorf("Expected type name 'User', got '%s'", schemaDef.TypeName)
	}

	// Verify fields (should have id + 3 bundle fields = 4 total)
	if len(schemaDef.Fields) != 4 {
		t.Errorf("Expected 4 fields, got %d", len(schemaDef.Fields))
	}

	// Check ID field
	idField := schemaDef.Fields[0]
	if idField.Name != "id" || idField.Type != "ID!" {
		t.Errorf("Expected ID field, got Name=%s, Type=%s", idField.Name, idField.Type)
	}

	// Verify field types and nullability
	fieldMap := make(map[string]schema.GraphQLField)
	for _, field := range schemaDef.Fields {
		fieldMap[field.Name] = field
	}

	// Check name field (required string)
	if nameField, exists := fieldMap["name"]; exists {
		if nameField.Type != "String!" {
			t.Errorf("Expected name type 'String!', got '%s'", nameField.Type)
		}
	} else {
		t.Error("name field not found")
	}

	// Check age field (optional int)
	if ageField, exists := fieldMap["age"]; exists {
		if ageField.Type != "Int" {
			t.Errorf("Expected age type 'Int', got '%s'", ageField.Type)
		}
	} else {
		t.Error("age field not found")
	}
}

// TestSchemaGeneratorTypeName tests bundle name to type name conversion
func TestSchemaGeneratorTypeName(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	tests := []struct {
		bundleName string
		expected   string
	}{
		{"users", "User"},
		{"blog_posts", "BlogPost"},
		{"user_profiles", "UserProfile"},
		{"addresses", "Addresse"}, // Doesn't remove 's' from "ss"
		{"data", "Data"},
	}

	for _, tt := range tests {
		bundle := &models.Bundle{
			Name: tt.bundleName,
			DocumentStructure: models.DocumentStructure{
				FieldDefinitions: map[string]models.FieldDefinition{
					"test": {Name: "test", Type: "string"},
				},
			},
		}

		schemaDef, err := generator.GenerateSchema(bundle)
		if err != nil {
			t.Fatalf("Failed to generate schema for %s: %v", tt.bundleName, err)
		}

		if schemaDef.TypeName != tt.expected {
			t.Errorf("Bundle '%s': expected type name '%s', got '%s'",
				tt.bundleName, tt.expected, schemaDef.TypeName)
		}
	}
}

// TestSchemaGeneratorTypeMapping tests field type mapping
func TestSchemaGeneratorTypeMapping(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	tests := []struct {
		fieldType    string
		expectedType string
	}{
		{"string", "String"},
		{"int", "Int"},
		{"float", "Float"},
		{"bool", "Boolean"},
		{"boolean", "Boolean"},
		{"id", "ID"},
	}

	for _, tt := range tests {
		bundle := &models.Bundle{
			Name: "test",
			DocumentStructure: models.DocumentStructure{
				FieldDefinitions: map[string]models.FieldDefinition{
					"testField": {
						Name: "testField",
						Type: tt.fieldType,
					},
				},
			},
		}

		schemaDef, err := generator.GenerateSchema(bundle)
		if err != nil {
			t.Fatalf("Failed to generate schema for type %s: %v", tt.fieldType, err)
		}

		// Find the test field
		var found bool
		for _, field := range schemaDef.Fields {
			if field.Name == "testField" {
				if field.Type != tt.expectedType {
					t.Errorf("Type %s: expected GraphQL type '%s', got '%s'",
						tt.fieldType, tt.expectedType, field.Type)
				}
				found = true
				break
			}
		}

		if !found {
			t.Errorf("testField not found in schema for type %s", tt.fieldType)
		}
	}
}

// TestSchemaGeneratorValidation tests schema validation
func TestSchemaGeneratorValidation(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	// Test nil schema
	err := generator.ValidateSchema(nil)
	if err == nil {
		t.Error("Expected error for nil schema")
	}

	// Test empty type name
	invalidSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "",
		Fields:   []schema.GraphQLField{{Name: "test", Type: "String"}},
	}
	err = generator.ValidateSchema(invalidSchema)
	if err == nil {
		t.Error("Expected error for empty type name")
	}

	// Test no fields
	invalidSchema = &schema.GraphQLSchemaDefinition{
		TypeName: "Test",
		Fields:   []schema.GraphQLField{},
	}
	err = generator.ValidateSchema(invalidSchema)
	if err == nil {
		t.Error("Expected error for no fields")
	}

	// Test duplicate field names
	invalidSchema = &schema.GraphQLSchemaDefinition{
		TypeName: "Test",
		Fields: []schema.GraphQLField{
			{Name: "field1", Type: "String"},
			{Name: "field1", Type: "Int"}, // Duplicate
		},
	}
	err = generator.ValidateSchema(invalidSchema)
	if err == nil {
		t.Error("Expected error for duplicate field names")
	}

	// Test valid schema
	validSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "Test",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"},
		},
	}
	err = generator.ValidateSchema(validSchema)
	if err != nil {
		t.Errorf("Unexpected error for valid schema: %v", err)
	}
}

// TestSchemaGeneratorBreakingChanges tests breaking change detection
func TestSchemaGeneratorBreakingChanges(t *testing.T) {
	generator := schema.NewSchemaGenerator()

	// Create old schema
	oldSchema := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"},
			{Name: "email", Type: "String!"},
			{Name: "age", Type: "Int"},
		},
	}

	// Test field removal (breaking)
	newSchema1 := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"},
			// email removed
			{Name: "age", Type: "Int"},
		},
	}

	changes := generator.DetectBreakingChanges(oldSchema, newSchema1)
	if len(changes) != 1 {
		t.Errorf("Expected 1 breaking change (field removed), got %d", len(changes))
	}
	if len(changes) > 0 && changes[0].ChangeType != "FIELD_REMOVED" {
		t.Errorf("Expected FIELD_REMOVED, got %s", changes[0].ChangeType)
	}

	// Test type change (breaking)
	newSchema2 := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"},
			{Name: "email", Type: "Int!"}, // Type changed
			{Name: "age", Type: "Int"},
		},
	}

	changes = generator.DetectBreakingChanges(oldSchema, newSchema2)
	if len(changes) != 1 {
		t.Errorf("Expected 1 breaking change (type changed), got %d", len(changes))
	}
	if len(changes) > 0 && changes[0].ChangeType != "TYPE_CHANGED" {
		t.Errorf("Expected TYPE_CHANGED, got %s", changes[0].ChangeType)
	}

	// Test nullability change (nullable -> non-null is breaking)
	newSchema3 := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String!"}, // Changed from String to String!
			{Name: "email", Type: "String!"},
			{Name: "age", Type: "Int"},
		},
	}

	changes = generator.DetectBreakingChanges(oldSchema, newSchema3)
	if len(changes) != 1 {
		t.Errorf("Expected 1 breaking change (nullability), got %d", len(changes))
	}
	if len(changes) > 0 && changes[0].ChangeType != "NULLABILITY_CHANGED" {
		t.Errorf("Expected NULLABILITY_CHANGED, got %s", changes[0].ChangeType)
	}

	// Test non-breaking change (non-null -> nullable is OK)
	newSchema4 := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"},
			{Name: "email", Type: "String"}, // Changed from String! to String
			{Name: "age", Type: "Int"},
		},
	}

	changes = generator.DetectBreakingChanges(oldSchema, newSchema4)
	if len(changes) != 0 {
		t.Errorf("Expected 0 breaking changes (nullable OK), got %d", len(changes))
	}

	// Test new field added (not breaking)
	newSchema5 := &schema.GraphQLSchemaDefinition{
		TypeName: "User",
		Fields: []schema.GraphQLField{
			{Name: "id", Type: "ID!"},
			{Name: "name", Type: "String"},
			{Name: "email", Type: "String!"},
			{Name: "age", Type: "Int"},
			{Name: "address", Type: "String"}, // New field
		},
	}

	changes = generator.DetectBreakingChanges(oldSchema, newSchema5)
	if len(changes) != 0 {
		t.Errorf("Expected 0 breaking changes (new field OK), got %d", len(changes))
	}
}

// TestSchemaGeneratorWithIntegration tests generator integration with SchemaManager
func TestSchemaGeneratorWithIntegration(t *testing.T) {
	tmpFile := "/tmp/test_generator_integration.gql"
	defer os.Remove(tmpFile)

	generator := schema.NewSchemaGenerator()
	manager, err := schema.NewSchemaManager(tmpFile, "testdb", "test1234")
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Create test bundle
	bundle := &models.Bundle{
		BundleID:  "bundle-001",
		Name:      "products",
		CreatedAt: time.Now(),
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"name": {
					Name:       "name",
					Type:       "string",
					IsRequired: true,
				},
				"price": {
					Name:       "price",
					Type:       "float",
					IsRequired: true,
				},
				"inStock": {
					Name:       "inStock",
					Type:       "bool",
					IsRequired: false,
				},
			},
		},
	}

	// Generate schema
	schemaDef, err := generator.GenerateSchema(bundle)
	if err != nil {
		t.Fatalf("Failed to generate schema: %v", err)
	}

	// Validate schema
	if err := generator.ValidateSchema(schemaDef); err != nil {
		t.Fatalf("Schema validation failed: %v", err)
	}

	// Add schema to manager
	var schemaID, bundleID [16]byte
	copy(schemaID[:], []byte("schema-id-000001"))
	copy(bundleID[:], []byte(bundle.BundleID))

	err = manager.AddNewSchema(schemaID, bundleID, bundle.Name, schemaDef)
	if err != nil {
		t.Fatalf("Failed to add schema: %v", err)
	}

	// Retrieve and verify
	retrieved, err := manager.GetCachedSchema(bundle.Name)
	if err != nil || retrieved == nil {
		t.Fatalf("Failed to retrieve schema: %v", err)
	}

	if retrieved.Payload.TypeName != "Product" {
		t.Errorf("Expected type name 'Product', got '%s'", retrieved.Payload.TypeName)
	}

	if len(retrieved.Payload.Fields) != 4 { // id + 3 bundle fields
		t.Errorf("Expected 4 fields, got %d", len(retrieved.Payload.Fields))
	}
}
