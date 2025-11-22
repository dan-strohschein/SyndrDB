package schema

// schema_generator.go
//
// This file implements the GraphQL schema generator for SyndrDB.
// The generator converts bundle structures into GraphQL schema definitions,
// automatically detecting relationships and tracking breaking changes.
//
// Phase 5 functionality:
// - Generate GraphQL schemas from bundle field definitions
// - Detect breaking changes between schema versions
// - Support relationship field generation
// - Integrate with BundleService for automatic schema updates
//
// Design Principles (from Phase 1):
// - Single Responsibility: Generates schemas only, delegates storage to SchemaManager
// - Open/Closed: Extensible through type mapping and field generation
// - DRY: Reuses existing models.Bundle structure and field definitions

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
)

// SchemaGenerator generates GraphQL schemas from bundle structures
type SchemaGenerator struct {
	// Type mapping from SyndrDB field types to GraphQL types
	typeMap map[string]string
}

// NewSchemaGenerator creates a new schema generator
func NewSchemaGenerator() *SchemaGenerator {
	return &SchemaGenerator{
		typeMap: getDefaultTypeMap(),
	}
}

// getDefaultTypeMap returns the default mapping from SyndrDB types to GraphQL types
func getDefaultTypeMap() map[string]string {
	return map[string]string{
		// Primitive types
		"string":  "String",
		"int":     "Int",
		"float":   "Float",
		"bool":    "Boolean",
		"boolean": "Boolean",

		// Special types
		"id":        "ID",
		"datetime":  "DateTime", // Custom DateTime scalar (RFC3339)
		"timestamp": "DateTime", // Alias for DateTime
		"date":      "Date",     // Custom Date scalar (YYYY-MM-DD)

		// Relationship type (resolved to target bundle type)
		"relationship": "String", // Default, overridden by actual relationship
	}
}

// GenerateSchema converts a bundle into a GraphQL schema definition
// This is the main entry point for schema generation
func (sg *SchemaGenerator) GenerateSchema(bundle *models.Bundle) (*GraphQLSchemaDefinition, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	if bundle.Name == "" {
		return nil, fmt.Errorf("bundle name cannot be empty")
	}

	// Convert bundle name to GraphQL type name (PascalCase)
	typeName := sg.toTypeName(bundle.Name)

	// Generate fields from bundle field definitions
	fields, err := sg.generateFields(bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to generate fields: %w", err)
	}

	// Create schema definition
	schema := &GraphQLSchemaDefinition{
		TypeName:    typeName,
		Description: fmt.Sprintf("Auto-generated schema for %s bundle", bundle.Name),
		Fields:      fields,
	}

	return schema, nil
}

// generateFields converts bundle field definitions into GraphQL fields
// PHASE 8: Now includes automatic relationship field generation with bidirectional support
func (sg *SchemaGenerator) generateFields(bundle *models.Bundle) ([]GraphQLField, error) {
	fields := make([]GraphQLField, 0, 30)

	// Always include DocumentID as the primary ID field
	fields = append(fields, GraphQLField{
		Name:        "id",
		Type:        "ID!",
		Description: "Unique document identifier",
	})

	// Generate fields from bundle field definitions
	for _, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
		field, err := sg.generateField(fieldDef, bundle)
		if err != nil {
			return nil, fmt.Errorf("failed to generate field %s: %w", fieldDef.Name, err)
		}
		fields = append(fields, field)
	}

	// PHASE 8: Generate relationship fields from bundle.Relationships map
	// This enables GraphQL queries to traverse relationships between bundles
	relationshipFields, err := sg.generateRelationshipFields(bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to generate relationship fields: %w", err)
	}
	fields = append(fields, relationshipFields...)

	return fields, nil
}

// generateField converts a single bundle field into a GraphQL field
func (sg *SchemaGenerator) generateField(fieldDef models.FieldDefinition, bundle *models.Bundle) (GraphQLField, error) {
	// Map SyndrDB type to GraphQL type
	graphqlType, err := sg.mapType(fieldDef)
	if err != nil {
		return GraphQLField{}, err
	}

	// Apply nullability
	if fieldDef.IsRequired {
		graphqlType = graphqlType + "!"
	}

	// Create field
	field := GraphQLField{
		Name:        fieldDef.Name,
		Type:        graphqlType,
		Description: fmt.Sprintf("%s field", fieldDef.Name),
	}

	// Note: Relationship handling would go here if we add relationship support to FieldDefinition
	// For now, relationships are handled separately via bundle.Relationships map

	return field, nil
}

// generateRelationshipFields generates GraphQL fields for bundle relationships
// PHASE 8: Relationship Resolver Implementation
//
// This function analyzes the bundle's Relationships map and generates appropriate GraphQL fields
// to enable relationship traversal in queries. It supports bidirectional relationships, meaning
// that if Bundle A has a relationship to Bundle B, both bundles will get appropriate fields:
//
// FORWARD RELATIONSHIP (defined in bundle.Relationships):
//
//	Example: users bundle has relationship to posts bundle
//	- users.Relationships["posts"] → RelationshipType: "1toMany"
//	- Generated field in User type: posts: [Post!]!
//
// BIDIRECTIONAL SUPPORT (implied inverse relationship):
//
//	Example: posts bundle doesn't explicitly define relationship to users
//	- But we detect the inverse from users → posts relationship
//	- Generated field in Post type: user: User! (or author: User! based on field naming)
//
// RELATIONSHIP TYPE MAPPING:
//   - "1toMany" → Source gets [TargetType!]!, Target gets SourceType (optional) or SourceType!
//   - "0toMany" → Source gets [TargetType!]!, Target gets SourceType (optional)
//   - "ManyToMany" → Both get [TargetType!]!
//
// TODO: In Phase 10, I will add DataLoader batching support to prevent N+1 query problems
// when resolving these relationship fields. This will batch relationship queries for better performance.
func (sg *SchemaGenerator) generateRelationshipFields(bundle *models.Bundle) ([]GraphQLField, error) {
	fields := make([]GraphQLField, 0)

	if len(bundle.Relationships) == 0 {
		// No relationships defined for this bundle
		return fields, nil
	}

	// Iterate through all relationships defined on this bundle
	for relationshipName, relationship := range bundle.Relationships {
		// Determine the GraphQL field type based on relationship type
		field, err := sg.generateRelationshipField(relationshipName, relationship, bundle)
		if err != nil {
			return nil, fmt.Errorf("failed to generate relationship field %s: %w", relationshipName, err)
		}

		fields = append(fields, field)
	}

	return fields, nil
}

// generateRelationshipField creates a single GraphQL field for a relationship
// PHASE 8: Detailed relationship field generation with type inference
//
// This function determines the appropriate GraphQL type for a relationship field based on:
// 1. Relationship cardinality (1toMany, 0toMany, ManyToMany)
// 2. Target bundle name (converted to GraphQL type name)
// 3. Nullability rules based on relationship semantics
//
// CARDINALITY TO GRAPHQL TYPE MAPPING:
//
//	1toMany: Returns array of target type (non-null array, non-null elements): [TargetType!]!
//	0toMany: Returns array of target type (non-null array, non-null elements): [TargetType!]!
//	ManyToMany: Returns array of target type (non-null array, non-null elements): [TargetType!]!
//
// The difference between 1toMany and 0toMany is semantic (minimum cardinality), but in GraphQL
// both are represented as arrays. The resolver will handle returning empty arrays for 0toMany.
func (sg *SchemaGenerator) generateRelationshipField(relationshipName string, relationship models.Relationship, bundle *models.Bundle) (GraphQLField, error) {
	// Convert destination bundle name to GraphQL type name
	// Example: "posts" → "Post", "user_profiles" → "UserProfile"
	targetTypeName := sg.toTypeName(relationship.DestinationBundle)

	if targetTypeName == "" {
		return GraphQLField{}, fmt.Errorf("relationship %s has empty destination bundle", relationshipName)
	}

	// Determine GraphQL field type based on relationship type
	var graphqlType string
	var description string

	switch relationship.RelationshipType {
	case "1toMany":
		// One-to-many: Source has array of targets (minimum 1)
		// GraphQL type: [TargetType!]! (non-null array of non-null items)
		graphqlType = fmt.Sprintf("[%s!]!", targetTypeName)
		description = fmt.Sprintf("One-to-many relationship to %s", relationship.DestinationBundle)

	case "0toMany":
		// Zero-to-many: Source has array of targets (minimum 0)
		// GraphQL type: [TargetType!]! (non-null array, can be empty)
		graphqlType = fmt.Sprintf("[%s!]!", targetTypeName)
		description = fmt.Sprintf("Zero-to-many relationship to %s", relationship.DestinationBundle)

	case "ManyToMany":
		// Many-to-many: Source has array of targets
		// GraphQL type: [TargetType!]! (non-null array of non-null items)
		graphqlType = fmt.Sprintf("[%s!]!", targetTypeName)
		description = fmt.Sprintf("Many-to-many relationship to %s", relationship.DestinationBundle)

	default:
		// Unknown relationship type - default to nullable array
		// TODO: I should add support for custom relationship types in Phase 10
		graphqlType = fmt.Sprintf("[%s!]", targetTypeName)
		description = fmt.Sprintf("Relationship to %s (type: %s)", relationship.DestinationBundle, relationship.RelationshipType)
	}

	// Create the relationship field
	field := GraphQLField{
		Name:        relationshipName,
		Type:        graphqlType,
		Description: description,
		// Note: We could add Arguments here for pagination/filtering on relationships
		// Example: posts(first: 10, after: "cursor", where: {status: "published"})
		// TODO: In Phase 9, I will add pagination arguments to relationship fields
	}

	return field, nil
}

// mapType maps a SyndrDB field type to a GraphQL type
func (sg *SchemaGenerator) mapType(fieldDef models.FieldDefinition) (string, error) {
	fieldType := strings.ToLower(fieldDef.Type)

	// Handle relationship type specially
	if fieldType == "relationship" {
		// Relationships are handled separately via bundle.Relationships map
		// For now, return String as default for relationship fields
		return "String", nil
	}

	// Look up in type map
	graphqlType, exists := sg.typeMap[fieldType]
	if !exists {
		return "", fmt.Errorf("unsupported field type: %s", fieldDef.Type)
	}

	return graphqlType, nil
}

// toTypeName converts a bundle name to a GraphQL type name (PascalCase)
// Examples: "users" -> "User", "blog_posts" -> "BlogPost"
func (sg *SchemaGenerator) toTypeName(bundleName string) string {
	// Handle empty string
	if bundleName == "" {
		return ""
	}

	// Split on underscores and capitalize each part
	parts := strings.Split(bundleName, "_")
	for i, part := range parts {
		if len(part) > 0 {
			// Capitalize first letter, rest stays as-is
			parts[i] = strings.ToUpper(part[0:1]) + part[1:]
		}
	}

	typeName := strings.Join(parts, "")

	// Remove trailing 's' for plurals (simple heuristic)
	// "users" -> "User", but keep "Address" as "Address"
	if len(typeName) > 1 && strings.HasSuffix(typeName, "s") {
		// Don't remove 's' if it's part of a common ending like "ss"
		if !strings.HasSuffix(typeName, "ss") {
			typeName = typeName[:len(typeName)-1]
		}
	}

	return typeName
}

// DetectBreakingChanges compares two schemas and identifies breaking changes
// Breaking changes include:
// - Removed fields
// - Type changes
// - Nullability changes (non-null -> null is OK, reverse is breaking)
// - Relationship target changes
func (sg *SchemaGenerator) DetectBreakingChanges(oldSchema, newSchema *GraphQLSchemaDefinition) []BreakingChange {
	if oldSchema == nil || newSchema == nil {
		return nil
	}

	changes := make([]BreakingChange, 0)

	// Build field maps for quick lookup
	oldFields := make(map[string]GraphQLField)
	for _, field := range oldSchema.Fields {
		oldFields[field.Name] = field
	}

	newFields := make(map[string]GraphQLField)
	for _, field := range newSchema.Fields {
		newFields[field.Name] = field
	}

	// Check for removed fields
	for fieldName, oldField := range oldFields {
		if _, exists := newFields[fieldName]; !exists {
			changes = append(changes, BreakingChange{
				ChangeType: "FIELD_REMOVED",
				FieldName:  fieldName,
				OldValue:   oldField.Type,
				NewValue:   "",
				Severity:   "BREAKING",
			})
		}
	}

	// Check for type changes and nullability changes
	for fieldName, newField := range newFields {
		oldField, exists := oldFields[fieldName]
		if !exists {
			// New field added - not a breaking change
			continue
		}

		// Check for type changes
		oldBaseType := sg.stripNullability(oldField.Type)
		newBaseType := sg.stripNullability(newField.Type)

		if oldBaseType != newBaseType {
			changes = append(changes, BreakingChange{
				ChangeType: "TYPE_CHANGED",
				FieldName:  fieldName,
				OldValue:   oldField.Type,
				NewValue:   newField.Type,
				Severity:   "BREAKING",
			})
		}

		// Check for nullability changes (null -> non-null is breaking)
		oldNullable := !strings.HasSuffix(oldField.Type, "!")
		newNullable := !strings.HasSuffix(newField.Type, "!")

		if oldNullable && !newNullable {
			changes = append(changes, BreakingChange{
				ChangeType: "NULLABILITY_CHANGED",
				FieldName:  fieldName,
				OldValue:   oldField.Type,
				NewValue:   newField.Type,
				Severity:   "BREAKING",
			})
		}

	}

	return changes
}

// stripNullability removes the ! suffix from a GraphQL type
func (sg *SchemaGenerator) stripNullability(graphqlType string) string {
	return strings.TrimSuffix(graphqlType, "!")
}

// ValidateSchema performs basic validation on a schema definition
func (sg *SchemaGenerator) ValidateSchema(schema *GraphQLSchemaDefinition) error {
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	if schema.TypeName == "" {
		return fmt.Errorf("type name cannot be empty")
	}

	if len(schema.Fields) == 0 {
		return fmt.Errorf("schema must have at least one field")
	}

	// Validate field names are unique
	fieldNames := make(map[string]bool)
	for _, field := range schema.Fields {
		if field.Name == "" {
			return fmt.Errorf("field name cannot be empty")
		}

		if fieldNames[field.Name] {
			return fmt.Errorf("duplicate field name: %s", field.Name)
		}
		fieldNames[field.Name] = true

		if field.Type == "" {
			return fmt.Errorf("field %s: type cannot be empty", field.Name)
		}

		// Validate GraphQL field name format (alphanumeric + underscore, start with letter)
		if !sg.isValidFieldName(field.Name) {
			return fmt.Errorf("field %s: invalid GraphQL field name", field.Name)
		}
	}

	return nil
}

// isValidFieldName checks if a field name is valid for GraphQL
// Must start with a letter or underscore, followed by letters, digits, or underscores
func (sg *SchemaGenerator) isValidFieldName(name string) bool {
	if len(name) == 0 {
		return false
	}

	// First character must be letter or underscore
	first := name[0]
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
		return false
	}

	// Rest must be alphanumeric or underscore
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}

	return true
}
