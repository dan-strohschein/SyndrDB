package mutations

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/graphQL/schema"

	"go.uber.org/zap"
)

// MutationGenerator auto-generates CRUD mutation schemas for SyndrDB bundles.
// This follows the Open/Closed Principle - the generator is open for extension
// (custom mutations can be added) but closed for modification (core logic is stable).
//
// Generated mutations follow GraphQL best practices:
// - Input types for create/update operations
// - ID-based targeting for update/delete operations
// - Consistent naming: create<Bundle>, update<Bundle>, delete<Bundle>
// - Return types match bundle types (for create/update) or Boolean (for delete)
//
// Design Philosophy:
// - Auto-generate from bundle schemas (no manual schema writing)
// - Consistent with query generation (same type system)
// - Extensible for custom mutations
type MutationGenerator struct {
	logger *zap.SugaredLogger
}

// NewMutationGenerator creates a new mutation schema generator.
func NewMutationGenerator(logger *zap.SugaredLogger) *MutationGenerator {
	return &MutationGenerator{
		logger: logger,
	}
}

// GenerateMutationSchema generates the complete Mutation type for a database.
// This includes CRUD mutations for all bundles in the database.
//
// Generated schema format:
//   type Mutation {
//     createUser(input: CreateUserInput!): User!
//     updateUser(id: ID!, input: UpdateUserInput!): User!
//     deleteUser(id: ID!): DeleteUserPayload!
//
//     createPost(input: CreatePostInput!): Post!
//     ...
//   }
//
// Returns GraphQL schema string to be included in the overall schema.
func (g *MutationGenerator) GenerateMutationSchema(database *models.Database, schemaManager *schema.SchemaManager) string {
	var mutationSchema strings.Builder

	mutationSchema.WriteString("\ttype Mutation {\n")

	// Generate CRUD mutations for each bundle
	for bundleName := range database.Bundles {
		// Get bundle schema for type information
		bundleSchema, err := g.getBundleSchema(database.Name, bundleName, schemaManager)
		if err != nil {
			g.logger.Warnf("Failed to get schema for bundle '%s': %v - skipping mutation generation", bundleName, err)
			continue
		}

		typeName := bundleSchema.Payload.TypeName

		// Generate CREATE mutation
		mutationSchema.WriteString(fmt.Sprintf("\t\t# Create a new %s\n", bundleName))
		mutationSchema.WriteString(fmt.Sprintf("\t\tcreate%s(input: Create%sInput!): %s!\n\n", bundleName, bundleName, typeName))

		// Generate UPDATE mutation
		mutationSchema.WriteString(fmt.Sprintf("\t\t# Update an existing %s\n", bundleName))
		mutationSchema.WriteString(fmt.Sprintf("\t\tupdate%s(id: ID!, input: Update%sInput!): %s!\n\n", bundleName, bundleName, typeName))

		// Generate DELETE mutation
		mutationSchema.WriteString(fmt.Sprintf("\t\t# Delete a %s\n", bundleName))
		mutationSchema.WriteString(fmt.Sprintf("\t\tdelete%s(id: ID!): Delete%sPayload!\n\n", bundleName, bundleName))

		// TODO: I will add batch mutation generation when SyndrDB supports batch operations.
		// Batch mutations would be generated like:
		// createUsers(inputs: [CreateUserInput!]!): CreateUsersPayload!
		// updateUsers(inputs: [UpdateUserInput!]!): UpdateUsersPayload!
		// deleteUsers(ids: [ID!]!): DeleteUsersPayload!
		// The payload types would include success/failure counts and error details.
	}

	mutationSchema.WriteString("\t}\n\n")

	return mutationSchema.String()
}

// GenerateInputTypes generates Input types for create and update mutations.
// Input types are derived from bundle field definitions.
//
// Generated format:
//   input CreateUserInput {
//     name: String!
//     email: String!
//     age: Int
//   }
//
//   input UpdateUserInput {
//     name: String
//     email: String
//     age: Int
//   }
//
// Key differences:
// - CreateInput: Required fields match bundle's required fields
// - UpdateInput: All fields are optional (partial updates)
// - DocumentID is never included (auto-generated or specified separately)
func (g *MutationGenerator) GenerateInputTypes(database *models.Database, schemaManager *schema.SchemaManager) string {
	var inputTypes strings.Builder

	for bundleName, bundle := range database.Bundles {
		// Generate CreateInput type (mirrors bundle fields, respects required constraints)
		inputTypes.WriteString(fmt.Sprintf("\t# Input type for creating a %s\n", bundleName))
		inputTypes.WriteString(fmt.Sprintf("\tinput Create%sInput {\n", bundleName))

		for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
			// Skip DocumentID (auto-generated)
			if fieldName == "DocumentID" {
				continue
			}

			graphqlType := g.mapFieldTypeToGraphQL(fieldDef.Type)
			requiredMarker := ""
			if fieldDef.IsRequired {
				requiredMarker = "!"
			}

			inputTypes.WriteString(fmt.Sprintf("\t\t%s: %s%s\n", fieldName, graphqlType, requiredMarker))
		}

		inputTypes.WriteString("\t}\n\n")

		// Generate UpdateInput type (all fields optional for partial updates)
		inputTypes.WriteString(fmt.Sprintf("\t# Input type for updating a %s\n", bundleName))
		inputTypes.WriteString(fmt.Sprintf("\tinput Update%sInput {\n", bundleName))

		for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
			// Skip DocumentID (specified separately as id argument)
			if fieldName == "DocumentID" {
				continue
			}

			graphqlType := g.mapFieldTypeToGraphQL(fieldDef.Type)
			// All fields optional in update (partial updates)
			inputTypes.WriteString(fmt.Sprintf("\t\t%s: %s\n", fieldName, graphqlType))
		}

		inputTypes.WriteString("\t}\n\n")
	}

	return inputTypes.String()
}

// GenerateDeletePayloadTypes generates payload types for delete mutations.
// Delete mutations return metadata rather than the deleted object.
//
// Generated format:
//   type DeleteUserPayload {
//     success: Boolean!
//     deletedId: ID!
//     message: String
//   }
func (g *MutationGenerator) GenerateDeletePayloadTypes(database *models.Database) string {
	var payloadTypes strings.Builder

	for bundleName := range database.Bundles {
		payloadTypes.WriteString(fmt.Sprintf("\t# Payload type for delete%s mutation\n", bundleName))
		payloadTypes.WriteString(fmt.Sprintf("\ttype Delete%sPayload {\n", bundleName))
		payloadTypes.WriteString("\t\tsuccess: Boolean!\n")
		payloadTypes.WriteString("\t\tdeletedId: ID!\n")
		payloadTypes.WriteString("\t\tmessage: String\n")
		payloadTypes.WriteString("\t}\n\n")
	}

	return payloadTypes.String()
}

// mapFieldTypeToGraphQL maps SyndrDB field types to GraphQL types.
// This ensures type consistency between queries and mutations.
func (g *MutationGenerator) mapFieldTypeToGraphQL(syndrType string) string {
	switch strings.ToUpper(syndrType) {
	case "STRING", "TEXT":
		return "String"
	case "INT", "INTEGER":
		return "Int"
	case "FLOAT", "DOUBLE", "DECIMAL":
		return "Float"
	case "BOOL", "BOOLEAN":
		return "Boolean"
	case "DATE", "DATETIME", "TIMESTAMP":
		// TODO: I will add custom scalar types (DateTime, Date, Time) when implementing
		// advanced GraphQL features. For now, dates are represented as String.
		// Custom scalars would provide:
		// - Proper serialization/deserialization
		// - Validation of date formats
		// - Timezone handling
		return "String"
	case "JSON", "OBJECT":
		// TODO: I will add JSON scalar type when implementing advanced features.
		// JSON scalar would allow passing arbitrary JSON objects without defining
		// explicit types. Useful for flexible/dynamic schemas.
		return "String"
	case "ARRAY", "LIST":
		// TODO: I will add proper array type handling when implementing advanced features.
		// Arrays in GraphQL require specifying the element type: [String], [Int], etc.
		// For now, arrays are represented as String (JSON-encoded).
		return "String"
	default:
		// Unknown type - default to String for safety
		g.logger.Warnf("Unknown field type '%s', defaulting to String", syndrType)
		return "String"
	}
}

// getBundleSchema retrieves the GraphQL schema for a bundle from the SchemaManager.
func (g *MutationGenerator) getBundleSchema(databaseName, bundleName string, schemaManager *schema.SchemaManager) (*schema.SchemaRecord, error) {
	if schemaManager == nil {
		return nil, fmt.Errorf("schema manager is nil")
	}

	// Load active schema from SchemaManager cache
	graphqlSchema, err := schemaManager.GetCachedSchema(bundleName)
	if err != nil {
		return nil, fmt.Errorf("failed to load schema: %w", err)
	}

	return graphqlSchema, nil
}

// GenerateCustomMutationField generates a schema field for a custom mutation.
// TODO: I will implement this when adding support for custom business logic mutations.
// Custom mutations allow defining arbitrary operations beyond CRUD:
//
// Examples:
//   publishPost(id: ID!): Post!
//   archiveComment(id: ID!, reason: String): Comment!
//   approveUser(id: ID!, approvalNotes: String): User!
//   sendEmail(userId: ID!, template: String!, vars: JSON): EmailPayload!
//
// Implementation would require:
// - Mutation registry to map names to handler functions
// - Custom input types for complex parameters
// - Custom payload types for rich responses
// - Handler functions that can access services and execute business logic
//
// Custom mutations would be registered like:
//   mutationRegistry.Register("publishPost", publishPostHandler)
//
// The handler would receive:
// - Context (user, database, permissions)
// - Parsed arguments
// - ServiceManager for executing operations
//
// This provides escape hatch for operations that don't fit CRUD pattern.
func (g *MutationGenerator) GenerateCustomMutationField(name string, inputType string, returnType string) string {
	return fmt.Sprintf("\t\t%s(input: %s!): %s!\n", name, inputType, returnType)
}

// ValidateMutationSchema validates that generated mutation schema is well-formed.
// TODO: I will implement schema validation when adding comprehensive error handling.
// Validation would check:
// - All referenced types exist
// - Input types match bundle structures
// - No naming conflicts
// - Required fields are properly marked
// - Custom mutations have valid signatures
func (g *MutationGenerator) ValidateMutationSchema(mutationSchema string) error {
	// Basic validation - check schema is not empty
	if mutationSchema == "" {
		return fmt.Errorf("generated mutation schema is empty")
	}

	// TODO: I will add comprehensive validation using gqlparser to parse and validate
	// the generated schema against GraphQL spec. This would catch syntax errors,
	// type mismatches, and other schema issues before runtime.

	return nil
}
