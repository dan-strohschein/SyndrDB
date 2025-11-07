package mutations

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"

	"github.com/vektah/gqlparser/v2/ast"
	"go.uber.org/zap"
)

// MutationParser handles parsing of GraphQL mutations into SyndrDB command structures.
// It converts GraphQL mutation AST nodes into DocumentCommand structs that can be
// executed by the BundleService layer.
//
// This parser follows the same pattern as query parsing but targets document mutation
// operations (create, update, delete) rather than read operations.
type MutationParser struct {
	logger *zap.SugaredLogger
}

// NewMutationParser creates a new mutation parser instance.
func NewMutationParser(logger *zap.SugaredLogger) *MutationParser {
	return &MutationParser{
		logger: logger,
	}
}

// ParseCreateMutation parses a GraphQL create mutation into a DocumentCommand.
// Supports mutations like:
//   createUser(input: { name: "Alice", email: "alice@example.com" }) { id name }
//
// The parser extracts:
// - Bundle name from mutation field name (createUser → User bundle)
// - Input fields from the input argument
// - Selection set for response formatting
func (p *MutationParser) ParseCreateMutation(field *ast.Field, bundleName string, variables map[string]interface{}) (*models.DocumentCommand, error) {
	p.logger.Debugf("Parsing create mutation for bundle '%s'", bundleName)

	// Extract input argument
	input, err := p.extractInputArgument(field, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to extract input for create mutation: %w", err)
	}

	// Validate input is a map
	inputMap, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("input must be an object, got %T", input)
	}

	// Convert input map to field key-value pairs
	fields := make([]models.KeyValue, 0, len(inputMap))
	for key, value := range inputMap {
		fields = append(fields, models.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	// Create DocumentCommand
	docCommand := &models.DocumentCommand{
		CommandType: "ADD_DOCUMENT",
		BundleName:  bundleName,
		Fields:      fields,
	}

	p.logger.Debugf("Parsed create mutation: bundle=%s, fields=%d", bundleName, len(fields))
	return docCommand, nil
}

// ParseUpdateMutation parses a GraphQL update mutation into a DocumentUpdateCommand.
// Supports mutations like:
//   updateUser(id: "123", input: { name: "Bob" }) { id name }
//
// The parser extracts:
// - Document ID from the id argument
// - Update fields from the input argument
// - WHERE clause is constructed from the ID
func (p *MutationParser) ParseUpdateMutation(field *ast.Field, bundleName string, variables map[string]interface{}) (*models.DocumentUpdateCommand, error) {
	p.logger.Debugf("Parsing update mutation for bundle '%s'", bundleName)

	// Extract ID argument
	documentID, err := p.extractIDArgument(field, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to extract id for update mutation: %w", err)
	}

	// Extract input argument
	input, err := p.extractInputArgument(field, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to extract input for update mutation: %w", err)
	}

	// Validate input is a map
	inputMap, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("input must be an object, got %T", input)
	}

	// Convert input map to field key-value pairs
	fields := make([]models.KeyValue, 0, len(inputMap))
	for key, value := range inputMap {
		fields = append(fields, models.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	// Construct WHERE clause targeting the specific document
	whereClause := fmt.Sprintf("DocumentID = '%s'", documentID)

	// Create DocumentUpdateCommand
	updateCommand := &models.DocumentUpdateCommand{
		BundleName:  bundleName,
		Fields:      fields,
		WhereClause: whereClause,
	}

	p.logger.Debugf("Parsed update mutation: bundle=%s, id=%s, fields=%d", bundleName, documentID, len(fields))
	return updateCommand, nil
}

// ParseDeleteMutation parses a GraphQL delete mutation into a DocumentDeleteCommand.
// Supports mutations like:
//   deleteUser(id: "123")
//
// The parser extracts:
// - Document ID from the id argument
// - WHERE clause is constructed from the ID
func (p *MutationParser) ParseDeleteMutation(field *ast.Field, bundleName string, variables map[string]interface{}) (*models.DocumentDeleteCommand, error) {
	p.logger.Debugf("Parsing delete mutation for bundle '%s'", bundleName)

	// Extract ID argument
	documentID, err := p.extractIDArgument(field, variables)
	if err != nil {
		return nil, fmt.Errorf("failed to extract id for delete mutation: %w", err)
	}

	// Construct WHERE clause targeting the specific document
	whereClause := fmt.Sprintf("DocumentID = '%s'", documentID)

	// Create DocumentDeleteCommand
	deleteCommand := &models.DocumentDeleteCommand{
		BundleName:  bundleName,
		WhereClause: whereClause,
	}

	p.logger.Debugf("Parsed delete mutation: bundle=%s, id=%s", bundleName, documentID)
	return deleteCommand, nil
}

// extractInputArgument extracts the 'input' argument from a mutation field.
// Handles both literal values and variables.
func (p *MutationParser) extractInputArgument(field *ast.Field, variables map[string]interface{}) (interface{}, error) {
	if field.Arguments == nil {
		return nil, fmt.Errorf("mutation requires an 'input' argument")
	}

	for _, arg := range field.Arguments {
		if arg.Name == "input" {
			return p.resolveArgumentValue(arg.Value, variables), nil
		}
	}

	return nil, fmt.Errorf("mutation requires an 'input' argument")
}

// extractIDArgument extracts the 'id' argument from a mutation field.
// Handles both literal values and variables.
func (p *MutationParser) extractIDArgument(field *ast.Field, variables map[string]interface{}) (string, error) {
	if field.Arguments == nil {
		return "", fmt.Errorf("mutation requires an 'id' argument")
	}

	for _, arg := range field.Arguments {
		if arg.Name == "id" {
			value := p.resolveArgumentValue(arg.Value, variables)
			if strValue, ok := value.(string); ok {
				return strValue, nil
			}
			return fmt.Sprintf("%v", value), nil
		}
	}

	return "", fmt.Errorf("mutation requires an 'id' argument")
}

// resolveArgumentValue resolves an AST value to its actual value.
// Handles variables, literals, objects, and arrays recursively.
// This follows the same pattern as the query parser's resolveArgumentValue.
func (p *MutationParser) resolveArgumentValue(value *ast.Value, variables map[string]interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch value.Kind {
	case ast.Variable:
		// Resolve variable from variables map
		if varValue, ok := variables[value.Raw]; ok {
			return varValue
		}
		p.logger.Warnf("Variable '$%s' not found in variables map", value.Raw)
		return nil

	case ast.IntValue:
		// Return integer value
		return value.Raw

	case ast.FloatValue:
		// Return float value
		return value.Raw

	case ast.StringValue:
		// Return string value (already unquoted by parser)
		return value.Raw

	case ast.BooleanValue:
		// Return boolean value
		return value.Raw == "true"

	case ast.NullValue:
		// Return nil for null
		return nil

	case ast.ListValue:
		// Recursively resolve list elements
		result := make([]interface{}, 0, len(value.Children))
		for _, child := range value.Children {
			result = append(result, p.resolveArgumentValue(child.Value, variables))
		}
		return result

	case ast.ObjectValue:
		// Recursively resolve object fields
		result := make(map[string]interface{}, len(value.Children))
		for _, field := range value.Children {
			// field.Name is the key, field.Value is the value
			if field.Name != "" {
				result[field.Name] = p.resolveArgumentValue(field.Value, variables)
			}
		}
		return result

	default:
		p.logger.Warnf("Unknown value kind: %v", value.Kind)
		return value.Raw
	}
}

// ExtractBundleNameFromMutation extracts the bundle name from a mutation field name.
// Conventions:
//   createUser → User
//   updatePost → Post
//   deleteComment → Comment
//
// This handles both singular and plural forms, and converts to proper case.
func (p *MutationParser) ExtractBundleNameFromMutation(mutationName string) (string, error) {
	// Remove mutation prefix (create, update, delete)
	var bundleName string

	if strings.HasPrefix(mutationName, "create") {
		bundleName = strings.TrimPrefix(mutationName, "create")
	} else if strings.HasPrefix(mutationName, "update") {
		bundleName = strings.TrimPrefix(mutationName, "update")
	} else if strings.HasPrefix(mutationName, "delete") {
		bundleName = strings.TrimPrefix(mutationName, "delete")
	} else {
		// TODO: I will add support for custom mutations that don't follow the create/update/delete pattern.
		// Custom mutations would be registered with a mutation registry and could have arbitrary names.
		// For example: publishPost, archiveComment, approveUser, etc.
		// The registry would map mutation names to handler functions.
		return "", fmt.Errorf("unknown mutation pattern: %s (expected create*, update*, or delete*)", mutationName)
	}

	// Validate bundle name is not empty
	if bundleName == "" {
		return "", fmt.Errorf("invalid mutation name: %s (missing entity name)", mutationName)
	}

	// Capitalize first letter (GraphQL uses PascalCase for types)
	// But SyndrDB bundles might use different casing
	// For now, preserve the casing as-is after the prefix
	p.logger.Debugf("Extracted bundle name '%s' from mutation '%s'", bundleName, mutationName)
	return bundleName, nil
}

// ParseBatchCreateMutation parses a batch create mutation.
// TODO: I will implement batch mutation support when SyndrDB adds batch operation capabilities.
// Batch mutations would allow creating multiple documents in a single operation:
//   createUsers(inputs: [
//     { name: "Alice", email: "alice@..." }
//     { name: "Bob", email: "bob@..." }
//   ]) {
//     success
//     count
//     users { id name }
//   }
//
// Implementation would:
// - Parse array of input objects
// - Execute batch insert via BundleService (when available)
// - Return aggregate results with individual document IDs
// - Handle partial failures with detailed error reporting
func (p *MutationParser) ParseBatchCreateMutation(field *ast.Field, bundleName string, variables map[string]interface{}) error {
	return fmt.Errorf("batch mutations not yet supported - requires SyndrDB batch operation implementation")
}
