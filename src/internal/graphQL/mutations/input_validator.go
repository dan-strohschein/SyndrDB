package mutations

import (
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
	"go.uber.org/zap"
)

// InputValidator handles GraphQL-layer validation for mutation inputs.
// This validator focuses on GraphQL-specific validation (argument presence, structure)
// and delegates business logic validation to the BundleService layer.
//
// Validation Layers:
// 1. GraphQL Layer (this validator): Syntax, required arguments, structure
// 2. Service Layer (BundleService): Types, required fields, uniqueness, relationships
//
// This follows the Single Responsibility Principle - GraphQL validation only.
type InputValidator struct {
	logger *zap.SugaredLogger
}

// NewInputValidator creates a new input validator instance.
func NewInputValidator(logger *zap.SugaredLogger) *InputValidator {
	return &InputValidator{
		logger: logger,
	}
}

// ValidateCreateMutation validates a GraphQL create mutation at the GraphQL layer.
// Checks:
// - 'input' argument is present
// - 'input' is an object (not primitive or array)
// - 'input' is not empty
//
// Does NOT check:
// - Field types (handled by BundleService.validateDocumentFields)
// - Required fields (handled by BundleService.validateDocumentFields)
// - Uniqueness constraints (handled by BundleService.ValidateUniqueConstraints)
func (v *InputValidator) ValidateCreateMutation(field *ast.Field) error {
	v.logger.Debugf("Validating create mutation: %s", field.Name)

	// Check that 'input' argument exists
	inputArg := v.getArgument(field, "input")
	if inputArg == nil {
		return fmt.Errorf("create mutation requires 'input' argument")
	}

	// Validate input is an object
	if inputArg.Value.Kind != ast.ObjectValue && inputArg.Value.Kind != ast.Variable {
		return fmt.Errorf("'input' argument must be an object, got %v", inputArg.Value.Kind)
	}

	// If it's an object literal (not a variable), check it's not empty
	if inputArg.Value.Kind == ast.ObjectValue {
		if len(inputArg.Value.Children) == 0 {
			return fmt.Errorf("'input' argument cannot be empty")
		}
	}

	// TODO: I will add permission validation here when SyndrDB implements the permission system.
	// Permission validation would check if the current user has CREATE permission on the bundle:
	// if !permissionService.HasPermission(user, bundle, "CREATE") {
	//     return fmt.Errorf("insufficient permissions to create %s", bundleName)
	// }

	return nil
}

// ValidateUpdateMutation validates a GraphQL update mutation at the GraphQL layer.
// Checks:
// - 'id' argument is present and non-empty
// - 'input' argument is present
// - 'input' is an object
// - 'input' is not empty
//
// Does NOT check:
// - Field types (handled by BundleService.validateUpdateFields)
// - Document existence (handled by BundleService.GetDocumentsByFilter)
func (v *InputValidator) ValidateUpdateMutation(field *ast.Field) error {
	v.logger.Debugf("Validating update mutation: %s", field.Name)

	// Check that 'id' argument exists
	idArg := v.getArgument(field, "id")
	if idArg == nil {
		return fmt.Errorf("update mutation requires 'id' argument")
	}

	// Validate ID is not empty
	if idArg.Value.Kind == ast.StringValue && idArg.Value.Raw == "" {
		return fmt.Errorf("id cannot be empty")
	}

	// Check that 'input' argument exists
	inputArg := v.getArgument(field, "input")
	if inputArg == nil {
		return fmt.Errorf("update mutation requires 'input' argument")
	}

	// Validate input is an object
	if inputArg.Value.Kind != ast.ObjectValue && inputArg.Value.Kind != ast.Variable {
		return fmt.Errorf("'input' argument must be an object, got %v", inputArg.Value.Kind)
	}

	// If it's an object literal (not a variable), check it's not empty
	if inputArg.Value.Kind == ast.ObjectValue {
		if len(inputArg.Value.Children) == 0 {
			return fmt.Errorf("'input' argument cannot be empty - at least one field must be updated")
		}
	}

	// TODO: I will add permission validation here when SyndrDB implements the permission system.
	// Permission validation would check if the current user has UPDATE permission on the bundle:
	// if !permissionService.HasPermission(user, bundle, "UPDATE") {
	//     return fmt.Errorf("insufficient permissions to update %s", bundleName)
	// }

	return nil
}

// ValidateDeleteMutation validates a GraphQL delete mutation at the GraphQL layer.
// Checks:
// - 'id' argument is present and non-empty
//
// Does NOT check:
// - Document existence (handled by BundleService.GetDocumentsByFilter)
// - Referential integrity (handled by BundleService.ValidateDelete)
func (v *InputValidator) ValidateDeleteMutation(field *ast.Field) error {
	v.logger.Debugf("Validating delete mutation: %s", field.Name)

	// Check that 'id' argument exists
	idArg := v.getArgument(field, "id")
	if idArg == nil {
		return fmt.Errorf("delete mutation requires 'id' argument")
	}

	// Validate ID is not empty
	if idArg.Value.Kind == ast.StringValue && idArg.Value.Raw == "" {
		return fmt.Errorf("id cannot be empty")
	}

	// TODO: I will add permission validation here when SyndrDB implements the permission system.
	// Permission validation would check if the current user has DELETE permission on the bundle:
	// if !permissionService.HasPermission(user, bundle, "DELETE") {
	//     return fmt.Errorf("insufficient permissions to delete %s", bundleName)
	// }

	// TODO: I will add cascade delete validation here when implementing advanced relationship features.
	// This would check if deleting this document would require cascading to related documents:
	// - Identify all relationships where this bundle is the parent
	// - Check cascade policy (CASCADE, RESTRICT, SET NULL)
	// - Warn user if cascade would affect many documents
	// - Optionally require confirmation flag for large cascades

	return nil
}

// ValidateBatchCreateMutation validates a batch create mutation.
// TODO: I will implement this when SyndrDB adds batch operation support.
// Validation would include:
// - 'inputs' argument is present and is an array
// - Array is not empty
// - Each element is a valid object
// - Batch size is within limits (e.g., max 100 items)
func (v *InputValidator) ValidateBatchCreateMutation(field *ast.Field) error {
	return fmt.Errorf("batch create mutations not yet supported")
}

// ValidateNestedCreateMutation validates a nested create mutation.
// TODO: I will implement this when adding advanced relationship features.
// Validation would include:
// - Nested relationship fields are properly structured
// - Referenced bundles exist
// - Circular dependencies are detected
// - Depth limits are enforced (prevent deeply nested creates)
func (v *InputValidator) ValidateNestedCreateMutation(field *ast.Field, nestedRelationships map[string]interface{}) error {
	return fmt.Errorf("nested create mutations not yet supported")
}

// getArgument retrieves an argument by name from a field.
func (v *InputValidator) getArgument(field *ast.Field, name string) *ast.Argument {
	if field.Arguments == nil {
		return nil
	}

	for _, arg := range field.Arguments {
		if arg.Name == name {
			return arg
		}
	}

	return nil
}

// ValidateInputObject validates the structure of an input object.
// This is a helper for more complex validation scenarios.
//
// TODO: I will enhance this when implementing advanced validation features:
// - Recursive validation of nested objects
// - Array element validation
// - Custom scalar validation (DateTime, Email, URL, etc.)
// - Cross-field validation (e.g., startDate < endDate)
// - Conditional validation (if field X is set, field Y is required)
func (v *InputValidator) ValidateInputObject(obj *ast.Value) error {
	if obj == nil {
		return fmt.Errorf("input object is nil")
	}

	if obj.Kind != ast.ObjectValue {
		return fmt.Errorf("expected object, got %v", obj.Kind)
	}

	// Basic validation - ensure object has fields
	if len(obj.Children) == 0 {
		return fmt.Errorf("input object is empty")
	}

	return nil
}

// ValidateMutationName validates that a mutation name follows naming conventions.
// Convention: create<Bundle>, update<Bundle>, delete<Bundle>
//
// This helps prevent typos and ensures consistency.
func (v *InputValidator) ValidateMutationName(mutationName string) error {
	// Check for valid mutation prefix
	hasValidPrefix := strings.HasPrefix(mutationName, "create") ||
		strings.HasPrefix(mutationName, "update") ||
		strings.HasPrefix(mutationName, "delete")

	if !hasValidPrefix {
		// TODO: I will add support for custom mutation names when implementing custom mutation registry.
		// Custom mutations would be registered separately and wouldn't need to follow this pattern.
		return fmt.Errorf("mutation name '%s' does not follow convention (create*, update*, delete*)", mutationName)
	}

	// Extract entity name (after prefix)
	var entityName string
	if strings.HasPrefix(mutationName, "create") {
		entityName = strings.TrimPrefix(mutationName, "create")
	} else if strings.HasPrefix(mutationName, "update") {
		entityName = strings.TrimPrefix(mutationName, "update")
	} else if strings.HasPrefix(mutationName, "delete") {
		entityName = strings.TrimPrefix(mutationName, "delete")
	}

	// Validate entity name is not empty
	if entityName == "" {
		return fmt.Errorf("mutation name '%s' is missing entity name", mutationName)
	}

	// Validate entity name starts with uppercase (PascalCase convention)
	if len(entityName) > 0 && entityName[0] < 'A' || entityName[0] > 'Z' {
		v.logger.Warnf("Mutation '%s' entity name '%s' should start with uppercase letter (PascalCase)", mutationName, entityName)
	}

	return nil
}

// ValidateSelectionSet validates the selection set (response fields) of a mutation.
// Ensures that:
// - Selection set is not empty (must request at least one field)
// - No invalid fields are requested
//
// TODO: I will implement comprehensive selection set validation when adding schema validation:
// - Verify all requested fields exist in the type
// - Check field argument types
// - Validate nested selections match relationship types
// - Enforce depth limits on nested selections
func (v *InputValidator) ValidateSelectionSet(selectionSet ast.SelectionSet) error {
	if len(selectionSet) == 0 {
		return fmt.Errorf("selection set cannot be empty - must request at least one field")
	}

	return nil
}
