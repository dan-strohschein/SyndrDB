package main

import (
	"testing"

	"syndrdb/src/internal/graphQL/mutations"

	"github.com/stretchr/testify/assert"
	"github.com/vektah/gqlparser/v2/ast"
	"go.uber.org/zap"
)

// Import mutations types
type InputValidator = mutations.InputValidator

var NewInputValidator = mutations.NewInputValidator

func TestNewInputValidator(t *testing.T) {
	logger := zap.NewNop().Sugar()
	validator := NewInputValidator(logger)

	assert.NotNil(t, validator)
	// Note: validator.logger is unexported and cannot be tested directly
}

func TestValidateCreateMutation(t *testing.T) {
	validator := NewInputValidator(zap.NewNop().Sugar())

	t.Run("Valid create mutation", func(t *testing.T) {
		field := &ast.Field{
			Name: "createUser",
			Arguments: ast.ArgumentList{
				{
					Name: "input",
					Value: &ast.Value{
						Kind: ast.ObjectValue,
						Children: ast.ChildValueList{
							{
								Name: "name",
								Value: &ast.Value{
									Kind: ast.StringValue,
									Raw:  "Alice",
								},
							},
						},
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
				&ast.Field{Name: "name"},
			},
		}

		err := validator.ValidateCreateMutation(field)
		assert.NoError(t, err)
	})

	t.Run("Missing input argument", func(t *testing.T) {
		field := &ast.Field{
			Name:      "createUser",
			Arguments: ast.ArgumentList{},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
			},
		}

		err := validator.ValidateCreateMutation(field)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "input")
	})

	t.Run("Input not an object", func(t *testing.T) {
		field := &ast.Field{
			Name: "createUser",
			Arguments: ast.ArgumentList{
				{
					Name: "input",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "invalid",
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
			},
		}

		err := validator.ValidateCreateMutation(field)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be an object")
	})

	t.Run("Empty input object", func(t *testing.T) {
		field := &ast.Field{
			Name: "createUser",
			Arguments: ast.ArgumentList{
				{
					Name: "input",
					Value: &ast.Value{
						Kind:     ast.ObjectValue,
						Children: ast.ChildValueList{},
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
			},
		}

		err := validator.ValidateCreateMutation(field)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})

}

// Note: SelectionSet validation is tested separately via ValidateSelectionSet method

func TestValidateUpdateMutation(t *testing.T) {
	validator := NewInputValidator(zap.NewNop().Sugar())

	t.Run("Valid update mutation", func(t *testing.T) {
		field := &ast.Field{
			Name: "updateUser",
			Arguments: ast.ArgumentList{
				{
					Name: "id",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "user123",
					},
				},
				{
					Name: "input",
					Value: &ast.Value{
						Kind: ast.ObjectValue,
						Children: ast.ChildValueList{
							{
								Name: "name",
								Value: &ast.Value{
									Kind: ast.StringValue,
									Raw:  "Updated Name",
								},
							},
						},
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
				&ast.Field{Name: "name"},
			},
		}

		err := validator.ValidateUpdateMutation(field)
		assert.NoError(t, err)
	})

	t.Run("Missing id argument", func(t *testing.T) {
		field := &ast.Field{
			Name: "updateUser",
			Arguments: ast.ArgumentList{
				{
					Name: "input",
					Value: &ast.Value{
						Kind:     ast.ObjectValue,
						Children: ast.ChildValueList{{Name: "name", Value: &ast.Value{Kind: ast.StringValue, Raw: "Alice"}}},
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
			},
		}

		err := validator.ValidateUpdateMutation(field)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "id")
	})

	t.Run("Empty id", func(t *testing.T) {
		field := &ast.Field{
			Name: "updateUser",
			Arguments: ast.ArgumentList{
				{
					Name: "id",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "",
					},
				},
				{
					Name: "input",
					Value: &ast.Value{
						Kind:     ast.ObjectValue,
						Children: ast.ChildValueList{{Name: "name", Value: &ast.Value{Kind: ast.StringValue, Raw: "Alice"}}},
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
			},
		}

		err := validator.ValidateUpdateMutation(field)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "id cannot be empty")
	})

	t.Run("Missing input argument", func(t *testing.T) {
		field := &ast.Field{
			Name: "updateUser",
			Arguments: ast.ArgumentList{
				{
					Name: "id",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "user123",
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
			},
		}

		err := validator.ValidateUpdateMutation(field)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "input")
	})

	t.Run("Empty input object", func(t *testing.T) {
		field := &ast.Field{
			Name: "updateUser",
			Arguments: ast.ArgumentList{
				{
					Name: "id",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "user123",
					},
				},
				{
					Name: "input",
					Value: &ast.Value{
						Kind:     ast.ObjectValue,
						Children: ast.ChildValueList{},
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "id"},
			},
		}

		err := validator.ValidateUpdateMutation(field)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})
}

func TestValidateDeleteMutation(t *testing.T) {
	validator := NewInputValidator(zap.NewNop().Sugar())

	t.Run("Valid delete mutation", func(t *testing.T) {
		field := &ast.Field{
			Name: "deleteUser",
			Arguments: ast.ArgumentList{
				{
					Name: "id",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "user123",
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "success"},
				&ast.Field{Name: "deletedId"},
			},
		}

		err := validator.ValidateDeleteMutation(field)
		assert.NoError(t, err)
	})

	t.Run("Missing id argument", func(t *testing.T) {
		field := &ast.Field{
			Name:      "deleteUser",
			Arguments: ast.ArgumentList{},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "success"},
			},
		}

		err := validator.ValidateDeleteMutation(field)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "id")
	})

	t.Run("Empty id", func(t *testing.T) {
		field := &ast.Field{
			Name: "deleteUser",
			Arguments: ast.ArgumentList{
				{
					Name: "id",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "",
					},
				},
			},
			SelectionSet: ast.SelectionSet{
				&ast.Field{Name: "success"},
			},
		}

		err := validator.ValidateDeleteMutation(field)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "id cannot be empty")
	})

}

func TestValidateMutationName(t *testing.T) {
	validator := NewInputValidator(zap.NewNop().Sugar())

	tests := []struct {
		name          string
		mutationName  string
		expectedValid bool
	}{
		{
			name:          "Valid create mutation",
			mutationName:  "createUser",
			expectedValid: true,
		},
		{
			name:          "Valid update mutation",
			mutationName:  "updateUser",
			expectedValid: true,
		},
		{
			name:          "Valid delete mutation",
			mutationName:  "deleteUser",
			expectedValid: true,
		},
		{
			name:          "Invalid prefix",
			mutationName:  "invalidUser",
			expectedValid: false,
		},
		{
			name:          "Too short",
			mutationName:  "create",
			expectedValid: false,
		},
		{
			name:          "Empty string",
			mutationName:  "",
			expectedValid: false,
		},
		{
			name:          "Multi-word bundle",
			mutationName:  "createUserProfile",
			expectedValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateMutationName(tt.mutationName)

			if tt.expectedValid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestValidateSelectionSet(t *testing.T) {
	validator := NewInputValidator(zap.NewNop().Sugar())

	t.Run("Valid selection set", func(t *testing.T) {
		selectionSet := ast.SelectionSet{
			&ast.Field{Name: "id"},
			&ast.Field{Name: "name"},
		}

		err := validator.ValidateSelectionSet(selectionSet)
		assert.NoError(t, err)
	})

	t.Run("Empty selection set", func(t *testing.T) {
		selectionSet := ast.SelectionSet{}

		err := validator.ValidateSelectionSet(selectionSet)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must specify fields to return")
	})

	t.Run("Nil selection set", func(t *testing.T) {
		var selectionSet ast.SelectionSet

		err := validator.ValidateSelectionSet(selectionSet)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must specify fields to return")
	})
}

func TestValidateInputObject(t *testing.T) {
	validator := NewInputValidator(zap.NewNop().Sugar())

	t.Run("Valid object", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.ObjectValue,
			Children: ast.ChildValueList{
				{
					Name: "name",
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "Alice",
					},
				},
			},
		}

		err := validator.ValidateInputObject(value)
		assert.NoError(t, err)
	})

	t.Run("Not an object", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.StringValue,
			Raw:  "not an object",
		}

		err := validator.ValidateInputObject(value)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be an object")
	})

	t.Run("Empty object", func(t *testing.T) {
		value := &ast.Value{
			Kind:     ast.ObjectValue,
			Children: ast.ChildValueList{},
		}

		err := validator.ValidateInputObject(value)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})

	t.Run("Nil value", func(t *testing.T) {
		err := validator.ValidateInputObject(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "input value is nil")
	})
}
