package mutations

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"go.uber.org/zap"
)

func TestNewMutationParser(t *testing.T) {
	logger := zap.NewNop().Sugar()
	parser := NewMutationParser(logger)

	assert.NotNil(t, parser)
	assert.NotNil(t, parser.logger)
}

func TestExtractBundleNameFromMutation(t *testing.T) {
	parser := NewMutationParser(zap.NewNop().Sugar())

	tests := []struct {
		name           string
		mutationName   string
		expectedBundle string
		expectedError  bool
	}{
		{
			name:           "Create mutation",
			mutationName:   "createUser",
			expectedBundle: "User",
			expectedError:  false,
		},
		{
			name:           "Update mutation",
			mutationName:   "updateUser",
			expectedBundle: "User",
			expectedError:  false,
		},
		{
			name:           "Delete mutation",
			mutationName:   "deleteUser",
			expectedBundle: "User",
			expectedError:  false,
		},
		{
			name:           "Multi-word bundle name",
			mutationName:   "createUserProfile",
			expectedBundle: "UserProfile",
			expectedError:  false,
		},
		{
			name:           "Invalid mutation name",
			mutationName:   "invalidMutation",
			expectedBundle: "",
			expectedError:  true,
		},
		{
			name:           "Too short",
			mutationName:   "create",
			expectedBundle: "",
			expectedError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bundleName, err := parser.ExtractBundleNameFromMutation(tt.mutationName)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedBundle, bundleName)
			}
		})
	}
}

func TestParseCreateMutation(t *testing.T) {
	parser := NewMutationParser(zap.NewNop().Sugar())

	t.Run("Valid create mutation", func(t *testing.T) {
		// Create AST field for: createUser(input: {name: "Alice", age: 30})
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
							{
								Name: "age",
								Value: &ast.Value{
									Kind: ast.IntValue,
									Raw:  "30",
								},
							},
						},
					},
				},
			},
		}

		docCommand, err := parser.ParseCreateMutation(field, "User", nil)

		require.NoError(t, err)
		assert.NotNil(t, docCommand)
		assert.Equal(t, "User", docCommand.BundleName)
		assert.Equal(t, 2, len(docCommand.Fields))
		// Fields are []KeyValue, so check by iterating
		foundName := false
		foundAge := false
		for _, kv := range docCommand.Fields {
			if kv.Key == "name" {
				assert.Equal(t, "Alice", kv.Value)
				foundName = true
			}
			if kv.Key == "age" {
				assert.Equal(t, "30", kv.Value)
				foundAge = true
			}
		}
		assert.True(t, foundName, "name field not found")
		assert.True(t, foundAge, "age field not found")
	})

	t.Run("Missing input argument", func(t *testing.T) {
		field := &ast.Field{
			Name:      "createUser",
			Arguments: ast.ArgumentList{},
		}

		docCommand, err := parser.ParseCreateMutation(field, "User", nil)

		assert.Error(t, err)
		assert.Nil(t, docCommand)
		assert.Contains(t, err.Error(), "input argument not found")
	})

	t.Run("Input argument not an object", func(t *testing.T) {
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
		}

		docCommand, err := parser.ParseCreateMutation(field, "User", nil)

		assert.Error(t, err)
		assert.Nil(t, docCommand)
		assert.Contains(t, err.Error(), "must be an ObjectValue")
	})

	t.Run("Nested object fields", func(t *testing.T) {
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
							{
								Name: "address",
								Value: &ast.Value{
									Kind: ast.ObjectValue,
									Children: ast.ChildValueList{
										{
											Name: "city",
											Value: &ast.Value{
												Kind: ast.StringValue,
												Raw:  "NYC",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		docCommand, err := parser.ParseCreateMutation(field, "User", nil)

		require.NoError(t, err)
		assert.NotNil(t, docCommand)
		assert.Contains(t, docCommand.Fields, "address")
		// TODO: I'll need to test how nested objects are handled - current implementation
		// TODO: may need adjustment based on how BundleService expects nested data
	})
}

func TestParseUpdateMutation(t *testing.T) {
	parser := NewMutationParser(zap.NewNop().Sugar())

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
		}

		updateCommand, err := parser.ParseUpdateMutation(field, "User", nil)

		require.NoError(t, err)
		assert.NotNil(t, updateCommand)
		assert.Equal(t, "User", updateCommand.BundleName)
		// WhereClause should contain the ID filter
		assert.Contains(t, updateCommand.WhereClause, "user123")
		assert.Equal(t, 1, len(updateCommand.Fields))
		assert.Equal(t, "name", updateCommand.Fields[0].Key)
		assert.Equal(t, "Updated Name", updateCommand.Fields[0].Value)
	})

	t.Run("Missing id argument", func(t *testing.T) {
		field := &ast.Field{
			Name: "updateUser",
			Arguments: ast.ArgumentList{
				{
					Name: "input",
					Value: &ast.Value{
						Kind:     ast.ObjectValue,
						Children: ast.ChildValueList{},
					},
				},
			},
		}

		updateCommand, err := parser.ParseUpdateMutation(field, "User", nil)

		assert.Error(t, err)
		assert.Nil(t, updateCommand)
		assert.Contains(t, err.Error(), "id argument not found")
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
		}

		updateCommand, err := parser.ParseUpdateMutation(field, "User", nil)

		assert.Error(t, err)
		assert.Nil(t, updateCommand)
		assert.Contains(t, err.Error(), "input argument not found")
	})

	t.Run("Empty ID", func(t *testing.T) {
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
						Children: ast.ChildValueList{},
					},
				},
			},
		}

		updateCommand, err := parser.ParseUpdateMutation(field, "User", nil)

		assert.Error(t, err)
		assert.Nil(t, updateCommand)
		assert.Contains(t, err.Error(), "cannot be empty")
	})
}

func TestParseDeleteMutation(t *testing.T) {
	parser := NewMutationParser(zap.NewNop().Sugar())

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
		}

		deleteCommand, err := parser.ParseDeleteMutation(field, "User", nil)

		require.NoError(t, err)
		assert.NotNil(t, deleteCommand)
		assert.Equal(t, "User", deleteCommand.BundleName)
		// WhereClause should contain the ID filter
		assert.Contains(t, deleteCommand.WhereClause, "user123")
	})

	t.Run("Missing id argument", func(t *testing.T) {
		field := &ast.Field{
			Name:      "deleteUser",
			Arguments: ast.ArgumentList{},
		}

		deleteCommand, err := parser.ParseDeleteMutation(field, "User", nil)

		assert.Error(t, err)
		assert.Nil(t, deleteCommand)
		assert.Contains(t, err.Error(), "id argument not found")
	})

	t.Run("Empty ID", func(t *testing.T) {
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
		}

		deleteCommand, err := parser.ParseDeleteMutation(field, "User", nil)

		assert.Error(t, err)
		assert.Nil(t, deleteCommand)
		assert.Contains(t, err.Error(), "cannot be empty")
	})
}

func TestResolveArgumentValue(t *testing.T) {
	parser := NewMutationParser(zap.NewNop().Sugar())

	t.Run("String value", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.StringValue,
			Raw:  "test string",
		}

		result := parser.resolveArgumentValue(value, nil)
		assert.Equal(t, "test string", result)
	})

	t.Run("Int value", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.IntValue,
			Raw:  "42",
		}

		result := parser.resolveArgumentValue(value, nil)
		assert.Equal(t, "42", result)
	})

	t.Run("Float value", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.FloatValue,
			Raw:  "3.14",
		}

		result := parser.resolveArgumentValue(value, nil)
		assert.Equal(t, "3.14", result)
	})

	t.Run("Boolean value", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.BooleanValue,
			Raw:  "true",
		}

		result := parser.resolveArgumentValue(value, nil)
		assert.Equal(t, "true", result)
	})

	t.Run("Null value", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.NullValue,
		}

		result := parser.resolveArgumentValue(value, nil)
		assert.Nil(t, result)
	})

	t.Run("List value", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.ListValue,
			Children: ast.ChildValueList{
				{
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "item1",
					},
				},
				{
					Value: &ast.Value{
						Kind: ast.StringValue,
						Raw:  "item2",
					},
				},
			},
		}

		result := parser.resolveArgumentValue(value, nil)
		list, ok := result.([]interface{})
		require.True(t, ok)
		assert.Equal(t, 2, len(list))
		assert.Equal(t, "item1", list[0])
		assert.Equal(t, "item2", list[1])
	})

	t.Run("Object value", func(t *testing.T) {
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
				{
					Name: "age",
					Value: &ast.Value{
						Kind: ast.IntValue,
						Raw:  "30",
					},
				},
			},
		}

		result := parser.resolveArgumentValue(value, nil)
		obj, ok := result.(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, 2, len(obj))
		assert.Equal(t, "Alice", obj["name"])
		assert.Equal(t, "30", obj["age"])
	})

	t.Run("Variable value with variables map", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.Variable,
			Raw:  "userName",
		}

		variables := map[string]interface{}{
			"userName": "Bob",
		}

		result := parser.resolveArgumentValue(value, variables)
		assert.Equal(t, "Bob", result)
	})

	t.Run("Variable value without variables map", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.Variable,
			Raw:  "userName",
		}

		result := parser.resolveArgumentValue(value, nil)
		// resolveArgumentValue returns nil for variables without a variables map
		assert.Nil(t, result)
	})

	t.Run("Unknown variable", func(t *testing.T) {
		value := &ast.Value{
			Kind: ast.Variable,
			Raw:  "unknownVar",
		}

		variables := map[string]interface{}{
			"userName": "Bob",
		}

		result := parser.resolveArgumentValue(value, variables)
		// resolveArgumentValue returns nil for unknown variables
		assert.Nil(t, result)
	})
}
