package main

// DISABLED: Tests internal unexported methods
//
// This test file requires access to:
//   - generator.mapFieldTypeToGraphQL() - unexported method
//
// To enable this test:
//   1. Move this file to src/internal/graphQL/mutations/mutation_generator_test.go
//      (tests in the same package can access unexported methods)
//   2. Change package declaration from "package main" to "package mutations"
//   3. Remove the `// +build ignore` line above
//
// Alternatively, export mapFieldTypeToGraphQL if it needs to be tested from external packages,
// or test the functionality through the public API only.

import (
	"syndrdb/src/internal/graphQL/mutations"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Integration tests for Phase 11 Mutations
// Tests basic instantiation and type mapping

func TestMutationComponentsCreation(t *testing.T) {
	logger := zap.NewNop().Sugar()

	t.Run("NewMutationParser creates parser", func(t *testing.T) {
		parser := mutations.NewMutationParser(logger)
		assert.NotNil(t, parser)
	})

	t.Run("NewMutationGenerator creates generator", func(t *testing.T) {
		generator := mutations.NewMutationGenerator(logger)
		assert.NotNil(t, generator)
	})
}

func TestMutationGeneratorTypeMapping(t *testing.T) {
	generator := mutations.NewMutationGenerator(zap.NewNop().Sugar())

	tests := []struct {
		syndrType    string
		expectedType string
	}{
		{"INTEGER", "Int"},
		{"STRING", "String"},
		{"FLOAT", "Float"},
		{"BOOLEAN", "Boolean"},
		{"UNKNOWN", "String"}, // Default case
	}

	for _, tt := range tests {
		t.Run(tt.syndrType, func(t *testing.T) {
			result := generator.MapFieldTypeToGraphQL(tt.syndrType)
			assert.Equal(t, tt.expectedType, result)
		})
	}
}
