package mutations

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Integration tests for Phase 11 Mutations
// Tests basic instantiation and type mapping

func TestMutationComponentsCreation(t *testing.T) {
	logger := zap.NewNop().Sugar()

	t.Run("NewMutationParser creates parser", func(t *testing.T) {
		parser := NewMutationParser(logger)
		assert.NotNil(t, parser)
	})

	t.Run("NewMutationGenerator creates generator", func(t *testing.T) {
		generator := NewMutationGenerator(logger)
		assert.NotNil(t, generator)
	})
}

func TestMutationGeneratorTypeMapping(t *testing.T) {
	generator := NewMutationGenerator(zap.NewNop().Sugar())

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
			result := generator.mapFieldTypeToGraphQL(tt.syndrType)
			assert.Equal(t, tt.expectedType, result)
		})
	}
}
