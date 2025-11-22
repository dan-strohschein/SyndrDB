package main

import (
	"testing"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner/sorting"
	"syndrdb/src/internal/query/queryparser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Function aliases for sorting functions
var (
	ShouldUseTopNHeap = sorting.ShouldUseTopNHeap
	TopNHeapSort      = sorting.TopNHeapSort
)

// TestTopNHeapSort_ASC tests ascending sort with Top-N heap
func TestTopNHeapSort_ASC(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	// Create test documents with ages: 28, 35, 25, 42, 18
	documents := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Alice")},
				"age":  {Name: "age", Value: models.NewIntValue(28)},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Bob")},
				"age":  {Name: "age", Value: models.NewIntValue(35)},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Charlie")},
				"age":  {Name: "age", Value: models.NewIntValue(25)},
			},
		},
		"doc4": {
			DocumentID: "doc4",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Diana")},
				"age":  {Name: "age", Value: models.NewIntValue(42)},
			},
		},
		"doc5": {
			DocumentID: "doc5",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Eve")},
				"age":  {Name: "age", Value: models.NewIntValue(18)},
			},
		},
	}

	// ORDER BY age ASC, LIMIT 3 should return: Eve(18), Charlie(25), Alice(28)
	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{
				FieldName: "age",
				Direction: queryparser.SortAsc,
			},
		},
	}

	result, err := TopNHeapSort(documents, 3, orderBy, sugar)
	require.NoError(t, err)
	require.Len(t, result, 3)

	// Verify correct order
	nameVal, _ := result[0].Fields["name"].Value.AsString()
	assert.Equal(t, "Eve", nameVal)
	ageVal, _ := result[0].Fields["age"].Value.AsInt()
	assert.Equal(t, int64(18), ageVal)

	nameVal, _ = result[1].Fields["name"].Value.AsString()
	assert.Equal(t, "Charlie", nameVal)
	ageVal, _ = result[1].Fields["age"].Value.AsInt()
	assert.Equal(t, int64(25), ageVal)

	nameVal, _ = result[2].Fields["name"].Value.AsString()
	assert.Equal(t, "Alice", nameVal)
	ageVal, _ = result[2].Fields["age"].Value.AsInt()
	assert.Equal(t, int64(28), ageVal)
}

// TestTopNHeapSort_DESC tests descending sort with Top-N heap
func TestTopNHeapSort_DESC(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	// Create test documents with ages: 28, 35, 25, 42, 18
	documents := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Alice")},
				"age":  {Name: "age", Value: models.NewIntValue(28)},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Bob")},
				"age":  {Name: "age", Value: models.NewIntValue(35)},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Charlie")},
				"age":  {Name: "age", Value: models.NewIntValue(25)},
			},
		},
		"doc4": {
			DocumentID: "doc4",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Diana")},
				"age":  {Name: "age", Value: models.NewIntValue(42)},
			},
		},
		"doc5": {
			DocumentID: "doc5",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: models.NewStringValue("Eve")},
				"age":  {Name: "age", Value: models.NewIntValue(18)},
			},
		},
	}

	// ORDER BY age DESC, LIMIT 3 should return: Diana(42), Bob(35), Alice(28)
	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{
				FieldName: "age",
				Direction: queryparser.SortDesc,
			},
		},
	}

	result, err := TopNHeapSort(documents, 3, orderBy, sugar)
	require.NoError(t, err)
	require.Len(t, result, 3)

	// Verify correct order
	nameVal, _ := result[0].Fields["name"].Value.AsString()
	assert.Equal(t, "Diana", nameVal)
	ageVal, _ := result[0].Fields["age"].Value.AsInt()
	assert.Equal(t, int64(42), ageVal)

	nameVal, _ = result[1].Fields["name"].Value.AsString()
	assert.Equal(t, "Bob", nameVal)
	ageVal, _ = result[1].Fields["age"].Value.AsInt()
	assert.Equal(t, int64(35), ageVal)

	nameVal, _ = result[2].Fields["name"].Value.AsString()
	assert.Equal(t, "Alice", nameVal)
	ageVal, _ = result[2].Fields["age"].Value.AsInt()
	assert.Equal(t, int64(28), ageVal)
}

// TestShouldUseTopNHeap tests the threshold logic
func TestShouldUseTopNHeap(t *testing.T) {
	tests := []struct {
		name        string
		datasetSize int
		limit       int
		threshold   float64
		shouldUse   bool
	}{
		{"Small limit 10%", 100, 10, 0.1, false},       // Exactly at threshold
		{"Small limit 5%", 100, 5, 0.1, true},          // Below threshold
		{"Large limit 50%", 100, 50, 0.1, false},       // Way above threshold
		{"No limit", 100, 0, 0.1, false},               // Invalid limit
		{"Empty dataset", 0, 10, 0.1, false},           // Empty
		{"Limit 1 of 1000", 1000, 1, 0.1, true},        // Very small limit
		{"Limit 1000 of 1000", 1000, 1000, 0.1, false}, // Limit == size
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldUseTopNHeap(tt.datasetSize, tt.limit, tt.threshold)
			assert.Equal(t, tt.shouldUse, result,
				"dataset=%d, limit=%d, threshold=%.2f",
				tt.datasetSize, tt.limit, tt.threshold)
		})
	}
}
