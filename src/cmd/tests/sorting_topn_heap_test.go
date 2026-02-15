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

	documents := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"name": "Alice", "age": int64(28)}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"name": "Bob", "age": int64(35)}},
		"doc3": {DocumentID: "doc3", Data: map[string]interface{}{"name": "Charlie", "age": int64(25)}},
		"doc4": {DocumentID: "doc4", Data: map[string]interface{}{"name": "Diana", "age": int64(42)}},
		"doc5": {DocumentID: "doc5", Data: map[string]interface{}{"name": "Eve", "age": int64(18)}},
	}

	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{FieldName: "age", Direction: queryparser.SortAsc},
		},
	}
	schema := sortSchema("age")

	result, err := TopNHeapSort(documents, 3, orderBy, sugar, schema)
	require.NoError(t, err)
	require.Len(t, result, 3)

	nameVal, _ := getSortResultString(result[0], nil, "name")
	assert.Equal(t, "Eve", nameVal)
	ageVal, _ := getSortResultInt(result[0], nil, "age")
	assert.Equal(t, int64(18), ageVal)

	nameVal, _ = getSortResultString(result[1], nil, "name")
	assert.Equal(t, "Charlie", nameVal)
	ageVal, _ = getSortResultInt(result[1], nil, "age")
	assert.Equal(t, int64(25), ageVal)

	nameVal, _ = getSortResultString(result[2], nil, "name")
	assert.Equal(t, "Alice", nameVal)
	ageVal, _ = getSortResultInt(result[2], nil, "age")
	assert.Equal(t, int64(28), ageVal)
}

// TestTopNHeapSort_DESC tests descending sort with Top-N heap
func TestTopNHeapSort_DESC(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	documents := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"name": "Alice", "age": int64(28)}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"name": "Bob", "age": int64(35)}},
		"doc3": {DocumentID: "doc3", Data: map[string]interface{}{"name": "Charlie", "age": int64(25)}},
		"doc4": {DocumentID: "doc4", Data: map[string]interface{}{"name": "Diana", "age": int64(42)}},
		"doc5": {DocumentID: "doc5", Data: map[string]interface{}{"name": "Eve", "age": int64(18)}},
	}

	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{FieldName: "age", Direction: queryparser.SortDesc},
		},
	}
	schema := sortSchema("age")

	result, err := TopNHeapSort(documents, 3, orderBy, sugar, schema)
	require.NoError(t, err)
	require.Len(t, result, 3)

	nameVal, _ := getSortResultString(result[0], nil, "name")
	assert.Equal(t, "Diana", nameVal)
	ageVal, _ := getSortResultInt(result[0], nil, "age")
	assert.Equal(t, int64(42), ageVal)

	nameVal, _ = getSortResultString(result[1], nil, "name")
	assert.Equal(t, "Bob", nameVal)
	ageVal, _ = getSortResultInt(result[1], nil, "age")
	assert.Equal(t, int64(35), ageVal)

	nameVal, _ = getSortResultString(result[2], nil, "name")
	assert.Equal(t, "Alice", nameVal)
	ageVal, _ = getSortResultInt(result[2], nil, "age")
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
