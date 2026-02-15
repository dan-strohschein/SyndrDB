package main

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner/sorting"
	"testing"

	"go.uber.org/zap"
)

// Function aliases for sorting functions (RadixSort is declared in sorting_parallel_radix_test.go)
var (
	ShouldUseRadixSort = sorting.ShouldUseRadixSort
)

// TestRadixSort_ASC tests radix sort in ascending order
func TestRadixSort_ASC(t *testing.T) {
	logger := zap.NewNop().Sugar()

	// Create test documents with integer ages (Data only; sort reads via schema/Data)
	docs := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"name": "Alice", "age": int64(35)}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"name": "Bob", "age": int64(28)}},
		"doc3": {DocumentID: "doc3", Data: map[string]interface{}{"name": "Charlie", "age": int64(42)}},
		"doc4": {DocumentID: "doc4", Data: map[string]interface{}{"name": "Diana", "age": int64(18)}},
		"doc5": {DocumentID: "doc5", Data: map[string]interface{}{"name": "Eve", "age": int64(25)}},
	}

	schema := sortSchema("age")
	result, err := RadixSort(docs, "age", true, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	if len(result) != 5 {
		t.Errorf("Expected 5 results, got %d", len(result))
	}

	expectedAges := []int64{18, 25, 28, 35, 42}
	for i, doc := range result {
		age, _ := getSortResultInt(doc, nil, "age")
		if age != expectedAges[i] {
			t.Errorf("Result[%d]: expected age %d, got %d", i, expectedAges[i], age)
		}
	}

	age0, _ := getSortResultInt(result[0], nil, "age")
	age1, _ := getSortResultInt(result[1], nil, "age")
	age2, _ := getSortResultInt(result[2], nil, "age")
	age3, _ := getSortResultInt(result[3], nil, "age")
	age4, _ := getSortResultInt(result[4], nil, "age")
	t.Logf("ASC radix sort correct: %d, %d, %d, %d, %d", age0, age1, age2, age3, age4)
}

// TestRadixSort_DESC tests radix sort in descending order
func TestRadixSort_DESC(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"score": int64(100)}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"score": int64(250)}},
		"doc3": {DocumentID: "doc3", Data: map[string]interface{}{"score": int64(175)}},
		"doc4": {DocumentID: "doc4", Data: map[string]interface{}{"score": int64(50)}},
	}

	schema := sortSchema("score")
	result, err := RadixSort(docs, "score", false, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	if len(result) != 4 {
		t.Errorf("Expected 4 results, got %d", len(result))
	}

	expectedScores := []int64{250, 175, 100, 50}
	for i, doc := range result {
		score, _ := getSortResultInt(doc, nil, "score")
		if score != expectedScores[i] {
			t.Errorf("Result[%d]: expected score %d, got %d", i, expectedScores[i], score)
		}
	}

	score0, _ := getSortResultInt(result[0], nil, "score")
	score1, _ := getSortResultInt(result[1], nil, "score")
	score2, _ := getSortResultInt(result[2], nil, "score")
	score3, _ := getSortResultInt(result[3], nil, "score")
	t.Logf("DESC radix sort correct: %d, %d, %d, %d", score0, score1, score2, score3)
}

// TestRadixSort_NegativeNumbers tests handling of negative integers
func TestRadixSort_NegativeNumbers(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"temperature": int64(-15)}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"temperature": int64(0)}},
		"doc3": {DocumentID: "doc3", Data: map[string]interface{}{"temperature": int64(25)}},
		"doc4": {DocumentID: "doc4", Data: map[string]interface{}{"temperature": int64(-5)}},
		"doc5": {DocumentID: "doc5", Data: map[string]interface{}{"temperature": int64(10)}},
	}

	schema := sortSchema("temperature")
	result, err := RadixSort(docs, "temperature", true, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	expectedTemps := []int64{-15, -5, 0, 10, 25}
	for i, doc := range result {
		temp, _ := getSortResultInt(doc, nil, "temperature")
		if temp != expectedTemps[i] {
			t.Errorf("Result[%d]: expected temp %d, got %d", i, expectedTemps[i], temp)
		}
	}

	temp0, _ := getSortResultInt(result[0], nil, "temperature")
	temp1, _ := getSortResultInt(result[1], nil, "temperature")
	temp2, _ := getSortResultInt(result[2], nil, "temperature")
	temp3, _ := getSortResultInt(result[3], nil, "temperature")
	temp4, _ := getSortResultInt(result[4], nil, "temperature")
	t.Logf("Negative number sort correct: %d, %d, %d, %d, %d", temp0, temp1, temp2, temp3, temp4)
}

// TestRadixSort_LargeNumbers tests with large int64 values
func TestRadixSort_LargeNumbers(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"value": int64(9223372036854775807)}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"value": int64(1000000000000)}},
		"min":  {DocumentID: "min", Data: map[string]interface{}{"value": int64(-9223372036854775808)}},
		"doc4": {DocumentID: "doc4", Data: map[string]interface{}{"value": int64(0)}},
	}

	schema := sortSchema("value")
	result, err := RadixSort(docs, "value", true, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	if len(result) != 4 {
		t.Fatalf("Expected 4 results, got %d", len(result))
	}

	expectedValues := []int64{
		-9223372036854775808,
		0,
		1000000000000,
		9223372036854775807,
	}

	for i, doc := range result {
		value, _ := getSortResultInt(doc, nil, "value")
		if value != expectedValues[i] {
			t.Errorf("Result[%d]: expected %d, got %d", i, expectedValues[i], value)
		}
	}

	t.Log("Large numbers sorted correctly")
}

// TestRadixSort_Int32 tests with int32 values
func TestRadixSort_Int32(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"count": int64(500)}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"count": int64(100)}},
		"doc3": {DocumentID: "doc3", Data: map[string]interface{}{"count": int64(300)}},
	}

	schema := sortSchema("count")
	result, err := RadixSort(docs, "count", true, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	expectedCounts := []int64{100, 300, 500}
	for i, doc := range result {
		count, _ := getSortResultInt(doc, nil, "count")
		if count != expectedCounts[i] {
			t.Errorf("Result[%d]: expected %d, got %d", i, expectedCounts[i], count)
		}
	}

	t.Log("int32 values sorted correctly")
}

// TestRadixSort_EmptyInput tests with empty document map
func TestRadixSort_EmptyInput(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{}

	schema := sortSchema("age")
	result, err := RadixSort(docs, "age", true, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected empty result, got %d documents", len(result))
	}
}

// TestRadixSort_MissingField tests behavior when field doesn't exist
func TestRadixSort_MissingField(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"name": "Alice"}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"name": "Bob"}},
	}

	schema := sortSchema("age")
	result, err := RadixSort(docs, "age", true, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	// Should skip documents without the field
	if len(result) != 0 {
		t.Errorf("Expected 0 results (field missing), got %d", len(result))
	}
}

// TestRadixSort_NonIntegerField tests with non-integer field type
func TestRadixSort_NonIntegerField(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"name": "Alice"}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"name": "Bob"}},
	}

	schema := sortSchema("name")
	result, err := RadixSort(docs, "name", true, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	// Should skip non-integer values
	if len(result) != 0 {
		t.Errorf("Expected 0 results (non-integer field), got %d", len(result))
	}
}

// TestRadixSort_SingleDocument tests with one document
func TestRadixSort_SingleDocument(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"age": int64(30)}},
	}

	schema := sortSchema("age")
	result, err := RadixSort(docs, "age", true, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result))
	}

	if result[0].DocumentID != "doc1" {
		t.Errorf("Expected doc1, got %s", result[0].DocumentID)
	}
}

// TestRadixSort_DuplicateValues tests with duplicate integer values
func TestRadixSort_DuplicateValues(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{
		"doc1": {DocumentID: "doc1", Data: map[string]interface{}{"score": int64(100)}},
		"doc2": {DocumentID: "doc2", Data: map[string]interface{}{"score": int64(200)}},
		"doc3": {DocumentID: "doc3", Data: map[string]interface{}{"score": int64(100)}},
		"doc4": {DocumentID: "doc4", Data: map[string]interface{}{"score": int64(200)}},
	}

	schema := sortSchema("score")
	result, err := RadixSort(docs, "score", true, logger, schema)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	if len(result) != 4 {
		t.Errorf("Expected 4 results, got %d", len(result))
	}

	for i := 0; i < 2; i++ {
		score, _ := getSortResultInt(result[i], nil, "score")
		if score != 100 {
			t.Errorf("Result[%d]: expected score 100, got %d", i, score)
		}
	}

	for i := 2; i < 4; i++ {
		score, _ := getSortResultInt(result[i], nil, "score")
		if score != 200 {
			t.Errorf("Result[%d]: expected score 200, got %d", i, score)
		}
	}

	t.Log("Duplicate values handled correctly (stable sort)")
}

// TestShouldUseRadixSort tests the threshold logic
func TestShouldUseRadixSort(t *testing.T) {
	tests := []struct {
		name         string
		totalDocs    int
		limit        int
		minRadixSize int
		limitRatio   float64
		expected     bool
	}{
		{
			name:         "Small dataset (< 1000)",
			totalDocs:    500,
			limit:        0,
			minRadixSize: 1000,
			limitRatio:   0.5,
			expected:     false, // Too small
		},
		{
			name:         "Large dataset, no limit",
			totalDocs:    10000,
			limit:        0,
			minRadixSize: 1000,
			limitRatio:   0.5,
			expected:     true, // Full sort, radix optimal
		},
		{
			name:         "Large dataset, large limit (80%)",
			totalDocs:    10000,
			limit:        8000,
			minRadixSize: 1000,
			limitRatio:   0.5,
			expected:     true, // 80% > 50% threshold
		},
		{
			name:         "Large dataset, small limit (10%)",
			totalDocs:    10000,
			limit:        1000,
			minRadixSize: 1000,
			limitRatio:   0.5,
			expected:     false, // 10% < 50% threshold, Top-N better
		},
		{
			name:         "Exactly at 50% threshold",
			totalDocs:    10000,
			limit:        5000,
			minRadixSize: 1000,
			limitRatio:   0.5,
			expected:     false, // Must be > 50%
		},
		{
			name:         "Just above 50% threshold",
			totalDocs:    10000,
			limit:        5001,
			minRadixSize: 1000,
			limitRatio:   0.5,
			expected:     true, // 50.01% > 50%
		},
		{
			name:         "Custom threshold 30%",
			totalDocs:    10000,
			limit:        3500,
			minRadixSize: 1000,
			limitRatio:   0.3,
			expected:     true, // 35% > 30% custom threshold
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldUseRadixSort(tt.totalDocs, tt.limit, tt.minRadixSize, tt.limitRatio)
			if result != tt.expected {
				t.Errorf("ShouldUseRadixSort(%d, %d, %d, %.2f) = %v, expected %v",
					tt.totalDocs, tt.limit, tt.minRadixSize, tt.limitRatio, result, tt.expected)
			}
		})
	}
}

// BenchmarkRadixSort_1000 benchmarks radix sort with 1000 elements
func BenchmarkRadixSort_1000(b *testing.B) {
	logger := zap.NewNop().Sugar()

	docs := make(map[string]*models.Document, 1000)
	for i := 0; i < 1000; i++ {
		docs[fmt.Sprintf("doc%d", i)] = &models.Document{
			DocumentID: fmt.Sprintf("doc%d", i),
			Data:       map[string]interface{}{"age": int64(i % 100)},
		}
	}
	schema := sortSchema("age")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RadixSort(docs, "age", true, logger, schema)
	}
}

// BenchmarkRadixSort_10000 benchmarks radix sort with 10000 elements
func BenchmarkRadixSort_10000(b *testing.B) {
	logger := zap.NewNop().Sugar()

	docs := make(map[string]*models.Document, 10000)
	for i := 0; i < 10000; i++ {
		docs[fmt.Sprintf("doc%d", i)] = &models.Document{
			DocumentID: fmt.Sprintf("doc%d", i),
			Data:       map[string]interface{}{"age": int64(i % 1000)},
		}
	}
	schema := sortSchema("age")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RadixSort(docs, "age", true, logger, schema)
	}
}
