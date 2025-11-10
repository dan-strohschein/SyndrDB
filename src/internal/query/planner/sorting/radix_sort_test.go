package sorting

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"testing"

	"go.uber.org/zap"
)

// TestRadixSort_ASC tests radix sort in ascending order
func TestRadixSort_ASC(t *testing.T) {
	logger := zap.NewNop().Sugar()

	// Create test documents with integer ages
	docs := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "Alice"},
				"age":  {Name: "age", Value: int64(35)},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "Bob"},
				"age":  {Name: "age", Value: int64(28)},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "Charlie"},
				"age":  {Name: "age", Value: int64(42)},
			},
		},
		"doc4": {
			DocumentID: "doc4",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "Diana"},
				"age":  {Name: "age", Value: int64(18)},
			},
		},
		"doc5": {
			DocumentID: "doc5",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "Eve"},
				"age":  {Name: "age", Value: int64(25)},
			},
		},
	}

	// Sort ascending
	result, err := RadixSort(docs, "age", true, logger)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	// Verify results
	if len(result) != 5 {
		t.Errorf("Expected 5 results, got %d", len(result))
	}

	// Check order: 18, 25, 28, 35, 42
	expectedAges := []int64{18, 25, 28, 35, 42}
	for i, doc := range result {
		age := doc.Fields["age"].Value.(int64)
		if age != expectedAges[i] {
			t.Errorf("Result[%d]: expected age %d, got %d", i, expectedAges[i], age)
		}
	}

	t.Logf("ASC radix sort correct: %d, %d, %d, %d, %d",
		result[0].Fields["age"].Value.(int64),
		result[1].Fields["age"].Value.(int64),
		result[2].Fields["age"].Value.(int64),
		result[3].Fields["age"].Value.(int64),
		result[4].Fields["age"].Value.(int64))
}

// TestRadixSort_DESC tests radix sort in descending order
func TestRadixSort_DESC(t *testing.T) {
	logger := zap.NewNop().Sugar()

	// Create test documents
	docs := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"score": {Name: "score", Value: int64(100)},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"score": {Name: "score", Value: int64(250)},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"score": {Name: "score", Value: int64(175)},
			},
		},
		"doc4": {
			DocumentID: "doc4",
			Fields: map[string]models.Field{
				"score": {Name: "score", Value: int64(50)},
			},
		},
	}

	// Sort descending
	result, err := RadixSort(docs, "score", false, logger)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	// Verify results
	if len(result) != 4 {
		t.Errorf("Expected 4 results, got %d", len(result))
	}

	// Check order: 250, 175, 100, 50
	expectedScores := []int64{250, 175, 100, 50}
	for i, doc := range result {
		score := doc.Fields["score"].Value.(int64)
		if score != expectedScores[i] {
			t.Errorf("Result[%d]: expected score %d, got %d", i, expectedScores[i], score)
		}
	}

	t.Logf("DESC radix sort correct: %d, %d, %d, %d",
		result[0].Fields["score"].Value.(int64),
		result[1].Fields["score"].Value.(int64),
		result[2].Fields["score"].Value.(int64),
		result[3].Fields["score"].Value.(int64))
}

// TestRadixSort_NegativeNumbers tests handling of negative integers
func TestRadixSort_NegativeNumbers(t *testing.T) {
	logger := zap.NewNop().Sugar()

	// Create test documents with negative, zero, and positive values
	docs := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"temperature": {Name: "temperature", Value: int64(-15)},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"temperature": {Name: "temperature", Value: int64(0)},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"temperature": {Name: "temperature", Value: int64(25)},
			},
		},
		"doc4": {
			DocumentID: "doc4",
			Fields: map[string]models.Field{
				"temperature": {Name: "temperature", Value: int64(-5)},
			},
		},
		"doc5": {
			DocumentID: "doc5",
			Fields: map[string]models.Field{
				"temperature": {Name: "temperature", Value: int64(10)},
			},
		},
	}

	// Sort ascending
	result, err := RadixSort(docs, "temperature", true, logger)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	// Verify results: -15, -5, 0, 10, 25
	expectedTemps := []int64{-15, -5, 0, 10, 25}
	for i, doc := range result {
		temp := doc.Fields["temperature"].Value.(int64)
		if temp != expectedTemps[i] {
			t.Errorf("Result[%d]: expected temp %d, got %d", i, expectedTemps[i], temp)
		}
	}

	t.Logf("Negative numbers handled correctly: %d, %d, %d, %d, %d",
		result[0].Fields["temperature"].Value.(int64),
		result[1].Fields["temperature"].Value.(int64),
		result[2].Fields["temperature"].Value.(int64),
		result[3].Fields["temperature"].Value.(int64),
		result[4].Fields["temperature"].Value.(int64))
}

// TestRadixSort_LargeNumbers tests with large int64 values
func TestRadixSort_LargeNumbers(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"value": {Name: "value", Value: int64(9223372036854775807)}, // Max int64
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"value": {Name: "value", Value: int64(1000000000000)}, // 1 trillion
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"value": {Name: "value", Value: int64(-9223372036854775808)}, // Min int64
			},
		},
		"doc4": {
			DocumentID: "doc4",
			Fields: map[string]models.Field{
				"value": {Name: "value", Value: int64(0)},
			},
		},
	}

	// Sort ascending
	result, err := RadixSort(docs, "value", true, logger)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	// Verify order: min, 0, 1T, max
	if len(result) != 4 {
		t.Fatalf("Expected 4 results, got %d", len(result))
	}

	expectedValues := []int64{
		-9223372036854775808, // Min int64
		0,
		1000000000000,       // 1 trillion
		9223372036854775807, // Max int64
	}

	for i, doc := range result {
		value := doc.Fields["value"].Value.(int64)
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
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"count": {Name: "count", Value: int32(500)},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"count": {Name: "count", Value: int32(100)},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"count": {Name: "count", Value: int32(300)},
			},
		},
	}

	// Sort ascending
	result, err := RadixSort(docs, "count", true, logger)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	// Verify order: 100, 300, 500
	expectedCounts := []int64{100, 300, 500} // Converted to int64
	for i, doc := range result {
		// int32 is converted to int64 internally
		count := doc.Fields["count"].Value.(int32)
		if int64(count) != expectedCounts[i] {
			t.Errorf("Result[%d]: expected %d, got %d", i, expectedCounts[i], count)
		}
	}

	t.Log("int32 values sorted correctly")
}

// TestRadixSort_EmptyInput tests with empty document map
func TestRadixSort_EmptyInput(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := map[string]*models.Document{}

	result, err := RadixSort(docs, "age", true, logger)
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
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "Alice"},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "Bob"},
			},
		},
	}

	// Try to sort by non-existent field
	result, err := RadixSort(docs, "age", true, logger)
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
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "Alice"},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "Bob"},
			},
		},
	}

	// Try to sort string field with radix sort
	result, err := RadixSort(docs, "name", true, logger)
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
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"age": {Name: "age", Value: int64(30)},
			},
		},
	}

	result, err := RadixSort(docs, "age", true, logger)
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
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"score": {Name: "score", Value: int64(100)},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"score": {Name: "score", Value: int64(200)},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"score": {Name: "score", Value: int64(100)},
			},
		},
		"doc4": {
			DocumentID: "doc4",
			Fields: map[string]models.Field{
				"score": {Name: "score", Value: int64(200)},
			},
		},
	}

	result, err := RadixSort(docs, "score", true, logger)
	if err != nil {
		t.Fatalf("RadixSort failed: %v", err)
	}

	if len(result) != 4 {
		t.Errorf("Expected 4 results, got %d", len(result))
	}

	// Verify all 100s come before all 200s
	for i := 0; i < 2; i++ {
		score := result[i].Fields["score"].Value.(int64)
		if score != 100 {
			t.Errorf("Result[%d]: expected score 100, got %d", i, score)
		}
	}

	for i := 2; i < 4; i++ {
		score := result[i].Fields["score"].Value.(int64)
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

	// Create 1000 documents with random ages
	docs := make(map[string]*models.Document, 1000)
	for i := 0; i < 1000; i++ {
		docs[fmt.Sprintf("doc%d", i)] = &models.Document{
			DocumentID: fmt.Sprintf("doc%d", i),
			Fields: map[string]models.Field{
				"age": {Name: "age", Value: int64(i % 100)},
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RadixSort(docs, "age", true, logger)
	}
}

// BenchmarkRadixSort_10000 benchmarks radix sort with 10000 elements
func BenchmarkRadixSort_10000(b *testing.B) {
	logger := zap.NewNop().Sugar()

	// Create 10000 documents
	docs := make(map[string]*models.Document, 10000)
	for i := 0; i < 10000; i++ {
		docs[fmt.Sprintf("doc%d", i)] = &models.Document{
			DocumentID: fmt.Sprintf("doc%d", i),
			Fields: map[string]models.Field{
				"age": {Name: "age", Value: int64(i % 1000)},
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RadixSort(docs, "age", true, logger)
	}
}
