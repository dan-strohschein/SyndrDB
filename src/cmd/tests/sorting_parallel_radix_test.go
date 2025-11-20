package main

import (
	"fmt"
	"runtime"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner/sorting"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Type aliases for sorting configuration types
//type SortingConfig = sorting.SortingConfig

// Function aliases for sorting functions
var (
	ParallelRadixSort      = sorting.ParallelRadixSort
	RadixSort              = sorting.RadixSort
	ShouldUseParallelRadix = sorting.ShouldUseParallelRadix
)

// TestParallelRadixSort_BasicCorrectness verifies parallel radix produces correct results
func TestParallelRadixSort_BasicCorrectness(t *testing.T) {
	logger := zap.NewNop().Sugar()

	tests := []struct {
		name       string
		values     []int64
		ascending  bool
		numWorkers int
	}{
		{"Small ASC", []int64{5, 2, 8, 1, 9, 3}, true, 2},
		{"Small DESC", []int64{5, 2, 8, 1, 9, 3}, false, 2},
		{"Medium ASC", generateSequence(1000, false), true, 4},
		{"Medium DESC", generateSequence(1000, false), false, 4},
		{"Large ASC", generateSequence(10000, false), true, 8},
		{"Negatives ASC", []int64{-5, 2, -8, 1, -9, 3, 0}, true, 2},
		{"Negatives DESC", []int64{-5, 2, -8, 1, -9, 3, 0}, false, 2},
		{"All negative", []int64{-1, -5, -3, -9, -2}, true, 2},
		{"Single worker", generateSequence(100, false), true, 1},
		{"Many workers", generateSequence(100, false), true, 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create documents
			docs := createDocsFromValues(tt.values, "value")

			// Run parallel radix sort
			result, err := ParallelRadixSort(docs, "value", tt.ascending, tt.numWorkers, logger)
			if err != nil {
				t.Fatalf("ParallelRadixSort failed: %v", err)
			}

			// Verify correct count
			if len(result) != len(tt.values) {
				t.Fatalf("Expected %d results, got %d", len(tt.values), len(result))
			}

			// Verify correct ordering
			for i := 0; i < len(result)-1; i++ {
				val1 := result[i].Fields["value"].Value.(int64)
				val2 := result[i+1].Fields["value"].Value.(int64)

				if tt.ascending {
					if val1 > val2 {
						t.Errorf("ASC ordering violated at index %d: %d > %d", i, val1, val2)
					}
				} else {
					if val1 < val2 {
						t.Errorf("DESC ordering violated at index %d: %d < %d", i, val1, val2)
					}
				}
			}
		})
	}
}

// TestParallelRadixSort_CompareWithSequential verifies parallel matches sequential
func TestParallelRadixSort_CompareWithSequential(t *testing.T) {
	logger := zap.NewNop().Sugar()

	tests := []struct {
		name   string
		values []int64
	}{
		{"Sequential positive", generateSequence(100, false)},
		{"Random order", []int64{42, 7, 15, 3, 99, 1, 88, 23, 56, 11}},
		{"With negatives", []int64{-10, 5, -3, 8, 0, -1, 12, -7}},
		{"Large dataset", generateSequence(5000, false)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docs := createDocsFromValues(tt.values, "num")

			// Run sequential radix sort
			seqResult, err := RadixSort(docs, "num", true, logger)
			if err != nil {
				t.Fatalf("Sequential RadixSort failed: %v", err)
			}

			// Run parallel radix sort
			parResult, err := ParallelRadixSort(docs, "num", true, 4, logger)
			if err != nil {
				t.Fatalf("ParallelRadixSort failed: %v", err)
			}

			// Verify results match
			if len(seqResult) != len(parResult) {
				t.Fatalf("Result count mismatch: sequential=%d, parallel=%d",
					len(seqResult), len(parResult))
			}

			for i := 0; i < len(seqResult); i++ {
				seqVal := seqResult[i].Fields["num"].Value.(int64)
				parVal := parResult[i].Fields["num"].Value.(int64)

				if seqVal != parVal {
					t.Errorf("Value mismatch at index %d: sequential=%d, parallel=%d",
						i, seqVal, parVal)
				}
			}
		})
	}
}

// TestParallelRadixSort_MixedTypes tests different integer types
func TestParallelRadixSort_MixedTypes(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := make(map[string]*models.Document)

	// Add int
	doc1 := &models.Document{
		DocumentID: uuid.New().String(),
		Fields: map[string]models.Field{
			"value": {Name: "value", Value: int(42)},
		},
	}
	docs[doc1.DocumentID] = doc1

	// Add int32
	doc2 := &models.Document{
		DocumentID: uuid.New().String(),
		Fields: map[string]models.Field{
			"value": {Name: "value", Value: int32(15)},
		},
	}
	docs[doc2.DocumentID] = doc2

	// Add int64
	doc3 := &models.Document{
		DocumentID: uuid.New().String(),
		Fields: map[string]models.Field{
			"value": {Name: "value", Value: models.NewIntValue(int64(99))},
		},
	}
	docs[doc3.DocumentID] = doc3

	// Add float64 (should convert to int)
	doc4 := &models.Document{
		DocumentID: uuid.New().String(),
		Fields: map[string]models.Field{
			"value": {Name: "value", Value: float64(7.9)}, // Converts to 7
		},
	}
	docs[doc4.DocumentID] = doc4

	result, err := ParallelRadixSort(docs, "value", true, 2, logger)
	if err != nil {
		t.Fatalf("ParallelRadixSort failed: %v", err)
	}

	if len(result) != 4 {
		t.Fatalf("Expected 4 results, got %d", len(result))
	}

	// Expected order: 7, 15, 42, 99
	expectedValues := []int64{7, 15, 42, 99}
	for i, expected := range expectedValues {
		actual := result[i].Fields["value"].Value
		var actualInt64 int64
		switch v := actual.(type) {
		case int:
			actualInt64 = int64(v)
		case int32:
			actualInt64 = int64(v)
		case int64:
			actualInt64 = v
		case float64:
			actualInt64 = int64(v)
		}

		if actualInt64 != expected {
			t.Errorf("At index %d: expected %d, got %d", i, expected, actualInt64)
		}
	}
}

// TestParallelRadixSort_EmptyInput tests handling of empty input
func TestParallelRadixSort_EmptyInput(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := make(map[string]*models.Document)

	result, err := ParallelRadixSort(docs, "value", true, 4, logger)
	if err != nil {
		t.Fatalf("Expected no error for empty input, got: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected empty result, got %d documents", len(result))
	}
}

// TestParallelRadixSort_MissingField tests handling of missing field
func TestParallelRadixSort_MissingField(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := createTestDocuments(100, "other_field")

	result, err := ParallelRadixSort(docs, "missing_field", true, 4, logger)
	if err != nil {
		t.Fatalf("Expected no error for missing field, got: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected empty result for missing field, got %d documents", len(result))
	}
}

// TestShouldUseParallelRadix verifies decision logic
func TestShouldUseParallelRadix(t *testing.T) {
	tests := []struct {
		name        string
		datasetSize int
		limit       int
		config      *SortingConfig
		expected    bool
	}{
		{
			name:        "Large dataset, no limit",
			datasetSize: 50000,
			limit:       0,
			config: &SortingConfig{
				ParallelEnabled: true,
				ParallelMinSize: 10000,
				RadixLimitRatio: 0.5,
			},
			expected: true,
		},
		{
			name:        "Large dataset, large limit",
			datasetSize: 50000,
			limit:       30000,
			config: &SortingConfig{
				ParallelEnabled: true,
				ParallelMinSize: 10000,
				RadixLimitRatio: 0.5,
			},
			expected: true,
		},
		{
			name:        "Disabled",
			datasetSize: 50000,
			limit:       0,
			config: &SortingConfig{
				ParallelEnabled: false,
				ParallelMinSize: 10000,
				RadixLimitRatio: 0.5,
			},
			expected: false,
		},
		{
			name:        "Dataset too small",
			datasetSize: 5000,
			limit:       0,
			config: &SortingConfig{
				ParallelEnabled: true,
				ParallelMinSize: 10000,
				RadixLimitRatio: 0.5,
			},
			expected: false,
		},
		{
			name:        "Limit too small (Top-N better)",
			datasetSize: 50000,
			limit:       1000,
			config: &SortingConfig{
				ParallelEnabled: true,
				ParallelMinSize: 10000,
				RadixLimitRatio: 0.5,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if runtime.NumCPU() < 2 && tt.expected == true {
				t.Skip("Skipping test that requires multiple CPUs")
			}

			result := ShouldUseParallelRadix(tt.datasetSize, tt.limit, tt.config)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// BenchmarkParallelRadixSort benchmarks parallel radix performance
func BenchmarkParallelRadixSort(b *testing.B) {
	logger := zap.NewNop().Sugar()

	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		values := generateSequence(size, false)
		docs := createDocsFromValues(values, "value")

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = ParallelRadixSort(docs, "value", true, runtime.NumCPU(), logger)
			}
		})
	}
}

// BenchmarkParallelRadixSort_Workers benchmarks different worker counts
func BenchmarkParallelRadixSort_Workers(b *testing.B) {
	logger := zap.NewNop().Sugar()

	values := generateSequence(50000, false)
	docs := createDocsFromValues(values, "value")

	workers := []int{1, 2, 4, 8, 16}

	for _, numWorkers := range workers {
		b.Run(fmt.Sprintf("workers=%d", numWorkers), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = ParallelRadixSort(docs, "value", true, numWorkers, logger)
			}
		})
	}
}

// BenchmarkParallelVsSequentialRadix compares parallel vs sequential radix
func BenchmarkParallelVsSequentialRadix(b *testing.B) {
	logger := zap.NewNop().Sugar()

	sizes := []int{10000, 50000, 100000}

	for _, size := range sizes {
		values := generateSequence(size, false)
		docs := createDocsFromValues(values, "value")

		b.Run(fmt.Sprintf("Sequential_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = RadixSort(docs, "value", true, logger)
			}
		})

		b.Run(fmt.Sprintf("Parallel_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = ParallelRadixSort(docs, "value", true, runtime.NumCPU(), logger)
			}
		})
	}
}

// Helper functions

// generateSequence creates a slice of int64 values
func generateSequence(count int, sorted bool) []int64 {
	values := make([]int64, count)
	for i := 0; i < count; i++ {
		if sorted {
			values[i] = int64(i)
		} else {
			// Reverse order for testing
			values[i] = int64(count - i - 1)
		}
	}
	return values
}

// createTestDocuments creates test documents with sequential integer values
func createTestDocuments(count int, fieldName string) map[string]*models.Document {
	values := make([]int64, count)
	for i := 0; i < count; i++ {
		values[i] = int64(i)
	}
	return createDocsFromValues(values, fieldName)
}

// createDocsFromValues creates test documents from a slice of int64 values
func createDocsFromValues(values []int64, fieldName string) map[string]*models.Document {
	docs := make(map[string]*models.Document, len(values))

	for _, val := range values {
		doc := &models.Document{
			DocumentID: uuid.New().String(),
			Fields: map[string]models.Field{
				fieldName: {
					Name:  fieldName,
					Value: val,
				},
			},
		}
		docs[doc.DocumentID] = doc
	}

	return docs
}
