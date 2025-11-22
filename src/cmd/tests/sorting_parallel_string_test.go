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

// Type aliases for sorting types
//type SortingConfig = sorting.SortingConfig

// Function aliases for sorting functions
var (
	ParallelStringSort      = sorting.ParallelStringSort
	ShouldUseParallelString = sorting.ShouldUseParallelString
)

// Helper function to create test documents with string values
func createTestDocumentsWithStrings(count int, fieldName string, prefix string) map[string]*models.Document {
	docs := make(map[string]*models.Document, count)

	for i := 0; i < count; i++ {
		doc := &models.Document{
			DocumentID: uuid.New().String(),
			Fields: map[string]models.Field{
				fieldName: {
					Name:  fieldName,
					Value: models.NewStringValue(fmt.Sprintf("%s%03d", prefix, i)),
				},
			},
		}
		docs[doc.DocumentID] = doc
	}

	return docs
}

// TestParallelStringSort_BasicCorrectness verifies parallel string sorting produces correct results
func TestParallelStringSort_BasicCorrectness(t *testing.T) {
	logger := zap.NewNop().Sugar()

	tests := []struct {
		name       string
		numDocs    int
		ascending  bool
		numWorkers int
	}{
		{"Small ASC", 100, true, 2},
		{"Small DESC", 100, false, 2},
		{"Medium ASC", 1000, true, 4},
		{"Medium DESC", 1000, false, 4},
		{"Large ASC", 10000, true, 8},
		{"Single worker", 1000, true, 1},
		{"Many workers", 100, true, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test documents with string field
			docs := createTestDocumentsWithStrings(tt.numDocs, "name", "user")

			// Run parallel string sort
			result, err := ParallelStringSort(docs, "name", tt.ascending, true, tt.numWorkers, logger)
			if err != nil {
				t.Fatalf("ParallelStringSort failed: %v", err)
			}

			// Verify result count
			if len(result) != tt.numDocs {
				t.Errorf("Expected %d results, got %d", tt.numDocs, len(result))
			}

			// Verify correct ordering
			for i := 0; i < len(result)-1; i++ {
				name1, _ := result[i].Fields["name"].Value.AsString()
				name2, _ := result[i+1].Fields["name"].Value.AsString()

				if tt.ascending {
					if name1 > name2 {
						t.Errorf("ASC ordering violated at index %d: %s > %s", i, name1, name2)
					}
				} else {
					if name1 < name2 {
						t.Errorf("DESC ordering violated at index %d: %s < %s", i, name1, name2)
					}
				}
			}
		})
	}
}

// TestParallelStringSort_VariedStrings tests with different string characteristics
func TestParallelStringSort_VariedStrings(t *testing.T) {
	logger := zap.NewNop().Sugar()

	// Create documents with varied string patterns
	docs := make(map[string]*models.Document)

	patterns := []string{
		"short",
		"medium_length_string",
		"very_long_string_with_many_characters_to_test_simd_acceleration",
		"UPPERCASE",
		"lowercase",
		"MixedCase",
		"with numbers 123",
		"special!@#chars",
	}

	for i := 0; i < 100; i++ {
		pattern := patterns[i%len(patterns)]
		doc := &models.Document{
			DocumentID: uuid.New().String(),
			Fields: map[string]models.Field{
				"text": {
					Name:  "text",
					Value: models.NewStringValue(fmt.Sprintf("%s_%03d", pattern, i)),
				},
			},
		}
		docs[doc.DocumentID] = doc
	}

	result, err := ParallelStringSort(docs, "text", true, true, 4, logger)
	if err != nil {
		t.Fatalf("ParallelStringSort failed: %v", err)
	}

	// Verify ordering
	for i := 0; i < len(result)-1; i++ {
		text1, _ := result[i].Fields["text"].Value.AsString()
		text2, _ := result[i+1].Fields["text"].Value.AsString()

		if text1 > text2 {
			t.Errorf("Ordering violated at index %d: %s > %s", i, text1, text2)
		}
	}
}

// TestParallelStringSort_WithSIMD tests SIMD acceleration toggle
func TestParallelStringSort_WithSIMD(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := createTestDocumentsWithStrings(1000, "description", "long_description_text_")

	// Sort with SIMD enabled
	resultWithSIMD, err := ParallelStringSort(docs, "description", true, true, 4, logger)
	if err != nil {
		t.Fatalf("ParallelStringSort with SIMD failed: %v", err)
	}

	// Sort with SIMD disabled
	resultWithoutSIMD, err := ParallelStringSort(docs, "description", true, false, 4, logger)
	if err != nil {
		t.Fatalf("ParallelStringSort without SIMD failed: %v", err)
	}

	// Both should produce identical ordering
	if len(resultWithSIMD) != len(resultWithoutSIMD) {
		t.Fatalf("Result count mismatch: withSIMD=%d, withoutSIMD=%d",
			len(resultWithSIMD), len(resultWithoutSIMD))
	}

	for i := 0; i < len(resultWithSIMD); i++ {
		val1, _ := resultWithSIMD[i].Fields["description"].Value.AsString()
		val2, _ := resultWithoutSIMD[i].Fields["description"].Value.AsString()

		if val1 != val2 {
			t.Errorf("SIMD vs non-SIMD mismatch at index %d: %s != %s", i, val1, val2)
		}
	}
}

// TestParallelStringSort_EmptyInput tests handling of empty input
func TestParallelStringSort_EmptyInput(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := make(map[string]*models.Document)

	result, err := ParallelStringSort(docs, "name", true, true, 4, logger)
	if err != nil {
		t.Fatalf("Expected no error for empty input, got: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected empty result, got %d documents", len(result))
	}
}

// TestParallelStringSort_MissingField tests handling of missing fields
func TestParallelStringSort_MissingField(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := make(map[string]*models.Document)
	for i := 0; i < 100; i++ {
		doc := &models.Document{
			DocumentID: uuid.New().String(),
			Fields: map[string]models.Field{
				"other_field": {Name: "other_field", Value: models.NewStringValue("value")},
			},
		}
		docs[doc.DocumentID] = doc
	}

	result, err := ParallelStringSort(docs, "name", true, true, 4, logger)
	if err != nil {
		t.Fatalf("Expected no error for missing field, got: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected empty result for missing field, got %d documents", len(result))
	}
}

// TestParallelStringSort_ByteArrayValues tests handling of []byte values
func TestParallelStringSort_ByteArrayValues(t *testing.T) {
	logger := zap.NewNop().Sugar()

	docs := make(map[string]*models.Document)
	for i := 0; i < 100; i++ {
		doc := &models.Document{
			DocumentID: uuid.New().String(),
			Fields: map[string]models.Field{
				"data": {
					Name:  "data",
					Value: models.NewInterfaceValue([]byte(fmt.Sprintf("bytes_%03d", i))),
				},
			},
		}
		docs[doc.DocumentID] = doc
	}

	result, err := ParallelStringSort(docs, "data", true, true, 4, logger)
	if err != nil {
		t.Fatalf("ParallelStringSort failed: %v", err)
	}

	if len(result) != 100 {
		t.Errorf("Expected 100 results, got %d", len(result))
	}

	// Verify ordering
	for i := 0; i < len(result)-1; i++ {
		bytes1 := result[i].Fields["data"].Value.AsInterface().([]byte)
		bytes2 := result[i+1].Fields["data"].Value.AsInterface().([]byte)
		str1 := string(bytes1)
		str2 := string(bytes2)

		if str1 > str2 {
			t.Errorf("Ordering violated at index %d: %s > %s", i, str1, str2)
		}
	}
}

// TestShouldUseParallelString verifies decision logic for using parallel string sort
func TestShouldUseParallelString(t *testing.T) {
	tests := []struct {
		name        string
		datasetSize int
		config      *SortingConfig
		expected    bool
	}{
		{
			name:        "Large dataset, enabled",
			datasetSize: 50000,
			config: &SortingConfig{
				ParallelEnabled: true,
				ParallelMinSize: 10000,
			},
			expected: true,
		},
		{
			name:        "Disabled",
			datasetSize: 50000,
			config: &SortingConfig{
				ParallelEnabled: false,
				ParallelMinSize: 10000,
			},
			expected: false,
		},
		{
			name:        "Dataset too small",
			datasetSize: 5000,
			config: &SortingConfig{
				ParallelEnabled: true,
				ParallelMinSize: 10000,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Skip single-CPU test if we actually have multiple CPUs
			if runtime.NumCPU() < 2 && tt.expected == true {
				t.Skip("Skipping test that requires multiple CPUs")
			}

			result := ShouldUseParallelString(tt.datasetSize, tt.config)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// BenchmarkParallelStringSort benchmarks parallel string sorting
func BenchmarkParallelStringSort(b *testing.B) {
	logger := zap.NewNop().Sugar()

	sizes := []int{1000, 10000, 50000}

	for _, size := range sizes {
		docs := createTestDocumentsWithStrings(size, "name", "user")

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = ParallelStringSort(docs, "name", true, true, runtime.NumCPU(), logger)
			}
		})
	}
}

// BenchmarkParallelStringSort_Workers benchmarks different worker counts
func BenchmarkParallelStringSort_Workers(b *testing.B) {
	logger := zap.NewNop().Sugar()

	docs := createTestDocumentsWithStrings(50000, "name", "user")

	workers := []int{1, 2, 4, 8, 16}

	for _, numWorkers := range workers {
		b.Run(fmt.Sprintf("workers=%d", numWorkers), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = ParallelStringSort(docs, "name", true, true, numWorkers, logger)
			}
		})
	}
}

// BenchmarkParallelStringSort_SIMD compares SIMD vs non-SIMD performance
func BenchmarkParallelStringSort_SIMD(b *testing.B) {
	logger := zap.NewNop().Sugar()

	// Use longer strings to benefit from SIMD
	docs := createTestDocumentsWithStrings(10000, "description", "very_long_string_description_")

	b.Run("WithSIMD", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = ParallelStringSort(docs, "description", true, true, runtime.NumCPU(), logger)
		}
	})

	b.Run("WithoutSIMD", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = ParallelStringSort(docs, "description", true, false, runtime.NumCPU(), logger)
		}
	})
}
