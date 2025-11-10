package sorting

import (
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// TestGenerateAbbreviatedKey tests the abbreviated key generation
func TestGenerateAbbreviatedKey(t *testing.T) {
	logger := zap.NewNop().Sugar()
	defer logger.Sync()

	tests := []struct {
		name     string
		input    string
		expected uint64
	}{
		{
			name:     "Short string (< 8 chars)",
			input:    "hello",
			expected: 0x68656c6c6f000000, // "hello" + 3 zero bytes
		},
		{
			name:     "Exactly 8 chars",
			input:    "database",
			expected: 0x6461746162617365, // "database"
		},
		{
			name:     "Long string (> 8 chars)",
			input:    "abcdefghijklmn",
			expected: 0x6162636465666768, // First 8: "abcdefgh"
		},
		{
			name:     "Empty string",
			input:    "",
			expected: 0x0000000000000000,
		},
		{
			name:     "Single char",
			input:    "a",
			expected: 0x6100000000000000, // "a" + 7 zero bytes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := &models.Document{DocumentID: "test"}
			key := generateAbbreviatedKey(tt.input, doc)

			if key.Abbreviated != tt.expected {
				t.Errorf("generateAbbreviatedKey(%q) = 0x%016x, want 0x%016x",
					tt.input, key.Abbreviated, tt.expected)
			}

			if key.FullString != tt.input {
				t.Errorf("FullString = %q, want %q", key.FullString, tt.input)
			}

			if key.Document != doc {
				t.Error("Document pointer not preserved")
			}
		})
	}
}

// TestCompareAbbreviatedKeys tests the comparison logic
func TestCompareAbbreviatedKeys(t *testing.T) {
	logger := zap.NewNop().Sugar()
	defer logger.Sync()

	doc1 := &models.Document{DocumentID: "doc1"}
	doc2 := &models.Document{DocumentID: "doc2"}

	tests := []struct {
		name        string
		str1        string
		str2        string
		expectedCmp int // -1, 0, or 1
		useSIMD     bool
		description string
	}{
		{
			name:        "Equal strings",
			str1:        "hello",
			str2:        "hello",
			expectedCmp: 0,
			useSIMD:     true,
			description: "Identical strings should compare as equal",
		},
		{
			name:        "str1 < str2 (abbreviated)",
			str1:        "apple",
			str2:        "banana",
			expectedCmp: -1,
			useSIMD:     true,
			description: "Comparison should work on abbreviated keys",
		},
		{
			name:        "str1 > str2 (abbreviated)",
			str1:        "zebra",
			str2:        "apple",
			expectedCmp: 1,
			useSIMD:     true,
			description: "Comparison should work on abbreviated keys",
		},
		{
			name:        "Same abbreviated, different full (SIMD)",
			str1:        "database_alpha",
			str2:        "database_beta",
			expectedCmp: -1,
			useSIMD:     true,
			description: "Should fall back to SIMD full string comparison",
		},
		{
			name:        "Same abbreviated, different full (no SIMD)",
			str1:        "database_alpha",
			str2:        "database_beta",
			expectedCmp: -1,
			useSIMD:     false,
			description: "Should fall back to standard string comparison",
		},
		{
			name:        "Different lengths",
			str1:        "cat",
			str2:        "category",
			expectedCmp: -1,
			useSIMD:     true,
			description: "Shorter string sorts before longer with same prefix",
		},
		{
			name:        "Empty vs non-empty",
			str1:        "",
			str2:        "hello",
			expectedCmp: -1,
			useSIMD:     true,
			description: "Empty string sorts before any non-empty",
		},
		{
			name:        "Case sensitive",
			str1:        "Apple",
			str2:        "apple",
			expectedCmp: -1, // 'A' < 'a' in ASCII
			useSIMD:     true,
			description: "Comparison should be case-sensitive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key1 := generateAbbreviatedKey(tt.str1, doc1)
			key2 := generateAbbreviatedKey(tt.str2, doc2)

			result := compareAbbreviatedKeys(key1, key2, tt.useSIMD)

			// Normalize result to -1, 0, or 1
			normalized := 0
			if result < 0 {
				normalized = -1
			} else if result > 0 {
				normalized = 1
			}

			if normalized != tt.expectedCmp {
				t.Errorf("%s: compareAbbreviatedKeys(%q, %q, useSIMD=%v) = %d, want %d",
					tt.description, tt.str1, tt.str2, tt.useSIMD, normalized, tt.expectedCmp)
			}
		})
	}
}

// TestStringHeapSort_ASC tests ascending string sorting
func TestStringHeapSort_ASC(t *testing.T) {
	logger := zap.NewNop().Sugar()
	defer logger.Sync()

	// Create test documents with string field
	documents := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "zebra"},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "apple"},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "mango"},
			},
		},
		"doc4": {
			DocumentID: "doc4",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "banana"},
			},
		},
		"doc5": {
			DocumentID: "doc5",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "orange"},
			},
		},
	}

	// Sort by name ASC, LIMIT 3
	results, err := StringHeapSort(
		documents,
		3,      // limit
		"name", // fieldName
		true,   // ascending
		true,   // useSIMD
		false,  // nullsFirst
		logger,
	)

	if err != nil {
		t.Fatalf("StringHeapSort failed: %v", err)
	}

	// Verify we got exactly 3 results
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Verify correct order: apple, banana, mango
	expectedNames := []string{"apple", "banana", "mango"}
	for i, doc := range results {
		nameField, exists := doc.Fields["name"]
		if !exists {
			t.Fatalf("Document %d missing 'name' field", i)
		}

		name := nameField.Value.(string)
		if name != expectedNames[i] {
			t.Errorf("Result[%d] = %q, want %q", i, name, expectedNames[i])
		}
	}

	t.Logf("ASC sort correct: %s, %s, %s",
		results[0].Fields["name"].Value,
		results[1].Fields["name"].Value,
		results[2].Fields["name"].Value)
}

// TestStringHeapSort_DESC tests descending string sorting
func TestStringHeapSort_DESC(t *testing.T) {
	logger := zap.NewNop().Sugar()
	defer logger.Sync()

	// Create test documents
	documents := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"city": {Name: "city", Value: "boston"},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"city": {Name: "city", Value: "seattle"},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"city": {Name: "city", Value: "austin"},
			},
		},
		"doc4": {
			DocumentID: "doc4",
			Fields: map[string]models.Field{
				"city": {Name: "city", Value: "denver"},
			},
		},
		"doc5": {
			DocumentID: "doc5",
			Fields: map[string]models.Field{
				"city": {Name: "city", Value: "portland"},
			},
		},
	}

	// Sort by city DESC, LIMIT 3
	results, err := StringHeapSort(
		documents,
		3,      // limit
		"city", // fieldName
		false,  // ascending (DESC)
		true,   // useSIMD
		false,  // nullsFirst
		logger,
	)

	if err != nil {
		t.Fatalf("StringHeapSort failed: %v", err)
	}

	// Verify we got exactly 3 results
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Verify correct order: seattle, portland, denver
	expectedCities := []string{"seattle", "portland", "denver"}
	for i, doc := range results {
		cityField, exists := doc.Fields["city"]
		if !exists {
			t.Fatalf("Document %d missing 'city' field", i)
		}

		city := cityField.Value.(string)
		if city != expectedCities[i] {
			t.Errorf("Result[%d] = %q, want %q", i, city, expectedCities[i])
		}
	}

	t.Logf("DESC sort correct: %s, %s, %s",
		results[0].Fields["city"].Value,
		results[1].Fields["city"].Value,
		results[2].Fields["city"].Value)
}

// TestStringHeapSort_SIMD_vs_NonSIMD verifies both paths work correctly
func TestStringHeapSort_SIMD_vs_NonSIMD(t *testing.T) {
	logger := zap.NewNop().Sugar()
	defer logger.Sync()

	// Create test documents with long strings to exercise SIMD fallback
	documents := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"description": {Name: "description", Value: "database_alpha_version_1"},
			},
		},
		"doc2": {
			DocumentID: "doc2",
			Fields: map[string]models.Field{
				"description": {Name: "description", Value: "database_beta_version_2"},
			},
		},
		"doc3": {
			DocumentID: "doc3",
			Fields: map[string]models.Field{
				"description": {Name: "description", Value: "database_gamma_version_3"},
			},
		},
	}

	// Test with SIMD
	resultsSIMD, err := StringHeapSort(documents, 2, "description", true, true, false, logger)
	if err != nil {
		t.Fatalf("SIMD sort failed: %v", err)
	}

	// Test without SIMD
	resultsNoSIMD, err := StringHeapSort(documents, 2, "description", true, false, false, logger)
	if err != nil {
		t.Fatalf("Non-SIMD sort failed: %v", err)
	}

	// Both should produce same results
	if len(resultsSIMD) != len(resultsNoSIMD) {
		t.Fatalf("Result count mismatch: SIMD=%d, NoSIMD=%d", len(resultsSIMD), len(resultsNoSIMD))
	}

	for i := range resultsSIMD {
		simdDesc := resultsSIMD[i].Fields["description"].Value.(string)
		noSimdDesc := resultsNoSIMD[i].Fields["description"].Value.(string)

		if simdDesc != noSimdDesc {
			t.Errorf("Result[%d] mismatch: SIMD=%q, NoSIMD=%q", i, simdDesc, noSimdDesc)
		}
	}

	t.Logf("SIMD and non-SIMD paths produce identical results")
}

// TestStringHeapSort_EmptyInput tests edge case
func TestStringHeapSort_EmptyInput(t *testing.T) {
	logger := zap.NewNop().Sugar()
	defer logger.Sync()

	documents := map[string]*models.Document{}

	results, err := StringHeapSort(documents, 10, "name", true, true, false, logger)
	if err != nil {
		t.Fatalf("StringHeapSort failed on empty input: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty input, got %d", len(results))
	}
}

// TestStringHeapSort_ZeroLimit tests edge case
func TestStringHeapSort_ZeroLimit(t *testing.T) {
	logger := zap.NewNop().Sugar()
	defer logger.Sync()

	documents := map[string]*models.Document{
		"doc1": {
			DocumentID: "doc1",
			Fields: map[string]models.Field{
				"name": {Name: "name", Value: "test"},
			},
		},
	}

	results, err := StringHeapSort(documents, 0, "name", true, true, false, logger)
	if err != nil {
		t.Fatalf("StringHeapSort failed with zero limit: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results for limit=0, got %d", len(results))
	}
}

// TestShouldUseStringHeapSort tests the threshold logic
func TestShouldUseStringHeapSort(t *testing.T) {
	tests := []struct {
		name      string
		totalDocs int
		limit     int
		threshold float64
		expected  bool
	}{
		{
			name:      "10% threshold - should use (5%)",
			totalDocs: 100,
			limit:     5,
			threshold: 0.1,
			expected:  true,
		},
		{
			name:      "10% threshold - should not use (15%)",
			totalDocs: 100,
			limit:     15,
			threshold: 0.1,
			expected:  false,
		},
		{
			name:      "10% threshold - exactly at boundary",
			totalDocs: 100,
			limit:     10,
			threshold: 0.1,
			expected:  false, // Equal to threshold, so don't use
		},
		{
			name:      "Empty dataset",
			totalDocs: 0,
			limit:     10,
			threshold: 0.1,
			expected:  false,
		},
		{
			name:      "Zero limit",
			totalDocs: 100,
			limit:     0,
			threshold: 0.1,
			expected:  false,
		},
		{
			name:      "Negative limit",
			totalDocs: 100,
			limit:     -5,
			threshold: 0.1,
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldUseStringHeapSort(tt.totalDocs, tt.limit, tt.threshold)
			if result != tt.expected {
				t.Errorf("ShouldUseStringHeapSort(%d, %d, %.2f) = %v, want %v",
					tt.totalDocs, tt.limit, tt.threshold, result, tt.expected)
			}
		})
	}
}
