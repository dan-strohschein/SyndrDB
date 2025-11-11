package planner

import (
	"fmt"
	"os"
	"testing"

	"syndrdb/src/internal/domain/index/btreeindexV2"
	"syndrdb/src/internal/domain/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestBTreeRangeScanIntegration tests the integration between query planner and B-tree range scans
func TestBTreeRangeScanIntegration(t *testing.T) {
	// Setup test environment
	testDir := "data/testdb/planner_range_test"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)

	// Create logger
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	// Create a B-tree index using proper configuration
	config := btreeindexV2.DefaultIndexConfig("test_bundle", "age", testDir, "testdb")
	config.PageSize = 8192
	config.CacheSize = 20

	btreeIndex, err := btreeindexV2.CreateBTreeIndex(config, sugaredLogger)
	require.NoError(t, err)
	defer btreeIndex.Close()

	// Insert test data: ages from 15 to 45
	testData := map[string]int{
		"doc_001": 15,
		"doc_002": 20,
		"doc_003": 25,
		"doc_004": 30,
		"doc_005": 35,
		"doc_006": 40,
		"doc_007": 45,
		"doc_008": 18,
		"doc_009": 22,
		"doc_010": 28,
	}

	for docID, age := range testData {
		key := []byte(fmt.Sprintf("%03d", age)) // Zero-padded for lexicographic ordering
		err := btreeIndex.Insert(key, docID)
		require.NoError(t, err, "Failed to insert age=%d, docID=%s", age, docID)
	}

	// Create mock bundle with documents
	documents := make(map[string]models.Document)
	for docID, age := range testData {
		documents[docID] = models.Document{
			DocumentID: docID,
			Fields: map[string]models.Field{
				"age": {
					Name:  "age",
					Value: age,
				},
				"name": {
					Name:  "name",
					Value: fmt.Sprintf("Person_%s", docID),
				},
			},
		}
	}

	bundle := &models.Bundle{
		Name:      "test_bundle",
		Documents: &documents,
		Indexes: map[string]models.IndexReference{
			"age_index": {
				IndexName:     "age_index",
				IndexType:     "btree",
				IndexInstance: btreeIndex,
			},
		},
	}

	// Test Case 1: Greater Than (age > 25)
	t.Run("GreaterThan", func(t *testing.T) {
		node := &IndexScanNode{
			Bundle:    bundle,
			IndexName: "age_index",
			ScanType:  BTreeRangeScan,
			Operator:  ">",
			SearchKey: "025", // age 25
			Logger:    sugaredLogger,
		}

		results, err := node.executeBTreeRangeScan()
		require.NoError(t, err)

		// Should return ages: 28, 30, 35, 40, 45
		assert.GreaterOrEqual(t, len(results), 5, "Should find at least 5 documents with age > 25")

		for docID, doc := range results {
			age := doc.Fields["age"].Value.(int)
			assert.Greater(t, age, 25, "Document %s has age %d, expected > 25", docID, age)
		}
	})

	// Test Case 2: Greater Than or Equal (age >= 30)
	t.Run("GreaterThanOrEqual", func(t *testing.T) {
		node := &IndexScanNode{
			Bundle:    bundle,
			IndexName: "age_index",
			ScanType:  BTreeRangeScan,
			Operator:  ">=",
			SearchKey: "030", // age 30
			Logger:    sugaredLogger,
		}

		results, err := node.executeBTreeRangeScan()
		require.NoError(t, err)

		// Should return ages: 30, 35, 40, 45
		assert.GreaterOrEqual(t, len(results), 4, "Should find at least 4 documents with age >= 30")

		for docID, doc := range results {
			age := doc.Fields["age"].Value.(int)
			assert.GreaterOrEqual(t, age, 30, "Document %s has age %d, expected >= 30", docID, age)
		}
	})

	// Test Case 3: Less Than or Equal (age <= 20)
	t.Run("LessThanOrEqual", func(t *testing.T) {
		node := &IndexScanNode{
			Bundle:    bundle,
			IndexName: "age_index",
			ScanType:  BTreeRangeScan,
			Operator:  "<=",
			SearchKey: "020", // age 20
			Logger:    sugaredLogger,
		}

		results, err := node.executeBTreeRangeScan()
		require.NoError(t, err)

		// Should return ages: 15, 18, 20
		assert.GreaterOrEqual(t, len(results), 3, "Should find at least 3 documents with age <= 20")

		for docID, doc := range results {
			age := doc.Fields["age"].Value.(int)
			assert.LessOrEqual(t, age, 20, "Document %s has age %d, expected <= 20", docID, age)
		}
	})

	// Test Case 4: BETWEEN (age BETWEEN 20 AND 30)
	t.Run("Between", func(t *testing.T) {
		node := &IndexScanNode{
			Bundle:     bundle,
			IndexName:  "age_index",
			ScanType:   BTreeRangeScan,
			Operator:   "BETWEEN",
			RangeStart: "020", // age 20
			RangeEnd:   "030", // age 30
			Logger:     sugaredLogger,
		}

		results, err := node.executeBTreeRangeScan()
		require.NoError(t, err)

		// Should return ages: 20, 22, 25, 28, 30
		assert.GreaterOrEqual(t, len(results), 5, "Should find at least 5 documents with age BETWEEN 20 AND 30")

		for docID, doc := range results {
			age := doc.Fields["age"].Value.(int)
			assert.GreaterOrEqual(t, age, 20, "Document %s has age %d, expected >= 20", docID, age)
			assert.LessOrEqual(t, age, 30, "Document %s has age %d, expected <= 30", docID, age)
		}
	})

	// Test Case 5: Empty Result Set (age > 100)
	t.Run("EmptyResultSet", func(t *testing.T) {
		node := &IndexScanNode{
			Bundle:    bundle,
			IndexName: "age_index",
			ScanType:  BTreeRangeScan,
			Operator:  ">",
			SearchKey: "100", // age 100
			Logger:    sugaredLogger,
		}

		results, err := node.executeBTreeRangeScan()
		require.NoError(t, err)
		assert.Empty(t, results, "Should find no documents with age > 100")
	})

	// Test Case 6: Error handling - missing range values for BETWEEN
	t.Run("BetweenMissingRangeValues", func(t *testing.T) {
		node := &IndexScanNode{
			Bundle:    bundle,
			IndexName: "age_index",
			ScanType:  BTreeRangeScan,
			Operator:  "BETWEEN",
			// Missing RangeStart and RangeEnd
			Logger: sugaredLogger,
		}

		_, err := node.executeBTreeRangeScan()
		assert.Error(t, err, "Should return error when BETWEEN is missing range values")
		assert.Contains(t, err.Error(), "requires both rangeStart and rangeEnd")
	})

	// Test Case 7: Error handling - unsupported operator
	t.Run("UnsupportedOperator", func(t *testing.T) {
		node := &IndexScanNode{
			Bundle:    bundle,
			IndexName: "age_index",
			ScanType:  BTreeRangeScan,
			Operator:  "LIKE", // Not supported for range scans
			SearchKey: "025",
			Logger:    sugaredLogger,
		}

		_, err := node.executeBTreeRangeScan()
		assert.Error(t, err, "Should return error for unsupported operator")
		assert.Contains(t, err.Error(), "unsupported range operator")
	})
}

// TestOperatorToKeyRange tests the operator to key range conversion logic
func TestOperatorToKeyRange(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	node := &IndexScanNode{
		Logger: sugaredLogger,
	}

	tests := []struct {
		name                 string
		operator             string
		searchKey            interface{}
		rangeStart           interface{}
		rangeEnd             interface{}
		expectedStart        string
		expectedEnd          string
		expectedExcludeStart bool
		expectedExcludeEnd   bool
		expectError          bool
	}{
		{
			name:                 "Greater Than",
			operator:             ">",
			searchKey:            100,
			expectedStart:        "\x80\x00\x00\x00\x00\x00\x00\x64", // Binary encoding of 100
			expectedEnd:          "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
			expectedExcludeStart: true, // Exclusive lower bound
			expectedExcludeEnd:   false,
		},
		{
			name:                 "Greater Than or Equal",
			operator:             ">=",
			searchKey:            100,
			expectedStart:        "\x80\x00\x00\x00\x00\x00\x00\x64", // Binary encoding of 100
			expectedEnd:          "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
			expectedExcludeStart: false,
			expectedExcludeEnd:   false,
		},
		{
			name:                 "Less Than",
			operator:             "<",
			searchKey:            100,
			expectedStart:        "\x00",
			expectedEnd:          "\x80\x00\x00\x00\x00\x00\x00\x64", // Binary encoding of 100
			expectedExcludeStart: false,
			expectedExcludeEnd:   true, // Exclusive upper bound
		},
		{
			name:                 "Less Than or Equal",
			operator:             "<=",
			searchKey:            100,
			expectedStart:        "\x00",
			expectedEnd:          "\x80\x00\x00\x00\x00\x00\x00\x64", // Binary encoding of 100
			expectedExcludeStart: false,
			expectedExcludeEnd:   false,
		},
		{
			name:                 "Between",
			operator:             "BETWEEN",
			rangeStart:           50,
			rangeEnd:             150,
			expectedStart:        "\x80\x00\x00\x00\x00\x00\x00\x32", // Binary encoding of 50
			expectedEnd:          "\x80\x00\x00\x00\x00\x00\x00\x96", // Binary encoding of 150
			expectedExcludeStart: false,
			expectedExcludeEnd:   false,
		},
		{
			name:        "Between Missing RangeStart",
			operator:    "BETWEEN",
			rangeEnd:    150,
			expectError: true,
		},
		{
			name:        "Unsupported Operator",
			operator:    "CONTAINS",
			searchKey:   "test",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startKey, endKey, excludeStart, excludeEnd, err := node.operatorToKeyRange(tt.operator, tt.searchKey, tt.rangeStart, tt.rangeEnd)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedStart, string(startKey), "Start key mismatch")
				assert.Equal(t, tt.expectedEnd, string(endKey), "End key mismatch")
				assert.Equal(t, tt.expectedExcludeStart, excludeStart, "ExcludeStart flag mismatch")
				assert.Equal(t, tt.expectedExcludeEnd, excludeEnd, "ExcludeEnd flag mismatch")
			}
		})
	}
}

// TestConvertToBytes tests the type conversion to byte slices
func TestConvertToBytes(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	node := &IndexScanNode{
		Logger: sugaredLogger,
	}

	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{
			name:     "String",
			input:    "test_string",
			expected: "test_string",
		},
		{
			name:     "Byte Slice",
			input:    []byte("byte_data"),
			expected: "byte_data",
		},
		{
			name:     "Integer",
			input:    42,
			expected: "42",
		},
		{
			name:     "Float",
			input:    3.14,
			expected: "3.14",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := node.convertToBytes(tt.input)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}
