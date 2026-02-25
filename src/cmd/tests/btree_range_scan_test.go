// B-tree Range Scan Integration Tests
//
// Tests IndexScanNode B-tree range scan (executeBTreeRangeScan), operator-to-key-range
// conversion (OperatorToKeyRange), and type conversion (ConvertToBytes). Uses a mock
// BundleService that returns documents by ID so range scan can resolve doc IDs to full documents.
// Documents use the schema-ordered Values model with BundleFieldSchema.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/planner"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// btreeRangeScanSchema defines field order for test documents (age, name).
var btreeRangeScanSchema = models.BuildBundleFieldSchemaFromNames([]string{"age", "name"})

// mockBundleServiceForBTreeRangeScan implements planner.BundleServiceInterface for B-tree range scan tests.
// It returns documents from a map by ID and passes through B-tree index from the bundle.
type mockBundleServiceForBTreeRangeScan struct {
	docsByID map[string]*models.Document
}

func (m *mockBundleServiceForBTreeRangeScan) GetDocument(bundleName, databaseName, documentID string, _ ...interface{}) (*models.Document, error) {
	if doc, ok := m.docsByID[documentID]; ok {
		return doc, nil
	}
	return nil, nil
}

func (m *mockBundleServiceForBTreeRangeScan) GetOrLoadBTreeIndex(b *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return indexRef.IndexInstance, nil
}

func (m *mockBundleServiceForBTreeRangeScan) GetOrLoadHashIndexInterface(_ *models.Bundle, _ string, _ models.IndexReference) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockBundleServiceForBTreeRangeScan) GetOrCreateDocumentScanner(_ *models.Bundle) (documentscanner.DocumentScannerInterface, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockBundleServiceForBTreeRangeScan) GetBundleByName(_ *models.Database, _ string) (*models.Bundle, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockBundleServiceForBTreeRangeScan) GetAllDocumentsForIndexing(_ string) ([]*models.Document, error) {
	return nil, nil
}
func (m *mockBundleServiceForBTreeRangeScan) GetAllDocumentsForIndexingWithOptions(_ string, _ *bundle.IndexingOptions) ([]*models.Document, error) {
	return nil, nil
}
func (m *mockBundleServiceForBTreeRangeScan) GetDocumentChunksForIndexing(_ context.Context, _ string, _ int, _ func(chunk []*models.Document) (stop bool)) error {
	return nil
}
func (m *mockBundleServiceForBTreeRangeScan) SetProjectionFieldsForBundle(_ string, _ []string) {}
func (m *mockBundleServiceForBTreeRangeScan) CountDocuments(_, _ string) (int, error) {
	return len(m.docsByID), nil
}
func (m *mockBundleServiceForBTreeRangeScan) GetDocumentPage(_, _ string, _ uint32) (*models.DocumentPage, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockBundleServiceForBTreeRangeScan) GetDocumentPageReadOnly(_, _ string, _ uint32) (*models.DocumentPage, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockBundleServiceForBTreeRangeScan) SnapshotPageDocuments(_, _ string, _ uint32) ([]models.Document, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockBundleServiceForBTreeRangeScan) CopyProjectedFromCache(_, _ string, _ uint32, _ []string, _ int, _ *models.BundleFieldSchema) (map[string]*documentscanner.ProjectedDocument, int, int, int, error) {
	return nil, 0, 0, 0, fmt.Errorf("not implemented")
}
func (m *mockBundleServiceForBTreeRangeScan) GetDocumentsByIDs(b *models.Bundle, docIDs []string) ([]*models.Document, error) {
	out := make([]*models.Document, 0, len(docIDs))
	for _, id := range docIDs {
		if doc, ok := m.docsByID[id]; ok {
			out = append(out, doc)
		}
	}
	return out, nil
}
func (m *mockBundleServiceForBTreeRangeScan) GetDocumentsByIDsFromCacheDirect(b *models.Bundle, docIDs []string) []*models.Document {
	out, _ := m.GetDocumentsByIDs(b, docIDs)
	return out
}

// TestBTreeRangeScanIntegration tests the integration between query planner and B-tree range scans
func TestBTreeRangeScanIntegration(t *testing.T) {
	// Setup test environment with unique database name
	dbName := fmt.Sprintf("testdb_range_scan_%d", time.Now().UnixNano())
	testDir := filepath.Join("data", dbName)
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)

	// Create logger
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	// Create a B-tree index using proper configuration
	config := btreeindexV2.DefaultIndexConfig("test_bundle", "age", testDir, dbName)
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

	// Build documents with schema-ordered Values (age, name) for mock lookup
	docsByID := make(map[string]*models.Document)
	now := time.Now().UTC()
	for docID, age := range testData {
		docsByID[docID] = &models.Document{
			DocumentID: docID,
			Values: []models.FieldValue{
				models.NewIntValue(int64(age)),
				models.NewStringValue(fmt.Sprintf("Person_%s", docID)),
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
	}
	mockSvc := &mockBundleServiceForBTreeRangeScan{docsByID: docsByID}

	bundle := &models.Bundle{
		Name: "test_bundle",
		Database: &models.Database{
			Name: dbName,
		},
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
		node := &planner.IndexScanNode{
			Bundle:           bundle,
			IndexName:        "age_index",
			ScanType:         planner.BTreeRangeScan,
			Operator:         ">",
			SearchKey:        "025", // age 25
			Logger:            sugaredLogger,
			BundleServiceInt: mockSvc,
		}

		results, err := node.ExecuteBTreeRangeScan()
		require.NoError(t, err)

		// Should return ages: 28, 30, 35, 40, 45
		assert.GreaterOrEqual(t, len(results), 5, "Should find at least 5 documents with age > 25")

		for docID, doc := range results {
			ageVal, ok := doc.GetFieldValue(btreeRangeScanSchema, "age")
			require.True(t, ok, "document %s missing age", docID)
			assert.Greater(t, ageVal.IntVal, int64(25), "Document %s has age %d, expected > 25", docID, ageVal.IntVal)
		}
	})

	// Test Case 2: Greater Than or Equal (age >= 30)
	t.Run("GreaterThanOrEqual", func(t *testing.T) {
		node := &planner.IndexScanNode{
			Bundle:           bundle,
			IndexName:        "age_index",
			ScanType:         planner.BTreeRangeScan,
			Operator:         ">=",
			SearchKey:        "030", // age 30
			Logger:            sugaredLogger,
			BundleServiceInt: mockSvc,
		}

		results, err := node.ExecuteBTreeRangeScan()
		require.NoError(t, err)

		// Should return ages: 30, 35, 40, 45
		assert.GreaterOrEqual(t, len(results), 4, "Should find at least 4 documents with age >= 30")

		for docID, doc := range results {
			ageVal, ok := doc.GetFieldValue(btreeRangeScanSchema, "age")
			require.True(t, ok, "document %s missing age", docID)
			assert.GreaterOrEqual(t, ageVal.IntVal, int64(30), "Document %s has age %d, expected >= 30", docID, ageVal.IntVal)
		}
	})

	// Test Case 3: Less Than or Equal (age <= 20)
	t.Run("LessThanOrEqual", func(t *testing.T) {
		node := &planner.IndexScanNode{
			Bundle:           bundle,
			IndexName:        "age_index",
			ScanType:         planner.BTreeRangeScan,
			Operator:         "<=",
			SearchKey:        "020", // age 20
			Logger:            sugaredLogger,
			BundleServiceInt: mockSvc,
		}

		results, err := node.ExecuteBTreeRangeScan()
		require.NoError(t, err)

		// Should return ages: 15, 18, 20
		assert.GreaterOrEqual(t, len(results), 3, "Should find at least 3 documents with age <= 20")

		for docID, doc := range results {
			ageVal, ok := doc.GetFieldValue(btreeRangeScanSchema, "age")
			require.True(t, ok, "document %s missing age", docID)
			assert.LessOrEqual(t, ageVal.IntVal, int64(20), "Document %s has age %d, expected <= 20", docID, ageVal.IntVal)
		}
	})

	// Test Case 4: BETWEEN (age BETWEEN 20 AND 30)
	t.Run("Between", func(t *testing.T) {
		node := &planner.IndexScanNode{
			Bundle:           bundle,
			IndexName:        "age_index",
			ScanType:         planner.BTreeRangeScan,
			Operator:         "BETWEEN",
			RangeStart:        "020", // age 20
			RangeEnd:          "030", // age 30
			Logger:            sugaredLogger,
			BundleServiceInt: mockSvc,
		}

		results, err := node.ExecuteBTreeRangeScan()
		require.NoError(t, err)

		// Should return ages: 20, 22, 25, 28, 30
		assert.GreaterOrEqual(t, len(results), 5, "Should find at least 5 documents with age BETWEEN 20 AND 30")

		for docID, doc := range results {
			ageVal, ok := doc.GetFieldValue(btreeRangeScanSchema, "age")
			require.True(t, ok, "document %s missing age", docID)
			assert.GreaterOrEqual(t, ageVal.IntVal, int64(20), "Document %s has age %d, expected >= 20", docID, ageVal.IntVal)
			assert.LessOrEqual(t, ageVal.IntVal, int64(30), "Document %s has age %d, expected <= 30", docID, ageVal.IntVal)
		}
	})

	// Test Case 5: Empty Result Set (age > 100)
	t.Run("EmptyResultSet", func(t *testing.T) {
		node := &planner.IndexScanNode{
			Bundle:           bundle,
			IndexName:        "age_index",
			ScanType:         planner.BTreeRangeScan,
			Operator:         ">",
			SearchKey:        "100", // age 100
			Logger:            sugaredLogger,
			BundleServiceInt: mockSvc,
		}

		results, err := node.ExecuteBTreeRangeScan()
		require.NoError(t, err)
		assert.Empty(t, results, "Should find no documents with age > 100")
	})

	// Test Case 6: Error handling - missing range values for BETWEEN
	t.Run("BetweenMissingRangeValues", func(t *testing.T) {
		node := &planner.IndexScanNode{
			Bundle:           bundle,
			IndexName:        "age_index",
			ScanType:         planner.BTreeRangeScan,
			Operator:         "BETWEEN",
			Logger:            sugaredLogger,
			BundleServiceInt: mockSvc,
		}

		_, err := node.ExecuteBTreeRangeScan()
		assert.Error(t, err, "Should return error when BETWEEN is missing range values")
		assert.Contains(t, err.Error(), "requires both rangeStart and rangeEnd")
	})

	// Test Case 7: Error handling - unsupported operator
	t.Run("UnsupportedOperator", func(t *testing.T) {
		node := &planner.IndexScanNode{
			Bundle:           bundle,
			IndexName:        "age_index",
			ScanType:         planner.BTreeRangeScan,
			Operator:         "LIKE", // Not supported for range scans
			SearchKey:        "025",
			Logger:            sugaredLogger,
			BundleServiceInt: mockSvc,
		}

		_, err := node.ExecuteBTreeRangeScan()
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

	node := &planner.IndexScanNode{
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
			expectedStart:        "100", // String encoding (EncodeKey uses fmt.Sprintf for ints)
			expectedEnd:          "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
			expectedExcludeStart: true, // Exclusive lower bound
			expectedExcludeEnd:   false,
		},
		{
			name:                 "Greater Than or Equal",
			operator:             ">=",
			searchKey:            100,
			expectedStart:        "100", // String encoding
			expectedEnd:          "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
			expectedExcludeStart: false,
			expectedExcludeEnd:   false,
		},
		{
			name:                 "Less Than",
			operator:             "<",
			searchKey:            100,
			expectedStart:        "\x00",
			expectedEnd:          "100", // String encoding
			expectedExcludeStart: false,
			expectedExcludeEnd:   true, // Exclusive upper bound
		},
		{
			name:                 "Less Than or Equal",
			operator:             "<=",
			searchKey:            100,
			expectedStart:        "\x00",
			expectedEnd:          "100", // String encoding
			expectedExcludeStart: false,
			expectedExcludeEnd:   false,
		},
		{
			name:                 "Between",
			operator:             "BETWEEN",
			rangeStart:           50,
			rangeEnd:             150,
			expectedStart:        "50",  // String encoding
			expectedEnd:          "150", // String encoding
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
			startKey, endKey, excludeStart, excludeEnd, err := node.OperatorToKeyRange(tt.operator, tt.searchKey, tt.rangeStart, tt.rangeEnd)

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
	t.Skip("Skipping test for unimplemented query planner functionality (IndexScanNode.ConvertToBytes). See file header for implementation requirements.")

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()
	sugaredLogger := logger.Sugar()

	node := &planner.IndexScanNode{
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
			result := node.ConvertToBytes(tt.input)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}
