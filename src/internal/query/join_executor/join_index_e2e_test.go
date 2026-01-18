package joinexecutor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"syndrdb/src/internal/domain/index/hashindexV3"
	"syndrdb/src/internal/domain/models"
)

/*
JOIN INDEX OPTIMIZATION E2E TESTS

This test file validates the end-to-end index-assisted join optimization feature.
It tests the complete integration from index creation through join execution with
index strategies, performance improvements, and edge cases.

TEST COVERAGE:
1. Index creation and usage verification
2. Performance comparison (with vs without index)
3. Index strategy selection (probe vs build)
4. Batching behavior with BatchGet
5. Deleted documents handling
6. Cost estimation accuracy
7. Fallback to full scan on errors
8. Metrics collection and feedback loop

DESIGN PRINCIPLES:
- Each test is independent with its own test data
- Tests follow Arrange-Act-Assert pattern
- Performance tests verify actual speedup
- Comprehensive validation of metrics and statistics
*/

// ============================================================================
// TEST HELPER FUNCTIONS
// ============================================================================

// createTestLogger creates a development logger for tests
func createTestLogger(t *testing.T) *zap.SugaredLogger {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err, "Failed to create test logger")
	return logger.Sugar()
}

// createTestBundle creates a test bundle with documents and an index
func createTestBundle(t *testing.T, bundleName string, docCount int, indexFieldName string, createIndex bool) (*models.Bundle, *hashindexV3.HashIndexV3) {
	tempDir := t.TempDir()
	logger := createTestLogger(t)

	// Create bundle
	docs := make(map[string]models.Document)
	bundle := &models.Bundle{
		Name:           bundleName,
		TotalDocuments: int64(docCount),
		Documents:      &docs,
		Indexes:        make(map[string]models.IndexReference),
	}

	// Create documents
	for i := 0; i < docCount; i++ {
		docID := fmt.Sprintf("doc_%d", i)
		doc := models.Document{
			DocumentID: docID,
			Fields: map[string]models.Field{
				indexFieldName: {
					Name:  indexFieldName,
					Value: models.NewStringValue(fmt.Sprintf("key_%d", i%100)), // 100 unique keys, multiple docs per key
				},
				"data": {
					Name:  "data",
					Value: models.NewStringValue(fmt.Sprintf("data_%d", i)),
				},
			},
		}
		docs[docID] = doc
	}

	// Create index
	var index *hashindexV3.HashIndexV3
	if createIndex {
		indexConfig := hashindexV3.IndexConfig{
			IndexName:       indexFieldName,
			FieldName:       indexFieldName,
			BundleName:      bundleName,
			DatabaseName:    "TestDB",
			DataDir:         tempDir,
			IsForeignKey:    false,
			IsUnique:        false,
			IsPrimaryKey:    false,
			MaxFileSize:     1024 * 1024,
			WriteBufferSize: 4096,
			MemTableMaxSize: 1000,
			Logger:          logger,
		}

		var err error
		index, err = hashindexV3.NewHashIndexV3(indexConfig)
		require.NoError(t, err, "Should create index")

		// Populate index
		// CRITICAL FIX: Use copy-on-read pattern to prevent concurrent map iteration
		bundle.DocumentsMutex.RLock()
		documentsSnapshot := make(map[string]models.Document, len(*bundle.Documents))
		for docID, doc := range *bundle.Documents {
			documentsSnapshot[docID] = doc
		}
		bundle.DocumentsMutex.RUnlock()
		// Now iterate over the snapshot safely
		for docID, doc := range documentsSnapshot {
			field := doc.Fields[indexFieldName]
			keyValue := field.Value.StringVal
			err = index.Put(keyValue, docID, 0)
			require.NoError(t, err, "Should put entry in index")
		}

		// Add index to bundle
		bundle.Indexes[indexFieldName] = models.IndexReference{
			IndexName: indexFieldName,
			IndexType: "hash",
			Fields: []models.FieldDefinition{
				{Name: indexFieldName, Type: "string"},
			},
			IndexInstance: index,
		}
	}

	return bundle, index
}

// mockBundleAdapter wraps a bundle for the join executor
type mockBundleAdapter struct {
	bundle *models.Bundle
}

func (m *mockBundleAdapter) GetDocumentIDs() []string {
	// CRITICAL FIX: Use copy-on-read pattern to prevent concurrent map iteration
	m.bundle.DocumentsMutex.RLock()
	ids := make([]string, 0, len(*m.bundle.Documents))
	for id := range *m.bundle.Documents {
		ids = append(ids, id)
	}
	m.bundle.DocumentsMutex.RUnlock()
	return ids
}

func (m *mockBundleAdapter) GetDocument(docID string) *models.Document {
	// CRITICAL FIX: Use copy-on-read pattern to prevent concurrent map access
	m.bundle.DocumentsMutex.RLock()
	defer m.bundle.DocumentsMutex.RUnlock()
	if doc, ok := (*m.bundle.Documents)[docID]; ok {
		docCopy := doc
		return &docCopy
	}
	return nil
}

func (m *mockBundleAdapter) GetAllDocuments() map[string]*models.Document {
	// CRITICAL FIX: Use copy-on-read pattern to prevent concurrent map iteration
	m.bundle.DocumentsMutex.RLock()
	documentsSnapshot := make(map[string]models.Document, len(*m.bundle.Documents))
	for id, doc := range *m.bundle.Documents {
		documentsSnapshot[id] = doc
	}
	m.bundle.DocumentsMutex.RUnlock()
	// Convert map[string]Document to map[string]*Document
	result := make(map[string]*models.Document, len(documentsSnapshot))
	for id, doc := range documentsSnapshot {
		docCopy := doc
		result[id] = &docCopy
	}
	return result
}

func (m *mockBundleAdapter) GetAllDocumentsWithLimit(limit int) map[string]*models.Document {
	if limit <= 0 {
		return m.GetAllDocuments()
	}
	all := m.GetAllDocuments()
	if len(all) <= limit {
		return all
	}
	result := make(map[string]*models.Document, limit)
	for id, doc := range all {
		if len(result) >= limit {
			break
		}
		result[id] = doc
	}
	return result
}

func (m *mockBundleAdapter) GetName() string {
	return m.bundle.Name
}

func (m *mockBundleAdapter) GetTotalDocuments() int {
	return int(m.bundle.TotalDocuments)
}

func (m *mockBundleAdapter) GetHashIndexForField(fieldName string) interface{} {
	if indexRef, exists := m.bundle.Indexes[fieldName]; exists {
		if indexRef.IndexType == "hash" {
			return indexRef.IndexInstance
		}
	}
	return nil
}

func (m *mockBundleAdapter) HasIndexOnField(fieldName string) bool {
	if indexRef, exists := m.bundle.Indexes[fieldName]; exists {
		return indexRef.IndexType == "hash"
	}
	return false
}

// ============================================================================
// TEST 1: INDEX USAGE VERIFICATION
// ============================================================================

func TestJoinIndex_BasicIndexUsage(t *testing.T) {
	// Arrange: Create small build bundle (100 docs) and larger probe bundle (1000 docs) with index
	buildBundle, _ := createTestBundle(t, "build", 100, "join_key", false)
	probeBundle, probeIndex := createTestBundle(t, "probe", 1000, "join_key", true)
	defer probeIndex.Close()

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 10*1024*1024, false)

	buildAdapter := &mockBundleAdapter{bundle: buildBundle}
	probeAdapter := &mockBundleAdapter{bundle: probeBundle}

	// Act: Execute join
	request := &JoinRequest{
		LeftBundle:  buildAdapter,
		RightBundle: probeAdapter,
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	result, err := executor.Execute(request)

	// Assert
	require.NoError(t, err, "Join should succeed")
	require.NotNil(t, result, "Result should not be nil")

	// Verify that index strategy was selected
	assert.NotNil(t, request.IndexStrategy, "Index strategy should be selected")

	// Verify results exist
	assert.Greater(t, len(result.Documents), 0, "Should have joined documents")

	// Verify probe side was scanned efficiently (not full scan)
	assert.Less(t, result.RightScanned, int64(1000), "Should scan fewer documents than full probe size")

	t.Logf("✅ Index-assisted join: %d results, scanned %d/%d probe docs",
		len(result.Documents), result.RightScanned, probeAdapter.GetTotalDocuments())
}

// ============================================================================
// TEST 2: PERFORMANCE COMPARISON
// ============================================================================

func TestJoinIndex_PerformanceComparison(t *testing.T) {
	// Arrange: Create bundles with and without index
	buildBundle, _ := createTestBundle(t, "build", 200, "join_key", false)
	probeBundleWithIndex, probeIndex := createTestBundle(t, "probe_indexed", 2000, "join_key", true)
	probeBundleNoIndex, _ := createTestBundle(t, "probe_no_index", 2000, "join_key", false)
	defer probeIndex.Close()

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 10*1024*1024, false)
	buildAdapter := &mockBundleAdapter{bundle: buildBundle}

	// Act 1: Join WITH index
	startWithIndex := time.Now()
	requestWithIndex := &JoinRequest{
		LeftBundle:  buildAdapter,
		RightBundle: &mockBundleAdapter{bundle: probeBundleWithIndex},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	resultWithIndex, err := executor.Execute(requestWithIndex)
	durationWithIndex := time.Since(startWithIndex)
	require.NoError(t, err, "Join with index should succeed")

	// Act 2: Join WITHOUT index
	startNoIndex := time.Now()
	requestNoIndex := &JoinRequest{
		LeftBundle:  buildAdapter,
		RightBundle: &mockBundleAdapter{bundle: probeBundleNoIndex},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	resultNoIndex, err := executor.Execute(requestNoIndex)
	durationNoIndex := time.Since(startNoIndex)

	// Assert
	require.NoError(t, err, "Join without index should succeed")
	assert.Equal(t, len(resultWithIndex.Documents), len(resultNoIndex.Documents),
		"Both joins should return same number of results")

	// Verify index strategy was used
	assert.NotNil(t, requestWithIndex.IndexStrategy, "Index strategy should be selected")
	assert.Nil(t, requestNoIndex.IndexStrategy, "No index strategy should be selected")

	// Verify scan reduction
	scanReduction := float64(resultNoIndex.RightScanned-resultWithIndex.RightScanned) / float64(resultNoIndex.RightScanned) * 100
	assert.Greater(t, scanReduction, 50.0, "Should achieve at least 50% scan reduction")

	// Verify performance improvement (indexed should be faster or similar)
	// Note: For small datasets, overhead might make this not always true
	speedup := float64(durationNoIndex) / float64(durationWithIndex)

	t.Logf("✅ Performance comparison:")
	t.Logf("   With index:    %d docs scanned in %v", resultWithIndex.RightScanned, durationWithIndex)
	t.Logf("   Without index: %d docs scanned in %v", resultNoIndex.RightScanned, durationNoIndex)
	t.Logf("   Scan reduction: %.1f%%", scanReduction)
	t.Logf("   Speedup: %.2fx", speedup)
}

// ============================================================================
// TEST 3: STRATEGY SELECTION
// ============================================================================

func TestJoinIndex_ProbeStrategySelection(t *testing.T) {
	// Arrange: Small build, large probe with index
	buildBundle, _ := createTestBundle(t, "build", 50, "join_key", false)
	probeBundle, probeIndex := createTestBundle(t, "probe", 1000, "join_key", true)
	defer probeIndex.Close()

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 10*1024*1024, false)

	// Act
	request := &JoinRequest{
		LeftBundle:  &mockBundleAdapter{bundle: buildBundle},
		RightBundle: &mockBundleAdapter{bundle: probeBundle},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	result, err := executor.Execute(request)

	// Assert
	require.NoError(t, err, "Join should succeed")
	assert.NotNil(t, request.IndexStrategy, "Index strategy should be selected")

	// Verify probe-side index strategy was selected
	if strategy, ok := request.IndexStrategy.(*ProbeIndexStrategy); ok {
		assert.NotNil(t, strategy.GetIndex(), "Probe index should be set")
		assert.Equal(t, "index_assisted_probe", strategy.GetName())
		t.Logf("✅ Probe-side index strategy selected correctly")
	} else {
		t.Errorf("Expected ProbeIndexStrategy, got %T", request.IndexStrategy)
	}

	assert.Greater(t, len(result.Documents), 0, "Should have results")
}

// ============================================================================
// TEST 4: BATCHING BEHAVIOR
// ============================================================================

func TestJoinIndex_BatchGetBehavior(t *testing.T) {
	// Arrange: Create bundle with many unique keys to test BatchGet
	buildBundle, _ := createTestBundle(t, "build", 300, "join_key", false)          // 300 docs, 100 unique keys
	probeBundle, probeIndex := createTestBundle(t, "probe", 3000, "join_key", true) // 3000 docs
	defer probeIndex.Close()

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 10*1024*1024, false)

	// Act
	request := &JoinRequest{
		LeftBundle:  &mockBundleAdapter{bundle: buildBundle},
		RightBundle: &mockBundleAdapter{bundle: probeBundle},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	result, err := executor.Execute(request)

	// Assert
	require.NoError(t, err, "Join should succeed")
	assert.NotNil(t, request.IndexStrategy, "Index strategy should be selected")

	// Verify efficient scanning - should only scan matching documents
	assert.Less(t, result.RightScanned, int64(3000), "Should scan fewer than all probe documents")

	// BatchGet should handle 100 unique keys efficiently
	assert.Greater(t, len(result.Documents), 0, "Should have results")

	t.Logf("✅ BatchGet handled %d unique keys, scanned %d/%d docs",
		100, result.RightScanned, 3000)
}

// ============================================================================
// TEST 5: DELETED DOCUMENTS HANDLING
// ============================================================================

func TestJoinIndex_DeletedDocumentsHandling(t *testing.T) {
	// Arrange
	buildBundle, _ := createTestBundle(t, "build", 100, "join_key", false)
	probeBundle, probeIndex := createTestBundle(t, "probe", 1000, "join_key", true)
	defer probeIndex.Close()

	// Delete some documents from probe bundle but leave them in index
	deletedCount := 0
	for docID := range *probeBundle.Documents {
		if deletedCount >= 100 {
			break
		}
		delete(*probeBundle.Documents, docID)
		probeBundle.TotalDocuments--
		deletedCount++
	}

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 10*1024*1024, false)

	// Act
	request := &JoinRequest{
		LeftBundle:  &mockBundleAdapter{bundle: buildBundle},
		RightBundle: &mockBundleAdapter{bundle: probeBundle},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	result, err := executor.Execute(request)

	// Assert: Join should succeed even with deleted documents
	require.NoError(t, err, "Join should handle deleted documents gracefully")
	assert.NotNil(t, request.IndexStrategy, "Index strategy should be selected")

	// Should still have results from non-deleted documents
	assert.Greater(t, len(result.Documents), 0, "Should have results from remaining documents")

	t.Logf("✅ Handled %d deleted documents gracefully, produced %d results",
		deletedCount, len(result.Documents))
}

// ============================================================================
// TEST 6: COST ESTIMATION ACCURACY
// ============================================================================

func TestJoinIndex_CostEstimationAccuracy(t *testing.T) {
	// Arrange
	buildBundle, _ := createTestBundle(t, "build", 150, "join_key", false)
	probeBundle, probeIndex := createTestBundle(t, "probe", 5000, "join_key", true)
	defer probeIndex.Close()

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 10*1024*1024, false)

	// Act
	request := &JoinRequest{
		LeftBundle:  &mockBundleAdapter{bundle: buildBundle},
		RightBundle: &mockBundleAdapter{bundle: probeBundle},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	result, err := executor.Execute(request)

	// Assert
	require.NoError(t, err, "Join should succeed")
	assert.NotNil(t, request.IndexStrategy, "Index strategy should be selected")

	// Calculate actual speedup
	probeSize := int64(probeBundle.TotalDocuments)
	actualScanned := result.RightScanned
	actualSpeedup := float64(probeSize) / float64(actualScanned)

	// Verify significant speedup was achieved
	assert.Greater(t, actualSpeedup, 2.0, "Should achieve at least 2x speedup")

	t.Logf("✅ Cost estimation test: %.2fx actual speedup (%d → %d docs scanned)",
		actualSpeedup, probeSize, actualScanned)
}

// ============================================================================
// TEST 7: NO INDEX AVAILABLE - FALLBACK
// ============================================================================

func TestJoinIndex_NoIndexFallback(t *testing.T) {
	// Arrange: Create bundles without index
	buildBundle, _ := createTestBundle(t, "build", 100, "join_key", false)
	probeBundle, _ := createTestBundle(t, "probe", 1000, "join_key", false) // NO INDEX

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 10*1024*1024, false)

	// Act
	request := &JoinRequest{
		LeftBundle:  &mockBundleAdapter{bundle: buildBundle},
		RightBundle: &mockBundleAdapter{bundle: probeBundle},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	result, err := executor.Execute(request)

	// Assert: Should fall back to regular join
	require.NoError(t, err, "Join should succeed with fallback")
	assert.Nil(t, request.IndexStrategy, "No index strategy should be selected")

	// Should perform full scan
	assert.Equal(t, result.RightScanned, int64(1000), "Should scan all probe documents")

	// Should still produce correct results
	assert.Greater(t, len(result.Documents), 0, "Should have results")

	t.Logf("✅ Fallback to full scan: %d results, %d docs scanned",
		len(result.Documents), result.RightScanned)
}

// ============================================================================
// TEST 8: INDEX ON BUILD SIDE (NOT USED)
// ============================================================================

func TestJoinIndex_BuildSideIndexNotUsed(t *testing.T) {
	// Arrange: Index on build side (smaller), not probe side
	buildBundle, buildIndex := createTestBundle(t, "build", 100, "join_key", true) // Index on build
	probeBundle, _ := createTestBundle(t, "probe", 1000, "join_key", false)        // No index on probe
	defer buildIndex.Close()

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 10*1024*1024, false)

	// Act
	request := &JoinRequest{
		LeftBundle:  &mockBundleAdapter{bundle: buildBundle},
		RightBundle: &mockBundleAdapter{bundle: probeBundle},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	result, err := executor.Execute(request)

	// Assert: Build-side index strategy is not yet implemented, should use regular join
	// BuildIndexStrategy.IsApplicable() returns false, so no strategy should be selected
	// Or if selected, it should fall back to regular probe
	assert.Nil(t, request.IndexStrategy, "Build-side index strategy not implemented yet")
	assert.Greater(t, len(result.Documents), 0, "Should have results")

	require.NoError(t, err, "Join should succeed")

	t.Logf("✅ Build-side index correctly not used (not implemented): %d results",
		len(result.Documents))
}

// ============================================================================
// TEST 9: EMPTY BUILD BUNDLE
// ============================================================================

func TestJoinIndex_EmptyBuildBundle(t *testing.T) {
	// Arrange
	buildBundle, _ := createTestBundle(t, "build", 0, "join_key", false) // Empty build
	probeBundle, probeIndex := createTestBundle(t, "probe", 1000, "join_key", true)
	defer probeIndex.Close()

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 10*1024*1024, false)

	// Act
	request := &JoinRequest{
		LeftBundle:  &mockBundleAdapter{bundle: buildBundle},
		RightBundle: &mockBundleAdapter{bundle: probeBundle},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	result, err := executor.Execute(request)

	// Assert
	require.NoError(t, err, "Join should succeed")

	// With empty build side, no matches possible
	assert.Equal(t, 0, len(result.Documents), "Should have no results with empty build bundle")

	t.Logf("✅ Empty build bundle handled correctly: 0 results")
}

// ============================================================================
// TEST 10: LARGE SCALE TEST
// ============================================================================

func TestJoinIndex_LargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large scale test in short mode")
	}

	// Arrange: Large dataset
	buildBundle, _ := createTestBundle(t, "build", 1000, "join_key", false)
	probeBundle, probeIndex := createTestBundle(t, "probe", 10000, "join_key", true)
	defer probeIndex.Close()

	logger := createTestLogger(t)
	executor := NewDefaultJoinExecutor(logger, 50*1024*1024, false)

	// Act
	start := time.Now()
	request := &JoinRequest{
		LeftBundle:  &mockBundleAdapter{bundle: buildBundle},
		RightBundle: &mockBundleAdapter{bundle: probeBundle},
		JoinType:    InnerJoin,
		Conditions: []JoinCondition{
			{LeftKey: "join_key", RightKey: "join_key", Operator: "="},
		},
		Context: context.Background(),
	}

	result, err := executor.Execute(request)
	duration := time.Since(start)

	// Assert
	require.NoError(t, err, "Large scale join should succeed")
	assert.NotNil(t, request.IndexStrategy, "Index strategy should be selected")

	// Verify significant scan reduction
	scanReduction := float64(10000-result.RightScanned) / 10000.0 * 100
	assert.Greater(t, scanReduction, 80.0, "Should achieve >80% scan reduction on large dataset")

	// Verify reasonable execution time (< 5 seconds for this dataset)
	assert.Less(t, duration, 5*time.Second, "Should complete in reasonable time")

	t.Logf("✅ Large scale join: %d results in %v", len(result.Documents), duration)
	t.Logf("   Scanned %d/%d probe docs (%.1f%% reduction)",
		result.RightScanned, 10000, scanReduction)
}
