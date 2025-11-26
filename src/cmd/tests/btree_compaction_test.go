package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"syndrdb/src/internal/domain/index/btreeindexV2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestCompaction_BasicFunctionality verifies that compaction runs without errors
// This is a basic smoke test for the compaction functionality
func TestCompaction_BasicFunctionality(t *testing.T) {
	// Create temporary directory for test files
	tempDir, err := os.MkdirTemp("", "btree-compaction-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Use unique database name to avoid test interference
	dbName := fmt.Sprintf("testdb_compaction_%d", time.Now().UnixNano())
	dbPath := filepath.Join("data", dbName)
	os.RemoveAll(dbPath)
	os.MkdirAll(dbPath, 0755)
	defer os.RemoveAll(dbPath)

	// Create test index using DefaultIndexConfig
	config := btreeindexV2.DefaultIndexConfig("testbundle", "testfield", tempDir, dbName)
	config.PageSize = 4096
	config.CacheSize = 100 // Keep cache small for testing
	config.FillFactor = 0.7

	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()

	idx, err := btreeindexV2.CreateBTreeIndex(config, sugaredLogger)
	require.NoError(t, err)
	require.NotNil(t, idx)
	defer idx.Close()

	// Perform compaction on new index
	err = idx.Compact()

	// Compaction should succeed even on empty/new index
	assert.NoError(t, err, "Compaction should succeed on new index")

	t.Log("Basic compaction test passed")
}

// TestCompactionOptions_BasicFunctionality verifies that CompactIndex
// with custom options runs without errors
func TestCompactionOptions_BasicFunctionality(t *testing.T) {
	// Create temporary directory for test files
	tempDir, err := os.MkdirTemp("", "btree-compaction-options-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Use unique database name to avoid test interference
	dbName := fmt.Sprintf("testdb_compaction_opts_%d", time.Now().UnixNano())
	dbPath := filepath.Join("data", dbName)
	os.RemoveAll(dbPath)
	os.MkdirAll(dbPath, 0755)
	defer os.RemoveAll(dbPath)

	// Create test index
	config := btreeindexV2.DefaultIndexConfig("testbundle", "testfield", tempDir, dbName)
	config.PageSize = 4096
	config.CacheSize = 100
	config.FillFactor = 0.7

	// Create a test logger
	logger, _ := zap.NewDevelopment()
	sugaredLogger := logger.Sugar()

	idx, err := btreeindexV2.CreateBTreeIndex(config, sugaredLogger)
	require.NoError(t, err)
	require.NotNil(t, idx)
	defer idx.Close()

	// Compact using CompactIndex with custom options
	// Test compaction with custom options
	options := &btreeindexV2.CompactionOptions{
		MaxPagesToProcess:   1000,
		MinFillFactorTarget: 0.8,
		ForceRebuild:        true,
		PreserveStatistics:  false,
		EnableParallelism:   false,
		MaxProcessingTimeMs: 5000,
	}

	result, err := btreeindexV2.CompactIndex(idx, options)

	// Should succeed
	assert.NoError(t, err, "CompactIndex should succeed")
	assert.NotNil(t, result, "Result should not be nil")

	if result != nil {
		// Verify result structure
		assert.True(t, result.Success, "Compaction should succeed")
		assert.Equal(t, "CompactIndex", result.Operation, "Operation should be CompactIndex")
		t.Logf("Compaction result: operation=%s, success=%v, pages_processed=%d, pages_reclaimed=%d, space_saved=%d",
			result.Operation, result.Success, result.PagesProcessed, result.PagesReclaimed, result.SpaceSaved)
	}
}
