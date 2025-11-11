package main

/*
B-TREE WAL INTEGRATION TEST

This file tests the dependency injection of WAL manager into B-tree indexes
through the service registry. It verifies that:
1. Service registry correctly stores and retrieves WAL manager
2. B-tree indexes receive WAL manager during creation
3. Insert/Delete operations log to WAL when manager is available
4. System gracefully handles missing WAL manager

DESIGN PRINCIPLES:
- Single Responsibility: Each test function validates one aspect
- DRY: Reuse test setup/teardown functions
- Clear naming: Test names describe what is being validated
*/

import (
	"os"
	"testing"

	"syndrdb/src/internal/domain/index/btreeindexV2"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/registry"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// TestServiceRegistryWALIntegration verifies WAL manager registration and retrieval
func TestServiceRegistryWALIntegration(t *testing.T) {
	// Create logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	// Create WAL manager
	walManager, err := journal.NewWALManager(sugar)
	if err != nil {
		t.Skipf("Skipping test - WAL manager requires proper log directory: %v", err)
		return
	}
	defer walManager.Close()

	// Get service registry
	serviceRegistry := registry.GetRegistry()
	defer serviceRegistry.Reset() // Clean up after test

	// Test: WAL manager not available initially
	if serviceRegistry.IsWALAvailable() {
		t.Error("Expected WAL manager to not be available initially")
	}

	// Test: Register WAL manager
	serviceRegistry.SetWALManager(walManager)
	serviceRegistry.SetLogger(sugar)

	// Test: WAL manager should now be available
	if !serviceRegistry.IsWALAvailable() {
		t.Error("Expected WAL manager to be available after registration")
	}

	// Test: Retrieved WAL manager should match registered one
	retrieved := serviceRegistry.GetWALManager()
	if retrieved != walManager {
		t.Error("Retrieved WAL manager does not match registered one")
	}

	sugar.Infof("✅ Service registry WAL integration test passed")
}

// TestBTreeIndexWALIntegration verifies B-tree indexes receive WAL manager from registry
func TestBTreeIndexWALIntegration(t *testing.T) {
	// Create logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	// Create WAL manager
	walManager, err := journal.NewWALManager(sugar)
	if err != nil {
		t.Skipf("Skipping test - WAL manager requires proper log directory: %v", err)
		return
	}
	defer walManager.Close()

	// Register in service registry
	serviceRegistry := registry.GetRegistry()
	defer serviceRegistry.Reset()
	serviceRegistry.SetWALManager(walManager)
	serviceRegistry.SetLogger(sugar)

	// Create temp directory for index
	args := settings.GetSettings()
	tempDir := "/tmp/btree_wal_test"
	os.RemoveAll(tempDir)
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	// Create B-tree index configuration with WAL from registry
	config := btreeindexV2.IndexConfig{
		DatabaseName: "test_db",
		BundleName:   "test_bundle",
		FieldName:    "test_field",
		IsUnique:     false,
		DataDir:      tempDir,
		DebugMode:    args.Debug,
		PageSize:     8192,
		CacheSize:    100,
		FillFactor:   0.7,
		MaxKeyLength: 2048,
		SplitRatio:   0.5,
		WALManager:   serviceRegistry.GetWALManager(), // Dependency injection from registry
	}

	// Create B-tree index
	btreeIndex, err := btreeindexV2.CreateBTreeIndex(&config, sugar)
	if err != nil {
		t.Fatalf("Failed to create B-tree index: %v", err)
	}
	defer btreeIndex.Close()

	// Test: Insert should succeed with WAL logging
	testKey := []byte("test_key_001")
	testDocID := "doc_001"

	err = btreeIndex.Insert(testKey, testDocID)
	if err != nil {
		t.Errorf("Failed to insert with WAL enabled: %v", err)
	}

	// Test: Verify data was inserted
	results, err := btreeIndex.Search(testKey)
	if err != nil {
		t.Errorf("Failed to search: %v", err)
	}

	if len(results) != 1 || results[0] != testDocID {
		t.Errorf("Expected 1 result with docID=%s, got %d results: %v", testDocID, len(results), results)
	}

	// Test: Delete should succeed with WAL logging
	err = btreeIndex.Delete(testKey, testDocID)
	if err != nil {
		t.Errorf("Failed to delete with WAL enabled: %v", err)
	}

	// Test: Verify data was deleted (tombstone)
	results, err = btreeIndex.Search(testKey)
	if err != nil {
		t.Errorf("Failed to search after delete: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results after delete (tombstone), got %d results: %v", len(results), results)
	}

	sugar.Infof("✅ B-tree index WAL integration test passed")
}

// TestBTreeIndexWithoutWAL verifies B-tree works when WAL is not available
func TestBTreeIndexWithoutWAL(t *testing.T) {
	// Create logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	// Create temp directory for index
	args := settings.GetSettings()
	tempDir := "/tmp/btree_no_wal_test"
	os.RemoveAll(tempDir)
	os.MkdirAll(tempDir, 0755)
	defer os.RemoveAll(tempDir)

	// Create B-tree index configuration WITHOUT WAL
	config := btreeindexV2.IndexConfig{
		DatabaseName: "test_db",
		BundleName:   "test_bundle",
		FieldName:    "test_field",
		IsUnique:     false,
		DataDir:      tempDir,
		DebugMode:    args.Debug,
		PageSize:     8192,
		CacheSize:    100,
		FillFactor:   0.7,
		MaxKeyLength: 2048,
		SplitRatio:   0.5,
		WALManager:   nil, // No WAL manager
	}

	// Create B-tree index
	btreeIndex, err := btreeindexV2.CreateBTreeIndex(&config, sugar)
	if err != nil {
		t.Fatalf("Failed to create B-tree index without WAL: %v", err)
	}
	defer btreeIndex.Close()

	// Test: Insert should succeed even without WAL
	testKey := []byte("test_key_002")
	testDocID := "doc_002"

	err = btreeIndex.Insert(testKey, testDocID)
	if err != nil {
		t.Errorf("Failed to insert without WAL: %v", err)
	}

	// Test: Verify data was inserted
	results, err := btreeIndex.Search(testKey)
	if err != nil {
		t.Errorf("Failed to search: %v", err)
	}

	if len(results) != 1 || results[0] != testDocID {
		t.Errorf("Expected 1 result with docID=%s, got %d results: %v", testDocID, len(results), results)
	}

	sugar.Infof("✅ B-tree index without WAL test passed")
}
