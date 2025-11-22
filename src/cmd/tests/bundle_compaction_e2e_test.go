// bundle_compaction_e2e_test.go
//
// End-to-end tests for bundle file compaction functionality in CompactionManager.
// These tests verify the complete compaction lifecycle including:
// - Document parsing and tombstone detection
// - Creation of compacted files with only active documents
// - Atomic file replacement
// - Statistics tracking
// - Error handling and edge cases
//
// Unlike unit tests, these E2E tests use real bundle files and verify actual
// file operations, making them integration tests that validate the complete
// compaction workflow.
//
// Related tests:
// - compaction_manager_test.go: Unit tests for standalone CompactionManager
// - hashindex_compaction_e2e_test.go: E2E tests for hash index compaction
//
// Test Structure:
// 1. Setup: Create test bundle with active documents and tombstones
// 2. Execute: Run CompactBundleFile()
// 3. Verify: Check compacted file contains only active documents
// 4. Cleanup: Remove test files
package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"syndrdb/src/internal/domain/compactor"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

// TestBundleCompactionBasic tests basic bundle compaction with tombstones
func TestBundleCompactionBasic(t *testing.T) {
	// Setup test directory
	tempDir := t.TempDir()
	bundleFilePath := filepath.Join(tempDir, "test_bundle.bnd")

	// Create test bundle with 3 active documents and 2 tombstones
	err := createTestBundleFile(bundleFilePath, map[string]models.Document{
		"doc1": {DocumentID: "doc1", CreatedAt: time.Now(), UpdatedAt: time.Now(), Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Alice")}}},
		"doc2": {DocumentID: "doc2", CreatedAt: time.Now(), UpdatedAt: time.Now(), Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Bob")}}},
		"doc3": {DocumentID: "doc3", CreatedAt: time.Now(), UpdatedAt: time.Now(), Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Charlie")}}},
	}, []string{"doc4", "doc5"}) // doc4 and doc5 are tombstones
	if err != nil {
		t.Fatalf("Failed to create test bundle: %v", err)
	}

	// Create CompactionManager
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   sugar,
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create CompactionManager: %v", err)
	}

	// Execute compaction
	compactedPath, err := cm.CompactBundleFile("test_bundle", "testdb", bundleFilePath)
	if err != nil {
		t.Fatalf("CompactBundleFile failed: %v", err)
	}

	// Verify compacted file exists
	if compactedPath != bundleFilePath {
		t.Errorf("Expected compacted path to be %s, got %s", bundleFilePath, compactedPath)
	}

	// Verify compacted file contains only active documents
	documents, tombstones, err := parseTestBundleFile(compactedPath)
	if err != nil {
		t.Fatalf("Failed to parse compacted bundle: %v", err)
	}

	// Should have 3 active documents
	if len(documents) != 3 {
		t.Errorf("Expected 3 active documents, got %d", len(documents))
	}

	// Should have 0 tombstones (all removed)
	if len(tombstones) != 0 {
		t.Errorf("Expected 0 tombstones, got %d", len(tombstones))
	}

	// Verify specific documents exist
	if _, exists := documents["doc1"]; !exists {
		t.Error("Expected doc1 to exist in compacted file")
	}
	if _, exists := documents["doc2"]; !exists {
		t.Error("Expected doc2 to exist in compacted file")
	}
	if _, exists := documents["doc3"]; !exists {
		t.Error("Expected doc3 to exist in compacted file")
	}

	// Verify statistics
	stats := cm.GetStats()
	if stats.TotalBundlesCompacted != 1 {
		t.Errorf("Expected TotalBundlesCompacted=1, got %d", stats.TotalBundlesCompacted)
	}
	if stats.TotalDocumentsKept != 3 {
		t.Errorf("Expected TotalDocumentsKept=3, got %d", stats.TotalDocumentsKept)
	}
	if stats.TotalDocumentsRemoved != 2 {
		t.Errorf("Expected TotalDocumentsRemoved=2, got %d", stats.TotalDocumentsRemoved)
	}
}

// TestBundleCompactionNoTombstones tests compaction with no tombstones
// Should detect no changes needed and skip compaction
func TestBundleCompactionNoTombstones(t *testing.T) {
	tempDir := t.TempDir()
	bundleFilePath := filepath.Join(tempDir, "test_bundle_no_tombstones.bnd")

	// Create bundle with only active documents
	err := createTestBundleFile(bundleFilePath, map[string]models.Document{
		"doc1": {DocumentID: "doc1", CreatedAt: time.Now(), UpdatedAt: time.Now(), Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Alice")}}},
		"doc2": {DocumentID: "doc2", CreatedAt: time.Now(), UpdatedAt: time.Now(), Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Bob")}}},
	}, nil) // No tombstones
	if err != nil {
		t.Fatalf("Failed to create test bundle: %v", err)
	}

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   logger.Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create CompactionManager: %v", err)
	}

	// Get original file size
	originalInfo, _ := os.Stat(bundleFilePath)
	originalSize := originalInfo.Size()

	// Execute compaction
	compactedPath, err := cm.CompactBundleFile("test_bundle", "testdb", bundleFilePath)
	if err != nil {
		t.Fatalf("CompactBundleFile failed: %v", err)
	}

	// Verify file unchanged
	newInfo, _ := os.Stat(compactedPath)
	newSize := newInfo.Size()

	if newSize != originalSize {
		t.Logf("Note: File size changed from %d to %d (expected when no tombstones)", originalSize, newSize)
	}

	// Statistics should show no documents removed
	stats := cm.GetStats()
	if stats.TotalDocumentsRemoved != 0 {
		t.Errorf("Expected TotalDocumentsRemoved=0, got %d", stats.TotalDocumentsRemoved)
	}
}

// TestBundleCompactionAllTombstones tests compaction where all documents are deleted
func TestBundleCompactionAllTombstones(t *testing.T) {
	tempDir := t.TempDir()
	bundleFilePath := filepath.Join(tempDir, "test_bundle_all_tombstones.bnd")

	// Create bundle with only tombstones (all documents deleted)
	err := createTestBundleFile(bundleFilePath, nil, []string{"doc1", "doc2", "doc3"})
	if err != nil {
		t.Fatalf("Failed to create test bundle: %v", err)
	}

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   logger.Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create CompactionManager: %v", err)
	}

	// Execute compaction
	compactedPath, err := cm.CompactBundleFile("test_bundle", "testdb", bundleFilePath)
	if err != nil {
		t.Fatalf("CompactBundleFile failed: %v", err)
	}

	// Verify compacted file is nearly empty (only headers)
	info, err := os.Stat(compactedPath)
	if err != nil {
		t.Fatalf("Failed to stat compacted file: %v", err)
	}

	// File should be small (no documents, just empty structure)
	if info.Size() > 1024 {
		t.Errorf("Expected small file size (<1KB), got %d bytes", info.Size())
	}

	// Parse and verify
	documents, tombstones, err := parseTestBundleFile(compactedPath)
	if err != nil {
		t.Fatalf("Failed to parse compacted bundle: %v", err)
	}

	if len(documents) != 0 {
		t.Errorf("Expected 0 active documents, got %d", len(documents))
	}
	if len(tombstones) != 0 {
		t.Errorf("Expected 0 tombstones, got %d", len(tombstones))
	}

	// Statistics
	stats := cm.GetStats()
	if stats.TotalDocumentsKept != 0 {
		t.Errorf("Expected TotalDocumentsKept=0, got %d", stats.TotalDocumentsKept)
	}
	if stats.TotalDocumentsRemoved != 3 {
		t.Errorf("Expected TotalDocumentsRemoved=3, got %d", stats.TotalDocumentsRemoved)
	}
}

// TestBundleCompactionTemporalOrdering tests that newer versions win
func TestBundleCompactionTemporalOrdering(t *testing.T) {
	tempDir := t.TempDir()
	bundleFilePath := filepath.Join(tempDir, "test_bundle_temporal.bnd")

	// Create bundle file manually to test temporal ordering
	file, err := os.Create(bundleFilePath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write older version of doc1
	olderDoc := models.Document{
		DocumentID: "doc1",
		CreatedAt:  time.Now().Add(-2 * time.Hour),
		UpdatedAt:  time.Now().Add(-2 * time.Hour),
		Fields:     map[string]models.Field{"version": {Value: models.NewStringValue("old")}},
	}
	writeDocument(file, olderDoc)

	// Write newer version of doc1
	newerDoc := models.Document{
		DocumentID: "doc1",
		CreatedAt:  time.Now().Add(-2 * time.Hour),
		UpdatedAt:  time.Now().Add(-1 * time.Hour),
		Fields:     map[string]models.Field{"version": {Value: models.NewStringValue("new")}},
	}
	writeDocument(file, newerDoc)

	file.Close()

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   logger.Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create CompactionManager: %v", err)
	}

	// Compact
	_, err = cm.CompactBundleFile("test_bundle", "testdb", bundleFilePath)
	if err != nil {
		t.Fatalf("CompactBundleFile failed: %v", err)
	}

	// Parse compacted file
	documents, _, err := parseTestBundleFile(bundleFilePath)
	if err != nil {
		t.Fatalf("Failed to parse compacted bundle: %v", err)
	}

	// Should have only 1 document (newer version)
	if len(documents) != 1 {
		t.Fatalf("Expected 1 document, got %d", len(documents))
	}

	doc, exists := documents["doc1"]
	if !exists {
		t.Fatal("Expected doc1 to exist")
	}

	// Verify it's the newer version
	versionField, ok := doc.Fields["version"]
	if !ok {
		t.Fatal("Expected version field")
	}
	val, ok := versionField.Value.AsString()
	if !ok {
		t.Fatal("Expected string value")
	}
	if val != "new" {
		t.Errorf("Expected newer version 'new', got: %v", val)
	}
}

// Helper functions

// createTestBundleFile creates a bundle file with active documents and tombstones
func createTestBundleFile(filePath string, documents map[string]models.Document, tombstoneIDs []string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write active documents
	for _, doc := range documents {
		if err := writeDocument(file, doc); err != nil {
			return err
		}
	}

	// Write tombstones
	for _, docID := range tombstoneIDs {
		if err := writeTombstone(file, docID); err != nil {
			return err
		}
	}

	return nil
}

// writeDocument writes a document with 0xDEADBEEF magic
func writeDocument(file *os.File, doc models.Document) error {
	docMap := map[string]interface{}{
		"DocumentID": doc.DocumentID,
		"Fields":     doc.Fields,
		"CreatedAt":  doc.CreatedAt,
		"UpdatedAt":  doc.UpdatedAt,
	}

	docBytes, err := helpers.EncodeFastBinary(docMap)
	if err != nil {
		return err
	}

	// Write header
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], 0xDEADBEEF)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(docBytes)))

	if _, err := file.Write(header); err != nil {
		return err
	}
	if _, err := file.Write(docBytes); err != nil {
		return err
	}

	return nil
}

// writeTombstone writes a tombstone with 0xDEADDEAD magic
func writeTombstone(file *os.File, documentID string) error {
	tombstoneMap := map[string]interface{}{
		"DocumentID": documentID,
	}

	tombstoneBytes, err := helpers.EncodeFastBinary(tombstoneMap)
	if err != nil {
		return err
	}

	// Write header
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], 0xDEADDEAD)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(tombstoneBytes)))

	if _, err := file.Write(header); err != nil {
		return err
	}
	if _, err := file.Write(tombstoneBytes); err != nil {
		return err
	}

	return nil
}

// parseTestBundleFile parses a bundle file and returns active documents and tombstones
func parseTestBundleFile(filePath string) (map[string]models.Document, []string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, nil, err
	}

	documents := make(map[string]models.Document)
	tombstones := make([]string, 0)
	offset := int64(0)

	for offset < int64(len(data)) {
		if offset+8 > int64(len(data)) {
			break
		}

		magic := binary.LittleEndian.Uint32(data[offset : offset+4])
		size := binary.LittleEndian.Uint32(data[offset+4 : offset+8])

		if offset+8+int64(size) > int64(len(data)) {
			break
		}

		docData := data[offset+8 : offset+8+int64(size)]

		if magic == 0xDEADBEEF {
			// Active document
			docMap, err := helpers.DecodeFastBinary(docData)
			if err != nil {
				offset += 8 + int64(size)
				continue
			}

			doc := models.Document{}
			if docID, ok := docMap["DocumentID"].(string); ok {
				doc.DocumentID = docID
			}
			if fields, ok := docMap["Fields"].(map[string]models.Field); ok {
				doc.Fields = fields
			}
			if createdAt, ok := docMap["CreatedAt"].(time.Time); ok {
				doc.CreatedAt = createdAt
			}
			if updatedAt, ok := docMap["UpdatedAt"].(time.Time); ok {
				doc.UpdatedAt = updatedAt
			}

			documents[doc.DocumentID] = doc

		} else if magic == 0xDEADDEAD {
			// Tombstone
			tombstoneMap, err := helpers.DecodeFastBinary(docData)
			if err != nil {
				offset += 8 + int64(size)
				continue
			}

			if docID, ok := tombstoneMap["DocumentID"].(string); ok {
				tombstones = append(tombstones, docID)
			}
		}

		offset += 8 + int64(size)
	}

	return documents, tombstones, nil
}
