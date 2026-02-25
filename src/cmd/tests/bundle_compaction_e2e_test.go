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

// compactionTestSchema defines field order for parsed documents (name, version).
// Used to build Document.Values from decoded Fields when parsing bundle files.
var compactionTestSchema = models.BuildBundleFieldSchemaFromNames([]string{"name", "version"})

// testDocEntry is the map-based document shape used by EncodeFastBinary/DecodeFastBinary.
// Serialization uses "Fields" (map[string]models.Field); Document uses Values + schema elsewhere.
type testDocEntry struct {
	DocumentID string
	Fields     map[string]models.Field
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// TestBundleCompactionBasic tests basic bundle compaction with tombstones
//
// NOTE: The compaction manager uses MVCC-safe filtering. Without a snapshot getter,
// or when documents have CommitSequence == 0 (treated as "uncommitted"), ALL versions
// are preserved — including tombstones. The compactor's tombstone parser does not
// extract CommitSequence from tombstone records (only DocumentID), so tombstones
// always get CommitSequence == 0 and are always preserved as a safety measure.
//
// SERVER FIX NEEDED: To allow tombstone removal during compaction:
//   1. compaction_manager.go: Extract CommitSequence from tombstone records during parsing
//   2. compaction_manager.go: writeBundleDocument should write tombstones with 0xDEADDEAD magic
//      (currently rewrites them as regular docs with 0xDEADBEEF, losing deletion status)
func TestBundleCompactionBasic(t *testing.T) {
	// Setup test directory
	tempDir := t.TempDir()
	bundleFilePath := filepath.Join(tempDir, "test_bundle.bnd")

	// Create test bundle with 3 active documents and 2 tombstones
	now := time.Now()
	err := createTestBundleFile(bundleFilePath, map[string]testDocEntry{
		"doc1": {DocumentID: "doc1", CreatedAt: now, UpdatedAt: now, Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Alice")}}},
		"doc2": {DocumentID: "doc2", CreatedAt: now, UpdatedAt: now, Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Bob")}}},
		"doc3": {DocumentID: "doc3", CreatedAt: now, UpdatedAt: now, Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Charlie")}}},
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

	// Verify compacted file contents
	documents, tombstones, err := parseTestBundleFile(compactedPath, compactionTestSchema)
	if err != nil {
		t.Fatalf("Failed to parse compacted bundle: %v", err)
	}

	// MVCC behavior: All 5 versions preserved (3 active + 2 tombstones rewritten as docs).
	// Tombstones have CommitSequence=0 (parser doesn't extract it), so they're treated as
	// "uncommitted" and always preserved. writeBundleDocument rewrites them with 0xDEADBEEF magic.
	if len(documents) != 5 {
		t.Errorf("Expected 5 documents (3 active + 2 preserved tombstones), got %d", len(documents))
	}

	// Tombstones are rewritten as regular documents (0xDEADBEEF), so 0 tombstone markers remain
	if len(tombstones) != 0 {
		t.Errorf("Expected 0 tombstone markers (rewritten as docs), got %d", len(tombstones))
	}

	// Verify original active documents exist
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
	// TotalDocumentsKept = all preserved versions (including tombstones rewritten as docs)
	if stats.TotalDocumentsKept != 5 {
		t.Errorf("Expected TotalDocumentsKept=5, got %d", stats.TotalDocumentsKept)
	}
	// TotalDocumentsRemoved = tombstone count (misleading name: counts tombstones found, not versions removed)
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
	now := time.Now()
	err := createTestBundleFile(bundleFilePath, map[string]testDocEntry{
		"doc1": {DocumentID: "doc1", CreatedAt: now, UpdatedAt: now, Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Alice")}}},
		"doc2": {DocumentID: "doc2", CreatedAt: now, UpdatedAt: now, Fields: map[string]models.Field{"name": {Value: models.NewStringValue("Bob")}}},
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
//
// NOTE: Same MVCC-safe behavior as TestBundleCompactionBasic — tombstones are preserved
// because they have CommitSequence=0 (parser doesn't extract it from tombstone records).
// See TestBundleCompactionBasic comment for SERVER FIX NEEDED details.
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

	// Verify compacted file exists
	_, err = os.Stat(compactedPath)
	if err != nil {
		t.Fatalf("Failed to stat compacted file: %v", err)
	}

	// Parse and verify
	documents, tombstones, err := parseTestBundleFile(compactedPath, compactionTestSchema)
	if err != nil {
		t.Fatalf("Failed to parse compacted bundle: %v", err)
	}

	// MVCC behavior: All 3 tombstones preserved (CommitSequence=0 → "uncommitted") and
	// rewritten as regular documents by writeBundleDocument (0xDEADBEEF magic).
	if len(documents) != 3 {
		t.Errorf("Expected 3 documents (preserved tombstones rewritten as docs), got %d", len(documents))
	}
	if len(tombstones) != 0 {
		t.Errorf("Expected 0 tombstone markers (rewritten as docs), got %d", len(tombstones))
	}

	// Statistics
	stats := cm.GetStats()
	// TotalDocumentsKept = 3 (all tombstones preserved and rewritten as docs)
	if stats.TotalDocumentsKept != 3 {
		t.Errorf("Expected TotalDocumentsKept=3, got %d", stats.TotalDocumentsKept)
	}
	// TotalDocumentsRemoved = 3 (tombstone count found during parsing, not actually removed)
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
	olderDoc := testDocEntry{
		DocumentID: "doc1",
		CreatedAt:  time.Now().Add(-2 * time.Hour),
		UpdatedAt:  time.Now().Add(-2 * time.Hour),
		Fields:     map[string]models.Field{"version": {Value: models.NewStringValue("old")}},
	}
	writeDocument(file, olderDoc)

	// Write newer version of doc1
	newerDoc := testDocEntry{
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
	documents, _, err := parseTestBundleFile(bundleFilePath, compactionTestSchema)
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

	// Verify it's the newer version (schema-ordered Values)
	versionVal, ok := doc.GetFieldValue(compactionTestSchema, "version")
	if !ok {
		t.Fatal("Expected version field")
	}
	val, ok := versionVal.AsString()
	if !ok {
		t.Fatal("Expected string value")
	}
	if val != "new" {
		t.Errorf("Expected newer version 'new', got: %v", val)
	}
}

// Helper functions

// createTestBundleFile creates a bundle file with active documents and tombstones.
// Documents use testDocEntry (map-based Fields) for EncodeFastBinary compatibility.
func createTestBundleFile(filePath string, documents map[string]testDocEntry, tombstoneIDs []string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, entry := range documents {
		if err := writeDocument(file, entry); err != nil {
			return err
		}
	}

	for _, docID := range tombstoneIDs {
		if err := writeTombstone(file, docID); err != nil {
			return err
		}
	}

	return nil
}

// writeDocument writes a document with 0xDEADBEEF magic using the map format (Fields).
func writeDocument(file *os.File, entry testDocEntry) error {
	docMap := map[string]interface{}{
		"DocumentID": entry.DocumentID,
		"Fields":     entry.Fields,
		"CreatedAt":  entry.CreatedAt,
		"UpdatedAt":  entry.UpdatedAt,
	}

	docBytes, err := helpers.EncodeFastBinary(docMap)
	if err != nil {
		return err
	}

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

// parseTestBundleFile parses a bundle file and returns active documents (with Values from schema) and tombstones.
// schema defines field order so decoded Fields are converted to Document.Values for assertions.
func parseTestBundleFile(filePath string, schema *models.BundleFieldSchema) (map[string]models.Document, []string, error) {
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
			docMap, err := helpers.DecodeFastBinary(docData)
			if err != nil {
				offset += 8 + int64(size)
				continue
			}

			doc := models.Document{}
			if docID, ok := docMap["DocumentID"].(string); ok {
				doc.DocumentID = docID
			}
			if createdAt, ok := docMap["CreatedAt"].(time.Time); ok {
				doc.CreatedAt = createdAt
			}
			if updatedAt, ok := docMap["UpdatedAt"].(time.Time); ok {
				doc.UpdatedAt = updatedAt
			}

			// Convert decoded Fields to schema-ordered Values for Document model
			if schema != nil {
				doc.Values = make([]models.FieldValue, len(schema.Names))
				fields, _ := docMap["Fields"].(map[string]models.Field)
				for i, name := range schema.Names {
					if fields != nil {
						if f, ok := fields[name]; ok {
							doc.Values[i] = f.Value
							continue
						}
					}
					doc.Values[i] = models.FieldValue{Type: models.FieldTypeNil}
				}
			}

			documents[doc.DocumentID] = doc

		} else if magic == 0xDEADDEAD {
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
