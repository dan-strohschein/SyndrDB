package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"syndrdb/src/internal/backup"
)

// Import backup types
type (
	Manifest     = backup.Manifest
	FileEntry    = backup.FileEntry
	PrimaryDBDoc = backup.PrimaryDBDoc
)

var (
	WriteManifest    = backup.WriteManifest
	ReadManifest     = backup.ReadManifest
	CalculateFileCRC = backup.CalculateFileCRC
	VerifyFileCRC    = backup.VerifyFileCRC
)

// TestWriteManifest verifies manifest serialization
func TestWriteManifest(t *testing.T) {
	manifest := &Manifest{
		BackupVersion:      "1.0",
		Timestamp:          time.Now(),
		DatabaseName:       "testdb",
		ServerVersion:      "1.0.0",
		Compression:        "gzip",
		IncludesIndexes:    true,
		TotalSizeBytes:     1024,
		CompressedSize:     512,
		Files:              []FileEntry{},
		PrimaryDBDocuments: []PrimaryDBDoc{},
	}

	var buf bytes.Buffer
	err := WriteManifest(manifest, &buf)
	if err != nil {
		t.Fatalf("WriteManifest failed: %v", err)
	}

	// Verify JSON is valid
	var result map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &result)
	if err != nil {
		t.Fatalf("Generated JSON is invalid: %v", err)
	}

	// Verify key fields exist
	if result["backup_version"] != "1.0" {
		t.Error("backup_version not serialized correctly")
	}
	if result["database_name"] != "testdb" {
		t.Error("database_name not serialized correctly")
	}
	if result["compression"] != "gzip" {
		t.Error("compression not serialized correctly")
	}
}

// TestReadManifest verifies manifest deserialization
func TestReadManifest(t *testing.T) {
	jsonData := `{
		"backup_version": "1.0",
		"timestamp": "2024-01-01T12:00:00Z",
		"database_name": "testdb",
		"server_version": "1.0.0",
		"compression": "zstd",
		"includes_indexes": true,
		"total_size_bytes": 2048,
		"compressed_size": 1024,
		"files": [],
		"primary_db_documents": []
	}`

	buf := bytes.NewBufferString(jsonData)
	manifest, err := ReadManifest(buf)
	if err != nil {
		t.Fatalf("ReadManifest failed: %v", err)
	}

	if manifest.BackupVersion != "1.0" {
		t.Errorf("Expected version 1.0, got %s", manifest.BackupVersion)
	}
	if manifest.DatabaseName != "testdb" {
		t.Errorf("Expected database testdb, got %s", manifest.DatabaseName)
	}
	if manifest.Compression != "zstd" {
		t.Errorf("Expected compression zstd, got %s", manifest.Compression)
	}
	if !manifest.IncludesIndexes {
		t.Error("IncludesIndexes should be true")
	}
	if manifest.TotalSizeBytes != 2048 {
		t.Errorf("Expected total size 2048, got %d", manifest.TotalSizeBytes)
	}
	if manifest.CompressedSize != 1024 {
		t.Errorf("Expected compressed size 1024, got %d", manifest.CompressedSize)
	}
}

// TestWriteReadManifestRoundTrip verifies write/read consistency
func TestWriteReadManifestRoundTrip(t *testing.T) {
	original := &Manifest{
		BackupVersion:   "1.0",
		Timestamp:       time.Now().Round(time.Second), // Round to avoid precision issues
		DatabaseName:    "production",
		ServerVersion:   "2.0.0",
		Compression:     "gzip",
		IncludesIndexes: true,
		TotalSizeBytes:  5000,
		CompressedSize:  2500,
		Files: []FileEntry{
			{Path: "data/bundle1.dat", SizeBytes: 1000, CRC32: 12345, CompressedSize: 500},
			{Path: "data/bundle2.dat", SizeBytes: 2000, CRC32: 67890, CompressedSize: 1000},
		},
		PrimaryDBDocuments: []PrimaryDBDoc{
			{Bundle: "Databases", DocumentID: "uuid1", Data: map[string]interface{}{"name": "testdb"}},
			{Bundle: "Bundles", DocumentID: "uuid2", Data: map[string]interface{}{"name": "bundle1"}},
		},
	}

	// Write to buffer
	var buf bytes.Buffer
	err := WriteManifest(original, &buf)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read it back
	result, err := ReadManifest(&buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// Compare fields
	if result.BackupVersion != original.BackupVersion {
		t.Error("BackupVersion mismatch")
	}
	if result.DatabaseName != original.DatabaseName {
		t.Error("DatabaseName mismatch")
	}
	if result.Compression != original.Compression {
		t.Error("Compression mismatch")
	}
	if len(result.Files) != len(original.Files) {
		t.Errorf("Expected %d files, got %d", len(original.Files), len(result.Files))
	}
	if len(result.PrimaryDBDocuments) != len(original.PrimaryDBDocuments) {
		t.Errorf("Expected %d documents, got %d", len(original.PrimaryDBDocuments), len(result.PrimaryDBDocuments))
	}
}

// TestCalculateFileCRC verifies CRC calculation
func TestCalculateFileCRC(t *testing.T) {
	tmpDir := t.TempDir()

	// Create temp file
	testFile := filepath.Join(tmpDir, "test.dat")
	content := []byte("Hello, World! This is test data for CRC calculation.")
	err := os.WriteFile(testFile, content, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Calculate CRC
	crc, err := CalculateFileCRC(testFile)
	if err != nil {
		t.Fatalf("CalculateFileCRC failed: %v", err)
	}

	// CRC should be non-zero for non-empty file
	if crc == 0 {
		t.Error("CRC32 is zero for non-empty file")
	}

	// Calculate again - should be same value
	crc2, err := CalculateFileCRC(testFile)
	if err != nil {
		t.Fatalf("Second CRC calculation failed: %v", err)
	}
	if crc != crc2 {
		t.Errorf("CRC values don't match: %d vs %d", crc, crc2)
	}
}

// TestCalculateFileCRC_NonExistent verifies error handling
func TestCalculateFileCRC_NonExistent(t *testing.T) {
	_, err := CalculateFileCRC("/nonexistent/file.dat")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

// TestVerifyFileCRC verifies CRC validation
func TestVerifyFileCRC(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	content := []byte("Test data for verification")
	err := os.WriteFile(testFile, content, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Calculate correct CRC
	correctCRC, err := CalculateFileCRC(testFile)
	if err != nil {
		t.Fatalf("Failed to calculate CRC: %v", err)
	}

	// Verify with correct CRC
	err = VerifyFileCRC(testFile, correctCRC)
	if err != nil {
		t.Errorf("Verification failed with correct CRC: %v", err)
	}

	// Verify with wrong CRC
	wrongCRC := correctCRC + 1
	err = VerifyFileCRC(testFile, wrongCRC)
	if err == nil {
		t.Error("Expected error when CRC doesn't match")
	}
}

// TestManifestFileEntries verifies file entry handling
func TestManifestFileEntries(t *testing.T) {
	manifest := &Manifest{
		Files: []FileEntry{
			{Path: "data/file1.dat", SizeBytes: 100, CRC32: 111, CompressedSize: 50},
			{Path: "data/file2.dat", SizeBytes: 200, CRC32: 222, CompressedSize: 100},
			{Path: "indexes/idx1.idx", SizeBytes: 300, CRC32: 333, CompressedSize: 150},
		},
	}

	// Verify file count
	if len(manifest.Files) != 3 {
		t.Errorf("Expected 3 files, got %d", len(manifest.Files))
	}

	// Verify first file
	if manifest.Files[0].Path != "data/file1.dat" {
		t.Error("File path mismatch")
	}
	if manifest.Files[0].SizeBytes != 100 {
		t.Error("File size mismatch")
	}
	if manifest.Files[0].CRC32 != 111 {
		t.Error("CRC mismatch")
	}

	// Calculate total size
	totalSize := int64(0)
	for _, file := range manifest.Files {
		totalSize += file.SizeBytes
	}
	if totalSize != 600 {
		t.Errorf("Expected total size 600, got %d", totalSize)
	}
}

// TestManifestPrimaryDBDocs verifies Primary DB document handling
func TestManifestPrimaryDBDocs(t *testing.T) {
	manifest := &Manifest{
		PrimaryDBDocuments: []PrimaryDBDoc{
			{
				Bundle:     "Databases",
				DocumentID: "db-uuid-1",
				Data: map[string]interface{}{
					"name":    "production",
					"version": "1.0",
				},
			},
			{
				Bundle:     "Bundles",
				DocumentID: "bundle-uuid-1",
				Data: map[string]interface{}{
					"name":     "users",
					"database": "production",
				},
			},
		},
	}

	// Verify document count
	if len(manifest.PrimaryDBDocuments) != 2 {
		t.Errorf("Expected 2 documents, got %d", len(manifest.PrimaryDBDocuments))
	}

	// Verify first document
	doc := manifest.PrimaryDBDocuments[0]
	if doc.Bundle != "Databases" {
		t.Error("Bundle name mismatch")
	}
	if doc.DocumentID != "db-uuid-1" {
		t.Error("Document ID mismatch")
	}
	if doc.Data["name"] != "production" {
		t.Error("Document data mismatch")
	}

	// Verify second document
	doc2 := manifest.PrimaryDBDocuments[1]
	if doc2.Bundle != "Bundles" {
		t.Error("Second document bundle mismatch")
	}
	if doc2.Data["database"] != "production" {
		t.Error("Second document data mismatch")
	}
}

// TestEmptyManifest verifies handling of empty manifest
func TestEmptyManifest(t *testing.T) {
	manifest := &Manifest{
		BackupVersion:      "1.0",
		Timestamp:          time.Now(),
		DatabaseName:       "empty",
		Files:              []FileEntry{},
		PrimaryDBDocuments: []PrimaryDBDoc{},
	}

	var buf bytes.Buffer
	err := WriteManifest(manifest, &buf)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	result, err := ReadManifest(&buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(result.Files) != 0 {
		t.Error("Expected empty files array")
	}
	if len(result.PrimaryDBDocuments) != 0 {
		t.Error("Expected empty documents array")
	}
}
