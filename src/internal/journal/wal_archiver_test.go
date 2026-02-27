package journal

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalFilesystemBackend_StoreAndList(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalFilesystemBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalFilesystemBackend: %v", err)
	}
	defer backend.Close()

	meta := WALArchiveFileEntry{
		FileName:   "2026-02-24.wal",
		FirstLSN:   1,
		LastLSN:    100,
		CRC32:      12345,
		SizeBytes:  1024,
		ArchivedAt: time.Now(),
	}

	reader := strings.NewReader("fake wal data")
	if err := backend.Store("2026-02-24.wal", reader, meta); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Verify file exists on disk
	if _, err := os.Stat(filepath.Join(dir, "2026-02-24.wal")); err != nil {
		t.Errorf("Stored file not found: %v", err)
	}

	// List should return the entry
	entries, err := backend.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0].FileName != "2026-02-24.wal" {
		t.Errorf("Expected filename '2026-02-24.wal', got '%s'", entries[0].FileName)
	}
	if entries[0].FirstLSN != 1 || entries[0].LastLSN != 100 {
		t.Errorf("LSN mismatch: first=%d, last=%d", entries[0].FirstLSN, entries[0].LastLSN)
	}
}

func TestLocalFilesystemBackend_Retrieve(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalFilesystemBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalFilesystemBackend: %v", err)
	}
	defer backend.Close()

	content := "test wal content for retrieve"
	meta := WALArchiveFileEntry{
		FileName:   "2026-02-24.wal",
		ArchivedAt: time.Now(),
	}

	if err := backend.Store("2026-02-24.wal", strings.NewReader(content), meta); err != nil {
		t.Fatalf("Store: %v", err)
	}

	rc, err := backend.Retrieve("2026-02-24.wal")
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != content {
		t.Errorf("Content mismatch: got %q, want %q", string(data), content)
	}
}

func TestLocalFilesystemBackend_Delete(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalFilesystemBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalFilesystemBackend: %v", err)
	}
	defer backend.Close()

	meta := WALArchiveFileEntry{
		FileName:   "2026-02-24.wal",
		ArchivedAt: time.Now(),
	}

	if err := backend.Store("2026-02-24.wal", strings.NewReader("data"), meta); err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := backend.Delete("2026-02-24.wal"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// File should be removed
	if _, err := os.Stat(filepath.Join(dir, "2026-02-24.wal")); !os.IsNotExist(err) {
		t.Errorf("Expected file to be deleted, got err: %v", err)
	}

	// List should be empty
	entries, err := backend.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries after delete, got %d", len(entries))
	}
}

func TestLocalFilesystemBackend_Idempotent(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalFilesystemBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalFilesystemBackend: %v", err)
	}
	defer backend.Close()

	meta := WALArchiveFileEntry{
		FileName:   "2026-02-24.wal",
		ArchivedAt: time.Now(),
	}

	// Store twice — should succeed both times (overwrite)
	if err := backend.Store("2026-02-24.wal", strings.NewReader("data1"), meta); err != nil {
		t.Fatalf("Store 1: %v", err)
	}
	if err := backend.Store("2026-02-24.wal", strings.NewReader("data2"), meta); err != nil {
		t.Fatalf("Store 2: %v", err)
	}

	entries, err := backend.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// Two manifest entries (the archiver deduplicates at the poll level, not the backend)
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries (idempotent store adds to manifest), got %d", len(entries))
	}
}

func TestLocalFilesystemBackend_EmptyManifest(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalFilesystemBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalFilesystemBackend: %v", err)
	}
	defer backend.Close()

	entries, err := backend.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries for new backend, got %d", len(entries))
	}
}

func TestLocalFilesystemBackend_CompressedStore(t *testing.T) {
	dir := t.TempDir()
	backend, err := NewLocalFilesystemBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalFilesystemBackend: %v", err)
	}
	defer backend.Close()

	meta := WALArchiveFileEntry{
		FileName:   "2026-02-24.wal",
		Compressed: true,
		ArchivedAt: time.Now(),
	}

	if err := backend.Store("2026-02-24.wal", strings.NewReader("compressed data"), meta); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Compressed file should exist
	if _, err := os.Stat(filepath.Join(dir, "2026-02-24.wal.compressed")); err != nil {
		t.Errorf("Compressed file not found: %v", err)
	}

	// Retrieve should find the compressed version
	rc, err := backend.Retrieve("2026-02-24.wal")
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	rc.Close()
}
