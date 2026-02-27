package backup

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadCatalog_MissingFile(t *testing.T) {
	catalog, err := LoadCatalog("/tmp/nonexistent_test_catalog.json")
	if err != nil {
		t.Fatalf("expected nil error for missing file, got: %v", err)
	}
	if catalog == nil {
		t.Fatal("expected non-nil catalog")
	}
	if len(catalog.Entries) != 0 {
		t.Fatalf("expected empty entries, got %d", len(catalog.Entries))
	}
	if catalog.Version != "1.0" {
		t.Fatalf("expected version 1.0, got %s", catalog.Version)
	}
}

func TestSaveCatalog_AndLoadBack(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "catalog.json")

	catalog := &BackupCatalog{
		Version: "1.0",
		Entries: []BackupCatalogEntry{
			{
				BackupID:     "id-001",
				BackupType:   "full",
				DatabaseName: "testdb",
				Timestamp:    time.Now(),
				FilePath:     "/backups/full.sdb",
				SizeBytes:    1024,
				FileCount:    5,
			},
		},
	}

	if err := SaveCatalog(catalog, path); err != nil {
		t.Fatalf("SaveCatalog failed: %v", err)
	}

	// Temp file should not exist
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp file should have been renamed")
	}

	loaded, err := LoadCatalog(path)
	if err != nil {
		t.Fatalf("LoadCatalog failed: %v", err)
	}
	if len(loaded.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(loaded.Entries))
	}
	if loaded.Entries[0].BackupID != "id-001" {
		t.Errorf("expected BackupID id-001, got %s", loaded.Entries[0].BackupID)
	}
}

func TestAddEntry(t *testing.T) {
	catalog := &BackupCatalog{Version: "1.0", Entries: []BackupCatalogEntry{}}
	catalog.AddEntry(BackupCatalogEntry{BackupID: "a"})
	catalog.AddEntry(BackupCatalogEntry{BackupID: "b"})
	if len(catalog.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(catalog.Entries))
	}
}

func TestFindEntry(t *testing.T) {
	catalog := &BackupCatalog{
		Entries: []BackupCatalogEntry{
			{BackupID: "aaa", BackupType: "full"},
			{BackupID: "bbb", BackupType: "incremental"},
		},
	}

	found := catalog.FindEntry("bbb")
	if found == nil {
		t.Fatal("expected to find entry bbb")
	}
	if found.BackupType != "incremental" {
		t.Errorf("expected incremental, got %s", found.BackupType)
	}

	notFound := catalog.FindEntry("ccc")
	if notFound != nil {
		t.Error("expected nil for missing entry")
	}
}

func TestFindEntryByPath(t *testing.T) {
	catalog := &BackupCatalog{
		Entries: []BackupCatalogEntry{
			{BackupID: "aaa", FilePath: "/backups/full.sdb"},
			{BackupID: "bbb", FilePath: "/backups/incr1.sdb"},
		},
	}

	found := catalog.FindEntryByPath("/backups/incr1.sdb")
	if found == nil || found.BackupID != "bbb" {
		t.Error("expected to find entry by path")
	}
}

func TestFindLatestBackupForDB(t *testing.T) {
	now := time.Now()
	catalog := &BackupCatalog{
		Entries: []BackupCatalogEntry{
			{BackupID: "old", DatabaseName: "mydb", Timestamp: now.Add(-time.Hour)},
			{BackupID: "new", DatabaseName: "mydb", Timestamp: now},
			{BackupID: "other", DatabaseName: "otherdb", Timestamp: now.Add(time.Hour)},
		},
	}

	latest := catalog.FindLatestBackupForDB("mydb")
	if latest == nil || latest.BackupID != "new" {
		t.Errorf("expected latest to be 'new', got %v", latest)
	}

	none := catalog.FindLatestBackupForDB("nonexistent")
	if none != nil {
		t.Error("expected nil for nonexistent db")
	}
}

func TestBuildChain_Simple(t *testing.T) {
	catalog := &BackupCatalog{
		Entries: []BackupCatalogEntry{
			{BackupID: "full-1", BackupType: "full", DatabaseName: "db1"},
			{BackupID: "incr-1", BackupType: "incremental", ParentBackupID: "full-1", DatabaseName: "db1", ChainDepth: 1},
			{BackupID: "incr-2", BackupType: "incremental", ParentBackupID: "incr-1", DatabaseName: "db1", ChainDepth: 2},
		},
	}

	chain, err := catalog.BuildChain("incr-2")
	if err != nil {
		t.Fatalf("BuildChain failed: %v", err)
	}
	if len(chain) != 3 {
		t.Fatalf("expected chain length 3, got %d", len(chain))
	}
	// Should be ordered: full, incr-1, incr-2
	if chain[0].BackupID != "full-1" {
		t.Errorf("expected first in chain to be full-1, got %s", chain[0].BackupID)
	}
	if chain[1].BackupID != "incr-1" {
		t.Errorf("expected second in chain to be incr-1, got %s", chain[1].BackupID)
	}
	if chain[2].BackupID != "incr-2" {
		t.Errorf("expected third in chain to be incr-2, got %s", chain[2].BackupID)
	}
}

func TestBuildChain_FullOnly(t *testing.T) {
	catalog := &BackupCatalog{
		Entries: []BackupCatalogEntry{
			{BackupID: "full-1", BackupType: "full"},
		},
	}

	chain, err := catalog.BuildChain("full-1")
	if err != nil {
		t.Fatalf("BuildChain failed: %v", err)
	}
	if len(chain) != 1 {
		t.Fatalf("expected chain length 1, got %d", len(chain))
	}
}

func TestBuildChain_BrokenChain(t *testing.T) {
	catalog := &BackupCatalog{
		Entries: []BackupCatalogEntry{
			{BackupID: "incr-1", BackupType: "incremental", ParentBackupID: "missing-parent"},
		},
	}

	_, err := catalog.BuildChain("incr-1")
	if err == nil {
		t.Fatal("expected error for broken chain")
	}
}

func TestBuildChain_NotFound(t *testing.T) {
	catalog := &BackupCatalog{Entries: []BackupCatalogEntry{}}

	_, err := catalog.BuildChain("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing backup")
	}
}

func TestValidateChain_Empty(t *testing.T) {
	err := ValidateChain([]BackupCatalogEntry{})
	if err == nil {
		t.Fatal("expected error for empty chain")
	}
}

func TestValidateChain_NotFullRoot(t *testing.T) {
	err := ValidateChain([]BackupCatalogEntry{
		{BackupType: "incremental", FilePath: "/tmp/test.sdb"},
	})
	if err == nil {
		t.Fatal("expected error when root is not full")
	}
}

func TestValidateChain_MissingArchive(t *testing.T) {
	err := ValidateChain([]BackupCatalogEntry{
		{BackupType: "full", FilePath: "/nonexistent/full.sdb"},
	})
	if err == nil {
		t.Fatal("expected error for missing archive file")
	}
}

func TestValidateChain_Valid(t *testing.T) {
	// Create temp files to validate against
	tmpDir := t.TempDir()
	fullPath := filepath.Join(tmpDir, "full.sdb")
	incrPath := filepath.Join(tmpDir, "incr.sdb")
	os.WriteFile(fullPath, []byte("full"), 0644)
	os.WriteFile(incrPath, []byte("incr"), 0644)

	err := ValidateChain([]BackupCatalogEntry{
		{BackupType: "full", FilePath: fullPath, EndCommitSeq: 100},
		{BackupType: "incremental", FilePath: incrPath, StartCommitSeq: 100, EndCommitSeq: 200},
	})
	if err != nil {
		t.Fatalf("expected valid chain, got: %v", err)
	}
}

func TestCatalogEntryFromManifest(t *testing.T) {
	m := &Manifest{
		BackupID:       "test-id",
		BackupType:     "incremental",
		ParentBackupID: "parent-id",
		BaseBackupID:   "base-id",
		ChainDepth:     2,
		DatabaseName:   "mydb",
		Timestamp:      time.Now(),
		StartLSN:       100,
		EndLSN:         200,
		StartCommitSeq: 50,
		EndCommitSeq:   75,
		Files:          []FileEntry{{Path: "a.bnd"}, {Path: "b.bnd"}},
	}

	entry := catalogEntryFromManifest(m, "/fake/path.sdb")
	if entry.BackupID != "test-id" {
		t.Errorf("expected BackupID test-id, got %s", entry.BackupID)
	}
	if entry.BackupType != "incremental" {
		t.Errorf("expected type incremental, got %s", entry.BackupType)
	}
	if entry.FileCount != 2 {
		t.Errorf("expected file count 2, got %d", entry.FileCount)
	}
	if entry.ChainDepth != 2 {
		t.Errorf("expected chain depth 2, got %d", entry.ChainDepth)
	}
}

func TestCatalogEntryFromManifest_EmptyType(t *testing.T) {
	m := &Manifest{DatabaseName: "db"}
	entry := catalogEntryFromManifest(m, "/fake/path.sdb")
	if entry.BackupType != "full" {
		t.Errorf("expected default type 'full', got '%s'", entry.BackupType)
	}
}

func TestSaveCatalog_CreatesDirIfNeeded(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "sub", "dir", "catalog.json")

	catalog := &BackupCatalog{Version: "1.0", Entries: []BackupCatalogEntry{}}
	if err := SaveCatalog(catalog, path); err != nil {
		t.Fatalf("SaveCatalog should create dirs: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatal("catalog file should exist")
	}
}

func TestLoadCatalog_Corrupted(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.json")
	os.WriteFile(path, []byte("not json"), 0644)

	_, err := LoadCatalog(path)
	if err == nil {
		t.Fatal("expected error for corrupted JSON")
	}
}
