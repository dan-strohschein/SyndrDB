package backup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPITRRecoveryTarget_Types(t *testing.T) {
	// Verify target type constants
	if PITRTargetTimestamp != 1 {
		t.Errorf("PITRTargetTimestamp should be 1, got %d", PITRTargetTimestamp)
	}
	if PITRTargetLSN != 2 {
		t.Errorf("PITRTargetLSN should be 2, got %d", PITRTargetLSN)
	}
	if PITRTargetRestorePoint != 3 {
		t.Errorf("PITRTargetRestorePoint should be 3, got %d", PITRTargetRestorePoint)
	}
}

func TestPITRRecoveryOptions_Fields(t *testing.T) {
	opts := PITRRecoveryOptions{
		BackupPath:    "/backups/test.sdb",
		TargetDBName:  "mydb_recovered",
		WALArchiveDir: "/wal_archive",
		Target: PITRRecoveryTarget{
			Type:       PITRTargetTimestamp,
			TargetTime: time.Now(),
		},
		Force:  true,
		DryRun: false,
	}

	if opts.BackupPath != "/backups/test.sdb" {
		t.Errorf("Unexpected backup path: %s", opts.BackupPath)
	}
	if opts.TargetDBName != "mydb_recovered" {
		t.Errorf("Unexpected target DB: %s", opts.TargetDBName)
	}
	if !opts.Force {
		t.Error("Expected Force=true")
	}
}

func TestValidateRecoveryChain_NoPITR(t *testing.T) {
	ps := &PITRService{}
	manifest := &Manifest{
		PITREnabled: false,
	}

	err := ps.ValidateRecoveryChain(manifest, PITRRecoveryTarget{Type: PITRTargetLSN, TargetLSN: 100}, "/tmp/nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-PITR backup")
	}
}

func TestValidateRecoveryChain_NoLSN(t *testing.T) {
	ps := &PITRService{}
	manifest := &Manifest{
		PITREnabled: true,
		StartLSN:    0,
		EndLSN:      0,
	}

	err := ps.ValidateRecoveryChain(manifest, PITRRecoveryTarget{Type: PITRTargetLSN, TargetLSN: 100}, "/tmp/nonexistent")
	if err == nil {
		t.Fatal("Expected error for zero LSN backup")
	}
}

func TestValidateRecoveryChain_EmptyArchive(t *testing.T) {
	dir := t.TempDir()
	ps := &PITRService{}
	manifest := &Manifest{
		PITREnabled: true,
		StartLSN:    1,
		EndLSN:      100,
	}

	err := ps.ValidateRecoveryChain(manifest, PITRRecoveryTarget{Type: PITRTargetLSN, TargetLSN: 200}, dir)
	if err == nil {
		t.Fatal("Expected error for empty archive")
	}
}

func TestValidateRecoveryChain_ValidChain(t *testing.T) {
	dir := t.TempDir()

	// Create a valid archive manifest
	archiveManifest := walArchiveManifestFile{
		Files: []WALArchiveEntry{
			{FileName: "2026-02-24.wal", FirstLSN: 50, LastLSN: 150, ArchivedAt: time.Now()},
			{FileName: "2026-02-24_10-00-00.wal", FirstLSN: 151, LastLSN: 300, ArchivedAt: time.Now()},
		},
	}
	data, _ := json.MarshalIndent(archiveManifest, "", "  ")
	os.WriteFile(filepath.Join(dir, "wal_archive.manifest"), data, 0644)

	ps := &PITRService{}
	manifest := &Manifest{
		PITREnabled: true,
		StartLSN:    1,
		EndLSN:      100,
	}

	err := ps.ValidateRecoveryChain(manifest, PITRRecoveryTarget{Type: PITRTargetLSN, TargetLSN: 200}, dir)
	if err != nil {
		t.Fatalf("Expected valid chain, got error: %v", err)
	}
}

func TestValidateRecoveryChain_GapInChain(t *testing.T) {
	dir := t.TempDir()

	// Archive starts after backup ends — gap
	archiveManifest := walArchiveManifestFile{
		Files: []WALArchiveEntry{
			{FileName: "2026-02-24.wal", FirstLSN: 200, LastLSN: 300, ArchivedAt: time.Now()},
		},
	}
	data, _ := json.MarshalIndent(archiveManifest, "", "  ")
	os.WriteFile(filepath.Join(dir, "wal_archive.manifest"), data, 0644)

	ps := &PITRService{}
	manifest := &Manifest{
		PITREnabled: true,
		StartLSN:    1,
		EndLSN:      100,
	}

	err := ps.ValidateRecoveryChain(manifest, PITRRecoveryTarget{Type: PITRTargetLSN, TargetLSN: 250}, dir)
	if err == nil {
		t.Fatal("Expected error for gap in chain")
	}
}

func TestValidateRecoveryChain_TargetBeyondArchive(t *testing.T) {
	dir := t.TempDir()

	archiveManifest := walArchiveManifestFile{
		Files: []WALArchiveEntry{
			{FileName: "2026-02-24.wal", FirstLSN: 50, LastLSN: 150, ArchivedAt: time.Now()},
		},
	}
	data, _ := json.MarshalIndent(archiveManifest, "", "  ")
	os.WriteFile(filepath.Join(dir, "wal_archive.manifest"), data, 0644)

	ps := &PITRService{}
	manifest := &Manifest{
		PITREnabled: true,
		StartLSN:    1,
		EndLSN:      100,
	}

	err := ps.ValidateRecoveryChain(manifest, PITRRecoveryTarget{Type: PITRTargetLSN, TargetLSN: 500}, dir)
	if err == nil {
		t.Fatal("Expected error for target beyond archive")
	}
}

func TestReadWALArchiveManifest_NonExistent(t *testing.T) {
	entries, err := readWALArchiveManifest("/tmp/nonexistent_dir_abc123")
	if err != nil {
		t.Fatalf("Expected nil error for nonexistent dir, got: %v", err)
	}
	if entries != nil {
		t.Errorf("Expected nil entries, got %d", len(entries))
	}
}

func TestReadWALArchiveManifest_ValidFile(t *testing.T) {
	dir := t.TempDir()

	manifest := walArchiveManifestFile{
		Files: []WALArchiveEntry{
			{FileName: "test.wal", FirstLSN: 1, LastLSN: 100},
		},
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	os.WriteFile(filepath.Join(dir, "wal_archive.manifest"), data, 0644)

	entries, err := readWALArchiveManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0].FileName != "test.wal" {
		t.Errorf("Expected 'test.wal', got '%s'", entries[0].FileName)
	}
}
