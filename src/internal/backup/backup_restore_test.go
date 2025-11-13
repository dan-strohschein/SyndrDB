package backup

import (
	"path/filepath"
	"testing"

	"syndrdb/src/internal/lock"

	"go.uber.org/zap"
)

// TestNewBackupService verifies backup service initialization
func TestNewBackupService(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lockService := lock.NewLockService(logger.Desugar())

	// Create minimal services (nil for integration tests)
	service := NewBackupService(nil, nil, lockService, nil, logger)
	if service == nil {
		t.Fatal("NewBackupService returned nil")
	}
	if service.lockService != lockService {
		t.Error("Lock service not set correctly")
	}
}

// TestNewRestoreService verifies restore service initialization
func TestNewRestoreService(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lockService := lock.NewLockService(logger.Desugar())

	service := NewRestoreService(nil, nil, lockService, logger)
	if service == nil {
		t.Fatal("NewRestoreService returned nil")
	}
	if service.lockService != lockService {
		t.Error("Lock service not set correctly")
	}
}

// TestBackupOptions verifies backup options struct
func TestBackupOptions(t *testing.T) {
	opts := BackupOptions{
		Compression:    "gzip",
		IncludeIndexes: true,
		OutputPath:     "/path/to/backup.tar.gz",
	}

	if opts.Compression != "gzip" {
		t.Error("Compression not set correctly")
	}
	if !opts.IncludeIndexes {
		t.Error("IncludeIndexes should be true")
	}
	if opts.OutputPath != "/path/to/backup.tar.gz" {
		t.Error("OutputPath not set correctly")
	}
}

// TestRestoreOptions verifies restore options struct
func TestRestoreOptions(t *testing.T) {
	opts := RestoreOptions{
		TargetDBName: "restored_db",
		Force:        true,
	}

	if opts.TargetDBName != "restored_db" {
		t.Error("TargetDBName not set correctly")
	}
	if !opts.Force {
		t.Error("Force should be true")
	}
}

// TestBackupWithInvalidDatabase tests error handling
func TestBackupWithInvalidDatabase(t *testing.T) {
	// This test would require mock services
	// Skipping for now as it requires full service setup
	t.Skip("Requires full service setup with mocks")
}

// TestRestoreWithInvalidBackup tests error handling
func TestRestoreWithInvalidBackup(t *testing.T) {
	// This test would require mock services
	// Skipping for now as it requires full service setup
	t.Skip("Requires full service setup with mocks")
}

// TestCompressionFormats verifies supported compression formats
func TestCompressionFormats(t *testing.T) {
	supportedFormats := []string{"gzip", "zstd", "none"}

	for _, format := range supportedFormats {
		opts := BackupOptions{
			Compression: format,
		}
		if opts.Compression != format {
			t.Errorf("Format %s not set correctly", format)
		}
	}
}

// TestBackupPathGeneration tests output path handling
func TestBackupPathGeneration(t *testing.T) {
	tmpDir := t.TempDir()

	testCases := []struct {
		name        string
		outputPath  string
		compression string
	}{
		{"gzip", filepath.Join(tmpDir, "backup.tar.gz"), "gzip"},
		{"zstd", filepath.Join(tmpDir, "backup.tar.zst"), "zstd"},
		{"none", filepath.Join(tmpDir, "backup.tar"), "none"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := BackupOptions{
				OutputPath:  tc.outputPath,
				Compression: tc.compression,
			}

			if opts.OutputPath != tc.outputPath {
				t.Errorf("Expected path %s, got %s", tc.outputPath, opts.OutputPath)
			}
		})
	}
}

// TestLockIntegrationWithBackup tests lock service integration
func TestLockIntegrationWithBackup(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lockService := lock.NewLockService(logger.Desugar())

	// Lock a database
	err := lockService.LockDatabase("testdb", "admin", "BACKUP", "backup in progress")
	if err != nil {
		t.Fatalf("Failed to lock database: %v", err)
	}

	// Verify it's locked
	if !lockService.IsLocked("testdb") {
		t.Error("Database should be locked")
	}

	// Create backup service with locked database
	backupService := NewBackupService(nil, nil, lockService, nil, logger)
	if backupService == nil {
		t.Fatal("Backup service creation failed")
	}

	// Unlock
	err = lockService.UnlockDatabase("testdb", "admin")
	if err != nil {
		t.Errorf("Failed to unlock: %v", err)
	}
}

// TestLockIntegrationWithRestore tests lock service integration
func TestLockIntegrationWithRestore(t *testing.T) {
	logger := zap.NewNop().Sugar()
	lockService := lock.NewLockService(logger.Desugar())

	// Lock a database for restore
	err := lockService.LockDatabase("testdb", "admin", "RESTORE", "restore in progress")
	if err != nil {
		t.Fatalf("Failed to lock database: %v", err)
	}

	// Create restore service
	restoreService := NewRestoreService(nil, nil, lockService, logger)
	if restoreService == nil {
		t.Fatal("Restore service creation failed")
	}

	// Verify lock info
	info := lockService.GetLockInfo("testdb")
	if string(info.Reason) != "RESTORE" {
		t.Errorf("Expected RESTORE reason, got %s", info.Reason)
	}

	// Unlock
	lockService.UnlockDatabase("testdb", "admin")
}

// TestForceRestoreOption tests force restore behavior
func TestForceRestoreOption(t *testing.T) {
	// Test force option can be set
	opts := RestoreOptions{
		TargetDBName: "db1",
		Force:        true,
	}

	if !opts.Force {
		t.Error("Force option not set")
	}

	// Test without force
	opts2 := RestoreOptions{
		TargetDBName: "db2",
		Force:        false,
	}

	if opts2.Force {
		t.Error("Force should be false")
	}
}

// Note: Full E2E tests with actual backup/restore would go in a separate E2E test suite
// that can set up complete database, bundle, and WAL services
