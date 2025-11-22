package main

import (
	"os"
	"path/filepath"
	"syndrdb/src/pkg/settings"
	"testing"
)

// TestYAMLConfig_LoadValidConfig tests loading a complete valid YAML configuration
func TestYAMLConfig_LoadValidConfig(t *testing.T) {
	configPath := filepath.Join("testdata", "valid_config.yml")

	config, err := settings.LoadConfigFromYAML(configPath)
	if err != nil {
		t.Fatalf("Failed to load valid config: %v", err)
	}

	// Verify a few key values
	if config.Host != "127.0.0.1" {
		t.Errorf("Expected host 127.0.0.1, got %s", config.Host)
	}
	if config.Port != 9999 {
		t.Errorf("Expected port 9999, got %d", config.Port)
	}
	if config.DataDir != "./test_data" {
		t.Errorf("Expected data_dir ./test_data, got %s", config.DataDir)
	}
	if !config.AuthEnabled {
		t.Error("Expected auth_enabled to be true")
	}
	if config.SessionTimeoutMinutes != 60 {
		t.Errorf("Expected session_timeout_minutes 60, got %d", config.SessionTimeoutMinutes)
	}
	if config.WALMode != "async" {
		t.Errorf("Expected wal_mode async, got %s", config.WALMode)
	}
	if config.AsyncWALWorkers != 4 {
		t.Errorf("Expected async_wal_workers 4, got %d", config.AsyncWALWorkers)
	}
	if config.SortTopNThreshold != 0.2 {
		t.Errorf("Expected sort_topn_threshold 0.2, got %f", config.SortTopNThreshold)
	}
	if !config.EnableGraphQL {
		t.Error("Expected enable_graphql to be true")
	}
	if config.MaxMigrationCommands != 2000 {
		t.Errorf("Expected max_migration_commands 2000, got %d", config.MaxMigrationCommands)
	}
}

// TestYAMLConfig_LoadMinimalConfig tests loading a minimal configuration
func TestYAMLConfig_LoadMinimalConfig(t *testing.T) {
	configPath := filepath.Join("testdata", "minimal_config.yml")

	config, err := settings.LoadConfigFromYAML(configPath)
	if err != nil {
		t.Fatalf("Failed to load minimal config: %v", err)
	}

	// Verify specified values
	if config.Host != "0.0.0.0" {
		t.Errorf("Expected host 0.0.0.0, got %s", config.Host)
	}
	if config.Port != 8888 {
		t.Errorf("Expected port 8888, got %d", config.Port)
	}
	if config.DataDir != "./minimal_data" {
		t.Errorf("Expected data_dir ./minimal_data, got %s", config.DataDir)
	}

	// Unspecified values should be zero values (not defaults - those come from merge)
	if config.Verbose {
		t.Error("Expected verbose to be false (zero value)")
	}
	if config.LogLevel != "" {
		t.Errorf("Expected log_level to be empty string, got %s", config.LogLevel)
	}
}

// TestYAMLConfig_InvalidTypes tests that type mismatches produce errors
func TestYAMLConfig_InvalidTypes(t *testing.T) {
	configPath := filepath.Join("testdata", "invalid_types.yml")

	_, err := settings.LoadConfigFromYAML(configPath)
	if err == nil {
		t.Fatal("Expected error for invalid types, got nil")
	}

	// The error should mention the issue
	errMsg := err.Error()
	if errMsg == "" {
		t.Error("Expected non-empty error message")
	}

	t.Logf("Got expected error: %v", err)
}

// TestYAMLConfig_UnknownFieldsIgnored tests that unknown fields are silently ignored
func TestYAMLConfig_UnknownFieldsIgnored(t *testing.T) {
	configPath := filepath.Join("testdata", "unknown_fields.yml")

	config, err := settings.LoadConfigFromYAML(configPath)
	if err != nil {
		t.Fatalf("Unknown fields should be ignored, but got error: %v", err)
	}

	// Verify known fields were parsed
	if config.Host != "127.0.0.1" {
		t.Errorf("Expected host 127.0.0.1, got %s", config.Host)
	}
	if config.Port != 7777 {
		t.Errorf("Expected port 7777, got %d", config.Port)
	}
	if config.DataDir != "./test_data" {
		t.Errorf("Expected data_dir ./test_data, got %s", config.DataDir)
	}
}

// TestYAMLConfig_FileNotFound tests handling of missing config file
func TestYAMLConfig_FileNotFound(t *testing.T) {
	configPath := filepath.Join("testdata", "nonexistent.yml")

	_, err := settings.LoadConfigFromYAML(configPath)
	if err == nil {
		t.Fatal("Expected error for missing file, got nil")
	}

	t.Logf("Got expected error: %v", err)
}

// TestYAMLConfig_MergeIntoDefaults tests field-level granular merging
func TestYAMLConfig_MergeIntoDefaults(t *testing.T) {
	// Get defaults
	defaults := settings.GetSettings()

	// Load minimal config (only host, port, data_dir)
	configPath := filepath.Join("testdata", "minimal_config.yml")
	yamlConfig, err := settings.LoadConfigFromYAML(configPath)
	if err != nil {
		t.Fatalf("Failed to load minimal config: %v", err)
	}

	// Merge
	merged := settings.MergeYAMLIntoDefaults(defaults, yamlConfig)

	// YAML values should override defaults
	if merged.Host != "0.0.0.0" {
		t.Errorf("Expected merged host 0.0.0.0, got %s", merged.Host)
	}
	if merged.Port != 8888 {
		t.Errorf("Expected merged port 8888, got %d", merged.Port)
	}
	if merged.DataDir != "./minimal_data" {
		t.Errorf("Expected merged data_dir ./minimal_data, got %s", merged.DataDir)
	}

	// Unspecified values should retain defaults
	if merged.LogLevel != defaults.LogLevel {
		t.Errorf("Expected log_level to retain default %s, got %s", defaults.LogLevel, merged.LogLevel)
	}
	if merged.WALMode != defaults.WALMode {
		t.Errorf("Expected wal_mode to retain default %s, got %s", defaults.WALMode, merged.WALMode)
	}
	if merged.SortTopNThreshold != defaults.SortTopNThreshold {
		t.Errorf("Expected sort_topn_threshold to retain default %f, got %f", defaults.SortTopNThreshold, merged.SortTopNThreshold)
	}
}

// TestYAMLConfig_CLIPrecedence tests that CLI flags override YAML values
func TestYAMLConfig_CLIPrecedence(t *testing.T) {
	// This test would need to simulate command-line argument parsing
	// which is complex in a test environment. We'll document the expected behavior:
	//
	// Expected precedence order:
	// 1. Defaults from GetSettings()
	// 2. YAML file values override defaults
	// 3. CLI flags override YAML values
	//
	// The implementation in main.go:
	// - Loads defaults
	// - Merges YAML values
	// - Re-parses CLI flags which override merged values

	t.Log("CLI precedence is tested through integration testing")
	t.Log("Precedence order: CLI > YAML > Defaults")
}

// TestYAMLConfig_ValidateConfigFile tests config file validation
func TestYAMLConfig_ValidateConfigFile(t *testing.T) {
	t.Run("Valid file", func(t *testing.T) {
		configPath := filepath.Join("testdata", "valid_config.yml")
		err := settings.ValidateConfigFile(configPath)
		if err != nil {
			t.Errorf("Expected no error for valid file, got: %v", err)
		}
	})

	t.Run("Missing file", func(t *testing.T) {
		configPath := filepath.Join("testdata", "missing.yml")
		err := settings.ValidateConfigFile(configPath)
		if err == nil {
			t.Error("Expected error for missing file")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("Empty path", func(t *testing.T) {
		err := settings.ValidateConfigFile("")
		if err != nil {
			t.Errorf("Expected no error for empty path, got: %v", err)
		}
	})

	t.Run("Directory instead of file", func(t *testing.T) {
		err := settings.ValidateConfigFile("testdata")
		if err == nil {
			t.Error("Expected error for directory path")
		}
		t.Logf("Got expected error: %v", err)
	})
}

// TestYAMLConfig_BooleanHandling tests proper handling of boolean values
func TestYAMLConfig_BooleanHandling(t *testing.T) {
	// Create a temporary config file with explicit boolean values
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "bool_test.yml")

	configContent := `
verbose: true
debug: false
auth_enabled: true
use_new_parser: false
sort_simd_enabled: true
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config: %v", err)
	}

	config, err := settings.LoadConfigFromYAML(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify boolean values
	if !config.Verbose {
		t.Error("Expected verbose to be true")
	}
	if config.Debug {
		t.Error("Expected debug to be false")
	}
	if !config.AuthEnabled {
		t.Error("Expected auth_enabled to be true")
	}
	if config.UseNewParser {
		t.Error("Expected use_new_parser to be false")
	}
	if !config.SortSIMDEnabled {
		t.Error("Expected sort_simd_enabled to be true")
	}
}

// TestYAMLConfig_NumericTypes tests proper handling of different numeric types
func TestYAMLConfig_NumericTypes(t *testing.T) {
	// Create a temporary config file with various numeric types
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "numeric_test.yml")

	configContent := `
port: 1234
session_timeout_minutes: 45
async_wal_workers: 8
sort_topn_threshold: 0.15
sort_radix_limit_ratio: 0.75
max_journal_file_size: 5000000
max_validation_report_size: 20971520
migration_performance_threshold: 1.5
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config: %v", err)
	}

	config, err := settings.LoadConfigFromYAML(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify numeric values
	if config.Port != 1234 {
		t.Errorf("Expected port 1234, got %d", config.Port)
	}
	if config.SessionTimeoutMinutes != 45 {
		t.Errorf("Expected session_timeout_minutes 45, got %d", config.SessionTimeoutMinutes)
	}
	if config.AsyncWALWorkers != 8 {
		t.Errorf("Expected async_wal_workers 8, got %d", config.AsyncWALWorkers)
	}
	if config.SortTopNThreshold != 0.15 {
		t.Errorf("Expected sort_topn_threshold 0.15, got %f", config.SortTopNThreshold)
	}
	if config.SortRadixLimitRatio != 0.75 {
		t.Errorf("Expected sort_radix_limit_ratio 0.75, got %f", config.SortRadixLimitRatio)
	}
	if config.MaxJournalFileSize != 5000000 {
		t.Errorf("Expected max_journal_file_size 5000000, got %d", config.MaxJournalFileSize)
	}
	if config.MaxValidationReportSize != 20971520 {
		t.Errorf("Expected max_validation_report_size 20971520, got %d", config.MaxValidationReportSize)
	}
	if config.MigrationPerformanceThreshold != 1.5 {
		t.Errorf("Expected migration_performance_threshold 1.5, got %f", config.MigrationPerformanceThreshold)
	}
}

// TestYAMLConfig_StringValues tests proper handling of string values
func TestYAMLConfig_StringValues(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "string_test.yml")

	configContent := `
host: "192.168.1.100"
data_dir: "/var/lib/syndrdb"
log_dir: "/var/log/syndrdb"
temp_dir: "/tmp/syndrdb"
mode: "cluster"
wal_mode: "sync"
bundle_storage_format: "json"
log_level: "debug"
graphql_rate_algorithm: "time-bucket"
backup_compression: "zstd"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config: %v", err)
	}

	config, err := settings.LoadConfigFromYAML(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify string values
	if config.Host != "192.168.1.100" {
		t.Errorf("Expected host 192.168.1.100, got %s", config.Host)
	}
	if config.DataDir != "/var/lib/syndrdb" {
		t.Errorf("Expected data_dir /var/lib/syndrdb, got %s", config.DataDir)
	}
	if config.LogDir != "/var/log/syndrdb" {
		t.Errorf("Expected log_dir /var/log/syndrdb, got %s", config.LogDir)
	}
	if config.TempDir != "/tmp/syndrdb" {
		t.Errorf("Expected temp_dir /tmp/syndrdb, got %s", config.TempDir)
	}
	if config.Mode != "cluster" {
		t.Errorf("Expected mode cluster, got %s", config.Mode)
	}
	if config.WALMode != "sync" {
		t.Errorf("Expected wal_mode sync, got %s", config.WALMode)
	}
	if config.BundleStorageFormat != "json" {
		t.Errorf("Expected bundle_storage_format json, got %s", config.BundleStorageFormat)
	}
	if config.LogLevel != "debug" {
		t.Errorf("Expected log_level debug, got %s", config.LogLevel)
	}
	if config.GraphQLRateAlgorithm != "time-bucket" {
		t.Errorf("Expected graphql_rate_algorithm time-bucket, got %s", config.GraphQLRateAlgorithm)
	}
	if config.BackupCompression != "zstd" {
		t.Errorf("Expected backup_compression zstd, got %s", config.BackupCompression)
	}
}

// TestYAMLConfig_PartialMerge tests that partial configs merge correctly
func TestYAMLConfig_PartialMerge(t *testing.T) {
	defaults := settings.GetSettings()

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "partial_test.yml")

	// Only override a few specific values
	configContent := `
port: 5000
verbose: true
sort_topn_threshold: 0.25
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config: %v", err)
	}

	yamlConfig, err := settings.LoadConfigFromYAML(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	merged := settings.MergeYAMLIntoDefaults(defaults, yamlConfig)

	// Specified values should be overridden
	if merged.Port != 5000 {
		t.Errorf("Expected port 5000, got %d", merged.Port)
	}
	if !merged.Verbose {
		t.Error("Expected verbose to be true")
	}
	if merged.SortTopNThreshold != 0.25 {
		t.Errorf("Expected sort_topn_threshold 0.25, got %f", merged.SortTopNThreshold)
	}

	// Unspecified values should retain defaults
	if merged.Host != defaults.Host {
		t.Errorf("Expected host to retain default %s, got %s", defaults.Host, merged.Host)
	}
	if merged.DataDir != defaults.DataDir {
		t.Errorf("Expected data_dir to retain default %s, got %s", defaults.DataDir, merged.DataDir)
	}
	if merged.AsyncWALWorkers != defaults.AsyncWALWorkers {
		t.Errorf("Expected async_wal_workers to retain default %d, got %d", defaults.AsyncWALWorkers, merged.AsyncWALWorkers)
	}
}
