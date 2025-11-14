package migration

import (
	"syndrdb/src/pkg/settings"
	"time"
)

/*
migration_config.go

This file provides configuration management for the migration system.
It bridges the gap between CLI flags/settings and the MigrationConfig struct
used internally by the migration service.

Key Responsibilities:
  - LoadConfigFromSettings: Convert settings.Arguments to MigrationConfig
  - Validation: Ensure settings are within valid ranges
  - Defaults: Provide sensible defaults when settings are not specified

Design Pattern:
  - Adapter Pattern: Translates external settings format to internal config structure
  - Fail-Safe Defaults: All fields have reasonable defaults to prevent runtime errors
*/

// LoadConfigFromSettings creates a MigrationConfig from the global settings
// This allows the migration system to use configuration values from CLI flags
// and configuration files without directly depending on the settings package
func LoadConfigFromSettings(args *settings.Arguments) MigrationConfig {
	// Convert seconds to time.Duration for timeout
	timeout := time.Duration(args.MigrationTimeoutSeconds) * time.Second

	// Convert performance threshold from seconds (float64) to milliseconds (int)
	// for internal use in performance tracking
	perfThresholdMs := int(args.MigrationPerformanceThreshold * 1000)

	return MigrationConfig{
		MaxCommandsPerMigration:       args.MaxMigrationCommands,
		EnableAutoReverse:             args.EnableAutoReverse,
		RequireExplicitDownCommands:   args.RequireExplicitDownCommands,
		PerformanceWarningThreshold:   perfThresholdMs, // Convert seconds to milliseconds
		MaxValidationReportSize:       int(args.MaxValidationReportSize),
		ValidationReportRetentionDays: args.ValidationReportRetentionDays,
		MigrationTimeout:              timeout,
	}
}

// DefaultMigrationConfig returns the default configuration settings
// Used when no settings are provided or as fallback values
func DefaultMigrationConfig() MigrationConfig {
	return MigrationConfig{
		MaxCommandsPerMigration:       1000,
		EnableAutoReverse:             true,
		RequireExplicitDownCommands:   false,
		PerformanceWarningThreshold:   1000,            // 1 second in milliseconds
		MaxValidationReportSize:       10485760,        // 10MB
		ValidationReportRetentionDays: 30,              // 30 days
		MigrationTimeout:              5 * time.Minute, // 5 minutes
	}
}
