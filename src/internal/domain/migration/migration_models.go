package migration

import "time"

// MigrationStatus represents the current state of a migration
type MigrationStatus string

const (
	PENDING     MigrationStatus = "PENDING"
	IN_PROGRESS MigrationStatus = "IN_PROGRESS"
	APPLIED     MigrationStatus = "APPLIED"
	FAILED      MigrationStatus = "FAILED"
	ROLLED_BACK MigrationStatus = "ROLLED_BACK"
)

// Migration represents a database migration with full execution history
type Migration struct {
	MigrationID         string          `json:"migration_id"`
	Version             int             `json:"version"`
	DatabaseName        string          `json:"database_name"`
	Description         string          `json:"description"`
	UpCommands          []string        `json:"up_commands"`
	DownCommands        []string        `json:"down_commands"`
	Status              MigrationStatus `json:"status"`
	Checksum            string          `json:"checksum"`
	AppliedBy           string          `json:"applied_by"`
	CreatedAt           time.Time       `json:"created_at"`
	AppliedAt           *time.Time      `json:"applied_at,omitempty"`
	RolledBackAt        *time.Time      `json:"rolled_back_at,omitempty"`
	ExecutionTimeMs     int64           `json:"execution_time_ms"`
	ErrorMessage        string          `json:"error_message,omitempty"`
	PerformanceWarnings []string        `json:"performance_warnings,omitempty"`
}

// MigrationCommand represents a user request to create and apply a migration
type MigrationCommand struct {
	DatabaseName string   `json:"database_name"`
	Description  string   `json:"description"`
	Commands     []string `json:"commands"`
	DownCommands []string `json:"down_commands,omitempty"`
	CreatedBy    string   `json:"created_by"`
}

// MigrationLock represents an active lock on a database during migration
type MigrationLock struct {
	DatabaseName     string          `json:"database_name"`
	LockedBy         string          `json:"locked_by"`
	LockedAt         time.Time       `json:"locked_at"`
	MigrationID      string          `json:"migration_id"`
	MigrationVersion int             `json:"migration_version"`
	Status           MigrationStatus `json:"status"`
}

// MigrationConfig holds configuration settings for the migration system
type MigrationConfig struct {
	MaxCommandsPerMigration       int           `json:"max_commands_per_migration"`
	EnableAutoReverse             bool          `json:"enable_auto_reverse"`
	RequireExplicitDownCommands   bool          `json:"require_explicit_down_commands"`
	PerformanceWarningThreshold   int           `json:"performance_warning_threshold"`
	MaxValidationReportSize       int           `json:"max_validation_report_size"`
	ValidationReportRetentionDays int           `json:"validation_report_retention_days"`
	MigrationTimeout              time.Duration `json:"migration_timeout"`
}

// ValidationReportType indicates the type of validation being performed
type ValidationReportType string

const (
	MIGRATION_VALIDATION ValidationReportType = "MIGRATION_VALIDATION"
	ROLLBACK_VALIDATION  ValidationReportType = "ROLLBACK_VALIDATION"
)

// ValidationReportStatus indicates the current status of a validation report
type ValidationReportStatus string

const (
	ACTIVE   ValidationReportStatus = "ACTIVE"
	ARCHIVED ValidationReportStatus = "ARCHIVED"
)

// ValidationReport stores detailed validation results for migrations
type ValidationReport struct {
	ReportID          string                 `json:"report_id"`
	MigrationVersion  int                    `json:"migration_version"`
	TargetVersion     int                    `json:"target_version"`
	DatabaseName      string                 `json:"database_name"`
	ReportType        ValidationReportType   `json:"report_type"`
	GeneratedAt       time.Time              `json:"generated_at"`
	GeneratedBy       string                 `json:"generated_by"`
	ValidationResults []string               `json:"validation_results"`
	ReportSizeBytes   int64                  `json:"report_size_bytes"`
	Status            ValidationReportStatus `json:"status"`
	ArchivedAt        *time.Time             `json:"archived_at,omitempty"`
	ExpiresAt         *time.Time             `json:"expires_at,omitempty"`
}
