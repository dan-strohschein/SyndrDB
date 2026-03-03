package pgimport

import "time"

// ImportFormat represents the PostgreSQL dump format
type ImportFormat int

const (
	FormatAuto   ImportFormat = iota // Auto-detect from file contents
	FormatPlain                      // Plain SQL (-Fp)
	FormatCustom                     // Custom binary (-Fc)
)

// ImportStatus represents the overall import result
type ImportStatus string

const (
	StatusSuccess ImportStatus = "SUCCESS"
	StatusPartial ImportStatus = "PARTIAL"
	StatusFailed  ImportStatus = "FAILED"
)

// ImportOptions configures the PostgreSQL import behavior
type ImportOptions struct {
	SourceFile        string       // Path to the PostgreSQL dump file
	TargetDatabase    string       // Target SyndrDB database name
	Format            ImportFormat // Dump format (auto/plain/custom)
	BatchSize         int          // Documents per batch insert (default 1000)
	CreateIndexesAfter bool       // Create indexes after data load (default true)
	ContinueOnError   bool        // Continue on row-level errors (default false)
	MaxErrors         int          // Maximum errors before abort (default 100)
	DryRun            bool         // Parse and validate without writing (default false)
	ProgressInterval  int          // Log progress every N records (default 10000)
}

// DefaultImportOptions returns ImportOptions with sensible defaults
func DefaultImportOptions() ImportOptions {
	return ImportOptions{
		BatchSize:          1000,
		CreateIndexesAfter: true,
		MaxErrors:          100,
		ProgressInterval:   10000,
	}
}

// ImportReport captures the results of an import operation
type ImportReport struct {
	BundlesCreated       int
	BundlesSkipped       int
	IndexesCreated       int
	IndexesSkipped       int
	RelationshipsCreated int
	RelationshipsSkipped int
	ViewsCreated         int
	ViewsSkipped         int
	RecordsImported      int
	RecordsSkipped       int
	Warnings             []ImportWarning
	Errors               []ImportError
	StartTime            time.Time
	EndTime              time.Time
	Status               ImportStatus
}

// Duration returns the total import duration
func (r *ImportReport) Duration() time.Duration {
	return r.EndTime.Sub(r.StartTime)
}

// ImportWarning records a non-fatal issue encountered during import
type ImportWarning struct {
	Phase      string // "schema", "data", "index", "relationship", "view"
	Object     string // table/index/constraint name
	Message    string
	SQLContext string // relevant SQL snippet
}

// ImportError records a fatal or row-level error during import
type ImportError struct {
	Phase      string
	Object     string
	Message    string
	SQLContext string
	Line       int // line number in the dump file (0 if unknown)
}

// --- PostgreSQL intermediate types ---

// PGColumn represents a parsed PostgreSQL column definition
type PGColumn struct {
	Name       string
	Type       string // Raw PG type string (e.g., "varchar(255)", "integer")
	NotNull    bool
	Unique     bool
	PrimaryKey bool
	Default    string // Raw default expression (e.g., "now()", "0", "nextval('seq')")
}

// PGConstraintType identifies the kind of PostgreSQL constraint
type PGConstraintType int

const (
	ConstraintPrimaryKey PGConstraintType = iota
	ConstraintForeignKey
	ConstraintUnique
	ConstraintCheck
)

// PGConstraint represents a parsed PostgreSQL table or ALTER TABLE constraint
type PGConstraint struct {
	Name       string
	Type       PGConstraintType
	Columns    []string // Columns in this constraint
	RefTable   string   // Referenced table (FK only)
	RefColumns []string // Referenced columns (FK only)
	CheckExpr  string   // CHECK expression (CHECK only)
}

// PGIndexDef represents a parsed PostgreSQL CREATE INDEX statement
type PGIndexDef struct {
	Name       string
	Table      string
	Columns    []string
	Unique     bool
	Method     string // btree, hash, brin, gin, gist
	Where      string // Partial index predicate
	Expression string // Expression index (e.g., "lower(name)")
}

// PGView represents a parsed PostgreSQL view definition
type PGView struct {
	Name         string
	Definition   string // The SELECT statement
	Materialized bool
}

// --- Parser output types ---

// StatementType classifies a parsed SQL statement
type StatementType int

const (
	StmtCreateTable StatementType = iota
	StmtCreateIndex
	StmtAlterTable
	StmtCopyFrom
	StmtInsertInto
	StmtCreateView
	StmtSkip // SET, COMMENT, CREATE SEQUENCE, etc.
)

// ParsedStatement is the output of statement parsing
type ParsedStatement struct {
	Type        StatementType
	TableName   string // For CREATE TABLE, ALTER TABLE, COPY, INSERT
	Columns     []PGColumn
	Constraints []PGConstraint
	IndexDef    *PGIndexDef
	View        *PGView
	RawSQL      string // Original SQL text

	// COPY-specific
	CopyMeta *COPYMetadata

	// INSERT-specific
	InsertColumns []string
	InsertRows    [][]string // Each row is a slice of string values
}

// COPYMetadata holds parsed COPY ... FROM stdin header info
type COPYMetadata struct {
	TableName string
	Columns   []string
}
