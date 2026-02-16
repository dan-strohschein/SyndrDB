package ir

// TrainingExample is one line of JSONL output: NL description, IR, and compiled SyndrQL.
type TrainingExample struct {
	NL      string    `json:"nl"`
	IR      IREnvelope `json:"ir"`
	SyndrQL string    `json:"syndrql"`
}

// IREnvelope wraps one or more IR statements with metadata.
type IREnvelope struct {
	Statements  []Statement `json:"statements"`
	Explanation string      `json:"explanation,omitempty"`
	Confidence  float64     `json:"confidence,omitempty"`
}

// Statement is the discriminated union for all SyndrQL command types.
// The StatementType field determines which sub-struct is populated.
type Statement struct {
	StatementType string `json:"statementType"`

	// DML
	Select *SelectStmt `json:"select,omitempty"`
	Insert *InsertStmt `json:"insert,omitempty"`
	Update *UpdateStmt `json:"update,omitempty"`
	Delete *DeleteStmt `json:"delete,omitempty"`

	// DDL - Bundle
	CreateBundle *CreateBundleStmt `json:"createBundle,omitempty"`
	DropBundle   *DropBundleStmt   `json:"dropBundle,omitempty"`
	UpdateBundle *UpdateBundleStmt `json:"updateBundle,omitempty"`

	// DDL - Index
	CreateIndex *CreateIndexStmt `json:"createIndex,omitempty"`

	// DDL - View
	CreateView  *CreateViewStmt  `json:"createView,omitempty"`
	DropView    *DropViewStmt    `json:"dropView,omitempty"`
	RefreshView *RefreshViewStmt `json:"refreshView,omitempty"`

	// Transaction
	BeginTransaction    *BeginTransactionStmt    `json:"beginTransaction,omitempty"`
	Commit              *CommitStmt              `json:"commit,omitempty"`
	Rollback            *RollbackStmt            `json:"rollback,omitempty"`
	Savepoint           *SavepointStmt           `json:"savepoint,omitempty"`
	RollbackToSavepoint *RollbackToSavepointStmt `json:"rollbackToSavepoint,omitempty"`

	// Cursor
	DeclareCursor *DeclareCursorStmt `json:"declareCursor,omitempty"`
	FetchCursor   *FetchCursorStmt   `json:"fetchCursor,omitempty"`
	CloseCursor   *CloseCursorStmt   `json:"closeCursor,omitempty"`

	// Prepared Statements
	Prepare    *PrepareStmt    `json:"prepare,omitempty"`
	Execute    *ExecuteStmt    `json:"execute,omitempty"`
	Deallocate *DeallocateStmt `json:"deallocate,omitempty"`

	// RBAC
	CreateUser      *CreateUserStmt      `json:"createUser,omitempty"`
	UpdateUser      *UpdateUserStmt      `json:"updateUser,omitempty"`
	DeleteUser      *DeleteUserStmt      `json:"deleteUser,omitempty"`
	CreateRole      *CreateRoleStmt      `json:"createRole,omitempty"`
	UpdateRole      *UpdateRoleStmt      `json:"updateRole,omitempty"`
	DeleteRole      *DeleteRoleStmt      `json:"deleteRole,omitempty"`
	GrantPermission *GrantPermissionStmt `json:"grantPermission,omitempty"`
	GrantRole       *GrantRoleStmt       `json:"grantRole,omitempty"`
	RevokePermission *RevokePermissionStmt `json:"revokePermission,omitempty"`
	RevokeRole       *RevokeRoleStmt       `json:"revokeRole,omitempty"`

	// Database
	UseDatabase    *UseDatabaseStmt    `json:"useDatabase,omitempty"`
	CreateDatabase *CreateDatabaseStmt `json:"createDatabase,omitempty"`
	DropDatabase   *DropDatabaseStmt   `json:"dropDatabase,omitempty"`
	RenameDatabase *RenameDatabaseStmt `json:"renameDatabase,omitempty"`
	AttachDatabase *AttachDatabaseStmt `json:"attachDatabase,omitempty"`

	// Utility / Show
	ShowDatabases       *ShowDatabasesStmt       `json:"showDatabases,omitempty"`
	ShowBundles         *ShowBundlesStmt         `json:"showBundles,omitempty"`
	ShowBundle          *ShowBundleStmt          `json:"showBundle,omitempty"`
	ShowUsers           *ShowUsersStmt           `json:"showUsers,omitempty"`
	ShowSessions        *ShowSessionsStmt        `json:"showSessions,omitempty"`
	ShowViews           *ShowViewsStmt           `json:"showViews,omitempty"`
	DescribeView        *DescribeViewStmt        `json:"describeView,omitempty"`
	ShowMigrations      *ShowMigrationsStmt      `json:"showMigrations,omitempty"`
	ShowRateLimit       *ShowRateLimitStmt       `json:"showRateLimit,omitempty"`
	ShowVersions        *ShowVersionsStmt        `json:"showVersions,omitempty"`
	ShowActiveSnapshots *ShowActiveSnapshotsStmt `json:"showActiveSnapshots,omitempty"`
	ShowConflictLog     *ShowConflictLogStmt     `json:"showConflictLog,omitempty"`
	ExplainSelect       *ExplainSelectStmt       `json:"explainSelect,omitempty"`

	// Migration
	StartMigration    *StartMigrationStmt    `json:"startMigration,omitempty"`
	ApplyMigration    *ApplyMigrationStmt    `json:"applyMigration,omitempty"`
	ValidateMigration *ValidateMigrationStmt `json:"validateMigration,omitempty"`
	ValidateRollback  *ValidateRollbackStmt  `json:"validateRollback,omitempty"`

	// Ops
	Checkpoint        *CheckpointStmt        `json:"checkpoint,omitempty"`
	BackupDatabase    *BackupDatabaseStmt    `json:"backupDatabase,omitempty"`
	RestoreDatabase   *RestoreDatabaseStmt   `json:"restoreDatabase,omitempty"`
	InvalidateSession *InvalidateSessionStmt `json:"invalidateSession,omitempty"`
}

// ============================================================
// DML Statements
// ============================================================

// SelectField represents a single field in a SELECT clause.
type SelectField struct {
	Expression *Expr  `json:"expression"`       // Field name, *, aggregate, function call
	Alias      string `json:"alias,omitempty"`   // AS alias
}

// OrderByClause represents an ORDER BY field with direction.
type OrderByClause struct {
	Field     string `json:"field"`
	Direction string `json:"direction"` // "ASC" or "DESC"
}

// JoinClause represents a JOIN in a SELECT.
type JoinClause struct {
	JoinType   string          `json:"joinType"`   // "INNER","LEFT","RIGHT","FULL OUTER"
	Bundle     string          `json:"bundle"`     // Joined bundle name
	Conditions []JoinCondition `json:"conditions"` // ON conditions
}

// JoinCondition represents one ON condition in a JOIN.
type JoinCondition struct {
	LeftBundle  string `json:"leftBundle"`
	LeftField   string `json:"leftField"`
	Operator    string `json:"operator"`    // Usually "=="
	RightBundle string `json:"rightBundle"`
	RightField  string `json:"rightField"`
}

// SelectStmt represents a SELECT query.
type SelectStmt struct {
	Fields    []SelectField  `json:"fields"`
	Bundle    string         `json:"bundle"`
	Distinct  bool           `json:"distinct,omitempty"`
	Where     *Expr          `json:"where,omitempty"`
	Joins     []JoinClause   `json:"joins,omitempty"`
	GroupBy   []string       `json:"groupBy,omitempty"`
	Having    *Expr          `json:"having,omitempty"`
	OrderBy   []OrderByClause `json:"orderBy,omitempty"`
	Limit     int            `json:"limit,omitempty"`
	Offset    int            `json:"offset,omitempty"`
	ForUpdate bool           `json:"forUpdate,omitempty"`
}

// FieldValue represents a field name/value pair for INSERT and UPDATE.
type FieldValue struct {
	Name  string `json:"name"`
	Value *Value `json:"value"`
}

// InsertStmt represents an ADD DOCUMENT (INSERT) command.
type InsertStmt struct {
	Bundle string       `json:"bundle"`
	Fields []FieldValue `json:"fields"`
}

// UpdateStmt represents an UPDATE DOCUMENTS command.
type UpdateStmt struct {
	Bundle    string       `json:"bundle"`
	Fields    []FieldValue `json:"fields"`
	Confirmed bool         `json:"confirmed,omitempty"`
	Where     *Expr        `json:"where,omitempty"`
}

// DeleteStmt represents a DELETE DOCUMENTS command.
type DeleteStmt struct {
	Bundle    string `json:"bundle"`
	Confirmed bool   `json:"confirmed,omitempty"`
	Where     *Expr  `json:"where,omitempty"`
}

// ============================================================
// DDL - Bundle
// ============================================================

// BundleFieldDef represents a field definition in CREATE BUNDLE.
type BundleFieldDef struct {
	Name         string `json:"name"`
	Type         string `json:"type"`                   // "STRING","INT","FLOAT","BOOL","DATETIME","ARRAY","DATE"
	Required     bool   `json:"required"`
	Unique       bool   `json:"unique"`
	DefaultValue *Value `json:"defaultValue,omitempty"`  // Optional default
}

// CreateBundleStmt represents a CREATE BUNDLE command.
type CreateBundleStmt struct {
	Bundle string           `json:"bundle"`
	Fields []BundleFieldDef `json:"fields"`
}

// DropBundleStmt represents a DROP BUNDLE command.
type DropBundleStmt struct {
	Bundle string `json:"bundle"`
	Force  bool   `json:"force,omitempty"`
}

// FieldChange represents a single field modification in UPDATE BUNDLE.
type FieldChange struct {
	Action   string          `json:"action"`            // "ADD","REMOVE","MODIFY","RENAME"
	Field    string          `json:"field"`
	NewName  string          `json:"newName,omitempty"` // For RENAME
	FieldDef *BundleFieldDef `json:"fieldDef,omitempty"` // For ADD/MODIFY
}

// UpdateBundleStmt represents an UPDATE BUNDLE command.
type UpdateBundleStmt struct {
	Bundle        string        `json:"bundle"`
	NewBundleName string        `json:"newBundleName,omitempty"` // RENAME TO
	Changes       []FieldChange `json:"changes,omitempty"`
}

// ============================================================
// DDL - Index
// ============================================================

// IndexFieldDef represents a field in a CREATE INDEX command.
type IndexFieldDef struct {
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Unique   bool   `json:"unique"`
}

// CreateIndexStmt represents a CREATE INDEX command.
type CreateIndexStmt struct {
	IndexType string          `json:"indexType"` // "B-INDEX","HASH INDEX","BRIN INDEX"
	IndexName string          `json:"indexName"`
	Bundle    string          `json:"bundle"`
	Fields    []IndexFieldDef `json:"fields"`
}

// ============================================================
// DDL - View
// ============================================================

// CreateViewStmt represents a CREATE VIEW or CREATE MATERIALIZED VIEW command.
type CreateViewStmt struct {
	ViewName       string      `json:"viewName"`
	IsMaterialized bool        `json:"isMaterialized,omitempty"`
	Query          *SelectStmt `json:"query"` // Nested SELECT IR (not raw string)
}

// DropViewStmt represents a DROP VIEW command.
type DropViewStmt struct {
	ViewName       string `json:"viewName"`
	IsMaterialized bool   `json:"isMaterialized,omitempty"`
}

// RefreshViewStmt represents a REFRESH MATERIALIZED VIEW command.
type RefreshViewStmt struct {
	ViewName string `json:"viewName"`
}

// ============================================================
// Transaction
// ============================================================

// BeginTransactionStmt represents a BEGIN TRANSACTION command.
type BeginTransactionStmt struct{}

// CommitStmt represents a COMMIT command.
type CommitStmt struct{}

// RollbackStmt represents a ROLLBACK command.
type RollbackStmt struct{}

// SavepointStmt represents a SAVEPOINT command.
type SavepointStmt struct {
	Name string `json:"name"`
}

// RollbackToSavepointStmt represents a ROLLBACK TO SAVEPOINT command.
type RollbackToSavepointStmt struct {
	Name string `json:"name"`
}

// ============================================================
// Cursor
// ============================================================

// DeclareCursorStmt represents a DECLARE ... CURSOR FOR SELECT command.
type DeclareCursorStmt struct {
	CursorName string      `json:"cursorName"`
	Query      *SelectStmt `json:"query"` // Full nested SELECT IR
}

// FetchCursorStmt represents a FETCH command.
type FetchCursorStmt struct {
	CursorName string `json:"cursorName"`
	Count      int    `json:"count"`    // 0=ALL, 1=NEXT, >1=specific count
	FetchAll   bool   `json:"fetchAll,omitempty"`
}

// CloseCursorStmt represents a CLOSE cursor command.
type CloseCursorStmt struct {
	CursorName string `json:"cursorName"`
}

// ============================================================
// Prepared Statements
// ============================================================

// PrepareStmt represents a PREPARE ... AS query command.
type PrepareStmt struct {
	StatementName string      `json:"statementName"`
	Query         *SelectStmt `json:"query"` // Full nested SELECT IR with $N parameters
}

// ExecuteStmt represents an EXECUTE command.
type ExecuteStmt struct {
	StatementName string `json:"statementName"`
}

// DeallocateStmt represents a DEALLOCATE command.
type DeallocateStmt struct {
	StatementName string `json:"statementName"`
}

// ============================================================
// RBAC
// ============================================================

// CreateUserStmt represents a CREATE USER command.
type CreateUserStmt struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// UpdateUserStmt represents an UPDATE USER command.
type UpdateUserStmt struct {
	Username    string `json:"username"`
	NewPassword string `json:"newPassword"`
	Force       bool   `json:"force,omitempty"`
}

// DeleteUserStmt represents a DELETE USER command.
type DeleteUserStmt struct {
	Username string `json:"username"`
	Force    bool   `json:"force,omitempty"`
}

// CreateRoleStmt represents a CREATE ROLE command.
type CreateRoleStmt struct {
	RoleName    string `json:"roleName"`
	Description string `json:"description,omitempty"`
}

// UpdateRoleStmt represents an UPDATE ROLE command.
type UpdateRoleStmt struct {
	RoleName       string `json:"roleName"`
	NewDescription string `json:"newDescription"`
	Force          bool   `json:"force,omitempty"`
}

// DeleteRoleStmt represents a DELETE ROLE command.
type DeleteRoleStmt struct {
	RoleName string `json:"roleName"`
	Force    bool   `json:"force,omitempty"`
}

// GrantPermissionStmt represents a GRANT permission TO USER command.
type GrantPermissionStmt struct {
	Permission string `json:"permission"`
	Username   string `json:"username"`
}

// GrantRoleStmt represents a GRANT role TO USER command.
type GrantRoleStmt struct {
	RoleName string `json:"roleName"`
	Username string `json:"username"`
}

// RevokePermissionStmt represents a REVOKE permission FROM USER command.
type RevokePermissionStmt struct {
	Permission string `json:"permission"`
	Username   string `json:"username"`
}

// RevokeRoleStmt represents a REVOKE role FROM USER command.
type RevokeRoleStmt struct {
	RoleName string `json:"roleName"`
	Username string `json:"username"`
}

// ============================================================
// Database
// ============================================================

// UseDatabaseStmt represents a USE DATABASE command.
type UseDatabaseStmt struct {
	Database string `json:"database"`
}

// CreateDatabaseStmt represents a CREATE DATABASE command.
type CreateDatabaseStmt struct {
	Database string `json:"database"`
}

// DropDatabaseStmt represents a DROP DATABASE command.
type DropDatabaseStmt struct {
	Database string `json:"database"`
	Force    bool   `json:"force,omitempty"`
}

// RenameDatabaseStmt represents a RENAME DATABASE command.
type RenameDatabaseStmt struct {
	OldName string `json:"oldName"`
	NewName string `json:"newName"`
}

// AttachDatabaseStmt represents an ATTACH DATABASE command.
type AttachDatabaseStmt struct {
	Path  string `json:"path"`
	Alias string `json:"alias,omitempty"`
}

// ============================================================
// Utility / Show
// ============================================================

type ShowDatabasesStmt struct{}
type ShowBundlesStmt struct{}
type ShowBundleStmt struct {
	Bundle string `json:"bundle"`
}
type ShowUsersStmt struct{}
type ShowSessionsStmt struct{}
type ShowViewsStmt struct {
	Database string `json:"database,omitempty"`
}
type DescribeViewStmt struct {
	ViewName string `json:"viewName"`
}
type ShowMigrationsStmt struct {
	Database string `json:"database"`
}
type ShowRateLimitStmt struct{}
type ShowVersionsStmt struct {
	DocumentID string `json:"documentId,omitempty"`
	Bundle     string `json:"bundle,omitempty"`
}
type ShowActiveSnapshotsStmt struct{}
type ShowConflictLogStmt struct{}

// ExplainSelectStmt represents EXPLAIN SELECT.
type ExplainSelectStmt struct {
	Query *SelectStmt `json:"query"`
}

// ============================================================
// Migration
// ============================================================

type StartMigrationStmt struct {
	Description string `json:"description,omitempty"`
}

type ApplyMigrationStmt struct {
	MigrationID string `json:"migrationId"`
}

type ValidateMigrationStmt struct {
	MigrationID string `json:"migrationId"`
}

type ValidateRollbackStmt struct {
	MigrationID string `json:"migrationId"`
}

// ============================================================
// Ops
// ============================================================

type CheckpointStmt struct{}

type BackupDatabaseStmt struct {
	Database string `json:"database"`
	Path     string `json:"path,omitempty"`
}

type RestoreDatabaseStmt struct {
	Path     string `json:"path"`
	Database string `json:"database,omitempty"`
}

type InvalidateSessionStmt struct {
	SessionID string `json:"sessionId"`
}
