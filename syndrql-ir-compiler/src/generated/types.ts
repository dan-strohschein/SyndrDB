// AUTO-GENERATED from Go IR structs — do not edit manually.
// Run: go run . -codegen-ts <outdir>


export interface TrainingExample {
  nl: string;
  ir: IREnvelope;
  syndrql: string;
}

export interface IREnvelope {
  statements: Statement[];
  explanation?: string;
  confidence?: number;
}

export interface Statement {
  statementType: string;
  select?: SelectStmt;
  insert?: InsertStmt;
  update?: UpdateStmt;
  delete?: DeleteStmt;
  createBundle?: CreateBundleStmt;
  dropBundle?: DropBundleStmt;
  updateBundle?: UpdateBundleStmt;
  createIndex?: CreateIndexStmt;
  createView?: CreateViewStmt;
  dropView?: DropViewStmt;
  refreshView?: RefreshViewStmt;
  beginTransaction?: BeginTransactionStmt;
  commit?: CommitStmt;
  rollback?: RollbackStmt;
  savepoint?: SavepointStmt;
  rollbackToSavepoint?: RollbackToSavepointStmt;
  declareCursor?: DeclareCursorStmt;
  fetchCursor?: FetchCursorStmt;
  closeCursor?: CloseCursorStmt;
  prepare?: PrepareStmt;
  execute?: ExecuteStmt;
  deallocate?: DeallocateStmt;
  createUser?: CreateUserStmt;
  updateUser?: UpdateUserStmt;
  deleteUser?: DeleteUserStmt;
  createRole?: CreateRoleStmt;
  updateRole?: UpdateRoleStmt;
  deleteRole?: DeleteRoleStmt;
  grantPermission?: GrantPermissionStmt;
  grantRole?: GrantRoleStmt;
  revokePermission?: RevokePermissionStmt;
  revokeRole?: RevokeRoleStmt;
  useDatabase?: UseDatabaseStmt;
  createDatabase?: CreateDatabaseStmt;
  dropDatabase?: DropDatabaseStmt;
  renameDatabase?: RenameDatabaseStmt;
  attachDatabase?: AttachDatabaseStmt;
  showDatabases?: ShowDatabasesStmt;
  showBundles?: ShowBundlesStmt;
  showBundle?: ShowBundleStmt;
  showUsers?: ShowUsersStmt;
  showSessions?: ShowSessionsStmt;
  showViews?: ShowViewsStmt;
  describeView?: DescribeViewStmt;
  showMigrations?: ShowMigrationsStmt;
  showRateLimit?: ShowRateLimitStmt;
  showVersions?: ShowVersionsStmt;
  showActiveSnapshots?: ShowActiveSnapshotsStmt;
  showConflictLog?: ShowConflictLogStmt;
  explainSelect?: ExplainSelectStmt;
  startMigration?: StartMigrationStmt;
  applyMigration?: ApplyMigrationStmt;
  validateMigration?: ValidateMigrationStmt;
  validateRollback?: ValidateRollbackStmt;
  checkpoint?: CheckpointStmt;
  backupDatabase?: BackupDatabaseStmt;
  restoreDatabase?: RestoreDatabaseStmt;
  invalidateSession?: InvalidateSessionStmt;
}

export interface Expr {
  type: string;
  left?: Expr;
  operator?: string;
  right?: Expr;
  value?: Value;
  name?: string;
  bundle?: string;
  args?: Expr[];
  isBuiltIn?: boolean;
  index?: number;
  query?: SelectStmt;
}

export interface Value {
  type: string;
  raw: unknown;
}

export interface SelectStmt {
  fields: SelectField[];
  bundle: string;
  distinct?: boolean;
  where?: Expr;
  joins?: JoinClause[];
  groupBy?: string[];
  having?: Expr;
  orderBy?: OrderByClause[];
  limit?: number;
  offset?: number;
  forUpdate?: boolean;
}

export interface SelectField {
  expression?: Expr;
  alias?: string;
}

export interface OrderByClause {
  field: string;
  direction: string;
}

export interface JoinClause {
  joinType: string;
  bundle: string;
  conditions: JoinCondition[];
}

export interface JoinCondition {
  leftBundle: string;
  leftField: string;
  operator: string;
  rightBundle: string;
  rightField: string;
}

export interface InsertStmt {
  bundle: string;
  fields: FieldValue[];
}

export interface FieldValue {
  name: string;
  value?: Value;
}

export interface UpdateStmt {
  bundle: string;
  fields: FieldValue[];
  confirmed?: boolean;
  where?: Expr;
}

export interface DeleteStmt {
  bundle: string;
  confirmed?: boolean;
  where?: Expr;
}

export interface CreateBundleStmt {
  bundle: string;
  fields: BundleFieldDef[];
}

export interface BundleFieldDef {
  name: string;
  type: string;
  required: boolean;
  unique: boolean;
  defaultValue?: Value;
}

export interface DropBundleStmt {
  bundle: string;
  force?: boolean;
}

export interface FieldChange {
  action: string;
  field: string;
  newName?: string;
  fieldDef?: BundleFieldDef;
}

export interface UpdateBundleStmt {
  bundle: string;
  newBundleName?: string;
  changes?: FieldChange[];
}

export interface CreateIndexStmt {
  indexType: string;
  indexName: string;
  bundle: string;
  fields: IndexFieldDef[];
}

export interface IndexFieldDef {
  name: string;
  required: boolean;
  unique: boolean;
}

export interface CreateViewStmt {
  viewName: string;
  isMaterialized?: boolean;
  query?: SelectStmt;
}

export interface DropViewStmt {
  viewName: string;
  isMaterialized?: boolean;
}

export interface RefreshViewStmt {
  viewName: string;
}

export interface BeginTransactionStmt {
}

export interface CommitStmt {
}

export interface RollbackStmt {
}

export interface SavepointStmt {
  name: string;
}

export interface RollbackToSavepointStmt {
  name: string;
}

export interface DeclareCursorStmt {
  cursorName: string;
  query?: SelectStmt;
}

export interface FetchCursorStmt {
  cursorName: string;
  count: number;
  fetchAll?: boolean;
}

export interface CloseCursorStmt {
  cursorName: string;
}

export interface PrepareStmt {
  statementName: string;
  query?: SelectStmt;
}

export interface ExecuteStmt {
  statementName: string;
}

export interface DeallocateStmt {
  statementName: string;
}

export interface CreateUserStmt {
  username: string;
  password: string;
}

export interface UpdateUserStmt {
  username: string;
  newPassword: string;
  force?: boolean;
}

export interface DeleteUserStmt {
  username: string;
  force?: boolean;
}

export interface CreateRoleStmt {
  roleName: string;
  description?: string;
}

export interface UpdateRoleStmt {
  roleName: string;
  newDescription: string;
  force?: boolean;
}

export interface DeleteRoleStmt {
  roleName: string;
  force?: boolean;
}

export interface GrantPermissionStmt {
  permission: string;
  username: string;
}

export interface GrantRoleStmt {
  roleName: string;
  username: string;
}

export interface RevokePermissionStmt {
  permission: string;
  username: string;
}

export interface RevokeRoleStmt {
  roleName: string;
  username: string;
}

export interface UseDatabaseStmt {
  database: string;
}

export interface CreateDatabaseStmt {
  database: string;
}

export interface DropDatabaseStmt {
  database: string;
  force?: boolean;
}

export interface RenameDatabaseStmt {
  oldName: string;
  newName: string;
}

export interface AttachDatabaseStmt {
  path: string;
  alias?: string;
}

export interface ShowDatabasesStmt {
}

export interface ShowBundlesStmt {
}

export interface ShowBundleStmt {
  bundle: string;
}

export interface ShowUsersStmt {
}

export interface ShowSessionsStmt {
}

export interface ShowViewsStmt {
  database?: string;
}

export interface DescribeViewStmt {
  viewName: string;
}

export interface ShowMigrationsStmt {
  database: string;
}

export interface ShowRateLimitStmt {
}

export interface ShowVersionsStmt {
  documentId?: string;
  bundle?: string;
}

export interface ShowActiveSnapshotsStmt {
}

export interface ShowConflictLogStmt {
}

export interface ExplainSelectStmt {
  query?: SelectStmt;
}

export interface StartMigrationStmt {
  description?: string;
}

export interface ApplyMigrationStmt {
  migrationId: string;
}

export interface ValidateMigrationStmt {
  migrationId: string;
}

export interface ValidateRollbackStmt {
  migrationId: string;
}

export interface CheckpointStmt {
}

export interface BackupDatabaseStmt {
  database: string;
  path?: string;
}

export interface RestoreDatabaseStmt {
  path: string;
  database?: string;
}

export interface InvalidateSessionStmt {
  sessionId: string;
}

// All possible statementType values
export type StatementType =
  | 'select'
  | 'insert'
  | 'update'
  | 'delete'
  | 'createBundle'
  | 'dropBundle'
  | 'updateBundle'
  | 'createIndex'
  | 'createView'
  | 'dropView'
  | 'refreshView'
  | 'beginTransaction'
  | 'commit'
  | 'rollback'
  | 'savepoint'
  | 'rollbackToSavepoint'
  | 'declareCursor'
  | 'fetchCursor'
  | 'closeCursor'
  | 'prepare'
  | 'execute'
  | 'deallocate'
  | 'createUser'
  | 'updateUser'
  | 'deleteUser'
  | 'createRole'
  | 'updateRole'
  | 'deleteRole'
  | 'grantPermission'
  | 'grantRole'
  | 'revokePermission'
  | 'revokeRole'
  | 'useDatabase'
  | 'createDatabase'
  | 'dropDatabase'
  | 'renameDatabase'
  | 'attachDatabase'
  | 'showDatabases'
  | 'showBundles'
  | 'showBundle'
  | 'showUsers'
  | 'showSessions'
  | 'showViews'
  | 'describeView'
  | 'showMigrations'
  | 'showRateLimit'
  | 'showVersions'
  | 'showActiveSnapshots'
  | 'showConflictLog'
  | 'explainSelect'
  | 'startMigration'
  | 'applyMigration'
  | 'validateMigration'
  | 'validateRollback'
  | 'checkpoint'
  | 'backupDatabase'
  | 'restoreDatabase'
  | 'invalidateSession';

// All possible expression type values
export type ExprType =
  | 'binary'
  | 'literal'
  | 'identifier'
  | 'qualified'
  | 'function'
  | 'in'
  | 'notIn'
  | 'isNull'
  | 'isNotNull'
  | 'not'
  | 'parameter'
  | 'exists';

// All possible value type discriminators
export type ValueType = 'string' | 'int' | 'float' | 'bool' | 'null';
