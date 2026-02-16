package compiler

import (
	"fmt"
	"strings"

	"syndrql-training/ir"
)

// Compile compiles an IR Statement to a SyndrQL string.
func Compile(stmt *ir.Statement) (string, error) {
	switch stmt.StatementType {
	// DML
	case "select":
		return compileSelectStmt(stmt.Select)
	case "insert":
		return compileInsert(stmt.Insert)
	case "update":
		return compileUpdate(stmt.Update)
	case "delete":
		return compileDelete(stmt.Delete)

	// DDL - Bundle
	case "createBundle":
		return compileCreateBundle(stmt.CreateBundle)
	case "dropBundle":
		return compileDropBundle(stmt.DropBundle)
	case "updateBundle":
		return compileUpdateBundle(stmt.UpdateBundle)

	// DDL - Index
	case "createIndex":
		return compileCreateIndex(stmt.CreateIndex)

	// DDL - View
	case "createView":
		return compileCreateView(stmt.CreateView)
	case "dropView":
		return compileDropView(stmt.DropView)
	case "refreshView":
		return compileRefreshView(stmt.RefreshView)

	// Transaction
	case "beginTransaction":
		return "BEGIN TRANSACTION;", nil
	case "commit":
		return "COMMIT;", nil
	case "rollback":
		return "ROLLBACK;", nil
	case "savepoint":
		return compileSavepoint(stmt.Savepoint)
	case "rollbackToSavepoint":
		return compileRollbackToSavepoint(stmt.RollbackToSavepoint)

	// Cursor
	case "declareCursor":
		return compileDeclareCursor(stmt.DeclareCursor)
	case "fetchCursor":
		return compileFetchCursor(stmt.FetchCursor)
	case "closeCursor":
		return compileCloseCursor(stmt.CloseCursor)

	// Prepared
	case "prepare":
		return compilePrepare(stmt.Prepare)
	case "execute":
		return compileExecute(stmt.Execute)
	case "deallocate":
		return compileDeallocate(stmt.Deallocate)

	// RBAC
	case "createUser":
		return compileCreateUser(stmt.CreateUser)
	case "updateUser":
		return compileUpdateUser(stmt.UpdateUser)
	case "deleteUser":
		return compileDeleteUser(stmt.DeleteUser)
	case "createRole":
		return compileCreateRole(stmt.CreateRole)
	case "updateRole":
		return compileUpdateRole(stmt.UpdateRole)
	case "deleteRole":
		return compileDeleteRole(stmt.DeleteRole)
	case "grantPermission":
		return compileGrantPermission(stmt.GrantPermission)
	case "grantRole":
		return compileGrantRole(stmt.GrantRole)
	case "revokePermission":
		return compileRevokePermission(stmt.RevokePermission)
	case "revokeRole":
		return compileRevokeRole(stmt.RevokeRole)

	// Database
	case "useDatabase":
		return compileUseDatabase(stmt.UseDatabase)
	case "createDatabase":
		return compileCreateDatabase(stmt.CreateDatabase)
	case "dropDatabase":
		return compileDropDatabase(stmt.DropDatabase)
	case "renameDatabase":
		return compileRenameDatabase(stmt.RenameDatabase)
	case "attachDatabase":
		return compileAttachDatabase(stmt.AttachDatabase)

	// Utility
	case "showDatabases":
		return "SHOW DATABASES;", nil
	case "showBundles":
		return "SHOW BUNDLES;", nil
	case "showBundle":
		return compileShowBundle(stmt.ShowBundle)
	case "showUsers":
		return "SHOW USERS;", nil
	case "showSessions":
		return "SHOW SESSIONS;", nil
	case "showViews":
		return compileShowViews(stmt.ShowViews)
	case "describeView":
		return compileDescribeView(stmt.DescribeView)
	case "showMigrations":
		return compileShowMigrations(stmt.ShowMigrations)
	case "showRateLimit":
		return "SHOW RATE LIMIT;", nil
	case "showVersions":
		return compileShowVersions(stmt.ShowVersions)
	case "showActiveSnapshots":
		return "SHOW ACTIVE SNAPSHOTS;", nil
	case "showConflictLog":
		return "SHOW CONFLICT LOG;", nil
	case "explainSelect":
		return compileExplainSelect(stmt.ExplainSelect)

	// Migration
	case "startMigration":
		return compileStartMigration(stmt.StartMigration)
	case "applyMigration":
		return compileApplyMigration(stmt.ApplyMigration)
	case "validateMigration":
		return compileValidateMigration(stmt.ValidateMigration)
	case "validateRollback":
		return compileValidateRollback(stmt.ValidateRollback)

	// Ops
	case "checkpoint":
		return "CHECKPOINT;", nil
	case "backupDatabase":
		return compileBackupDatabase(stmt.BackupDatabase)
	case "restoreDatabase":
		return compileRestoreDatabase(stmt.RestoreDatabase)
	case "invalidateSession":
		return compileInvalidateSession(stmt.InvalidateSession)

	default:
		return "", fmt.Errorf("unknown statement type: %s", stmt.StatementType)
	}
}

// CompileAll compiles all statements in an IREnvelope.
func CompileAll(env *ir.IREnvelope) ([]string, error) {
	var results []string
	for i, stmt := range env.Statements {
		sql, err := Compile(&stmt)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", i, err)
		}
		results = append(results, sql)
	}
	return results, nil
}

// CompileSelect compiles a SelectStmt to SyndrQL. Exported for reuse by
// view/cursor/prepare compilers.
func CompileSelect(s *ir.SelectStmt) string {
	if s == nil {
		return ""
	}
	return compileSelectInner(s) + ";"
}

func compileSelectStmt(s *ir.SelectStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil select statement")
	}
	return CompileSelect(s), nil
}

func compileSelectInner(s *ir.SelectStmt) string {
	var b strings.Builder

	b.WriteString("SELECT ")
	if s.Distinct {
		b.WriteString("DISTINCT ")
	}

	// Fields
	if len(s.Fields) == 0 {
		b.WriteString("*")
	} else {
		for i, f := range s.Fields {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(CompileSelectFieldExpr(f.Expression))
			if f.Alias != "" {
				b.WriteString(" AS ")
				b.WriteString(f.Alias)
			}
		}
	}

	// FROM
	b.WriteString(fmt.Sprintf(` FROM "%s"`, s.Bundle))

	// JOIN
	for _, j := range s.Joins {
		b.WriteString(" ")
		b.WriteString(j.JoinType)
		b.WriteString(fmt.Sprintf(` JOIN "%s" ON `, j.Bundle))
		for ci, c := range j.Conditions {
			if ci > 0 {
				b.WriteString(" AND ")
			}
			b.WriteString(fmt.Sprintf(`"%s"."%s" %s "%s"."%s"`,
				c.LeftBundle, c.LeftField, c.Operator, c.RightBundle, c.RightField))
		}
	}

	// WHERE
	if s.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(CompileExpr(s.Where))
	}

	// GROUP BY
	if len(s.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		b.WriteString(strings.Join(s.GroupBy, ", "))
	}

	// HAVING
	if s.Having != nil {
		b.WriteString(" HAVING ")
		b.WriteString(CompileExpr(s.Having))
	}

	// ORDER BY
	if len(s.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, o := range s.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(o.Field)
			b.WriteString(" ")
			b.WriteString(o.Direction)
		}
	}

	// LIMIT
	if s.Limit > 0 {
		b.WriteString(fmt.Sprintf(" LIMIT %d", s.Limit))
	}

	// OFFSET
	if s.Offset > 0 {
		b.WriteString(fmt.Sprintf(" OFFSET %d", s.Offset))
	}

	// FOR UPDATE
	if s.ForUpdate {
		b.WriteString(" FOR UPDATE")
	}

	return b.String()
}

// compileValue compiles a Value to its SyndrQL representation.
func compileValue(v *ir.Value) string {
	return compileLiteral(v)
}

func compileInsert(s *ir.InsertStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil insert statement")
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "%s" WITH (`, s.Bundle))

	for i, f := range s.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("{%s = %s}", f.Name, compileValue(f.Value)))
	}

	b.WriteString(");")
	return b.String(), nil
}

func compileUpdate(s *ir.UpdateStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil update statement")
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`UPDATE DOCUMENTS IN BUNDLE "%s" (`, s.Bundle))

	for i, f := range s.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("%s = %s", f.Name, compileValue(f.Value)))
	}

	b.WriteString(")")

	if s.Confirmed {
		b.WriteString(" CONFIRMED")
	}

	if s.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(CompileExpr(s.Where))
	}

	b.WriteString(";")
	return b.String(), nil
}

func compileDelete(s *ir.DeleteStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil delete statement")
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`DELETE DOCUMENTS FROM "%s"`, s.Bundle))

	if s.Confirmed {
		b.WriteString(" CONFIRMED")
	}

	if s.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(CompileExpr(s.Where))
	}

	b.WriteString(";")
	return b.String(), nil
}

func compileCreateBundle(s *ir.CreateBundleStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil create bundle statement")
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`CREATE BUNDLE "%s" WITH FIELDS (`, s.Bundle))

	for i, f := range s.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf(`{"%s", "%s", %t, %t`, f.Name, f.Type, f.Required, f.Unique))
		if f.DefaultValue != nil {
			b.WriteString(", ")
			b.WriteString(compileValue(f.DefaultValue))
		}
		b.WriteString("}")
	}

	b.WriteString(");")
	return b.String(), nil
}

func compileDropBundle(s *ir.DropBundleStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil drop bundle statement")
	}
	result := fmt.Sprintf(`DROP BUNDLE "%s"`, s.Bundle)
	if s.Force {
		result += " FORCE"
	}
	return result + ";", nil
}

func compileUpdateBundle(s *ir.UpdateBundleStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil update bundle statement")
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`UPDATE BUNDLE "%s"`, s.Bundle))

	if s.NewBundleName != "" {
		b.WriteString(fmt.Sprintf(` RENAME TO "%s"`, s.NewBundleName))
	}

	for _, c := range s.Changes {
		switch c.Action {
		case "ADD":
			if c.FieldDef != nil {
				b.WriteString(fmt.Sprintf(` ADD FIELD "%s" {"%s", "%s", %t, %t}`,
					c.Field, c.FieldDef.Name, c.FieldDef.Type, c.FieldDef.Required, c.FieldDef.Unique))
			}
		case "REMOVE":
			b.WriteString(fmt.Sprintf(` REMOVE FIELD "%s"`, c.Field))
		case "MODIFY":
			if c.FieldDef != nil {
				b.WriteString(fmt.Sprintf(` MODIFY FIELD "%s" {"%s", "%s", %t, %t}`,
					c.Field, c.FieldDef.Name, c.FieldDef.Type, c.FieldDef.Required, c.FieldDef.Unique))
			}
		}
	}

	b.WriteString(";")
	return b.String(), nil
}

func compileCreateIndex(s *ir.CreateIndexStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil create index statement")
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`CREATE %s "%s" ON BUNDLE "%s" WITH FIELDS (`,
		s.IndexType, s.IndexName, s.Bundle))

	for i, f := range s.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf(`{"%s", %t, %t}`, f.Name, f.Required, f.Unique))
	}

	b.WriteString(");")
	return b.String(), nil
}

func compileCreateView(s *ir.CreateViewStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil create view statement")
	}

	var b strings.Builder
	if s.IsMaterialized {
		b.WriteString("CREATE MATERIALIZED VIEW ")
	} else {
		b.WriteString("CREATE VIEW ")
	}
	b.WriteString(fmt.Sprintf(`"%s" AS `, s.ViewName))
	b.WriteString(CompileSelect(s.Query))
	return b.String(), nil
}

func compileDropView(s *ir.DropViewStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil drop view statement")
	}
	if s.IsMaterialized {
		return fmt.Sprintf(`DROP MATERIALIZED VIEW "%s";`, s.ViewName), nil
	}
	return fmt.Sprintf(`DROP VIEW "%s";`, s.ViewName), nil
}

func compileRefreshView(s *ir.RefreshViewStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil refresh view statement")
	}
	return fmt.Sprintf(`REFRESH MATERIALIZED VIEW "%s";`, s.ViewName), nil
}

func compileSavepoint(s *ir.SavepointStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil savepoint statement")
	}
	return fmt.Sprintf(`SAVEPOINT "%s";`, s.Name), nil
}

func compileRollbackToSavepoint(s *ir.RollbackToSavepointStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil rollback to savepoint statement")
	}
	return fmt.Sprintf(`ROLLBACK TO SAVEPOINT "%s";`, s.Name), nil
}

func compileDeclareCursor(s *ir.DeclareCursorStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil declare cursor statement")
	}
	inner := compileSelectInner(s.Query)
	return fmt.Sprintf("DECLARE %s CURSOR FOR %s;", s.CursorName, inner), nil
}

func compileFetchCursor(s *ir.FetchCursorStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil fetch cursor statement")
	}
	if s.FetchAll {
		return fmt.Sprintf("FETCH ALL FROM %s;", s.CursorName), nil
	}
	if s.Count == 1 {
		return fmt.Sprintf("FETCH NEXT FROM %s;", s.CursorName), nil
	}
	return fmt.Sprintf("FETCH %d FROM %s;", s.Count, s.CursorName), nil
}

func compileCloseCursor(s *ir.CloseCursorStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil close cursor statement")
	}
	return fmt.Sprintf("CLOSE %s;", s.CursorName), nil
}

func compilePrepare(s *ir.PrepareStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil prepare statement")
	}
	inner := compileSelectInner(s.Query)
	return fmt.Sprintf("PREPARE %s AS %s;", s.StatementName, inner), nil
}

func compileExecute(s *ir.ExecuteStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil execute statement")
	}
	return fmt.Sprintf("EXECUTE %s;", s.StatementName), nil
}

func compileDeallocate(s *ir.DeallocateStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil deallocate statement")
	}
	return fmt.Sprintf("DEALLOCATE %s;", s.StatementName), nil
}

func compileCreateUser(s *ir.CreateUserStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil create user statement")
	}
	return fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD "%s";`, s.Username, s.Password), nil
}

func compileUpdateUser(s *ir.UpdateUserStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil update user statement")
	}
	result := fmt.Sprintf(`UPDATE USER "%s" SET PASSWORD = "%s"`, s.Username, s.NewPassword)
	if s.Force {
		result += " FORCE"
	}
	return result + ";", nil
}

func compileDeleteUser(s *ir.DeleteUserStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil delete user statement")
	}
	result := fmt.Sprintf(`DELETE USER "%s"`, s.Username)
	if s.Force {
		result += " FORCE"
	}
	return result + ";", nil
}

func compileCreateRole(s *ir.CreateRoleStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil create role statement")
	}
	result := fmt.Sprintf(`CREATE ROLE "%s"`, s.RoleName)
	if s.Description != "" {
		result += fmt.Sprintf(` WITH DESCRIPTION "%s"`, s.Description)
	}
	return result + ";", nil
}

func compileUpdateRole(s *ir.UpdateRoleStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil update role statement")
	}
	result := fmt.Sprintf(`UPDATE ROLE "%s" SET DESCRIPTION = "%s"`, s.RoleName, s.NewDescription)
	if s.Force {
		result += " FORCE"
	}
	return result + ";", nil
}

func compileDeleteRole(s *ir.DeleteRoleStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil delete role statement")
	}
	result := fmt.Sprintf(`DELETE ROLE "%s"`, s.RoleName)
	if s.Force {
		result += " FORCE"
	}
	return result + ";", nil
}

func compileGrantPermission(s *ir.GrantPermissionStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil grant permission statement")
	}
	return fmt.Sprintf(`GRANT "%s" TO USER "%s";`, s.Permission, s.Username), nil
}

func compileGrantRole(s *ir.GrantRoleStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil grant role statement")
	}
	return fmt.Sprintf(`GRANT "%s" TO USER "%s";`, s.RoleName, s.Username), nil
}

func compileRevokePermission(s *ir.RevokePermissionStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil revoke permission statement")
	}
	return fmt.Sprintf(`REVOKE "%s" FROM USER "%s";`, s.Permission, s.Username), nil
}

func compileRevokeRole(s *ir.RevokeRoleStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil revoke role statement")
	}
	return fmt.Sprintf(`REVOKE "%s" FROM USER "%s";`, s.RoleName, s.Username), nil
}

func compileUseDatabase(s *ir.UseDatabaseStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil use database statement")
	}
	return fmt.Sprintf(`USE DATABASE "%s";`, s.Database), nil
}

func compileCreateDatabase(s *ir.CreateDatabaseStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil create database statement")
	}
	return fmt.Sprintf(`CREATE DATABASE "%s";`, s.Database), nil
}

func compileDropDatabase(s *ir.DropDatabaseStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil drop database statement")
	}
	result := fmt.Sprintf(`DROP DATABASE "%s"`, s.Database)
	if s.Force {
		result += " FORCE"
	}
	return result + ";", nil
}

func compileRenameDatabase(s *ir.RenameDatabaseStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil rename database statement")
	}
	return fmt.Sprintf(`RENAME DATABASE "%s" TO "%s";`, s.OldName, s.NewName), nil
}

func compileAttachDatabase(s *ir.AttachDatabaseStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil attach database statement")
	}
	result := fmt.Sprintf(`ATTACH DATABASE "%s"`, s.Path)
	if s.Alias != "" {
		result += fmt.Sprintf(` AS "%s"`, s.Alias)
	}
	return result + ";", nil
}

func compileShowBundle(s *ir.ShowBundleStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil show bundle statement")
	}
	return fmt.Sprintf(`SHOW BUNDLE "%s";`, s.Bundle), nil
}

func compileShowViews(s *ir.ShowViewsStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil show views statement")
	}
	if s.Database != "" {
		return fmt.Sprintf(`SHOW VIEWS IN DATABASE "%s";`, s.Database), nil
	}
	return "SHOW VIEWS;", nil
}

func compileDescribeView(s *ir.DescribeViewStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil describe view statement")
	}
	return fmt.Sprintf(`DESCRIBE VIEW "%s";`, s.ViewName), nil
}

func compileShowMigrations(s *ir.ShowMigrationsStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil show migrations statement")
	}
	return fmt.Sprintf(`SHOW MIGRATIONS FOR "%s";`, s.Database), nil
}

func compileShowVersions(s *ir.ShowVersionsStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil show versions statement")
	}
	var b strings.Builder
	b.WriteString("SHOW VERSIONS")
	if s.DocumentID != "" {
		b.WriteString(fmt.Sprintf(` FOR "%s"`, s.DocumentID))
	}
	if s.Bundle != "" {
		b.WriteString(fmt.Sprintf(` IN BUNDLE "%s"`, s.Bundle))
	}
	b.WriteString(";")
	return b.String(), nil
}

func compileExplainSelect(s *ir.ExplainSelectStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil explain select statement")
	}
	inner := compileSelectInner(s.Query)
	return "EXPLAIN " + inner + ";", nil
}

func compileStartMigration(s *ir.StartMigrationStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil start migration statement")
	}
	if s.Description != "" {
		return fmt.Sprintf(`START MIGRATION WITH DESCRIPTION "%s";`, s.Description), nil
	}
	return "START MIGRATION;", nil
}

func compileApplyMigration(s *ir.ApplyMigrationStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil apply migration statement")
	}
	return fmt.Sprintf(`APPLY MIGRATION "%s";`, s.MigrationID), nil
}

func compileValidateMigration(s *ir.ValidateMigrationStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil validate migration statement")
	}
	return fmt.Sprintf(`VALIDATE MIGRATION "%s";`, s.MigrationID), nil
}

func compileValidateRollback(s *ir.ValidateRollbackStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil validate rollback statement")
	}
	return fmt.Sprintf(`VALIDATE ROLLBACK "%s";`, s.MigrationID), nil
}

func compileBackupDatabase(s *ir.BackupDatabaseStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil backup database statement")
	}
	result := fmt.Sprintf(`BACKUP DATABASE "%s"`, s.Database)
	if s.Path != "" {
		result += fmt.Sprintf(` TO "%s"`, s.Path)
	}
	return result + ";", nil
}

func compileRestoreDatabase(s *ir.RestoreDatabaseStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil restore database statement")
	}
	result := fmt.Sprintf(`RESTORE DATABASE FROM "%s"`, s.Path)
	if s.Database != "" {
		result += fmt.Sprintf(` AS "%s"`, s.Database)
	}
	return result + ";", nil
}

func compileInvalidateSession(s *ir.InvalidateSessionStmt) (string, error) {
	if s == nil {
		return "", fmt.Errorf("nil invalidate session statement")
	}
	return fmt.Sprintf(`INVALIDATE SESSION "%s";`, s.SessionID), nil
}
