package codegen

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"syndrql-training/ir"
)

// GenerateTypeScript generates TypeScript interfaces from Go IR structs
// and writes them to the specified output directory.
func GenerateTypeScript(outDir string) error {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	var b strings.Builder
	b.WriteString("// AUTO-GENERATED from Go IR structs — do not edit manually.\n")
	b.WriteString("// Run: go run . -codegen-ts <outdir>\n\n")

	// Collect all types we need to generate
	types := []reflect.Type{
		reflect.TypeOf(ir.TrainingExample{}),
		reflect.TypeOf(ir.IREnvelope{}),
		reflect.TypeOf(ir.Statement{}),
		reflect.TypeOf(ir.Expr{}),
		reflect.TypeOf(ir.Value{}),
		reflect.TypeOf(ir.SelectStmt{}),
		reflect.TypeOf(ir.SelectField{}),
		reflect.TypeOf(ir.OrderByClause{}),
		reflect.TypeOf(ir.JoinClause{}),
		reflect.TypeOf(ir.JoinCondition{}),
		reflect.TypeOf(ir.InsertStmt{}),
		reflect.TypeOf(ir.FieldValue{}),
		reflect.TypeOf(ir.UpdateStmt{}),
		reflect.TypeOf(ir.DeleteStmt{}),
		reflect.TypeOf(ir.CreateBundleStmt{}),
		reflect.TypeOf(ir.BundleFieldDef{}),
		reflect.TypeOf(ir.DropBundleStmt{}),
		reflect.TypeOf(ir.FieldChange{}),
		reflect.TypeOf(ir.UpdateBundleStmt{}),
		reflect.TypeOf(ir.CreateIndexStmt{}),
		reflect.TypeOf(ir.IndexFieldDef{}),
		reflect.TypeOf(ir.CreateViewStmt{}),
		reflect.TypeOf(ir.DropViewStmt{}),
		reflect.TypeOf(ir.RefreshViewStmt{}),
		reflect.TypeOf(ir.BeginTransactionStmt{}),
		reflect.TypeOf(ir.CommitStmt{}),
		reflect.TypeOf(ir.RollbackStmt{}),
		reflect.TypeOf(ir.SavepointStmt{}),
		reflect.TypeOf(ir.RollbackToSavepointStmt{}),
		reflect.TypeOf(ir.DeclareCursorStmt{}),
		reflect.TypeOf(ir.FetchCursorStmt{}),
		reflect.TypeOf(ir.CloseCursorStmt{}),
		reflect.TypeOf(ir.PrepareStmt{}),
		reflect.TypeOf(ir.ExecuteStmt{}),
		reflect.TypeOf(ir.DeallocateStmt{}),
		reflect.TypeOf(ir.CreateUserStmt{}),
		reflect.TypeOf(ir.UpdateUserStmt{}),
		reflect.TypeOf(ir.DeleteUserStmt{}),
		reflect.TypeOf(ir.CreateRoleStmt{}),
		reflect.TypeOf(ir.UpdateRoleStmt{}),
		reflect.TypeOf(ir.DeleteRoleStmt{}),
		reflect.TypeOf(ir.GrantPermissionStmt{}),
		reflect.TypeOf(ir.GrantRoleStmt{}),
		reflect.TypeOf(ir.RevokePermissionStmt{}),
		reflect.TypeOf(ir.RevokeRoleStmt{}),
		reflect.TypeOf(ir.UseDatabaseStmt{}),
		reflect.TypeOf(ir.CreateDatabaseStmt{}),
		reflect.TypeOf(ir.DropDatabaseStmt{}),
		reflect.TypeOf(ir.RenameDatabaseStmt{}),
		reflect.TypeOf(ir.AttachDatabaseStmt{}),
		reflect.TypeOf(ir.ShowDatabasesStmt{}),
		reflect.TypeOf(ir.ShowBundlesStmt{}),
		reflect.TypeOf(ir.ShowBundleStmt{}),
		reflect.TypeOf(ir.ShowUsersStmt{}),
		reflect.TypeOf(ir.ShowSessionsStmt{}),
		reflect.TypeOf(ir.ShowViewsStmt{}),
		reflect.TypeOf(ir.DescribeViewStmt{}),
		reflect.TypeOf(ir.ShowMigrationsStmt{}),
		reflect.TypeOf(ir.ShowRateLimitStmt{}),
		reflect.TypeOf(ir.ShowVersionsStmt{}),
		reflect.TypeOf(ir.ShowActiveSnapshotsStmt{}),
		reflect.TypeOf(ir.ShowConflictLogStmt{}),
		reflect.TypeOf(ir.ExplainSelectStmt{}),
		reflect.TypeOf(ir.StartMigrationStmt{}),
		reflect.TypeOf(ir.ApplyMigrationStmt{}),
		reflect.TypeOf(ir.ValidateMigrationStmt{}),
		reflect.TypeOf(ir.ValidateRollbackStmt{}),
		reflect.TypeOf(ir.CheckpointStmt{}),
		reflect.TypeOf(ir.BackupDatabaseStmt{}),
		reflect.TypeOf(ir.RestoreDatabaseStmt{}),
		reflect.TypeOf(ir.InvalidateSessionStmt{}),
	}

	generated := make(map[string]bool)
	for _, t := range types {
		generateInterface(&b, t, generated)
	}

	// Write statement type union
	b.WriteString("\n// All possible statementType values\n")
	b.WriteString("export type StatementType =\n")
	stmtTypes := []string{
		"select", "insert", "update", "delete",
		"createBundle", "dropBundle", "updateBundle",
		"createIndex",
		"createView", "dropView", "refreshView",
		"beginTransaction", "commit", "rollback", "savepoint", "rollbackToSavepoint",
		"declareCursor", "fetchCursor", "closeCursor",
		"prepare", "execute", "deallocate",
		"createUser", "updateUser", "deleteUser",
		"createRole", "updateRole", "deleteRole",
		"grantPermission", "grantRole", "revokePermission", "revokeRole",
		"useDatabase", "createDatabase", "dropDatabase", "renameDatabase", "attachDatabase",
		"showDatabases", "showBundles", "showBundle", "showUsers", "showSessions",
		"showViews", "describeView", "showMigrations", "showRateLimit",
		"showVersions", "showActiveSnapshots", "showConflictLog", "explainSelect",
		"startMigration", "applyMigration", "validateMigration", "validateRollback",
		"checkpoint", "backupDatabase", "restoreDatabase", "invalidateSession",
	}
	for i, st := range stmtTypes {
		if i < len(stmtTypes)-1 {
			b.WriteString(fmt.Sprintf("  | '%s'\n", st))
		} else {
			b.WriteString(fmt.Sprintf("  | '%s';\n", st))
		}
	}

	// Write expression type union
	b.WriteString("\n// All possible expression type values\n")
	b.WriteString("export type ExprType =\n")
	b.WriteString("  | 'binary'\n")
	b.WriteString("  | 'literal'\n")
	b.WriteString("  | 'identifier'\n")
	b.WriteString("  | 'qualified'\n")
	b.WriteString("  | 'function'\n")
	b.WriteString("  | 'in'\n")
	b.WriteString("  | 'notIn'\n")
	b.WriteString("  | 'isNull'\n")
	b.WriteString("  | 'isNotNull'\n")
	b.WriteString("  | 'not'\n")
	b.WriteString("  | 'parameter'\n")
	b.WriteString("  | 'exists';\n")

	// Write value type union
	b.WriteString("\n// All possible value type discriminators\n")
	b.WriteString("export type ValueType = 'string' | 'int' | 'float' | 'bool' | 'null';\n")

	outPath := filepath.Join(outDir, "types.ts")
	return os.WriteFile(outPath, []byte(b.String()), 0644)
}

func generateInterface(b *strings.Builder, t reflect.Type, generated map[string]bool) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}

	name := t.Name()
	if generated[name] {
		return
	}
	generated[name] = true

	b.WriteString(fmt.Sprintf("\nexport interface %s {\n", name))

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}

		parts := strings.Split(jsonTag, ",")
		jsonName := parts[0]
		if jsonName == "" {
			jsonName = field.Name
		}

		isOptional := false
		for _, p := range parts[1:] {
			if p == "omitempty" {
				isOptional = true
			}
		}

		tsType := goTypeToTS(field.Type)
		optMark := ""
		if isOptional || field.Type.Kind() == reflect.Ptr {
			optMark = "?"
		}

		b.WriteString(fmt.Sprintf("  %s%s: %s;\n", jsonName, optMark, tsType))
	}

	b.WriteString("}\n")
}

func goTypeToTS(t reflect.Type) string {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Slice:
		elem := goTypeToTS(t.Elem())
		return elem + "[]"
	case reflect.Map:
		keyType := goTypeToTS(t.Key())
		valType := goTypeToTS(t.Elem())
		return fmt.Sprintf("Record<%s, %s>", keyType, valType)
	case reflect.Interface:
		return "unknown"
	case reflect.Struct:
		return t.Name()
	default:
		return "unknown"
	}
}
