package compiler

import (
	"testing"

	"syndrql-training/ir"
)

func TestCompileSelectStar(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Customers",
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Customers";`, got)
}

func TestCompileSelectFields(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{
				{Expression: ir.IdentExpr("name")},
				{Expression: ir.IdentExpr("email")},
			},
			Bundle: "Customers",
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT name, email FROM "Customers";`, got)
}

func TestCompileSelectDistinct(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields:   []ir.SelectField{{Expression: ir.IdentExpr("status")}},
			Bundle:   "Orders",
			Distinct: true,
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT DISTINCT status FROM "Orders";`, got)
}

func TestCompileSelectWhere(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Customers",
			Where:  ir.BinaryExpr(ir.IdentExpr("status"), "==", ir.LiteralExpr(ir.NewStringValue("active"))),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Customers" WHERE status == "active";`, got)
}

func TestCompileSelectCompoundWhere(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Products",
			Where: ir.BinaryExpr(
				ir.BinaryExpr(ir.IdentExpr("price"), ">", ir.LiteralExpr(ir.NewIntValue(100))),
				"AND",
				ir.BinaryExpr(ir.IdentExpr("category"), "==", ir.LiteralExpr(ir.NewStringValue("Electronics"))),
			),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Products" WHERE price > 100 AND category == "Electronics";`, got)
}

func TestCompileSelectOrderBy(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Customers",
			OrderBy: []ir.OrderByClause{
				{Field: "name", Direction: "ASC"},
			},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Customers" ORDER BY name ASC;`, got)
}

func TestCompileSelectLimitOffset(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Products",
			Limit:  10,
			Offset: 20,
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Products" LIMIT 10 OFFSET 20;`, got)
}

func TestCompileSelectGroupBy(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{
				{Expression: ir.IdentExpr("category")},
				{Expression: ir.FuncExpr("COUNT", false, ir.IdentExpr("*")), Alias: "total"},
			},
			Bundle:  "Products",
			GroupBy: []string{"category"},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT category, COUNT(*) AS total FROM "Products" GROUP BY category;`, got)
}

func TestCompileSelectHaving(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{
				{Expression: ir.IdentExpr("status")},
				{Expression: ir.FuncExpr("COUNT", false, ir.IdentExpr("*")), Alias: "cnt"},
			},
			Bundle:  "Orders",
			GroupBy: []string{"status"},
			Having:  ir.BinaryExpr(ir.FuncExpr("COUNT", false, ir.IdentExpr("*")), ">", ir.LiteralExpr(ir.NewIntValue(5))),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT status, COUNT(*) AS cnt FROM "Orders" GROUP BY status HAVING COUNT(*) > 5;`, got)
}

func TestCompileSelectJoin(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Orders",
			Joins: []ir.JoinClause{
				{
					JoinType: "INNER",
					Bundle:   "Customers",
					Conditions: []ir.JoinCondition{
						{LeftBundle: "Orders", LeftField: "customerID", Operator: "==", RightBundle: "Customers", RightField: "id"},
					},
				},
			},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Orders" INNER JOIN "Customers" ON "Orders"."customerID" == "Customers"."id";`, got)
}

func TestCompileSelectForUpdate(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields:    []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle:    "Inventory",
			Where:     ir.BinaryExpr(ir.IdentExpr("productID"), "==", ir.LiteralExpr(ir.NewStringValue("SKU-001"))),
			ForUpdate: true,
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Inventory" WHERE productID == "SKU-001" FOR UPDATE;`, got)
}

func TestCompileSelectLike(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Customers",
			Where:  ir.BinaryExpr(ir.IdentExpr("name"), "LIKE", ir.LiteralExpr(ir.NewStringValue("John%"))),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Customers" WHERE name LIKE "John%";`, got)
}

func TestCompileSelectIsNull(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Customers",
			Where:  ir.IsNullExpr(ir.IdentExpr("email")),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Customers" WHERE email IS NULL;`, got)
}

func TestCompileSelectIn(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Orders",
			Where: ir.InExpr(ir.IdentExpr("status"), []*ir.Expr{
				ir.LiteralExpr(ir.NewStringValue("pending")),
				ir.LiteralExpr(ir.NewStringValue("shipped")),
			}),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Orders" WHERE status IN ("pending", "shipped");`, got)
}

func TestCompileSelectFunction(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{
				{Expression: ir.FuncExpr("UPPER", true, ir.IdentExpr("name")), Alias: "upper_name"},
			},
			Bundle: "Customers",
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT F:UPPER(name) AS upper_name FROM "Customers";`, got)
}

func TestCompileSelectParameter(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Users",
			Where:  ir.BinaryExpr(ir.IdentExpr("age"), ">", ir.ParamExpr(1)),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Users" WHERE age > $1;`, got)
}

func TestCompileInsert(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "insert",
		Insert: &ir.InsertStmt{
			Bundle: "Customers",
			Fields: []ir.FieldValue{
				{Name: "name", Value: ir.NewStringValue("Alice")},
				{Name: "email", Value: ir.NewStringValue("alice@example.com")},
				{Name: "age", Value: ir.NewIntValue(30)},
			},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `ADD DOCUMENT TO BUNDLE "Customers" WITH ({name = "Alice"}, {email = "alice@example.com"}, {age = 30});`, got)
}

func TestCompileUpdate(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "update",
		Update: &ir.UpdateStmt{
			Bundle: "Orders",
			Fields: []ir.FieldValue{
				{Name: "status", Value: ir.NewStringValue("shipped")},
			},
			Where: ir.BinaryExpr(ir.IdentExpr("orderID"), "==", ir.LiteralExpr(ir.NewStringValue("ORD-123"))),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `UPDATE DOCUMENTS IN BUNDLE "Orders" (status = "shipped") WHERE orderID == "ORD-123";`, got)
}

func TestCompileUpdateConfirmed(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "update",
		Update: &ir.UpdateStmt{
			Bundle: "Logs",
			Fields: []ir.FieldValue{
				{Name: "archived", Value: ir.NewBoolValue(true)},
			},
			Confirmed: true,
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `UPDATE DOCUMENTS IN BUNDLE "Logs" (archived = true) CONFIRMED;`, got)
}

func TestCompileDelete(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "delete",
		Delete: &ir.DeleteStmt{
			Bundle: "Sessions",
			Where:  ir.BinaryExpr(ir.IdentExpr("expired"), "==", ir.LiteralExpr(ir.NewBoolValue(true))),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `DELETE DOCUMENTS FROM "Sessions" WHERE expired == true;`, got)
}

func TestCompileDeleteConfirmed(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "delete",
		Delete: &ir.DeleteStmt{
			Bundle:    "Logs",
			Confirmed: true,
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `DELETE DOCUMENTS FROM "Logs" CONFIRMED;`, got)
}

func TestCompileCreateBundle(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "createBundle",
		CreateBundle: &ir.CreateBundleStmt{
			Bundle: "Products",
			Fields: []ir.BundleFieldDef{
				{Name: "name", Type: "STRING", Required: true, Unique: false},
				{Name: "price", Type: "FLOAT", Required: true, Unique: false},
				{Name: "inStock", Type: "BOOL", Required: false, Unique: false, DefaultValue: ir.NewBoolValue(true)},
			},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `CREATE BUNDLE "Products" WITH FIELDS ({"name", "STRING", true, false}, {"price", "FLOAT", true, false}, {"inStock", "BOOL", false, false, true});`, got)
}

func TestCompileDropBundle(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "dropBundle",
		DropBundle:    &ir.DropBundleStmt{Bundle: "TempData", Force: true},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `DROP BUNDLE "TempData" FORCE;`, got)
}

func TestCompileCreateIndex(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "createIndex",
		CreateIndex: &ir.CreateIndexStmt{
			IndexType: "B-INDEX",
			IndexName: "idx_products_name",
			Bundle:    "Products",
			Fields:    []ir.IndexFieldDef{{Name: "name", Required: true, Unique: false}},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `CREATE B-INDEX "idx_products_name" ON BUNDLE "Products" WITH FIELDS ({"name", true, false});`, got)
}

func TestCompileCreateHashIndex(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "createIndex",
		CreateIndex: &ir.CreateIndexStmt{
			IndexType: "HASH INDEX",
			IndexName: "idx_customers_email",
			Bundle:    "Customers",
			Fields:    []ir.IndexFieldDef{{Name: "email", Required: true, Unique: true}},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `CREATE HASH INDEX "idx_customers_email" ON BUNDLE "Customers" WITH FIELDS ({"email", true, true});`, got)
}

func TestCompileCreateView(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "createView",
		CreateView: &ir.CreateViewStmt{
			ViewName: "ActiveCustomers",
			Query: &ir.SelectStmt{
				Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
				Bundle: "Customers",
				Where:  ir.BinaryExpr(ir.IdentExpr("active"), "==", ir.LiteralExpr(ir.NewBoolValue(true))),
			},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `CREATE VIEW "ActiveCustomers" AS SELECT * FROM "Customers" WHERE active == true;`, got)
}

func TestCompileCreateMaterializedView(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "createView",
		CreateView: &ir.CreateViewStmt{
			ViewName:       "OrderSummary",
			IsMaterialized: true,
			Query: &ir.SelectStmt{
				Fields: []ir.SelectField{
					{Expression: ir.IdentExpr("status")},
					{Expression: ir.FuncExpr("COUNT", false, ir.IdentExpr("*")), Alias: "total"},
				},
				Bundle:  "Orders",
				GroupBy: []string{"status"},
			},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `CREATE MATERIALIZED VIEW "OrderSummary" AS SELECT status, COUNT(*) AS total FROM "Orders" GROUP BY status;`, got)
}

func TestCompileDropView(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "dropView",
		DropView:      &ir.DropViewStmt{ViewName: "OldView"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `DROP VIEW "OldView";`, got)
}

func TestCompileRefreshView(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "refreshView",
		RefreshView:   &ir.RefreshViewStmt{ViewName: "OrderSummary"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `REFRESH MATERIALIZED VIEW "OrderSummary";`, got)
}

func TestCompileTransaction(t *testing.T) {
	tests := []struct {
		name string
		stmt *ir.Statement
		want string
	}{
		{"begin", &ir.Statement{StatementType: "beginTransaction", BeginTransaction: &ir.BeginTransactionStmt{}}, "BEGIN TRANSACTION;"},
		{"commit", &ir.Statement{StatementType: "commit", Commit: &ir.CommitStmt{}}, "COMMIT;"},
		{"rollback", &ir.Statement{StatementType: "rollback", Rollback: &ir.RollbackStmt{}}, "ROLLBACK;"},
		{"savepoint", &ir.Statement{StatementType: "savepoint", Savepoint: &ir.SavepointStmt{Name: "sp1"}}, `SAVEPOINT "sp1";`},
		{"rollback to savepoint", &ir.Statement{StatementType: "rollbackToSavepoint", RollbackToSavepoint: &ir.RollbackToSavepointStmt{Name: "sp1"}}, `ROLLBACK TO SAVEPOINT "sp1";`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compile(tt.stmt)
			assertNoError(t, err)
			assertEqual(t, tt.want, got)
		})
	}
}

func TestCompileDeclareCursor(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "declareCursor",
		DeclareCursor: &ir.DeclareCursorStmt{
			CursorName: "my_cursor",
			Query: &ir.SelectStmt{
				Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
				Bundle: "Orders",
				OrderBy: []ir.OrderByClause{{Field: "createdAt", Direction: "DESC"}},
			},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `DECLARE my_cursor CURSOR FOR SELECT * FROM "Orders" ORDER BY createdAt DESC;`, got)
}

func TestCompileFetchCursor(t *testing.T) {
	tests := []struct {
		name string
		stmt *ir.Statement
		want string
	}{
		{
			"fetch n",
			&ir.Statement{StatementType: "fetchCursor", FetchCursor: &ir.FetchCursorStmt{CursorName: "c1", Count: 10}},
			"FETCH 10 FROM c1;",
		},
		{
			"fetch next",
			&ir.Statement{StatementType: "fetchCursor", FetchCursor: &ir.FetchCursorStmt{CursorName: "c1", Count: 1}},
			"FETCH NEXT FROM c1;",
		},
		{
			"fetch all",
			&ir.Statement{StatementType: "fetchCursor", FetchCursor: &ir.FetchCursorStmt{CursorName: "c1", FetchAll: true}},
			"FETCH ALL FROM c1;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compile(tt.stmt)
			assertNoError(t, err)
			assertEqual(t, tt.want, got)
		})
	}
}

func TestCompileCloseCursor(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "closeCursor",
		CloseCursor:   &ir.CloseCursorStmt{CursorName: "my_cursor"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, "CLOSE my_cursor;", got)
}

func TestCompilePrepare(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "prepare",
		Prepare: &ir.PrepareStmt{
			StatementName: "find_user",
			Query: &ir.SelectStmt{
				Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
				Bundle: "Users",
				Where:  ir.BinaryExpr(ir.IdentExpr("age"), ">", ir.ParamExpr(1)),
			},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `PREPARE find_user AS SELECT * FROM "Users" WHERE age > $1;`, got)
}

func TestCompileExecute(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "execute",
		Execute:       &ir.ExecuteStmt{StatementName: "find_user"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, "EXECUTE find_user;", got)
}

func TestCompileDeallocate(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "deallocate",
		Deallocate:    &ir.DeallocateStmt{StatementName: "find_user"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, "DEALLOCATE find_user;", got)
}

func TestCompileCreateUser(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "createUser",
		CreateUser:    &ir.CreateUserStmt{Username: "admin", Password: "secret123"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `CREATE USER "admin" WITH PASSWORD "secret123";`, got)
}

func TestCompileGrantPermission(t *testing.T) {
	stmt := &ir.Statement{
		StatementType:   "grantPermission",
		GrantPermission: &ir.GrantPermissionStmt{Permission: "SELECT", Username: "analyst"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `GRANT "SELECT" TO USER "analyst";`, got)
}

func TestCompileRevokePermission(t *testing.T) {
	stmt := &ir.Statement{
		StatementType:    "revokePermission",
		RevokePermission: &ir.RevokePermissionStmt{Permission: "DELETE", Username: "intern"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `REVOKE "DELETE" FROM USER "intern";`, got)
}

func TestCompileUseDatabase(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "useDatabase",
		UseDatabase:   &ir.UseDatabaseStmt{Database: "production"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `USE DATABASE "production";`, got)
}

func TestCompileCreateDatabase(t *testing.T) {
	stmt := &ir.Statement{
		StatementType:  "createDatabase",
		CreateDatabase: &ir.CreateDatabaseStmt{Database: "analytics"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `CREATE DATABASE "analytics";`, got)
}

func TestCompileShowVersions(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "showVersions",
		ShowVersions:  &ir.ShowVersionsStmt{DocumentID: "doc-123", Bundle: "Orders"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SHOW VERSIONS FOR "doc-123" IN BUNDLE "Orders";`, got)
}

func TestCompileExplainSelect(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "explainSelect",
		ExplainSelect: &ir.ExplainSelectStmt{
			Query: &ir.SelectStmt{
				Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
				Bundle: "Products",
				Where:  ir.BinaryExpr(ir.IdentExpr("price"), ">", ir.LiteralExpr(ir.NewIntValue(50))),
			},
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `EXPLAIN SELECT * FROM "Products" WHERE price > 50;`, got)
}

func TestCompileCheckpoint(t *testing.T) {
	stmt := &ir.Statement{StatementType: "checkpoint", Checkpoint: &ir.CheckpointStmt{}}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, "CHECKPOINT;", got)
}

func TestCompileShowDatabases(t *testing.T) {
	stmt := &ir.Statement{StatementType: "showDatabases", ShowDatabases: &ir.ShowDatabasesStmt{}}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, "SHOW DATABASES;", got)
}

func TestCompileCreateRole(t *testing.T) {
	stmt := &ir.Statement{
		StatementType: "createRole",
		CreateRole:    &ir.CreateRoleStmt{RoleName: "analyst", Description: "Data analysis role"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `CREATE ROLE "analyst" WITH DESCRIPTION "Data analysis role";`, got)
}

func TestCompileStartMigration(t *testing.T) {
	stmt := &ir.Statement{
		StatementType:  "startMigration",
		StartMigration: &ir.StartMigrationStmt{Description: "Add user preferences"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `START MIGRATION WITH DESCRIPTION "Add user preferences";`, got)
}

func TestCompileOrPrecedence(t *testing.T) {
	// (status == "active" OR status == "pending") AND category == "VIP"
	stmt := &ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: "Customers",
			Where: ir.BinaryExpr(
				ir.BinaryExpr(
					ir.BinaryExpr(ir.IdentExpr("status"), "==", ir.LiteralExpr(ir.NewStringValue("active"))),
					"OR",
					ir.BinaryExpr(ir.IdentExpr("status"), "==", ir.LiteralExpr(ir.NewStringValue("pending"))),
				),
				"AND",
				ir.BinaryExpr(ir.IdentExpr("category"), "==", ir.LiteralExpr(ir.NewStringValue("VIP"))),
			),
		},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `SELECT * FROM "Customers" WHERE (status == "active" OR status == "pending") AND category == "VIP";`, got)
}

func TestCompileBackupDatabase(t *testing.T) {
	stmt := &ir.Statement{
		StatementType:  "backupDatabase",
		BackupDatabase: &ir.BackupDatabaseStmt{Database: "production", Path: "/backups/prod"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `BACKUP DATABASE "production" TO "/backups/prod";`, got)
}

func TestCompileInvalidateSession(t *testing.T) {
	stmt := &ir.Statement{
		StatementType:     "invalidateSession",
		InvalidateSession: &ir.InvalidateSessionStmt{SessionID: "sess-abc-123"},
	}
	got, err := Compile(stmt)
	assertNoError(t, err)
	assertEqual(t, `INVALIDATE SESSION "sess-abc-123";`, got)
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertEqual(t *testing.T, want, got string) {
	t.Helper()
	if got != want {
		t.Errorf("mismatch:\n  want: %s\n  got:  %s", want, got)
	}
}
