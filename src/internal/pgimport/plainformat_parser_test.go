package pgimport

import (
	"strings"
	"testing"
)

func TestParseCreateTable_Simple(t *testing.T) {
	sql := `CREATE TABLE "users" (
		id serial PRIMARY KEY,
		name varchar(100) NOT NULL,
		email text UNIQUE,
		age integer DEFAULT 0,
		created_at timestamp DEFAULT now(),
		is_active boolean DEFAULT true
	)`

	stmt, err := parseCreateTable(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.TableName != "users" {
		t.Errorf("table name = %q, want %q", stmt.TableName, "users")
	}
	if len(stmt.Columns) != 6 {
		t.Fatalf("got %d columns, want 6", len(stmt.Columns))
	}

	// Check column: id serial PRIMARY KEY
	col := stmt.Columns[0]
	if col.Name != "id" || !strings.Contains(strings.ToLower(col.Type), "serial") {
		t.Errorf("col[0] = %+v", col)
	}
	if !col.PrimaryKey {
		t.Error("col 'id' should be PrimaryKey")
	}

	// Check column: name varchar(100) NOT NULL
	col = stmt.Columns[1]
	if col.Name != "name" || !col.NotNull {
		t.Errorf("col[1] = %+v", col)
	}

	// Check column: email text UNIQUE
	col = stmt.Columns[2]
	if col.Name != "email" || !col.Unique {
		t.Errorf("col[2] = %+v", col)
	}

	// Check column: age integer DEFAULT 0
	col = stmt.Columns[3]
	if col.Name != "age" || col.Default != "0" {
		t.Errorf("col[3] = %+v", col)
	}
}

func TestParseCreateTable_SchemaQualified(t *testing.T) {
	sql := `CREATE TABLE "public"."orders" (
		id serial PRIMARY KEY,
		user_id integer REFERENCES "users"(id),
		total numeric(10,2) NOT NULL,
		status varchar(20) DEFAULT 'pending'
	)`

	stmt, err := parseCreateTable(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.TableName != "orders" {
		t.Errorf("table name = %q, want %q", stmt.TableName, "orders")
	}
	if len(stmt.Columns) != 4 {
		t.Fatalf("got %d columns, want 4", len(stmt.Columns))
	}

	// Check FK constraint from inline REFERENCES
	if len(stmt.Constraints) < 1 {
		t.Fatal("expected at least 1 constraint from inline REFERENCES")
	}
	foundFK := false
	for _, c := range stmt.Constraints {
		if c.Type == ConstraintForeignKey && c.RefTable == "users" {
			foundFK = true
			if len(c.Columns) != 1 || c.Columns[0] != "user_id" {
				t.Errorf("FK columns = %v, want [user_id]", c.Columns)
			}
		}
	}
	if !foundFK {
		t.Error("expected FK constraint referencing 'users'")
	}
}

func TestParseCreateTable_TableConstraints(t *testing.T) {
	sql := `CREATE TABLE "orders" (
		id integer,
		user_id integer,
		product_id integer,
		CONSTRAINT orders_pk PRIMARY KEY (id),
		CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES "users" (id),
		CONSTRAINT uq_pair UNIQUE (user_id, product_id),
		CHECK (id > 0)
	)`

	stmt, err := parseCreateTable(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stmt.Columns) != 3 {
		t.Errorf("got %d columns, want 3", len(stmt.Columns))
	}

	pkFound, fkFound, uqFound, checkFound := false, false, false, false
	for _, c := range stmt.Constraints {
		switch c.Type {
		case ConstraintPrimaryKey:
			pkFound = true
			if len(c.Columns) != 1 || c.Columns[0] != "id" {
				t.Errorf("PK columns = %v", c.Columns)
			}
		case ConstraintForeignKey:
			fkFound = true
			if c.RefTable != "users" {
				t.Errorf("FK ref table = %q", c.RefTable)
			}
		case ConstraintUnique:
			uqFound = true
			if len(c.Columns) != 2 {
				t.Errorf("UNIQUE columns = %v", c.Columns)
			}
		case ConstraintCheck:
			checkFound = true
		}
	}
	if !pkFound {
		t.Error("missing PK constraint")
	}
	if !fkFound {
		t.Error("missing FK constraint")
	}
	if !uqFound {
		t.Error("missing UNIQUE constraint")
	}
	if !checkFound {
		t.Error("missing CHECK constraint")
	}
}

func TestParseCreateTable_IfNotExists(t *testing.T) {
	sql := `CREATE TABLE IF NOT EXISTS "test_table" (id integer)`
	stmt, err := parseCreateTable(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmt.TableName != "test_table" {
		t.Errorf("table name = %q, want %q", stmt.TableName, "test_table")
	}
}

func TestParseCreateIndex(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantTable  string
		wantUnique bool
		wantMethod string
		wantCols   int
	}{
		{
			name:       "simple btree",
			sql:        `CREATE INDEX idx_orders_user ON "orders" (user_id)`,
			wantName:   "idx_orders_user",
			wantTable:  "orders",
			wantUnique: false,
			wantMethod: "btree",
			wantCols:   1,
		},
		{
			name:       "unique index",
			sql:        `CREATE UNIQUE INDEX idx_users_email ON "users" (email)`,
			wantName:   "idx_users_email",
			wantTable:  "users",
			wantUnique: true,
			wantMethod: "btree",
			wantCols:   1,
		},
		{
			name:       "hash index",
			sql:        `CREATE INDEX idx_hash ON "users" USING hash (email)`,
			wantName:   "idx_hash",
			wantTable:  "users",
			wantMethod: "hash",
			wantCols:   1,
		},
		{
			name:       "multi-column",
			sql:        `CREATE INDEX idx_multi ON "orders" (user_id, status)`,
			wantName:   "idx_multi",
			wantTable:  "orders",
			wantCols:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parseCreateIndex(tt.sql)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if stmt.IndexDef == nil {
				t.Fatal("IndexDef is nil")
			}
			if stmt.IndexDef.Name != tt.wantName {
				t.Errorf("name = %q, want %q", stmt.IndexDef.Name, tt.wantName)
			}
			if stmt.IndexDef.Table != tt.wantTable {
				t.Errorf("table = %q, want %q", stmt.IndexDef.Table, tt.wantTable)
			}
			if stmt.IndexDef.Unique != tt.wantUnique {
				t.Errorf("unique = %v, want %v", stmt.IndexDef.Unique, tt.wantUnique)
			}
			if tt.wantMethod != "" && stmt.IndexDef.Method != tt.wantMethod {
				t.Errorf("method = %q, want %q", stmt.IndexDef.Method, tt.wantMethod)
			}
			if tt.wantCols > 0 && len(stmt.IndexDef.Columns) != tt.wantCols {
				t.Errorf("columns = %v (len %d), want %d", stmt.IndexDef.Columns, len(stmt.IndexDef.Columns), tt.wantCols)
			}
		})
	}
}

func TestParseAlterTableConstraint(t *testing.T) {
	sql := `ALTER TABLE ONLY "public"."orders" ADD CONSTRAINT "fk_orders_user" FOREIGN KEY (user_id) REFERENCES "users" (id)`

	stmt, err := parseAlterTableConstraint(sql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if stmt.TableName != "orders" {
		t.Errorf("table = %q, want %q", stmt.TableName, "orders")
	}
	if len(stmt.Constraints) != 1 {
		t.Fatalf("got %d constraints, want 1", len(stmt.Constraints))
	}
	c := stmt.Constraints[0]
	if c.Type != ConstraintForeignKey {
		t.Errorf("constraint type = %v, want FK", c.Type)
	}
	if c.RefTable != "users" {
		t.Errorf("ref table = %q, want %q", c.RefTable, "users")
	}
}

func TestParseCopyHeader(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		wantName string
		wantCols int
	}{
		{
			name:     "with columns",
			line:     `COPY "users" (id, name, email, age) FROM stdin`,
			wantName: "users",
			wantCols: 4,
		},
		{
			name:     "schema qualified",
			line:     `COPY "public"."users" (id, name) FROM stdin`,
			wantName: "users",
			wantCols: 2,
		},
		{
			name:     "without columns",
			line:     `COPY "users" FROM stdin`,
			wantName: "users",
			wantCols: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, err := parseCopyHeader(tt.line)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			if meta.TableName != tt.wantName {
				t.Errorf("table = %q, want %q", meta.TableName, tt.wantName)
			}
			if len(meta.Columns) != tt.wantCols {
				t.Errorf("columns = %v (len %d), want %d", meta.Columns, len(meta.Columns), tt.wantCols)
			}
		})
	}
}

func TestParseInsert(t *testing.T) {
	sql := `INSERT INTO "users" (id, name, email) VALUES (1, 'Alice', 'alice@test.com'), (2, 'Bob', NULL)`

	stmt, err := parseInsert(sql)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if stmt.TableName != "users" {
		t.Errorf("table = %q, want %q", stmt.TableName, "users")
	}
	if len(stmt.InsertColumns) != 3 {
		t.Errorf("columns = %v", stmt.InsertColumns)
	}
	if len(stmt.InsertRows) != 2 {
		t.Fatalf("got %d rows, want 2", len(stmt.InsertRows))
	}
	if stmt.InsertRows[0][1] != "'Alice'" {
		t.Errorf("row[0][1] = %q, want %q", stmt.InsertRows[0][1], "'Alice'")
	}
}

func TestUnescapeCopyField(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"\\\\", "\\"},
		{"line1\\nline2", "line1\nline2"},
		{"col1\\tcol2", "col1\tcol2"},
		{"\\N", ""}, // NULL marker
	}

	for _, tt := range tests {
		got := unescapeCopyField(tt.input)
		if got != tt.want {
			t.Errorf("unescapeCopyField(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPlainFormatParser_FullDump(t *testing.T) {
	dump := `
-- PostgreSQL dump
SET statement_timeout = 0;
SET client_encoding = 'UTF8';

CREATE TABLE "users" (
    id serial PRIMARY KEY,
    name varchar(100) NOT NULL,
    email text UNIQUE
);

CREATE TABLE "orders" (
    id serial PRIMARY KEY,
    user_id integer,
    total numeric(10,2) NOT NULL
);

CREATE INDEX idx_orders_user ON "orders" (user_id);

ALTER TABLE ONLY "orders" ADD CONSTRAINT "fk_orders_user" FOREIGN KEY (user_id) REFERENCES "users" (id);

COPY "users" (id, name, email) FROM stdin;
1	Alice	alice@test.com
2	Bob	\N
3	Charlie	charlie@test.com
\.

INSERT INTO "orders" (id, user_id, total) VALUES (1, 1, 99.99), (2, 2, 49.50);

CREATE SEQUENCE users_id_seq;
CREATE FUNCTION test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
COMMENT ON TABLE "users" IS 'User table';
`

	parser := NewPlainFormatParser(strings.NewReader(dump), nil)

	var stmts []*ParsedStatement
	for {
		stmt, err := parser.NextStatement()
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if stmt == nil {
			break
		}
		stmts = append(stmts, stmt)
	}

	// Count by type
	counts := make(map[StatementType]int)
	for _, s := range stmts {
		counts[s.Type]++
	}

	if counts[StmtCreateTable] != 2 {
		t.Errorf("CREATE TABLE count = %d, want 2", counts[StmtCreateTable])
	}
	if counts[StmtCreateIndex] != 1 {
		t.Errorf("CREATE INDEX count = %d, want 1", counts[StmtCreateIndex])
	}
	if counts[StmtAlterTable] != 1 {
		t.Errorf("ALTER TABLE count = %d, want 1", counts[StmtAlterTable])
	}
	if counts[StmtCopyFrom] < 1 {
		t.Errorf("COPY FROM count = %d, want >= 1", counts[StmtCopyFrom])
	}
	if counts[StmtInsertInto] != 1 {
		t.Errorf("INSERT INTO count = %d, want 1", counts[StmtInsertInto])
	}

	// Verify COPY data was parsed
	for _, s := range stmts {
		if s.Type == StmtCopyFrom && s.InsertRows != nil {
			if len(s.InsertRows) != 3 {
				t.Errorf("COPY rows = %d, want 3", len(s.InsertRows))
			}
			// Check Bob's NULL email
			if len(s.InsertRows) >= 2 {
				bobEmail := s.InsertRows[1][2]
				// \N is unescaped to "" by unescapeCopyField
				if bobEmail != "" {
					t.Errorf("Bob's email = %q, want empty (NULL)", bobEmail)
				}
			}
			break
		}
	}
}

func TestPlainFormatParser_DollarQuoting(t *testing.T) {
	dump := `
CREATE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE NOTICE 'hello; world';
END;
$$ LANGUAGE plpgsql;

CREATE TABLE "test" (id integer);
`

	parser := NewPlainFormatParser(strings.NewReader(dump), nil)

	var tableFound bool
	for {
		stmt, err := parser.NextStatement()
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if stmt == nil {
			break
		}
		if stmt.Type == StmtCreateTable && stmt.TableName == "test" {
			tableFound = true
		}
	}

	if !tableFound {
		t.Error("CREATE TABLE 'test' not found after dollar-quoted function")
	}
}

func TestStripSchemaAndQuotes(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`"public"."users"`, "users"},
		{`"users"`, "users"},
		{`users`, "users"},
		{`public.users`, "users"},
		{`"schema"."table_name"`, "table_name"},
	}

	for _, tt := range tests {
		got := stripSchemaAndQuotes(tt.input)
		if got != tt.want {
			t.Errorf("stripSchemaAndQuotes(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestExtractParenBody(t *testing.T) {
	tests := []struct {
		input string
		idx   int
		want  string
	}{
		{"(a, b, c)", 0, "a, b, c"},
		{"foo(bar, baz)", 3, "bar, baz"},
		{"(nested (inner) outer)", 0, "nested (inner) outer"},
	}

	for _, tt := range tests {
		got := extractParenBody(tt.input, tt.idx)
		if got != tt.want {
			t.Errorf("extractParenBody(%q, %d) = %q, want %q", tt.input, tt.idx, got, tt.want)
		}
	}
}
