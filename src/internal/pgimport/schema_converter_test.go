package pgimport

import (
	"testing"
)

func TestConvertTable_Simple(t *testing.T) {
	stmt := &ParsedStatement{
		Type:      StmtCreateTable,
		TableName: "users",
		Columns: []PGColumn{
			{Name: "id", Type: "serial", PrimaryKey: true, NotNull: true},
			{Name: "name", Type: "varchar(100)", NotNull: true},
			{Name: "email", Type: "text", Unique: true},
			{Name: "age", Type: "integer", Default: "0"},
			{Name: "created_at", Type: "timestamp", Default: "now()"},
			{Name: "is_active", Type: "boolean", Default: "true"},
		},
	}

	cmd, warnings := ConvertTable(stmt)

	if cmd.BundleName != "users" {
		t.Errorf("BundleName = %q, want %q", cmd.BundleName, "users")
	}
	if cmd.CommandType != "CREATE" {
		t.Errorf("CommandType = %q, want %q", cmd.CommandType, "CREATE")
	}
	if len(cmd.Fields) != 6 {
		t.Fatalf("got %d fields, want 6", len(cmd.Fields))
	}

	// id: serial → int, PK → required+unique
	f := cmd.Fields[0]
	if f.Type != "int" {
		t.Errorf("id.Type = %q, want %q", f.Type, "int")
	}
	if !f.IsRequired || !f.IsUnique {
		t.Errorf("id should be required+unique, got required=%v unique=%v", f.IsRequired, f.IsUnique)
	}

	// name: varchar → string, NOT NULL
	f = cmd.Fields[1]
	if f.Type != "string" || !f.IsRequired {
		t.Errorf("name = %+v", f)
	}

	// email: text → string, UNIQUE
	f = cmd.Fields[2]
	if f.Type != "string" || !f.IsUnique {
		t.Errorf("email = %+v", f)
	}

	// age: integer → int, DEFAULT 0
	f = cmd.Fields[3]
	if f.Type != "int" || f.DefaultValue != "0" {
		t.Errorf("age = %+v", f)
	}

	// created_at: timestamp → datetime, DEFAULT F:NOW()
	f = cmd.Fields[4]
	if f.Type != "datetime" || f.DefaultValue != "F:NOW()" {
		t.Errorf("created_at = %+v", f)
	}

	// is_active: boolean → bool, DEFAULT true
	f = cmd.Fields[5]
	if f.Type != "bool" || f.DefaultValue != true {
		t.Errorf("is_active = %+v", f)
	}

	// Should have warning for serial type
	foundSerialWarning := false
	for _, w := range warnings {
		if w.Object == "users.id" {
			foundSerialWarning = true
		}
	}
	if !foundSerialWarning {
		t.Error("expected warning about serial type")
	}
}

func TestConvertTable_TableConstraints(t *testing.T) {
	stmt := &ParsedStatement{
		Type:      StmtCreateTable,
		TableName: "orders",
		Columns: []PGColumn{
			{Name: "id", Type: "integer"},
			{Name: "email", Type: "text"},
		},
		Constraints: []PGConstraint{
			{Type: ConstraintPrimaryKey, Columns: []string{"id"}},
			{Type: ConstraintUnique, Columns: []string{"email"}},
			{Type: ConstraintCheck, CheckExpr: "id > 0"},
		},
	}

	cmd, warnings := ConvertTable(stmt)

	// id should be required+unique (from PK constraint)
	f := cmd.Fields[0]
	if !f.IsRequired || !f.IsUnique {
		t.Errorf("id from PK constraint: required=%v unique=%v", f.IsRequired, f.IsUnique)
	}

	// email should be unique (from UNIQUE constraint)
	f = cmd.Fields[1]
	if !f.IsUnique {
		t.Error("email should be unique from constraint")
	}

	// Should have CHECK warning
	foundCheck := false
	for _, w := range warnings {
		if w.Phase == "schema" && w.Object == "orders" {
			foundCheck = true
		}
	}
	if !foundCheck {
		t.Error("expected CHECK constraint warning")
	}
}

func TestConvertIndex(t *testing.T) {
	tests := []struct {
		name           string
		def            *PGIndexDef
		wantIndexType  string
		wantWarning    bool
	}{
		{
			name:          "btree",
			def:           &PGIndexDef{Name: "idx1", Table: "t", Method: "btree", Columns: []string{"a"}},
			wantIndexType: "B-INDEX",
		},
		{
			name:          "hash",
			def:           &PGIndexDef{Name: "idx2", Table: "t", Method: "hash", Columns: []string{"a"}},
			wantIndexType: "HASH INDEX",
		},
		{
			name:          "brin",
			def:           &PGIndexDef{Name: "idx3", Table: "t", Method: "brin", Columns: []string{"a"}},
			wantIndexType: "BRIN INDEX",
		},
		{
			name:          "gin best-effort",
			def:           &PGIndexDef{Name: "idx4", Table: "t", Method: "gin", Columns: []string{"a"}},
			wantIndexType: "B-INDEX",
			wantWarning:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &ParsedStatement{IndexDef: tt.def}
			conv, warning := ConvertIndex(stmt)
			if conv.IndexType != tt.wantIndexType {
				t.Errorf("IndexType = %q, want %q", conv.IndexType, tt.wantIndexType)
			}
			if tt.wantWarning && warning.Message == "" {
				t.Error("expected warning")
			}
		})
	}
}

func TestConvertFK(t *testing.T) {
	fk := PGConstraint{
		Name:       "fk_orders_user",
		Type:       ConstraintForeignKey,
		Columns:    []string{"user_id"},
		RefTable:   "users",
		RefColumns: []string{"id"},
	}

	conv := ConvertFK("orders", fk)
	if conv == nil {
		t.Fatal("expected conversion result")
	}
	if conv.Command.SourceBundle != "orders" {
		t.Errorf("SourceBundle = %q", conv.Command.SourceBundle)
	}
	if conv.Command.SourceField != "user_id" {
		t.Errorf("SourceField = %q", conv.Command.SourceField)
	}
	if conv.Command.DestinationBundle != "users" {
		t.Errorf("DestinationBundle = %q", conv.Command.DestinationBundle)
	}
	if conv.Command.DestinationField != "id" {
		t.Errorf("DestinationField = %q", conv.Command.DestinationField)
	}
}

func TestConvertFK_NonFK(t *testing.T) {
	c := PGConstraint{Type: ConstraintPrimaryKey}
	if ConvertFK("t", c) != nil {
		t.Error("non-FK should return nil")
	}
}
