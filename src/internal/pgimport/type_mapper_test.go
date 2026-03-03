package pgimport

import (
	"testing"
)

func TestMapPGType(t *testing.T) {
	tests := []struct {
		pgType      string
		wantType    string
		wantWarning bool
	}{
		// Integer types
		{"integer", "int", false},
		{"bigint", "int", false},
		{"smallint", "int", false},
		{"int2", "int", false},
		{"int4", "int", false},
		{"int8", "int", false},
		{"INTEGER", "int", false},

		// Serial types
		{"serial", "int", true},
		{"bigserial", "int", true},
		{"smallserial", "int", true},

		// Float types
		{"real", "float", false},
		{"float4", "float", false},
		{"double precision", "float", false},
		{"float8", "float", false},
		{"numeric", "float", true},
		{"numeric(10,2)", "float", true},
		{"decimal", "float", true},
		{"money", "float", true},

		// String types
		{"varchar(255)", "string", false},
		{"character varying(100)", "string", false},
		{"text", "string", false},
		{"char(10)", "string", false},
		{"character(1)", "string", false},

		// Boolean
		{"boolean", "bool", false},
		{"bool", "bool", false},

		// DateTime
		{"timestamp", "datetime", false},
		{"timestamptz", "datetime", false},
		{"timestamp with time zone", "datetime", false},
		{"timestamp without time zone", "datetime", false},

		// Date
		{"date", "date", false},

		// UUID
		{"uuid", "string", true},

		// JSON
		{"json", "string", true},
		{"jsonb", "string", true},

		// Array types
		{"integer[]", "string", true},
		{"text[]", "string", true},
		{"varchar(255) array", "string", true},

		// Binary
		{"bytea", "string", true},

		// Interval
		{"interval", "string", true},

		// Unknown types
		{"hstore", "string", true},
		{"tsvector", "string", true},
	}

	for _, tt := range tests {
		t.Run(tt.pgType, func(t *testing.T) {
			gotType, gotWarning := mapPGType(tt.pgType)
			if gotType != tt.wantType {
				t.Errorf("mapPGType(%q) type = %q, want %q", tt.pgType, gotType, tt.wantType)
			}
			if tt.wantWarning && gotWarning == "" {
				t.Errorf("mapPGType(%q) expected warning but got none", tt.pgType)
			}
			if !tt.wantWarning && gotWarning != "" {
				t.Errorf("mapPGType(%q) unexpected warning: %q", tt.pgType, gotWarning)
			}
		})
	}
}

func TestMapPGDefault(t *testing.T) {
	tests := []struct {
		name        string
		pgDefault   string
		wantValue   interface{}
		wantWarning bool
	}{
		{"empty", "", nil, false},
		{"now()", "now()", "F:NOW()", false},
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP", "F:NOW()", false},
		{"current_date", "current_date", "F:NOW()", false},
		{"nextval", "nextval('users_id_seq'::regclass)", nil, true},
		{"true", "true", true, false},
		{"false", "false", false, false},
		{"null", "NULL", nil, false},
		{"string literal", "'pending'", "pending", false},
		{"number", "0", "0", false},
		{"cast expression", "'now()'::timestamp", "F:NOW()", false},
		{"string with cast", "'default_val'::character varying", "default_val", false},
		{"bool true quote", "'t'", true, false},
		{"bool false quote", "'f'", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotWarning := mapPGDefault(tt.pgDefault)
			if gotValue != tt.wantValue {
				t.Errorf("mapPGDefault(%q) value = %v (%T), want %v (%T)", tt.pgDefault, gotValue, gotValue, tt.wantValue, tt.wantValue)
			}
			if tt.wantWarning && gotWarning == "" {
				t.Errorf("mapPGDefault(%q) expected warning", tt.pgDefault)
			}
			if !tt.wantWarning && gotWarning != "" {
				t.Errorf("mapPGDefault(%q) unexpected warning: %q", tt.pgDefault, gotWarning)
			}
		})
	}
}

func TestMapPGConstraint(t *testing.T) {
	tests := []struct {
		name         string
		constraint   PGConstraint
		wantRequired bool
		wantUnique   bool
		wantWarning  bool
	}{
		{
			name:         "primary key",
			constraint:   PGConstraint{Type: ConstraintPrimaryKey, Columns: []string{"id"}},
			wantRequired: true,
			wantUnique:   true,
		},
		{
			name:         "unique",
			constraint:   PGConstraint{Type: ConstraintUnique, Columns: []string{"email"}},
			wantRequired: false,
			wantUnique:   true,
		},
		{
			name:         "foreign key",
			constraint:   PGConstraint{Type: ConstraintForeignKey, Columns: []string{"user_id"}, RefTable: "users", RefColumns: []string{"id"}},
			wantRequired: false,
			wantUnique:   false,
		},
		{
			name:         "check",
			constraint:   PGConstraint{Type: ConstraintCheck, CheckExpr: "age > 0"},
			wantRequired: false,
			wantUnique:   false,
			wantWarning:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRequired, gotUnique, gotWarning := mapPGConstraint(tt.constraint)
			if gotRequired != tt.wantRequired {
				t.Errorf("isRequired = %v, want %v", gotRequired, tt.wantRequired)
			}
			if gotUnique != tt.wantUnique {
				t.Errorf("isUnique = %v, want %v", gotUnique, tt.wantUnique)
			}
			if tt.wantWarning && gotWarning == "" {
				t.Errorf("expected warning")
			}
			if !tt.wantWarning && gotWarning != "" {
				t.Errorf("unexpected warning: %q", gotWarning)
			}
		})
	}
}
