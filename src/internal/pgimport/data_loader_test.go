package pgimport

import (
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
)

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name      string
		val       string
		syndrType string
		wantErr   bool
	}{
		{"int", "42", "int", false},
		{"int negative", "-10", "int", false},
		{"int bad", "abc", "int", true},
		{"float", "3.14", "float", false},
		{"float int", "42", "float", false},
		{"bool true", "t", "bool", false},
		{"bool false", "false", "bool", false},
		{"bool bad", "maybe", "bool", true},
		{"string", "hello", "string", false},
		{"datetime", "2024-01-15 10:30:00", "datetime", false},
		{"datetime tz", "2024-01-15 10:30:00+00", "datetime", false},
		{"date", "2024-01-15", "date", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := convertValue(tt.val, tt.syndrType)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val == nil {
				t.Error("expected non-nil value")
			}
		})
	}
}

func TestConvertValue_TypeCheck(t *testing.T) {
	// Int
	val, _ := convertValue("42", "int")
	if v, ok := val.(int64); !ok || v != 42 {
		t.Errorf("int = %v (%T)", val, val)
	}

	// Float
	val, _ = convertValue("3.14", "float")
	if v, ok := val.(float64); !ok || v != 3.14 {
		t.Errorf("float = %v (%T)", val, val)
	}

	// Bool
	val, _ = convertValue("t", "bool")
	if v, ok := val.(bool); !ok || !v {
		t.Errorf("bool = %v (%T)", val, val)
	}

	// DateTime
	val, _ = convertValue("2024-01-15 10:30:00", "datetime")
	if _, ok := val.(time.Time); !ok {
		t.Errorf("datetime = %v (%T)", val, val)
	}
}

func TestDataLoader_ConvertRow(t *testing.T) {
	dl := NewDataLoader(100, nil)

	bundleCmd := &models.BundleCommand{
		Fields: []models.FieldDefinition{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int"},
			{Name: "active", Type: "bool"},
		},
	}
	dl.RegisterSchema("users", []string{"id", "name", "age", "active"}, bundleCmd)

	// Normal row (COPY mode)
	kvs, err := dl.ConvertRow("users", []string{"1", "Alice", "30", "t"}, true)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(kvs) != 4 {
		t.Fatalf("got %d kvs, want 4", len(kvs))
	}
	if kvs[0].Key != "id" || kvs[0].Value != int64(1) {
		t.Errorf("kvs[0] = %+v", kvs[0])
	}
	if kvs[1].Key != "name" || kvs[1].Value != "Alice" {
		t.Errorf("kvs[1] = %+v", kvs[1])
	}

	// Row with NULL (COPY mode: empty string = NULL)
	kvs, err = dl.ConvertRow("users", []string{"2", "Bob", "", "f"}, true)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	// age should be skipped (NULL)
	if len(kvs) != 3 {
		t.Errorf("got %d kvs, want 3 (NULL skipped)", len(kvs))
	}
}

func TestDataLoader_BatchRows(t *testing.T) {
	dl := NewDataLoader(2, nil) // batch size 2

	bundleCmd := &models.BundleCommand{
		Fields: []models.FieldDefinition{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
	}
	dl.RegisterSchema("users", []string{"id", "name"}, bundleCmd)

	rows := [][]string{
		{"1", "Alice"},
		{"2", "Bob"},
		{"3", "Charlie"},
		{"4", "Diana"},
		{"5", "Eve"},
	}

	batches, warnings, errors := dl.BatchRows("users", rows, true, false)
	if len(errors) > 0 {
		t.Fatalf("errors: %v", errors)
	}
	if len(warnings) > 0 {
		t.Logf("warnings: %v", warnings)
	}

	// 5 rows / batch size 2 = 3 batches
	if len(batches) != 3 {
		t.Errorf("got %d batches, want 3", len(batches))
	}
	if len(batches[0].Documents) != 2 {
		t.Errorf("batch[0] has %d docs, want 2", len(batches[0].Documents))
	}
	if len(batches[2].Documents) != 1 {
		t.Errorf("batch[2] has %d docs, want 1", len(batches[2].Documents))
	}
}

func TestDataLoader_ContinueOnError(t *testing.T) {
	dl := NewDataLoader(100, nil)

	bundleCmd := &models.BundleCommand{
		Fields: []models.FieldDefinition{
			{Name: "id", Type: "int"},
		},
	}
	dl.RegisterSchema("t", []string{"id"}, bundleCmd)

	rows := [][]string{
		{"1"},
		{"not_a_number"},
		{"3"},
	}

	// With continueOnError = true, should skip bad row
	batches, _, errs := dl.BatchRows("t", rows, true, true)
	if len(errs) != 1 {
		t.Errorf("expected 1 error, got %d", len(errs))
	}
	if len(batches) != 1 || len(batches[0].Documents) != 2 {
		t.Errorf("expected 1 batch with 2 docs, got %d batches", len(batches))
	}

	// With continueOnError = false, should abort
	_, _, errs = dl.BatchRows("t", rows, true, false)
	if len(errs) != 1 {
		t.Errorf("expected 1 error, got %d", len(errs))
	}
}

func TestStripSQLQuotes(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"'hello'", "hello"},
		{"'it''s'", "it's"},
		{"NULL", "NULL"},
		{"42", "42"},
		{"'with spaces'", "with spaces"},
	}

	for _, tt := range tests {
		got := stripSQLQuotes(tt.input)
		if got != tt.want {
			t.Errorf("stripSQLQuotes(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
