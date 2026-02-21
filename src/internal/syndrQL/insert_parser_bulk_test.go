package syndrQL

import (
	"testing"
)

// TestParseBulkAdd_SingleDocument tests parsing BULK ADD with one document
func TestParseBulkAdd_SingleDocument(t *testing.T) {
	input := `BULK ADD DOCUMENTS TO BUNDLE "Users" WITH (({"name" = "Alice"}, {age = 28}))`
	parser, err := NewInsertParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.ParseBulkAdd()
	if err != nil {
		t.Fatalf("ParseBulkAdd failed: %v", err)
	}

	if stmt.BundleName != "Users" {
		t.Errorf("Expected bundle name 'Users', got '%s'", stmt.BundleName)
	}
	if !stmt.IsBulk {
		t.Error("Expected IsBulk to be true")
	}
	if len(stmt.Documents) != 1 {
		t.Fatalf("Expected 1 document, got %d", len(stmt.Documents))
	}
	if stmt.Documents[0]["name"] != "Alice" {
		t.Errorf("Expected name 'Alice', got '%v'", stmt.Documents[0]["name"])
	}
	if stmt.Documents[0]["age"] != int64(28) {
		t.Errorf("Expected age 28, got '%v'", stmt.Documents[0]["age"])
	}
}

// TestParseBulkAdd_MultipleDocuments tests parsing BULK ADD with multiple documents
func TestParseBulkAdd_MultipleDocuments(t *testing.T) {
	input := `BULK ADD DOCUMENTS TO BUNDLE "Users" WITH (
		({"name" = "Alice"}, {age = 28}),
		({"name" = "Bob"}, {age = 34}),
		({"name" = "Charlie"}, {age = 42})
	)`
	parser, err := NewInsertParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.ParseBulkAdd()
	if err != nil {
		t.Fatalf("ParseBulkAdd failed: %v", err)
	}

	if stmt.BundleName != "Users" {
		t.Errorf("Expected bundle name 'Users', got '%s'", stmt.BundleName)
	}
	if len(stmt.Documents) != 3 {
		t.Fatalf("Expected 3 documents, got %d", len(stmt.Documents))
	}

	expected := []struct {
		name string
		age  int64
	}{
		{"Alice", 28},
		{"Bob", 34},
		{"Charlie", 42},
	}

	for i, exp := range expected {
		if stmt.Documents[i]["name"] != exp.name {
			t.Errorf("Document %d: expected name '%s', got '%v'", i, exp.name, stmt.Documents[i]["name"])
		}
		if stmt.Documents[i]["age"] != exp.age {
			t.Errorf("Document %d: expected age %d, got '%v'", i, exp.age, stmt.Documents[i]["age"])
		}
	}
}

// TestParseBulkInsert_MultipleDocuments tests parsing BULK INSERT INTO syntax
func TestParseBulkInsert_MultipleDocuments(t *testing.T) {
	input := `BULK INSERT INTO BUNDLE "Products" VALUES (
		({sku = "ABC123"}, {price = 19.99}, {active = true}),
		({sku = "DEF456"}, {price = 29.99}, {active = false})
	)`
	parser, err := NewInsertParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.ParseBulkInsert()
	if err != nil {
		t.Fatalf("ParseBulkInsert failed: %v", err)
	}

	if stmt.BundleName != "Products" {
		t.Errorf("Expected bundle name 'Products', got '%s'", stmt.BundleName)
	}
	if !stmt.IsBulk {
		t.Error("Expected IsBulk to be true")
	}
	if len(stmt.Documents) != 2 {
		t.Fatalf("Expected 2 documents, got %d", len(stmt.Documents))
	}

	// First document
	if stmt.Documents[0]["sku"] != "ABC123" {
		t.Errorf("Expected sku 'ABC123', got '%v'", stmt.Documents[0]["sku"])
	}
	if stmt.Documents[0]["price"] != 19.99 {
		t.Errorf("Expected price 19.99, got '%v'", stmt.Documents[0]["price"])
	}
	if stmt.Documents[0]["active"] != true {
		t.Errorf("Expected active true, got '%v'", stmt.Documents[0]["active"])
	}

	// Second document
	if stmt.Documents[1]["active"] != false {
		t.Errorf("Expected active false, got '%v'", stmt.Documents[1]["active"])
	}
}

// TestParseBulkAdd_MixedFieldTypes tests various field types in bulk inserts
func TestParseBulkAdd_MixedFieldTypes(t *testing.T) {
	input := `BULK ADD DOCUMENTS TO BUNDLE "Data" WITH (
		({str_field = "hello"}, {int_field = 42}, {float_field = 3.14}, {bool_field = true}, {null_field = null})
	)`
	parser, err := NewInsertParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.ParseBulkAdd()
	if err != nil {
		t.Fatalf("ParseBulkAdd failed: %v", err)
	}

	if len(stmt.Documents) != 1 {
		t.Fatalf("Expected 1 document, got %d", len(stmt.Documents))
	}

	doc := stmt.Documents[0]
	if doc["str_field"] != "hello" {
		t.Errorf("Expected str_field 'hello', got '%v'", doc["str_field"])
	}
	if doc["int_field"] != int64(42) {
		t.Errorf("Expected int_field 42, got '%v'", doc["int_field"])
	}
	if doc["float_field"] != 3.14 {
		t.Errorf("Expected float_field 3.14, got '%v'", doc["float_field"])
	}
	if doc["bool_field"] != true {
		t.Errorf("Expected bool_field true, got '%v'", doc["bool_field"])
	}
	if doc["null_field"] != nil {
		t.Errorf("Expected null_field nil, got '%v'", doc["null_field"])
	}
}

// TestParseBulkAdd_WithSemicolon tests that trailing semicolons are accepted
func TestParseBulkAdd_WithSemicolon(t *testing.T) {
	input := `BULK ADD DOCUMENTS TO BUNDLE "Users" WITH (({"name" = "Alice"}, {age = 28}));`
	parser, err := NewInsertParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.ParseBulkAdd()
	if err != nil {
		t.Fatalf("ParseBulkAdd failed: %v", err)
	}

	if stmt.BundleName != "Users" {
		t.Errorf("Expected bundle name 'Users', got '%s'", stmt.BundleName)
	}
}

// TestParseBulkAdd_Error_EmptyDocument tests that empty document groups are rejected
func TestParseBulkAdd_Error_EmptyDocument(t *testing.T) {
	// A document group with no fields should fail at parseFieldValues
	input := `BULK ADD DOCUMENTS TO BUNDLE "Users" WITH (())`
	parser, err := NewInsertParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	_, err = parser.ParseBulkAdd()
	if err == nil {
		t.Error("Expected error for empty document group, got nil")
	}
}

// TestParseBulkAdd_Error_MissingSyntax tests various malformed syntax cases
func TestParseBulkAdd_Error_MissingSyntax(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"missing DOCUMENTS", `BULK ADD TO BUNDLE "Users" WITH (({"name" = "Alice"}))`},
		{"missing BUNDLE", `BULK ADD DOCUMENTS TO "Users" WITH (({"name" = "Alice"}))`},
		{"missing WITH", `BULK ADD DOCUMENTS TO BUNDLE "Users" (({"name" = "Alice"}))`},
		{"missing bundle name", `BULK ADD DOCUMENTS TO BUNDLE WITH (({"name" = "Alice"}))`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := NewInsertParser(tt.input)
			if err != nil {
				// Tokenization failure is acceptable
				return
			}

			_, err = parser.ParseBulkAdd()
			if err == nil {
				t.Errorf("Expected error for '%s', got nil", tt.name)
			}
		})
	}
}

// TestParseBulkInsert_Error_MissingSyntax tests malformed BULK INSERT INTO syntax
func TestParseBulkInsert_Error_MissingSyntax(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"missing INTO", `BULK INSERT BUNDLE "Users" VALUES (({"name" = "Alice"}))`},
		{"missing VALUES", `BULK INSERT INTO BUNDLE "Users" (({"name" = "Alice"}))`},
		{"missing BUNDLE", `BULK INSERT INTO "Users" VALUES (({"name" = "Alice"}))`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := NewInsertParser(tt.input)
			if err != nil {
				return
			}

			_, err = parser.ParseBulkInsert()
			if err == nil {
				t.Errorf("Expected error for '%s', got nil", tt.name)
			}
		})
	}
}

// TestParseBulkAdd_100Documents tests parsing a large batch
func TestParseBulkAdd_100Documents(t *testing.T) {
	// Build a BULK ADD with 100 documents
	var docs string
	for i := 0; i < 100; i++ {
		if i > 0 {
			docs += ","
		}
		docs += `({id = ` + itoa(i) + `})`
	}
	input := `BULK ADD DOCUMENTS TO BUNDLE "Items" WITH (` + docs + `)`

	parser, err := NewInsertParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.ParseBulkAdd()
	if err != nil {
		t.Fatalf("ParseBulkAdd failed: %v", err)
	}

	if len(stmt.Documents) != 100 {
		t.Errorf("Expected 100 documents, got %d", len(stmt.Documents))
	}
}

// TestParse_BackwardCompat tests that the existing Parse() method still works
func TestParse_BackwardCompat(t *testing.T) {
	// Note: "name" is TOKEN_NAME keyword, so use "username" as field name
	input := `ADD DOCUMENT TO BUNDLE "Users" WITH ({username = "Alice"}, {age = 28})`
	parser, err := NewInsertParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if stmt.BundleName != "Users" {
		t.Errorf("Expected bundle name 'Users', got '%s'", stmt.BundleName)
	}
	if stmt.IsBulk {
		t.Error("Expected IsBulk to be false for single document insert")
	}
	if stmt.Fields["username"] != "Alice" {
		t.Errorf("Expected username 'Alice', got '%v'", stmt.Fields["username"])
	}
}

// itoa is a simple int-to-string helper for test generation
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}
