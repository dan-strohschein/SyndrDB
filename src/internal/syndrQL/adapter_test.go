package syndrQL

import (
	"strings"
	"testing"

	"syndrdb/src/internal/query/queryparser"
)

// TestSerializeWhereGroupToClauseString_QuotesFieldNames verifies that
// serializeWhereGroupToClauseString double-quotes field names so that
// reserved words and names with spaces are parseable.
func TestSerializeWhereGroupToClauseString_QuotesFieldNames(t *testing.T) {
	// Field name that would be ambiguous without quotes (reserved word)
	whereGroup := &queryparser.WhereGroup{
		Clauses: []queryparser.WhereClause{
			{Field: "select", Operator: "==", Value: 1},
			{Field: "field name", Operator: "==", Value: "x"},
		},
		SubGroups: nil,
		Operator:  "AND",
	}

	serialized := serializeWhereGroupToClauseString(whereGroup)

	// Serialized form must quote field names so it is parseable
	if serialized == "" {
		t.Fatal("expected non-empty serialized WHERE clause")
	}
	// Should contain quoted "select" and "field name"
	if !containsQuoted(serialized, "select") {
		t.Errorf("serialized WHERE should quote reserved word field: %q", serialized)
	}
	if !containsQuoted(serialized, "field name") {
		t.Errorf("serialized WHERE should quote field with space: %q", serialized)
	}

	// Re-parse via expression path to ensure round-trip is parseable
	tokenizer := NewTokenizer(serialized)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Errorf("serialized WHERE should be tokenizable: %v", err)
	}
	if len(tokens) == 0 {
		t.Error("expected tokens from serialized WHERE")
	}
}

func containsQuoted(s, substr string) bool {
	quoted := "\"" + substr + "\""
	return strings.Contains(s, quoted)
}

// TestParseQualifiedFieldName verifies the multi-dot rule: one part → unqualified,
// two parts → (bundle, field), more than two → (first, rest joined with dots).
func TestParseQualifiedFieldName(t *testing.T) {
	adapter := NewSelectStatementAdapter(nil)

	tests := []struct {
		name          string
		qualifiedName string
		wantBundle    string
		wantField     string
	}{
		{"unqualified", "A", "", "A"},
		{"qualified two", "A.B", "A", "B"},
		{"qualified three", "A.B.C", "A", "B.C"},
		{"quoted unqualified", `"Field"`, "", "Field"},
		{"quoted two", `"Bundle"."Field"`, "Bundle", "Field"},
		{"quoted three", `"A"."B"."C"`, "A", "B.C"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBundle, gotField := adapter.parseQualifiedFieldName(tt.qualifiedName)
			if gotBundle != tt.wantBundle || gotField != tt.wantField {
				t.Errorf("parseQualifiedFieldName(%q) = (%q, %q), want (%q, %q)",
					tt.qualifiedName, gotBundle, gotField, tt.wantBundle, tt.wantField)
			}
		})
	}
}
