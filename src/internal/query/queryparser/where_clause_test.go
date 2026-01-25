package queryparser

import (
	"testing"
)

func TestWhereClauseWithMultipleParenGroups(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "Two paren groups with AND",
			input: `("category" == "Electronics") AND ("rating" <= 3)`,
		},
		{
			name:  "Three paren groups with AND",
			input: `("category" == "A") AND ("rating" <= 3) AND ("stock" >= 101 AND "stock" <= 200)`,
		},
		{
			name:  "Simple conditions without parens",
			input: `"category" == "Electronics" AND "rating" <= 3`,
		},
		{
			name:  "Three simple paren groups",
			input: `("field1" == 1) AND ("field2" == 2) AND ("field3" == 3)`,
		},
		{
			name:  "Mixed parens and no parens",
			input: `"x" == 1 AND ("y" == 2) AND "z" == 3`,
		},
		{
			name:  "Deeply nested",
			input: `("a" == 1 AND ("b" == 2 AND "c" == 3))`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			group, err := ParseWhereClause(tc.input)
			if err != nil {
				t.Errorf("ParseWhereClause(%q) failed: %v", tc.input, err)
				return
			}
			t.Logf("ParseWhereClause(%q) = %d clauses, %d subgroups", tc.input, len(group.Clauses), len(group.SubGroups))
		})
	}
}
