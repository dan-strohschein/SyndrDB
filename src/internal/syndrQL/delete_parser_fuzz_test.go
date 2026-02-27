package syndrQL

import (
	"testing"
)

func FuzzDeleteParser(f *testing.F) {
	// Seed corpus: DELETE DOCUMENTS statements
	seeds := []string{
		`DELETE DOCUMENTS FROM "Users" WHERE "id" == 1`,
		`DELETE DOCUMENTS FROM "Users" WHERE "age" < 18 AND "status" == "inactive"`,
		`DELETE DOCUMENTS FROM "Users" CONFIRMED`,
		`DELETE DOCUMENTS FROM "Logs" WHERE "created_at" < "2025-01-01"`,
		`DELETE DOCUMENTS FROM "Sessions" WHERE "expired" == true`,
		// Edge cases
		`DELETE`,
		`DELETE DOCUMENTS`,
		`DELETE DOCUMENTS FROM`,
		`DELETE DOCUMENTS FROM ""`,
		``,
		`DELETE DOCUMENTS FROM "Users"`,
		`DELETE DOCUMENTS FROM "Users" WHERE "a" > 1 OR "b" < 2`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		parser, err := NewDeleteParser(input)
		if err != nil {
			// Tokenization failed — fine
			return
		}

		stmt, err := parser.Parse()

		if err != nil {
			// Parse error — fine, just no panic
			return
		}

		// Property: successful parse returns non-nil statement
		if stmt == nil {
			t.Fatal("Parse returned nil statement without error")
		}
	})
}
