package syndrQL

import (
	"testing"
)

func FuzzUpdateParser(f *testing.F) {
	// Seed corpus: UPDATE DOCUMENTS statements
	seeds := []string{
		`UPDATE DOCUMENTS IN BUNDLE "Users" ("status" = "active") WHERE "id" == 1`,
		`UPDATE DOCUMENTS IN BUNDLE "Users" ("name" = "Bob", "age" = 25) WHERE "id" == 2`,
		`UPDATE DOCUMENTS IN BUNDLE "Users" ("deleted" = true) CONFIRMED`,
		`UPDATE DOCUMENTS IN BUNDLE "Products" ("price" = 29.99) WHERE "category" == "books"`,
		`UPDATE DOCUMENTS IN BUNDLE "Users" ("email" = NULL) WHERE "id" == 3`,
		`UPDATE DOCUMENT IN BUNDLE "Users" ("name" = "test") WHERE "id" == 1`,
		// Edge cases
		`UPDATE`,
		`UPDATE DOCUMENTS`,
		`UPDATE DOCUMENTS IN`,
		`UPDATE DOCUMENTS IN BUNDLE`,
		`UPDATE DOCUMENTS IN BUNDLE ""`,
		`UPDATE DOCUMENTS IN BUNDLE "Users"`,
		``,
		`UPDATE DOCUMENTS IN BUNDLE "Users" ("a" = 1) WHERE "x" > 0 AND "y" < 100`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		parser, err := NewUpdateParser(input)
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
