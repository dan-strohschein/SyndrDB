package syndrQL

import (
	"testing"
)

func FuzzInsertParser(f *testing.F) {
	// Seed corpus: ADD DOCUMENT statements
	seeds := []string{
		`ADD DOCUMENT TO BUNDLE "Users" WITH ({"name" = "Alice"})`,
		`ADD DOCUMENT TO BUNDLE "Users" WITH ({"name" = "Alice"}, {"age" = 30})`,
		`ADD DOCUMENT TO BUNDLE "Users" WITH ({"name" = "Bob"}, {"email" = "bob@test.com"}, {"active" = true})`,
		`ADD DOCUMENT TO BUNDLE "Products" WITH ({"price" = 19.99}, {"quantity" = 100})`,
		`ADD DOCUMENT TO BUNDLE "Logs" WITH ({"message" = "hello world"}, {"level" = "info"})`,
		// Edge cases
		`ADD DOCUMENT TO BUNDLE "" WITH ({"a" = 1})`,
		`ADD DOCUMENT TO BUNDLE "Users" WITH ()`,
		`ADD`,
		`ADD DOCUMENT`,
		`ADD DOCUMENT TO`,
		`ADD DOCUMENT TO BUNDLE`,
		``,
		`ADD DOCUMENT TO BUNDLE "Users"`,
		`BULK ADD DOCUMENTS TO BUNDLE "Users" WITH [({"a" = 1}), ({"a" = 2})]`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		parser, err := NewInsertParser(input)
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
