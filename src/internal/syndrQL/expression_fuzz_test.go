package syndrQL

import (
	"testing"
)

func FuzzExpressionParser(f *testing.F) {
	// Seed corpus: expression strings
	seeds := []string{
		// Simple comparisons
		`"age" > 21`,
		`"name" == "Alice"`,
		`"price" >= 9.99`,
		`"active" == true`,
		`"count" != 0`,
		// Boolean connectives
		`"age" > 18 AND "status" == "active"`,
		`"a" == 1 OR "b" == 2`,
		`"x" > 1 AND "y" > 2 OR "z" > 3`,
		`NOT "deleted"`,
		`NOT ("age" < 18)`,
		// Nested parentheses
		`("a" == 1)`,
		`(("a" == 1))`,
		`("a" == 1) AND ("b" == 2 OR "c" == 3)`,
		// NULL checks
		`"email" IS NULL`,
		`"email" IS NOT NULL`,
		// LIKE / CONTAINS
		`"name" LIKE "%john%"`,
		`"name" LIKE "test%"`,
		`"bio" CONTAINS "hello"`,
		// IN
		`"id" IN (1, 2, 3)`,
		`"status" IN ("active", "pending")`,
		`"id" NOT IN (1, 2)`,
		// Function calls
		`F:UPPER("name") == "ALICE"`,
		`F:LENGTH("name") > 5`,
		`F:NOW()`,
		`F:TRIM("  hello  ")`,
		// Arithmetic
		`"price" * "quantity"`,
		`"a" + "b" - "c"`,
		`"total" / 100.0`,
		// Subquery (EXISTS)
		`EXISTS (SELECT * FROM "Orders" WHERE "userId" == 1)`,
		// COALESCE
		`COALESCE("nickname", "name")`,
		// Edge cases
		``,
		`(`,
		`)`,
		`AND`,
		`OR`,
		`== ==`,
		`"field"`,
		`42`,
		`"a" == "b" == "c"`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		expr, err := ParseExpression(input)

		if err != nil {
			// Parse error — fine, just no panic
			return
		}

		// Property: successful parse returns non-nil expression
		if expr == nil {
			t.Fatal("ParseExpression returned nil expression without error")
		}

		// Property: String() doesn't panic
		_ = expr.String()
	})
}
