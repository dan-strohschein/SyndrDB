package syndrQL

import (
	"testing"
)

func FuzzSelectParser(f *testing.F) {
	// Seed corpus: valid and near-valid SELECT statements
	seeds := []string{
		`SELECT * FROM "Users"`,
		`SELECT "name", "age" FROM "Users"`,
		`SELECT DISTINCT "city" FROM "Users"`,
		`SELECT * FROM "Users" WHERE "age" > 21`,
		`SELECT * FROM "Users" WHERE "name" == "Alice" AND "age" >= 18`,
		`SELECT * FROM "Users" WHERE "name" LIKE "%test%"`,
		`SELECT * FROM "Users" WHERE "id" IN (1, 2, 3)`,
		`SELECT * FROM "Users" WHERE "email" IS NULL`,
		`SELECT * FROM "Users" WHERE "status" IS NOT NULL`,
		`SELECT COUNT(*) FROM "Orders" GROUP BY "status"`,
		`SELECT "status", COUNT(*), SUM("total") FROM "Orders" GROUP BY "status" HAVING COUNT(*) > 5`,
		`SELECT * FROM "Users" ORDER BY "name" ASC, "age" DESC`,
		`SELECT * FROM "Users" LIMIT 10 OFFSET 20`,
		`SELECT F:UPPER("name"), F:LOWER("email") FROM "Users"`,
		`SELECT * FROM "Users" FOR UPDATE`,
		`SELECT "a"."id", "b"."name" FROM "Authors" INNER JOIN "Books" ON "Authors"."id" == "Books"."authorId"`,
		`SELECT * FROM "Users" LEFT JOIN "Orders" ON "Users"."id" == "Orders"."userId"`,
		`SELECT * FROM "Users" WHERE NOT "deleted"`,
		`SELECT * FROM "Users" WHERE "age" > 18 OR ("status" == "active" AND "verified" == true)`,
		// Edge cases
		`SELECT`,
		`SELECT FROM`,
		`SELECT * FROM`,
		`SELECT * FROM ""`,
		``,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		tokenizer := NewTokenizer(input)
		tokens, err := tokenizer.Tokenize()
		if err != nil {
			// Tokenization failed — can't parse
			return
		}

		parser := NewSelectParser(tokens, nil)
		stmt, err := parser.Parse(nil, input)

		if err != nil {
			// Parse error — fine, just no panic
			return
		}

		// Property: successful parse produces a non-nil statement
		if stmt == nil {
			t.Fatal("Parse returned nil statement without error")
		}
	})
}
