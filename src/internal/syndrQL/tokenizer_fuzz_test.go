package syndrQL

import (
	"testing"
)

func FuzzTokenizer(f *testing.F) {
	// Seed corpus: representative SyndrQL statements
	seeds := []string{
		// SELECT variants
		`SELECT * FROM "Users"`,
		`SELECT "name", "age" FROM "Users" WHERE "age" > 21`,
		`SELECT DISTINCT "city" FROM "Users"`,
		`SELECT COUNT(*) FROM "Users" GROUP BY "status" HAVING COUNT(*) > 5`,
		`SELECT "a"."id", "b"."name" FROM "Authors" INNER JOIN "Books" ON "Authors"."id" == "Books"."authorId"`,
		`SELECT * FROM "Users" WHERE "name" LIKE "%john%"`,
		`SELECT * FROM "Users" WHERE "id" IN (1, 2, 3)`,
		`SELECT * FROM "Users" WHERE "email" IS NULL`,
		`SELECT * FROM "Users" WHERE "email" IS NOT NULL`,
		`SELECT F:UPPER("name") FROM "Users"`,
		`SELECT * FROM "Users" ORDER BY "created_at" DESC LIMIT 10 OFFSET 5`,
		`SELECT * FROM "Users" FOR UPDATE`,
		// DML
		`ADD DOCUMENT TO BUNDLE "Users" WITH ({"name" = "Alice"}, {"age" = 30})`,
		`UPDATE DOCUMENTS IN BUNDLE "Users" ("status" = "active") WHERE "id" == 1`,
		`DELETE DOCUMENTS FROM "Users" WHERE "id" == 1`,
		// Transactions
		`BEGIN TRANSACTION`,
		`COMMIT`,
		`ROLLBACK`,
		`SAVEPOINT "sp1"`,
		// Cursors
		`DECLARE my_cursor CURSOR FOR SELECT * FROM "Users"`,
		`FETCH 10 FROM my_cursor`,
		`FETCH ALL FROM my_cursor`,
		`CLOSE my_cursor`,
		// Prepared statements
		`PREPARE my_stmt AS SELECT * FROM "Users" WHERE "id" == $1`,
		`EXECUTE my_stmt`,
		`DEALLOCATE my_stmt`,
		// Edge cases
		``,
		`   `,
		`SELECT`,
		`"quoted string"`,
		`123.456`,
		`!=`,
		`>=`,
		`<=`,
		`==`,
		`(((`,
		`)))`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		tokenizer := NewTokenizer(input)
		tokens, err := tokenizer.Tokenize()

		if err != nil {
			// Tokenization failed — that's fine, just no panic
			return
		}

		// Property: successful tokenization always ends with TOKEN_EOF
		if len(tokens) == 0 {
			t.Fatal("successful tokenization returned empty token slice")
		}
		lastToken := tokens[len(tokens)-1]
		if lastToken.Type != TOKEN_EOF {
			t.Fatalf("last token is %v, expected TOKEN_EOF", lastToken.Type)
		}

		// Property: no TOKEN_ILLEGAL in a successful result
		for i, tok := range tokens {
			if tok.Type == TOKEN_ILLEGAL {
				t.Fatalf("TOKEN_ILLEGAL at index %d in successful result: %q", i, tok.Value)
			}
		}
	})
}
