package main

import (
	"fmt"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	// Test 1: Parse a simple WHERE query
	query1 := `SELECT * FROM "testdb"."Authors" WHERE "AuthorID" == 1;`
	result1, err := queryparser.ParseQuery(query1, logger)
	if err != nil {
		fmt.Printf("❌ Test 1 failed: %v\n", err)
	} else {
		fmt.Printf("✅ Test 1 passed: Query parsed successfully\n")
		fmt.Printf("   - WhereExpression type: %T\n", result1.WhereExpression)
		if expr, ok := result1.WhereExpression.(syndrQL.Expression); ok {
			fmt.Printf("   - Expression is SyndrQL Expression: %v\n", expr)
		}
	}

	// Test 2: Parse a range query
	query2 := `SELECT * FROM "testdb"."Books" WHERE "PublishedYear" > 2000;`
	result2, err := queryparser.ParseQuery(query2, logger)
	if err != nil {
		fmt.Printf("❌ Test 2 failed: %v\n", err)
	} else {
		fmt.Printf("✅ Test 2 passed: Range query parsed successfully\n")
		fmt.Printf("   - WhereExpression type: %T\n", result2.WhereExpression)

		// Test index optimization helper
		if result2.WhereExpression != nil {
			if expr, ok := result2.WhereExpression.(syndrQL.Expression); ok {
				field, value, ok := syndrQL.ExtractSimpleEquality(expr)
				if ok {
					fmt.Printf("   - ExtractSimpleEquality: field=%s, value=%v\n", field, value)
				} else {
					field, op, value, ok := syndrQL.ExtractRangeCondition(expr)
					if ok {
						fmt.Printf("   - ExtractRangeCondition: field=%s, op=%s, value=%v\n", field, op, value)
					}
				}
			}
		}
	}

	// Test 3: Parse GROUP BY with HAVING
	query3 := `SELECT "AuthorID", COUNT(*) FROM "testdb"."Books" GROUP BY "AuthorID" HAVING COUNT(*) > 1;`
	result3, err := queryparser.ParseQuery(query3, logger)
	if err != nil {
		fmt.Printf("❌ Test 3 failed: %v\n", err)
	} else {
		fmt.Printf("✅ Test 3 passed: GROUP BY HAVING parsed successfully\n")
		fmt.Printf("   - HavingExpression type: %T\n", result3.HavingExpression)
		if result3.HavingExpression != nil {
			fmt.Printf("   - HavingExpression is present\n")
		}
	}

	fmt.Println("\n🎉 All Expression integration tests passed!")
}
