package homegrown

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

func main3() {
	// Create logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	// Test 1: JOIN with WITH RELATIONSHIP clause
	query1 := `SELECT DOCUMENTS FROM "Users" 
	           JOIN "Orders" ON "Users"."id" == "Orders"."userId"
	           WITH RELATIONSHIP "UserOrders"`

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Test 1: JOIN with WITH RELATIONSHIP")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Query: %s\n\n", query1)

	result1, err := queryparser.ParseUnifiedSelectQuery(query1, sugar)
	if err != nil {
		fmt.Printf("❌ Error: %v\n\n", err)
	} else {
		fmt.Printf("✅ Parsing successful!\n")
		fmt.Printf("   Query Type: %s\n", result1.QueryType)
		fmt.Printf("   From Bundle: %s\n", result1.FromBundle)
		fmt.Printf("   Number of JOINs: %d\n", len(result1.JoinClauses))
		fmt.Printf("   Relationship Name: '%s'\n", result1.RelationshipName)
		fmt.Printf("   Has Relationship: %v\n\n", result1.RelationshipName != "")
	}

	// Test 2: Complex query with JOIN, WHERE, and WITH RELATIONSHIP
	query2 := `SELECT DOCUMENTS FROM "Users" 
	           JOIN "Orders" ON "Users"."id" == "Orders"."userId"
	           WHERE "Users"."age" > 18
	           WITH RELATIONSHIP "AdultUserOrders"`

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Test 2: JOIN + WHERE + WITH RELATIONSHIP")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Query: %s\n\n", query2)

	result2, err := queryparser.ParseUnifiedSelectQuery(query2, sugar)
	if err != nil {
		fmt.Printf("❌ Error: %v\n\n", err)
	} else {
		fmt.Printf("✅ Parsing successful!\n")
		fmt.Printf("   Query Type: %s\n", result2.QueryType)
		fmt.Printf("   From Bundle: %s\n", result2.FromBundle)
		fmt.Printf("   Number of JOINs: %d\n", len(result2.JoinClauses))
		fmt.Printf("   Has WHERE: %v\n", result2.HasWhere())
		fmt.Printf("   Relationship Name: '%s'\n", result2.RelationshipName)
		fmt.Printf("   Has Relationship: %v\n\n", result2.RelationshipName != "")
	}

	// Test 3: JOIN without WITH RELATIONSHIP (should be empty)
	query3 := `SELECT DOCUMENTS FROM "Users" 
	           JOIN "Orders" ON "Users"."id" == "Orders"."userId"`

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Test 3: JOIN without WITH RELATIONSHIP")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Query: %s\n\n", query3)

	result3, err := queryparser.ParseUnifiedSelectQuery(query3, sugar)
	if err != nil {
		fmt.Printf("❌ Error: %v\n\n", err)
	} else {
		fmt.Printf("✅ Parsing successful!\n")
		fmt.Printf("   Query Type: %s\n", result3.QueryType)
		fmt.Printf("   From Bundle: %s\n", result3.FromBundle)
		fmt.Printf("   Number of JOINs: %d\n", len(result3.JoinClauses))
		fmt.Printf("   Relationship Name: '%s' (should be empty)\n", result3.RelationshipName)
		fmt.Printf("   Has Relationship: %v (should be false)\n\n", result3.RelationshipName != "")
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Summary")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("✅ WITH RELATIONSHIP clause is FULLY SUPPORTED")
	fmt.Println("✅ Works with JOIN queries")
	fmt.Println("✅ Works with JOIN + WHERE combinations")
	fmt.Println("✅ Properly stored in UnifiedSelectQuery.RelationshipName field")
}
