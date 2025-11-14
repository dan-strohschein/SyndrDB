/*
GROUP BY TEST SUITE

This file provides comprehensive tests for the GROUP BY functionality in SyndrDB.
It tests parsing, execution strategies, aggregate functions, and integration.

TEST CATEGORIES:
1. Parser Tests - Validates GROUP BY query parsing
2. Execution Tests - Tests Hash and Sort strategies
3. Aggregate Function Tests - COUNT, SUM, AVG, MIN, MAX
4. Integration Tests - End-to-end SELECT with GROUP BY
5. Performance Tests - Strategy selection and optimization

SAMPLE QUERIES TESTED:
- SELECT category, COUNT(*) FROM "Sales" GROUP BY category
- SELECT region, SUM(amount), AVG(amount) FROM "Sales" GROUP BY region
- SELECT dept, MIN(salary), MAX(salary) FROM "Employees" GROUP BY dept HAVING COUNT(*) > 5
- SELECT status, COUNT(*) FROM "Orders" GROUP BY status ORDER BY COUNT(*) DESC
*/

package homegrown

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/executor"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

func TestGroupByFunctionality() {
	args := settings.GetSettings()
	args.LogLevel = "warn"

	// Setup logger
	config := zap.NewDevelopmentConfig()

	// Configure encoder to show only the message
	config.EncoderConfig.TimeKey = ""       // Remove timestamp
	config.EncoderConfig.LevelKey = ""      // Remove log level
	config.EncoderConfig.CallerKey = ""     // Remove source file info
	config.EncoderConfig.NameKey = ""       // Remove logger name
	config.EncoderConfig.FunctionKey = ""   // Remove function name
	config.EncoderConfig.StacktraceKey = "" // Remove stack trace
	config.EncoderConfig.MessageKey = "msg" // Keep only the message

	config.Level, _ = zap.ParseAtomicLevel("Warn") // Set default log level to warn
	logger, err := config.Build()
	if err != nil {
		fmt.Printf("Error setting up logger: %v\n", err)
		return
	}
	sugaredLogger := logger.Sugar()
	//logger, _ := zap.NewDevelopment()
	//sugaredLogger := logger.Sugar()

	fmt.Println("=== SyndrDB GROUP BY Test Suite ===")
	fmt.Println()

	// Test 1: Parser Tests
	fmt.Println("1. TESTING GROUP BY PARSER")
	testGroupByParser(sugaredLogger)
	fmt.Println()

	// Test 2: Execution Strategy Tests
	fmt.Println("2. TESTING EXECUTION STRATEGIES")
	testExecutionStrategies(sugaredLogger)
	fmt.Println()

	// Test 3: Aggregate Function Tests
	fmt.Println("3. TESTING AGGREGATE FUNCTIONS")
	testAggregateFunctions(sugaredLogger)
	fmt.Println()

	// Test 4: Complex Query Tests
	fmt.Println("4. TESTING COMPLEX QUERIES")
	testComplexQueries(sugaredLogger)
	fmt.Println()

	// Test 5: Aggregate Value Verification
	fmt.Println("5. TESTING AGGREGATE VALUE CALCULATIONS")
	testAggregateValueCalculations(sugaredLogger)
	fmt.Println()

	fmt.Println("=== GROUP BY Test Suite Complete ===")
}

// testGroupByParser tests the GROUP BY query parser
func testGroupByParser(logger *zap.SugaredLogger) {
	testCases := []struct {
		name  string
		query string
		valid bool
	}{
		{
			name:  "Basic COUNT(*)",
			query: `SELECT category, COUNT(*) FROM "Sales" GROUP BY category`,
			valid: true,
		},
		{
			name:  "Multiple Aggregates",
			query: `SELECT region, SUM(amount), AVG(amount), COUNT(*) FROM "Sales" GROUP BY region`,
			valid: true,
		},
		{
			name:  "With HAVING Clause",
			query: `SELECT dept, COUNT(*) FROM "Employees" GROUP BY dept HAVING COUNT(*) > 5`,
			valid: true,
		},
		{
			name:  "With ORDER BY",
			query: `SELECT status, COUNT(*) FROM "Orders" GROUP BY status ORDER BY COUNT(*) DESC`,
			valid: true,
		},
		{
			name:  "Aggregate Aliases",
			query: `SELECT category, COUNT(*) as total_count, AVG(price) as avg_price FROM "Products" GROUP BY category`,
			valid: true,
		},
		{
			name:  "Multiple GROUP BY Fields",
			query: `SELECT region, category, SUM(sales) FROM "Revenue" GROUP BY region, category`,
			valid: true,
		},
		{
			name:  "Invalid: Missing GROUP BY",
			query: `SELECT category, COUNT(*) FROM "Sales"`,
			valid: false,
		},
		{
			name:  "Invalid: Non-grouped field in SELECT",
			query: `SELECT category, name, COUNT(*) FROM "Sales" GROUP BY category`,
			valid: false,
		},
	}

	for _, tc := range testCases {
		fmt.Printf("  Testing: %s\n", tc.name)
		fmt.Printf("  Query: %s\n", tc.query)

		result, err := queryparser.ParseSelectQueryWithGroupBy(tc.query, logger)

		if tc.valid {
			if err != nil {
				fmt.Printf("  ❌ Expected valid query but got error: %v\n", err)
			} else {
				fmt.Printf("  ✅ Successfully parsed\n")
				fmt.Printf("     FROM: %s\n", result.FromBundle)
				fmt.Printf("     GROUP BY: %v\n", result.GroupBy.Fields)
				fmt.Printf("     AGGREGATES: %d\n", len(result.AggregateFields))
				fmt.Printf("     STRATEGY: %s\n", result.ExecutionStrategy.String())
			}
		} else {
			if err == nil {
				fmt.Printf("  ❌ Expected error but query parsed successfully\n")
			} else {
				fmt.Printf("  ✅ Correctly rejected: %v\n", err)
			}
		}
		fmt.Println()
	}
}

// testExecutionStrategies tests Hash vs Sort strategies
func testExecutionStrategies(logger *zap.SugaredLogger) {
	// Create test documents
	testDocs := createGroupByTestDocuments()

	fmt.Printf("  Created %d test documents\n", len(testDocs))

	// Test Hash Aggregate Strategy
	fmt.Println("  Testing Hash Aggregate Strategy:")
	query1 := `SELECT category, COUNT(*) FROM "TestBundle" GROUP BY category`
	groupByQuery1, err := queryparser.ParseSelectQueryWithGroupBy(query1, logger)
	if err != nil {
		fmt.Printf("  ❌ Parser error: %v\n", err)
		return
	}

	// Force Hash strategy
	groupByQuery1.ExecutionStrategy = queryparser.HashAggregate
	executor1 := executor.NewGroupByExecutor(groupByQuery1, logger)
	result1, err := executor1.Execute(testDocs)
	if err != nil {
		fmt.Printf("  ❌ Hash execution error: %v\n", err)
	} else {
		fmt.Printf("  ✅ Hash strategy returned %d groups\n", len(result1))
	}

	// Test Sort + GroupAggregate Strategy
	fmt.Println("  Testing Sort + GroupAggregate Strategy:")
	groupByQuery2, _ := queryparser.ParseSelectQueryWithGroupBy(query1, logger)
	groupByQuery2.ExecutionStrategy = queryparser.SortGroupAggregate
	executor2 := executor.NewGroupByExecutor(groupByQuery2, logger)
	result2, err := executor2.Execute(testDocs)
	if err != nil {
		fmt.Printf("  ❌ Sort execution error: %v\n", err)
	} else {
		fmt.Printf("  ✅ Sort strategy returned %d groups\n", len(result2))
	}

	// Compare results
	if len(result1) == len(result2) {
		fmt.Printf("  ✅ Both strategies returned same number of groups\n")
	} else {
		fmt.Printf("  ❌ Strategies returned different results: Hash=%d, Sort=%d\n", len(result1), len(result2))
	}
}

// testAggregateFunctions tests all aggregate functions
func testAggregateFunctions(logger *zap.SugaredLogger) {
	testDocs := createGroupByTestDocuments()

	aggregateTests := []struct {
		name  string
		query string
	}{
		{
			name:  "COUNT(*) - Count all rows",
			query: `SELECT category, COUNT(*) FROM "TestBundle" GROUP BY category`,
		},
		{
			name:  "COUNT(field) - Count non-null values",
			query: `SELECT category, COUNT(price) FROM "TestBundle" GROUP BY category`,
		},
		{
			name:  "SUM - Sum numeric values",
			query: `SELECT category, SUM(price) FROM "TestBundle" GROUP BY category`,
		},
		{
			name:  "AVG - Average values",
			query: `SELECT category, AVG(price) FROM "TestBundle" GROUP BY category`,
		},
		{
			name:  "MIN - Minimum values",
			query: `SELECT category, MIN(price) FROM "TestBundle" GROUP BY category`,
		},
		{
			name:  "MAX - Maximum values",
			query: `SELECT category, MAX(price) FROM "TestBundle" GROUP BY category`,
		},
		{
			name:  "Multiple Aggregates",
			query: `SELECT category, COUNT(*), SUM(price), AVG(price), MIN(price), MAX(price) FROM "TestBundle" GROUP BY category`,
		},
	}

	for _, test := range aggregateTests {
		fmt.Printf("  Testing: %s\n", test.name)

		groupByQuery, err := queryparser.ParseSelectQueryWithGroupBy(test.query, logger)
		if err != nil {
			fmt.Printf("  ❌ Parser error: %v\n", err)
			continue
		}

		executorInstance := executor.NewGroupByExecutor(groupByQuery, logger)
		result, err := executorInstance.Execute(testDocs)
		if err != nil {
			fmt.Printf("  ❌ Execution error: %v\n", err)
		} else {
			fmt.Printf("  ✅ Successfully executed, %d groups\n", len(result))
		}
		fmt.Println()
	}
}

// testComplexQueries tests complex GROUP BY scenarios
func testComplexQueries(logger *zap.SugaredLogger) {
	complexQueries := []struct {
		name  string
		query string
	}{
		{
			name:  "GROUP BY with HAVING",
			query: `SELECT category, COUNT(*) FROM "TestBundle" GROUP BY category HAVING COUNT(*) > 1`,
		},
		{
			name:  "GROUP BY with ORDER BY",
			query: `SELECT category, COUNT(*) FROM "TestBundle" GROUP BY category ORDER BY COUNT(*) DESC`,
		},
		{
			name:  "Multiple GROUP BY fields",
			query: `SELECT category, status, COUNT(*) FROM "TestBundle" GROUP BY category, status`,
		},
		{
			name:  "Complex aggregate expressions",
			query: `SELECT category, COUNT(*) as total, SUM(price) as revenue, AVG(price) as avg_price FROM "TestBundle" GROUP BY category`,
		},
	}

	for _, test := range complexQueries {
		fmt.Printf("  Testing: %s\n", test.name)
		fmt.Printf("  Query: %s\n", test.query)

		groupByQuery, err := queryparser.ParseSelectQueryWithGroupBy(test.query, logger)
		if err != nil {
			fmt.Printf("  ❌ Parser error: %v\n", err)
		} else {
			fmt.Printf("  ✅ Successfully parsed complex query\n")
			fmt.Printf("     GROUP BY: %v\n", groupByQuery.GroupBy.Fields)
			fmt.Printf("     AGGREGATES: %d\n", len(groupByQuery.AggregateFields))
			if groupByQuery.HavingClause != nil {
				fmt.Printf("     HAVING: %s\n", groupByQuery.HavingClause.Condition)
			}
			if groupByQuery.OrderBy != nil {
				fmt.Printf("     ORDER BY: %v\n", groupByQuery.OrderBy.Fields)
			}
		}
		fmt.Println()
	}
}

// createGroupByTestDocuments creates sample documents for testing GROUP BY functionality
func createGroupByTestDocuments() map[string]*models.Document {
	docs := make(map[string]*models.Document)

	// Sample sales data
	testData := []struct {
		id       string
		category string
		price    float64
		status   string
		region   string
	}{
		{"1", "Electronics", 299.99, "active", "North"},
		{"2", "Electronics", 599.99, "active", "South"},
		{"3", "Electronics", 199.99, "inactive", "North"},
		{"4", "Books", 29.99, "active", "North"},
		{"5", "Books", 39.99, "active", "South"},
		{"6", "Books", 19.99, "active", "East"},
		{"7", "Clothing", 89.99, "active", "North"},
		{"8", "Clothing", 129.99, "inactive", "South"},
		{"9", "Electronics", 999.99, "active", "East"},
		{"10", "Books", 49.99, "active", "West"},
	}

	for _, data := range testData {
		fields := map[string]models.Field{
			"category": {Name: "category", Value: data.category},
			"price":    {Name: "price", Value: data.price},
			"status":   {Name: "status", Value: data.status},
			"region":   {Name: "region", Value: data.region},
		}

		docs[data.id] = &models.Document{
			DocumentID: data.id,
			Fields:     fields,
		}
	}

	return docs
}

// testAggregateValueCalculations tests that aggregate functions calculate correct values
func testAggregateValueCalculations(logger *zap.SugaredLogger) {
	// Create test documents with known values for verification
	testDocs := createKnownValueDocuments()

	fmt.Println("  Test Data:")
	fmt.Println("    Electronics: $100, $200, $300 (3 items)")
	fmt.Println("    Books: $10, $20 (2 items)")
	fmt.Println("    Clothing: $50 (1 item)")
	fmt.Println()

	// Test COUNT(*)
	fmt.Println("  Testing COUNT(*) calculations:")
	countQuery := `SELECT category, COUNT(*) FROM "TestBundle" GROUP BY category`
	countResult := executeAndDisplayResults(countQuery, testDocs, logger)

	// Verify COUNT results
	expectedCounts := map[string]int64{"Electronics": 3, "Books": 2, "Clothing": 1}
	verifyCountResults(countResult, expectedCounts)

	// Test SUM
	fmt.Println("  Testing SUM(price) calculations:")
	sumQuery := `SELECT category, SUM(price) FROM "TestBundle" GROUP BY category`
	sumResult := executeAndDisplayResults(sumQuery, testDocs, logger)

	// Verify SUM results
	expectedSums := map[string]float64{"Electronics": 600.0, "Books": 30.0, "Clothing": 50.0}
	verifyFloatResults(sumResult, expectedSums, "SUM")

	// Test AVG
	fmt.Println("  Testing AVG(price) calculations:")
	avgQuery := `SELECT category, AVG(price) FROM "TestBundle" GROUP BY category`
	avgResult := executeAndDisplayResults(avgQuery, testDocs, logger)

	// Verify AVG results
	expectedAvgs := map[string]float64{"Electronics": 200.0, "Books": 15.0, "Clothing": 50.0}
	verifyFloatResults(avgResult, expectedAvgs, "AVG")

	// Test MIN
	fmt.Println("  Testing MIN(price) calculations:")
	minQuery := `SELECT category, MIN(price) FROM "TestBundle" GROUP BY category`
	minResult := executeAndDisplayResults(minQuery, testDocs, logger)

	// Verify MIN results
	expectedMins := map[string]float64{"Electronics": 100.0, "Books": 10.0, "Clothing": 50.0}
	verifyFloatResults(minResult, expectedMins, "MIN")

	// Test MAX
	fmt.Println("  Testing MAX(price) calculations:")
	maxQuery := `SELECT category, MAX(price) FROM "TestBundle" GROUP BY category`
	maxResult := executeAndDisplayResults(maxQuery, testDocs, logger)

	// Verify MAX results
	expectedMaxs := map[string]float64{"Electronics": 300.0, "Books": 20.0, "Clothing": 50.0}
	verifyFloatResults(maxResult, expectedMaxs, "MAX")
}

// executeAndDisplayResults executes a query and displays the results
func executeAndDisplayResults(query string, testDocs map[string]*models.Document, logger *zap.SugaredLogger) map[string]*models.Document {
	groupByQuery, err := queryparser.ParseSelectQueryWithGroupBy(query, logger)
	if err != nil {
		fmt.Printf("    ❌ Parser error: %v\n", err)
		return nil
	}

	executorInstance := executor.NewGroupByExecutor(groupByQuery, logger)
	results, err := executorInstance.Execute(testDocs)
	if err != nil {
		fmt.Printf("    ❌ Execution error: %v\n", err)
		return nil
	}

	fmt.Printf("    Results: ")
	for _, doc := range results {
		var category string
		var aggregateValue interface{}

		// Extract category
		if catField, exists := doc.Fields["category"]; exists {
			category = catField.Value.(string)
		}

		// Extract aggregate value (find the first non-category field)
		for fieldName, field := range doc.Fields {
			if fieldName != "category" {
				aggregateValue = field.Value
				break
			}
		}
		fmt.Printf("%s=%v ", category, aggregateValue)
	}
	fmt.Println()

	return results
}

// verifyCountResults verifies COUNT results
func verifyCountResults(results map[string]*models.Document, expected map[string]int64) {
	if results == nil {
		fmt.Println("    ❌ No results to verify")
		return
	}

	success := true
	for _, doc := range results {
		var category string
		var count int64

		if catField, exists := doc.Fields["category"]; exists {
			category = catField.Value.(string)
		}

		for fieldName, field := range doc.Fields {
			if fieldName != "category" {
				count = field.Value.(int64)
				break
			}
		}

		if expectedCount, exists := expected[category]; exists {
			if count != expectedCount {
				fmt.Printf("    ❌ %s: Expected %d, got %d\n", category, expectedCount, count)
				success = false
			}
		}
	}

	if success {
		fmt.Println("    ✅ All COUNT values correct!")
	}
}

// verifyFloatResults verifies float-based results (SUM, AVG, MIN, MAX)
func verifyFloatResults(results map[string]*models.Document, expected map[string]float64, operation string) {
	if results == nil {
		fmt.Printf("    ❌ No results to verify for %s\n", operation)
		return
	}

	success := true
	for _, doc := range results {
		var category string
		var value float64

		if catField, exists := doc.Fields["category"]; exists {
			category = catField.Value.(string)
		}

		for fieldName, field := range doc.Fields {
			if fieldName != "category" {
				value = field.Value.(float64)
				break
			}
		}

		if expectedValue, exists := expected[category]; exists {
			// Use a small epsilon for float comparison
			epsilon := 0.01
			if value < expectedValue-epsilon || value > expectedValue+epsilon {
				fmt.Printf("    ❌ %s: Expected %.2f, got %.2f\n", category, expectedValue, value)
				success = false
			}
		}
	}

	if success {
		fmt.Printf("    ✅ All %s values correct!\n", operation)
	}
}

// createKnownValueDocuments creates test documents with known values for verification
func createKnownValueDocuments() map[string]*models.Document {
	docs := make(map[string]*models.Document)

	// Test data with known values for easy verification
	testData := []struct {
		id       string
		category string
		price    float64
	}{
		// Electronics: 3 items, prices: 100, 200, 300
		// Total: 600, Average: 200, Min: 100, Max: 300
		{"1", "Electronics", 100.0},
		{"2", "Electronics", 200.0},
		{"3", "Electronics", 300.0},

		// Books: 2 items, prices: 10, 20
		// Total: 30, Average: 15, Min: 10, Max: 20
		{"4", "Books", 10.0},
		{"5", "Books", 20.0},

		// Clothing: 1 item, price: 50
		// Total: 50, Average: 50, Min: 50, Max: 50
		{"6", "Clothing", 50.0},
	}

	for _, data := range testData {
		fields := map[string]models.Field{
			"category": {Name: "category", Value: data.category},
			"price":    {Name: "price", Value: data.price},
		}

		docs[data.id] = &models.Document{
			DocumentID: data.id,
			Fields:     fields,
		}
	}

	return docs
}
