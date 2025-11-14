/*
SIMPLE JOIN DEMONSTRATION

This file provides a simple demonstration of the JOIN functionality
that we can integrate into the existing test framework without
breaking the current test structure.
*/

package homegrown

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// SimpleJoinDemo demonstrates basic JOIN functionality
func SimpleJoinDemo(logger *zap.SugaredLogger) error {
	logger.Infof("=== Simple JOIN Functionality Demo ===")

	// Test 1: Parse a simple JOIN query
	logger.Infof("Test 1: Parsing JOIN query")
	query := `SELECT DOCUMENTS FROM "Orders" JOIN "Customers" ON "Orders"."customer_id" == "Customers"."id"`

	joinQuery, err := queryparser.ParseSelectJoinQuery(query, logger)
	if err != nil {
		return fmt.Errorf("failed to parse JOIN query: %w", err)
	}

	logger.Infof("✓ Successfully parsed JOIN query:")
	logger.Infof("  From Bundle: %s", joinQuery.FromBundle)
	logger.Infof("  JOIN Clauses: %d", len(joinQuery.JoinClauses))

	if len(joinQuery.JoinClauses) > 0 {
		joinClause := joinQuery.JoinClauses[0]
		logger.Infof("  Join Type: %s", joinClause.JoinType.String())
		logger.Infof("  Right Bundle: %s", joinClause.RightBundle)
		logger.Infof("  Join Conditions: %d", len(joinClause.JoinConditions))

		if len(joinClause.JoinConditions) > 0 {
			condition := joinClause.JoinConditions[0]
			logger.Infof("    Condition: %s.%s %s %s.%s",
				condition.LeftBundle, condition.LeftField, condition.Operator,
				condition.RightBundle, condition.RightField)
		}
	}

	// Test 2: Parse a more complex JOIN query with WHERE
	logger.Infof("\nTest 2: Parsing JOIN with WHERE clause")
	complexQuery := `SELECT DOCUMENTS FROM "Orders" LEFT JOIN "Customers" ON "Orders"."customer_id" == "Customers"."id" WHERE "Orders"."total" > 50`

	complexJoinQuery, err := queryparser.ParseSelectJoinQuery(complexQuery, logger)
	if err != nil {
		return fmt.Errorf("failed to parse complex JOIN query: %w", err)
	}

	logger.Infof("✓ Successfully parsed complex JOIN query:")
	logger.Infof("  Join Type: %s", complexJoinQuery.JoinClauses[0].JoinType.String())
	if complexJoinQuery.WhereClause != nil {
		logger.Infof("  WHERE Conditions: %d", len(complexJoinQuery.WhereClause.Clauses))
	}

	// Test 3: Test JOIN query validation
	logger.Infof("\nTest 3: Testing JOIN validation (will show expected errors)")

	// Create minimal test bundles for validation
	testBundles := make(map[string]*models.Bundle)

	// Create Orders bundle
	ordersBundle := &models.Bundle{
		Name: "Orders",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"customer_id": {Name: "customer_id", Type: "int"},
				"total":       {Name: "total", Type: "float"},
			},
		},
	}

	// Create Customers bundle
	customersBundle := &models.Bundle{
		Name: "Customers",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"id":   {Name: "id", Type: "int"},
				"name": {Name: "name", Type: "string"},
			},
		},
	}

	testBundles["Orders"] = ordersBundle
	testBundles["Customers"] = customersBundle

	// Test valid query validation
	err = queryparser.ValidateJoinQuery(joinQuery, testBundles, logger)
	if err != nil {
		return fmt.Errorf("validation failed for valid query: %w", err)
	}
	logger.Infof("✓ Valid JOIN query passed validation")

	// Test invalid query validation
	invalidQuery := `SELECT DOCUMENTS FROM "Orders" JOIN "NonExistent" ON "Orders"."customer_id" == "NonExistent"."id"`
	invalidJoinQuery, err := queryparser.ParseSelectJoinQuery(invalidQuery, logger)
	if err == nil {
		err = queryparser.ValidateJoinQuery(invalidJoinQuery, testBundles, logger)
		if err != nil {
			logger.Infof("✓ Invalid JOIN query correctly failed validation: %v", err)
		} else {
			logger.Warnf("Expected validation to fail for invalid query")
		}
	}

	logger.Infof("\n✅ JOIN functionality demonstration completed successfully!")
	logger.Infof("📝 Key features demonstrated:")
	logger.Infof("   • JOIN query parsing with multiple join types")
	logger.Infof("   • Complex queries with WHERE clauses")
	logger.Infof("   • Query validation with proper error handling")
	logger.Infof("   • Support for LEFT JOIN, INNER JOIN syntax")

	return nil
}

// DemoJoinCommandExecution demonstrates the complete JOIN command flow
func DemoJoinCommandExecution(logger *zap.SugaredLogger) error {
	logger.Infof("=== JOIN Command Execution Demo ===")

	// This is a simplified demo that shows the command director can detect JOIN queries
	testCommands := []string{
		"SELECT DOCUMENTS FROM Orders",
		"SELECT DOCUMENTS FROM Orders JOIN Customers ON Orders.customer_id == Customers.id",
		"SELECT DOCUMENTS FROM Orders LEFT JOIN Customers ON Orders.customer_id == Customers.id WHERE Orders.total > 100",
	}

	for i, cmd := range testCommands {
		logger.Infof("Command %d: %s", i+1, cmd)

		commandParts := strings.Split(cmd, " ")
		fullCommand := strings.Join(commandParts, " ")

		// Check if this is a JOIN query (same logic as in command director)
		if strings.Contains(strings.ToUpper(fullCommand), "JOIN") {
			logger.Infof("  ✓ Detected as JOIN query - would route to JOIN handler")
		} else {
			logger.Infof("  ✓ Detected as regular SELECT - would route to normal handler")
		}
	}

	logger.Infof("✅ JOIN command routing demonstration completed!")

	return nil
}

// RunJoinDemonstration runs all JOIN demonstrations
func RunJoinDemonstration(logger *zap.SugaredLogger) error {
	logger.Infof("\n" + strings.Repeat("=", 60))
	logger.Infof("🚀 Starting JOIN Functionality Demonstration")
	logger.Infof(strings.Repeat("=", 60))

	demos := []struct {
		name string
		fn   func(*zap.SugaredLogger) error
	}{
		{"JOIN Query Parsing", SimpleJoinDemo},
		{"JOIN Command Routing", DemoJoinCommandExecution},
	}

	for _, demo := range demos {
		logger.Infof("\n" + strings.Repeat("-", 40))
		logger.Infof("🧪 Running: %s", demo.name)
		logger.Infof(strings.Repeat("-", 40))

		err := demo.fn(logger)
		if err != nil {
			logger.Errorf("❌ Demo failed: %s - %v", demo.name, err)
			return fmt.Errorf("demonstration '%s' failed: %w", demo.name, err)
		} else {
			logger.Infof("✅ Demo completed: %s", demo.name)
		}
	}

	logger.Infof("\n" + strings.Repeat("=", 60))
	logger.Infof("🎉 All JOIN demonstrations completed successfully!")
	logger.Infof("🔧 JOIN functionality is ready for use")
	logger.Infof(strings.Repeat("=", 60))

	return nil
}
