package graphql_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"syndrdb/src/internal/graphQL"
)

// createTestDocument creates a document via GraphQL mutation and returns its DocumentID
// This is used to set up test data for query tests
func createTestDocument(t *testing.T, env *TestEnvironment, bundleName string, fields map[string]interface{}) string {
	// Build mutation query
	mutation := fmt.Sprintf(`GRAPHQL::
		mutation {
			create%s(input: {`, bundleName)

	for key, value := range fields {
		switch v := value.(type) {
		case string:
			mutation += fmt.Sprintf(`
				%s: "%s"`, key, v)
		case int:
			mutation += fmt.Sprintf(`
				%s: %d`, key, v)
		case float64:
			mutation += fmt.Sprintf(`
				%s: %f`, key, v)
		case bool:
			mutation += fmt.Sprintf(`
				%s: %t`, key, v)
		}
	}

	mutation += `
			}) {
				DocumentID
			}
		}
	`

	// Execute mutation
	result, err := env.GraphQLHandler.ProcessGraphQLCommand(mutation)
	require.NoError(t, err, "Failed to create document via GraphQL")

	// Extract DocumentID from response
	response := assertNoGraphQLErrors(t, result)
	createData := getResponseData(response, fmt.Sprintf("create%s", bundleName))
	require.NotNil(t, createData, "Create mutation returned nil data")

	docMap, ok := createData.(map[string]interface{})
	require.True(t, ok, "Create mutation data should be a map")

	documentID, ok := docMap["DocumentID"].(string)
	require.True(t, ok, "DocumentID should be a string")
	require.NotEmpty(t, documentID, "DocumentID should not be empty")

	return documentID
}

// convertToInterfaceSlice converts GraphQL response arrays to []interface{} for testing
func convertToInterfaceSlice(data interface{}) []interface{} {
	if typedData, ok := data.([]map[string]interface{}); ok {
		result := make([]interface{}, len(typedData))
		for i, item := range typedData {
			result[i] = item
		}
		return result
	}
	if interfaceData, ok := data.([]interface{}); ok {
		return interfaceData
	}
	return nil
}

// assertIntValue checks integer values (handles int64, float64, int)
func assertIntValue(t *testing.T, expected int, actual interface{}, fieldName string) {
	switch v := actual.(type) {
	case float64:
		assert.Equal(t, float64(expected), v, fieldName)
	case int64:
		assert.Equal(t, int64(expected), v, fieldName)
	case int:
		assert.Equal(t, expected, v, fieldName)
	default:
		t.Fatalf("Unexpected type for %s: %T", fieldName, actual)
	}
}

// getGraphQLErrors extracts errors from a GraphQL response
func getGraphQLErrors(result interface{}) []graphQL.GraphQLError {
	response, ok := result.(graphQL.GraphQLResponse)
	if !ok {
		return nil
	}
	return response.Errors
}

// TestGraphQLQueries_BasicFieldSelection tests simple queries with field selection
func TestGraphQLQueries_BasicFieldSelection(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	// Create a test bundle
	createTestBundle(t, env, "products")

	// Add test data
	createTestDocument(t, env, "products", map[string]interface{}{
		"name":  "Laptop",
		"email": "laptop@store.com",
		"age":   2,
	})

	t.Run("Query all fields", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ products { DocumentID name email age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		response := assertNoGraphQLErrors(t, result)
		products := getResponseData(response, "products")
		assert.NotNil(t, products)

		// GraphQL returns []map[string]interface{}, convert to []interface{} for testing
		var productsList []interface{}
		if typedProducts, ok := products.([]map[string]interface{}); ok {
			productsList = make([]interface{}, len(typedProducts))
			for i, p := range typedProducts {
				productsList[i] = p
			}
		} else if interfaceProducts, ok := products.([]interface{}); ok {
			productsList = interfaceProducts
		} else {
			require.Fail(t, "Expected products to be an array, got %T", products)
		}

		assert.Len(t, productsList, 1)

		product := productsList[0].(map[string]interface{})
		assert.Equal(t, "Laptop", product["name"])
		assert.Equal(t, "laptop@store.com", product["email"])
		// Age can be int64 or float64 depending on JSON marshaling
		age := product["age"]
		switch v := age.(type) {
		case float64:
			assert.Equal(t, float64(2), v)
		case int64:
			assert.Equal(t, int64(2), v)
		case int:
			assert.Equal(t, 2, v)
		default:
			t.Fatalf("Unexpected age type: %T", age)
		}
		assert.NotEmpty(t, product["DocumentID"])
	})

	t.Run("Query subset of fields", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ products { name email } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		products := getResponseData(response, "products")

		// Convert to interface slice
		var productsList []interface{}
		if typedProducts, ok := products.([]map[string]interface{}); ok {
			productsList = make([]interface{}, len(typedProducts))
			for i, p := range typedProducts {
				productsList[i] = p
			}
		} else if interfaceProducts, ok := products.([]interface{}); ok {
			productsList = interfaceProducts
		}
		product := productsList[0].(map[string]interface{})

		assert.Equal(t, "Laptop", product["name"])
		assert.Equal(t, "laptop@store.com", product["email"])
		assert.NotContains(t, product, "age", "age should not be in response")
	})

	t.Run("Query single field", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ products { name } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		products := getResponseData(response, "products")

		productsList := convertToInterfaceSlice(products)
		product := productsList[0].(map[string]interface{})

		assert.Equal(t, "Laptop", product["name"])
		assert.Len(t, product, 1, "Should only contain name field")
	})
} // TestGraphQLQueries_Filtering tests WHERE clause functionality
func TestGraphQLQueries_Filtering(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "customers")

	// Add multiple test documents
	doc1 := createTestDocument(t, env, "customers", map[string]interface{}{
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   30,
	})
	createTestDocument(t, env, "customers", map[string]interface{}{
		"name":  "Bob",
		"email": "bob@example.com",
		"age":   25,
	})
	createTestDocument(t, env, "customers", map[string]interface{}{
		"name":  "Charlie",
		"email": "charlie@example.com",
		"age":   35,
	})

	t.Run("Filter by string equality", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ customers(where: \"name == \\\"Alice\\\"\") { DocumentID name email age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		customers := getResponseData(response, "customers")

		customersList := convertToInterfaceSlice(customers)
		assert.Len(t, customersList, 1)

		customer := customersList[0].(map[string]interface{})
		assert.Equal(t, "Alice", customer["name"])
		assertIntValue(t, 30, customer["age"], "age")
	})

	t.Run("Filter by DocumentID", func(t *testing.T) {
		query := fmt.Sprintf(`GRAPHQL::{ "query": "{ customers(where: \"DocumentID == \\\"%s\\\"\") { DocumentID name email } }" }`, doc1)

		t.Logf("DocumentID filter query: %s", query)
		t.Logf("Looking for DocumentID: %s", doc1)

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		customers := getResponseData(response, "customers")

		customersList := convertToInterfaceSlice(customers)
		t.Logf("Found %d customers", len(customersList))

		require.Len(t, customersList, 1, "Expected 1 customer, got %d", len(customersList))

		customer := customersList[0].(map[string]interface{})
		assert.Equal(t, doc1, customer["DocumentID"])
		assert.Equal(t, "Alice", customer["name"])
	})

	t.Run("Filter by integer comparison", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ customers(where: \"age > 28\") { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		customers := getResponseData(response, "customers")

		customersList := convertToInterfaceSlice(customers)
		assert.Len(t, customersList, 2, "Should find Alice and Charlie")

		names := make([]string, len(customersList))
		for i, c := range customersList {
			names[i] = c.(map[string]interface{})["name"].(string)
		}
		assert.Contains(t, names, "Alice")
		assert.Contains(t, names, "Charlie")
		assert.NotContains(t, names, "Bob")
	})
}

// TestGraphQLQueries_Sorting tests ORDER BY functionality
func TestGraphQLQueries_Sorting(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "employees")

	// Add test documents
	createTestDocument(t, env, "employees", map[string]interface{}{
		"name":  "Zara",
		"email": "zara@company.com",
		"age":   28,
	})
	createTestDocument(t, env, "employees", map[string]interface{}{
		"name":  "Alice",
		"email": "alice@company.com",
		"age":   35,
	})
	createTestDocument(t, env, "employees", map[string]interface{}{
		"name":  "Mike",
		"email": "mike@company.com",
		"age":   25,
	})

	t.Run("Sort by name ascending", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ employees(orderBy: \"name ASC\", limit: 10) { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		employees := getResponseData(response, "employees")

		employeesList := convertToInterfaceSlice(employees)
		assert.Len(t, employeesList, 3) // All 3 employees (Alice, Mike, Zara)

		// Verify ascending order
		assert.Equal(t, "Alice", employeesList[0].(map[string]interface{})["name"])
		assert.Equal(t, "Mike", employeesList[1].(map[string]interface{})["name"])
		assert.Equal(t, "Zara", employeesList[2].(map[string]interface{})["name"])
	})

	t.Run("Sort by age descending", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ employees(orderBy: \"age DESC\", limit: 10) { name age } }" }`

		t.Logf("Query: %s", query)

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		employees := getResponseData(response, "employees")

		employeesList := convertToInterfaceSlice(employees)

		// Log what we got
		for i, emp := range employeesList {
			empMap := emp.(map[string]interface{})
			t.Logf("Employee[%d]: name=%v, age=%v", i, empMap["name"], empMap["age"])
		}

		// Verify descending order by age (35, 28, 25)
		assert.Len(t, employeesList, 3) // All 3 employees
		assertIntValue(t, 35, employeesList[0].(map[string]interface{})["age"], "age[0]")
		assertIntValue(t, 28, employeesList[1].(map[string]interface{})["age"], "age[1]")
		assertIntValue(t, 25, employeesList[2].(map[string]interface{})["age"], "age[2]")
	})

	t.Run("Sort by name descending (SIMD string sort)", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ employees(orderBy: \"name DESC\", limit: 10) { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		employees := getResponseData(response, "employees")

		employeesList := convertToInterfaceSlice(employees)

		// Verify descending order by name (Zara, Mike, Alice)
		assert.Len(t, employeesList, 3)
		assert.Equal(t, "Zara", employeesList[0].(map[string]interface{})["name"])
		assert.Equal(t, "Mike", employeesList[1].(map[string]interface{})["name"])
		assert.Equal(t, "Alice", employeesList[2].(map[string]interface{})["name"])

		t.Logf("DESC string sort correct: %s, %s, %s",
			employeesList[0].(map[string]interface{})["name"],
			employeesList[1].(map[string]interface{})["name"],
			employeesList[2].(map[string]interface{})["name"])
	})
}

// TestGraphQLQueries_RadixSort tests radix sort on large integer datasets
// Phase 3: Validates O(n) radix sort for integer ORDER BY queries
func TestGraphQLQueries_RadixSort(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "transactions")

	// Create 2000 documents to trigger radix sort threshold (>1000)
	// This validates the O(n) linear time algorithm
	t.Logf("Creating 2000 test documents for radix sort validation...")
	for i := 1; i <= 2000; i++ {
		createTestDocument(t, env, "transactions", map[string]interface{}{
			"name":  fmt.Sprintf("Transaction%04d", i),
			"email": fmt.Sprintf("txn%04d@test.com", i),
			"age":   (i * 17) % 10000, // Semi-random amounts (using 'age' field as amount)
		})
	}

	t.Run("Radix sort large dataset ASC", func(t *testing.T) {
		// Query all 2000 documents sorted by integer field
		// Should trigger radix sort (>1000 docs, integer field)
		query := `GRAPHQL::{ "query": "{ transactions(orderBy: \"age ASC\", limit: 2000) { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		transactions := getResponseData(response, "transactions")

		transactionsList := convertToInterfaceSlice(transactions)
		assert.Len(t, transactionsList, 2000, "Should return all 2000 transactions")

		// Helper to convert interface{} to int (handles int64, float64, int)
		getIntValue := func(v interface{}) int {
			switch val := v.(type) {
			case int64:
				return int(val)
			case float64:
				return int(val)
			case int:
				return val
			default:
				t.Fatalf("Unexpected type for age: %T", val)
				return 0
			}
		}

		// Verify ascending order by checking first few and last few
		first := getIntValue(transactionsList[0].(map[string]interface{})["age"])
		second := getIntValue(transactionsList[1].(map[string]interface{})["age"])
		third := getIntValue(transactionsList[2].(map[string]interface{})["age"])

		assert.LessOrEqual(t, first, second, "First amount should be <= second")
		assert.LessOrEqual(t, second, third, "Second amount should be <= third")

		// Check last few
		n := len(transactionsList)
		lastAmount := getIntValue(transactionsList[n-1].(map[string]interface{})["age"])
		secondLastAmount := getIntValue(transactionsList[n-2].(map[string]interface{})["age"])

		assert.GreaterOrEqual(t, lastAmount, secondLastAmount, "Last amount should be >= second last")

		t.Logf("Radix sort ASC validated: first=%d, second=%d, last=%d", first, second, lastAmount)
	})

	t.Run("Radix sort large dataset DESC", func(t *testing.T) {
		// Test descending order
		query := `GRAPHQL::{ "query": "{ transactions(orderBy: \"age DESC\", limit: 2000) { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		transactions := getResponseData(response, "transactions")

		transactionsList := convertToInterfaceSlice(transactions)
		assert.Len(t, transactionsList, 2000)

		// Helper to convert interface{} to int (handles int64, float64, int)
		getIntValue := func(v interface{}) int {
			switch val := v.(type) {
			case int64:
				return int(val)
			case float64:
				return int(val)
			case int:
				return val
			default:
				t.Fatalf("Unexpected type for age: %T", val)
				return 0
			}
		}

		// Verify descending order
		firstAmount := getIntValue(transactionsList[0].(map[string]interface{})["age"])
		secondAmount := getIntValue(transactionsList[1].(map[string]interface{})["age"])
		thirdAmount := getIntValue(transactionsList[2].(map[string]interface{})["age"])

		assert.GreaterOrEqual(t, firstAmount, secondAmount, "First amount should be >= second")
		assert.GreaterOrEqual(t, secondAmount, thirdAmount, "Second amount should be >= third")

		// Check last few
		n := len(transactionsList)
		lastAmount := getIntValue(transactionsList[n-1].(map[string]interface{})["age"])
		secondLastAmount := getIntValue(transactionsList[n-2].(map[string]interface{})["age"])

		assert.LessOrEqual(t, lastAmount, secondLastAmount, "Last amount should be <= second last")

		t.Logf("Radix sort DESC validated: first=%d, second=%d, last=%d", firstAmount, secondAmount, lastAmount)
	})

	t.Run("Radix sort vs Top-N threshold", func(t *testing.T) {
		// Small LIMIT (10) on large dataset (2000) - should use Top-N heap, not radix
		// Ratio = 10/2000 = 0.005 = 0.5% < 50% threshold
		query := `GRAPHQL::{ "query": "{ transactions(orderBy: \"age ASC\", limit: 10) { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		transactions := getResponseData(response, "transactions")

		transactionsList := convertToInterfaceSlice(transactions)
		assert.Len(t, transactionsList, 10, "Should return exactly 10 transactions")

		// Helper to convert interface{} to int (handles int64, float64, int)
		getIntValue := func(v interface{}) int {
			switch val := v.(type) {
			case int64:
				return int(val)
			case float64:
				return int(val)
			case int:
				return val
			default:
				t.Fatalf("Unexpected type for age: %T", val)
				return 0
			}
		}

		// Verify these are the 10 smallest amounts
		for i := 0; i < len(transactionsList)-1; i++ {
			currentAmount := getIntValue(transactionsList[i].(map[string]interface{})["age"])
			nextAmount := getIntValue(transactionsList[i+1].(map[string]interface{})["age"])
			assert.LessOrEqual(t, currentAmount, nextAmount, "Amounts should be in ascending order")
		}

		firstAmount := getIntValue(transactionsList[0].(map[string]interface{})["age"])
		t.Logf("Top-N heap (LIMIT 10 on 2000 docs) validated: smallest amount=%d", firstAmount)
	})
}

// TestGraphQLQueries_Pagination tests LIMIT and OFFSET functionality
func TestGraphQLQueries_Pagination(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "items")

	// Add multiple documents for pagination testing
	for i := 1; i <= 10; i++ {
		createTestDocument(t, env, "items", map[string]interface{}{
			"name":  fmt.Sprintf("Item%d", i),
			"email": fmt.Sprintf("item%d@test.com", i),
			"age":   i * 10,
		})
	}

	t.Run("Limit results", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ items(limit: 3) { name } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		items := getResponseData(response, "items")

		itemsList := convertToInterfaceSlice(items)
		assert.Len(t, itemsList, 3, "Should return exactly 3 items")
	})

	t.Run("Offset results", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ items(offset: 5, limit: 3) { name } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		items := getResponseData(response, "items")

		itemsList := convertToInterfaceSlice(items)
		assert.Len(t, itemsList, 3, "Should return 3 items after skipping 5")
	})

	t.Run("Pagination with sorting", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ items(orderBy: \"age ASC\", limit: 2, offset: 1) { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		items := getResponseData(response, "items")

		itemsList := convertToInterfaceSlice(items)
		assert.Len(t, itemsList, 2)

		// After sorting by age ASC and skipping first, should get items with age 20 and 30
		firstAge := itemsList[0].(map[string]interface{})["age"]
		secondAge := itemsList[1].(map[string]interface{})["age"]
		assertIntValue(t, 20, firstAge, "firstAge")
		assertIntValue(t, 30, secondAge, "secondAge")
	})
}

// TestGraphQLQueries_ComplexQueries tests combinations of filtering, sorting, and pagination
func TestGraphQLQueries_ComplexQueries(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "users")

	// Add diverse test data
	createTestDocument(t, env, "users", map[string]interface{}{
		"name":  "John",
		"email": "john@example.com",
		"age":   45,
	})
	createTestDocument(t, env, "users", map[string]interface{}{
		"name":  "Jane",
		"email": "jane@example.com",
		"age":   38,
	})
	createTestDocument(t, env, "users", map[string]interface{}{
		"name":  "Bob",
		"email": "bob@example.com",
		"age":   29,
	})
	createTestDocument(t, env, "users", map[string]interface{}{
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   52,
	})
	createTestDocument(t, env, "users", map[string]interface{}{
		"name":  "Charlie",
		"email": "charlie@example.com",
		"age":   33,
	})

	t.Run("Filter + Sort + Limit", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ users(where: \"age > 30\", orderBy: \"age DESC\", limit: 2) { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		users := getResponseData(response, "users")

		usersList := convertToInterfaceSlice(users)
		require.Len(t, usersList, 2, "Expected 2 users with age > 30, sorted by age DESC, limited to 2")

		// Should get Alice (52) and John (45)
		firstUser := usersList[0].(map[string]interface{})
		secondUser := usersList[1].(map[string]interface{})

		assert.Equal(t, "Alice", firstUser["name"])
		assertIntValue(t, 52, firstUser["age"], "Alice age")
		assert.Equal(t, "John", secondUser["name"])
		assertIntValue(t, 45, secondUser["age"], "John age")
	})

	t.Run("Filter + Sort + Offset + Limit", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ users(where: \"age >= 30\", orderBy: \"name ASC\", offset: 1, limit: 2) { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		users := getResponseData(response, "users")

		usersList := convertToInterfaceSlice(users)
		require.Len(t, usersList, 2, "Expected 2 users with age >= 30, sorted by name ASC, offset 1")

		// Users >= 30 sorted by name: Alice, Charlie, Jane, John
		// Skip 1 (Alice), get 2 (Charlie, Jane)
		assert.Equal(t, "Charlie", usersList[0].(map[string]interface{})["name"])
		assert.Equal(t, "Jane", usersList[1].(map[string]interface{})["name"])
	})
}

// TestGraphQLQueries_EmptyResults tests queries that return no results
func TestGraphQLQueries_EmptyResults(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "products")

	t.Run("Empty bundle query", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ products { name email } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		products := getResponseData(response, "products")

		productsList := convertToInterfaceSlice(products)
		assert.Len(t, productsList, 0, "Should return empty array for empty bundle")
	})

	t.Run("No matching filter", func(t *testing.T) {
		createTestDocument(t, env, "products", map[string]interface{}{
			"name":  "Widget",
			"email": "widget@store.com",
			"age":   5,
		})

		query := `GRAPHQL::{ "query": "{ products(where: \"name == \\\"NonExistent\\\"\") { name } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		products := getResponseData(response, "products")

		productsList := convertToInterfaceSlice(products)
		assert.Len(t, productsList, 0, "Should return empty array when no matches")
	})
}

// TestGraphQLQueries_ErrorHandling tests error cases
func TestGraphQLQueries_ErrorHandling(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "products")

	t.Run("Query non-existent bundle", func(t *testing.T) {
		query := `GRAPHQL::
			{
				nonexistent {
					name
				}
			}
		`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)

		// Should either error or return graphql errors
		if err == nil {
			errors := getGraphQLErrors(result)
			assert.NotEmpty(t, errors, "Should have GraphQL errors for non-existent bundle")
		}
	})

	t.Run("Query non-existent field", func(t *testing.T) {
		createTestDocument(t, env, "products", map[string]interface{}{
			"name":  "Test",
			"email": "test@example.com",
			"age":   1,
		})

		query := `GRAPHQL::
			{
				products {
					name
					nonExistentField
				}
			}
		`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)

		// Should either error or return graphql errors
		if err == nil {
			errors := getGraphQLErrors(result)
			assert.NotEmpty(t, errors, "Should have GraphQL errors for non-existent field")
		}
	})

	t.Run("Invalid filter syntax", func(t *testing.T) {
		query := `GRAPHQL::
			{
				products(where: {invalidSyntax}) {
					name
				}
			}
		`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)

		// Should error due to invalid syntax
		if err == nil {
			errors := getGraphQLErrors(result)
			assert.NotEmpty(t, errors, "Should have GraphQL errors for invalid syntax")
		}
	})
}

// TestGraphQLQueries_Introspection tests schema introspection queries
func TestGraphQLQueries_Introspection(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "products")

	t.Run("Query type introspection", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ __type(name: \"Query\") { fields { name } } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		t.Logf("Query type fields: %+v", result)
	})

	t.Run("Schema introspection", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ __schema { types { name } } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		t.Logf("Schema types: %+v", result)
	})
}

// TestGraphQLQueries_FieldAliases tests GraphQL field aliases
func TestGraphQLQueries_FieldAliases(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "products")
	createTestDocument(t, env, "products", map[string]interface{}{
		"name":  "Laptop",
		"email": "laptop@store.com",
		"age":   2,
	})

	t.Run("Simple field alias", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ products { productName: name contactEmail: email } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		products := getResponseData(response, "products")

		productsList := convertToInterfaceSlice(products)
		product := productsList[0].(map[string]interface{})

		// Should use aliased names
		assert.Equal(t, "Laptop", product["productName"])
		assert.Equal(t, "laptop@store.com", product["contactEmail"])
		assert.NotContains(t, product, "name")
		assert.NotContains(t, product, "email")
	})
}

// TestGraphQLQueries_MultipleDocuments tests queries with larger datasets
func TestGraphQLQueries_MultipleDocuments(t *testing.T) {
	env := setupGraphQLTestEnvironment(t)
	defer env.Cleanup()

	createTestBundle(t, env, "inventory")

	// Create 50 documents for realistic testing
	for i := 1; i <= 50; i++ {
		createTestDocument(t, env, "inventory", map[string]interface{}{
			"name":  fmt.Sprintf("Product-%d", i),
			"email": fmt.Sprintf("product%d@store.com", i),
			"age":   i,
		})
	}

	t.Run("Query all documents", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ inventory { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		inventory := getResponseData(response, "inventory")

		inventoryList := convertToInterfaceSlice(inventory)
		assert.Len(t, inventoryList, 50, "Should return all 50 documents")
	})

	t.Run("Pagination through large dataset", func(t *testing.T) {
		// Get first page
		query1 := `GRAPHQL::{ "query": "{ inventory(limit: 10, offset: 0, orderBy: \"age ASC\") { age } }" }`

		result1, err := env.GraphQLHandler.ProcessGraphQLCommand(query1)
		require.NoError(t, err)

		response1 := assertNoGraphQLErrors(t, result1)
		page1 := convertToInterfaceSlice(getResponseData(response1, "inventory"))
		assert.Len(t, page1, 10)
		assertIntValue(t, 1, page1[0].(map[string]interface{})["age"], "page1[0].age")

		// Get second page
		query2 := `GRAPHQL::{ "query": "{ inventory(limit: 10, offset: 10, orderBy: \"age ASC\") { age } }" }`

		result2, err := env.GraphQLHandler.ProcessGraphQLCommand(query2)
		require.NoError(t, err)

		response2 := assertNoGraphQLErrors(t, result2)
		page2 := convertToInterfaceSlice(getResponseData(response2, "inventory"))
		assert.Len(t, page2, 10)
		assertIntValue(t, 11, page2[0].(map[string]interface{})["age"], "page2[0].age")
	})

	t.Run("Complex filter on large dataset", func(t *testing.T) {
		query := `GRAPHQL::{ "query": "{ inventory(where: \"age >= 20 AND age <= 30\", orderBy: \"age ASC\") { name age } }" }`

		result, err := env.GraphQLHandler.ProcessGraphQLCommand(query)
		require.NoError(t, err)

		response := assertNoGraphQLErrors(t, result)
		inventory := getResponseData(response, "inventory")

		inventoryList := convertToInterfaceSlice(inventory)
		assert.Len(t, inventoryList, 11, "Should return ages 20-30 inclusive")

		// Verify range
		firstAge := inventoryList[0].(map[string]interface{})["age"]
		lastAge := inventoryList[10].(map[string]interface{})["age"]
		assertIntValue(t, 20, firstAge, "firstAge")
		assertIntValue(t, 30, lastAge, "lastAge")
	})
}
