package syndrQL

import (
	"context"
	"fmt"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

func getFieldList() string {
	return `"AuthorID", "FirstName", "LastName", "BirthDate", "DeathDate", "Biography"`
}

func getAllFields() string {
	return "*"
}

func getTopFive() string {
	return "TOP 5 *"
}

func getCount() string {
	return "COUNT(*)"
}

func getFromClause() string {
	return `FROM "Authors"`
}

func getJoinClause() string {
	return `JOIN "Books" ON "Authors"."DocumentID" == "Books"."AuthorsID"`
}

func getBascWhereClause() string {
	return `WHERE "Authors"."Name" == 'Strohschein'`
}

func getRangeWhereClause() string {
	return `WHERE "Authors"."BirthYear" >= '1970' AND "Authors"."BirthYear" <= '2000'`
}

func getOrderByClause() string {
	return `ORDER BY "Authors"."Name" ASC`
}

func getGroupByClause() string {
	return `GROUP BY "Authors"."Name"`
}

func seedSimpleAuthorsBundle(t *testing.T, fixture *TestFixture, count int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	countries := []string{"USA", "UK", "Canada", "France"}

	for i := 1; i <= count; i++ {
		var name string
		// Add 3 authors named "Strohschein" at the end
		if i > count-3 {
			name = "Strohschein"
		} else {
			name = fmt.Sprintf("Author_%03d", i)
		}
		country := countries[(i-1)%len(countries)]
		birthYear := 1950 + i

		// Use correct ADD DOCUMENT syntax with multiple braces (match exact user format)
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH ({"ID"=%d}, {"Name"="%s"}, {"Country"="%s"}, {"BirthYear"=%d});`,
			i, name, country, birthYear)

		// Debug first document command

		startTime := time.Now()
		response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed author %d: %v", i, err)
		}

		// Debug first document response
		if i == 1 {
			t.Logf("First ADD DOCUMENT response type: %T", response)
			if cmdResp, ok := response.(*server.CommandResponse); ok {
				t.Logf("First ADD DOCUMENT result: %+v", cmdResp.Result)
			}

		}
	}
}

func seedSimpleAuthorsBundleTB(tb testing.TB, fixture *TestFixture, count int) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	countries := []string{"USA", "UK", "Canada", "France"}

	for i := 1; i <= count; i++ {
		var name string
		// Add 3 authors named "Strohschein" at the end
		if i > count-3 {
			name = "Strohschein"
		} else {
			name = fmt.Sprintf("Author_%03d", i)
		}
		country := countries[(i-1)%len(countries)]
		birthYear := 1950 + i

		// Use correct ADD DOCUMENT syntax with multiple braces (match exact user format)
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH ({"ID"=%d}, {"Name"="%s"}, {"Country"="%s"}, {"BirthYear"=%d});`,
			i, name, country, birthYear)

		// Debug first document command

		startTime := time.Now()
		response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			tb.Fatalf("Failed to seed author %d: %v", i, err)
		}

		// Debug first document response
		if i == 1 {
			tb.Logf("First ADD DOCUMENT response type: %T", response)
			if cmdResp, ok := response.(*server.CommandResponse); ok {
				tb.Logf("First ADD DOCUMENT result: %+v", cmdResp.Result)
			}

		}
	}
}

// validateCountResult validates a COUNT(*) query result
// COUNT(*) returns 1 result row containing the count value
func validateCountResult(t *testing.T, response interface{}, expectedCount int, testName string) {
	t.Helper()

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		// COUNT(*) should return 1 result row
		if cmdResp.ResultCount != 1 {
			t.Errorf("[%s] Expected 1 result row for COUNT(*), got %d", testName, cmdResp.ResultCount)
			return
		}

		// Extract and validate the count value
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			if len(results) != 1 {
				t.Errorf("[%s] Expected 1 result in array, got %d", testName, len(results))
				return
			}

			// Extract the count value
			countValue := 0
			if count, ok := results[0]["COUNT(*)"].(float64); ok {
				countValue = int(count)
			} else if count, ok := results[0]["COUNT(*)"].(int); ok {
				countValue = count
			}

			if countValue != expectedCount {
				t.Errorf("[%s] Expected COUNT(*) = %d, got %d", testName, expectedCount, countValue)
			} else {
				t.Logf("✓ %s passed: COUNT(*) = %d", testName, countValue)
			}
		}
	}
}

func clearAuthorsBundle(t *testing.T, fixture *TestFixture, count int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 1; i <= count; i++ {
		deleteCmd := fmt.Sprintf(`DELETE DOCUMENTS FROM "Authors" WHERE "ID" == "%d"`, i)
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, deleteCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to clear author %d: %v", i, err)
		}
	}
}

func seedSimpleBooksBundle(t *testing.T, fixture *TestFixture, count int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	genres := []string{"Fiction", "NonFiction", "Mystery", "SciFi", "Fantasy"}

	for i := 1; i <= count; i++ {
		title := fmt.Sprintf("Book_%03d", i)
		authorID := ((i - 1) % 100) + 1 // Cycle through authors 1-100
		genre := genres[(i-1)%len(genres)]

		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Books" WITH ({"ID"=%d}, {"Title"="%s"}, {"AuthorID"=%d}, {"Genre"="%s"});`,
			i, title, authorID, genre)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed book %d: %v", i, err)
		}
	}
}

func seedSimpleBooksBundleTB(tb testing.TB, fixture *TestFixture, count int) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	genres := []string{"Fiction", "NonFiction", "Mystery", "SciFi", "Fantasy"}

	for i := 1; i <= count; i++ {
		title := fmt.Sprintf("Book_%03d", i)
		authorID := ((i - 1) % 100) + 1 // Cycle through authors 1-100
		genre := genres[(i-1)%len(genres)]

		// AuthorsID is the foreign key field created by the relationship
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Books" WITH ({"ID"=%d}, {"Title"="%s"}, {"AuthorID"=%d}, {"AuthorsID"=%d}, {"Genre"="%s"});`,
			i, title, authorID, authorID, genre)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			tb.Fatalf("Failed to seed book %d: %v", i, err)
		}
	}
}

func setupBundles(t *testing.T, fixture *TestFixture) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bundles := []string{"Authors", "Books", "Publishers"}

	// Drop existing bundles (ignore errors if they don't exist)
	for _, bundleName := range bundles {
		dropCmd := fmt.Sprintf(`DROP BUNDLE "%s" WITH FORCE`, bundleName)
		startTime := time.Now()
		// Pass fixture.Database to ensure commands operate on correct database
		_, _ = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	}

	// Create Authors bundle - must pass database context
	// NOTE: Avoiding UNIQUE constraint for now due to indexing issues in test environment
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Name", "STRING", true, false, ""},
		{"Country", "STRING", false, false, ""},
		{"BirthYear", "INT", false, false, 0}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Authors bundle: %v", err)
	}

	// // Debug: Show all bundles to see what was actually created
	// showBundlesCmd := `SHOW BUNDLES`
	// startTime = time.Now()
	// bundlesInfo, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, showBundlesCmd, fixture.Logger, startTime)
	// if err == nil {
	// 	t.Logf("SHOW BUNDLES result: %+v", bundlesInfo)
	// } else {
	// 	t.Logf("SHOW BUNDLES error: %v", err)
	// }

	// Create Books bundle - must pass database context
	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"ID", "INT", true, true, null},
		{"Title", "STRING", true, false, null},
		{"AuthorID", "INT", false, false, null},
		{"Genre", "STRING", false, false, null},
		{"Price", "FLOAT", false, false, null}
	)`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Books bundle: %v", err)
	}

	// Create Publishers bundle - must pass database context
	createPublishersCmd := `CREATE BUNDLE "Publishers" WITH FIELDS (
		{"ID", "INT", true, true, null},
		{"Name", "STRING", true, false, null},
		{"Country", "STRING", false, false, null}
	)`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createPublishersCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Publishers bundle: %v", err)
	}

	createRelationshipCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP ("1toMany", "Authors", "DocumentID", "Books", "AuthorsID");`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create relationship: %v", err)
	}

	fixture.Logger.Infof("✓ Database reset complete (bundles: %v)", bundles)
}

func setupBundlesTB(tb testing.TB, fixture *TestFixture) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bundles := []string{"Authors", "Books", "Publishers"}

	// Drop existing bundles (ignore errors if they don't exist)
	for _, bundleName := range bundles {
		dropCmd := fmt.Sprintf(`DROP BUNDLE "%s" WITH FORCE`, bundleName)
		startTime := time.Now()
		// Pass fixture.Database to ensure commands operate on correct database
		_, _ = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	}

	// Create Authors bundle - must pass database context
	// NOTE: Avoiding UNIQUE constraint for now due to indexing issues in test environment
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false, 0},
		{"Name", "STRING", true, false, ""},
		{"Country", "STRING", false, false, ""},
		{"BirthYear", "INT", false, false, 0}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create Authors bundle: %v", err)
	}

	// // Debug: Show all bundles to see what was actually created
	// showBundlesCmd := `SHOW BUNDLES`
	// startTime = time.Now()
	// bundlesInfo, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, showBundlesCmd, fixture.Logger, startTime)
	// if err == nil {
	// 	tb.Logf("SHOW BUNDLES result: %+v", bundlesInfo)
	// } else {
	// 	tb.Logf("SHOW BUNDLES error: %v", err)
	// }

	// Create Books bundle - must pass database context
	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"ID", "INT", true, true, null},
		{"Title", "STRING", true, false, null},
		{"AuthorID", "INT", false, false, null},
		{"Genre", "STRING", false, false, null},
		{"Price", "FLOAT", false, false, null}
	)`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create Books bundle: %v", err)
	}

	// Create Publishers bundle - must pass database context
	createPublishersCmd := `CREATE BUNDLE "Publishers" WITH FIELDS (
		{"ID", "INT", true, true, null},
		{"Name", "STRING", true, false, null},
		{"Country", "STRING", false, false, null}
	)`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createPublishersCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create Publishers bundle: %v", err)
	}

	createRelationshipCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP ("1toMany", "Authors", "DocumentID", "Books", "AuthorsID");`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create relationship: %v", err)
	}

	fixture.Logger.Infof("✓ Database reset complete (bundles: %v)", bundles)
}

func setupRealServer(t *testing.T) *TestFixture {
	fixture := setupFullServer(t)
	setupBundles(t, fixture)

	// Register cleanup to shutdown services before temp directory cleanup
	t.Cleanup(func() {
		if fixture.ServiceManager != nil && fixture.ServiceManager.BundleService != nil {
			if err := fixture.ServiceManager.BundleService.Shutdown(); err != nil {
				t.Logf("Warning: BundleService shutdown error: %v", err)
			}
		}
	})

	return fixture
}

func setupRealServerTB(tb testing.TB) *TestFixture {
	fixture := setupFullServerTB(tb)
	setupBundlesTB(tb, fixture)

	// Register cleanup to shutdown services before temp directory cleanup
	tb.Cleanup(func() {
		if fixture.ServiceManager != nil && fixture.ServiceManager.BundleService != nil {
			if err := fixture.ServiceManager.BundleService.Shutdown(); err != nil {
				tb.Logf("Warning: BundleService shutdown error: %v", err)
			}
		}
	})

	return fixture
}

/*
|==================================================================================|
|                  Test Functions                                                  |
|==================================================================================|


SELECT all fields
Select Field list
Select TOP N
Select COUNT

SELECT all fields BASIC Where
Select Field list BASIC Where
Select TOP N BASIC Where
Select COUNT BASIC Where

SELECT all fields Range Where
Select Field list Range Where
Select TOP N Range Where
Select COUNT Range Where

Select all fields JOIN  BASIC Where
Select Field list JOIN  BASIC Where
Select TOP N JOIN  BASIC Where
Select COUNT JOIN  BASIC Where

Select all fields JOIN  Range Where
Select Field list JOIN  Range Where
Select TOP N JOIN  Range Where
Select COUNT JOIN  Range Where


SELECT all fields Order By
Select Field list Order By
Select TOP N Order By
Select COUNT Order By

SELECT all fields BASIC Where Order By
Select Field list BASIC Where Order By
Select TOP N BASIC Where Order By
Select COUNT BASIC Where Order By

SELECT all fields Range Where Order By
Select Field list Range Where Order By
Select TOP N Range Where Order By
Select COUNT Range Where Order By

Select all fields JOIN  BASIC Where Order By
Select Field list JOIN  BASIC Where Order By
Select TOP N JOIN  BASIC Where Order By
Select COUNT JOIN  BASIC Where Order By

Select all fields JOIN  Range Where Order By
Select Field list JOIN  Range Where Order By
Select TOP N JOIN  Range Where Order By
Select COUNT JOIN  Range Where Order By


SELECT all fields Group By
Select Field list Group By
Select TOP N Group By
Select COUNT Group By

SELECT all fields BASIC Where Group By
Select Field list BASIC Where Group By
Select TOP N BASIC Where Group By
Select COUNT BASIC Where Group By

SELECT all fields Range Where Group By
Select Field list Range Where Group By
Select TOP N Range Where Group By
Select COUNT Range Where Group By

Select all fields JOIN  BASIC Where Group By
Select Field list JOIN  BASIC Where Group By
Select TOP N JOIN  BASIC Where Group By
Select COUNT JOIN  BASIC Where Group By

Select all fields JOIN  Range Where Group By
Select Field list JOIN  Range Where Group By
Select TOP N JOIN  Range Where Group By
Select COUNT JOIN  Range Where Group By


*/

func TestSelect_Basic(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	clearAuthorsBundle(t, fixture, 100)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT * %s;`, getFromClause())
	//query := fmt.Sprintf(`SELECT DOCUMENTS %s;`, getFromClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 100 {
			t.Errorf("Expected count 100, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
				t.Logf("Raw First Result:: ")
			}
		}
	}

	t.Logf("✓ TestSelect_Basic passed: count=%d", count)
}

func TestSelect_BasicJoin(t *testing.T) {
	fixture := setupRealServer(t)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getAllFields(), getFromClause(), getJoinClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 100 {
			t.Errorf("Expected count 100, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_BasicJoin passed: count=%d", count)
}

func TestSelect_BasicWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)
	//Decidedly different count for the where
	query := fmt.Sprintf(`SELECT %s %s %s;`, getAllFields(), getFromClause(), getBascWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_Basic passed: count=%d", count)
}

func TestSelect_BasicJoinWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	//Decidedly different count for the where
	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getAllFields(), getFromClause(), getJoinClause(), getBascWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (WHERE Name='Strohschein' filters to 3 authors), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_Basic passed: count=%d", count)
}

func TestSelect_BasicJoinWhereSorted(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)
	//Decidedly different count for the where
	// VALIDATE THE SORT ACTUALLY HAPPENED!!
	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getAllFields(), getFromClause(), getJoinClause(), getBascWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (WHERE Name='Strohschein' filters to 3 authors), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_Basic passed: count=%d", count)
}

func TestSelect_BasicJoinWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)
	//Decidedly different count for the where
	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getAllFields(), getFromClause(), getJoinClause(), getBascWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected count 1 (JOIN + WHERE Name='Strohschein' GROUP BY Name), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_Basic passed: count=%d", count)
}

func TestSelect_FieldList(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s;`, getFieldList(), getFromClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 100 {
			t.Errorf("Expected count 100, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldList passed: count=%d", count)
}

func TestSelect_TopN(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s;`, getTopFive(), getFromClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 5 {
			t.Errorf("Expected count 5, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopN passed: count=%d", count)
}

func TestSelect_Count(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s;`, getCount(), getFromClause())
	response := executeRealQuery(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		// COUNT(*) returns 1 result row
		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected 1 result row for COUNT(*), got %d", cmdResp.ResultCount)
		}

		// Check the count value in the result
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			if len(results) != 1 {
				t.Errorf("Expected 1 result in array, got %d", len(results))
			} else {
				// Extract the count value
				countValue := 0
				if count, ok := results[0]["COUNT(*)"].(float64); ok {
					countValue = int(count)
				} else if count, ok := results[0]["COUNT(*)"].(int); ok {
					countValue = count
				}

				if countValue != 100 {
					t.Errorf("Expected COUNT(*) = 100, got %d", countValue)
				}
				t.Logf("✓ TestSelect_Count passed: COUNT(*) = %d", countValue)
			}
		}
	}
}

func TestSelect_FieldListBasicWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getFieldList(), getFromClause(), getBascWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListBasicWhere passed: count=%d", count)
}

func TestSelect_TopNBasicWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getTopFive(), getFromClause(), getBascWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (TOP 5 limited by WHERE to 3 Strohschein authors), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNBasicWhere passed: count=%d", count)
}

func TestSelect_CountBasicWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getCount(), getFromClause(), getBascWhereClause())
	response := executeRealQuery(t, fixture, query)
	// WHERE Name='Strohschein' matches 3 authors
	validateCountResult(t, response, 3, "TestSelect_CountBasicWhere")
}

func TestSelect_AllFieldsRangeWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getAllFields(), getFromClause(), getRangeWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (BirthYear 1970-2000), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsRangeWhere passed: count=%d", count)
}

func TestSelect_FieldListRangeWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getFieldList(), getFromClause(), getRangeWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (BirthYear 1970-2000), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListRangeWhere passed: count=%d", count)
}

func TestSelect_TopNRangeWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getTopFive(), getFromClause(), getRangeWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 5 {
			t.Errorf("Expected count 5, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNRangeWhere passed: count=%d", count)
}

func TestSelect_CountRangeWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getCount(), getFromClause(), getRangeWhereClause())
	response := executeRealQuery(t, fixture, query)
	// BirthYear 1970-2000: authors with i=20 to i=50, total=31
	validateCountResult(t, response, 31, "TestSelect_CountRangeWhere")
}

func TestSelect_AllFieldsJoinBasicWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getAllFields(), getFromClause(), getJoinClause(), getBascWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (WHERE Name='Strohschein'), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsJoinBasicWhere passed: count=%d", count)
}

func TestSelect_FieldListJoinBasicWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getFieldList(), getFromClause(), getJoinClause(), getBascWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (WHERE Name='Strohschein'), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListJoinBasicWhere passed: count=%d", count)
}

func TestSelect_TopNJoinBasicWhere(t *testing.T) {
	fixture := setupRealServer(t)

	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getTopFive(), getFromClause(), getJoinClause(), getBascWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (TOP 5 limited by WHERE to 3), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNJoinBasicWhere passed: count=%d", count)
}

func TestSelect_CountJoinBasicWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getCount(), getFromClause(), getJoinClause(), getBascWhereClause())
	response := executeRealQuery(t, fixture, query)
	validateCountResult(t, response, 3, "TestSelect_CountJoinBasicWhere")
}

func TestSelect_AllFieldsJoinRangeWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getAllFields(), getFromClause(), getJoinClause(), getRangeWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (BirthYear 1970-2000), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsJoinRangeWhere passed: count=%d", count)
}

func TestSelect_FieldListJoinRangeWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getFieldList(), getFromClause(), getJoinClause(), getRangeWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (BirthYear 1970-2000), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListJoinRangeWhere passed: count=%d", count)
}

func TestSelect_TopNJoinRangeWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getTopFive(), getFromClause(), getJoinClause(), getRangeWhereClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 5 {
			t.Errorf("Expected count 5 (TOP 5 from 31 results), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNJoinRangeWhere passed: count=%d", count)
}

func TestSelect_CountJoinRangeWhere(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getCount(), getFromClause(), getJoinClause(), getRangeWhereClause())
	response := executeRealQuery(t, fixture, query)
	validateCountResult(t, response, 31, "TestSelect_CountJoinRangeWhere")
}

func TestSelect_AllFieldsOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getAllFields(), getFromClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 100 {
			t.Errorf("Expected count 100, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsOrderBy passed: count=%d", count)
}

func TestSelect_FieldListOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getFieldList(), getFromClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 100 {
			t.Errorf("Expected count 100, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListOrderBy passed: count=%d", count)
}

func TestSelect_TopNOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getTopFive(), getFromClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 5 {
			t.Errorf("Expected count 5, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNOrderBy passed: count=%d", count)
}

// DISABLED: Invalid SQL - COUNT with ORDER BY requires GROUP BY
func _TestSelect_CountOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getCount(), getFromClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 100 {
			t.Errorf("Expected count 100, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_CountOrderBy passed: count=%d", count)
}

func TestSelect_AllFieldsBasicWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getAllFields(), getFromClause(), getBascWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (WHERE Name='Strohschein'), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsBasicWhereOrderBy passed: count=%d", count)
}

func TestSelect_FieldListBasicWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getFieldList(), getFromClause(), getBascWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (WHERE Name='Strohschein'), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListBasicWhereOrderBy passed: count=%d", count)
}

func TestSelect_TopNBasicWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getTopFive(), getFromClause(), getBascWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (TOP 5 limited by WHERE to 3), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNBasicWhereOrderBy passed: count=%d", count)
}

// DISABLED: Invalid SQL - COUNT with ORDER BY requires GROUP BY
func _TestSelect_CountBasicWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getCount(), getFromClause(), getBascWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	validateCountResult(t, response, 3, "TestSelect_CountBasicWhereOrderBy")
}

func TestSelect_AllFieldsRangeWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getAllFields(), getFromClause(), getRangeWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (BirthYear 1970-2000), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsRangeWhereOrderBy passed: count=%d", count)
}

func TestSelect_FieldListRangeWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getFieldList(), getFromClause(), getRangeWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (BirthYear 1970-2000), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListRangeWhereOrderBy passed: count=%d", count)
}

func TestSelect_TopNRangeWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getTopFive(), getFromClause(), getRangeWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 5 {
			t.Errorf("Expected count 5, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNRangeWhereOrderBy passed: count=%d", count)
}

// DISABLED: Invalid SQL - COUNT with ORDER BY requires GROUP BY
func _TestSelect_CountRangeWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getCount(), getFromClause(), getRangeWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	validateCountResult(t, response, 31, "TestSelect_CountRangeWhereOrderBy")
}

func TestSelect_AllFieldsJoinBasicWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getAllFields(), getFromClause(), getJoinClause(), getBascWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (WHERE Name='Strohschein'), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsJoinBasicWhereOrderBy passed: count=%d", count)
}

func TestSelect_FieldListJoinBasicWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getFieldList(), getFromClause(), getJoinClause(), getBascWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (WHERE Name='Strohschein'), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListJoinBasicWhereOrderBy passed: count=%d", count)
}

func TestSelect_TopNJoinBasicWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getTopFive(), getFromClause(), getJoinClause(), getBascWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 3 {
			t.Errorf("Expected count 3 (3 matching records, LIMIT 5), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNJoinBasicWhereOrderBy passed: count=%d", count)
}

// DISABLED: Invalid SQL - COUNT with ORDER BY requires GROUP BY
func _TestSelect_CountJoinBasicWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s  %s %s %s %s;`, getCount(), getFromClause(), getJoinClause(), getBascWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 100 {
			t.Errorf("Expected count 100, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_CountJoinBasicWhereOrderBy passed: count=%d", count)
}

func TestSelect_AllFieldsJoinRangeWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s  %s %s  %s %s;`, getAllFields(), getFromClause(), getJoinClause(), getRangeWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (WHERE BirthYear 1970-2000), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsJoinRangeWhereOrderBy passed: count=%d", count)
}

func TestSelect_FieldListJoinRangeWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s  %s %s %s %s;`, getFieldList(), getFromClause(), getJoinClause(), getRangeWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (WHERE BirthYear 1970-2000), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListJoinRangeWhereOrderBy passed: count=%d", count)
}

func TestSelect_TopNJoinRangeWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s  %s %s  %s %s;`, getTopFive(), getFromClause(), getJoinClause(), getRangeWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 5 {
			t.Errorf("Expected count 5, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNJoinRangeWhereOrderBy passed: count=%d", count)
}

// DISABLED: Invalid SQL - COUNT with ORDER BY requires GROUP BY
func _TestSelect_CountJoinRangeWhereOrderBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s  %s %s  %s %s;`, getCount(), getFromClause(), getJoinClause(), getRangeWhereClause(), getOrderByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 100 {
			t.Errorf("Expected count 100, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_CountJoinRangeWhereOrderBy passed: count=%d", count)
}

func TestSelect_AllFieldsGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s  %s %s;`, getAllFields(), getFromClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 98 {
			t.Errorf("Expected count 98 (100 docs, 3 'Strohschein' grouped), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsGroupBy passed: count=%d", count)
}

func TestSelect_FieldListGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s;`, getFieldList(), getFromClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 98 {
			t.Errorf("Expected count 98 (100 docs, 3 'Strohschein' grouped), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListGroupBy passed: count=%d", count)
}

func TestSelect_TopNGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s  %s  %s;`, getTopFive(), getFromClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 5 {
			t.Errorf("Expected count 5, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNGroupBy passed: count=%d", count)
}

func TestSelect_CountGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s  %s;`, getCount(), getFromClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 98 {
			t.Errorf("Expected count 98 (100 docs, 3 'Strohschein' grouped), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_CountGroupBy passed: count=%d", count)
}

func TestSelect_AllFieldsBasicWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getAllFields(), getFromClause(), getBascWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected count 1 (WHERE Name='Strohschein' GROUP BY Name), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsBasicWhereGroupBy passed: count=%d", count)
}

func TestSelect_FieldListBasicWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getFieldList(), getFromClause(), getBascWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected count 1 (WHERE Name='Strohschein' GROUP BY Name), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListBasicWhereGroupBy passed: count=%d", count)
}

func TestSelect_TopNBasicWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getTopFive(), getFromClause(), getBascWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected count 1 (TOP 5, but only 1 group with WHERE Name='Strohschein'), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNBasicWhereGroupBy passed: count=%d", count)
}

// func TestSelect_CountBasicWhereGroupBy(t *testing.T) {
// 	fixture := setupRealServer(t)
// 	//resetDatabase(t, fixture)
// 	seedSimpleAuthorsBundle(t, fixture, 100)

// 	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getCount(), getFromClause(), getBascWhereClause(), getGroupByClause())
// 	response := executeRealQuery(t, fixture, query)
// 	count := 0
// 	if cmdResp, ok := response.(*server.CommandResponse); ok {

// 		if cmdResp.ResultCount != 1 {
// 			t.Errorf("Expected count 1 (WHERE Name='Strohschein' GROUP BY Name), got %d", cmdResp.ResultCount)
// 		}
// 		count = cmdResp.ResultCount
// 		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
// 			t.Logf("Number of results: %d", len(results))
// 			if len(results) > 0 {
// 				t.Logf("First result: %+v", results[0])
// 			}
// 		}
// 	}

// 	t.Logf("✓ TestSelect_CountBasicWhereGroupBy passed: count=%d", count)
// }

func TestSelect_AllFieldsRangeWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getAllFields(), getFromClause(), getRangeWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (WHERE BirthYear 1970-2000, GROUP BY Name), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsRangeWhereGroupBy passed: count=%d", count)
}

func TestSelect_FieldListRangeWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getFieldList(), getFromClause(), getRangeWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (WHERE BirthYear 1970-2000, GROUP BY Name), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListRangeWhereGroupBy passed: count=%d", count)
}

func TestSelect_TopNRangeWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedSimpleAuthorsBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getTopFive(), getFromClause(), getRangeWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 5 {
			t.Errorf("Expected count 5, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNRangeWhereGroupBy passed: count=%d", count)
}

// func TestSelect_CountRangeWhereGroupBy(t *testing.T) {
// 	fixture := setupRealServer(t)
// 	//resetDatabase(t, fixture)
// 	seedSimpleAuthorsBundle(t, fixture, 100)

// 	query := fmt.Sprintf(`SELECT %s %s %s %s;`, getCount(), getFromClause(), getRangeWhereClause(), getGroupByClause())
// 	response := executeRealQuery(t, fixture, query)
// 	count := 0
// 	if cmdResp, ok := response.(*server.CommandResponse); ok {

// 		if cmdResp.ResultCount != 31 {
// 			t.Errorf("Expected count 31 (WHERE BirthYear 1970-2000, GROUP BY Name), got %d", cmdResp.ResultCount)
// 		}
// 		count = cmdResp.ResultCount
// 		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
// 			t.Logf("Number of results: %d", len(results))
// 			if len(results) > 0 {
// 				t.Logf("First result: %+v", results[0])
// 			}
// 		}
// 	}

// 	t.Logf("✓ TestSelect_CountRangeWhereGroupBy passed: count=%d", count)
// }

func TestSelect_AllFieldsJoinBasicWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getAllFields(), getFromClause(), getJoinClause(), getBascWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected count 1 (JOIN + WHERE Name='Strohschein' GROUP BY Name), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsJoinBasicWhereGroupBy passed: count=%d", count)
}

func TestSelect_FieldListJoinBasicWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getFieldList(), getFromClause(), getJoinClause(), getBascWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected count 1 (JOIN + WHERE Name='Strohschein' GROUP BY Name), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListJoinBasicWhereGroupBy passed: count=%d", count)
}

func TestSelect_TopNJoinBasicWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getTopFive(), getFromClause(), getJoinClause(), getBascWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 1 {
			t.Errorf("Expected count 1 (TOP 5, but only 1 group after JOIN + WHERE), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNJoinBasicWhereGroupBy passed: count=%d", count)
}

// func TestSelect_CountJoinBasicWhereGroupBy(t *testing.T) {
// 	fixture := setupRealServer(t)
// 	//resetDatabase(t, fixture)
// 	seedAuthorsBundle(t, fixture, 100)
// 	seedBooksBundle(t, fixture, 100)

// 	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getCount(), getFromClause(), getJoinClause(), getBascWhereClause(), getGroupByClause())
// 	response := executeRealQuery(t, fixture, query)
// 	count := 0
// 	if cmdResp, ok := response.(*server.CommandResponse); ok {

// 		if cmdResp.ResultCount != 1 {
// 			t.Errorf("Expected count 1 (JOIN + WHERE Name='Strohschein' GROUP BY Name), got %d", cmdResp.ResultCount)
// 		}
// 		count = cmdResp.ResultCount
// 		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
// 			t.Logf("Number of results: %d", len(results))
// 			if len(results) > 0 {
// 				t.Logf("First result: %+v", results[0])
// 			}
// 		}
// 	}

// 	t.Logf("✓ TestSelect_CountJoinBasicWhereGroupBy passed: count=%d", count)
// }

func TestSelect_AllFieldsJoinRangeWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getAllFields(), getFromClause(), getJoinClause(), getRangeWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (JOIN + WHERE BirthYear 1970-2000, GROUP BY Name), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_AllFieldsJoinRangeWhereGroupBy passed: count=%d", count)
}

func TestSelect_FieldListJoinRangeWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getFieldList(), getFromClause(), getJoinClause(), getRangeWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 31 {
			t.Errorf("Expected count 31 (JOIN + WHERE BirthYear 1970-2000, GROUP BY Name), got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_FieldListJoinRangeWhereGroupBy passed: count=%d", count)
}

func TestSelect_TopNJoinRangeWhereGroupBy(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getTopFive(), getFromClause(), getJoinClause(), getRangeWhereClause(), getGroupByClause())
	response := executeRealQuery(t, fixture, query)
	count := 0
	if cmdResp, ok := response.(*server.CommandResponse); ok {

		if cmdResp.ResultCount != 5 {
			t.Errorf("Expected count 5, got %d", cmdResp.ResultCount)
		}
		count = cmdResp.ResultCount
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}

	t.Logf("✓ TestSelect_TopNJoinRangeWhereGroupBy passed: count=%d", count)
}

// func TestSelect_CountJoinRangeWhereGroupBy(t *testing.T) {
// 	fixture := setupRealServer(t)
// 	//resetDatabase(t, fixture)
// 	seedAuthorsBundle(t, fixture, 100)
// 	seedBooksBundle(t, fixture, 100)

// 	query := fmt.Sprintf(`SELECT %s %s %s %s %s;`, getCount(), getFromClause(), getJoinClause(), getRangeWhereClause(), getGroupByClause())
// 	response := executeRealQuery(t, fixture, query)
// 	count := 0
// 	if cmdResp, ok := response.(*server.CommandResponse); ok {

// 		if cmdResp.ResultCount != 31 {
// 			t.Errorf("Expected count 31 (JOIN + WHERE BirthYear 1970-2000, GROUP BY Name), got %d", cmdResp.ResultCount)
// 		}
// 		count = cmdResp.ResultCount
// 		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
// 			t.Logf("Number of results: %d", len(results))
// 			if len(results) > 0 {
// 				t.Logf("First result: %+v", results[0])
// 			}
// 		}
// 	}

// 	t.Logf("✓ TestSelect_CountJoinRangeWhereGroupBy passed: count=%d", count)
// }
