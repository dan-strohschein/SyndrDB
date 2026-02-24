package syndrQL

import (
	"context"
	"fmt"
	"syndrdb/src/internal/server"
	"testing"
	"time"
)

// setupSubqueryBundles creates test bundles for subquery tests
func setupSubqueryBundles(tb testing.TB, fixture *TestFixture) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bundles := []string{"Authors", "Books", "Publishers"}

	// Drop existing bundles
	for _, bundleName := range bundles {
		dropCmd := fmt.Sprintf(`DROP BUNDLE "%s" WITH FORCE`, bundleName)
		startTime := time.Now()
		_, _ = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	}

	// Create Authors bundle
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", false, false},
		{"BirthYear", "INT", false, false},
		{"Active", "BOOL", false, false}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create Authors bundle: %v", err)
	}

	// Create Books bundle
	createBooksCmd := `CREATE BUNDLE "Books" WITH FIELDS (
		{"ID", "INT", true, true},
		{"Title", "STRING", true, false},
		{"AuthorID", "INT", false, false},
		{"PublisherID", "INT", false, false},
		{"Genre", "STRING", false, false},
		{"Price", "FLOAT", false, false},
		{"PublishedYear", "INT", false, false}
	)`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createBooksCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create Books bundle: %v", err)
	}

	// Create Publishers bundle
	createPublishersCmd := `CREATE BUNDLE "Publishers" WITH FIELDS (
		{"ID", "INT", true, true},
		{"Name", "STRING", true, false},
		{"Country", "STRING", false, false},
		{"EstablishedYear", "INT", false, false}
	)`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createPublishersCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create Publishers bundle: %v", err)
	}

	// Create relationship
	createRelationshipCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP ("AuthorsBooks" {"1toMany", "Authors", "DocumentID", "Books", "AuthorsID"});`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Failed to create relationship: %v", err)
	}

	fixture.Logger.Infof("Subquery test bundles created")
}

// seedAuthorsForSubquery seeds authors with varied data
func seedAuthorsForSubquery(tb testing.TB, fixture *TestFixture, count int) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	countries := []string{"USA", "UK", "Canada", "France", "Germany", "Japan"}

	for i := 1; i <= count; i++ {
		var name string
		var active bool

		if i <= 10 {
			name = fmt.Sprintf("Famous_Author_%03d", i)
			active = true
		} else if i > count-30 {
			name = "Strohschein"
			active = true
		} else {
			name = fmt.Sprintf("Author_%03d", i)
			active = (i%3 != 0)
		}

		country := countries[(i-1)%len(countries)]
		birthYear := 1920 + (i % 80)

		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH ({"ID"=%d}, {"Name"="%s"}, {"Country"="%s"}, {"BirthYear"=%d}, {"Active"=%t});`,
			i, name, country, birthYear, active)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			tb.Fatalf("Failed to seed author %d: %v", i, err)
		}
	}

	tb.Logf("Seeded %d authors", count)
}

// seedBooksForSubquery seeds books with distribution
func seedBooksForSubquery(tb testing.TB, fixture *TestFixture, count int) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	genres := []string{"Fiction", "NonFiction", "Mystery", "SciFi", "Fantasy", "Biography"}
	publisherIDs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for i := 1; i <= count; i++ {
		title := fmt.Sprintf("Book_%03d", i)

		var authorID int
		if i%2 == 0 {
			authorID = 71 + ((i/2 - 1) % 30)
		} else if i%5 == 1 {
			authorID = 1 + ((i / 5) % 10)
		} else {
			authorID = 11 + ((i / 3) % 60)
		}

		genre := genres[(i-1)%len(genres)]
		publisherID := publisherIDs[(i-1)%len(publisherIDs)]
		price := 9.99 + float64(i%50)
		publishedYear := 1980 + (i % 44)

		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Books" WITH ({"ID"=%d}, {"Title"="%s"}, {"AuthorID"=%d}, {"AuthorsID"=%d}, {"PublisherID"=%d}, {"Genre"="%s"}, {"Price"=%.2f}, {"PublishedYear"=%d});`,
			i, title, authorID, authorID, publisherID, genre, price, publishedYear)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			tb.Fatalf("Failed to seed book %d: %v", i, err)
		}
	}

	tb.Logf("Seeded %d books", count)
}

// seedPublishersForSubquery seeds publishers
func seedPublishersForSubquery(tb testing.TB, fixture *TestFixture, count int) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	countries := []string{"USA", "UK", "Canada", "France", "Germany"}

	for i := 1; i <= count; i++ {
		name := fmt.Sprintf("Publisher_%03d", i)
		country := countries[(i-1)%len(countries)]
		establishedYear := 1900 + (i * 5)

		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Publishers" WITH ({"ID"=%d}, {"Name"="%s"}, {"Country"="%s"}, {"EstablishedYear"=%d});`,
			i, name, country, establishedYear)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			tb.Fatalf("Failed to seed publisher %d: %v", i, err)
		}
	}

	tb.Logf("Seeded %d publishers", count)
}

// executeSubqueryTest executes a query and returns response
func executeSubqueryTest(tb testing.TB, fixture *TestFixture, query string) interface{} {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		tb.Fatalf("Query failed: %v\nQuery: %s", err, query)
	}

	return response
}

// validateSubqueryResultCount validates result count
func validateSubqueryResultCount(tb testing.TB, response interface{}, expectedCount int, testName string) {
	tb.Helper()

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != expectedCount {
			tb.Errorf("%s: Expected %d results, got %d", testName, expectedCount, cmdResp.ResultCount)
		}
		tb.Logf("PASS %s: %d results", testName, cmdResp.ResultCount)
	} else {
		tb.Errorf("%s: Invalid response type: %T", testName, response)
	}
}

// Test: IN subquery with small result set
func TestSubquery_IN_SmallResultSet(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Authors" WHERE "ID" IN (SELECT "AuthorID" FROM "Books" WHERE "Genre" == "Mystery");`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount == 0 {
			t.Errorf("Expected some results, got 0")
		}
		t.Logf("PASS TestSubquery_IN_SmallResultSet: %d authors have Mystery books", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: IN subquery with medium result set
func TestSubquery_IN_MediumResultSet(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Books" WHERE "AuthorID" IN (SELECT "ID" FROM "Authors" WHERE "Country" == "USA");`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount == 0 {
			t.Errorf("Expected some results, got 0")
		}
		t.Logf("PASS TestSubquery_IN_MediumResultSet: %d books by USA authors", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: EXISTS subquery
func TestSubquery_EXISTS_Basic(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Authors" WHERE EXISTS (SELECT "ID" FROM "Books" WHERE "Books"."AuthorID" == "Authors"."ID");`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount == 0 {
			t.Errorf("Expected some results, got 0")
		}
		if cmdResp.ResultCount > 100 {
			t.Errorf("Expected less than 100 authors, got %d", cmdResp.ResultCount)
		}
		t.Logf("PASS TestSubquery_EXISTS_Basic: %d authors have published books", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: NOT IN subquery
func TestSubquery_NOT_IN_Basic(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Authors" WHERE "ID" NOT IN (SELECT "AuthorID" FROM "Books" WHERE "Genre" == "Mystery");`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount == 0 {
			t.Errorf("Expected some results, got 0")
		}
		if cmdResp.ResultCount >= 100 {
			t.Errorf("Expected less than 100 authors, got %d", cmdResp.ResultCount)
		}
		t.Logf("PASS TestSubquery_NOT_IN_Basic: %d authors have NOT written Mystery books", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: NOT EXISTS subquery
func TestSubquery_NOT_EXISTS_Basic(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Authors" WHERE NOT EXISTS (SELECT "ID" FROM "Books" WHERE "Books"."AuthorID" == "Authors"."ID");`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount > 50 {
			t.Errorf("Expected less than 50 authors without books, got %d", cmdResp.ResultCount)
		}
		t.Logf("PASS TestSubquery_NOT_EXISTS_Basic: %d authors have NOT published books", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: Nested subqueries
func TestSubquery_Nested_TwoLevels(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Books" WHERE "AuthorID" IN (SELECT "ID" FROM "Authors" WHERE "ID" IN (SELECT "AuthorID" FROM "Books" WHERE "Genre" == "Mystery"));`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount == 0 {
			t.Errorf("Expected some results, got 0")
		}
		t.Logf("PASS TestSubquery_Nested_TwoLevels: %d books by authors who wrote Mystery", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: IN with outer WHERE clause
func TestSubquery_IN_WithOuterWHERE(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Authors" WHERE "Active" == true AND "ID" IN (SELECT "AuthorID" FROM "Books" WHERE "Genre" == "Fiction" AND "PublishedYear" >= 2000);`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		t.Logf("PASS TestSubquery_IN_WithOuterWHERE: %d active authors with Fiction after 2000", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: EXISTS with complex subquery
func TestSubquery_EXISTS_ComplexSubquery(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)
	seedPublishersForSubquery(t, fixture, 20)

	query := `SELECT * FROM "Authors" WHERE EXISTS (SELECT "ID" FROM "Books" WHERE "Books"."AuthorID" == "Authors"."ID" AND "Books"."PublishedYear" >= 2000);`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		t.Logf("PASS TestSubquery_EXISTS_ComplexSubquery: %d authors with recent books", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: Multiple IN clauses
func TestSubquery_Multiple_IN_Clauses(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Authors" WHERE "ID" IN (SELECT "AuthorID" FROM "Books" WHERE "Genre" == "Mystery") AND "ID" IN (SELECT "AuthorID" FROM "Books" WHERE "Genre" == "Fiction");`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		t.Logf("PASS TestSubquery_Multiple_IN_Clauses: %d authors with both Mystery and Fiction", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: IN with ORDER BY
func TestSubquery_IN_WithORDERBY(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Authors" WHERE "ID" IN (SELECT "AuthorID" FROM "Books" WHERE "Genre" == "Mystery") ORDER BY "Name" ASC;`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount == 0 {
			t.Errorf("Expected some results, got 0")
		}
		t.Logf("PASS TestSubquery_IN_WithORDERBY: %d authors sorted by name", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}

// Test: IN with LIMIT
func TestSubquery_IN_WithLIMIT(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT TOP 5 * FROM "Authors" WHERE "ID" IN (SELECT "AuthorID" FROM "Books" WHERE "Genre" == "Mystery") ORDER BY "Name" ASC;`

	response := executeSubqueryTest(t, fixture, query)

	validateSubqueryResultCount(t, response, 5, "TestSubquery_IN_WithLIMIT")
}

// Test: Empty subquery result
func TestSubquery_IN_EmptyResult(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 150)

	query := `SELECT * FROM "Authors" WHERE "ID" IN (SELECT "AuthorID" FROM "Books" WHERE "Genre" == "NonExistentGenre");`

	response := executeSubqueryTest(t, fixture, query)

	validateSubqueryResultCount(t, response, 0, "TestSubquery_IN_EmptyResult")
}

// Test: Large result set performance
func TestSubquery_Performance_LargeResultSet(t *testing.T) {
	fixture := setupFullServerTB(t)
	setupSubqueryBundles(t, fixture)
	seedAuthorsForSubquery(t, fixture, 100)
	seedBooksForSubquery(t, fixture, 500)

	query := `SELECT * FROM "Books" WHERE "AuthorID" IN (SELECT "ID" FROM "Authors");`

	response := executeSubqueryTest(t, fixture, query)

	if cmdResp, ok := response.(*server.CommandResponse); ok {
		if cmdResp.ResultCount != 500 {
			t.Errorf("Expected 500 books, got %d", cmdResp.ResultCount)
		}
		t.Logf("PASS TestSubquery_Performance_LargeResultSet: %d books processed", cmdResp.ResultCount)
	} else {
		t.Errorf("Invalid response type: %T", response)
	}
}
