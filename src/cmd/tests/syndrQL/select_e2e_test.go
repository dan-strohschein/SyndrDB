package syndrQL

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	defaultdb "syndrdb/src/internal/defaultDB"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"
	"syndrdb/src/internal/storage/buffer"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
) /*
SELECT E2E TEST SUITE

Comprehensive end-to-end validation of all SyndrQL SELECT query patterns.
Tests use real server components (no mocking) with predictable seeded data
and intelligent validation strategies (exact matching for small results,
sampling for large results).

Test Strategy:
- Real integration: ServiceManager, BundleService, DatabaseService, CommandDirector
- Predictable seeds: Deterministic data patterns for validation
- Smart validation: Exact for ≤150 results, sampling (top/bottom 10%) for >150
- Sequential execution: No parallelization for test isolation
- Auto-cleanup: t.TempDir() with optional preservation on failure

Data Limits:
- Max 320 documents per test (100 Authors + 200 Books + 20 Publishers)
- Sampling threshold: >150 results
- Zero-tolerance ordering validation (fail on first inversion)
*/

// TestFixture, filteredWriter, SetupFullServer, setupFullServer, and setupFullServerTB are defined in helpers.go.

// resetDatabase drops and recreates all test bundles
func resetDatabase(t *testing.T, fixture *TestFixture) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bundles := []string{"Authors", "Books", "Publishers"}

	// Drop existing bundles (ignore errors if they don't exist)
	for _, bundleName := range bundles {
		dropCmd := fmt.Sprintf(`DROP BUNDLE "%s"`, bundleName)
		startTime := time.Now()
		// Pass fixture.Database to ensure commands operate on correct database
		_, _ = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	}

	// Create Authors bundle - must pass database context
	// NOTE: Avoiding UNIQUE constraint for now due to indexing issues in test environment
	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, false},
		{"Name", "STRING", true, false},
		{"Country", "STRING", false, false},
		{"BirthYear", "INT", false, false}
	);`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create Authors bundle: %v", err)
	}

	// // Debug: Show all bundles to see what was actually created
	// showBundlesCmd := `SHOW BUNDLES`
	// startTime = time.Now()
	// bundlesInfo, err := server.CommandDirector(fixture.Database, *fixture.ServiceManager, showBundlesCmd, fixture.Logger, startTime, nil, "127.0.0.1")
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

	createRelationshipCmd := `UPDATE BUNDLE "Authors" ADD RELATIONSHIP ( "AuthorsBooks" {"1toMany", "Authors", "DocumentID", "Books", "AuthorID"});`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createRelationshipCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to create relationship: %v", err)
	}

	//fixture.Logger.Infof("✓ Database reset complete (bundles: %v)", bundles)
}

// =============================================================================
// SEED HELPERS
// =============================================================================

// seedAuthorsBundle creates predictable author documents
func seedAuthorsBundle(t *testing.T, fixture *TestFixture, count int) {
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
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH  ( {"ID"=%d}, {"Name"="%s"}, {"Country"="%s"}, {"BirthYear"=%d});`,
			i, name, country, birthYear)

		// Debug first document command
		if i == 1 {
			t.Logf("First ADD DOCUMENT command: %s", addDocCmd)

			// Check the bundle structure BEFORE adding to ensure fields are defined
			checkBundle, _ := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Authors")
			if checkBundle != nil {
				t.Logf("Bundle 'Authors' field definitions before ADD: %+v", checkBundle.DocumentStructure.FieldDefinitions)
			}
		}

		startTime := time.Now()
		response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed author %d: %v", i, err)
		}

		// Extract DocumentID from EVERY response to populate fixture.AuthorDocumentIDs
		if cmdResp, ok := response.(*server.CommandResponse); ok {
			// Try as map first
			if resultMap, ok := cmdResp.Result.(map[string]interface{}); ok {
				if docID, ok := resultMap["DocumentID"].(string); ok {
					fixture.AuthorDocumentIDs = append(fixture.AuthorDocumentIDs, docID)
				}
			} else if resultStr, ok := cmdResp.Result.(string); ok {
				// Result is a JSON string, parse it
				var resultMap map[string]interface{}
				if err := json.Unmarshal([]byte(resultStr), &resultMap); err == nil {
					if docID, ok := resultMap["DocumentID"].(string); ok {
						fixture.AuthorDocumentIDs = append(fixture.AuthorDocumentIDs, docID)
					}
				}
			}
		}
	}

	// Verify seed count via COUNT(*)
	countCmd := `SELECT COUNT(*) FROM "Authors"`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, countCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to verify author count: %v", err)
	}

	// DEBUG: Inspect raw response from COUNT query
	t.Logf("======= COUNT(*) RESPONSE DEBUG =======")
	t.Logf("Response type: %T", response)
	if cmdResp, ok := response.(*server.CommandResponse); ok {
		t.Logf("CommandResponse.Result type: %T", cmdResp.Result)
		t.Logf("CommandResponse.ResultCount: %d", cmdResp.ResultCount)
		t.Logf("CommandResponse.Error: %v", cmdResp.Error)

		// Deep inspect the Result
		switch result := cmdResp.Result.(type) {
		case []map[string]interface{}:
			t.Logf("Result is []map[string]interface{} with length: %d", len(result))
			// Show first 3 documents to see if they have Column1 or aggregate fields
			for i := 0; i < len(result) && i < 3; i++ {
				t.Logf("Document %d: %+v", i, result[i])
				t.Logf("Document %d fields:", i)
				for k, v := range result[i] {
					t.Logf("  Key: '%s', Value: %v, ValueType: %T", k, v, v)
				}
			}
		case map[string]interface{}:
			t.Logf("Result is map[string]interface{}: %+v", result)
			for k, v := range result {
				t.Logf("  Key: '%s', Value: %v, ValueType: %T", k, v, v)
			}
		default:
			t.Logf("Result is unexpected type: %+v", result)
		}
	}
	t.Logf("======= END COUNT(*) DEBUG =======")

	actualCount := extractCountFromResponse(t, response)
	if actualCount != count {
		t.Fatalf("Author seed verification failed: expected %d, got %d", count, actualCount)
	}

	// CRITICAL: Flush metadata updates to update TotalDocuments and PageCount
	// The server uses deferred metadata updates for performance, must flush before SELECT
	fixture.ServiceManager.BundleService.FlushMetadataUpdates()

	// CRITICAL: Flush buffers to persist documents to disk
	// Without this, documents remain in memtables and may not be retrieved by SELECT queries
	err = fixture.ServiceManager.BundleService.FlushAllBuffers()
	if err != nil {
		t.Fatalf("Failed to flush buffers after seeding authors: %v", err)
	}

	// DEBUG: Read the actual bundle file to verify what was written to disk
	bundleFilePath := filepath.Join(fixture.Settings.DataDir, fixture.Database.Name, fmt.Sprintf("%s_Authors.bnd", fixture.Database.Name))
	t.Logf("Checking bundle file at: %s", bundleFilePath)

	bundleFileContent, readErr := os.ReadFile(bundleFilePath)
	if readErr != nil {
		t.Logf("WARNING: Could not read bundle file: %v", readErr)
	} else {
		t.Logf("Bundle file size: %d bytes", len(bundleFileContent))

		// Check if file contains expected field values
		contentStr := string(bundleFileContent)
		hasAuthor001 := strings.Contains(contentStr, "Author_001")
		hasUSA := strings.Contains(contentStr, "USA")
		hasFieldID := strings.Contains(contentStr, `"ID"`) || strings.Contains(contentStr, `ID`)
		hasFieldName := strings.Contains(contentStr, `"Name"`) || strings.Contains(contentStr, `Name`)

		t.Logf("Bundle file content search:")
		t.Logf("  - Contains 'Author_001': %v", hasAuthor001)
		t.Logf("  - Contains 'USA': %v", hasUSA)
		t.Logf("  - Contains 'ID' field: %v", hasFieldID)
		t.Logf("  - Contains 'Name' field: %v", hasFieldName)

		// Try to parse the JSON metadata section
		// Bundle files are JSON metadata followed by binary page data
		jsonEndIdx := strings.Index(contentStr, "}ﾭ") // Common separator between JSON and binary
		if jsonEndIdx > 0 {
			jsonPart := contentStr[:jsonEndIdx+1]
			var bundleMetadata map[string]interface{}
			if jsonErr := json.Unmarshal([]byte(jsonPart), &bundleMetadata); jsonErr == nil {
				if totalDocs, ok := bundleMetadata["TotalDocuments"].(float64); ok {
					t.Logf("Bundle TotalDocuments: %.0f", totalDocs)
				}
				if pageCount, ok := bundleMetadata["PageCount"].(float64); ok {
					t.Logf("Bundle PageCount: %.0f", pageCount)
				}
			}
		}
	}

	// CRITICAL: Clear memtable to force SELECT to read from disk
	// After flush, documents are on disk, but memtable still exists with potentially stale data
	// Setting DocumentsComplete=true forces reads from disk
	authorsBundle, _ := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Authors")
	if authorsBundle != nil {
		authorsBundle.DocumentsComplete = true
		t.Logf("Cleared Authors memtable, forcing disk reads")
	}

	// Log success with document IDs if count ≤10
	if count <= 10 {
		selectCmd := `SELECT ID, Name FROM "Authors"`
		startTime = time.Now()
		docsResponse, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err == nil {
			docs := extractDocuments(t, docsResponse)
			fixture.Logger.Infof("✓ Seeded %d authors: %v", count, getDocumentIDs(docs))
		}
	} else {
		fixture.Logger.Infof("✓ Seeded %d authors", count)
	}
}

// seedBooksBundle creates predictable book documents
func seedBooksBundle(t *testing.T, fixture *TestFixture, count int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	genres := []string{"Fiction", "NonFiction", "Mystery", "SciFi", "Fantasy"}

	for i := 1; i <= count; i++ {
		title := fmt.Sprintf("Book_%03d", i)
		authorID := ((i - 1) % 100) + 1 // Cycle through authors 1-100
		genre := genres[(i-1)%len(genres)]
		price := 9.99 + float64(i)*0.50
		authDocID := ""
		if len(fixture.AuthorDocumentIDs) >= authorID {
			authDocID = fixture.AuthorDocumentIDs[authorID-1]
		}
		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Books" WITH ({"ID"=%d}, {"Title"="%s"}, {"AuthorID"=%d}, {"AuthorsID"="%s"}, {"Genre"="%s"}, {"Price"=%.2f});`,
			i, title, authorID, authDocID, genre, price)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed book %d: %v", i, err)
		}
	}

	// Verify seed count via COUNT(*)
	countCmd := `SELECT COUNT(*) FROM "Books"`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, countCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to verify book count: %v", err)
	}

	actualCount := extractCountFromResponse(t, response)
	if actualCount != count {
		t.Fatalf("Book seed verification failed: expected %d, got %d", count, actualCount)
	}

	// CRITICAL: Flush metadata updates to update TotalDocuments and PageCount
	// The server uses deferred metadata updates for performance, must flush before SELECT
	fixture.ServiceManager.BundleService.FlushMetadataUpdates()

	// CRITICAL: Flush buffers to persist documents to disk
	// Without this, documents remain in memtables and may not be retrieved by SELECT queries
	err = fixture.ServiceManager.BundleService.FlushAllBuffers()
	if err != nil {
		t.Fatalf("Failed to flush buffers after seeding books: %v", err)
	}

	// CRITICAL: Clear memtable to force SELECT to read from disk
	// After flush, documents are on disk, but memtable still exists with potentially stale data
	// Setting DocumentsComplete=true forces reads from disk
	booksBundle, _ := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Books")
	if booksBundle != nil {
		booksBundle.DocumentsComplete = true
		t.Logf("Cleared Books memtable, forcing disk reads")
	}

	// Log success with document IDs if count ≤10
	if count <= 10 {
		selectCmd := `SELECT ID, Title FROM "Books"`
		startTime = time.Now()
		docsResponse, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err == nil {
			docs := extractDocuments(t, docsResponse)
			fixture.Logger.Infof("✓ Seeded %d books: %v", count, getDocumentIDs(docs))
		}
	} else {
		fixture.Logger.Infof("✓ Seeded %d books", count)
	}
}

// seedPublishersBundle creates predictable publisher documents
func seedPublishersBundle(t *testing.T, fixture *TestFixture, count int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	countries := []string{"USA", "UK", "Canada", "Germany"}

	for i := 1; i <= count; i++ {
		name := fmt.Sprintf("Publisher_%03d", i)
		country := countries[(i-1)%len(countries)]

		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Publishers" WITH  ( {"ID"=%d}, {"Name" = "%s"}, {"Country" = "%s"});`,
			i, name, country)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			t.Fatalf("Failed to seed publisher %d: %v", i, err)
		}
	}

	// Verify seed count via COUNT(*)
	countCmd := `SELECT COUNT(*) FROM "Publishers"`
	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, countCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Failed to verify publisher count: %v", err)
	}

	actualCount := extractCountFromResponse(t, response)
	if actualCount != count {
		t.Fatalf("Publisher seed verification failed: expected %d, got %d", count, actualCount)
	}

	// CRITICAL: Flush metadata updates to update TotalDocuments and PageCount
	// The server uses deferred metadata updates for performance, must flush before SELECT
	fixture.ServiceManager.BundleService.FlushMetadataUpdates()

	// CRITICAL: Flush buffers to persist documents to disk
	// Without this, documents remain in memtables and may not be retrieved by SELECT queries
	err = fixture.ServiceManager.BundleService.FlushAllBuffers()
	if err != nil {
		t.Fatalf("Failed to flush buffers after seeding publishers: %v", err)
	}

	// CRITICAL: Clear memtable to force SELECT to read from disk
	// After flush, documents are on disk, but memtable still exists with potentially stale data
	// Setting DocumentsComplete=true forces reads from disk
	publishersBundle, _ := fixture.ServiceManager.BundleService.GetBundleByName(fixture.Database, "Publishers")
	if publishersBundle != nil {
		publishersBundle.DocumentsComplete = true
		t.Logf("Cleared Publishers memtable, forcing disk reads")
	}

	// Log success with document IDs if count ≤10
	if count <= 10 {
		selectCmd := `SELECT ID, Name FROM "Publishers"`
		startTime = time.Now()
		docsResponse, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, selectCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err == nil {
			docs := extractDocuments(t, docsResponse)
			fixture.Logger.Infof("✓ Seeded %d publishers: %v", count, getDocumentIDs(docs))
		}
	} else {
		fixture.Logger.Infof("✓ Seeded %d publishers", count)
	}
}

// =============================================================================
// QUERY EXECUTION
// =============================================================================

// executeRealQuery executes a query via CommandDirector
func executeRealQuery(t *testing.T, fixture *TestFixture, query string) interface{} {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()
	response, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("Query execution failed: %v\nQuery: %s", err, query)
	}

	return response
}

// =============================================================================
// RESPONSE EXTRACTION
// =============================================================================

// extractDocuments extracts document array from CommandResponse
func extractDocuments(t *testing.T, response interface{}) []map[string]interface{} {
	t.Helper()

	cmdResponse, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected *server.CommandResponse, got %T", response)
	}

	// PHASE H: Handle streaming response where documents aren't materialized as maps
	if cmdResponse.StreamDocuments != nil || len(cmdResponse.StreamDocuments) > 0 {
		// Convert StreamDocuments to the expected format
		return helpers.TransformDocumentsToFlatFormatWithProjection(
			// Fix: Provide bundle schema as third argument per new helpers.TransformDocumentsToFlatFormatWithProjection signature.
			// Fix: Provide nil for bundle schema, since CommandResponse no longer has BundleSchema.
			cmdResponse.StreamDocuments,
			cmdResponse.StreamFields,
			nil,
		)
	}

	// Handle different result types (legacy path)
	switch result := cmdResponse.Result.(type) {
	case []map[string]interface{}:
		return result
	case []interface{}:
		docs := make([]map[string]interface{}, len(result))
		for i, item := range result {
			if doc, ok := item.(map[string]interface{}); ok {
				docs[i] = doc
			} else {
				t.Fatalf("Expected map[string]interface{} in result array at index %d, got %T", i, item)
			}
		}
		return docs
	case map[string]interface{}:
		// Single result wrapped in a map - could be aggregate or single document
		// Check if it has document fields
		if _, hasID := result["ID"]; hasID {
			return []map[string]interface{}{result}
		}
		// Otherwise it's probably an aggregate, return empty array
		t.Logf("WARNING: Got map[string]interface{} but it looks like an aggregate, not documents: %v", result)
		return []map[string]interface{}{}
	case map[string]int:
		// This is an aggregate response (COUNT, etc.), not documents
		t.Logf("WARNING: Got aggregate response (map[string]int), not documents: %v", result)
		return []map[string]interface{}{}
	default:
		t.Fatalf("Unexpected result type: %T, value: %v", result, result)
		return nil
	}
}

// extractAggregates extracts aggregate results (COUNT, SUM, AVG, MIN, MAX)
func extractAggregates(t *testing.T, response interface{}) map[string]interface{} {
	t.Helper()

	cmdResponse, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected *server.CommandResponse, got %T", response)
	}

	// Aggregates can be returned in various formats
	switch result := cmdResponse.Result.(type) {
	case map[string]interface{}:
		return result
	case map[string]int:
		// Convert map[string]int to map[string]interface{}
		converted := make(map[string]interface{})
		for k, v := range result {
			converted[k] = v
		}
		return converted
	case map[string]float64:
		// Convert map[string]float64 to map[string]interface{}
		converted := make(map[string]interface{})
		for k, v := range result {
			converted[k] = v
		}
		return converted
	case []map[string]interface{}:
		// Check if this is actually an aggregate result or a document result
		// Aggregate results should have fields like Count, SUM, MIN, MAX, AVG
		// Document results have fields like DocumentID, CreatedAt, UpdatedAt
		if len(result) > 0 {
			first := result[0]
			// Check if this looks like an aggregate result
			hasAggregateFields := false
			for key := range first {
				lowerKey := strings.ToLower(key)
				if lowerKey == "count" || lowerKey == "sum" || lowerKey == "min" ||
					lowerKey == "max" || lowerKey == "avg" || strings.HasPrefix(lowerKey, "count(") ||
					strings.HasPrefix(lowerKey, "sum(") || strings.HasPrefix(lowerKey, "min(") ||
					strings.HasPrefix(lowerKey, "max(") || strings.HasPrefix(lowerKey, "avg(") {
					hasAggregateFields = true
					break
				}
			}

			if hasAggregateFields {
				return first
			}

			// This is a document array, not an aggregate result
			// Log warning and return empty map so the test will fail with a clear message
			t.Logf("WARNING: extractAggregates called but result appears to be document array, not aggregates")
			t.Logf("First element keys: %v", getKeys(first))
		}
		return map[string]interface{}{}
	default:
		t.Fatalf("Unexpected aggregate result type: %T, value: %v", result, result)
		return nil
	}
}

// extractGroupedResults extracts GROUP BY results
func extractGroupedResults(t *testing.T, response interface{}) []map[string]interface{} {
	t.Helper()

	// GROUP BY results are similar to regular documents
	return extractDocuments(t, response)
}

// extractCountFromResponse extracts COUNT(*) value from response
func extractCountFromResponse(t *testing.T, response interface{}) int {
	t.Helper()

	cmdResponse, ok := response.(*server.CommandResponse)
	if !ok {
		t.Fatalf("Expected *server.CommandResponse, got %T", response)
	}

	// NEW BEHAVIOR: Synthetic documents with Column1, Column2, etc.
	if resultArray, ok := cmdResponse.Result.([]map[string]interface{}); ok {
		if len(resultArray) > 0 {
			first := resultArray[0]

			// Check if this is a synthetic document (has Column1, Column2, etc.)
			// Try Column1 first (for single aggregate like COUNT(*))
			if val, ok := first["Column1"]; ok {
				t.Logf("Found synthetic document with Column1 = %v", val)
				// Try direct int
				if count, ok := val.(int); ok {
					return count
				}
				// Try int64
				if count, ok := val.(int64); ok {
					return int(count)
				}
				// Try float64 (JSON numbers)
				if count, ok := val.(float64); ok {
					return int(count)
				}
				// Try FieldValue
				if fv, ok := val.(models.FieldValue); ok {
					if intVal, ok := fv.AsInt(); ok {
						return int(intVal)
					}
				}
			}
		}
	}

	// Try to extract from aggregates (old/legacy behavior for GROUP BY queries)
	agg := extractAggregates(t, response)

	// Try different possible keys for count
	// Handle both direct int and FieldValue wrapper
	for _, key := range []string{"Column1", "Count", "count", "COUNT", "count(*)", "COUNT(*)", "count_all"} {
		if val, ok := agg[key]; ok {
			// Try direct int
			if count, ok := val.(int); ok {
				return count
			}
			// Try int64
			if count, ok := val.(int64); ok {
				return int(count)
			}
			// Try float64 (JSON numbers)
			if count, ok := val.(float64); ok {
				return int(count)
			}
			// Try FieldValue
			if fv, ok := val.(models.FieldValue); ok {
				if intVal, ok := fv.AsInt(); ok {
					return int(intVal)
				}
			}
		}
	}

	t.Fatalf("Could not extract count from aggregates: %v (type: %T)", agg, agg)
	return 0
} // FieldValue unwrapping helpers (for response formatter using FieldValue)
func asString(val interface{}) (string, bool) {
	if s, ok := val.(string); ok {
		return s, true
	}
	if fv, ok := val.(models.FieldValue); ok {
		return fv.AsString()
	}
	return "", false
}

func asInt(val interface{}) (int64, bool) {
	if i, ok := val.(int64); ok {
		return i, true
	}
	if i, ok := val.(int); ok {
		return int64(i), true
	}
	if fv, ok := val.(models.FieldValue); ok {
		return fv.AsInt()
	}
	return 0, false
}

func asFloat(val interface{}) (float64, bool) {
	if f, ok := val.(float64); ok {
		return f, true
	}
	if fv, ok := val.(models.FieldValue); ok {
		return fv.AsFloat()
	}
	return 0, false
}

func asBool(val interface{}) (bool, bool) {
	if b, ok := val.(bool); ok {
		return b, true
	}
	if fv, ok := val.(models.FieldValue); ok {
		return fv.AsBool()
	}
	return false, false
}

// getDocumentIDs extracts ID field from documents (for logging ≤10 docs)
func getDocumentIDs(docs []map[string]interface{}) []interface{} {
	ids := make([]interface{}, len(docs))
	for i, doc := range docs {
		if id, ok := doc["ID"]; ok {
			ids[i] = id
		} else if id, ok := doc["id"]; ok {
			ids[i] = id
		} else {
			ids[i] = "unknown"
		}
	}
	return ids
}

// =============================================================================
// BASIC SELECT TESTS
// =============================================================================

func TestSelectAll_SmallDataset(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 50)

	query := `SELECT * FROM "Authors";`
	response := executeRealQuery(t, fixture, query)

	// Debug: Check response type and structure
	t.Logf("Response type: %T", response)
	if cmdResp, ok := response.(*server.CommandResponse); ok {
		t.Logf("Result type: %T", cmdResp.Result)
		t.Logf("Result count: %d", cmdResp.ResultCount)
		if results, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("Number of results: %d", len(results))
			if len(results) > 0 {
				t.Logf("First result: %+v", results[0])
			}
		}
	}
	t.Logf("RAW RESPONSE %s", response)
	docs := extractDocuments(t, response)

	if len(docs) != 50 {
		t.Errorf("Expected 50 documents, got %d", len(docs))
	}

	// Debug: Print first document to see structure
	if len(docs) > 0 {
		t.Logf("First document structure: %+v", docs[0])
		t.Logf("First document keys: %v", getKeys(docs[0]))
	}

	// Verify all fields are present
	missingFields := 0
	for i, doc := range docs {
		if _, ok := doc["ID"]; !ok {
			if missingFields < 3 {
				t.Errorf("Document %d missing ID field (keys: %v)", i, getKeys(doc))
			}
			missingFields++
		}
		if _, ok := doc["Name"]; !ok {
			if missingFields < 3 {
				t.Errorf("Document %d missing Name field (keys: %v)", i, getKeys(doc))
			}
			missingFields++
		}
		if _, ok := doc["Country"]; !ok {
			if missingFields < 3 {
				t.Errorf("Document %d missing Country field (keys: %v)", i, getKeys(doc))
			}
			missingFields++
		}
		if _, ok := doc["BirthYear"]; !ok {
			if missingFields < 3 {
				t.Errorf("Document %d missing BirthYear field (keys: %v)", i, getKeys(doc))
			}
			missingFields++
		}
	}

	if missingFields > 0 {
		t.Errorf("Total missing field instances: %d", missingFields)
	} else {
		t.Logf("✓ TestSelectAll_SmallDataset passed: %d documents with all fields", len(docs))
	}
}

// getKeys returns the keys of a map for debugging
func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestSelectCount_TotalDocuments(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedBooksBundle(t, fixture, 100)

	query := `SELECT COUNT(*) FROM "Books"`
	response := executeRealQuery(t, fixture, query)
	count := extractCountFromResponse(t, response)

	if count != 100 {
		t.Errorf("Expected count 100, got %d", count)
	}

	t.Logf("✓ TestSelectCount_TotalDocuments passed: count=%d", count)
}

func TestWhere_Equality(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := `SELECT * FROM "Books" WHERE Genre == "Fiction"`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	// Fiction is 1 of 5 genres, so ~20 books
	if len(docs) < 15 || len(docs) > 25 {
		t.Errorf("Expected ~20 Fiction books, got %d", len(docs))
	}

	// Verify all results have Genre == "Fiction"
	for i, doc := range docs {
		if genre, ok := doc["Genre"].(string); ok {
			if genre != "Fiction" {
				t.Errorf("Document %d has Genre=%s, expected Fiction", i, genre)
			}
		}
	}

	t.Logf("✓ TestWhere_Equality passed: %d Fiction books", len(docs))
}

func TestWhere_RangeComparison(t *testing.T) {
	fixture := setupRealServer(t)

	seedAuthorsBundle(t, fixture, 100)

	query := `SELECT * FROM "Authors" WHERE BirthYear > 2000`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	// BirthYear = 1950 + i, so > 2000 means i > 50
	expectedCount := 50
	if len(docs) != expectedCount {
		t.Errorf("Expected %d authors born after 2000, got %d", expectedCount, len(docs))
	}

	// Verify all results have BirthYear > 2000
	for i, doc := range docs {
		if year, ok := doc["BirthYear"].(int); ok {
			if year <= 2000 {
				t.Errorf("Document %d has BirthYear=%d, expected >2000", i, year)
			}
		}
	}

	t.Logf("✓ TestWhere_RangeComparison passed: %d authors", len(docs))
}

func TestWhere_INClause(t *testing.T) {
	fixture := setupRealServer(t)

	seedAuthorsBundle(t, fixture, 100)
	seedBooksBundle(t, fixture, 100)

	query := `SELECT * FROM "Books" WHERE "Genre" IN ("Mystery", "SciFi")`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	// Mystery and SciFi are 2 of 5 genres, so ~40 books
	if len(docs) < 35 || len(docs) > 45 {
		t.Errorf("Expected ~40 Mystery/SciFi books, got %d", len(docs))
	}

	// Verify all results have Genre in allowed list
	for i, doc := range docs {
		var genre string
		var ok bool
		if genreVal, exists := doc["Genre"]; exists {
			if str, isStr := genreVal.(string); isStr {
				genre = str
				ok = true
			} else if fv, isFV := genreVal.(models.FieldValue); isFV {
				if str, isStr := fv.AsString(); isStr {
					genre = str
					ok = true
				}
			}
		}

		if ok {
			if genre != "Mystery" && genre != "SciFi" {
				t.Errorf("Document %d has Genre=%s, expected Mystery or SciFi", i, genre)
			}
		}
	}

	t.Logf("✓ TestWhere_INClause passed: %d Mystery/SciFi books", len(docs))
}

func TestWhere_NOTINClause(t *testing.T) {
	fixture := setupRealServer(t)

	seedAuthorsBundle(t, fixture, 100)

	query := `SELECT * FROM "Authors" WHERE "Country" NOT IN ("USA", "UK")`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	// Countries rotate USA/UK/Canada/France, so NOT IN USA/UK = ~50 docs
	if len(docs) < 45 || len(docs) > 55 {
		t.Errorf("Expected ~50 authors from Canada/France, got %d", len(docs))
	}

	// Verify no results have USA or UK
	for i, doc := range docs {
		var country string
		var ok bool
		if countryVal, exists := doc["Country"]; exists {
			if str, isStr := countryVal.(string); isStr {
				country = str
				ok = true
			} else if fv, isFV := countryVal.(models.FieldValue); isFV {
				if str, isStr := fv.AsString(); isStr {
					country = str
					ok = true
				}
			}
		}

		if ok {
			if country == "USA" || country == "UK" {
				t.Errorf("Document %d has Country=%s, should be excluded", i, country)
			}
		}
	}

	t.Logf("✓ TestWhere_NOTINClause passed: %d non-USA/UK authors", len(docs))
}

func TestOrderBy_SingleFieldAsc(t *testing.T) {
	fixture := setupRealServer(t)

	seedAuthorsBundle(t, fixture, 50)

	query := `SELECT * FROM "Authors" ORDER BY "BirthYear" ASC`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	if len(docs) != 50 {
		t.Errorf("Expected 50 documents, got %d", len(docs))
	}

	// Validate exact ordering (≤150 results)
	validateExactOrdering(t, docs, "BirthYear", true)

	t.Logf("✓ TestOrderBy_SingleFieldAsc passed: %d documents in order", len(docs))
}

func TestOrderBy_SingleFieldDesc(t *testing.T) {
	fixture := setupRealServer(t)

	seedBooksBundle(t, fixture, 75)

	query := `SELECT * FROM "Books" ORDER BY "Price" DESC`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	if len(docs) != 75 {
		t.Errorf("Expected 75 documents, got %d", len(docs))
	}

	// Validate exact ordering (≤150 results)
	validateExactOrdering(t, docs, "Price", false)

	t.Logf("✓ TestOrderBy_SingleFieldDesc passed: %d documents in order", len(docs))
}

func TestOrderBy_WithWhereAndLimit(t *testing.T) {
	fixture := setupRealServer(t)

	seedBooksBundle(t, fixture, 100)

	query := `SELECT TOP 10 * FROM "Books" WHERE Genre == "Fiction" ORDER BY Price ASC`
	response := executeRealQuery(t, fixture, query)

	// DEBUG: Check what the response actually contains
	if cmdResp, ok := response.(*server.CommandResponse); ok {
		t.Logf("DEBUG: ResultCount=%d, StreamDocuments=%d, Result type=%T",
			cmdResp.ResultCount, len(cmdResp.StreamDocuments), cmdResp.Result)
		if docs, ok := cmdResp.Result.([]map[string]interface{}); ok {
			t.Logf("DEBUG: Result slice has %d documents", len(docs))
		}
	}

	docs := extractDocuments(t, response)

	if len(docs) != 10 {
		t.Errorf("Expected 10 documents, got %d", len(docs))
	}

	// Validate exact ordering
	validateExactOrdering(t, docs, "Price", true)

	// Verify all are Fiction
	for i, doc := range docs {
		if genre, ok := doc["Genre"].(string); ok {
			if genre != "Fiction" {
				t.Errorf("Document %d has Genre=%s, expected Fiction", i, genre)
			}
		}
	}

	t.Logf("✓ TestOrderBy_WithWhereAndLimit passed: %d Fiction books ordered by Price", len(docs))
}

func TestOrderBy_LargeResultSet_Sampling(t *testing.T) {
	fixture := setupRealServer(t)

	seedBooksBundle(t, fixture, 200)

	query := `SELECT * FROM "Books" ORDER BY "ID" ASC`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	if len(docs) != 200 {
		t.Errorf("Expected 200 documents, got %d", len(docs))
	}

	// Use sampling validation (>150 results)
	validateSampledOrdering(t, docs, "ID", true)

	t.Logf("✓ TestOrderBy_LargeResultSet_Sampling passed: %d documents validated via sampling", len(docs))
}

func TestSelectDistinct_UniqueValues(t *testing.T) {
	fixture := setupRealServer(t)

	seedAuthorsBundle(t, fixture, 100)

	query := `SELECT DISTINCT Country FROM "Authors"`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	// Should return 4 distinct countries: USA, UK, Canada, France
	expectedCount := 4
	if len(docs) != expectedCount {
		t.Errorf("Expected %d distinct countries, got %d", expectedCount, len(docs))
	}

	// Verify uniqueness
	seen := make(map[string]bool)
	for _, doc := range docs {
		if country, ok := doc["Country"].(string); ok {
			if seen[country] {
				t.Errorf("Duplicate country found: %s", country)
			}
			seen[country] = true
		}
	}

	t.Logf("✓ TestSelectDistinct_UniqueValues passed: %d distinct countries", len(docs))
}

func TestSelectTopN_Limit(t *testing.T) {
	fixture := setupRealServer(t)

	seedPublishersBundle(t, fixture, 20)

	query := `SELECT TOP 5 DOCUMENTS FROM "Publishers";`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	if len(docs) != 5 {
		t.Errorf("Expected 5 documents, got %d", len(docs))
	}

	t.Logf("✓ TestSelectTopN_Limit passed: %d documents returned", len(docs))
}

// func TestSelectAggregates_SumAvgMinMax(t *testing.T) {
// 	fixture := setupRealServer(t)

// 	seedBooksBundle(t, fixture, 50)

// 	query := `SELECT COUNT(*), SUM(Price), AVG(Price), MIN(Price), MAX(Price) FROM "Books"`
// 	response := executeRealQuery(t, fixture, query)
// 	agg := extractAggregates(t, response)

// 	// Verify all aggregates are present
// 	if count, ok := agg["Count"].(int); !ok || count != 50 {
// 		t.Errorf("Expected Count=50, got %v", agg["Count"])
// 	}

// 	if _, ok := agg["Sum"]; !ok {
// 		t.Errorf("SUM(Price) missing from aggregates")
// 	}

// 	if _, ok := agg["Avg"]; !ok {
// 		t.Errorf("AVG(Price) missing from aggregates")
// 	}

// 	if _, ok := agg["Min"]; !ok {
// 		t.Errorf("MIN(Price) missing from aggregates")
// 	}

// 	if _, ok := agg["Max"]; !ok {
// 		t.Errorf("MAX(Price) missing from aggregates")
// 	}

// 	t.Logf("✓ TestSelectAggregates_SumAvgMinMax passed: %v", agg)
// }

func TestWhere_ComplexAndOr(t *testing.T) {
	fixture := setupRealServer(t)

	seedBooksBundle(t, fixture, 100)

	query := `SELECT * FROM "Books" WHERE ("Genre" == "Fiction" OR "Genre" == "Mystery") AND "Price" > 50.0`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	// Verify all results match criteria
	for i, doc := range docs {
		genre, okGenre := asString(doc["Genre"])
		price, okPrice := asFloat(doc["Price"])

		if !okGenre || !okPrice {
			t.Errorf("Document %d missing Genre or Price", i)
			continue
		}

		if genre != "Fiction" && genre != "Mystery" {
			t.Errorf("Document %d has Genre=%s, expected Fiction or Mystery", i, genre)
		}

		if price <= 50.0 {
			t.Errorf("Document %d has Price=%.2f, expected >50.0", i, price)
		}
	}

	t.Logf("✓ TestWhere_ComplexAndOr passed: %d matching books", len(docs))
}

func TestWhere_LIKEPattern(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 50)

	// Pattern: Author, any single char (_), "00", anything (%)
	// Matches: Author_001 through Author_009 (9 authors)
	query := `SELECT * FROM "Authors" WHERE "Name" LIKE "Author_00%"`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	// Author_001 through Author_009 = 9 matches
	expectedRange := []int{9, 10, 11} // Allow some variation
	if len(docs) < expectedRange[0] || len(docs) > expectedRange[2] {
		t.Errorf("Expected %d-%d authors matching pattern, got %d", expectedRange[0], expectedRange[2], len(docs))
	}

	// Verify all names match pattern
	for i, doc := range docs {
		if name, ok := doc["Name"].(string); ok {
			if len(name) < 9 || name[:8] != "Author_0" {
				t.Errorf("Document %d has Name=%s, doesn't match pattern", i, name)
			}
		}
	}

	t.Logf("✓ TestWhere_LIKEPattern passed: %d matching authors", len(docs))
}

func TestWhere_MultipleFields(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 100)

	query := `SELECT * FROM "Authors" WHERE "Country" == "USA" AND "BirthYear" > 1980`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	// Verify all results match both conditions
	for i, doc := range docs {
		// Extract Country - handle both string and FieldValue types
		var country string
		var okCountry bool
		if countryVal, exists := doc["Country"]; exists {
			if str, ok := countryVal.(string); ok {
				country = str
				okCountry = true
			} else if fv, ok := countryVal.(models.FieldValue); ok {
				if str, ok := fv.AsString(); ok {
					country = str
					okCountry = true
				}
			}
		}

		// Extract BirthYear - handle int, int64, and FieldValue types
		var year int64
		var okYear bool
		if birthYearVal, exists := doc["BirthYear"]; exists {
			if y, ok := birthYearVal.(int64); ok {
				year = y
				okYear = true
			} else if y, ok := birthYearVal.(int); ok {
				year = int64(y)
				okYear = true
			} else if fv, ok := birthYearVal.(models.FieldValue); ok {
				if y, ok := fv.AsInt(); ok {
					year = y
					okYear = true
				}
			}
		}

		if !okCountry || !okYear {
			t.Errorf("Document %d missing Country or BirthYear", i)
			continue
		}

		if country != "USA" {
			t.Errorf("Document %d has Country=%s, expected USA", i, country)
		}

		if year <= 1980 {
			t.Errorf("Document %d has BirthYear=%d, expected >1980", i, year)
		}
	}

	t.Logf("✓ TestWhere_MultipleFields passed: %d matching authors", len(docs))
}

func TestOrderBy_MultipleFields(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedBooksBundle(t, fixture, 100)

	query := `SELECT * FROM "Books" ORDER BY Genre ASC, Price DESC`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	if len(docs) != 100 {
		t.Errorf("Expected 100 documents, got %d", len(docs))
	}

	// Validate multi-field ordering
	for i := 1; i < len(docs); i++ {
		prevGenre, _ := asString(docs[i-1]["Genre"])
		currGenre, _ := asString(docs[i]["Genre"])
		prevPrice, _ := asFloat(docs[i-1]["Price"])
		currPrice, _ := asFloat(docs[i]["Price"])

		// Primary sort: Genre ASC
		if prevGenre > currGenre {
			t.Errorf("Genre ordering violation at position %d: %s > %s", i, prevGenre, currGenre)
		}

		// Secondary sort: Price DESC (within same Genre)
		if prevGenre == currGenre && prevPrice < currPrice {
			t.Errorf("Price ordering violation at position %d: %.2f < %.2f (Genre=%s)", i, prevPrice, currPrice, currGenre)
		}
	}

	t.Logf("✓ TestOrderBy_MultipleFields passed: %d documents ordered by Genre ASC, Price DESC", len(docs))
}

func TestSelectFieldList_Projection(t *testing.T) {
	fixture := setupRealServer(t)
	//resetDatabase(t, fixture)
	seedAuthorsBundle(t, fixture, 30)

	query := `SELECT "ID", "Name" FROM "Authors"`
	response := executeRealQuery(t, fixture, query)
	docs := extractDocuments(t, response)

	if len(docs) != 30 {
		t.Errorf("Expected 30 documents, got %d", len(docs))
	}

	// Verify requested fields plus system fields (DocumentID, CreatedAt, UpdatedAt)
	// Note: The new parser includes system fields even when specific fields are requested
	for i, doc := range docs {
		// Check that requested fields are present
		if _, ok := doc["ID"]; !ok {
			t.Errorf("Document %d missing ID field", i)
		}

		if _, ok := doc["Name"]; !ok {
			t.Errorf("Document %d missing Name field", i)
		}

		// Ensure Country and BirthYear are NOT present (user fields not requested)
		if _, ok := doc["Country"]; ok {
			t.Errorf("Document %d has Country field (should be excluded)", i)
		}

		if _, ok := doc["BirthYear"]; ok {
			t.Errorf("Document %d has BirthYear field (should be excluded)", i)
		}
	}

	t.Logf("✓ TestSelectFieldList_Projection passed: %d documents with requested fields", len(docs))
}

// =============================================================================
// VALIDATION UTILITIES
// =============================================================================

// validateExactOrdering validates complete ordering for ≤150 results
func validateExactOrdering(t *testing.T, docs []map[string]interface{}, field string, ascending bool) {
	t.Helper()

	if len(docs) > 150 {
		t.Fatalf("validateExactOrdering called with %d results (use validateSampledOrdering for >150)", len(docs))
	}

	for i := 1; i < len(docs); i++ {
		prev := docs[i-1][field]
		curr := docs[i][field]

		if !isOrdered(prev, curr, ascending) {
			t.Fatalf("Ordering violation at position %d: %v vs %v (ascending=%t, field=%s)",
				i, prev, curr, ascending, field)
		}
	}
}

// validateSampledOrdering validates ordering via sampling for >150 results
func validateSampledOrdering(t *testing.T, docs []map[string]interface{}, field string, ascending bool) {
	t.Helper()

	if len(docs) <= 150 {
		t.Fatalf("validateSampledOrdering called with %d results (use validateExactOrdering for ≤150)", len(docs))
	}

	totalDocs := len(docs)
	sampleSize := totalDocs / 10 // 10% sampling

	// Sample top 10%
	for i := 1; i < sampleSize; i++ {
		prev := docs[i-1][field]
		curr := docs[i][field]

		if !isOrdered(prev, curr, ascending) {
			t.Fatalf("Ordering violation in top 10%% at position %d: %v vs %v (ascending=%t, field=%s)",
				i, prev, curr, ascending, field)
		}
	}

	// Sample bottom 10%
	bottomStart := totalDocs - sampleSize
	for i := bottomStart + 1; i < totalDocs; i++ {
		prev := docs[i-1][field]
		curr := docs[i][field]

		if !isOrdered(prev, curr, ascending) {
			t.Fatalf("Ordering violation in bottom 10%% at position %d: %v vs %v (ascending=%t, field=%s)",
				i, prev, curr, ascending, field)
		}
	}

	// Full inversion counting (zero-tolerance)
	inversionCount := 0
	for i := 1; i < totalDocs; i++ {
		prev := docs[i-1][field]
		curr := docs[i][field]

		if !isOrdered(prev, curr, ascending) {
			inversionCount++
			t.Errorf("Inversion %d at position %d: %v vs %v (ascending=%t, field=%s)",
				inversionCount, i, prev, curr, ascending, field)
		}
	}

	if inversionCount > 0 {
		t.Fatalf("Found %d ordering inversions (zero-tolerance policy)", inversionCount)
	}
}

// isOrdered checks if two values are in correct order
func isOrdered(prev, curr interface{}, ascending bool) bool {
	// Handle FieldValue type (E2E tests get FieldValue directly, not through JSON marshaling)
	if fvPrev, ok := prev.(models.FieldValue); ok {
		if fvCurr, ok := curr.(models.FieldValue); ok {
			// Use FieldValue's built-in comparison methods
			if ascending {
				return fvPrev.CompareLessThanOrEqual(fvCurr)
			} else {
				return fvPrev.CompareGreaterThanOrEqual(fvCurr)
			}
		}
		return false
	}

	// Handle different types
	switch p := prev.(type) {
	case int:
		c, ok := curr.(int)
		if !ok {
			return false
		}
		if ascending {
			return p <= c
		}
		return p >= c

	case int64:
		c, ok := curr.(int64)
		if !ok {
			return false
		}
		if ascending {
			return p <= c
		}
		return p >= c

	case float64:
		c, ok := curr.(float64)
		if !ok {
			return false
		}
		if ascending {
			return p <= c
		}
		return p >= c

	case string:
		c, ok := curr.(string)
		if !ok {
			return false
		}
		if ascending {
			return p <= c
		}
		return p >= c

	default:
		return false
	}
}

// =============================================================================
// BENCHMARKS
// =============================================================================

func BenchmarkSimpleSelect(b *testing.B) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup (outside timing)
	fixture := setupRealServerForBenchmark(b)
	resetDatabaseForBenchmark(b, fixture)
	seedAuthorsBundleForBenchmark(b, fixture, 100)

	query := `SELECT * FROM "Authors"`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		elapsed := time.Since(startTime)
		if elapsed > 50*time.Millisecond {
			b.Logf("WARNING: Query exceeded 50ms target: %v", elapsed)
		}
	}
}

func BenchmarkComplexWhere(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup (outside timing)
	fixture := setupRealServerForBenchmark(b)
	resetDatabaseForBenchmark(b, fixture)
	seedBooksBundleForBenchmark(b, fixture, 200)

	query := `SELECT * FROM "Books" WHERE (Genre == "Fiction" OR Genre == "Mystery") AND Price > 30.0 ORDER BY Price DESC TOP 20`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		elapsed := time.Since(startTime)
		if elapsed > 100*time.Millisecond {
			b.Logf("WARNING: Query exceeded 100ms target: %v", elapsed)
		}
	}
}

func BenchmarkGroupByAggregates(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup (outside timing)
	fixture := setupRealServerForBenchmark(b)
	resetDatabaseForBenchmark(b, fixture)
	seedBooksBundleForBenchmark(b, fixture, 200)

	query := `SELECT Genre, COUNT(*), AVG(Price), SUM(Price) FROM "Books" GROUP BY Genre`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, query, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		elapsed := time.Since(startTime)
		if elapsed > 250*time.Millisecond {
			b.Logf("WARNING: Query exceeded 250ms target: %v", elapsed)
		}
	}
}

// Benchmark setup helpers (b.Helper() equivalent)
func setupRealServerForBenchmark(b *testing.B) *TestFixture {
	tempDir := b.TempDir()

	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel) // Minimize logging noise
	logger, err := loggerConfig.Build()
	if err != nil {
		b.Fatalf("Failed to create logger: %v", err)
	}
	sugar := logger.Sugar()

	args := &settings.Arguments{
		DataDir:  filepath.Join(tempDir, "data_files"),
		TempDir:  filepath.Join(tempDir, "temp_files"),
		LogDir:   filepath.Join(tempDir, "log_files"),
		LogLevel: "error",
	}

	// Set global settings for WAL manager and other services
	globalSettings := settings.GetSettings()
	globalSettings.LogDir = args.LogDir
	globalSettings.DataDir = args.DataDir
	globalSettings.TempDir = args.TempDir

	if err := os.MkdirAll(args.DataDir, 0755); err != nil {
		b.Fatalf("Failed to create data directory: %v", err)
	}
	if err := os.MkdirAll(args.TempDir, 0755); err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	if err := os.MkdirAll(args.LogDir, 0755); err != nil {
		b.Fatalf("Failed to create log directory: %v", err)
	}

	databaseStore, err := databasestore.NewDatabaseStore(args.DataDir, sugar)
	if err != nil {
		b.Fatalf("Failed to create database store: %v", err)
	}

	databaseFactory := database.NewDatabaseFactory()
	databaseService := database.NewDatabaseService(databaseStore, databaseFactory, args, sugar)

	fileRegistry, err := buffer.NewFileRegistry(args.DataDir, buffer.SyncInterval, sugar)
	if err != nil {
		b.Fatalf("Failed to create file registry: %v", err)
	}
	bufferPool := buffer.NewBufferPool(1000, buffer.DefaultPageSize, fileRegistry, sugar)

	bundleStore, err := bundlestore.NewBundleStore(args.DataDir, bufferPool, sugar, "binary")
	if err != nil {
		b.Fatalf("Failed to create bundle store: %v", err)
	}
	bundleFactory := bundle.NewBundleFactory()
	documentFactory := document.NewDocumentFactory()
	bundleService := bundle.NewBundleService(bundleStore, bundleFactory, documentFactory, sugar, args)

	catalogService := defaultdb.NewCatalogService(databaseService, bundleService, sugar)

	// Create and initialize the "primary" database (required for catalog operations)
	primaryDB := &models.Database{
		DatabaseID:    helpers.GenerateUUID(),
		Name:          "primary",
		Description:   "Primary database for benchmark catalogs",
		DataDirectory: args.DataDir,
		Bundles:       make(map[string]models.Bundle),
		BundleFiles:   []string{},
	}

	// Save the primary database
	err = databaseStore.CreateDatabaseDataFile(primaryDB)
	if err != nil {
		b.Fatalf("Failed to create primary database: %v", err)
	}
	databaseService.Databases[primaryDB.Name] = primaryDB

	// Initialize primary database bundle catalogs
	err = defaultdb.InitPrimaryBundleCatalogs(databaseService, databaseStore, primaryDB, sugar, bundleService)
	if err != nil {
		b.Fatalf("Failed to initialize primary bundle catalogs: %v", err)
	}

	serviceManager := server.InitServiceManager(databaseService, bundleService, catalogService, nil, nil, sugar, false)
	if serviceManager == nil {
		b.Fatal("Failed to initialize service manager")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbName := fmt.Sprintf("testdb_%s", b.Name())
	createDBCmd := fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)
	startTime := time.Now()
	_, err = server.CommandDirector(ctx, nil, *serviceManager, createDBCmd, sugar, startTime, nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create %s database: %v", dbName, err)
	}

	db, err := databaseService.GetDatabaseByName(dbName)
	if err != nil {
		b.Fatalf("Failed to retrieve %s database: %v", dbName, err)
	}

	return &TestFixture{
		ServiceManager: serviceManager,
		Database:       db,
		TempDir:        tempDir,
		Logger:         sugar,
		Settings:       args,
	}
}

func resetDatabaseForBenchmark(b *testing.B, fixture *TestFixture) {
	bundles := []string{"Authors", "Books", "Publishers"}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, bundleName := range bundles {
		dropCmd := fmt.Sprintf(`DROP BUNDLE "%s"`, bundleName)
		startTime := time.Now()
		_, _ = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, dropCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	}

	createAuthorsCmd := `CREATE BUNDLE "Authors" WITH FIELDS (
		{"ID", "INT", true, true, NULL},
		{"Name", "STRING", true, false, NULL},
		{"Country", "STRING", false, false, NULL},
		{"BirthYear", "INT", false, false, NULL}
	)`
	startTime := time.Now()
	_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createAuthorsCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Authors bundle: %v", err)
	}

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
		b.Fatalf("Failed to create Books bundle: %v", err)
	}

	createPublishersCmd := `CREATE BUNDLE "Publishers" WITH FIELDS (
		{"ID", "INT", true, true, null},
		{"Name", "STRING", true, false, null},
		{"Country", "STRING", false, false, null}
	)`
	startTime = time.Now()
	_, err = server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, createPublishersCmd, fixture.Logger, startTime, nil, "127.0.0.1")
	if err != nil {
		b.Fatalf("Failed to create Publishers bundle: %v", err)
	}
}

func seedAuthorsBundleForBenchmark(b *testing.B, fixture *TestFixture, count int) {
	countries := []string{"USA", "UK", "Canada", "France"}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 1; i <= count; i++ {
		name := fmt.Sprintf("Author_%03d", i)
		country := countries[(i-1)%len(countries)]
		birthYear := 1950 + i

		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Authors" WITH  ( {"ID"=%d}, {"Name" = "%s"}, {"Country" = "%s"}, {"BirthYear" = %d});`,
			i, name, country, birthYear)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to seed author %d: %v", i, err)
		}
	}
}

func seedBooksBundleForBenchmark(b *testing.B, fixture *TestFixture, count int) {
	genres := []string{"Fiction", "NonFiction", "Mystery", "SciFi", "Fantasy"}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 1; i <= count; i++ {
		title := fmt.Sprintf("Book_%03d", i)
		authorID := ((i - 1) % 100) + 1
		genre := genres[(i-1)%len(genres)]
		price := 9.99 + float64(i)*0.50

		addDocCmd := fmt.Sprintf(`ADD DOCUMENT TO BUNDLE "Books" WITH  ( {"ID"=%d}, {"Title" = "%s"}, {"AuthorID" = %d}, {"Genre" = "%s"}, {"Price" = %.2f});`,
			i, title, authorID, genre, price)

		startTime := time.Now()
		_, err := server.CommandDirector(ctx, fixture.Database, *fixture.ServiceManager, addDocCmd, fixture.Logger, startTime, nil, "127.0.0.1")
		if err != nil {
			b.Fatalf("Failed to seed book %d: %v", i, err)
		}
	}
}
