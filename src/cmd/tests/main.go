/*
SYNDRDB TEST RUNNER MAIN

This file implements the main test runner for SyndrDB database creation use cases.
It provides colorized console output for test results and comprehensive reporting
of test execution status, timing, and detailed error information.

ALGORITHM OVERVIEW:
The test runner executes predefined use cases for database creation, measures
execution time, and provides detailed reporting with color-coded results.
Green indicates successful tests, red indicates failures, and yellow shows
warnings or skipped tests.

TEST EXECUTION:
Tests are organized by category and executed sequentially with proper setup
and cleanup. Each test provides detailed logging and error reporting to
facilitate debugging and validation of database functionality.

This implementation follows the Single Responsibility Principle where each function
handles a specific aspect of test execution while maintaining the robust
error handling and data integrity standards required by the SyndrDB project.
*/

package main

import (
	"flag"
	"fmt"
	"runtime"
	"strings"
	"time"

	"syndrdb/src/tests/homegrown"

	"go.uber.org/zap"
)

// TestResult represents the result of a single test execution
// Following SyndrDB comprehensive error handling, encapsulates test outcome
type TestResult[T any] struct {
	UseCase       T
	Success       bool
	ExecutionTime time.Duration
	Error         error
	Details       string
}

// TestSummary provides aggregate information about test execution
// Following SyndrDB comprehensive error handling, summarizes test results
type TestSummary[T any] struct {
	TotalTests   int
	PassedTests  int
	FailedTests  int
	SkippedTests int
	TotalTime    time.Duration
	Results      []TestResult[T]
	Categories   map[string]int
}

// filterTestsByNames filters test use cases based on provided test names
// If testNames is empty, returns all use cases
func filterTestsByNames[T homegrown.UseCase](useCases []T, testNames []string) []T {
	if len(testNames) == 0 {
		return useCases
	}

	var filtered []T
	for _, useCase := range useCases {
		if containsTestName(testNames, useCase.GetName()) {
			filtered = append(filtered, useCase)
		}
	}
	return filtered
}

// containsTestName checks if a test name exists in the provided list
func containsTestName(testNames []string, testName string) bool {
	for _, name := range testNames {
		if strings.EqualFold(name, testName) {
			return true
		}
	}
	return false
}

// hasAnyTestFromCategory checks if any test names contain the category prefix
func hasAnyTestFromCategory(testNames []string, category string) bool {
	for _, name := range testNames {
		if strings.Contains(strings.ToLower(name), strings.ToLower(category)) {
			return true
		}
	}
	return false
}

func main() {
	// Parse command-line arguments
	var testFilter string
	flag.StringVar(&testFilter, "test", "", "Comma-delimited list of test names to run (e.g., -test=InitializePrimaryDatabase,ValidateSystemBundles)")
	flag.Parse()

	// Parse test filter into slice
	var testNames []string
	if testFilter != "" {
		testNames = strings.Split(testFilter, ",")
		// Trim whitespace from test names
		for i, name := range testNames {
			testNames[i] = strings.TrimSpace(name)
		}
	}

	// Initialize logger
	homegrown.ColorLogger = setupLogger()

	// Display welcome banner
	displayWelcomeBanner()

	// Show filter information if tests are filtered
	if len(testNames) > 0 {
		homegrown.ColorLogger.Info(homegrown.HighlightYellow(fmt.Sprintf("Running filtered tests: %s", strings.Join(testNames, ", "))))
	}

	var err error

	// Stand up test database service (skip if only running RootUser tests which have their own isolated environment)
	skipSharedSetup := len(testNames) == 1 && containsTestName(testNames, "RootUser")
	if !skipSharedSetup {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Setting up test database service..."))
		_, _, err = homegrown.StandupTestDatabaseService()
		if err != nil {
			homegrown.ColorLogger.Error(homegrown.HighlightRed("Failed to setup test database service"), zap.Error(err))
			return
		}
		homegrown.ColorLogger.Info(homegrown.HighlightGreen("✓ Test database service setup complete"))
	} else {
		homegrown.ColorLogger.Info(homegrown.HighlightYellow("Skipping shared test database setup (RootUser tests use isolated environment)"))
	}

	// Execute database creation use case tests
	homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting database creation use case tests..."))
	dbUseCases := homegrown.GetDatabaseCreationUseCases()
	filteredDbUseCases := filterTestsByNames(dbUseCases, testNames)
	dbSummary := executeAllTests(filteredDbUseCases)

	// Execute primary database initialization tests
	homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting primary database initialization tests..."))
	primaryDbUseCases := homegrown.GetPrimaryDatabaseUseCases()
	filteredPrimaryDbUseCases := filterTestsByNames(primaryDbUseCases, testNames)
	primaryDbSummary := executeAllTests(filteredPrimaryDbUseCases)

	// Execute bundle management use case tests
	homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting bundle management use case tests..."))
	bundleUseCases := homegrown.GetBundleManagementUseCases()
	filteredBundleUseCases := filterTestsByNames(bundleUseCases, testNames)
	bundleSummary := executeAllTests(filteredBundleUseCases)

	// Execute JOIN functionality demonstration (if not filtered or explicitly included)
	if len(testNames) == 0 || containsTestName(testNames, "JoinDemonstration") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting JOIN functionality demonstration..."))
		err = homegrown.RunJoinDemonstration(homegrown.ColorLogger)
		if err != nil {
			homegrown.ColorLogger.Error(homegrown.HighlightRed("JOIN demonstration failed"), zap.Error(err))
		} else {
			homegrown.ColorLogger.Info(homegrown.HighlightGreen("✓ JOIN demonstration completed successfully"))
		}
	}

	// Execute comprehensive end-to-end JOIN testing (if not filtered or explicitly included)
	if len(testNames) == 0 || containsTestName(testNames, "ComprehensiveJoinTests") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting comprehensive end-to-end JOIN testing..."))
		err = homegrown.RunComprehensiveJoinTests()
		if err != nil {
			homegrown.ColorLogger.Error(homegrown.HighlightRed("Comprehensive JOIN tests failed"), zap.Error(err))
		} else {
			homegrown.ColorLogger.Info(homegrown.HighlightGreen("✓ Comprehensive JOIN tests completed successfully"))
		}
	}

	// Execute ORDER BY functionality demo (if not filtered or explicitly included)
	if len(testNames) == 0 || containsTestName(testNames, "OrderByDemo") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting ORDER BY functionality demo..."))
		err = homegrown.RunOrderByDemo()
		if err != nil {
			homegrown.ColorLogger.Error(homegrown.HighlightRed("ORDER BY demo failed"), zap.Error(err))
		} else {
			homegrown.ColorLogger.Info(homegrown.HighlightGreen("✓ ORDER BY demo completed successfully"))
		}
	}

	// Execute LIKE query functionality demo (if not filtered or explicitly included)
	if len(testNames) == 0 || containsTestName(testNames, "LikeQueryDemo") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting LIKE query functionality demo..."))
		err = homegrown.RunLikeQueryDemo()
		if err != nil {
			homegrown.ColorLogger.Error(homegrown.HighlightRed("LIKE query demo failed"), zap.Error(err))
		} else {
			homegrown.ColorLogger.Info(homegrown.HighlightGreen("✓ LIKE query demo completed successfully"))
		}
	}

	// Execute GROUP BY functionality tests (if not filtered or explicitly included)
	if len(testNames) == 0 || containsTestName(testNames, "GroupByTests") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting GROUP BY functionality tests..."))
		homegrown.TestGroupByFunctionality()
		homegrown.ColorLogger.Info(homegrown.HighlightGreen("✓ GROUP BY tests completed successfully"))
	}

	// Execute Session Management tests
	if len(testNames) == 0 || hasAnyTestFromCategory(testNames, "Session") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting Session Management tests..."))
		sessionUseCases := homegrown.GetSessionManagementUseCases()
		filteredSessionUseCases := filterTestsByNames(sessionUseCases, testNames)
		sessionSummary := executeAllTests(filteredSessionUseCases)

		// Display session test results if any were run
		if len(filteredSessionUseCases) > 0 {
			homegrown.ColorLogger.Info(homegrown.HighlightBlue("Session Management Test Results:"))
			displayTestSummaryGeneric(sessionSummary)
		}
	}

	// Execute Security Validation tests
	if len(testNames) == 0 || hasAnyTestFromCategory(testNames, "Security") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting Security Validation tests..."))
		securityUseCases := homegrown.GetSecurityTestUseCases()
		filteredSecurityUseCases := filterTestsByNames(securityUseCases, testNames)
		securitySummary := executeAllTests(filteredSecurityUseCases)

		// Display security test results if any were run
		if len(filteredSecurityUseCases) > 0 {
			homegrown.ColorLogger.Info(homegrown.HighlightBlue("Security Validation Test Results:"))
			displayTestSummaryGeneric(securitySummary)
		}
	}

	// Execute Audit Logging tests
	if len(testNames) == 0 || hasAnyTestFromCategory(testNames, "Audit") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting Audit Logging tests..."))
		auditUseCases := homegrown.GetAuditLoggingUseCases()
		filteredAuditUseCases := filterTestsByNames(auditUseCases, testNames)
		auditSummary := executeAllTests(filteredAuditUseCases)

		// Display audit test results if any were run
		if len(filteredAuditUseCases) > 0 {
			homegrown.ColorLogger.Info(homegrown.HighlightBlue("Audit Logging Test Results:"))
			displayTestSummaryGeneric(auditSummary)
		}
	}

	// Execute Root User Validation tests (if not filtered or explicitly included)
	if len(testNames) == 0 || containsTestName(testNames, "RootUser") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting Root User Validation tests..."))
		err = homegrown.RunRootUserTests()
		if err != nil {
			homegrown.ColorLogger.Error(homegrown.HighlightRed("Root User tests failed"), zap.Error(err))
		} else {
			homegrown.ColorLogger.Info(homegrown.HighlightGreen("✓ Root User tests completed successfully"))
		}
	}

	// Execute WAL functionality tests
	if len(testNames) == 0 || hasAnyTestFromCategory(testNames, "WAL") {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Starting Write Ahead Logging functionality tests..."))
		walUseCases := homegrown.GetWALTestUseCases()
		walResults := homegrown.ExecuteAllWALTests(walUseCases)
		_ = walResults // Suppress unused variable warning
	}

	// Display core test results (always shown when tests were run)
	if len(filteredDbUseCases) > 0 {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Database Creation Test Results:"))
		displayTestSummaryGeneric(dbSummary)
	}

	if len(filteredPrimaryDbUseCases) > 0 {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Primary Database Initialization Test Results:"))
		displayTestSummaryGeneric(primaryDbSummary)
	}

	if len(filteredBundleUseCases) > 0 {
		homegrown.ColorLogger.Info(homegrown.HighlightBlue("Bundle Management Test Results:"))
		displayTestSummaryGeneric(bundleSummary)
	}

	homegrown.ColorLogger.Info(homegrown.HighlightBlue("Test execution complete"))
}

// setupLogger creates a logger that displays only messages without timestamps, levels, or source info
// Following SyndrDB comprehensive error handling, it provides proper logging configuration
func setupLogger() *zap.SugaredLogger {
	config := zap.NewDevelopmentConfig()

	// Configure encoder to show only the message
	config.EncoderConfig.TimeKey = ""       // Remove timestamp
	config.EncoderConfig.LevelKey = ""      // Remove log level
	config.EncoderConfig.CallerKey = ""     // Remove source file info
	config.EncoderConfig.NameKey = ""       // Remove logger name
	config.EncoderConfig.FunctionKey = ""   // Remove function name
	config.EncoderConfig.StacktraceKey = "" // Remove stack trace
	config.EncoderConfig.MessageKey = "msg" // Keep only the message

	config.Level, _ = zap.ParseAtomicLevel("Info") // Set default log level to warn
	// Use console encoder for clean output
	config.Encoding = "console"

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	return logger.Sugar()
}

// displayWelcomeBanner shows the test runner welcome message
// This function follows the Single Responsibility Principle by handling only banner display
// Following SyndrDB comprehensive error handling, it provides clear test identification
func displayWelcomeBanner() {
	fmt.Println()
	fmt.Println(homegrown.HighlightCyan("╔══════════════════════════════════════════════════════════════╗"))
	fmt.Println(homegrown.HighlightCyan("║") + "                    " + homegrown.HighlightGreen("SyndrDB Test Runner") + "                    " + homegrown.HighlightCyan("║"))
	fmt.Println(homegrown.HighlightCyan("║") + "              " + homegrown.Normal("Database Creation Use Cases") + "               " + homegrown.HighlightCyan("║"))
	fmt.Println(homegrown.HighlightCyan("╚══════════════════════════════════════════════════════════════╝"))
	fmt.Println()
}

// groupTestsByCategory organizes tests by their category
// This function follows the Single Responsibility Principle by handling only test grouping
// Following SyndrDB comprehensive error handling, it organizes tests for better execution flow
func groupTestsByCategory(useCases []homegrown.DatabaseCreationUseCase) map[string][]homegrown.DatabaseCreationUseCase {
	categories := make(map[string][]homegrown.DatabaseCreationUseCase)

	for _, useCase := range useCases {
		if categories[useCase.Category] == nil {
			categories[useCase.Category] = make([]homegrown.DatabaseCreationUseCase, 0)
		}
		categories[useCase.Category] = append(categories[useCase.Category], useCase)
	}

	return categories
}

// executeTestCaseGeneric runs a single test case with full lifecycle management for any use case type
// This function follows the Single Responsibility Principle by handling only single test execution
// Following SyndrDB comprehensive error handling, it manages test lifecycle with proper cleanup
func executeTestCaseGeneric[T homegrown.UseCase](useCase T) TestResult[T] {
	startTime := time.Now()
	result := TestResult[T]{
		UseCase: useCase,
		Success: false,
	}

	homegrown.ColorLogger.Debug(homegrown.HighlightYellow(fmt.Sprintf("Starting test: %s", useCase.GetName())))

	// Execute test lifecycle
	defer func() {
		result.ExecutionTime = time.Since(startTime)

		// Always attempt cleanup
		if cleanupErr := useCase.Cleanup(); cleanupErr != nil {
			homegrown.ColorLogger.Warn(homegrown.HighlightYellow(fmt.Sprintf("Cleanup warning for %s: %v", useCase.GetName(), cleanupErr)))
		}
	}()

	// Setup phase
	if err := useCase.Setup(); err != nil {
		result.Error = fmt.Errorf("setup failed: %w", err)
		result.Details = "Failed during test setup phase"
		return result
	}

	// Execute phase
	err := useCase.Execute()

	// Handle expected vs unexpected errors
	if useCase.GetExpectSuccess() {
		if err != nil {
			result.Error = fmt.Errorf("execution failed: %w", err)
			result.Details = "Test expected success but execution failed"
			return result
		}
	} else {
		if err == nil {
			result.Error = fmt.Errorf("execution should have failed but succeeded")
			result.Details = "Test expected failure but execution succeeded"
			return result
		} else {
			result.Success = true
			result.Details = fmt.Sprintf("Test expected failure and execution failed as expected. Err: %v", err)

		}
		// Expected failure occurred - this is actually success for negative tests
	}

	// Validate phase
	if err := useCase.Validate(); err != nil {
		result.Error = fmt.Errorf("validation failed: %w", err)
		result.Details = "Test execution succeeded but validation failed"
		return result
	}

	// Test passed
	result.Success = true
	result.Details = "Test completed successfully"
	return result
}

// executeAllTests runs all test cases for a specific use case type
// This function follows the Single Responsibility Principle by handling only test execution
// Following SyndrDB comprehensive error handling, it executes tests with proper error handling
func executeAllTests[T homegrown.UseCase](useCases []T) TestSummary[T] {
	summary := TestSummary[T]{
		TotalTests: len(useCases),
		Results:    make([]TestResult[T], 0, len(useCases)),
		Categories: make(map[string]int),
	}

	startTime := time.Now()

	// Group tests by category for better organization
	categorizedTests := groupTestsByCategoryGeneric(useCases)

	for category, tests := range categorizedTests {
		homegrown.ColorLogger.Infof(homegrown.HighlightBlue(fmt.Sprintf("\n=== Testing Category: %s ===", category)))

		for _, useCase := range tests {
			result := executeTestCaseGeneric(useCase)
			summary.Results = append(summary.Results, result)

			// Update counters
			if result.Success {
				summary.PassedTests++
			} else {
				summary.FailedTests++
			}

			// Update category counters
			summary.Categories[category]++

			// Display immediate result
			displayTestResultGeneric(result)
		}
	}

	summary.TotalTime = time.Since(startTime)
	return summary
}

// groupTestsByCategoryGeneric organizes tests by their category for any use case type
// This function follows the Single Responsibility Principle by handling only test grouping
// Following SyndrDB comprehensive error handling, it organizes tests for better execution flow
func groupTestsByCategoryGeneric[T homegrown.UseCase](useCases []T) map[string][]T {
	categories := make(map[string][]T)

	for _, useCase := range useCases {
		category := useCase.GetCategory()
		if categories[category] == nil {
			categories[category] = make([]T, 0)
		}
		categories[category] = append(categories[category], useCase)
	}

	return categories
}

// displayTestResultGeneric shows the result of a single test with color coding for any use case type
// This function follows the Single Responsibility Principle by handling only result display
// Following SyndrDB comprehensive error handling, it provides clear visual feedback
func displayTestResultGeneric[T homegrown.UseCase](result TestResult[T]) {
	duration := fmt.Sprintf("%.2fms", float64(result.ExecutionTime.Nanoseconds())/1e6)

	if result.Success {
		fmt.Printf("  %s %s %s %s\n",
			homegrown.HighlightGreen("✓"),
			homegrown.HighlightGreen("PASS"),
			homegrown.Normal(result.UseCase.GetName()),
			homegrown.HighlightBlue(fmt.Sprintf("(%s)", duration)))

		if result.UseCase.GetDescription() != "" {
			fmt.Printf("    %s\n", homegrown.Normal(result.UseCase.GetDescription()))
		}
	} else {
		fmt.Printf("  %s %s %s %s\n",
			homegrown.HighlightRed("✗"),
			homegrown.HighlightRed("FAIL"),
			homegrown.Normal(result.UseCase.GetName()),
			homegrown.HighlightBlue(fmt.Sprintf("(%s)", duration)))

		if result.UseCase.GetDescription() != "" {
			fmt.Printf("    %s\n", homegrown.Normal(result.UseCase.GetDescription()))
		}

		if result.Error != nil {
			fmt.Printf("    %s %s\n", homegrown.HighlightRed("Error:"), homegrown.Normal(result.Error.Error()))
		}

		if result.Details != "" {
			fmt.Printf("    %s %s\n", homegrown.HighlightYellow("Details:"), homegrown.Normal(result.Details))
		}
	}

	fmt.Println()
}

// displayTestSummary shows comprehensive test results with statistics
// This function follows the Single Responsibility Principle by handling only summary display
// Following SyndrDB comprehensive error handling, it provides complete test analysis
func displayTestSummaryGeneric[T homegrown.UseCase](summary TestSummary[T]) {
	fmt.Println()
	fmt.Println(homegrown.HighlightCyan("╔══════════════════════════════════════════════════════════════╗"))
	fmt.Println(homegrown.HighlightCyan("║") + "                     " + homegrown.HighlightGreen("Test Summary") + "                   		" + homegrown.HighlightCyan("║"))
	fmt.Println(homegrown.HighlightCyan("╚══════════════════════════════════════════════════════════════╝"))
	fmt.Println()

	// Overall statistics
	successRate := float64(summary.PassedTests) / float64(summary.TotalTests) * 100
	totalTime := fmt.Sprintf("%.2fs", summary.TotalTime.Seconds())

	fmt.Printf("Total Tests:     %s\n", homegrown.HighlightBlue(fmt.Sprintf("%d", summary.TotalTests)))
	fmt.Printf("Passed:          %s\n", homegrown.HighlightGreen(fmt.Sprintf("%d", summary.PassedTests)))
	fmt.Printf("Failed:          %s\n", homegrown.HighlightRed(fmt.Sprintf("%d", summary.FailedTests)))
	fmt.Printf("Success Rate:    %s\n", getColoredSuccessRate(successRate))
	fmt.Printf("Total Time:      %s\n", homegrown.HighlightBlue(totalTime))
	fmt.Println()

	// Category breakdown
	fmt.Println(homegrown.HighlightYellow("Test Results by Category:"))
	for category, count := range summary.Categories {
		categoryPassed := countPassedInCategory(summary.Results, category)
		categoryRate := float64(categoryPassed) / float64(count) * 100

		fmt.Printf("  %s: %s/%s %s\n",
			homegrown.Normal(category),
			homegrown.HighlightGreen(fmt.Sprintf("%d", categoryPassed)),
			homegrown.Normal(fmt.Sprintf("%d", count)),
			getColoredSuccessRate(categoryRate))
	}
	fmt.Println()

	// Failed tests details
	if summary.FailedTests > 0 {
		fmt.Println(homegrown.HighlightRed("Failed Tests:"))
		for _, result := range summary.Results {
			if !result.Success {
				fmt.Printf("  %s %s\n", homegrown.HighlightRed("✗"), homegrown.Normal(result.UseCase.GetName()))
				if result.Error != nil {
					fmt.Printf("    %s\n", homegrown.HighlightRed(result.Error.Error()))
				}
			}
		}
		fmt.Println()
	}

	// Performance insights
	if len(summary.Results) > 0 {
		slowestTest := findSlowestTest(summary.Results)
		fastestTest := findFastestTest(summary.Results)

		fmt.Println(homegrown.HighlightYellow("Performance Insights:"))
		fmt.Printf("  Slowest: %s %s\n",
			homegrown.Normal(slowestTest.UseCase.GetName()),
			homegrown.HighlightRed(fmt.Sprintf("%.2fms", float64(slowestTest.ExecutionTime.Nanoseconds())/1e6)))
		fmt.Printf("  Fastest: %s %s\n",
			homegrown.Normal(fastestTest.UseCase.GetName()),
			homegrown.HighlightGreen(fmt.Sprintf("%.2fms", float64(fastestTest.ExecutionTime.Nanoseconds())/1e6)))
	}

	fmt.Println()

	// Final verdict
	if summary.FailedTests == 0 {
		fmt.Println(homegrown.HighlightGreen("🎉 All tests passed! SyndrDB functionality is working correctly."))
	} else {
		fmt.Printf("%s %d test(s) failed. Please review the errors above.\n",
			homegrown.HighlightRed("⚠️"), summary.FailedTests)
	}
}

// getColoredSuccessRate returns color-coded success rate string
// This function follows the Single Responsibility Principle by handling only rate coloring
// Following SyndrDB comprehensive error handling, it provides visual success indicators
func getColoredSuccessRate(rate float64) string {
	rateStr := fmt.Sprintf("(%.1f%%)", rate)

	if rate >= 90.0 {
		return homegrown.HighlightGreen(rateStr)
	} else if rate >= 70.0 {
		return homegrown.HighlightYellow(rateStr)
	} else {
		return homegrown.HighlightRed(rateStr)
	}
}

// countPassedInCategory counts passed tests in a specific category
// This function follows the Single Responsibility Principle by handling only category counting
// Following SyndrDB comprehensive error handling, it accurately counts category results
func countPassedInCategory[T homegrown.UseCase](results []TestResult[T], category string) int {
	count := 0
	for _, result := range results {
		if result.UseCase.GetCategory() == category && result.Success {
			count++
		}
	}
	return count
}

// findSlowestTest finds the test with the longest execution time
// This function follows the Single Responsibility Principle by handling only performance analysis
// Following SyndrDB comprehensive error handling, it identifies performance outliers
func findSlowestTest[T homegrown.UseCase](results []TestResult[T]) TestResult[T] {
	if len(results) == 0 {
		return TestResult[T]{}
	}

	slowest := results[0]
	for _, result := range results[1:] {
		if result.ExecutionTime > slowest.ExecutionTime {
			slowest = result
		}
	}
	return slowest
}

// findFastestTest finds the test with the shortest execution time
// This function follows the Single Responsibility Principle by handling only performance analysis
// Following SyndrDB comprehensive error handling, it identifies performance benchmarks
func findFastestTest[T homegrown.UseCase](results []TestResult[T]) TestResult[T] {
	if len(results) == 0 {
		return TestResult[T]{}
	}

	fastest := results[0]
	for _, result := range results[1:] {
		if result.ExecutionTime < fastest.ExecutionTime {
			fastest = result
		}
	}
	return fastest
}

// forceCompleteStateReset performs aggressive cache clearing and state reset between tests
// This function ensures complete isolation between test executions to prevent cache conflicts
func forceCompleteStateReset() {
	// Force cleanup of all problematic bundles
	problematicBundles := []string{
		"documents_bundle", "test_bundle", "primary_bundle", "related_bundle",
		"Customer", "Customers", "Order", "Orders", "invalid_json_bundle",
		"duplicate_bundle_test", "empty_test_bundle", "custom_name_bundle_test",
		"nonexistent_doc_bundle", "performance_test_bundle", "integration_test_bundle",
		"concurrent_ops_bundle", "backup_restore_bundle", "delete_test_bundle",
		"schema_bundle_test", "wal_test", "multi_document_bundle", "old_bundle_name",
		"workflow_bundle_1", "workflow_bundle_2", "workflow_bundle_3",
		"index_verification_bundle", "index_test_bundle", "query_test_bundle",
		"update_test_bundle",
	}

	homegrown.ColorLogger.Debug("Performing complete state reset...")

	// Execute DELETE commands for each bundle
	for _, bundleName := range problematicBundles {
		deleteCommand := fmt.Sprintf("DELETE BUNDLE %s", bundleName)
		_, _ = homegrown.ExecuteClientCommand(deleteCommand) // Ignore errors during cleanup
	}

	// Add a small delay to ensure operations complete
	time.Sleep(100 * time.Millisecond)

	// Force garbage collection to clear any cached references
	runtime.GC()

	homegrown.ColorLogger.Debug("State reset complete")
}
