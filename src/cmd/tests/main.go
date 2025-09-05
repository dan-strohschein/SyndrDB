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
	"fmt"
	"time"

	"github.com/fatih/color"
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

// WALTestUseCase represents a single test case for WAL operations
type WALTestUseCase struct {
	Name          string
	Description   string
	Category      string
	SetupFunc     func() error
	ExecuteFunc   func() error
	ValidateFunc  func() error
	CleanupFunc   func() error
	ExpectSuccess bool
	Tags          []string
	Timeout       time.Duration
}

// WALTestResult represents the result of a single WAL test execution
type WALTestResult struct {
	UseCase   WALTestUseCase
	Success   bool
	Duration  time.Duration
	Error     error
	StartTime time.Time
	EndTime   time.Time
	Details   string
}

// UseCase defines the interface that all test use cases must implement
// Following SyndrDB comprehensive error handling, standardizes use case behavior
// This implementation follows the Single Responsibility Principle by defining only use case contract
type UseCase interface {
	GetName() string
	GetDescription() string
	GetCategory() string
	GetExpectSuccess() bool
	Setup() error
	Execute() error
	Validate() error
	Cleanup() error
}

// DatabaseCreationUseCase implements the UseCase interface with method receivers
func (d DatabaseCreationUseCase) GetName() string        { return d.Name }
func (d DatabaseCreationUseCase) GetDescription() string { return d.Description }
func (d DatabaseCreationUseCase) GetCategory() string    { return d.Category }
func (d DatabaseCreationUseCase) GetExpectSuccess() bool { return d.ExpectSuccess }
func (d DatabaseCreationUseCase) Setup() error {
	if d.SetupFunc != nil {
		return d.SetupFunc()
	}
	return nil
}
func (d DatabaseCreationUseCase) Execute() error {
	if d.ExecuteFunc != nil {
		return d.ExecuteFunc()
	}
	return nil
}
func (d DatabaseCreationUseCase) Validate() error {
	if d.ValidateFunc != nil {
		return d.ValidateFunc()
	}
	return nil
}
func (d DatabaseCreationUseCase) Cleanup() error {
	if d.CleanupFunc != nil {
		return d.CleanupFunc()
	}
	return nil
}

// BundleManagementUseCase implements the UseCase interface with method receivers

func (b BundleManagementUseCase) GetName() string        { return b.Name }
func (b BundleManagementUseCase) GetDescription() string { return b.Description }
func (b BundleManagementUseCase) GetCategory() string    { return b.Category }
func (b BundleManagementUseCase) GetExpectSuccess() bool { return b.ExpectSuccess }
func (b BundleManagementUseCase) Setup() error {
	if b.SetupFunc != nil {
		return b.SetupFunc()
	}
	return nil
}
func (b BundleManagementUseCase) Execute() error {
	if b.ExecuteFunc != nil {
		return b.ExecuteFunc()
	}
	return nil
}
func (b BundleManagementUseCase) Validate() error {
	if b.ValidateFunc != nil {
		return b.ValidateFunc()
	}
	return nil
}
func (b BundleManagementUseCase) Cleanup() error {
	if b.CleanupFunc != nil {
		return b.CleanupFunc()
	}
	return nil
}

// Color functions for console output
var (
	HighlightGreen  = color.New(color.FgGreen, color.Bold).SprintFunc()
	HighlightRed    = color.New(color.FgRed, color.Bold).SprintFunc()
	HighlightYellow = color.New(color.FgYellow, color.Bold).SprintFunc()
	HighlightBlue   = color.New(color.FgBlue, color.Bold).SprintFunc()
	HighlightCyan   = color.New(color.FgCyan, color.Bold).SprintFunc()
	Normal          = color.New(color.Reset).SprintFunc()
)

// ColorLogger is the global logger instance
var ColorLogger *zap.SugaredLogger

func main() {

	// Initialize logger
	ColorLogger = setupLogger()

	// Display welcome banner
	displayWelcomeBanner()

	// Stand up test database service
	ColorLogger.Info(HighlightBlue("Setting up test database service..."))
	_, _, err := StandupTestDatabaseService()
	if err != nil {
		ColorLogger.Error(HighlightRed("Failed to setup test database service"), zap.Error(err))
		return
	}

	ColorLogger.Info(HighlightGreen("✓ Test database service setup complete"))

	// Execute database creation use case tests
	ColorLogger.Info(HighlightBlue("Starting database creation use case tests..."))
	dbUseCases := GetDatabaseCreationUseCases()
	dbSummary := executeAllTests(dbUseCases)

	// Execute bundle management use case tests
	ColorLogger.Info(HighlightBlue("Starting bundle management use case tests..."))
	bundleUseCases := GetBundleManagementUseCases()
	bundleSummary := executeAllTests(bundleUseCases)

	// Execute JOIN functionality demonstration
	ColorLogger.Info(HighlightBlue("Starting JOIN functionality demonstration..."))
	err = RunJoinDemonstration(ColorLogger)
	if err != nil {
		ColorLogger.Error(HighlightRed("JOIN demonstration failed"), zap.Error(err))
	} else {
		ColorLogger.Info(HighlightGreen("✓ JOIN demonstration completed successfully"))
	}

	// Execute comprehensive end-to-end JOIN testing
	ColorLogger.Info(HighlightBlue("Starting comprehensive end-to-end JOIN testing..."))
	err = RunComprehensiveJoinTests()
	if err != nil {
		ColorLogger.Error(HighlightRed("Comprehensive JOIN tests failed"), zap.Error(err))
	} else {
		ColorLogger.Info(HighlightGreen("✓ Comprehensive JOIN tests completed successfully"))
	}

	// Execute ORDER BY functionality demo
	ColorLogger.Info(HighlightBlue("Starting ORDER BY functionality demo..."))
	err = RunOrderByDemo()
	if err != nil {
		ColorLogger.Error(HighlightRed("ORDER BY demo failed"), zap.Error(err))
	} else {
		ColorLogger.Info(HighlightGreen("✓ ORDER BY demo completed successfully"))
	}

	// Execute WAL functionality tests
	ColorLogger.Info(HighlightBlue("Starting Write Ahead Logging functionality tests..."))
	walUseCases := GetWALTestUseCases()
	walResults := executeAllWALTests(walUseCases)
	_ = walResults // Suppress unused variable warning

	// Display comprehensive test results
	ColorLogger.Info(HighlightBlue("Database Creation Test Results:"))
	displayTestSummaryGeneric(dbSummary)

	ColorLogger.Info(HighlightBlue("Bundle Management Test Results:"))
	displayTestSummaryGeneric(bundleSummary)

	ColorLogger.Info(HighlightBlue("Test execution complete"))
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

	config.Level, _ = zap.ParseAtomicLevel("Warn") // Set default log level to warn
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
	fmt.Println(HighlightCyan("╔══════════════════════════════════════════════════════════════╗"))
	fmt.Println(HighlightCyan("║") + "                    " + HighlightGreen("SyndrDB Test Runner") + "                    " + HighlightCyan("║"))
	fmt.Println(HighlightCyan("║") + "              " + Normal("Database Creation Use Cases") + "               " + HighlightCyan("║"))
	fmt.Println(HighlightCyan("╚══════════════════════════════════════════════════════════════╝"))
	fmt.Println()
}

// groupTestsByCategory organizes tests by their category
// This function follows the Single Responsibility Principle by handling only test grouping
// Following SyndrDB comprehensive error handling, it organizes tests for better execution flow
func groupTestsByCategory(useCases []DatabaseCreationUseCase) map[string][]DatabaseCreationUseCase {
	categories := make(map[string][]DatabaseCreationUseCase)

	for _, useCase := range useCases {
		if categories[useCase.Category] == nil {
			categories[useCase.Category] = make([]DatabaseCreationUseCase, 0)
		}
		categories[useCase.Category] = append(categories[useCase.Category], useCase)
	}

	return categories
}

// executeTestCaseGeneric runs a single test case with full lifecycle management for any use case type
// This function follows the Single Responsibility Principle by handling only single test execution
// Following SyndrDB comprehensive error handling, it manages test lifecycle with proper cleanup
func executeTestCaseGeneric[T UseCase](useCase T) TestResult[T] {
	startTime := time.Now()
	result := TestResult[T]{
		UseCase: useCase,
		Success: false,
	}

	ColorLogger.Debug(HighlightYellow(fmt.Sprintf("Starting test: %s", useCase.GetName())))

	// Execute test lifecycle
	defer func() {
		result.ExecutionTime = time.Since(startTime)

		// Always attempt cleanup
		if cleanupErr := useCase.Cleanup(); cleanupErr != nil {
			ColorLogger.Warn(HighlightYellow(fmt.Sprintf("Cleanup warning for %s: %v", useCase.GetName(), cleanupErr)))
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
func executeAllTests[T UseCase](useCases []T) TestSummary[T] {
	summary := TestSummary[T]{
		TotalTests: len(useCases),
		Results:    make([]TestResult[T], 0, len(useCases)),
		Categories: make(map[string]int),
	}

	startTime := time.Now()

	// Group tests by category for better organization
	categorizedTests := groupTestsByCategoryGeneric(useCases)

	for category, tests := range categorizedTests {
		ColorLogger.Infof(HighlightBlue(fmt.Sprintf("\n=== Testing Category: %s ===", category)))

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
func groupTestsByCategoryGeneric[T UseCase](useCases []T) map[string][]T {
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
func displayTestResultGeneric[T UseCase](result TestResult[T]) {
	duration := fmt.Sprintf("%.2fms", float64(result.ExecutionTime.Nanoseconds())/1e6)

	if result.Success {
		fmt.Printf("  %s %s %s %s\n",
			HighlightGreen("✓"),
			HighlightGreen("PASS"),
			Normal(result.UseCase.GetName()),
			HighlightBlue(fmt.Sprintf("(%s)", duration)))

		if result.UseCase.GetDescription() != "" {
			fmt.Printf("    %s\n", Normal(result.UseCase.GetDescription()))
		}
	} else {
		fmt.Printf("  %s %s %s %s\n",
			HighlightRed("✗"),
			HighlightRed("FAIL"),
			Normal(result.UseCase.GetName()),
			HighlightBlue(fmt.Sprintf("(%s)", duration)))

		if result.UseCase.GetDescription() != "" {
			fmt.Printf("    %s\n", Normal(result.UseCase.GetDescription()))
		}

		if result.Error != nil {
			fmt.Printf("    %s %s\n", HighlightRed("Error:"), Normal(result.Error.Error()))
		}

		if result.Details != "" {
			fmt.Printf("    %s %s\n", HighlightYellow("Details:"), Normal(result.Details))
		}
	}

	fmt.Println()
}

// displayTestSummary shows comprehensive test results with statistics
// This function follows the Single Responsibility Principle by handling only summary display
// Following SyndrDB comprehensive error handling, it provides complete test analysis
func displayTestSummaryGeneric[T UseCase](summary TestSummary[T]) {
	fmt.Println()
	fmt.Println(HighlightCyan("╔══════════════════════════════════════════════════════════════╗"))
	fmt.Println(HighlightCyan("║") + "                     " + HighlightGreen("Test Summary") + "                   		" + HighlightCyan("║"))
	fmt.Println(HighlightCyan("╚══════════════════════════════════════════════════════════════╝"))
	fmt.Println()

	// Overall statistics
	successRate := float64(summary.PassedTests) / float64(summary.TotalTests) * 100
	totalTime := fmt.Sprintf("%.2fs", summary.TotalTime.Seconds())

	fmt.Printf("Total Tests:     %s\n", HighlightBlue(fmt.Sprintf("%d", summary.TotalTests)))
	fmt.Printf("Passed:          %s\n", HighlightGreen(fmt.Sprintf("%d", summary.PassedTests)))
	fmt.Printf("Failed:          %s\n", HighlightRed(fmt.Sprintf("%d", summary.FailedTests)))
	fmt.Printf("Success Rate:    %s\n", getColoredSuccessRate(successRate))
	fmt.Printf("Total Time:      %s\n", HighlightBlue(totalTime))
	fmt.Println()

	// Category breakdown
	fmt.Println(HighlightYellow("Test Results by Category:"))
	for category, count := range summary.Categories {
		categoryPassed := countPassedInCategory(summary.Results, category)
		categoryRate := float64(categoryPassed) / float64(count) * 100

		fmt.Printf("  %s: %s/%s %s\n",
			Normal(category),
			HighlightGreen(fmt.Sprintf("%d", categoryPassed)),
			Normal(fmt.Sprintf("%d", count)),
			getColoredSuccessRate(categoryRate))
	}
	fmt.Println()

	// Failed tests details
	if summary.FailedTests > 0 {
		fmt.Println(HighlightRed("Failed Tests:"))
		for _, result := range summary.Results {
			if !result.Success {
				fmt.Printf("  %s %s\n", HighlightRed("✗"), Normal(result.UseCase.GetName()))
				if result.Error != nil {
					fmt.Printf("    %s\n", HighlightRed(result.Error.Error()))
				}
			}
		}
		fmt.Println()
	}

	// Performance insights
	if len(summary.Results) > 0 {
		slowestTest := findSlowestTest(summary.Results)
		fastestTest := findFastestTest(summary.Results)

		fmt.Println(HighlightYellow("Performance Insights:"))
		fmt.Printf("  Slowest: %s %s\n",
			Normal(slowestTest.UseCase.GetName()),
			HighlightRed(fmt.Sprintf("%.2fms", float64(slowestTest.ExecutionTime.Nanoseconds())/1e6)))
		fmt.Printf("  Fastest: %s %s\n",
			Normal(fastestTest.UseCase.GetName()),
			HighlightGreen(fmt.Sprintf("%.2fms", float64(fastestTest.ExecutionTime.Nanoseconds())/1e6)))
	}

	fmt.Println()

	// Final verdict
	if summary.FailedTests == 0 {
		fmt.Println(HighlightGreen("🎉 All tests passed! Database creation functionality is working correctly."))
	} else {
		fmt.Printf("%s %d test(s) failed. Please review the errors above.\n",
			HighlightRed("⚠️"), summary.FailedTests)
	}
}

// getColoredSuccessRate returns color-coded success rate string
// This function follows the Single Responsibility Principle by handling only rate coloring
// Following SyndrDB comprehensive error handling, it provides visual success indicators
func getColoredSuccessRate(rate float64) string {
	rateStr := fmt.Sprintf("(%.1f%%)", rate)

	if rate >= 90.0 {
		return HighlightGreen(rateStr)
	} else if rate >= 70.0 {
		return HighlightYellow(rateStr)
	} else {
		return HighlightRed(rateStr)
	}
}

// countPassedInCategory counts passed tests in a specific category
// This function follows the Single Responsibility Principle by handling only category counting
// Following SyndrDB comprehensive error handling, it accurately counts category results
func countPassedInCategory[T UseCase](results []TestResult[T], category string) int {
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
func findSlowestTest[T UseCase](results []TestResult[T]) TestResult[T] {
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
func findFastestTest[T UseCase](results []TestResult[T]) TestResult[T] {
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
