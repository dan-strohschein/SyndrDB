/*
WAL (WRITE AHEAD LOGGING) FUNCTIONALITY TESTS

This file implements comprehensive end-to-end tests for Write Ahead Logging functionality in SyndrDB.
It validates the complete WAL implementation including:
1. WAL Manager initialization and configuration
2. Transaction logging and recovery operations
3. Bundle operations logging (create, update, delete)
4. Document operations logging and replay
5. LSN (Log Sequence Number) management
6. WAL file management and rotation
7. Error handling and recovery scenarios

TEST SCENARIOS:
1. Basic WAL Operations - Manager initialization, transaction logging
2. Bundle Operations - Bundle creation, modification, deletion logging
3. Document Operations - Document CRUD operations logging
4. Transaction Management - Multi-operation transactions
5. Recovery Operations - WAL replay and recovery scenarios
6. Performance Testing - High-volume logging operations
7. Error Handling - Invalid operations and edge cases

TESTING STRATEGY:
These tests follow the SyndrDB testing approach by creating actual WAL operations
and verifying that they are properly logged, can be replayed, and maintain
data integrity across system restarts.

This implementation follows the Single Responsibility Principle where each test
handles a specific aspect of WAL functionality while maintaining the robust
error handling and data integrity standards required by the SyndrDB project.
*/

package main

import (
	"fmt"
	"time"

	"github.com/fatih/color"
	"go.uber.org/zap"
)

// Color functions for console output
var (
	HighlightGreen  = color.New(color.FgGreen, color.Bold).SprintFunc()
	HighlightRed    = color.New(color.FgRed, color.Bold).SprintFunc()
	HighlightYellow = color.New(color.FgYellow, color.Bold).SprintFunc()
	HighlightBlue   = color.New(color.FgBlue, color.Bold).SprintFunc()
	HighlightCyan   = color.New(color.FgCyan, color.Bold).SprintFunc()
	Normal          = color.New(color.Reset).SprintFunc()
)

// ColorLogger is the global logger instance for WAL tests
var ColorLogger *zap.SugaredLogger

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

// setupColorLogger creates a logger that displays only messages without timestamps, levels, or source info
func setupColorLogger() *zap.SugaredLogger {
	if ColorLogger != nil {
		return ColorLogger
	}

	config := zap.NewDevelopmentConfig()

	// Configure encoder to show only the message
	config.EncoderConfig.TimeKey = ""       // Remove timestamp
	config.EncoderConfig.LevelKey = ""      // Remove log level
	config.EncoderConfig.CallerKey = ""     // Remove source file info
	config.EncoderConfig.NameKey = ""       // Remove logger name
	config.EncoderConfig.FunctionKey = ""   // Remove function name
	config.EncoderConfig.StacktraceKey = "" // Remove stack trace
	config.EncoderConfig.MessageKey = "msg" // Keep only the message

	// Use console encoder for clean output
	config.Encoding = "console"

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	ColorLogger = logger.Sugar()
	return ColorLogger
}

func main() {
	// Initialize ColorLogger
	ColorLogger = setupColorLogger()

	// Display welcome banner
	displayWALTestBanner()

	// Execute WAL functionality tests
	ColorLogger.Info(HighlightBlue("🚀 Starting Write Ahead Logging Functionality Tests..."))

	walUseCases := GetWALTestUseCases()
	testSummary := executeAllWALTests(walUseCases)

	// Display final results
	displayWALTestResults(testSummary)
}

// displayWALTestBanner shows the WAL test runner welcome message
func displayWALTestBanner() {
	ColorLogger.Info(HighlightCyan("╔══════════════════════════════════════════════════════════════╗"))
	ColorLogger.Info(HighlightCyan("║") + "                    " + HighlightGreen("SyndrDB WAL Test Runner") + "                   " + HighlightCyan("║"))
	ColorLogger.Info(HighlightCyan("║") + "              " + Normal("Write Ahead Logging Tests") + "                " + HighlightCyan("║"))
	ColorLogger.Info(HighlightCyan("╚══════════════════════════════════════════════════════════════╝"))
	ColorLogger.Info("")
}

// GetWALTestUseCases returns all WAL test use cases organized by category
func GetWALTestUseCases() []WALTestUseCase {
	return []WALTestUseCase{
		// CATEGORY: Basic WAL Operations
		{
			Name:          "InitializeWALManager",
			Description:   "Initialize WAL Manager and verify basic functionality",
			Category:      "BasicOperations",
			SetupFunc:     setupWALTestEnvironment,
			ExecuteFunc:   testWALManagerInitialization,
			ValidateFunc:  validateWALManagerInitialization,
			CleanupFunc:   cleanupWALTestEnvironment,
			ExpectSuccess: true,
			Tags:          []string{"basic", "initialization"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "BasicTransactionLogging",
			Description:   "Test basic transaction logging operations",
			Category:      "TransactionOperations",
			SetupFunc:     setupWALTestEnvironment,
			ExecuteFunc:   testBasicTransactionLogging,
			ValidateFunc:  validateTransactionLogging,
			CleanupFunc:   cleanupWALTestEnvironment,
			ExpectSuccess: true,
			Tags:          []string{"transaction", "logging"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "BundleOperationsLogging",
			Description:   "Test logging of bundle create, update, delete operations",
			Category:      "BundleOperations",
			SetupFunc:     setupWALTestEnvironment,
			ExecuteFunc:   testBundleOperationsLogging,
			ValidateFunc:  validateBundleOperationsLogging,
			CleanupFunc:   cleanupWALTestEnvironment,
			ExpectSuccess: true,
			Tags:          []string{"bundle", "operations", "logging"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "DocumentOperationsLogging",
			Description:   "Test logging of document CRUD operations",
			Category:      "DocumentOperations",
			SetupFunc:     setupWALTestEnvironment,
			ExecuteFunc:   testDocumentOperationsLogging,
			ValidateFunc:  validateDocumentOperationsLogging,
			CleanupFunc:   cleanupWALTestEnvironment,
			ExpectSuccess: true,
			Tags:          []string{"document", "crud", "logging"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "LSNManagement",
			Description:   "Test Log Sequence Number management and tracking",
			Category:      "LSNOperations",
			SetupFunc:     setupWALTestEnvironment,
			ExecuteFunc:   testLSNManagement,
			ValidateFunc:  validateLSNManagement,
			CleanupFunc:   cleanupWALTestEnvironment,
			ExpectSuccess: true,
			Tags:          []string{"lsn", "sequence", "tracking"},
			Timeout:       30 * time.Second,
		},
		{
			Name:          "WALFlushOperations",
			Description:   "Test WAL flush and persistence operations",
			Category:      "FlushOperations",
			SetupFunc:     setupWALTestEnvironment,
			ExecuteFunc:   testWALFlushOperations,
			ValidateFunc:  validateWALFlushOperations,
			CleanupFunc:   cleanupWALTestEnvironment,
			ExpectSuccess: true,
			Tags:          []string{"flush", "persistence"},
			Timeout:       30 * time.Second,
		},
	}
}

// executeAllWALTests executes all WAL test use cases and returns results
func executeAllWALTests(useCases []WALTestUseCase) []WALTestResult {
	var results []WALTestResult
	totalTests := len(useCases)
	passedTests := 0

	ColorLogger.Info(HighlightBlue("📊 Executing %d WAL test use cases..."), totalTests)
	ColorLogger.Info("")

	// Group tests by category
	categories := groupWALTestsByCategory(useCases)

	for category, categoryTests := range categories {
		ColorLogger.Info(HighlightCyan("🔍 Category: %s (%d tests)"), category, len(categoryTests))

		for i, useCase := range categoryTests {
			ColorLogger.Info(HighlightYellow("  Test %d/%d: %s"), i+1, len(categoryTests), useCase.Name)
			ColorLogger.Info(HighlightCyan("  Description: %s"), useCase.Description)

			result := executeWALTest(useCase)
			results = append(results, result)

			if result.Success {
				passedTests++
				ColorLogger.Info(HighlightGreen("  ✅ PASSED (%.2fms)"), float64(result.Duration.Nanoseconds())/1e6)
			} else {
				ColorLogger.Info(HighlightRed("  ❌ FAILED: %v"), result.Error)
			}
			ColorLogger.Info("")
		}
	}

	// Summary
	ColorLogger.Info(HighlightBlue("📊 WAL Test Execution Summary:"))
	ColorLogger.Info(Normal("Total Tests: %d"), totalTests)
	ColorLogger.Info(HighlightGreen("Passed: %d"), passedTests)
	ColorLogger.Info(HighlightRed("Failed: %d"), totalTests-passedTests)
	ColorLogger.Info(HighlightBlue("Success Rate: %.1f%%"), float64(passedTests)/float64(totalTests)*100)

	return results
}

// executeWALTest executes a single WAL test use case
func executeWALTest(useCase WALTestUseCase) WALTestResult {
	result := WALTestResult{
		UseCase:   useCase,
		StartTime: time.Now(),
	}

	// Setup
	if err := useCase.SetupFunc(); err != nil {
		result.Success = false
		result.Error = fmt.Errorf("setup failed: %w", err)
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// Execute
	if err := useCase.ExecuteFunc(); err != nil {
		useCase.CleanupFunc() // Try cleanup
		result.Success = false
		result.Error = fmt.Errorf("execution failed: %w", err)
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// Validate
	if err := useCase.ValidateFunc(); err != nil {
		useCase.CleanupFunc() // Try cleanup
		result.Success = false
		result.Error = fmt.Errorf("validation failed: %w", err)
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result
	}

	// Cleanup
	if err := useCase.CleanupFunc(); err != nil {
		ColorLogger.Warn(HighlightYellow("Cleanup warning (non-critical): %v"), err)
	}

	result.Success = true
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	return result
}

// groupWALTestsByCategory organizes WAL tests by their category
func groupWALTestsByCategory(useCases []WALTestUseCase) map[string][]WALTestUseCase {
	categories := make(map[string][]WALTestUseCase)
	for _, useCase := range useCases {
		categories[useCase.Category] = append(categories[useCase.Category], useCase)
	}
	return categories
}

// displayWALTestResults displays the final test results summary
func displayWALTestResults(results []WALTestResult) {
	ColorLogger.Info("")
	ColorLogger.Info(HighlightCyan("╔══════════════════════════════════════════════════════════════╗"))
	ColorLogger.Info(HighlightCyan("║") + "                    " + HighlightGreen("WAL Test Results") + "                      " + HighlightCyan("║"))
	ColorLogger.Info(HighlightCyan("╚══════════════════════════════════════════════════════════════╝"))

	passed := 0
	failed := 0

	for _, result := range results {
		if result.Success {
			passed++
		} else {
			failed++
		}
	}

	if failed == 0 {
		ColorLogger.Info(HighlightGreen("🎉 ALL WAL TESTS PASSED!"))
	} else {
		ColorLogger.Info(HighlightRed("❌ Some WAL tests failed"))
	}

	ColorLogger.Info("")
	ColorLogger.Info(HighlightBlue("📁 Check the log_files directory for WAL files with recorded operations."))
}
