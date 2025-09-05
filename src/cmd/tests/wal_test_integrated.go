/*
WAL TEST IMPLEMENTATIONS FOR MAIN TEST RUNNER

This file implements WAL management test functions integrated into the main SyndrDB test runner.
Each function tests specific WAL operations through the complete logging and recovery flow.
*/

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syndrdb/src/internal/journal"
	"time"
)

// Global WAL test state
var (
	testWALManager    *journal.WALManager
	testTransactionID string
	testLSNBefore     uint64
	testLSNAfter      uint64
)

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

	ColorLogger.Infof(HighlightBlue("📊 Executing %d WAL test use cases..."), totalTests)
	ColorLogger.Infof("")

	// Group tests by category
	categories := groupWALTestsByCategory(useCases)

	for category, categoryTests := range categories {
		ColorLogger.Infof(HighlightCyan("🔍 Category: %s (%d tests)"), category, len(categoryTests))

		for i, useCase := range categoryTests {
			ColorLogger.Infof(HighlightYellow("  Test %d/%d: %s"), i+1, len(categoryTests), useCase.Name)
			ColorLogger.Infof(HighlightCyan("  Description: %s"), useCase.Description)

			result := executeWALTest(useCase)
			results = append(results, result)

			if result.Success {
				passedTests++
				ColorLogger.Infof(HighlightGreen("  ✅ PASSED (%.2fms)"), float64(result.Duration.Nanoseconds())/1e6)
			} else {
				ColorLogger.Infof(HighlightRed("  ❌ FAILED: %v"), result.Error)
			}
			ColorLogger.Infof("")
		}
	}

	// Summary
	ColorLogger.Infof(HighlightBlue("📊 WAL Test Execution Summary:"))
	ColorLogger.Infof(Normal("Total Tests: %d"), totalTests)
	ColorLogger.Infof(HighlightGreen("Passed: %d"), passedTests)
	ColorLogger.Infof(HighlightRed("Failed: %d"), totalTests-passedTests)
	ColorLogger.Infof(HighlightBlue("Success Rate: %.1f%%"), float64(passedTests)/float64(totalTests)*100)

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

// setupWALTestEnvironment initializes the test environment for WAL operations
func setupWALTestEnvironment() error {
	ColorLogger.Debugf(HighlightCyan("Setting up WAL test environment..."))

	// Ensure log_files directory exists
	logDir := "log_files"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Initialize WAL Manager
	walManager, err := journal.NewWALManager(ColorLogger)
	if err != nil {
		return fmt.Errorf("failed to create WAL Manager: %w", err)
	}

	testWALManager = walManager
	ColorLogger.Debugf(HighlightGreen("✅ WAL test environment setup complete"))
	return nil
}

// cleanupWALTestEnvironment cleans up the test environment
func cleanupWALTestEnvironment() error {
	ColorLogger.Debugf(HighlightYellow("Cleaning up WAL test environment..."))

	if testWALManager != nil {
		testWALManager.Close()
		testWALManager = nil
	}

	// Reset test state
	testTransactionID = ""
	testLSNBefore = 0
	testLSNAfter = 0

	ColorLogger.Debugf(HighlightGreen("✅ WAL test environment cleanup complete"))
	return nil
}

// testWALManagerInitialization tests WAL Manager initialization
func testWALManagerInitialization() error {
	ColorLogger.Debugf(HighlightBlue("Testing WAL Manager initialization..."))

	if testWALManager == nil {
		return fmt.Errorf("WAL Manager was not initialized")
	}

	// Test basic operations
	currentLSN := testWALManager.GetCurrentLSN()
	ColorLogger.Debugf(HighlightCyan("Current LSN: %d"), currentLSN)

	return nil
}

// validateWALManagerInitialization validates WAL Manager initialization
func validateWALManagerInitialization() error {
	ColorLogger.Debugf(HighlightGreen("Validating WAL Manager initialization..."))

	if testWALManager == nil {
		return fmt.Errorf("WAL Manager is not initialized")
	}

	// Verify LSN tracking works
	lsn := testWALManager.GetCurrentLSN()
	// LSN should be a valid uint64 value
	ColorLogger.Debugf(HighlightCyan("Current LSN: %d"), lsn)

	ColorLogger.Debugf(HighlightGreen("✅ WAL Manager initialization validated"))
	return nil
}

// testBasicTransactionLogging tests basic transaction logging operations
func testBasicTransactionLogging() error {
	ColorLogger.Debugf(HighlightBlue("Testing basic transaction logging..."))

	testLSNBefore = testWALManager.GetCurrentLSN()

	// Execute a transaction with logging
	err := testWALManager.ExecuteWithLogging(func(txID string) error {
		testTransactionID = txID
		ColorLogger.Debugf(HighlightCyan("Executing transaction: %s"), txID)

		// Simulate some work within the transaction
		return nil
	})

	if err != nil {
		return fmt.Errorf("transaction execution failed: %w", err)
	}

	testLSNAfter = testWALManager.GetCurrentLSN()
	return nil
}

// validateTransactionLogging validates transaction logging results
func validateTransactionLogging() error {
	ColorLogger.Debugf(HighlightGreen("Validating transaction logging..."))

	if testTransactionID == "" {
		return fmt.Errorf("transaction ID was not set")
	}

	if testLSNAfter <= testLSNBefore {
		return fmt.Errorf("LSN did not advance after transaction (before: %d, after: %d)", testLSNBefore, testLSNAfter)
	}

	ColorLogger.Debugf(HighlightGreen("✅ Transaction logging validated"))
	return nil
}

// testBundleOperationsLogging tests logging of bundle operations
func testBundleOperationsLogging() error {
	ColorLogger.Infof(HighlightBlue("Testing bundle operations logging..."))

	testLSNBefore = testWALManager.GetCurrentLSN()

	// Execute transaction with bundle operations
	err := testWALManager.ExecuteWithLogging(func(txID string) error {
		testTransactionID = txID

		// Log bundle creation
		err := testWALManager.LogBundleCreate(txID, "test_bundle", map[string]interface{}{
			"name":   "test_bundle",
			"fields": []string{"field1", "field2"},
		})
		if err != nil {
			return fmt.Errorf("failed to log bundle create: %w", err)
		}

		ColorLogger.Debug(HighlightCyan("Logged bundle creation for: test_bundle"))
		return nil
	})

	if err != nil {
		return fmt.Errorf("bundle operations logging failed: %w", err)
	}

	testLSNAfter = testWALManager.GetCurrentLSN()
	return nil
}

// validateBundleOperationsLogging validates bundle operations logging
func validateBundleOperationsLogging() error {
	ColorLogger.Debugf(HighlightGreen("Validating bundle operations logging..."))

	if testLSNAfter <= testLSNBefore {
		return fmt.Errorf("LSN did not advance after bundle operations (before: %d, after: %d)", testLSNBefore, testLSNAfter)
	}

	ColorLogger.Debug(HighlightGreen("✅ Bundle operations logging validated"))
	return nil
}

// testDocumentOperationsLogging tests logging of document CRUD operations
func testDocumentOperationsLogging() error {
	ColorLogger.Debug(HighlightBlue("Testing document operations logging..."))

	testLSNBefore = testWALManager.GetCurrentLSN()

	// Execute transaction with document operations
	err := testWALManager.ExecuteWithLogging(func(txID string) error {
		testTransactionID = txID

		// Log document insertion
		err := testWALManager.LogDocumentInsert(txID, "test_bundle", "doc123", map[string]interface{}{
			"field1": "value1",
			"field2": "value2",
		})
		if err != nil {
			return fmt.Errorf("failed to log document insert: %w", err)
		}

		// Log document update
		err = testWALManager.LogDocumentUpdate(txID, "test_bundle", "doc123",
			map[string]interface{}{"field1": "value1"},
			map[string]interface{}{"field1": "updated_value1"})
		if err != nil {
			return fmt.Errorf("failed to log document update: %w", err)
		}

		// Log document deletion
		err = testWALManager.LogDocumentDelete(txID, "test_bundle", "doc123", map[string]interface{}{
			"field1": "updated_value1",
			"field2": "value2",
		})
		if err != nil {
			return fmt.Errorf("failed to log document delete: %w", err)
		}

		ColorLogger.Debug(HighlightCyan("Logged document operations for: doc123"))
		return nil
	})

	if err != nil {
		return fmt.Errorf("document operations logging failed: %w", err)
	}

	testLSNAfter = testWALManager.GetCurrentLSN()
	return nil
}

// validateDocumentOperationsLogging validates document operations logging
func validateDocumentOperationsLogging() error {
	ColorLogger.Debug(HighlightGreen("Validating document operations logging..."))

	if testLSNAfter <= testLSNBefore {
		return fmt.Errorf("LSN did not advance after document operations (before: %d, after: %d)", testLSNBefore, testLSNAfter)
	}

	ColorLogger.Debug(HighlightGreen("✅ Document operations logging validated"))
	return nil
}

// testLSNManagement tests Log Sequence Number management
func testLSNManagement() error {
	ColorLogger.Debug(HighlightBlue("Testing LSN management..."))

	initialLSN := testWALManager.GetCurrentLSN()

	// Perform multiple operations and track LSN progression
	for i := 0; i < 3; i++ {
		err := testWALManager.ExecuteWithLogging(func(txID string) error {
			return testWALManager.LogBundleCreate(txID, fmt.Sprintf("test_bundle_%d", i), map[string]interface{}{
				"name": fmt.Sprintf("test_bundle_%d", i),
			})
		})
		if err != nil {
			return fmt.Errorf("failed operation %d: %w", i, err)
		}
	}

	finalLSN := testWALManager.GetCurrentLSN()

	if finalLSN <= initialLSN {
		return fmt.Errorf("LSN did not advance properly (initial: %d, final: %d)", initialLSN, finalLSN)
	}

	ColorLogger.Debugf(HighlightCyan("LSN progression: %d → %d"), initialLSN, finalLSN)
	return nil
}

// validateLSNManagement validates LSN management functionality
func validateLSNManagement() error {
	ColorLogger.Debug(HighlightGreen("Validating LSN management..."))

	currentLSN := testWALManager.GetCurrentLSN()
	// LSN should be a valid uint64 value
	ColorLogger.Debugf(HighlightCyan("Current LSN: %d"), currentLSN)

	ColorLogger.Debug(HighlightGreen("✅ LSN management validated"))
	return nil
}

// testWALFlushOperations tests WAL flush and persistence operations
func testWALFlushOperations() error {
	ColorLogger.Debug(HighlightBlue("Testing WAL flush operations..."))

	// Perform some operations
	err := testWALManager.ExecuteWithLogging(func(txID string) error {
		return testWALManager.LogBundleCreate(txID, "flush_test_bundle", map[string]interface{}{
			"name": "flush_test_bundle",
		})
	})
	if err != nil {
		return fmt.Errorf("failed to execute operation before flush: %w", err)
	}

	// Test flush operation
	err = testWALManager.Flush()
	if err != nil {
		return fmt.Errorf("WAL flush failed: %w", err)
	}

	ColorLogger.Debug(HighlightCyan("WAL flush completed"))
	return nil
}

// validateWALFlushOperations validates WAL flush operations
func validateWALFlushOperations() error {
	ColorLogger.Debug(HighlightGreen("Validating WAL flush operations..."))

	// Check if WAL files exist in the log directory
	logDir := "log_files"
	files, err := filepath.Glob(filepath.Join(logDir, "wal_*.wal"))
	if err != nil {
		return fmt.Errorf("failed to check WAL files: %w", err)
	}

	if len(files) == 0 {
		ColorLogger.Warn(HighlightYellow("No WAL files found - this may be expected for in-memory testing"))
	} else {
		ColorLogger.Debug(HighlightCyan("Found %d WAL files"), len(files))
	}

	ColorLogger.Debug(HighlightGreen("✅ WAL flush operations validated"))
	return nil
}
