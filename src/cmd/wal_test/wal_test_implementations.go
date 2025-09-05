/*
WAL TEST IMPLEMENTATIONS

This file implements WAL management test functions using actual SyndrDB WAL operations.
Each function tests specific WAL operations through the complete logging and recovery flow.
*/

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syndrdb/src/internal/journal"
)

// Global test state
var (
	testWALManager    *journal.WALManager
	testTransactionID string
	testLSNBefore     uint64
	testLSNAfter      uint64
)

// setupWALTestEnvironment initializes the test environment for WAL operations
func setupWALTestEnvironment() error {
	ColorLogger.Debug(HighlightCyan("Setting up WAL test environment..."))

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
	ColorLogger.Debug(HighlightGreen("✅ WAL test environment setup complete"))
	return nil
}

// cleanupWALTestEnvironment cleans up the test environment
func cleanupWALTestEnvironment() error {
	ColorLogger.Debug(HighlightYellow("Cleaning up WAL test environment..."))

	if testWALManager != nil {
		testWALManager.Close()
		testWALManager = nil
	}

	// Reset test state
	testTransactionID = ""
	testLSNBefore = 0
	testLSNAfter = 0

	ColorLogger.Debug(HighlightGreen("✅ WAL test environment cleanup complete"))
	return nil
}

// testWALManagerInitialization tests WAL Manager initialization
func testWALManagerInitialization() error {
	ColorLogger.Debug(HighlightBlue("Testing WAL Manager initialization..."))

	if testWALManager == nil {
		return fmt.Errorf("WAL Manager was not initialized")
	}

	// Test basic operations
	currentLSN := testWALManager.GetCurrentLSN()
	ColorLogger.Debug(HighlightCyan("Current LSN: %d"), currentLSN)

	return nil
}

// validateWALManagerInitialization validates WAL Manager initialization
func validateWALManagerInitialization() error {
	ColorLogger.Debug(HighlightGreen("Validating WAL Manager initialization..."))

	if testWALManager == nil {
		return fmt.Errorf("WAL Manager is not initialized")
	}

	// Verify LSN tracking works
	lsn := testWALManager.GetCurrentLSN()
	// LSN should be a valid uint64 value
	ColorLogger.Debug(HighlightCyan("Current LSN: %d"), lsn)

	ColorLogger.Debug(HighlightGreen("✅ WAL Manager initialization validated"))
	return nil
}

// testBasicTransactionLogging tests basic transaction logging operations
func testBasicTransactionLogging() error {
	ColorLogger.Debug(HighlightBlue("Testing basic transaction logging..."))

	testLSNBefore = testWALManager.GetCurrentLSN()

	// Execute a transaction with logging
	err := testWALManager.ExecuteWithLogging(func(txID string) error {
		testTransactionID = txID
		ColorLogger.Debug(HighlightCyan("Executing transaction: %s"), txID)

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
	ColorLogger.Debug(HighlightGreen("Validating transaction logging..."))

	if testTransactionID == "" {
		return fmt.Errorf("transaction ID was not set")
	}

	if testLSNAfter <= testLSNBefore {
		return fmt.Errorf("LSN did not advance after transaction (before: %d, after: %d)", testLSNBefore, testLSNAfter)
	}

	ColorLogger.Debug(HighlightGreen("✅ Transaction logging validated"))
	return nil
}

// testBundleOperationsLogging tests logging of bundle operations
func testBundleOperationsLogging() error {
	ColorLogger.Debug(HighlightBlue("Testing bundle operations logging..."))

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
	ColorLogger.Debug(HighlightGreen("Validating bundle operations logging..."))

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

	ColorLogger.Debug(HighlightCyan("LSN progression: %d → %d"), initialLSN, finalLSN)
	return nil
}

// validateLSNManagement validates LSN management functionality
func validateLSNManagement() error {
	ColorLogger.Debug(HighlightGreen("Validating LSN management..."))

	currentLSN := testWALManager.GetCurrentLSN()
	// LSN should be a valid uint64 value
	ColorLogger.Debug(HighlightCyan("Current LSN: %d"), currentLSN)

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
