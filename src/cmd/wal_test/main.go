package main

/*
WAL Test Program

This simple test program verifies that the Write Ahead Logging functionality
is working correctly in SyndrDB. It tests WAL initialization, transaction
logging, and file operations.
*/

import (
	"fmt"
	"log"
	"syndrdb/src/internal/journal"

	"go.uber.org/zap"
)

func main() {
	// Create a simple logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	sugaredLogger := logger.Sugar()

	// Test WAL Manager initialization
	fmt.Println("🚀 Testing SyndrDB Write Ahead Logging...")

	walManager, err := journal.NewWALManager(sugaredLogger)
	if err != nil {
		log.Fatalf("Failed to create WAL Manager: %v", err)
	}
	defer walManager.Close()

	fmt.Println("✅ WAL Manager initialized successfully")

	// Test transaction operations
	err = walManager.ExecuteWithLogging(func(txID string) error {
		fmt.Printf("📝 Executing transaction: %s\n", txID)

		// Log a bundle creation
		err := walManager.LogBundleCreate(txID, "test_bundle", map[string]interface{}{
			"name":   "test_bundle",
			"fields": []string{"field1", "field2"},
		})
		if err != nil {
			return fmt.Errorf("failed to log bundle create: %w", err)
		}

		// Log a document insertion
		err = walManager.LogDocumentInsert(txID, "test_bundle", "doc123", map[string]interface{}{
			"field1": "value1",
			"field2": "value2",
		})
		if err != nil {
			return fmt.Errorf("failed to log document insert: %w", err)
		}

		// Log a document update
		err = walManager.LogDocumentUpdate(txID, "test_bundle", "doc123",
			map[string]interface{}{"field1": "value1"},
			map[string]interface{}{"field1": "updated_value1"})
		if err != nil {
			return fmt.Errorf("failed to log document update: %w", err)
		}

		// Log a document deletion
		err = walManager.LogDocumentDelete(txID, "test_bundle", "doc123", map[string]interface{}{
			"field1": "updated_value1",
			"field2": "value2",
		})
		if err != nil {
			return fmt.Errorf("failed to log document delete: %w", err)
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Transaction failed: %v", err)
	}

	fmt.Println("✅ Transaction completed successfully")

	// Force flush
	err = walManager.Flush()
	if err != nil {
		log.Fatalf("Failed to flush WAL: %v", err)
	}

	fmt.Printf("🎯 Current LSN: %d\n", walManager.GetCurrentLSN())
	fmt.Println("✅ WAL functionality test completed successfully!")
	fmt.Println("")
	fmt.Println("📁 Check the log_files directory for the WAL file with recorded operations.")
}
