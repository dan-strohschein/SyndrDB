/*
BUNDLE WRITE OPERATION LOGGER

This file implements detailed logging for document write operations to help
debug incomplete writes and data corruption issues. It tracks write boundaries,
sizes, and offsets following PostgreSQL's approach to write operation auditing.

WRITE TRACKING:
- Logs write start with expected size and offset
- Logs write completion with actual bytes written
- Detects mismatches between expected and actual sizes
- Provides stack traces for debugging write paths

CORRUPTION DETECTION:
- Identifies incomplete writes
- Tracks failed write operations
- Provides detailed diagnostics for debugging

This follows the Single Responsibility Principle by focusing exclusively on
write operation logging and auditing.
*/

package bundlestore

import (
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"go.uber.org/zap"
)

// WriteOperation tracks a single write operation for verification
// This structure captures all details needed to debug write failures
type WriteOperation struct {
	BundleName   string
	Offset       int64
	ExpectedSize int
	ActualSize   int
	StartTime    time.Time
	EndTime      time.Time
	Success      bool
	ErrorMessage string
	StackTrace   string
}

// BundleWriteLogger tracks write operations for debugging
// Single Responsibility: Manages write operation history and diagnostics
type BundleWriteLogger struct {
	operations []WriteOperation
	mutex      sync.RWMutex
	logger     *zap.SugaredLogger
	maxOps     int // Keep last N operations
}

// NewBundleWriteLogger creates a new write logger
// Parameters:
//   - logger: Logger for debug and error messages
//   - maxOps: Maximum number of operations to keep in history
//
// Returns:
//   - *BundleWriteLogger: The created logger instance
func NewBundleWriteLogger(logger *zap.SugaredLogger, maxOps int) *BundleWriteLogger {
	return &BundleWriteLogger{
		operations: make([]WriteOperation, 0, maxOps),
		logger:     logger,
		maxOps:     maxOps,
	}
}

// LogWriteStart records the start of a write operation
// Single Responsibility: Captures write operation initiation
//
// Parameters:
//   - bundleName: Name of the bundle being written to
//   - offset: File offset of the write
//   - expectedSize: Expected number of bytes to write
func (wl *BundleWriteLogger) LogWriteStart(bundleName string, offset int64, expectedSize int) {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()

	op := WriteOperation{
		BundleName:   bundleName,
		Offset:       offset,
		ExpectedSize: expectedSize,
		StartTime:    time.Now(),
		StackTrace:   string(debug.Stack()),
	}

	wl.operations = append(wl.operations, op)
	if len(wl.operations) > wl.maxOps {
		wl.operations = wl.operations[1:] // Keep last N
	}

	wl.logger.Infof("WRITE START: bundle=%s offset=%d expectedSize=%d",
		bundleName, offset, expectedSize)
}

// LogWriteEnd records the completion of a write operation
// Single Responsibility: Captures write operation completion and validates results
//
// Parameters:
//   - bundleName: Name of the bundle being written to
//   - offset: File offset of the write
//   - actualSize: Actual number of bytes written
//   - err: Error if write failed
func (wl *BundleWriteLogger) LogWriteEnd(bundleName string, offset int64, actualSize int, err error) {
	wl.mutex.Lock()
	defer wl.mutex.Unlock()

	// Find matching operation
	for i := len(wl.operations) - 1; i >= 0; i-- {
		op := &wl.operations[i]
		if op.BundleName == bundleName && op.Offset == offset && op.EndTime.IsZero() {
			op.EndTime = time.Now()
			op.ActualSize = actualSize
			op.Success = (err == nil && op.ExpectedSize == actualSize)
			if err != nil {
				op.ErrorMessage = err.Error()
			}

			duration := op.EndTime.Sub(op.StartTime)

			if err != nil {
				wl.logger.Errorf("WRITE FAILED: bundle=%s offset=%d expected=%d actual=%d duration=%v error=%v",
					bundleName, offset, op.ExpectedSize, actualSize, duration, err)
			} else if op.ExpectedSize != actualSize {
				wl.logger.Warnf("WRITE SIZE MISMATCH: bundle=%s offset=%d expected=%d actual=%d duration=%v",
					bundleName, offset, op.ExpectedSize, actualSize, duration)
			} else {
				wl.logger.Infof("WRITE SUCCESS: bundle=%s offset=%d size=%d duration=%v",
					bundleName, offset, actualSize, duration)
			}
			return
		}
	}

	wl.logger.Warnf("WRITE END WITHOUT START: bundle=%s offset=%d actual=%d",
		bundleName, offset, actualSize)
}

// GetRecentFailures returns recent failed write operations
// Single Responsibility: Provides diagnostic information for debugging
//
// Returns:
//   - []WriteOperation: List of failed operations
func (wl *BundleWriteLogger) GetRecentFailures() []WriteOperation {
	wl.mutex.RLock()
	defer wl.mutex.RUnlock()

	failures := make([]WriteOperation, 0)
	for _, op := range wl.operations {
		if !op.Success && !op.EndTime.IsZero() {
			failures = append(failures, op)
		}
	}
	return failures
}

// DumpDiagnostics writes detailed diagnostics to file and halts server
// Single Responsibility: Handles critical failure response
//
// Parameters:
//   - reason: Description of the corruption detected
//   - offset: File offset where corruption was detected
//   - bundleName: Name of the corrupted bundle
func (wl *BundleWriteLogger) DumpDiagnostics(reason string, offset int64, bundleName string) {
	wl.mutex.RLock()
	defer wl.mutex.RUnlock()

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	diagnosticFile := fmt.Sprintf("corruption_diagnostics_%s.txt", timestamp)

	f, err := os.Create(diagnosticFile)
	if err != nil {
		wl.logger.Errorf("Failed to create diagnostic file: %v", err)
		return
	}
	defer f.Close()

	// Write header
	fmt.Fprintf(f, "==============================================\n")
	fmt.Fprintf(f, "CRITICAL: DATA CORRUPTION DETECTED\n")
	fmt.Fprintf(f, "==============================================\n")
	fmt.Fprintf(f, "Timestamp: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(f, "Bundle: %s\n", bundleName)
	fmt.Fprintf(f, "Offset: %d\n", offset)
	fmt.Fprintf(f, "Reason: %s\n", reason)
	fmt.Fprintf(f, "==============================================\n\n")

	// Write recent operations
	fmt.Fprintf(f, "RECENT WRITE OPERATIONS (Last %d):\n", len(wl.operations))
	fmt.Fprintf(f, "==============================================\n")
	for i, op := range wl.operations {
		fmt.Fprintf(f, "\nOperation #%d:\n", i+1)
		fmt.Fprintf(f, "  Bundle: %s\n", op.BundleName)
		fmt.Fprintf(f, "  Offset: %d\n", op.Offset)
		fmt.Fprintf(f, "  Expected Size: %d bytes\n", op.ExpectedSize)
		fmt.Fprintf(f, "  Actual Size: %d bytes\n", op.ActualSize)
		fmt.Fprintf(f, "  Start Time: %s\n", op.StartTime.Format(time.RFC3339Nano))
		if !op.EndTime.IsZero() {
			fmt.Fprintf(f, "  End Time: %s\n", op.EndTime.Format(time.RFC3339Nano))
			fmt.Fprintf(f, "  Duration: %v\n", op.EndTime.Sub(op.StartTime))
		} else {
			fmt.Fprintf(f, "  End Time: INCOMPLETE (never finished)\n")
		}
		fmt.Fprintf(f, "  Success: %v\n", op.Success)
		if op.ErrorMessage != "" {
			fmt.Fprintf(f, "  Error: %s\n", op.ErrorMessage)
		}
		if op.ExpectedSize != op.ActualSize && op.ActualSize > 0 {
			fmt.Fprintf(f, "  SIZE MISMATCH: %d bytes difference\n", op.ExpectedSize-op.ActualSize)
		}
		fmt.Fprintf(f, "  Stack Trace:\n%s\n", op.StackTrace)
		fmt.Fprintf(f, "----------------------------------------------\n")
	}

	// Write failures summary
	failures := wl.GetRecentFailures()
	fmt.Fprintf(f, "\n\nFAILED OPERATIONS SUMMARY:\n")
	fmt.Fprintf(f, "==============================================\n")
	fmt.Fprintf(f, "Total Failures: %d\n\n", len(failures))
	for i, fail := range failures {
		fmt.Fprintf(f, "Failure #%d: Bundle=%s Offset=%d Size=%d/%d Error=%s\n",
			i+1, fail.BundleName, fail.Offset, fail.ActualSize, fail.ExpectedSize, fail.ErrorMessage)
	}

	wl.logger.Errorf("==============================================")
	wl.logger.Errorf("CRITICAL: DATA CORRUPTION DETECTED")
	wl.logger.Errorf("==============================================")
	wl.logger.Errorf("Reason: %s", reason)
	wl.logger.Errorf("Bundle: %s", bundleName)
	wl.logger.Errorf("Offset: %d", offset)
	wl.logger.Errorf("Diagnostics written to: %s", diagnosticFile)
	wl.logger.Errorf("==============================================")
	wl.logger.Errorf("SERVER HALTING TO PREVENT FURTHER CORRUPTION")
	wl.logger.Errorf("==============================================")

	// Halt the server
	os.Exit(1)
}
