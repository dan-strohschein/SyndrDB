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
	"sync/atomic"
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

// BundleWriteLogger tracks write operations for debugging using lock-free ring buffer
// Single Responsibility: Manages write operation history and diagnostics
// Uses atomic operations to avoid locks and deadlocks on the critical path
//
// PERFORMANCE: Disabled by default. When disabled, all methods are no-ops with zero overhead.
// Enable only when debugging corruption issues - stack trace capture is expensive (~4KB alloc per call).
type BundleWriteLogger struct {
	operations []WriteOperation // Fixed-size ring buffer
	writeIndex uint64           // Atomic counter for ring position
	logger     *zap.SugaredLogger
	maxOps     int
	enabled    int32 // Atomic flag: 0=disabled (default), 1=enabled
}

// NewBundleWriteLogger creates a new write logger
// PERFORMANCE: Logger is DISABLED by default to avoid overhead in production.
// Call Enable() to activate logging when debugging corruption issues.
// Parameters:
//   - logger: Logger for debug and error messages
//   - maxOps: Maximum number of operations to keep in history
//
// Returns:
//   - *BundleWriteLogger: The created logger instance (disabled by default)
func NewBundleWriteLogger(logger *zap.SugaredLogger, maxOps int) *BundleWriteLogger {
	return &BundleWriteLogger{
		operations: make([]WriteOperation, maxOps),
		writeIndex: 0,
		logger:     logger,
		maxOps:     maxOps,
		enabled:    0, // Disabled by default for production performance
	}
}

// Enable activates write logging for debugging corruption issues
// CAUTION: Enabling causes significant performance overhead due to stack trace capture
func (wl *BundleWriteLogger) Enable() {
	atomic.StoreInt32(&wl.enabled, 1)
	wl.logger.Warn("BundleWriteLogger ENABLED - performance will be degraded")
}

// Disable deactivates write logging to restore production performance
func (wl *BundleWriteLogger) Disable() {
	atomic.StoreInt32(&wl.enabled, 0)
	wl.logger.Info("BundleWriteLogger disabled - production performance restored")
}

// IsEnabled returns true if write logging is currently active
func (wl *BundleWriteLogger) IsEnabled() bool {
	return atomic.LoadInt32(&wl.enabled) == 1
}

// LogWriteStart records the start of a write operation
// Single Responsibility: Captures write operation initiation
// Lock-free: Uses atomic operations for zero contention
// PERFORMANCE: No-op when disabled (default) - zero overhead in production
//
// Parameters:
//   - bundleName: Name of the bundle being written to
//   - offset: File offset of the write
//   - expectedSize: Expected number of bytes to write
func (wl *BundleWriteLogger) LogWriteStart(bundleName string, offset int64, expectedSize int) {
	// PERFORMANCE: Early return when disabled - zero overhead in production
	if atomic.LoadInt32(&wl.enabled) == 0 {
		return
	}

	// Atomically get next slot in ring buffer
	idx := atomic.AddUint64(&wl.writeIndex, 1) - 1
	slot := int(idx % uint64(wl.maxOps))

	// Write directly to slot (no lock needed)
	wl.operations[slot] = WriteOperation{
		BundleName:   bundleName,
		Offset:       offset,
		ExpectedSize: expectedSize,
		StartTime:    time.Now(),
		StackTrace:   string(debug.Stack()),
	}

	wl.logger.Debugf("WRITE START: bundle=%s offset=%d expectedSize=%d",
		bundleName, offset, expectedSize)
}

// LogWriteEnd records the completion of a write operation
// Single Responsibility: Captures write operation completion and validates results
// Lock-free: Updates operation in place
// PERFORMANCE: No-op when disabled (default) - zero overhead in production
//
// Parameters:
//   - bundleName: Name of the bundle being written to
//   - offset: File offset of the write
//   - actualSize: Actual number of bytes written
//   - err: Error if write failed
func (wl *BundleWriteLogger) LogWriteEnd(bundleName string, offset int64, actualSize int, err error) {
	// PERFORMANCE: Early return when disabled - zero overhead in production
	if atomic.LoadInt32(&wl.enabled) == 0 {
		return
	}

	// Find matching operation by scanning recent entries
	currentIdx := atomic.LoadUint64(&wl.writeIndex)
	startScan := int(currentIdx) - wl.maxOps
	if startScan < 0 {
		startScan = 0
	}

	for i := int(currentIdx) - 1; i >= startScan; i-- {
		slot := i % wl.maxOps
		op := &wl.operations[slot]

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
				// Use Debug level for success to reduce log noise
				wl.logger.Debugf("WRITE SUCCESS: bundle=%s offset=%d size=%d duration=%v",
					bundleName, offset, actualSize, duration)
			}
			return
		}
	}

	wl.logger.Debugf("WRITE END WITHOUT START: bundle=%s offset=%d actual=%d",
		bundleName, offset, actualSize)
}

// GetRecentFailures returns recent failed write operations
// Single Responsibility: Provides diagnostic information for debugging
// Lock-free: Creates snapshot of current ring buffer state
//
// Returns:
//   - []WriteOperation: List of failed operations
func (wl *BundleWriteLogger) GetRecentFailures() []WriteOperation {
	// Create snapshot without locks
	snapshot := wl.getSnapshot()

	failures := make([]WriteOperation, 0)
	for _, op := range snapshot {
		if op.BundleName != "" && !op.Success && !op.EndTime.IsZero() {
			failures = append(failures, op)
		}
	}
	return failures
}

// getSnapshot creates a consistent snapshot of operations
// Lock-free: Copies ring buffer without blocking writers
func (wl *BundleWriteLogger) getSnapshot() []WriteOperation {
	snapshot := make([]WriteOperation, wl.maxOps)
	copy(snapshot, wl.operations)
	return snapshot
}

// DumpDiagnostics writes detailed diagnostics to file and halts server
// Single Responsibility: Handles critical failure response
// Lock-free: Takes snapshot first, then does I/O without blocking writers
//
// Parameters:
//   - reason: Description of the corruption detected
//   - offset: File offset where corruption was detected
//   - bundleName: Name of the corrupted bundle
func (wl *BundleWriteLogger) DumpDiagnostics(reason string, offset int64, bundleName string) {
	// Get snapshot without holding any locks
	snapshot := wl.getSnapshot()
	currentIdx := atomic.LoadUint64(&wl.writeIndex)

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
	fmt.Fprintf(f, "Write Index: %d\n", currentIdx)
	fmt.Fprintf(f, "==============================================\n\n")

	// Write recent operations
	opCount := 0
	for _, op := range snapshot {
		if op.BundleName != "" {
			opCount++
		}
	}

	fmt.Fprintf(f, "RECENT WRITE OPERATIONS (Last %d):\n", opCount)
	fmt.Fprintf(f, "==============================================\n")

	i := 0
	for _, op := range snapshot {
		if op.BundleName == "" {
			continue
		}
		i++
		fmt.Fprintf(f, "\nOperation #%d:\n", i)
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
	failures := make([]WriteOperation, 0)
	for _, op := range snapshot {
		if op.BundleName != "" && !op.Success && !op.EndTime.IsZero() {
			failures = append(failures, op)
		}
	}

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
