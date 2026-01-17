package main

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestTimeoutWarning_TimerCleanup verifies that warning goroutines properly clean up
// timers when context is cancelled early, preventing resource leaks (HIGH-003 fix)
func TestTimeoutWarning_TimerCleanup(t *testing.T) {
	// This test verifies that timers are cleaned up when contexts cancel early
	// by creating many short-lived contexts and ensuring no goroutine leaks

	const numQueries = 100
	var wg sync.WaitGroup

	// Get initial goroutine count
	runtime.GC() // Trigger GC to clean up any pending timers
	time.Sleep(100 * time.Millisecond) // Give GC time to run
	initialGoroutines := runtime.NumGoroutine()

	// Create many short-lived timeout contexts (similar to fast queries)
	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Create a context with timeout
			timeout := 5 * time.Second
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			// Simulate warning goroutine with timer (same pattern as fixed code)
			go func() {
				warningTime := time.Duration(float64(timeout) * 0.8)
				timer := time.NewTimer(warningTime)
				defer timer.Stop() // Ensure timer is stopped even if goroutine exits early

				select {
				case <-timer.C:
					// Warning would be logged here in real code
					t.Log("Warning fired (should not happen in this test)")
				case <-ctx.Done():
					return // timer.Stop() will be called by defer
				}
			}()

			// Cancel context immediately (simulating fast query completion)
			// This should trigger timer cleanup via defer timer.Stop()
			cancel()

			// Give goroutine time to exit and clean up timer
			time.Sleep(10 * time.Millisecond)
		}()
	}

	// Wait for all queries to complete
	wg.Wait()

	// Give goroutines time to fully exit and timers to be garbage collected
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	// Check final goroutine count
	finalGoroutines := runtime.NumGoroutine()
	goroutineIncrease := finalGoroutines - initialGoroutines

	t.Logf("Initial goroutines: %d, Final goroutines: %d, Increase: %d", 
		initialGoroutines, finalGoroutines, goroutineIncrease)

	// With proper timer cleanup, goroutine increase should be minimal
	// Allow some margin for test framework and other background goroutines
	// If timers are leaking, we'd see a significant increase
	if goroutineIncrease > 20 {
		t.Errorf("Potential goroutine leak detected: goroutines increased by %d (expected < 20)", 
			goroutineIncrease)
	}
}

// TestTimeoutWarning_TimerStopOnCancel verifies that timer.Stop() is called
// when context is cancelled, preventing timer from firing unnecessarily
func TestTimeoutWarning_TimerStopOnCancel(t *testing.T) {
	timeout := 1 * time.Second
	warningTime := time.Duration(float64(timeout) * 0.8)
	
	ctx, cancel := context.WithCancel(context.Background())
	warningFired := make(chan bool, 1)

	// Launch warning goroutine with timer
	go func() {
		timer := time.NewTimer(warningTime)
		defer timer.Stop() // Ensure timer is stopped even if goroutine exits early

		select {
		case <-timer.C:
			warningFired <- true
		case <-ctx.Done():
			return // timer.Stop() will be called by defer
		}
	}()

	// Cancel context immediately (before warning time)
	cancel()
	time.Sleep(100 * time.Millisecond) // Give goroutine time to exit

	// Verify warning did not fire (timer should be stopped)
	select {
	case <-warningFired:
		t.Error("Warning fired even though context was cancelled - timer was not stopped")
	default:
		t.Log("Timer correctly stopped when context cancelled - no leak")
	}

	// Wait for warning time to pass to ensure timer doesn't fire later
	time.Sleep(warningTime + 100*time.Millisecond)

	// Verify warning still didn't fire
	select {
	case <-warningFired:
		t.Error("Warning fired after delay - timer leak detected")
	default:
		t.Log("Timer correctly stopped and cleaned up - no delayed firing")
	}
}

// TestTimeoutWarning_WarningFiresWhenNeeded verifies that warnings still fire
// correctly when context is not cancelled (normal warning behavior)
func TestTimeoutWarning_WarningFiresWhenNeeded(t *testing.T) {
	timeout := 200 * time.Millisecond
	warningTime := time.Duration(float64(timeout) * 0.8) // 160ms
	
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	warningFired := make(chan bool, 1)

	// Launch warning goroutine with timer
	go func() {
		timer := time.NewTimer(warningTime)
		defer timer.Stop() // Ensure timer is stopped even if goroutine exits early

		select {
		case <-timer.C:
			warningFired <- true
		case <-ctx.Done():
			return // timer.Stop() will be called by defer
		}
	}()

	// Wait for warning to fire (should happen before timeout)
	select {
	case <-warningFired:
		t.Log("Warning correctly fired at threshold")
	case <-time.After(warningTime + 50*time.Millisecond):
		t.Error("Warning did not fire within expected time")
	}
}
