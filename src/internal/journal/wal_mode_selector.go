package journal

import (
	"fmt"

	"syndrdb/src/pkg/settings"
)

// WALModeSelector manages switching between sync and async WAL modes
// This provides the safety mechanism for fallback to sync WAL
type WALModeSelector struct {
	syncAdapter  *SyncWALAdapter
	asyncAdapter *AsyncWALAdapter
	currentMode  WALModeInterface
	settings     *settings.Arguments
}

// NewWALModeSelector creates a new WAL mode selector with both adapters
func NewWALModeSelector(walManager *WALManager, settings *settings.Arguments) (*WALModeSelector, error) {
	// Always create sync adapter as fallback
	syncAdapter := NewSyncWALAdapter(walManager)

	selector := &WALModeSelector{
		syncAdapter: syncAdapter,
		settings:    settings,
	}

	// Create async adapter if async mode is enabled
	if settings.WALMode == "async" {
		asyncAdapter, err := NewAsyncWALAdapter(walManager, settings.AsyncWALWorkers, settings.AsyncWALQueueSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create async WAL adapter: %w", err)
		}
		selector.asyncAdapter = asyncAdapter

		// Start async adapter
		if err := asyncAdapter.Start(); err != nil {
			return nil, fmt.Errorf("failed to start async WAL adapter: %w", err)
		}

		selector.currentMode = asyncAdapter
	} else {
		// Default to sync mode
		selector.currentMode = syncAdapter
	}

	return selector, nil
}

// GetCurrentMode returns the currently active WAL mode interface
func (wms *WALModeSelector) GetCurrentMode() WALModeInterface {
	return wms.currentMode
}

// SetSequenceMinimumAfterRecovery sets the async adapter's sequence generator minimum
// to at least (currentLSN + 1) so new operations get sequences above recovered WAL state.
// Call this after WAL replay during startup when using async mode.
func (wms *WALModeSelector) SetSequenceMinimumAfterRecovery(walManager *WALManager) {
	if wms.asyncAdapter == nil {
		return
	}
	lsn := walManager.GetCurrentLSN()
	wms.asyncAdapter.GetSequenceGenerator().SetMinimum(lsn + 1)
}

// SwitchMode switches between sync and async WAL modes
// This provides runtime switching capability for safety
func (wms *WALModeSelector) SwitchMode(mode string) error {
	switch mode {
	case "sync":
		return wms.switchToSync()
	case "async":
		return wms.switchToAsync()
	default:
		return fmt.Errorf("unsupported WAL mode: %s", mode)
	}
}

// switchToSync switches to synchronous WAL mode
func (wms *WALModeSelector) switchToSync() error {
	// If already in sync mode, nothing to do
	if wms.currentMode == wms.syncAdapter {
		return nil
	}

	// Stop async adapter if running
	if wms.asyncAdapter != nil {
		if err := wms.asyncAdapter.Close(); err != nil {
			return fmt.Errorf("failed to close async WAL adapter: %w", err)
		}
		wms.asyncAdapter = nil
	}

	// Switch to sync adapter
	wms.currentMode = wms.syncAdapter
	wms.settings.WALMode = "sync"

	return nil
}

// switchToAsync switches to asynchronous WAL mode
func (wms *WALModeSelector) switchToAsync() error {
	// If already in async mode, nothing to do
	if wms.asyncAdapter != nil && wms.currentMode == wms.asyncAdapter {
		return nil
	}

	// Create new async adapter if it doesn't exist
	if wms.asyncAdapter == nil {
		// Get WAL manager from sync adapter
		walManager := wms.syncAdapter.walManager

		asyncAdapter, err := NewAsyncWALAdapter(walManager, wms.settings.AsyncWALWorkers, wms.settings.AsyncWALQueueSize)
		if err != nil {
			return fmt.Errorf("failed to create async WAL adapter: %w", err)
		}

		// Start async adapter
		if err := asyncAdapter.Start(); err != nil {
			return fmt.Errorf("failed to start async WAL adapter: %w", err)
		}

		wms.asyncAdapter = asyncAdapter
	}

	// Switch to async adapter
	wms.currentMode = wms.asyncAdapter
	wms.settings.WALMode = "async"

	return nil
}

// IsAsync returns true if currently using async WAL mode
func (wms *WALModeSelector) IsAsync() bool {
	return wms.currentMode.IsAsync()
}

// GetMode returns the current WAL mode string
func (wms *WALModeSelector) GetMode() string {
	return wms.currentMode.GetMode()
}

// Close gracefully shuts down all WAL adapters
func (wms *WALModeSelector) Close() error {
	var lastErr error

	// Close async adapter if it exists
	if wms.asyncAdapter != nil {
		if err := wms.asyncAdapter.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close async WAL adapter: %w", err)
		}
	}

	// Close sync adapter (this closes the underlying WAL manager)
	if err := wms.syncAdapter.Close(); err != nil {
		if lastErr != nil {
			return fmt.Errorf("multiple close errors - async: %v, sync: %w", lastErr, err)
		}
		return fmt.Errorf("failed to close sync WAL adapter: %w", err)
	}

	return lastErr
}

// WriteEntry delegates to the current WAL mode
func (wms *WALModeSelector) WriteEntry(entry WALEntry) error {
	return wms.currentMode.WriteEntry(entry)
}

// WriteEntries delegates to the current WAL mode
func (wms *WALModeSelector) WriteEntries(entries []WALEntry) error {
	return wms.currentMode.WriteEntries(entries)
}

// Flush delegates to the current WAL mode
func (wms *WALModeSelector) Flush() error {
	return wms.currentMode.Flush()
}

// GetMetrics returns metrics from the current WAL mode
// This is an extension to provide insight into async vs sync performance
func (wms *WALModeSelector) GetMetrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"mode":     wms.GetMode(),
		"is_async": wms.IsAsync(),
	}

	// Add async-specific metrics if available
	if wms.asyncAdapter != nil && wms.currentMode == wms.asyncAdapter {
		// TODO: Add async-specific metrics once available in the async infrastructure
		metrics["async_workers"] = wms.settings.AsyncWALWorkers
		metrics["async_queue_size"] = wms.settings.AsyncWALQueueSize
	}

	return metrics
}

// SafeShutdown performs a safe shutdown with fallback to sync mode
// This ensures no data loss during shutdown
func (wms *WALModeSelector) SafeShutdown() error {
	// If in async mode, switch to sync first to ensure all operations complete
	if wms.IsAsync() {
		// Flush any pending async operations
		if err := wms.Flush(); err != nil {
			return fmt.Errorf("failed to flush async operations during safe shutdown: %w", err)
		}

		// Switch to sync mode to ensure stability
		if err := wms.switchToSync(); err != nil {
			return fmt.Errorf("failed to switch to sync mode during safe shutdown: %w", err)
		}
	}

	// Final flush in sync mode
	if err := wms.Flush(); err != nil {
		return fmt.Errorf("failed to final flush during safe shutdown: %w", err)
	}

	// Close everything
	return wms.Close()
}
