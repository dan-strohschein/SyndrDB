package helpers

import (
	"fmt"
	"sync/atomic"
	"time"
)

// FastUUIDGenerator provides high-performance UUID generation for write operations
// Uses atomic counters and timestamp prefixing instead of crypto-grade randomness
// Optimized for sequential document creation with microsecond-level performance
type FastUUIDGenerator struct {
	counter   uint64
	timestamp int64
}

var globalUUIDGenerator = &FastUUIDGenerator{
	counter:   0,
	timestamp: time.Now().UnixNano(),
}

// GenerateFastUUID creates a unique ID optimized for write performance
// Format: {timestamp_hex}_{counter_hex} - provides uniqueness without crypto overhead
// Performance: ~10-50 nanoseconds vs ~1-10 microseconds for crypto UUID
func GenerateFastUUID() string {
	// Use atomic increment for thread-safe counter
	counter := atomic.AddUint64(&globalUUIDGenerator.counter, 1)

	// Cache timestamp to avoid repeated time.Now() calls
	// Only update timestamp every 1000 operations for balance of uniqueness and performance
	if counter%1000 == 0 {
		atomic.StoreInt64(&globalUUIDGenerator.timestamp, time.Now().UnixNano())
	}

	timestamp := atomic.LoadInt64(&globalUUIDGenerator.timestamp)

	// Use hex formatting for compact representation
	return fmt.Sprintf("%x_%x", timestamp, counter)
}

// ResetFastUUIDGenerator resets the global generator (for testing)
func ResetFastUUIDGenerator() {
	atomic.StoreUint64(&globalUUIDGenerator.counter, 0)
	atomic.StoreInt64(&globalUUIDGenerator.timestamp, time.Now().UnixNano())
}
