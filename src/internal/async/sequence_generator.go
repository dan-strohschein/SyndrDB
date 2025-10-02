package async

import (
	"sync/atomic"
	"time"
)

// SequenceGenerator provides monotonic sequence numbers for ordering operations
// This ensures proper ordering in async operations and enables dependency tracking
type SequenceGenerator struct {
	counter uint64
}

// NewSequenceGenerator creates a new sequence generator starting from the current timestamp
// This helps with ordering across restarts and provides roughly chronological ordering
func NewSequenceGenerator() *SequenceGenerator {
	// Start with current timestamp in microseconds to ensure ordering across restarts
	startValue := uint64(time.Now().UnixMicro())

	return &SequenceGenerator{
		counter: startValue,
	}
}

// Next returns the next sequence number in a thread-safe manner
// Sequence numbers are guaranteed to be monotonically increasing
func (sg *SequenceGenerator) Next() uint64 {
	return atomic.AddUint64(&sg.counter, 1)
}

// Current returns the current sequence number without incrementing
// Useful for checking the latest sequence without consuming a number
func (sg *SequenceGenerator) Current() uint64 {
	return atomic.LoadUint64(&sg.counter)
}

// SetMinimum ensures the sequence generator starts from at least the given value
// This is useful during recovery to ensure we don't reuse sequence numbers
func (sg *SequenceGenerator) SetMinimum(minValue uint64) {
	for {
		current := atomic.LoadUint64(&sg.counter)
		if current >= minValue {
			break
		}

		// Try to set to minValue if current is still less than minValue
		if atomic.CompareAndSwapUint64(&sg.counter, current, minValue) {
			break
		}
		// If CAS failed, someone else updated it, try again
	}
}
