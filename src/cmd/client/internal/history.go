package internal

import (
	"encoding/gob"
	"os"
	"strings"
)

/*
Command History Ring Buffer Module

This module manages persistent command history for the SyndrDB client using a
gob-encoded ring buffer for compact binary storage.

Design Philosophy:
- Single Responsibility: Only handles history storage and retrieval
- DRY: Reuses Go's standard gob encoding for serialization
- Open/Closed: Easy to extend with additional history features
- Performance: O(1) insertions, compact binary format

Architecture:
Uses a circular buffer (ring buffer) to maintain a fixed-size command history.
When capacity is reached, oldest entries are overwritten. History is persisted
to disk in binary format using Go's gob encoding for space efficiency.
*/

// RingBuffer represents a fixed-size circular buffer for command history
type RingBuffer struct {
	Entries  []string // Stored commands
	Head     int      // Current write position (index of next insertion)
	Count    int      // Number of entries currently stored (capped at Capacity)
	Capacity int      // Maximum number of entries
}

// NewRingBuffer creates a new ring buffer with the specified capacity
//
// Parameters:
//   - capacity: Maximum number of commands to store
//
// Returns:
//   - *RingBuffer: Initialized ring buffer ready for use
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		Entries:  make([]string, capacity),
		Head:     0,
		Count:    0,
		Capacity: capacity,
	}
}

// Add inserts a command into the ring buffer
//
// Skips empty or whitespace-only commands to keep history useful.
// When capacity is reached, overwrites oldest entry (circular behavior).
//
// Parameters:
//   - cmd: Command string to add to history
//
// Performance: O(1)
func (rb *RingBuffer) Add(cmd string) {
	// Skip empty or whitespace-only commands
	trimmed := strings.TrimSpace(cmd)
	if trimmed == "" {
		return
	}

	// Insert command at current head position
	rb.Entries[rb.Head] = cmd

	// Advance head with wraparound (circular buffer)
	rb.Head = (rb.Head + 1) % rb.Capacity

	// Increment count until we reach capacity
	if rb.Count < rb.Capacity {
		rb.Count++
	}
}

// ToSlice returns all stored commands in chronological order
//
// Returns commands from oldest to newest, suitable for readline initialization.
// Empty slots in the ring buffer are excluded.
//
// Returns:
//   - []string: Slice of commands in chronological order
//
// Performance: O(n) where n = number of stored commands
func (rb *RingBuffer) ToSlice() []string {
	if rb.Count == 0 {
		return []string{}
	}

	result := make([]string, 0, rb.Count)

	if rb.Count < rb.Capacity {
		// Buffer not yet full - entries are from [0, Count)
		for i := 0; i < rb.Count; i++ {
			if rb.Entries[i] != "" {
				result = append(result, rb.Entries[i])
			}
		}
	} else {
		// Buffer is full - oldest entry is at Head, read circularly
		for i := 0; i < rb.Capacity; i++ {
			idx := (rb.Head + i) % rb.Capacity
			if rb.Entries[idx] != "" {
				result = append(result, rb.Entries[idx])
			}
		}
	}

	return result
}

// SaveHistory persists the ring buffer to disk using gob encoding
//
// Saves the entire ring buffer structure to a binary file for space efficiency.
// If the file already exists, it will be overwritten.
//
// Parameters:
//   - path: File path where history should be saved
//
// Returns:
//   - error: Non-nil if file creation or encoding fails
//
// Performance: O(n) where n = capacity (entire buffer encoded)
func (rb *RingBuffer) SaveHistory(path string) error {
	// Create or truncate history file
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Encode ring buffer structure to gob format
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(rb)
	if err != nil {
		return err
	}

	return nil
}

// LoadHistory loads command history from disk into a new ring buffer
//
// Decodes a gob-encoded ring buffer from the specified file. If the file
// doesn't exist or is corrupted, returns an empty ring buffer with the
// specified capacity instead of failing.
//
// Parameters:
//   - path: File path to load history from
//   - capacity: Capacity for new ring buffer (used if file missing/corrupt)
//
// Returns:
//   - *RingBuffer: Ring buffer loaded from file, or empty buffer if load fails
//
// Performance: O(n) where n = capacity (entire buffer decoded)
func LoadHistory(path string, capacity int) *RingBuffer {
	// Attempt to open history file
	file, err := os.Open(path)
	if err != nil {
		// File doesn't exist or can't be opened - return empty buffer
		return NewRingBuffer(capacity)
	}
	defer file.Close()

	// Decode ring buffer from gob format
	var rb RingBuffer
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&rb)
	if err != nil {
		// File is corrupted - return empty buffer
		return NewRingBuffer(capacity)
	}

	// Validate capacity matches (or adjust if needed)
	if rb.Capacity != capacity {
		// TODO: Handle capacity mismatch - for now, use loaded capacity
		// Could implement migration logic to resize buffer while preserving entries
	}

	return &rb
}

// TODO (FUTURE ENHANCEMENT): Add search functionality
// Search through history for commands matching a pattern
//
// func (rb *RingBuffer) Search(pattern string) []string

// TODO (FUTURE ENHANCEMENT): Add history statistics
// Track command frequency, last used timestamps, etc.
//
// type HistoryStats struct {
//     TotalCommands int
//     UniqueCommands int
//     MostUsed []string
// }
//
// func (rb *RingBuffer) GetStats() HistoryStats

// TODO (FUTURE ENHANCEMENT): Add history deduplication
// Optionally remove consecutive duplicate commands (like bash HISTCONTROL=ignoredups)
//
// func (rb *RingBuffer) AddWithDedup(cmd string)

// TODO (FUTURE ENHANCEMENT): Add timestamp tracking
// Store timestamp with each command for temporal analysis
//
// type HistoryEntry struct {
//     Command string
//     Timestamp time.Time
// }
