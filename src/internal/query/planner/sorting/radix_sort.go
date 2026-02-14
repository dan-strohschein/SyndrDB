package sorting

import (
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

/*
RADIX SORT FOR INTEGERS

This file implements LSD (Least Significant Digit) Radix Sort for integer fields,
following SQL Server's approach for high-performance numeric sorting.

ALGORITHM:
1. Sort by each byte (8 passes for int64), from least to most significant
2. Count occurrences of each byte value (0-255)
3. Compute cumulative positions
4. Place documents in sorted order based on current byte

COMPLEXITY:
- Time: O(n * k) where n = documents, k = 8 bytes for int64
- Space: O(n) for output buffer
- Comparison: O(n log n) for comparison-based sorts

PERFORMANCE ADVANTAGE:
- Linear time vs logarithmic for comparison sorts
- Cache-friendly: Sequential memory access
- SIMD opportunity: Parallel byte extraction (future optimization)

WHEN TO USE:
- Integer fields (int, int32, int64)
- Large datasets (> 1000 documents) where linear time matters
- No LIMIT or large LIMIT (radix sort processes all elements)

TARGET PERFORMANCE (from Order_by_impl_v1.md):
- ~12μs per 1000 integers (SQL Server benchmark)
- 6.7x faster than comparison-based quicksort
- Scales linearly with dataset size

DESIGN PRINCIPLES:
- Single Responsibility: Each function handles one radix pass or phase
- DRY: Reusable counting and positioning logic
- Open/Closed: Extends sorting strategies without modifying existing code
*/

// RadixSortKey represents a document with its integer sort key
type RadixSortKey struct {
	Key      int64            // The integer value to sort by
	Document *models.Document // The complete document
}

// RadixSort performs LSD (Least Significant Digit) radix sort on integer fields.
//
// Algorithm (SQL Server approach):
//  1. Extract integer sort keys from all documents
//  2. For each byte position (0-7 for int64):
//     a. Count occurrences of each byte value (0-255)
//     b. Compute cumulative positions
//     c. Redistribute documents based on current byte
//  3. Handle negative numbers (sign bit correction)
//
// Performance:
// - O(n * 8) = O(n) linear time for int64
// - 6.7x faster than quicksort on large datasets
// - Cache-friendly sequential access pattern
//
// Parameters:
// - documents: Map of all documents to sort
// - fieldName: Integer field to sort by
// - ascending: true for ASC, false for DESC
// - logger: For debugging and metrics
//
// Returns: Sorted slice of documents
//
// TODO: I could add SIMD acceleration for parallel byte extraction across
// multiple documents, potentially achieving 2-4x additional speedup.
func RadixSort(
	documents map[string]*models.Document,
	fieldName string,
	ascending bool,
	logger *zap.SugaredLogger,
	schema *models.BundleFieldSchema,
) ([]*models.Document, error) {
	if len(documents) == 0 {
		return []*models.Document{}, nil
	}

	keys := make([]RadixSortKey, 0, len(documents))
	for _, doc := range documents {
		fv, exists := GetFieldValueForSort(doc, fieldName, schema)
		if !exists {
			continue
		}

		var intValue int64
		switch v := fv.AsInterface().(type) {
		case int:
			intValue = int64(v)
		case int32:
			intValue = int64(v)
		case int64:
			intValue = v
		case float32:
			// Allow implicit conversion for numeric types
			intValue = int64(v)
		case float64:
			intValue = int64(v)
		default:
			// Skip non-integer values
			// TODO: I could add automatic type coercion or warning logging here
			// for better error visibility in production
			continue
		}

		keys = append(keys, RadixSortKey{
			Key:      intValue,
			Document: doc,
		})
	}

	if len(keys) == 0 {
		return []*models.Document{}, nil
	}

	// Perform LSD radix sort
	sortedKeys := radixSortInt64(keys, ascending, logger)

	// Extract documents in sorted order
	result := make([]*models.Document, len(sortedKeys))
	for i, key := range sortedKeys {
		result[i] = key.Document
	}

	return result, nil
}

// radixSortInt64 performs the core LSD radix sort algorithm
//
// Single Responsibility: Handles the 8-pass radix sort for int64 values
// DRY Principle: Reuses countingSort for each byte position
func radixSortInt64(keys []RadixSortKey, ascending bool, logger *zap.SugaredLogger) []RadixSortKey {
	n := len(keys)
	if n <= 1 {
		return keys
	}

	// Two buffers for ping-pong sorting
	input := keys
	output := make([]RadixSortKey, n)

	// Sort by each byte, from least to most significant
	// For int64: 8 bytes (64 bits), process byte 0-7
	for bytePos := 0; bytePos < 8; bytePos++ {
		// Count sort on current byte position
		countingSortByByte(input, output, bytePos)

		// Swap buffers (ping-pong)
		input, output = output, input
	}

	// After 8 passes, result is in 'input'
	// Handle negative numbers: they end up at the end due to sign bit
	// Need to rotate them to the front for correct ordering
	result := handleSignBit(input, ascending)

	return result
}

// countingSortByByte performs counting sort on a specific byte position
//
// Algorithm:
// 1. Count occurrences of each byte value (0-255)
// 2. Compute cumulative positions
// 3. Place elements in sorted order
//
// Single Responsibility: Handles one radix pass
// Performance: O(n) linear time, cache-friendly
func countingSortByByte(input, output []RadixSortKey, bytePos int) {
	n := len(input)

	// Count occurrences of each byte value (0-255)
	counts := [256]int{}
	shift := uint(bytePos * 8)

	for i := 0; i < n; i++ {
		// Extract byte at position bytePos
		digit := (input[i].Key >> shift) & 0xFF
		counts[digit]++
	}

	// Compute cumulative positions (prefix sum)
	// This tells us where to place each element
	positions := [256]int{}
	positions[0] = 0
	for i := 1; i < 256; i++ {
		positions[i] = positions[i-1] + counts[i-1]
	}

	// Place elements in sorted order based on current byte
	for i := 0; i < n; i++ {
		digit := (input[i].Key >> shift) & 0xFF
		output[positions[digit]] = input[i]
		positions[digit]++
	}
}

// handleSignBit corrects the ordering after radix sort to handle negative numbers
//
// Problem: After radix sort treating int64 as unsigned:
//   - Positive numbers (sign bit = 0): 0x0000... to 0x7FFF... come first, correctly sorted
//   - Negative numbers (sign bit = 1): 0x8000... to 0xFFFF... come last, correctly sorted!
//     Key insight: -15 = 0xFFF...FF1, -5 = 0xFFF...FFB
//     0xFFF...FF1 < 0xFFF...FFB, so -15 comes before -5 (which is correct!)
//
// Solution: Just rotate negatives to front, NO reversal needed!
//
// For ascending order: [negatives (already correct), positives (already correct)]
// For descending order: reverse the entire array
//
// TODO: I could optimize this with SIMD-accelerated rotation/copy for
// large datasets (>10K elements) to further improve performance.
func handleSignBit(sorted []RadixSortKey, ascending bool) []RadixSortKey {
	n := len(sorted)
	if n == 0 {
		return sorted
	}

	// Find split point: where positives end and negatives begin
	// After radix sort: [positives in order] [negatives in order]
	splitPoint := -1
	for i := 0; i < n; i++ {
		if sorted[i].Key < 0 {
			splitPoint = i
			break
		}
	}

	// All positive: already correct
	if splitPoint == -1 {
		if !ascending {
			return reverseKeys(sorted)
		}
		return sorted
	}

	// All negative: already correct
	if splitPoint == 0 {
		if !ascending {
			return reverseKeys(sorted)
		}
		return sorted
	}

	// Mixed: rotate negatives to front
	result := make([]RadixSortKey, n)

	// Copy negatives to front (they're already in correct order!)
	negCount := n - splitPoint
	copy(result[0:negCount], sorted[splitPoint:n])

	// Copy positives after negatives
	copy(result[negCount:n], sorted[0:splitPoint])

	if !ascending {
		return reverseKeys(result)
	}

	return result
}

// reverseKeys reverses a slice of RadixSortKey
// DRY Principle: Reusable reversal logic
func reverseKeys(keys []RadixSortKey) []RadixSortKey {
	n := len(keys)
	reversed := make([]RadixSortKey, n)
	for i := 0; i < n; i++ {
		reversed[i] = keys[n-1-i]
	}
	return reversed
}

// ShouldUseRadixSort determines if radix sort is beneficial
//
// Heuristics:
// - Large datasets (> minRadixSize documents): radix sort excels
// - Integer fields: radix sort is optimal
// - No LIMIT or large LIMIT: radix sort processes all elements efficiently
//
// Parameters:
//   - totalDocs: Total number of documents in dataset
//   - limit: LIMIT value (0 or negative means no limit)
//   - minRadixSize: Minimum dataset size for radix activation
//   - limitRatio: Minimum LIMIT/total ratio (e.g., 0.5 = 50%)
//
// Threshold: Use radix sort when:
// 1. Dataset size > minRadixSize
// 2. Field is integer type (checked by caller)
// 3. No LIMIT or LIMIT > limitRatio * dataset
//
// TODO: I could add benchmarking to auto-tune the threshold based on
// hardware characteristics (CPU cache size, memory bandwidth)
func ShouldUseRadixSort(totalDocs, limit, minRadixSize int, limitRatio float64) bool {
	// Too small: overhead of radix sort not worth it
	if totalDocs < minRadixSize {
		return false
	}

	// No limit: radix sort is optimal for full sort
	if limit <= 0 {
		return true
	}

	// Large limit (> limitRatio of dataset): radix sort still beneficial
	ratio := float64(limit) / float64(totalDocs)
	return ratio > limitRatio
}
