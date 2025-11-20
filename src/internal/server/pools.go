package server

import (
	"strings"
	"sync"
)

// Global object pools for reducing allocations
// These pools are used across the server package to reuse frequently allocated objects

// responseMapPool pools map[string]interface{} for command responses
// Used heavily in CommandDirector, database operations, and GraphQL responses
// Typical size: 5-20 keys per response
var responseMapPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 10)
	},
}

// stringBuilderPool pools strings.Builder for string concatenation
// Used in filter parsing, security sanitization, and query building
// Typical size: 256-1024 bytes
var stringBuilderPool = sync.Pool{
	New: func() interface{} {
		sb := &strings.Builder{}
		sb.Grow(256) // Pre-allocate 256 bytes
		return sb
	},
}

// GetResponseMap gets a response map from the pool
// Remember to call PutResponseMap when done
func GetResponseMap() map[string]interface{} {
	m := responseMapPool.Get().(map[string]interface{})
	// Clear the map before returning
	for k := range m {
		delete(m, k)
	}
	return m
}

// PutResponseMap returns a response map to the pool
// Only pool maps with reasonable size to avoid memory bloat
func PutResponseMap(m map[string]interface{}) {
	if len(m) <= 50 { // Only pool reasonable-sized maps
		responseMapPool.Put(m)
	}
}

// GetStringBuilder gets a strings.Builder from the pool
// Remember to call PutStringBuilder when done
func GetStringBuilder() *strings.Builder {
	sb := stringBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	return sb
}

// PutStringBuilder returns a strings.Builder to the pool
// Only pool builders with reasonable capacity
func PutStringBuilder(sb *strings.Builder) {
	if sb.Cap() <= 8192 { // Only pool up to 8KB
		stringBuilderPool.Put(sb)
	}
}

// tokenSlicePool pools []string slices for filter parsing tokens
// Typical size: 10-50 tokens per WHERE clause
var tokenSlicePool = sync.Pool{
	New: func() interface{} {
		tokens := make([]string, 0, 20)
		return &tokens
	},
}

// GetTokenSlice gets a token slice from the pool
// Remember to call PutTokenSlice when done
func GetTokenSlice() *[]string {
	slice := tokenSlicePool.Get().(*[]string)
	*slice = (*slice)[:0] // Reset length but keep capacity
	return slice
}

// PutTokenSlice returns a token slice to the pool
// Only pool slices with reasonable capacity
func PutTokenSlice(slice *[]string) {
	if cap(*slice) <= 200 { // Only pool up to 200 tokens
		tokenSlicePool.Put(slice)
	}
}
