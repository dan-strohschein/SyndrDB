package syndrQL

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/domain/models"
)

// FunctionSignature defines metadata and implementation for a registered function
// TODO: I will add return type inference when implementing compile-time type checking
// TODO: I will add cost estimation (CPU cycles) when implementing query cost-based optimization
type FunctionSignature struct {
	Name           string                                                                                // Function name (uppercase)
	MinArgs        int                                                                                   // Minimum argument count
	MaxArgs        int                                                                                   // Maximum argument count (-1 for variadic)
	Implementation func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) // Function implementation
	CallCount      uint64                                                                                // Atomic counter for telemetry (wrapping intentional)
	Description    string                                                                                // Documentation for error messages
}

// EvaluationContext provides runtime context for function evaluation
// TODO: I will add TransactionID when implementing MVCC isolation levels
// TODO: I will add QueryTimeout when implementing per-query resource limits
type EvaluationContext struct {
	CurrentTime   time.Time                    // Cached NOW() value (query-scoped)
	TimezoneCache interface{}                  // Timezone cache reference (avoid import cycle)
	Document      map[string]interface{}       // Current document being evaluated
	BundleName    string                       // Bundle being queried
	FieldValues   map[string]models.FieldValue // Pre-evaluated field values for performance
}

// FunctionRegistry manages registered functions with thread-safe access
// Design: Single global registry initialized at startup (init() only)
// TODO: I will add support for user-defined functions (UDFs) when implementing extension system
// TODO: I will add function categories (datetime, string, math, etc.) when implementing function discovery
type FunctionRegistry struct {
	functions map[string]*FunctionSignature // map[uppercase_name]*FunctionSignature
	mu        sync.RWMutex                  // Protects functions map (read-heavy workload)
}

// Global registry instance (lazily initialized)
var globalRegistry *FunctionRegistry
var registryOnce sync.Once

// GetRegistry returns the global function registry
// Uses sync.Once to ensure thread-safe lazy initialization
// TODO: I will add per-database registries when implementing database-scoped UDFs
func GetRegistry() *FunctionRegistry {
	registryOnce.Do(func() {
		globalRegistry = &FunctionRegistry{
			functions: make(map[string]*FunctionSignature, 32), // Pre-allocate for built-in functions
		}
	})
	return globalRegistry
}

// Register adds a function to the registry
// Only callable during initialization (init() functions)
// TODO: I will add runtime registration with version tracking when implementing hot-reload UDFs
func (fr *FunctionRegistry) Register(sig *FunctionSignature) error {
	if sig == nil {
		return fmt.Errorf("cannot register nil function signature")
	}
	if sig.Name == "" {
		return fmt.Errorf("function name cannot be empty")
	}
	if sig.MinArgs < 0 {
		return fmt.Errorf("function '%s': MinArgs cannot be negative", sig.Name)
	}
	if sig.MaxArgs != -1 && sig.MaxArgs < sig.MinArgs {
		return fmt.Errorf("function '%s': MaxArgs (%d) cannot be less than MinArgs (%d)", sig.Name, sig.MaxArgs, sig.MinArgs)
	}
	if sig.Implementation == nil {
		return fmt.Errorf("function '%s': implementation cannot be nil", sig.Name)
	}

	// Uppercase for case-insensitive lookup
	upperName := strings.ToUpper(sig.Name)
	sig.Name = upperName // Normalize in signature

	fr.mu.Lock()
	defer fr.mu.Unlock()

	if _, exists := fr.functions[upperName]; exists {
		return fmt.Errorf("function '%s' already registered", upperName)
	}

	fr.functions[upperName] = sig
	return nil
}

// Get retrieves a function signature by name (case-insensitive)
// Returns nil if function not found
// TODO: I will add caching for Get() results when profiling shows >5% time in lookups
func (fr *FunctionRegistry) Get(name string) *FunctionSignature {
	upperName := strings.ToUpper(name)

	fr.mu.RLock()
	defer fr.mu.RUnlock()

	return fr.functions[upperName]
}

// Call executes a registered function with the given arguments
// Increments call count atomically for telemetry
// TODO: I will add execution time tracking when implementing query profiling
func (fr *FunctionRegistry) Call(name string, args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
	sig := fr.Get(name)
	if sig == nil {
		// Generate suggestions using Levenshtein distance
		suggestions := fr.suggestFunctions(name, 3)
		if len(suggestions) > 0 {
			return models.FieldValue{}, fmt.Errorf("unknown function '%s'. Did you mean: %s?", name, strings.Join(suggestions, ", "))
		}
		return models.FieldValue{}, fmt.Errorf("unknown function '%s'", name)
	}

	// Validate argument count
	argCount := len(args)
	if argCount < sig.MinArgs {
		return models.FieldValue{}, fmt.Errorf("function '%s' requires at least %d argument(s), got %d", sig.Name, sig.MinArgs, argCount)
	}
	if sig.MaxArgs != -1 && argCount > sig.MaxArgs {
		return models.FieldValue{}, fmt.Errorf("function '%s' accepts at most %d argument(s), got %d", sig.Name, sig.MaxArgs, argCount)
	}

	// Increment call count (atomic, wrapping intentional for telemetry)
	atomic.AddUint64(&sig.CallCount, 1)

	// Execute function
	return sig.Implementation(args, evalCtx)
}

// suggestFunctions returns up to maxSuggestions function names with Levenshtein distance ≤ 2
// Suggestions are uppercased and sorted alphabetically
// TODO: I will add phonetic matching (Soundex/Metaphone) when implementing advanced error suggestions
func (fr *FunctionRegistry) suggestFunctions(name string, maxSuggestions int) []string {
	upperName := strings.ToUpper(name)

	fr.mu.RLock()
	defer fr.mu.RUnlock()

	type suggestion struct {
		name     string
		distance int
	}

	candidates := make([]suggestion, 0, len(fr.functions))

	for funcName := range fr.functions {
		dist := levenshteinDistance(upperName, funcName)
		if dist <= 2 { // Threshold: distance ≤ 2
			candidates = append(candidates, suggestion{name: funcName, distance: dist})
		}
	}

	// Sort by distance (ascending), then alphabetically
	// TODO: I will optimize with heap if profiling shows >1% CPU time in sorting
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[i].distance > candidates[j].distance ||
				(candidates[i].distance == candidates[j].distance && candidates[i].name > candidates[j].name) {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	// Return top N suggestions
	count := maxSuggestions
	if count > len(candidates) {
		count = len(candidates)
	}

	suggestions := make([]string, count)
	for i := 0; i < count; i++ {
		suggestions[i] = candidates[i].name
	}

	return suggestions
}

// levenshteinDistance calculates the Levenshtein distance between two strings
// Using Wagner-Fischer dynamic programming algorithm (O(m*n) time, O(min(m,n)) space)
// TODO: I will optimize to O(n) space with rolling array when profiling shows memory pressure
func levenshteinDistance(s1, s2 string) int {
	len1, len2 := len(s1), len(s2)

	// Optimization: swap to ensure s1 is shorter (minimize space)
	if len1 > len2 {
		s1, s2 = s2, s1
		len1, len2 = len2, len1
	}

	// Edge cases
	if len1 == 0 {
		return len2
	}

	// Use single row DP (space optimization)
	prevRow := make([]int, len1+1)
	currRow := make([]int, len1+1)

	// Initialize first row
	for i := 0; i <= len1; i++ {
		prevRow[i] = i
	}

	// Fill DP table
	for j := 1; j <= len2; j++ {
		currRow[0] = j
		for i := 1; i <= len1; i++ {
			cost := 1
			if s1[i-1] == s2[j-1] {
				cost = 0
			}

			currRow[i] = min(
				prevRow[i]+1,      // Deletion
				currRow[i-1]+1,    // Insertion
				prevRow[i-1]+cost, // Substitution
			)
		}
		prevRow, currRow = currRow, prevRow
	}

	return prevRow[len1]
}

// min returns the minimum of three integers
func min(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

// GetCallCount returns the call count for a function (for telemetry)
// Returns 0 if function not found
// TODO: I will add GetAllCallCounts() when implementing function usage analytics
func (fr *FunctionRegistry) GetCallCount(name string) uint64 {
	sig := fr.Get(name)
	if sig == nil {
		return 0
	}
	return atomic.LoadUint64(&sig.CallCount)
}

// ListFunctions returns all registered function names (uppercased, sorted)
// TODO: I will add filtering by category when implementing function categorization
func (fr *FunctionRegistry) ListFunctions() []string {
	fr.mu.RLock()
	defer fr.mu.RUnlock()

	names := make([]string, 0, len(fr.functions))
	for name := range fr.functions {
		names = append(names, name)
	}

	// Sort alphabetically
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[i] > names[j] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}

	return names
}
