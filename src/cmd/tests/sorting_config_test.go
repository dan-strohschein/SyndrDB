// Package sorting provides configuration for the SyndrDB query sorting subsystem.
// This test file validates the SortingConfig struct, ensuring that default values
// are correct and that validation logic properly catches invalid configurations.
//
// Test Coverage:
// - DefaultSortingConfig() returns valid configuration
// - Validate() accepts all valid parameter ranges
// - Validate() rejects out-of-range parameters with clear error messages
// - Edge cases for boundary values and typical production configurations

package main

import (
	"strings"
	"testing"

	"syndrdb/src/internal/query/planner/sorting"
)

// Type aliases for sorting configuration types
type SortingConfig = sorting.SortingConfig

// Function aliases for sorting configuration functions
var (
	DefaultSortingConfig = sorting.DefaultSortingConfig
)

// TestDefaultSortingConfig verifies that DefaultSortingConfig returns a valid
// configuration with production-tested defaults that pass validation.
func TestDefaultSortingConfig(t *testing.T) {
	config := DefaultSortingConfig()

	if config == nil {
		t.Fatal("DefaultSortingConfig returned nil")
	}

	// Verify defaults match documented values
	if config.TopNThreshold != 0.1 {
		t.Errorf("Expected TopNThreshold=0.1, got %.2f", config.TopNThreshold)
	}
	if config.TopNMinSize != 100 {
		t.Errorf("Expected TopNMinSize=100, got %d", config.TopNMinSize)
	}
	if config.RadixMinSize != 1000 {
		t.Errorf("Expected RadixMinSize=1000, got %d", config.RadixMinSize)
	}
	if config.RadixLimitRatio != 0.5 {
		t.Errorf("Expected RadixLimitRatio=0.5, got %.2f", config.RadixLimitRatio)
	}
	if !config.SIMDEnabled {
		t.Error("Expected SIMDEnabled=true")
	}
	if config.SIMDAbbrevBytes != 8 {
		t.Errorf("Expected SIMDAbbrevBytes=8, got %d", config.SIMDAbbrevBytes)
	}
	if config.SIMDMinSize != 100 {
		t.Errorf("Expected SIMDMinSize=100, got %d", config.SIMDMinSize)
	}
	if config.HeapInitialCapacity != 1000 {
		t.Errorf("Expected HeapInitialCapacity=1000, got %d", config.HeapInitialCapacity)
	}
	if config.RadixMaxPasses != 8 {
		t.Errorf("Expected RadixMaxPasses=8, got %d", config.RadixMaxPasses)
	}
	if config.EnableParallelSort {
		t.Error("Expected EnableParallelSort=false (Phase 5 not implemented)")
	}
	if config.ParallelThreshold != 10000 {
		t.Errorf("Expected ParallelThreshold=10000, got %d", config.ParallelThreshold)
	}
	if config.MaxSortMemoryMB != 512 {
		t.Errorf("Expected MaxSortMemoryMB=512, got %d", config.MaxSortMemoryMB)
	}

	// Verify defaults pass validation
	if err := config.Validate(); err != nil {
		t.Errorf("Default configuration failed validation: %v", err)
	}
}

// TestValidateTopNThreshold tests validation of TopNThreshold parameter.
func TestValidateTopNThreshold(t *testing.T) {
	tests := []struct {
		name      string
		threshold float64
		expectErr bool
	}{
		{"Valid minimum", 0.01, false},
		{"Valid typical", 0.1, false},
		{"Valid maximum", 0.5, false},
		{"Invalid too low", 0.005, true},
		{"Invalid zero", 0.0, true},
		{"Invalid negative", -0.1, true},
		{"Invalid too high", 0.6, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.TopNThreshold = tt.threshold
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for TopNThreshold=%.2f", tt.threshold)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for TopNThreshold=%.2f: %v", tt.threshold, err)
			}
			if err != nil && !strings.Contains(err.Error(), "TopNThreshold") {
				t.Errorf("Error message should mention TopNThreshold: %v", err)
			}
		})
	}
}

// TestValidateTopNMinSize tests validation of TopNMinSize parameter.
func TestValidateTopNMinSize(t *testing.T) {
	tests := []struct {
		name      string
		minSize   int
		expectErr bool
	}{
		{"Valid minimum", 10, false},
		{"Valid typical", 100, false},
		{"Valid maximum", 10000, false},
		{"Invalid too low", 5, true},
		{"Invalid zero", 0, true},
		{"Invalid negative", -1, true},
		{"Invalid too high", 10001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.TopNMinSize = tt.minSize
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for TopNMinSize=%d", tt.minSize)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for TopNMinSize=%d: %v", tt.minSize, err)
			}
			if err != nil && !strings.Contains(err.Error(), "TopNMinSize") {
				t.Errorf("Error message should mention TopNMinSize: %v", err)
			}
		})
	}
}

// TestValidateRadixMinSize tests validation of RadixMinSize parameter.
func TestValidateRadixMinSize(t *testing.T) {
	tests := []struct {
		name      string
		minSize   int
		expectErr bool
	}{
		{"Valid minimum", 100, false},
		{"Valid typical", 1000, false},
		{"Valid maximum", 100000, false},
		{"Invalid too low", 50, true},
		{"Invalid zero", 0, true},
		{"Invalid too high", 100001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.RadixMinSize = tt.minSize
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for RadixMinSize=%d", tt.minSize)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for RadixMinSize=%d: %v", tt.minSize, err)
			}
			if err != nil && !strings.Contains(err.Error(), "RadixMinSize") {
				t.Errorf("Error message should mention RadixMinSize: %v", err)
			}
		})
	}
}

// TestValidateRadixLimitRatio tests validation of RadixLimitRatio parameter.
func TestValidateRadixLimitRatio(t *testing.T) {
	tests := []struct {
		name      string
		ratio     float64
		expectErr bool
	}{
		{"Valid minimum", 0.1, false},
		{"Valid typical", 0.5, false},
		{"Valid maximum", 1.0, false},
		{"Invalid too low", 0.05, true},
		{"Invalid zero", 0.0, true},
		{"Invalid too high", 1.1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.RadixLimitRatio = tt.ratio
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for RadixLimitRatio=%.2f", tt.ratio)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for RadixLimitRatio=%.2f: %v", tt.ratio, err)
			}
			if err != nil && !strings.Contains(err.Error(), "RadixLimitRatio") {
				t.Errorf("Error message should mention RadixLimitRatio: %v", err)
			}
		})
	}
}

// TestValidateRadixMaxPasses tests validation of RadixMaxPasses parameter.
func TestValidateRadixMaxPasses(t *testing.T) {
	tests := []struct {
		name      string
		maxPasses int
		expectErr bool
	}{
		{"Valid minimum (uint8)", 1, false},
		{"Valid uint32", 4, false},
		{"Valid maximum (int64)", 8, false},
		{"Invalid zero", 0, true},
		{"Invalid too high", 9, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.RadixMaxPasses = tt.maxPasses
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for RadixMaxPasses=%d", tt.maxPasses)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for RadixMaxPasses=%d: %v", tt.maxPasses, err)
			}
			if err != nil && !strings.Contains(err.Error(), "RadixMaxPasses") {
				t.Errorf("Error message should mention RadixMaxPasses: %v", err)
			}
		})
	}
}

// TestValidateSIMDAbbrevBytes tests validation of SIMDAbbrevBytes parameter.
func TestValidateSIMDAbbrevBytes(t *testing.T) {
	tests := []struct {
		name      string
		abbrevLen int
		expectErr bool
	}{
		{"Valid minimum", 4, false},
		{"Valid typical", 8, false},
		{"Valid maximum", 16, false},
		{"Invalid too low", 3, true},
		{"Invalid zero", 0, true},
		{"Invalid too high", 17, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.SIMDAbbrevBytes = tt.abbrevLen
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for SIMDAbbrevBytes=%d", tt.abbrevLen)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for SIMDAbbrevBytes=%d: %v", tt.abbrevLen, err)
			}
			if err != nil && !strings.Contains(err.Error(), "SIMDAbbrevBytes") {
				t.Errorf("Error message should mention SIMDAbbrevBytes: %v", err)
			}
		})
	}
}

// TestValidateSIMDMinSize tests validation of SIMDMinSize parameter.
func TestValidateSIMDMinSize(t *testing.T) {
	tests := []struct {
		name      string
		minSize   int
		expectErr bool
	}{
		{"Valid minimum", 10, false},
		{"Valid typical", 100, false},
		{"Valid maximum", 10000, false},
		{"Invalid too low", 5, true},
		{"Invalid zero", 0, true},
		{"Invalid too high", 10001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.SIMDMinSize = tt.minSize
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for SIMDMinSize=%d", tt.minSize)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for SIMDMinSize=%d: %v", tt.minSize, err)
			}
			if err != nil && !strings.Contains(err.Error(), "SIMDMinSize") {
				t.Errorf("Error message should mention SIMDMinSize: %v", err)
			}
		})
	}
}

// TestValidateHeapInitialCapacity tests validation of HeapInitialCapacity parameter.
func TestValidateHeapInitialCapacity(t *testing.T) {
	tests := []struct {
		name      string
		capacity  int
		expectErr bool
	}{
		{"Valid minimum", 10, false},
		{"Valid typical", 1000, false},
		{"Valid maximum", 100000, false},
		{"Invalid too low", 5, true},
		{"Invalid zero", 0, true},
		{"Invalid too high", 100001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.HeapInitialCapacity = tt.capacity
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for HeapInitialCapacity=%d", tt.capacity)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for HeapInitialCapacity=%d: %v", tt.capacity, err)
			}
			if err != nil && !strings.Contains(err.Error(), "HeapInitialCapacity") {
				t.Errorf("Error message should mention HeapInitialCapacity: %v", err)
			}
		})
	}
}

// TestValidateParallelThreshold tests validation of ParallelThreshold parameter.
func TestValidateParallelThreshold(t *testing.T) {
	tests := []struct {
		name      string
		threshold int
		expectErr bool
	}{
		{"Valid minimum", 1000, false},
		{"Valid typical", 10000, false},
		{"Valid maximum", 1000000, false},
		{"Invalid too low", 500, true},
		{"Invalid zero", 0, true},
		{"Invalid too high", 1000001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.ParallelThreshold = tt.threshold
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for ParallelThreshold=%d", tt.threshold)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for ParallelThreshold=%d: %v", tt.threshold, err)
			}
			if err != nil && !strings.Contains(err.Error(), "ParallelThreshold") {
				t.Errorf("Error message should mention ParallelThreshold: %v", err)
			}
		})
	}
}

// TestValidateMaxSortMemoryMB tests validation of MaxSortMemoryMB parameter.
func TestValidateMaxSortMemoryMB(t *testing.T) {
	tests := []struct {
		name      string
		memoryMB  int
		expectErr bool
	}{
		{"Valid minimum", 10, false},
		{"Valid typical", 512, false},
		{"Valid large", 2048, false},
		{"Valid maximum", 10240, false},
		{"Invalid too low", 5, true},
		{"Invalid zero", 0, true},
		{"Invalid too high", 10241, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultSortingConfig()
			config.MaxSortMemoryMB = tt.memoryMB
			err := config.Validate()

			if tt.expectErr && err == nil {
				t.Errorf("Expected validation error for MaxSortMemoryMB=%d", tt.memoryMB)
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected validation error for MaxSortMemoryMB=%d: %v", tt.memoryMB, err)
			}
			if err != nil && !strings.Contains(err.Error(), "MaxSortMemoryMB") {
				t.Errorf("Error message should mention MaxSortMemoryMB: %v", err)
			}
		})
	}
}

// TestValidateMultipleErrors ensures Validate returns the first error encountered
// when multiple parameters are invalid.
func TestValidateMultipleErrors(t *testing.T) {
	config := DefaultSortingConfig()

	// Set multiple invalid values
	config.TopNThreshold = 0.0 // Invalid
	config.RadixMinSize = 50   // Invalid
	config.SIMDAbbrevBytes = 3 // Invalid

	err := config.Validate()
	if err == nil {
		t.Fatal("Expected validation error for multiple invalid parameters")
	}

	// Should return first error (TopNThreshold)
	if !strings.Contains(err.Error(), "TopNThreshold") {
		t.Errorf("Expected first error to be TopNThreshold, got: %v", err)
	}
}

// TestValidateProductionScenarios tests realistic production configurations.
func TestValidateProductionScenarios(t *testing.T) {
	scenarios := []struct {
		name   string
		config *SortingConfig
		valid  bool
	}{
		{
			name: "High-performance server",
			config: &SortingConfig{
				TopNThreshold:       0.05,
				TopNMinSize:         500,
				RadixMinSize:        5000,
				RadixLimitRatio:     0.3,
				SIMDEnabled:         true,
				SIMDAbbrevBytes:     16,
				SIMDMinSize:         500,
				HeapInitialCapacity: 5000,
				RadixMaxPasses:      8,
				EnableParallelSort:  false,
				ParallelThreshold:   50000,
				ParallelEnabled:     true,
				ParallelMinSize:     10000,
				MaxSortMemoryMB:     4096,
			},
			valid: true,
		},
		{
			name: "Memory-constrained environment",
			config: &SortingConfig{
				TopNThreshold:       0.2,
				TopNMinSize:         50,
				RadixMinSize:        2000,
				RadixLimitRatio:     0.7,
				SIMDEnabled:         true,
				SIMDAbbrevBytes:     4,
				SIMDMinSize:         50,
				HeapInitialCapacity: 500,
				RadixMaxPasses:      8,
				EnableParallelSort:  false,
				ParallelThreshold:   20000,
				ParallelEnabled:     false,
				ParallelMinSize:     20000,
				MaxSortMemoryMB:     128,
			},
			valid: true,
		},
		{
			name: "SIMD disabled configuration",
			config: &SortingConfig{
				TopNThreshold:       0.1,
				TopNMinSize:         100,
				RadixMinSize:        1000,
				RadixLimitRatio:     0.5,
				SIMDEnabled:         false, // Disabled
				SIMDAbbrevBytes:     8,
				SIMDMinSize:         100,
				HeapInitialCapacity: 1000,
				RadixMaxPasses:      8,
				EnableParallelSort:  false,
				ParallelThreshold:   10000,
				ParallelEnabled:     false,
				ParallelMinSize:     10000,
				MaxSortMemoryMB:     512,
			},
			valid: true,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			err := scenario.config.Validate()
			if scenario.valid && err != nil {
				t.Errorf("Expected valid configuration, got error: %v", err)
			}
			if !scenario.valid && err == nil {
				t.Error("Expected validation error for invalid configuration")
			}
		})
	}
}
