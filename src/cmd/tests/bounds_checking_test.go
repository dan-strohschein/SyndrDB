package main

import (
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"
)

// TestDateTimeFunctions_BoundsChecking verifies that datetime functions
// properly handle insufficient arguments without panicking (HIGH-006 fix)
func TestDateTimeFunctions_BoundsChecking(t *testing.T) {
	registry := syndrQL.GetRegistry()
	evalCtx := &syndrQL.EvaluationContext{}

	// Test EXTRACT with insufficient arguments
	t.Run("EXTRACT_insufficient_args", func(t *testing.T) {
		_, err := registry.Call("EXTRACT", []models.FieldValue{}, evalCtx)
		if err == nil {
			t.Error("Expected error for EXTRACT with no arguments")
		}

		_, err = registry.Call("EXTRACT", []models.FieldValue{
			models.NewStringValue("YEAR"),
		}, evalCtx)
		if err == nil {
			t.Error("Expected error for EXTRACT with only 1 argument")
		}
	})

	// Test DATE_TRUNC with insufficient arguments
	t.Run("DATE_TRUNC_insufficient_args", func(t *testing.T) {
		_, err := registry.Call("DATE_TRUNC", []models.FieldValue{}, evalCtx)
		if err == nil {
			t.Error("Expected error for DATE_TRUNC with no arguments")
		}

		_, err = registry.Call("DATE_TRUNC", []models.FieldValue{
			models.NewStringValue("DAY"),
		}, evalCtx)
		if err == nil {
			t.Error("Expected error for DATE_TRUNC with only 1 argument")
		}
	})

	// Test DATE_ADD with insufficient arguments
	t.Run("DATE_ADD_insufficient_args", func(t *testing.T) {
		_, err := registry.Call("DATE_ADD", []models.FieldValue{}, evalCtx)
		if err == nil {
			t.Error("Expected error for DATE_ADD with no arguments")
		}

		testTime := time.Now().UTC()
		_, err = registry.Call("DATE_ADD", []models.FieldValue{
			models.NewDateTimeValue(testTime),
		}, evalCtx)
		if err == nil {
			t.Error("Expected error for DATE_ADD with only 1 argument")
		}

		_, err = registry.Call("DATE_ADD", []models.FieldValue{
			models.NewDateTimeValue(testTime),
			models.NewIntValue(7),
		}, evalCtx)
		if err == nil {
			t.Error("Expected error for DATE_ADD with only 2 arguments")
		}
	})

	// Test DATE_SUB with insufficient arguments
	t.Run("DATE_SUB_insufficient_args", func(t *testing.T) {
		_, err := registry.Call("DATE_SUB", []models.FieldValue{}, evalCtx)
		if err == nil {
			t.Error("Expected error for DATE_SUB with no arguments")
		}

		testTime := time.Now().UTC()
		_, err = registry.Call("DATE_SUB", []models.FieldValue{
			models.NewDateTimeValue(testTime),
		}, evalCtx)
		if err == nil {
			t.Error("Expected error for DATE_SUB with only 1 argument")
		}
	})

	// Test AGE with insufficient arguments
	t.Run("AGE_insufficient_args", func(t *testing.T) {
		_, err := registry.Call("AGE", []models.FieldValue{}, evalCtx)
		if err == nil {
			t.Error("Expected error for AGE with no arguments")
		}
	})
}
