package graphQL

import (
	"testing"
)

// TestDateTimeScalarRecognition verifies DateTime and Date are recognized as scalars
func TestDateTimeScalarRecognition(t *testing.T) {
	handler := &GraphQLHandler{}

	if !handler.isScalarType("DateTime") {
		t.Error("DateTime should be recognized as a scalar type")
	}

	if !handler.isScalarType("Date") {
		t.Error("Date should be recognized as a scalar type")
	}

	// Verify standard scalars still work
	if !handler.isScalarType("String") || !handler.isScalarType("Int") {
		t.Error("Standard scalars should still be recognized")
	}
	
	// Verify complete list
	expectedScalars := []string{"String", "Int", "Float", "Boolean", "ID", "DateTime", "Date"}
	for _, scalar := range expectedScalars {
		if !handler.isScalarType(scalar) {
			t.Errorf("Expected scalar %s not recognized", scalar)
		}
	}
	
	t.Logf("All %d scalars recognized correctly", len(expectedScalars))
}
