package syndrQL

import (
	"math"
	"testing"

	"syndrdb/src/internal/domain/models"
)

func TestPOWER_IntegerExponent(t *testing.T) {
	result, err := GetRegistry().Call("POWER", []models.FieldValue{
		models.NewIntValue(2),
		models.NewIntValue(3),
	}, nil)
	if err != nil {
		t.Fatalf("POWER(2, 3) error: %v", err)
	}
	if result.Type != models.FieldTypeFloat || result.FloatVal != 8.0 {
		t.Errorf("POWER(2, 3) = %v, want 8.0", result)
	}
}

func TestPOWER_FractionalExponent(t *testing.T) {
	result, err := GetRegistry().Call("POWER", []models.FieldValue{
		models.NewIntValue(4),
		models.NewFloatValue(0.5),
	}, nil)
	if err != nil {
		t.Fatalf("POWER(4, 0.5) error: %v", err)
	}
	if result.Type != models.FieldTypeFloat || result.FloatVal != 2.0 {
		t.Errorf("POWER(4, 0.5) = %v, want 2.0", result)
	}
}

func TestPOWER_NullPropagation(t *testing.T) {
	result, err := GetRegistry().Call("POWER", []models.FieldValue{
		{Type: models.FieldTypeNil},
		models.NewIntValue(3),
	}, nil)
	if err != nil {
		t.Fatalf("POWER(nil, 3) error: %v", err)
	}
	if result.Type != models.FieldTypeNil {
		t.Errorf("POWER(nil, 3) = %v, want nil", result)
	}
}

func TestPOWER_NonNumericError(t *testing.T) {
	_, err := GetRegistry().Call("POWER", []models.FieldValue{
		models.NewStringValue("abc"),
		models.NewIntValue(2),
	}, nil)
	if err == nil {
		t.Fatal("POWER('abc', 2) should error")
	}
}

func TestSQRT_PerfectSquare(t *testing.T) {
	result, err := GetRegistry().Call("SQRT", []models.FieldValue{
		models.NewIntValue(9),
	}, nil)
	if err != nil {
		t.Fatalf("SQRT(9) error: %v", err)
	}
	if result.Type != models.FieldTypeFloat || result.FloatVal != 3.0 {
		t.Errorf("SQRT(9) = %v, want 3.0", result)
	}
}

func TestSQRT_Zero(t *testing.T) {
	result, err := GetRegistry().Call("SQRT", []models.FieldValue{
		models.NewIntValue(0),
	}, nil)
	if err != nil {
		t.Fatalf("SQRT(0) error: %v", err)
	}
	if result.Type != models.FieldTypeFloat || result.FloatVal != 0.0 {
		t.Errorf("SQRT(0) = %v, want 0.0", result)
	}
}

func TestSQRT_Negative(t *testing.T) {
	_, err := GetRegistry().Call("SQRT", []models.FieldValue{
		models.NewIntValue(-1),
	}, nil)
	if err == nil {
		t.Fatal("SQRT(-1) should error")
	}
}

func TestSQRT_NullPropagation(t *testing.T) {
	result, err := GetRegistry().Call("SQRT", []models.FieldValue{
		{Type: models.FieldTypeNil},
	}, nil)
	if err != nil {
		t.Fatalf("SQRT(nil) error: %v", err)
	}
	if result.Type != models.FieldTypeNil {
		t.Errorf("SQRT(nil) = %v, want nil", result)
	}
}

func TestMOD_IntegerArgs(t *testing.T) {
	result, err := GetRegistry().Call("MOD", []models.FieldValue{
		models.NewIntValue(10),
		models.NewIntValue(3),
	}, nil)
	if err != nil {
		t.Fatalf("MOD(10, 3) error: %v", err)
	}
	if result.Type != models.FieldTypeInt || result.IntVal != 1 {
		t.Errorf("MOD(10, 3) = %v, want 1 (int)", result)
	}
}

func TestMOD_FloatArgs(t *testing.T) {
	result, err := GetRegistry().Call("MOD", []models.FieldValue{
		models.NewFloatValue(10.5),
		models.NewFloatValue(3.0),
	}, nil)
	if err != nil {
		t.Fatalf("MOD(10.5, 3.0) error: %v", err)
	}
	if result.Type != models.FieldTypeFloat || math.Abs(result.FloatVal-1.5) > 1e-9 {
		t.Errorf("MOD(10.5, 3.0) = %v, want 1.5 (float)", result)
	}
}

func TestMOD_DivisionByZero(t *testing.T) {
	_, err := GetRegistry().Call("MOD", []models.FieldValue{
		models.NewIntValue(7),
		models.NewIntValue(0),
	}, nil)
	if err == nil {
		t.Fatal("MOD(7, 0) should error")
	}
}

func TestMOD_NullPropagation(t *testing.T) {
	result, err := GetRegistry().Call("MOD", []models.FieldValue{
		{Type: models.FieldTypeNil},
		models.NewIntValue(3),
	}, nil)
	if err != nil {
		t.Fatalf("MOD(nil, 3) error: %v", err)
	}
	if result.Type != models.FieldTypeNil {
		t.Errorf("MOD(nil, 3) = %v, want nil", result)
	}
}

func TestLOG_NaturalLog(t *testing.T) {
	result, err := GetRegistry().Call("LOG", []models.FieldValue{
		models.NewFloatValue(math.E),
	}, nil)
	if err != nil {
		t.Fatalf("LOG(e) error: %v", err)
	}
	if result.Type != models.FieldTypeFloat || math.Abs(result.FloatVal-1.0) > 1e-9 {
		t.Errorf("LOG(e) = %v, want 1.0", result)
	}
}

func TestLOG_NaturalLogOfOne(t *testing.T) {
	result, err := GetRegistry().Call("LOG", []models.FieldValue{
		models.NewFloatValue(1.0),
	}, nil)
	if err != nil {
		t.Fatalf("LOG(1) error: %v", err)
	}
	if result.Type != models.FieldTypeFloat || result.FloatVal != 0.0 {
		t.Errorf("LOG(1) = %v, want 0.0", result)
	}
}

func TestLOG_NegativeError(t *testing.T) {
	_, err := GetRegistry().Call("LOG", []models.FieldValue{
		models.NewFloatValue(-1.0),
	}, nil)
	if err == nil {
		t.Fatal("LOG(-1) should error")
	}
}

func TestLOG_BaseAndNumber(t *testing.T) {
	result, err := GetRegistry().Call("LOG", []models.FieldValue{
		models.NewIntValue(10),
		models.NewIntValue(100),
	}, nil)
	if err != nil {
		t.Fatalf("LOG(10, 100) error: %v", err)
	}
	if result.Type != models.FieldTypeFloat || math.Abs(result.FloatVal-2.0) > 1e-9 {
		t.Errorf("LOG(10, 100) = %v, want 2.0", result)
	}
}

func TestLOG_BaseOneError(t *testing.T) {
	_, err := GetRegistry().Call("LOG", []models.FieldValue{
		models.NewIntValue(1),
		models.NewIntValue(100),
	}, nil)
	if err == nil {
		t.Fatal("LOG(1, 100) should error (base=1 is undefined)")
	}
}

func TestLOG_NullPropagation(t *testing.T) {
	result, err := GetRegistry().Call("LOG", []models.FieldValue{
		{Type: models.FieldTypeNil},
	}, nil)
	if err != nil {
		t.Fatalf("LOG(nil) error: %v", err)
	}
	if result.Type != models.FieldTypeNil {
		t.Errorf("LOG(nil) = %v, want nil", result)
	}
}

func TestSIGN_Positive(t *testing.T) {
	result, err := GetRegistry().Call("SIGN", []models.FieldValue{
		models.NewIntValue(42),
	}, nil)
	if err != nil {
		t.Fatalf("SIGN(42) error: %v", err)
	}
	if result.Type != models.FieldTypeInt || result.IntVal != 1 {
		t.Errorf("SIGN(42) = %v, want 1", result)
	}
}

func TestSIGN_Negative(t *testing.T) {
	result, err := GetRegistry().Call("SIGN", []models.FieldValue{
		models.NewIntValue(-7),
	}, nil)
	if err != nil {
		t.Fatalf("SIGN(-7) error: %v", err)
	}
	if result.Type != models.FieldTypeInt || result.IntVal != -1 {
		t.Errorf("SIGN(-7) = %v, want -1", result)
	}
}

func TestSIGN_Zero(t *testing.T) {
	result, err := GetRegistry().Call("SIGN", []models.FieldValue{
		models.NewIntValue(0),
	}, nil)
	if err != nil {
		t.Fatalf("SIGN(0) error: %v", err)
	}
	if result.Type != models.FieldTypeInt || result.IntVal != 0 {
		t.Errorf("SIGN(0) = %v, want 0", result)
	}
}

func TestSIGN_Float(t *testing.T) {
	result, err := GetRegistry().Call("SIGN", []models.FieldValue{
		models.NewFloatValue(3.14),
	}, nil)
	if err != nil {
		t.Fatalf("SIGN(3.14) error: %v", err)
	}
	if result.Type != models.FieldTypeInt || result.IntVal != 1 {
		t.Errorf("SIGN(3.14) = %v, want 1", result)
	}
}

func TestSIGN_NullPropagation(t *testing.T) {
	result, err := GetRegistry().Call("SIGN", []models.FieldValue{
		{Type: models.FieldTypeNil},
	}, nil)
	if err != nil {
		t.Fatalf("SIGN(nil) error: %v", err)
	}
	if result.Type != models.FieldTypeNil {
		t.Errorf("SIGN(nil) = %v, want nil", result)
	}
}
