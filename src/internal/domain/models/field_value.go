package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// FieldValueType is an enum for the discriminator
type FieldValueType uint8

const (
	FieldTypeNil FieldValueType = iota
	FieldTypeString
	FieldTypeInt
	FieldTypeFloat
	FieldTypeBool
	FieldTypeDateTime  // Full timestamp with time zone, stored as UTC
	FieldTypeDate      // Date only (time component zeroed at midnight UTC)
	FieldTypeInterface // Fallback for complex types (arrays, objects, etc.)
)

// FieldValue is a zero-allocation union type that replaces interface{}
// This eliminates ~100-200 allocations per query by avoiding boxing
type FieldValue struct {
	// Discriminator (which field is active)
	Type FieldValueType

	// Union storage (only one is used at a time)
	StringVal   string
	IntVal      int64
	FloatVal    float64
	BoolVal     bool
	DateTimeVal time.Time // DateTime: full timestamp in UTC
	DateVal     time.Time // Date: date only (time at midnight UTC)

	// Fallback for complex types that still need interface{}
	// (e.g., arrays, nested objects, null values)
	InterfaceVal interface{}
}

// Constructor functions (zero allocations)

func NewStringValue(s string) FieldValue {
	return FieldValue{
		Type:      FieldTypeString,
		StringVal: s,
	}
}

func NewIntValue(i int64) FieldValue {
	return FieldValue{
		Type:   FieldTypeInt,
		IntVal: i,
	}
}

func NewFloatValue(f float64) FieldValue {
	return FieldValue{
		Type:     FieldTypeFloat,
		FloatVal: f,
	}
}

func NewBoolValue(b bool) FieldValue {
	return FieldValue{
		Type:    FieldTypeBool,
		BoolVal: b,
	}
}

func NewDateTimeValue(t time.Time) FieldValue {
	// Ensure DateTime is stored in UTC
	return FieldValue{
		Type:        FieldTypeDateTime,
		DateTimeVal: t.UTC(),
	}
}

func NewDateValue(t time.Time) FieldValue {
	// Date stores only date component - zero out time to midnight UTC
	utc := t.UTC()
	return FieldValue{
		Type:    FieldTypeDate,
		DateVal: time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC),
	}
}

func NewInterfaceValue(v interface{}) FieldValue {
	// Try to avoid interface{} if possible
	switch val := v.(type) {
	case string:
		return NewStringValue(val)
	case int:
		return NewIntValue(int64(val))
	case int64:
		return NewIntValue(val)
	case int32:
		return NewIntValue(int64(val))
	case float64:
		return NewFloatValue(val)
	case float32:
		return NewFloatValue(float64(val))
	case bool:
		return NewBoolValue(val)
	case time.Time:
		// Default to DateTime for time.Time values from interface{}
		return NewDateTimeValue(val)
	case nil:
		return FieldValue{Type: FieldTypeNil}
	default:
		// Complex types (arrays, maps, etc.) must use interface{}
		return FieldValue{
			Type:         FieldTypeInterface,
			InterfaceVal: v,
		}
	}
}

// Accessor methods (zero allocations)

func (fv FieldValue) AsInterface() interface{} {
	switch fv.Type {
	case FieldTypeString:
		return fv.StringVal
	case FieldTypeInt:
		return fv.IntVal
	case FieldTypeFloat:
		return fv.FloatVal
	case FieldTypeBool:
		return fv.BoolVal
	case FieldTypeDateTime:
		return fv.DateTimeVal
	case FieldTypeDate:
		return fv.DateVal
	case FieldTypeInterface:
		return fv.InterfaceVal
	case FieldTypeNil:
		return nil
	default:
		return nil
	}
}

func (fv FieldValue) AsString() (string, bool) {
	if fv.Type == FieldTypeString {
		return fv.StringVal, true
	}
	return "", false
}

func (fv FieldValue) AsInt() (int64, bool) {
	if fv.Type == FieldTypeInt {
		return fv.IntVal, true
	}
	return 0, false
}

func (fv FieldValue) AsFloat() (float64, bool) {
	if fv.Type == FieldTypeFloat {
		return fv.FloatVal, true
	}
	return 0, false
}

func (fv FieldValue) AsBool() (bool, bool) {
	if fv.Type == FieldTypeBool {
		return fv.BoolVal, true
	}
	return false, false
}

func (fv FieldValue) AsDateTime() (time.Time, bool) {
	if fv.Type == FieldTypeDateTime {
		return fv.DateTimeVal, true
	}
	return time.Time{}, false
}

func (fv FieldValue) AsDate() (time.Time, bool) {
	if fv.Type == FieldTypeDate {
		return fv.DateVal, true
	}
	return time.Time{}, false
}

func (fv FieldValue) IsNil() bool {
	return fv.Type == FieldTypeNil
}

// String representation for debugging
func (fv FieldValue) String() string {
	switch fv.Type {
	case FieldTypeString:
		return fv.StringVal
	case FieldTypeInt:
		return fmt.Sprintf("%d", fv.IntVal)
	case FieldTypeFloat:
		return fmt.Sprintf("%f", fv.FloatVal)
	case FieldTypeBool:
		return fmt.Sprintf("%t", fv.BoolVal)
	case FieldTypeDateTime:
		return fv.DateTimeVal.Format(time.RFC3339)
	case FieldTypeDate:
		return fv.DateVal.Format("2006-01-02")
	case FieldTypeInterface:
		return fmt.Sprintf("%v", fv.InterfaceVal)
	case FieldTypeNil:
		return "nil"
	default:
		return "unknown"
	}
}

// BSON Marshaling/Unmarshaling (eliminates interface{} boxing during serialization)

func (fv FieldValue) MarshalBSON() ([]byte, error) {
	return bson.Marshal(fv.AsInterface())
}

func (fv *FieldValue) UnmarshalBSON(data []byte) error {
	// Unmarshal to raw BSON value to inspect type
	var raw bson.RawValue
	if err := bson.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Decode based on BSON type (avoids interface{} boxing)
	switch raw.Type {
	case bsontype.String:
		s, ok := raw.StringValueOK()
		if !ok {
			return fmt.Errorf("failed to decode string")
		}
		*fv = NewStringValue(s)

	case bsontype.Int32:
		i, ok := raw.Int32OK()
		if !ok {
			return fmt.Errorf("failed to decode int32")
		}
		*fv = NewIntValue(int64(i))

	case bsontype.Int64:
		i, ok := raw.Int64OK()
		if !ok {
			return fmt.Errorf("failed to decode int64")
		}
		*fv = NewIntValue(i)

	case bsontype.Double:
		f, ok := raw.DoubleOK()
		if !ok {
			return fmt.Errorf("failed to decode double")
		}
		*fv = NewFloatValue(f)

	case bsontype.Boolean:
		b, ok := raw.BooleanOK()
		if !ok {
			return fmt.Errorf("failed to decode boolean")
		}
		*fv = NewBoolValue(b)

	case bsontype.DateTime:
		// BSON DateTime is milliseconds since epoch
		ms, ok := raw.DateTimeOK()
		if !ok {
			return fmt.Errorf("failed to decode datetime")
		}
		t := time.Unix(0, ms*int64(time.Millisecond)).UTC()

		// Distinguish between DateTime and Date by checking if time is exactly midnight UTC
		if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
			*fv = NewDateValue(t)
		} else {
			*fv = NewDateTimeValue(t)
		}

	case bsontype.Null:
		*fv = FieldValue{Type: FieldTypeNil}

	default:
		// Complex types (arrays, documents, etc.)
		var v interface{}
		if err := bson.Unmarshal(data, &v); err != nil {
			return err
		}
		*fv = FieldValue{
			Type:         FieldTypeInterface,
			InterfaceVal: v,
		}
	}

	return nil
}

// Comparison helpers (for WHERE clauses, GROUP BY, etc.)

func (fv FieldValue) Equals(other FieldValue) bool {
	if fv.Type != other.Type {
		return false
	}

	switch fv.Type {
	case FieldTypeString:
		return fv.StringVal == other.StringVal
	case FieldTypeInt:
		return fv.IntVal == other.IntVal
	case FieldTypeFloat:
		return fv.FloatVal == other.FloatVal
	case FieldTypeBool:
		return fv.BoolVal == other.BoolVal
	case FieldTypeDateTime:
		// DateTime comparison with millisecond precision
		return fv.DateTimeVal.Truncate(time.Millisecond).Equal(other.DateTimeVal.Truncate(time.Millisecond))
	case FieldTypeDate:
		// Date comparison (time already zeroed)
		return fv.DateVal.Equal(other.DateVal)
	case FieldTypeNil:
		return true
	case FieldTypeInterface:
		return fmt.Sprintf("%v", fv.InterfaceVal) == fmt.Sprintf("%v", other.InterfaceVal)
	default:
		return false
	}
}

// Numeric conversion for aggregations (COUNT, SUM, AVG, etc.)

func (fv FieldValue) AsNumeric() (float64, bool) {
	switch fv.Type {
	case FieldTypeInt:
		return float64(fv.IntVal), true
	case FieldTypeFloat:
		return fv.FloatVal, true
	default:
		return 0, false
	}
}

// ========================
// CUSTOM JSON MARSHALING (zero allocations for response formatting)
// This eliminates the need to call .AsInterface() during response building
// ========================

func (fv FieldValue) MarshalJSON() ([]byte, error) {
	switch fv.Type {
	case FieldTypeString:
		return json.Marshal(fv.StringVal)
	case FieldTypeInt:
		return json.Marshal(fv.IntVal)
	case FieldTypeFloat:
		return json.Marshal(fv.FloatVal)
	case FieldTypeBool:
		return json.Marshal(fv.BoolVal)
	case FieldTypeDateTime:
		// Format DateTime as RFC3339 string for JSON
		return json.Marshal(fv.DateTimeVal.Format(time.RFC3339))
	case FieldTypeDate:
		// Format Date as "YYYY-MM-DD" string for JSON
		return json.Marshal(fv.DateVal.Format("2006-01-02"))
	case FieldTypeInterface:
		return json.Marshal(fv.InterfaceVal)
	case FieldTypeNil:
		return []byte("null"), nil
	default:
		return []byte("null"), nil
	}
}

// ========================
// ZERO-ALLOCATION COMPARISONS (for WHERE clauses)
// These work directly on FieldValue without boxing to interface{}
// ========================

// CompareEqual compares two FieldValues for equality (==)
// Handles type coercion for numeric comparisons
func (fv FieldValue) CompareEqual(other FieldValue) bool {
	// Handle NULL comparisons
	if fv.Type == FieldTypeString && fv.StringVal == "::SYNDR_NULL::" {
		if other.Type == FieldTypeString && other.StringVal == "::SYNDR_NULL::" {
			return true
		}
		return false
	}
	if other.Type == FieldTypeString && other.StringVal == "::SYNDR_NULL::" {
		return false
	}

	// Same type - direct comparison
	if fv.Type == other.Type {
		switch fv.Type {
		case FieldTypeString:
			return fv.StringVal == other.StringVal
		case FieldTypeInt:
			return fv.IntVal == other.IntVal
		case FieldTypeFloat:
			return fv.FloatVal == other.FloatVal
		case FieldTypeBool:
			return fv.BoolVal == other.BoolVal
		case FieldTypeNil:
			return true
		}
	}

	// Cross-type numeric comparison (int vs float)
	aNum, aOk := fv.AsNumeric()
	bNum, bOk := other.AsNumeric()
	if aOk && bOk {
		return aNum == bNum
	}

	// Cross-type: try to parse string as number for numeric equality
	if fv.Type == FieldTypeString {
		if aNum, err := parseNumeric(fv.StringVal); err == nil {
			if bNum, bOk := other.AsNumeric(); bOk {
				return aNum == bNum
			}
		}
	}
	if other.Type == FieldTypeString {
		if bNum, err := parseNumeric(other.StringVal); err == nil {
			if aNum, aOk := fv.AsNumeric(); aOk {
				return aNum == bNum
			}
		}
	}

	return false
}

// CompareLessThan compares two FieldValues (<)
func (fv FieldValue) CompareLessThan(other FieldValue) bool {
	// Handle NULL
	if fv.Type == FieldTypeString && fv.StringVal == "::SYNDR_NULL::" {
		return false
	}
	if other.Type == FieldTypeString && other.StringVal == "::SYNDR_NULL::" {
		return false
	}

	// String comparison (including numeric strings like "1970")
	if fv.Type == FieldTypeString && other.Type == FieldTypeString {
		return fv.StringVal < other.StringVal
	}

	// Numeric comparison (handles int vs float)
	aNum, aOk := fv.AsNumeric()
	bNum, bOk := other.AsNumeric()
	if aOk && bOk {
		return aNum < bNum
	}

	// Cross-type: try to parse string as number
	if fv.Type == FieldTypeString {
		if aNum, err := parseNumeric(fv.StringVal); err == nil {
			if bNum, bOk := other.AsNumeric(); bOk {
				return aNum < bNum
			}
		}
	}
	if other.Type == FieldTypeString {
		if bNum, err := parseNumeric(other.StringVal); err == nil {
			if aNum, aOk := fv.AsNumeric(); aOk {
				return aNum < bNum
			}
		}
	}

	return false
}

// CompareLessThanOrEqual compares two FieldValues (<=)
func (fv FieldValue) CompareLessThanOrEqual(other FieldValue) bool {
	return fv.CompareLessThan(other) || fv.CompareEqual(other)
}

// CompareGreaterThan compares two FieldValues (>)
func (fv FieldValue) CompareGreaterThan(other FieldValue) bool {
	// Handle NULL
	if fv.Type == FieldTypeString && fv.StringVal == "::SYNDR_NULL::" {
		return false
	}
	if other.Type == FieldTypeString && other.StringVal == "::SYNDR_NULL::" {
		return false
	}

	// String comparison
	if fv.Type == FieldTypeString && other.Type == FieldTypeString {
		return fv.StringVal > other.StringVal
	}

	// Numeric comparison (handles int vs float)
	aNum, aOk := fv.AsNumeric()
	bNum, bOk := other.AsNumeric()
	if aOk && bOk {
		return aNum > bNum
	}

	// Cross-type: try to parse string as number
	if fv.Type == FieldTypeString {
		if aNum, err := parseNumeric(fv.StringVal); err == nil {
			if bNum, bOk := other.AsNumeric(); bOk {
				return aNum > bNum
			}
		}
	}
	if other.Type == FieldTypeString {
		if bNum, err := parseNumeric(other.StringVal); err == nil {
			if aNum, aOk := fv.AsNumeric(); aOk {
				return aNum > bNum
			}
		}
	}

	return false
}

// CompareGreaterThanOrEqual compares two FieldValues (>=)
func (fv FieldValue) CompareGreaterThanOrEqual(other FieldValue) bool {
	return fv.CompareGreaterThan(other) || fv.CompareEqual(other)
}

// CompareNotEqual compares two FieldValues (!=)
func (fv FieldValue) CompareNotEqual(other FieldValue) bool {
	return !fv.CompareEqual(other)
}

// parseNumeric attempts to parse a string as a numeric value
// Returns the numeric value and nil error if successful
func parseNumeric(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
