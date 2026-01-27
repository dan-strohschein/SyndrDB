package models

import (
	"fmt"
	"strconv"
	"time"

	"github.com/dan-strohschein/HVJson/hvjson"
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
	// Convert to UTC for storage consistency
	// This ensures all DateTime values are stored in a canonical format
	utc := t.UTC()
	return FieldValue{
		Type:        FieldTypeDateTime,
		DateTimeVal: utc,
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
	// CRITICAL FIX: If already a FieldValue, return it directly (don't double-wrap)
	// This happens when validateAndConvertFieldTypeFast returns a FieldValue (e.g., NewDateValue)
	// and MakeDocumentFieldsPooled calls NewInterfaceValue on it
	if fv, ok := v.(FieldValue); ok {
		return fv
	}

	// Try to avoid interface{} if possible
	switch val := v.(type) {
	case string:
		// Auto-parse DateTime only when string looks like a date (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss).
		// Skip tryParseDateTime for plain strings (names, IDs, enums) to avoid 5x failed time.Parse
		// per value, which allocates ~204GB via time.newParseError under load (see pprof alloc_space).
		if len(val) >= 10 && val[4] == '-' && val[7] == '-' {
			if parsedTime, err := tryParseDateTime(val); err == nil {
				return NewDateTimeValue(parsedTime)
			}
		}
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
		// Return time.Time directly (string formatting handled by JSON marshaler)
		return fv.DateTimeVal
	case FieldTypeDate:
		// Return time.Time directly (string formatting handled by JSON marshaler)
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
// OPTIMIZED: Uses strconv instead of json.Marshal for primitive types
// to avoid reflection overhead and reduce allocations by ~10x
// ========================

// Pre-allocated byte slices for common JSON literals (zero allocation)
var (
	jsonTrue  = []byte("true")
	jsonFalse = []byte("false")
	jsonNull  = []byte("null")
)

func (fv FieldValue) MarshalJSON() ([]byte, error) {
	switch fv.Type {
	case FieldTypeString:
		// Fast path: use strconv.AppendQuote to avoid json.Marshal overhead
		// This eliminates reflection and reduces allocations
		return strconv.AppendQuote(make([]byte, 0, len(fv.StringVal)+2), fv.StringVal), nil
	case FieldTypeInt:
		// Fast path: strconv.FormatInt is ~10x faster than json.Marshal
		return []byte(strconv.FormatInt(fv.IntVal, 10)), nil
	case FieldTypeFloat:
		// Fast path: strconv.FormatFloat avoids reflection
		// Use 'g' format for compact representation (like JSON)
		return []byte(strconv.FormatFloat(fv.FloatVal, 'g', -1, 64)), nil
	case FieldTypeBool:
		// Fast path: use pre-allocated slices (zero allocation)
		if fv.BoolVal {
			return jsonTrue, nil
		}
		return jsonFalse, nil
	case FieldTypeDateTime:
		// Format DateTime as RFC3339 string for JSON
		// Pre-allocate buffer for RFC3339 format (25 chars + quotes)
		buf := make([]byte, 0, 32)
		buf = append(buf, '"')
		buf = fv.DateTimeVal.AppendFormat(buf, time.RFC3339)
		buf = append(buf, '"')
		return buf, nil
	case FieldTypeDate:
		// Format Date as "YYYY-MM-DD" string for JSON
		// Pre-allocate buffer for date format (10 chars + quotes)
		buf := make([]byte, 0, 14)
		buf = append(buf, '"')
		buf = fv.DateVal.AppendFormat(buf, "2006-01-02")
		buf = append(buf, '"')
		return buf, nil
	case FieldTypeInterface:
		// Fallback to hvjson.Marshal for complex types (arrays, objects)
		// PERF: hvjson is 27-42% faster than encoding/json
		return hvjson.Marshal(&fv.InterfaceVal)
	case FieldTypeNil:
		return jsonNull, nil
	default:
		return jsonNull, nil
	}
}

// ToJSONValue returns the FieldValue as a concrete Go value suitable for JSON encoding.
// This is used when FieldValue is stored in map[string]interface{} and the JSON encoder
// needs the actual value, not the FieldValue struct wrapper.
func (fv FieldValue) ToJSONValue() interface{} {
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
		case FieldTypeDateTime:
			return fv.DateTimeVal.Equal(other.DateTimeVal)
		case FieldTypeDate:
			return fv.DateVal.Equal(other.DateVal)
		case FieldTypeNil:
			return true
		}
	}

	// Cross-type: DateTime vs Date (compare time components)
	if fv.Type == FieldTypeDateTime && other.Type == FieldTypeDate {
		return fv.DateTimeVal.Equal(other.DateVal)
	}
	if fv.Type == FieldTypeDate && other.Type == FieldTypeDateTime {
		return fv.DateVal.Equal(other.DateTimeVal)
	}

	// Cross-type: DateTime/Date vs String (parse string as time)
	if fv.Type == FieldTypeDateTime && other.Type == FieldTypeString {
		if otherTime, err := parseDateTime(other.StringVal); err == nil {
			return fv.DateTimeVal.Equal(otherTime)
		}
	}
	if fv.Type == FieldTypeString && other.Type == FieldTypeDateTime {
		if fvTime, err := parseDateTime(fv.StringVal); err == nil {
			return fvTime.Equal(other.DateTimeVal)
		}
	}
	if fv.Type == FieldTypeDate && other.Type == FieldTypeString {
		if otherTime, err := parseDate(other.StringVal); err == nil {
			return fv.DateVal.Equal(otherTime)
		}
	}
	if fv.Type == FieldTypeString && other.Type == FieldTypeDate {
		if fvTime, err := parseDate(fv.StringVal); err == nil {
			return fvTime.Equal(other.DateVal)
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

	// DateTime/Date comparison (same type)
	if fv.Type == FieldTypeDateTime && other.Type == FieldTypeDateTime {
		return fv.DateTimeVal.Before(other.DateTimeVal)
	}
	if fv.Type == FieldTypeDate && other.Type == FieldTypeDate {
		return fv.DateVal.Before(other.DateVal)
	}

	// Cross-type: DateTime vs Date
	if fv.Type == FieldTypeDateTime && other.Type == FieldTypeDate {
		return fv.DateTimeVal.Before(other.DateVal)
	}
	if fv.Type == FieldTypeDate && other.Type == FieldTypeDateTime {
		return fv.DateVal.Before(other.DateTimeVal)
	}

	// Cross-type: DateTime/Date vs String
	if fv.Type == FieldTypeDateTime && other.Type == FieldTypeString {
		if otherTime, err := parseDateTime(other.StringVal); err == nil {
			return fv.DateTimeVal.Before(otherTime)
		}
	}
	if fv.Type == FieldTypeString && other.Type == FieldTypeDateTime {
		if fvTime, err := parseDateTime(fv.StringVal); err == nil {
			return fvTime.Before(other.DateTimeVal)
		}
	}
	if fv.Type == FieldTypeDate && other.Type == FieldTypeString {
		if otherTime, err := parseDate(other.StringVal); err == nil {
			return fv.DateVal.Before(otherTime)
		}
	}
	if fv.Type == FieldTypeString && other.Type == FieldTypeDate {
		if fvTime, err := parseDate(fv.StringVal); err == nil {
			return fvTime.Before(other.DateVal)
		}
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

	// DateTime/Date comparison (same type)
	if fv.Type == FieldTypeDateTime && other.Type == FieldTypeDateTime {
		return fv.DateTimeVal.After(other.DateTimeVal)
	}
	if fv.Type == FieldTypeDate && other.Type == FieldTypeDate {
		return fv.DateVal.After(other.DateVal)
	}

	// Cross-type: DateTime vs Date
	if fv.Type == FieldTypeDateTime && other.Type == FieldTypeDate {
		return fv.DateTimeVal.After(other.DateVal)
	}
	if fv.Type == FieldTypeDate && other.Type == FieldTypeDateTime {
		return fv.DateVal.After(other.DateTimeVal)
	}

	// Cross-type: DateTime/Date vs String
	if fv.Type == FieldTypeDateTime && other.Type == FieldTypeString {
		if otherTime, err := parseDateTime(other.StringVal); err == nil {
			return fv.DateTimeVal.After(otherTime)
		}
	}
	if fv.Type == FieldTypeString && other.Type == FieldTypeDateTime {
		if fvTime, err := parseDateTime(fv.StringVal); err == nil {
			return fvTime.After(other.DateTimeVal)
		}
	}
	if fv.Type == FieldTypeDate && other.Type == FieldTypeString {
		if otherTime, err := parseDate(other.StringVal); err == nil {
			return fv.DateVal.After(otherTime)
		}
	}
	if fv.Type == FieldTypeString && other.Type == FieldTypeDate {
		if fvTime, err := parseDate(fv.StringVal); err == nil {
			return fvTime.After(other.DateVal)
		}
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

// parseDateTime attempts to parse a string as a DateTime value
// Supports multiple common formats: RFC3339, ISO8601, etc.
func parseDateTime(s string) (time.Time, error) {
	// Try common formats in order
	formats := []string{
		time.RFC3339,           // "2006-01-02T15:04:05Z07:00"
		time.RFC3339Nano,       // "2006-01-02T15:04:05.999999999Z07:00"
		"2006-01-02T15:04:05Z", // ISO8601 with Z
		"2006-01-02T15:04:05",  // ISO8601 without timezone
		"2006-01-02 15:04:05",  // SQL format
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse '%s' as datetime", s)
}

// tryParseDateTime is a non-error version for auto-detection
// Returns parsed time and nil error if string matches DateTime format
// Returns zero time and error if string is not a DateTime
func tryParseDateTime(s string) (time.Time, error) {
	return parseDateTime(s)
}

// parseDate attempts to parse a string as a Date value (time at midnight UTC)
// Supports formats like: "2024-11-22", "2024-11-22T00:00:00Z", etc.
func parseDate(s string) (time.Time, error) {
	// Try date-only format first
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	}

	// Try datetime formats and extract date component
	if t, err := parseDateTime(s); err == nil {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), nil
	}

	return time.Time{}, fmt.Errorf("unable to parse '%s' as date", s)
}
