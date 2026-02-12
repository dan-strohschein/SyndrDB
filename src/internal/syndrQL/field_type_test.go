package syndrQL

import "testing"

func TestFieldTypeString(t *testing.T) {
	tests := []struct {
		ft   FieldType
		want string
	}{
		{FieldTypeUnknown, "Unknown"},
		{FieldTypeString, "String"},
		{FieldTypeInt, "Int"},
		{FieldTypeFloat, "Float"},
		{FieldTypeBool, "Bool"},
		{FieldTypeNull, "Null"},
		{FieldTypeDateTime, "DateTime"},
		{FieldTypeDate, "Date"},
		{FieldTypeArray, "Array"},
		{FieldTypeInterval, "Interval"},
		{FieldTypeDocument, "Document"},
		{FieldType(999), "Unknown"}, // Out of range defaults to Unknown
	}
	for _, tc := range tests {
		if got := tc.ft.String(); got != tc.want {
			t.Errorf("FieldType(%d).String() = %q, want %q", tc.ft, got, tc.want)
		}
	}
}

func TestIsNumeric(t *testing.T) {
	numeric := []FieldType{FieldTypeInt, FieldTypeFloat, FieldTypeUnknown}
	notNumeric := []FieldType{FieldTypeString, FieldTypeBool, FieldTypeNull, FieldTypeDateTime, FieldTypeDate, FieldTypeArray, FieldTypeInterval, FieldTypeDocument}

	for _, ft := range numeric {
		if !ft.IsNumeric() {
			t.Errorf("%s.IsNumeric() = false, want true", ft)
		}
	}
	for _, ft := range notNumeric {
		if ft.IsNumeric() {
			t.Errorf("%s.IsNumeric() = true, want false", ft)
		}
	}
}

func TestIsComparable(t *testing.T) {
	comparable := []FieldType{FieldTypeInt, FieldTypeFloat, FieldTypeString, FieldTypeDateTime, FieldTypeDate, FieldTypeUnknown}
	notComparable := []FieldType{FieldTypeBool, FieldTypeNull, FieldTypeArray, FieldTypeInterval, FieldTypeDocument}

	for _, ft := range comparable {
		if !ft.IsComparable() {
			t.Errorf("%s.IsComparable() = false, want true", ft)
		}
	}
	for _, ft := range notComparable {
		if ft.IsComparable() {
			t.Errorf("%s.IsComparable() = true, want false", ft)
		}
	}
}

func TestIsCompatibleWith(t *testing.T) {
	tests := []struct {
		a, b FieldType
		want bool
	}{
		// Unknown is compatible with everything
		{FieldTypeUnknown, FieldTypeString, true},
		{FieldTypeString, FieldTypeUnknown, true},
		{FieldTypeUnknown, FieldTypeUnknown, true},

		// Null is compatible with everything
		{FieldTypeNull, FieldTypeString, true},
		{FieldTypeInt, FieldTypeNull, true},
		{FieldTypeNull, FieldTypeNull, true},

		// Int and Float are cross-compatible
		{FieldTypeInt, FieldTypeFloat, true},
		{FieldTypeFloat, FieldTypeInt, true},
		{FieldTypeInt, FieldTypeInt, true},
		{FieldTypeFloat, FieldTypeFloat, true},

		// DateTime and Date are cross-compatible
		{FieldTypeDateTime, FieldTypeDate, true},
		{FieldTypeDate, FieldTypeDateTime, true},

		// Same types are compatible
		{FieldTypeString, FieldTypeString, true},
		{FieldTypeBool, FieldTypeBool, true},

		// Incompatible types
		{FieldTypeString, FieldTypeInt, false},
		{FieldTypeString, FieldTypeFloat, false},
		{FieldTypeBool, FieldTypeInt, false},
		{FieldTypeString, FieldTypeBool, false},
		{FieldTypeArray, FieldTypeString, false},
		{FieldTypeDateTime, FieldTypeInt, false},
	}

	for _, tc := range tests {
		got := tc.a.IsCompatibleWith(tc.b)
		if got != tc.want {
			t.Errorf("%s.IsCompatibleWith(%s) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestBundleFieldTypeToFieldType(t *testing.T) {
	tests := []struct {
		input string
		want  FieldType
	}{
		{"string", FieldTypeString},
		{"String", FieldTypeString},
		{"STRING", FieldTypeString},
		{"int", FieldTypeInt},
		{"integer", FieldTypeInt},
		{"float", FieldTypeFloat},
		{"double", FieldTypeFloat},
		{"number", FieldTypeFloat},
		{"bool", FieldTypeBool},
		{"boolean", FieldTypeBool},
		{"datetime", FieldTypeDateTime},
		{"timestamp", FieldTypeDateTime},
		{"date", FieldTypeDate},
		{"json", FieldTypeDocument},
		{"document", FieldTypeDocument},
		{"object", FieldTypeDocument},
		{"array", FieldTypeArray},
		{"unknown_type", FieldTypeUnknown},
		{"", FieldTypeUnknown},
	}

	for _, tc := range tests {
		got := BundleFieldTypeToFieldType(tc.input)
		if got != tc.want {
			t.Errorf("BundleFieldTypeToFieldType(%q) = %s, want %s", tc.input, got, tc.want)
		}
	}
}

func TestGetResolvedType(t *testing.T) {
	// Nil expression
	if got := GetResolvedType(nil); got != FieldTypeUnknown {
		t.Errorf("GetResolvedType(nil) = %s, want Unknown", got)
	}

	// LiteralExpression
	lit := &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(42), ResolvedType: FieldTypeInt}
	if got := GetResolvedType(lit); got != FieldTypeInt {
		t.Errorf("GetResolvedType(literal) = %s, want Int", got)
	}

	// IdentifierExpression
	ident := &IdentifierExpression{Name: "age", ResolvedType: FieldTypeFloat}
	if got := GetResolvedType(ident); got != FieldTypeFloat {
		t.Errorf("GetResolvedType(identifier) = %s, want Float", got)
	}

	// BinaryExpression
	bin := &BinaryExpression{Left: lit, Operator: TOKEN_GT, Right: lit, ResolvedType: FieldTypeBool}
	if got := GetResolvedType(bin); got != FieldTypeBool {
		t.Errorf("GetResolvedType(binary) = %s, want Bool", got)
	}

	// Default (unset) is Unknown
	unset := &LiteralExpression{Token: TOKEN_STRING, Value: "hello"}
	if got := GetResolvedType(unset); got != FieldTypeUnknown {
		t.Errorf("GetResolvedType(unset) = %s, want Unknown", got)
	}

	// QualifiedIdentifierExpression
	qid := &QualifiedIdentifierExpression{Bundle: "Users", Field: "name", ResolvedType: FieldTypeString}
	if got := GetResolvedType(qid); got != FieldTypeString {
		t.Errorf("GetResolvedType(qualified) = %s, want String", got)
	}

	// CallExpression
	call := &CallExpression{Function: "COUNT", Arguments: nil, ResolvedType: FieldTypeInt}
	if got := GetResolvedType(call); got != FieldTypeInt {
		t.Errorf("GetResolvedType(call) = %s, want Int", got)
	}

	// IntervalExpression
	interval := &IntervalExpression{Value: "7", Unit: TOKEN_DAY, ResolvedType: FieldTypeInterval}
	if got := GetResolvedType(interval); got != FieldTypeInterval {
		t.Errorf("GetResolvedType(interval) = %s, want Interval", got)
	}
}
