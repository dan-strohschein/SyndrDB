package ir

import "fmt"

// Expr represents a recursive expression tree used in WHERE, HAVING, and other clauses.
type Expr struct {
	Type     string  `json:"type"`               // "binary","literal","identifier","function","in","notIn","isNull","isNotNull","not","between","exists","parameter","qualified"
	Left     *Expr   `json:"left,omitempty"`      // Binary: left operand; Not: inner expr
	Operator string  `json:"operator,omitempty"`  // Binary: "==","!=",">",">=","<","<=","AND","OR","LIKE","CONTAINS"
	Right    *Expr   `json:"right,omitempty"`     // Binary: right operand
	Value    *Value  `json:"value,omitempty"`     // Literal: typed value
	Name     string  `json:"name,omitempty"`      // Identifier: field name; Function: function name; Qualified: field name
	Bundle   string  `json:"bundle,omitempty"`    // Qualified: bundle name for "Bundle"."Field"
	Args     []*Expr `json:"args,omitempty"`      // Function: arguments; In/NotIn: value list
	IsBuiltIn bool   `json:"isBuiltIn,omitempty"` // Function: true means F: prefix (e.g. F:UPPER)
	Index    int     `json:"index,omitempty"`     // Parameter: $1, $2, etc. (1-indexed)
	Query    *SelectStmt `json:"query,omitempty"` // Exists/subquery: nested SELECT
}

// Value represents a typed literal value in expressions.
// Raw is always a string to ensure consistent JSON schema across all rows
// (required by PyArrow/Hugging Face datasets). The Type discriminator tells
// you how to interpret Raw.
type Value struct {
	Type string `json:"type"` // "string","int","float","bool","null"
	Raw  string `json:"raw"`  // Always a string: "active", "100", "9.99", "true", ""
}

// NewStringValue creates a string literal value.
func NewStringValue(s string) *Value {
	return &Value{Type: "string", Raw: s}
}

// NewIntValue creates an integer literal value.
func NewIntValue(n int64) *Value {
	return &Value{Type: "int", Raw: fmt.Sprintf("%d", n)}
}

// NewFloatValue creates a float literal value.
func NewFloatValue(f float64) *Value {
	return &Value{Type: "float", Raw: fmt.Sprintf("%g", f)}
}

// NewBoolValue creates a boolean literal value.
func NewBoolValue(b bool) *Value {
	if b {
		return &Value{Type: "bool", Raw: "true"}
	}
	return &Value{Type: "bool", Raw: "false"}
}

// NewNullValue creates a null literal value.
func NewNullValue() *Value {
	return &Value{Type: "null", Raw: ""}
}

// LiteralExpr creates a literal expression from a Value.
func LiteralExpr(v *Value) *Expr {
	return &Expr{Type: "literal", Value: v}
}

// IdentExpr creates an identifier expression.
func IdentExpr(name string) *Expr {
	return &Expr{Type: "identifier", Name: name}
}

// QualifiedExpr creates a qualified identifier expression ("Bundle"."Field").
func QualifiedExpr(bundle, field string) *Expr {
	return &Expr{Type: "qualified", Bundle: bundle, Name: field}
}

// BinaryExpr creates a binary expression.
func BinaryExpr(left *Expr, op string, right *Expr) *Expr {
	return &Expr{Type: "binary", Left: left, Operator: op, Right: right}
}

// NotExpr creates a NOT expression.
func NotExpr(inner *Expr) *Expr {
	return &Expr{Type: "not", Left: inner}
}

// IsNullExpr creates an IS NULL expression.
func IsNullExpr(field *Expr) *Expr {
	return &Expr{Type: "isNull", Left: field}
}

// IsNotNullExpr creates an IS NOT NULL expression.
func IsNotNullExpr(field *Expr) *Expr {
	return &Expr{Type: "isNotNull", Left: field}
}

// InExpr creates an IN expression.
func InExpr(field *Expr, values []*Expr) *Expr {
	return &Expr{Type: "in", Left: field, Args: values}
}

// NotInExpr creates a NOT IN expression.
func NotInExpr(field *Expr, values []*Expr) *Expr {
	return &Expr{Type: "notIn", Left: field, Args: values}
}

// FuncExpr creates a function call expression.
func FuncExpr(name string, isBuiltIn bool, args ...*Expr) *Expr {
	return &Expr{Type: "function", Name: name, IsBuiltIn: isBuiltIn, Args: args}
}

// ParamExpr creates a parameter placeholder ($N).
func ParamExpr(index int) *Expr {
	return &Expr{Type: "parameter", Index: index}
}

// ExistsExpr creates an EXISTS subquery expression.
func ExistsExpr(query *SelectStmt) *Expr {
	return &Expr{Type: "exists", Query: query}
}
