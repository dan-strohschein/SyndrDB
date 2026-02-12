package syndrQL

import (
	"fmt"
	"strings"
)

// SemanticErrorCode categorizes semantic errors for programmatic handling.
type SemanticErrorCode int

const (
	ErrUnknownField SemanticErrorCode = iota
	ErrTypeMismatch
	ErrInvalidOperator
	ErrUnknownFunction
	ErrWrongArity
	ErrAggregateInWhere
	ErrNonAggregateInHaving
	ErrGroupByMismatch
	ErrAmbiguousField
	ErrInvalidNullUsage
)

// SemanticError represents an error detected during semantic analysis.
type SemanticError struct {
	Code       SemanticErrorCode
	Message    string
	BundleName string    // Bundle where the error occurred (if applicable)
	FieldName  string    // Field involved (if applicable)
	LeftType   FieldType // Left operand type (for type mismatches)
	RightType  FieldType // Right operand type (for type mismatches)
	Expression string    // String representation of the offending expression
	Suggestion string    // Suggested fix
}

func (e *SemanticError) Error() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("semantic error: %s", e.Message))
	if e.BundleName != "" {
		parts = append(parts, fmt.Sprintf("bundle: %s", e.BundleName))
	}
	if e.FieldName != "" {
		parts = append(parts, fmt.Sprintf("field: %s", e.FieldName))
	}
	if e.LeftType != FieldTypeUnknown && e.RightType != FieldTypeUnknown {
		parts = append(parts, fmt.Sprintf("types: %s vs %s", e.LeftType, e.RightType))
	}
	if e.Expression != "" {
		parts = append(parts, fmt.Sprintf("in: %s", e.Expression))
	}
	if e.Suggestion != "" {
		parts = append(parts, fmt.Sprintf("suggestion: %s", e.Suggestion))
	}
	return strings.Join(parts, " | ")
}

// NewSemanticError creates a SemanticError with the given code and message.
func NewSemanticError(code SemanticErrorCode, message string) *SemanticError {
	return &SemanticError{
		Code:    code,
		Message: message,
	}
}

// WithField adds field context.
func (e *SemanticError) WithField(bundle, field string) *SemanticError {
	e.BundleName = bundle
	e.FieldName = field
	return e
}

// WithTypes adds type context.
func (e *SemanticError) WithTypes(left, right FieldType) *SemanticError {
	e.LeftType = left
	e.RightType = right
	return e
}

// WithExpression adds expression context.
func (e *SemanticError) WithExpression(expr Expression) *SemanticError {
	if expr != nil {
		e.Expression = expr.String()
	}
	return e
}

// WithSuggestion adds a suggested fix.
func (e *SemanticError) WithSuggestion(s string) *SemanticError {
	e.Suggestion = s
	return e
}
