package syndrQL

import (
	"fmt"
	"strings"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// SchemaInfo holds the resolved schema for a bundle, used during analysis.
type SchemaInfo struct {
	BundleName string
	Fields     map[string]FieldType // field name -> resolved type
	HasSchema  bool                 // false for schema-less bundles
}

// NewSchemaInfoFromBundle creates a SchemaInfo from a models.Bundle.
func NewSchemaInfoFromBundle(bundle *models.Bundle) *SchemaInfo {
	if bundle == nil {
		return &SchemaInfo{HasSchema: false}
	}

	si := &SchemaInfo{
		BundleName: bundle.Name,
		HasSchema: bundle.DocumentStructure.FieldDefinitions != nil &&
			len(bundle.DocumentStructure.FieldDefinitions) > 0,
		Fields: make(map[string]FieldType),
	}

	if si.HasSchema {
		for name, def := range bundle.DocumentStructure.FieldDefinitions {
			si.Fields[name] = BundleFieldTypeToFieldType(def.Type)
		}
	}

	// Always include DocumentID as a known string field
	si.Fields["DocumentID"] = FieldTypeString
	si.Fields["documentid"] = FieldTypeString

	return si
}

// AnalyzerConfig controls semantic analysis behavior.
type AnalyzerConfig struct {
	// StrictMode rejects Unknown-type comparisons instead of allowing them.
	StrictMode bool

	// ValidateFieldNames checks that field names exist in the bundle schema.
	// When false, unknown fields resolve to FieldTypeUnknown.
	ValidateFieldNames bool
}

// SemanticAnalyzer performs semantic analysis on parsed SelectStatements.
type SemanticAnalyzer struct {
	schemas          map[string]*SchemaInfo
	primaryBundle    string
	config           AnalyzerConfig
	functionRegistry *FunctionRegistry
	errors           []*SemanticError
	logger           *zap.SugaredLogger
}

// NewSemanticAnalyzer creates a new semantic analyzer.
func NewSemanticAnalyzer(logger *zap.SugaredLogger) *SemanticAnalyzer {
	return &SemanticAnalyzer{
		schemas:          make(map[string]*SchemaInfo),
		functionRegistry: GetRegistry(),
		errors:           make([]*SemanticError, 0),
		logger:           logger,
	}
}

// WithConfig sets the analyzer configuration.
func (sa *SemanticAnalyzer) WithConfig(config AnalyzerConfig) *SemanticAnalyzer {
	sa.config = config
	return sa
}

// WithBundle registers a bundle's schema for name/type resolution.
func (sa *SemanticAnalyzer) WithBundle(bundle *models.Bundle) *SemanticAnalyzer {
	if bundle != nil {
		si := NewSchemaInfoFromBundle(bundle)
		sa.schemas[bundle.Name] = si
	}
	return sa
}

// SetPrimaryBundle sets the primary FROM bundle.
func (sa *SemanticAnalyzer) SetPrimaryBundle(name string) *SemanticAnalyzer {
	sa.primaryBundle = name
	return sa
}

// Analyze performs semantic analysis on a SelectStatement.
// It modifies expression nodes in-place by setting their ResolvedType fields.
// Returns the first SemanticError encountered, or nil if analysis succeeds.
func (sa *SemanticAnalyzer) Analyze(stmt *SelectStatement) error {
	if stmt == nil {
		return nil
	}

	sa.primaryBundle = stmt.BundleName
	sa.errors = sa.errors[:0]

	// 1. Resolve SELECT fields
	for i := range stmt.Fields {
		if stmt.Fields[i].Expression != nil {
			sa.resolveExpression(stmt.Fields[i].Expression)
		}
	}

	// 2. Resolve WHERE clause
	if stmt.WhereClause != nil {
		sa.resolveExpression(stmt.WhereClause)
		resultType := GetResolvedType(stmt.WhereClause)
		if resultType != FieldTypeBool && resultType != FieldTypeUnknown {
			sa.addError(NewSemanticError(ErrTypeMismatch,
				fmt.Sprintf("WHERE clause must be boolean, got %s", resultType)).
				WithExpression(stmt.WhereClause))
		}
		sa.checkNoAggregatesIn(stmt.WhereClause, "WHERE")
	}

	// 3. Resolve HAVING clause
	if stmt.Having != nil {
		sa.resolveExpression(stmt.Having)
		resultType := GetResolvedType(stmt.Having)
		if resultType != FieldTypeBool && resultType != FieldTypeUnknown {
			sa.addError(NewSemanticError(ErrTypeMismatch,
				fmt.Sprintf("HAVING clause must be boolean, got %s", resultType)).
				WithExpression(stmt.Having))
		}
	}

	// 4. Validate GROUP BY / aggregate rules
	sa.validateGroupByRules(stmt)

	if len(sa.errors) > 0 {
		return sa.errors[0]
	}
	return nil
}

// AnalyzeExpression analyzes a standalone expression (e.g., for WHERE-only
// analysis in UPDATE/DELETE statements).
func (sa *SemanticAnalyzer) AnalyzeExpression(expr Expression) error {
	if expr == nil {
		return nil
	}
	sa.errors = sa.errors[:0]
	sa.resolveExpression(expr)
	if len(sa.errors) > 0 {
		return sa.errors[0]
	}
	return nil
}

// Errors returns all accumulated semantic errors.
func (sa *SemanticAnalyzer) Errors() []*SemanticError {
	return sa.errors
}

func (sa *SemanticAnalyzer) addError(err *SemanticError) {
	sa.errors = append(sa.errors, err)
}

// resolveExpression is the main dispatch method. It walks the expression tree
// depth-first, resolving types bottom-up.
func (sa *SemanticAnalyzer) resolveExpression(expr Expression) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *LiteralExpression:
		sa.resolveLiteral(e)
	case *IdentifierExpression:
		sa.resolveIdentifier(e)
	case *QualifiedIdentifierExpression:
		sa.resolveQualifiedIdentifier(e)
	case *BinaryExpression:
		sa.resolveBinary(e)
	case *UnaryExpression:
		sa.resolveUnary(e)
	case *CallExpression:
		sa.resolveCall(e)
	case *GroupedExpression:
		sa.resolveGrouped(e)
	case *ArrayExpression:
		sa.resolveArray(e)
	case *SubqueryExpression:
		sa.resolveSubquery(e)
	case *ParameterExpression:
		e.ResolvedType = FieldTypeUnknown
	case *IntervalExpression:
		e.ResolvedType = FieldTypeInterval
	case *AtTimeZoneExpression:
		sa.resolveExpression(e.Expression)
		e.ResolvedType = FieldTypeDateTime
	case *CastExpression:
		sa.resolveExpression(e.Expression)
		e.ResolvedType = e.TargetType
	}
}

func (sa *SemanticAnalyzer) resolveLiteral(expr *LiteralExpression) {
	switch expr.Token {
	case TOKEN_NUMBER:
		switch expr.Value.(type) {
		case int64:
			expr.ResolvedType = FieldTypeInt
		case float64:
			expr.ResolvedType = FieldTypeFloat
		default:
			expr.ResolvedType = FieldTypeFloat
		}
	case TOKEN_STRING:
		expr.ResolvedType = FieldTypeString
	case TOKEN_TRUE, TOKEN_FALSE:
		expr.ResolvedType = FieldTypeBool
	case TOKEN_NULL:
		expr.ResolvedType = FieldTypeNull
	default:
		expr.ResolvedType = FieldTypeUnknown
	}
}

func (sa *SemanticAnalyzer) resolveIdentifier(expr *IdentifierExpression) {
	fieldName := strings.Trim(expr.Name, "\"")

	// Handle bundle.field notation in identifier names
	if strings.Contains(fieldName, ".") {
		parts := strings.SplitN(fieldName, ".", 2)
		bundleName := strings.Trim(parts[0], "\"")
		fieldOnly := strings.Trim(parts[1], "\"")
		expr.BundleName = bundleName
		expr.ResolvedType = sa.lookupFieldType(bundleName, fieldOnly)
		return
	}

	// Wildcard
	if fieldName == "*" {
		expr.ResolvedType = FieldTypeUnknown
		return
	}

	// Special: DocumentID is always String
	if strings.EqualFold(fieldName, "documentid") {
		expr.ResolvedType = FieldTypeString
		return
	}

	// Look up in primary bundle first, then joined bundles
	resolved := false
	if sa.primaryBundle != "" {
		if schema, ok := sa.schemas[sa.primaryBundle]; ok {
			if ft, found := schema.Fields[fieldName]; found {
				expr.ResolvedType = ft
				expr.BundleName = sa.primaryBundle
				resolved = true
			}
		}
	}

	if !resolved {
		// Search all schemas (for JOINs)
		matches := 0
		for bundleName, schema := range sa.schemas {
			if ft, found := schema.Fields[fieldName]; found {
				expr.ResolvedType = ft
				expr.BundleName = bundleName
				matches++
			}
		}
		if matches > 1 {
			sa.addError(NewSemanticError(ErrAmbiguousField,
				fmt.Sprintf("field '%s' is ambiguous; found in multiple bundles", fieldName)).
				WithField("", fieldName).
				WithSuggestion("qualify with bundle name"))
			return
		}
		if matches == 0 {
			expr.ResolvedType = FieldTypeUnknown
			if sa.config.ValidateFieldNames {
				if schema, hasSchema := sa.schemas[sa.primaryBundle]; hasSchema && schema.HasSchema {
					sa.addError(NewSemanticError(ErrUnknownField,
						fmt.Sprintf("field '%s' not found in bundle '%s'", fieldName, sa.primaryBundle)).
						WithField(sa.primaryBundle, fieldName))
				}
			}
		}
	}
}

func (sa *SemanticAnalyzer) resolveQualifiedIdentifier(expr *QualifiedIdentifierExpression) {
	bundleName := strings.Trim(expr.Bundle, "\"")
	fieldName := strings.Trim(expr.Field, "\"")
	expr.ResolvedType = sa.lookupFieldType(bundleName, fieldName)
}

func (sa *SemanticAnalyzer) resolveBinary(expr *BinaryExpression) {
	sa.resolveExpression(expr.Left)
	if expr.Right != nil {
		sa.resolveExpression(expr.Right)
	}

	leftType := GetResolvedType(expr.Left)
	rightType := FieldTypeNull
	if expr.Right != nil {
		rightType = GetResolvedType(expr.Right)
	}

	switch expr.Operator {
	// Logical operators: result is always Bool
	case TOKEN_AND, TOKEN_OR:
		expr.ResolvedType = FieldTypeBool
		if leftType != FieldTypeBool && leftType != FieldTypeUnknown {
			sa.addError(NewSemanticError(ErrTypeMismatch,
				fmt.Sprintf("%s operator requires boolean operands, left is %s",
					expr.Operator.String(), leftType)).
				WithExpression(expr))
		}
		if rightType != FieldTypeBool && rightType != FieldTypeUnknown {
			sa.addError(NewSemanticError(ErrTypeMismatch,
				fmt.Sprintf("%s operator requires boolean operands, right is %s",
					expr.Operator.String(), rightType)).
				WithExpression(expr))
		}

	// Comparison operators: result is Bool
	case TOKEN_EQ, TOKEN_NEQ, TOKEN_LT, TOKEN_LTE, TOKEN_GT, TOKEN_GTE,
		TOKEN_LIKE, TOKEN_IN, TOKEN_NOTIN, TOKEN_CONTAINS,
		TOKEN_IS_NULL, TOKEN_IS_NOT_NULL, TOKEN_ASSIGN:
		expr.ResolvedType = FieldTypeBool

		if expr.Operator == TOKEN_IS_NULL || expr.Operator == TOKEN_IS_NOT_NULL {
			break
		}
		if expr.Operator == TOKEN_LIKE {
			if leftType != FieldTypeString && leftType != FieldTypeUnknown {
				sa.addError(NewSemanticError(ErrTypeMismatch,
					"LIKE requires string operands").
					WithTypes(leftType, rightType).
					WithExpression(expr))
			}
			break
		}
		if expr.Operator == TOKEN_IN || expr.Operator == TOKEN_NOTIN {
			break
		}
		if expr.Operator == TOKEN_ASSIGN {
			break // Single = used as equality in WHERE
		}

		if !leftType.IsCompatibleWith(rightType) {
			sa.addError(NewSemanticError(ErrTypeMismatch,
				fmt.Sprintf("cannot compare %s with %s", leftType, rightType)).
				WithTypes(leftType, rightType).
				WithExpression(expr))
		}

	// Arithmetic operators: result is numeric
	case TOKEN_PLUS, TOKEN_MINUS, TOKEN_MULTIPLY, TOKEN_DIVIDE, TOKEN_MODULO:
		if leftType == FieldTypeInt && rightType == FieldTypeInt {
			expr.ResolvedType = FieldTypeInt
		} else if leftType.IsNumeric() && rightType.IsNumeric() {
			expr.ResolvedType = FieldTypeFloat
		} else {
			expr.ResolvedType = FieldTypeUnknown
			if !leftType.IsNumeric() && leftType != FieldTypeUnknown {
				sa.addError(NewSemanticError(ErrTypeMismatch,
					fmt.Sprintf("arithmetic requires numeric operands, left is %s", leftType)).
					WithExpression(expr))
			}
			if !rightType.IsNumeric() && rightType != FieldTypeUnknown {
				sa.addError(NewSemanticError(ErrTypeMismatch,
					fmt.Sprintf("arithmetic requires numeric operands, right is %s", rightType)).
					WithExpression(expr))
			}
		}

	default:
		expr.ResolvedType = FieldTypeUnknown
	}
}

func (sa *SemanticAnalyzer) resolveUnary(expr *UnaryExpression) {
	sa.resolveExpression(expr.Right)
	childType := GetResolvedType(expr.Right)

	switch expr.Operator {
	case TOKEN_NOT:
		expr.ResolvedType = FieldTypeBool
		if childType != FieldTypeBool && childType != FieldTypeUnknown {
			sa.addError(NewSemanticError(ErrTypeMismatch,
				fmt.Sprintf("NOT requires boolean operand, got %s", childType)).
				WithExpression(expr))
		}
	case TOKEN_MINUS, TOKEN_PLUS:
		if childType == FieldTypeInt {
			expr.ResolvedType = FieldTypeInt
		} else if childType.IsNumeric() {
			expr.ResolvedType = FieldTypeFloat
		} else {
			expr.ResolvedType = FieldTypeUnknown
		}
	default:
		expr.ResolvedType = FieldTypeUnknown
	}
}

func (sa *SemanticAnalyzer) resolveCall(expr *CallExpression) {
	for _, arg := range expr.Arguments {
		sa.resolveExpression(arg)
	}

	funcName := strings.ToUpper(expr.Function)

	// Validate against FunctionRegistry
	sig := sa.functionRegistry.Get(funcName)
	if sig != nil {
		argCount := len(expr.Arguments)
		if argCount < sig.MinArgs {
			sa.addError(NewSemanticError(ErrWrongArity,
				fmt.Sprintf("function %s requires at least %d arguments, got %d",
					funcName, sig.MinArgs, argCount)))
		}
		if sig.MaxArgs >= 0 && argCount > sig.MaxArgs {
			sa.addError(NewSemanticError(ErrWrongArity,
				fmt.Sprintf("function %s accepts at most %d arguments, got %d",
					funcName, sig.MaxArgs, argCount)))
		}
	}

	// Resolve return type
	switch funcName {
	case "COUNT":
		expr.ResolvedType = FieldTypeInt
	case "SUM":
		if len(expr.Arguments) > 0 {
			argType := GetResolvedType(expr.Arguments[0])
			if argType == FieldTypeInt {
				expr.ResolvedType = FieldTypeInt
			} else {
				expr.ResolvedType = FieldTypeFloat
			}
		} else {
			expr.ResolvedType = FieldTypeFloat
		}
	case "AVG":
		expr.ResolvedType = FieldTypeFloat
	case "MIN", "MAX":
		if len(expr.Arguments) > 0 {
			expr.ResolvedType = GetResolvedType(expr.Arguments[0])
		} else {
			expr.ResolvedType = FieldTypeUnknown
		}
	case "UPPER", "LOWER", "TRIM":
		expr.ResolvedType = FieldTypeString
	case "LENGTH":
		expr.ResolvedType = FieldTypeInt
	case "ABS", "CEIL", "FLOOR", "ROUND", "MOD":
		if len(expr.Arguments) > 0 {
			argType := GetResolvedType(expr.Arguments[0])
			if argType == FieldTypeInt {
				expr.ResolvedType = FieldTypeInt
			} else {
				expr.ResolvedType = FieldTypeFloat
			}
		} else {
			expr.ResolvedType = FieldTypeFloat
		}
	case "POWER", "SQRT", "LOG":
		expr.ResolvedType = FieldTypeFloat
	case "SIGN":
		expr.ResolvedType = FieldTypeInt
	case "NOW":
		expr.ResolvedType = FieldTypeDateTime
	case "EXTRACT":
		expr.ResolvedType = FieldTypeInt
	case "DATE_TRUNC":
		expr.ResolvedType = FieldTypeDateTime
	case "DATE_ADD", "DATE_SUB":
		expr.ResolvedType = FieldTypeDateTime
	case "AGE":
		expr.ResolvedType = FieldTypeInterval
	default:
		if sig == nil && sa.config.StrictMode {
			sa.addError(NewSemanticError(ErrUnknownFunction,
				fmt.Sprintf("unknown function: %s", funcName)))
		}
		expr.ResolvedType = FieldTypeUnknown
	}
}

func (sa *SemanticAnalyzer) resolveGrouped(expr *GroupedExpression) {
	sa.resolveExpression(expr.Expression)
	expr.ResolvedType = GetResolvedType(expr.Expression)
}

func (sa *SemanticAnalyzer) resolveArray(expr *ArrayExpression) {
	expr.ResolvedType = FieldTypeArray
	if len(expr.Elements) == 0 {
		expr.ElementType = FieldTypeUnknown
		return
	}

	sa.resolveExpression(expr.Elements[0])
	commonType := GetResolvedType(expr.Elements[0])

	for i := 1; i < len(expr.Elements); i++ {
		sa.resolveExpression(expr.Elements[i])
		elemType := GetResolvedType(expr.Elements[i])
		if elemType != commonType {
			if commonType == FieldTypeInt && elemType == FieldTypeFloat {
				commonType = FieldTypeFloat
			} else if commonType == FieldTypeFloat && elemType == FieldTypeInt {
				// Already float
			} else {
				commonType = FieldTypeUnknown
			}
		}
	}
	expr.ElementType = commonType
}

func (sa *SemanticAnalyzer) resolveSubquery(expr *SubqueryExpression) {
	switch expr.SubqueryType {
	case SUBQUERY_EXISTS, SUBQUERY_NOT_EXISTS:
		expr.ResolvedType = FieldTypeBool
	case SUBQUERY_IN, SUBQUERY_NOT_IN:
		if expr.InnerQuery != nil && len(expr.InnerQuery.Fields) > 0 {
			if expr.InnerQuery.Fields[0].Expression != nil {
				sa.resolveExpression(expr.InnerQuery.Fields[0].Expression)
				expr.ResolvedType = GetResolvedType(expr.InnerQuery.Fields[0].Expression)
			} else {
				expr.ResolvedType = FieldTypeUnknown
			}
		} else {
			expr.ResolvedType = FieldTypeUnknown
		}
	default:
		expr.ResolvedType = FieldTypeUnknown
	}
}

// lookupFieldType resolves the type of a field in a specific bundle.
func (sa *SemanticAnalyzer) lookupFieldType(bundleName, fieldName string) FieldType {
	if strings.EqualFold(fieldName, "documentid") {
		return FieldTypeString
	}

	schema, ok := sa.schemas[bundleName]
	if !ok {
		return FieldTypeUnknown
	}
	if !schema.HasSchema {
		return FieldTypeUnknown
	}

	ft, found := schema.Fields[fieldName]
	if !found {
		if sa.config.ValidateFieldNames {
			sa.addError(NewSemanticError(ErrUnknownField,
				fmt.Sprintf("field '%s' not found in bundle '%s'", fieldName, bundleName)).
				WithField(bundleName, fieldName))
		}
		return FieldTypeUnknown
	}
	return ft
}

// checkNoAggregatesIn walks the expression tree and reports an error if any
// aggregate function call is found.
func (sa *SemanticAnalyzer) checkNoAggregatesIn(expr Expression, clauseName string) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *CallExpression:
		funcName := strings.ToUpper(e.Function)
		if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" ||
			funcName == "MIN" || funcName == "MAX" {
			sa.addError(NewSemanticError(ErrAggregateInWhere,
				fmt.Sprintf("aggregate function %s is not allowed in %s clause", funcName, clauseName)).
				WithExpression(expr).
				WithSuggestion("move aggregate condition to HAVING clause"))
		}
		for _, arg := range e.Arguments {
			sa.checkNoAggregatesIn(arg, clauseName)
		}
	case *BinaryExpression:
		sa.checkNoAggregatesIn(e.Left, clauseName)
		sa.checkNoAggregatesIn(e.Right, clauseName)
	case *UnaryExpression:
		sa.checkNoAggregatesIn(e.Right, clauseName)
	case *GroupedExpression:
		sa.checkNoAggregatesIn(e.Expression, clauseName)
	}
}

// validateGroupByRules checks GROUP BY consistency.
func (sa *SemanticAnalyzer) validateGroupByRules(stmt *SelectStatement) {
	if len(stmt.GroupBy) == 0 {
		return
	}

	groupBySet := make(map[string]bool, len(stmt.GroupBy))
	for _, g := range stmt.GroupBy {
		groupBySet[strings.Trim(strings.ToLower(g), "\"")] = true
	}

	for _, field := range stmt.Fields {
		if field.Expression == nil {
			continue
		}
		// Skip aggregate functions
		if call, ok := field.Expression.(*CallExpression); ok {
			funcName := strings.ToUpper(call.Function)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" ||
				funcName == "MIN" || funcName == "MAX" {
				continue
			}
		}
		// Skip wildcard
		if ident, ok := field.Expression.(*IdentifierExpression); ok {
			if ident.Name == "*" {
				continue
			}
			fieldName := strings.Trim(strings.ToLower(ident.Name), "\"")
			if !groupBySet[fieldName] {
				sa.addError(NewSemanticError(ErrGroupByMismatch,
					fmt.Sprintf("field '%s' must appear in GROUP BY clause or be used in an aggregate function",
						ident.Name)).
					WithField(sa.primaryBundle, ident.Name))
			}
		}
		if qid, ok := field.Expression.(*QualifiedIdentifierExpression); ok {
			fieldName := strings.Trim(strings.ToLower(qid.Field), "\"")
			if !groupBySet[fieldName] {
				sa.addError(NewSemanticError(ErrGroupByMismatch,
					fmt.Sprintf("field '%s'.'%s' must appear in GROUP BY clause or be used in an aggregate function",
						qid.Bundle, qid.Field)).
					WithField(qid.Bundle, qid.Field))
			}
		}
	}
}
