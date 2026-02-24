package syndrQL

import (
	"fmt"
	"strings"

	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

// Expression represents a parsed expression node in the AST
type Expression interface {
	expressionNode()
	String() string
}

// ExpressionType categorizes expressions for optimization
type ExpressionType int

const (
	EXPR_LITERAL ExpressionType = iota
	EXPR_IDENTIFIER
	EXPR_BINARY
	EXPR_UNARY
	EXPR_CALL
	EXPR_ARRAY
	EXPR_GROUPED
	EXPR_SUBQUERY     // Added for Tier 1 subquery support
	EXPR_INTERVAL     // Added for DateTime INTERVAL literals
	EXPR_AT_TIME_ZONE // Added for AT TIME ZONE operator
)

// LiteralExpression represents a literal value (string, number, bool, null)
type LiteralExpression struct {
	Token        TokenType
	Value        interface{} // Actual value (string, int64, float64, bool, nil)
	ResolvedType FieldType   // Set by semantic analyzer
}

func (le *LiteralExpression) expressionNode() {}
func (le *LiteralExpression) String() string {
	if le.Value == nil {
		return "NULL"
	}
	// Add quotes for string literals so they can be re-parsed correctly
	if str, ok := le.Value.(string); ok {
		return fmt.Sprintf("\"%s\"", str)
	}
	return fmt.Sprintf("%v", le.Value)
}

// IdentifierExpression represents a field name or identifier
type IdentifierExpression struct {
	Name         string
	ResolvedType FieldType // Set by semantic analyzer from bundle schema
	BundleName   string    // Set when resolved to a specific bundle (for JOINs)
}

func (ie *IdentifierExpression) expressionNode() {}
func (ie *IdentifierExpression) String() string {
	return ie.Name
}

// BinaryExpression represents a binary operation (left op right)
type BinaryExpression struct {
	Left         Expression
	Operator     TokenType
	Right        Expression
	ResolvedType FieldType // Result type of the operation (e.g., Bool for comparisons)
}

func (be *BinaryExpression) expressionNode() {}
func (be *BinaryExpression) String() string {
	// Handle IS NULL and IS NOT NULL which don't have a right operand
	if be.Right == nil {
		return fmt.Sprintf("(%s %s)", be.Left.String(), be.Operator.String())
	}
	return fmt.Sprintf("(%s %s %s)", be.Left.String(), be.Operator.String(), be.Right.String())
}

// UnaryExpression represents a unary operation (op right)
type UnaryExpression struct {
	Operator     TokenType
	Right        Expression
	ResolvedType FieldType
}

func (ue *UnaryExpression) expressionNode() {}
func (ue *UnaryExpression) String() string {
	return fmt.Sprintf("(%s%s)", ue.Operator.String(), ue.Right.String())
}

// CallExpression represents a function call
type CallExpression struct {
	Function     string
	Arguments    []Expression
	ResolvedType FieldType // Return type of the function
}

func (ce *CallExpression) expressionNode() {}
func (ce *CallExpression) String() string {
	args := make([]string, len(ce.Arguments))
	for i, arg := range ce.Arguments {
		args[i] = arg.String()
	}
	// Keep F: prefix for function calls to ensure correct re-parsing
	return fmt.Sprintf("F:%s(%s)", ce.Function, strings.Join(args, ", "))
}

// ArrayExpression represents an array literal [value1, value2, ...]
type ArrayExpression struct {
	Elements    []Expression
	ResolvedType FieldType // FieldTypeArray
	ElementType  FieldType // Common type of elements (Unknown if mixed)
}

func (ae *ArrayExpression) expressionNode() {}
func (ae *ArrayExpression) String() string {
	elements := make([]string, len(ae.Elements))
	for i, elem := range ae.Elements {
		elements[i] = elem.String()
	}
	return fmt.Sprintf("[%s]", strings.Join(elements, ", "))
}

// GroupedExpression represents a parenthesized expression
type GroupedExpression struct {
	Expression   Expression
	ResolvedType FieldType // Same as inner expression's type
}

func (ge *GroupedExpression) expressionNode() {}
func (ge *GroupedExpression) String() string {
	return fmt.Sprintf("(%s)", ge.Expression.String())
}

// QualifiedIdentifierExpression represents a qualified field name like "Bundle"."Field"
type QualifiedIdentifierExpression struct {
	Bundle       string
	Field        string
	ResolvedType FieldType
}

func (qie *QualifiedIdentifierExpression) expressionNode() {}
func (qie *QualifiedIdentifierExpression) String() string {
	return fmt.Sprintf("\"%s\".\"%s\"", qie.Bundle, qie.Field)
}

// SubqueryExpression represents a subquery in IN or EXISTS clauses
// Supports uncorrelated subqueries for Tier 1 implementation.
// TODO: Add correlated subquery support in Tier 3 with parameter binding
// TODO: Add support for scalar subqueries (SELECT field FROM ... LIMIT 1)
// TODO: Add support for derived table subqueries (FROM (SELECT ...) AS alias)
type SubqueryExpression struct {
	SubqueryType     SubqueryType     // IN, EXISTS, NOT IN (Tier 1), or scalar/derived (future)
	InnerQuery       *SelectStatement // The SELECT statement inside the subquery
	NestingDepth     int              // Current nesting level (enforced max from config)
	IsCorrelated     bool             // Whether subquery references outer query (false for Tier 1)
	CorrelatedFields []string         // Fields that reference outer query (empty for Tier 1)
	ResolvedType     FieldType        // Bool for EXISTS, matches inner for IN

	// Tier 3: Correlated subquery rewrite metadata (set by SubqueryRewriter during planning)
	RewriteStrategy    int        // 0=unchanged, 1=semi-join, 2=anti-join, 3=materialization, 4=nested-loop
	OuterJoinFields    []string   // Outer bundle fields for join (e.g., ["ID"])
	InnerJoinFields    []string   // Inner bundle fields for join (e.g., ["AuthorID"])
	NonCorrelatedWhere Expression // Inner WHERE with correlated predicates removed (nil = no remaining predicates)
}

// SubqueryType categorizes the type of subquery
type SubqueryType int

const (
	SUBQUERY_IN         SubqueryType = iota // field IN (SELECT ...)
	SUBQUERY_EXISTS                         // EXISTS (SELECT ...)
	SUBQUERY_NOT_IN                         // field NOT IN (SELECT ...) - requires special NULL handling
	SUBQUERY_NOT_EXISTS                     // NOT EXISTS (SELECT ...)
	// TODO: Add in future tiers:
	// SUBQUERY_SCALAR                // (SELECT single_value FROM ...)
	// SUBQUERY_DERIVED_TABLE         // FROM (SELECT ...) AS alias
)

// String returns the string representation of a SubqueryType
func (st SubqueryType) String() string {
	switch st {
	case SUBQUERY_IN:
		return "IN"
	case SUBQUERY_EXISTS:
		return "EXISTS"
	case SUBQUERY_NOT_IN:
		return "NOT IN"
	case SUBQUERY_NOT_EXISTS:
		return "NOT EXISTS"
	default:
		return "UNKNOWN"
	}
}

func (se *SubqueryExpression) expressionNode() {}
func (se *SubqueryExpression) String() string {
	return fmt.Sprintf("%s (SELECT ... FROM %s)", se.SubqueryType.String(), se.InnerQuery.BundleName)
}

// ParameterExpression represents a parameter placeholder ($1, $2, etc.) for parameterized queries
// Parameters use 1-based indexing matching PostgreSQL convention.
// Parameter reuse is allowed (e.g., WHERE x=$1 AND y=$1 binds same value twice).
// TODO: Could add type hint validation here for PREPARE stmt (text, integer) FROM ... syntax in future enhancement
type ParameterExpression struct {
	Index        int // 1-indexed parameter number ($1 = Index 1, $2 = Index 2, etc.)
	ResolvedType FieldType
}

func (pe *ParameterExpression) expressionNode() {}
func (pe *ParameterExpression) String() string {
	return fmt.Sprintf("$%d", pe.Index)
}

// IntervalExpression represents an INTERVAL literal for date/time arithmetic
// Supports both numeric format: INTERVAL '7' DAY
// And string format: INTERVAL '7 days', INTERVAL '1 month 2 days'
// TODO: I will add support for ISO 8601 duration format (P1Y2M3DT4H5M6S) when implementing advanced date parsing
// TODO: I will add interval multiplication/division (e.g., INTERVAL '1 day' * 3) when arithmetic extensions are needed
type IntervalExpression struct {
	Value           string    // Raw interval value: '7', '7 days', '1 month 2 days'
	Unit            TokenType // Unit token: TOKEN_YEAR, TOKEN_MONTH, TOKEN_DAY, TOKEN_HOUR, TOKEN_MINUTE, TOKEN_SECOND
	IsNumericFormat bool      // true for INTERVAL '7' DAY, false for INTERVAL '7 days'
	ResolvedType    FieldType // Always FieldTypeInterval
}

func (ie *IntervalExpression) expressionNode() {}
func (ie *IntervalExpression) String() string {
	if ie.IsNumericFormat {
		return fmt.Sprintf("INTERVAL '%s' %s", ie.Value, ie.Unit.String())
	}
	return fmt.Sprintf("INTERVAL '%s'", ie.Value)
}

// AtTimeZoneExpression represents the AT TIME ZONE operator
// Converts DateTime values between timezones at query time
// Storage remains UTC-only for consistency
// TODO: I will add compile-time timezone validation when implementing static analysis optimization
// TODO: I will add support for expression-based timezone (e.g., field reference) when dynamic timezone conversion is needed
type AtTimeZoneExpression struct {
	Expression   Expression // The DateTime expression to convert
	Timezone     string     // Target timezone name (IANA format: 'America/New_York', 'UTC', etc.)
	ResolvedType FieldType  // Always FieldTypeDateTime
}

func (atze *AtTimeZoneExpression) expressionNode() {}
func (atze *AtTimeZoneExpression) String() string {
	return fmt.Sprintf("(%s AT TIME ZONE '%s')", atze.Expression.String(), atze.Timezone)
}

// Precedence levels for operator precedence parsing (Pratt parser)
type Precedence int

const (
	PRECEDENCE_LOWEST       Precedence = iota
	PRECEDENCE_LOGICAL_OR              // OR
	PRECEDENCE_LOGICAL_AND             // AND
	PRECEDENCE_EQUALITY                // == !=
	PRECEDENCE_COMPARISON              // > < >= <= LIKE IN CONTAINS
	PRECEDENCE_SUM                     // + -
	PRECEDENCE_PRODUCT                 // * / %
	PRECEDENCE_UNARY                   // NOT -expr
	PRECEDENCE_CALL                    // function()
	PRECEDENCE_AT_TIME_ZONE            // AT TIME ZONE
	PRECEDENCE_INDEX                   // array[index]
	PRECEDENCE_MEMBER                  // Bundle.Field (member access - highest precedence)
)

// precedences maps token types to their precedence levels
var precedences = map[TokenType]Precedence{
	TOKEN_OR:          PRECEDENCE_LOGICAL_OR,
	TOKEN_AND:         PRECEDENCE_LOGICAL_AND,
	TOKEN_ASSIGN:      PRECEDENCE_EQUALITY, // Single = for equality in WHERE clauses (SyndrDB syntax)
	TOKEN_EQ:          PRECEDENCE_EQUALITY, // Double == for equality (standard SQL syntax)
	TOKEN_NEQ:         PRECEDENCE_EQUALITY,
	TOKEN_LT:          PRECEDENCE_COMPARISON,
	TOKEN_LTE:         PRECEDENCE_COMPARISON,
	TOKEN_GT:          PRECEDENCE_COMPARISON,
	TOKEN_GTE:         PRECEDENCE_COMPARISON,
	TOKEN_LIKE:        PRECEDENCE_COMPARISON,
	TOKEN_IN:          PRECEDENCE_COMPARISON,
	TOKEN_NOTIN:       PRECEDENCE_COMPARISON,
	TOKEN_IS_NULL:     PRECEDENCE_COMPARISON,
	TOKEN_IS_NOT_NULL: PRECEDENCE_COMPARISON,
	TOKEN_CONTAINS:    PRECEDENCE_COMPARISON,
	TOKEN_PLUS:        PRECEDENCE_SUM,
	TOKEN_MINUS:       PRECEDENCE_SUM,
	TOKEN_MULTIPLY:    PRECEDENCE_PRODUCT,
	TOKEN_DIVIDE:      PRECEDENCE_PRODUCT,
	TOKEN_MODULO:      PRECEDENCE_PRODUCT,
	TOKEN_LPAREN:      PRECEDENCE_CALL,
	TOKEN_AT:          PRECEDENCE_AT_TIME_ZONE, // AT TIME ZONE operator
	TOKEN_LBRACKET:    PRECEDENCE_INDEX,
	TOKEN_DOT:         PRECEDENCE_MEMBER, // Member access (Bundle.Field)
}

// ExpressionParser parses expressions using Pratt parsing algorithm
// This is the core reusable component for WHERE clauses, defaults, constraints, etc.
type ExpressionParser struct {
	tokens  []Token
	pos     int
	current Token

	// Prefix parsing functions - how to parse token at the start of an expression
	prefixParsers map[TokenType]prefixParseFunc

	// Infix parsing functions - how to parse token in the middle of an expression
	infixParsers map[TokenType]infixParseFunc

	// Error tracking
	errors []string

	// SECURITY: Depth tracking to prevent stack overflow from deeply nested expressions
	depth    int
	maxDepth int

	// Logger for error reporting
	logger *zap.SugaredLogger
}

// Function types for Pratt parser
type prefixParseFunc func() (Expression, error)
type infixParseFunc func(Expression) (Expression, error)

// NewExpressionParser creates a new expression parser
func NewExpressionParser(tokens []Token, logger *zap.SugaredLogger) *ExpressionParser {
	p := &ExpressionParser{
		tokens:        tokens,
		pos:           0,
		prefixParsers: make(map[TokenType]prefixParseFunc),
		infixParsers:  make(map[TokenType]infixParseFunc),
		errors:        make([]string, 0),
		depth:         0,
		maxDepth:      constants.ExpressionMaxDepth,
		logger:        logger,
	}

	if len(tokens) > 0 {
		p.current = tokens[0]
	}

	// Register prefix parsers (tokens that can start an expression)
	p.registerPrefix(TOKEN_IDENT, p.parseIdentifier)
	p.registerPrefix(TOKEN_NUMBER, p.parseLiteral)
	p.registerPrefix(TOKEN_STRING, p.parseQuotedIdentifierOrLiteral)
	p.registerPrefix(TOKEN_TRUE, p.parseLiteral)
	p.registerPrefix(TOKEN_FALSE, p.parseLiteral)
	p.registerPrefix(TOKEN_NULL, p.parseLiteral)
	p.registerPrefix(TOKEN_PARAMETER, p.parseParameter)
	p.registerPrefix(TOKEN_NOT, p.parseUnaryExpression)
	p.registerPrefix(TOKEN_MINUS, p.parseUnaryExpression)
	p.registerPrefix(TOKEN_LPAREN, p.parseGroupedExpression)
	p.registerPrefix(TOKEN_LBRACKET, p.parseArrayExpression)
	p.registerPrefix(TOKEN_EXISTS, p.parseExistsExpression)
	p.registerPrefix(TOKEN_MULTIPLY, p.parseMultiplyOrWildcard) // Handle * as wildcard in COUNT(*) or multiplication

	// Register DateTime function prefix parsers
	// TODO: I will add support for custom function prefixes when implementing UDF system
	p.registerPrefix(TOKEN_NOW, p.parseFunctionCall)
	p.registerPrefix(TOKEN_EXTRACT, p.parseFunctionCall)
	p.registerPrefix(TOKEN_DATE_TRUNC, p.parseFunctionCall)
	p.registerPrefix(TOKEN_DATE_ADD, p.parseFunctionCall)
	p.registerPrefix(TOKEN_DATE_SUB, p.parseFunctionCall)
	p.registerPrefix(TOKEN_AGE, p.parseFunctionCall)
	p.registerPrefix(TOKEN_FUNCTION, p.parseFunctionCall) // Fallback for unknown F:FUNCTION_NAME
	p.registerPrefix(TOKEN_INTERVAL, p.parseInterval)

	// Register keyword-as-identifier parsers for keywords that might be used as field names
	// This allows reserved words to be used as field names in WHERE clauses and expressions
	// Common examples: Version, Name, Database, User, Migration, Bundle, View, Role, etc.
	p.registerPrefix(TOKEN_VERSION, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_NAME, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_DATABASE, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_USER, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_ROLE, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_MIGRATION, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_MIGRATIONS, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_BUNDLE, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_BUNDLES, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_VIEW, p.parseKeywordAsIdentifier)
	p.registerPrefix(TOKEN_VIEWS, p.parseKeywordAsIdentifier)

	// Register infix parsers (tokens that can appear between expressions)
	p.registerInfix(TOKEN_PLUS, p.parseBinaryExpression)
	p.registerInfix(TOKEN_MINUS, p.parseBinaryExpression)
	p.registerInfix(TOKEN_MULTIPLY, p.parseBinaryExpression)
	p.registerInfix(TOKEN_DIVIDE, p.parseBinaryExpression)
	p.registerInfix(TOKEN_MODULO, p.parseBinaryExpression)
	p.registerInfix(TOKEN_ASSIGN, p.parseBinaryExpression) // Single = for equality in WHERE clauses
	p.registerInfix(TOKEN_EQ, p.parseBinaryExpression)     // Double == for equality
	p.registerInfix(TOKEN_NEQ, p.parseBinaryExpression)
	p.registerInfix(TOKEN_LT, p.parseBinaryExpression)
	p.registerInfix(TOKEN_LTE, p.parseBinaryExpression)
	p.registerInfix(TOKEN_GT, p.parseBinaryExpression)
	p.registerInfix(TOKEN_GTE, p.parseBinaryExpression)
	p.registerInfix(TOKEN_AND, p.parseBinaryExpression)
	p.registerInfix(TOKEN_OR, p.parseBinaryExpression)
	p.registerInfix(TOKEN_LIKE, p.parseBinaryExpression)
	p.registerInfix(TOKEN_IN, p.parseInExpression)
	p.registerInfix(TOKEN_NOTIN, p.parseInExpression)
	p.registerInfix(TOKEN_IS_NULL, p.parseIsNullExpression)
	p.registerInfix(TOKEN_IS_NOT_NULL, p.parseIsNullExpression)
	p.registerInfix(TOKEN_CONTAINS, p.parseBinaryExpression)
	p.registerInfix(TOKEN_LPAREN, p.parseCallExpression)
	p.registerInfix(TOKEN_DOT, p.parseQualifiedIdentifier)
	p.registerInfix(TOKEN_AT, p.parseAtTimeZone) // AT TIME ZONE operator

	// TODO: I should add support for array indexing (field[0])

	return p
}

// Parse parses the tokens into an expression AST
func (p *ExpressionParser) Parse() (Expression, error) {
	expr, err := p.parseExpression(PRECEDENCE_LOWEST)
	if err != nil {
		return nil, err
	}

	// Verify we consumed all tokens (except EOF)
	if p.current.Type != TOKEN_EOF && !p.isStatementTerminator() {
		// For expression parser, we don't have original command, so create a simple error
		// The calling parser can enhance it if needed
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("unexpected token after expression: %s", p.current.Type.String()),
			errors.LayerParser)
	}

	return expr, nil
}

// parseExpression is the core Pratt parser algorithm
func (p *ExpressionParser) parseExpression(precedence Precedence) (Expression, error) {
	// SECURITY: Check expression nesting depth to prevent stack overflow
	p.depth++
	defer func() { p.depth-- }()
	if p.depth > p.maxDepth {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("expression nesting depth exceeds maximum of %d", p.maxDepth),
			errors.LayerParser)
	}

	// Get prefix parser for current token
	prefix := p.prefixParsers[p.current.Type]
	if prefix == nil {
		// For expression parser errors, create a simple error - calling parser can enhance it
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("no prefix parser for token type: %s (value: %s)", p.current.Type.String(), p.current.Value),
			errors.LayerParser)
	}

	leftExpr, err := prefix()
	if err != nil {
		return nil, err
	}

	// Process infix operators while they have higher precedence
	for !p.isStatementTerminator() && precedence < p.currentPrecedence() {
		infix := p.infixParsers[p.current.Type]
		if infix == nil {
			return leftExpr, nil
		}

		leftExpr, err = infix(leftExpr)
		if err != nil {
			return nil, err
		}
	}

	return leftExpr, nil
}

// Prefix parsers

func (p *ExpressionParser) parseIdentifier() (Expression, error) {
	expr := &IdentifierExpression{Name: p.current.Value}
	p.advance()
	return expr, nil
}

// parseKeywordAsIdentifier handles reserved keywords when they appear in identifier positions
// This allows field names like "Version", "Status", "Type", etc. to be used even though
// they are reserved keywords in SyndrQL
func (p *ExpressionParser) parseKeywordAsIdentifier() (Expression, error) {
	// Use the token's string representation as the field name
	// This maintains the original casing (e.g., "Version" not "VERSION")
	fieldName := p.current.Value
	if fieldName == "" {
		// Fallback to token type string if value is empty
		fieldName = p.current.Type.String()
	}
	expr := &IdentifierExpression{Name: fieldName}
	p.advance()
	return expr, nil
}

// parseQuotedIdentifierOrLiteral handles TOKEN_STRING which can be either:
// - An identifier (field/table name) when followed by operators like IN, =, <, >, etc.
// - A literal string value when used as a value (e.g., in IN clause values, comparison values)
// In SyndrQL, double quotes are used for both purposes - context determines meaning.
func (p *ExpressionParser) parseQuotedIdentifierOrLiteral() (Expression, error) {
	value := p.current.Value

	// Look ahead to see what comes next
	p.advance()

	// If followed by an operator that takes a field on the left side, treat as identifier
	// Examples: "Genre" IN (...), "Name" = "value", "Price" > 10, "Email" IS NULL
	switch p.current.Type {
	case TOKEN_IN, TOKEN_NOTIN, TOKEN_EQ, TOKEN_NEQ,
		TOKEN_LT, TOKEN_LTE, TOKEN_GT, TOKEN_GTE, TOKEN_LIKE,
		TOKEN_IS_NULL, TOKEN_IS_NOT_NULL:
		// This is a field identifier
		return &IdentifierExpression{Name: value}, nil

	case TOKEN_DOT:
		// This is a qualified identifier like "Books"."Genre"
		return &IdentifierExpression{Name: value}, nil

	default:
		// This is a literal string value
		return &LiteralExpression{
			Token: TOKEN_STRING,
			Value: value,
		}, nil
	}
}

func (p *ExpressionParser) parseLiteral() (Expression, error) {
	expr := &LiteralExpression{
		Token: p.current.Type,
		Value: p.current.Literal,
	}
	p.advance()
	return expr, nil
}

func (p *ExpressionParser) parseParameter() (Expression, error) {
	paramNum, ok := p.current.Literal.(int)
	if !ok {
		return nil, fmt.Errorf("invalid parameter index")
	}

	expr := &ParameterExpression{
		Index: paramNum,
	}
	p.advance()
	return expr, nil
}

func (p *ExpressionParser) parseMultiplyOrWildcard() (Expression, error) {
	// When * appears as a prefix (not as infix multiplication),
	// treat it as a wildcard identifier (for COUNT(*))
	expr := &IdentifierExpression{
		Name: "*",
	}
	p.advance()
	return expr, nil
}

func (p *ExpressionParser) parseUnaryExpression() (Expression, error) {
	operator := p.current.Type
	p.advance()

	right, err := p.parseExpression(PRECEDENCE_UNARY)
	if err != nil {
		return nil, err
	}

	return &UnaryExpression{
		Operator: operator,
		Right:    right,
	}, nil
}

func (p *ExpressionParser) parseGroupedExpression() (Expression, error) {
	p.advance() // consume '('

	expr, err := p.parseExpression(PRECEDENCE_LOWEST)
	if err != nil {
		return nil, err
	}

	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected ')', got %s", p.current.Type.String())
	}
	p.advance() // consume ')'

	return &GroupedExpression{Expression: expr}, nil
}

func (p *ExpressionParser) parseArrayExpression() (Expression, error) {
	p.advance() // consume '['

	elements := make([]Expression, 0)

	// Empty array
	if p.current.Type == TOKEN_RBRACKET {
		p.advance()
		return &ArrayExpression{Elements: elements}, nil
	}

	// Parse first element
	elem, err := p.parseExpression(PRECEDENCE_LOWEST)
	if err != nil {
		return nil, err
	}
	elements = append(elements, elem)

	// Parse remaining elements
	for p.current.Type == TOKEN_COMMA {
		p.advance() // consume ','

		elem, err := p.parseExpression(PRECEDENCE_LOWEST)
		if err != nil {
			return nil, err
		}
		elements = append(elements, elem)
	}

	if p.current.Type != TOKEN_RBRACKET {
		return nil, fmt.Errorf("expected ']', got %s", p.current.Type.String())
	}
	p.advance() // consume ']'

	return &ArrayExpression{Elements: elements}, nil
}

// Infix parsers

func (p *ExpressionParser) parseBinaryExpression(left Expression) (Expression, error) {
	operator := p.current.Type
	precedence := p.currentPrecedence()
	p.advance()

	right, err := p.parseExpression(precedence)
	if err != nil {
		return nil, err
	}

	// Validate: Check if user is trying to compare with NULL using == or !=
	// This is a common mistake - they should use IS NULL or IS NOT NULL instead
	if litExpr, ok := right.(*LiteralExpression); ok {
		if litExpr.Token == TOKEN_NULL {
			if operator == TOKEN_EQ || operator == TOKEN_NEQ {
				suggestedOp := "IS NULL"
				if operator == TOKEN_NEQ {
					suggestedOp = "IS NOT NULL"
				}
				return nil, fmt.Errorf("cannot use '%s' with NULL value; use '%s' instead (e.g., WHERE field %s)", operator.String(), suggestedOp, suggestedOp)
			}
		}
	}

	return &BinaryExpression{
		Left:     left,
		Operator: operator,
		Right:    right,
	}, nil
}

// parseInExpression parses IN and NOT IN expressions
// Syntax: field IN (value1, value2, ...) or field IN (SELECT ...) or field NOT IN (...)
func (p *ExpressionParser) parseInExpression(left Expression) (Expression, error) {
	operator := p.current.Type // Either TOKEN_IN or TOKEN_NOTIN
	p.advance()                // consume 'IN' or 'NOT IN'

	// Expect opening parenthesis
	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected '(' after %s, got %s", operator.String(), p.current.Type.String())
	}
	p.advance() // consume '('

	// Check for subquery: field IN (SELECT ...)
	if p.current.Type == TOKEN_SELECT {
		// Collect tokens for the subquery SELECT statement
		// We need to find the matching ')' for the IN clause
		// The SELECT parser will handle nested parentheses in its own expressions
		subqueryTokens := p.collectSubqueryTokens()
		if len(subqueryTokens) == 0 {
			return nil, fmt.Errorf("failed to collect subquery tokens")
		}

		// Parse SELECT statement using SelectParser
		selectParser := NewSelectParser(subqueryTokens, p.logger)

		innerQuery, err := selectParser.Parse(p.logger)
		if err != nil {
			return nil, fmt.Errorf("failed to parse subquery: %w", err)
		}

		// Expect closing parenthesis (we should be positioned right after subquery)
		if p.current.Type != TOKEN_RPAREN {
			return nil, fmt.Errorf("expected ')' after subquery, got %s", p.current.Type.String())
		}
		p.advance() // consume ')'

		// Determine subquery type based on operator
		var subqueryType SubqueryType
		if operator == TOKEN_NOTIN {
			subqueryType = SUBQUERY_NOT_IN
		} else {
			subqueryType = SUBQUERY_IN
		}

		// Create SubqueryExpression
		subqueryExpr := &SubqueryExpression{
			SubqueryType:     subqueryType,
			InnerQuery:       innerQuery,
			NestingDepth:     1,     // Will be updated during validation
			IsCorrelated:     false, // Tier 1: only uncorrelated
			CorrelatedFields: nil,
		}

		// Wrap SubqueryExpression in BinaryExpression with left side and operator
		// This is critical: the WHERE clause needs "field IN (subquery)" structure
		// not just the subquery itself
		return &BinaryExpression{
			Left:     left,
			Operator: operator,
			Right:    subqueryExpr,
		}, nil
	}

	// Parse list of values (original behavior)
	values := make([]Expression, 0)

	// Empty list
	if p.current.Type == TOKEN_RPAREN {
		p.advance()
		return &BinaryExpression{
			Left:     left,
			Operator: operator,
			Right:    &ArrayExpression{Elements: values},
		}, nil
	}

	// Parse first value
	val, err := p.parseExpression(PRECEDENCE_LOWEST)
	if err != nil {
		return nil, err
	}
	values = append(values, val)

	// Parse remaining values
	for p.current.Type == TOKEN_COMMA {
		p.advance() // consume ','

		val, err := p.parseExpression(PRECEDENCE_LOWEST)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}

	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected ')', got %s", p.current.Type.String())
	}
	p.advance() // consume ')'

	return &BinaryExpression{
		Left:     left,
		Operator: operator,
		Right:    &ArrayExpression{Elements: values},
	}, nil
}

// parseIsNullExpression handles IS NULL and IS NOT NULL expressions
// Converts them to BinaryExpression with the IS NULL/IS NOT NULL operator
// The evaluator will handle these specially to check the ::SYNDR_NULL:: magic value
func (p *ExpressionParser) parseIsNullExpression(left Expression) (Expression, error) {
	isNullOperator := p.current.Type // Either TOKEN_IS_NULL or TOKEN_IS_NOT_NULL
	p.advance()                      // consume 'IS NULL' or 'IS NOT NULL'

	// I will keep the IS NULL/IS NOT NULL operator intact
	// The evaluator (like the legacy filter parser) will recognize these operators
	// and check if the field value == "::SYNDR_NULL::" magic value
	// This allows uniform handling across both parsers
	return &BinaryExpression{
		Left:     left,
		Operator: isNullOperator,
		Right:    nil, // IS NULL/IS NOT NULL don't have a right operand
	}, nil
}

// parseExistsExpression parses EXISTS subquery expressions
// Syntax: EXISTS (SELECT ...)
func (p *ExpressionParser) parseExistsExpression() (Expression, error) {
	p.advance() // consume 'EXISTS'

	// Expect opening parenthesis
	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected '(' after EXISTS, got %s", p.current.Type.String())
	}
	p.advance() // consume '('

	// Expect SELECT statement
	if p.current.Type != TOKEN_SELECT {
		return nil, fmt.Errorf("expected SELECT after EXISTS (, got %s", p.current.Type.String())
	}

	// Collect tokens for the subquery SELECT statement
	subqueryTokens := p.collectSubqueryTokens()
	if len(subqueryTokens) == 0 {
		return nil, fmt.Errorf("failed to collect EXISTS subquery tokens")
	}

	// Parse SELECT statement using SelectParser
	selectParser := NewSelectParser(subqueryTokens, p.logger)

	innerQuery, err := selectParser.Parse(p.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse EXISTS subquery: %w", err)
	}

	// Expect closing parenthesis (we should be positioned right after subquery)
	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected ')' after EXISTS subquery, got %s", p.current.Type.String())
	}
	p.advance() // consume ')'

	// Create SubqueryExpression with EXISTS type
	// Note: NestingDepth will be calculated by validator during query planning
	return &SubqueryExpression{
		SubqueryType:     SUBQUERY_EXISTS,
		InnerQuery:       innerQuery,
		NestingDepth:     1,     // Will be updated during validation
		IsCorrelated:     false, // Tier 1: only uncorrelated
		CorrelatedFields: nil,
	}, nil
}

func (p *ExpressionParser) parseCallExpression(left Expression) (Expression, error) {
	// Function name must be an identifier
	ident, ok := left.(*IdentifierExpression)
	if !ok {
		return nil, fmt.Errorf("expected function name, got %T", left)
	}

	p.advance() // consume '('

	args := make([]Expression, 0)

	// Empty argument list
	if p.current.Type == TOKEN_RPAREN {
		p.advance()
		return &CallExpression{
			Function:  ident.Name,
			Arguments: args,
		}, nil
	}

	// Parse first argument
	arg, err := p.parseExpression(PRECEDENCE_LOWEST)
	if err != nil {
		return nil, err
	}
	args = append(args, arg)

	// Parse remaining arguments
	for p.current.Type == TOKEN_COMMA {
		p.advance() // consume ','

		arg, err := p.parseExpression(PRECEDENCE_LOWEST)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected ')', got %s", p.current.Type.String())
	}
	p.advance() // consume ')'

	return &CallExpression{
		Function:  ident.Name,
		Arguments: args,
	}, nil
}

// parseQualifiedIdentifier parses qualified field names like "Bundle"."Field"
func (p *ExpressionParser) parseQualifiedIdentifier(left Expression) (Expression, error) {
	// Bundle name can be an identifier or a string literal (quoted identifier)
	var bundleName string
	switch expr := left.(type) {
	case *IdentifierExpression:
		bundleName = expr.Name
	case *LiteralExpression:
		// Handle quoted identifiers like "Authors" (TOKEN_STRING)
		if strVal, ok := expr.Value.(string); ok {
			bundleName = strVal
		} else {
			return nil, fmt.Errorf("expected bundle name before '.', got literal %T", expr.Value)
		}
	default:
		return nil, fmt.Errorf("expected bundle name before '.', got %T", left)
	}

	p.advance() // consume '.'

	// Field name can be an identifier or string literal (quoted identifier)
	var fieldName string
	if p.current.Type == TOKEN_IDENT {
		fieldName = p.current.Value
	} else if p.current.Type == TOKEN_STRING {
		fieldName = p.current.Value
	} else {
		return nil, fmt.Errorf("expected field name after '.', got %s", p.current.Type.String())
	}

	p.advance() // consume field name

	return &QualifiedIdentifierExpression{
		Bundle: bundleName,
		Field:  fieldName,
	}, nil
}

// parseFunctionCall parses F:FUNCTION_NAME(...) style function calls
// All functions use uniform comma-separated argument syntax:
// F:NOW(), F:EXTRACT("YEAR", datetime), F:DATE_ADD(datetime, amount, "DAY"), etc.
// TODO: I will add support for named parameters when implementing advanced function features
func (p *ExpressionParser) parseFunctionCall() (Expression, error) {
	// Extract function name from token value (e.g., "F:NOW" -> "NOW")
	funcName := p.current.Value
	if strings.HasPrefix(funcName, "F:") {
		funcName = funcName[2:] // Remove "F:" prefix
	} else {
		// For TOKEN_NOW, TOKEN_EXTRACT, etc., use the token's string representation
		funcName = strings.TrimPrefix(p.current.Type.String(), "TOKEN_")
	}

	p.advance() // consume function name token

	// Expect opening parenthesis
	if p.current.Type != TOKEN_LPAREN {
		return nil, fmt.Errorf("expected '(' after function name '%s', got %s", funcName, p.current.Type.String())
	}
	p.advance() // consume '('

	args := make([]Expression, 0)

	// Empty argument list
	if p.current.Type == TOKEN_RPAREN {
		p.advance()
		return &CallExpression{
			Function:  funcName,
			Arguments: args,
		}, nil
	}

	// Parse first argument
	arg, err := p.parseExpression(PRECEDENCE_LOWEST)
	if err != nil {
		return nil, fmt.Errorf("error parsing first argument: %w", err)
	}
	args = append(args, arg)

	// Parse remaining comma-separated arguments
	for p.current.Type == TOKEN_COMMA {
		p.advance() // consume ','

		arg, err := p.parseExpression(PRECEDENCE_LOWEST)
		if err != nil {
			return nil, fmt.Errorf("error parsing argument: %w", err)
		}
		args = append(args, arg)
	}

	if p.current.Type != TOKEN_RPAREN {
		return nil, fmt.Errorf("expected ')' after function arguments, got %s", p.current.Type.String())
	}
	p.advance() // consume ')'

	return &CallExpression{
		Function:  funcName,
		Arguments: args,
	}, nil
}

// parseInterval parses INTERVAL literals
// Supports: INTERVAL '7' DAY, INTERVAL 7 DAY, and INTERVAL '7 days' / INTERVAL '1 month 2 days'
// TODO: I will add support for INTERVAL arithmetic (INTERVAL + INTERVAL) when implementing complex date operations
func (p *ExpressionParser) parseInterval() (Expression, error) {
	p.advance() // consume INTERVAL keyword

	// Accept either string literal or number for interval value
	var intervalValue string
	if p.current.Type == TOKEN_STRING {
		intervalValue = p.current.Value
		p.advance() // consume interval value
	} else if p.current.Type == TOKEN_NUMBER {
		// Support bare number format: INTERVAL 66 DAY
		intervalValue = p.current.Value
		p.advance() // consume interval value
	} else {
		return nil, fmt.Errorf("expected string or number after INTERVAL, got %s", p.current.Type.String())
	}

	// Check for numeric format: INTERVAL '7' DAY or INTERVAL 7 DAY
	if p.current.Type == TOKEN_YEAR || p.current.Type == TOKEN_MONTH ||
		p.current.Type == TOKEN_DAY || p.current.Type == TOKEN_HOUR ||
		p.current.Type == TOKEN_MINUTE || p.current.Type == TOKEN_SECOND {

		unit := p.current.Type
		p.advance() // consume unit

		return &IntervalExpression{
			Value:           intervalValue,
			Unit:            unit,
			IsNumericFormat: true,
		}, nil
	}

	// String format: INTERVAL '7 days', INTERVAL '1 month 2 days'
	return &IntervalExpression{
		Value:           intervalValue,
		Unit:            TOKEN_ILLEGAL, // No explicit unit for string format
		IsNumericFormat: false,
	}, nil
}

// parseAtTimeZone parses the AT TIME ZONE operator
// Syntax: datetime_expression AT TIME ZONE 'timezone_name'
// TODO: I will add support for expression-based timezone (field reference) when implementing dynamic timezone conversion
func (p *ExpressionParser) parseAtTimeZone(left Expression) (Expression, error) {
	p.advance() // consume AT

	// Expect TIME ZONE keywords
	if p.current.Type != TOKEN_TIME {
		return nil, fmt.Errorf("expected TIME after AT, got %s", p.current.Type.String())
	}
	p.advance() // consume TIME

	if p.current.Type != TOKEN_ZONE {
		return nil, fmt.Errorf("expected ZONE after AT TIME, got %s", p.current.Type.String())
	}
	p.advance() // consume ZONE

	// Expect timezone string literal
	if p.current.Type != TOKEN_STRING {
		return nil, fmt.Errorf("expected timezone string after AT TIME ZONE, got %s", p.current.Type.String())
	}

	timezone := p.current.Value
	p.advance() // consume timezone

	return &AtTimeZoneExpression{
		Expression: left,
		Timezone:   timezone,
	}, nil
}

// Helper methods

func (p *ExpressionParser) registerPrefix(tokenType TokenType, fn prefixParseFunc) {
	p.prefixParsers[tokenType] = fn
}

func (p *ExpressionParser) registerInfix(tokenType TokenType, fn infixParseFunc) {
	p.infixParsers[tokenType] = fn
}

func (p *ExpressionParser) advance() {
	p.pos++
	if p.pos < len(p.tokens) {
		p.current = p.tokens[p.pos]
	} else {
		p.current = Token{Type: TOKEN_EOF}
	}
}

func (p *ExpressionParser) currentPrecedence() Precedence {
	if prec, ok := precedences[p.current.Type]; ok {
		return prec
	}
	return PRECEDENCE_LOWEST
}

func (p *ExpressionParser) isStatementTerminator() bool {
	return p.current.Type == TOKEN_EOF ||
		p.current.Type == TOKEN_SEMICOLON ||
		p.current.Type == TOKEN_COMMA ||
		p.current.Type == TOKEN_RPAREN ||
		p.current.Type == TOKEN_RBRACKET ||
		p.current.Type == TOKEN_RBRACE
}

// collectSubqueryTokens collects tokens for a subquery SELECT statement
// Starts at the current position (should be SELECT token) and collects tokens
// until the matching ')' is found. Handles nested parentheses correctly.
// Returns the collected tokens and advances the parser position to the ')'.
func (p *ExpressionParser) collectSubqueryTokens() []Token {
	tokens := make([]Token, 0)
	parenDepth := 0 // Track parenthesis depth for nested expressions

	// Collect tokens until we find the matching ')' for the IN/EXISTS clause
	for p.current.Type != TOKEN_EOF {
		// Track parenthesis depth
		if p.current.Type == TOKEN_LPAREN {
			parenDepth++
		} else if p.current.Type == TOKEN_RPAREN {
			// If we're at depth 0, this is the closing ')' for the IN/EXISTS clause
			if parenDepth == 0 {
				// Don't include the ')' in subquery tokens
				// Add EOF token for the SELECT parser
				tokens = append(tokens, Token{Type: TOKEN_EOF})
				return tokens
			}
			parenDepth--
		}

		// Collect token
		tokens = append(tokens, p.current)
		p.advance()
	}

	// If we reach here, we hit EOF before finding the matching ')'
	// Add EOF token anyway
	tokens = append(tokens, Token{Type: TOKEN_EOF})
	return tokens
}

// ParseExpression is a convenience function for parsing an expression from a string
func ParseExpression(input string) (Expression, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization error: %w", err)
	}

	parser := NewExpressionParser(tokens, nil) // nil logger for standalone parsing
	return parser.Parse()
}

// ExpressionPattern represents a recognized pattern for optimization
type ExpressionPattern struct {
	Pattern    string
	Type       ExpressionType
	Complexity int // Estimated evaluation cost
	IndexHint  string
	Cacheable  bool
}

// PatternRecognizer identifies common expression patterns for optimization
// This enables hot path optimization for frequently used expressions
type PatternRecognizer struct {
	patterns map[string]*ExpressionPattern
}

// NewPatternRecognizer creates a new pattern recognizer
func NewPatternRecognizer() *PatternRecognizer {
	pr := &PatternRecognizer{
		patterns: make(map[string]*ExpressionPattern),
	}

	// Pre-register common patterns
	pr.registerCommonPatterns()

	return pr
}

// registerCommonPatterns registers frequently used expression patterns
func (pr *PatternRecognizer) registerCommonPatterns() {
	// Simple equality: field == value
	pr.patterns["equality"] = &ExpressionPattern{
		Pattern:    "field == literal",
		Type:       EXPR_BINARY,
		Complexity: 1,
		IndexHint:  "use_index",
		Cacheable:  true,
	}

	// Range comparison: field > value
	pr.patterns["range"] = &ExpressionPattern{
		Pattern:    "field > literal",
		Type:       EXPR_BINARY,
		Complexity: 2,
		IndexHint:  "use_range_index",
		Cacheable:  true,
	}

	// IN clause: field IN [values]
	pr.patterns["in_list"] = &ExpressionPattern{
		Pattern:    "field IN array",
		Type:       EXPR_BINARY,
		Complexity: 5,
		IndexHint:  "use_multi_lookup",
		Cacheable:  true,
	}

	// CONTAINS: field CONTAINS value
	pr.patterns["contains"] = &ExpressionPattern{
		Pattern:    "field CONTAINS literal",
		Type:       EXPR_BINARY,
		Complexity: 10,
		IndexHint:  "scan_required",
		Cacheable:  false,
	}

	// TODO: I should add patterns for function calls (COUNT, SUM, etc.)
	// TODO: I should add patterns for complex AND/OR combinations
	// TODO: I should add patterns for subfield access (field.subfield)
	// TODO: I should integrate with hot key tracking from query planner
}

// RecognizePattern analyzes an expression and returns its optimization pattern
func (pr *PatternRecognizer) RecognizePattern(expr Expression) *ExpressionPattern {
	switch e := expr.(type) {
	case *BinaryExpression:
		return pr.recognizeBinaryPattern(e)
	case *UnaryExpression:
		return pr.recognizeUnaryPattern(e)
	case *CallExpression:
		return pr.recognizeCallPattern(e)
	default:
		return nil
	}
}

func (pr *PatternRecognizer) recognizeBinaryPattern(expr *BinaryExpression) *ExpressionPattern {
	_, leftIsIdent := expr.Left.(*IdentifierExpression)
	_, rightIsLiteral := expr.Right.(*LiteralExpression)

	if leftIsIdent && rightIsLiteral {
		switch expr.Operator {
		case TOKEN_ASSIGN, TOKEN_EQ: // Both = and == are equality operators
			return pr.patterns["equality"]
		case TOKEN_GT, TOKEN_GTE, TOKEN_LT, TOKEN_LTE:
			return pr.patterns["range"]
		case TOKEN_IN:
			return pr.patterns["in_list"]
		case TOKEN_CONTAINS:
			return pr.patterns["contains"]
		}
	}

	// TODO: I should handle reversed patterns (literal == field)
	// TODO: I should handle field-to-field comparisons
	// TODO: I should handle complex nested expressions

	return nil
}

func (pr *PatternRecognizer) recognizeUnaryPattern(expr *UnaryExpression) *ExpressionPattern {
	// TODO: I should add pattern recognition for NOT expressions
	// TODO: I should add pattern recognition for negation
	return nil
}

func (pr *PatternRecognizer) recognizeCallPattern(expr *CallExpression) *ExpressionPattern {
	// TODO: I should add pattern recognition for aggregate functions
	// TODO: I should add pattern recognition for string functions
	// TODO: I should add pattern recognition for date/time functions
	return nil
}

// ExpressionOptimizer applies optimization transformations to expressions
type ExpressionOptimizer struct {
	recognizer *PatternRecognizer
}

// NewExpressionOptimizer creates a new expression optimizer
func NewExpressionOptimizer() *ExpressionOptimizer {
	return &ExpressionOptimizer{
		recognizer: NewPatternRecognizer(),
	}
}

// Optimize applies optimization transformations to an expression
func (eo *ExpressionOptimizer) Optimize(expr Expression) Expression {
	// Pattern recognition
	pattern := eo.recognizer.RecognizePattern(expr)
	if pattern != nil && pattern.Cacheable {
		// Mark for caching or hot compilation
		// TODO: I should integrate with statement cache
		// TODO: I should track expression frequency for hot path promotion
	}

	// Constant folding
	expr = eo.constantFold(expr)

	// Boolean simplification
	expr = eo.simplifyBoolean(expr)

	return expr
}

// constantFold performs compile-time evaluation of constant expressions
func (eo *ExpressionOptimizer) constantFold(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryExpression:
		// Fold binary operations with two literals
		leftLit, leftIsLit := e.Left.(*LiteralExpression)
		rightLit, rightIsLit := e.Right.(*LiteralExpression)

		if leftIsLit && rightIsLit {
			result := eo.evaluateBinaryLiterals(leftLit, e.Operator, rightLit)
			if result != nil {
				return result
			}
		}
	}

	// TODO: I should add constant folding for unary expressions
	// TODO: I should add constant folding for function calls with constant args
	// TODO: I should add algebraic simplifications (x + 0 = x, x * 1 = x)

	return expr
}

// evaluateBinaryLiterals evaluates binary operations on two literals
func (eo *ExpressionOptimizer) evaluateBinaryLiterals(left *LiteralExpression, op TokenType, right *LiteralExpression) Expression {
	// Arithmetic operations
	leftInt, leftIsInt := left.Value.(int64)
	rightInt, rightIsInt := right.Value.(int64)

	if leftIsInt && rightIsInt {
		var result int64
		switch op {
		case TOKEN_PLUS:
			result = leftInt + rightInt
		case TOKEN_MINUS:
			result = leftInt - rightInt
		case TOKEN_MULTIPLY:
			result = leftInt * rightInt
		case TOKEN_DIVIDE:
			if rightInt != 0 {
				result = leftInt / rightInt
			} else {
				return nil // Division by zero
			}
		case TOKEN_MODULO:
			if rightInt != 0 {
				result = leftInt % rightInt
			} else {
				return nil
			}
		default:
			return nil
		}

		return &LiteralExpression{Token: TOKEN_NUMBER, Value: result}
	}

	// TODO: I should add float arithmetic evaluation
	// TODO: I should add string concatenation evaluation
	// TODO: I should add comparison operations evaluation

	return nil
}

// simplifyBoolean performs boolean logic simplifications
func (eo *ExpressionOptimizer) simplifyBoolean(expr Expression) Expression {
	switch e := expr.(type) {
	case *BinaryExpression:
		// true AND x = x
		if e.Operator == TOKEN_AND {
			if leftLit, ok := e.Left.(*LiteralExpression); ok {
				if leftBool, ok := leftLit.Value.(bool); ok && leftBool {
					return e.Right
				}
			}
		}

		// false OR x = x
		if e.Operator == TOKEN_OR {
			if leftLit, ok := e.Left.(*LiteralExpression); ok {
				if leftBool, ok := leftLit.Value.(bool); ok && !leftBool {
					return e.Right
				}
			}
		}
	case *UnaryExpression:
		// NOT NOT x = x
		if e.Operator == TOKEN_NOT {
			if inner, ok := e.Right.(*UnaryExpression); ok && inner.Operator == TOKEN_NOT {
				return inner.Right
			}
		}
	}

	// TODO: I should add De Morgan's law transformations
	// TODO: I should add distribution of NOT over AND/OR
	// TODO: I should add elimination of contradictions (x AND NOT x = false)

	return expr
}

// ExpressionValidator validates that an expression is semantically correct
type ExpressionValidator struct {
	allowedFunctions map[string]bool
}

// NewExpressionValidator creates a new expression validator
func NewExpressionValidator() *ExpressionValidator {
	ev := &ExpressionValidator{
		allowedFunctions: make(map[string]bool),
	}

	// Register allowed functions
	ev.registerAllowedFunctions()

	return ev
}

// registerAllowedFunctions registers the built-in functions
func (ev *ExpressionValidator) registerAllowedFunctions() {
	// String functions
	ev.allowedFunctions["UPPER"] = true
	ev.allowedFunctions["LOWER"] = true
	ev.allowedFunctions["TRIM"] = true
	ev.allowedFunctions["LENGTH"] = true

	// Math functions
	ev.allowedFunctions["ABS"] = true
	ev.allowedFunctions["CEIL"] = true
	ev.allowedFunctions["FLOOR"] = true
	ev.allowedFunctions["ROUND"] = true

	// Aggregate functions
	ev.allowedFunctions["COUNT"] = true
	ev.allowedFunctions["SUM"] = true
	ev.allowedFunctions["AVG"] = true
	ev.allowedFunctions["MIN"] = true
	ev.allowedFunctions["MAX"] = true

	// TODO: I should add date/time functions
	// TODO: I should add array manipulation functions
	// TODO: I should add type conversion functions
	// TODO: I should allow custom user-defined functions
}

// Validate validates an expression for semantic correctness
func (ev *ExpressionValidator) Validate(expr Expression) error {
	switch e := expr.(type) {
	case *CallExpression:
		funcName := strings.ToUpper(e.Function)
		if !ev.allowedFunctions[funcName] {
			return fmt.Errorf("unknown function: %s", e.Function)
		}

		// Validate arguments
		for _, arg := range e.Arguments {
			if err := ev.Validate(arg); err != nil {
				return err
			}
		}

	case *BinaryExpression:
		// Validate left and right
		if err := ev.Validate(e.Left); err != nil {
			return err
		}
		if err := ev.Validate(e.Right); err != nil {
			return err
		}

	case *UnaryExpression:
		// Validate right
		if err := ev.Validate(e.Right); err != nil {
			return err
		}

	case *GroupedExpression:
		return ev.Validate(e.Expression)

	case *ArrayExpression:
		for _, elem := range e.Elements {
			if err := ev.Validate(elem); err != nil {
				return err
			}
		}
	}

	// TODO: I should add type compatibility checking
	// TODO: I should add operator compatibility checking
	// TODO: I should validate array element type consistency
	// TODO: I should check for undefined identifiers against bundle schema

	return nil
}

// ExpressionStringBuilder builds a normalized string representation of an expression
// Used for caching and pattern matching
type ExpressionStringBuilder struct{}

// Build creates a normalized string representation
func (esb *ExpressionStringBuilder) Build(expr Expression) string {
	switch e := expr.(type) {
	case *LiteralExpression:
		// Normalize literals to types for pattern matching
		switch e.Token {
		case TOKEN_NUMBER:
			if _, isInt := e.Value.(int64); isInt {
				return "INT"
			}
			return "FLOAT"
		case TOKEN_STRING:
			return "STRING"
		case TOKEN_TRUE, TOKEN_FALSE:
			return "BOOL"
		case TOKEN_NULL:
			return "NULL"
		}
		return "LITERAL"

	case *IdentifierExpression:
		return fmt.Sprintf("FIELD(%s)", e.Name)

	case *BinaryExpression:
		left := esb.Build(e.Left)
		right := esb.Build(e.Right)
		return fmt.Sprintf("(%s %s %s)", left, e.Operator.String(), right)

	case *UnaryExpression:
		right := esb.Build(e.Right)
		return fmt.Sprintf("(%s %s)", e.Operator.String(), right)

	case *CallExpression:
		args := make([]string, len(e.Arguments))
		for i, arg := range e.Arguments {
			args[i] = esb.Build(arg)
		}
		return fmt.Sprintf("%s(%s)", e.Function, strings.Join(args, ", "))

	case *ArrayExpression:
		return "ARRAY"

	case *GroupedExpression:
		return esb.Build(e.Expression)
	}

	return "UNKNOWN"
}

// GenerateCacheKey generates a cache key for an expression
func (esb *ExpressionStringBuilder) GenerateCacheKey(expr Expression) string {
	normalized := esb.Build(expr)
	// TODO: I should use a faster hash function for cache keys
	// TODO: I should include parameter placeholders for prepared statements
	return normalized
}
