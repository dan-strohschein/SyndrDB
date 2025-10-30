package syndrQL

import (
	"fmt"
	"strings"
)

// SelectStatement represents a parsed SELECT query
type SelectStatement struct {
	// Core components
	Fields      []SelectField // Fields to select (empty or ["*"] means all)
	BundleName  string        // Bundle to select from
	WhereClause Expression    // Optional WHERE condition (nil if none)

	// Query modifiers
	Distinct bool // SELECT DISTINCT
	TopN     int  // SELECT TOP N (0 means no limit)
	Limit    int  // LIMIT N (0 means no limit)
	Offset   int  // OFFSET N (0 means no offset)

	// Advanced features (for extension)
	OrderBy []OrderByField // ORDER BY clause
	GroupBy []string       // GROUP BY fields
	Having  Expression     // HAVING clause

	// Pattern recognition metadata
	Pattern    SelectPattern // Recognized query pattern
	Complexity int           // Estimated execution cost
	IndexHints []string      // Suggested indexes to use
}

// SelectField represents a field in the SELECT clause
type SelectField struct {
	Expression Expression // Field expression (can be identifier, function call, etc.)
	Alias      string     // Optional alias (AS alias)
}

// OrderByField represents a field in ORDER BY clause
type OrderByField struct {
	Field      string
	Descending bool
}

// SelectPattern identifies common SELECT query patterns for optimization
type SelectPattern int

const (
	PATTERN_SELECT_ALL           SelectPattern = iota // SELECT * FROM bundle
	PATTERN_SELECT_FIELDS                             // SELECT field1, field2 FROM bundle
	PATTERN_SELECT_WHERE_SIMPLE                       // SELECT * WHERE field = value
	PATTERN_SELECT_WHERE_COMPLEX                      // SELECT * WHERE complex_condition
	PATTERN_SELECT_AGGREGATE                          // SELECT COUNT(*), SUM(field)
	PATTERN_SELECT_JOIN                               // SELECT with JOIN
	PATTERN_SELECT_GROUPBY                            // SELECT with GROUP BY
	PATTERN_SELECT_CUSTOM                             // Complex custom query
)

// String returns the string representation of a SelectPattern
func (sp SelectPattern) String() string {
	switch sp {
	case PATTERN_SELECT_ALL:
		return "SELECT_ALL"
	case PATTERN_SELECT_FIELDS:
		return "SELECT_FIELDS"
	case PATTERN_SELECT_WHERE_SIMPLE:
		return "SELECT_WHERE_SIMPLE"
	case PATTERN_SELECT_WHERE_COMPLEX:
		return "SELECT_WHERE_COMPLEX"
	case PATTERN_SELECT_AGGREGATE:
		return "SELECT_AGGREGATE"
	case PATTERN_SELECT_JOIN:
		return "SELECT_JOIN"
	case PATTERN_SELECT_GROUPBY:
		return "SELECT_GROUPBY"
	default:
		return "SELECT_CUSTOM"
	}
}

// SelectParser parses SELECT statements with pattern recognition
// Optimized for DML-heavy workloads with hot path detection
type SelectParser struct {
	tokens  []Token
	pos     int
	current Token

	// Pattern detector for hot path optimization
	patternDetector *SelectPatternDetector
}

// NewSelectParser creates a new SELECT parser
func NewSelectParser(tokens []Token) *SelectParser {
	p := &SelectParser{
		tokens:          tokens,
		pos:             0,
		patternDetector: NewSelectPatternDetector(),
	}

	if len(tokens) > 0 {
		p.current = tokens[0]
	}

	return p
}

// Parse parses a SELECT statement
func (p *SelectParser) Parse() (*SelectStatement, error) {
	stmt := &SelectStatement{
		Fields:  make([]SelectField, 0),
		OrderBy: make([]OrderByField, 0),
		GroupBy: make([]string, 0),
	}

	// Fast pattern detection for hot paths
	pattern := p.patternDetector.DetectPattern(p.tokens)
	stmt.Pattern = pattern

	// Use fast path for common patterns
	if pattern == PATTERN_SELECT_ALL || pattern == PATTERN_SELECT_FIELDS {
		return p.parseFastPath(stmt, pattern)
	}

	// Full parsing for complex queries
	return p.parseFullPath(stmt)
}

// parseFastPath handles common SELECT patterns with minimal overhead
func (p *SelectParser) parseFastPath(stmt *SelectStatement, pattern SelectPattern) (*SelectStatement, error) {
	// Expect SELECT keyword
	if p.current.Type != TOKEN_SELECT {
		return nil, fmt.Errorf("expected SELECT, got %s", p.current.Type.String())
	}
	p.advance()

	// Parse modifiers (DISTINCT, TOP)
	if err := p.parseSelectModifiers(stmt); err != nil {
		return nil, err
	}

	// Fast path: SELECT *
	if p.current.Type == TOKEN_MULTIPLY {
		stmt.Fields = []SelectField{{
			Expression: &IdentifierExpression{Name: "*"},
		}}
		p.advance()
	} else {
		// Fast path: SELECT field1, field2, ...
		fields, err := p.parseFieldList()
		if err != nil {
			return nil, err
		}
		stmt.Fields = fields
	}

	// Expect FROM keyword
	if p.current.Type != TOKEN_FROM {
		return nil, fmt.Errorf("expected FROM, got %s", p.current.Type.String())
	}
	p.advance()

	// Parse bundle name
	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_STRING {
		return nil, fmt.Errorf("expected bundle name, got %s", p.current.Type.String())
	}
	stmt.BundleName = p.current.Value
	p.advance()

	// Check for WHERE clause (optional)
	if p.current.Type == TOKEN_WHERE {
		p.advance()
		whereExpr, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}
		stmt.WhereClause = whereExpr

		// Update pattern based on WHERE complexity
		if isSimpleWhereClause(whereExpr) {
			stmt.Pattern = PATTERN_SELECT_WHERE_SIMPLE
		} else {
			stmt.Pattern = PATTERN_SELECT_WHERE_COMPLEX
		}
	}

	// Parse optional clauses
	if err := p.parseOptionalClauses(stmt); err != nil {
		return nil, err
	}

	// Set complexity and index hints
	p.analyzeQueryComplexity(stmt)

	return stmt, nil
}

// parseFullPath handles complex SELECT queries with all features
func (p *SelectParser) parseFullPath(stmt *SelectStatement) (*SelectStatement, error) {
	// Expect SELECT keyword
	if p.current.Type != TOKEN_SELECT {
		return nil, fmt.Errorf("expected SELECT, got %s", p.current.Type.String())
	}
	p.advance()

	// Parse modifiers
	if err := p.parseSelectModifiers(stmt); err != nil {
		return nil, err
	}

	// Parse field list or *
	if p.current.Type == TOKEN_MULTIPLY {
		stmt.Fields = []SelectField{{
			Expression: &IdentifierExpression{Name: "*"},
		}}
		p.advance()
	} else {
		fields, err := p.parseFieldList()
		if err != nil {
			return nil, err
		}
		stmt.Fields = fields
	}

	// Parse FROM clause
	if p.current.Type != TOKEN_FROM {
		return nil, fmt.Errorf("expected FROM, got %s", p.current.Type.String())
	}
	p.advance()

	// Parse bundle name
	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_STRING {
		return nil, fmt.Errorf("expected bundle name, got %s", p.current.Type.String())
	}
	stmt.BundleName = p.current.Value
	p.advance()

	// Parse optional clauses in order
	if err := p.parseOptionalClauses(stmt); err != nil {
		return nil, err
	}

	// Analyze query for optimization hints
	p.analyzeQueryComplexity(stmt)

	return stmt, nil
}

// parseSelectModifiers parses DISTINCT and TOP modifiers
func (p *SelectParser) parseSelectModifiers(stmt *SelectStatement) error {
	// Check for DISTINCT
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Value) == "DISTINCT" {
		stmt.Distinct = true
		p.advance()
	}

	// Check for TOP N
	if p.current.Type == TOKEN_IDENT && strings.ToUpper(p.current.Value) == "TOP" {
		p.advance()

		if p.current.Type != TOKEN_NUMBER {
			return fmt.Errorf("expected number after TOP, got %s", p.current.Type.String())
		}

		if topN, ok := p.current.Literal.(int64); ok {
			stmt.TopN = int(topN)
		} else {
			return fmt.Errorf("TOP value must be an integer")
		}
		p.advance()
	}

	return nil
}

// parseFieldList parses a comma-separated list of fields/expressions
func (p *SelectParser) parseFieldList() ([]SelectField, error) {
	fields := make([]SelectField, 0)

	for {
		// Parse field expression
		field, err := p.parseSelectField()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)

		// Check for comma (more fields)
		if p.current.Type != TOKEN_COMMA {
			break
		}
		p.advance() // consume comma
	}

	return fields, nil
}

// parseSelectField parses a single field/expression with optional alias
func (p *SelectParser) parseSelectField() (SelectField, error) {
	field := SelectField{}

	// Parse the expression (can be identifier, function call, arithmetic, etc.)
	expr, err := p.parseFieldExpression()
	if err != nil {
		return field, err
	}
	field.Expression = expr

	// Check for AS alias
	if p.current.Type == TOKEN_AS {
		p.advance()

		if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_STRING {
			return field, fmt.Errorf("expected alias name, got %s", p.current.Type.String())
		}
		field.Alias = p.current.Value
		p.advance()
	}

	return field, nil
}

// parseFieldExpression parses an expression for a SELECT field
// Reuses the ExpressionParser for complex expressions
func (p *SelectParser) parseFieldExpression() (Expression, error) {
	// Collect tokens until we hit a delimiter (comma, FROM, AS, etc.)
	exprTokens := p.collectExpressionTokens()

	if len(exprTokens) == 0 {
		return nil, fmt.Errorf("expected expression")
	}

	// Use ExpressionParser to parse the collected tokens
	exprParser := NewExpressionParser(exprTokens)
	expr, err := exprParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("error parsing field expression: %w", err)
	}

	return expr, nil
}

// collectExpressionTokens collects tokens until a SELECT-level delimiter
func (p *SelectParser) collectExpressionTokens() []Token {
	tokens := make([]Token, 0)

	for p.current.Type != TOKEN_EOF &&
		p.current.Type != TOKEN_COMMA &&
		p.current.Type != TOKEN_FROM &&
		p.current.Type != TOKEN_WHERE &&
		p.current.Type != TOKEN_ORDER &&
		p.current.Type != TOKEN_GROUP &&
		p.current.Type != TOKEN_LIMIT &&
		p.current.Type != TOKEN_OFFSET &&
		p.current.Type != TOKEN_AS {

		tokens = append(tokens, p.current)
		p.advance()
	}

	// Add EOF token for expression parser
	tokens = append(tokens, Token{Type: TOKEN_EOF})

	return tokens
}

// parseWhereClause parses the WHERE condition
// Reuses the ExpressionParser for WHERE expressions
func (p *SelectParser) parseWhereClause() (Expression, error) {
	// Collect tokens until we hit a clause delimiter
	whereTokens := p.collectWhereTokens()

	if len(whereTokens) == 0 {
		return nil, fmt.Errorf("expected WHERE condition")
	}

	// Use ExpressionParser to parse WHERE condition
	exprParser := NewExpressionParser(whereTokens)
	expr, err := exprParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("error parsing WHERE clause: %w", err)
	}

	return expr, nil
}

// collectWhereTokens collects tokens for WHERE clause
func (p *SelectParser) collectWhereTokens() []Token {
	tokens := make([]Token, 0)

	for p.current.Type != TOKEN_EOF &&
		p.current.Type != TOKEN_ORDER &&
		p.current.Type != TOKEN_GROUP &&
		p.current.Type != TOKEN_HAVING &&
		p.current.Type != TOKEN_LIMIT &&
		p.current.Type != TOKEN_OFFSET {

		tokens = append(tokens, p.current)
		p.advance()
	}

	// Add EOF token
	tokens = append(tokens, Token{Type: TOKEN_EOF})

	return tokens
}

// parseOptionalClauses parses optional clauses in SQL execution order
func (p *SelectParser) parseOptionalClauses(stmt *SelectStatement) error {
	// Parse ORDER BY (if present)
	if p.current.Type == TOKEN_ORDER {
		if err := p.parseOrderByClause(stmt); err != nil {
			return err
		}
	}

	// Parse GROUP BY (if present)
	if p.current.Type == TOKEN_GROUP {
		if err := p.parseGroupByClause(stmt); err != nil {
			return err
		}
	}

	// Parse HAVING (if present)
	if p.current.Type == TOKEN_HAVING {
		if err := p.parseHavingClause(stmt); err != nil {
			return err
		}
	}

	// Parse LIMIT (if present)
	if p.current.Type == TOKEN_LIMIT {
		if err := p.parseLimitClause(stmt); err != nil {
			return err
		}
	}

	// Parse OFFSET (if present)
	if p.current.Type == TOKEN_OFFSET {
		if err := p.parseOffsetClause(stmt); err != nil {
			return err
		}
	}

	return nil
}

// parseOrderByClause parses ORDER BY clause
func (p *SelectParser) parseOrderByClause(stmt *SelectStatement) error {
	p.advance() // consume ORDER

	if p.current.Type != TOKEN_BY {
		return fmt.Errorf("expected BY after ORDER, got %s", p.current.Type.String())
	}
	p.advance() // consume BY

	for {
		if p.current.Type != TOKEN_IDENT {
			return fmt.Errorf("expected field name in ORDER BY, got %s", p.current.Type.String())
		}

		orderField := OrderByField{
			Field:      p.current.Value,
			Descending: false,
		}
		p.advance()

		// Check for ASC/DESC
		if p.current.Type == TOKEN_IDENT {
			direction := strings.ToUpper(p.current.Value)
			if direction == "DESC" {
				orderField.Descending = true
				p.advance()
			} else if direction == "ASC" {
				p.advance()
			}
		}

		stmt.OrderBy = append(stmt.OrderBy, orderField)

		// Check for comma (more fields)
		if p.current.Type != TOKEN_COMMA {
			break
		}
		p.advance() // consume comma
	}

	return nil
}

// parseGroupByClause parses GROUP BY clause
func (p *SelectParser) parseGroupByClause(stmt *SelectStatement) error {
	p.advance() // consume GROUP

	if p.current.Type != TOKEN_BY {
		return fmt.Errorf("expected BY after GROUP, got %s", p.current.Type.String())
	}
	p.advance() // consume BY

	for {
		if p.current.Type != TOKEN_IDENT {
			return fmt.Errorf("expected field name in GROUP BY, got %s", p.current.Type.String())
		}

		stmt.GroupBy = append(stmt.GroupBy, p.current.Value)
		p.advance()

		// Check for comma (more fields)
		if p.current.Type != TOKEN_COMMA {
			break
		}
		p.advance() // consume comma
	}

	stmt.Pattern = PATTERN_SELECT_GROUPBY

	return nil
}

// parseHavingClause parses HAVING clause
func (p *SelectParser) parseHavingClause(stmt *SelectStatement) error {
	p.advance() // consume HAVING

	// Collect tokens for HAVING condition
	havingTokens := p.collectWhereTokens() // Reuse WHERE token collection

	if len(havingTokens) == 0 {
		return fmt.Errorf("expected HAVING condition")
	}

	// Use ExpressionParser
	exprParser := NewExpressionParser(havingTokens)
	expr, err := exprParser.Parse()
	if err != nil {
		return fmt.Errorf("error parsing HAVING clause: %w", err)
	}

	stmt.Having = expr

	return nil
}

// parseLimitClause parses LIMIT clause
func (p *SelectParser) parseLimitClause(stmt *SelectStatement) error {
	p.advance() // consume LIMIT

	if p.current.Type != TOKEN_NUMBER {
		return fmt.Errorf("expected number after LIMIT, got %s", p.current.Type.String())
	}

	if limit, ok := p.current.Literal.(int64); ok {
		stmt.Limit = int(limit)
	} else {
		return fmt.Errorf("LIMIT value must be an integer")
	}
	p.advance()

	return nil
}

// parseOffsetClause parses OFFSET clause
func (p *SelectParser) parseOffsetClause(stmt *SelectStatement) error {
	p.advance() // consume OFFSET

	if p.current.Type != TOKEN_NUMBER {
		return fmt.Errorf("expected number after OFFSET, got %s", p.current.Type.String())
	}

	if offset, ok := p.current.Literal.(int64); ok {
		stmt.Offset = int(offset)
	} else {
		return fmt.Errorf("OFFSET value must be an integer")
	}
	p.advance()

	return nil
}

// Helper methods

func (p *SelectParser) advance() {
	p.pos++
	if p.pos < len(p.tokens) {
		p.current = p.tokens[p.pos]
	} else {
		p.current = Token{Type: TOKEN_EOF}
	}
}

// analyzeQueryComplexity analyzes the query and sets complexity/hints
func (p *SelectParser) analyzeQueryComplexity(stmt *SelectStatement) {
	complexity := 1 // Base complexity

	// Increase complexity for WHERE clause
	if stmt.WhereClause != nil {
		complexity += 2
		stmt.IndexHints = p.extractIndexHints(stmt.WhereClause)
	}

	// Increase complexity for GROUP BY
	if len(stmt.GroupBy) > 0 {
		complexity += 5
	}

	// Increase complexity for ORDER BY
	if len(stmt.OrderBy) > 0 {
		complexity += 3
	}

	// Increase complexity for DISTINCT
	if stmt.Distinct {
		complexity += 2
	}

	stmt.Complexity = complexity
}

// extractIndexHints extracts field names from WHERE clause for index optimization
func (p *SelectParser) extractIndexHints(expr Expression) []string {
	hints := make([]string, 0)

	switch e := expr.(type) {
	case *BinaryExpression:
		// Check if left side is an identifier (field name)
		if ident, ok := e.Left.(*IdentifierExpression); ok {
			hints = append(hints, ident.Name)
		}
		// Recursively check right side
		hints = append(hints, p.extractIndexHints(e.Right)...)

	case *UnaryExpression:
		hints = append(hints, p.extractIndexHints(e.Right)...)
	}

	return hints
}

// isSimpleWhereClause determines if a WHERE clause is simple (single comparison)
func isSimpleWhereClause(expr Expression) bool {
	binary, ok := expr.(*BinaryExpression)
	if !ok {
		return false
	}

	// Simple if it's a single comparison with identifier and literal
	_, leftIsIdent := binary.Left.(*IdentifierExpression)
	_, rightIsLit := binary.Right.(*LiteralExpression)

	return leftIsIdent && rightIsLit && binary.Operator.IsComparison()
}

// SelectPatternDetector detects common SELECT patterns for optimization
type SelectPatternDetector struct {
	// TODO: I should add hot pattern tracking to promote frequently used patterns
	// TODO: I should integrate with query plan cache for repeated queries
	// TODO: I should add statistics tracking for pattern frequency
}

// NewSelectPatternDetector creates a new pattern detector
func NewSelectPatternDetector() *SelectPatternDetector {
	return &SelectPatternDetector{}
}

// DetectPattern performs fast pattern detection on tokenized query
func (spd *SelectPatternDetector) DetectPattern(tokens []Token) SelectPattern {
	if len(tokens) < 4 {
		return PATTERN_SELECT_CUSTOM
	}

	// Fast check: SELECT * FROM bundle
	if tokens[0].Type == TOKEN_SELECT &&
		tokens[1].Type == TOKEN_MULTIPLY &&
		tokens[2].Type == TOKEN_FROM {

		// Check if there's a WHERE clause
		for _, token := range tokens {
			if token.Type == TOKEN_WHERE {
				return PATTERN_SELECT_WHERE_SIMPLE // Will be refined during parsing
			}
		}

		return PATTERN_SELECT_ALL
	}

	// Fast check: SELECT field1, field2 FROM bundle (no WHERE)
	if tokens[0].Type == TOKEN_SELECT && tokens[1].Type == TOKEN_IDENT {
		hasWhere := false
		for _, token := range tokens {
			if token.Type == TOKEN_WHERE {
				hasWhere = true
				break
			}
		}

		if !hasWhere {
			return PATTERN_SELECT_FIELDS
		}
	}

	// Check for aggregate functions
	for _, token := range tokens {
		if token.Type == TOKEN_IDENT {
			upper := strings.ToUpper(token.Value)
			if upper == "COUNT" || upper == "SUM" || upper == "AVG" || upper == "MIN" || upper == "MAX" {
				return PATTERN_SELECT_AGGREGATE
			}
		}
	}

	// Check for GROUP BY
	for i := 0; i < len(tokens)-1; i++ {
		if tokens[i].Type == TOKEN_GROUP && tokens[i+1].Type == TOKEN_BY {
			return PATTERN_SELECT_GROUPBY
		}
	}

	// Check for JOIN
	for _, token := range tokens {
		if token.Type == TOKEN_JOIN {
			return PATTERN_SELECT_JOIN
		}
	}

	return PATTERN_SELECT_CUSTOM
}

// ParseSelect is a convenience function for parsing a SELECT statement from a string
func ParseSelect(input string) (*SelectStatement, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization error: %w", err)
	}

	parser := NewSelectParser(tokens)
	return parser.Parse()
}

// TODO: I should add support for JOIN clauses (INNER, LEFT, RIGHT, FULL)
// TODO: I should add support for subqueries in FROM clause
// TODO: I should add support for UNION/INTERSECT/EXCEPT operations
// TODO: I should add support for window functions (OVER, PARTITION BY)
// TODO: I should integrate with statement cache for repeated query patterns
// TODO: I should add support for SELECT INTO (create new bundle from results)
// TODO: I should add parallel parsing for very large queries
// TODO: I should add query rewrite optimizations (predicate pushdown, etc.)
// TODO: I should track hot queries and promote to compiled fast path
// TODO: I should add cost estimation for query execution planning
// TODO: I should integrate with hot key tracker for index optimization hints
// TODO: I should add support for CTEs (WITH clause / Common Table Expressions)
