package syndrQL

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
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
	JoinClauses      []JoinClause   // JOIN operations - added for JOIN support
	RelationshipName string         // WITH RELATIONSHIP clause - field name for hierarchical JOIN results
	OrderBy          []OrderByField // ORDER BY clause
	GroupBy          []string       // GROUP BY fields
	Having           Expression     // HAVING clause

	// Pattern recognition metadata
	Pattern    SelectPattern // Recognized query pattern
	Complexity int           // Estimated execution cost
	IndexHints []string      // Suggested indexes to use
}

// JoinClause represents a JOIN operation in a SELECT query
// This structure maps to queryparser.JoinClause for compatibility
type JoinClause struct {
	JoinType       JoinType        // Type of join (INNER, LEFT, RIGHT, FULL)
	RightBundle    string          // Bundle being joined
	JoinConditions []JoinCondition // Join conditions (ON clause)
}

// JoinType represents the type of JOIN operation
type JoinType int

const (
	InnerJoin     JoinType = iota // INNER JOIN (default)
	LeftJoin                      // LEFT JOIN / LEFT OUTER JOIN
	RightJoin                     // RIGHT JOIN / RIGHT OUTER JOIN
	FullOuterJoin                 // FULL OUTER JOIN
)

// String returns the string representation of a JoinType
func (jt JoinType) String() string {
	switch jt {
	case InnerJoin:
		return "INNER JOIN"
	case LeftJoin:
		return "LEFT JOIN"
	case RightJoin:
		return "RIGHT JOIN"
	case FullOuterJoin:
		return "FULL OUTER JOIN"
	default:
		return "UNKNOWN JOIN"
	}
}

// JoinCondition represents a single condition in the ON clause
// Example: "Authors"."DocumentID" == "Books"."AuthorID"
type JoinCondition struct {
	LeftField  string // Field from left bundle (e.g., "Authors"."DocumentID")
	RightField string // Field from right bundle (e.g., "Books"."AuthorID")
	Operator   string // Comparison operator (typically "==")
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
	PATTERN_SELECT_ALL             SelectPattern = iota // SELECT * FROM bundle
	PATTERN_SELECT_FIELDS                               // SELECT field1, field2 FROM bundle
	PATTERN_SELECT_WHERE_SIMPLE                         // SELECT * WHERE field = value
	PATTERN_SELECT_WHERE_COMPLEX                        // SELECT * WHERE complex_condition
	PATTERN_SELECT_AGGREGATE                            // SELECT COUNT(*), SUM(field)
	PATTERN_SELECT_JOIN                                 // SELECT with JOIN
	PATTERN_SELECT_GROUPBY                              // SELECT with GROUP BY
	PATTERN_SELECT_EXPRESSION_ONLY                      // SELECT without FROM (expression evaluator)
	PATTERN_SELECT_CUSTOM                               // Complex custom query
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

	// Logger for error reporting and debugging
	logger *zap.SugaredLogger

	// Original command string for error reporting (set during Parse)
	originalCommand string
}

// NewSelectParser creates a new SELECT parser. logger may be nil; if set, it is
// used for error paths before Parse is called and can be overridden in Parse.
func NewSelectParser(tokens []Token, logger *zap.SugaredLogger) *SelectParser {
	p := &SelectParser{
		tokens:          tokens,
		pos:             0,
		patternDetector: NewSelectPatternDetector(),
		logger:          logger,
	}

	if len(tokens) > 0 {
		p.current = tokens[0]
	}

	return p
}

// Parse parses a SELECT statement.
// logger may be nil; if non-nil it overrides the constructor logger for this parse.
// command is the original command string for error reporting (optional, will reconstruct if not provided).
func (p *SelectParser) Parse(logger *zap.SugaredLogger, command ...string) (*SelectStatement, error) {
	if logger != nil {
		p.logger = logger
	}

	// Store original command if provided
	if len(command) > 0 && command[0] != "" {
		p.originalCommand = command[0]
	} else {
		// Reconstruct command from tokens for error reporting
		p.originalCommand = reconstructCommandFromTokens(p.tokens)
	}

	stmt := &SelectStatement{
		Fields:  make([]SelectField, 0),
		OrderBy: make([]OrderByField, 0),
		GroupBy: make([]string, 0),
	}

	// Fast pattern detection for hot paths
	pattern := p.patternDetector.DetectPattern(p.tokens)
	stmt.Pattern = pattern

	// Use fast path for common patterns
	if pattern == PATTERN_SELECT_ALL || pattern == PATTERN_SELECT_FIELDS ||
		pattern == PATTERN_SELECT_WHERE_SIMPLE {
		return p.parseFastPath(stmt, pattern, logger)
	}

	// TODO : Refactor this to get away from the fast path / full path split
	// Full parsing for complex queries
	return p.parseFullPath(stmt, logger)
}

// parseFastPath handles common SELECT patterns with minimal overhead
func (p *SelectParser) parseFastPath(stmt *SelectStatement, pattern SelectPattern, logger *zap.SugaredLogger) (*SelectStatement, error) {
	// Expect SELECT keyword
	if p.current.Type != TOKEN_SELECT {
		return nil, CreateParserErrorWithToken(
			fmt.Sprintf("expected SELECT, got %s", p.current.Type.String()),
			p.current,
			"SELECT",
			p.originalCommand,
		)
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

	// Parse FROM clause (optional for expression-only queries)
	if p.current.Type == TOKEN_FROM {
		p.advance()

		// Parse bundle name
		if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_STRING {
			return nil, CreateParserErrorWithToken(
				fmt.Sprintf("expected bundle name after FROM, got %s", p.current.Type.String()),
				p.current,
				"bundle name (identifier or string)",
				p.originalCommand,
			)
		}
		stmt.BundleName = p.current.Value
		p.advance()
	}
	// else: BundleName remains empty - expression-only query

	// Check for WHERE clause (optional)
	//moved to the optional clauses parser
	// if p.current.Type == TOKEN_WHERE {
	// 	p.advance()
	// 	whereExpr, err := p.parseWhereClause()
	// 	if err != nil {
	// 		logger.Errorf("FATAL: Error parsing WHERE clause: %v", err)
	// 		return nil, err
	// 	}
	// 	stmt.WhereClause = whereExpr

	// 	// Update pattern based on WHERE complexity
	// 	if isSimpleWhereClause(whereExpr) {
	// 		stmt.Pattern = PATTERN_SELECT_WHERE_SIMPLE
	// 	} else {
	// 		stmt.Pattern = PATTERN_SELECT_WHERE_COMPLEX
	// 	}
	// }

	// Parse optional clauses
	if err := p.parseOptionalClauses(stmt, logger); err != nil {
		return nil, err
	}

	// Validate expression-only constraints
	if stmt.BundleName == "" {
		if err := validateExpressionOnly(stmt); err != nil {
			return nil, err
		}
		stmt.Pattern = PATTERN_SELECT_EXPRESSION_ONLY
	}

	// Set complexity and index hints
	p.analyzeQueryComplexity(stmt)

	return stmt, nil
}

// parseFullPath handles complex SELECT queries with all features
func (p *SelectParser) parseFullPath(stmt *SelectStatement, logger *zap.SugaredLogger) (*SelectStatement, error) {
	// Expect SELECT keyword
	if p.current.Type != TOKEN_SELECT {
		return nil, CreateParserErrorWithToken(
			fmt.Sprintf("expected SELECT, got %s", p.current.Type.String()),
			p.current,
			"SELECT",
			p.originalCommand,
		)
	}
	p.advance()

	// Parse modifiers
	if err := p.parseSelectModifiers(stmt); err != nil {
		return nil, err
	}

	// Parse field list, *, or DOCUMENTS
	// DOCUMENTS is SyndrDB-specific syntax meaning "all fields"
	if p.current.Type == TOKEN_MULTIPLY {
		stmt.Fields = []SelectField{{
			Expression: &IdentifierExpression{Name: "*"},
		}}
		p.advance()
	} else if p.current.Type == TOKEN_DOCUMENTS || p.current.Type == TOKEN_DOCUMENT {
		// SELECT DOCUMENTS or SELECT DOCUMENT means select all fields
		// This is SyndrDB's legacy syntax, equivalent to SELECT *
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

	// Parse FROM clause (optional for expression-only queries)
	if p.current.Type == TOKEN_FROM {
		p.advance()

		// Parse bundle name
		if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_STRING {
			return nil, CreateParserErrorWithToken(
				fmt.Sprintf("expected bundle name after FROM, got %s", p.current.Type.String()),
				p.current,
				"bundle name (identifier or string)",
				p.originalCommand,
			)
		}
		stmt.BundleName = p.current.Value
		p.advance()
	}
	// else: BundleName remains empty - expression-only query

	// Parse optional clauses in order
	if err := p.parseOptionalClauses(stmt, logger); err != nil {
		return nil, err
	}

	// Validate expression-only constraints
	if stmt.BundleName == "" {
		if err := validateExpressionOnly(stmt); err != nil {
			return nil, err
		}
		stmt.Pattern = PATTERN_SELECT_EXPRESSION_ONLY
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
	fields := make([]SelectField, 0, 20)

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
	exprParser := NewExpressionParser(exprTokens, p.logger)
	expr, err := exprParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("error parsing field expression: %w", err)
	}

	return expr, nil
}

// collectExpressionTokens collects tokens until a SELECT-level delimiter
// Handles nested parentheses to correctly capture function arguments
func (p *SelectParser) collectExpressionTokens() []Token {
	tokens := make([]Token, 0)
	parenDepth := 0 // Track parenthesis nesting

	for p.current.Type != TOKEN_EOF {
		// Track parenthesis depth
		if p.current.Type == TOKEN_LPAREN {
			parenDepth++
		} else if p.current.Type == TOKEN_RPAREN {
			parenDepth--
		}

		// Only stop at delimiters when we're at depth 0 (not inside function calls)
		if parenDepth == 0 {
			if p.current.Type == TOKEN_COMMA ||
				p.current.Type == TOKEN_FROM ||
				p.current.Type == TOKEN_WHERE ||
				p.current.Type == TOKEN_ORDER ||
				p.current.Type == TOKEN_GROUP ||
				p.current.Type == TOKEN_LIMIT ||
				p.current.Type == TOKEN_OFFSET ||
				p.current.Type == TOKEN_AS {
				break
			}
		}

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
	exprParser := NewExpressionParser(whereTokens, p.logger)
	expr, err := exprParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("error parsing WHERE clause: %w", err)
	}

	if p.logger != nil {
		p.logger.Debugf("parseWhereClause: parsed expression type=%T", expr)
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
		p.current.Type != TOKEN_OFFSET &&
		p.current.Type != TOKEN_WITH { // Stop at WITH RELATIONSHIP clause

		tokens = append(tokens, p.current)
		p.advance()
	}

	// Add EOF token
	tokens = append(tokens, Token{Type: TOKEN_EOF})

	return tokens
}

// parseOptionalClauses parses optional clauses in SQL execution order
func (p *SelectParser) parseOptionalClauses(stmt *SelectStatement, logger *zap.SugaredLogger) error {

	// Parse JOIN clauses (if present)
	// JOIN must come before WHERE clause in SQL syntax
	// Example: SELECT * FROM Authors JOIN Books ON Authors.ID == Books.AuthorID WHERE ...
	for p.current.Type == TOKEN_JOIN {
		joinClause, err := p.parseJoinClause(logger)
		if err != nil {
			return fmt.Errorf("failed to parse JOIN clause: %w", err)
		}
		stmt.JoinClauses = append(stmt.JoinClauses, *joinClause)

		// Update pattern to indicate this is a JOIN query
		stmt.Pattern = PATTERN_SELECT_JOIN
	}

	// Check for WHERE clause (optional)
	if p.current.Type == TOKEN_WHERE {
		p.advance()
		whereExpr, err := p.parseWhereClause()
		if err != nil {
			logger.Errorf("FATAL: Error parsing WHERE clause: %v", err)
			return err
		}
		stmt.WhereClause = whereExpr

		// Update pattern based on WHERE complexity (only if not already a JOIN query)
		// JOIN queries with WHERE keep the JOIN pattern as primary
		if stmt.Pattern != PATTERN_SELECT_JOIN {
			if isSimpleWhereClause(whereExpr) {
				stmt.Pattern = PATTERN_SELECT_WHERE_SIMPLE
			} else {
				stmt.Pattern = PATTERN_SELECT_WHERE_COMPLEX
			}
		}
	}

	// Parse WITH RELATIONSHIP clause (if present)
	// This clause specifies the field name for hierarchical JOIN results
	// Example: ... JOIN "Books" ON ... WITH RELATIONSHIP "Books"
	// The relationship name becomes the field that contains child records in 1-to-many JOINs
	if p.current.Type == TOKEN_WITH {
		if err := p.parseWithRelationshipClause(stmt, logger); err != nil {
			return err
		}
	}

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
		// Handle qualified identifiers: "Bundle"."Field" or just Field
		var fieldName string

		if p.current.Type == TOKEN_STRING {
			// Could be qualified identifier: "Bundle"."Field"
			bundleName := p.current.Value
			p.advance() // consume first part

			if p.current.Type == TOKEN_DOT {
				// Qualified identifier
				p.advance() // consume dot

				if p.current.Type != TOKEN_STRING && p.current.Type != TOKEN_IDENT {
					return fmt.Errorf("expected field name after '.' in ORDER BY, got %s", p.current.Type.String())
				}

				fieldName = fmt.Sprintf(`"%s"."%s"`, bundleName, p.current.Value)
				p.advance()
			} else {
				// Just a quoted field name
				fieldName = bundleName
			}
		} else if p.current.Type == TOKEN_IDENT {
			// Unqualified identifier
			fieldName = p.current.Value
			p.advance()
		} else {
			return fmt.Errorf("expected field name in ORDER BY, got %s", p.current.Type.String())
		}

		orderField := OrderByField{
			Field:      fieldName,
			Descending: false,
		}

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
		// Handle qualified identifiers: "Bundle"."Field" or just Field
		var fieldName string

		if p.current.Type == TOKEN_STRING {
			// Could be either:
			// 1. Qualified identifier: "Bundle"."Field"
			// 2. Just a quoted field name: "Field"

			firstPart := p.current.Value
			p.advance() // consume first string

			if p.current.Type == TOKEN_DOT {
				// This is a qualified identifier "Bundle"."Field"
				p.advance() // consume dot

				if p.current.Type != TOKEN_STRING && p.current.Type != TOKEN_IDENT {
					return fmt.Errorf("expected field name after '.' in GROUP BY, got %s", p.current.Type.String())
				}

				fieldName = fmt.Sprintf(`"%s"."%s"`, firstPart, p.current.Value)
				p.advance()
			} else {
				// This is just a quoted field name "Field" - use the value without re-adding quotes
				// The quotes are just SQL syntax, the field name itself doesn't include them
				fieldName = firstPart
			}
		} else if p.current.Type == TOKEN_IDENT {
			// Unqualified identifier
			fieldName = p.current.Value
			p.advance()
		} else {
			return fmt.Errorf("expected field name in GROUP BY, got %s", p.current.Type.String())
		}

		stmt.GroupBy = append(stmt.GroupBy, fieldName)

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
	exprParser := NewExpressionParser(havingTokens, p.logger)
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

// parseWithRelationshipClause parses the WITH RELATIONSHIP clause for hierarchical JOIN results
// Syntax: WITH RELATIONSHIP "FieldName"
// This clause specifies the field name that will contain child records in a 1-to-many JOIN
// Example: SELECT * FROM "Authors" JOIN "Books" ON ... WITH RELATIONSHIP "Books"
// Result: Each Author document will have a "Books" field containing an array of Book documents
func (p *SelectParser) parseWithRelationshipClause(stmt *SelectStatement, logger *zap.SugaredLogger) error {
	// Verify we're on WITH token
	if p.current.Type != TOKEN_WITH {
		return fmt.Errorf("expected WITH keyword, got %s", p.current.Type.String())
	}
	p.advance() // consume WITH

	// Verify RELATIONSHIP keyword
	if p.current.Type != TOKEN_RELATIONSHIP {
		return fmt.Errorf("expected RELATIONSHIP after WITH, got %s", p.current.Type.String())
	}
	p.advance() // consume RELATIONSHIP

	// Parse the relationship field name (can be quoted string or identifier)
	var relationshipName string
	if p.current.Type == TOKEN_STRING {
		relationshipName = p.current.Value
		logger.Debugf("Parsed WITH RELATIONSHIP: %s", relationshipName)
	} else if p.current.Type == TOKEN_IDENT {
		relationshipName = p.current.Value
		logger.Debugf("Parsed WITH RELATIONSHIP: %s", relationshipName)
	} else {
		return fmt.Errorf("expected relationship field name (string or identifier), got %s", p.current.Type.String())
	}
	p.advance() // consume field name

	// Store the relationship name in the statement
	stmt.RelationshipName = relationshipName

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
	if len(tokens) < 2 {
		return PATTERN_SELECT_CUSTOM
	}

	// Check for expression-only SELECT (no FROM clause)
	hasFrom := false
	for _, token := range tokens {
		if token.Type == TOKEN_FROM {
			hasFrom = true
			break
		}
	}

	if !hasFrom && tokens[0].Type == TOKEN_SELECT {
		return PATTERN_SELECT_EXPRESSION_ONLY
	}

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
func ParseSelect(input string, logger *zap.SugaredLogger) (*SelectStatement, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization error: %w", err)
	}

	parser := NewSelectParser(tokens, logger)
	return parser.Parse(logger)
}

// parseJoinClause parses a single JOIN clause including type, bundle name, and ON conditions
// Syntax: [INNER|LEFT|RIGHT|FULL [OUTER]] JOIN "BundleName" ON condition [AND condition ...]
// This function implements JOIN parsing for the new SyndrQL parser
func (p *SelectParser) parseJoinClause(logger *zap.SugaredLogger) (*JoinClause, error) {
	joinClause := &JoinClause{
		JoinType:       InnerJoin, // Default to INNER JOIN
		JoinConditions: make([]JoinCondition, 0),
	}

	// Current token should be JOIN
	if p.current.Type != TOKEN_JOIN {
		return nil, fmt.Errorf("expected JOIN keyword, got %s", p.current.Type.String())
	}

	// Check for JOIN type modifier before JOIN keyword
	// Note: In standard SQL, type comes BEFORE JOIN (e.g., "LEFT JOIN", "INNER JOIN")
	// We'll look backward at the previous token if we need to support that
	// For now, we assume standard "JOIN" means INNER JOIN
	// TODO: I should add support for detecting LEFT/RIGHT/FULL modifiers before JOIN token

	p.advance() // Consume JOIN keyword

	// Parse bundle name (table being joined)
	if p.current.Type != TOKEN_IDENT && p.current.Type != TOKEN_STRING {
		return nil, fmt.Errorf("expected bundle name after JOIN, got %s", p.current.Type.String())
	}
	joinClause.RightBundle = p.current.Value
	logger.Debugf("Parsing JOIN to bundle: %s", joinClause.RightBundle)
	p.advance()

	// Expect ON keyword
	if p.current.Type != TOKEN_ON {
		return nil, fmt.Errorf("expected ON keyword after JOIN bundle name, got %s", p.current.Type.String())
	}
	p.advance() // Consume ON

	// Parse JOIN conditions (ON clause)
	// Format: field1 == field2 [AND field3 == field4 ...]
	for {
		condition, err := p.parseJoinCondition(logger)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JOIN condition: %w", err)
		}
		joinClause.JoinConditions = append(joinClause.JoinConditions, *condition)

		// Check for AND to continue with more conditions
		if p.current.Type == TOKEN_AND {
			p.advance() // Consume AND
			continue    // Parse next condition
		}

		// No more conditions, break out
		break
	}

	logger.Debugf("Parsed JOIN clause: %s to %s with %d conditions",
		joinClause.JoinType.String(), joinClause.RightBundle, len(joinClause.JoinConditions))

	return joinClause, nil
}

// parseJoinCondition parses a single JOIN condition from the ON clause
// Format: "Bundle1"."Field1" == "Bundle2"."Field2"
// This follows the SyndrDB convention of qualified field names
func (p *SelectParser) parseJoinCondition(logger *zap.SugaredLogger) (*JoinCondition, error) {
	condition := &JoinCondition{}

	// Parse left field (can be quoted identifier or bundle.field format)
	leftField, err := p.parseQualifiedFieldName()
	if err != nil {
		return nil, fmt.Errorf("failed to parse left field in JOIN condition: %w", err)
	}
	condition.LeftField = leftField

	// Parse operator (typically ==, but could support !=, <, >, etc.)
	// TODO: I should add support for other comparison operators in JOIN conditions
	if p.current.Type != TOKEN_EQ {
		return nil, fmt.Errorf("expected == operator in JOIN condition, got %s", p.current.Type.String())
	}
	condition.Operator = "==" // Store as string for compatibility with queryparser.JoinCondition
	p.advance()

	// Parse right field
	rightField, err := p.parseQualifiedFieldName()
	if err != nil {
		return nil, fmt.Errorf("failed to parse right field in JOIN condition: %w", err)
	}
	condition.RightField = rightField

	logger.Debugf("Parsed JOIN condition: %s %s %s", condition.LeftField, condition.Operator, condition.RightField)

	return condition, nil
}

// parseQualifiedFieldName parses a qualified field name for JOIN conditions
// Supports formats:
//   - "BundleName"."FieldName" (quoted bundle and field)
//   - BundleName.FieldName (unquoted identifiers)
//   - "FieldName" (just field name, no bundle qualification)
func (p *SelectParser) parseQualifiedFieldName() (string, error) {
	var parts []string

	// Parse first identifier or string
	if p.current.Type == TOKEN_IDENT {
		parts = append(parts, p.current.Value)
		p.advance()
	} else if p.current.Type == TOKEN_STRING {
		parts = append(parts, p.current.Value)
		p.advance()
	} else {
		return "", fmt.Errorf("expected field name, got %s", p.current.Type.String())
	}

	// Check for dot notation (bundle.field)
	if p.current.Type == TOKEN_DOT {
		p.advance() // Consume dot

		// Parse second part (field name)
		if p.current.Type == TOKEN_IDENT {
			parts = append(parts, p.current.Value)
			p.advance()
		} else if p.current.Type == TOKEN_STRING {
			parts = append(parts, p.current.Value)
			p.advance()
		} else {
			return "", fmt.Errorf("expected field name after dot, got %s", p.current.Type.String())
		}

		// Return as "Bundle"."Field" format for compatibility
		return fmt.Sprintf("\"%s\".\"%s\"", parts[0], parts[1]), nil
	}

	// Just a field name without bundle qualification
	return fmt.Sprintf("\"%s\"", parts[0]), nil
}

// TODO: I should add support for JOIN type keywords (INNER, LEFT, RIGHT, FULL) before JOIN token
// TODO: I should add support for subqueries in FROM clause
// TODO: I should add support for UNION/INTERSECT/EXCEPT operations
// TODO: I should add support for window functions (OVER, PARTITION BY)

// validateExpressionOnly validates that an expression-only SELECT query (no FROM clause)
// doesn't use clauses that require a bundle context
func validateExpressionOnly(stmt *SelectStatement) error {
	var errors []string

	// Check for clauses that require FROM
	if stmt.WhereClause != nil {
		errors = append(errors, "WHERE clause requires FROM clause to specify bundle")
	}

	if len(stmt.JoinClauses) > 0 {
		errors = append(errors, "JOIN requires FROM clause to specify bundle")
	}

	if len(stmt.GroupBy) > 0 {
		errors = append(errors, "GROUP BY requires FROM clause to specify bundle")
	}

	if stmt.Having != nil {
		errors = append(errors, "HAVING requires FROM clause to specify bundle")
	}

	if len(stmt.OrderBy) > 0 {
		errors = append(errors, "ORDER BY requires FROM clause to specify bundle")
	}

	if stmt.Limit > 0 || stmt.Offset > 0 {
		errors = append(errors, "LIMIT/OFFSET requires FROM clause to specify bundle")
	}

	if stmt.Distinct {
		errors = append(errors, "DISTINCT modifier cannot be used in expression-only SELECT queries")
	}

	// Recursively check for aggregates and subqueries in expressions
	var aggregateFuncs []string
	for _, field := range stmt.Fields {
		if field.Expression != nil {
			aggs := findAggregatesInExpression(field.Expression)
			aggregateFuncs = append(aggregateFuncs, aggs...)

			if hasSubqueryInExpression(field.Expression) {
				errors = append(errors, "Subquery found in expression - subqueries require FROM clause for evaluation")
			}
		}
	}

	if len(aggregateFuncs) > 0 {
		// TODO: Future enhancement - Consider allowing aggregate-only expressions that compute results
		// without documents. For example, mathematical aggregates like COUNT(1+1) could theoretically
		// return computed values. This would require aggregate evaluation without document context.
		funcList := strings.Join(uniqueStrings(aggregateFuncs), ", ")
		errors = append(errors, fmt.Sprintf("Aggregate functions %s found in expression - aggregate functions require FROM clause to specify bundle", funcList))
	}

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "; "))
	}

	return nil
}

// findAggregatesInExpression recursively searches for aggregate function calls in an expression
func findAggregatesInExpression(expr Expression) []string {
	if expr == nil {
		return nil
	}

	var aggregates []string

	switch e := expr.(type) {
	case *CallExpression:
		// Check if this is an aggregate function
		funcName := strings.ToUpper(e.Function)
		if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" {
			aggregates = append(aggregates, funcName)
		}
		// Recursively check arguments
		for _, arg := range e.Arguments {
			aggregates = append(aggregates, findAggregatesInExpression(arg)...)
		}

	case *BinaryExpression:
		aggregates = append(aggregates, findAggregatesInExpression(e.Left)...)
		aggregates = append(aggregates, findAggregatesInExpression(e.Right)...)

	case *UnaryExpression:
		aggregates = append(aggregates, findAggregatesInExpression(e.Right)...)

	case *GroupedExpression:
		aggregates = append(aggregates, findAggregatesInExpression(e.Expression)...)

	case *IntervalExpression:
		// INTERVAL expressions don't contain aggregates

	case *AtTimeZoneExpression:
		aggregates = append(aggregates, findAggregatesInExpression(e.Expression)...)
	}

	return aggregates
}

// hasSubqueryInExpression recursively checks if an expression contains a subquery
func hasSubqueryInExpression(expr Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *SubqueryExpression:
		return true

	case *CallExpression:
		for _, arg := range e.Arguments {
			if hasSubqueryInExpression(arg) {
				return true
			}
		}

	case *BinaryExpression:
		return hasSubqueryInExpression(e.Left) || hasSubqueryInExpression(e.Right)

	case *UnaryExpression:
		return hasSubqueryInExpression(e.Right)

	case *GroupedExpression:
		return hasSubqueryInExpression(e.Expression)

	case *AtTimeZoneExpression:
		return hasSubqueryInExpression(e.Expression)
	}

	return false
}

// uniqueStrings returns a deduplicated slice of strings
func uniqueStrings(strs []string) []string {
	seen := make(map[string]bool)
	result := []string{}
	for _, str := range strs {
		if !seen[str] {
			seen[str] = true
			result = append(result, str)
		}
	}
	return result
}

// HasAggregates returns true if the SELECT statement contains any aggregate functions
func (stmt *SelectStatement) HasAggregates() bool {
	if len(stmt.Fields) == 0 {
		return false
	}

	for _, field := range stmt.Fields {
		if call, isCall := field.Expression.(*CallExpression); isCall {
			funcName := strings.ToUpper(call.Function)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" {
				return true
			}
		}
	}
	return false
}

// IsAggregateOnly returns true if ALL fields in the SELECT list are aggregate functions
// Note: COUNT(*) where * is an argument inside the CallExpression is still an aggregate
func (stmt *SelectStatement) IsAggregateOnly() bool {
	if len(stmt.Fields) == 0 {
		return false
	}

	for _, field := range stmt.Fields {
		if call, isCall := field.Expression.(*CallExpression); isCall {
			// Check if it's an aggregate function
			funcName := strings.ToUpper(call.Function)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" {
				// This is an aggregate - continue checking other fields
				continue
			}
			// It's a non-aggregate function call
			return false
		} else if ident, isIdent := field.Expression.(*IdentifierExpression); isIdent {
			// Wildcard selector (SELECT *) - not aggregate-only
			if ident.Name == "*" {
				return false
			}
			// Regular field identifier - not aggregate-only
			return false
		} else {
			// Any other expression type (literal, binary expr, etc.) - not aggregate-only
			return false
		}
	}

	// All fields are aggregate functions
	return true
}

// CountAggregates returns the number of aggregate functions in the SELECT list
func (stmt *SelectStatement) CountAggregates() int {
	count := 0
	for _, field := range stmt.Fields {
		if call, isCall := field.Expression.(*CallExpression); isCall {
			funcName := strings.ToUpper(call.Function)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" {
				count++
			}
		}
	}
	return count
}

// CountNonAggregates returns the number of non-aggregate fields in the SELECT list
func (stmt *SelectStatement) CountNonAggregates() int {
	count := 0
	for _, field := range stmt.Fields {
		isAggregate := false
		if call, isCall := field.Expression.(*CallExpression); isCall {
			funcName := strings.ToUpper(call.Function)
			if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || funcName == "MIN" || funcName == "MAX" {
				isAggregate = true
			}
		}
		if !isAggregate {
			count++
		}
	}
	return count
}

// TODO: I should integrate with statement cache for repeated query patterns
// TODO: I should add support for SELECT INTO (create new bundle from results)
// TODO: I should add parallel parsing for very large queries
// TODO: I should add query rewrite optimizations (predicate pushdown, etc.)
// TODO: I should track hot queries and promote to compiled fast path
// TODO: I should add cost estimation for query execution planning
// TODO: I should integrate with hot key tracker for index optimization hints
// TODO: I should add support for CTEs (WITH clause / Common Table Expressions)
