package index

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"
)

// Expression represents a parsed expression node within the index package.
// This is a self-contained AST that avoids importing syndrQL (to prevent import cycles).
type Expression interface {
	exprNode()
}

type LiteralExpr struct{ Value interface{} }
type IdentifierExpr struct{ Name string }
type BinaryExpr struct {
	Left, Right Expression
	Op          string // "+", "-", "*", "/", etc.
}
type UnaryExpr struct {
	Op    string
	Right Expression
}
type CallExpr struct {
	Function  string
	Arguments []Expression
}

func (LiteralExpr) exprNode()    {}
func (IdentifierExpr) exprNode() {}
func (BinaryExpr) exprNode()     {}
func (UnaryExpr) exprNode()      {}
func (CallExpr) exprNode()       {}

// CompileIndexExpressionAST compiles an expression string into an IndexExpressionCompiled.
// Supports: LOWER(field), UPPER(TRIM(field)), "price" * "quantity", COALESCE("a", "b"), etc.
// Rejects: non-deterministic functions (NOW, AGE with <2 args).
func CompileIndexExpressionAST(expr string) (*models.IndexExpressionCompiled, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty index expression")
	}

	// Parse with lightweight recursive descent parser
	parsed, err := parseExpr(expr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse index expression %q: %w", expr, err)
	}

	// Validate immutability
	if err := validateImmutability(parsed); err != nil {
		return nil, err
	}

	// Extract all field names
	fieldNames := extractFields(parsed)

	primaryField := ""
	if len(fieldNames) > 0 {
		primaryField = fieldNames[0]
	}

	// Produce normalized canonical form
	normalized := exprToString(parsed)

	// Build the evaluation function
	evalFn := buildEvalFn(parsed)

	return &models.IndexExpressionCompiled{
		FieldNames:   fieldNames,
		PrimaryField: primaryField,
		EvalFn:       evalFn,
		Normalized:   normalized,
	}, nil
}

// CompileIndexExpression is the backward-compatible wrapper that returns the legacy
// (fieldName, fn, error) tuple.
func CompileIndexExpression(expr string) (fieldName string, fn func(interface{}) interface{}, err error) {
	compiled, compileErr := CompileIndexExpressionAST(expr)
	if compileErr != nil {
		return "", nil, compileErr
	}

	fieldName = compiled.PrimaryField

	fn = func(v interface{}) interface{} {
		fieldMap := map[string]interface{}{
			compiled.PrimaryField: v,
		}
		return compiled.EvalFn(fieldMap)
	}

	return fieldName, fn, nil
}

// --- Lightweight recursive descent parser ---
// This parses expressions like: LOWER("name"), "price" * "qty", COALESCE("a", "b"), LOWER(TRIM("x"))
// without importing syndrQL to avoid import cycles.

type exprParser struct {
	input string
	pos   int
}

func parseExpr(input string) (Expression, error) {
	p := &exprParser{input: strings.TrimSpace(input), pos: 0}
	expr, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()
	if p.pos < len(p.input) {
		return nil, fmt.Errorf("unexpected character at position %d: %q", p.pos, string(p.input[p.pos]))
	}
	return expr, nil
}

func (p *exprParser) skipWhitespace() {
	for p.pos < len(p.input) && (p.input[p.pos] == ' ' || p.input[p.pos] == '\t' || p.input[p.pos] == '\n' || p.input[p.pos] == '\r') {
		p.pos++
	}
}

func (p *exprParser) parseAddSub() (Expression, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for {
		p.skipWhitespace()
		if p.pos >= len(p.input) {
			break
		}
		ch := p.input[p.pos]
		if ch == '+' || ch == '-' {
			p.pos++
			right, err := p.parseMulDiv()
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Left: left, Right: right, Op: string(ch)}
		} else {
			break
		}
	}
	return left, nil
}

func (p *exprParser) parseMulDiv() (Expression, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		p.skipWhitespace()
		if p.pos >= len(p.input) {
			break
		}
		ch := p.input[p.pos]
		if ch == '*' || ch == '/' {
			p.pos++
			right, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Left: left, Right: right, Op: string(ch)}
		} else {
			break
		}
	}
	return left, nil
}

func (p *exprParser) parseUnary() (Expression, error) {
	p.skipWhitespace()
	if p.pos < len(p.input) && p.input[p.pos] == '-' {
		p.pos++
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "-", Right: right}, nil
	}
	return p.parsePrimary()
}

func (p *exprParser) parsePrimary() (Expression, error) {
	p.skipWhitespace()
	if p.pos >= len(p.input) {
		return nil, fmt.Errorf("unexpected end of expression")
	}

	ch := p.input[p.pos]

	// Parenthesized expression
	if ch == '(' {
		p.pos++ // skip '('
		expr, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		p.skipWhitespace()
		if p.pos >= len(p.input) || p.input[p.pos] != ')' {
			return nil, fmt.Errorf("expected ')' at position %d", p.pos)
		}
		p.pos++ // skip ')'
		return expr, nil
	}

	// Quoted identifier: "fieldname"
	if ch == '"' {
		return p.parseQuotedIdentifier()
	}

	// String literal: 'value'
	if ch == '\'' {
		return p.parseStringLiteral()
	}

	// Number literal
	if ch >= '0' && ch <= '9' {
		return p.parseNumber()
	}

	// Identifier or function call: NAME or NAME(...)
	if isIdentStart(ch) {
		return p.parseIdentOrCall()
	}

	return nil, fmt.Errorf("unexpected character %q at position %d", string(ch), p.pos)
}

func (p *exprParser) parseQuotedIdentifier() (Expression, error) {
	p.pos++ // skip opening '"'
	start := p.pos
	for p.pos < len(p.input) && p.input[p.pos] != '"' {
		p.pos++
	}
	if p.pos >= len(p.input) {
		return nil, fmt.Errorf("unterminated quoted identifier")
	}
	name := p.input[start:p.pos]
	p.pos++ // skip closing '"'
	return &IdentifierExpr{Name: name}, nil
}

func (p *exprParser) parseStringLiteral() (Expression, error) {
	p.pos++ // skip opening '\''
	var sb strings.Builder
	for p.pos < len(p.input) {
		if p.input[p.pos] == '\'' {
			// Check for escaped quote ''
			if p.pos+1 < len(p.input) && p.input[p.pos+1] == '\'' {
				sb.WriteByte('\'')
				p.pos += 2
				continue
			}
			p.pos++ // skip closing '\''
			return &LiteralExpr{Value: sb.String()}, nil
		}
		sb.WriteByte(p.input[p.pos])
		p.pos++
	}
	return nil, fmt.Errorf("unterminated string literal")
}

func (p *exprParser) parseNumber() (Expression, error) {
	start := p.pos
	isFloat := false
	for p.pos < len(p.input) && (p.input[p.pos] >= '0' && p.input[p.pos] <= '9' || p.input[p.pos] == '.') {
		if p.input[p.pos] == '.' {
			isFloat = true
		}
		p.pos++
	}
	numStr := p.input[start:p.pos]
	if isFloat {
		f, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number: %s", numStr)
		}
		return &LiteralExpr{Value: f}, nil
	}
	i, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number: %s", numStr)
	}
	return &LiteralExpr{Value: i}, nil
}

func (p *exprParser) parseIdentOrCall() (Expression, error) {
	start := p.pos
	for p.pos < len(p.input) && isIdentChar(p.input[p.pos]) {
		p.pos++
	}
	name := p.input[start:p.pos]
	p.skipWhitespace()

	// Check for function call: NAME(...)
	if p.pos < len(p.input) && p.input[p.pos] == '(' {
		p.pos++ // skip '('
		var args []Expression
		p.skipWhitespace()
		if p.pos < len(p.input) && p.input[p.pos] != ')' {
			arg, err := p.parseAddSub()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			for {
				p.skipWhitespace()
				if p.pos >= len(p.input) || p.input[p.pos] != ',' {
					break
				}
				p.pos++ // skip ','
				arg, err = p.parseAddSub()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
			}
		}
		p.skipWhitespace()
		if p.pos >= len(p.input) || p.input[p.pos] != ')' {
			return nil, fmt.Errorf("expected ')' after function arguments at position %d", p.pos)
		}
		p.pos++ // skip ')'
		return &CallExpr{Function: strings.ToUpper(name), Arguments: args}, nil
	}

	// Check for special keywords
	upper := strings.ToUpper(name)
	if upper == "NULL" {
		return &LiteralExpr{Value: nil}, nil
	}
	if upper == "TRUE" {
		return &LiteralExpr{Value: true}, nil
	}
	if upper == "FALSE" {
		return &LiteralExpr{Value: false}, nil
	}

	// Bare identifier (unquoted field name)
	return &IdentifierExpr{Name: name}, nil
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentChar(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9')
}

// --- Expression to string (normalization) ---

func exprToString(expr Expression) string {
	switch e := expr.(type) {
	case *LiteralExpr:
		if e.Value == nil {
			return "NULL"
		}
		switch v := e.Value.(type) {
		case string:
			return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		case bool:
			if v {
				return "true"
			}
			return "false"
		default:
			return fmt.Sprintf("%v", v)
		}
	case *IdentifierExpr:
		return fmt.Sprintf("\"%s\"", e.Name)
	case *BinaryExpr:
		return fmt.Sprintf("%s %s %s", exprToString(e.Left), e.Op, exprToString(e.Right))
	case *UnaryExpr:
		return fmt.Sprintf("%s%s", e.Op, exprToString(e.Right))
	case *CallExpr:
		args := make([]string, len(e.Arguments))
		for i, a := range e.Arguments {
			args[i] = exprToString(a)
		}
		return fmt.Sprintf("%s(%s)", e.Function, strings.Join(args, ", "))
	default:
		return fmt.Sprintf("%v", expr)
	}
}

// --- Field name extraction ---

func extractFields(expr Expression) []string {
	seen := make(map[string]bool)
	var result []string
	extractFieldsRecursive(expr, seen, &result)
	return result
}

func extractFieldsRecursive(expr Expression, seen map[string]bool, result *[]string) {
	switch e := expr.(type) {
	case *IdentifierExpr:
		if !seen[e.Name] {
			seen[e.Name] = true
			*result = append(*result, e.Name)
		}
	case *BinaryExpr:
		extractFieldsRecursive(e.Left, seen, result)
		extractFieldsRecursive(e.Right, seen, result)
	case *UnaryExpr:
		extractFieldsRecursive(e.Right, seen, result)
	case *CallExpr:
		for _, arg := range e.Arguments {
			extractFieldsRecursive(arg, seen, result)
		}
	}
}

// --- Immutability validation ---

var immutableFunctions = map[string]bool{
	"LOWER": true, "UPPER": true, "TRIM": true, "LENGTH": true,
	"YEAR": true, "MONTH": true,
	"ABS": true, "CEIL": true, "CEILING": true, "FLOOR": true, "ROUND": true,
	"POWER": true, "SQRT": true, "MOD": true, "LOG": true, "SIGN": true,
	"SUBSTRING": true, "SUBSTR": true, "CONCAT": true, "COALESCE": true,
	"DATE_TRUNC": true, "EXTRACT": true,
	"DATE_ADD": true, "DATE_SUB": true,
}

func validateImmutability(expr Expression) error {
	switch e := expr.(type) {
	case *CallExpr:
		if !immutableFunctions[e.Function] {
			return fmt.Errorf("function %s() is not immutable and cannot be used in index expressions", e.Function)
		}
		for _, arg := range e.Arguments {
			if err := validateImmutability(arg); err != nil {
				return err
			}
		}
	case *BinaryExpr:
		if err := validateImmutability(e.Left); err != nil {
			return err
		}
		return validateImmutability(e.Right)
	case *UnaryExpr:
		return validateImmutability(e.Right)
	case *LiteralExpr, *IdentifierExpr:
		// always immutable
	}
	return nil
}

// --- Evaluation ---

func buildEvalFn(expr Expression) func(map[string]interface{}) interface{} {
	return func(fieldValues map[string]interface{}) interface{} {
		result, err := evalExpr(expr, fieldValues)
		if err != nil {
			return nil
		}
		return result
	}
}

func evalExpr(expr Expression, fieldValues map[string]interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case *LiteralExpr:
		return e.Value, nil
	case *IdentifierExpr:
		v, ok := fieldValues[e.Name]
		if !ok {
			return nil, nil
		}
		return v, nil
	case *BinaryExpr:
		return evalBinary(e, fieldValues)
	case *UnaryExpr:
		return evalUnary(e, fieldValues)
	case *CallExpr:
		return evalCall(e, fieldValues)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func evalBinary(e *BinaryExpr, fieldValues map[string]interface{}) (interface{}, error) {
	left, err := evalExpr(e.Left, fieldValues)
	if err != nil {
		return nil, err
	}
	right, err := evalExpr(e.Right, fieldValues)
	if err != nil {
		return nil, err
	}
	return evalArithmetic(left, right, e.Op)
}

func evalArithmetic(left, right interface{}, op string) (interface{}, error) {
	if left == nil || right == nil {
		return nil, nil
	}

	lf, lok := toFloat(left)
	rf, rok := toFloat(right)
	if !lok || !rok {
		return nil, fmt.Errorf("cannot perform arithmetic on non-numeric values")
	}

	li, liOk := toInt(left)
	ri, riOk := toInt(right)
	useInt := liOk && riOk

	switch op {
	case "+":
		if useInt {
			return li + ri, nil
		}
		return lf + rf, nil
	case "-":
		if useInt {
			return li - ri, nil
		}
		return lf - rf, nil
	case "*":
		if useInt {
			return li * ri, nil
		}
		return lf * rf, nil
	case "/":
		if rf == 0 {
			return nil, nil
		}
		if useInt && ri != 0 && li%ri == 0 {
			return li / ri, nil
		}
		return lf / rf, nil
	}
	return nil, fmt.Errorf("unknown arithmetic operator: %s", op)
}

func evalUnary(e *UnaryExpr, fieldValues map[string]interface{}) (interface{}, error) {
	val, err := evalExpr(e.Right, fieldValues)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	if e.Op == "-" {
		if i, ok := toInt(val); ok {
			return -i, nil
		}
		if f, ok := toFloat(val); ok {
			return -f, nil
		}
		return nil, fmt.Errorf("cannot negate non-numeric value")
	}
	return nil, fmt.Errorf("unsupported unary operator: %s", e.Op)
}

func evalCall(e *CallExpr, fieldValues map[string]interface{}) (interface{}, error) {
	args := make([]interface{}, len(e.Arguments))
	for i, arg := range e.Arguments {
		val, err := evalExpr(arg, fieldValues)
		if err != nil {
			return nil, err
		}
		args[i] = val
	}

	switch e.Function {
	case "LOWER":
		if len(args) < 1 {
			return nil, fmt.Errorf("LOWER() requires at least 1 argument")
		}
		if s, ok := toString(args[0]); ok {
			return strings.ToLower(s), nil
		}
		return args[0], nil
	case "UPPER":
		if len(args) < 1 {
			return nil, fmt.Errorf("UPPER() requires at least 1 argument")
		}
		if s, ok := toString(args[0]); ok {
			return strings.ToUpper(s), nil
		}
		return args[0], nil
	case "TRIM":
		if len(args) < 1 {
			return nil, fmt.Errorf("TRIM() requires at least 1 argument")
		}
		if s, ok := toString(args[0]); ok {
			return strings.TrimSpace(s), nil
		}
		return args[0], nil
	case "LENGTH":
		if len(args) < 1 {
			return nil, fmt.Errorf("LENGTH() requires at least 1 argument")
		}
		if s, ok := toString(args[0]); ok {
			return int64(len(s)), nil
		}
		return args[0], nil
	case "YEAR":
		if len(args) < 1 {
			return nil, fmt.Errorf("YEAR() requires at least 1 argument")
		}
		return extractYear(args[0])
	case "MONTH":
		if len(args) < 1 {
			return nil, fmt.Errorf("MONTH() requires at least 1 argument")
		}
		return extractMonth(args[0])
	case "ABS":
		if len(args) < 1 {
			return nil, fmt.Errorf("ABS() requires at least 1 argument")
		}
		return evalAbs(args[0])
	case "CEIL", "CEILING":
		if len(args) < 1 {
			return nil, fmt.Errorf("CEIL() requires at least 1 argument")
		}
		return evalCeil(args[0])
	case "FLOOR":
		if len(args) < 1 {
			return nil, fmt.Errorf("FLOOR() requires at least 1 argument")
		}
		return evalFloor(args[0])
	case "ROUND":
		return evalRound(args)
	case "SUBSTRING", "SUBSTR":
		return evalSubstring(args)
	case "CONCAT":
		return evalConcat(args)
	case "COALESCE":
		return evalCoalesce(args)
	case "POWER":
		return evalPower(args)
	case "SQRT":
		return evalSqrt(args)
	case "MOD":
		return evalMod(args)
	case "LOG":
		return evalLog(args)
	case "SIGN":
		return evalSign(args)
	case "DATE_TRUNC":
		return evalDateTrunc(args)
	case "EXTRACT":
		return evalExtract(args)
	default:
		return nil, fmt.Errorf("unsupported function in index expression: %s", e.Function)
	}
}

// --- Function implementations ---

func extractYear(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch t := v.(type) {
	case time.Time:
		return int64(t.Year()), nil
	case models.FieldValue:
		if t.Type == models.FieldTypeDateTime {
			return int64(t.DateTimeVal.Year()), nil
		}
		if t.Type == models.FieldTypeDate {
			return int64(t.DateVal.Year()), nil
		}
		if t.Type == models.FieldTypeString && len(t.StringVal) >= 4 {
			return t.StringVal[:4], nil
		}
		return v, nil
	default:
		if s, ok := toString(v); ok && len(s) >= 4 {
			return s[:4], nil
		}
		return v, nil
	}
}

func extractMonth(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch t := v.(type) {
	case time.Time:
		return int64(t.Month()), nil
	case models.FieldValue:
		if t.Type == models.FieldTypeDateTime {
			return int64(t.DateTimeVal.Month()), nil
		}
		if t.Type == models.FieldTypeDate {
			return int64(t.DateVal.Month()), nil
		}
		if t.Type == models.FieldTypeString && len(t.StringVal) >= 7 {
			return t.StringVal[5:7], nil
		}
		return v, nil
	default:
		if s, ok := toString(v); ok && len(s) >= 7 {
			return s[5:7], nil
		}
		return v, nil
	}
}

func evalAbs(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	if i, ok := toInt(v); ok {
		if i < 0 {
			return -i, nil
		}
		return i, nil
	}
	if f, ok := toFloat(v); ok {
		return math.Abs(f), nil
	}
	return nil, fmt.Errorf("ABS: non-numeric argument")
}

func evalCeil(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	if i, ok := toInt(v); ok {
		return i, nil
	}
	if f, ok := toFloat(v); ok {
		return int64(math.Ceil(f)), nil
	}
	return nil, fmt.Errorf("CEIL: non-numeric argument")
}

func evalFloor(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	if i, ok := toInt(v); ok {
		return i, nil
	}
	if f, ok := toFloat(v); ok {
		return int64(math.Floor(f)), nil
	}
	return nil, fmt.Errorf("FLOOR: non-numeric argument")
}

func evalRound(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("ROUND() requires at least 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	decimals := 0
	if len(args) >= 2 && args[1] != nil {
		if d, ok := toInt(args[1]); ok {
			decimals = int(d)
		}
	}
	if decimals == 0 {
		if i, ok := toInt(args[0]); ok {
			return i, nil
		}
		if f, ok := toFloat(args[0]); ok {
			return int64(math.Round(f)), nil
		}
		return nil, fmt.Errorf("ROUND: non-numeric argument")
	}
	f, ok := toFloat(args[0])
	if !ok {
		return nil, fmt.Errorf("ROUND: non-numeric argument")
	}
	pow := math.Pow(10, float64(decimals))
	return math.Round(f*pow) / pow, nil
}

func evalSubstring(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("SUBSTRING() requires at least 2 arguments")
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := toString(args[0])
	if !ok {
		return nil, fmt.Errorf("SUBSTRING: first argument must be a string")
	}
	start, sok := toInt(args[1])
	if !sok {
		return nil, fmt.Errorf("SUBSTRING: start must be an integer")
	}
	startIdx := int(start) - 1 // 1-based → 0-based
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= len(s) {
		return "", nil
	}
	if len(args) >= 3 && args[2] != nil {
		length, lok := toInt(args[2])
		if !lok {
			return nil, fmt.Errorf("SUBSTRING: length must be an integer")
		}
		endIdx := startIdx + int(length)
		if endIdx > len(s) {
			endIdx = len(s)
		}
		if endIdx <= startIdx {
			return "", nil
		}
		return s[startIdx:endIdx], nil
	}
	return s[startIdx:], nil
}

func evalConcat(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("CONCAT requires at least 2 arguments")
	}
	var sb strings.Builder
	for _, arg := range args {
		if arg == nil {
			continue
		}
		if s, ok := toString(arg); ok {
			sb.WriteString(s)
		}
	}
	return sb.String(), nil
}

func evalCoalesce(args []interface{}) (interface{}, error) {
	for _, arg := range args {
		if arg != nil {
			return arg, nil
		}
	}
	return nil, nil
}

func evalDateTrunc(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("DATE_TRUNC() requires 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	unit, ok := toString(args[0])
	if !ok {
		return nil, fmt.Errorf("DATE_TRUNC: unit must be a string")
	}
	t, tok := toTime(args[1])
	if !tok {
		return nil, fmt.Errorf("DATE_TRUNC: second argument must be a datetime")
	}
	switch strings.ToUpper(unit) {
	case "YEAR":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()), nil
	case "MONTH":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()), nil
	case "DAY":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
	case "HOUR":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()), nil
	default:
		return nil, fmt.Errorf("DATE_TRUNC: unsupported unit %q", unit)
	}
}

func evalExtract(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("EXTRACT() requires 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	unit, ok := toString(args[0])
	if !ok {
		return nil, fmt.Errorf("EXTRACT: unit must be a string")
	}
	t, tok := toTime(args[1])
	if !tok {
		return nil, fmt.Errorf("EXTRACT: second argument must be a datetime")
	}
	switch strings.ToUpper(unit) {
	case "YEAR":
		return int64(t.Year()), nil
	case "MONTH":
		return int64(t.Month()), nil
	case "DAY":
		return int64(t.Day()), nil
	case "HOUR":
		return int64(t.Hour()), nil
	case "MINUTE":
		return int64(t.Minute()), nil
	case "SECOND":
		return int64(t.Second()), nil
	default:
		return nil, fmt.Errorf("EXTRACT: unsupported unit %q", unit)
	}
}

func evalPower(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("POWER() requires 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	base, ok := toFloat(args[0])
	if !ok {
		return nil, fmt.Errorf("POWER: base must be numeric")
	}
	exp, ok := toFloat(args[1])
	if !ok {
		return nil, fmt.Errorf("POWER: exponent must be numeric")
	}
	return math.Pow(base, exp), nil
}

func evalSqrt(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("SQRT() requires 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	v, ok := toFloat(args[0])
	if !ok {
		return nil, fmt.Errorf("SQRT: argument must be numeric")
	}
	if v < 0 {
		return nil, fmt.Errorf("SQRT: cannot take square root of negative number")
	}
	return math.Sqrt(v), nil
}

func evalMod(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("MOD() requires 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	// Integer path
	li, liOk := toInt(args[0])
	ri, riOk := toInt(args[1])
	if liOk && riOk {
		if ri == 0 {
			return nil, fmt.Errorf("MOD: division by zero")
		}
		return li % ri, nil
	}
	// Float path
	lf, lok := toFloat(args[0])
	if !lok {
		return nil, fmt.Errorf("MOD: dividend must be numeric")
	}
	rf, rok := toFloat(args[1])
	if !rok {
		return nil, fmt.Errorf("MOD: divisor must be numeric")
	}
	if rf == 0 {
		return nil, fmt.Errorf("MOD: division by zero")
	}
	return math.Mod(lf, rf), nil
}

func evalLog(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("LOG() requires at least 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	if len(args) == 1 {
		v, ok := toFloat(args[0])
		if !ok {
			return nil, fmt.Errorf("LOG: argument must be numeric")
		}
		if v <= 0 {
			return nil, fmt.Errorf("LOG: argument must be positive")
		}
		return math.Log(v), nil
	}
	// LOG(base, number)
	if args[1] == nil {
		return nil, nil
	}
	base, ok := toFloat(args[0])
	if !ok {
		return nil, fmt.Errorf("LOG: base must be numeric")
	}
	number, ok := toFloat(args[1])
	if !ok {
		return nil, fmt.Errorf("LOG: number must be numeric")
	}
	if base <= 0 || base == 1 {
		return nil, fmt.Errorf("LOG: base must be positive and not equal to 1")
	}
	if number <= 0 {
		return nil, fmt.Errorf("LOG: number must be positive")
	}
	return math.Log(number) / math.Log(base), nil
}

func evalSign(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("SIGN() requires 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	if i, ok := toInt(args[0]); ok {
		if i > 0 {
			return int64(1), nil
		} else if i < 0 {
			return int64(-1), nil
		}
		return int64(0), nil
	}
	if f, ok := toFloat(args[0]); ok {
		if f > 0 {
			return int64(1), nil
		} else if f < 0 {
			return int64(-1), nil
		}
		return int64(0), nil
	}
	return nil, fmt.Errorf("SIGN: argument must be numeric")
}

// --- Type conversion helpers ---

func toString(v interface{}) (string, bool) {
	if v == nil {
		return "", false
	}
	switch s := v.(type) {
	case string:
		return s, true
	case models.FieldValue:
		if s.Type == models.FieldTypeString {
			return s.StringVal, true
		}
		iface := s.AsInterface()
		if iface == nil {
			return "", false
		}
		return fmt.Sprintf("%v", iface), true
	case fmt.Stringer:
		return s.String(), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

func toFloat(v interface{}) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint64:
		return float64(n), true
	case models.FieldValue:
		switch n.Type {
		case models.FieldTypeFloat:
			return n.FloatVal, true
		case models.FieldTypeInt:
			return float64(n.IntVal), true
		}
		return 0, false
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func toInt(v interface{}) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case uint:
		return int64(n), true
	case uint64:
		return int64(n), true
	case models.FieldValue:
		if n.Type == models.FieldTypeInt {
			return n.IntVal, true
		}
		return 0, false
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

func toTime(v interface{}) (time.Time, bool) {
	if v == nil {
		return time.Time{}, false
	}
	switch t := v.(type) {
	case time.Time:
		return t, true
	case models.FieldValue:
		if t.Type == models.FieldTypeDateTime {
			return t.DateTimeVal, true
		}
		if t.Type == models.FieldTypeDate {
			return t.DateVal, true
		}
		return time.Time{}, false
	case string:
		parsed, err := time.Parse(time.RFC3339, t)
		return parsed, err == nil
	default:
		return time.Time{}, false
	}
}
