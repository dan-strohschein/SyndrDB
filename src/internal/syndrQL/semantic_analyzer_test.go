package syndrQL

import (
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func testBundle() *models.Bundle {
	return &models.Bundle{
		Name: "Users",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"name":       {Type: "string"},
				"age":        {Type: "int"},
				"salary":     {Type: "float"},
				"active":     {Type: "bool"},
				"created_at": {Type: "datetime"},
				"birthday":   {Type: "date"},
			},
		},
	}
}

// --- Literal resolution ---

func TestResolveLiteralTypes(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())

	tests := []struct {
		name string
		expr *LiteralExpression
		want FieldType
	}{
		{"int", &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(42)}, FieldTypeInt},
		{"float", &LiteralExpression{Token: TOKEN_NUMBER, Value: float64(3.14)}, FieldTypeFloat},
		{"string", &LiteralExpression{Token: TOKEN_STRING, Value: "hello"}, FieldTypeString},
		{"true", &LiteralExpression{Token: TOKEN_TRUE, Value: true}, FieldTypeBool},
		{"false", &LiteralExpression{Token: TOKEN_FALSE, Value: false}, FieldTypeBool},
		{"null", &LiteralExpression{Token: TOKEN_NULL, Value: nil}, FieldTypeNull},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sa.resolveExpression(tc.expr)
			if tc.expr.ResolvedType != tc.want {
				t.Errorf("got %s, want %s", tc.expr.ResolvedType, tc.want)
			}
		})
	}
}

// --- Identifier resolution ---

func TestResolveIdentifierWithSchema(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	tests := []struct {
		name     string
		field    string
		wantType FieldType
	}{
		{"string field", "name", FieldTypeString},
		{"int field", "age", FieldTypeInt},
		{"float field", "salary", FieldTypeFloat},
		{"bool field", "active", FieldTypeBool},
		{"datetime field", "created_at", FieldTypeDateTime},
		{"date field", "birthday", FieldTypeDate},
		{"documentid", "DocumentID", FieldTypeString},
		{"unknown field", "nonexistent", FieldTypeUnknown},
		{"wildcard", "*", FieldTypeUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr := &IdentifierExpression{Name: tc.field}
			sa.resolveExpression(expr)
			if expr.ResolvedType != tc.wantType {
				t.Errorf("field %q: got %s, want %s", tc.field, expr.ResolvedType, tc.wantType)
			}
		})
	}
}

func TestResolveIdentifierSchemaless(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())
	sa.primaryBundle = "Logs"
	sa.schemas["Logs"] = &SchemaInfo{
		BundleName: "Logs",
		HasSchema:  false,
		Fields:     map[string]FieldType{"DocumentID": FieldTypeString, "documentid": FieldTypeString},
	}

	expr := &IdentifierExpression{Name: "anything"}
	sa.resolveExpression(expr)
	if expr.ResolvedType != FieldTypeUnknown {
		t.Errorf("schemaless field got %s, want Unknown", expr.ResolvedType)
	}
}

func TestResolveQualifiedIdentifier(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	expr := &QualifiedIdentifierExpression{Bundle: "Users", Field: "age"}
	sa.resolveExpression(expr)
	if expr.ResolvedType != FieldTypeInt {
		t.Errorf("got %s, want Int", expr.ResolvedType)
	}
}

// --- Binary expression typing ---

func TestResolveBinaryComparison(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	tests := []struct {
		name     string
		left     Expression
		op       TokenType
		right    Expression
		wantType FieldType
		wantErr  bool
	}{
		{
			"int = int",
			&IdentifierExpression{Name: "age"},
			TOKEN_EQ,
			&LiteralExpression{Token: TOKEN_NUMBER, Value: int64(30)},
			FieldTypeBool, false,
		},
		{
			"int > float (compatible)",
			&IdentifierExpression{Name: "age"},
			TOKEN_GT,
			&LiteralExpression{Token: TOKEN_NUMBER, Value: float64(3.14)},
			FieldTypeBool, false,
		},
		{
			"string = string",
			&IdentifierExpression{Name: "name"},
			TOKEN_EQ,
			&LiteralExpression{Token: TOKEN_STRING, Value: "Alice"},
			FieldTypeBool, false,
		},
		{
			"string > int (type mismatch)",
			&IdentifierExpression{Name: "name"},
			TOKEN_GT,
			&LiteralExpression{Token: TOKEN_NUMBER, Value: int64(5)},
			FieldTypeBool, true,
		},
		{
			"IS NULL",
			&IdentifierExpression{Name: "name"},
			TOKEN_IS_NULL,
			nil,
			FieldTypeBool, false,
		},
		{
			"AND (bool operands)",
			&BinaryExpression{
				Left:     &IdentifierExpression{Name: "active"},
				Operator: TOKEN_EQ,
				Right:    &LiteralExpression{Token: TOKEN_TRUE, Value: true},
			},
			TOKEN_AND,
			&BinaryExpression{
				Left:     &IdentifierExpression{Name: "age"},
				Operator: TOKEN_GT,
				Right:    &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(18)},
			},
			FieldTypeBool, false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sa.errors = sa.errors[:0]
			expr := &BinaryExpression{Left: tc.left, Operator: tc.op, Right: tc.right}
			sa.resolveExpression(expr)
			if expr.ResolvedType != tc.wantType {
				t.Errorf("got type %s, want %s", expr.ResolvedType, tc.wantType)
			}
			if tc.wantErr && len(sa.errors) == 0 {
				t.Error("expected error, got none")
			}
			if !tc.wantErr && len(sa.errors) > 0 {
				t.Errorf("unexpected error: %v", sa.errors[0])
			}
		})
	}
}

func TestResolveBinaryArithmetic(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	tests := []struct {
		name     string
		left     Expression
		op       TokenType
		right    Expression
		wantType FieldType
	}{
		{
			"int + int = int",
			&LiteralExpression{Token: TOKEN_NUMBER, Value: int64(1)},
			TOKEN_PLUS,
			&LiteralExpression{Token: TOKEN_NUMBER, Value: int64(2)},
			FieldTypeInt,
		},
		{
			"int + float = float",
			&LiteralExpression{Token: TOKEN_NUMBER, Value: int64(1)},
			TOKEN_PLUS,
			&LiteralExpression{Token: TOKEN_NUMBER, Value: float64(2.5)},
			FieldTypeFloat,
		},
		{
			"float * float = float",
			&LiteralExpression{Token: TOKEN_NUMBER, Value: float64(1.5)},
			TOKEN_MULTIPLY,
			&LiteralExpression{Token: TOKEN_NUMBER, Value: float64(2.5)},
			FieldTypeFloat,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sa.errors = sa.errors[:0]
			expr := &BinaryExpression{Left: tc.left, Operator: tc.op, Right: tc.right}
			sa.resolveExpression(expr)
			if expr.ResolvedType != tc.wantType {
				t.Errorf("got %s, want %s", expr.ResolvedType, tc.wantType)
			}
		})
	}
}

// --- Unary expression typing ---

func TestResolveUnary(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())

	// NOT bool → bool
	boolExpr := &LiteralExpression{Token: TOKEN_TRUE, Value: true}
	notExpr := &UnaryExpression{Operator: TOKEN_NOT, Right: boolExpr}
	sa.resolveExpression(notExpr)
	if notExpr.ResolvedType != FieldTypeBool {
		t.Errorf("NOT bool: got %s, want Bool", notExpr.ResolvedType)
	}

	// -int → int
	intExpr := &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(42)}
	negExpr := &UnaryExpression{Operator: TOKEN_MINUS, Right: intExpr}
	sa.resolveExpression(negExpr)
	if negExpr.ResolvedType != FieldTypeInt {
		t.Errorf("-int: got %s, want Int", negExpr.ResolvedType)
	}

	// -float → float
	floatExpr := &LiteralExpression{Token: TOKEN_NUMBER, Value: float64(3.14)}
	negFloat := &UnaryExpression{Operator: TOKEN_MINUS, Right: floatExpr}
	sa.resolveExpression(negFloat)
	if negFloat.ResolvedType != FieldTypeFloat {
		t.Errorf("-float: got %s, want Float", negFloat.ResolvedType)
	}
}

// --- Function call typing ---

func TestResolveFunctionCalls(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	tests := []struct {
		name     string
		funcName string
		args     []Expression
		wantType FieldType
	}{
		{"COUNT(*)", "COUNT", []Expression{&IdentifierExpression{Name: "*"}}, FieldTypeInt},
		{"SUM(int)", "SUM", []Expression{&IdentifierExpression{Name: "age"}}, FieldTypeInt},
		{"SUM(float)", "SUM", []Expression{&IdentifierExpression{Name: "salary"}}, FieldTypeFloat},
		{"AVG(age)", "AVG", []Expression{&IdentifierExpression{Name: "age"}}, FieldTypeFloat},
		{"MIN(age)", "MIN", []Expression{&IdentifierExpression{Name: "age"}}, FieldTypeInt},
		{"MAX(salary)", "MAX", []Expression{&IdentifierExpression{Name: "salary"}}, FieldTypeFloat},
		{"UPPER(name)", "UPPER", []Expression{&IdentifierExpression{Name: "name"}}, FieldTypeString},
		{"LOWER(name)", "LOWER", []Expression{&IdentifierExpression{Name: "name"}}, FieldTypeString},
		{"LENGTH(name)", "LENGTH", []Expression{&IdentifierExpression{Name: "name"}}, FieldTypeInt},
		{"ABS(int)", "ABS", []Expression{&IdentifierExpression{Name: "age"}}, FieldTypeInt},
		{"NOW()", "NOW", nil, FieldTypeDateTime},
		{"EXTRACT()", "EXTRACT", nil, FieldTypeInt},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sa.errors = sa.errors[:0]
			expr := &CallExpression{Function: tc.funcName, Arguments: tc.args}
			sa.resolveExpression(expr)
			if expr.ResolvedType != tc.wantType {
				t.Errorf("got %s, want %s", expr.ResolvedType, tc.wantType)
			}
		})
	}
}

// --- Aggregate in WHERE ---

func TestAggregateInWhereIsRejected(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	stmt := &SelectStatement{
		BundleName: "Users",
		Fields: []SelectField{
			{Expression: &CallExpression{Function: "COUNT", Arguments: []Expression{&IdentifierExpression{Name: "*"}}}},
		},
		WhereClause: &BinaryExpression{
			Left: &CallExpression{
				Function:  "COUNT",
				Arguments: []Expression{&IdentifierExpression{Name: "*"}},
			},
			Operator: TOKEN_GT,
			Right:    &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(5)},
		},
	}

	err := sa.Analyze(stmt)
	if err == nil {
		t.Fatal("expected error for aggregate in WHERE, got nil")
	}

	semErr, ok := err.(*SemanticError)
	if !ok {
		t.Fatalf("expected *SemanticError, got %T", err)
	}
	if semErr.Code != ErrAggregateInWhere {
		t.Errorf("got error code %d, want ErrAggregateInWhere (%d)", semErr.Code, ErrAggregateInWhere)
	}
}

// --- GROUP BY validation ---

func TestGroupByMismatch(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	stmt := &SelectStatement{
		BundleName: "Users",
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "name"}},
			{Expression: &IdentifierExpression{Name: "age"}}, // not in GROUP BY
			{Expression: &CallExpression{Function: "COUNT", Arguments: []Expression{&IdentifierExpression{Name: "*"}}}},
		},
		GroupBy: []string{"name"},
	}

	err := sa.Analyze(stmt)
	if err == nil {
		t.Fatal("expected GROUP BY mismatch error, got nil")
	}

	semErr, ok := err.(*SemanticError)
	if !ok {
		t.Fatalf("expected *SemanticError, got %T", err)
	}
	if semErr.Code != ErrGroupByMismatch {
		t.Errorf("got error code %d, want ErrGroupByMismatch (%d)", semErr.Code, ErrGroupByMismatch)
	}
}

func TestGroupByValid(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	stmt := &SelectStatement{
		BundleName: "Users",
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "name"}},
			{Expression: &CallExpression{Function: "COUNT", Arguments: []Expression{&IdentifierExpression{Name: "*"}}}},
		},
		GroupBy: []string{"name"},
	}

	err := sa.Analyze(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- ValidateFieldNames (strict mode) ---

func TestStrictModeRejectsUnknownFields(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users").
		WithConfig(AnalyzerConfig{ValidateFieldNames: true})

	stmt := &SelectStatement{
		BundleName: "Users",
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "nonexistent_field"}},
		},
	}

	err := sa.Analyze(stmt)
	if err == nil {
		t.Fatal("expected error for unknown field in strict mode, got nil")
	}
	semErr := err.(*SemanticError)
	if semErr.Code != ErrUnknownField {
		t.Errorf("got error code %d, want ErrUnknownField", semErr.Code)
	}
}

func TestPermissiveModeAllowsUnknownFields(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	stmt := &SelectStatement{
		BundleName: "Users",
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "nonexistent_field"}},
		},
	}

	err := sa.Analyze(stmt)
	if err != nil {
		t.Fatalf("unexpected error in permissive mode: %v", err)
	}
}

// --- Schema-less mode ---

func TestSchemalessBundle(t *testing.T) {
	schemalessBundle := &models.Bundle{
		Name:              "Logs",
		DocumentStructure: models.DocumentStructure{},
	}

	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(schemalessBundle).
		SetPrimaryBundle("Logs")

	stmt := &SelectStatement{
		BundleName: "Logs",
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "message"}},
		},
		WhereClause: &BinaryExpression{
			Left:     &IdentifierExpression{Name: "level"},
			Operator: TOKEN_EQ,
			Right:    &LiteralExpression{Token: TOKEN_STRING, Value: "ERROR"},
		},
	}

	err := sa.Analyze(stmt)
	if err != nil {
		t.Fatalf("schemaless analysis should succeed: %v", err)
	}
}

// --- Full Analyze pipeline ---

func TestAnalyzeFullQuery(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	stmt := &SelectStatement{
		BundleName: "Users",
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "name"}},
			{Expression: &IdentifierExpression{Name: "age"}},
		},
		WhereClause: &BinaryExpression{
			Left: &BinaryExpression{
				Left:     &IdentifierExpression{Name: "age"},
				Operator: TOKEN_GT,
				Right:    &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(18)},
			},
			Operator: TOKEN_AND,
			Right: &BinaryExpression{
				Left:     &IdentifierExpression{Name: "active"},
				Operator: TOKEN_EQ,
				Right:    &LiteralExpression{Token: TOKEN_TRUE, Value: true},
			},
		},
	}

	err := sa.Analyze(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify types were resolved
	nameExpr := stmt.Fields[0].Expression.(*IdentifierExpression)
	if nameExpr.ResolvedType != FieldTypeString {
		t.Errorf("name field: got %s, want String", nameExpr.ResolvedType)
	}

	ageExpr := stmt.Fields[1].Expression.(*IdentifierExpression)
	if ageExpr.ResolvedType != FieldTypeInt {
		t.Errorf("age field: got %s, want Int", ageExpr.ResolvedType)
	}

	// WHERE clause should be Bool
	whereType := GetResolvedType(stmt.WhereClause)
	if whereType != FieldTypeBool {
		t.Errorf("WHERE clause: got %s, want Bool", whereType)
	}
}

// --- AnalyzeExpression (standalone) ---

func TestAnalyzeExpressionStandalone(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	expr := &BinaryExpression{
		Left:     &IdentifierExpression{Name: "age"},
		Operator: TOKEN_GT,
		Right:    &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(21)},
	}

	err := sa.AnalyzeExpression(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expr.ResolvedType != FieldTypeBool {
		t.Errorf("got %s, want Bool", expr.ResolvedType)
	}
}

// --- Nil handling ---

func TestAnalyzeNilStatement(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())
	if err := sa.Analyze(nil); err != nil {
		t.Fatalf("Analyze(nil) should return nil, got %v", err)
	}
}

func TestAnalyzeExpressionNil(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())
	if err := sa.AnalyzeExpression(nil); err != nil {
		t.Fatalf("AnalyzeExpression(nil) should return nil, got %v", err)
	}
}

// --- Array expression typing ---

func TestResolveArray(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())

	// Homogeneous int array
	arr := &ArrayExpression{
		Elements: []Expression{
			&LiteralExpression{Token: TOKEN_NUMBER, Value: int64(1)},
			&LiteralExpression{Token: TOKEN_NUMBER, Value: int64(2)},
			&LiteralExpression{Token: TOKEN_NUMBER, Value: int64(3)},
		},
	}
	sa.resolveExpression(arr)
	if arr.ResolvedType != FieldTypeArray {
		t.Errorf("array type: got %s, want Array", arr.ResolvedType)
	}
	if arr.ElementType != FieldTypeInt {
		t.Errorf("element type: got %s, want Int", arr.ElementType)
	}

	// Mixed int/float promotes to float
	arr2 := &ArrayExpression{
		Elements: []Expression{
			&LiteralExpression{Token: TOKEN_NUMBER, Value: int64(1)},
			&LiteralExpression{Token: TOKEN_NUMBER, Value: float64(2.5)},
		},
	}
	sa.resolveExpression(arr2)
	if arr2.ElementType != FieldTypeFloat {
		t.Errorf("mixed element type: got %s, want Float", arr2.ElementType)
	}

	// Empty array
	arr3 := &ArrayExpression{Elements: nil}
	sa.resolveExpression(arr3)
	if arr3.ElementType != FieldTypeUnknown {
		t.Errorf("empty array element type: got %s, want Unknown", arr3.ElementType)
	}
}

// --- IntervalExpression and AtTimeZoneExpression ---

func TestResolveSpecialExpressions(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())

	interval := &IntervalExpression{Value: "7", Unit: TOKEN_DAY}
	sa.resolveExpression(interval)
	if interval.ResolvedType != FieldTypeInterval {
		t.Errorf("interval: got %s, want Interval", interval.ResolvedType)
	}

	atTz := &AtTimeZoneExpression{
		Expression: &IdentifierExpression{Name: "created_at"},
		Timezone:   "UTC",
	}
	sa.resolveExpression(atTz)
	if atTz.ResolvedType != FieldTypeDateTime {
		t.Errorf("at time zone: got %s, want DateTime", atTz.ResolvedType)
	}
}

// --- LIKE type check ---

func TestLikeRequiresString(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	// Valid: string LIKE string
	sa.errors = sa.errors[:0]
	expr1 := &BinaryExpression{
		Left:     &IdentifierExpression{Name: "name"},
		Operator: TOKEN_LIKE,
		Right:    &LiteralExpression{Token: TOKEN_STRING, Value: "%alice%"},
	}
	sa.resolveExpression(expr1)
	if len(sa.errors) > 0 {
		t.Errorf("string LIKE string should not error: %v", sa.errors[0])
	}

	// Invalid: int LIKE string
	sa.errors = sa.errors[:0]
	expr2 := &BinaryExpression{
		Left:     &IdentifierExpression{Name: "age"},
		Operator: TOKEN_LIKE,
		Right:    &LiteralExpression{Token: TOKEN_STRING, Value: "%5%"},
	}
	sa.resolveExpression(expr2)
	if len(sa.errors) == 0 {
		t.Error("int LIKE string should produce a type mismatch error")
	}
}

// --- SchemaInfo construction ---

func TestNewSchemaInfoFromBundle(t *testing.T) {
	// nil bundle
	si := NewSchemaInfoFromBundle(nil)
	if si.HasSchema {
		t.Error("nil bundle should have HasSchema=false")
	}

	// Bundle with schema
	si2 := NewSchemaInfoFromBundle(testBundle())
	if !si2.HasSchema {
		t.Error("bundle with fields should have HasSchema=true")
	}
	if si2.Fields["name"] != FieldTypeString {
		t.Errorf("name field: got %s, want String", si2.Fields["name"])
	}
	if si2.Fields["age"] != FieldTypeInt {
		t.Errorf("age field: got %s, want Int", si2.Fields["age"])
	}
	if si2.Fields["DocumentID"] != FieldTypeString {
		t.Errorf("DocumentID: got %s, want String", si2.Fields["DocumentID"])
	}
}

// --- Ambiguous field in JOIN ---

func TestAmbiguousFieldInJoin(t *testing.T) {
	ordersBundle := &models.Bundle{
		Name: "Orders",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"name":   {Type: "string"}, // overlaps with Users.name
				"amount": {Type: "float"},
			},
		},
	}

	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		WithBundle(ordersBundle)
	// No primary bundle set — forces search across all schemas

	expr := &IdentifierExpression{Name: "name"}
	sa.resolveExpression(expr)
	if len(sa.errors) == 0 {
		t.Fatal("expected ambiguous field error")
	}
	if sa.errors[0].Code != ErrAmbiguousField {
		t.Errorf("got error code %d, want ErrAmbiguousField", sa.errors[0].Code)
	}
}

// --- Errors() accessor ---

func TestErrorsAccessor(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())
	if len(sa.Errors()) != 0 {
		t.Error("new analyzer should have no errors")
	}

	sa.addError(NewSemanticError(ErrTypeMismatch, "test"))
	if len(sa.Errors()) != 1 {
		t.Errorf("expected 1 error, got %d", len(sa.Errors()))
	}
}

// --- Subquery expression typing ---

func TestResolveSubquery(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())

	// EXISTS → Bool
	exists := &SubqueryExpression{SubqueryType: SUBQUERY_EXISTS}
	sa.resolveExpression(exists)
	if exists.ResolvedType != FieldTypeBool {
		t.Errorf("EXISTS: got %s, want Bool", exists.ResolvedType)
	}

	// NOT EXISTS → Bool
	notExists := &SubqueryExpression{SubqueryType: SUBQUERY_NOT_EXISTS}
	sa.resolveExpression(notExists)
	if notExists.ResolvedType != FieldTypeBool {
		t.Errorf("NOT EXISTS: got %s, want Bool", notExists.ResolvedType)
	}

	// IN with inner query
	inSubq := &SubqueryExpression{
		SubqueryType: SUBQUERY_IN,
		InnerQuery: &SelectStatement{
			Fields: []SelectField{
				{Expression: &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(1)}},
			},
		},
	}
	sa.resolveExpression(inSubq)
	if inSubq.ResolvedType != FieldTypeInt {
		t.Errorf("IN subquery: got %s, want Int", inSubq.ResolvedType)
	}
}

// --- HAVING clause type check ---

func TestHavingMustBeBool(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger()).
		WithBundle(testBundle()).
		SetPrimaryBundle("Users")

	// Valid HAVING: COUNT(*) > 5 (resolves to Bool)
	stmt := &SelectStatement{
		BundleName: "Users",
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "name"}},
			{Expression: &CallExpression{Function: "COUNT", Arguments: []Expression{&IdentifierExpression{Name: "*"}}}},
		},
		GroupBy: []string{"name"},
		Having: &BinaryExpression{
			Left:     &CallExpression{Function: "COUNT", Arguments: []Expression{&IdentifierExpression{Name: "*"}}},
			Operator: TOKEN_GT,
			Right:    &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(5)},
		},
	}

	err := sa.Analyze(stmt)
	if err != nil {
		t.Fatalf("valid HAVING should not error: %v", err)
	}
}

// --- Grouped expression ---

func TestResolveGroupedExpression(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())

	inner := &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(42)}
	grouped := &GroupedExpression{Expression: inner}
	sa.resolveExpression(grouped)
	if grouped.ResolvedType != FieldTypeInt {
		t.Errorf("grouped: got %s, want Int", grouped.ResolvedType)
	}
}

// --- ParameterExpression ---

func TestResolveParameter(t *testing.T) {
	sa := NewSemanticAnalyzer(testLogger())

	param := &ParameterExpression{Index: 1}
	sa.resolveExpression(param)
	if param.ResolvedType != FieldTypeUnknown {
		t.Errorf("parameter: got %s, want Unknown", param.ResolvedType)
	}
}
