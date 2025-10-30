package syndrQL

import (
	"testing"

	"go.uber.org/zap"
)

// Test basic SELECT patterns

func TestSelectParser_SelectAll(t *testing.T) {
	input := "SELECT * FROM Authors"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Pattern != PATTERN_SELECT_ALL {
		t.Errorf("Expected PATTERN_SELECT_ALL, got %s", stmt.Pattern.String())
	}

	if stmt.BundleName != "Authors" {
		t.Errorf("Expected bundle 'Authors', got '%s'", stmt.BundleName)
	}

	if len(stmt.Fields) != 1 {
		t.Errorf("Expected 1 field, got %d", len(stmt.Fields))
	}

	if stmt.WhereClause != nil {
		t.Error("Expected no WHERE clause")
	}
}

func TestSelectParser_SelectFields(t *testing.T) {
	input := "SELECT AuthorName, Age, Country FROM Authors"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Pattern != PATTERN_SELECT_FIELDS {
		t.Errorf("Expected PATTERN_SELECT_FIELDS, got %s", stmt.Pattern.String())
	}

	if len(stmt.Fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(stmt.Fields))
	}

	expectedFields := []string{"AuthorName", "Age", "Country"}
	for i, field := range stmt.Fields {
		if ident, ok := field.Expression.(*IdentifierExpression); ok {
			if ident.Name != expectedFields[i] {
				t.Errorf("Field %d: expected '%s', got '%s'", i, expectedFields[i], ident.Name)
			}
		} else {
			t.Errorf("Field %d: expected IdentifierExpression, got %T", i, field.Expression)
		}
	}
}

func TestSelectParser_SelectWithQuotedBundleName(t *testing.T) {
	input := `SELECT * FROM "Authors"`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.BundleName != "Authors" {
		t.Errorf("Expected bundle 'Authors', got '%s'", stmt.BundleName)
	}
}

// Test WHERE clause

func TestSelectParser_SimpleWhereClause(t *testing.T) {
	input := "SELECT * FROM Authors WHERE Age > 25"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Pattern != PATTERN_SELECT_WHERE_SIMPLE {
		t.Errorf("Expected PATTERN_SELECT_WHERE_SIMPLE, got %s", stmt.Pattern.String())
	}

	if stmt.WhereClause == nil {
		t.Fatal("Expected WHERE clause")
	}

	// Verify WHERE clause is a binary expression
	binary, ok := stmt.WhereClause.(*BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression, got %T", stmt.WhereClause)
	}

	if binary.Operator != TOKEN_GT {
		t.Errorf("Expected GT operator, got %s", binary.Operator.String())
	}
}

func TestSelectParser_ComplexWhereClause(t *testing.T) {
	input := "SELECT * FROM Authors WHERE Age >= 18 AND Country == \"USA\""
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Pattern != PATTERN_SELECT_WHERE_COMPLEX {
		t.Errorf("Expected PATTERN_SELECT_WHERE_COMPLEX, got %s", stmt.Pattern.String())
	}

	if stmt.WhereClause == nil {
		t.Fatal("Expected WHERE clause")
	}

	// Verify it's an AND expression
	binary, ok := stmt.WhereClause.(*BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression, got %T", stmt.WhereClause)
	}

	if binary.Operator != TOKEN_AND {
		t.Errorf("Expected AND operator, got %s", binary.Operator.String())
	}
}

func TestSelectParser_WhereWithInClause(t *testing.T) {
	input := "SELECT * FROM Authors WHERE Age IN [18, 21, 25]"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.WhereClause == nil {
		t.Fatal("Expected WHERE clause")
	}

	binary, ok := stmt.WhereClause.(*BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression, got %T", stmt.WhereClause)
	}

	if binary.Operator != TOKEN_IN {
		t.Errorf("Expected IN operator, got %s", binary.Operator.String())
	}

	// Verify right side is an array
	_, ok = binary.Right.(*ArrayExpression)
	if !ok {
		t.Errorf("Expected ArrayExpression on right side, got %T", binary.Right)
	}
}

// Test SELECT modifiers

func TestSelectParser_SelectDistinct(t *testing.T) {
	input := "SELECT DISTINCT Country FROM Authors"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if !stmt.Distinct {
		t.Error("Expected DISTINCT flag to be true")
	}
}

func TestSelectParser_SelectTop(t *testing.T) {
	input := "SELECT TOP 10 * FROM Authors"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.TopN != 10 {
		t.Errorf("Expected TopN = 10, got %d", stmt.TopN)
	}
}

func TestSelectParser_SelectTopWithFields(t *testing.T) {
	input := "SELECT TOP 5 AuthorName, Age FROM Authors"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.TopN != 5 {
		t.Errorf("Expected TopN = 5, got %d", stmt.TopN)
	}

	if len(stmt.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(stmt.Fields))
	}
}

// Test ORDER BY

func TestSelectParser_OrderByAscending(t *testing.T) {
	input := "SELECT * FROM Authors ORDER BY Age"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmt.OrderBy) != 1 {
		t.Fatalf("Expected 1 ORDER BY field, got %d", len(stmt.OrderBy))
	}

	if stmt.OrderBy[0].Field != "Age" {
		t.Errorf("Expected field 'Age', got '%s'", stmt.OrderBy[0].Field)
	}

	if stmt.OrderBy[0].Descending {
		t.Error("Expected ascending order")
	}
}

func TestSelectParser_OrderByDescending(t *testing.T) {
	input := "SELECT * FROM Authors ORDER BY Age DESC"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmt.OrderBy) != 1 {
		t.Fatalf("Expected 1 ORDER BY field, got %d", len(stmt.OrderBy))
	}

	if !stmt.OrderBy[0].Descending {
		t.Error("Expected descending order")
	}
}

func TestSelectParser_OrderByMultipleFields(t *testing.T) {
	input := "SELECT * FROM Authors ORDER BY Country ASC, Age DESC"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmt.OrderBy) != 2 {
		t.Fatalf("Expected 2 ORDER BY fields, got %d", len(stmt.OrderBy))
	}

	if stmt.OrderBy[0].Field != "Country" || stmt.OrderBy[0].Descending {
		t.Error("First field should be Country ASC")
	}

	if stmt.OrderBy[1].Field != "Age" || !stmt.OrderBy[1].Descending {
		t.Error("Second field should be Age DESC")
	}
}

// Test LIMIT and OFFSET

func TestSelectParser_Limit(t *testing.T) {
	input := "SELECT * FROM Authors LIMIT 10"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Limit != 10 {
		t.Errorf("Expected LIMIT 10, got %d", stmt.Limit)
	}
}

func TestSelectParser_LimitOffset(t *testing.T) {
	input := "SELECT * FROM Authors LIMIT 10 OFFSET 20"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Limit != 10 {
		t.Errorf("Expected LIMIT 10, got %d", stmt.Limit)
	}

	if stmt.Offset != 20 {
		t.Errorf("Expected OFFSET 20, got %d", stmt.Offset)
	}
}

// Test GROUP BY

func TestSelectParser_GroupBy(t *testing.T) {
	input := "SELECT Country FROM Authors GROUP BY Country"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Pattern != PATTERN_SELECT_GROUPBY {
		t.Errorf("Expected PATTERN_SELECT_GROUPBY, got %s", stmt.Pattern.String())
	}

	if len(stmt.GroupBy) != 1 {
		t.Fatalf("Expected 1 GROUP BY field, got %d", len(stmt.GroupBy))
	}

	if stmt.GroupBy[0] != "Country" {
		t.Errorf("Expected GROUP BY 'Country', got '%s'", stmt.GroupBy[0])
	}
}

func TestSelectParser_GroupByMultipleFields(t *testing.T) {
	input := "SELECT Country, Status FROM Authors GROUP BY Country, Status"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmt.GroupBy) != 2 {
		t.Fatalf("Expected 2 GROUP BY fields, got %d", len(stmt.GroupBy))
	}

	expectedFields := []string{"Country", "Status"}
	for i, field := range stmt.GroupBy {
		if field != expectedFields[i] {
			t.Errorf("GROUP BY field %d: expected '%s', got '%s'", i, expectedFields[i], field)
		}
	}
}

// Test HAVING

func TestSelectParser_Having(t *testing.T) {
	input := "SELECT Country FROM Authors GROUP BY Country HAVING COUNT(*) > 5"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Having == nil {
		t.Fatal("Expected HAVING clause")
	}

	// Verify HAVING is a binary expression
	binary, ok := stmt.Having.(*BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression, got %T", stmt.Having)
	}

	if binary.Operator != TOKEN_GT {
		t.Errorf("Expected GT operator, got %s", binary.Operator.String())
	}
}

// Test field aliases

func TestSelectParser_FieldAlias(t *testing.T) {
	input := "SELECT AuthorName AS Name, Age AS Years FROM Authors"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmt.Fields) != 2 {
		t.Fatalf("Expected 2 fields, got %d", len(stmt.Fields))
	}

	if stmt.Fields[0].Alias != "Name" {
		t.Errorf("Expected alias 'Name', got '%s'", stmt.Fields[0].Alias)
	}

	if stmt.Fields[1].Alias != "Years" {
		t.Errorf("Expected alias 'Years', got '%s'", stmt.Fields[1].Alias)
	}
}

// Test function calls in SELECT

func TestSelectParser_FunctionCall(t *testing.T) {
	input := "SELECT COUNT(*) FROM Authors"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Pattern != PATTERN_SELECT_AGGREGATE {
		t.Errorf("Expected PATTERN_SELECT_AGGREGATE, got %s", stmt.Pattern.String())
	}

	if len(stmt.Fields) != 1 {
		t.Fatalf("Expected 1 field, got %d", len(stmt.Fields))
	}

	// Verify it's a function call
	_, ok := stmt.Fields[0].Expression.(*CallExpression)
	if !ok {
		t.Errorf("Expected CallExpression, got %T", stmt.Fields[0].Expression)
	}
}

func TestSelectParser_MultipleFunctions(t *testing.T) {
	input := "SELECT COUNT(*), SUM(Age), AVG(Salary) FROM Authors"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmt.Fields) != 3 {
		t.Fatalf("Expected 3 fields, got %d", len(stmt.Fields))
	}

	for i, field := range stmt.Fields {
		_, ok := field.Expression.(*CallExpression)
		if !ok {
			t.Errorf("Field %d: expected CallExpression, got %T", i, field.Expression)
		}
	}
}

// Test arithmetic expressions in SELECT

func TestSelectParser_ArithmeticExpression(t *testing.T) {
	input := "SELECT Price * Quantity FROM Orders"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmt.Fields) != 1 {
		t.Fatalf("Expected 1 field, got %d", len(stmt.Fields))
	}

	// Verify it's a binary expression
	binary, ok := stmt.Fields[0].Expression.(*BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression, got %T", stmt.Fields[0].Expression)
	}

	if binary.Operator != TOKEN_MULTIPLY {
		t.Errorf("Expected MULTIPLY operator, got %s", binary.Operator.String())
	}
}

func TestSelectParser_ComplexArithmetic(t *testing.T) {
	input := "SELECT (Price * Quantity) + Tax AS Total FROM Orders"
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmt.Fields) != 1 {
		t.Fatalf("Expected 1 field, got %d", len(stmt.Fields))
	}

	if stmt.Fields[0].Alias != "Total" {
		t.Errorf("Expected alias 'Total', got '%s'", stmt.Fields[0].Alias)
	}

	// Verify it's a binary expression at the root
	binary, ok := stmt.Fields[0].Expression.(*BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression at root, got %T", stmt.Fields[0].Expression)
	}

	if binary.Operator != TOKEN_PLUS {
		t.Errorf("Expected PLUS operator at root, got %s", binary.Operator.String())
	}
}

// Test complex queries

func TestSelectParser_ComplexQuery(t *testing.T) {
	input := `SELECT AuthorName, Age, Country 
	          FROM Authors 
	          WHERE Age >= 18 AND Country == "USA" 
	          ORDER BY Age DESC 
	          LIMIT 10`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	// Verify all components
	if len(stmt.Fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(stmt.Fields))
	}

	if stmt.BundleName != "Authors" {
		t.Errorf("Expected bundle 'Authors', got '%s'", stmt.BundleName)
	}

	if stmt.WhereClause == nil {
		t.Error("Expected WHERE clause")
	}

	if len(stmt.OrderBy) != 1 {
		t.Errorf("Expected 1 ORDER BY field, got %d", len(stmt.OrderBy))
	}

	if stmt.Limit != 10 {
		t.Errorf("Expected LIMIT 10, got %d", stmt.Limit)
	}
}

func TestSelectParser_VeryComplexQuery(t *testing.T) {
	input := `SELECT DISTINCT Country, COUNT(*) AS Total, AVG(Age) AS AvgAge
	          FROM Authors
	          WHERE Age >= 18 AND Status != "inactive"
	          GROUP BY Country
	          HAVING COUNT(*) > 5
	          ORDER BY Total DESC
	          LIMIT 20 OFFSET 10`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	// Verify all components are present
	if !stmt.Distinct {
		t.Error("Expected DISTINCT")
	}

	if len(stmt.Fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(stmt.Fields))
	}

	if stmt.WhereClause == nil {
		t.Error("Expected WHERE clause")
	}

	if len(stmt.GroupBy) != 1 {
		t.Errorf("Expected 1 GROUP BY field, got %d", len(stmt.GroupBy))
	}

	if stmt.Having == nil {
		t.Error("Expected HAVING clause")
	}

	if len(stmt.OrderBy) != 1 {
		t.Errorf("Expected 1 ORDER BY field, got %d", len(stmt.OrderBy))
	}

	if stmt.Limit != 20 {
		t.Errorf("Expected LIMIT 20, got %d", stmt.Limit)
	}

	if stmt.Offset != 10 {
		t.Errorf("Expected OFFSET 10, got %d", stmt.Offset)
	}
}

// Test index hints extraction

func TestSelectParser_IndexHints(t *testing.T) {
	input := "SELECT * FROM Authors WHERE Age > 18 AND Country == \"USA\""
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if len(stmt.IndexHints) == 0 {
		t.Error("Expected index hints to be extracted")
	}

	// Should detect Age and Country as potential index fields
	expectedHints := map[string]bool{"Age": true, "Country": true}
	for _, hint := range stmt.IndexHints {
		if !expectedHints[hint] {
			t.Errorf("Unexpected index hint: %s", hint)
		}
	}
}

// Test pattern detection

func TestSelectPatternDetector_DetectAllPattern(t *testing.T) {
	input := "SELECT * FROM Authors"
	tokenizer := NewTokenizer(input)
	tokens, _ := tokenizer.Tokenize()

	detector := NewSelectPatternDetector()
	pattern := detector.DetectPattern(tokens)

	if pattern != PATTERN_SELECT_ALL {
		t.Errorf("Expected PATTERN_SELECT_ALL, got %s", pattern.String())
	}
}

func TestSelectPatternDetector_DetectFieldsPattern(t *testing.T) {
	input := "SELECT AuthorName, Age FROM Authors"
	tokenizer := NewTokenizer(input)
	tokens, _ := tokenizer.Tokenize()

	detector := NewSelectPatternDetector()
	pattern := detector.DetectPattern(tokens)

	if pattern != PATTERN_SELECT_FIELDS {
		t.Errorf("Expected PATTERN_SELECT_FIELDS, got %s", pattern.String())
	}
}

func TestSelectPatternDetector_DetectAggregatePattern(t *testing.T) {
	input := "SELECT COUNT(*) FROM Authors"
	tokenizer := NewTokenizer(input)
	tokens, _ := tokenizer.Tokenize()

	detector := NewSelectPatternDetector()
	pattern := detector.DetectPattern(tokens)

	if pattern != PATTERN_SELECT_AGGREGATE {
		t.Errorf("Expected PATTERN_SELECT_AGGREGATE, got %s", pattern.String())
	}
}

func TestSelectPatternDetector_DetectGroupByPattern(t *testing.T) {
	input := "SELECT Country FROM Authors GROUP BY Country"
	tokenizer := NewTokenizer(input)
	tokens, _ := tokenizer.Tokenize()

	detector := NewSelectPatternDetector()
	pattern := detector.DetectPattern(tokens)

	if pattern != PATTERN_SELECT_GROUPBY {
		t.Errorf("Expected PATTERN_SELECT_GROUPBY, got %s", pattern.String())
	}
}

// Test error handling

func TestSelectParser_MissingFrom(t *testing.T) {
	input := "SELECT * WHERE Age > 18"
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Error("Expected error for missing FROM clause")
	}
}

func TestSelectParser_MissingBundleName(t *testing.T) {
	input := "SELECT * FROM"
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Error("Expected error for missing bundle name")
	}
}

func TestSelectParser_EmptyFieldList(t *testing.T) {
	input := "SELECT FROM Authors"
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Error("Expected error for empty field list")
	}
}

func TestSelectParser_InvalidWhereClause(t *testing.T) {
	input := "SELECT * FROM Authors WHERE"
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Error("Expected error for incomplete WHERE clause")
	}
}

// Benchmark tests

func BenchmarkSelectParser_SimpleSelectAll(b *testing.B) {
	input := "SELECT * FROM Authors"
	logger := zap.NewExample().Sugar()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseSelect(input, logger)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectParser_SelectWithWhere(b *testing.B) {
	input := "SELECT * FROM Authors WHERE Age > 25"
	logger := zap.NewExample().Sugar()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseSelect(input, logger)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectParser_ComplexQuery(b *testing.B) {
	input := `SELECT AuthorName, Age, Country 
	          FROM Authors 
	          WHERE Age >= 18 AND Country == "USA" 
	          ORDER BY Age DESC 
	          LIMIT 10`
	logger := zap.NewExample().Sugar()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseSelect(input, logger)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectParser_VeryComplexQuery(b *testing.B) {
	input := `SELECT DISTINCT Country, COUNT(*) AS Total, AVG(Age) AS AvgAge
	          FROM Authors
	          WHERE Age >= 18 AND Status != "inactive"
	          GROUP BY Country
	          HAVING COUNT(*) > 5
	          ORDER BY Total DESC
	          LIMIT 20 OFFSET 10`
	logger := zap.NewExample().Sugar()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseSelect(input, logger)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectPatternDetector_Detect(b *testing.B) {
	input := "SELECT * FROM Authors WHERE Age > 25"
	tokenizer := NewTokenizer(input)
	tokens, _ := tokenizer.Tokenize()
	detector := NewSelectPatternDetector()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = detector.DetectPattern(tokens)
	}
}

// Test JOIN parsing functionality

// TestSelectParser_BasicJoin tests a simple INNER JOIN query with single condition
func TestSelectParser_BasicJoin(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."DocumentID" == "Books"."AuthorID"`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse JOIN query: %v", err)
	}

	// Verify pattern detection
	if stmt.Pattern != PATTERN_SELECT_JOIN {
		t.Errorf("Expected PATTERN_SELECT_JOIN, got %s", stmt.Pattern.String())
	}

	// Verify bundle name
	if stmt.BundleName != "Authors" {
		t.Errorf("Expected bundle 'Authors', got '%s'", stmt.BundleName)
	}

	// Verify JOIN clause count
	if len(stmt.JoinClauses) != 1 {
		t.Fatalf("Expected 1 JOIN clause, got %d", len(stmt.JoinClauses))
	}

	// Verify JOIN details
	join := stmt.JoinClauses[0]
	if join.JoinType != InnerJoin {
		t.Errorf("Expected InnerJoin, got %d", join.JoinType)
	}

	if join.RightBundle != "Books" {
		t.Errorf("Expected right bundle 'Books', got '%s'", join.RightBundle)
	}

	// Verify JOIN conditions
	if len(join.JoinConditions) != 1 {
		t.Fatalf("Expected 1 JOIN condition, got %d", len(join.JoinConditions))
	}

	cond := join.JoinConditions[0]
	if cond.LeftField != "\"Authors\".\"DocumentID\"" {
		t.Errorf("Expected left field '\"Authors\".\"DocumentID\"', got '%s'", cond.LeftField)
	}

	if cond.RightField != "\"Books\".\"AuthorID\"" {
		t.Errorf("Expected right field '\"Books\".\"AuthorID\"', got '%s'", cond.RightField)
	}

	if cond.Operator != "==" {
		t.Errorf("Expected operator '==', got '%s'", cond.Operator)
	}
}

// TestSelectParser_JoinWithWhere tests JOIN query combined with WHERE clause
// Note: WHERE clause uses unqualified field names as the WHERE parser doesn't yet support qualified names
func TestSelectParser_JoinWithWhere(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" WHERE Country == "USA"`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse JOIN with WHERE: %v", err)
	}

	// Verify pattern
	if stmt.Pattern != PATTERN_SELECT_JOIN {
		t.Errorf("Expected PATTERN_SELECT_JOIN, got %s", stmt.Pattern.String())
	}

	// Verify JOIN clause exists
	if len(stmt.JoinClauses) != 1 {
		t.Fatalf("Expected 1 JOIN clause, got %d", len(stmt.JoinClauses))
	}

	// Verify WHERE clause exists
	if stmt.WhereClause == nil {
		t.Fatal("Expected WHERE clause to be present")
	}

	// Verify JOIN details
	join := stmt.JoinClauses[0]
	if join.RightBundle != "Books" {
		t.Errorf("Expected right bundle 'Books', got '%s'", join.RightBundle)
	}
}

// TestSelectParser_JoinMultipleConditions tests JOIN with multiple AND conditions
func TestSelectParser_JoinMultipleConditions(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" AND "Authors"."Country" == "Books"."Country"`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse JOIN with multiple conditions: %v", err)
	}

	// Verify JOIN clause exists
	if len(stmt.JoinClauses) != 1 {
		t.Fatalf("Expected 1 JOIN clause, got %d", len(stmt.JoinClauses))
	}

	join := stmt.JoinClauses[0]

	// Verify multiple conditions
	if len(join.JoinConditions) != 2 {
		t.Fatalf("Expected 2 JOIN conditions, got %d", len(join.JoinConditions))
	}

	// Verify first condition
	cond1 := join.JoinConditions[0]
	if cond1.LeftField != "\"Authors\".\"ID\"" {
		t.Errorf("Expected first condition left field '\"Authors\".\"ID\"', got '%s'", cond1.LeftField)
	}

	// Verify second condition
	cond2 := join.JoinConditions[1]
	if cond2.LeftField != "\"Authors\".\"Country\"" {
		t.Errorf("Expected second condition left field '\"Authors\".\"Country\"', got '%s'", cond2.LeftField)
	}
}

// TestSelectParser_MultipleJoins tests query with multiple JOIN clauses
func TestSelectParser_MultipleJoins(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" JOIN "Publishers" ON "Books"."PublisherID" == "Publishers"."ID"`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse multiple JOINs: %v", err)
	}

	// Verify multiple JOIN clauses
	if len(stmt.JoinClauses) != 2 {
		t.Fatalf("Expected 2 JOIN clauses, got %d", len(stmt.JoinClauses))
	}

	// Verify first JOIN
	join1 := stmt.JoinClauses[0]
	if join1.RightBundle != "Books" {
		t.Errorf("Expected first JOIN right bundle 'Books', got '%s'", join1.RightBundle)
	}

	// Verify second JOIN
	join2 := stmt.JoinClauses[1]
	if join2.RightBundle != "Publishers" {
		t.Errorf("Expected second JOIN right bundle 'Publishers', got '%s'", join2.RightBundle)
	}
}

// TestSelectParser_JoinWithOrderBy tests JOIN with ORDER BY clause
// Note: ORDER BY uses unqualified field names as the ORDER BY parser doesn't yet support quoted strings
func TestSelectParser_JoinWithOrderBy(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" ORDER BY Name ASC`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse JOIN with ORDER BY: %v", err)
	}

	// Verify JOIN exists
	if len(stmt.JoinClauses) != 1 {
		t.Fatalf("Expected 1 JOIN clause, got %d", len(stmt.JoinClauses))
	}

	// Verify ORDER BY exists
	if len(stmt.OrderBy) != 1 {
		t.Fatalf("Expected 1 ORDER BY field, got %d", len(stmt.OrderBy))
	}

	// Verify ORDER BY field
	orderBy := stmt.OrderBy[0]
	if orderBy.Field != "Name" {
		t.Errorf("Expected ORDER BY field 'Name', got '%s'", orderBy.Field)
	}

	if orderBy.Descending {
		t.Error("Expected ascending order, got descending")
	}
}

// TestSelectParser_JoinWithLimitOffset tests JOIN with LIMIT and OFFSET
func TestSelectParser_JoinWithLimitOffset(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" LIMIT 10 OFFSET 5`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse JOIN with LIMIT/OFFSET: %v", err)
	}

	// Verify JOIN exists
	if len(stmt.JoinClauses) != 1 {
		t.Fatalf("Expected 1 JOIN clause, got %d", len(stmt.JoinClauses))
	}

	// Verify LIMIT
	if stmt.Limit != 10 {
		t.Errorf("Expected LIMIT 10, got %d", stmt.Limit)
	}

	// Verify OFFSET
	if stmt.Offset != 5 {
		t.Errorf("Expected OFFSET 5, got %d", stmt.Offset)
	}
}

// TestSelectParser_JoinErrorCases tests error handling for invalid JOIN queries

// TestSelectParser_JoinMissingON tests error when ON keyword is missing
func TestSelectParser_JoinMissingON(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" "Authors"."ID" == "Books"."AuthorID"`
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Fatal("Expected error for missing ON keyword, got nil")
	}
}

// TestSelectParser_JoinInvalidFieldName tests error when field name is invalid
func TestSelectParser_JoinInvalidFieldName(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON 123 == "Books"."AuthorID"`
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Fatal("Expected error for invalid field name, got nil")
	}
}

// TestSelectParser_JoinMissingOperator tests error when operator is missing
func TestSelectParser_JoinMissingOperator(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" "Books"."AuthorID"`
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Fatal("Expected error for missing operator, got nil")
	}
}

// Test WITH RELATIONSHIP clause functionality

// TestSelectParser_WithRelationship tests basic WITH RELATIONSHIP clause
func TestSelectParser_WithRelationship(t *testing.T) {
	input := `SELECT DOCUMENTS FROM "Authors" JOIN "Books" ON "Authors"."DocumentID" == "Books"."AuthorsID" WITH RELATIONSHIP "Books"`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse WITH RELATIONSHIP clause: %v", err)
	}

	// Verify JOIN exists
	if len(stmt.JoinClauses) != 1 {
		t.Fatalf("Expected 1 JOIN clause, got %d", len(stmt.JoinClauses))
	}

	// Verify relationship name
	if stmt.RelationshipName != "Books" {
		t.Errorf("Expected relationship name 'Books', got '%s'", stmt.RelationshipName)
	}

	// Verify pattern still shows JOIN
	if stmt.Pattern != PATTERN_SELECT_JOIN {
		t.Errorf("Expected PATTERN_SELECT_JOIN, got %s", stmt.Pattern.String())
	}
}

// TestSelectParser_WithRelationshipWhere tests WITH RELATIONSHIP with WHERE clause
func TestSelectParser_WithRelationshipWhere(t *testing.T) {
	input := `SELECT DOCUMENTS FROM "Authors" JOIN "Books" ON "Authors"."DocumentID" == "Books"."AuthorsID" WHERE DocumentID == "187320fc9a770e28_2f" WITH RELATIONSHIP "Books"`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse WITH RELATIONSHIP and WHERE: %v", err)
	}

	// Verify JOIN exists
	if len(stmt.JoinClauses) != 1 {
		t.Fatalf("Expected 1 JOIN clause, got %d", len(stmt.JoinClauses))
	}

	// Verify WHERE exists
	if stmt.WhereClause == nil {
		t.Fatal("Expected WHERE clause to be present")
	}

	// Verify relationship name
	if stmt.RelationshipName != "Books" {
		t.Errorf("Expected relationship name 'Books', got '%s'", stmt.RelationshipName)
	}
}

// TestSelectParser_WithRelationshipUnquoted tests WITH RELATIONSHIP with unquoted identifier
func TestSelectParser_WithRelationshipUnquoted(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" WITH RELATIONSHIP Books`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse WITH RELATIONSHIP with unquoted name: %v", err)
	}

	// Verify relationship name (should work with unquoted identifiers too)
	if stmt.RelationshipName != "Books" {
		t.Errorf("Expected relationship name 'Books', got '%s'", stmt.RelationshipName)
	}
}

// TestSelectParser_WithRelationshipOrderBy tests WITH RELATIONSHIP with ORDER BY
func TestSelectParser_WithRelationshipOrderBy(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" WITH RELATIONSHIP "Books" ORDER BY Name ASC`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse WITH RELATIONSHIP with ORDER BY: %v", err)
	}

	// Verify relationship name
	if stmt.RelationshipName != "Books" {
		t.Errorf("Expected relationship name 'Books', got '%s'", stmt.RelationshipName)
	}

	// Verify ORDER BY exists
	if len(stmt.OrderBy) != 1 {
		t.Fatalf("Expected 1 ORDER BY field, got %d", len(stmt.OrderBy))
	}
}

// TestSelectParser_WithRelationshipLimitOffset tests WITH RELATIONSHIP with LIMIT and OFFSET
func TestSelectParser_WithRelationshipLimitOffset(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" WITH RELATIONSHIP "Books" LIMIT 10 OFFSET 5`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse WITH RELATIONSHIP with LIMIT/OFFSET: %v", err)
	}

	// Verify relationship name
	if stmt.RelationshipName != "Books" {
		t.Errorf("Expected relationship name 'Books', got '%s'", stmt.RelationshipName)
	}

	// Verify LIMIT
	if stmt.Limit != 10 {
		t.Errorf("Expected LIMIT 10, got %d", stmt.Limit)
	}

	// Verify OFFSET
	if stmt.Offset != 5 {
		t.Errorf("Expected OFFSET 5, got %d", stmt.Offset)
	}
}

// TestSelectParser_WithRelationshipMultipleJoins tests WITH RELATIONSHIP with multiple JOINs
// Note: The relationship name applies to the hierarchical structure of the JOIN results
func TestSelectParser_WithRelationshipMultipleJoins(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" JOIN "Publishers" ON "Books"."PublisherID" == "Publishers"."ID" WITH RELATIONSHIP "Books"`
	logger := zap.NewExample().Sugar()
	stmt, err := ParseSelect(input, logger)
	if err != nil {
		t.Fatalf("Failed to parse WITH RELATIONSHIP with multiple JOINs: %v", err)
	}

	// Verify multiple JOINs
	if len(stmt.JoinClauses) != 2 {
		t.Fatalf("Expected 2 JOIN clauses, got %d", len(stmt.JoinClauses))
	}

	// Verify relationship name
	if stmt.RelationshipName != "Books" {
		t.Errorf("Expected relationship name 'Books', got '%s'", stmt.RelationshipName)
	}
}

// TestSelectParser_WithRelationshipErrorCases tests error handling for WITH RELATIONSHIP

// TestSelectParser_WithRelationshipMissingKeyword tests error when RELATIONSHIP keyword is missing
func TestSelectParser_WithRelationshipMissingKeyword(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" WITH "Books"`
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Fatal("Expected error for missing RELATIONSHIP keyword, got nil")
	}
}

// TestSelectParser_WithRelationshipMissingName tests error when relationship name is missing
func TestSelectParser_WithRelationshipMissingName(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" WITH RELATIONSHIP`
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Fatal("Expected error for missing relationship name, got nil")
	}
}

// TestSelectParser_WithRelationshipInvalidName tests error when relationship name is invalid
func TestSelectParser_WithRelationshipInvalidName(t *testing.T) {
	input := `SELECT * FROM "Authors" JOIN "Books" ON "Authors"."ID" == "Books"."AuthorID" WITH RELATIONSHIP 123`
	logger := zap.NewExample().Sugar()
	_, err := ParseSelect(input, logger)
	if err == nil {
		t.Fatal("Expected error for invalid relationship name, got nil")
	}
}
