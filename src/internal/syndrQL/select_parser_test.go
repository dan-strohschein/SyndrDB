package syndrQL

import (
	"testing"
)

// Test basic SELECT patterns

func TestSelectParser_SelectAll(t *testing.T) {
	input := "SELECT * FROM Authors"

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if !stmt.Distinct {
		t.Error("Expected DISTINCT flag to be true")
	}
}

func TestSelectParser_SelectTop(t *testing.T) {
	input := "SELECT TOP 10 * FROM Authors"

	stmt, err := ParseSelect(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.TopN != 10 {
		t.Errorf("Expected TopN = 10, got %d", stmt.TopN)
	}
}

func TestSelectParser_SelectTopWithFields(t *testing.T) {
	input := "SELECT TOP 5 AuthorName, Age FROM Authors"

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Limit != 10 {
		t.Errorf("Expected LIMIT 10, got %d", stmt.Limit)
	}
}

func TestSelectParser_LimitOffset(t *testing.T) {
	input := "SELECT * FROM Authors LIMIT 10 OFFSET 20"

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	stmt, err := ParseSelect(input)
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

	_, err := ParseSelect(input)
	if err == nil {
		t.Error("Expected error for missing FROM clause")
	}
}

func TestSelectParser_MissingBundleName(t *testing.T) {
	input := "SELECT * FROM"

	_, err := ParseSelect(input)
	if err == nil {
		t.Error("Expected error for missing bundle name")
	}
}

func TestSelectParser_EmptyFieldList(t *testing.T) {
	input := "SELECT FROM Authors"

	_, err := ParseSelect(input)
	if err == nil {
		t.Error("Expected error for empty field list")
	}
}

func TestSelectParser_InvalidWhereClause(t *testing.T) {
	input := "SELECT * FROM Authors WHERE"

	_, err := ParseSelect(input)
	if err == nil {
		t.Error("Expected error for incomplete WHERE clause")
	}
}

// Benchmark tests

func BenchmarkSelectParser_SimpleSelectAll(b *testing.B) {
	input := "SELECT * FROM Authors"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseSelect(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectParser_SelectWithWhere(b *testing.B) {
	input := "SELECT * FROM Authors WHERE Age > 25"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseSelect(input)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseSelect(input)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseSelect(input)
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
