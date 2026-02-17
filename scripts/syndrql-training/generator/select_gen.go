package generator

import (
	"fmt"
	"strings"

	"syndrql-training/ir"
)

// 1. SELECT * (all fields)
func (g *Generator) genSelectStar() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	tmpl := g.pickTemplate(SelectAllTemplates, "select_star")
	nl := replaceTemplate(tmpl, map[string]string{"bundle": b.Name})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: b.Name,
		},
	}
	return g.makeExample(nl, stmt)
}

// 2. Named fields
func (g *Generator) genSelectFields() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	n := 2 + g.rng.Intn(3) // 2-4 fields
	fields := g.pickNFields(b, n)

	tmpl := g.pickTemplate(SelectFieldsTemplates, "select_fields")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"fields": fieldNamesStr(fields),
	})

	selectFields := make([]ir.SelectField, len(fields))
	for i, f := range fields {
		selectFields[i] = ir.SelectField{Expression: ir.IdentExpr(f.Name)}
	}

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: selectFields,
			Bundle: b.Name,
		},
	}
	return g.makeExample(nl, stmt)
}

// 3. Simple WHERE (equality, range, LIKE)
func (g *Generator) genSelectWhereSimple() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	field := g.pickField(b)

	var op string
	var value *ir.Value
	switch field.Type {
	case "STRING":
		choices := []string{"==", "!=", "LIKE"}
		op = choices[g.rng.Intn(len(choices))]
		if op == "LIKE" {
			val := StringValues[g.rng.Intn(len(StringValues))]
			value = ir.NewStringValue(val + "%")
		} else {
			value = g.randomValue(field.Type)
		}
	case "INT", "FLOAT":
		op = g.randomOp()
		value = g.randomValue(field.Type)
	case "BOOL":
		op = "=="
		value = g.randomValue(field.Type)
	default:
		op = "=="
		value = g.randomValue(field.Type)
	}

	tmpl := g.pickTemplate(SelectWhereTemplates, "select_where_simple")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle":    b.Name,
		"condition": ConditionNL(field.Name, op, value),
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: b.Name,
			Where:  ir.BinaryExpr(ir.IdentExpr(field.Name), op, ir.LiteralExpr(value)),
		},
	}
	return g.makeExample(nl, stmt)
}

// 4. Compound WHERE (AND/OR)
func (g *Generator) genSelectWhereCompound() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	fields := g.pickNFields(b, 2)
	f1, f2 := fields[0], fields[1]

	op1 := g.randomOp()
	op2 := g.randomOp()
	// Use == for string fields
	if f1.Type == "STRING" {
		op1 = g.randomEqualityOp()
	}
	if f2.Type == "STRING" {
		op2 = g.randomEqualityOp()
	}

	v1 := g.randomValue(f1.Type)
	v2 := g.randomValue(f2.Type)

	logicOp := "AND"
	logicNL := "and"
	if g.rng.Intn(3) == 0 {
		logicOp = "OR"
		logicNL = "or"
	}

	cond1NL := ConditionNL(f1.Name, op1, v1)
	cond2NL := ConditionNL(f2.Name, op2, v2)

	tmpl := g.pickTemplate(SelectWhereTemplates, "select_where_compound")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle":    b.Name,
		"condition": cond1NL + " " + logicNL + " " + cond2NL,
	})

	where := ir.BinaryExpr(
		ir.BinaryExpr(ir.IdentExpr(f1.Name), op1, ir.LiteralExpr(v1)),
		logicOp,
		ir.BinaryExpr(ir.IdentExpr(f2.Name), op2, ir.LiteralExpr(v2)),
	)

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: b.Name,
			Where:  where,
		},
	}
	return g.makeExample(nl, stmt)
}

// 5. ORDER BY (ASC/DESC)
func (g *Generator) genSelectOrderBy() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	field := g.pickField(b)
	dir := g.randomDirection()

	tmpl := g.pickTemplate(SelectOrderByTemplates, "select_orderby")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"field":  field.Name,
		"dir":    g.directionNL(dir),
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields:  []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle:  b.Name,
			OrderBy: []ir.OrderByClause{{Field: field.Name, Direction: dir}},
		},
	}
	return g.makeExample(nl, stmt)
}

// 6. LIMIT/OFFSET
// Fixed: uses separate template pools for with-offset vs without-offset.
// "Show the first 10 Customers" always means OFFSET 0.
// "Get 10 Customers starting from position 25" uses explicit OFFSET.
func (g *Generator) genSelectLimit() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	limits := []int{5, 10, 20, 25, 50, 100}
	limit := limits[g.rng.Intn(len(limits))]

	var offset int
	var nl string

	if g.rng.Intn(3) == 0 {
		// With explicit offset — use offset-aware templates
		offset = g.rng.Intn(50) + 1
		tmpl := g.pickTemplate(SelectLimitOffsetTemplates, "select_limit_offset")
		nl = replaceTemplate(tmpl, map[string]string{
			"bundle": b.Name,
			"limit":  fmt.Sprintf("%d", limit),
			"offset": fmt.Sprintf("%d", offset),
		})
	} else {
		// No offset — use "first N" style templates
		offset = 0
		tmpl := g.pickTemplate(SelectLimitNoOffsetTemplates, "select_limit")
		nl = replaceTemplate(tmpl, map[string]string{
			"bundle": b.Name,
			"limit":  fmt.Sprintf("%d", limit),
		})
	}

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: b.Name,
			Limit:  limit,
			Offset: offset,
		},
	}
	return g.makeExample(nl, stmt)
}

// 7. GROUP BY + aggregates
func (g *Generator) genSelectGroupBy() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	groupField := g.pickStringField(b)
	aggIdx := g.rng.Intn(len(AggFunctions))
	agg := AggFunctions[aggIdx]

	var aggExpr *ir.Expr
	var aggFieldName string
	if agg.SQL == "COUNT" {
		aggExpr = ir.FuncExpr("COUNT", false, ir.IdentExpr("*"))
		aggFieldName = "*"
	} else {
		numField := g.pickNumericField(b)
		aggExpr = ir.FuncExpr(agg.SQL, false, ir.IdentExpr(numField.Name))
		aggFieldName = numField.Name
	}

	tmpl := g.pickTemplate(SelectGroupByTemplates, "select_groupby")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle":   b.Name,
		"field":    groupField.Name,
		"agg":      agg.NL,
		"aggfield": aggFieldName,
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{
				{Expression: ir.IdentExpr(groupField.Name)},
				{Expression: aggExpr, Alias: agg.NL + "_val"},
			},
			Bundle:  b.Name,
			GroupBy: []string{groupField.Name},
		},
	}
	return g.makeExample(nl, stmt)
}

// 8. GROUP BY + HAVING
func (g *Generator) genSelectHaving() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	groupField := g.pickStringField(b)
	threshold := IntValues[g.rng.Intn(len(IntValues))]

	nl := fmt.Sprintf("Show %s grouped by %s where the count is greater than %d",
		b.Name, groupField.Name, threshold)

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{
				{Expression: ir.IdentExpr(groupField.Name)},
				{Expression: ir.FuncExpr("COUNT", false, ir.IdentExpr("*")), Alias: "cnt"},
			},
			Bundle:  b.Name,
			GroupBy: []string{groupField.Name},
			Having:  ir.BinaryExpr(ir.FuncExpr("COUNT", false, ir.IdentExpr("*")), ">", ir.LiteralExpr(ir.NewIntValue(threshold))),
		},
	}
	return g.makeExample(nl, stmt)
}

// 9. DISTINCT
func (g *Generator) genSelectDistinct() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	field := g.pickStringField(b)

	tmpl := g.pickTemplate(SelectDistinctTemplates, "select_distinct")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"field":  field.Name,
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields:   []ir.SelectField{{Expression: ir.IdentExpr(field.Name)}},
			Bundle:   b.Name,
			Distinct: true,
		},
	}
	return g.makeExample(nl, stmt)
}

// 10. JOIN (INNER/LEFT/RIGHT)
func (g *Generator) genSelectJoin() (*ir.TrainingExample, error) {
	pair := JoinablePairs[g.rng.Intn(len(JoinablePairs))]
	joinTypes := []string{"INNER", "LEFT", "RIGHT"}
	joinType := joinTypes[g.rng.Intn(len(joinTypes))]

	tmpl := g.pickTemplate(SelectJoinTemplates, "select_join")
	nl := replaceTemplate(tmpl, map[string]string{
		"left":  pair.Left,
		"right": pair.Right,
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: pair.Left,
			Joins: []ir.JoinClause{
				{
					JoinType: joinType,
					Bundle:   pair.Right,
					Conditions: []ir.JoinCondition{
						{
							LeftBundle:  pair.Left,
							LeftField:   pair.LeftField,
							Operator:    "==",
							RightBundle: pair.Right,
							RightField:  pair.RightField,
						},
					},
				},
			},
		},
	}
	return g.makeExample(nl, stmt)
}

// 11. Aggregate-only (COUNT, SUM, AVG, MIN, MAX)
func (g *Generator) genSelectAggregate() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	aggIdx := g.rng.Intn(len(AggFunctions))
	agg := AggFunctions[aggIdx]

	var aggExpr *ir.Expr
	var fieldName string

	if agg.SQL == "COUNT" {
		aggExpr = ir.FuncExpr("COUNT", false, ir.IdentExpr("*"))
		fieldName = "*"
	} else {
		numField := g.pickNumericField(b)
		aggExpr = ir.FuncExpr(agg.SQL, false, ir.IdentExpr(numField.Name))
		fieldName = numField.Name
	}

	tmpl := g.pickTemplate(SelectAggregateTemplates, "select_aggregate")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"agg":    agg.NL,
		"field":  fieldName,
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: aggExpr}},
			Bundle: b.Name,
		},
	}
	return g.makeExample(nl, stmt)
}

// 12. Function calls (F:UPPER, F:NOW, F:EXTRACT)
func (g *Generator) genSelectFunction() (*ir.TrainingExample, error) {
	b := g.pickBundle()

	// Pick a string function
	fnIdx := g.rng.Intn(4) // UPPER, LOWER, TRIM, LENGTH
	fn := BuiltInFunctions[fnIdx]

	field := g.pickStringField(b)

	tmpl := g.pickTemplate(SelectFunctionTemplates, "select_function")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"func":   fn.NL,
		"field":  field.Name,
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{
				{Expression: ir.FuncExpr(fn.Name, true, ir.IdentExpr(field.Name))},
			},
			Bundle: b.Name,
		},
	}
	return g.makeExample(nl, stmt)
}

// 13. IN / NOT IN
func (g *Generator) genSelectIn() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	field := g.pickStringField(b)

	n := 2 + g.rng.Intn(3) // 2-4 values
	values := make([]*ir.Expr, n)
	valStrs := make([]string, n)
	for i := 0; i < n; i++ {
		v := g.randomValue(field.Type)
		values[i] = ir.LiteralExpr(v)
		valStrs[i] = ValueNL(v)
	}

	isNotIn := g.rng.Intn(3) == 0
	var whereExpr *ir.Expr
	keyword := "in"
	if isNotIn {
		whereExpr = ir.NotInExpr(ir.IdentExpr(field.Name), values)
		keyword = "not in"
	} else {
		whereExpr = ir.InExpr(ir.IdentExpr(field.Name), values)
	}

	nl := fmt.Sprintf("Find %s where %s is %s (%s)", b.Name, field.Name, keyword, strings.Join(valStrs, ", "))

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: b.Name,
			Where:  whereExpr,
		},
	}
	return g.makeExample(nl, stmt)
}

// 14. Complex combinations (WHERE + ORDER BY + LIMIT)
func (g *Generator) genSelectComplex() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	whereField := g.pickField(b)
	orderField := g.pickField(b)
	dir := g.randomDirection()
	limit := 10 + g.rng.Intn(40)

	op := g.randomOp()
	if whereField.Type == "STRING" {
		op = g.randomEqualityOp()
	}
	value := g.randomValue(whereField.Type)

	nl := fmt.Sprintf("Get the top %d %s where %s, sorted by %s %s",
		limit, b.Name, ConditionNL(whereField.Name, op, value), orderField.Name, g.directionNL(dir))

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields:  []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle:  b.Name,
			Where:   ir.BinaryExpr(ir.IdentExpr(whereField.Name), op, ir.LiteralExpr(value)),
			OrderBy: []ir.OrderByClause{{Field: orderField.Name, Direction: dir}},
			Limit:   limit,
		},
	}
	return g.makeExample(nl, stmt)
}

// 15. FOR UPDATE
func (g *Generator) genSelectForUpdate() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	field := g.pickStringField(b)
	value := g.randomValue(field.Type)

	nl := fmt.Sprintf("Select %s for update where %s is %s", b.Name, field.Name, ValueNL(value))

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields:    []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle:    b.Name,
			Where:     ir.BinaryExpr(ir.IdentExpr(field.Name), "==", ir.LiteralExpr(value)),
			ForUpdate: true,
		},
	}
	return g.makeExample(nl, stmt)
}

// 16. Compound WHERE with 3 fields (A op B) logicOp1 ((C op D) logicOp2 (E op F))
// Teaches the model to build deeper recursive binary expression trees.
func (g *Generator) genSelectWhereThreeField() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	fields := g.pickNFields(b, 3)
	f1, f2, f3 := fields[0], fields[1], fields[2]

	// Pick operators appropriate to field types
	op1 := g.opForType(f1.Type)
	op2 := g.opForType(f2.Type)
	op3 := g.opForType(f3.Type)

	v1 := g.randomValue(f1.Type)
	v2 := g.randomValue(f2.Type)
	v3 := g.randomValue(f3.Type)

	// Pick logic operators — AND is 2x more likely than OR
	logicOp1 := "AND"
	logicNL1 := "and"
	if g.rng.Intn(3) == 0 {
		logicOp1 = "OR"
		logicNL1 = "or"
	}
	logicOp2 := "AND"
	logicNL2 := "and"
	if g.rng.Intn(3) == 0 {
		logicOp2 = "OR"
		logicNL2 = "or"
	}

	cond1NL := ConditionNL(f1.Name, op1, v1)
	cond2NL := ConditionNL(f2.Name, op2, v2)
	cond3NL := ConditionNL(f3.Name, op3, v3)

	tmpl := g.pickTemplate(SelectWhereThreeFieldTemplates, "select_where_three")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"cond1":  cond1NL,
		"logic1": logicNL1,
		"cond2":  cond2NL,
		"logic2": logicNL2,
		"cond3":  cond3NL,
	})

	// Build tree: (cond1 logicOp1 cond2) logicOp2 cond3
	where := ir.BinaryExpr(
		ir.BinaryExpr(
			ir.BinaryExpr(ir.IdentExpr(f1.Name), op1, ir.LiteralExpr(v1)),
			logicOp1,
			ir.BinaryExpr(ir.IdentExpr(f2.Name), op2, ir.LiteralExpr(v2)),
		),
		logicOp2,
		ir.BinaryExpr(ir.IdentExpr(f3.Name), op3, ir.LiteralExpr(v3)),
	)

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: b.Name,
			Where:  where,
		},
	}
	return g.makeExample(nl, stmt)
}

// 17. Named fields with WHERE clause
// Combines multi-field SELECT with filtering — reinforces both patterns.
func (g *Generator) genSelectFieldsWhere() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	n := 2 + g.rng.Intn(3) // 2-4 fields
	fields := g.pickNFields(b, n)

	// Pick a field for the WHERE (can be any field, not just selected ones)
	whereField := g.pickField(b)
	op := g.opForType(whereField.Type)
	value := g.randomValue(whereField.Type)

	tmpl := g.pickTemplate(SelectFieldsWhereTemplates, "select_fields_where")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle":    b.Name,
		"fields":    fieldNamesStr(fields),
		"condition": ConditionNL(whereField.Name, op, value),
	})

	selectFields := make([]ir.SelectField, len(fields))
	for i, f := range fields {
		selectFields[i] = ir.SelectField{Expression: ir.IdentExpr(f.Name)}
	}

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: selectFields,
			Bundle: b.Name,
			Where:  ir.BinaryExpr(ir.IdentExpr(whereField.Name), op, ir.LiteralExpr(value)),
		},
	}
	return g.makeExample(nl, stmt)
}

// opForType returns an appropriate comparison operator for a field type.
func (g *Generator) opForType(fieldType string) string {
	switch fieldType {
	case "STRING":
		return g.randomEqualityOp()
	case "INT", "FLOAT":
		return g.randomOp()
	case "BOOL":
		return "=="
	default:
		return "=="
	}
}

// 18. ORDER BY + WHERE — filtered and sorted results.
func (g *Generator) genSelectOrderByWhere() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	whereField := g.pickField(b)
	orderField := g.pickField(b)
	dir := g.randomDirection()

	op := g.opForType(whereField.Type)
	value := g.randomValue(whereField.Type)

	tmpl := g.pickTemplate(SelectOrderByWhereTemplates, "select_orderby_where")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle":    b.Name,
		"condition": ConditionNL(whereField.Name, op, value),
		"field":     orderField.Name,
		"dir":       g.directionNL(dir),
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields:  []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle:  b.Name,
			Where:   ir.BinaryExpr(ir.IdentExpr(whereField.Name), op, ir.LiteralExpr(value)),
			OrderBy: []ir.OrderByClause{{Field: orderField.Name, Direction: dir}},
		},
	}
	return g.makeExample(nl, stmt)
}

// 19. Multi-field ORDER BY — sort by two fields.
func (g *Generator) genSelectOrderByMulti() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	fields := g.pickNFields(b, 2)
	dir1 := g.randomDirection()
	dir2 := g.randomDirection()

	tmpl := g.pickTemplate(SelectOrderByMultiTemplates, "select_orderby_multi")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"field1": fields[0].Name,
		"dir1":   g.directionNL(dir1),
		"field2": fields[1].Name,
		"dir2":   g.directionNL(dir2),
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle: b.Name,
			OrderBy: []ir.OrderByClause{
				{Field: fields[0].Name, Direction: dir1},
				{Field: fields[1].Name, Direction: dir2},
			},
		},
	}
	return g.makeExample(nl, stmt)
}

// 20. ORDER BY + LIMIT — "top N" queries.
func (g *Generator) genSelectOrderByLimit() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	field := g.pickField(b)
	dir := g.randomDirection()
	limits := []int{5, 10, 20, 25, 50}
	limit := limits[g.rng.Intn(len(limits))]

	tmpl := g.pickTemplate(SelectOrderByLimitTemplates, "select_orderby_limit")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"field":  field.Name,
		"dir":    g.directionNL(dir),
		"limit":  fmt.Sprintf("%d", limit),
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields:  []ir.SelectField{{Expression: ir.IdentExpr("*")}},
			Bundle:  b.Name,
			OrderBy: []ir.OrderByClause{{Field: field.Name, Direction: dir}},
			Limit:   limit,
		},
	}
	return g.makeExample(nl, stmt)
}

// 21. GROUP BY + WHERE — filter before grouping.
func (g *Generator) genSelectGroupByWhere() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	groupField := g.pickStringField(b)
	whereField := g.pickField(b)
	aggIdx := g.rng.Intn(len(AggFunctions))
	agg := AggFunctions[aggIdx]

	op := g.opForType(whereField.Type)
	value := g.randomValue(whereField.Type)

	var aggExpr *ir.Expr
	var aggFieldName string
	if agg.SQL == "COUNT" {
		aggExpr = ir.FuncExpr("COUNT", false, ir.IdentExpr("*"))
		aggFieldName = "*"
	} else {
		numField := g.pickNumericField(b)
		aggExpr = ir.FuncExpr(agg.SQL, false, ir.IdentExpr(numField.Name))
		aggFieldName = numField.Name
	}

	tmpl := g.pickTemplate(SelectGroupByWhereTemplates, "select_groupby_where")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle":    b.Name,
		"field":     groupField.Name,
		"agg":       agg.NL,
		"aggfield":  aggFieldName,
		"condition": ConditionNL(whereField.Name, op, value),
	})

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{
				{Expression: ir.IdentExpr(groupField.Name)},
				{Expression: aggExpr, Alias: agg.NL + "_val"},
			},
			Bundle:  b.Name,
			Where:   ir.BinaryExpr(ir.IdentExpr(whereField.Name), op, ir.LiteralExpr(value)),
			GroupBy: []string{groupField.Name},
		},
	}
	return g.makeExample(nl, stmt)
}

// 22. HAVING with varied aggregate types (not just COUNT).
func (g *Generator) genSelectHavingVaried() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	groupField := g.pickStringField(b)
	numField := g.pickNumericField(b)

	// Pick an aggregate for HAVING — weighted toward SUM/AVG/COUNT
	havingAggs := []struct {
		sql string
		nl  string
	}{
		{"COUNT", "count"},
		{"SUM", "total"},
		{"AVG", "average"},
		{"MAX", "maximum"},
		{"MIN", "minimum"},
	}
	havingAgg := havingAggs[g.rng.Intn(len(havingAggs))]

	// Pick comparison for HAVING
	havingOps := []string{">", ">=", "<", "<="}
	havingOp := havingOps[g.rng.Intn(len(havingOps))]
	threshold := IntValues[g.rng.Intn(len(IntValues))]

	var havingExpr *ir.Expr
	var havingFieldName string
	if havingAgg.sql == "COUNT" {
		havingExpr = ir.FuncExpr("COUNT", false, ir.IdentExpr("*"))
		havingFieldName = "records"
	} else {
		havingExpr = ir.FuncExpr(havingAgg.sql, false, ir.IdentExpr(numField.Name))
		havingFieldName = numField.Name
	}

	opNL := ConditionNL(havingAgg.nl+" of "+havingFieldName, havingOp, ir.NewIntValue(threshold))
	nl := fmt.Sprintf("Show %s grouped by %s where the %s",
		b.Name, groupField.Name, opNL)

	// SELECT: groupField, aggregate
	var selectAggExpr *ir.Expr
	if havingAgg.sql == "COUNT" {
		selectAggExpr = ir.FuncExpr("COUNT", false, ir.IdentExpr("*"))
	} else {
		selectAggExpr = ir.FuncExpr(havingAgg.sql, false, ir.IdentExpr(numField.Name))
	}

	stmt := ir.Statement{
		StatementType: "select",
		Select: &ir.SelectStmt{
			Fields: []ir.SelectField{
				{Expression: ir.IdentExpr(groupField.Name)},
				{Expression: selectAggExpr, Alias: havingAgg.nl + "_val"},
			},
			Bundle:  b.Name,
			GroupBy: []string{groupField.Name},
			Having: ir.BinaryExpr(
				havingExpr,
				havingOp,
				ir.LiteralExpr(ir.NewIntValue(threshold)),
			),
		},
	}
	return g.makeExample(nl, stmt)
}
