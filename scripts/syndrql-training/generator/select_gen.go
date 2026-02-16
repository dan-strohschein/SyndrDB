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
func (g *Generator) genSelectLimit() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	limits := []int{5, 10, 20, 25, 50, 100}
	limit := limits[g.rng.Intn(len(limits))]

	offset := 0
	if g.rng.Intn(3) == 0 {
		offset = g.rng.Intn(50) + 1
	}

	tmpl := g.pickTemplate(SelectLimitTemplates, "select_limit")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"limit":  fmt.Sprintf("%d", limit),
		"offset": fmt.Sprintf("%d", offset),
	})

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
