package generator

import (
	"fmt"
	"strings"

	"syndrql-training/ir"
)

// INSERT generator
func (g *Generator) genInsert() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	n := 2 + g.rng.Intn(4) // 2-5 fields
	fields := g.pickNFields(b, n)

	fieldValues := make([]ir.FieldValue, len(fields))
	fieldDescs := make([]string, len(fields))
	for i, f := range fields {
		v := g.randomValue(f.Type)
		fieldValues[i] = ir.FieldValue{Name: f.Name, Value: v}
		fieldDescs[i] = f.Name + "=" + ValueNL(v)
	}

	tmpl := g.pickTemplate(InsertTemplates, "insert")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"fields": strings.Join(fieldDescs, ", "),
	})

	stmt := ir.Statement{
		StatementType: "insert",
		Insert: &ir.InsertStmt{
			Bundle: b.Name,
			Fields: fieldValues,
		},
	}
	return g.makeExample(nl, stmt)
}

// UPDATE generator
func (g *Generator) genUpdate() (*ir.TrainingExample, error) {
	b := g.pickBundle()

	// Fields to update (1-3)
	n := 1 + g.rng.Intn(2)
	updateFields := g.pickNFields(b, n)
	fieldValues := make([]ir.FieldValue, len(updateFields))
	fieldDescs := make([]string, len(updateFields))
	for i, f := range updateFields {
		v := g.randomValue(f.Type)
		fieldValues[i] = ir.FieldValue{Name: f.Name, Value: v}
		fieldDescs[i] = f.Name + "=" + ValueNL(v)
	}

	// WHERE condition (optional for confirmed)
	hasWhere := g.rng.Intn(3) != 0
	var where *ir.Expr
	condNL := ""
	confirmed := false

	if hasWhere {
		whereField := g.pickField(b)
		op := g.randomEqualityOp()
		if whereField.Type != "STRING" {
			op = g.randomOp()
		}
		val := g.randomValue(whereField.Type)
		where = ir.BinaryExpr(ir.IdentExpr(whereField.Name), op, ir.LiteralExpr(val))
		condNL = ConditionNL(whereField.Name, op, val)
	} else {
		confirmed = true
	}

	tmpl := g.pickTemplate(UpdateTemplates, "update")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle":    b.Name,
		"fields":    strings.Join(fieldDescs, ", "),
		"condition": condNL,
	})
	if !hasWhere {
		nl = fmt.Sprintf("Update all %s, set %s (confirmed)", b.Name, strings.Join(fieldDescs, ", "))
	}

	stmt := ir.Statement{
		StatementType: "update",
		Update: &ir.UpdateStmt{
			Bundle:    b.Name,
			Fields:    fieldValues,
			Where:     where,
			Confirmed: confirmed,
		},
	}
	return g.makeExample(nl, stmt)
}

// DELETE generator
func (g *Generator) genDelete() (*ir.TrainingExample, error) {
	b := g.pickBundle()

	hasWhere := g.rng.Intn(4) != 0
	var where *ir.Expr
	condNL := ""
	confirmed := false

	if hasWhere {
		whereField := g.pickField(b)
		op := g.randomEqualityOp()
		if whereField.Type != "STRING" {
			op = g.randomOp()
		}
		val := g.randomValue(whereField.Type)
		where = ir.BinaryExpr(ir.IdentExpr(whereField.Name), op, ir.LiteralExpr(val))
		condNL = ConditionNL(whereField.Name, op, val)
	} else {
		confirmed = true
	}

	tmpl := g.pickTemplate(DeleteTemplates, "delete")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle":    b.Name,
		"condition": condNL,
	})
	if !hasWhere {
		nl = fmt.Sprintf("Delete all records from %s (confirmed)", b.Name)
	}

	stmt := ir.Statement{
		StatementType: "delete",
		Delete: &ir.DeleteStmt{
			Bundle:    b.Name,
			Where:     where,
			Confirmed: confirmed,
		},
	}
	return g.makeExample(nl, stmt)
}
