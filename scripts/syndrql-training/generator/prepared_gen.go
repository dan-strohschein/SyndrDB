package generator

import (
	"fmt"

	"syndrql-training/ir"
)

func (g *Generator) genPrepare() (*ir.TrainingExample, error) {
	stmtName := PreparedStmtNames[g.rng.Intn(len(PreparedStmtNames))]
	b := g.pickBundle()
	field := g.pickField(b)

	nl := fmt.Sprintf("Prepare a statement called %s to query %s by %s", stmtName, b.Name, field.Name)

	stmt := ir.Statement{
		StatementType: "prepare",
		Prepare: &ir.PrepareStmt{
			StatementName: stmtName,
			Query: &ir.SelectStmt{
				Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
				Bundle: b.Name,
				Where:  ir.BinaryExpr(ir.IdentExpr(field.Name), "==", ir.ParamExpr(1)),
			},
		},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genExecute() (*ir.TrainingExample, error) {
	stmtName := PreparedStmtNames[g.rng.Intn(len(PreparedStmtNames))]
	nl := fmt.Sprintf("Execute prepared statement %s", stmtName)

	stmt := ir.Statement{
		StatementType: "execute",
		Execute:       &ir.ExecuteStmt{StatementName: stmtName},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genDeallocate() (*ir.TrainingExample, error) {
	stmtName := PreparedStmtNames[g.rng.Intn(len(PreparedStmtNames))]
	nl := fmt.Sprintf("Deallocate prepared statement %s", stmtName)

	stmt := ir.Statement{
		StatementType: "deallocate",
		Deallocate:    &ir.DeallocateStmt{StatementName: stmtName},
	}
	return g.makeExample(nl, stmt)
}
