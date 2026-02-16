package generator

import (
	"fmt"

	"syndrql-training/ir"
)

func (g *Generator) genCreateView() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	field := g.pickStringField(b)
	value := g.randomValue(field.Type)
	isMat := g.rng.Intn(2) == 0

	viewName := "Active" + b.Name
	viewType := "view"
	if isMat {
		viewType = "materialized view"
	}

	nl := fmt.Sprintf("Create a %s called %s showing %s where %s is %s",
		viewType, viewName, b.Name, field.Name, ValueNL(value))

	stmt := ir.Statement{
		StatementType: "createView",
		CreateView: &ir.CreateViewStmt{
			ViewName:       viewName,
			IsMaterialized: isMat,
			Query: &ir.SelectStmt{
				Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
				Bundle: b.Name,
				Where:  ir.BinaryExpr(ir.IdentExpr(field.Name), "==", ir.LiteralExpr(value)),
			},
		},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genDropView() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	viewName := "Active" + b.Name
	isMat := g.rng.Intn(2) == 0

	viewType := "view"
	if isMat {
		viewType = "materialized view"
	}
	nl := fmt.Sprintf("Drop the %s %s", viewName, viewType)

	stmt := ir.Statement{
		StatementType: "dropView",
		DropView: &ir.DropViewStmt{
			ViewName:       viewName,
			IsMaterialized: isMat,
		},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genRefreshView() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	viewName := b.Name + "Summary"

	nl := fmt.Sprintf("Refresh the materialized view %s", viewName)

	stmt := ir.Statement{
		StatementType: "refreshView",
		RefreshView:   &ir.RefreshViewStmt{ViewName: viewName},
	}
	return g.makeExample(nl, stmt)
}
