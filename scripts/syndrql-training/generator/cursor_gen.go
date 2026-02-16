package generator

import (
	"fmt"

	"syndrql-training/ir"
)

func (g *Generator) genDeclareCursor() (*ir.TrainingExample, error) {
	cursorName := CursorNames[g.rng.Intn(len(CursorNames))]
	b := g.pickBundle()
	orderField := g.pickField(b)
	dir := g.randomDirection()

	nl := fmt.Sprintf("Declare a cursor called %s for all %s ordered by %s %s",
		cursorName, b.Name, orderField.Name, g.directionNL(dir))

	stmt := ir.Statement{
		StatementType: "declareCursor",
		DeclareCursor: &ir.DeclareCursorStmt{
			CursorName: cursorName,
			Query: &ir.SelectStmt{
				Fields:  []ir.SelectField{{Expression: ir.IdentExpr("*")}},
				Bundle:  b.Name,
				OrderBy: []ir.OrderByClause{{Field: orderField.Name, Direction: dir}},
			},
		},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genFetchCursor() (*ir.TrainingExample, error) {
	cursorName := CursorNames[g.rng.Intn(len(CursorNames))]

	choice := g.rng.Intn(3)
	switch choice {
	case 0:
		// FETCH ALL
		nl := fmt.Sprintf("Fetch all remaining rows from cursor %s", cursorName)
		stmt := ir.Statement{
			StatementType: "fetchCursor",
			FetchCursor:   &ir.FetchCursorStmt{CursorName: cursorName, FetchAll: true},
		}
		return g.makeExample(nl, stmt)
	case 1:
		// FETCH NEXT
		nl := fmt.Sprintf("Get the next row from cursor %s", cursorName)
		stmt := ir.Statement{
			StatementType: "fetchCursor",
			FetchCursor:   &ir.FetchCursorStmt{CursorName: cursorName, Count: 1},
		}
		return g.makeExample(nl, stmt)
	default:
		// FETCH N
		counts := []int{5, 10, 20, 50, 100}
		count := counts[g.rng.Intn(len(counts))]
		nl := fmt.Sprintf("Fetch %d rows from cursor %s", count, cursorName)
		stmt := ir.Statement{
			StatementType: "fetchCursor",
			FetchCursor:   &ir.FetchCursorStmt{CursorName: cursorName, Count: count},
		}
		return g.makeExample(nl, stmt)
	}
}

func (g *Generator) genCloseCursor() (*ir.TrainingExample, error) {
	cursorName := CursorNames[g.rng.Intn(len(CursorNames))]
	nl := fmt.Sprintf("Close cursor %s", cursorName)

	stmt := ir.Statement{
		StatementType: "closeCursor",
		CloseCursor:   &ir.CloseCursorStmt{CursorName: cursorName},
	}
	return g.makeExample(nl, stmt)
}
