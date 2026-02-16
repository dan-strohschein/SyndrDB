package generator

import (
	"fmt"

	"syndrql-training/ir"
)

func (g *Generator) genShowDatabases() (*ir.TrainingExample, error) {
	templates := []string{"Show all databases", "List available databases", "What databases exist?", "Show me the databases"}
	nl := templates[g.templateCounters["show_db"]%len(templates)]
	g.templateCounters["show_db"]++

	stmt := ir.Statement{
		StatementType: "showDatabases",
		ShowDatabases: &ir.ShowDatabasesStmt{},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genShowBundles() (*ir.TrainingExample, error) {
	templates := []string{"Show all bundles", "List all collections", "What bundles are available?", "Show me all bundles"}
	nl := templates[g.templateCounters["show_bundles"]%len(templates)]
	g.templateCounters["show_bundles"]++

	stmt := ir.Statement{
		StatementType: "showBundles",
		ShowBundles:   &ir.ShowBundlesStmt{},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genShowBundle() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	nl := fmt.Sprintf("Show details about the %s bundle", b.Name)

	stmt := ir.Statement{
		StatementType: "showBundle",
		ShowBundle:    &ir.ShowBundleStmt{Bundle: b.Name},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genShowUsers() (*ir.TrainingExample, error) {
	templates := []string{"Show all users", "List all users", "Who has access to the database?", "Show me the users"}
	nl := templates[g.templateCounters["show_users"]%len(templates)]
	g.templateCounters["show_users"]++

	stmt := ir.Statement{
		StatementType: "showUsers",
		ShowUsers:     &ir.ShowUsersStmt{},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genShowSessions() (*ir.TrainingExample, error) {
	templates := []string{"Show active sessions", "List all sessions", "What sessions are running?", "Show current sessions"}
	nl := templates[g.templateCounters["show_sessions"]%len(templates)]
	g.templateCounters["show_sessions"]++

	stmt := ir.Statement{
		StatementType: "showSessions",
		ShowSessions:  &ir.ShowSessionsStmt{},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genShowViews() (*ir.TrainingExample, error) {
	if g.rng.Intn(2) == 0 {
		nl := "Show all views"
		stmt := ir.Statement{
			StatementType: "showViews",
			ShowViews:     &ir.ShowViewsStmt{},
		}
		return g.makeExample(nl, stmt)
	}
	db := DatabaseNames[g.rng.Intn(len(DatabaseNames))]
	nl := fmt.Sprintf("Show views in the %s database", db)
	stmt := ir.Statement{
		StatementType: "showViews",
		ShowViews:     &ir.ShowViewsStmt{Database: db},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genExplainSelect() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	field := g.pickField(b)
	op := g.randomOp()
	if field.Type == "STRING" {
		op = g.randomEqualityOp()
	}
	value := g.randomValue(field.Type)

	nl := fmt.Sprintf("Explain the query plan for selecting from %s where %s",
		b.Name, ConditionNL(field.Name, op, value))

	stmt := ir.Statement{
		StatementType: "explainSelect",
		ExplainSelect: &ir.ExplainSelectStmt{
			Query: &ir.SelectStmt{
				Fields: []ir.SelectField{{Expression: ir.IdentExpr("*")}},
				Bundle: b.Name,
				Where:  ir.BinaryExpr(ir.IdentExpr(field.Name), op, ir.LiteralExpr(value)),
			},
		},
	}
	return g.makeExample(nl, stmt)
}
