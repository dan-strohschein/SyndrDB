package generator

import (
	"fmt"

	"syndrql-training/ir"
)

func (g *Generator) genUseDatabase() (*ir.TrainingExample, error) {
	db := DatabaseNames[g.rng.Intn(len(DatabaseNames))]
	templates := []string{
		fmt.Sprintf("Switch to the %s database", db),
		fmt.Sprintf("Use the %s database", db),
		fmt.Sprintf("Connect to %s", db),
		fmt.Sprintf("Change to database %s", db),
	}
	nl := templates[g.templateCounters["use_db"]%len(templates)]
	g.templateCounters["use_db"]++

	stmt := ir.Statement{
		StatementType: "useDatabase",
		UseDatabase:   &ir.UseDatabaseStmt{Database: db},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genCreateDatabase() (*ir.TrainingExample, error) {
	db := DatabaseNames[g.rng.Intn(len(DatabaseNames))]
	nl := fmt.Sprintf("Create a new database called %s", db)

	stmt := ir.Statement{
		StatementType:  "createDatabase",
		CreateDatabase: &ir.CreateDatabaseStmt{Database: db},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genDropDatabase() (*ir.TrainingExample, error) {
	db := DatabaseNames[g.rng.Intn(len(DatabaseNames))]
	force := g.rng.Intn(3) == 0

	nl := fmt.Sprintf("Drop the %s database", db)
	if force {
		nl += " (force)"
	}

	stmt := ir.Statement{
		StatementType: "dropDatabase",
		DropDatabase:  &ir.DropDatabaseStmt{Database: db, Force: force},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genRenameDatabase() (*ir.TrainingExample, error) {
	idx := g.rng.Intn(len(DatabaseNames))
	oldName := DatabaseNames[idx]
	newName := oldName + "_v2"

	nl := fmt.Sprintf("Rename database %s to %s", oldName, newName)

	stmt := ir.Statement{
		StatementType:  "renameDatabase",
		RenameDatabase: &ir.RenameDatabaseStmt{OldName: oldName, NewName: newName},
	}
	return g.makeExample(nl, stmt)
}
