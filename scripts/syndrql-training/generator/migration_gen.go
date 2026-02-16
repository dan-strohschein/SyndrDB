package generator

import (
	"fmt"

	"syndrql-training/ir"
)

func (g *Generator) genStartMigration() (*ir.TrainingExample, error) {
	descriptions := []string{
		"Add user preferences",
		"Update schema for v2",
		"Add indexes for performance",
		"Restructure order fields",
		"Add audit trail fields",
	}
	desc := descriptions[g.rng.Intn(len(descriptions))]

	nl := fmt.Sprintf("Start a migration: %s", desc)

	stmt := ir.Statement{
		StatementType:  "startMigration",
		StartMigration: &ir.StartMigrationStmt{Description: desc},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genShowMigrations() (*ir.TrainingExample, error) {
	db := DatabaseNames[g.rng.Intn(len(DatabaseNames))]
	nl := fmt.Sprintf("Show migrations for the %s database", db)

	stmt := ir.Statement{
		StatementType:  "showMigrations",
		ShowMigrations: &ir.ShowMigrationsStmt{Database: db},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genCheckpoint() (*ir.TrainingExample, error) {
	templates := []string{
		"Create a checkpoint",
		"Force a database checkpoint",
		"Take a checkpoint now",
		"Trigger a WAL checkpoint",
	}
	nl := templates[g.templateCounters["checkpoint"]%len(templates)]
	g.templateCounters["checkpoint"]++

	stmt := ir.Statement{
		StatementType: "checkpoint",
		Checkpoint:    &ir.CheckpointStmt{},
	}
	return g.makeExample(nl, stmt)
}
