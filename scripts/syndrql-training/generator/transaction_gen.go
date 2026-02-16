package generator

import (
	"fmt"

	"syndrql-training/ir"
)

func (g *Generator) genBeginTransaction() (*ir.TrainingExample, error) {
	templates := []string{
		"Start a new transaction",
		"Begin a database transaction",
		"Open a new transaction",
		"Start a transaction for the following operations",
	}
	nl := templates[g.templateCounters["begin_tx"]%len(templates)]
	g.templateCounters["begin_tx"]++

	stmt := ir.Statement{
		StatementType:    "beginTransaction",
		BeginTransaction: &ir.BeginTransactionStmt{},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genCommit() (*ir.TrainingExample, error) {
	templates := []string{
		"Commit the current transaction",
		"Save the current changes",
		"Finalize the transaction",
		"Commit all pending changes",
	}
	nl := templates[g.templateCounters["commit"]%len(templates)]
	g.templateCounters["commit"]++

	stmt := ir.Statement{
		StatementType: "commit",
		Commit:        &ir.CommitStmt{},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genRollback() (*ir.TrainingExample, error) {
	templates := []string{
		"Roll back the current transaction",
		"Undo the current changes",
		"Abort the transaction",
		"Rollback all pending changes",
	}
	nl := templates[g.templateCounters["rollback"]%len(templates)]
	g.templateCounters["rollback"]++

	stmt := ir.Statement{
		StatementType: "rollback",
		Rollback:      &ir.RollbackStmt{},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genSavepoint() (*ir.TrainingExample, error) {
	name := SavepointNames[g.rng.Intn(len(SavepointNames))]
	nl := fmt.Sprintf("Create a savepoint named %s", name)

	stmt := ir.Statement{
		StatementType: "savepoint",
		Savepoint:     &ir.SavepointStmt{Name: name},
	}
	return g.makeExample(nl, stmt)
}

func (g *Generator) genRollbackToSavepoint() (*ir.TrainingExample, error) {
	name := SavepointNames[g.rng.Intn(len(SavepointNames))]
	nl := fmt.Sprintf("Roll back to savepoint %s", name)

	stmt := ir.Statement{
		StatementType:       "rollbackToSavepoint",
		RollbackToSavepoint: &ir.RollbackToSavepointStmt{Name: name},
	}
	return g.makeExample(nl, stmt)
}
