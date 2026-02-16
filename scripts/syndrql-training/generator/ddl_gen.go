package generator

import (
	"fmt"
	"strings"

	"syndrql-training/ir"
)

// CREATE BUNDLE generator
func (g *Generator) genCreateBundle() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	n := 3 + g.rng.Intn(4) // 3-6 fields
	fields := g.pickNFields(b, n)

	fieldDefs := make([]ir.BundleFieldDef, len(fields))
	fieldDescs := make([]string, len(fields))
	for i, f := range fields {
		required := g.rng.Intn(2) == 0
		unique := g.rng.Intn(5) == 0
		fieldDefs[i] = ir.BundleFieldDef{
			Name:     f.Name,
			Type:     f.Type,
			Required: required,
			Unique:   unique,
		}
		fieldDescs[i] = fmt.Sprintf("%s (%s)", f.Name, f.Type)
	}

	tmpl := g.pickTemplate(CreateBundleTemplates, "create_bundle")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"fields": strings.Join(fieldDescs, ", "),
	})

	stmt := ir.Statement{
		StatementType: "createBundle",
		CreateBundle: &ir.CreateBundleStmt{
			Bundle: b.Name,
			Fields: fieldDefs,
		},
	}
	return g.makeExample(nl, stmt)
}

// DROP BUNDLE generator
func (g *Generator) genDropBundle() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	force := g.rng.Intn(3) == 0

	nl := fmt.Sprintf("Drop the %s bundle", b.Name)
	if force {
		nl += " (force)"
	}

	stmt := ir.Statement{
		StatementType: "dropBundle",
		DropBundle: &ir.DropBundleStmt{
			Bundle: b.Name,
			Force:  force,
		},
	}
	return g.makeExample(nl, stmt)
}

// UPDATE BUNDLE generator (rename or add/remove field)
func (g *Generator) genUpdateBundle() (*ir.TrainingExample, error) {
	b := g.pickBundle()

	if g.rng.Intn(2) == 0 {
		// Rename
		newName := b.Name + "V2"
		nl := fmt.Sprintf("Rename the %s bundle to %s", b.Name, newName)
		stmt := ir.Statement{
			StatementType: "updateBundle",
			UpdateBundle: &ir.UpdateBundleStmt{
				Bundle:        b.Name,
				NewBundleName: newName,
			},
		}
		return g.makeExample(nl, stmt)
	}

	// Add or remove field
	if g.rng.Intn(2) == 0 {
		// Add field
		nl := fmt.Sprintf("Add a new field 'notes' (STRING) to %s", b.Name)
		stmt := ir.Statement{
			StatementType: "updateBundle",
			UpdateBundle: &ir.UpdateBundleStmt{
				Bundle: b.Name,
				Changes: []ir.FieldChange{
					{
						Action: "ADD",
						Field:  "notes",
						FieldDef: &ir.BundleFieldDef{
							Name:     "notes",
							Type:     "STRING",
							Required: false,
							Unique:   false,
						},
					},
				},
			},
		}
		return g.makeExample(nl, stmt)
	}

	// Remove field
	field := g.pickField(b)
	nl := fmt.Sprintf("Remove the %s field from %s", field.Name, b.Name)
	stmt := ir.Statement{
		StatementType: "updateBundle",
		UpdateBundle: &ir.UpdateBundleStmt{
			Bundle: b.Name,
			Changes: []ir.FieldChange{
				{Action: "REMOVE", Field: field.Name},
			},
		},
	}
	return g.makeExample(nl, stmt)
}

// CREATE INDEX generator
func (g *Generator) genCreateIndex() (*ir.TrainingExample, error) {
	b := g.pickBundle()
	field := g.pickField(b)

	indexTypes := []string{"B-INDEX", "HASH INDEX", "BRIN INDEX"}
	indexType := indexTypes[g.rng.Intn(len(indexTypes))]
	indexName := fmt.Sprintf("idx_%s_%s", strings.ToLower(b.Name), field.Name)

	tmpl := g.pickTemplate(CreateIndexTemplates, "create_index")
	nl := replaceTemplate(tmpl, map[string]string{
		"bundle": b.Name,
		"field":  field.Name,
		"type":   indexType,
	})

	stmt := ir.Statement{
		StatementType: "createIndex",
		CreateIndex: &ir.CreateIndexStmt{
			IndexType: indexType,
			IndexName: indexName,
			Bundle:    b.Name,
			Fields: []ir.IndexFieldDef{
				{Name: field.Name, Required: true, Unique: false},
			},
		},
	}
	return g.makeExample(nl, stmt)
}
