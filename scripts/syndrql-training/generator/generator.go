package generator

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"

	"syndrql-training/compiler"
	"syndrql-training/ir"
)

// Generator produces training data examples.
type Generator struct {
	rng   *rand.Rand
	count int

	// Round-robin template counters for variety
	templateCounters map[string]int
}

// New creates a new Generator with the given seed.
func New(seed int64, count int) *Generator {
	return &Generator{
		rng:              rand.New(rand.NewSource(seed)),
		count:            count,
		templateCounters: make(map[string]int),
	}
}

// Generate produces all training examples and writes them to the output path.
func (g *Generator) Generate(outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)

	// Distribution (per 500)
	distribution := []struct {
		category string
		count    int
		genFunc  func() (*ir.TrainingExample, error)
	}{
		// SELECT variants - 260 total (52%)
		{"select_star", 20, g.genSelectStar},
		{"select_fields", 20, g.genSelectFields},
		{"select_where_simple", 30, g.genSelectWhereSimple},
		{"select_where_compound", 20, g.genSelectWhereCompound},
		{"select_orderby", 20, g.genSelectOrderBy},
		{"select_limit", 15, g.genSelectLimit},
		{"select_groupby", 25, g.genSelectGroupBy},
		{"select_having", 15, g.genSelectHaving},
		{"select_distinct", 15, g.genSelectDistinct},
		{"select_join", 20, g.genSelectJoin},
		{"select_aggregate", 20, g.genSelectAggregate},
		{"select_function", 15, g.genSelectFunction},
		{"select_in", 10, g.genSelectIn},
		{"select_complex", 10, g.genSelectComplex},
		{"select_forupdate", 5, g.genSelectForUpdate},

		// DML
		{"insert", 40, g.genInsert},
		{"update", 35, g.genUpdate},
		{"delete", 25, g.genDelete},

		// DDL - Bundle
		{"create_bundle", 15, g.genCreateBundle},
		{"drop_bundle", 8, g.genDropBundle},
		{"update_bundle", 7, g.genUpdateBundle},

		// DDL - Index
		{"create_index", 15, g.genCreateIndex},

		// Views
		{"create_view", 8, g.genCreateView},
		{"drop_view", 4, g.genDropView},
		{"refresh_view", 3, g.genRefreshView},

		// Transactions
		{"begin_tx", 4, g.genBeginTransaction},
		{"commit", 4, g.genCommit},
		{"rollback", 3, g.genRollback},
		{"savepoint", 2, g.genSavepoint},
		{"rollback_sp", 2, g.genRollbackToSavepoint},

		// Cursors
		{"declare_cursor", 4, g.genDeclareCursor},
		{"fetch_cursor", 4, g.genFetchCursor},
		{"close_cursor", 2, g.genCloseCursor},

		// Prepared
		{"prepare", 4, g.genPrepare},
		{"execute", 3, g.genExecute},
		{"deallocate", 3, g.genDeallocate},

		// RBAC
		{"create_user", 3, g.genCreateUser},
		{"delete_user", 2, g.genDeleteUser},
		{"grant_perm", 3, g.genGrantPermission},
		{"grant_role", 2, g.genGrantRole},
		{"revoke_perm", 3, g.genRevokePermission},
		{"revoke_role", 2, g.genRevokeRole},

		// Utility
		{"show_databases", 2, g.genShowDatabases},
		{"show_bundles", 2, g.genShowBundles},
		{"show_bundle", 2, g.genShowBundle},
		{"show_users", 2, g.genShowUsers},
		{"show_sessions", 2, g.genShowSessions},
		{"show_views", 2, g.genShowViews},
		{"explain", 3, g.genExplainSelect},

		// Database
		{"use_db", 3, g.genUseDatabase},
		{"create_db", 3, g.genCreateDatabase},
		{"drop_db", 2, g.genDropDatabase},
		{"rename_db", 2, g.genRenameDatabase},

		// Migration
		{"start_migration", 2, g.genStartMigration},
		{"show_migrations", 3, g.genShowMigrations},

		// Ops
		{"checkpoint", 2, g.genCheckpoint},
	}

	// Scale distribution to requested count
	totalBase := 0
	for _, d := range distribution {
		totalBase += d.count
	}

	generated := 0
	for _, d := range distribution {
		scaled := (d.count * g.count) / totalBase
		if scaled == 0 && d.count > 0 {
			scaled = 1
		}
		for i := 0; i < scaled && generated < g.count; i++ {
			example, err := d.genFunc()
			if err != nil {
				return fmt.Errorf("generate %s #%d: %w", d.category, i, err)
			}

			if err := enc.Encode(example); err != nil {
				return fmt.Errorf("encode: %w", err)
			}
			generated++
		}
	}

	// Fill remaining with random SELECT variants
	for generated < g.count {
		example, err := g.genSelectStar()
		if err != nil {
			return fmt.Errorf("generate filler: %w", err)
		}
		if err := enc.Encode(example); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
		generated++
	}

	return nil
}

// makeExample creates a training example from NL, IR statement, and validates compilation.
func (g *Generator) makeExample(nl string, stmt ir.Statement) (*ir.TrainingExample, error) {
	syndrql, err := compiler.Compile(&stmt)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}

	return &ir.TrainingExample{
		NL: nl,
		IR: ir.IREnvelope{
			Statements:  []ir.Statement{stmt},
			Explanation: nl,
			Confidence:  0.95,
		},
		SyndrQL: syndrql,
	}, nil
}

// pickBundle returns a random bundle schema.
func (g *Generator) pickBundle() BundleSchema {
	return Bundles[g.rng.Intn(len(Bundles))]
}

// pickField returns a random field from a bundle.
func (g *Generator) pickField(b BundleSchema) FieldSchema {
	return b.Fields[g.rng.Intn(len(b.Fields))]
}

// pickStringField returns a random STRING field from a bundle.
func (g *Generator) pickStringField(b BundleSchema) FieldSchema {
	stringFields := g.fieldsOfType(b, "STRING")
	if len(stringFields) == 0 {
		return b.Fields[0]
	}
	return stringFields[g.rng.Intn(len(stringFields))]
}

// pickNumericField returns a random INT or FLOAT field from a bundle.
func (g *Generator) pickNumericField(b BundleSchema) FieldSchema {
	numFields := g.fieldsOfType(b, "INT", "FLOAT")
	if len(numFields) == 0 {
		return b.Fields[0]
	}
	return numFields[g.rng.Intn(len(numFields))]
}

func (g *Generator) fieldsOfType(b BundleSchema, types ...string) []FieldSchema {
	var result []FieldSchema
	typeSet := make(map[string]bool)
	for _, t := range types {
		typeSet[t] = true
	}
	for _, f := range b.Fields {
		if typeSet[f.Type] {
			result = append(result, f)
		}
	}
	return result
}

// pickNFields returns n random distinct fields from a bundle.
func (g *Generator) pickNFields(b BundleSchema, n int) []FieldSchema {
	if n > len(b.Fields) {
		n = len(b.Fields)
	}
	perm := g.rng.Perm(len(b.Fields))
	result := make([]FieldSchema, n)
	for i := 0; i < n; i++ {
		result[i] = b.Fields[perm[i]]
	}
	return result
}

// pickTemplate returns a template from the pool using round-robin.
func (g *Generator) pickTemplate(pool []string, category string) string {
	idx := g.templateCounters[category] % len(pool)
	g.templateCounters[category]++
	return pool[idx]
}

// randomOp returns a random comparison operator.
func (g *Generator) randomOp() string {
	ops := []string{"==", "!=", ">", ">=", "<", "<="}
	return ops[g.rng.Intn(len(ops))]
}

// randomEqualityOp returns == or !=.
func (g *Generator) randomEqualityOp() string {
	if g.rng.Intn(2) == 0 {
		return "=="
	}
	return "!="
}

// randomValue creates a random value appropriate for a field type.
func (g *Generator) randomValue(fieldType string) *ir.Value {
	return RandomValue(fieldType, g.rng)
}

// replaceTemplate fills in template placeholders.
func replaceTemplate(tmpl string, replacements map[string]string) string {
	result := tmpl
	for k, v := range replacements {
		result = strings.ReplaceAll(result, "{"+k+"}", v)
	}
	return result
}

// fieldNamesStr joins field names with commas for NL.
func fieldNamesStr(fields []FieldSchema) string {
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return strings.Join(names, ", ")
}

// randomDirection returns ASC or DESC.
func (g *Generator) randomDirection() string {
	if g.rng.Intn(2) == 0 {
		return "ASC"
	}
	return "DESC"
}

func (g *Generator) directionNL(dir string) string {
	if dir == "ASC" {
		return "ascending"
	}
	return "descending"
}
