package pgimport

import (
	"syndrdb/src/internal/domain/models"
)

// ConvertTable converts a parsed CREATE TABLE statement into a SyndrDB BundleCommand.
func ConvertTable(stmt *ParsedStatement) (*models.BundleCommand, []ImportWarning) {
	cmd := &models.BundleCommand{
		CommandType: "CREATE",
		BundleName:  stmt.TableName,
	}

	var warnings []ImportWarning

	// Track PK and UNIQUE columns from table-level constraints
	pkCols := make(map[string]bool)
	uqCols := make(map[string]bool)
	for _, c := range stmt.Constraints {
		switch c.Type {
		case ConstraintPrimaryKey:
			for _, col := range c.Columns {
				pkCols[col] = true
			}
		case ConstraintUnique:
			for _, col := range c.Columns {
				uqCols[col] = true
			}
		case ConstraintCheck:
			warnings = append(warnings, ImportWarning{
				Phase:   "schema",
				Object:  stmt.TableName,
				Message: "CHECK constraint skipped: " + c.CheckExpr,
			})
		}
	}

	for _, col := range stmt.Columns {
		syndrType, typeWarning := mapPGType(col.Type)
		if typeWarning != "" {
			warnings = append(warnings, ImportWarning{
				Phase:   "schema",
				Object:  stmt.TableName + "." + col.Name,
				Message: typeWarning,
			})
		}

		fd := models.FieldDefinition{
			Name:       col.Name,
			Type:       syndrType,
			IsRequired: col.NotNull || pkCols[col.Name],
			IsUnique:   col.Unique || col.PrimaryKey || pkCols[col.Name] || uqCols[col.Name],
		}

		// Map DEFAULT value
		if col.Default != "" {
			defVal, defWarning := mapPGDefault(col.Default)
			if defWarning != "" {
				warnings = append(warnings, ImportWarning{
					Phase:   "schema",
					Object:  stmt.TableName + "." + col.Name,
					Message: defWarning,
				})
			}
			if defVal != nil {
				fd.DefaultValue = defVal
			}
		}

		cmd.Fields = append(cmd.Fields, fd)
	}

	return cmd, warnings
}

// IndexConversion holds the result of converting a PG index to SyndrDB.
type IndexConversion struct {
	IndexType  string // "B-INDEX", "HASH INDEX", "BRIN INDEX"
	BundleName string
	Fields     []string
	Unique     bool
	Expression string // For expression indexes
	Where      string // For partial indexes
	Warning    string
}

// ConvertIndex converts a parsed CREATE INDEX statement into SyndrDB index parameters.
func ConvertIndex(stmt *ParsedStatement) (*IndexConversion, ImportWarning) {
	if stmt.IndexDef == nil {
		return nil, ImportWarning{}
	}

	def := stmt.IndexDef
	conv := &IndexConversion{
		BundleName: def.Table,
		Fields:     def.Columns,
		Unique:     def.Unique,
		Expression: def.Expression,
		Where:      def.Where,
	}

	switch def.Method {
	case "btree", "":
		conv.IndexType = "B-INDEX"
	case "hash":
		conv.IndexType = "HASH INDEX"
	case "brin":
		conv.IndexType = "BRIN INDEX"
	case "gin", "gist":
		conv.IndexType = "B-INDEX"
		return conv, ImportWarning{
			Phase:   "index",
			Object:  def.Name,
			Message: def.Method + " index mapped to B-INDEX (best-effort)",
		}
	default:
		conv.IndexType = "B-INDEX"
		return conv, ImportWarning{
			Phase:   "index",
			Object:  def.Name,
			Message: "unknown index method " + def.Method + ", using B-INDEX",
		}
	}

	return conv, ImportWarning{}
}

// RelationshipConversion holds the result of converting a PG FK to SyndrDB.
type RelationshipConversion struct {
	Command *models.RelationshipCommand
	Warning string
}

// ConvertFK converts a foreign key constraint into a SyndrDB RelationshipCommand.
func ConvertFK(tableName string, constraint PGConstraint) *RelationshipConversion {
	if constraint.Type != ConstraintForeignKey {
		return nil
	}

	relName := constraint.Name
	if relName == "" {
		// Generate a name
		if len(constraint.Columns) > 0 {
			relName = "fk_" + tableName + "_" + constraint.Columns[0]
		} else {
			relName = "fk_" + tableName + "_" + constraint.RefTable
		}
	}

	sourceField := ""
	if len(constraint.Columns) > 0 {
		sourceField = constraint.Columns[0]
	}
	destField := ""
	if len(constraint.RefColumns) > 0 {
		destField = constraint.RefColumns[0]
	}

	cmd := &models.RelationshipCommand{
		CommandType:       "CREATE",
		BundleName:        tableName,
		Name:              relName,
		RelationshipType:  "0toMany",
		SourceBundle:      tableName,
		SourceField:       sourceField,
		DestinationBundle: constraint.RefTable,
		DestinationField:  destField,
	}

	result := &RelationshipConversion{Command: cmd}

	// Multi-column FKs are not directly supported
	if len(constraint.Columns) > 1 {
		result.Warning = "multi-column FK simplified to first column only"
	}

	return result
}
