package pgimport

import (
	"strings"
)

// mapPGType converts a PostgreSQL column type to a SyndrDB field type.
// Returns the SyndrDB type name and an optional warning message.
func mapPGType(pgType string) (string, string) {
	// Normalize: lowercase, strip leading/trailing whitespace
	normalized := strings.ToLower(strings.TrimSpace(pgType))

	// Strip array suffix for detection (e.g., "integer[]", "text[]")
	if strings.HasSuffix(normalized, "[]") || strings.Contains(normalized, " array") {
		return "string", "array type stored as JSON string"
	}

	// Strip type parameters for matching (e.g., "varchar(255)" → "varchar")
	base := normalized
	if idx := strings.Index(base, "("); idx != -1 {
		base = strings.TrimSpace(base[:idx])
	}

	// Strip "without time zone" / "with time zone" suffixes
	base = strings.TrimSuffix(base, " without time zone")
	base = strings.TrimSuffix(base, " with time zone")
	base = strings.TrimSpace(base)

	switch base {
	// Integer types
	case "integer", "int", "int4", "bigint", "int8", "smallint", "int2":
		return "int", ""
	case "serial", "bigserial", "smallserial", "serial4", "serial8", "serial2":
		return "int", "auto-increment not supported"

	// Float types
	case "real", "float4", "double precision", "float8", "float":
		return "float", ""
	case "numeric", "decimal", "money":
		return "float", "precision may be lost"

	// String types
	case "varchar", "character varying", "text", "char", "character", "bpchar", "name", "citext":
		return "string", ""

	// Boolean
	case "boolean", "bool":
		return "bool", ""

	// Timestamp/DateTime
	case "timestamp", "timestamptz", "timestamp with time zone":
		return "datetime", ""

	// Date
	case "date":
		return "date", ""

	// UUID
	case "uuid":
		return "string", "stored as string"

	// JSON
	case "json", "jsonb":
		return "string", "stored as JSON string"

	// Binary
	case "bytea":
		return "string", "stored as base64"

	// Interval
	case "interval":
		return "string", "stored as string"

	// Network types
	case "inet", "cidr", "macaddr", "macaddr8":
		return "string", "stored as string"

	// Geometric types
	case "point", "line", "lseg", "box", "path", "polygon", "circle":
		return "string", "stored as string"

	// XML
	case "xml":
		return "string", "stored as string"

	// Bit string types
	case "bit", "bit varying", "varbit":
		return "string", "stored as string"

	// Time types (no direct SyndrDB equivalent)
	case "time", "timetz", "time with time zone", "time without time zone":
		return "string", "stored as string"

	// OID types
	case "oid", "regclass", "regtype", "regproc":
		return "int", ""

	// User-defined / enum types
	default:
		return "string", "unsupported type, defaulting to string"
	}
}

// mapPGDefault converts a PostgreSQL DEFAULT expression to a SyndrDB default value.
// Returns the converted default value and an optional warning.
func mapPGDefault(pgDefault string) (interface{}, string) {
	if pgDefault == "" {
		return nil, ""
	}

	trimmed := strings.TrimSpace(pgDefault)
	lower := strings.ToLower(trimmed)

	// Strip type cast suffixes (e.g., "'value'::character varying", "'now()'::timestamp")
	if idx := strings.Index(lower, "::"); idx != -1 {
		trimmed = strings.TrimSpace(trimmed[:idx])
		lower = strings.ToLower(trimmed)
	}

	// Strip surrounding single quotes if present (cast expressions often have quoted values)
	unquoted := lower
	if len(trimmed) >= 2 && trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'' {
		unquoted = strings.ToLower(trimmed[1 : len(trimmed)-1])
	}

	// now() / current_timestamp → F:NOW()
	if unquoted == "now()" || unquoted == "current_timestamp" || unquoted == "current_date" ||
		lower == "now()" || lower == "current_timestamp" || lower == "current_date" {
		return "F:NOW()", ""
	}

	// nextval('sequence_name') → skip
	if strings.HasPrefix(lower, "nextval(") {
		return nil, "sequence default skipped (auto-increment not supported)"
	}

	// Boolean literals
	if lower == "true" || lower == "'t'" {
		return true, ""
	}
	if lower == "false" || lower == "'f'" {
		return false, ""
	}

	// NULL
	if lower == "null" {
		return nil, ""
	}

	// Strip surrounding single quotes for string literals
	if len(trimmed) >= 2 && trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'' {
		return trimmed[1 : len(trimmed)-1], ""
	}

	// Return as-is for numeric or other literals
	return trimmed, ""
}

// mapPGConstraint interprets a PostgreSQL constraint and returns field-level actions.
// Returns: isRequired, isUnique, and an optional warning.
func mapPGConstraint(c PGConstraint) (isRequired bool, isUnique bool, warning string) {
	switch c.Type {
	case ConstraintPrimaryKey:
		// SyndrDB uses DocumentID as PK; PG PKs become unique fields
		return true, true, ""
	case ConstraintUnique:
		return false, true, ""
	case ConstraintForeignKey:
		// FK handling is deferred to relationship creation
		return false, false, ""
	case ConstraintCheck:
		return false, false, "CHECK constraint skipped: " + c.CheckExpr
	default:
		return false, false, ""
	}
}
