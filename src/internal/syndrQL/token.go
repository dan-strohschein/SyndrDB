package syndrQL

// Token represents a lexical token in SyndrQL
type Token struct {
	Type    TokenType
	Value   string
	Literal interface{} // Parsed value for literals (int, float, bool, string)
	Line    int         // Line number for error reporting
	Column  int         // Column number for error reporting
}

// TokenType represents the type of token
type TokenType int

const (
	// Special tokens
	TOKEN_ILLEGAL TokenType = iota
	TOKEN_EOF
	TOKEN_WHITESPACE // Only used internally, filtered out

	// Identifiers and literals
	TOKEN_IDENT  // bundle_name, field_name, variable
	TOKEN_STRING // "quoted string"
	TOKEN_NUMBER // 123, 45.67
	TOKEN_TRUE   // true
	TOKEN_FALSE  // false
	TOKEN_NULL   // NULL

	// Operators - Arithmetic
	TOKEN_PLUS     // +
	TOKEN_MINUS    // -
	TOKEN_MULTIPLY // *
	TOKEN_DIVIDE   // /
	TOKEN_MODULO   // %

	// Operators - Assignment
	TOKEN_ASSIGN // = (single equals for assignment)

	// Operators - Comparison
	TOKEN_EQ          // ==
	TOKEN_NEQ         // !=
	TOKEN_LT          // <
	TOKEN_LTE         // <=
	TOKEN_GT          // >
	TOKEN_GTE         // >=
	TOKEN_LIKE        // LIKE
	TOKEN_IN          // IN
	TOKEN_NOTIN       // NOT IN
	TOKEN_IS_NULL     // IS NULL
	TOKEN_IS_NOT_NULL // IS NOT NULL
	TOKEN_EXISTS      // EXISTS (for subquery support - Tier 1)
	TOKEN_CONTAINS    // CONTAINS

	// Operators - Logical
	TOKEN_AND // AND
	TOKEN_OR  // OR
	TOKEN_NOT // NOT

	// Delimiters
	TOKEN_COMMA     // ,
	TOKEN_SEMICOLON // ;
	TOKEN_COLON     // :
	TOKEN_DOT       // . (for qualified field names like Bundle.Field)
	TOKEN_LPAREN    // (
	TOKEN_RPAREN    // )
	TOKEN_LBRACE    // {
	TOKEN_RBRACE    // }
	TOKEN_LBRACKET  // [
	TOKEN_RBRACKET  // ]

	// Keywords - DML (Hot Path)
	TOKEN_SELECT    // SELECT
	TOKEN_INSERT    // INSERT
	TOKEN_UPDATE    // UPDATE
	TOKEN_DELETE    // DELETE
	TOKEN_FROM      // FROM
	TOKEN_WHERE     // WHERE
	TOKEN_INTO      // INTO
	TOKEN_VALUES    // VALUES
	TOKEN_SET       // SET
	TOKEN_DOCUMENT  // DOCUMENT
	TOKEN_DOCUMENTS // DOCUMENTS

	// Keywords - DDL (Warm Path)
	TOKEN_CREATE // CREATE
	TOKEN_ALTER  // ALTER
	TOKEN_DROP   // DROP
	TOKEN_BUNDLE // BUNDLE
	TOKEN_WITH   // WITH
	TOKEN_FOR    // FOR
	TOKEN_FIELDS // FIELDS
	TOKEN_ADD    // ADD
	TOKEN_TO     // TO
	TOKEN_NAME   // NAME

	// Keywords - Query Modifiers
	TOKEN_ORDER        // ORDER
	TOKEN_BY           // BY
	TOKEN_LIMIT        // LIMIT
	TOKEN_OFFSET       // OFFSET
	TOKEN_GROUP        // GROUP
	TOKEN_HAVING       // HAVING
	TOKEN_JOIN         // JOIN
	TOKEN_ON           // ON
	TOKEN_AS           // AS
	TOKEN_RELATIONSHIP // RELATIONSHIP (for WITH RELATIONSHIP clause)
	TOKEN_FORCE        // FORCE (for FORCE switch)
	TOKEN_CONFIRMED    // CONFIRMED (for bulk operation safety)

	// Keywords - Utility (Cold Path)
	TOKEN_SHOW     // SHOW
	TOKEN_DESCRIBE // DESCRIBE
	TOKEN_USE      // USE
	TOKEN_DATABASE // DATABASE
	TOKEN_BUNDLES  // BUNDLES

	// Keywords - Views
	TOKEN_VIEW         // VIEW
	TOKEN_VIEWS        // VIEWS
	TOKEN_MATERIALIZED // MATERIALIZED
	TOKEN_REFRESH      // REFRESH

	// Keywords - RBAC
	TOKEN_USER        // USER
	TOKEN_PASSWORD    // PASSWORD
	TOKEN_GRANT       // GRANT
	TOKEN_REVOKE      // REVOKE
	TOKEN_ROLE        // ROLE
	TOKEN_DESCRIPTION // DESCRIPTION

	// Keywords - Types
	TOKEN_STRING_TYPE   // STRING
	TOKEN_INT_TYPE      // INT
	TOKEN_FLOAT_TYPE    // FLOAT
	TOKEN_BOOL_TYPE     // BOOL
	TOKEN_ARRAY_TYPE    // ARRAY
	TOKEN_DATE_TYPE     // DATE
	TOKEN_DATETIME_TYPE // DATETIME

	// Keywords - Field Modification Types
	TOKEN_REMOVE_FIELD // REMOVE
	TOKEN_MODIFY_FIELD // MODIFY

	// Keywords - Migration System
	TOKEN_MIGRATION  // MIGRATION
	TOKEN_START      // START
	TOKEN_COMMIT     // COMMIT
	TOKEN_VERSION    // VERSION
	TOKEN_APPLY      // APPLY
	TOKEN_VALIDATE   // VALIDATE
	TOKEN_ROLLBACK   // ROLLBACK
	TOKEN_MIGRATIONS // MIGRATIONS (for SHOW MIGRATIONS)

	// Keywords - Transaction System
	TOKEN_BEGIN       // BEGIN
	TOKEN_TRANSACTION // TRANSACTION
	TOKEN_SAVEPOINT   // SAVEPOINT

	// Keywords - Prepared Statements
	TOKEN_PREPARE    // PREPARE
	TOKEN_EXECUTE    // EXECUTE
	TOKEN_DEALLOCATE // DEALLOCATE

	// Parameterized Queries
	TOKEN_PARAMETER // $1, $2, $3, ... (for prepared statements and parameter binding)

	// Keywords - DateTime Functions
	// TODO: I will add TOKEN_DATE_DIFF when implementing interval-based date arithmetic extensions
	// TODO: I will add TOKEN_FORMAT when implementing custom DateTime format output (Phase 2)
	TOKEN_FUNCTION   // F: prefix for function calls (case-insensitive)
	TOKEN_NOW        // NOW
	TOKEN_EXTRACT    // EXTRACT
	TOKEN_DATE_TRUNC // DATE_TRUNC
	TOKEN_DATE_ADD   // DATE_ADD
	TOKEN_DATE_SUB   // DATE_SUB
	TOKEN_AGE        // AGE
	TOKEN_INTERVAL   // INTERVAL
	TOKEN_AT         // AT (for AT TIME ZONE)
	TOKEN_TIME       // TIME (for AT TIME ZONE)
	TOKEN_ZONE       // ZONE (for AT TIME ZONE)

	// Keywords - DateTime Units (for EXTRACT and DATE_TRUNC)
	// TODO: I will add TOKEN_WEEK, TOKEN_QUARTER when implementing extended date part extraction
	TOKEN_YEAR   // YEAR
	TOKEN_MONTH  // MONTH
	TOKEN_DAY    // DAY
	TOKEN_HOUR   // HOUR
	TOKEN_MINUTE // MINUTE
	TOKEN_SECOND // SECOND
)

// String returns the string representation of a token type
func (tt TokenType) String() string {
	switch tt {
	case TOKEN_ILLEGAL:
		return "ILLEGAL"
	case TOKEN_EOF:
		return "EOF"
	case TOKEN_WHITESPACE:
		return "WHITESPACE"
	case TOKEN_IDENT:
		return "IDENT"
	case TOKEN_STRING:
		return "STRING"
	case TOKEN_NUMBER:
		return "NUMBER"
	case TOKEN_TRUE:
		return "TRUE"
	case TOKEN_FALSE:
		return "FALSE"
	case TOKEN_NULL:
		return "NULL"
	case TOKEN_PLUS:
		return "+"
	case TOKEN_MINUS:
		return "-"
	case TOKEN_MULTIPLY:
		return "*"
	case TOKEN_DIVIDE:
		return "/"
	case TOKEN_MODULO:
		return "%"
	case TOKEN_ASSIGN:
		return "="
	case TOKEN_EQ:
		return "=="
	case TOKEN_NEQ:
		return "!="
	case TOKEN_LT:
		return "<"
	case TOKEN_LTE:
		return "<="
	case TOKEN_GT:
		return ">"
	case TOKEN_GTE:
		return ">="
	case TOKEN_LIKE:
		return "LIKE"
	case TOKEN_IN:
		return "IN"
	case TOKEN_NOTIN:
		return "NOT IN"
	case TOKEN_IS_NULL:
		return "IS NULL"
	case TOKEN_IS_NOT_NULL:
		return "IS NOT NULL"
	case TOKEN_EXISTS:
		return "EXISTS"
	case TOKEN_CONTAINS:
		return "CONTAINS"
	case TOKEN_AND:
		return "AND"
	case TOKEN_OR:
		return "OR"
	case TOKEN_NOT:
		return "NOT"
	case TOKEN_COMMA:
		return ","
	case TOKEN_SEMICOLON:
		return ";"
	case TOKEN_COLON:
		return ":"
	case TOKEN_DOT:
		return "."
	case TOKEN_LPAREN:
		return "("
	case TOKEN_RPAREN:
		return ")"
	case TOKEN_LBRACE:
		return "{"
	case TOKEN_RBRACE:
		return "}"
	case TOKEN_LBRACKET:
		return "["
	case TOKEN_RBRACKET:
		return "]"
	case TOKEN_SELECT:
		return "SELECT"
	case TOKEN_INSERT:
		return "INSERT"
	case TOKEN_UPDATE:
		return "UPDATE"
	case TOKEN_DELETE:
		return "DELETE"
	case TOKEN_FROM:
		return "FROM"
	case TOKEN_WHERE:
		return "WHERE"
	case TOKEN_INTO:
		return "INTO"
	case TOKEN_VALUES:
		return "VALUES"
	case TOKEN_SET:
		return "SET"
	case TOKEN_DOCUMENT:
		return "DOCUMENT"
	case TOKEN_DOCUMENTS:
		return "DOCUMENTS"
	case TOKEN_CREATE:
		return "CREATE"
	case TOKEN_ALTER:
		return "ALTER"
	case TOKEN_DROP:
		return "DROP"
	case TOKEN_BUNDLE:
		return "BUNDLE"
	case TOKEN_WITH:
		return "WITH"
	case TOKEN_FIELDS:
		return "FIELDS"
	case TOKEN_ADD:
		return "ADD"
	case TOKEN_TO:
		return "TO"
	case TOKEN_ORDER:
		return "ORDER"
	case TOKEN_BY:
		return "BY"
	case TOKEN_LIMIT:
		return "LIMIT"
	case TOKEN_OFFSET:
		return "OFFSET"
	case TOKEN_GROUP:
		return "GROUP"
	case TOKEN_HAVING:
		return "HAVING"
	case TOKEN_JOIN:
		return "JOIN"
	case TOKEN_ON:
		return "ON"
	case TOKEN_AS:
		return "AS"
	case TOKEN_FOR:
		return "FOR"
	case TOKEN_RELATIONSHIP:
		return "RELATIONSHIP"
	case TOKEN_SHOW:
		return "SHOW"
	case TOKEN_DESCRIBE:
		return "DESCRIBE"
	case TOKEN_USE:
		return "USE"
	case TOKEN_DATABASE:
		return "DATABASE"
	case TOKEN_BUNDLES:
		return "BUNDLES"
	case TOKEN_VIEW:
		return "VIEW"
	case TOKEN_VIEWS:
		return "VIEWS"
	case TOKEN_MATERIALIZED:
		return "MATERIALIZED"
	case TOKEN_REFRESH:
		return "REFRESH"
	case TOKEN_USER:
		return "USER"
	case TOKEN_PASSWORD:
		return "PASSWORD"
	case TOKEN_GRANT:
		return "GRANT"
	case TOKEN_REVOKE:
		return "REVOKE"
	case TOKEN_ROLE:
		return "ROLE"
	case TOKEN_DESCRIPTION:
		return "DESCRIPTION"
	case TOKEN_STRING_TYPE:
		return "STRING_TYPE"
	case TOKEN_INT_TYPE:
		return "INT_TYPE"
	case TOKEN_FLOAT_TYPE:
		return "FLOAT_TYPE"
	case TOKEN_BOOL_TYPE:
		return "BOOL_TYPE"
	case TOKEN_ARRAY_TYPE:
		return "ARRAY_TYPE"
	case TOKEN_DATE_TYPE:
		return "DATE_TYPE"
	case TOKEN_DATETIME_TYPE:
		return "DATETIME_TYPE"
	case TOKEN_FORCE:
		return "FORCE"
	case TOKEN_CONFIRMED:
		return "CONFIRMED"
	case TOKEN_REMOVE_FIELD:
		return "REMOVE_FIELD"
	case TOKEN_MODIFY_FIELD:
		return "MODIFY_FIELD"
	case TOKEN_NAME:
		return "NAME"
	case TOKEN_MIGRATION:
		return "MIGRATION"
	case TOKEN_START:
		return "START"
	case TOKEN_COMMIT:
		return "COMMIT"
	case TOKEN_VERSION:
		return "VERSION"
	case TOKEN_APPLY:
		return "APPLY"
	case TOKEN_VALIDATE:
		return "VALIDATE"
	case TOKEN_ROLLBACK:
		return "ROLLBACK"
	case TOKEN_MIGRATIONS:
		return "MIGRATIONS"
	case TOKEN_BEGIN:
		return "BEGIN"
	case TOKEN_TRANSACTION:
		return "TRANSACTION"
	case TOKEN_SAVEPOINT:
		return "SAVEPOINT"
	case TOKEN_PREPARE:
		return "PREPARE"
	case TOKEN_EXECUTE:
		return "EXECUTE"
	case TOKEN_DEALLOCATE:
		return "DEALLOCATE"
	case TOKEN_PARAMETER:
		return "PARAMETER"
	case TOKEN_FUNCTION:
		return "F:"
	case TOKEN_NOW:
		return "NOW"
	case TOKEN_EXTRACT:
		return "EXTRACT"
	case TOKEN_DATE_TRUNC:
		return "DATE_TRUNC"
	case TOKEN_DATE_ADD:
		return "DATE_ADD"
	case TOKEN_DATE_SUB:
		return "DATE_SUB"
	case TOKEN_AGE:
		return "AGE"
	case TOKEN_INTERVAL:
		return "INTERVAL"
	case TOKEN_AT:
		return "AT"
	case TOKEN_TIME:
		return "TIME"
	case TOKEN_ZONE:
		return "ZONE"
	case TOKEN_YEAR:
		return "YEAR"
	case TOKEN_MONTH:
		return "MONTH"
	case TOKEN_DAY:
		return "DAY"
	case TOKEN_HOUR:
		return "HOUR"
	case TOKEN_MINUTE:
		return "MINUTE"
	case TOKEN_SECOND:
		return "SECOND"
	default:
		return "UNKNOWN"
	}
}

// IsKeyword returns true if the token type is a keyword
func (tt TokenType) IsKeyword() bool {
	return tt >= TOKEN_SELECT && tt <= TOKEN_ARRAY_TYPE
}

// IsOperator returns true if the token type is an operator
func (tt TokenType) IsOperator() bool {
	return (tt >= TOKEN_PLUS && tt <= TOKEN_MODULO) ||
		(tt >= TOKEN_EQ && tt <= TOKEN_CONTAINS) ||
		(tt >= TOKEN_AND && tt <= TOKEN_NOT)
}

// IsComparison returns true if the token is a comparison operator
func (tt TokenType) IsComparison() bool {
	return tt >= TOKEN_EQ && tt <= TOKEN_CONTAINS
}

// IsLogical returns true if the token is a logical operator
func (tt TokenType) IsLogical() bool {
	return tt >= TOKEN_AND && tt <= TOKEN_NOT
}

// keywords maps keyword strings to their token types
// This is used for O(1) keyword lookup during tokenization
var keywords = map[string]TokenType{
	// DML Keywords (Hot Path)
	"SELECT":    TOKEN_SELECT,
	"INSERT":    TOKEN_INSERT,
	"UPDATE":    TOKEN_UPDATE,
	"DELETE":    TOKEN_DELETE,
	"FROM":      TOKEN_FROM,
	"WHERE":     TOKEN_WHERE,
	"INTO":      TOKEN_INTO,
	"VALUES":    TOKEN_VALUES,
	"SET":       TOKEN_SET,
	"DOCUMENT":  TOKEN_DOCUMENT,
	"DOCUMENTS": TOKEN_DOCUMENTS,

	// DDL Keywords (Warm Path)
	"CREATE": TOKEN_CREATE,
	"ALTER":  TOKEN_ALTER,
	"DROP":   TOKEN_DROP,
	"BUNDLE": TOKEN_BUNDLE,
	"WITH":   TOKEN_WITH,
	"FIELDS": TOKEN_FIELDS,
	"ADD":    TOKEN_ADD,
	"TO":     TOKEN_TO,
	"NAME":   TOKEN_NAME,

	// Query Modifiers
	"ORDER":        TOKEN_ORDER,
	"BY":           TOKEN_BY,
	"LIMIT":        TOKEN_LIMIT,
	"OFFSET":       TOKEN_OFFSET,
	"GROUP":        TOKEN_GROUP,
	"HAVING":       TOKEN_HAVING,
	"JOIN":         TOKEN_JOIN,
	"ON":           TOKEN_ON,
	"AS":           TOKEN_AS,
	"FOR":          TOKEN_FOR,
	"RELATIONSHIP": TOKEN_RELATIONSHIP,
	"DESCRIPTION":  TOKEN_DESCRIPTION,
	"FORCE":        TOKEN_FORCE,
	"CONFIRMED":    TOKEN_CONFIRMED,

	// Utility Keywords (Cold Path)
	"SHOW":     TOKEN_SHOW,
	"DESCRIBE": TOKEN_DESCRIBE,
	"USE":      TOKEN_USE,
	"DATABASE": TOKEN_DATABASE,
	"BUNDLES":  TOKEN_BUNDLES,

	// View Keywords
	"VIEW":         TOKEN_VIEW,
	"VIEWS":        TOKEN_VIEWS,
	"MATERIALIZED": TOKEN_MATERIALIZED,
	"REFRESH":      TOKEN_REFRESH,

	// RBAC Keywords
	"USER":     TOKEN_USER,
	"PASSWORD": TOKEN_PASSWORD,
	"GRANT":    TOKEN_GRANT,
	"REVOKE":   TOKEN_REVOKE,
	"ROLE":     TOKEN_ROLE,

	// Operators as keywords
	"AND":      TOKEN_AND,
	"OR":       TOKEN_OR,
	"NOT":      TOKEN_NOT,
	"LIKE":     TOKEN_LIKE,
	"IN":       TOKEN_IN,
	"EXISTS":   TOKEN_EXISTS,
	"CONTAINS": TOKEN_CONTAINS,

	// Literals as keywords
	"TRUE":  TOKEN_TRUE,
	"FALSE": TOKEN_FALSE,
	"NULL":  TOKEN_NULL,

	// Type keywords
	"STRING":   TOKEN_STRING_TYPE,
	"INT":      TOKEN_INT_TYPE,
	"FLOAT":    TOKEN_FLOAT_TYPE,
	"BOOL":     TOKEN_BOOL_TYPE,
	"ARRAY":    TOKEN_ARRAY_TYPE,
	"DATE":     TOKEN_DATE_TYPE,
	"DATETIME": TOKEN_DATETIME_TYPE,

	"REMOVE": TOKEN_REMOVE_FIELD,
	"MODIFY": TOKEN_MODIFY_FIELD,

	// Migration Keywords
	"MIGRATION":  TOKEN_MIGRATION,
	"START":      TOKEN_START,
	"COMMIT":     TOKEN_COMMIT,
	"VERSION":    TOKEN_VERSION,
	"APPLY":      TOKEN_APPLY,
	"VALIDATE":   TOKEN_VALIDATE,
	"ROLLBACK":   TOKEN_ROLLBACK,
	"MIGRATIONS": TOKEN_MIGRATIONS,

	// Transaction Keywords
	"BEGIN":       TOKEN_BEGIN,
	"TRANSACTION": TOKEN_TRANSACTION,
	"SAVEPOINT":   TOKEN_SAVEPOINT,

	// Prepared Statement Keywords
	"PREPARE":    TOKEN_PREPARE,
	"EXECUTE":    TOKEN_EXECUTE,
	"DEALLOCATE": TOKEN_DEALLOCATE,

	// DateTime Function Keywords - REMOVED from keywords map
	// These are ONLY recognized when prefixed with F: (handled by readFunctionToken)
	// "NOW":        TOKEN_NOW,        // Only as F:NOW()
	// "EXTRACT":    TOKEN_EXTRACT,    // Only as F:EXTRACT()
	// "DATE_TRUNC": TOKEN_DATE_TRUNC, // Only as F:DATE_TRUNC()
	// "DATE_ADD":   TOKEN_DATE_ADD,   // Only as F:DATE_ADD()
	// "DATE_SUB":   TOKEN_DATE_SUB,   // Only as F:DATE_SUB()
	// "AGE":        TOKEN_AGE,        // Only as F:AGE()

	// DateTime Operators and Keywords (NOT functions)
	"INTERVAL": TOKEN_INTERVAL,
	"AT":       TOKEN_AT,
	"TIME":     TOKEN_TIME,
	"ZONE":     TOKEN_ZONE,

	// DateTime Unit Keywords (for INTERVAL and EXTRACT)
	"YEAR":   TOKEN_YEAR,
	"MONTH":  TOKEN_MONTH,
	"DAY":    TOKEN_DAY,
	"HOUR":   TOKEN_HOUR,
	"MINUTE": TOKEN_MINUTE,
	"SECOND": TOKEN_SECOND,
}

// LookupKeyword checks if an identifier is a keyword and returns its token type
// Returns TOKEN_IDENT if not a keyword
func LookupKeyword(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TOKEN_IDENT
}
