package pgimport

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"

	"go.uber.org/zap"
)

// PlainFormatParser streams a PostgreSQL plain-format dump file and yields parsed statements.
type PlainFormatParser struct {
	scanner *bufio.Scanner
	logger  *zap.SugaredLogger
	lineNo  int

	// COPY data streaming state
	inCopy   bool
	copyMeta *COPYMetadata
}

// NewPlainFormatParser creates a parser that reads from the given reader.
func NewPlainFormatParser(reader io.Reader, logger *zap.SugaredLogger) *PlainFormatParser {
	s := bufio.NewScanner(reader)
	s.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024) // 10MB max line
	return &PlainFormatParser{
		scanner: s,
		logger:  logger,
	}
}

// NextStatement reads lines until a complete SQL statement is found, then parses it.
// Returns nil at EOF. Returns StmtSkip statements for unsupported SQL.
func (p *PlainFormatParser) NextStatement() (*ParsedStatement, error) {
	// If we're in COPY data mode, stream until end marker
	if p.inCopy {
		return p.readCopyData()
	}

	var sb strings.Builder
	inString := false
	inDollarQuote := false
	dollarTag := ""
	parenDepth := 0

	for p.scanner.Scan() {
		p.lineNo++
		line := p.scanner.Text()

		// Skip empty lines and SQL comments
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			if sb.Len() == 0 {
				continue
			}
			// Inside a multi-line statement, comments are part of it — skip them
			if !inString && !inDollarQuote {
				continue
			}
		}

		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(line)

		// Track quoting state to find real statement boundaries
		for i := 0; i < len(line); i++ {
			ch := line[i]
			switch {
			case inDollarQuote:
				// Check for closing dollar tag
				if ch == '$' {
					rest := line[i:]
					if strings.HasPrefix(rest, dollarTag) {
						inDollarQuote = false
						i += len(dollarTag) - 1
					}
				}
			case inString:
				if ch == '\'' {
					// Check for escaped quote ('')
					if i+1 < len(line) && line[i+1] == '\'' {
						i++ // skip escaped quote
					} else {
						inString = false
					}
				}
			default:
				switch ch {
				case '\'':
					inString = true
				case '$':
					// Check for dollar quoting ($$, $tag$)
					tag := extractDollarTag(line[i:])
					if tag != "" {
						inDollarQuote = true
						dollarTag = tag
						i += len(tag) - 1
					}
				case '(':
					parenDepth++
				case ')':
					if parenDepth > 0 {
						parenDepth--
					}
				case '-':
					// Check for -- line comment (outside strings)
					if i+1 < len(line) && line[i+1] == '-' {
						// Rest of line is comment, stop scanning this line
						i = len(line)
					}
				case ';':
					if parenDepth == 0 {
						// Statement complete
						return p.classifyAndParse(sb.String())
					}
				}
			}
		}
	}

	if err := p.scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error at line %d: %w", p.lineNo, err)
	}

	// Handle any remaining text without a trailing semicolon
	if sb.Len() > 0 {
		return p.classifyAndParse(sb.String())
	}

	return nil, nil // EOF
}

// extractDollarTag returns the dollar-quoted tag (e.g., "$$" or "$func$") starting at s,
// or "" if s does not start with a valid dollar-quote delimiter.
func extractDollarTag(s string) string {
	if len(s) < 2 || s[0] != '$' {
		return ""
	}
	// $$ case
	if s[1] == '$' {
		return "$$"
	}
	// $tag$ case
	for i := 1; i < len(s); i++ {
		ch := s[i]
		if ch == '$' {
			return s[:i+1]
		}
		if !isTagChar(ch) {
			return ""
		}
	}
	return ""
}

func isTagChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') || ch == '_'
}

// classifyAndParse routes a complete SQL statement to the appropriate mini-parser.
func (p *PlainFormatParser) classifyAndParse(sql string) (*ParsedStatement, error) {
	trimmed := strings.TrimSpace(sql)
	trimmed = strings.TrimSuffix(trimmed, ";")
	trimmed = strings.TrimSpace(trimmed)

	if trimmed == "" {
		return &ParsedStatement{Type: StmtSkip, RawSQL: sql}, nil
	}

	upper := strings.ToUpper(trimmed)

	// COPY ... FROM stdin
	if strings.HasPrefix(upper, "COPY ") && strings.Contains(upper, "FROM STDIN") {
		meta, err := parseCopyHeader(trimmed)
		if err != nil {
			if p.logger != nil {
				p.logger.Warnf("pgimport: failed to parse COPY header: %v", err)
			}
			return &ParsedStatement{Type: StmtSkip, RawSQL: sql}, nil
		}
		p.inCopy = true
		p.copyMeta = meta
		return &ParsedStatement{
			Type:      StmtCopyFrom,
			TableName: meta.TableName,
			CopyMeta:  meta,
			RawSQL:    sql,
		}, nil
	}

	// CREATE TABLE
	if strings.HasPrefix(upper, "CREATE TABLE") ||
		strings.HasPrefix(upper, "CREATE UNLOGGED TABLE") ||
		strings.HasPrefix(upper, "CREATE TEMP TABLE") ||
		strings.HasPrefix(upper, "CREATE TEMPORARY TABLE") {
		return parseCreateTable(trimmed)
	}

	// CREATE INDEX / CREATE UNIQUE INDEX
	if strings.HasPrefix(upper, "CREATE INDEX") || strings.HasPrefix(upper, "CREATE UNIQUE INDEX") {
		return parseCreateIndex(trimmed)
	}

	// CREATE VIEW / CREATE MATERIALIZED VIEW
	if strings.HasPrefix(upper, "CREATE VIEW") || strings.HasPrefix(upper, "CREATE MATERIALIZED VIEW") ||
		strings.HasPrefix(upper, "CREATE OR REPLACE VIEW") {
		return parseCreateView(trimmed)
	}

	// ALTER TABLE ... ADD CONSTRAINT
	if strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ADD CONSTRAINT") {
		return parseAlterTableConstraint(trimmed)
	}

	// INSERT INTO
	if strings.HasPrefix(upper, "INSERT INTO") {
		return parseInsert(trimmed)
	}

	// Skip: SET, COMMENT, CREATE SEQUENCE, CREATE FUNCTION, CREATE TRIGGER, etc.
	return &ParsedStatement{Type: StmtSkip, RawSQL: sql}, nil
}

// readCopyData reads COPY data lines until the `\.` terminator.
func (p *PlainFormatParser) readCopyData() (*ParsedStatement, error) {
	meta := p.copyMeta
	stmt := &ParsedStatement{
		Type:      StmtCopyFrom,
		TableName: meta.TableName,
		CopyMeta:  meta,
	}

	var rows [][]string
	for p.scanner.Scan() {
		p.lineNo++
		line := p.scanner.Text()

		// `\.` on its own line terminates COPY data
		if line == "\\." {
			p.inCopy = false
			p.copyMeta = nil
			break
		}

		// Parse tab-delimited row
		fields := strings.Split(line, "\t")
		row := make([]string, len(fields))
		for i, f := range fields {
			row[i] = unescapeCopyField(f)
		}
		rows = append(rows, row)
	}

	if err := p.scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error during COPY data at line %d: %w", p.lineNo, err)
	}

	stmt.InsertColumns = meta.Columns
	stmt.InsertRows = rows
	return stmt, nil
}

// unescapeCopyField handles PostgreSQL COPY text-format escapes.
// \N = NULL, \\ = backslash, \t = tab, \n = newline, \r = carriage return.
func unescapeCopyField(s string) string {
	if s == "\\N" {
		return "" // NULL marker — caller should check for "\\N" before calling
	}
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case '\\':
				sb.WriteByte('\\')
				i++
			case 'n':
				sb.WriteByte('\n')
				i++
			case 't':
				sb.WriteByte('\t')
				i++
			case 'r':
				sb.WriteByte('\r')
				i++
			default:
				sb.WriteByte(s[i])
			}
		} else {
			sb.WriteByte(s[i])
		}
	}
	return sb.String()
}

// --- Mini-parsers ---

// Regex patterns for CREATE TABLE parsing
var (
	reCreateTable = regexp.MustCompile(`(?i)CREATE\s+(?:UNLOGGED\s+|TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?((?:"[^"]+"\.)?"?[\w]+"?)\s*\(`)
	reSchemaTable = regexp.MustCompile(`(?:"?(\w+)"?\.)?"?(\w+)"?`)
)

// parseCreateTable parses a CREATE TABLE statement.
func parseCreateTable(sql string) (*ParsedStatement, error) {
	matches := reCreateTable.FindStringSubmatchIndex(sql)
	if matches == nil {
		return nil, fmt.Errorf("could not parse CREATE TABLE: %s", truncate(sql, 120))
	}

	// Extract table name (strip schema)
	rawName := sql[matches[2]:matches[3]]
	tableName := stripSchemaAndQuotes(rawName)

	// Find the column definitions between the outermost parentheses
	parenStart := matches[1] - 1 // position of '('
	body := extractParenBody(sql, parenStart)
	if body == "" {
		return nil, fmt.Errorf("could not find column definitions in CREATE TABLE: %s", truncate(sql, 120))
	}

	columns, constraints, err := parseColumnDefs(body)
	if err != nil {
		return nil, fmt.Errorf("error parsing columns for table %s: %w", tableName, err)
	}

	return &ParsedStatement{
		Type:        StmtCreateTable,
		TableName:   tableName,
		Columns:     columns,
		Constraints: constraints,
		RawSQL:      sql,
	}, nil
}

// parseColumnDefs parses the comma-separated list of column definitions and constraints
// inside CREATE TABLE parentheses.
func parseColumnDefs(body string) ([]PGColumn, []PGConstraint, error) {
	parts := splitColumnDefs(body)
	var columns []PGColumn
	var constraints []PGConstraint

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		upper := strings.ToUpper(part)

		// Table-level constraints
		if strings.HasPrefix(upper, "CONSTRAINT ") || strings.HasPrefix(upper, "PRIMARY KEY") ||
			strings.HasPrefix(upper, "UNIQUE") || strings.HasPrefix(upper, "FOREIGN KEY") ||
			strings.HasPrefix(upper, "CHECK") || strings.HasPrefix(upper, "EXCLUDE") {
			c, err := parseTableConstraint(part)
			if err == nil {
				constraints = append(constraints, c)
			}
			continue
		}

		// Column definition
		col, inlineConstraints := parseColumnDef(part)
		if col.Name != "" {
			columns = append(columns, col)
			constraints = append(constraints, inlineConstraints...)
		}
	}

	return columns, constraints, nil
}

// splitColumnDefs splits column definitions by comma, respecting parentheses depth.
func splitColumnDefs(body string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inStr := false

	for i := 0; i < len(body); i++ {
		ch := body[i]
		switch {
		case inStr:
			current.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(body) && body[i+1] == '\'' {
					current.WriteByte('\'')
					i++
				} else {
					inStr = false
				}
			}
		case ch == '\'':
			inStr = true
			current.WriteByte(ch)
		case ch == '(':
			depth++
			current.WriteByte(ch)
		case ch == ')':
			depth--
			current.WriteByte(ch)
		case ch == ',' && depth == 0:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// parseColumnDef parses a single column definition like: name varchar(255) NOT NULL DEFAULT 'foo'
func parseColumnDef(def string) (PGColumn, []PGConstraint) {
	// Tokenize by whitespace, respecting parentheses
	tokens := tokenizeColumnDef(def)
	if len(tokens) < 2 {
		return PGColumn{}, nil
	}

	col := PGColumn{
		Name: stripQuotes(tokens[0]),
	}

	// tokens[1] is the type — but may span multiple tokens (e.g., "double precision", "character varying")
	typeEnd := 2
	upperT1 := strings.ToUpper(tokens[1])

	// Multi-word types
	switch upperT1 {
	case "DOUBLE":
		if typeEnd < len(tokens) && strings.ToUpper(tokens[typeEnd]) == "PRECISION" {
			typeEnd++
		}
	case "CHARACTER":
		if typeEnd < len(tokens) {
			next := strings.ToUpper(tokens[typeEnd])
			if next == "VARYING" || strings.HasPrefix(next, "VARYING(") {
				typeEnd++
			}
		}
	case "TIMESTAMP", "TIME":
		// Look for "with time zone" / "without time zone"
		remaining := strings.Join(tokens[typeEnd:], " ")
		upper := strings.ToUpper(remaining)
		if strings.HasPrefix(upper, "WITH TIME ZONE") {
			typeEnd += 3
		} else if strings.HasPrefix(upper, "WITHOUT TIME ZONE") {
			typeEnd += 3
		}
	case "BIT":
		if typeEnd < len(tokens) && strings.ToUpper(tokens[typeEnd]) == "VARYING" {
			typeEnd++
		}
	}

	col.Type = strings.Join(tokens[1:typeEnd], " ")

	var constraints []PGConstraint

	// Parse remaining modifiers
	for i := typeEnd; i < len(tokens); i++ {
		upper := strings.ToUpper(tokens[i])
		switch upper {
		case "NOT":
			if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "NULL" {
				col.NotNull = true
				i++
			}
		case "NULL":
			// explicitly nullable, no-op
		case "UNIQUE":
			col.Unique = true
		case "PRIMARY":
			if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "KEY" {
				col.PrimaryKey = true
				col.NotNull = true
				i++
			}
		case "DEFAULT":
			// Collect everything until next modifier keyword or end
			defVal := collectDefault(tokens, i+1)
			col.Default = defVal
			// Skip over consumed tokens
			i += countDefaultTokens(tokens, i+1)
		case "REFERENCES":
			// Inline FK: REFERENCES table(col)
			if i+1 < len(tokens) {
				refTable := stripQuotes(tokens[i+1])
				// Check if next token has (col)
				refCols := extractParenList(tokens[i+1])
				if len(refCols) == 0 && i+2 < len(tokens) {
					refCols = extractParenList(tokens[i+2])
					if len(refCols) > 0 {
						i++
					}
				}
				refTable = stripQuotes(strings.Split(tokens[i+1], "(")[0])
				constraints = append(constraints, PGConstraint{
					Type:       ConstraintForeignKey,
					Columns:    []string{col.Name},
					RefTable:   stripSchemaAndQuotes(refTable),
					RefColumns: refCols,
				})
				i++
			}
		case "CHECK":
			// Skip CHECK constraints on columns
		case "COLLATE":
			if i+1 < len(tokens) {
				i++ // skip collation name
			}
		case "GENERATED", "CONSTRAINT":
			// Skip GENERATED ALWAYS AS ... / inline constraint name
			break
		}
	}

	return col, constraints
}

// tokenizeColumnDef splits a column definition into tokens, keeping parenthesized groups together.
func tokenizeColumnDef(def string) []string {
	var tokens []string
	var current strings.Builder
	depth := 0
	inStr := false

	for i := 0; i < len(def); i++ {
		ch := def[i]
		switch {
		case inStr:
			current.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(def) && def[i+1] == '\'' {
					current.WriteByte('\'')
					i++
				} else {
					inStr = false
				}
			}
		case ch == '\'':
			inStr = true
			current.WriteByte(ch)
		case ch == '(':
			depth++
			current.WriteByte(ch)
		case ch == ')':
			depth--
			current.WriteByte(ch)
		case (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') && depth == 0:
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// collectDefault gathers a DEFAULT value expression from tokens[start:].
func collectDefault(tokens []string, start int) string {
	var parts []string
	depth := 0
	for i := start; i < len(tokens); i++ {
		upper := strings.ToUpper(tokens[i])
		// Stop at modifier keywords (not inside parens)
		if depth == 0 {
			switch upper {
			case "NOT", "NULL", "UNIQUE", "PRIMARY", "REFERENCES", "CHECK",
				"CONSTRAINT", "COLLATE", "GENERATED", "DEFERRABLE":
				goto done
			}
		}
		parts = append(parts, tokens[i])
		depth += strings.Count(tokens[i], "(") - strings.Count(tokens[i], ")")
	}
done:
	return strings.Join(parts, " ")
}

// countDefaultTokens counts how many tokens a DEFAULT value consumes.
func countDefaultTokens(tokens []string, start int) int {
	count := 0
	depth := 0
	for i := start; i < len(tokens); i++ {
		upper := strings.ToUpper(tokens[i])
		if depth == 0 {
			switch upper {
			case "NOT", "NULL", "UNIQUE", "PRIMARY", "REFERENCES", "CHECK",
				"CONSTRAINT", "COLLATE", "GENERATED", "DEFERRABLE":
				return count
			}
		}
		count++
		depth += strings.Count(tokens[i], "(") - strings.Count(tokens[i], ")")
	}
	return count
}

// parseTableConstraint parses a table-level constraint.
func parseTableConstraint(def string) (PGConstraint, error) {
	upper := strings.ToUpper(def)
	var name string

	// Strip CONSTRAINT name prefix
	working := def
	if strings.HasPrefix(upper, "CONSTRAINT ") {
		rest := def[len("CONSTRAINT "):]
		tokens := tokenizeColumnDef(rest)
		if len(tokens) > 0 {
			name = stripQuotes(tokens[0])
			working = strings.Join(tokens[1:], " ")
			upper = strings.ToUpper(working)
		}
	}

	if strings.HasPrefix(upper, "PRIMARY KEY") {
		cols := extractConstraintColumns(working)
		return PGConstraint{Name: name, Type: ConstraintPrimaryKey, Columns: cols}, nil
	}
	if strings.HasPrefix(upper, "UNIQUE") {
		cols := extractConstraintColumns(working)
		return PGConstraint{Name: name, Type: ConstraintUnique, Columns: cols}, nil
	}
	if strings.HasPrefix(upper, "FOREIGN KEY") {
		return parseForeignKeyConstraint(name, working), nil
	}
	if strings.HasPrefix(upper, "CHECK") {
		expr := extractParenBody(working, strings.Index(working, "("))
		return PGConstraint{Name: name, Type: ConstraintCheck, CheckExpr: expr}, nil
	}

	return PGConstraint{}, fmt.Errorf("unknown constraint type: %s", truncate(def, 80))
}

// parseForeignKeyConstraint parses: FOREIGN KEY (col1, col2) REFERENCES table (col3, col4)
func parseForeignKeyConstraint(name, def string) PGConstraint {
	c := PGConstraint{Name: name, Type: ConstraintForeignKey}

	upper := strings.ToUpper(def)
	fkIdx := strings.Index(upper, "FOREIGN KEY")
	if fkIdx < 0 {
		return c
	}
	rest := def[fkIdx+len("FOREIGN KEY"):]

	// Extract FK columns
	parenIdx := strings.Index(rest, "(")
	if parenIdx >= 0 {
		c.Columns = extractConstraintColumnsFromStr(extractParenBody(rest, parenIdx))
	}

	// Extract REFERENCES table (cols)
	upperRest := strings.ToUpper(rest)
	refIdx := strings.Index(upperRest, "REFERENCES")
	if refIdx >= 0 {
		afterRef := strings.TrimSpace(rest[refIdx+len("REFERENCES"):])
		tokens := tokenizeColumnDef(afterRef)
		if len(tokens) > 0 {
			c.RefTable = stripSchemaAndQuotes(tokens[0])
			if len(tokens) > 1 {
				c.RefColumns = extractConstraintColumnsFromStr(extractParenBody(strings.Join(tokens, " "), strings.Index(strings.Join(tokens, " "), "(")))
			} else {
				// Columns may be in the same token: table(col)
				if idx := strings.Index(tokens[0], "("); idx >= 0 {
					c.RefTable = stripSchemaAndQuotes(tokens[0][:idx])
					c.RefColumns = extractParenList(tokens[0])
				}
			}
		}
	}

	return c
}

// parseCreateIndex parses CREATE [UNIQUE] INDEX statements.
func parseCreateIndex(sql string) (*ParsedStatement, error) {
	upper := strings.ToUpper(sql)
	def := &PGIndexDef{Method: "btree"} // default method

	unique := strings.HasPrefix(upper, "CREATE UNIQUE")

	// Extract index name and table
	// Pattern: CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] "name" ON "table" [USING method] (cols) [WHERE ...]
	re := regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?("?[\w]+"?)\s+ON\s+(?:ONLY\s+)?("?[\w.]+"?)\s*`)
	matches := re.FindStringSubmatch(sql)
	if matches == nil {
		return &ParsedStatement{Type: StmtSkip, RawSQL: sql}, nil
	}

	def.Name = stripQuotes(matches[1])
	def.Table = stripSchemaAndQuotes(matches[2])
	def.Unique = unique

	// Check for USING method
	usingRe := regexp.MustCompile(`(?i)\bUSING\s+(\w+)\s*\(`)
	if usingMatch := usingRe.FindStringSubmatch(sql); usingMatch != nil {
		def.Method = strings.ToLower(usingMatch[1])
	}

	// Extract columns from the parenthesized list after ON table
	parenIdx := strings.Index(sql[re.FindStringIndex(sql)[1]:], "(")
	if parenIdx >= 0 {
		body := extractParenBody(sql, re.FindStringIndex(sql)[1]+parenIdx)
		def.Columns = extractConstraintColumnsFromStr(body)

		// Check if it looks like an expression index
		if len(def.Columns) == 1 && (strings.Contains(def.Columns[0], "(") || strings.ContainsAny(def.Columns[0], "+-*/")) {
			def.Expression = def.Columns[0]
		}
	}

	// Check for WHERE clause (partial index)
	whereRe := regexp.MustCompile(`(?i)\)\s+WHERE\s+(.+)$`)
	if whereMatch := whereRe.FindStringSubmatch(sql); whereMatch != nil {
		def.Where = strings.TrimSpace(whereMatch[1])
	}

	return &ParsedStatement{
		Type:      StmtCreateIndex,
		TableName: def.Table,
		IndexDef:  def,
		RawSQL:    sql,
	}, nil
}

// parseCreateView parses CREATE [MATERIALIZED] VIEW statements.
func parseCreateView(sql string) (*ParsedStatement, error) {
	upper := strings.ToUpper(sql)
	materialized := strings.Contains(upper, "MATERIALIZED")

	re := regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+("?[\w.]+"?)\s+(?:AS\s+)?(.+)`)
	matches := re.FindStringSubmatch(sql)
	if matches == nil {
		return &ParsedStatement{Type: StmtSkip, RawSQL: sql}, nil
	}

	return &ParsedStatement{
		Type:      StmtCreateView,
		TableName: stripSchemaAndQuotes(matches[1]),
		View: &PGView{
			Name:         stripSchemaAndQuotes(matches[1]),
			Definition:   strings.TrimSpace(matches[2]),
			Materialized: materialized,
		},
		RawSQL: sql,
	}, nil
}

// parseAlterTableConstraint parses ALTER TABLE ... ADD CONSTRAINT ... statements.
func parseAlterTableConstraint(sql string) (*ParsedStatement, error) {
	re := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:ONLY\s+)?((?:"[^"]+"\.)?"?[\w]+"?)\s+ADD\s+CONSTRAINT\s+`)
	matches := re.FindStringSubmatch(sql)
	if matches == nil {
		return &ParsedStatement{Type: StmtSkip, RawSQL: sql}, nil
	}

	tableName := stripSchemaAndQuotes(matches[1])

	// The constraint definition follows "ADD CONSTRAINT name ..."
	constraintRe := regexp.MustCompile(`(?i)ADD\s+CONSTRAINT\s+("?[\w]+"?)\s+(.+)`)
	cMatch := constraintRe.FindStringSubmatch(sql)
	if cMatch == nil {
		return &ParsedStatement{Type: StmtSkip, RawSQL: sql}, nil
	}

	constraintDef := "CONSTRAINT " + cMatch[1] + " " + cMatch[2]
	constraint, err := parseTableConstraint(constraintDef)
	if err != nil {
		return &ParsedStatement{Type: StmtSkip, RawSQL: sql}, nil
	}

	return &ParsedStatement{
		Type:        StmtAlterTable,
		TableName:   tableName,
		Constraints: []PGConstraint{constraint},
		RawSQL:      sql,
	}, nil
}

// parseCopyHeader parses the COPY header line.
// Format: COPY "table" (col1, col2, ...) FROM stdin
func parseCopyHeader(line string) (*COPYMetadata, error) {
	re := regexp.MustCompile(`(?i)COPY\s+((?:"[^"]+"\.)?"?[\w]+"?)\s*\(([^)]+)\)\s+FROM\s+STDIN`)
	matches := re.FindStringSubmatch(line)
	if matches == nil {
		// Try without column list: COPY "table" FROM stdin
		re2 := regexp.MustCompile(`(?i)COPY\s+((?:"[^"]+"\.)?"?[\w]+"?)\s+FROM\s+STDIN`)
		matches2 := re2.FindStringSubmatch(line)
		if matches2 == nil {
			return nil, fmt.Errorf("could not parse COPY header: %s", truncate(line, 120))
		}
		return &COPYMetadata{
			TableName: stripSchemaAndQuotes(matches2[1]),
		}, nil
	}

	cols := strings.Split(matches[2], ",")
	columns := make([]string, 0, len(cols))
	for _, c := range cols {
		columns = append(columns, stripQuotes(strings.TrimSpace(c)))
	}

	return &COPYMetadata{
		TableName: stripSchemaAndQuotes(matches[1]),
		Columns:   columns,
	}, nil
}

// parseInsert parses an INSERT INTO statement.
// Handles: INSERT INTO "table" (col1, col2) VALUES (v1, v2), (v3, v4)
func parseInsert(sql string) (*ParsedStatement, error) {
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+("?[\w.]+"?)\s*\(([^)]+)\)\s*VALUES\s*(.+)`)
	matches := re.FindStringSubmatch(sql)
	if matches == nil {
		return &ParsedStatement{Type: StmtSkip, RawSQL: sql}, nil
	}

	tableName := stripSchemaAndQuotes(matches[1])

	cols := strings.Split(matches[2], ",")
	columns := make([]string, 0, len(cols))
	for _, c := range cols {
		columns = append(columns, stripQuotes(strings.TrimSpace(c)))
	}

	// Parse VALUES tuples
	valuesStr := matches[3]
	rows := parseValuesTuples(valuesStr)

	return &ParsedStatement{
		Type:          StmtInsertInto,
		TableName:     tableName,
		InsertColumns: columns,
		InsertRows:    rows,
		RawSQL:        sql,
	}, nil
}

// parseValuesTuples parses: (v1, v2), (v3, v4)
func parseValuesTuples(s string) [][]string {
	var rows [][]string
	depth := 0
	var current strings.Builder
	inStr := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inStr:
			current.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					current.WriteByte('\'')
					i++
				} else {
					inStr = false
				}
			}
		case ch == '\'':
			inStr = true
			current.WriteByte(ch)
		case ch == '(':
			depth++
			if depth == 1 {
				current.Reset()
				continue
			}
			current.WriteByte(ch)
		case ch == ')':
			depth--
			if depth == 0 {
				// End of a tuple
				row := parseInsertRow(current.String())
				rows = append(rows, row)
				current.Reset()
				continue
			}
			current.WriteByte(ch)
		default:
			if depth > 0 {
				current.WriteByte(ch)
			}
		}
	}
	return rows
}

// parseInsertRow parses a comma-separated row inside parentheses.
func parseInsertRow(s string) []string {
	var vals []string
	var current strings.Builder
	inStr := false
	depth := 0

	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inStr:
			current.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					current.WriteByte('\'')
					i++
				} else {
					inStr = false
				}
			}
		case ch == '\'':
			inStr = true
			current.WriteByte(ch)
		case ch == '(':
			depth++
			current.WriteByte(ch)
		case ch == ')':
			depth--
			current.WriteByte(ch)
		case ch == ',' && depth == 0:
			vals = append(vals, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		vals = append(vals, strings.TrimSpace(current.String()))
	}
	return vals
}

// --- Utility functions ---

// stripQuotes removes surrounding double quotes from an identifier.
func stripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// stripSchemaAndQuotes removes schema prefix (e.g., "public".) and surrounding quotes.
func stripSchemaAndQuotes(s string) string {
	s = strings.TrimSpace(s)
	// Remove schema prefix
	if idx := strings.LastIndex(s, "."); idx >= 0 {
		s = s[idx+1:]
	}
	return stripQuotes(s)
}

// extractParenBody extracts the content between matching parentheses starting at position parenStart.
func extractParenBody(s string, parenStart int) string {
	if parenStart < 0 || parenStart >= len(s) || s[parenStart] != '(' {
		return ""
	}
	depth := 0
	for i := parenStart; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[parenStart+1 : i]
			}
		}
	}
	return s[parenStart+1:] // unmatched, return everything after
}

// extractConstraintColumns extracts column names from a constraint like PRIMARY KEY (col1, col2).
func extractConstraintColumns(def string) []string {
	parenIdx := strings.Index(def, "(")
	if parenIdx < 0 {
		return nil
	}
	body := extractParenBody(def, parenIdx)
	return extractConstraintColumnsFromStr(body)
}

// extractConstraintColumnsFromStr splits a comma-separated column list.
func extractConstraintColumnsFromStr(body string) []string {
	if body == "" {
		return nil
	}
	parts := strings.Split(body, ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		// Strip sort direction and nulls handling
		p = strings.TrimSpace(strings.Split(p, " ")[0])
		if p != "" {
			cols = append(cols, stripQuotes(p))
		}
	}
	return cols
}

// extractParenList extracts column names from a token like "table(col1, col2)".
func extractParenList(token string) []string {
	idx := strings.Index(token, "(")
	if idx < 0 {
		return nil
	}
	body := extractParenBody(token, idx)
	return extractConstraintColumnsFromStr(body)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
