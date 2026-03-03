package pgimport

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// DataLoader converts parsed rows into typed SyndrDB documents and batches them for insert.
type DataLoader struct {
	logger    *zap.SugaredLogger
	batchSize int

	// Schema info per table
	schemas map[string]*tableSchema
}

// tableSchema holds the column names and types for a table.
type tableSchema struct {
	columns []string
	types   []string // SyndrDB types for each column
}

// NewDataLoader creates a DataLoader with the given batch size.
func NewDataLoader(batchSize int, logger *zap.SugaredLogger) *DataLoader {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &DataLoader{
		logger:    logger,
		batchSize: batchSize,
		schemas:   make(map[string]*tableSchema),
	}
}

// RegisterSchema registers the column names and types for a table.
func (dl *DataLoader) RegisterSchema(tableName string, columns []string, bundleCmd *models.BundleCommand) {
	types := make([]string, len(columns))
	fieldMap := make(map[string]string, len(bundleCmd.Fields))
	for _, f := range bundleCmd.Fields {
		fieldMap[f.Name] = f.Type
	}
	for i, col := range columns {
		if t, ok := fieldMap[col]; ok {
			types[i] = t
		} else {
			types[i] = "string"
		}
	}
	dl.schemas[tableName] = &tableSchema{columns: columns, types: types}
}

// ConvertRow converts a single row of string values into a []models.KeyValue slice,
// applying type conversion based on the registered schema.
// Returns nil for the row if all values are NULL/empty (skip empty rows).
func (dl *DataLoader) ConvertRow(tableName string, rawValues []string, copyMode bool) ([]models.KeyValue, error) {
	schema, ok := dl.schemas[tableName]
	if !ok {
		return nil, fmt.Errorf("no schema registered for table %q", tableName)
	}

	if len(rawValues) > len(schema.columns) {
		rawValues = rawValues[:len(schema.columns)]
	}

	kvs := make([]models.KeyValue, 0, len(rawValues))
	for i, val := range rawValues {
		if i >= len(schema.columns) {
			break
		}

		colName := schema.columns[i]
		colType := schema.types[i]

		// Handle NULL
		if (copyMode && val == "") || (!copyMode && isInsertNull(val)) {
			// Skip NULL values — SyndrDB will use defaults
			continue
		}

		// Strip surrounding single quotes for INSERT values
		if !copyMode {
			val = stripSQLQuotes(val)
		}

		converted, err := convertValue(val, colType)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", colName, err)
		}

		kvs = append(kvs, models.KeyValue{Key: colName, Value: converted})
	}

	return kvs, nil
}

// BatchRows converts rows into batches of BulkDocumentCommand.
func (dl *DataLoader) BatchRows(tableName string, rows [][]string, copyMode bool, continueOnError bool) ([]*models.BulkDocumentCommand, []ImportWarning, []ImportError) {
	var warnings []ImportWarning
	var errors []ImportError

	var allKVs [][]models.KeyValue
	for i, row := range rows {
		kvs, err := dl.ConvertRow(tableName, row, copyMode)
		if err != nil {
			if continueOnError {
				errors = append(errors, ImportError{
					Phase:   "data",
					Object:  tableName,
					Message: err.Error(),
					Line:    i + 1,
				})
				continue
			}
			return nil, warnings, []ImportError{{
				Phase:   "data",
				Object:  tableName,
				Message: err.Error(),
				Line:    i + 1,
			}}
		}
		if len(kvs) > 0 {
			allKVs = append(allKVs, kvs)
		}
	}

	// Split into batches
	var batches []*models.BulkDocumentCommand
	for start := 0; start < len(allKVs); start += dl.batchSize {
		end := start + dl.batchSize
		if end > len(allKVs) {
			end = len(allKVs)
		}

		batch := &models.BulkDocumentCommand{
			CommandType: "BULK_ADD_DOCUMENTS",
			BundleName:  tableName,
			Documents:   allKVs[start:end],
		}
		batches = append(batches, batch)
	}

	return batches, warnings, errors
}

// isInsertNull checks if an INSERT value represents SQL NULL.
func isInsertNull(val string) bool {
	upper := strings.ToUpper(strings.TrimSpace(val))
	return upper == "NULL"
}

// isCopyNull checks if a COPY field is the NULL marker (\N).
func isCopyNull(val string) bool {
	return val == "\\N"
}

// stripSQLQuotes strips surrounding single quotes from SQL string values.
func stripSQLQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		inner := s[1 : len(s)-1]
		return strings.ReplaceAll(inner, "''", "'")
	}
	return s
}

// convertValue converts a string value to the appropriate Go type based on SyndrDB field type.
func convertValue(val string, syndrType string) (interface{}, error) {
	switch syndrType {
	case "int":
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			// Try parsing as float and truncating
			f, err2 := strconv.ParseFloat(val, 64)
			if err2 != nil {
				return nil, fmt.Errorf("cannot convert %q to int: %w", val, err)
			}
			return int64(f), nil
		}
		return n, nil

	case "float":
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to float: %w", val, err)
		}
		return f, nil

	case "bool":
		switch strings.ToLower(val) {
		case "t", "true", "1", "yes", "on":
			return true, nil
		case "f", "false", "0", "no", "off":
			return false, nil
		default:
			return nil, fmt.Errorf("cannot convert %q to bool", val)
		}

	case "datetime":
		return parseDateTime(val)

	case "date":
		return parseDate(val)

	case "string":
		return val, nil

	default:
		return val, nil
	}
}

// parseDateTime tries common PostgreSQL timestamp formats.
func parseDateTime(val string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999+00",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05+00",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		time.RFC3339,
	}
	for _, fmt := range formats {
		if t, err := time.Parse(fmt, val); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse datetime %q", val)
}

// parseDate tries common PostgreSQL date formats.
func parseDate(val string) (time.Time, error) {
	formats := []string{
		"2006-01-02",
		"01/02/2006",
		"02-Jan-2006",
	}
	for _, fmt := range formats {
		if t, err := time.Parse(fmt, val); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse date %q", val)
}
