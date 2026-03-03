package pgimport

import (
	"fmt"
	"os"
	"strings"
	"time"

	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// PGImportService orchestrates the import of PostgreSQL dumps into SyndrDB.
type PGImportService struct {
	bundleService   *bundle.BundleService
	databaseService *database.DatabaseService
	logger          *zap.SugaredLogger
}

// NewPGImportService creates a new import service.
func NewPGImportService(
	bundleService *bundle.BundleService,
	databaseService *database.DatabaseService,
	logger *zap.SugaredLogger,
) *PGImportService {
	return &PGImportService{
		bundleService:   bundleService,
		databaseService: databaseService,
		logger:          logger,
	}
}

// Import executes the full import pipeline.
func (s *PGImportService) Import(opts ImportOptions) (*ImportReport, error) {
	report := &ImportReport{
		StartTime: time.Now(),
		Status:    StatusSuccess,
	}

	defer func() {
		report.EndTime = time.Now()
	}()

	// Phase 1: Validate
	if err := s.validate(&opts); err != nil {
		report.Status = StatusFailed
		report.Errors = append(report.Errors, ImportError{
			Phase:   "validate",
			Message: err.Error(),
		})
		return report, err
	}

	// Phase 2: Convert format if needed
	filePath := opts.SourceFile
	if opts.Format == FormatCustom {
		plainPath, cleanup, err := ConvertCustomToPlain(opts.SourceFile, "", s.logger)
		if err != nil {
			report.Status = StatusFailed
			report.Errors = append(report.Errors, ImportError{
				Phase:   "format_convert",
				Message: err.Error(),
			})
			return report, err
		}
		defer cleanup()
		filePath = plainPath
	}

	// Get target database
	db, err := s.databaseService.GetDatabaseByName(opts.TargetDatabase)
	if err != nil {
		report.Status = StatusFailed
		report.Errors = append(report.Errors, ImportError{
			Phase:   "validate",
			Message: fmt.Sprintf("database %q not found: %v", opts.TargetDatabase, err),
		})
		return report, fmt.Errorf("database %q not found: %w", opts.TargetDatabase, err)
	}

	// Phase 3: First pass — parse and create schemas
	s.logger.Infof("pgimport: starting schema pass for %s", filePath)

	tables, indexes, fks, views, err := s.schemaPass(filePath, db, &opts, report)
	if err != nil {
		report.Status = StatusFailed
		return report, err
	}

	if opts.DryRun {
		s.logger.Infof("pgimport: dry run completed — %d bundles, %d indexes, %d relationships, %d views",
			report.BundlesCreated, len(indexes), len(fks), len(views))
		return report, nil
	}

	// Phase 4: Second pass — load data
	s.logger.Infof("pgimport: starting data pass for %s", filePath)

	err = s.dataPass(filePath, db, tables, &opts, report)
	if err != nil && !opts.ContinueOnError {
		report.Status = StatusFailed
		return report, err
	}

	// Phase 5: Post-load — create indexes, relationships, views
	s.logger.Infof("pgimport: starting post-load phase")

	if opts.CreateIndexesAfter {
		s.createIndexes(db, indexes, report)
	}
	s.createRelationships(db, fks, report)
	s.createViews(views, report)

	// Set final status
	if len(report.Errors) > 0 {
		report.Status = StatusPartial
	}

	s.logger.Infof("pgimport: completed — bundles=%d indexes=%d relationships=%d records=%d warnings=%d errors=%d duration=%v",
		report.BundlesCreated, report.IndexesCreated, report.RelationshipsCreated,
		report.RecordsImported, len(report.Warnings), len(report.Errors), report.Duration())

	return report, nil
}

// validate checks prerequisites.
func (s *PGImportService) validate(opts *ImportOptions) error {
	if opts.SourceFile == "" {
		return fmt.Errorf("source file is required")
	}

	info, err := os.Stat(opts.SourceFile)
	if err != nil {
		return fmt.Errorf("cannot access source file %q: %w", opts.SourceFile, err)
	}
	if info.IsDir() {
		return fmt.Errorf("source file %q is a directory", opts.SourceFile)
	}

	// Auto-detect format
	if opts.Format == FormatAuto {
		detected, err := DetectFormat(opts.SourceFile)
		if err != nil {
			return fmt.Errorf("cannot detect dump format: %w", err)
		}
		opts.Format = detected
		s.logger.Infof("pgimport: auto-detected format: %v", formatName(opts.Format))
	}

	// Apply defaults
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}
	if opts.MaxErrors <= 0 {
		opts.MaxErrors = 100
	}
	if opts.ProgressInterval <= 0 {
		opts.ProgressInterval = 10000
	}

	return nil
}

// schemaPass reads the dump file and creates bundles. Returns deferred items.
func (s *PGImportService) schemaPass(
	filePath string,
	db *models.Database,
	opts *ImportOptions,
	report *ImportReport,
) (
	tables map[string]*models.BundleCommand,
	indexes []*ParsedStatement,
	fks []fkItem,
	views []*PGView,
	err error,
) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("cannot open dump file: %w", err)
	}
	defer f.Close()

	parser := NewPlainFormatParser(f, s.logger)
	tables = make(map[string]*models.BundleCommand)

	for {
		stmt, err := parser.NextStatement()
		if err != nil {
			report.Warnings = append(report.Warnings, ImportWarning{
				Phase:   "schema",
				Message: fmt.Sprintf("parse error: %v", err),
			})
			continue
		}
		if stmt == nil {
			break
		}

		switch stmt.Type {
		case StmtCreateTable:
			cmd, warnings := ConvertTable(stmt)
			report.Warnings = append(report.Warnings, warnings...)

			if opts.DryRun {
				report.BundlesCreated++
				tables[stmt.TableName] = cmd
				continue
			}

			// Create the bundle
			_, createErr := s.bundleService.AddBundle(s.databaseService, db, cmd)
			if createErr != nil {
				if strings.Contains(createErr.Error(), "already exists") {
					report.BundlesSkipped++
					report.Warnings = append(report.Warnings, ImportWarning{
						Phase:   "schema",
						Object:  stmt.TableName,
						Message: "bundle already exists, skipping",
					})
				} else {
					report.Errors = append(report.Errors, ImportError{
						Phase:   "schema",
						Object:  stmt.TableName,
						Message: createErr.Error(),
					})
					if !opts.ContinueOnError {
						return nil, nil, nil, nil, createErr
					}
				}
			} else {
				report.BundlesCreated++
				s.logger.Infof("pgimport: created bundle %q", stmt.TableName)
			}
			tables[stmt.TableName] = cmd

		case StmtCreateIndex:
			indexes = append(indexes, stmt)

		case StmtAlterTable:
			for _, c := range stmt.Constraints {
				if c.Type == ConstraintForeignKey {
					fks = append(fks, fkItem{tableName: stmt.TableName, constraint: c})
				}
			}

		case StmtCreateView:
			if stmt.View != nil {
				views = append(views, stmt.View)
			}

		case StmtCopyFrom, StmtInsertInto:
			// Data statements — skip during schema pass
			// For COPY, we still need to consume the data lines
			// The parser handles this internally

		case StmtSkip:
			// Silently skip
		}
	}

	return tables, indexes, fks, views, nil
}

type fkItem struct {
	tableName  string
	constraint PGConstraint
}

// dataPass re-reads the file and loads COPY/INSERT data.
func (s *PGImportService) dataPass(
	filePath string,
	db *models.Database,
	tables map[string]*models.BundleCommand,
	opts *ImportOptions,
	report *ImportReport,
) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("cannot open dump file for data pass: %w", err)
	}
	defer f.Close()

	parser := NewPlainFormatParser(f, s.logger)
	loader := NewDataLoader(opts.BatchSize, s.logger)

	// Register schemas from the tables created in schema pass
	for name, cmd := range tables {
		// Use field names from the BundleCommand
		columns := make([]string, len(cmd.Fields))
		for i, f := range cmd.Fields {
			columns[i] = f.Name
		}
		loader.RegisterSchema(name, columns, cmd)
	}

	totalImported := 0

	for {
		stmt, err := parser.NextStatement()
		if err != nil {
			report.Warnings = append(report.Warnings, ImportWarning{
				Phase:   "data",
				Message: fmt.Sprintf("parse error: %v", err),
			})
			continue
		}
		if stmt == nil {
			break
		}

		var tableName string
		var rows [][]string
		var copyMode bool

		switch stmt.Type {
		case StmtCopyFrom:
			if stmt.InsertRows == nil {
				continue // Just the COPY header, data comes next
			}
			tableName = stmt.TableName
			rows = stmt.InsertRows
			copyMode = true

			// If COPY had explicit columns, re-register schema with them
			if stmt.CopyMeta != nil && len(stmt.CopyMeta.Columns) > 0 {
				if cmd, ok := tables[tableName]; ok {
					loader.RegisterSchema(tableName, stmt.CopyMeta.Columns, cmd)
				}
			}

		case StmtInsertInto:
			tableName = stmt.TableName
			rows = stmt.InsertRows
			copyMode = false

			// If INSERT had explicit columns, re-register schema with them
			if len(stmt.InsertColumns) > 0 {
				if cmd, ok := tables[tableName]; ok {
					loader.RegisterSchema(tableName, stmt.InsertColumns, cmd)
				}
			}

		default:
			continue
		}

		if len(rows) == 0 {
			continue
		}

		// Check if we have the bundle
		bndl, bErr := s.bundleService.GetBundleByName(db, tableName)
		if bErr != nil {
			report.Warnings = append(report.Warnings, ImportWarning{
				Phase:   "data",
				Object:  tableName,
				Message: fmt.Sprintf("bundle not found, skipping data: %v", bErr),
			})
			continue
		}

		// Convert and batch
		batches, batchWarnings, batchErrors := loader.BatchRows(tableName, rows, copyMode, opts.ContinueOnError)
		report.Warnings = append(report.Warnings, batchWarnings...)

		if len(batchErrors) > 0 && !opts.ContinueOnError {
			for _, e := range batchErrors {
				report.Errors = append(report.Errors, e)
			}
			report.RecordsSkipped += len(batchErrors)
			return fmt.Errorf("data load failed for table %q: %s", tableName, batchErrors[0].Message)
		}
		report.RecordsSkipped += len(batchErrors)
		for _, e := range batchErrors {
			report.Errors = append(report.Errors, e)
		}

		// Insert batches
		for _, batch := range batches {
			_, insertErr := s.bundleService.AddDocumentsToBundle(db, bndl, batch)
			if insertErr != nil {
				report.Errors = append(report.Errors, ImportError{
					Phase:   "data",
					Object:  tableName,
					Message: insertErr.Error(),
				})
				if !opts.ContinueOnError {
					return insertErr
				}
				report.RecordsSkipped += len(batch.Documents)
			} else {
				report.RecordsImported += len(batch.Documents)
				totalImported += len(batch.Documents)
			}

			// Progress logging
			if opts.ProgressInterval > 0 && totalImported%opts.ProgressInterval == 0 && totalImported > 0 {
				s.logger.Infof("pgimport: loaded %d records...", totalImported)
			}
		}
	}

	// Check error limit
	if len(report.Errors) > opts.MaxErrors {
		return fmt.Errorf("import aborted: exceeded maximum errors (%d)", opts.MaxErrors)
	}

	return nil
}

// createIndexes creates deferred indexes.
func (s *PGImportService) createIndexes(db *models.Database, indexes []*ParsedStatement, report *ImportReport) {
	for _, stmt := range indexes {
		conv, warning := ConvertIndex(stmt)
		if warning.Message != "" {
			report.Warnings = append(report.Warnings, warning)
		}
		if conv == nil {
			report.IndexesSkipped++
			continue
		}

		bndl, err := s.bundleService.GetBundleByName(db, conv.BundleName)
		if err != nil {
			report.IndexesSkipped++
			report.Warnings = append(report.Warnings, ImportWarning{
				Phase:   "index",
				Object:  stmt.IndexDef.Name,
				Message: fmt.Sprintf("bundle %q not found for index: %v", conv.BundleName, err),
			})
			continue
		}

		// Build CreateIndexCommand
		indexCmd := &models.CreateIndexCommand{
			IndexName:      stmt.IndexDef.Name,
			BundleName:     conv.BundleName,
			WherePredicate: conv.Where,
			Expression:     conv.Expression,
		}

		// Map fields
		for _, col := range conv.Fields {
			indexCmd.Fields = append(indexCmd.Fields, models.FieldDefinition{
				Name:     col,
				IsUnique: conv.Unique,
			})
		}

		var createErr error
		switch conv.IndexType {
		case "B-INDEX":
			indexCmd.IndexType = "BTree"
			createErr = bundle.CreateBTreeIndex(s.bundleService, bndl, indexCmd)
		case "HASH INDEX":
			indexCmd.IndexType = "Hash"
			createErr = bundle.CreateHashIndex(s.bundleService, bndl, indexCmd)
		case "BRIN INDEX":
			indexCmd.IndexType = "brin"
			createErr = bundle.CreateBRINIndex(s.bundleService, bndl, indexCmd)
		}

		if createErr != nil {
			report.IndexesSkipped++
			report.Warnings = append(report.Warnings, ImportWarning{
				Phase:   "index",
				Object:  stmt.IndexDef.Name,
				Message: fmt.Sprintf("index creation failed: %v", createErr),
			})
		} else {
			report.IndexesCreated++
			s.logger.Infof("pgimport: created %s %q on %q", conv.IndexType, stmt.IndexDef.Name, conv.BundleName)
		}
	}
}

// createRelationships creates deferred FK relationships.
func (s *PGImportService) createRelationships(db *models.Database, fks []fkItem, report *ImportReport) {
	for _, fk := range fks {
		conv := ConvertFK(fk.tableName, fk.constraint)
		if conv == nil {
			report.RelationshipsSkipped++
			continue
		}

		if conv.Warning != "" {
			report.Warnings = append(report.Warnings, ImportWarning{
				Phase:   "relationship",
				Object:  conv.Command.Name,
				Message: conv.Warning,
			})
		}

		bndl, err := s.bundleService.GetBundleByName(db, fk.tableName)
		if err != nil {
			report.RelationshipsSkipped++
			report.Warnings = append(report.Warnings, ImportWarning{
				Phase:   "relationship",
				Object:  conv.Command.Name,
				Message: fmt.Sprintf("source bundle %q not found: %v", fk.tableName, err),
			})
			continue
		}

		err = s.bundleService.AddRelationshipToBundle(bndl, conv.Command)
		if err != nil {
			report.RelationshipsSkipped++
			report.Warnings = append(report.Warnings, ImportWarning{
				Phase:   "relationship",
				Object:  conv.Command.Name,
				Message: fmt.Sprintf("relationship creation failed: %v", err),
			})
		} else {
			report.RelationshipsCreated++
			s.logger.Infof("pgimport: created relationship %q on %q", conv.Command.Name, fk.tableName)
		}
	}
}

// createViews logs view creation as skipped (SyndrQL view translation is best-effort).
func (s *PGImportService) createViews(views []*PGView, report *ImportReport) {
	for _, v := range views {
		// View SQL translation from PostgreSQL to SyndrQL is non-trivial
		// and may fail for many real-world views. Log as skipped with warning.
		report.ViewsSkipped++
		report.Warnings = append(report.Warnings, ImportWarning{
			Phase:   "view",
			Object:  v.Name,
			Message: "view import not yet supported; SQL dialect translation required",
		})
	}
}

func formatName(f ImportFormat) string {
	switch f {
	case FormatPlain:
		return "plain SQL (-Fp)"
	case FormatCustom:
		return "custom (-Fc)"
	default:
		return "auto"
	}
}
