package server

import (
	"fmt"
	"strconv"
	"time"

	"syndrdb/src/internal/pgimport"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// HandleImportCommand handles the IMPORT POSTGRES command.
// Syntax: IMPORT POSTGRES FROM '/path/to/dump.sql' INTO DATABASE "targetDB" [WITH OPTIONS (...)]
func HandleImportCommand(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager, session *Session, startTime time.Time) (*CommandResponse, error) {
	// Parse the IMPORT command
	importCmd, err := syndrQL.ParseImportCommand(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse IMPORT command", errors.LayerCommand)
	}

	logger.Infof("Starting PostgreSQL import: source=%s, target=%s", importCmd.SourceFile, importCmd.TargetDatabase)

	// Build import options
	s := settings.GetSettings()
	opts := pgimport.DefaultImportOptions()
	opts.SourceFile = importCmd.SourceFile
	opts.TargetDatabase = importCmd.TargetDatabase

	// Apply settings defaults
	if s.PGImportDefaultBatchSize > 0 {
		opts.BatchSize = s.PGImportDefaultBatchSize
	}
	if s.PGImportProgressInterval > 0 {
		opts.ProgressInterval = s.PGImportProgressInterval
	}
	if s.PGImportMaxErrors > 0 {
		opts.MaxErrors = s.PGImportMaxErrors
	}

	// Override with command options
	for key, val := range importCmd.Options {
		switch key {
		case "batch_size":
			if n, err := strconv.Atoi(val); err == nil {
				opts.BatchSize = n
			}
		case "create_indexes_after":
			opts.CreateIndexesAfter = val == "true"
		case "continue_on_error":
			opts.ContinueOnError = val == "true"
		case "dry_run":
			opts.DryRun = val == "true"
		case "max_errors":
			if n, err := strconv.Atoi(val); err == nil {
				opts.MaxErrors = n
			}
		case "progress_interval":
			if n, err := strconv.Atoi(val); err == nil {
				opts.ProgressInterval = n
			}
		case "format":
			switch val {
			case "plain":
				opts.Format = pgimport.FormatPlain
			case "custom":
				opts.Format = pgimport.FormatCustom
			default:
				opts.Format = pgimport.FormatAuto
			}
		}
	}

	// Create the import service
	importService := pgimport.NewPGImportService(
		serviceManager.BundleService,
		serviceManager.DatabaseService,
		logger,
	)

	// Execute the import
	report, err := importService.Import(opts)
	if err != nil && report == nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
			"import failed", errors.LayerCommand)
	}

	// Build response
	result := buildImportResult(report)

	cmdResponse := &CommandResponse{
		ResultCount:     1,
		Result:          result,
		ExecutionTimeMS: float64(time.Since(startTime).Milliseconds()),
	}

	if err != nil {
		errMsg := err.Error()
		cmdResponse.Error = &errMsg
	}

	return cmdResponse, nil
}

// buildImportResult formats the import report as a result map.
func buildImportResult(report *pgimport.ImportReport) map[string]interface{} {
	result := map[string]interface{}{
		"status":                string(report.Status),
		"bundles_created":       report.BundlesCreated,
		"bundles_skipped":       report.BundlesSkipped,
		"indexes_created":       report.IndexesCreated,
		"indexes_skipped":       report.IndexesSkipped,
		"relationships_created": report.RelationshipsCreated,
		"relationships_skipped": report.RelationshipsSkipped,
		"views_created":         report.ViewsCreated,
		"views_skipped":         report.ViewsSkipped,
		"records_imported":      report.RecordsImported,
		"records_skipped":       report.RecordsSkipped,
		"duration_ms":           report.Duration().Milliseconds(),
		"warning_count":         len(report.Warnings),
		"error_count":           len(report.Errors),
	}

	// Include first few warnings
	if len(report.Warnings) > 0 {
		maxWarnings := 20
		if len(report.Warnings) < maxWarnings {
			maxWarnings = len(report.Warnings)
		}
		warningStrs := make([]string, maxWarnings)
		for i := 0; i < maxWarnings; i++ {
			w := report.Warnings[i]
			warningStrs[i] = fmt.Sprintf("[%s] %s: %s", w.Phase, w.Object, w.Message)
		}
		result["warnings"] = warningStrs
	}

	// Include first few errors
	if len(report.Errors) > 0 {
		maxErrors := 10
		if len(report.Errors) < maxErrors {
			maxErrors = len(report.Errors)
		}
		errorStrs := make([]string, maxErrors)
		for i := 0; i < maxErrors; i++ {
			e := report.Errors[i]
			errorStrs[i] = fmt.Sprintf("[%s] %s: %s", e.Phase, e.Object, e.Message)
		}
		result["errors"] = errorStrs
	}

	return result
}
