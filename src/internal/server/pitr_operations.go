package server

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"syndrdb/src/internal/backup"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
)

// CreateRestorePoint handles the CREATE RESTORE POINT "name" command.
// It creates a named recovery target with the current WAL LSN and commit sequence.
func CreateRestorePoint(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager) (*CommandResponse, error) {
	args := settings.GetSettings()

	if serviceManager.WALManager == nil || !args.WALEnabled {
		return nil, errors.New(errors.ERR_INTERNAL_WAL,
			"WAL is not enabled; restore points require WAL",
			errors.LayerWAL)
	}

	// Parse restore point name: CREATE RESTORE POINT "name"
	name, err := parseRestorePointName(command)
	if err != nil {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			err.Error(),
			errors.LayerCommand)
	}

	rp, err := serviceManager.WALManager.CreateRestorePoint(name)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
			"failed to create restore point", errors.LayerWAL)
	}

	result := fmt.Sprintf("Restore point '%s' created at LSN %d, commit sequence %d, timestamp %s",
		rp.Name, rp.LSN, rp.CommitSeq, rp.Timestamp.Format(time.RFC3339))

	return &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}, nil
}

// ShowRestorePoints handles the SHOW RESTORE POINTS command.
func ShowRestorePoints(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager) (*CommandResponse, error) {
	args := settings.GetSettings()

	if serviceManager.WALManager == nil || !args.WALEnabled {
		return nil, errors.New(errors.ERR_INTERNAL_WAL,
			"WAL is not enabled; restore points require WAL",
			errors.LayerWAL)
	}

	points := serviceManager.WALManager.GetRestorePointManager().List()

	if len(points) == 0 {
		return &CommandResponse{
			ResultCount: 0,
			Result:      "No restore points found.",
		}, nil
	}

	results := make([]map[string]interface{}, 0, len(points))
	for _, rp := range points {
		results = append(results, map[string]interface{}{
			"Name":           rp.Name,
			"LSN":            rp.LSN,
			"CommitSequence": rp.CommitSeq,
			"Timestamp":      rp.Timestamp.Format(time.RFC3339),
			"DatabaseName":   rp.DatabaseName,
		})
	}

	return &CommandResponse{
		ResultCount: len(results),
		Result:      results,
	}, nil
}

// ShowWALArchive handles the SHOW WAL ARCHIVE command.
func ShowWALArchive(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager) (*CommandResponse, error) {
	args := settings.GetSettings()

	if !args.WALArchiveEnabled {
		return &CommandResponse{
			ResultCount: 0,
			Result:      "WAL archiving is not enabled. Set wal_archive_enabled=true to enable.",
		}, nil
	}

	// Read the archive manifest directly from the backend
	archiveDir := args.WALArchiveDir
	backend, err := backup.NewWALArchiveBackendFromDir(archiveDir)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
			"failed to read WAL archive", errors.LayerWAL)
	}

	entries, err := backend.List()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
			"failed to list WAL archive", errors.LayerWAL)
	}

	if len(entries) == 0 {
		return &CommandResponse{
			ResultCount: 0,
			Result:      "WAL archive is empty.",
		}, nil
	}

	results := make([]map[string]interface{}, 0, len(entries))
	for _, entry := range entries {
		results = append(results, map[string]interface{}{
			"FileName":       entry.FileName,
			"FirstLSN":       entry.FirstLSN,
			"LastLSN":        entry.LastLSN,
			"FirstTimestamp": entry.FirstTimestamp.Format(time.RFC3339),
			"LastTimestamp":  entry.LastTimestamp.Format(time.RFC3339),
			"SizeBytes":     entry.SizeBytes,
			"Compressed":    entry.Compressed,
			"ArchivedAt":    entry.ArchivedAt.Format(time.RFC3339),
		})
	}

	return &CommandResponse{
		ResultCount: len(results),
		Result:      results,
	}, nil
}

// PITRRestore handles RESTORE DATABASE ... TO POINT IN TIME / TO LSN / TO RESTORE POINT commands.
func PITRRestore(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager) (*CommandResponse, error) {
	args := settings.GetSettings()

	if serviceManager.WALManager == nil || !args.WALEnabled {
		return nil, errors.New(errors.ERR_INTERNAL_WAL,
			"WAL is not enabled; PITR requires WAL",
			errors.LayerWAL)
	}

	// Parse the PITR restore command
	opts, err := parsePITRRestoreCommand(command)
	if err != nil {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			err.Error(),
			errors.LayerCommand)
	}

	// Create PITR service
	pitrService := backup.NewPITRService(
		serviceManager.BundleService,
		serviceManager.DatabaseService,
		serviceManager.LockService,
		serviceManager.WALManager,
		logger,
	)

	// Execute PITR recovery
	result, err := pitrService.RecoverToPointInTime(opts)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
			"PITR recovery failed", errors.LayerWAL)
	}

	resultStr := fmt.Sprintf("PITR restore complete. Restored to LSN %d, commit sequence %d, time %s. "+
		"Replayed %d operations from %d WAL files (%d skipped).",
		result.RestoredToLSN, result.RestoredToSeq,
		result.RestoredToTime.Format(time.RFC3339),
		result.RedoneOps, result.WALFilesReplayed, result.SkippedOps)

	return &CommandResponse{
		ResultCount: 1,
		Result:      resultStr,
	}, nil
}

// parseRestorePointName extracts the name from CREATE RESTORE POINT "name"
func parseRestorePointName(command string) (string, error) {
	// Normalize: CREATE RESTORE POINT "name"
	lower := strings.ToLower(strings.TrimSpace(command))
	if !strings.HasPrefix(lower, "create restore point") {
		return "", fmt.Errorf("expected CREATE RESTORE POINT command")
	}

	// Extract the part after "create restore point"
	rest := strings.TrimSpace(command[len("create restore point"):])

	// Remove quotes if present
	name := strings.Trim(rest, "\"' ")
	if name == "" {
		return "", fmt.Errorf("restore point name cannot be empty")
	}

	return name, nil
}

// parsePITRRestoreCommand parses RESTORE DATABASE FROM "path" AS "db" TO ... commands.
func parsePITRRestoreCommand(command string) (backup.PITRRecoveryOptions, error) {
	var opts backup.PITRRecoveryOptions

	lower := strings.ToLower(command)

	// Extract backup path: FROM "path"
	fromIdx := strings.Index(lower, "from ")
	if fromIdx < 0 {
		return opts, fmt.Errorf("expected FROM clause in PITR restore command")
	}
	afterFrom := strings.TrimSpace(command[fromIdx+5:])

	// Use the existing extractQuotedString (returns string, error)
	backupPath, err := extractQuotedString(afterFrom)
	if err != nil {
		return opts, fmt.Errorf("invalid backup path: %w", err)
	}
	opts.BackupPath = backupPath

	// Extract target DB name: AS "name"
	asIdx := strings.Index(lower, " as ")
	if asIdx >= 0 {
		afterAS := strings.TrimSpace(command[asIdx+4:])
		dbName, err := extractQuotedString(afterAS)
		if err != nil {
			return opts, fmt.Errorf("invalid database name: %w", err)
		}
		opts.TargetDBName = dbName
	}

	// Extract WAL archive dir (use default if not specified)
	s := settings.GetSettings()
	opts.WALArchiveDir = s.WALArchiveDir

	// Parse target: TO POINT IN TIME 'timestamp' / TO LSN <number> / TO RESTORE POINT "name"
	if idx := strings.Index(lower, "to point in time"); idx >= 0 {
		afterTO := strings.TrimSpace(command[idx+len("to point in time"):])
		tsStr := strings.Trim(afterTO, "\"' ")
		t, err := time.Parse(time.RFC3339, tsStr)
		if err != nil {
			return opts, fmt.Errorf("invalid timestamp '%s': %w", tsStr, err)
		}
		opts.Target = backup.PITRRecoveryTarget{
			Type:       backup.PITRTargetTimestamp,
			TargetTime: t,
		}
	} else if idx := strings.Index(lower, "to restore point"); idx >= 0 {
		afterTO := strings.TrimSpace(command[idx+len("to restore point"):])
		name := strings.Trim(afterTO, "\"' ")
		if name == "" {
			return opts, fmt.Errorf("restore point name cannot be empty")
		}
		opts.Target = backup.PITRRecoveryTarget{
			Type:             backup.PITRTargetRestorePoint,
			RestorePointName: name,
		}
	} else if idx := strings.Index(lower, "to lsn"); idx >= 0 {
		afterTO := strings.TrimSpace(command[idx+len("to lsn"):])
		var lsn uint64
		if _, err := fmt.Sscanf(afterTO, "%d", &lsn); err != nil {
			return opts, fmt.Errorf("invalid LSN value: %w", err)
		}
		opts.Target = backup.PITRRecoveryTarget{
			Type:      backup.PITRTargetLSN,
			TargetLSN: lsn,
		}
	} else {
		return opts, fmt.Errorf("expected TO POINT IN TIME, TO LSN, or TO RESTORE POINT clause")
	}

	return opts, nil
}
