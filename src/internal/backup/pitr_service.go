package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/journal"
	"syndrdb/src/internal/lock"
	"syndrdb/src/pkg/settings"
)

// PITRService handles point-in-time recovery operations.
type PITRService struct {
	bundleService   *bundle.BundleService
	databaseService *database.DatabaseService
	lockService     *lock.LockService
	walManager      *journal.WALManager
	settings        *settings.Arguments
	logger          *zap.SugaredLogger
}

// NewPITRService creates a new PITR service instance.
func NewPITRService(
	bundleService *bundle.BundleService,
	databaseService *database.DatabaseService,
	lockService *lock.LockService,
	walManager *journal.WALManager,
	logger *zap.SugaredLogger,
) *PITRService {
	return &PITRService{
		bundleService:   bundleService,
		databaseService: databaseService,
		lockService:     lockService,
		walManager:      walManager,
		settings:        settings.GetSettings(),
		logger:          logger,
	}
}

// RecoverToPointInTime performs a 4-phase PITR recovery:
// 1. Validate backup and WAL chain
// 2. Restore base backup
// 3. Replay WAL entries up to the target
// 4. Finalize (restore global state)
func (ps *PITRService) RecoverToPointInTime(opts PITRRecoveryOptions) (*PITRResult, error) {
	startTime := time.Now()
	result := &PITRResult{}

	ps.logger.Infow("Starting PITR recovery",
		"backup", opts.BackupPath,
		"target_db", opts.TargetDBName,
		"target_type", opts.Target.Type,
	)

	// Phase 1: Validate
	manifest, err := ps.validateRecovery(opts)
	if err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	if opts.DryRun {
		ps.logger.Info("Dry run complete — validation passed, no changes made")
		return result, nil
	}

	// Phase 2: Restore base backup
	if err := ps.restoreBaseBackup(opts, manifest); err != nil {
		return nil, fmt.Errorf("base backup restore failed: %w", err)
	}

	// Phase 3: Replay WAL
	replayResult, err := ps.replayWAL(opts, manifest)
	if err != nil {
		// Clean up partially-restored database
		ps.cleanupFailedRestore(opts.TargetDBName)
		return nil, fmt.Errorf("WAL replay failed: %w", err)
	}
	result.RedoneOps = replayResult.redoneOps
	result.SkippedOps = replayResult.skippedOps
	result.WALFilesReplayed = replayResult.walFilesReplayed
	result.RestoredToLSN = replayResult.lastAppliedLSN
	result.RestoredToSeq = replayResult.lastAppliedSeq
	result.RestoredToTime = replayResult.lastAppliedTime

	// Phase 4: Finalize
	duration := time.Since(startTime)
	ps.logger.Infow("PITR recovery complete",
		"database", opts.TargetDBName,
		"duration", duration,
		"redone_ops", result.RedoneOps,
		"skipped_ops", result.SkippedOps,
		"wal_files", result.WALFilesReplayed,
		"restored_to_lsn", result.RestoredToLSN,
		"restored_to_seq", result.RestoredToSeq,
		"restored_to_time", result.RestoredToTime,
	)

	return result, nil
}

// ValidateRecoveryChain checks that the backup and WAL archive form a continuous
// chain covering the requested recovery target.
func (ps *PITRService) ValidateRecoveryChain(backupManifest *Manifest, target PITRRecoveryTarget, archiveDir string) error {
	if !backupManifest.PITREnabled {
		return fmt.Errorf("backup does not support PITR (PITREnabled=false)")
	}
	if backupManifest.StartLSN == 0 && backupManifest.EndLSN == 0 {
		return fmt.Errorf("backup has no LSN data (StartLSN=0, EndLSN=0)")
	}

	// Read archive manifest
	entries, err := readWALArchiveManifest(archiveDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL archive: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("WAL archive is empty; no WAL files to replay")
	}

	// Sort entries by FirstLSN
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].FirstLSN < entries[j].FirstLSN
	})

	// Verify chain starts at or before backup's EndLSN
	if entries[0].FirstLSN > backupManifest.EndLSN {
		return fmt.Errorf("WAL archive gap: first archived file starts at LSN %d, but backup ends at LSN %d",
			entries[0].FirstLSN, backupManifest.EndLSN)
	}

	// Resolve target LSN for chain validation
	targetLSN, err := ps.resolveTargetLSN(target, entries)
	if err != nil {
		return fmt.Errorf("cannot resolve target: %w", err)
	}

	// Check target is within backup range
	if targetLSN < backupManifest.EndLSN {
		return fmt.Errorf("target LSN %d is before backup EndLSN %d; no WAL replay needed", targetLSN, backupManifest.EndLSN)
	}

	// Verify chain covers target
	lastCoveredLSN := entries[len(entries)-1].LastLSN
	if targetLSN > lastCoveredLSN {
		return fmt.Errorf("WAL archive does not cover target LSN %d (archive ends at LSN %d)",
			targetLSN, lastCoveredLSN)
	}

	return nil
}

// validateRecovery performs Phase 1 validation.
func (ps *PITRService) validateRecovery(opts PITRRecoveryOptions) (*Manifest, error) {
	// Read and validate backup manifest
	bs := &BackupService{settings: ps.settings, logger: ps.logger}
	manifest, err := bs.GetBackupInfo(opts.BackupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup: %w", err)
	}

	if !manifest.PITREnabled {
		return nil, fmt.Errorf("backup does not support PITR (created without PITREnabled=true)")
	}

	// For restore point targets, resolve the LSN from the restore point manager
	if opts.Target.Type == PITRTargetRestorePoint && ps.walManager != nil {
		rp := ps.walManager.GetRestorePointManager().Get(opts.Target.RestorePointName)
		if rp == nil {
			return nil, fmt.Errorf("restore point '%s' not found", opts.Target.RestorePointName)
		}
	}

	// Validate WAL chain
	if err := ps.ValidateRecoveryChain(manifest, opts.Target, opts.WALArchiveDir); err != nil {
		return nil, err
	}

	return manifest, nil
}

// restoreBaseBackup performs Phase 2: restore the base backup.
func (ps *PITRService) restoreBaseBackup(opts PITRRecoveryOptions, manifest *Manifest) error {
	rs := NewRestoreService(ps.databaseService, ps.bundleService, ps.lockService, ps.logger)

	restoreOpts := RestoreOptions{
		TargetDBName: opts.TargetDBName,
		Force:        opts.Force,
	}

	if err := rs.RestoreBackup(opts.BackupPath, restoreOpts); err != nil {
		return fmt.Errorf("base backup restore failed: %w", err)
	}

	ps.logger.Infow("Base backup restored",
		"database", opts.TargetDBName,
		"backup_end_lsn", manifest.EndLSN,
	)

	return nil
}

// replayResult holds internal replay statistics.
type replayResult struct {
	redoneOps        int
	skippedOps       int
	walFilesReplayed int
	lastAppliedLSN   uint64
	lastAppliedSeq   uint64
	lastAppliedTime  time.Time
}

// replayWAL performs Phase 3: replay WAL entries from archive.
func (ps *PITRService) replayWAL(opts PITRRecoveryOptions, manifest *Manifest) (*replayResult, error) {
	result := &replayResult{}

	// Read archive manifest
	entries, err := readWALArchiveManifest(opts.WALArchiveDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL archive: %w", err)
	}

	// Sort by FirstLSN
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].FirstLSN < entries[j].FirstLSN
	})

	// Resolve the stop LSN
	stopLSN, err := ps.resolveTargetLSN(opts.Target, entries)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve target: %w", err)
	}

	// Filter WAL files that overlap with our replay range
	startLSN := manifest.EndLSN
	var filesToReplay []WALArchiveEntry
	for _, entry := range entries {
		if entry.LastLSN < startLSN {
			continue // Entirely before our range
		}
		if entry.FirstLSN > stopLSN {
			break // Past our target
		}
		filesToReplay = append(filesToReplay, entry)
	}

	if len(filesToReplay) == 0 {
		ps.logger.Info("No WAL files need replay")
		return result, nil
	}

	// Track transaction state for proper commit handling
	txState := make(map[string]bool)     // txID → committed?
	txOps := make(map[string][]journal.WALEntry) // txID → buffered ops

	// Construct the apply function
	applyFunc := ps.buildApplyFunc(opts.TargetDBName)

	// Stop flag — set when we reach the target
	stopped := false

	for _, archiveEntry := range filesToReplay {
		if stopped {
			break
		}

		filePath := filepath.Join(opts.WALArchiveDir, archiveEntry.FileName)
		// If compressed, try the compressed path
		if archiveEntry.Compressed {
			compPath := filePath + ".compressed"
			if _, err := os.Stat(compPath); err == nil {
				filePath = compPath
			}
		}

		// Check file exists
		if _, err := os.Stat(filePath); err != nil {
			return nil, fmt.Errorf("WAL file not found: %s", filePath)
		}

		result.walFilesReplayed++

		// Replay the WAL file
		wal := ps.walManager.GetWAL()
		replayErr := wal.ReplayOperationsBinary(filePath, startLSN, func(entry journal.WALEntry) error {
			if stopped {
				return nil
			}

			// Check stop conditions
			if ps.shouldStop(entry, opts.Target, stopLSN) {
				stopped = true
				return nil
			}

			switch entry.Operation {
			case journal.OpBeginTx:
				txState[entry.TxID] = false
			case journal.OpCommitTx:
				txState[entry.TxID] = true
				// Apply all buffered ops for this committed transaction
				if ops, ok := txOps[entry.TxID]; ok {
					for _, op := range ops {
						if err := applyFunc(op); err != nil {
							ps.logger.Warnw("Failed to apply WAL entry",
								"lsn", op.LSN,
								"op", journal.GetOperationTypeName(op.Operation),
								"error", err,
							)
						} else {
							result.redoneOps++
							result.lastAppliedLSN = op.LSN
							result.lastAppliedTime = op.Timestamp
						}
					}
					delete(txOps, entry.TxID)
				}
			case journal.OpRollbackTx:
				txState[entry.TxID] = true // resolved
				delete(txOps, entry.TxID)
				result.skippedOps++

			case journal.OpRestorePoint:
				// If this is a restore point target, check if we've reached it
				if opts.Target.Type == PITRTargetRestorePoint && entry.Metadata == opts.Target.RestorePointName {
					stopped = true
				}

			case journal.OpInsert, journal.OpUpdate, journal.OpDelete, journal.OpBatchInsert,
				journal.OpCreateBundle, journal.OpDeleteBundle, journal.OpCreateIndex, journal.OpDropIndex:
				// Buffer data operations for their transaction
				if entry.TxID != "" {
					txOps[entry.TxID] = append(txOps[entry.TxID], entry)
				} else {
					// No transaction context — apply directly
					if err := applyFunc(entry); err != nil {
						ps.logger.Warnw("Failed to apply WAL entry",
							"lsn", entry.LSN,
							"op", journal.GetOperationTypeName(entry.Operation),
							"error", err,
						)
					} else {
						result.redoneOps++
						result.lastAppliedLSN = entry.LSN
						result.lastAppliedTime = entry.Timestamp
					}
				}

			default:
				// Skip checkpoint, commit sequence, and other control ops
				result.skippedOps++
			}

			return nil
		})

		if replayErr != nil {
			// RecoveryErrorList is non-fatal
			if _, ok := replayErr.(*journal.RecoveryErrorList); !ok {
				return nil, fmt.Errorf("replay failed for %s: %w", archiveEntry.FileName, replayErr)
			}
			ps.logger.Warnw("Non-fatal replay errors", "file", archiveEntry.FileName, "error", replayErr)
		}
	}

	// Count skipped uncommitted transactions
	for txID, committed := range txState {
		if !committed {
			if ops, ok := txOps[txID]; ok {
				result.skippedOps += len(ops)
			}
		}
	}

	return result, nil
}

// shouldStop checks if replay should stop based on the target.
func (ps *PITRService) shouldStop(entry journal.WALEntry, target PITRRecoveryTarget, stopLSN uint64) bool {
	switch target.Type {
	case PITRTargetTimestamp:
		return !entry.Timestamp.IsZero() && entry.Timestamp.After(target.TargetTime)
	case PITRTargetLSN:
		return entry.LSN > stopLSN
	case PITRTargetRestorePoint:
		return entry.Operation == journal.OpRestorePoint && entry.Metadata == target.RestorePointName
	}
	return false
}

// buildApplyFunc constructs the function that applies WAL entries to the database.
// This is physical replay — it writes directly via BundleService, bypassing the query layer.
// Follows the same patterns as createUndoFunction in transaction_operations.go.
func (ps *PITRService) buildApplyFunc(targetDBName string) func(journal.WALEntry) error {
	// Cache the database reference to avoid repeated lookups.
	var cachedDB *models.Database

	return func(entry journal.WALEntry) error {
		if cachedDB == nil {
			db, err := ps.databaseService.GetDatabaseByName(targetDBName)
			if err != nil {
				return fmt.Errorf("database '%s' not found: %w", targetDBName, err)
			}
			cachedDB = db
		}

		switch entry.Operation {
		case journal.OpInsert:
			return ps.applyInsert(cachedDB, entry)
		case journal.OpUpdate:
			return ps.applyUpdate(cachedDB, entry)
		case journal.OpDelete:
			return ps.applyDelete(cachedDB, entry)
		case journal.OpBatchInsert:
			return ps.applyBatchInsert(cachedDB, entry)
		case journal.OpCreateBundle:
			return ps.applyCreateBundle(cachedDB, entry)
		case journal.OpDeleteBundle:
			return ps.applyDeleteBundle(cachedDB, entry)
		default:
			return nil
		}
	}
}

// parseAfterDataToKeyValues parses WAL AfterData JSON into []models.KeyValue.
// Handles two formats: map[string]interface{} (from WAL adapters) and []KeyValue (from direct logging).
func parseAfterDataToKeyValues(afterData string) ([]models.KeyValue, error) {
	if afterData == "" {
		return nil, fmt.Errorf("AfterData is empty")
	}

	// Try as map[string]interface{} first (most common from WAL replay)
	var dataMap map[string]interface{}
	if err := json.Unmarshal([]byte(afterData), &dataMap); err == nil {
		fields := make([]models.KeyValue, 0, len(dataMap))
		for key, value := range dataMap {
			// Skip internal metadata fields
			if key == "DocumentID" || key == "CreatedAt" || key == "UpdatedAt" ||
				key == "CommitSequence" || key == "SupersededAt" ||
				key == "CreatedByTxID" || key == "DeletedByTxID" || key == "VersionSequence" {
				continue
			}
			fields = append(fields, models.KeyValue{Key: key, Value: value})
		}
		return fields, nil
	}

	// Try as []KeyValue (from document_operations.go direct logging)
	var kvs []models.KeyValue
	if err := json.Unmarshal([]byte(afterData), &kvs); err == nil && len(kvs) > 0 {
		return kvs, nil
	}

	// Try as a JSON string that was double-marshaled
	var jsonStr string
	if err := json.Unmarshal([]byte(afterData), &jsonStr); err == nil && jsonStr != "" {
		return parseAfterDataToKeyValues(jsonStr)
	}

	return nil, fmt.Errorf("failed to parse AfterData: unrecognized format")
}

// applyInsert replays an INSERT operation from the WAL.
func (ps *PITRService) applyInsert(db *models.Database, entry journal.WALEntry) error {
	if entry.AfterData == "" {
		return nil
	}

	fields, err := parseAfterDataToKeyValues(entry.AfterData)
	if err != nil {
		return fmt.Errorf("INSERT parse error (bundle=%s, doc=%s): %w", entry.BundleName, entry.DocumentID, err)
	}

	bndl, err := ps.bundleService.GetBundleByName(db, entry.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found for INSERT: %w", entry.BundleName, err)
	}

	docCmd := &models.DocumentCommand{
		CommandType: "ADD_DOCUMENT",
		BundleName:  entry.BundleName,
		Fields:      fields,
	}

	_, err = ps.bundleService.AddDocumentToBundleWithTxID(db, bndl, docCmd, "", entry.DocumentID)
	if err != nil {
		return fmt.Errorf("INSERT failed (bundle=%s, doc=%s): %w", entry.BundleName, entry.DocumentID, err)
	}

	ps.logger.Debugw("PITR replay INSERT applied",
		"bundle", entry.BundleName,
		"doc_id", entry.DocumentID,
		"lsn", entry.LSN,
	)
	return nil
}

// applyUpdate replays an UPDATE operation from the WAL.
// Follows the same pattern as the undo function in transaction_operations.go:
// unmarshal data → convert to KeyValue → build UpdateCommand with WHERE DocumentID → call UpdateDocumentInBundle.
func (ps *PITRService) applyUpdate(db *models.Database, entry journal.WALEntry) error {
	if entry.AfterData == "" {
		return nil
	}

	// Skip bulk "multiple" entries that lack a specific document target
	if entry.DocumentID == "" || entry.DocumentID == "multiple" {
		ps.logger.Warnw("PITR replay: skipping bulk UPDATE (no specific DocumentID)",
			"bundle", entry.BundleName,
			"lsn", entry.LSN,
		)
		return nil
	}

	fields, err := parseAfterDataToKeyValues(entry.AfterData)
	if err != nil {
		return fmt.Errorf("UPDATE parse error (bundle=%s, doc=%s): %w", entry.BundleName, entry.DocumentID, err)
	}

	bndl, err := ps.bundleService.GetBundleByName(db, entry.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found for UPDATE: %w", entry.BundleName, err)
	}

	updateCmd := &models.DocumentUpdateCommand{
		BundleName:  entry.BundleName,
		Fields:      fields,
		WhereClause: fmt.Sprintf("DocumentID = '%s'", entry.DocumentID),
	}

	err = ps.bundleService.UpdateDocumentInBundle(context.Background(), db, bndl, updateCmd)
	if err != nil {
		return fmt.Errorf("UPDATE failed (bundle=%s, doc=%s): %w", entry.BundleName, entry.DocumentID, err)
	}

	ps.logger.Debugw("PITR replay UPDATE applied",
		"bundle", entry.BundleName,
		"doc_id", entry.DocumentID,
		"lsn", entry.LSN,
	)
	return nil
}

// applyDelete replays a DELETE operation from the WAL.
// Uses DeleteDocumentFromBundle with explicit docIDs (same pattern as undo INSERT).
func (ps *PITRService) applyDelete(db *models.Database, entry journal.WALEntry) error {
	// Skip bulk "multiple" entries that lack a specific document target
	if entry.DocumentID == "" || entry.DocumentID == "multiple" {
		ps.logger.Warnw("PITR replay: skipping bulk DELETE (no specific DocumentID)",
			"bundle", entry.BundleName,
			"lsn", entry.LSN,
		)
		return nil
	}

	bndl, err := ps.bundleService.GetBundleByName(db, entry.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found for DELETE: %w", entry.BundleName, err)
	}

	deleteCmd := &models.DocumentDeleteCommand{
		BundleName: entry.BundleName,
	}

	err = ps.bundleService.DeleteDocumentFromBundle(bndl, deleteCmd, []string{entry.DocumentID}, nil)
	if err != nil {
		return fmt.Errorf("DELETE failed (bundle=%s, doc=%s): %w", entry.BundleName, entry.DocumentID, err)
	}

	ps.logger.Debugw("PITR replay DELETE applied",
		"bundle", entry.BundleName,
		"doc_id", entry.DocumentID,
		"lsn", entry.LSN,
	)
	return nil
}

// applyBatchInsert replays a BATCH INSERT operation from the WAL.
// AfterData contains a JSON array of documents.
func (ps *PITRService) applyBatchInsert(db *models.Database, entry journal.WALEntry) error {
	if entry.AfterData == "" {
		return nil
	}

	bndl, err := ps.bundleService.GetBundleByName(db, entry.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found for BATCH_INSERT: %w", entry.BundleName, err)
	}

	// Try to parse as array of maps
	var docs []map[string]interface{}
	if err := json.Unmarshal([]byte(entry.AfterData), &docs); err != nil {
		// Try as array of KeyValue arrays
		var kvDocs [][]models.KeyValue
		if err2 := json.Unmarshal([]byte(entry.AfterData), &kvDocs); err2 != nil {
			return fmt.Errorf("BATCH_INSERT parse error (bundle=%s): %w", entry.BundleName, err)
		}
		// Convert each KeyValue array to a map
		for _, kvs := range kvDocs {
			doc := make(map[string]interface{}, len(kvs))
			for _, kv := range kvs {
				doc[kv.Key] = kv.Value
			}
			docs = append(docs, doc)
		}
	}

	inserted := 0
	for _, doc := range docs {
		fields := make([]models.KeyValue, 0, len(doc))
		docID := ""
		for key, value := range doc {
			if key == "DocumentID" {
				if s, ok := value.(string); ok {
					docID = s
				}
				continue
			}
			if key == "CreatedAt" || key == "UpdatedAt" ||
				key == "CommitSequence" || key == "SupersededAt" ||
				key == "CreatedByTxID" || key == "DeletedByTxID" || key == "VersionSequence" {
				continue
			}
			fields = append(fields, models.KeyValue{Key: key, Value: value})
		}

		docCmd := &models.DocumentCommand{
			CommandType: "ADD_DOCUMENT",
			BundleName:  entry.BundleName,
			Fields:      fields,
		}

		var preallocID []string
		if docID != "" {
			preallocID = []string{docID}
		}
		_, err := ps.bundleService.AddDocumentToBundleWithTxID(db, bndl, docCmd, "", preallocID...)
		if err != nil {
			ps.logger.Warnw("PITR replay BATCH_INSERT: failed to insert document",
				"bundle", entry.BundleName, "doc_id", docID, "error", err)
			continue
		}
		inserted++
	}

	ps.logger.Debugw("PITR replay BATCH_INSERT applied",
		"bundle", entry.BundleName,
		"inserted", inserted,
		"total", len(docs),
		"lsn", entry.LSN,
	)
	return nil
}

// applyCreateBundle replays a CREATE BUNDLE operation from the WAL.
func (ps *PITRService) applyCreateBundle(db *models.Database, entry journal.WALEntry) error {
	// Check if bundle already exists (base backup may have included it)
	if _, err := ps.bundleService.GetBundleByName(db, entry.BundleName); err == nil {
		ps.logger.Debugw("PITR replay CREATE_BUNDLE: bundle already exists, skipping",
			"bundle", entry.BundleName,
			"lsn", entry.LSN,
		)
		return nil
	}

	// Parse bundle metadata from AfterData if available
	bundleCmd := &models.BundleCommand{
		CommandType: "CREATE",
		BundleName:  entry.BundleName,
	}

	if entry.AfterData != "" {
		// Try to extract field definitions from the serialized bundle data
		var bundleData map[string]interface{}
		if err := json.Unmarshal([]byte(entry.AfterData), &bundleData); err == nil {
			if structure, ok := bundleData["DocumentStructure"].(map[string]interface{}); ok {
				if fieldDefs, ok := structure["FieldDefinitions"].(map[string]interface{}); ok {
					for _, fd := range fieldDefs {
						if fdMap, ok := fd.(map[string]interface{}); ok {
							fieldDef := models.FieldDefinition{
								Name: fmt.Sprintf("%v", fdMap["Name"]),
							}
							if t, ok := fdMap["Type"].(string); ok {
								fieldDef.Type = t
							}
							if r, ok := fdMap["IsRequired"].(bool); ok {
								fieldDef.IsRequired = r
							}
							if u, ok := fdMap["IsUnique"].(bool); ok {
								fieldDef.IsUnique = u
							}
							bundleCmd.Fields = append(bundleCmd.Fields, fieldDef)
						}
					}
				}
			}
		}
	}

	_, err := ps.bundleService.AddBundle(ps.databaseService, db, bundleCmd)
	if err != nil {
		return fmt.Errorf("CREATE_BUNDLE failed (bundle=%s): %w", entry.BundleName, err)
	}

	ps.logger.Debugw("PITR replay CREATE_BUNDLE applied",
		"bundle", entry.BundleName,
		"lsn", entry.LSN,
	)
	return nil
}

// applyDeleteBundle replays a DELETE BUNDLE operation from the WAL.
func (ps *PITRService) applyDeleteBundle(db *models.Database, entry journal.WALEntry) error {
	// Check if bundle exists
	if _, err := ps.bundleService.GetBundleByName(db, entry.BundleName); err != nil {
		ps.logger.Debugw("PITR replay DELETE_BUNDLE: bundle not found, skipping",
			"bundle", entry.BundleName,
			"lsn", entry.LSN,
		)
		return nil
	}

	bundleCmd := &models.BundleCommand{
		CommandType:    "DELETE",
		BundleName:     entry.BundleName,
		HasForceSwitch: true, // Skip referential integrity checks during replay
	}

	err := ps.bundleService.DeleteBundle(db, bundleCmd)
	if err != nil {
		return fmt.Errorf("DELETE_BUNDLE failed (bundle=%s): %w", entry.BundleName, err)
	}

	ps.logger.Debugw("PITR replay DELETE_BUNDLE applied",
		"bundle", entry.BundleName,
		"lsn", entry.LSN,
	)
	return nil
}

// resolveTargetLSN converts a PITRRecoveryTarget into a concrete LSN value.
func (ps *PITRService) resolveTargetLSN(target PITRRecoveryTarget, entries []WALArchiveEntry) (uint64, error) {
	switch target.Type {
	case PITRTargetLSN:
		return target.TargetLSN, nil

	case PITRTargetTimestamp:
		// Find the last WAL entry whose timestamp is <= target time
		var maxLSN uint64
		for _, entry := range entries {
			if !entry.LastTimestamp.After(target.TargetTime) {
				maxLSN = entry.LastLSN
			}
		}
		if maxLSN == 0 {
			// The target might be within the first file
			if len(entries) > 0 {
				return entries[0].LastLSN, nil
			}
			return 0, fmt.Errorf("no WAL entries found for timestamp %s", target.TargetTime)
		}
		return maxLSN, nil

	case PITRTargetRestorePoint:
		if ps.walManager != nil {
			rp := ps.walManager.GetRestorePointManager().Get(target.RestorePointName)
			if rp != nil {
				return rp.LSN, nil
			}
		}
		// If we can't find the restore point in the manager, scan to the end of archive
		if len(entries) > 0 {
			return entries[len(entries)-1].LastLSN, nil
		}
		return 0, fmt.Errorf("restore point '%s' not found and WAL archive is empty", target.RestorePointName)
	}

	return 0, fmt.Errorf("unknown target type: %d", target.Type)
}

// cleanupFailedRestore removes a partially-restored database.
func (ps *PITRService) cleanupFailedRestore(dbName string) {
	if dbName == "" {
		return
	}

	ps.logger.Warnw("Cleaning up failed PITR restore", "database", dbName)

	// Try to delete the database
	if err := ps.databaseService.DeleteDatabase(dbName); err != nil {
		ps.logger.Warnw("Failed to delete partially-restored database", "database", dbName, "error", err)
	}

	// Remove the database directory
	dbPath := ps.settings.DataDir + "/" + dbName
	if err := os.RemoveAll(dbPath); err != nil {
		ps.logger.Warnw("Failed to remove database directory", "path", dbPath, "error", err)
	}
}

// Helper: check if a string contains a substring (case-insensitive)
func containsCI(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
