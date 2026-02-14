package server

/*
MVCC DEBUG COMMANDS

This file implements debug commands for MVCC (Multi-Version Concurrency Control) system.
These commands help developers and administrators inspect MVCC state, including:
- Document versions and their metadata
- Active transaction snapshots
- Conflict detection history

Commands:
- SHOW VERSIONS [FOR "documentID"] [IN BUNDLE "bundleName"]
- SHOW ACTIVE SNAPSHOTS
- SHOW CONFLICT LOG

PHASE 6: MVCC - Debug and monitoring commands
*/

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/errors"
	"time"

	"go.uber.org/zap"
)

// ShowVersions displays all versions of a document with MVCC metadata
// Syntax: SHOW VERSIONS [FOR "documentID"] [IN BUNDLE "bundleName"]
// PHASE 6: MVCC - Debug command to inspect document version chains
func ShowVersions(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Debugf("Processing SHOW VERSIONS command: %s", command)

	// Parse command to extract documentID and bundleName
	// Format: SHOW VERSIONS [FOR "documentID"] [IN BUNDLE "bundleName"]
	words := strings.Fields(command)
	if len(words) < 2 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"incomplete SHOW VERSIONS command. Usage: SHOW VERSIONS FOR \"documentID\" [IN BUNDLE \"bundleName\"]",
			errors.LayerCommand)
	}

	var documentID string
	var bundleName string
	var foundFor bool

	for i := 2; i < len(words); i++ {
		word := strings.ToLower(words[i])
		if word == "for" && i+1 < len(words) {
			// Extract documentID (remove quotes)
			docIDStr := words[i+1]
			documentID = strings.Trim(docIDStr, "\"")
			foundFor = true
			i++ // Skip the documentID
		} else if word == "in" && i+1 < len(words) && strings.ToLower(words[i+1]) == "bundle" && i+2 < len(words) {
			// Extract bundleName (remove quotes)
			bundleStr := words[i+2]
			bundleName = strings.Trim(bundleStr, "\"")
			i += 2 // Skip "bundle" and bundleName
		}
	}

	if !foundFor || documentID == "" {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"documentID is required. Usage: SHOW VERSIONS FOR \"documentID\" [IN BUNDLE \"bundleName\"]",
			errors.LayerCommand)
	}

	// If bundleName not specified, try to find it from database
	if bundleName == "" && database != nil {
		// Try to find the document in any bundle
		// This is a simplified approach - in production, might want to search all bundles
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"bundleName is required. Usage: SHOW VERSIONS FOR \"documentID\" IN BUNDLE \"bundleName\"",
			errors.LayerCommand)
	}

	if database == nil {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"no database selected. Use USE DATABASE first",
			errors.LayerCommand)
	}

	// Get all versions of the document
	versions, err := serviceManager.BundleService.GetDocumentVersions(bundleName, database.Name, documentID)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
			fmt.Sprintf("failed to get document versions for %s", documentID),
			errors.LayerCommand).WithContext("documentID", documentID).WithContext("bundleName", bundleName)
	}

	if len(versions) == 0 {
		return &CommandResponse{
			Result:          fmt.Sprintf("No versions found for document %s in bundle %s", documentID, bundleName),
			ResultCount:     0,
			ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
		}, nil
	}

	// Format versions for display
	versionInfo := make([]map[string]interface{}, 0, len(versions))
	for _, doc := range versions {
		versionData := map[string]interface{}{
			"DocumentID":      doc.DocumentID,
			"VersionSequence": doc.VersionSequence,
			"CommitSequence":  doc.CommitSequence,
			"CreatedByTxID":   doc.CreatedByTxID,
			"DeletedByTxID":   doc.DeletedByTxID,
			"IsDeleted":       doc.DeletedByTxID > 0,
			"IsCommitted":     doc.CommitSequence > 0,
			"CreatedAt":       doc.CreatedAt.Format("2006-01-02 15:04:05.000000"),
			"UpdatedAt":       doc.UpdatedAt.Format("2006-01-02 15:04:05.000000"),
		}

		// Add field count for summary (Option B: Values or Data)
		if len(doc.Values) > 0 {
			versionData["FieldCount"] = len(doc.Values)
		} else if doc.Data != nil {
			versionData["FieldCount"] = len(doc.Data)
		}

		versionInfo = append(versionInfo, versionData)
	}

	response := &CommandResponse{
		Result:          versionInfo,
		ResultCount:     len(versionInfo),
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	logger.Debugf("SHOW VERSIONS: Found %d versions for document %s in bundle %s", len(versions), documentID, bundleName)
	return response, nil
}

// ShowActiveSnapshots displays all active transaction snapshots
// Syntax: SHOW ACTIVE SNAPSHOTS
// PHASE 6: MVCC - Debug command to inspect active transaction snapshots
func ShowActiveSnapshots(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Debugf("Processing SHOW ACTIVE SNAPSHOTS command: %s", command)

	if serviceManager.WALManager == nil {
		return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
			"WAL Manager not available",
			errors.LayerCommand)
	}

	snapshotMgr := serviceManager.WALManager.GetSnapshotManager()
	if snapshotMgr == nil {
		return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
			"Snapshot Manager not available",
			errors.LayerCommand)
	}

	// Get active snapshot count
	activeCount := snapshotMgr.GetActiveTransactionCount()
	oldestSnapshot := snapshotMgr.GetOldestActiveSnapshot()
	currentSequence := snapshotMgr.GetCurrentSequence()

	// Build snapshot information
	snapshotInfo := map[string]interface{}{
		"active_snapshot_count":    activeCount,
		"oldest_snapshot_sequence": oldestSnapshot,
		"current_global_sequence":  currentSequence,
		"note":                     "For detailed snapshot information, check server logs with debug mode enabled",
	}

	response := &CommandResponse{
		Result:          snapshotInfo,
		ResultCount:     1,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	logger.Debugf("SHOW ACTIVE SNAPSHOTS: %d active snapshots, oldest sequence: %d, current sequence: %d",
		activeCount, oldestSnapshot, currentSequence)
	return response, nil
}

// ShowConflictLog displays conflict detection statistics
// Syntax: SHOW CONFLICT LOG
// PHASE 6: MVCC - Debug command to inspect conflict detection history
func ShowConflictLog(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Debugf("Processing SHOW CONFLICT LOG command: %s", command)

	if serviceManager.ConflictTracker == nil {
		return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
			"Conflict Tracker not available",
			errors.LayerCommand)
	}

	// Get conflict tracker statistics
	stats := serviceManager.ConflictTracker.GetStats()

	conflictInfo := map[string]interface{}{
		"tracked_documents": stats["tracked_documents"],
		"note":              "Conflict tracker monitors document commit sequences for write-write conflict detection",
		"description":       "This shows how many documents are currently tracked for conflict detection",
	}

	response := &CommandResponse{
		Result:          conflictInfo,
		ResultCount:     1,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	logger.Debugf("SHOW CONFLICT LOG: %v documents tracked", stats["tracked_documents"])
	return response, nil
}
