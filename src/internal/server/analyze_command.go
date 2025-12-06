/*
analyze_command.go

This file implements the ANALYZE BUNDLE command for manual statistics collection.

COMMAND SYNTAX:
  ANALYZE BUNDLE <bundle_name>

PURPOSE:
- Manually trigger statistics collection for a specific bundle
- Force re-analysis even if statistics are current
- Provide progress feedback during long-running analysis

DESIGN PRINCIPLES:
- User-Friendly: Clear progress reporting and error messages
- Synchronous Execution: Wait for analysis to complete
- Integration: Use existing StatisticsCollector infrastructure

This implements PostgreSQL's ANALYZE command with SyndrDB adaptations
for document-based storage.
*/

package server

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/domain/statistics"
	"time"

	"go.uber.org/zap"
)

// AnalyzeCommand handles ANALYZE BUNDLE commands
type AnalyzeCommand struct {
	statsCollector *statistics.StatisticsCollector
	statsStorage   *statistics.StatisticsStorage
	autoAnalyzer   *statistics.AutoAnalyzeTracker
	logger         *zap.SugaredLogger
}

// AnalyzeCommandRequest represents an ANALYZE command request
type AnalyzeCommandRequest struct {
	BundleName string
}

// AnalyzeCommandResult represents the result of an ANALYZE command
type AnalyzeCommandResult struct {
	BundleName       string        `json:"bundle_name"`
	TotalDocuments   int64         `json:"total_documents"`
	FieldsAnalyzed   int           `json:"fields_analyzed"`
	AnalysisDuration time.Duration `json:"analysis_duration_ms"`
	CompressedSize   int64         `json:"compressed_size_bytes,omitempty"`
	UncompressedSize int64         `json:"uncompressed_size_bytes,omitempty"`
	StatsVersion     int           `json:"stats_version"`
	Message          string        `json:"message"`
}

// NewAnalyzeCommand creates a new ANALYZE command handler
func NewAnalyzeCommand(
	statsCollector *statistics.StatisticsCollector,
	statsStorage *statistics.StatisticsStorage,
	autoAnalyzer *statistics.AutoAnalyzeTracker,
	logger *zap.SugaredLogger,
) *AnalyzeCommand {
	return &AnalyzeCommand{
		statsCollector: statsCollector,
		statsStorage:   statsStorage,
		autoAnalyzer:   autoAnalyzer,
		logger:         logger,
	}
}

// Execute runs the ANALYZE command
func (ac *AnalyzeCommand) Execute(request *AnalyzeCommandRequest, database *models.Database) (*AnalyzeCommandResult, error) {
	if request.BundleName == "" {
		return nil, fmt.Errorf("bundle name is required")
	}

	ac.logger.Infof("ANALYZE BUNDLE %s: starting statistics collection", request.BundleName)
	startTime := time.Now()

	// Load bundle from database
	bundle, err := ac.loadBundle(request.BundleName, database)
	if err != nil {
		return nil, fmt.Errorf("failed to load bundle: %w", err)
	}

	// Collect statistics
	ac.logger.Debugf("ANALYZE BUNDLE %s: analyzing %d documents", request.BundleName, bundle.TotalDocuments)
	stats, err := ac.statsCollector.AnalyzeBundle(bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze bundle: %w", err)
	}

	// Save statistics
	ac.logger.Debugf("ANALYZE BUNDLE %s: saving statistics to disk", request.BundleName)
	if err := ac.statsStorage.SaveStatistics(stats); err != nil {
		return nil, fmt.Errorf("failed to save statistics: %w", err)
	}

	// Update auto-analyze tracker to reset change counter
	if ac.autoAnalyzer != nil {
		ac.autoAnalyzer.MarkAnalyzed(request.BundleName)
	}

	duration := time.Since(startTime)

	result := &AnalyzeCommandResult{
		BundleName:       request.BundleName,
		TotalDocuments:   stats.TotalDocuments,
		FieldsAnalyzed:   len(stats.FieldStats),
		AnalysisDuration: duration,
		CompressedSize:   stats.CompressedSize,
		UncompressedSize: stats.UncompressedSize,
		StatsVersion:     stats.StatsVersion,
		Message: fmt.Sprintf("Successfully analyzed %d documents across %d fields in %v",
			stats.TotalDocuments, len(stats.FieldStats), duration),
	}

	ac.logger.Infof("ANALYZE BUNDLE %s: completed in %v (version: %d, fields: %d)",
		request.BundleName, duration, stats.StatsVersion, len(stats.FieldStats))

	return result, nil
}

// AnalyzeAllBundles analyzes statistics for all bundles in a database
func (ac *AnalyzeCommand) AnalyzeAllBundles(database *models.Database) ([]*AnalyzeCommandResult, error) {
	ac.logger.Infof("ANALYZE ALL: starting analysis of all bundles")

	results := make([]*AnalyzeCommandResult, 0)
	errors := make([]error, 0)

	// Get all bundles
	bundleNames := ac.listBundles(database)

	for _, bundleName := range bundleNames {
		ac.logger.Infof("ANALYZE ALL: processing bundle %s", bundleName)

		result, err := ac.Execute(&AnalyzeCommandRequest{BundleName: bundleName}, database)
		if err != nil {
			ac.logger.Errorf("ANALYZE ALL: failed to analyze %s: %v", bundleName, err)
			errors = append(errors, fmt.Errorf("bundle %s: %w", bundleName, err))
			continue
		}

		results = append(results, result)
	}

	if len(errors) > 0 {
		ac.logger.Warnf("ANALYZE ALL: completed with %d errors out of %d bundles",
			len(errors), len(bundleNames))
		// Return partial results with error
		return results, fmt.Errorf("analysis completed with %d errors: %v", len(errors), errors[0])
	}

	ac.logger.Infof("ANALYZE ALL: completed successfully (%d bundles)", len(results))
	return results, nil
}

// GetStatistics retrieves current statistics for a bundle
func (ac *AnalyzeCommand) GetStatistics(bundleName string) (*statistics.BundleStatistics, error) {
	stats, err := ac.statsStorage.LoadStatistics(bundleName)
	if err != nil {
		return nil, fmt.Errorf("failed to load statistics: %w", err)
	}
	return stats, nil
}

// DeleteStatistics removes statistics for a bundle
func (ac *AnalyzeCommand) DeleteStatistics(bundleName string) error {
	if err := ac.statsStorage.DeleteStatistics(bundleName); err != nil {
		return fmt.Errorf("failed to delete statistics: %w", err)
	}
	ac.logger.Infof("Deleted statistics for bundle: %s", bundleName)
	return nil
}

// Helper functions

func (ac *AnalyzeCommand) loadBundle(bundleName string, database *models.Database) (*models.Bundle, error) {
	// TODO: I will replace this with actual bundle loading from database when integrated
	// For now, return error indicating integration is needed
	return nil, fmt.Errorf("bundle loading integration pending - bundleName: %s, database: %s",
		bundleName, database.Name)
}

func (ac *AnalyzeCommand) listBundles(database *models.Database) []string {
	// TODO: I will replace this with actual bundle listing from database when integrated
	// For now, return empty list
	return []string{}
}
