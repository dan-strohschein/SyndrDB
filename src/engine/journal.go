package engine

// This file contains the journal functionality for the database engine.
// all transactions in the database are logged to the journal first
// and then applied to the database files.

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// JournalEntry represents a single entry in the journal.
type JournalEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Command   string    `json:"command"`
	Bundle    string    `json:"bundle"`
	Details   string    `json:"details"`
}

// Journal represents the journal for the database engine.
type Journal struct {
	Entries            []JournalEntry `json:"entries"`
	file               *os.File       // File handle for the journal file
	baseFilePath       string         // Base path for journal files (without date)
	currentDate        time.Time      // The date of the current journal file
	maxJournalFileSize int64
	currentSize        int64
	retentionDays      int
}

// NewJournal creates a new journal instance.
func NewJournal(journalFilePath string) (*Journal, error) {
	// Store the base file path (without date)
	baseFilePath := getBaseFilePath(journalFilePath)

	// Create a journal with today's date
	journal := &Journal{
		Entries:      []JournalEntry{},
		baseFilePath: baseFilePath,
		currentDate:  time.Now().Truncate(24 * time.Hour), // Start with today's date
	}

	// Open the current day's journal file
	if err := journal.ensureCorrectFileOpen(); err != nil {
		return nil, err
	}

	return journal, nil
}

// getBaseFilePath extracts the base path without date component
func getBaseFilePath(journalFilePath string) string {
	// If the path already contains a date pattern, remove it
	dir := filepath.Dir(journalFilePath)
	base := filepath.Base(journalFilePath)
	ext := filepath.Ext(journalFilePath)

	// Remove any existing date pattern (assuming YYYY-MM-DD format)
	baseName := strings.TrimSuffix(base, ext)
	datePattern := regexp.MustCompile(`_\d{4}-\d{2}-\d{2}$`)
	baseName = datePattern.ReplaceAllString(baseName, "")

	return filepath.Join(dir, baseName)
}

// ensureCorrectFileOpen ensures the correct journal file is open based on current date
func (j *Journal) ensureCorrectFileOpen() error {
	today := time.Now().Truncate(24 * time.Hour)

	// If we already have the correct file open, do nothing
	if j.file != nil && j.currentDate.Equal(today) {
		return nil
	}

	// Close the current file if it's open
	if j.file != nil {
		if err := j.file.Close(); err != nil {
			return fmt.Errorf("failed to close previous journal file: %w", err)
		}
		j.file = nil
	}

	// Create the filename with today's date
	dateStr := today.Format("2006-01-02")
	fileName := fmt.Sprintf("%s_%s%s", j.baseFilePath, dateStr, filepath.Ext(j.baseFilePath))
	if filepath.Ext(j.baseFilePath) == "" {
		fileName = fmt.Sprintf("%s_%s.journal", j.baseFilePath, dateStr)
	}

	// Ensure the directory exists
	dir := filepath.Dir(fileName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create journal directory: %w", err)
	}

	// Open the new journal file
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open journal file %s: %w", fileName, err)
	}

	// Update journal state
	j.file = file
	j.currentDate = today

	return nil
}

// AddEntry adds a new entry to the journal.
func (j *Journal) AddEntry(command, bundle, details string) error {
	// Ensure the correct file is open based on current date
	if err := j.ensureCorrectFileOpen(); err != nil {
		return err
	}

	entry := JournalEntry{
		Timestamp: time.Now(),
		Command:   command,
		Bundle:    bundle,
		Details:   details,
	}

	j.Entries = append(j.Entries, entry)

	// Write the entry to the journal file
	line := fmt.Sprintf("%s | %s | %s | %s\n", entry.Timestamp.Format(time.RFC3339), entry.Command, entry.Bundle, entry.Details)
	if j.currentSize > j.maxJournalFileSize {
		// TODO Close current file and create a new one with timestamp
		// TODO Reset currentSize
	}

	if _, err := j.file.WriteString(line); err != nil {
		return fmt.Errorf("failed to write to journal file: %w", err)
	}
	// Update the current size of the journal file
	j.currentSize += int64(len(line))

	return nil
}

// Close closes the journal file.
func (j *Journal) Close() error {
	if j.file != nil {
		if err := j.file.Close(); err != nil {
			return fmt.Errorf("failed to close journal file: %w", err)
		}
		j.file = nil
	}
	return nil
}

func (j *Journal) CompressOldJournals() error {
	// Find journal files older than X days
	// Compress them using gzip/something else
	return nil
}

// Add cleanup function
func (j *Journal) CleanupOldJournals() error {
	cutoff := time.Now().AddDate(0, 0, -j.retentionDays)

	if err := j.ensureCorrectFileOpen(); err != nil {
		return fmt.Errorf("failed to ensure correct file open: %w", err)
	}
	//TODO review this logic more
	if cutoff.After(j.currentDate) {
		// If the cutoff date is before the current date, we can remove the current file
		if err := j.file.Close(); err != nil {
			return fmt.Errorf("failed to close current journal file: %w", err)
		}
		j.file = nil
		j.currentDate = cutoff // Update current date to cutoff
	}

	// Find and remove journal files older than cutoff
	return nil
}
