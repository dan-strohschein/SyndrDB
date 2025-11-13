/*
Package server provides database lock command operations for the SyndrDB database server.

This file implements the LOCK DATABASE and UNLOCK DATABASE commands, which control
database access during maintenance operations like backups and restores.

Lock Features:
- Places database in read-only mode (all write operations fail)
- Admin-only access (non-admin users cannot read or write)
- Tracks who locked the database, when, and why
- Supports optional comments for documentation
- Enforced at command processing level

Lock Use Cases:
- Before/during backup operations (ensure data consistency)
- After restore operations (prevent accidental writes)
- During maintenance windows
- For database migration preparation

Main Functions:
- LockDatabaseCommand: Parses LOCK DATABASE command and creates lock
- UnlockDatabaseCommand: Parses UNLOCK DATABASE command and removes lock
- parseLockCommand: Extracts database name, reason, and comment from tokens

Locked databases reject all write operations and non-admin read operations.
*/

package server

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// LockDatabaseCommand handles the LOCK DATABASE command
// Command syntax: LOCK DATABASE "dbname" [FOR "reason"] [COMMENT "comment"]
// Example: LOCK DATABASE "production" FOR "backup" COMMENT "scheduled weekly backup"
//
// Returns a CommandResponse with lock information
func LockDatabaseCommand(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager) (*CommandResponse, error) {
	// Parse the command to extract database name, reason, and comment
	dbName, reason, comment, err := parseLockCommand(command)
	if err != nil {
		return nil, fmt.Errorf("failed to parse LOCK command: %w", err)
	}

	logger.Infof("Locking database: database=%s, reason=%s, comment=%s", dbName, reason, comment)

	// TODO: Check user permissions - only admins can lock databases
	// This requires session/authentication context to be passed through

	// Lock the database
	adminUser := "admin" // TODO: Get from session context
	if err := serviceManager.LockService.LockDatabase(dbName, adminUser, reason, comment); err != nil {
		return nil, fmt.Errorf("failed to lock database: %w", err)
	}

	logger.Infof("Database locked successfully: database=%s", dbName)

	// Return success response
	response := &CommandResponse{
		ResultCount:     1,
		Result:          fmt.Sprintf("Database '%s' locked successfully. Database is now in read-only mode (admin access only).", dbName),
		ExecutionTimeMS: 0,
	}

	return response, nil
}

// UnlockDatabaseCommand handles the UNLOCK DATABASE command
// Command syntax: UNLOCK DATABASE "dbname"
//
// Returns a CommandResponse confirming unlock
func UnlockDatabaseCommand(command string, logger *zap.SugaredLogger, serviceManager *ServiceManager) (*CommandResponse, error) {
	// Parse the command to extract database name
	dbName, err := parseUnlockCommand(command)
	if err != nil {
		return nil, fmt.Errorf("failed to parse UNLOCK command: %w", err)
	}

	logger.Infof("Unlocking database: database=%s", dbName)

	// TODO: Check user permissions - only admins can unlock databases
	// This requires session/authentication context to be passed through

	// Unlock the database
	adminUser := "admin" // TODO: Get from session context
	if err := serviceManager.LockService.UnlockDatabase(dbName, adminUser); err != nil {
		return nil, fmt.Errorf("failed to unlock database: %w", err)
	}

	logger.Infof("Database unlocked successfully: database=%s", dbName)

	// Return success response
	response := &CommandResponse{
		ResultCount:     1,
		Result:          fmt.Sprintf("Database '%s' unlocked successfully. Database is now available for normal operations.", dbName),
		ExecutionTimeMS: 0,
	}

	return response, nil
}

// parseLockCommand extracts database name, reason, and comment from command tokens
// Expected format: LOCK DATABASE "dbname" [FOR "reason"] [COMMENT "comment"]
func parseLockCommand(command string) (string, string, string, error) {
	tokens := strings.Fields(command)

	// Minimum tokens: LOCK DATABASE "name" = 3 tokens
	if len(tokens) < 3 {
		return "", "", "", fmt.Errorf("invalid LOCK syntax: expected LOCK DATABASE \"name\"")
	}

	// Validate command structure
	if strings.ToUpper(tokens[0]) != "LOCK" || strings.ToUpper(tokens[1]) != "DATABASE" {
		return "", "", "", fmt.Errorf("invalid LOCK syntax: expected LOCK DATABASE")
	}

	// Extract database name (token 2)
	dbName := strings.Trim(tokens[2], "\"'")
	if dbName == "" {
		return "", "", "", fmt.Errorf("database name cannot be empty")
	}

	// Parse optional FOR and COMMENT clauses
	reason := "manual lock" // Default reason
	comment := ""

	i := 3
	for i < len(tokens) {
		keyword := strings.ToUpper(tokens[i])

		switch keyword {
		case "FOR":
			// Next token is the reason
			if i+1 >= len(tokens) {
				return "", "", "", fmt.Errorf("FOR clause requires a reason")
			}
			reason = strings.Trim(tokens[i+1], "\"'")
			i += 2

		case "COMMENT":
			// Next token is the comment
			if i+1 >= len(tokens) {
				return "", "", "", fmt.Errorf("COMMENT clause requires a comment")
			}
			comment = strings.Trim(tokens[i+1], "\"'")
			i += 2

		default:
			return "", "", "", fmt.Errorf("unexpected keyword in LOCK command: %s", tokens[i])
		}
	}

	return dbName, reason, comment, nil
}

// parseUnlockCommand extracts database name from command tokens
// Expected format: UNLOCK DATABASE "dbname"
func parseUnlockCommand(command string) (string, error) {
	tokens := strings.Fields(command)

	// Minimum tokens: UNLOCK DATABASE "name" = 3 tokens
	if len(tokens) < 3 {
		return "", fmt.Errorf("invalid UNLOCK syntax: expected UNLOCK DATABASE \"name\"")
	}

	// Validate command structure
	if strings.ToUpper(tokens[0]) != "UNLOCK" || strings.ToUpper(tokens[1]) != "DATABASE" {
		return "", fmt.Errorf("invalid UNLOCK syntax: expected UNLOCK DATABASE")
	}

	// Extract database name (token 2)
	dbName := strings.Trim(tokens[2], "\"'")
	if dbName == "" {
		return "", fmt.Errorf("database name cannot be empty")
	}

	// UNLOCK command doesn't support additional options
	if len(tokens) > 3 {
		return "", fmt.Errorf("UNLOCK DATABASE does not accept additional options")
	}

	return dbName, nil
}

// TODO: I will add a SHOW LOCKS command to list all currently locked databases
// This would be useful for administrators to see which databases are locked and why
// Command syntax: SHOW LOCKS
// Returns: List of all locked databases with lock metadata
