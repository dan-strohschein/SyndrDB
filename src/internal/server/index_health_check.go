package server

import (
	"fmt"
	"os"
	"time"
)

// ANSI color codes for terminal output
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorBold   = "\033[1m"
	colorYellow = "\033[33m"
)

// CheckUnhealthyIndexes scans all bundles for unhealthy indexes and prints warnings
// Returns the count of unhealthy indexes found
func (s *Server) CheckUnhealthyIndexes() int {
	unhealthyCount := 0
	var unhealthyIndexes []UnhealthyIndexInfo
	
	// Get all databases
	databases := s.ServiceManager.DatabaseService.ListDatabases()
	
	for _, db := range databases {
		// Get all bundles
		allBundles := s.ServiceManager.BundleService.GetAllBundles()
		
		for _, bundle := range allBundles {
			// Only process bundles belonging to this database
			if bundle.Database != nil && bundle.Database.DatabaseID != db.DatabaseID {
				continue
			}
			
			// Check each index in the bundle
			if bundle.Indexes != nil {
				for indexName, indexRef := range bundle.Indexes {
					if indexRef.Maintenance != nil && !indexRef.Maintenance.IsHealthy {
						unhealthyCount++
						unhealthyIndexes = append(unhealthyIndexes, UnhealthyIndexInfo{
							DatabaseName:   db.Name,
							BundleName:     bundle.Name,
							IndexName:      indexName,
							IndexType:      indexRef.IndexType,
							FailureReason:  indexRef.Maintenance.LastFailureReason,
							FailureTime:    indexRef.Maintenance.LastFailureTime,
						})
					}
				}
			}
		}
	}
	
	// Print warnings if any unhealthy indexes found
	if unhealthyCount > 0 {
		fmt.Fprintf(os.Stderr, "\n%s%s╔════════════════════════════════════════════════════════════════════════════╗%s\n",
			colorRed, colorBold, colorReset)
		fmt.Fprintf(os.Stderr, "%s%s║                           🔴 CRITICAL ALERT 🔴                            ║%s\n",
			colorRed, colorBold, colorReset)
		fmt.Fprintf(os.Stderr, "%s%s╠════════════════════════════════════════════════════════════════════════════╣%s\n",
			colorRed, colorBold, colorReset)
		fmt.Fprintf(os.Stderr, "%s%s║  %d UNHEALTHY INDEX(ES) DETECTED - QUERIES WILL USE FULL TABLE SCANS     ║%s\n",
			colorRed, colorBold, unhealthyCount, colorReset)
		fmt.Fprintf(os.Stderr, "%s%s╚════════════════════════════════════════════════════════════════════════════╝%s\n\n",
			colorRed, colorBold, colorReset)
		
		for _, idx := range unhealthyIndexes {
			fmt.Fprintf(os.Stderr, "%s🔴 UNHEALTHY INDEX:%s\n", colorRed, colorReset)
			fmt.Fprintf(os.Stderr, "   Database: %s%s%s\n", colorYellow, idx.DatabaseName, colorReset)
			fmt.Fprintf(os.Stderr, "   Bundle:   %s%s%s\n", colorYellow, idx.BundleName, colorReset)
			fmt.Fprintf(os.Stderr, "   Index:    %s%s%s (type: %s)\n", colorYellow, idx.IndexName, colorReset, idx.IndexType)
			fmt.Fprintf(os.Stderr, "   Reason:   %s\n", idx.FailureReason)
			fmt.Fprintf(os.Stderr, "   Time:     %s\n", idx.FailureTime.Format("2006-01-02 15:04:05"))
			fmt.Fprintf(os.Stderr, "   %sAction:   Queries will automatically fall back to full table scans%s\n\n", colorBold, colorReset)
		}
		
		fmt.Fprintf(os.Stderr, "%s%sRECOMMENDED ACTION:%s Manually rebuild affected indexes using:\n", colorBold, colorYellow, colorReset)
		fmt.Fprintf(os.Stderr, "  REBUILD INDEX <index_name> ON <bundle_name>;\n\n")
		
		s.logger.Errorf("Server started with %d unhealthy indexes - see console for details", unhealthyCount)
	}
	
	return unhealthyCount
}

// PrintUnhealthyIndexesOnShutdown prints unhealthy index warnings during graceful shutdown
func (s *Server) PrintUnhealthyIndexesOnShutdown() {
	unhealthyCount := s.CheckUnhealthyIndexes()
	
	if unhealthyCount > 0 {
		fmt.Fprintf(os.Stderr, "\n%s%s⚠️  WARNING: Server shutting down with %d unhealthy index(es)%s\n",
			colorRed, colorBold, unhealthyCount, colorReset)
		fmt.Fprintf(os.Stderr, "%sResolve index health issues before next startup to restore optimal performance.%s\n\n",
			colorYellow, colorReset)
	}
}

// UnhealthyIndexInfo contains information about an unhealthy index
type UnhealthyIndexInfo struct {
	DatabaseName  string
	BundleName    string
	IndexName     string
	IndexType     string
	FailureReason string
	FailureTime   time.Time
}
