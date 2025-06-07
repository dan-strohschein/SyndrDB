package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syndrdb/src/server"
	"syndrdb/src/settings"
	"syscall"
	"time"
)

// printUsage prints helpful usage information
func printUsage() {
	log.Println("SyndrDB - A relational document database by Dan Strohschein")
	log.Println("\nUsage:")
	log.Println("  syndrdb [options]")
	log.Println("\nOptions:")
	flag.PrintDefaults()

	log.Println("\nExamples:")
	log.Println("  syndrdb --datadir=/data")
	log.Println("  syndrdb --port=1776 --logfile=syndrdb.log")
}

func main() {
	// Create a new settings.Arguments instance
	// Get the global settings instance
	args := settings.GetSettings()

	//args := settings.Arguments{}

	// Define command line flags that map to the Arguments struct
	flag.StringVar(&args.DataDir, "datadir", "./datafiles", "Directory to store data files")
	flag.StringVar(&args.LogDir, "logdir", "./log_files", "Directory to store log files (default: stdout)")
	flag.StringVar(&args.TempDir, "tempdir", "./temp", "Temporary directory for intermediate files/indexes/sorts")
	flag.Int64Var(&args.MaxJournalFileSize, "maxjournalfilesize", 1000000, "Maximum size of journal files in bytes (default: 1MB)")
	flag.StringVar(&args.Host, "host", "127.0.0.1", "Host name or IP address to listen on")
	flag.IntVar(&args.Port, "port", 1776, "Port for the HTTP server")
	flag.BoolVar(&args.Verbose, "verbose", true, "Enable verbose logging")
	flag.StringVar(&args.ConfigFile, "config", "", "Path to config file")
	flag.StringVar(&args.Mode, "mode", "standalone", "Operation mode (standalone, cluster)")
	flag.BoolVar(&args.AuthEnabled, "auth", false, "Enable authentication")
	flag.StringVar(&args.Version, "version", "0.0.1alpha", "Shows version")
	flag.BoolVar(&args.PrintToScreen, "print", true, "Print Log Messages to screen")
	flag.BoolVar(&args.Debug, "debug", true, "Enable debug mode")
	flag.BoolVar(&args.UserDebug, "userdebug", false, "Enable user debug mode")

	// Parse the command line
	flag.Parse()

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	logFilename := fmt.Sprintf("%s_%s_ServerLog.txt", timestamp, args.Host)

	// Combine with the directory path from args.LogFile
	args.LogDir = filepath.Join(args.LogDir, logFilename)

	// Validate the arguments
	if err := validateArguments(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n\n", err)
		printUsage()
		os.Exit(1)
	}

	// Configure logger
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	// Print the arguments if in verbose mode
	if args.Verbose {
		log.Println("SyndrDB starting with options:")
		log.Printf("  Data Directory: %s\n", args.DataDir)
		log.Printf("  Log File: %s\n", args.LogDir)
		log.Printf("  Host: %s\n", args.Host)
		log.Printf("  Port: %d\n", args.Port)
		log.Printf("  Verbose: %v\n", args.Verbose)
		log.Printf("  Config File: %s\n", args.ConfigFile)
		log.Printf("  Mode: %s\n", args.Mode)

	}

	// Set up logging
	if args.LogDir != "" {
		// Create timestamped log filename
		// timestamp := time.Now().Format("2006-01-02_15-04-05")
		// logFilename := fmt.Sprintf("%s_%s_ServerLog.txt", timestamp, args.Host)

		// // Combine with the directory path from args.LogFile
		// logFilePath := filepath.Join(args.LogDir, logFilename)

		// Ensure log directory exists
		logDir := filepath.Dir(args.LogDir)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			log.Fatalf("Failed to create log directory: %v", err)
		}

		log.Printf("Logging to file: %s", args.LogDir)

		logFile, err := os.OpenFile(args.LogDir, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		defer logFile.Close()

		// Use MultiWriter to write logs to both file and stdout if PrintToScreen is enabled
		if args.PrintToScreen {
			mw := io.MultiWriter(os.Stdout, logFile)
			log.SetOutput(mw)
		} else {
			log.SetOutput(logFile)
		}
	}

	// Ensure data directory exists
	if err := os.MkdirAll(args.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Initialize the database
	// db := &engine.Database{
	// 	DataDirectory: args.DataDir,
	// 	Bundles:       make(map[string]engine.Bundle),
	// }

	// Create and start the server
	srv, err := server.InitServer(args)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}
	//srv := server.NewServer(args.Host, args.Port, db, args.AuthEnabled)

	// Add users if authentication is enabled
	if args.AuthEnabled {
		srv.AddUser("admin", "admin123")   // Example user
		srv.AddUser("syndrdb", "password") // Example user
	}

	// Start the server
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Handle graceful shutdown
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	<-shutdownSignal
	fmt.Println("\nShutting down server...")

	if err := srv.Stop(); err != nil {
		log.Printf("Error stopping server: %v", err)
	}

	fmt.Println("Server shutdown complete")
}

// validateArguments validates the arguments and returns an error if invalid
func validateArguments(args *settings.Arguments) error {
	// Check if data directory exists and is accessible
	dirInfo, err := os.Stat(args.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			// Try to create the directory
			err = os.MkdirAll(args.DataDir, 0755)
			if err != nil {
				return fmt.Errorf("could not create data directory: %w", err)
			}
		} else {
			return fmt.Errorf("error accessing data directory: %w", err)
		}
	} else if !dirInfo.IsDir() {
		return fmt.Errorf("data directory path exists but is not a directory: %s", args.DataDir)
	}

	// Check if log file can be written to
	if args.LogDir != "" {
		logDir := filepath.Dir(args.LogDir)
		if logDir != "." {
			if _, err := os.Stat(logDir); os.IsNotExist(err) {
				err = os.MkdirAll(logDir, 0755)
				if err != nil {
					return fmt.Errorf("could not create log directory: %w", err)
				}
			}
		}

		// Check if we can create/open the log file
		logFile, err := os.OpenFile(args.LogDir, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("could not open log file for writing: %w", err)
		}
		logFile.Close()
	}

	// Validate port range
	if args.Port < 1 || args.Port > 65535 {
		return fmt.Errorf("invalid port number: %d (must be between 1 and 65535)", args.Port)
	}

	// If config file is specified, check if it exists and is readable
	if args.ConfigFile != "" {
		_, err := os.Stat(args.ConfigFile)
		if err != nil {
			return fmt.Errorf("could not access config file: %w", err)
		}
	}

	// Validate mode
	validModes := map[string]bool{"standalone": true, "cluster": true}
	if _, valid := validModes[args.Mode]; !valid {
		return fmt.Errorf("invalid mode: %s (must be 'standalone' or 'cluster')", args.Mode)
	}

	return nil
}
