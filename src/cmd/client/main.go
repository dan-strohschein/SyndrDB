package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syndrdb/src/cmd/client/internal"
	"syndrdb/src/cmd/client/settings"
	"syscall"
	"time"

	"golang.org/x/term"
)

func main() {
	// Create a new settings.Arguments instance
	args := settings.GetSettings()

	flag.StringVar(&args.ConnectionString, "connection_string", "", "Connection string format syndrdb://host:port:database:username:password")
	flag.StringVar(&args.ServerHost, "host", "localhost", "Server host")
	flag.IntVar(&args.ServerPort, "port", 1776, "Server port")
	flag.StringVar(&args.Database, "database", "testdb", "Database name")
	flag.StringVar(&args.Username, "username", "user", "Username")
	flag.StringVar(&args.Password, "password", "password", "Password")
	flag.BoolVar(&args.PrettyPrintResults, "pretty_print", true, "Pretty print JSON results with indentation (default: true)")
	flag.BoolVar(&args.Compress, "compress", false, "Enable zstd response compression")
	flag.BoolVar(&args.Pipeline, "pipeline", false, "Enable pipeline mode (READY sentinel framing for batch commands)")
	flag.IntVar(&args.HistorySize, "history_size", 250, "Maximum number of commands to keep in history")
	// Parse the command line
	flag.Parse()

	// Process non-flag arguments (commands)
	nonFlagArgs := flag.Args()

	if len(nonFlagArgs) >= 1 {
		args.Command = nonFlagArgs[0]

		// If additional arguments exist after the command
		if len(nonFlagArgs) > 1 {
			args.CommandArgs = nonFlagArgs[1:]
		}
	}

	//Parse the connection String if provided
	if args.ConnectionString != "" {
		// Append compression option if --compress flag is set
		if args.Compress && !strings.Contains(args.ConnectionString, "compress=") {
			// Connection string format: syndrdb://host:port:db:user:pass[:options]
			// Append :compress=zstd as the 6th colon-separated field
			trimmed := strings.TrimSuffix(args.ConnectionString, ";")
			trimmed = strings.TrimSpace(trimmed)
			args.ConnectionString = trimmed + ":compress=zstd"
		}

		// Append pipeline option if --pipeline flag is set
		if args.Pipeline && !strings.Contains(args.ConnectionString, "pipeline=") {
			trimmed := strings.TrimSuffix(args.ConnectionString, ";")
			trimmed = strings.TrimSpace(trimmed)
			// If options field already exists (6th colon-separated field), append with &
			parts := strings.Split(strings.TrimPrefix(trimmed, "syndrdb://"), ":")
			if len(parts) > 5 {
				args.ConnectionString = trimmed + "&pipeline=true"
			} else {
				args.ConnectionString = trimmed + ":pipeline=true"
			}
		}

		// Parse the connection string and set the fields accordingly
		err := internal.ValidateConnectionString(args.ConnectionString)
		if err != nil {
			log.Printf("Error parsing connection string: %v\n", err)
			fmt.Fprintf(os.Stderr, "Error parsing connection string: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Attempting to connect to %s:%d...\n", args.ServerHost, args.ServerPort)

	// Create a new client
	dbClient := internal.NewClient(
		args.ServerHost,
		args.ServerPort,
		args.Database,
		args.Username,
		args.Password,
	)

	// Connect to the server
	err := dbClient.Connect()
	if err != nil {
		log.Printf("Error connecting to server: %v\n", err)
		fmt.Fprintf(os.Stderr, "Error connecting to server: %v\n", err)
		os.Exit(1)
	}
	defer dbClient.Close()

	// Set pipeline mode on the client if requested
	dbClient.PipelineMode = args.Pipeline

	// Send connection string to the server immediately after connecting
	connectionCommand := fmt.Sprintf("%s;\n", args.ConnectionString)
	fmt.Printf("Sending connection info: database=%s, user=%s\n", args.Database, args.Username)

	err = dbClient.SendCommand(connectionCommand)
	if err != nil {
		log.Printf("Error sending connection string: %v\n", err)
		fmt.Fprintf(os.Stderr, "Error sending connection string: %v\n", err)
		os.Exit(1)
	}

	// Initial welcome message might arrive automatically
	response, err := dbClient.ReceiveResponse()
	if err != nil {
		log.Printf("Error receiving connection confirmation: %v\n", err)
		fmt.Fprintf(os.Stderr, "Error receiving connection confirmation: %v\n", err)
		os.Exit(1)
	}

	if args.PrettyPrintResults {
		response = prettyPrintJSON(response)
	}
	fmt.Printf("Server: %s\n", response)

	// Start interactive shell with async message handling
	startInteractiveShellWithAsync(dbClient, args)
}

// isTTY returns true when stdout is a terminal (used to decide raw mode output).
var isTTY = term.IsTerminal(int(os.Stdout.Fd()))

// rawPrintf is like fmt.Printf but translates \n → \r\n when the terminal is
// in raw mode (raw mode disables automatic output newline translation).
func rawPrintf(format string, a ...interface{}) {
	s := fmt.Sprintf(format, a...)
	if isTTY {
		s = strings.ReplaceAll(s, "\n", "\r\n")
	}
	fmt.Print(s)
}

// rawPrint is like fmt.Print but translates \n → \r\n in raw mode.
func rawPrint(a ...interface{}) {
	s := fmt.Sprint(a...)
	if isTTY {
		s = strings.ReplaceAll(s, "\n", "\r\n")
	}
	fmt.Print(s)
}

// getHistoryFilePath returns the path to the history file (~/.syndrdb_history).
func getHistoryFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".syndrdb_history"
	}
	return filepath.Join(home, ".syndrdb_history")
}

// startInteractiveShellWithAsync runs an interactive shell that can receive asynchronous messages
func startInteractiveShellWithAsync(dbClient *internal.Client, args *settings.Arguments) {
	var inputBuffer strings.Builder

	// Channel for receiving async messages from server
	messageChan := make(chan string)
	errorChan := make(chan error)
	inputChan := make(chan string)

	// Use WaitGroup to manage our goroutines
	var wg sync.WaitGroup
	wg.Add(2) // server listener + line editor

	// Context for cancelling goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load history and create line editor
	historyPath := getHistoryFilePath()
	history := internal.LoadHistory(historyPath, args.HistorySize)
	lineEditor := internal.NewLineEditor(history)

	// Install signal handler to restore terminal on SIGTERM/SIGINT
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		lineEditor.Cleanup()
		os.Exit(0)
	}()

	// Flag to signal goroutines to exit
	done := false

	// Start goroutine to listen for server messages
	go func() {
		defer wg.Done()

		for !done {
			// Non-blocking check for messages from server
			message, err := dbClient.CheckForMessage()
			if err != nil {
				if !done { // Only send error if we're not shutting down
					errorChan <- err
				}
				return
			}

			if message != "" {
				messageChan <- message
			}

			// Small sleep to avoid hammering the connection
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Start line editor goroutine (replaces old bufio.Reader goroutine)
	go func() {
		defer wg.Done()
		lineEditor.Run(ctx, inputChan)
	}()

	// Track TimeOnly mode for the current command (accessible to both sync and async response handlers)
	var currentTimeOnlyMode bool
	// Track if we're currently receiving a streamed response
	var isStreaming bool
	// Buffer for reassembling streamed responses
	var responseBuffer strings.Builder
	// Track chunk count for progress dots
	var chunkCount int

	for !done {
		select {
		case message := <-messageChan:
			// Pipeline mode: skip READY sentinel (it's a framing marker, not content)
			if strings.TrimSpace(message) == "READY" {
				continue
			}

			// Check if this is a streamed response (large chunk, typically 4096 bytes)
			isLargeChunk := len(message) >= 4096

			// If this is a large chunk and we're not already streaming, start streaming mode
			if isLargeChunk && !isStreaming {
				isStreaming = true
				chunkCount = 0
				responseBuffer.Reset()
				rawPrint("Receiving response")
			}

			if isStreaming {
				// Buffer the chunk
				responseBuffer.WriteString(message)
				chunkCount++

				// Show progress dot
				rawPrint(".")

				// Check if response is complete (balanced braces and ends with })
				assembledResponse := responseBuffer.String()
				trimmedResponse := strings.TrimSpace(assembledResponse)
				openBraces := strings.Count(trimmedResponse, "{")
				closeBraces := strings.Count(trimmedResponse, "}")
				hasBalancedBraces := openBraces > 0 && openBraces == closeBraces
				endsWithBrace := strings.HasSuffix(trimmedResponse, "}")

				// Try to parse as complete JSON
				var jsonData map[string]interface{}
				parseErr := json.Unmarshal([]byte(trimmedResponse), &jsonData)

				// If we can parse it and it appears complete, we're done streaming
				if parseErr == nil && hasBalancedBraces && endsWithBrace {
					// Streaming complete - clear the progress dots line
					rawPrint("\r                                                    \r")

					// Display the response based on TimeOnly mode
					if currentTimeOnlyMode {
						filteredResponse := filterTimeOnlyResponse(assembledResponse, args.PrettyPrintResults)
						rawPrintf("Server: %s\n", filteredResponse)
					} else {
						// Display full response
						if args.PrettyPrintResults {
							formattedResponse := prettyPrintJSON(assembledResponse)
							rawPrintf("Server: %s\n", formattedResponse)
						} else {
							rawPrintf("Server: %s\n", assembledResponse)
						}
					}

					// Reset streaming state
					isStreaming = false
					currentTimeOnlyMode = false
					responseBuffer.Reset()
					chunkCount = 0

					// Re-print the prompt
					lineEditor.RedrawPrompt()
					continue
				}
				// Otherwise, continue buffering and showing dots
				continue
			} else {
				// Not streaming - handle as normal response
				// Check if this looks like a complete JSON response
				trimmedMessage := strings.TrimSpace(message)
				var jsonData map[string]interface{}
				parseErr := json.Unmarshal([]byte(trimmedMessage), &jsonData)

				if parseErr == nil {
					// Complete JSON response - handle based on TimeOnly mode
					if currentTimeOnlyMode {
						filteredResponse := filterTimeOnlyResponse(message, args.PrettyPrintResults)
						rawPrintf("Server: %s\n", filteredResponse)
						currentTimeOnlyMode = false
					} else {
						if args.PrettyPrintResults {
							message = prettyPrintJSON(message)
						}
						rawPrintf("Server: %s\n", message)
					}
				} else {
					// Not JSON or incomplete - display as-is
					if args.PrettyPrintResults {
						message = prettyPrintJSON(message)
					}
					rawPrintf("Server: %s\n", message)
				}

				// Re-print the prompt and any partial input
				lineEditor.RedrawPrompt()
			}

		case err := <-errorChan:
			// Handle error from the message listener
			rawPrintf("\nError receiving server message: %v\n", err)

			// Close the connection
			dbClient.Close()

			// Print a goodbye message
			rawPrint("Connection lost. Disconnecting from server.\n")

			// Signal the goroutines to stop
			done = true
			cancel()

			// Save history before exit
			history.SaveHistory(historyPath)
			lineEditor.Cleanup()

			// Exit the program (wait for goroutines happens at end of function)
			os.Exit(1)

		case input := <-inputChan:
			// Add the input to our buffer
			inputBuffer.WriteString(input)

			// Check for exit commands
			trimmedInput := strings.TrimSpace(input)
			if trimmedInput == "exit;" || trimmedInput == "quit;" {
				rawPrint("Disconnecting from server. Goodbye!\n")
				done = true
				cancel()
				history.SaveHistory(historyPath)
				lineEditor.Cleanup()
				continue
			}

			// Check for Ctrl+Q
			if strings.EqualFold("^Q", trimmedInput) {
				rawPrint("Disconnecting from server. Goodbye!\n")
				done = true
				cancel()
				history.SaveHistory(historyPath)
				lineEditor.Cleanup()
				continue
			}

			// Check if the input contains a semicolon ending with newline
			if strings.HasSuffix(trimmedInput, ";") {
				// Get the complete command
				command := inputBuffer.String()

				// Reset the buffer for next command
				inputBuffer.Reset()

				// Reset response buffer and streaming state for new command
				responseBuffer.Reset()
				isStreaming = false
				chunkCount = 0

				// Add to history (the full command, trimmed)
				trimmedCommand := strings.TrimSpace(command)
				history.Add(trimmedCommand)

				// Check if command starts with "TimeOnly" prefix (case-insensitive)
				currentTimeOnlyMode = false
				if len(trimmedCommand) >= 8 && strings.EqualFold(trimmedCommand[:8], "TimeOnly") {
					currentTimeOnlyMode = true
					// Remove "TimeOnly" prefix and any following whitespace
					if len(trimmedCommand) > 8 {
						command = strings.TrimSpace(trimmedCommand[8:])
					} else {
						command = ""
					}
				}

				// Send the command to the server
				response, err := sendCommandToServer(dbClient, command)
				if err != nil {
					// Check if it's a timeout error - for large queries, response comes through async
					if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "i/o timeout") {
						// Timeout is expected for large queries - response will come through async
						// Keep currentTimeOnlyMode true so async chunks get filtered
					} else {
						// Real error - reset TimeOnly mode
						rawPrintf("Error: %v\n", err)
						log.Printf("Error: %v\n", err)
						currentTimeOnlyMode = false
					}
				} else if response != "" { // Only print non-empty responses
					if currentTimeOnlyMode {
						// Check if this is a complete JSON response
						var jsonData map[string]interface{}
						if err := json.Unmarshal([]byte(strings.TrimSpace(response)), &jsonData); err == nil {
							// Complete JSON - filter it
							response = filterTimeOnlyResponse(response, args.PrettyPrintResults)
							// Reset after processing
							currentTimeOnlyMode = false
							responseBuffer.Reset()
						} else {
							// Incomplete JSON - buffer it and wait for async chunks
							responseBuffer.WriteString(response)
							// Don't reset currentTimeOnlyMode - keep it true for async chunks
							// Don't print this partial response yet
							response = "" // Suppress printing
						}
					} else {
						if args.PrettyPrintResults {
							response = prettyPrintJSON(response)
						}
					}
					rawPrintf("Response: %s\n", response)
				}

				// Prompt will be redrawn by the line editor
				lineEditor.RedrawPrompt()
			}
		}
	}

	// Signal to our goroutines to exit and wait for them
	done = true
	cancel()
	history.SaveHistory(historyPath)
	lineEditor.Cleanup()
	wg.Wait()
}

// sendCommandToServer sends a command to the server and returns the response
func sendCommandToServer(dbClient *internal.Client, command string) (string, error) {
	// Send the command to the server
	err := dbClient.SendCommand(command)
	if err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}

	// Get the response from the server
	response, err := dbClient.ReceiveResponse()
	if err != nil {
		return "", fmt.Errorf("failed to receive response: %w", err)
	}

	return response, nil
}

func prettyPrintJSON(jsonStr string) string {
	// Try to parse the JSON
	var jsonData interface{}
	err := json.Unmarshal([]byte(jsonStr), &jsonData)
	if err != nil {
		// If it's not valid JSON, return the original string
		return jsonStr
	}

	// Pretty print the JSON with indentation
	prettyJSON, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		// If formatting fails, return the original string
		return jsonStr
	}

	return string(prettyJSON)
}

// filterTimeOnlyResponse extracts only ResultCount and ExecutionTimeMS from the JSON response
func filterTimeOnlyResponse(jsonStr string, prettyPrint bool) string {
	// Trim whitespace/newlines from the JSON string before parsing
	jsonStr = strings.TrimSpace(jsonStr)

	// Try to parse the JSON
	var jsonData map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &jsonData)
	if err != nil {
		// If it's not valid JSON, return the original string
		return jsonStr
	}

	// Create a new map with only ResultCount and ExecutionTimeMS
	filteredData := make(map[string]interface{})
	if resultCount, ok := jsonData["ResultCount"]; ok {
		filteredData["ResultCount"] = resultCount
	}
	if executionTime, ok := jsonData["ExecutionTimeMS"]; ok {
		filteredData["ExecutionTimeMS"] = executionTime
	}

	// Marshal the filtered data
	var filteredJSON []byte
	if prettyPrint {
		filteredJSON, err = json.MarshalIndent(filteredData, "", "  ")
	} else {
		filteredJSON, err = json.Marshal(filteredData)
	}
	if err != nil {
		// If marshaling fails, return the original string
		return jsonStr
	}

	return string(filteredJSON)
}
