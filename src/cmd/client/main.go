package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"syndrdb/src/cmd/client/internal"
	"syndrdb/src/cmd/client/settings"
	"time"
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

// startInteractiveShellWithAsync runs an interactive shell that can receive asynchronous messages
func startInteractiveShellWithAsync(dbClient *internal.Client, args *settings.Arguments) {
	reader := bufio.NewReader(os.Stdin)
	var inputBuffer strings.Builder

	// Channel for receiving async messages from server
	messageChan := make(chan string)
	errorChan := make(chan error)
	inputChan := make(chan string) // New channel for handling input

	// Use WaitGroup to manage our goroutines
	var wg sync.WaitGroup
	wg.Add(2) // Now we have 2 goroutines to manage

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

	// Start goroutine to handle user input
	go func() {
		defer wg.Done()

		for !done {
			// This will block until the user enters a line
			line, err := reader.ReadString('\n')
			if err != nil {
				if !done { // Only report errors if we're not shutting down
					fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
				}
				continue
			}

			if !done { // Only send if we're not shutting down
				inputChan <- line
			}
		}
	}()

	// Print the initial prompt
	fmt.Print("> ")

	for !done {
		select {
		case message := <-messageChan:
			// Clear the current line if needed
			fmt.Print("\r                                                    \r")

			// Print the received message
			if args.PrettyPrintResults {
				message = prettyPrintJSON(message)
			}
			fmt.Printf("Server: %s\n", message)

			// Re-print the prompt and any partial input
			fmt.Print("> " + inputBuffer.String())

		case err := <-errorChan:
			// Handle error from the message listener
			fmt.Printf("\nError receiving server message: %v\n", err)

			// Close the connection
			dbClient.Close()

			// Print a goodbye message
			fmt.Println("Connection lost. Disconnecting from server.")

			// Signal the goroutines to stop
			done = true

			// Exit the program (wait for goroutines happens at end of function)
			os.Exit(1)

		case input := <-inputChan:
			// Add the input to our buffer
			inputBuffer.WriteString(input)

			// Check for exit commands
			trimmedInput := strings.TrimSpace(input)
			if trimmedInput == "exit;" || trimmedInput == "quit;" {
				fmt.Println("\nDisconnecting from server. Goodbye!")
				done = true
				continue
			}

			// Check for Ctrl+Q
			if strings.EqualFold("^Q", trimmedInput) {
				fmt.Println("\nDisconnecting from server. Goodbye!")
				done = true
				continue
			}

			// Check if the input contains a semicolon ending with newline
			if strings.HasSuffix(trimmedInput, ";") {
				// Get the complete command
				command := inputBuffer.String()

				// For debugging
				fmt.Printf("Debug: Sending command: %q\n", command)

				// Reset the buffer for next command
				inputBuffer.Reset()

				// Send the command to the server
				response, err := sendCommandToServer(dbClient, command)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					log.Printf("Error: %v\n", err)
				} else if response != "" { // Only print non-empty responses
					if args.PrettyPrintResults {
						response = prettyPrintJSON(response)
					}
					fmt.Printf("Response: %s\n", response)
				}

				// Print the prompt for the next command
				fmt.Print("> ")
			}
		}
	}

	// Signal to our goroutines to exit and wait for them
	done = true
	wg.Wait()
}

// readInputWithTimeout reads user input with a short timeout to allow for async message handling
// readInputWithTimeout reads user input with a short timeout to allow for async message handling
func readInputWithTimeout(reader *bufio.Reader) (string, error) {
	// Check if there's data in the buffer
	if reader.Buffered() > 0 {
		return reader.ReadString('\n')
	}

	// Use a better approach to check for input availability
	var readString string
	var err error

	// This is a simple non-blocking check using a goroutine and channel
	inputCh := make(chan struct{})
	go func() {
		readString, err = reader.ReadString('\n')
		close(inputCh)
	}()

	// Try to read for a very short time, then continue with the main loop
	select {
	case <-inputCh:
		// Input was available and read successfully
		return readString, err
	case <-time.After(10 * time.Millisecond):
		// No input available in our short timeout
		return "", fmt.Errorf("no input available")
	}
}

// isInputAvailable checks if there is input ready to be read
func isInputAvailable() bool {
	// This is a simplified implementation - a more robust version would use platform-specific
	// mechanisms to check stdin without blocking
	return false
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
