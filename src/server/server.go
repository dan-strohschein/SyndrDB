package server

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	"syndrdb/src/buffermgr"
	"syndrdb/src/data"
	"syndrdb/src/directors"
	"syndrdb/src/engine"

	"syndrdb/src/helpers"
	"syndrdb/src/models"
	"syndrdb/src/settings"
	"time"

	"go.uber.org/zap"
)

// Server represents the main TCP server for SyndrDB
type Server struct {
	Host              string
	Port              int
	Databases         map[string]*models.Database
	Listener          net.Listener
	AuthEnabled       bool
	Users             map[string]string // username -> hashed password
	ActiveConnections map[string]*Connection
	mu                sync.Mutex
	Running           bool
	databaseService   *directors.DatabaseService
	logger            *zap.SugaredLogger
	bufferPool        *buffermgr.BufferPool
}

// Connection represents an active client connection
type Connection struct {
	ID           string
	Conn         net.Conn
	Reader       *bufio.Reader
	Writer       *bufio.Writer
	DatabaseName string
	Database     *models.Database // Current database for this connection
	User         string
	Authorized   bool
	LastActive   time.Time
	Logger       *zap.SugaredLogger
}

// ConnectionString represents parsed MongoDB connection string
type ConnectionString struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Options  map[string]string
}

// // NewServer creates a new SyndrDB server instance
// func NewServer(host string, port int, database *engine.Database, authEnabled bool) *Server {

// 	databases, err := engine.LoadAllDatabases(settings.GetSettings().DataDir)
// 	if err != nil {
// 		log.Fatalf("Failed to load databases: %v", err)
// 	}

// 	return &Server{
// 		Host:              host,
// 		Port:              port,
// 		Databases:         make(map[string]*engine.Database),
// 		AuthEnabled:       authEnabled,
// 		Users:             make(map[string]string),
// 		ActiveConnections: make(map[string]*Connection),
// 	}
// }

// InitServer initializes the SyndrDB server
func InitServer(config *settings.Arguments) (*Server, error) {

	var logger *zap.Logger
	var err error

	if config.Debug {
		// Development configuration with more verbose output
		//logger, err = zap.NewDevelopment()
		z := zap.NewDevelopmentConfig()
		z.OutputPaths = []string{"stdout"}
		logger, err = z.Build()
	} else {
		// Production configuration
		logger, err = zap.NewProduction()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	// Create a sugared logger for easier API
	sugar := logger.Sugar()

	// Replace standard log with zap
	zap.ReplaceGlobals(logger)

	// Create database storage
	databaseStore, err := engine.NewDatabaseStore(config.DataDir, logger.Sugar())
	if err != nil {
		return nil, fmt.Errorf("failed to create database store: %w", err)
	}
	databaseFactory := engine.NewDatabaseFactory()

	// Create service
	databaseService := directors.NewDatabaseService(databaseStore, databaseFactory, config, sugar)

	// Create buffer pool
	bufferPool := buffermgr.NewBufferPool(config.BundleBufferSize, buffermgr.DefaultPageSize, sugar)

	// Create bundle service
	bundleStore, err := engine.NewBundleStore(config.DataDir, bufferPool, logger.Sugar())
	if err != nil {
		return nil, fmt.Errorf("failed to create bundle store: %w", err)
	}
	bundleFactory := engine.NewBundleFactory()
	documentFactory := engine.NewDocumentFactory()
	bundleService := directors.NewBundleService(bundleStore, bundleFactory, documentFactory, sugar, config)

	// Initialize the singleton
	directors.InitServiceManager(databaseService, bundleService, sugar)

	// Create a new server
	server := &Server{
		Host:              config.Host,
		Port:              config.Port,
		Databases:         make(map[string]*models.Database),
		AuthEnabled:       config.AuthEnabled,
		Users:             make(map[string]string),
		ActiveConnections: make(map[string]*Connection),
		databaseService:   databaseService,
		logger:            sugar,
		bufferPool:        bufferPool,
	}

	// Load all databases
	databases, err := databaseStore.LoadAllDatabaseDataFiles(config.DataDir)
	if err != nil {
		log.Printf("Warning: Error loading databases: %v", err)
		// Continue with empty database map - this allows creating new databases
	} else {
		server.Databases = databases
		log.Printf("Loaded %d databases", len(databases))
	}

	// If no databases were found, create a default database
	if len(server.Databases) == 0 && config.CreateDefaultDB {
		defaultDB := &models.Database{
			DatabaseID:    helpers.GenerateUUID(),
			Name:          "default",
			Description:   "Default database created at startup",
			DataDirectory: config.DataDir,
			Bundles:       make(map[string]models.Bundle),
			BundleFiles:   []string{},
		}

		// Save the default database
		err := databaseStore.CreateDatabaseDataFile(defaultDB)
		if err != nil {
			log.Printf("Warning: Failed to save default database: %v", err)
		} else {
			server.Databases[defaultDB.DatabaseID] = defaultDB
			log.Printf("Created default database with ID %s", defaultDB.DatabaseID)
		}
	}

	return server, nil
}

// Start begins listening for incoming connections
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.Host, s.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("error starting server on %s: %w", addr, err)
	}

	s.Listener = listener
	s.Running = true

	log.Printf("SyndrDB server listening on %s", addr)

	go s.acceptConnections()

	return nil
}

// Stop gracefully shuts down the server
func (s *Server) Stop() error {
	s.Running = false

	// Close all active connections
	s.mu.Lock()
	for id, conn := range s.ActiveConnections {
		conn.Conn.Close()
		delete(s.ActiveConnections, id)
	}
	s.mu.Unlock()

	// Close the listener
	if s.Listener != nil {
		return s.Listener.Close()
	}

	wg.Wait()

	// Flush any dirty pages
	s.bufferPool.FlushAllDirty()

	// Close the buffer pool & Release buffer memory
	err := s.bufferPool.ShutDown()
	if err != nil {
		s.logger.Warnf("Error during buffer pool shutdown: %v", err)
	}
	// Close open files

	// Flush any buffered log entries
	s.logger.Info("Server shutdown complete")
	s.logger.Sync()

	return nil
}

// AddUser adds a user with the given password
func (s *Server) AddUser(username, password string) {
	hashedPassword := hashPassword(password)
	s.Users[username] = hashedPassword
}

// Authentication function
func (s *Server) authenticate(username, password string) bool {
	hashedPassword, exists := s.Users[username]
	if !exists {
		return false
	}

	return hashedPassword == hashPassword(password)
}

var wg sync.WaitGroup

// acceptConnections handles incoming connection requests
func (s *Server) acceptConnections() {
	s.logger.Info("Server started accepting connections",
		zap.String("host", s.Host),
		zap.Int("port", s.Port))

	for s.Running {
		conn, err := s.Listener.Accept()
		if err != nil {
			if s.Running { // Only log if we're still supposed to be running
				s.logger.Errorw("Error accepting connection", "error", err)
			}
			continue
		}
		wg.Add(1)

		s.logger.Info("New connection received",
			zap.String("remoteAddr", conn.RemoteAddr().String()))

		// Handle each connection in a new goroutine
		//go s.handleConnection(conn)
		go func(c net.Conn) {
			defer wg.Done()
			s.handleConnection(c)
		}(conn)
	}
}

// handleConnection processes a single client connection
func (s *Server) handleConnection(conn net.Conn) {
	connID := generateConnectionID()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Create a connection-specific logger with connection ID context
	// Create a connection-specific logger with connection ID context
	connLogger := s.logger

	if settings.GetSettings().UserDebug {
		connLogger = connLogger.With(
			zap.String("connID", connID),
			zap.String("remoteAddr", conn.RemoteAddr().String()))
	}

	connection := &Connection{
		ID:         connID,
		Conn:       conn,
		Reader:     reader,
		Writer:     writer,
		Authorized: !s.AuthEnabled, // If auth is disabled, connection is automatically authorized
		LastActive: time.Now(),
		Logger:     connLogger,
	}

	// Register the connection
	s.mu.Lock()
	s.ActiveConnections[connID] = connection
	s.mu.Unlock()

	// Ensure connection is removed when this function exits
	defer func() {

		conn.Close()
		s.mu.Lock()
		delete(s.ActiveConnections, connID)
		s.mu.Unlock()
		connLogger.Info("Connection closed: %s", connID)
		connLogger.Sync()
	}()

	// Channel for received data
	dataCh := make(chan string)
	// Channel for errors
	errCh := make(chan error)
	// Done channel to signal termination
	doneCh := make(chan struct{})

	// Start a goroutine for reading
	go func() {

		defer close(dataCh)
		defer close(errCh)

		buffer := make([]byte, 1024)
		var partialData string // For storing incomplete data between reads

		for {
			select {
			case <-doneCh:
				return
			default:
				// Set a short deadline to avoid blocking
				conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

				// Try to read available data
				n, err := conn.Read(buffer)

				// Reset deadline
				conn.SetReadDeadline(time.Time{})

				if err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						// No data available, just continue
						continue
					}

					// Handle real errors (EOF, connection closed, etc.)
					errCh <- err
					return
				}

				if n > 0 {
					data := string(buffer[:n])
					connLogger.Infof("Read %d bytes", n)
					connLogger.Sync()
					// Append to any previous partial data
					data = partialData + data

					// Look for complete lines
					if strings.Contains(data, "\n") {
						lines := strings.Split(data, "\n")

						// The last element might be incomplete
						partialData = lines[len(lines)-1]

						// Process complete lines
						for i := 0; i < len(lines)-1; i++ {
							if line := strings.TrimSpace(lines[i]); line != "" {
								connLogger.Infof("Received line: %s", line)
								connLogger.Sync()
								dataCh <- line
							}
						}
					} else {
						// No newline, store entire chunk as partial
						partialData = data
						connLogger.Infof("Received data: %s", data)
						connLogger.Sync()
					}
				} else {
					connLogger.Info("No Data Seen")
					connLogger.Sync()
				}
			}
		}
	}()

	// Send welcome message
	writer.WriteString(fmt.Sprintf("%s\n", data.Welcome))
	writer.Flush()

	// Main processing loop
	for {
		select {
		case line, ok := <-dataCh:
			if !ok {
				// Channel closed

				goto cleanup
			}
			// Process the line
			connLogger.Infof("Received: %s", line)

			// Your existing logic for handling commands
			//When the client connects, it should send the connection string
			//as the first command.
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "syndrdb://") {
				connLogger.Debug("Reading connection string")

				connStr, err := parseConnectionString(s, line)
				if err != nil {
					connLogger.Errorw("Error parsing connection string", "error", err, "input", line)
					connLogger.Sync()
					sendError(writer, fmt.Sprintf("Invalid connection string: %v", err))
					// Give TCP stack time to send the data
					time.Sleep(100 * time.Millisecond)

					close(doneCh)
					return
				}

				connLogger.Infof("Client %s: Connected:", connection.ID)
				connLogger.Infof("Database: %s", connStr.Database)
				connLogger.Infof("User: %s", connStr.Username)

				connLogger.Sync()

				// Read client input

				line = strings.TrimSpace(line)
				connection.LastActive = time.Now()
				connection.DatabaseName = connStr.Database
				connection.Database = s.Databases[connStr.Database]

				connection.User = connStr.Username

				if !connection.Authorized {

					if !strings.EqualFold(connStr.Database, "default") {
						db, err := s.databaseService.GetDatabaseByName(connStr.Database)
						if err != nil {
							sendError(writer, fmt.Sprintf("Database %s does not exist", connStr.Database))
							return
						}
						if db == nil {
							sendError(writer, fmt.Sprintf("Database %s does not exist", connStr.Database))
							return
						}
					}

					// TODO: IF the db is legit, check to see if the user is allowed to access it
					if s.AuthEnabled && !s.authenticate(connStr.Username, connStr.Password) {
						sendError(writer, "Authentication failed")
						return
					}

					connection.Authorized = true
					connection.DatabaseName = connStr.Database
					connection.User = connStr.Username
					connection.Logger = connLogger.Desugar().Sugar()

					connLogger.Infow("Client authenticated",
						"user", connection.User,
						"database", connection.DatabaseName)

					sendSuccess(writer, "Authentication successful")
					continue
				}
			}

			// Process command for authenticated clients
			//log.Printf("Processing command from %s: %s", connection.ID, line)
			result, err := s.processCommand(connection, line)
			if err != nil {
				sendError(writer, err.Error())
			} else {
				sendResult(writer, result, connLogger)
			}
		case err, ok := <-errCh:
			if !ok {
				// Channel closed
				connLogger.Errorw("Error reading from client", "error", err)
				connLogger.Sync()
				goto cleanup
			}
			//connLogger.Errorw("Error reading from client", "error", err)
			return

		case <-time.After(30 * time.Second):
			// Idle timeout - send ping or check connection health
			connLogger.Info("Connection idle for 30 seconds")
		}
	}

cleanup:
	// Cleanup
	close(doneCh)

}

// Process a client command
func (s *Server) processCommand(conn *Connection, command string) (interface{}, error) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	// Use the new function to process and print the client data
	return s.ProcessClientData(conn, command)

}

func (s *Server) ProcessClientData(conn *Connection, data string) (interface{}, error) {
	// Log the received data
	// Get logger with connection context
	logger := s.logger.With("connID", conn.ID)

	// Log the received data
	logger.Infow("Received from client", "data", data)

	// If not JSON, treat as plain text command
	//fmt.Printf("\n--- Client Data (Plain Text) ---\n%s\n------------------------------\n", data)

	// Split the data into command parts
	parts := strings.Fields(data)
	if len(parts) == 0 {
		return map[string]interface{}{
			"status":    "received",
			"message":   "Empty command",
			"timestamp": time.Now().Format(time.RFC3339),
		}, nil
	}

	// Process the command
	return s.handleTextCommand(conn, data, parts[1:])
}

// handleTextCommand processes commands received in plain text format
func (s *Server) handleTextCommand(conn *Connection, command string, args []string) (interface{}, error) {
	serviceManager := directors.GetServiceManager()

	//s.logger.Infof("Debugging the command received: %s", command)
	//s.logger.Sync()

	stats := s.bufferPool.GetStats()
	s.logger.Debugf("Buffer stats before command: hits=%d, misses=%d, ratio=%.2f, used=%d/%d",
		stats.Hits, stats.Misses, stats.HitRatio, stats.UsedBuffers, stats.TotalBuffers)

	result, err := directors.CommandDirector(conn.Database, *serviceManager, command, s.logger)

	stats = s.bufferPool.GetStats()
	s.logger.Debugf("Buffer stats after command: hits=%d, misses=%d, ratio=%.2f, used=%d/%d",
		stats.Hits, stats.Misses, stats.HitRatio, stats.UsedBuffers, stats.TotalBuffers)

	return result, err

}

func parseConnectionString(server *Server, connStr string) (ConnectionString, error) {
	result := ConnectionString{
		Options: make(map[string]string),
	}

	// Strip syndr:// prefix if present

	// if strings.HasPrefix(connStr, prefix) {
	// 	connStr = connStr[len(prefix):]
	// }

	connStr = strings.TrimPrefix(connStr, "syndrdb://")
	// Extract options
	optionsParts := strings.Split(connStr, ":")
	// The Connection String is like this: host:port:database:username:password
	result.Host = optionsParts[0]
	// Convert port string to integer
	portNum, err := strconv.Atoi(optionsParts[1])
	if err != nil {
		return result, fmt.Errorf("invalid port number: %v", err)
	}
	result.Port = portNum
	result.Database = optionsParts[2]

	if result.Database == "" {
		return result, fmt.Errorf("database name cannot be empty")
	}

	// Check to make sure the database exists
	if !DatabaseExists(server.Databases, result.Database) {
		return result, fmt.Errorf("invalid database name: %s", result.Database)
	}

	result.Username = optionsParts[3]
	result.Password = optionsParts[4]
	// TODO Check to make sure the user exists
	// TODO Check to make sure the user has access to the database

	// connStr = optionsParts[0]
	// if len(optionsParts) > 1 {
	// 	optionsStr := optionsParts[1]
	// 	optionsList := strings.Split(optionsStr, "&")
	// 	for _, option := range optionsList {
	// 		keyValue := strings.SplitN(option, "=", 2)
	// 		if len(keyValue) == 2 {
	// 			result.Options[keyValue[0]] = keyValue[1]
	// 		}
	// 	}
	// }

	// // Extract database name
	// dbParts := strings.Split(connStr, "/")
	// if len(dbParts) > 1 {
	// 	result.Database = dbParts[1]
	// }
	// connStr = dbParts[0]

	// // Extract authentication details and host/port
	// authParts := strings.Split(connStr, "@")
	// hostPort := connStr

	// if len(authParts) > 1 {
	// 	credentialsPart := authParts[0]
	// 	hostPort = authParts[1]

	// 	credentialsParts := strings.SplitN(credentialsPart, ":", 2)
	// 	if len(credentialsParts) == 2 {
	// 		result.Username = credentialsParts[0]
	// 		result.Password = credentialsParts[1]
	// 	}
	// }

	// // Extract host and port
	// hostPortParts := strings.SplitN(hostPort, ":", 2)
	// result.Host = hostPortParts[0]

	// if len(hostPortParts) > 1 {
	// 	_, err := fmt.Sscanf(hostPortParts[1], "%d", &result.Port)
	// 	if err != nil {
	// 		return result, fmt.Errorf("invalid port: %v", err)
	// 	}
	// }

	return result, nil
}

func DatabaseExists(databases map[string]*models.Database, dbName string) bool {
	for _, db := range databases {
		if strings.EqualFold(db.Name, dbName) {
			return true
		}
	}
	return false
}

// Helper functions
func sendError(writer *bufio.Writer, message string) {
	response := map[string]interface{}{
		"status":  "error",
		"message": message,
	}
	jsonResponse, _ := json.Marshal(response)
	writer.WriteString(string(jsonResponse) + "\n")
	writer.Flush()
}

func sendSuccess(writer *bufio.Writer, message string) {
	response := map[string]interface{}{
		"status":  "success",
		"message": message,
	}
	jsonResponse, _ := json.Marshal(response)
	writer.WriteString(string(jsonResponse) + "\n")
	writer.Flush()
}

func sendResult(writer *bufio.Writer, result interface{}, logger *zap.SugaredLogger) {
	var data []byte

	switch typedResult := result.(type) {
	case *string:
		if typedResult != nil {
			// For string pointers, just write the string directly
			writer.WriteString(*typedResult + "\n")
			writer.Flush()
			return
		}
	case string:
		// For direct strings
		writer.WriteString(typedResult + "\n")
		writer.Flush()
		return
	default:
		// For other types, marshal to JSON

		data, _ = json.Marshal(result)
		logger.Infof("Sending result: %s", data)
		logger.Sync()
		writer.WriteString(string(data) + "\n")
		writer.Flush()
	}
}

func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

func generateConnectionID() string {
	now := time.Now().UnixNano()
	return fmt.Sprintf("conn_%x", now)
}
