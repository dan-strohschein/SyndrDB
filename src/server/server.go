package server

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"syndrdb/src/engine"
	"time"
)

// Server represents the main TCP server for SyndrDB
type Server struct {
	Host              string
	Port              int
	Database          *engine.Database
	Listener          net.Listener
	AuthEnabled       bool
	Users             map[string]string // username -> hashed password
	ActiveConnections map[string]*Connection
	mu                sync.Mutex
	Running           bool
}

// Connection represents an active client connection
type Connection struct {
	ID         string
	Conn       net.Conn
	Reader     *bufio.Reader
	Writer     *bufio.Writer
	Database   string
	User       string
	Authorized bool
	LastActive time.Time
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

// NewServer creates a new SyndrDB server instance
func NewServer(host string, port int, database *engine.Database, authEnabled bool) *Server {
	return &Server{
		Host:              host,
		Port:              port,
		Database:          database,
		AuthEnabled:       authEnabled,
		Users:             make(map[string]string),
		ActiveConnections: make(map[string]*Connection),
	}
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

// acceptConnections handles incoming connection requests
func (s *Server) acceptConnections() {
	for s.Running {
		conn, err := s.Listener.Accept()
		if err != nil {
			if s.Running { // Only log if we're still supposed to be running
				log.Printf("Error accepting connection: %v", err)
			}
			continue
		}

		log.Printf("New connection from %s", conn.RemoteAddr().String())

		// Handle each connection in a new goroutine
		go s.handleConnection(conn)
	}
}

// handleConnection processes a single client connection
func (s *Server) handleConnection(conn net.Conn) {
	connID := generateConnectionID()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	connection := &Connection{
		ID:         connID,
		Conn:       conn,
		Reader:     reader,
		Writer:     writer,
		Authorized: !s.AuthEnabled, // If auth is disabled, connection is automatically authorized
		LastActive: time.Now(),
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
		log.Printf("Connection closed: %s", connID)
	}()

	// Send welcome message
	writer.WriteString("SyndrDB server ready. Please authenticate.\n")
	writer.Flush()

	// Process client commands until disconnection
	for {
		// Read client input
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading from client: %v", err)
			return
		}

		line = strings.TrimSpace(line)
		connection.LastActive = time.Now()

		// Handle connection string if not yet authenticated
		if !connection.Authorized {
			connStr, err := parseConnectionString(line)
			if err != nil {
				sendError(writer, fmt.Sprintf("Invalid connection string: %v", err))
				continue
			}

			// Authenticate
			if s.AuthEnabled && !s.authenticate(connStr.Username, connStr.Password) {
				sendError(writer, "Authentication failed")
				continue
			}

			connection.Authorized = true
			connection.Database = connStr.Database
			connection.User = connStr.Username

			log.Printf("Client authenticated: user=%s db=%s", connection.User, connection.Database)
			sendSuccess(writer, "Authentication successful")
			continue
		}

		// Process command for authenticated clients
		result, err := s.processCommand(connection, line)
		if err != nil {
			sendError(writer, err.Error())
		} else {
			sendResult(writer, result)
		}
	}
}

// Process a client command
func (s *Server) processCommand(conn *Connection, command string) (interface{}, error) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	// switch strings.ToUpper(parts[0]) {
	// case "QUERY":
	//     if len(parts) < 2 {
	//         return nil, fmt.Errorf("QUERY requires a query string")
	//     }
	//     queryStr := strings.Join(parts[1:], " ")
	//     // Here you'd call your query engine
	//     return map[string]interface{}{
	//         "command": "query",
	//         "status": "received",
	//         "query": queryStr,
	//     }, nil

	// case "CREATE":
	//     if len(parts) < 2 {
	//         return nil, fmt.Errorf("CREATE requires a bundle specification")
	//     }
	//     bundleStr := strings.Join(parts[1:], " ")
	//     // Here you'd call your bundle creation logic
	//     return map[string]interface{}{
	//         "command": "create",
	//         "status": "received",
	//         "bundle": bundleStr,
	//     }, nil

	// case "UPDATE":
	//     if len(parts) < 2 {
	//         return nil, fmt.Errorf("UPDATE requires a bundle specification")
	//     }
	//     bundleStr := strings.Join(parts[1:], " ")
	//     // Here you'd call your bundle update logic
	//     return map[string]interface{}{
	//         "command": "update",
	//         "status": "received",
	//         "bundle": bundleStr,
	//     }, nil

	// case "DELETE":
	//     if len(parts) < 2 {
	//         return nil, fmt.Errorf("DELETE requires a bundle name")
	//     }
	//     bundleName := parts[1]
	//     // Here you'd call your bundle deletion logic
	//     return map[string]interface{}{
	//         "command": "delete",
	//         "status": "received",
	//         "bundle": bundleName,
	//     }, nil

	// default:
	//     return nil, fmt.Errorf("unknown command: %s", parts[0])
	// }
	return nil, nil
}

// Parse MongoDB-like connection string
// mongodb://username:password@host:port/database?option1=value1&option2=value2
func parseConnectionString(connStr string) (ConnectionString, error) {
	result := ConnectionString{
		Options: make(map[string]string),
	}

	// Strip mongodb:// prefix if present
	prefix := "syndr://"
	if strings.HasPrefix(connStr, prefix) {
		connStr = connStr[len(prefix):]
	}

	// Extract options
	optionsParts := strings.Split(connStr, "?")
	connStr = optionsParts[0]
	if len(optionsParts) > 1 {
		optionsStr := optionsParts[1]
		optionsList := strings.Split(optionsStr, "&")
		for _, option := range optionsList {
			keyValue := strings.SplitN(option, "=", 2)
			if len(keyValue) == 2 {
				result.Options[keyValue[0]] = keyValue[1]
			}
		}
	}

	// Extract database name
	dbParts := strings.Split(connStr, "/")
	if len(dbParts) > 1 {
		result.Database = dbParts[1]
	}
	connStr = dbParts[0]

	// Extract authentication details and host/port
	authParts := strings.Split(connStr, "@")
	hostPort := connStr

	if len(authParts) > 1 {
		credentialsPart := authParts[0]
		hostPort = authParts[1]

		credentialsParts := strings.SplitN(credentialsPart, ":", 2)
		if len(credentialsParts) == 2 {
			result.Username = credentialsParts[0]
			result.Password = credentialsParts[1]
		}
	}

	// Extract host and port
	hostPortParts := strings.SplitN(hostPort, ":", 2)
	result.Host = hostPortParts[0]

	if len(hostPortParts) > 1 {
		_, err := fmt.Sscanf(hostPortParts[1], "%d", &result.Port)
		if err != nil {
			return result, fmt.Errorf("invalid port: %v", err)
		}
	}

	return result, nil
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

func sendResult(writer *bufio.Writer, result interface{}) {
	jsonResponse, _ := json.Marshal(result)
	writer.WriteString(string(jsonResponse) + "\n")
	writer.Flush()
}

func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

func generateConnectionID() string {
	now := time.Now().UnixNano()
	return fmt.Sprintf("conn_%x", now)
}
