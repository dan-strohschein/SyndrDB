package internal

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

// Client represents a TCP connection to the database server
type Client struct {
	conn     net.Conn
	reader   *bufio.Reader
	host     string
	port     int
	database string
	username string
	password string
}

// NewClient creates a new client instance
func NewClient(host string, port int, database, username, password string) *Client {
	return &Client{
		host:     host,
		port:     port,
		database: database,
		username: username,
		password: password,
	}
}

// CommandTerminator is a non-printable character signaling end of complete command batch.
// Using ASCII EOT (End of Transmission) for semantic clarity and protocol framing.
// This allows multi-statement commands (migrations, transactions) to be processed as a unit.
const CommandTerminator = "\x04"

// ParameterDelimiter is used to separate parameters in parameterized commands.
// Using ASCII ENQ (Enquiry) for parameter separation in prepared statement protocol.
// Format: EXECUTE stmt_name\x05param1\x05param2\x05...\x04
const ParameterDelimiter = "\x05"

// / This would be in your client package
func (c *Client) Connect() error {
	address := fmt.Sprintf("%s:%d", c.host, c.port)
	fmt.Printf("Connecting to TCP address: %s\n", address)

	var err error
	c.conn, err = net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	fmt.Println("TCP connection established successfully")

	// Set up your reader if needed
	c.reader = bufio.NewReader(c.conn)

	return nil
}

// Close closes the client connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SendCommand sends a command string to the server
func (c *Client) SendCommand(command string) error {
	if c.conn == nil {
		return fmt.Errorf("not connected to server")
	}

	// Make sure command ends with newline
	command = command + CommandTerminator

	// Debug output
	//fmt.Printf("Debug: Raw bytes being sent: %v\n", []byte(command))

	// Write the full command to the connection
	_, err := c.conn.Write([]byte(command))
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	return nil
}

// SendCommandWithParams sends a parameterized command with parameters using delimiter-based protocol
// Parameters are separated by ParameterDelimiter (\x05) with escape sequences for special characters:
// - \x05 in parameter values is escaped as \x05\x05
// - \x04 in parameter values is escaped as \x04\x04
// This provides efficient serialization without JSON overhead while maintaining safety.
func (c *Client) SendCommandWithParams(command string, params []string) error {
	if c.conn == nil {
		return fmt.Errorf("not connected to server")
	}

	// Build parameterized command: command\x05param1\x05param2\x05...\x04
	var builder strings.Builder
	builder.WriteString(command)

	for _, param := range params {
		builder.WriteString(ParameterDelimiter)
		// Escape special characters in parameter value
		escapedParam := escapeParameterValue(param)
		builder.WriteString(escapedParam)
	}

	builder.WriteString(CommandTerminator)

	// Write to connection
	_, err := c.conn.Write([]byte(builder.String()))
	if err != nil {
		return fmt.Errorf("failed to send parameterized command: %w", err)
	}

	return nil
}

// escapeParameterValue escapes special characters in parameter values
// - \x05 (ENQ/ParameterDelimiter) -> \x05\x05
// - \x04 (EOT/CommandTerminator) -> \x04\x04
func escapeParameterValue(value string) string {
	// Fast path: no special characters to escape
	if !strings.ContainsAny(value, "\x04\x05") {
		return value
	}

	var result strings.Builder
	result.Grow(len(value))

	for _, ch := range value {
		switch ch {
		case '\x04':
			result.WriteString("\x04\x04") // Escape EOT
		case '\x05':
			result.WriteString("\x05\x05") // Escape ENQ
		default:
			result.WriteRune(ch)
		}
	}

	return result.String()
}

// ReceiveResponse reads the server's response as a string
func (c *Client) ReceiveResponse() (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("not connected to server")
	}

	// Set a read deadline to avoid hanging if server doesn't respond
	err := c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return "", fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Read response line by line until we get an empty line or a specific terminator
	// Adjust this logic based on your server's protocol
	var responseBuilder strings.Builder

	// For simple line-based protocols:
	response, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read server response: %w", err)
	}

	responseBuilder.WriteString(response)

	// Reset the read deadline
	err = c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return "", fmt.Errorf("failed to reset read deadline: %w", err)
	}

	return responseBuilder.String(), nil

}

func (c *Client) CheckForMessage() (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("not connected to server")
	}

	// Set a very short read deadline to make this non-blocking
	c.conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	defer c.conn.SetReadDeadline(time.Time{}) // Reset deadline

	// Try to read data if available
	var buf [4096]byte
	n, err := c.conn.Read(buf[:])

	// Handle the case where no data is available
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			// This is expected for a non-blocking check - no data available
			return "", nil
		}
		// Return any other errors
		return "", err
	}

	// If we got data, return it
	if n > 0 {
		return string(buf[:n]), nil
	}

	return "", nil
}

// Send transmits data to the server
func (c *Client) Send(data []byte) (int, error) {
	if c.conn == nil {
		return 0, fmt.Errorf("not connected to server")
	}
	return c.conn.Write(data)
}

// Receive reads data from the server
func (c *Client) Receive(buffer []byte) (int, error) {
	if c.conn == nil {
		return 0, fmt.Errorf("not connected to server")
	}
	return c.conn.Read(buffer)
}
