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
	if !strings.HasSuffix(command, "\n") {
		command = command + "\n"
	}

	// Debug output
	//fmt.Printf("Debug: Raw bytes being sent: %v\n", []byte(command))

	// Write the full command to the connection
	_, err := c.conn.Write([]byte(command))
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	return nil
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
