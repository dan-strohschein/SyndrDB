package internal

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// zstdDecoder is a package-level singleton for decompressing zstd-compressed responses.
// zstd.Decoder is goroutine-safe after creation.
var zstdDecoder *zstd.Decoder

func init() {
	var err error
	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic("failed to create zstd decoder: " + err.Error())
	}
}

// Client represents a TCP connection to the database server
type Client struct {
	conn         net.Conn
	reader       *bufio.Reader
	host         string
	port         int
	database     string
	username     string
	password     string
	PipelineMode bool // When true, expect READY sentinel between pipeline responses
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

// ReceiveResponse reads the server's response as a string.
// Detects zstd-compressed responses (ZSTD:<length>\n<compressed bytes>\n)
// and decompresses them transparently.
func (c *Client) ReceiveResponse() (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("not connected to server")
	}

	// Set a read deadline to avoid hanging if server doesn't respond
	err := c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return "", fmt.Errorf("failed to set read deadline: %w", err)
	}

	// Read the first line — either raw JSON or ZSTD:<length> header
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read server response: %w", err)
	}

	// Check for zstd-compressed response
	if strings.HasPrefix(line, "ZSTD:") {
		// Parse compressed length from "ZSTD:<length>\n"
		lengthStr := strings.TrimSpace(strings.TrimPrefix(line, "ZSTD:"))
		compLen, parseErr := strconv.Atoi(lengthStr)
		if parseErr != nil {
			return "", fmt.Errorf("invalid ZSTD header length: %w", parseErr)
		}

		// Read exactly compLen bytes of compressed data
		compressed := make([]byte, compLen)
		_, readErr := io.ReadFull(c.reader, compressed)
		if readErr != nil {
			return "", fmt.Errorf("failed to read compressed data: %w", readErr)
		}

		// Read the trailing newline
		c.reader.ReadByte()

		// Decompress
		decompressed, decErr := zstdDecoder.DecodeAll(compressed, nil)
		if decErr != nil {
			return "", fmt.Errorf("zstd decompression failed: %w", decErr)
		}

		// Reset the read deadline
		c.conn.SetReadDeadline(time.Time{})

		return string(decompressed) + "\n", nil
	}

	// Reset the read deadline
	err = c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return "", fmt.Errorf("failed to reset read deadline: %w", err)
	}

	return line, nil
}

func (c *Client) CheckForMessage() (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("not connected to server")
	}

	// Set a very short read deadline to make this non-blocking
	c.conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))

	// Peek at available data to check for ZSTD header
	peekBuf, peekErr := c.reader.Peek(5)
	if peekErr != nil {
		c.conn.SetReadDeadline(time.Time{})
		if netErr, ok := peekErr.(net.Error); ok && netErr.Timeout() {
			return "", nil
		}
		return "", peekErr
	}

	// If this looks like a compressed response, switch to blocking read
	if string(peekBuf) == "ZSTD:" {
		// Give enough time to read the full compressed payload
		c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		defer c.conn.SetReadDeadline(time.Time{})

		// Read the ZSTD:<length>\n header line
		headerLine, readErr := c.reader.ReadString('\n')
		if readErr != nil {
			return "", fmt.Errorf("failed to read ZSTD header: %w", readErr)
		}

		// Parse length
		lengthStr := strings.TrimSpace(strings.TrimPrefix(headerLine, "ZSTD:"))
		compLen, parseErr := strconv.Atoi(lengthStr)
		if parseErr != nil {
			return "", fmt.Errorf("invalid ZSTD header length: %w", parseErr)
		}

		// Read exactly compLen bytes
		compressed := make([]byte, compLen)
		_, readErr = io.ReadFull(c.reader, compressed)
		if readErr != nil {
			return "", fmt.Errorf("failed to read compressed data: %w", readErr)
		}

		// Read trailing newline
		c.reader.ReadByte()

		// Decompress
		decompressed, decErr := zstdDecoder.DecodeAll(compressed, nil)
		if decErr != nil {
			return "", fmt.Errorf("zstd decompression failed: %w", decErr)
		}

		return string(decompressed) + "\n", nil
	}

	// Not compressed — read available raw data from the buffered reader.
	// Peek already loaded data into the bufio buffer, so we must read from c.reader.
	// The 1ms deadline from Peek is still active — serves as non-blocking timeout.
	defer c.conn.SetReadDeadline(time.Time{})
	var buf [4096]byte
	n, err := c.reader.Read(buf[:])

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return "", nil
		}
		return "", err
	}

	if n > 0 {
		return string(buf[:n]), nil
	}

	return "", nil
}

// SendPipelineCommands sends multiple commands in a single TCP write for pipeline processing.
// Commands are concatenated with \x04 terminators so the server processes them as a batch.
// Returns the number of commands sent. Use ReceivePipelineResponses to read the responses.
func (c *Client) SendPipelineCommands(commands []string) (int, error) {
	if c.conn == nil {
		return 0, fmt.Errorf("not connected to server")
	}

	var builder strings.Builder
	for _, cmd := range commands {
		builder.WriteString(cmd)
		builder.WriteString(CommandTerminator)
	}

	_, err := c.conn.Write([]byte(builder.String()))
	if err != nil {
		return 0, fmt.Errorf("failed to send pipeline commands: %w", err)
	}

	return len(commands), nil
}

// ReceivePipelineResponses reads exactly n responses from the server.
// In pipeline mode (READY sentinel framing), each response is terminated by a "READY\n" line.
// In non-pipeline mode, each response is a single line (JSON or ZSTD-compressed).
func (c *Client) ReceivePipelineResponses(n int) ([]string, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("not connected to server")
	}

	responses := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if c.PipelineMode {
			// In pipeline mode, read lines until we see READY sentinel
			var respBuilder strings.Builder
			for {
				err := c.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
				if err != nil {
					return responses, fmt.Errorf("failed to set read deadline: %w", err)
				}

				line, err := c.reader.ReadString('\n')
				if err != nil {
					return responses, fmt.Errorf("failed to read pipeline response %d: %w", i+1, err)
				}

				if strings.TrimSpace(line) == "READY" {
					break
				}

				// Handle ZSTD-compressed response within pipeline
				if strings.HasPrefix(line, "ZSTD:") {
					lengthStr := strings.TrimSpace(strings.TrimPrefix(line, "ZSTD:"))
					compLen, parseErr := strconv.Atoi(lengthStr)
					if parseErr != nil {
						return responses, fmt.Errorf("invalid ZSTD header in pipeline response %d: %w", i+1, parseErr)
					}

					compressed := make([]byte, compLen)
					_, readErr := io.ReadFull(c.reader, compressed)
					if readErr != nil {
						return responses, fmt.Errorf("failed to read compressed pipeline data %d: %w", i+1, readErr)
					}
					c.reader.ReadByte() // trailing \n

					decompressed, decErr := zstdDecoder.DecodeAll(compressed, nil)
					if decErr != nil {
						return responses, fmt.Errorf("zstd decompression failed for pipeline response %d: %w", i+1, decErr)
					}
					respBuilder.WriteString(string(decompressed))
					respBuilder.WriteByte('\n')
					continue
				}

				respBuilder.WriteString(line)
			}
			responses = append(responses, strings.TrimSpace(respBuilder.String()))
		} else {
			// Non-pipeline mode: each response is from a single ReceiveResponse call
			resp, err := c.ReceiveResponse()
			if err != nil {
				return responses, fmt.Errorf("failed to receive response %d: %w", i+1, err)
			}
			responses = append(responses, strings.TrimSpace(resp))
		}
	}

	c.conn.SetReadDeadline(time.Time{})
	return responses, nil
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
