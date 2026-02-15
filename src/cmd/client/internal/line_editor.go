package internal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/term"
)

// LineEditor provides readline-like line editing with arrow key support,
// history navigation, and cursor movement in raw terminal mode.
type LineEditor struct {
	history    *RingBuffer
	prompt     string
	buf        []rune  // current line buffer
	pos        int     // cursor position within buf
	histIdx    int     // current position in history (-1 = live line)
	savedLine  string  // saved live line when browsing history
	fd         int     // stdin file descriptor
	oldState   *term.State
	mu         sync.Mutex
	redrawCh   chan struct{}
	isRaw      bool
}

// NewLineEditor creates a line editor backed by the given history.
func NewLineEditor(history *RingBuffer) *LineEditor {
	return &LineEditor{
		history:  history,
		prompt:   "> ",
		histIdx:  -1,
		fd:       int(os.Stdin.Fd()),
		redrawCh: make(chan struct{}, 1),
	}
}

// SetPrompt changes the displayed prompt string.
func (le *LineEditor) SetPrompt(p string) {
	le.mu.Lock()
	le.prompt = p
	le.mu.Unlock()
}

// RedrawPrompt re-renders the current prompt and line buffer.
// Safe to call from any goroutine (e.g. after printing async server output).
func (le *LineEditor) RedrawPrompt() {
	select {
	case le.redrawCh <- struct{}{}:
	default:
	}
}

// Cleanup restores the terminal to its original state.
func (le *LineEditor) Cleanup() {
	le.mu.Lock()
	defer le.mu.Unlock()
	if le.isRaw && le.oldState != nil {
		term.Restore(le.fd, le.oldState)
		le.isRaw = false
	}
}

// Run enters raw mode and reads lines from stdin, sending completed lines
// to inputChan. It blocks until ctx is cancelled or EOF is reached.
// Falls back to bufio line-buffered mode if stdin is not a terminal.
func (le *LineEditor) Run(ctx context.Context, inputChan chan<- string) {
	if !term.IsTerminal(le.fd) {
		le.runLineBuffered(ctx, inputChan)
		return
	}
	le.runRaw(ctx, inputChan)
}

// runLineBuffered is the fallback for piped/non-TTY input.
func (le *LineEditor) runLineBuffered(ctx context.Context, inputChan chan<- string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF && line != "" {
				select {
				case inputChan <- line + "\n":
				case <-ctx.Done():
				}
			}
			return
		}
		select {
		case inputChan <- line:
		case <-ctx.Done():
			return
		}
	}
}

// runRaw enters raw mode and processes individual keypresses.
func (le *LineEditor) runRaw(ctx context.Context, inputChan chan<- string) {
	oldState, err := term.MakeRaw(le.fd)
	if err != nil {
		// Fall back to line-buffered if raw mode fails
		le.runLineBuffered(ctx, inputChan)
		return
	}
	le.mu.Lock()
	le.oldState = oldState
	le.isRaw = true
	le.mu.Unlock()

	defer le.Cleanup()

	// Byte pump goroutine — reads from os.Stdin into a channel
	byteCh := make(chan byte, 64)
	go func() {
		b := make([]byte, 1)
		for {
			n, err := os.Stdin.Read(b)
			if n > 0 {
				byteCh <- b[0]
			}
			if err != nil {
				close(byteCh)
				return
			}
		}
	}()

	le.refreshLine()

	for {
		select {
		case <-ctx.Done():
			return

		case <-le.redrawCh:
			le.refreshLine()

		case b, ok := <-byteCh:
			if !ok {
				// EOF — send remaining buffer if non-empty, then exit
				if len(le.buf) > 0 {
					line := string(le.buf) + "\n"
					select {
					case inputChan <- line:
					case <-ctx.Done():
					}
				}
				return
			}
			action := le.handleByte(b, byteCh)
			switch action {
			case actionEmitLine:
				line := string(le.buf) + "\n"
				le.buf = le.buf[:0]
				le.pos = 0
				le.histIdx = -1
				le.savedLine = ""
				select {
				case inputChan <- line:
				case <-ctx.Done():
					return
				}
				le.refreshLine()
			case actionEOF:
				return
			case actionRefresh:
				le.refreshLine()
			}
		}
	}
}

type lineAction int

const (
	actionNone     lineAction = iota
	actionEmitLine            // Enter was pressed — send the line
	actionEOF                 // Ctrl-D on empty line
	actionRefresh             // Need to redraw
)

// handleByte processes a single byte (or escape sequence) and returns what action to take.
func (le *LineEditor) handleByte(b byte, byteCh <-chan byte) lineAction {
	switch b {
	case '\r', '\n': // Enter
		rawWrite("\r\n")
		return actionEmitLine

	case 0x03: // Ctrl-C — send SIGINT so the signal handler can exit gracefully
		le.Cleanup()
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		return actionNone

	case 0x04: // Ctrl-D — EOF on empty, delete char otherwise
		if len(le.buf) == 0 {
			rawWrite("\r\n")
			return actionEOF
		}
		le.deleteChar()
		return actionRefresh

	case 0x7f, 0x08: // Backspace (0x7f) or Ctrl-H (0x08)
		le.backspace()
		return actionRefresh

	case 0x01: // Ctrl-A — beginning of line
		le.pos = 0
		return actionRefresh

	case 0x05: // Ctrl-E — end of line
		le.pos = len(le.buf)
		return actionRefresh

	case 0x0b: // Ctrl-K — kill to end of line
		le.buf = le.buf[:le.pos]
		return actionRefresh

	case 0x15: // Ctrl-U — kill to beginning of line
		le.buf = le.buf[le.pos:]
		le.pos = 0
		return actionRefresh

	case 0x17: // Ctrl-W — delete word backwards
		le.deleteWordBack()
		return actionRefresh

	case 0x0c: // Ctrl-L — clear screen and redraw
		rawWrite("\x1b[2J\x1b[H")
		return actionRefresh

	case 0x1b: // ESC — start of escape sequence
		return le.handleEscape(byteCh)

	case '\t': // Tab — ignore for now
		return actionNone

	default:
		if b >= 0x20 { // printable ASCII
			le.insertRune(rune(b))
			return actionRefresh
		}
		return actionNone
	}
}

// handleEscape parses an escape sequence (arrow keys, Home, End, Delete).
func (le *LineEditor) handleEscape(byteCh <-chan byte) lineAction {
	// Wait up to 50ms for the next byte to distinguish lone ESC from sequence
	select {
	case b2, ok := <-byteCh:
		if !ok {
			return actionNone
		}
		if b2 != '[' && b2 != 'O' {
			return actionNone // unknown sequence, ignore
		}
		// Read the sequence identifier
		select {
		case b3, ok := <-byteCh:
			if !ok {
				return actionNone
			}
			return le.dispatchCSI(b3, byteCh)
		case <-time.After(50 * time.Millisecond):
			return actionNone
		}
	case <-time.After(50 * time.Millisecond):
		return actionNone // lone ESC key
	}
}

// dispatchCSI handles CSI (Control Sequence Introducer) sequences: ESC [ <b3>
func (le *LineEditor) dispatchCSI(b3 byte, byteCh <-chan byte) lineAction {
	switch b3 {
	case 'A': // Up arrow — history previous
		le.historyUp()
		return actionRefresh
	case 'B': // Down arrow — history next
		le.historyDown()
		return actionRefresh
	case 'C': // Right arrow
		if le.pos < len(le.buf) {
			le.pos++
		}
		return actionRefresh
	case 'D': // Left arrow
		if le.pos > 0 {
			le.pos--
		}
		return actionRefresh
	case 'H': // Home
		le.pos = 0
		return actionRefresh
	case 'F': // End
		le.pos = len(le.buf)
		return actionRefresh
	case '3': // Delete key: ESC [ 3 ~
		// Consume the trailing '~'
		select {
		case <-byteCh:
		case <-time.After(50 * time.Millisecond):
		}
		le.deleteChar()
		return actionRefresh
	case '1': // Home: ESC [ 1 ~
		select {
		case <-byteCh:
		case <-time.After(50 * time.Millisecond):
		}
		le.pos = 0
		return actionRefresh
	case '4': // End: ESC [ 4 ~
		select {
		case <-byteCh:
		case <-time.After(50 * time.Millisecond):
		}
		le.pos = len(le.buf)
		return actionRefresh
	default:
		return actionNone
	}
}

// insertRune inserts a character at the current cursor position.
func (le *LineEditor) insertRune(r rune) {
	if le.pos == len(le.buf) {
		le.buf = append(le.buf, r)
	} else {
		le.buf = append(le.buf, 0)
		copy(le.buf[le.pos+1:], le.buf[le.pos:])
		le.buf[le.pos] = r
	}
	le.pos++
}

// backspace deletes the character before the cursor.
func (le *LineEditor) backspace() {
	if le.pos > 0 {
		le.buf = append(le.buf[:le.pos-1], le.buf[le.pos:]...)
		le.pos--
	}
}

// deleteChar deletes the character at the cursor (like the Delete key).
func (le *LineEditor) deleteChar() {
	if le.pos < len(le.buf) {
		le.buf = append(le.buf[:le.pos], le.buf[le.pos+1:]...)
	}
}

// deleteWordBack deletes the word before the cursor (Ctrl-W).
func (le *LineEditor) deleteWordBack() {
	if le.pos == 0 {
		return
	}
	// Skip trailing spaces
	end := le.pos
	for le.pos > 0 && le.buf[le.pos-1] == ' ' {
		le.pos--
	}
	// Delete until next space
	for le.pos > 0 && le.buf[le.pos-1] != ' ' {
		le.pos--
	}
	le.buf = append(le.buf[:le.pos], le.buf[end:]...)
}

// historyUp navigates to the previous (older) history entry.
func (le *LineEditor) historyUp() {
	if le.history.Len() == 0 {
		return
	}
	if le.histIdx == -1 {
		// Save current line before entering history
		le.savedLine = string(le.buf)
		le.histIdx = le.history.Len() - 1
	} else if le.histIdx > 0 {
		le.histIdx--
	} else {
		return // already at oldest
	}
	entry := le.history.Get(le.histIdx)
	le.buf = []rune(strings.TrimRight(entry, "\n"))
	le.pos = len(le.buf)
}

// historyDown navigates to the next (newer) history entry.
func (le *LineEditor) historyDown() {
	if le.histIdx == -1 {
		return // not in history
	}
	if le.histIdx < le.history.Len()-1 {
		le.histIdx++
		entry := le.history.Get(le.histIdx)
		le.buf = []rune(strings.TrimRight(entry, "\n"))
		le.pos = len(le.buf)
	} else {
		// Return to the saved live line
		le.histIdx = -1
		le.buf = []rune(le.savedLine)
		le.pos = len(le.buf)
	}
}

// refreshLine redraws the prompt and buffer.
func (le *LineEditor) refreshLine() {
	le.mu.Lock()
	prompt := le.prompt
	le.mu.Unlock()

	line := string(le.buf)
	cursorOffset := len(prompt) + le.pos

	// \r = go to column 0
	// print prompt + buffer
	// \x1b[K = clear from cursor to end of line
	// \r\x1b[<n>C = reposition cursor
	out := "\r" + prompt + line + "\x1b[K"
	if cursorOffset > 0 {
		out += fmt.Sprintf("\r\x1b[%dC", cursorOffset)
	} else {
		out += "\r"
	}
	rawWrite(out)
}

// rawWrite writes directly to stdout — needed in raw mode where \n doesn't
// translate to \r\n automatically.
func rawWrite(s string) {
	os.Stdout.WriteString(s)
}
