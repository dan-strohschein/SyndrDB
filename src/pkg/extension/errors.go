package extension

import "errors"

// ErrNotHandled signals that the extension chose not to handle this command.
// CommandDirector will fall through to core routing.
var ErrNotHandled = errors.New("extension: command not handled")
