package server

import (
	"testing"
)

func FuzzParameterizedCommand(f *testing.F) {
	// Seed corpus: parameterized commands
	seeds := []string{
		// No parameters
		`SELECT * FROM "Users"`,
		`BEGIN TRANSACTION`,
		`COMMIT`,
		// With parameters (\x05 delimiter)
		"EXECUTE my_stmt\x05param1\x05param2",
		"EXECUTE stmt\x05value1",
		"EXECUTE stmt\x05",
		"EXECUTE stmt\x05a\x05b\x05c\x05d",
		// Edge cases
		"",
		"\x05",
		"\x05\x05",
		"\x05param",
		"cmd\x05",
		"cmd\x05\x05",
		// Escaped command terminator
		"EXECUTE stmt\x04\x04",
		// Long parameters
		"EXECUTE stmt\x05" + "a]a]a]a]a]a]a]a]a]a]a]a]a]a]a]a]",
		// Multiple delimiters
		"\x05\x05\x05",
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, rawCommand string) {
		command, params, hasParams, err := ParseParameterizedCommand(rawCommand)

		if err != nil {
			// Validation error — fine, just no panic
			return
		}

		// Property: if hasParams, params slice is non-nil
		if hasParams && params == nil {
			t.Fatal("hasParams=true but params is nil")
		}

		// Property: if !hasParams, command equals original input
		if !hasParams && command != rawCommand {
			t.Fatalf("no params: expected command=%q, got %q", rawCommand, command)
		}
	})
}
