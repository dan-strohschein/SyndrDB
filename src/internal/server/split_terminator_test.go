package server

import (
	"reflect"
	"testing"
)

func TestSplitOnCommandTerminator(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		wantComp  []string
		wantIncomp string
	}{
		{
			name:      "single command with terminator",
			data:      "cmd1\x04",
			wantComp:  []string{"cmd1"},
			wantIncomp: "",
		},
		{
			name:      "two commands in one batch - reproduces bunching bug",
			data:      "cmd1\x04cmd2\x04",
			wantComp:  []string{"cmd1", "cmd2"},
			wantIncomp: "",
		},
		{
			name:      "incomplete second command - carry-over",
			data:      "cmd1\x04cmd2",
			wantComp:  []string{"cmd1"},
			wantIncomp: "cmd2",
		},
		{
			name:      "escaped \\x04\\x04 in param value - do not split",
			data:      "EXECUTE x\x05p\x04\x04q\x05y\x04",
			wantComp:  []string{"EXECUTE x\x05p\x04\x04q\x05y"},
			wantIncomp: "",
		},
		{
			name:      "no terminator - all incomplete",
			data:      "cmd1",
			wantComp:  nil,
			wantIncomp: "cmd1",
		},
		{
			name:      "empty after trim - no send",
			data:      "   \x04",
			wantComp:  nil,
			wantIncomp: "",
		},
		{
			name:      "multiple commands - three",
			data:      "a\x04b\x04c\x04",
			wantComp:  []string{"a", "b", "c"},
			wantIncomp: "",
		},
		{
			name:      "first complete second has escaped and is incomplete",
			data:      "a\x04b\x04\x04c",
			wantComp:  []string{"a"},
			wantIncomp: "b\x04\x04c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotComp, gotIncomp := splitOnCommandTerminator(tt.data)
			if !reflect.DeepEqual(gotComp, tt.wantComp) {
				t.Errorf("splitOnCommandTerminator() complete = %q, want %q", gotComp, tt.wantComp)
			}
			if gotIncomp != tt.wantIncomp {
				t.Errorf("splitOnCommandTerminator() incomplete = %q, want %q", gotIncomp, tt.wantIncomp)
			}
		})
	}
}
