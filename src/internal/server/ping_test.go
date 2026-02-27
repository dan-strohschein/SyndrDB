package server

import (
	"testing"
)

func TestPingCommandDirector(t *testing.T) {
	// classifyCommandPermission should return "" (no permission) for PING
	perm := classifyCommandPermission([]string{"PING"})
	if perm != "" {
		t.Fatalf("expected empty permission for PING, got %q", perm)
	}
}

func TestPingCommandDirectorLowercase(t *testing.T) {
	// classifyCommandPermission normalizes to lowercase, so "ping" should also work
	perm := classifyCommandPermission([]string{"ping"})
	if perm != "" {
		t.Fatalf("expected empty permission for ping, got %q", perm)
	}
}

func TestPingCommandDirectorMixedCase(t *testing.T) {
	perm := classifyCommandPermission([]string{"Ping"})
	if perm != "" {
		t.Fatalf("expected empty permission for Ping, got %q", perm)
	}
}
