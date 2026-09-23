package config

import "testing"

func TestParseRoleAcceptsSupportedRoles(t *testing.T) {
	for _, input := range []string{"api", "publisher", "worker", "scheduler", "all"} {
		if got, err := ParseRole(input); err != nil || string(got) != input {
			t.Fatalf("ParseRole(%q) = %q, %v", input, got, err)
		}
	}
}

func TestParseRoleRejectsUnknownRole(t *testing.T) {
	if _, err := ParseRole("unknown"); err == nil {
		t.Fatal("expected unsupported role to be rejected")
	}
}
