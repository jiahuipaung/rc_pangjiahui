package destination

import (
	"net"
	"testing"
)

func TestValidateResolvedIPsRejectsAnyUnsafeAddress(t *testing.T) {
	err := ValidateResolvedIPs([]net.IP{net.ParseIP("198.51.100.10"), net.ParseIP("169.254.169.254")})
	if err == nil {
		t.Fatal("expected mixed public and link-local results to be rejected")
	}
}

func TestValidateResolvedIPsAcceptsPublicAddresses(t *testing.T) {
	err := ValidateResolvedIPs([]net.IP{net.ParseIP("198.51.100.10"), net.ParseIP("2001:db8::1")})
	if err != nil {
		t.Fatalf("ValidateResolvedIPs() error = %v", err)
	}
}

func TestValidateResolvedIPsRejectsEmptyResult(t *testing.T) {
	if err := ValidateResolvedIPs(nil); err == nil {
		t.Fatal("expected empty DNS result to be rejected")
	}
}
