package app

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeRole struct {
	mu      sync.Mutex
	events  *[]string
	stopped chan struct{}
}

func (role *fakeRole) Run(ctx context.Context) error {
	<-ctx.Done()
	role.mu.Lock()
	*role.events = append(*role.events, "run-stopped")
	role.mu.Unlock()
	close(role.stopped)
	return ctx.Err()
}
func (role *fakeRole) StopClaims() {
	role.mu.Lock()
	*role.events = append(*role.events, "claims-stopped")
	role.mu.Unlock()
}

func TestShutdownStopsClaimsBeforeWaitingForInflight(t *testing.T) {
	events := []string{}
	role := &fakeRole{events: &events, stopped: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, []Role{role}, 2*time.Second) }()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown timed out")
	}
	if len(events) != 2 || events[0] != "claims-stopped" || events[1] != "run-stopped" {
		t.Fatalf("events=%v", events)
	}
}
