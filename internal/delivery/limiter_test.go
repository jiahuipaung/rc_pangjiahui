package delivery

import (
	"context"
	"testing"
	"time"
)

func TestLimiterBlocksAtDestinationCapacity(t *testing.T) {
	limiter := NewLimiter()
	releaseFirst, err := limiter.Acquire(t.Context(), "crm", 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := limiter.Acquire(ctx, "crm", 1); err == nil {
		t.Fatal("second call acquired above destination capacity")
	}
	releaseFirst()
	release, err := limiter.Acquire(t.Context(), "crm", 1)
	if err != nil {
		t.Fatalf("second call did not acquire released capacity: %v", err)
	}
	release()
}

func TestLimiterDoesNotBlockDifferentDestinations(t *testing.T) {
	limiter := NewLimiter()
	releaseCRM, err := limiter.Acquire(t.Context(), "crm", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseCRM()
	releaseInventory, err := limiter.Acquire(t.Context(), "inventory", 1)
	if err != nil {
		t.Fatalf("different destination was blocked: %v", err)
	}
	releaseInventory()
}

func TestLimiterCancellationDoesNotLeakCapacity(t *testing.T) {
	limiter := NewLimiter()
	release, err := limiter.Acquire(t.Context(), "crm", 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := limiter.Acquire(ctx, "crm", 1); err == nil {
		t.Fatal("canceled waiter acquired capacity")
	}
	release()
	acquired, err := limiter.Acquire(t.Context(), "crm", 1)
	if err != nil {
		t.Fatalf("capacity leaked after cancellation: %v", err)
	}
	acquired()
}

func TestLimiterUsesSmallestObservedSnapshotLimit(t *testing.T) {
	limiter := NewLimiter()
	release, err := limiter.Acquire(t.Context(), "crm", 1)
	if err != nil {
		t.Fatal(err)
	}
	release()
	first, err := limiter.Acquire(t.Context(), "crm", 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := limiter.Acquire(ctx, "crm", 2); err == nil {
		t.Fatal("larger new snapshot limit replaced conservative limit")
	}
	first()
}
