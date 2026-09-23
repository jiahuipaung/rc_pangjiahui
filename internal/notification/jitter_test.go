package notification

import (
	"bytes"
	"testing"
	"time"
)

func TestBoundedJitterStaysWithinTwentyPercent(t *testing.T) {
	jitter, err := newBoundedJitter(bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	if err != nil {
		t.Fatal(err)
	}
	const base = 10 * time.Second
	seen := map[time.Duration]bool{}
	for range 100 {
		got := jitter.Apply(base)
		if got < 8*time.Second || got > 12*time.Second {
			t.Fatalf("jittered delay = %s, want [8s, 12s]", got)
		}
		seen[got] = true
	}
	if len(seen) < 2 {
		t.Fatal("jitter produced a constant sequence")
	}
}

func TestBoundedJitterKeepsPositiveDelayPositive(t *testing.T) {
	jitter, err := newBoundedJitter(bytes.NewReader([]byte{8, 7, 6, 5, 4, 3, 2, 1}))
	if err != nil {
		t.Fatal(err)
	}
	if got := jitter.Apply(time.Nanosecond); got < time.Nanosecond {
		t.Fatalf("jittered delay = %s, want positive", got)
	}
}

func TestBoundedJitterRejectsMissingSecureSeed(t *testing.T) {
	if _, err := newBoundedJitter(bytes.NewReader(nil)); err == nil {
		t.Fatal("expected secure seed error")
	}
}
