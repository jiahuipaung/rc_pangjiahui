package notification

import (
	"testing"
	"time"
)

type fixedJitter time.Duration

func (j fixedJitter) Apply(time.Duration) time.Duration { return time.Duration(j) }

func TestNextAttemptUsesConfiguredDelayAndJitter(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	policy := RetryPolicy{MaxAttempts: 3, Lifetime: time.Hour, Delays: []time.Duration{5 * time.Second, 30 * time.Second}}

	got, ok := NextAttempt(policy, 1, nil, now, fixedJitter(7*time.Second))
	if !ok {
		t.Fatal("expected another attempt")
	}
	want := now.Add(7 * time.Second)
	if !got.Equal(want) {
		t.Fatalf("next attempt = %s, want %s", got, want)
	}
}

func TestNextAttemptHonorsRetryAfterWithinLifetime(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	retryAfter := now.Add(10 * time.Minute)
	policy := RetryPolicy{MaxAttempts: 3, Lifetime: time.Hour, Delays: []time.Duration{time.Second}}

	got, ok := NextAttempt(policy, 1, &retryAfter, now, fixedJitter(time.Second))
	if !ok || !got.Equal(retryAfter) {
		t.Fatalf("next attempt = %s, %v; want %s, true", got, ok, retryAfter)
	}
}

func TestNextAttemptStopsAtAttemptOrLifetimeLimit(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	policy := RetryPolicy{MaxAttempts: 2, Lifetime: time.Minute, Delays: []time.Duration{5 * time.Second}}

	if _, ok := NextAttempt(policy, 2, nil, now, fixedJitter(5*time.Second)); ok {
		t.Fatal("expected max-attempt limit to stop retry")
	}
	retryAfter := now.Add(2 * time.Minute)
	if _, ok := NextAttempt(policy, 1, &retryAfter, now, fixedJitter(5*time.Second)); ok {
		t.Fatal("expected lifetime limit to stop retry")
	}
}
