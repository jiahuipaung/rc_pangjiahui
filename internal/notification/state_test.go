package notification

import (
	"context"
	"errors"
	"testing"
)

func TestClassifyResult(t *testing.T) {
	tests := []struct {
		name   string
		status int
		err    error
		want   Outcome
	}{
		{name: "success", status: 204, want: Delivered},
		{name: "request timeout", status: 408, want: Retryable},
		{name: "too early", status: 425, want: Retryable},
		{name: "rate limited", status: 429, want: Retryable},
		{name: "supplier failure", status: 503, want: Retryable},
		{name: "permanent client error", status: 422, want: Permanent},
		{name: "transport timeout", err: context.DeadlineExceeded, want: Retryable},
		{name: "transport error", err: errors.New("connection reset"), want: Retryable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyResult(tt.status, tt.err); got != tt.want {
				t.Fatalf("ClassifyResult(%d, %v) = %q, want %q", tt.status, tt.err, got, tt.want)
			}
		})
	}
}

func TestCanTransitionRejectsInvalidStateChange(t *testing.T) {
	if CanTransition(StatusDelivered, StatusDelivering) {
		t.Fatal("delivered task must not transition back to delivering")
	}
	if !CanTransition(StatusPending, StatusDelivering) {
		t.Fatal("pending task must be claimable for delivery")
	}
	if !CanTransition(StatusDead, StatusPending) {
		t.Fatal("dead task must support authenticated replay")
	}
}
