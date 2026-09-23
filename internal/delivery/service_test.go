package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

type fakeRepository struct {
	claim       notification.Claim
	claimErr    error
	applyResult bool
	applyErr    error
	applied     notification.AttemptResult
}

func (repo *fakeRepository) ClaimDelivery(context.Context, string, string, time.Time) (notification.Claim, error) {
	return repo.claim, repo.claimErr
}
func (repo *fakeRepository) ApplyDeliveryResult(_ context.Context, _, _ string, result notification.AttemptResult) (bool, error) {
	repo.applied = result
	return repo.applyResult, repo.applyErr
}

type fakeSender struct {
	result Result
	calls  int
}

type fixedDelayJitter time.Duration

func (jitter fixedDelayJitter) Apply(time.Duration) time.Duration { return time.Duration(jitter) }

func (sender *fakeSender) Send(context.Context, notification.DeliverySnapshot) Result {
	sender.calls++
	return sender.result
}

func worker(repo *fakeRepository, sender *fakeSender) *Service {
	return NewService(repo, sender, NewLimiter(), fixedDelayJitter(time.Second), func() time.Time { return time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC) }, func() string { return "lease-1" }, time.Minute)
}

func TestHandleDoesNotSendWhenClaimNotAcquired(t *testing.T) {
	repo := &fakeRepository{claim: notification.Claim{Acquired: false}}
	sender := &fakeSender{}
	disposition := worker(repo, sender).Handle(t.Context(), Message{EventID: "e-1", NotificationID: "n-1"})
	if disposition != Ack || sender.calls != 0 {
		t.Fatalf("disposition=%v calls=%d", disposition, sender.calls)
	}
}

func TestHandleRequeuesDatabaseFailures(t *testing.T) {
	repo := &fakeRepository{claimErr: errors.New("database unavailable")}
	if got := worker(repo, &fakeSender{}).Handle(t.Context(), Message{NotificationID: "n-1"}); got != Requeue {
		t.Fatalf("disposition = %v", got)
	}
}

func TestHandleAppliesDeliveryResultAndAcknowledges(t *testing.T) {
	task := notification.Task{ID: "n-1", Snapshot: notification.DeliverySnapshot{DestinationID: "crm", ConcurrencyLimit: 1}, Status: notification.StatusDelivering}
	repo := &fakeRepository{claim: notification.Claim{Task: task, Acquired: true}, applyResult: true}
	sender := &fakeSender{result: Result{Outcome: notification.Delivered, HTTPStatus: 204}}
	if got := worker(repo, sender).Handle(t.Context(), Message{NotificationID: "n-1"}); got != Ack {
		t.Fatalf("disposition = %v", got)
	}
	if repo.applied.Outcome != notification.Delivered {
		t.Fatalf("applied = %+v", repo.applied)
	}
}

func TestHandleRequeuesWhenResultCannotCommit(t *testing.T) {
	task := notification.Task{ID: "n-1", Snapshot: notification.DeliverySnapshot{DestinationID: "crm", ConcurrencyLimit: 1}, Status: notification.StatusDelivering}
	repo := &fakeRepository{claim: notification.Claim{Task: task, Acquired: true}, applyErr: errors.New("commit failed")}
	if got := worker(repo, &fakeSender{}).Handle(t.Context(), Message{NotificationID: "n-1"}); got != Requeue {
		t.Fatalf("disposition = %v", got)
	}
}

func TestHandleDoesNotCallSupplierWithoutDestinationCapacity(t *testing.T) {
	limiter := NewLimiter()
	release, err := limiter.Acquire(t.Context(), "crm", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	task := notification.Task{ID: "n-1", Snapshot: notification.DeliverySnapshot{DestinationID: "crm", ConcurrencyLimit: 1}, Status: notification.StatusDelivering}
	repo := &fakeRepository{claim: notification.Claim{Task: task, Acquired: true}, applyResult: true}
	sender := &fakeSender{result: Result{Outcome: notification.Delivered, HTTPStatus: 204}}
	service := NewService(repo, sender, limiter, fixedDelayJitter(time.Second), time.Now, func() string { return "lease-1" }, time.Minute)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if got := service.Handle(ctx, Message{NotificationID: "n-1"}); got != Requeue {
		t.Fatalf("disposition = %v, want Requeue", got)
	}
	if sender.calls != 0 {
		t.Fatalf("supplier calls = %d, want 0", sender.calls)
	}
}

func TestHandleUsesInjectedJitterForRetrySchedule(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	task := notification.Task{ID: "n-1", AttemptCount: 1, Snapshot: notification.DeliverySnapshot{
		DestinationID: "crm", ConcurrencyLimit: 1,
		Retry: notification.RetryPolicy{MaxAttempts: 3, Lifetime: time.Hour, Delays: []time.Duration{5 * time.Second}},
	}, Status: notification.StatusDelivering}
	repo := &fakeRepository{claim: notification.Claim{Task: task, Acquired: true}, applyResult: true}
	sender := &fakeSender{result: Result{Outcome: notification.Retryable, HTTPStatus: 503}}
	service := NewService(repo, sender, NewLimiter(), fixedDelayJitter(7*time.Second), func() time.Time { return now }, func() string { return "lease-1" }, time.Minute)
	if got := service.Handle(t.Context(), Message{NotificationID: "n-1"}); got != Ack {
		t.Fatalf("disposition = %v", got)
	}
	if repo.applied.NextAttemptAt == nil || !repo.applied.NextAttemptAt.Equal(now.Add(7*time.Second)) {
		t.Fatalf("next attempt = %v, want %s", repo.applied.NextAttemptAt, now.Add(7*time.Second))
	}
}

func TestHandleDoesNotJitterSupplierRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	retryAfter := now.Add(20 * time.Second)
	task := notification.Task{ID: "n-1", AttemptCount: 1, Snapshot: notification.DeliverySnapshot{
		DestinationID: "crm", ConcurrencyLimit: 1,
		Retry: notification.RetryPolicy{MaxAttempts: 3, Lifetime: time.Hour, Delays: []time.Duration{5 * time.Second}},
	}, Status: notification.StatusDelivering}
	repo := &fakeRepository{claim: notification.Claim{Task: task, Acquired: true}, applyResult: true}
	sender := &fakeSender{result: Result{Outcome: notification.Retryable, HTTPStatus: 503, RetryAfter: &retryAfter}}
	service := NewService(repo, sender, NewLimiter(), fixedDelayJitter(7*time.Second), func() time.Time { return now }, func() string { return "lease-1" }, time.Minute)
	_ = service.Handle(t.Context(), Message{NotificationID: "n-1"})
	if repo.applied.NextAttemptAt == nil || !repo.applied.NextAttemptAt.Equal(retryAfter) {
		t.Fatalf("next attempt = %v, want supplier Retry-After %s", repo.applied.NextAttemptAt, retryAfter)
	}
}
