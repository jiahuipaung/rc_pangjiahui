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

func (sender *fakeSender) Send(context.Context, notification.DeliverySnapshot) Result {
	sender.calls++
	return sender.result
}

func worker(repo *fakeRepository, sender *fakeSender) *Service {
	return NewService(repo, sender, func() time.Time { return time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC) }, func() string { return "lease-1" }, time.Minute)
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
	task := notification.Task{ID: "n-1", Snapshot: notification.DeliverySnapshot{}, Status: notification.StatusDelivering}
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
	task := notification.Task{ID: "n-1", Snapshot: notification.DeliverySnapshot{}, Status: notification.StatusDelivering}
	repo := &fakeRepository{claim: notification.Claim{Task: task, Acquired: true}, applyErr: errors.New("commit failed")}
	if got := worker(repo, &fakeSender{}).Handle(t.Context(), Message{NotificationID: "n-1"}); got != Requeue {
		t.Fatalf("disposition = %v", got)
	}
}
