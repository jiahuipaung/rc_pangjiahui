package notification

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeQueryRepository struct {
	task      Task
	getErr    error
	replayed  bool
	replayErr error
}

func (repo *fakeQueryRepository) GetForCaller(context.Context, string, string) (Task, error) {
	return repo.task, repo.getErr
}
func (repo *fakeQueryRepository) ReplayDeadAtomic(context.Context, string, time.Time, string) (bool, error) {
	return repo.replayed, repo.replayErr
}

func TestQueryReturnsRedactedView(t *testing.T) {
	repo := &fakeQueryRepository{task: Task{ID: "n-1", CallerID: "orders", IdempotencyKey: "secret-key", Snapshot: DeliverySnapshot{DestinationID: "crm", Body: []byte(`{"secret":true}`), SecretHeaders: map[string]string{"Authorization": "TOKEN"}}, Status: StatusRetryWait, AttemptCount: 2}}
	view, err := NewQueryService(repo).Get(t.Context(), "orders", "n-1")
	if err != nil {
		t.Fatal(err)
	}
	if view.ID != "n-1" || view.DestinationID != "crm" || view.AttemptCount != 2 {
		t.Fatalf("view = %+v", view)
	}
}

func TestReplayRequiresDeadTask(t *testing.T) {
	repo := &fakeQueryRepository{replayed: false}
	err := NewReplayService(repo, time.Now, func() string { return "event-1" }).Replay(t.Context(), "admin", "n-1")
	if !errors.Is(err, ErrNotDead) {
		t.Fatalf("Replay() error = %v", err)
	}
}

func TestReplayPropagatesRepositoryFailure(t *testing.T) {
	repo := &fakeQueryRepository{replayErr: errors.New("database unavailable")}
	err := NewReplayService(repo, time.Now, func() string { return "event-1" }).Replay(t.Context(), "admin", "n-1")
	if err == nil || errors.Is(err, ErrNotDead) {
		t.Fatalf("Replay() error = %v", err)
	}
}
