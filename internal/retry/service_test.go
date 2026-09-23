package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeRepository struct {
	result Result
	err    error
	calls  int
	now    time.Time
	limit  int
}

func (repo *fakeRepository) ScheduleDue(_ context.Context, now time.Time, limit int) (Result, error) {
	repo.calls++
	repo.now, repo.limit = now, limit
	return repo.result, repo.err
}

func TestRunOnceSchedulesDueAndExpiredTasks(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	repo := &fakeRepository{result: Result{Scheduled: 2, Dead: 1, RecoveredLeases: 1}}
	result, err := NewService(repo, func() time.Time { return now }, Config{BatchSize: 20}).RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result != repo.result || repo.calls != 1 || !repo.now.Equal(now) || repo.limit != 20 {
		t.Fatalf("result=%+v repo=%+v", result, repo)
	}
}

func TestRunOnceReturnsRepositoryFailure(t *testing.T) {
	repo := &fakeRepository{err: errors.New("database unavailable")}
	if _, err := NewService(repo, time.Now, Config{BatchSize: 10}).RunOnce(t.Context()); err == nil {
		t.Fatal("expected repository error")
	}
}

func TestRunStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := NewService(&fakeRepository{}, time.Now, Config{BatchSize: 10, PollInterval: time.Hour}).Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
}
