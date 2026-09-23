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

type fakeRetryObserver struct{ results []Result }

func (observer *fakeRetryObserver) ObserveSchedule(result Result) {
	observer.results = append(observer.results, result)
}

func (repo *fakeRepository) ScheduleDue(_ context.Context, now time.Time, limit int) (Result, error) {
	repo.calls++
	repo.now, repo.limit = now, limit
	return repo.result, repo.err
}

func TestRunOnceObservesScheduledAndRecoveredCounts(t *testing.T) {
	result := Result{Scheduled: 2, Dead: 1, RecoveredLeases: 1}
	observer := &fakeRetryObserver{}
	if _, err := NewService(&fakeRepository{result: result}, time.Now, Config{BatchSize: 10}, observer).RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(observer.results) != 1 || observer.results[0] != result {
		t.Fatalf("observed = %+v", observer.results)
	}
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
