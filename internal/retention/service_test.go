package retention

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeRepository struct {
	deleted int64
	err     error
	cutoff  time.Time
	limit   int
}

func (repo *fakeRepository) DeleteTerminalBefore(_ context.Context, cutoff time.Time, limit int) (int64, error) {
	repo.cutoff, repo.limit = cutoff, limit
	return repo.deleted, repo.err
}

func TestRunOnceDeletesOnlyThroughRepositoryBoundary(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	repo := &fakeRepository{deleted: 12}
	count, err := NewService(repo, func() time.Time { return now }, Config{Retention: 30 * 24 * time.Hour, BatchSize: 100}).RunOnce(t.Context())
	if err != nil || count != 12 || !repo.cutoff.Equal(now.Add(-30*24*time.Hour)) || repo.limit != 100 {
		t.Fatalf("count=%d err=%v repo=%+v", count, err, repo)
	}
}

func TestRunOnceReturnsDeleteFailure(t *testing.T) {
	_, err := NewService(&fakeRepository{err: errors.New("database unavailable")}, time.Now, Config{Retention: time.Hour, BatchSize: 10}).RunOnce(t.Context())
	if err == nil {
		t.Fatal("expected error")
	}
}
