//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	_, file, _, _ := runtime.Caller(0)
	migration, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "migrations", "000001_initial.up.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err = pool.Exec(t.Context(), "DROP TABLE IF EXISTS outbox_events; DROP TABLE IF EXISTS notification_tasks;"); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	if _, err = pool.Exec(t.Context(), string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	return New(pool)
}

func fixtureTask(id, key string) notification.Task {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	return notification.Task{
		ID: id, CallerID: "orders", IdempotencyKey: key, RequestHash: [32]byte{1},
		Snapshot: notification.DeliverySnapshot{
			DestinationID: "crm", Method: "POST", URL: "https://198.51.100.10/hook",
			StaticHeaders: map[string][]string{"Content-Type": {"application/json"}},
			SecretHeaders: map[string]string{"Authorization": "CRM_TOKEN"}, Body: json.RawMessage(`{"order_id":"1"}`),
			Timeout: 5 * time.Second, Retry: notification.RetryPolicy{MaxAttempts: 3, Lifetime: time.Hour, Delays: []time.Duration{time.Second}},
			ConcurrencyLimit: 2,
		},
		Status: notification.StatusPending, CreatedAt: now, UpdatedAt: now,
	}
}

func fixtureEvent(id, aggregate string, generation int) notification.OutboxEvent {
	return notification.OutboxEvent{ID: id, AggregateID: aggregate, Generation: generation, EventType: "notification.ready", Payload: json.RawMessage(`{"notification_id":"` + aggregate + `"}`), CreatedAt: time.Now().UTC()}
}

func TestCreateTaskAndOutboxRollBackTogether(t *testing.T) {
	store := newStore(t)
	errSentinel := errors.New("rollback")
	err := store.WithTx(t.Context(), func(tx Tx) error {
		if err := tx.CreateNotification(t.Context(), fixtureTask("n-1", "key-1")); err != nil {
			return err
		}
		if err := tx.InsertOutbox(t.Context(), fixtureEvent("e-1", "n-1", 0)); err != nil {
			return err
		}
		return errSentinel
	})
	if !errors.Is(err, errSentinel) {
		t.Fatalf("WithTx() error = %v, want rollback sentinel", err)
	}
	for _, table := range []string{"notification_tasks", "outbox_events"} {
		var count int
		if err := store.pool.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s count = %d, want 0", table, count)
		}
	}
}

func TestUniqueCallerIdempotencyKey(t *testing.T) {
	store := newStore(t)
	if err := store.WithTx(t.Context(), func(tx Tx) error { return tx.CreateNotification(t.Context(), fixtureTask("n-1", "same")) }); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err := store.WithTx(t.Context(), func(tx Tx) error { return tx.CreateNotification(t.Context(), fixtureTask("n-2", "same")) })
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("second insert error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestUniqueOutboxGeneration(t *testing.T) {
	store := newStore(t)
	err := store.WithTx(t.Context(), func(tx Tx) error {
		if err := tx.CreateNotification(t.Context(), fixtureTask("n-1", "key-1")); err != nil {
			return err
		}
		if err := tx.InsertOutbox(t.Context(), fixtureEvent("e-1", "n-1", 0)); err != nil {
			return err
		}
		return tx.InsertOutbox(t.Context(), fixtureEvent("e-2", "n-1", 0))
	})
	if !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("duplicate generation error = %v, want ErrGenerationConflict", err)
	}
}

func TestConcurrentOutboxClaimsAreDisjoint(t *testing.T) {
	store := newStore(t)
	if err := store.WithTx(t.Context(), func(tx Tx) error {
		for i, id := range []string{"n-1", "n-2"} {
			if err := tx.CreateNotification(t.Context(), fixtureTask(id, id)); err != nil {
				return err
			}
			if err := tx.InsertOutbox(t.Context(), fixtureEvent("e-"+id, id, i)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	start := make(chan struct{})
	results := make(chan []notification.OutboxEvent, 2)
	var wg sync.WaitGroup
	for _, token := range []string{"claim-a", "claim-b"} {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			<-start
			events, err := store.ClaimOutbox(context.Background(), time.Now(), 1, token, time.Now().Add(time.Minute))
			if err != nil {
				t.Errorf("ClaimOutbox(%s): %v", token, err)
				return
			}
			results <- events
		}(token)
	}
	close(start)
	wg.Wait()
	close(results)
	seen := map[string]bool{}
	for events := range results {
		if len(events) != 1 {
			t.Fatalf("claim length = %d, want 1", len(events))
		}
		if seen[events[0].ID] {
			t.Fatalf("event %s claimed twice", events[0].ID)
		}
		seen[events[0].ID] = true
	}
}

func TestDeliveryResultRejectsStaleLease(t *testing.T) {
	store := newStore(t)
	if err := store.WithTx(t.Context(), func(tx Tx) error { return tx.CreateNotification(t.Context(), fixtureTask("n-1", "key-1")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	claim, err := store.ClaimDelivery(t.Context(), "n-1", "current-token", time.Now().Add(time.Minute))
	if err != nil || !claim.Acquired {
		t.Fatalf("claim = %+v, %v", claim, err)
	}
	if claim.Task.Snapshot.ConcurrencyLimit != 2 {
		t.Fatalf("concurrency limit = %d, want 2", claim.Task.Snapshot.ConcurrencyLimit)
	}
	applied, err := store.ApplyDeliveryResult(t.Context(), "n-1", "old-token", notification.AttemptResult{Outcome: notification.Delivered, CompletedAt: time.Now()})
	if err != nil {
		t.Fatalf("ApplyDeliveryResult: %v", err)
	}
	if applied {
		t.Fatal("stale lease unexpectedly changed task")
	}
}

func TestScheduleDueRecoversExpiredLeaseOnce(t *testing.T) {
	store := newStore(t)
	if err := store.WithTx(t.Context(), func(tx Tx) error { return tx.CreateNotification(t.Context(), fixtureTask("n-1", "key-1")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.ClaimDelivery(t.Context(), "n-1", "lease-1", time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("claim: %v", err)
	}
	result, err := store.ScheduleDue(t.Context(), time.Now(), 10)
	if err != nil {
		t.Fatalf("ScheduleDue: %v", err)
	}
	if result.Scheduled != 1 || result.RecoveredLeases != 1 {
		t.Fatalf("result = %+v", result)
	}
	second, err := store.ScheduleDue(t.Context(), time.Now(), 10)
	if err != nil {
		t.Fatalf("second ScheduleDue: %v", err)
	}
	if second.Scheduled != 0 {
		t.Fatalf("second result = %+v", second)
	}
	var events int
	if err := store.pool.QueryRow(t.Context(), "SELECT count(*) FROM outbox_events WHERE aggregate_id='n-1'").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("outbox events = %d, want 1", events)
	}
}

func TestScheduleDueMovesExhaustedTaskToDead(t *testing.T) {
	store := newStore(t)
	task := fixtureTask("n-1", "key-1")
	task.Snapshot.Retry.MaxAttempts = 1
	if err := store.WithTx(t.Context(), func(tx Tx) error { return tx.CreateNotification(t.Context(), task) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.ClaimDelivery(t.Context(), "n-1", "lease-1", time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("claim: %v", err)
	}
	result, err := store.ScheduleDue(t.Context(), time.Now(), 10)
	if err != nil {
		t.Fatalf("ScheduleDue: %v", err)
	}
	if result.Dead != 1 || result.Scheduled != 0 {
		t.Fatalf("result = %+v", result)
	}
}

func TestReplayDeadIsAtomicAndOwnerQueryIsScoped(t *testing.T) {
	store := newStore(t)
	if err := store.WithTx(t.Context(), func(tx Tx) error { return tx.CreateNotification(t.Context(), fixtureTask("n-1", "key-1")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.pool.Exec(t.Context(), "UPDATE notification_tasks SET status='dead',dead_at=now() WHERE id='n-1'"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetForCaller(t.Context(), "other-caller", "n-1"); !errors.Is(err, notification.ErrTaskNotFound) {
		t.Fatalf("cross-caller query error = %v", err)
	}
	applied, err := store.ReplayDeadAtomic(t.Context(), "n-1", time.Now(), "replay-event-1")
	if err != nil || !applied {
		t.Fatalf("ReplayDeadAtomic = %v, %v", applied, err)
	}
	var status string
	var events int
	if err := store.pool.QueryRow(t.Context(), "SELECT status FROM notification_tasks WHERE id='n-1'").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(t.Context(), "SELECT count(*) FROM outbox_events WHERE aggregate_id='n-1'").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || events != 1 {
		t.Fatalf("status=%s events=%d", status, events)
	}
	second, err := store.ReplayDeadAtomic(t.Context(), "n-1", time.Now(), "replay-event-2")
	if err != nil || second {
		t.Fatalf("second replay = %v, %v", second, err)
	}
}
