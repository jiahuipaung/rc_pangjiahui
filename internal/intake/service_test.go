package intake

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/destination"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

type fakeRepository struct {
	existing *notification.Task
	created  notification.Task
	event    notification.OutboxEvent
	err      error
}

func (repo *fakeRepository) CreateOrGet(_ context.Context, task notification.Task, event notification.OutboxEvent) (*notification.Task, error) {
	repo.created, repo.event = task, event
	return repo.existing, repo.err
}

type fakeRegistry struct{ destination destination.Destination }

type fakeIntakeObserver struct{ outcomes []string }

func (observer *fakeIntakeObserver) ObserveIntake(outcome string) {
	observer.outcomes = append(observer.outcomes, outcome)
}

func (registry fakeRegistry) Get(id string) (destination.Destination, bool) {
	return registry.destination, id == registry.destination.ID
}

func newService(repo *fakeRepository, observers ...Observer) *Service {
	parsed, _ := url.Parse("https://198.51.100.10/hook")
	registry := fakeRegistry{destination: destination.Destination{
		ID: "crm", Method: http.MethodPost, URL: parsed, StaticHeaders: http.Header{"Content-Type": {"application/json"}},
		SecretHeaders: map[string]string{"Authorization": "CRM_TOKEN"}, Timeout: 5 * time.Second,
		Retry: notification.RetryPolicy{MaxAttempts: 3, Lifetime: time.Hour, Delays: []time.Duration{time.Second}}, ConcurrencyLimit: 2,
	}}
	return New(repo, registry, func() time.Time { return time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC) }, sequenceID(), observers...)
}

func TestCreateObservesAcceptedAndRejectedOutcomes(t *testing.T) {
	observer := &fakeIntakeObserver{}
	if _, _, err := newService(&fakeRepository{}, observer).Create(t.Context(), "orders", "key-1", "crm", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := newService(&fakeRepository{}, observer).Create(t.Context(), "orders", "key-2", "missing", json.RawMessage(`{}`)); !errors.Is(err, ErrUnknownDestination) {
		t.Fatalf("error = %v", err)
	}
	if len(observer.outcomes) != 2 || observer.outcomes[0] != "accepted" || observer.outcomes[1] != "unknown_destination" {
		t.Fatalf("outcomes = %v", observer.outcomes)
	}
}

func sequenceID() func() string {
	ids := []string{"notification-1", "event-1"}
	index := 0
	return func() string { value := ids[index]; index++; return value }
}

func TestCreateSameKeyDifferentBodyReturnsConflict(t *testing.T) {
	existingHash, _ := notification.RequestHash("crm", json.RawMessage(`{"order_id":"1"}`))
	repo := &fakeRepository{existing: &notification.Task{ID: "existing", CallerID: "orders", IdempotencyKey: "key-1", RequestHash: existingHash}}
	_, _, err := newService(repo).Create(t.Context(), "orders", "key-1", "crm", json.RawMessage(`{"order_id":"2"}`))
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("Create() error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestCreateSameCanonicalRequestReturnsExistingTask(t *testing.T) {
	hash, _ := notification.RequestHash("crm", json.RawMessage(`{"b":2,"a":1}`))
	existing := notification.Task{ID: "existing", CallerID: "orders", IdempotencyKey: "key-1", RequestHash: hash}
	repo := &fakeRepository{existing: &existing}
	got, replay, err := newService(repo).Create(t.Context(), "orders", "key-1", "crm", json.RawMessage(`{"a":1,"b":2}`))
	if err != nil || !replay || got.ID != existing.ID {
		t.Fatalf("Create() = %+v, %v, %v", got, replay, err)
	}
}

func TestCreateBuildsTaskAndInitialOutboxAtomically(t *testing.T) {
	repo := &fakeRepository{}
	task, replay, err := newService(repo).Create(t.Context(), "orders", "key-1", "crm", json.RawMessage(`{"order_id":"1"}`))
	if err != nil || replay {
		t.Fatalf("Create() = %+v, %v, %v", task, replay, err)
	}
	if repo.created.ID != "notification-1" || repo.event.AggregateID != repo.created.ID || repo.event.Generation != 0 {
		t.Fatalf("created task/event mismatch: %+v %+v", repo.created, repo.event)
	}
	if string(repo.created.Snapshot.Body) != `{"order_id":"1"}` {
		t.Fatalf("body = %s", repo.created.Snapshot.Body)
	}
	if repo.created.Snapshot.ConcurrencyLimit != 2 {
		t.Fatalf("concurrency limit = %d, want 2", repo.created.Snapshot.ConcurrencyLimit)
	}
}

func TestCreateRejectsUnknownDestination(t *testing.T) {
	_, _, err := newService(&fakeRepository{}).Create(t.Context(), "orders", "key-1", "missing", json.RawMessage(`{}`))
	if !errors.Is(err, ErrUnknownDestination) {
		t.Fatalf("error = %v", err)
	}
}
