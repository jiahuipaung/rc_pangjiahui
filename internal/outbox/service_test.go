package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

type fakeRepository struct {
	events      []notification.OutboxEvent
	claimErr    error
	markApplied bool
	marked      []string
}

func (repo *fakeRepository) ClaimOutbox(context.Context, time.Time, int, string, time.Time) ([]notification.OutboxEvent, error) {
	return repo.events, repo.claimErr
}
func (repo *fakeRepository) MarkPublished(_ context.Context, id, _ string, _ time.Time) (bool, error) {
	repo.marked = append(repo.marked, id)
	return repo.markApplied, nil
}

type fakePublisher struct {
	err      error
	messages []Message
}

type fakeOutboxObserver struct{ outcomes []string }

func (observer *fakeOutboxObserver) ObserveOutboxPublish(outcome string) {
	observer.outcomes = append(observer.outcomes, outcome)
}

func (publisher *fakePublisher) Publish(_ context.Context, message Message) error {
	publisher.messages = append(publisher.messages, message)
	return publisher.err
}

func oneEvent() notification.OutboxEvent {
	return notification.OutboxEvent{ID: "event-1", AggregateID: "notification-1", EventType: "notification.ready", Payload: json.RawMessage(`{"notification_id":"notification-1"}`)}
}

func newService(repo *fakeRepository, publisher *fakePublisher, observers ...Observer) *Service {
	now := func() time.Time { return time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC) }
	return NewService(repo, publisher, now, func() string { return "claim-1" }, Config{BatchSize: 10, ClaimTTL: time.Minute}, observers...)
}

func TestRunOnceObservesConfirmAndFailure(t *testing.T) {
	observer := &fakeOutboxObserver{}
	successRepo := &fakeRepository{events: []notification.OutboxEvent{oneEvent()}, markApplied: true}
	if _, err := newService(successRepo, &fakePublisher{}, observer).RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	failureRepo := &fakeRepository{events: []notification.OutboxEvent{oneEvent()}, markApplied: true}
	if _, err := newService(failureRepo, &fakePublisher{err: ErrConfirmLost}, observer).RunOnce(t.Context()); !errors.Is(err, ErrConfirmLost) {
		t.Fatalf("error = %v", err)
	}
	if len(observer.outcomes) != 2 || observer.outcomes[0] != "confirmed" || observer.outcomes[1] != "failed" {
		t.Fatalf("outcomes = %v", observer.outcomes)
	}
}

func TestRunOnceDoesNotMarkPublishedWhenConfirmFails(t *testing.T) {
	repo := &fakeRepository{events: []notification.OutboxEvent{oneEvent()}, markApplied: true}
	publisher := &fakePublisher{err: ErrConfirmLost}
	_, err := newService(repo, publisher).RunOnce(t.Context())
	if !errors.Is(err, ErrConfirmLost) {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if len(repo.marked) != 0 {
		t.Fatalf("marked published = %v", repo.marked)
	}
}

func TestRunOncePublishesCompactIdentityAndMarksConfirmedEvent(t *testing.T) {
	repo := &fakeRepository{events: []notification.OutboxEvent{oneEvent()}, markApplied: true}
	publisher := &fakePublisher{}
	count, err := newService(repo, publisher).RunOnce(t.Context())
	if err != nil || count != 1 {
		t.Fatalf("RunOnce() = %d, %v", count, err)
	}
	if len(publisher.messages) != 1 || publisher.messages[0].EventID != "event-1" || publisher.messages[0].NotificationID != "notification-1" {
		t.Fatalf("messages = %+v", publisher.messages)
	}
	if len(repo.marked) != 1 {
		t.Fatalf("marked = %v", repo.marked)
	}
}

func TestRunOnceTreatsStaleConfirmAsBenign(t *testing.T) {
	repo := &fakeRepository{events: []notification.OutboxEvent{oneEvent()}, markApplied: false}
	count, err := newService(repo, &fakePublisher{}).RunOnce(t.Context())
	if err != nil || count != 0 {
		t.Fatalf("RunOnce() = %d, %v", count, err)
	}
}
