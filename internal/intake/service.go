package intake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/destination"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key reused with different request")
	ErrUnknownDestination  = errors.New("unknown destination")
)

type Repository interface {
	CreateOrGet(context.Context, notification.Task, notification.OutboxEvent) (*notification.Task, error)
}

type Registry interface {
	Get(string) (destination.Destination, bool)
}

type Observer interface {
	ObserveIntake(string)
}

type noopObserver struct{}

func (noopObserver) ObserveIntake(string) {}

type Service struct {
	repository Repository
	registry   Registry
	now        func() time.Time
	newID      func() string
	observer   Observer
}

func New(repository Repository, registry Registry, now func() time.Time, newID func() string, observers ...Observer) *Service {
	observer := Observer(noopObserver{})
	if len(observers) > 0 && observers[0] != nil {
		observer = observers[0]
	}
	return &Service{repository: repository, registry: registry, now: now, newID: newID, observer: observer}
}

func (service *Service) Create(ctx context.Context, caller, idempotencyKey, destinationID string, payload json.RawMessage) (notification.Task, bool, error) {
	outcome := "internal_error"
	defer func() { service.observer.ObserveIntake(outcome) }()
	target, ok := service.registry.Get(destinationID)
	if !ok {
		outcome = "unknown_destination"
		return notification.Task{}, false, ErrUnknownDestination
	}
	hash, err := notification.RequestHash(destinationID, payload)
	if err != nil {
		outcome = "invalid_payload"
		return notification.Task{}, false, fmt.Errorf("hash request: %w", err)
	}
	now := service.now()
	task := notification.Task{
		ID: service.newID(), CallerID: caller, IdempotencyKey: idempotencyKey, RequestHash: hash,
		Snapshot: notification.DeliverySnapshot{
			DestinationID: destinationID, Method: target.Method, URL: target.URL.String(), StaticHeaders: target.StaticHeaders.Clone(),
			SecretHeaders: cloneMap(target.SecretHeaders), Body: append(json.RawMessage(nil), payload...), Timeout: target.Timeout,
			Retry: target.Retry, ConcurrencyLimit: target.ConcurrencyLimit, IdempotencyKey: idempotencyKey,
		},
		Status: notification.StatusPending, CreatedAt: now, UpdatedAt: now,
	}
	eventID := service.newID()
	eventPayload, _ := json.Marshal(map[string]string{"event_id": eventID, "notification_id": task.ID})
	event := notification.OutboxEvent{ID: eventID, AggregateID: task.ID, Generation: 0, EventType: "notification.ready", Payload: eventPayload, CreatedAt: now}
	existing, err := service.repository.CreateOrGet(ctx, task, event)
	if err != nil {
		return notification.Task{}, false, err
	}
	if existing == nil {
		outcome = "accepted"
		return task, false, nil
	}
	if existing.RequestHash != hash {
		outcome = "idempotency_conflict"
		return notification.Task{}, false, ErrIdempotencyConflict
	}
	outcome = "accepted"
	return *existing, true, nil
}

func cloneMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
