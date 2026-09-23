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

type Service struct {
	repository Repository
	registry   Registry
	now        func() time.Time
	newID      func() string
}

func New(repository Repository, registry Registry, now func() time.Time, newID func() string) *Service {
	return &Service{repository: repository, registry: registry, now: now, newID: newID}
}

func (service *Service) Create(ctx context.Context, caller, idempotencyKey, destinationID string, payload json.RawMessage) (notification.Task, bool, error) {
	target, ok := service.registry.Get(destinationID)
	if !ok {
		return notification.Task{}, false, ErrUnknownDestination
	}
	hash, err := notification.RequestHash(destinationID, payload)
	if err != nil {
		return notification.Task{}, false, fmt.Errorf("hash request: %w", err)
	}
	now := service.now()
	task := notification.Task{
		ID: service.newID(), CallerID: caller, IdempotencyKey: idempotencyKey, RequestHash: hash,
		Snapshot: notification.DeliverySnapshot{
			DestinationID: destinationID, Method: target.Method, URL: target.URL.String(), StaticHeaders: target.StaticHeaders.Clone(),
			SecretHeaders: cloneMap(target.SecretHeaders), Body: append(json.RawMessage(nil), payload...), Timeout: target.Timeout,
			Retry: target.Retry, IdempotencyKey: idempotencyKey,
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
		return task, false, nil
	}
	if existing.RequestHash != hash {
		return notification.Task{}, false, ErrIdempotencyConflict
	}
	return *existing, true, nil
}

func cloneMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
