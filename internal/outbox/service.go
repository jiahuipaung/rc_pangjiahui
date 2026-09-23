package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

var ErrConfirmLost = errors.New("publisher confirm lost")

type Message struct {
	EventID        string `json:"event_id"`
	NotificationID string `json:"notification_id"`
}

type Repository interface {
	ClaimOutbox(context.Context, time.Time, int, string, time.Time) ([]notification.OutboxEvent, error)
	MarkPublished(context.Context, string, string, time.Time) (bool, error)
}

type Publisher interface {
	Publish(context.Context, Message) error
}

type Config struct {
	BatchSize    int
	ClaimTTL     time.Duration
	PollInterval time.Duration
}

type Service struct {
	repository Repository
	publisher  Publisher
	now        func() time.Time
	newToken   func() string
	config     Config
}

func NewService(repository Repository, publisher Publisher, now func() time.Time, newToken func() string, config Config) *Service {
	return &Service{repository: repository, publisher: publisher, now: now, newToken: newToken, config: config}
}

func (service *Service) RunOnce(ctx context.Context) (int, error) {
	now := service.now()
	token := service.newToken()
	events, err := service.repository.ClaimOutbox(ctx, now, service.config.BatchSize, token, now.Add(service.config.ClaimTTL))
	if err != nil {
		return 0, fmt.Errorf("claim outbox: %w", err)
	}
	published := 0
	for _, event := range events {
		if err := service.publisher.Publish(ctx, Message{EventID: event.ID, NotificationID: event.AggregateID}); err != nil {
			return published, err
		}
		applied, err := service.repository.MarkPublished(ctx, event.ID, token, service.now())
		if err != nil {
			return published, fmt.Errorf("mark published: %w", err)
		}
		if applied {
			published++
		}
	}
	return published, nil
}

func (service *Service) Run(ctx context.Context) error {
	interval := service.config.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := service.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
