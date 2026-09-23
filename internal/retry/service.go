package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

type Result = notification.ScheduleResult

type Repository interface {
	ScheduleDue(context.Context, time.Time, int) (notification.ScheduleResult, error)
}
type Observer interface {
	ObserveSchedule(Result)
}
type noopObserver struct{}

func (noopObserver) ObserveSchedule(Result) {}

type Config struct {
	BatchSize    int
	PollInterval time.Duration
}
type Service struct {
	repository Repository
	now        func() time.Time
	config     Config
	observer   Observer
}

func NewService(repository Repository, now func() time.Time, config Config, observers ...Observer) *Service {
	observer := Observer(noopObserver{})
	if len(observers) > 0 && observers[0] != nil {
		observer = observers[0]
	}
	return &Service{repository: repository, now: now, config: config, observer: observer}
}
func (service *Service) RunOnce(ctx context.Context) (Result, error) {
	result, err := service.repository.ScheduleDue(ctx, service.now(), service.config.BatchSize)
	if err != nil {
		return Result{}, fmt.Errorf("schedule due notifications: %w", err)
	}
	service.observer.ObserveSchedule(result)
	return result, nil
}
func (service *Service) Run(ctx context.Context) error {
	interval := service.config.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if _, err := service.RunOnce(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
