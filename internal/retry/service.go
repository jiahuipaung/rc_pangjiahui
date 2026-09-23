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
type Config struct {
	BatchSize    int
	PollInterval time.Duration
}
type Service struct {
	repository Repository
	now        func() time.Time
	config     Config
}

func NewService(repository Repository, now func() time.Time, config Config) *Service {
	return &Service{repository: repository, now: now, config: config}
}
func (service *Service) RunOnce(ctx context.Context) (Result, error) {
	result, err := service.repository.ScheduleDue(ctx, service.now(), service.config.BatchSize)
	if err != nil {
		return Result{}, fmt.Errorf("schedule due notifications: %w", err)
	}
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
