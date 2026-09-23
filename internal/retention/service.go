package retention

import (
	"context"
	"fmt"
	"time"
)

type Repository interface {
	DeleteTerminalBefore(context.Context, time.Time, int) (int64, error)
}
type Config struct {
	Retention    time.Duration
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
func (service *Service) RunOnce(ctx context.Context) (int64, error) {
	count, err := service.repository.DeleteTerminalBefore(ctx, service.now().Add(-service.config.Retention), service.config.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("delete retained notifications: %w", err)
	}
	return count, nil
}
