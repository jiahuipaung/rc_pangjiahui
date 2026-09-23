package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Role interface {
	Run(context.Context) error
	StopClaims()
}

func Run(parent context.Context, roles []Role, shutdownTimeout time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errorsChannel := make(chan error, len(roles))
	var workers sync.WaitGroup
	for _, role := range roles {
		workers.Add(1)
		go func(role Role) { defer workers.Done(); errorsChannel <- role.Run(ctx) }(role)
	}
	select {
	case <-parent.Done():
		for _, role := range roles {
			role.StopClaims()
		}
		cancel()
	case err := <-errorsChannel:
		for _, role := range roles {
			role.StopClaims()
		}
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("role failed: %w", err)
		}
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	if shutdownTimeout <= 0 {
		shutdownTimeout = 10 * time.Second
	}
	select {
	case <-done:
		return nil
	case <-time.After(shutdownTimeout):
		return fmt.Errorf("graceful shutdown exceeded %s", shutdownTimeout)
	}
}
