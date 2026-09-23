package delivery

import (
	"context"
	"fmt"
	"sync"
)

type destinationCapacity struct {
	limit   int
	active  int
	changed chan struct{}
}

type Limiter struct {
	mu           sync.Mutex
	destinations map[string]*destinationCapacity
}

func NewLimiter() *Limiter {
	return &Limiter{destinations: make(map[string]*destinationCapacity)}
}

func (limiter *Limiter) Acquire(ctx context.Context, destinationID string, limit int) (func(), error) {
	if destinationID == "" || limit < 1 {
		return nil, fmt.Errorf("destination id and positive concurrency limit are required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		limiter.mu.Lock()
		capacity := limiter.destinations[destinationID]
		if capacity == nil {
			capacity = &destinationCapacity{limit: limit, changed: make(chan struct{})}
			limiter.destinations[destinationID] = capacity
		} else if limit < capacity.limit {
			capacity.limit = limit
			capacity.notify()
		}
		if capacity.active < capacity.limit {
			capacity.active++
			limiter.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					limiter.mu.Lock()
					capacity.active--
					capacity.notify()
					limiter.mu.Unlock()
				})
			}, nil
		}
		changed := capacity.changed
		limiter.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (capacity *destinationCapacity) notify() {
	close(capacity.changed)
	capacity.changed = make(chan struct{})
}
