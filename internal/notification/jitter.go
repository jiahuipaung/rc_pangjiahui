package notification

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"
)

type BoundedJitter struct {
	mu     sync.Mutex
	random *rand.Rand
}

func NewBoundedJitter() (*BoundedJitter, error) {
	return newBoundedJitter(cryptorand.Reader)
}

func newBoundedJitter(source io.Reader) (*BoundedJitter, error) {
	var seedBytes [8]byte
	if _, err := io.ReadFull(source, seedBytes[:]); err != nil {
		return nil, fmt.Errorf("seed retry jitter: %w", err)
	}
	seed := int64(binary.LittleEndian.Uint64(seedBytes[:]))
	return &BoundedJitter{random: rand.New(rand.NewSource(seed))}, nil
}

func (jitter *BoundedJitter) Apply(delay time.Duration) time.Duration {
	if delay <= 0 {
		return delay
	}
	jitter.mu.Lock()
	factor := 0.8 + jitter.random.Float64()*0.4
	jitter.mu.Unlock()
	result := time.Duration(float64(delay) * factor)
	if result < time.Nanosecond {
		return time.Nanosecond
	}
	return result
}
