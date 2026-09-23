package testsupport

import "time"

type FixedJitter struct {
	Duration time.Duration
}

func (jitter FixedJitter) Apply(time.Duration) time.Duration { return jitter.Duration }
