package testsupport

import "time"

type Clock interface {
	Now() time.Time
}

type FixedClock struct {
	Time time.Time
}

func (clock FixedClock) Now() time.Time { return clock.Time }
