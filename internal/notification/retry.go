package notification

import "time"

type Jitter interface {
	Apply(time.Duration) time.Duration
}

func NextAttempt(policy RetryPolicy, attempt int, retryAfter *time.Time, now time.Time, jitter Jitter) (time.Time, bool) {
	if attempt >= policy.MaxAttempts || attempt <= 0 || len(policy.Delays) == 0 {
		return time.Time{}, false
	}

	if retryAfter != nil && retryAfter.After(now) {
		if retryAfter.Sub(now) > policy.Lifetime {
			return time.Time{}, false
		}
		return *retryAfter, true
	}

	index := attempt - 1
	if index >= len(policy.Delays) {
		index = len(policy.Delays) - 1
	}
	delay := policy.Delays[index]
	if jitter != nil {
		delay = jitter.Apply(delay)
	}
	if delay < 0 || delay > policy.Lifetime {
		return time.Time{}, false
	}
	return now.Add(delay), true
}
