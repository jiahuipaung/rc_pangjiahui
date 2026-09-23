package notification

func ClassifyResult(status int, err error) Outcome {
	if err != nil {
		return Retryable
	}
	if status >= 200 && status < 300 {
		return Delivered
	}
	switch status {
	case 408, 425, 429:
		return Retryable
	}
	if status >= 500 && status <= 599 {
		return Retryable
	}
	return Permanent
}

func CanTransition(from, to Status) bool {
	switch from {
	case StatusPending:
		return to == StatusDelivering
	case StatusDelivering:
		return to == StatusDelivered || to == StatusRetryWait || to == StatusDead
	case StatusRetryWait:
		return to == StatusPending || to == StatusDead
	case StatusDead:
		return to == StatusPending
	default:
		return false
	}
}
