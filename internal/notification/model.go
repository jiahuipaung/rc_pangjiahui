package notification

import (
	"encoding/json"
	"net/http"
	"time"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusDelivering Status = "delivering"
	StatusRetryWait  Status = "retry_wait"
	StatusDelivered  Status = "delivered"
	StatusDead       Status = "dead"
)

type Outcome string

const (
	Delivered Outcome = "delivered"
	Retryable Outcome = "retryable"
	Permanent Outcome = "permanent"
)

type RetryPolicy struct {
	MaxAttempts int
	Lifetime    time.Duration
	Delays      []time.Duration
}

type DeliverySnapshot struct {
	DestinationID  string
	Method         string
	URL            string
	StaticHeaders  http.Header
	SecretHeaders  map[string]string
	Body           json.RawMessage
	Timeout        time.Duration
	Retry          RetryPolicy
	IdempotencyKey string
}

type Task struct {
	ID             string
	CallerID       string
	IdempotencyKey string
	RequestHash    [32]byte
	Snapshot       DeliverySnapshot
	Status         Status
	AttemptCount   int
	Generation     int
	NextAttemptAt  *time.Time
	LeaseToken     string
	LeaseUntil     *time.Time
	LastHTTPStatus int
	LastErrorCode  string
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeliveredAt    *time.Time
	DeadAt         *time.Time
}

type OutboxEvent struct {
	ID              string
	AggregateID     string
	Generation      int
	EventType       string
	Payload         json.RawMessage
	CreatedAt       time.Time
	PublishedAt     *time.Time
	ClaimToken      string
	ClaimUntil      *time.Time
	PublishAttempts int
}

type AttemptResult struct {
	Outcome       Outcome
	HTTPStatus    int
	ErrorCode     string
	ErrorMessage  string
	NextAttemptAt *time.Time
	CompletedAt   time.Time
}

type Claim struct {
	Task     Task
	Acquired bool
}

type ScheduleResult struct {
	Scheduled       int
	Dead            int
	RecoveredLeases int
}
