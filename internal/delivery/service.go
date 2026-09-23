package delivery

import (
	"context"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

type Message struct{ EventID, NotificationID string }
type Disposition int

const (
	Ack Disposition = iota
	Requeue
	Reject
)

type Repository interface {
	ClaimDelivery(context.Context, string, string, time.Time) (notification.Claim, error)
	ApplyDeliveryResult(context.Context, string, string, notification.AttemptResult) (bool, error)
}
type Sender interface {
	Send(context.Context, notification.DeliverySnapshot) Result
}
type CapacityLimiter interface {
	Acquire(context.Context, string, int) (func(), error)
}
type Service struct {
	repository Repository
	sender     Sender
	limiter    CapacityLimiter
	jitter     notification.Jitter
	now        func() time.Time
	newLease   func() string
	leaseTTL   time.Duration
}

func NewService(repository Repository, sender Sender, limiter CapacityLimiter, jitter notification.Jitter, now func() time.Time, newLease func() string, leaseTTL time.Duration) *Service {
	return &Service{repository: repository, sender: sender, limiter: limiter, jitter: jitter, now: now, newLease: newLease, leaseTTL: leaseTTL}
}

func (service *Service) Handle(ctx context.Context, message Message) Disposition {
	lease, now := service.newLease(), service.now()
	claim, err := service.repository.ClaimDelivery(ctx, message.NotificationID, lease, now.Add(service.leaseTTL))
	if err != nil {
		return Requeue
	}
	if !claim.Acquired {
		return Ack
	}
	release, err := service.limiter.Acquire(ctx, claim.Task.Snapshot.DestinationID, claim.Task.Snapshot.ConcurrencyLimit)
	if err != nil {
		return Requeue
	}
	result := func() Result {
		defer release()
		return service.sender.Send(ctx, claim.Task.Snapshot)
	}()
	attempt := notification.AttemptResult{Outcome: result.Outcome, HTTPStatus: result.HTTPStatus, ErrorCode: result.ErrorCode, ErrorMessage: result.ErrorMessage, CompletedAt: service.now()}
	if result.Outcome == notification.Retryable {
		next, ok := notification.NextAttempt(claim.Task.Snapshot.Retry, claim.Task.AttemptCount, result.RetryAfter, attempt.CompletedAt, service.jitter)
		if ok {
			attempt.NextAttemptAt = &next
		} else {
			attempt.Outcome = notification.Permanent
		}
	}
	applied, err := service.repository.ApplyDeliveryResult(ctx, message.NotificationID, lease, attempt)
	if err != nil {
		return Requeue
	}
	if !applied {
		return Ack
	}
	return Ack
}
