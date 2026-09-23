package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

func (store *Store) ClaimDelivery(ctx context.Context, id, leaseToken string, leaseUntil time.Time) (notification.Claim, error) {
	row := store.pool.QueryRow(ctx, `UPDATE notification_tasks
SET status='delivering',lease_token=$2,lease_until=$3,attempt_count=attempt_count+1,updated_at=now()
WHERE id=$1 AND status='pending'
RETURNING id,caller_id,idempotency_key,status,attempt_count,generation,created_at,updated_at,
destination_id,method,url,static_headers,secret_headers,body,timeout_ns,max_attempts,lifetime_ns,retry_delays_ns`, id, leaseToken, leaseUntil)
	var task notification.Task
	var staticHeaders, secretHeaders, retryDelays []byte
	var timeoutNS, lifetimeNS int64
	err := row.Scan(&task.ID, &task.CallerID, &task.IdempotencyKey, &task.Status, &task.AttemptCount, &task.Generation, &task.CreatedAt, &task.UpdatedAt,
		&task.Snapshot.DestinationID, &task.Snapshot.Method, &task.Snapshot.URL, &staticHeaders, &secretHeaders, &task.Snapshot.Body,
		&timeoutNS, &task.Snapshot.Retry.MaxAttempts, &lifetimeNS, &retryDelays)
	if err == pgx.ErrNoRows {
		return notification.Claim{}, nil
	}
	if err != nil {
		return notification.Claim{}, fmt.Errorf("claim delivery: %w", err)
	}
	task.Snapshot.StaticHeaders = make(http.Header)
	if err := json.Unmarshal(staticHeaders, &task.Snapshot.StaticHeaders); err != nil {
		return notification.Claim{}, fmt.Errorf("decode static headers: %w", err)
	}
	if err := json.Unmarshal(secretHeaders, &task.Snapshot.SecretHeaders); err != nil {
		return notification.Claim{}, fmt.Errorf("decode secret headers: %w", err)
	}
	if err := json.Unmarshal(retryDelays, &task.Snapshot.Retry.Delays); err != nil {
		return notification.Claim{}, fmt.Errorf("decode retry delays: %w", err)
	}
	task.Snapshot.Timeout = time.Duration(timeoutNS)
	task.Snapshot.Retry.Lifetime = time.Duration(lifetimeNS)
	task.Snapshot.IdempotencyKey = task.IdempotencyKey
	task.LeaseToken = leaseToken
	task.LeaseUntil = &leaseUntil
	return notification.Claim{Task: task, Acquired: true}, nil
}

func (store *Store) ApplyDeliveryResult(ctx context.Context, id, leaseToken string, result notification.AttemptResult) (bool, error) {
	status := notification.StatusDead
	var next *time.Time
	var deliveredAt, deadAt *time.Time
	switch result.Outcome {
	case notification.Delivered:
		status = notification.StatusDelivered
		deliveredAt = &result.CompletedAt
	case notification.Retryable:
		status = notification.StatusRetryWait
		next = result.NextAttemptAt
	default:
		deadAt = &result.CompletedAt
	}
	command, err := store.pool.Exec(ctx, `UPDATE notification_tasks SET
status=$3,next_attempt_at=$4,lease_token=NULL,lease_until=NULL,last_http_status=NULLIF($5,0),
last_error_code=NULLIF($6,''),last_error_message=NULLIF($7,''),updated_at=$8,delivered_at=$9,dead_at=$10
WHERE id=$1 AND status='delivering' AND lease_token=$2`, id, leaseToken, status, next, result.HTTPStatus,
		result.ErrorCode, result.ErrorMessage, result.CompletedAt, deliveredAt, deadAt)
	if err != nil {
		return false, fmt.Errorf("apply delivery result: %w", err)
	}
	return command.RowsAffected() == 1, nil
}
