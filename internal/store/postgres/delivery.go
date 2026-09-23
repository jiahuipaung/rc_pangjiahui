package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

func (store *Store) ClaimDelivery(ctx context.Context, id, leaseToken string, leaseUntil time.Time) (notification.Claim, error) {
	row := store.pool.QueryRow(ctx, `UPDATE notification_tasks
SET status='delivering',lease_token=$2,lease_until=$3,attempt_count=attempt_count+1,updated_at=now()
WHERE id=$1 AND status='pending'
RETURNING id,caller_id,idempotency_key,status,attempt_count,generation,created_at,updated_at`, id, leaseToken, leaseUntil)
	var task notification.Task
	err := row.Scan(&task.ID, &task.CallerID, &task.IdempotencyKey, &task.Status, &task.AttemptCount, &task.Generation, &task.CreatedAt, &task.UpdatedAt)
	if err == pgx.ErrNoRows {
		return notification.Claim{}, nil
	}
	if err != nil {
		return notification.Claim{}, fmt.Errorf("claim delivery: %w", err)
	}
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
