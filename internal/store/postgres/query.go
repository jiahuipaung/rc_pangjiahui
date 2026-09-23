package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

var ErrNotFound = errors.New("notification not found")

func (store *Store) GetByCallerAndKey(ctx context.Context, callerID, key string) (notification.Task, error) {
	var task notification.Task
	var hash []byte
	err := store.pool.QueryRow(ctx, `SELECT id,caller_id,idempotency_key,request_hash,status,attempt_count,generation,
created_at,updated_at FROM notification_tasks WHERE caller_id=$1 AND idempotency_key=$2`, callerID, key).
		Scan(&task.ID, &task.CallerID, &task.IdempotencyKey, &hash, &task.Status, &task.AttemptCount, &task.Generation, &task.CreatedAt, &task.UpdatedAt)
	if err == pgx.ErrNoRows {
		return notification.Task{}, ErrNotFound
	}
	if err != nil {
		return notification.Task{}, fmt.Errorf("get notification: %w", err)
	}
	copy(task.RequestHash[:], hash)
	return task, nil
}
