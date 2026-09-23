package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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

func (store *Store) GetForCaller(ctx context.Context, callerID, id string) (notification.Task, error) {
	var task notification.Task
	err := store.pool.QueryRow(ctx, `SELECT id,caller_id,destination_id,status,attempt_count,next_attempt_at,
COALESCE(last_http_status,0),COALESCE(last_error_code,''),COALESCE(last_error_message,''),created_at,updated_at,delivered_at,dead_at
FROM notification_tasks WHERE id=$1 AND caller_id=$2`, id, callerID).Scan(
		&task.ID, &task.CallerID, &task.Snapshot.DestinationID, &task.Status, &task.AttemptCount, &task.NextAttemptAt,
		&task.LastHTTPStatus, &task.LastErrorCode, &task.LastError, &task.CreatedAt, &task.UpdatedAt, &task.DeliveredAt, &task.DeadAt)
	if err == pgx.ErrNoRows {
		return notification.Task{}, notification.ErrTaskNotFound
	}
	if err != nil {
		return notification.Task{}, fmt.Errorf("get notification for caller: %w", err)
	}
	return task, nil
}

func (store *Store) ReplayDeadAtomic(ctx context.Context, id string, now time.Time, eventID string) (bool, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin replay: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var generation int
	err = tx.QueryRow(ctx, `UPDATE notification_tasks SET status='pending',generation=generation+1,next_attempt_at=NULL,
lease_token=NULL,lease_until=NULL,dead_at=NULL,updated_at=$2 WHERE id=$1 AND status='dead' RETURNING generation`, id, now).Scan(&generation)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("update replay task: %w", err)
	}
	payload, _ := json.Marshal(map[string]string{"notification_id": id})
	if _, err := tx.Exec(ctx, `INSERT INTO outbox_events (id,aggregate_id,generation,event_type,payload,created_at)
VALUES ($1,$2,$3,'notification.ready',$4,$5)`, eventID, id, generation, payload, now); err != nil {
		return false, fmt.Errorf("insert replay outbox: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit replay: %w", err)
	}
	return true, nil
}
