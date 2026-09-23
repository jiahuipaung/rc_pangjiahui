package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

func (store *Store) CreateOrGet(ctx context.Context, task notification.Task, event notification.OutboxEvent) (*notification.Task, error) {
	err := store.WithTx(ctx, func(tx Tx) error {
		if err := tx.CreateNotification(ctx, task); err != nil {
			return err
		}
		return tx.InsertOutbox(ctx, event)
	})
	if err == nil {
		return nil, nil
	}
	if !errors.Is(err, ErrIdempotencyConflict) {
		return nil, err
	}
	existing, getErr := store.GetByCallerAndKey(ctx, task.CallerID, task.IdempotencyKey)
	if getErr != nil {
		return nil, getErr
	}
	return &existing, nil
}

func (tx transaction) CreateNotification(ctx context.Context, task notification.Task) error {
	staticHeaders, err := json.Marshal(task.Snapshot.StaticHeaders)
	if err != nil {
		return fmt.Errorf("marshal static headers: %w", err)
	}
	secretHeaders, err := json.Marshal(task.Snapshot.SecretHeaders)
	if err != nil {
		return fmt.Errorf("marshal secret headers: %w", err)
	}
	retryDelays, err := json.Marshal(task.Snapshot.Retry.Delays)
	if err != nil {
		return fmt.Errorf("marshal retry delays: %w", err)
	}
	_, err = tx.tx.Exec(ctx, `
INSERT INTO notification_tasks (
 id, caller_id, idempotency_key, request_hash, destination_id, method, url,
 static_headers, secret_headers, body, timeout_ns, max_attempts, lifetime_ns,
 retry_delays_ns, status, attempt_count, generation, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
		task.ID, task.CallerID, task.IdempotencyKey, task.RequestHash[:], task.Snapshot.DestinationID,
		task.Snapshot.Method, task.Snapshot.URL, staticHeaders, secretHeaders, task.Snapshot.Body,
		int64(task.Snapshot.Timeout), task.Snapshot.Retry.MaxAttempts, int64(task.Snapshot.Retry.Lifetime),
		retryDelays, task.Status, task.AttemptCount, task.Generation, task.CreatedAt, task.UpdatedAt)
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.ConstraintName == "notification_tasks_caller_id_idempotency_key_key" {
		return ErrIdempotencyConflict
	}
	return fmt.Errorf("insert notification: %w", err)
}
