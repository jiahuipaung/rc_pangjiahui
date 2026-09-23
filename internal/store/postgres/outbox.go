package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

func (tx transaction) InsertOutbox(ctx context.Context, event notification.OutboxEvent) error {
	_, err := tx.tx.Exec(ctx, `INSERT INTO outbox_events
(id, aggregate_id, generation, event_type, payload, created_at)
VALUES ($1,$2,$3,$4,$5,$6)`, event.ID, event.AggregateID, event.Generation, event.EventType, event.Payload, event.CreatedAt)
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.ConstraintName == "outbox_events_aggregate_id_generation_key" {
		return ErrGenerationConflict
	}
	return fmt.Errorf("insert outbox event: %w", err)
}

func (store *Store) ClaimOutbox(ctx context.Context, now time.Time, limit int, token string, until time.Time) ([]notification.OutboxEvent, error) {
	rows, err := store.pool.Query(ctx, `
WITH candidates AS (
 SELECT id FROM outbox_events
 WHERE published_at IS NULL AND (claim_until IS NULL OR claim_until < $1)
 ORDER BY created_at, id
 FOR UPDATE SKIP LOCKED LIMIT $2
)
UPDATE outbox_events AS e
SET claim_token=$3, claim_until=$4, publish_attempts=publish_attempts+1
FROM candidates WHERE e.id=candidates.id
RETURNING e.id,e.aggregate_id,e.generation,e.event_type,e.payload,e.created_at,e.published_at,e.claim_token,e.claim_until,e.publish_attempts`, now, limit, token, until)
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	defer rows.Close()
	var events []notification.OutboxEvent
	for rows.Next() {
		var event notification.OutboxEvent
		if err := rows.Scan(&event.ID, &event.AggregateID, &event.Generation, &event.EventType, &event.Payload, &event.CreatedAt, &event.PublishedAt, &event.ClaimToken, &event.ClaimUntil, &event.PublishAttempts); err != nil {
			return nil, fmt.Errorf("scan claimed outbox: %w", err)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (store *Store) MarkPublished(ctx context.Context, eventID, token string, at time.Time) (bool, error) {
	result, err := store.pool.Exec(ctx, `UPDATE outbox_events SET published_at=$3,claim_token=NULL,claim_until=NULL
WHERE id=$1 AND claim_token=$2 AND published_at IS NULL`, eventID, token, at)
	if err != nil {
		return false, fmt.Errorf("mark outbox published: %w", err)
	}
	return result.RowsAffected() == 1, nil
}
