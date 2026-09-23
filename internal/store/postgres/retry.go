package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

func (store *Store) ScheduleRetry(ctx context.Context, id, expectedLease string, next time.Time, result notification.AttemptResult) (bool, error) {
	result.NextAttemptAt = &next
	return store.ApplyDeliveryResult(ctx, id, expectedLease, result)
}

func (store *Store) ReplayDead(ctx context.Context, id string, now time.Time) (bool, error) {
	command, err := store.pool.Exec(ctx, `UPDATE notification_tasks SET status='pending',generation=generation+1,
next_attempt_at=NULL,lease_token=NULL,lease_until=NULL,dead_at=NULL,updated_at=$2 WHERE id=$1 AND status='dead'`, id, now)
	if err != nil {
		return false, fmt.Errorf("replay dead task: %w", err)
	}
	return command.RowsAffected() == 1, nil
}

func (store *Store) ScheduleDue(ctx context.Context, now time.Time, limit int) (notification.ScheduleResult, error) {
	result := notification.ScheduleResult{}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return result, fmt.Errorf("begin schedule transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT id,status,attempt_count,max_attempts,created_at,lifetime_ns,generation
FROM notification_tasks
WHERE (status='retry_wait' AND next_attempt_at <= $1)
   OR (status='delivering' AND lease_until < $1)
ORDER BY COALESCE(next_attempt_at,lease_until),id
FOR UPDATE SKIP LOCKED LIMIT $2`, now, limit)
	if err != nil {
		return result, fmt.Errorf("select due notifications: %w", err)
	}
	type candidate struct {
		id                    string
		status                notification.Status
		attempts, maxAttempts int
		created               time.Time
		lifetimeNS            int64
		generation            int
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.id, &item.status, &item.attempts, &item.maxAttempts, &item.created, &item.lifetimeNS, &item.generation); err != nil {
			rows.Close()
			return result, err
		}
		candidates = append(candidates, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result, err
	}
	for _, item := range candidates {
		exhausted := item.attempts >= item.maxAttempts || !now.Before(item.created.Add(time.Duration(item.lifetimeNS)))
		if exhausted {
			if _, err := tx.Exec(ctx, `UPDATE notification_tasks SET status='dead',dead_at=$2,updated_at=$2,lease_token=NULL,lease_until=NULL,next_attempt_at=NULL WHERE id=$1`, item.id, now); err != nil {
				return result, err
			}
			result.Dead++
			continue
		}
		newGeneration := item.generation + 1
		errorCode := ""
		if item.status == notification.StatusDelivering {
			errorCode = "delivery_lease_expired"
			result.RecoveredLeases++
		}
		if _, err := tx.Exec(ctx, `UPDATE notification_tasks SET status='pending',generation=$2,updated_at=$3,
next_attempt_at=NULL,lease_token=NULL,lease_until=NULL,last_error_code=NULLIF($4,'') WHERE id=$1`, item.id, newGeneration, now, errorCode); err != nil {
			return result, err
		}
		payload, _ := json.Marshal(map[string]string{"notification_id": item.id})
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", item.id, newGeneration)))
		eventID := hex.EncodeToString(digest[:16])
		if _, err := tx.Exec(ctx, `INSERT INTO outbox_events (id,aggregate_id,generation,event_type,payload,created_at)
VALUES ($1,$2,$3,'notification.ready',$4,$5) ON CONFLICT (aggregate_id,generation) DO NOTHING`, eventID, item.id, newGeneration, payload, now); err != nil {
			return result, err
		}
		result.Scheduled++
	}
	if err := tx.Commit(ctx); err != nil {
		return notification.ScheduleResult{}, fmt.Errorf("commit schedule transaction: %w", err)
	}
	return result, nil
}
