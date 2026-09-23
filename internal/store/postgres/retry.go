package postgres

import (
	"context"
	"fmt"
	"time"

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
