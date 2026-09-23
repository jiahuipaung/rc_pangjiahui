package postgres

import (
	"context"
	"fmt"
	"time"
)

func (store *Store) DeleteTerminalBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	result, err := store.pool.Exec(ctx, `WITH candidates AS (
SELECT task.id FROM notification_tasks task
WHERE task.status IN ('delivered','dead') AND task.updated_at < $1
AND NOT EXISTS (SELECT 1 FROM outbox_events event WHERE event.aggregate_id=task.id AND event.published_at IS NULL)
ORDER BY task.updated_at,task.id LIMIT $2 FOR UPDATE SKIP LOCKED
) DELETE FROM notification_tasks task USING candidates WHERE task.id=candidates.id`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("delete terminal notifications: %w", err)
	}
	return result.RowsAffected(), nil
}
