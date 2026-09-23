package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key conflict")
	ErrGenerationConflict  = errors.New("outbox generation conflict")
)

type Tx interface {
	CreateNotification(context.Context, notification.Task) error
	InsertOutbox(context.Context, notification.OutboxEvent) error
}

type transaction struct{ tx pgx.Tx }

func (store *Store) WithTx(ctx context.Context, fn func(Tx) error) error {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(transaction{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
