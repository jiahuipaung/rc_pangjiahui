//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jiahuipaung/rc_pangjiahui/internal/delivery"
	"github.com/jiahuipaung/rc_pangjiahui/internal/destination"
	"github.com/jiahuipaung/rc_pangjiahui/internal/intake"
	broker "github.com/jiahuipaung/rc_pangjiahui/internal/messaging/rabbitmq"
	"github.com/jiahuipaung/rc_pangjiahui/internal/notification"
	"github.com/jiahuipaung/rc_pangjiahui/internal/outbox"
	retrysvc "github.com/jiahuipaung/rc_pangjiahui/internal/retry"
	"github.com/jiahuipaung/rc_pangjiahui/internal/store/postgres"
)

var recoverySequence atomic.Int64

type taskState struct {
	status     notification.Status
	attempts   int
	generation int
}

func recoveryIDs() func() string {
	var sequence atomic.Int64
	return func() string { return fmt.Sprintf("recovery-%d", sequence.Add(1)) }
}

func recoveryRegistry(t *testing.T, supplierURL string) destination.Registry {
	t.Helper()
	registry, err := destination.NewRegistry([]destination.Destination{{
		ID: "supplier", Method: http.MethodPost, URL: mustURL(t, supplierURL), StaticHeaders: http.Header{"Content-Type": {"application/json"}},
		Timeout: time.Second, Retry: notification.RetryPolicy{MaxAttempts: 3, Lifetime: time.Hour, Delays: []time.Duration{time.Second}}, ConcurrencyLimit: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func createRecoveryTask(t *testing.T, store *postgres.Store, registry destination.Registry, now func() time.Time, newID func() string) notification.Task {
	t.Helper()
	task, _, err := intake.New(store, registry, now, newID).Create(t.Context(), "orders", "recovery-key", "supplier", []byte(`{"order_id":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func recoveryBroker(t *testing.T, ctx context.Context) (*broker.Client, <-chan broker.Delivery) {
	t.Helper()
	suffix := fmt.Sprintf("%d-%d", time.Now().UnixNano(), recoverySequence.Add(1))
	client, err := broker.Open(integrationEnv(t, "TEST_RABBITMQ_URL"), broker.Topology{Exchange: "notifications.recovery." + suffix, Queue: "notifications.recovery." + suffix, RoutingKey: "notification.ready"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	deliveries, err := client.Consume(ctx, "recovery-consumer-"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	return client, deliveries
}

func receiveRecovery(t *testing.T, ctx context.Context, deliveries <-chan broker.Delivery) broker.Delivery {
	t.Helper()
	select {
	case message := <-deliveries:
		return message
	case <-ctx.Done():
		t.Fatal("timed out waiting for recovery broker delivery")
		return broker.Delivery{}
	}
}

func recoveryWorker(store *postgres.Store, now func() time.Time, newID func() string) *delivery.Service {
	sender := delivery.NewHTTPSender(delivery.SenderConfig{AllowPrivateNetworks: true, MaxDiagnosticBytes: 1024}, os.LookupEnv)
	return delivery.NewService(store, sender, delivery.NewLimiter(), identityJitter{}, now, newID, time.Minute)
}

func readTaskState(t *testing.T, pool *pgxpool.Pool, id string) taskState {
	t.Helper()
	var state taskState
	if err := pool.QueryRow(t.Context(), "SELECT status,attempt_count,generation FROM notification_tasks WHERE id=$1", id).Scan(&state.status, &state.attempts, &state.generation); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestRetryableSupplierFailureRecoversThroughScheduler(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	supplier := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer supplier.Close()
	store, pool := newDatabase(t)
	newID := recoveryIDs()
	task := createRecoveryTask(t, store, recoveryRegistry(t, supplier.URL), func() time.Time { return base }, newID)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, deliveries := recoveryBroker(t, ctx)
	publisher := outbox.NewService(store, client, func() time.Time { return base }, newID, outbox.Config{BatchSize: 10, ClaimTTL: time.Minute})
	if count, err := publisher.RunOnce(ctx); err != nil || count != 1 {
		t.Fatalf("initial publish = %d, %v", count, err)
	}
	first := receiveRecovery(t, ctx, deliveries)
	if got := recoveryWorker(store, func() time.Time { return base }, newID).Handle(ctx, delivery.Message{EventID: first.Message.EventID, NotificationID: first.Message.NotificationID}); got != delivery.Ack {
		t.Fatalf("first disposition = %v", got)
	}
	if err := first.Ack(); err != nil {
		t.Fatal(err)
	}
	if state := readTaskState(t, pool, task.ID); state.status != notification.StatusRetryWait || state.attempts != 1 || state.generation != 0 {
		t.Fatalf("after 503 = %+v", state)
	}
	retryNow := base.Add(2 * time.Second)
	result, err := retrysvc.NewService(store, func() time.Time { return retryNow }, retrysvc.Config{BatchSize: 10}).RunOnce(ctx)
	if err != nil || result.Scheduled != 1 {
		t.Fatalf("schedule = %+v, %v", result, err)
	}
	retryPublisher := outbox.NewService(store, client, func() time.Time { return retryNow }, newID, outbox.Config{BatchSize: 10, ClaimTTL: time.Minute})
	if count, err := retryPublisher.RunOnce(ctx); err != nil || count != 1 {
		t.Fatalf("retry publish = %d, %v", count, err)
	}
	second := receiveRecovery(t, ctx, deliveries)
	if got := recoveryWorker(store, func() time.Time { return retryNow }, newID).Handle(ctx, delivery.Message{EventID: second.Message.EventID, NotificationID: second.Message.NotificationID}); got != delivery.Ack {
		t.Fatalf("second disposition = %v", got)
	}
	if err := second.Ack(); err != nil {
		t.Fatal(err)
	}
	if state := readTaskState(t, pool, task.ID); state.status != notification.StatusDelivered || state.attempts != 2 || state.generation != 1 {
		t.Fatalf("final state = %+v", state)
	}
	if calls.Load() != 2 {
		t.Fatalf("supplier calls = %d, want 2", calls.Load())
	}
}

func TestDuplicateBrokerMessageDoesNotRepeatTerminalDelivery(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	supplier := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer supplier.Close()
	store, pool := newDatabase(t)
	newID := recoveryIDs()
	task := createRecoveryTask(t, store, recoveryRegistry(t, supplier.URL), func() time.Time { return base }, newID)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, deliveries := recoveryBroker(t, ctx)
	publisher := outbox.NewService(store, client, func() time.Time { return base }, newID, outbox.Config{BatchSize: 10, ClaimTTL: time.Minute})
	if _, err := publisher.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	first := receiveRecovery(t, ctx, deliveries)
	if err := client.Publish(ctx, outbox.Message{EventID: first.Message.EventID, NotificationID: first.Message.NotificationID}); err != nil {
		t.Fatal(err)
	}
	worker := recoveryWorker(store, func() time.Time { return base }, newID)
	for _, message := range []broker.Delivery{first, receiveRecovery(t, ctx, deliveries)} {
		if got := worker.Handle(ctx, delivery.Message{EventID: message.Message.EventID, NotificationID: message.Message.NotificationID}); got != delivery.Ack {
			t.Fatalf("disposition = %v", got)
		}
		if err := message.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	if state := readTaskState(t, pool, task.ID); state.status != notification.StatusDelivered || state.attempts != 1 {
		t.Fatalf("state = %+v", state)
	}
	if calls.Load() != 1 {
		t.Fatalf("supplier calls = %d, want 1", calls.Load())
	}
}

func TestExpiredDeliveryLeaseIsRecoveredAndCompletes(t *testing.T) {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	supplier := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) }))
	defer supplier.Close()
	store, pool := newDatabase(t)
	newID := recoveryIDs()
	task := createRecoveryTask(t, store, recoveryRegistry(t, supplier.URL), func() time.Time { return base }, newID)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, deliveries := recoveryBroker(t, ctx)
	publisher := outbox.NewService(store, client, func() time.Time { return base }, newID, outbox.Config{BatchSize: 10, ClaimTTL: time.Minute})
	if _, err := publisher.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	original := receiveRecovery(t, ctx, deliveries)
	if err := original.Ack(); err != nil {
		t.Fatal(err)
	}
	if claim, err := store.ClaimDelivery(ctx, task.ID, "crashed-worker", base.Add(-time.Second)); err != nil || !claim.Acquired {
		t.Fatalf("crashed claim = %+v, %v", claim, err)
	}
	recoveryNow := base.Add(time.Second)
	result, err := retrysvc.NewService(store, func() time.Time { return recoveryNow }, retrysvc.Config{BatchSize: 10}).RunOnce(ctx)
	if err != nil || result.Scheduled != 1 || result.RecoveredLeases != 1 {
		t.Fatalf("schedule = %+v, %v", result, err)
	}
	if count, err := outbox.NewService(store, client, func() time.Time { return recoveryNow }, newID, outbox.Config{BatchSize: 10, ClaimTTL: time.Minute}).RunOnce(ctx); err != nil || count != 1 {
		t.Fatalf("recovery publish = %d, %v", count, err)
	}
	recovery := receiveRecovery(t, ctx, deliveries)
	if got := recoveryWorker(store, func() time.Time { return recoveryNow }, newID).Handle(ctx, delivery.Message{EventID: recovery.Message.EventID, NotificationID: recovery.Message.NotificationID}); got != delivery.Ack {
		t.Fatalf("recovery disposition = %v", got)
	}
	if err := recovery.Ack(); err != nil {
		t.Fatal(err)
	}
	if state := readTaskState(t, pool, task.ID); state.status != notification.StatusDelivered || state.attempts != 2 || state.generation != 1 {
		t.Fatalf("final state = %+v", state)
	}
}
