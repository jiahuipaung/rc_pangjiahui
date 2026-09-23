//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	"github.com/jiahuipaung/rc_pangjiahui/internal/store/postgres"
	"github.com/jiahuipaung/rc_pangjiahui/internal/transport/httpapi"
)

func integrationEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Skip(key + " is required")
	}
	return value
}

type identityJitter struct{}

func (identityJitter) Apply(delay time.Duration) time.Duration { return delay }

func newDatabase(t *testing.T) *postgres.Store {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), integrationEnv(t, "TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, file, _, _ := runtime.Caller(0)
	migration, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "migrations", "000001_initial.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(t.Context(), "DROP TABLE IF EXISTS outbox_events; DROP TABLE IF EXISTS notification_tasks;"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(t.Context(), string(migration)); err != nil {
		t.Fatal(err)
	}
	return postgres.New(pool)
}

func TestEndToEndDurableNotificationDelivery(t *testing.T) {
	var received atomic.Int32
	supplier := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Idempotency-Key") != "order-1" {
			t.Errorf("idempotency key=%q", request.Header.Get("Idempotency-Key"))
		}
		received.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer supplier.Close()
	store := newDatabase(t)
	registry, err := destination.NewRegistry([]destination.Destination{{
		ID: "supplier", Method: http.MethodPost, URL: mustURL(t, supplier.URL), StaticHeaders: http.Header{"Content-Type": {"application/json"}},
		Timeout: time.Second, Retry: notification.RetryPolicy{MaxAttempts: 3, Lifetime: time.Hour, Delays: []time.Duration{time.Second}}, ConcurrencyLimit: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var idCounter atomic.Int64
	newID := func() string { return fmt.Sprintf("id-%d", idCounter.Add(1)) }
	now := func() time.Time { return time.Now().UTC() }
	intakeService := intake.New(store, registry, now, newID)
	queryService := notification.NewQueryService(store)
	replayService := notification.NewReplayService(store, now, newID)
	api := httpapi.NewRouter(httpapi.Dependencies{Creator: intakeService, Querier: queryService, Replayer: replayService, CallerTokens: map[string]string{"caller-token": "orders"}, AdminTokens: map[string]string{"admin-token": "operator"}})
	req := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(`{"destination_id":"supplier","payload":{"order_id":"1"}}`))
	req.Header.Set("Authorization", "Bearer caller-token")
	req.Header.Set("Idempotency-Key", "order-1")
	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", rr.Code, rr.Body.String())
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	client, err := broker.Open(integrationEnv(t, "TEST_RABBITMQ_URL"), broker.Topology{Exchange: "notifications.e2e." + suffix, Queue: "notifications.e2e." + suffix, RoutingKey: "notification.ready"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	publisher := outbox.NewService(store, client, now, newID, outbox.Config{BatchSize: 10, ClaimTTL: time.Minute})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	deliveries, err := client.Consume(ctx, "e2e-consumer-"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := publisher.RunOnce(ctx); err != nil || count != 1 {
		t.Fatalf("publish=%d,%v", count, err)
	}
	deliveryService := delivery.NewService(store, delivery.NewHTTPSender(delivery.SenderConfig{AllowPrivateNetworks: true, MaxDiagnosticBytes: 1024}, os.LookupEnv), delivery.NewLimiter(), identityJitter{}, now, newID, time.Minute)
	select {
	case message := <-deliveries:
		if disposition := deliveryService.Handle(ctx, delivery.Message{EventID: message.Message.EventID, NotificationID: message.Message.NotificationID}); disposition != delivery.Ack {
			t.Fatalf("disposition=%v", disposition)
		}
		if err := message.Ack(); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for broker delivery")
	}
	if received.Load() != 1 {
		t.Fatalf("supplier calls=%d", received.Load())
	}
}

func mustURL(t *testing.T, value string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
