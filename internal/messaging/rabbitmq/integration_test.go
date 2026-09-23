//go:build integration

package rabbitmq

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jiahuipaung/rc_pangjiahui/internal/outbox"
)

func rabbitURL(t *testing.T) string {
	t.Helper()
	value := os.Getenv("TEST_RABBITMQ_URL")
	if value == "" {
		t.Skip("TEST_RABBITMQ_URL is required")
	}
	return value
}

func TestPublisherConfirmsPersistentMessage(t *testing.T) {
	client, err := Open(rabbitURL(t), Topology{Exchange: "notifications.test", Queue: "notifications.test", RoutingKey: "notification.ready"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err = client.Publish(ctx, outbox.Message{EventID: "event-1", NotificationID: "notification-1"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deliveries, err := client.Consume(ctx, "test-consumer")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	select {
	case delivery := <-deliveries:
		if delivery.Message.EventID != "event-1" || delivery.Message.NotificationID != "notification-1" {
			t.Fatalf("message = %+v", delivery.Message)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for delivery")
	}
}
