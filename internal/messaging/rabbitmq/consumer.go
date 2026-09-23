package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jiahuipaung/rc_pangjiahui/internal/outbox"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Delivery struct {
	Message     outbox.Message
	Redelivered bool
	raw         amqp.Delivery
}

func (delivery Delivery) Ack() error              { return delivery.raw.Ack(false) }
func (delivery Delivery) Nack(requeue bool) error { return delivery.raw.Nack(false, requeue) }

func (client *Client) Consume(ctx context.Context, consumer string) (<-chan Delivery, error) {
	rawDeliveries, err := client.channel.ConsumeWithContext(ctx, client.topology.Queue, consumer, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("consume messages: %w", err)
	}
	deliveries := make(chan Delivery)
	go func() {
		defer close(deliveries)
		for raw := range rawDeliveries {
			var message outbox.Message
			if err := json.Unmarshal(raw.Body, &message); err != nil || message.EventID == "" || message.NotificationID == "" {
				_ = raw.Reject(false)
				continue
			}
			select {
			case deliveries <- Delivery{Message: message, Redelivered: raw.Redelivered, raw: raw}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return deliveries, nil
}
