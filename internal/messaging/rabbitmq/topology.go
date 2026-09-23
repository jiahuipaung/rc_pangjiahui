package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Topology struct {
	Exchange   string
	Queue      string
	RoutingKey string
}

func declare(channel *amqp.Channel, topology Topology) error {
	if err := channel.ExchangeDeclare(topology.Exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	if _, err := channel.QueueDeclare(topology.Queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	if err := channel.QueueBind(topology.Queue, topology.RoutingKey, topology.Exchange, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}
	return nil
}
