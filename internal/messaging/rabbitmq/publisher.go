package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/jiahuipaung/rc_pangjiahui/internal/outbox"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	topology   Topology
	confirms   <-chan amqp.Confirmation
	returns    <-chan amqp.Return
	publishMu  sync.Mutex
}

func Open(rawURL string, topology Topology) (*Client, error) {
	connection, err := amqp.Dial(rawURL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}
	if err := declare(channel, topology); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, err
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, fmt.Errorf("enable confirms: %w", err)
	}
	return &Client{
		connection: connection, channel: channel, topology: topology,
		confirms: channel.NotifyPublish(make(chan amqp.Confirmation, 1)),
		returns:  channel.NotifyReturn(make(chan amqp.Return, 1)),
	}, nil
}

func (client *Client) Publish(ctx context.Context, message outbox.Message) error {
	client.publishMu.Lock()
	defer client.publishMu.Unlock()
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	err = client.channel.PublishWithContext(ctx, client.topology.Exchange, client.topology.RoutingKey, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", MessageId: message.EventID,
		CorrelationId: message.NotificationID, Body: body,
	})
	if err != nil {
		return fmt.Errorf("publish message: %w", err)
	}
	select {
	case returned := <-client.returns:
		return fmt.Errorf("message returned by broker: %s", returned.ReplyText)
	case confirmation, ok := <-client.confirms:
		if !ok || !confirmation.Ack {
			return outbox.ErrConfirmLost
		}
		select {
		case returned := <-client.returns:
			return fmt.Errorf("message returned by broker: %s", returned.ReplyText)
		default:
			return nil
		}
	case <-ctx.Done():
		return errors.Join(outbox.ErrConfirmLost, ctx.Err())
	}
}

func (client *Client) Close() error {
	channelErr := client.channel.Close()
	connectionErr := client.connection.Close()
	return errors.Join(channelErr, connectionErr)
}
