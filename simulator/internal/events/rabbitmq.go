package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dvasconcellos/elo/simulator/internal/outbox"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQPublisher struct {
	channel  *amqp.Channel
	exchange string
}

func NewRabbitMQPublisher(channel *amqp.Channel, exchange string) *RabbitMQPublisher {
	return &RabbitMQPublisher{
		channel:  channel,
		exchange: exchange,
	}
}

// RabbitMQMessage wraps the outbox event along with its global StreamPosition.
// This is exactly what travels over the wire.
type RabbitMQMessage struct {
	EventID        string          `json:"event_id"`
	AggregateID    string          `json:"aggregate_id"`
	Sequence       int64           `json:"sequence"`
	StreamPosition int64           `json:"stream_position"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
}

func (p *RabbitMQPublisher) Publish(ctx context.Context, event outbox.OutboxEvent) error {
	msg := RabbitMQMessage{
		EventID:        event.ID,
		AggregateID:    event.AggregateID, // Note: OutboxEvent.AggregateID is string in domain, wait, is it UUID or string?
		Sequence:       event.Sequence,
		StreamPosition: event.StreamPosition,
		EventType:      event.EventType,
		Payload:        event.Payload,
	}
	
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal amqp message: %w", err)
	}
	
	routingKey := fmt.Sprintf("%s.%s", event.AggregateType, event.EventType)

	err = p.channel.PublishWithContext(
		ctx,
		p.exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			MessageId:   event.ID,
			Type:        event.EventType,
			ContentType: "application/json",
			Body:        body,
		},
	)
	
	if err != nil {
		return fmt.Errorf("amqp publish: %w", err)
	}
	
	return nil
}
