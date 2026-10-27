package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/dvasconcellos/elo/simulator/internal/events"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQConsumer struct {
	liveConsumer *domain.LiveConsumer
}

func NewRabbitMQConsumer(liveConsumer *domain.LiveConsumer) *RabbitMQConsumer {
	return &RabbitMQConsumer{
		liveConsumer: liveConsumer,
	}
}

// Handle processes a single raw AMQP message through the Domain lifecycle and ACKs only on success.
func (c *RabbitMQConsumer) Handle(ctx context.Context, msg amqp.Delivery) {
	err := c.process(ctx, msg)
	if err != nil {
		log.Printf("error processing message %s: %v", msg.MessageId, err)
		// NACK without requeue for now. DLQ handles it or redelivery handles transient errors if we used requeue=true
		// For the sake of the crash lab, redeliveries happen when connections drop BEFORE ACK.
		msg.Nack(false, false)
		return
	}

	msg.Ack(false)
}

func (c *RabbitMQConsumer) process(ctx context.Context, msg amqp.Delivery) error {
	var envelope events.RabbitMQMessage
	if err := json.Unmarshal(msg.Body, &envelope); err != nil {
		return fmt.Errorf("deserialize message envelope: %w", err)
	}

	event, err := domain.DeserializeEvent(envelope.EventType, envelope.Payload)
	if err != nil {
		return fmt.Errorf("deserialize domain event: %w", err)
	}

	// In the real system, LiveConsumer works per-aggregate. We assume this LiveConsumer is properly matched,
	// or we use a Dispatcher. For our lab, it's 1:1.
	if err := c.liveConsumer.Consume(ctx, event); err != nil {
		return fmt.Errorf("consume domain event: %w", err)
	}

	return nil
}

// Start consuming from a channel and block
func (c *RabbitMQConsumer) ConsumeLoop(ctx context.Context, messages <-chan amqp.Delivery) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			c.Handle(ctx, msg)
		}
	}
}
