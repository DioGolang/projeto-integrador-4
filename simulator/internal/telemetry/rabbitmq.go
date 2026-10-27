package telemetry

import (
	"context"
	"encoding/json"
	"time"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type TelemetryEnvelope struct {
	EventID   string      `json:"event_id"`
	EventType string      `json:"event_type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type RabbitMQPublisher struct {
	channel  *amqp.Channel
	exchange string
}

func NewRabbitMQPublisher(ch *amqp.Channel, exchange string) *RabbitMQPublisher {
	return &RabbitMQPublisher{
		channel:  ch,
		exchange: exchange,
	}
}

func (p *RabbitMQPublisher) PublishLocation(ctx context.Context, position domain.DriverPosition) error {
	routingKey := "driver.location.updated"

	envelope := TelemetryEnvelope{
		EventID:   "tel-" + fmt.Sprintf("%d", time.Now().UnixNano()),
		EventType: "DriverLocationUpdated",
		Timestamp: time.Now().UTC(),
		Payload:   position,
	}

	body, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	return p.channel.PublishWithContext(ctx,
		p.exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}
