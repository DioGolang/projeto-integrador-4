package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	eventsExchange        = "logistics.events"
	projectionQueue       = "api.delivery_projection"
	projectionConsumerTag = "api.projection.consumer"
	dlxExchange           = "logistics.events.dlx"
	dlqQueue              = "api.delivery_projection.dlq"
)

var projectionRoutingKeys = []string{
	"DeliveryCreated",
	"DriverArrivedAtPickup",
	"DriverEnRouteToDropoff",
	"DeliveryCompleted",
	"DeliveryRouteAssigned",
}

// declareProjectionTopology declares exchange/queue/bindings and starts
// consuming with manual ACK. Any failure is returned: a half-declared topology
// would silently disconnect the read side from the command side.
func declareProjectionTopology(ch *amqp.Channel) (<-chan amqp.Delivery, error) {
	if err := ch.ExchangeDeclare(eventsExchange, "direct", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	// Declare Dead Letter Exchange and Queue for unrecoverable/poison messages
	if err := ch.ExchangeDeclare(dlxExchange, "fanout", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare dlx exchange: %w", err)
	}
	dlq, err := ch.QueueDeclare(dlqQueue, true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("declare dlq: %w", err)
	}
	if err := ch.QueueBind(dlq.Name, "#", dlxExchange, false, nil); err != nil {
		return nil, fmt.Errorf("bind dlq: %w", err)
	}

	q, err := ch.QueueDeclare(projectionQueue, true, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("declare queue: %w", err)
	}
	for _, key := range projectionRoutingKeys {
		if err := ch.QueueBind(q.Name, key, eventsExchange, false, nil); err != nil {
			return nil, fmt.Errorf("bind %s: %w", key, err)
		}
	}
	// Process one message at a time: causal order per aggregate depends on it.
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("qos: %w", err)
	}
	msgs, err := ch.Consume(q.Name, projectionConsumerTag, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("consume: %w", err)
	}
	return msgs, nil
}
