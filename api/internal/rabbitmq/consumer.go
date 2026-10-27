package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/pkg/logger"
	"github.com/dvasconcellos/elo/api/pkg/metrics"
	appotel "github.com/dvasconcellos/elo/api/pkg/otel"
)

type StreamProcessor interface {
	Process(ctx context.Context, ev stream.Event) error
}

type ConsumerOption func(*Consumer)

func WithConsumerMetrics(m metrics.Metrics) ConsumerOption {
	return func(c *Consumer) {
		c.metrics = m
	}
}

func WithConsumerTracer(t trace.Tracer) ConsumerOption {
	return func(c *Consumer) {
		c.tracer = t
	}
}

func WithConsumerLogger(l logger.Logger) ConsumerOption {
	return func(c *Consumer) {
		c.logger = l
	}
}

type Consumer struct {
	ch      *amqp.Channel
	target  StreamProcessor
	metrics metrics.Metrics
	tracer  trace.Tracer
	logger  logger.Logger
}

func NewConsumer(ch *amqp.Channel, target StreamProcessor, opts ...ConsumerOption) *Consumer {
	c := &Consumer{
		ch:     ch,
		target: target,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.tracer == nil {
		c.tracer = otel.GetTracerProvider().Tracer("rabbitmq-consumer")
	}
	return c
}

type envelope struct {
	AggregateID    uuid.UUID       `json:"aggregate_id"`
	StreamPosition int64           `json:"stream_position"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
}

func (c *Consumer) HandleDelivery(ctx context.Context, d amqp.Delivery) error {
	start := time.Now()

	// 1. Extract W3C trace context propagated through AMQP message headers
	extractedCtx := appotel.ExtractAMQPHeaders(ctx, d.Headers)

	tr := c.tracer
	if tr == nil {
		tr = otel.GetTracerProvider().Tracer("rabbitmq-consumer")
	}

	consumerCtx, span := tr.Start(extractedCtx, "logistics.events process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", "logistics.events"),
			attribute.String("messaging.operation", "process"),
			attribute.Bool("messaging.rabbitmq.redelivered", d.Redelivered),
		),
	)
	defer span.End()

	var env envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent("unknown", "dlq", d.Redelivered, time.Since(start))
		}
		if c.logger != nil {
			c.logger.Error(extractedCtx, "Malformed AMQP message envelope rejected to DLQ", logger.WithError(err))
		}
		d.Reject(false)
		return fmt.Errorf("failed to unmarshal envelope: %w", err)
	}

	span.SetAttributes(
		attribute.String("messaging.message.id", env.AggregateID.String()),
		attribute.String("messaging.rabbitmq.routing_key", env.EventType),
		attribute.String("app.event_type", env.EventType),
		attribute.Int64("app.stream_position", env.StreamPosition),
	)

	// Deserialize DomainEvent
	var ev delivery.DomainEvent
	switch env.EventType {
	case "DeliveryCreated":
		var e delivery.EventDeliveryCreated
		if err := json.Unmarshal(env.Payload, &e); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Error(consumerCtx, "Malformed DeliveryCreated payload rejected to DLQ",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.WithError(err),
				)
			}
			d.Reject(false)
			return err
		}
		ev = e
	case "DriverArrivedAtPickup":
		var e delivery.EventDriverArrivedAtPickup
		if err := json.Unmarshal(env.Payload, &e); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Error(consumerCtx, "Malformed DriverArrivedAtPickup payload rejected to DLQ",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.WithError(err),
				)
			}
			d.Reject(false)
			return err
		}
		ev = e
	case "DriverEnRouteToDropoff":
		var e delivery.EventDriverEnRouteToDropoff
		if err := json.Unmarshal(env.Payload, &e); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Error(consumerCtx, "Malformed DriverEnRouteToDropoff payload rejected to DLQ",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.WithError(err),
				)
			}
			d.Reject(false)
			return err
		}
		ev = e
	case "DeliveryCompleted":
		var e delivery.EventDeliveryCompleted
		if err := json.Unmarshal(env.Payload, &e); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Error(consumerCtx, "Malformed DeliveryCompleted payload rejected to DLQ",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.WithError(err),
				)
			}
			d.Reject(false)
			return err
		}
		ev = e
	case "DeliveryRouteAssigned":
		var e delivery.EventDeliveryRouteAssigned
		if err := json.Unmarshal(env.Payload, &e); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Error(consumerCtx, "Malformed DeliveryRouteAssigned payload rejected to DLQ",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.WithError(err),
				)
			}
			d.Reject(false)
			return err
		}
		ev = e
	default:
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent(env.EventType, "ack", d.Redelivered, time.Since(start))
		}
		span.SetStatus(codes.Ok, "ignored unknown event")
		d.Ack(false) // Unknown event, ignore
		return nil
	}

	streamEv := stream.Event{
		StreamPosition: env.StreamPosition,
		DomainEvent:    ev,
	}

	if err := c.target.Process(consumerCtx, streamEv); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		isUnrecoverable := errors.Is(err, stream.ErrUnrecoverableGap)
		if isUnrecoverable || d.Redelivered {
			// Permanent failure or max retries reached: route to DLQ
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Error(consumerCtx, "Routing unrecoverable or exceeded-retry message to DLQ",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.Any("stream_position", env.StreamPosition),
					logger.Any("redelivered", d.Redelivered),
					logger.WithError(err),
				)
			}
			if rejectErr := d.Reject(false); rejectErr != nil {
				return fmt.Errorf("failed to reject unrecoverable message to DLQ: %w (original error: %v)", rejectErr, err)
			}
			return fmt.Errorf("message unrecoverable, rejected to DLQ: %w", err)
		}

		// Transient first-time failure: requeue
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent(env.EventType, "nack", d.Redelivered, time.Since(start))
		}
		if c.logger != nil {
			c.logger.Warn(consumerCtx, "Transient processing failure, requeuing message",
				logger.String("event_type", env.EventType),
				logger.String("aggregate_id", env.AggregateID.String()),
				logger.WithError(err),
			)
		}
		if nackErr := d.Nack(false, true); nackErr != nil {
			return fmt.Errorf("failed to nack message: %w (original error: %v)", nackErr, err)
		}
		return fmt.Errorf("processing failed (transient, requeued): %w", err)
	}

	if err := d.Ack(false); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent(env.EventType, "failure", d.Redelivered, time.Since(start))
		}
		return err
	}

	span.SetStatus(codes.Ok, "")
	if c.metrics != nil {
		c.metrics.RecordConsumerEvent(env.EventType, "ack", d.Redelivered, time.Since(start))
	}
	return nil
}
