package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/pkg/logger"
	"github.com/dvasconcellos/elo/api/pkg/metrics"
	appotel "github.com/dvasconcellos/elo/api/pkg/otel"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// AMQPChannel defines the publisher operations needed by the Outbox worker.
type AMQPChannel interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

const (
	defaultBatchSize    = 50
	defaultPollInterval = 200 * time.Millisecond
)

type OutboxOption func(*Outbox)

func WithMetrics(m metrics.Metrics) OutboxOption {
	return func(w *Outbox) {
		w.metrics = m
	}
}

func WithTracer(t trace.Tracer) OutboxOption {
	return func(w *Outbox) {
		w.tracer = t
	}
}

func WithBatchSize(size int) OutboxOption {
	return func(w *Outbox) {
		if size > 0 {
			w.batchSize = size
		}
	}
}

func WithPollInterval(d time.Duration) OutboxOption {
	return func(w *Outbox) {
		if d > 0 {
			w.pollInterval = d
		}
	}
}

type Outbox struct {
	db           *sql.DB
	queries      *db.Queries
	logger       logger.Logger
	channel      AMQPChannel
	metrics      metrics.Metrics
	tracer       trace.Tracer
	batchSize    int
	pollInterval time.Duration
}

func NewOutboxWorker(database *sql.DB, queries *db.Queries, l logger.Logger, ch AMQPChannel, opts ...OutboxOption) *Outbox {
	w := &Outbox{
		db:           database,
		queries:      queries,
		logger:       l,
		channel:      ch,
		batchSize:    defaultBatchSize,
		pollInterval: defaultPollInterval,
	}
	for _, opt := range opts {
		opt(w)
	}
	if w.tracer == nil {
		w.tracer = otel.GetTracerProvider().Tracer("outbox-worker")
	}
	return w
}

// Start initiates the loop that polls the outbox and publishes events with continuous backlog draining
func (w *Outbox) Start(ctx context.Context) {
	w.logger.Info(ctx, "Starting Outbox Worker", logger.Int("batch_size", w.batchSize))
	pollTimer := time.NewTimer(w.pollInterval)
	defer pollTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info(ctx, "Stopping Outbox Worker gracefully")
			return
		default:
		}

		processed, err := w.processBatch(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		if processed == w.batchSize {
			// Saturated batch: immediately proceed to drain remaining backlog without waiting
			continue
		}

		// Backlog is drained or empty: update backlog metrics and wait pollInterval
		w.updateBacklogMetrics(ctx)

		pollTimer.Reset(w.pollInterval)
		select {
		case <-ctx.Done():
			w.logger.Info(ctx, "Stopping Outbox Worker gracefully")
			return
		case <-pollTimer.C:
		}
	}
}

func (w *Outbox) processOutbox(ctx context.Context) {
	_, _ = w.processBatch(ctx)
}

func (w *Outbox) processBatch(ctx context.Context) (int, error) {
	if w.db == nil || w.queries == nil {
		return 0, nil
	}

	// Begin a transaction to use FOR UPDATE SKIP LOCKED
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		w.logger.Error(ctx, "Failed to begin transaction for outbox", logger.WithError(err))
		return 0, err
	}
	defer tx.Rollback()

	qtx := w.queries.WithTx(tx)

	// Fetch up to batchSize pending events
	events, err := qtx.GetPendingOutboxEvents(ctx, int32(w.batchSize))
	if err != nil {
		w.logger.Error(ctx, "Failed to fetch outbox events", logger.WithError(err))
		return 0, err
	}

	if len(events) == 0 {
		return 0, nil
	}

	processedCount := 0
	for _, event := range events {
		if ctx.Err() != nil {
			break
		}

		// Publish to RabbitMQ
		pubCtx, err := w.publishEvent(ctx, event)
		if err != nil {
			if w.metrics != nil {
				w.metrics.IncOutboxEventsProcessed("failure")
			}
			w.logger.Error(pubCtx, "Failed to publish event", logger.WithError(err), logger.String("event_id", event.ID.String()))
			// Stop batch to prevent sequence gaps on publish failure
			break
		}

		// Mark as processed
		err = qtx.MarkOutboxEventProcessed(ctx, event.ID)
		if err != nil {
			if w.metrics != nil {
				w.metrics.IncOutboxEventsProcessed("failure")
			}
			w.logger.Error(pubCtx, "Failed to mark event as processed", logger.WithError(err), logger.String("event_id", event.ID.String()))
			break
		}

		w.logger.Info(pubCtx, "Successfully processed outbox event", logger.String("event_id", event.ID.String()), logger.String("event_type", event.EventType))
		if w.metrics != nil {
			w.metrics.IncOutboxEventsProcessed("success")
		}
		processedCount++
	}

	if processedCount > 0 {
		if err := tx.Commit(); err != nil {
			w.logger.Error(ctx, "Failed to commit outbox transaction", logger.WithError(err))
			return 0, err
		}
	}

	return processedCount, nil
}

func (w *Outbox) updateBacklogMetrics(ctx context.Context) {
	if w.metrics == nil || w.db == nil {
		return
	}
	var count float64
	var oldestAge float64
	row := w.db.QueryRowContext(ctx, "SELECT count(*), coalesce(extract(epoch from (now() - min(created_at))), 0) FROM outbox_events WHERE status = 'PENDING'")
	if err := row.Scan(&count, &oldestAge); err == nil {
		w.metrics.SetOutboxPending(count, oldestAge)
	}
}

// streamEnvelope is the wire contract between the outbox relay and every
// consumer of logistics.events. stream_position is the PostgreSQL-assigned
// global position of the outbox row; per-aggregate causal order lives inside
// the payload (Seq).
type streamEnvelope struct {
	EventID        string          `json:"event_id"`
	AggregateID    string          `json:"aggregate_id"`
	StreamPosition int64           `json:"stream_position"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
}

func (w *Outbox) publishEvent(ctx context.Context, event db.OutboxEvent) (context.Context, error) {
	body, err := json.Marshal(streamEnvelope{
		EventID:        event.ID.String(),
		AggregateID:    event.AggregateID.String(),
		StreamPosition: event.StreamPosition.Int64,
		EventType:      event.EventType,
		Payload:        event.Payload,
	})
	if err != nil {
		return ctx, err
	}

	parentCtx := ctx
	if event.Traceparent.Valid && event.Traceparent.String != "" {
		extractedCtx := appotel.ContextFromTraceparent(context.Background(), event.Traceparent.String, event.Tracestate.String)
		remoteSpan := trace.SpanContextFromContext(extractedCtx)
		if remoteSpan.IsValid() {
			parentCtx = extractedCtx
		}
	}

	tr := w.tracer
	if tr == nil {
		tr = otel.GetTracerProvider().Tracer("outbox-worker")
	}

	spanOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", "logistics.events"),
			attribute.String("messaging.rabbitmq.routing_key", event.EventType),
			attribute.String("messaging.message.id", event.ID.String()),
			attribute.String("messaging.operation", "publish"),
		),
	}

	pubCtx, span := tr.Start(parentCtx, "logistics.events publish", spanOpts...)
	defer span.End()

	// Inject W3C traceparent into AMQP publishing headers
	headers := appotel.InjectAMQPHeaders(pubCtx, amqp.Table{})

	pubErr := w.channel.PublishWithContext(
		ctx,
		"logistics.events",
		event.EventType,
		false,
		false,
		amqp.Publishing{
			Headers:      headers,
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			MessageId:    event.ID.String(),
			Type:         event.EventType,
			Timestamp:    event.CreatedAt.Time,
		},
	)

	if pubErr != nil {
		span.RecordError(pubErr)
		span.SetStatus(codes.Error, pubErr.Error())
		if w.metrics != nil {
			w.metrics.IncOutboxEventsProcessed("failure")
		}
		return pubCtx, pubErr
	}

	span.SetStatus(codes.Ok, "")
	if w.metrics != nil {
		w.metrics.IncOutboxEventsProcessed("success")
	}
	return pubCtx, nil
}
