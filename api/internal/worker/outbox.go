package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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

// Confirmation provides methods to await broker confirmation.
type Confirmation interface {
	WaitContext(ctx context.Context) (bool, error)
	Acked() bool
}

// AMQPChannel defines the publisher operations needed by the Outbox worker.
type AMQPChannel interface {
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (Confirmation, error)
	NotifyReturn(c chan amqp.Return) chan amqp.Return
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
}

// ChannelAdapter wraps a real *amqp.Channel to implement AMQPChannel.
type ChannelAdapter struct {
	*amqp.Channel
}

// WrapChannel wraps a concrete *amqp.Channel into an AMQPChannel.
func WrapChannel(ch *amqp.Channel) AMQPChannel {
	if ch == nil {
		return nil
	}
	return &ChannelAdapter{Channel: ch}
}

// PublishWithDeferredConfirmWithContext delegates to the underlying *amqp.Channel.
func (a *ChannelAdapter) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (Confirmation, error) {
	dc, err := a.Channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		return nil, err
	}
	if dc == nil {
		return nil, nil
	}
	return dc, nil
}

const (
	defaultBatchSize      = 50
	defaultPollInterval   = 200 * time.Millisecond
	defaultConfirmTimeout = 2 * time.Second
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

func WithConfirmTimeout(d time.Duration) OutboxOption {
	return func(w *Outbox) {
		if d > 0 {
			w.confirmTimeout = d
		}
	}
}

type Outbox struct {
	db             *sql.DB
	queries        *db.Queries
	logger         logger.Logger
	channel        AMQPChannel
	metrics        metrics.Metrics
	tracer         trace.Tracer
	batchSize      int
	pollInterval   time.Duration
	confirmTimeout time.Duration

	returnChan chan amqp.Return
	closeChan  chan *amqp.Error
}

func NewOutboxWorker(database *sql.DB, queries *db.Queries, l logger.Logger, ch AMQPChannel, opts ...OutboxOption) *Outbox {
	timeout := defaultConfirmTimeout
	if envTimeout := os.Getenv("OUTBOX_CONFIRM_TIMEOUT"); envTimeout != "" {
		if d, err := time.ParseDuration(envTimeout); err == nil && d > 0 {
			timeout = d
		}
	}

	w := &Outbox{
		db:             database,
		queries:        queries,
		logger:         l,
		channel:        ch,
		batchSize:      defaultBatchSize,
		pollInterval:   defaultPollInterval,
		confirmTimeout: timeout,
	}
	for _, opt := range opts {
		opt(w)
	}
	if w.tracer == nil {
		w.tracer = otel.GetTracerProvider().Tracer("outbox-worker")
	}

	if w.channel != nil {
		bufSize := 1000
		if w.batchSize*4 > bufSize {
			bufSize = w.batchSize * 4
		}
		w.returnChan = make(chan amqp.Return, bufSize)
		w.channel.NotifyReturn(w.returnChan)

		w.closeChan = make(chan *amqp.Error, 2)
		w.channel.NotifyClose(w.closeChan)
	}

	return w
}

// Start initiates the loop that polls the outbox and publishes events with continuous backlog draining
func (w *Outbox) Start(ctx context.Context) {
	w.logger.Info(ctx, "Starting Outbox Worker", logger.Int("batch_size", w.batchSize), logger.Any("confirm_timeout", w.confirmTimeout))
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

func (w *Outbox) drainReturns(returned map[string]amqp.Return) {
	if w.returnChan == nil {
		return
	}
	for {
		select {
		case ret, ok := <-w.returnChan:
			if !ok {
				return
			}
			if ret.MessageId != "" {
				returned[ret.MessageId] = ret
			}
		default:
			return
		}
	}
}

func (w *Outbox) processBatch(ctx context.Context) (int, error) {
	if w.db == nil || w.queries == nil || w.channel == nil {
		return 0, nil
	}

	// Check if AMQP channel was already closed
	select {
	case err, ok := <-w.closeChan:
		if !ok {
			w.logger.Error(ctx, "AMQP channel closed, skipping outbox batch")
			return 0, errors.New("amqp channel closed")
		}
		w.logger.Error(ctx, "AMQP channel closed with error, skipping outbox batch", logger.Any("amqp_error", err))
		return 0, fmt.Errorf("amqp channel closed: %w", err)
	default:
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

	returnedMap := make(map[string]amqp.Return)
	w.drainReturns(returnedMap)

	processedCount := 0
	for _, event := range events {
		if ctx.Err() != nil {
			w.logger.Warn(ctx, "Context cancelled during outbox batch, stopping batch", logger.WithError(ctx.Err()))
			break
		}

		select {
		case closeErr, ok := <-w.closeChan:
			w.logger.Error(ctx, "AMQP channel closed during outbox batch", logger.Any("close_err", closeErr), logger.Any("ok", ok))
			goto commitPhase
		default:
		}

		pubCtx, pubErr := w.publishAndConfirmEvent(ctx, event, returnedMap)
		if pubErr != nil {
			if w.metrics != nil {
				w.metrics.IncOutboxEventsProcessed("failure")
			}
			w.logger.Warn(pubCtx, "Outbox event not positively confirmed or returned by broker; leaving event PENDING",
				logger.WithError(pubErr),
				logger.String("event_id", event.ID.String()),
				logger.String("event_type", event.EventType))
			// Stop batch to prevent sequence gaps on publish failure
			break
		}

		// Mark as processed in PostgreSQL
		err = qtx.MarkOutboxEventProcessed(ctx, event.ID)
		if err != nil {
			if w.metrics != nil {
				w.metrics.IncOutboxEventsProcessed("failure")
			}
			w.logger.Error(pubCtx, "Failed to mark event as processed in DB",
				logger.WithError(err),
				logger.String("event_id", event.ID.String()))
			break
		}

		w.logger.Info(pubCtx, "Successfully confirmed and processed outbox event",
			logger.String("event_id", event.ID.String()),
			logger.String("event_type", event.EventType),
			logger.Any("stream_position", event.StreamPosition.Int64))
		if w.metrics != nil {
			w.metrics.IncOutboxEventsProcessed("success")
		}
		processedCount++
	}

commitPhase:
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
	OutboxID       string          `json:"outbox_id,omitempty"`
	AggregateID    string          `json:"aggregate_id"`
	StreamPosition int64           `json:"stream_position"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
}

func (w *Outbox) publishAndConfirmEvent(ctx context.Context, event db.OutboxEvent, returnedMap map[string]amqp.Return) (context.Context, error) {
	var idExtractor struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(event.Payload, &idExtractor)
	eventID := idExtractor.ID
	if eventID == "" {
		eventID = event.ID.String()
	}

	body, err := json.Marshal(streamEnvelope{
		EventID:        eventID,
		OutboxID:       event.ID.String(),
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

	conf, pubErr := w.channel.PublishWithDeferredConfirmWithContext(
		pubCtx,
		"logistics.events",
		event.EventType,
		true,  // mandatory: true ensures broker returns unroutable messages
		false, // immediate: false
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
		return pubCtx, pubErr
	}

	if conf == nil {
		err := errors.New("channel not in confirm mode: DeferredConfirmation is nil")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return pubCtx, err
	}

	// Await broker confirmation with timeout
	confirmCtx, cancelConfirm := context.WithTimeout(pubCtx, w.confirmTimeout)
	acked, waitErr := conf.WaitContext(confirmCtx)
	cancelConfirm()

	// Draining returnChan immediately after WaitContext:
	// In amqp091-go, basic.return is dispatched to return listeners before basic.ack unblocks WaitContext.
	// Therefore, any return frame for this publishing is guaranteed to be in returnChan.
	w.drainReturns(returnedMap)

	if waitErr != nil {
		span.RecordError(waitErr)
		span.SetStatus(codes.Error, waitErr.Error())
		return pubCtx, fmt.Errorf("publisher confirmation wait: %w", waitErr)
	}

	if !acked {
		nackErr := errors.New("broker returned negative acknowledgment (NACK)")
		span.RecordError(nackErr)
		span.SetStatus(codes.Error, nackErr.Error())
		return pubCtx, nackErr
	}

	// Verify message was not returned by broker
	if ret, isReturned := returnedMap[event.ID.String()]; isReturned {
		retErr := fmt.Errorf("message returned by broker as unroutable (code %d: %s)", ret.ReplyCode, ret.ReplyText)
		span.RecordError(retErr)
		span.SetStatus(codes.Error, retErr.Error())
		return pubCtx, retErr
	}

	span.SetStatus(codes.Ok, "")
	return pubCtx, nil
}

// publishEvent publishes and confirms a single event (used for direct calls and testing).
func (w *Outbox) publishEvent(ctx context.Context, event db.OutboxEvent) (context.Context, error) {
	returnedMap := make(map[string]amqp.Return)
	w.drainReturns(returnedMap)
	pubCtx, err := w.publishAndConfirmEvent(ctx, event, returnedMap)
	if err == nil && w.metrics != nil {
		w.metrics.IncOutboxEventsProcessed("success")
	} else if err != nil && w.metrics != nil {
		w.metrics.IncOutboxEventsProcessed("failure")
	}
	return pubCtx, err
}
