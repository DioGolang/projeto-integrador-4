package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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

func WithConsumerBackoff(b *BackoffCalculator) ConsumerOption {
	return func(c *Consumer) {
		c.backoff = b
	}
}

func WithConsumerSleeper(s Sleeper) ConsumerOption {
	return func(c *Consumer) {
		c.sleeper = s
	}
}

func WithConsumerTag(tag string) ConsumerOption {
	return func(c *Consumer) {
		c.consumerTag = tag
	}
}

func WithConsumerDrainTimeout(d time.Duration) ConsumerOption {
	return func(c *Consumer) {
		c.drainTimeout = d
	}
}

type Consumer struct {
	ch            *amqp.Channel
	target        StreamProcessor
	metrics       metrics.Metrics
	tracer        trace.Tracer
	logger        logger.Logger
	backoff       *BackoffCalculator
	sleeper       Sleeper
	mu            sync.RWMutex
	suspended     bool
	suspendReason string

	// Gate 1.5B-4D: Coordinated Shutdown fields
	consumerTag          string
	drainTimeout         time.Duration
	stopCh               chan struct{}
	notifyCloseCh        chan *amqp.Error
	channelErr           error
	started              bool
	stopping             bool
	stopped              bool
	inFlightWg           sync.WaitGroup
	runnerWg             sync.WaitGroup
	inFlightMu           sync.Mutex
	activeInFlightCancel context.CancelFunc
}

func (c *Consumer) IsSuspended() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.suspended
}

func (c *Consumer) Suspend(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.suspended = true
	c.suspendReason = reason
}

func (c *Consumer) NotifyClose() <-chan *amqp.Error {
	return c.notifyCloseCh
}

func (c *Consumer) ChannelError() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.channelErr
}

func (c *Consumer) IsStopping() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stopping
}

func (c *Consumer) DrainTimeout() time.Duration {
	return c.drainTimeout
}

func NewConsumer(ch *amqp.Channel, target StreamProcessor, opts ...ConsumerOption) *Consumer {
	c := &Consumer{
		ch:            ch,
		target:        target,
		drainTimeout:  5 * time.Second,
		stopCh:        make(chan struct{}),
		notifyCloseCh: make(chan *amqp.Error, 1),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.tracer == nil {
		c.tracer = otel.GetTracerProvider().Tracer("rabbitmq-consumer")
	}
	if c.backoff == nil {
		c.backoff = NewBackoffCalculator(100*time.Millisecond, 5*time.Second, nil)
	}
	if c.sleeper == nil {
		c.sleeper = DefaultSleeper
	}
	if c.ch != nil {
		c.ch.NotifyClose(c.notifyCloseCh)
	}
	return c
}

// Start initiates consumption of deliveries in a managed background goroutine.
// It uses consumeCtx to control the subscription lifetime.
func (c *Consumer) Start(consumeCtx context.Context, deliveries <-chan amqp.Delivery) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("consumer already started")
	}
	if c.stopped || c.stopping {
		c.mu.Unlock()
		return errors.New("consumer is stopped or stopping")
	}
	c.started = true
	c.mu.Unlock()

	c.runnerWg.Add(1)
	go c.runConsumeLoop(consumeCtx, deliveries)
	return nil
}

func (c *Consumer) runConsumeLoop(consumeCtx context.Context, deliveries <-chan amqp.Delivery) {
	defer c.runnerWg.Done()

	for {
		select {
		case <-consumeCtx.Done():
			if c.logger != nil {
				c.logger.Info(context.Background(), "Consumer subscription context canceled, exiting consume loop")
			}
			return

		case err, ok := <-c.notifyCloseCh:
			if ok && err != nil {
				if c.logger != nil {
					c.logger.Error(context.Background(), "RabbitMQ channel closed unexpectedly by broker", logger.WithError(err))
				}
				c.mu.Lock()
				c.channelErr = err
				c.mu.Unlock()
				return
			}

		case d, ok := <-deliveries:
			if !ok {
				if c.logger != nil {
					c.logger.Info(context.Background(), "RabbitMQ deliveries channel closed")
				}
				return
			}

			// Previne race condition entre Add e Wait: protegida pelo mutex de estado
			c.mu.Lock()
			if c.stopping {
				c.mu.Unlock()
				// Já em shutdown; não aceitar nova entrega. A mensagem permanece unacknowledged no broker.
				return
			}
			c.inFlightWg.Add(1)
			c.mu.Unlock()

			c.processDeliveryInFlight(d)
		}
	}
}

func (c *Consumer) processDeliveryInFlight(d amqp.Delivery) {
	defer c.inFlightWg.Done()

	// Modelo de Contextos (Gate 1.5B-4D): inFlightCtx é independente de consumeCtx / rootSignalCtx.
	// Criado especificamente para esta entrega, permitindo que a operação em andamento
	// conclua dentro do prazo de drenagem (drainTimeout), mesmo após o sinal de shutdown.
	inFlightCtx, inFlightCancel := context.WithCancel(context.Background())

	c.inFlightMu.Lock()
	c.activeInFlightCancel = inFlightCancel
	c.inFlightMu.Unlock()

	defer func() {
		c.inFlightMu.Lock()
		c.activeInFlightCancel = nil
		c.inFlightMu.Unlock()
		inFlightCancel()
	}()

	if err := c.HandleDelivery(inFlightCtx, d); err != nil {
		if c.logger != nil {
			c.logger.Error(inFlightCtx, "Failed to handle delivery in-flight", logger.WithError(err))
		}
	}
}

// Stop coordinates the graceful shutdown sequence:
// 1. Closes stopCh to immediately interrupt any retry backoff sleep.
// 2. Cancels the AMQP subscription (Channel.Cancel) so broker stops sending new deliveries.
// 3. Waits for in-flight processing to complete up to drainCtx.
// 4. If drainCtx expires, cancels the in-flight context to abort SQL transaction.
// 5. Closes the AMQP channel, preserving any unacknowledged deliveries.
// 6. Waits for consumer goroutines to terminate.
func (c *Consumer) Stop(drainCtx context.Context) error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	if !c.stopping {
		c.stopping = true
		close(c.stopCh) // Interrompe imediatamente qualquer sleeper de backoff ativo
	}
	c.mu.Unlock()

	// 1. Cancelar assinatura AMQP no broker para impedir novas entregas
	if c.consumerTag != "" && c.ch != nil && !c.ch.IsClosed() {
		_ = c.ch.Cancel(c.consumerTag, false)
	}

	// 2. Aguardar conclusão do processamento em andamento até o drainCtx expirar
	inFlightDone := make(chan struct{})
	go func() {
		c.inFlightWg.Wait()
		close(inFlightDone)
	}()

	var drainTimeoutOccurred bool
	select {
	case <-inFlightDone:
		// Processamento em andamento concluiu com sucesso dentro do prazo de drenagem
	case <-drainCtx.Done():
		drainTimeoutOccurred = true
		if c.logger != nil {
			c.logger.Warn(context.Background(), "Drain timeout expired; canceling in-flight operation context")
		}
		c.inFlightMu.Lock()
		if c.activeInFlightCancel != nil {
			c.activeInFlightCancel()
		}
		c.inFlightMu.Unlock()

		// Aguardar a finalização da goroutine in-flight após o cancelamento
		<-inFlightDone
	}

	// 3. Fechar canal AMQP, devolvendo entregas não reconhecidas ao broker
	if c.ch != nil && !c.ch.IsClosed() {
		_ = c.ch.Close()
	}

	// 4. Aguardar encerramento da goroutine do loop de consumo
	c.runnerWg.Wait()

	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()

	if drainTimeoutOccurred {
		return fmt.Errorf("shutdown completed after drain timeout expiration: in-flight processing canceled")
	}
	return nil
}

var ErrPoisonMessage = errors.New("poison message: envelope/payload validation failed")

type envelope struct {
	EventID        string          `json:"event_id"`
	OutboxID       string          `json:"outbox_id,omitempty"`
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

	// Validate stream_position
	if env.StreamPosition <= 0 {
		err := fmt.Errorf("%w: invalid stream_position %d", ErrPoisonMessage, env.StreamPosition)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
		}
		if c.logger != nil {
			c.logger.Error(consumerCtx, "Poison message: invalid stream_position rejected to DLQ",
				logger.Any("stream_position", env.StreamPosition),
				logger.WithError(err),
			)
		}
		d.Reject(false)
		return err
	}

	// Validate aggregate_id
	if env.AggregateID == uuid.Nil {
		err := fmt.Errorf("%w: aggregate_id is nil", ErrPoisonMessage)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
		}
		if c.logger != nil {
			c.logger.Error(consumerCtx, "Poison message: nil aggregate_id rejected to DLQ", logger.WithError(err))
		}
		d.Reject(false)
		return err
	}

	span.SetAttributes(
		attribute.String("messaging.message.id", env.AggregateID.String()),
		attribute.String("messaging.rabbitmq.routing_key", env.EventType),
		attribute.String("app.event_type", env.EventType),
		attribute.Int64("app.stream_position", env.StreamPosition),
	)

	// Check if payload contains an explicit event_type/type that disagrees with envelope
	var rawMeta struct {
		EventType string `json:"event_type"`
		Type      string `json:"type"`
	}
	if err := json.Unmarshal(env.Payload, &rawMeta); err == nil {
		payloadType := rawMeta.EventType
		if payloadType == "" {
			payloadType = rawMeta.Type
		}
		if payloadType != "" && payloadType != env.EventType {
			err := fmt.Errorf("%w: envelope event_type '%s' != payload event_type '%s'", ErrPoisonMessage, env.EventType, payloadType)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Error(consumerCtx, "Poison message: event_type mismatch rejected to DLQ", logger.WithError(err))
			}
			d.Reject(false)
			return err
		}
	}

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
		err := fmt.Errorf("%w: unknown or incompatible event_type '%s'", ErrPoisonMessage, env.EventType)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
		}
		if c.logger != nil {
			c.logger.Error(consumerCtx, "Poison message: unknown event_type rejected to DLQ",
				logger.String("event_type", env.EventType),
				logger.String("aggregate_id", env.AggregateID.String()),
				logger.WithError(err),
			)
		}
		d.Reject(false)
		return err
	}

	// 4. Validate mandatory consistency between envelope and domain payload
	if env.EventType != ev.EventName() {
		err := fmt.Errorf("%w: envelope event_type '%s' != payload event_type '%s'", ErrPoisonMessage, env.EventType, ev.EventName())
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
		}
		if c.logger != nil {
			c.logger.Error(consumerCtx, "Poison message: event_type mismatch rejected to DLQ", logger.WithError(err))
		}
		d.Reject(false)
		return err
	}

	if env.AggregateID != ev.AggregateID() {
		err := fmt.Errorf("%w: envelope aggregate_id '%s' != payload aggregate_id '%s'", ErrPoisonMessage, env.AggregateID, ev.AggregateID())
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
		}
		if c.logger != nil {
			c.logger.Error(consumerCtx, "Poison message: aggregate_id mismatch rejected to DLQ", logger.WithError(err))
		}
		d.Reject(false)
		return err
	}

	if env.EventID != "" && ev.EventID() != "" && env.EventID != ev.EventID() {
		err := fmt.Errorf("%w: envelope event_id '%s' != payload event_id '%s'", ErrPoisonMessage, env.EventID, ev.EventID())
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if c.metrics != nil {
			c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
		}
		if c.logger != nil {
			c.logger.Error(consumerCtx, "Poison message: event_id mismatch rejected to DLQ", logger.WithError(err))
		}
		d.Reject(false)
		return err
	}

	streamEv := stream.Event{
		StreamPosition: env.StreamPosition,
		DomainEvent:    ev,
	}

	attempt := 0
	for {
		c.mu.RLock()
		if c.suspended {
			reason := c.suspendReason
			c.mu.RUnlock()
			return fmt.Errorf("consumer is suspended due to previous unknown error (%s); message processing halted", reason)
		}
		c.mu.RUnlock()

		select {
		case <-c.stopCh:
			return fmt.Errorf("message processing interrupted by shutdown, preserving delivery unacknowledged")
		case <-consumerCtx.Done():
			return fmt.Errorf("message processing interrupted by context cancellation: %w", consumerCtx.Err())
		default:
		}

		err := c.target.Process(consumerCtx, streamEv)
		if err == nil {
			if ctxErr := consumerCtx.Err(); ctxErr != nil {
				return fmt.Errorf("processing finished but context already canceled: %w", ctxErr)
			}
			if ackErr := d.Ack(false); ackErr != nil {
				span.RecordError(ackErr)
				span.SetStatus(codes.Error, ackErr.Error())
				if c.metrics != nil {
					c.metrics.RecordConsumerEvent(env.EventType, "failure", d.Redelivered, time.Since(start))
				}
				return fmt.Errorf("failed to ack message: %w", ackErr)
			}
			span.SetStatus(codes.Ok, "")
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "ack", d.Redelivered, time.Since(start))
			}
			return nil
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		classification := ClassifyError(err, consumerCtx)
		span.SetAttributes(
			attribute.String("app.error_category", string(classification.Category)),
			attribute.String("app.error_reason", classification.Reason),
			attribute.Int("app.retry_attempt", attempt),
		)

		switch classification.Category {
		case CategoryPermanent:
			// Permanent failure: route to DLQ
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "dlq", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Error(consumerCtx, "Routing permanent failure to DLQ",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.Any("stream_position", env.StreamPosition),
					logger.String("category", string(classification.Category)),
					logger.String("reason", classification.Reason),
					logger.WithError(err),
				)
			}
			if rejectErr := d.Reject(false); rejectErr != nil {
				return fmt.Errorf("failed to reject permanent message to DLQ: %w (original error: %v)", rejectErr, err)
			}
			return fmt.Errorf("message permanent error, rejected to DLQ: %w", err)

		case CategoryTransient:
			// Se o shutdown já foi iniciado, aborta o retry imediatamente sem dormir
			select {
			case <-c.stopCh:
				return fmt.Errorf("retry aborted: shutdown in progress, preserving delivery unacknowledged (last error: %w)", err)
			case <-consumerCtx.Done():
				return fmt.Errorf("retry aborted by context cancellation, preserving delivery unacknowledged (last error: %w)", consumerCtx.Err())
			default:
			}

			// Transient operational failure: in-place retry with exponential backoff and full jitter!
			delay := c.backoff.CalculateDelay(attempt)
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "retry", d.Redelivered, delay)
			}
			if c.logger != nil {
				c.logger.Warn(consumerCtx, "Transient operational failure, executing in-place retry with backoff",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.Any("stream_position", env.StreamPosition),
					logger.Int("attempt", attempt),
					logger.String("delay", delay.String()),
					logger.String("reason", classification.Reason),
					logger.WithError(err),
				)
			}

			// Pausa respeitando cancelamento de contexto e interrupção imediata por shutdown
			sleepCtx, cancelSleep := context.WithCancel(consumerCtx)
			stopWatcherDone := make(chan struct{})
			go func() {
				select {
				case <-c.stopCh:
					cancelSleep()
				case <-sleepCtx.Done():
				case <-stopWatcherDone:
				}
			}()

			sleepErr := c.sleeper(sleepCtx, delay)
			close(stopWatcherDone)
			cancelSleep()

			if sleepErr != nil {
				if c.logger != nil {
					c.logger.Warn(consumerCtx, "Retry backoff interrupted by shutdown or context cancellation, preserving delivery unacknowledged",
						logger.WithError(sleepErr),
					)
				}
				// NÃO dar Ack, NÃO dar Reject. Preservar mensagem sem reconhecimento para reentrega pelo broker.
				return fmt.Errorf("retry backoff interrupted by shutdown or context cancellation: %w (last error: %v)", sleepErr, err)
			}

			attempt++
			// Continua o loop para retentar target.Process com a mesma entrega em-place

		case CategoryContextCanceled:
			// Context canceled / shutdown: preservar mensagem sem ACK
			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "context_canceled", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Warn(consumerCtx, "Processing interrupted by context cancellation, preserving delivery unacknowledged without discard",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.Any("stream_position", env.StreamPosition),
					logger.String("category", string(classification.Category)),
					logger.String("reason", classification.Reason),
					logger.WithError(err),
				)
			}
			// Não executar Ack, não descartar. O fechamento do canal permitirá sua reentrega natural pelo broker.
			return fmt.Errorf("processing interrupted by context cancellation: %w", err)

		default: // CategoryUnknown
			// Erro desconhecido: suspender novas entregas, preservar mensagem sem ACK e gerar alerta operacional
			c.mu.Lock()
			c.suspended = true
			c.suspendReason = err.Error()
			c.mu.Unlock()

			if c.metrics != nil {
				c.metrics.RecordConsumerEvent(env.EventType, "suspended", d.Redelivered, time.Since(start))
			}
			if c.logger != nil {
				c.logger.Error(consumerCtx, "CRITICAL: Consumer suspended due to unknown error; delivery preserved unacknowledged",
					logger.String("event_type", env.EventType),
					logger.String("aggregate_id", env.AggregateID.String()),
					logger.Any("stream_position", env.StreamPosition),
					logger.String("category", string(classification.Category)),
					logger.String("reason", classification.Reason),
					logger.WithError(err),
				)
			}
			// NÃO executar Nack imediato, NÃO enviar para DLQ.
			// Com QoS 1 e mensagem unacked, o broker não entregará novas mensagens neste canal.
			return fmt.Errorf("consumer suspended due to unknown unclassified error: %w", err)
		}
	}
}
