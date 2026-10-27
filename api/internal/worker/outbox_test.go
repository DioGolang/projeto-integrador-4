package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/postgres"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/pkg/logger"
	appotel "github.com/dvasconcellos/elo/api/pkg/otel"
)

func init() {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

type mockAMQPChannel struct {
	publishedExchange string
	publishedKey      string
	publishedMsg      amqp.Publishing
	publishErr        error
	publishCallCount  int
}

func (m *mockAMQPChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	m.publishCallCount++
	m.publishedExchange = exchange
	m.publishedKey = key
	m.publishedMsg = msg
	return m.publishErr
}

type mockMetrics struct {
	outboxSuccessCount int
	outboxFailureCount int
}

func (m *mockMetrics) RecordOrderCreated(status string)    {}
func (m *mockMetrics) RecordOrderDispatched(status string) {}
func (m *mockMetrics) RecordUseCaseExecution(useCaseName string, success bool, duration time.Duration) {
}
func (m *mockMetrics) ObserveHTTPRequestDuration(method, path, statusCode string, duration float64) {}
func (m *mockMetrics) ObserveGRPCRequestDuration(service, method, code string, duration float64)    {}
func (m *mockMetrics) IncCacheHit(cacheType string)                                                 {}
func (m *mockMetrics) IncCacheMiss(cacheType string)                                                {}
func (m *mockMetrics) IncOutboxEventsProcessed(status string) {
	if status == "success" {
		m.outboxSuccessCount++
	} else if status == "failure" {
		m.outboxFailureCount++
	}
}
func (m *mockMetrics) SetOutboxPending(count, oldestAgeSeconds float64) {}
func (m *mockMetrics) RecordConsumerEvent(eventType, status string, redelivered bool, duration time.Duration) {
}

func TestOutboxWorker_PublishEvent(t *testing.T) {
	nopLogger := logger.NewZapLogger("test-worker", false)

	t.Run("injects_W3C_traceparent_into_AMQP_headers_and_records_metric", func(t *testing.T) {
		mockCh := &mockAMQPChannel{}
		metricsSpy := &mockMetrics{}

		w := NewOutboxWorker(nil, nil, nopLogger, mockCh, WithMetrics(metricsSpy))

		origTraceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		eventID := uuid.New()
		aggID := uuid.New()
		rawPayload := json.RawMessage(`{"order_id":"123","status":"PENDING"}`)

		event := db.OutboxEvent{
			ID:             eventID,
			AggregateID:    aggID,
			AggregateType:  "order",
			EventType:      "OrderCreated",
			Payload:        rawPayload,
			StreamPosition: sql.NullInt64{Int64: 42, Valid: true},
			Traceparent:    sql.NullString{String: origTraceparent, Valid: true},
			CreatedAt:      sql.NullTime{Time: time.Now(), Valid: true},
		}

		ctx, err := w.publishEvent(context.Background(), event)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if mockCh.publishCallCount != 1 {
			t.Fatalf("expected 1 publish call, got %d", mockCh.publishCallCount)
		}
		if mockCh.publishedExchange != "logistics.events" {
			t.Errorf("expected exchange logistics.events, got %q", mockCh.publishedExchange)
		}
		if mockCh.publishedKey != "OrderCreated" {
			t.Errorf("expected routing key OrderCreated, got %q", mockCh.publishedKey)
		}

		// Verify W3C traceparent was injected into headers
		carrier := appotel.NewAMQPHeadersCarrier(mockCh.publishedMsg.Headers)
		injectedTP := carrier.Get("traceparent")
		if injectedTP == "" {
			t.Fatal("expected traceparent header in AMQP message, got none")
		}

		// Verify that the TraceID was preserved from the original transaction
		extractedCtx := appotel.ExtractAMQPHeaders(context.Background(), mockCh.publishedMsg.Headers)
		sc := trace.SpanContextFromContext(extractedCtx)
		if !sc.IsValid() {
			t.Fatal("expected valid span context from AMQP headers")
		}
		if sc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Errorf("expected trace ID 4bf92f3577b34da6a3ce929d0e0e4736, got %s", sc.TraceID().String())
		}

		// Verify success metric was incremented
		if metricsSpy.outboxSuccessCount != 1 {
			t.Errorf("expected 1 outbox success metric, got %d", metricsSpy.outboxSuccessCount)
		}

		// Verify returned context carries the active span
		returnedSC := trace.SpanContextFromContext(ctx)
		if !returnedSC.IsValid() {
			t.Errorf("expected returned context to carry valid span")
		}
	})

	t.Run("handles_publish_failure_and_increments_failure_metric", func(t *testing.T) {
		mockCh := &mockAMQPChannel{
			publishErr: errors.New("rabbit broker down"),
		}
		metricsSpy := &mockMetrics{}

		w := NewOutboxWorker(nil, nil, nopLogger, mockCh, WithMetrics(metricsSpy))

		event := db.OutboxEvent{
			ID:          uuid.New(),
			AggregateID: uuid.New(),
			EventType:   "OrderCreated",
			Payload:     json.RawMessage(`{}`),
		}

		_, err := w.publishEvent(context.Background(), event)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if metricsSpy.outboxFailureCount != 1 {
			t.Errorf("expected 1 outbox failure metric, got %d", metricsSpy.outboxFailureCount)
		}
	})

	t.Run("publishes_event_without_traceparent_generating_fresh_trace", func(t *testing.T) {
		mockCh := &mockAMQPChannel{}
		metricsSpy := &mockMetrics{}

		w := NewOutboxWorker(nil, nil, nopLogger, mockCh, WithMetrics(metricsSpy))

		event := db.OutboxEvent{
			ID:          uuid.New(),
			AggregateID: uuid.New(),
			EventType:   "DeliveryCreated",
			Payload:     json.RawMessage(`{"delivery_id":"abc"}`),
			Traceparent: sql.NullString{Valid: false},
		}

		ctx, err := w.publishEvent(context.Background(), event)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// A fresh span and trace should have been produced
		carrier := appotel.NewAMQPHeadersCarrier(mockCh.publishedMsg.Headers)
		if carrier.Get("traceparent") == "" {
			t.Error("expected traceparent header injected even when source had no prior trace")
		}

		if metricsSpy.outboxSuccessCount != 1 {
			t.Errorf("expected 1 success metric, got %d", metricsSpy.outboxSuccessCount)
		}

		if !trace.SpanContextFromContext(ctx).IsValid() {
			t.Errorf("expected valid span in returned context")
		}
	})
}

type mockAck struct{}

func (m *mockAck) Ack(tag uint64, multiple bool) error                { return nil }
func (m *mockAck) Nack(tag uint64, multiple bool, requeue bool) error { return nil }
func (m *mockAck) Reject(tag uint64, requeue bool) error              { return nil }

type mockFaultInjectingChannel struct {
	onPublish func(msg amqp.Publishing) error
}

func (m *mockFaultInjectingChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if m.onPublish != nil {
		return m.onPublish(msg)
	}
	return nil
}

func TestOutbox_AmbiguousPublisherConfirmRecoveryAndDeduplication(t *testing.T) {
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://root:password@127.0.0.1:5432/logistics?sslmode=disable"
	}
	dbConn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}
	defer dbConn.Close()

	ctx := context.Background()

	// Initialize test tables cleanly
	_, err = dbConn.ExecContext(ctx, `
		DROP TABLE IF EXISTS outbox_events;
		DROP TABLE IF EXISTS projection_checkpoints;
		DROP TABLE IF EXISTS delivery_read_model;
		CREATE TABLE delivery_read_model (
			delivery_id UUID PRIMARY KEY,
			driver_id TEXT NOT NULL,
			state TEXT NOT NULL,
			sequence BIGINT NOT NULL,
			pickup_lat DOUBLE PRECISION NOT NULL,
			pickup_lng DOUBLE PRECISION NOT NULL,
			dropoff_lat DOUBLE PRECISION NOT NULL,
			dropoff_lng DOUBLE PRECISION NOT NULL,
			active_route JSONB,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE projection_checkpoints (
			projection_name TEXT PRIMARY KEY,
			last_sequence BIGINT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE outbox_events (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			aggregate_id UUID NOT NULL,
			aggregate_type VARCHAR(50) NOT NULL,
			event_type VARCHAR(100) NOT NULL,
			payload JSONB NOT NULL,
			status VARCHAR(50) DEFAULT 'PENDING',
			created_at TIMESTAMPTZ DEFAULT now(),
			stream_position BIGSERIAL UNIQUE,
			traceparent TEXT,
			tracestate TEXT
		);
		ALTER SEQUENCE outbox_events_stream_position_seq RESTART WITH 1;
	`)
	if err != nil {
		t.Fatalf("failed to setup test schema: %v", err)
	}

	q := db.New(dbConn)
	nopLogger := logger.NewZapLogger("test-outbox-ambig", false)

	// Set up consumer side with a Spy LiveHub
	hub := stream.NewMemoryLiveHub()
	aggID := uuid.New()
	sub, unsub := hub.Subscribe(ctx, aggID)
	defer unsub()

	evStore := postgres.NewEventStore(q)
	proj := delivery.NewProjection(dbConn, "OutboxPublisherAmbiguityProjection")
	validator := stream.NewSequenceValidator()
	recovery := stream.NewGapRecovery(evStore, validator, proj, hub)
	strConsumer := stream.NewConsumer(validator, recovery, proj, hub)
	rabbitConsumer := rabbitmq.NewConsumer(nil, strConsumer)

	// Insert event into outbox_events with status 'PENDING'
	evPayload := delivery.EventDeliveryCreated{
		ID:         "ev-ambig-1",
		DeliveryID: aggID,
		DriverID:   "driver-1",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}
	payloadBytes, _ := json.Marshal(evPayload)
	var eventID uuid.UUID
	err = dbConn.QueryRowContext(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, status)
		VALUES ($1, 'delivery', 'DeliveryCreated', $2, 'PENDING')
		RETURNING id
	`, aggID, payloadBytes).Scan(&eventID)
	if err != nil {
		t.Fatalf("failed to insert outbox event: %v", err)
	}

	// Fault-injecting AMQP Channel
	var publishCount int
	var deliveredToConsumerCount int
	faultChan := &mockFaultInjectingChannel{
		onPublish: func(msg amqp.Publishing) error {
			publishCount++
			// Simulate broker accepting message into exchange/queue and dispatching to consumer:
			del := amqp.Delivery{
				Body:         msg.Body,
				Headers:      msg.Headers,
				Redelivered:  publishCount > 1,
				Acknowledger: &mockAck{},
			}
			deliveredToConsumerCount++
			if err := rabbitConsumer.HandleDelivery(ctx, del); err != nil {
				return err
			}

			// On attempt 1: Publisher confirm is lost / times out back to the publisher
			if publishCount == 1 {
				return errors.New("simulated publisher confirm timeout: connection dropped after queue ingress")
			}
			return nil
		},
	}

	worker := NewOutboxWorker(dbConn, q, nopLogger, faultChan)

	// =========================================================================
	// Iteration 1: Ambiguous publisher failure
	// (Broker accepted and delivered message to consumer, but confirm timed out to worker)
	// =========================================================================
	worker.processOutbox(ctx)

	// Assertions for Iteration 1:
	// 1. Publisher executed exactly 1 attempt
	if publishCount != 1 {
		t.Fatalf("expected 1 publish attempt, got %d", publishCount)
	}
	// 2. Message was accepted by broker and delivered to consumer
	if deliveredToConsumerCount != 1 {
		t.Fatalf("expected 1 delivery to consumer, got %d", deliveredToConsumerCount)
	}
	// 3. PostgreSQL outbox_events MUST STILL BE 'PENDING' because publishEvent returned an error
	var status1 string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status1)
	if err != nil || status1 != "PENDING" {
		t.Fatalf("expected outbox event to remain PENDING after confirm timeout, got %q (err: %v)", status1, err)
	}
	// 4. Exactly 1 live broadcast was emitted to subscribers on this first delivery
	select {
	case liveEv := <-sub:
		if liveEv.DomainEvent.Sequence() != 1 {
			t.Fatalf("expected sequence 1, got %d", liveEv.DomainEvent.Sequence())
		}
	default:
		t.Fatal("expected live event received on initial delivery")
	}
	// 5. Database projection is at sequence 1
	model1, err := q.GetDeliveryReadModel(ctx, aggID)
	if err != nil || model1.Sequence != 1 {
		t.Fatalf("expected sequence 1 in projection, got %d (err: %v)", model1.Sequence, err)
	}

	// =========================================================================
	// Iteration 2: Outbox relay retry
	// (Relay re-polls PENDING event and republishes it with success confirm)
	// =========================================================================
	worker.processOutbox(ctx)

	// Assertions for Iteration 2:
	// 1. Total publishes must now be 2
	if publishCount != 2 {
		t.Fatalf("expected 2 total publish attempts, got %d", publishCount)
	}
	// 2. Total deliveries to consumer must now be 2
	if deliveredToConsumerCount != 2 {
		t.Fatalf("expected 2 deliveries to consumer, got %d", deliveredToConsumerCount)
	}
	// 3. PostgreSQL outbox_events MUST NOW BE 'PROCESSED'
	var status2 string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status2)
	if err != nil || status2 != "PROCESSED" {
		t.Fatalf("expected outbox event to be PROCESSED after retry, got %q (err: %v)", status2, err)
	}
	// 4. CRITICAL: LiveHub must NOT have received a second duplicate broadcast!
	select {
	case dup := <-sub:
		t.Fatalf("IDEMPOTENCY VIOLATION: Duplicate broadcast received on LiveHub for seq %d", dup.DomainEvent.Sequence())
	default:
		// Passed! Zero duplicate broadcasts to external consumers
	}
	// 5. Database projection is still sequence 1 with exactly 1 row
	model2, err := q.GetDeliveryReadModel(ctx, aggID)
	if err != nil || model2.Sequence != 1 {
		t.Fatalf("expected sequence 1 in projection, got %d", model2.Sequence)
	}

	t.Log("Ambiguous publisher confirmation test passed: 2 messages dispatched, outbox transitioned PENDING -> PROCESSED, exactly 1 business side-effect realized.")
}

func TestOutboxWorker_ConfigurationAndGracefulShutdown(t *testing.T) {
	nopLogger := logger.NewZapLogger("test-worker-cfg", false)
	mockCh := &mockAMQPChannel{}

	w := NewOutboxWorker(nil, nil, nopLogger, mockCh,
		WithBatchSize(25),
		WithPollInterval(50*time.Millisecond),
	)

	if w.batchSize != 25 {
		t.Errorf("expected batch size 25, got %d", w.batchSize)
	}
	if w.pollInterval != 50*time.Millisecond {
		t.Errorf("expected poll interval 50ms, got %v", w.pollInterval)
	}

	// Verify graceful shutdown when context is cancelled
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		w.Start(ctx)
		close(done)
	}()

	// Allow loop to start and then cancel immediately
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Succeeded in shutting down cleanly
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not shut down gracefully within timeout")
	}
}

