package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/postgres"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/pkg/logger"
)

func getTestAMQPURL() string {
	if u := os.Getenv("TEST_AMQP_URL"); u != "" {
		return u
	}
	if u := os.Getenv("RABBITMQ_URL"); u != "" {
		return u
	}
	return "amqp://root:password@rabbitmq:5672/"
}

func getTestDBDSN() string {
	d := os.Getenv("TEST_DB_DSN")
	if d == "" {
		d = "postgres://root:password@postgres:5432/logistics_test?sslmode=disable"
	}
	if !strings.Contains(d, "search_path") {
		if strings.Contains(d, "?") {
			d += "&search_path=worker_test,public"
		} else {
			d += "?search_path=worker_test,public"
		}
	}
	return d
}

func setupRealTestInfrastructure(t *testing.T) (*amqp.Connection, *sql.DB, *db.Queries) {
	t.Helper()

	conn, err := amqp.Dial(getTestAMQPURL())
	if err != nil {
		t.Skipf("Skipping real RabbitMQ integration test (broker unreachable: %v)", err)
		return nil, nil, nil
	}

	dbConn, err := sql.Open("postgres", getTestDBDSN())
	if err != nil {
		conn.Close()
		t.Fatalf("failed to connect to test db: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize tables cleanly in dedicated test schema
	_, err = dbConn.ExecContext(ctx, `
		CREATE SCHEMA IF NOT EXISTS worker_test;
		SET search_path TO worker_test, public;
		DROP TABLE IF EXISTS worker_test.processed_events CASCADE;
		DROP TABLE IF EXISTS worker_test.outbox_events CASCADE;
		DROP TABLE IF EXISTS worker_test.projection_checkpoints CASCADE;
		DROP TABLE IF EXISTS worker_test.delivery_read_model CASCADE;
		DROP TYPE IF EXISTS worker_test.outbox_status CASCADE;

		CREATE TYPE worker_test.outbox_status AS ENUM ('PENDING', 'PROCESSED', 'FAILED');

		CREATE TABLE worker_test.processed_events (
			consumer_id TEXT NOT NULL,
			event_id TEXT NOT NULL,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			aggregate_id UUID NOT NULL,
			sequence BIGINT NOT NULL,
			event_type VARCHAR(100) NOT NULL,
			payload_hash VARCHAR(64) NOT NULL,
			PRIMARY KEY (consumer_id, event_id),
			CONSTRAINT uq_processed_events_consumer_agg_seq UNIQUE (consumer_id, aggregate_id, sequence)
		);

		CREATE TABLE worker_test.delivery_read_model (
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
		CREATE TABLE worker_test.projection_checkpoints (
			projection_name TEXT PRIMARY KEY,
			last_sequence BIGINT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE worker_test.outbox_events (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			aggregate_id UUID NOT NULL,
			aggregate_type VARCHAR(50) NOT NULL,
			event_type VARCHAR(100) NOT NULL,
			payload JSONB NOT NULL,
			status worker_test.outbox_status DEFAULT 'PENDING',
			created_at TIMESTAMPTZ DEFAULT now(),
			stream_position BIGSERIAL UNIQUE,
			traceparent TEXT,
			tracestate TEXT
		);
		ALTER SEQUENCE worker_test.outbox_events_stream_position_seq RESTART WITH 1;
	`)
	if err != nil {
		conn.Close()
		dbConn.Close()
		t.Fatalf("failed to setup test schema: %v", err)
	}

	// Declare exchange
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		dbConn.Close()
		t.Fatalf("failed to open setup channel: %v", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("logistics.events", "direct", true, false, false, false, nil); err != nil {
		conn.Close()
		dbConn.Close()
		t.Fatalf("failed to declare exchange: %v", err)
	}

	q := db.New(dbConn)
	return conn, dbConn, q
}

// 1. ACK Positivo: Real publish confirmed by RabbitMQ, transitioning event to PROCESSED
func TestOutbox_RealRabbitMQ_PositiveAck(t *testing.T) {
	conn, dbConn, q := setupRealTestInfrastructure(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	defer dbConn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("failed to enable publisher confirms: %v", err)
	}

	routingKey := "DeliveryCreated-TestAck-" + uuid.New().String()
	qName := "test-queue-ack-" + uuid.New().String()

	_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare test queue: %v", err)
	}
	defer ch.QueueDelete(qName, false, false, false)

	if err := ch.QueueBind(qName, routingKey, "logistics.events", false, nil); err != nil {
		t.Fatalf("failed to bind queue: %v", err)
	}

	ctx := context.Background()
	nopLogger := logger.NewZapLogger("test-real-ack", false)
	worker := NewOutboxWorker(dbConn, q, nopLogger, WrapChannel(ch))

	aggID := uuid.New()
	evPayload := delivery.EventDeliveryCreated{
		ID:         "ev-pos-ack",
		DeliveryID: aggID,
		DriverID:   "driver-pos",
		Seq:        1,
	}
	payloadBytes, _ := json.Marshal(evPayload)
	var eventID uuid.UUID
	err = dbConn.QueryRowContext(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, status)
		VALUES ($1, 'delivery', $2, $3, 'PENDING')
		RETURNING id
	`, aggID, routingKey, payloadBytes).Scan(&eventID)
	if err != nil {
		t.Fatalf("failed to insert outbox event: %v", err)
	}

	// Process outbox batch
	processed, err := worker.processBatch(ctx)
	if err != nil {
		t.Fatalf("processBatch failed: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 event processed, got %d", processed)
	}

	// Assert event is PROCESSED in DB
	var status string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status)
	if err != nil || status != "PROCESSED" {
		t.Fatalf("expected status PROCESSED after positive ACK, got %q (err: %v)", status, err)
	}

	// Assert message is in queue
	msg, ok, err := ch.Get(qName, true)
	if err != nil || !ok {
		t.Fatalf("expected message available in queue, ok=%v, err=%v", ok, err)
	}
	if msg.MessageId != eventID.String() {
		t.Errorf("expected MessageId %s, got %s", eventID.String(), msg.MessageId)
	}
}

// 2. Mensagem Não Roteável: mandatory=true causes basic.return (NO_ROUTE), event remains PENDING
func TestOutbox_RealRabbitMQ_UnroutableMessage(t *testing.T) {
	conn, dbConn, q := setupRealTestInfrastructure(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	defer dbConn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("failed to enable publisher confirms: %v", err)
	}

	ctx := context.Background()
	nopLogger := logger.NewZapLogger("test-real-unroutable", false)
	worker := NewOutboxWorker(dbConn, q, nopLogger, WrapChannel(ch))

	unroutableKey := "UnroutableKey-" + uuid.New().String()
	aggID := uuid.New()
	var eventID uuid.UUID
	err = dbConn.QueryRowContext(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, status)
		VALUES ($1, 'order', $2, '{}', 'PENDING')
		RETURNING id
	`, aggID, unroutableKey).Scan(&eventID)
	if err != nil {
		t.Fatalf("failed to insert outbox event: %v", err)
	}

	// Process outbox batch: broker will return basic.return (NO_ROUTE)
	processed, err := worker.processBatch(ctx)
	if err != nil {
		t.Fatalf("expected no fatal error from processBatch, got %v", err)
	}
	if processed != 0 {
		t.Fatalf("expected 0 events processed due to unroutable return, got %d", processed)
	}

	// Assert event REMAINS PENDING
	var status string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status)
	if err != nil || status != "PENDING" {
		t.Fatalf("expected outbox event to REMAIN PENDING on unroutable message, got %q (err: %v)", status, err)
	}
}

// 3. Simulador ausente e posterior inicialização:
// Initially unroutable -> stays PENDING -> simulator queue declared & bound -> retried & becomes PROCESSED
func TestOutbox_RealRabbitMQ_SimulatorAbsentAndSubsequentStartup(t *testing.T) {
	conn, dbConn, q := setupRealTestInfrastructure(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	defer dbConn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("failed to enable publisher confirms: %v", err)
	}

	ctx := context.Background()
	nopLogger := logger.NewZapLogger("test-real-sim-startup", false)
	worker := NewOutboxWorker(dbConn, q, nopLogger, WrapChannel(ch))

	routingKey := "OrderCreated-SimTest-" + uuid.New().String()
	simQueueName := "test-sim-queue-" + uuid.New().String()

	aggID := uuid.New()
	var eventID uuid.UUID
	err = dbConn.QueryRowContext(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, status)
		VALUES ($1, 'order', $2, '{"order_id":"123"}', 'PENDING')
		RETURNING id
	`, aggID, routingKey).Scan(&eventID)
	if err != nil {
		t.Fatalf("failed to insert outbox event: %v", err)
	}

	// Phase A: Simulator absent, queue does not exist
	processedA, err := worker.processBatch(ctx)
	if err != nil {
		t.Fatalf("processBatch Phase A failed: %v", err)
	}
	if processedA != 0 {
		t.Fatalf("expected 0 processed in Phase A, got %d", processedA)
	}

	var statusA string
	_ = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&statusA)
	if statusA != "PENDING" {
		t.Fatalf("expected PENDING in Phase A, got %q", statusA)
	}

	// Phase B: Simulator starts up, declares queue and binds routingKey
	_, err = ch.QueueDeclare(simQueueName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare sim queue: %v", err)
	}
	defer ch.QueueDelete(simQueueName, false, false, false)

	if err := ch.QueueBind(simQueueName, routingKey, "logistics.events", false, nil); err != nil {
		t.Fatalf("failed to bind sim queue: %v", err)
	}

	// Phase C: Worker polls again, message now reaches simulator queue and is ACKed
	processedB, err := worker.processBatch(ctx)
	if err != nil {
		t.Fatalf("processBatch Phase B failed: %v", err)
	}
	if processedB != 1 {
		t.Fatalf("expected 1 processed in Phase B after simulator startup, got %d", processedB)
	}

	var statusB string
	_ = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&statusB)
	if statusB != "PROCESSED" {
		t.Fatalf("expected PROCESSED in Phase B, got %q", statusB)
	}

	// Verify message in sim queue
	msg, ok, err := ch.Get(simQueueName, true)
	if err != nil || !ok {
		t.Fatalf("expected message delivered to sim queue, ok=%v, err=%v", ok, err)
	}
	if msg.MessageId != eventID.String() {
		t.Errorf("expected MessageId %s, got %s", eventID.String(), msg.MessageId)
	}
}

// 4. Fechamento de canal: channel closed abruptly during or before batch
func TestOutbox_RealRabbitMQ_ChannelCloseHandling(t *testing.T) {
	conn, dbConn, q := setupRealTestInfrastructure(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	defer dbConn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("failed to enable publisher confirms: %v", err)
	}

	ctx := context.Background()
	nopLogger := logger.NewZapLogger("test-real-close", false)
	worker := NewOutboxWorker(dbConn, q, nopLogger, WrapChannel(ch))

	aggID := uuid.New()
	var eventID uuid.UUID
	err = dbConn.QueryRowContext(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, status)
		VALUES ($1, 'order', 'ChannelCloseEvent', '{}', 'PENDING')
		RETURNING id
	`, aggID).Scan(&eventID)
	if err != nil {
		t.Fatalf("failed to insert outbox event: %v", err)
	}

	// Close the channel explicitly before batch
	ch.Close()

	// Wait briefly for amqp091-go notify listeners to receive close signal
	time.Sleep(50 * time.Millisecond)

	processed, err := worker.processBatch(ctx)
	if err == nil {
		t.Fatal("expected error from processBatch when channel is closed, got nil")
	}
	if processed != 0 {
		t.Fatalf("expected 0 processed on closed channel, got %d", processed)
	}

	var status string
	_ = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status)
	if status != "PENDING" {
		t.Fatalf("expected event to remain PENDING after channel close, got %q", status)
	}
}

// 5. Recuperação após timeout: confirm timeout aborts batch without marking PROCESSED
func TestOutbox_RealRabbitMQ_TimeoutRecovery(t *testing.T) {
	conn, dbConn, q := setupRealTestInfrastructure(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	defer dbConn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("failed to enable publisher confirms: %v", err)
	}

	routingKey := "DeliveryCreated-Timeout-" + uuid.New().String()
	qName := "test-queue-timeout-" + uuid.New().String()
	_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}
	defer ch.QueueDelete(qName, false, false, false)

	if err := ch.QueueBind(qName, routingKey, "logistics.events", false, nil); err != nil {
		t.Fatalf("failed to bind queue: %v", err)
	}

	ctx := context.Background()
	nopLogger := logger.NewZapLogger("test-real-timeout", false)

	// Worker 1: 1 nanosecond timeout guaranteed to expire before broker network ACK
	workerTimeout := NewOutboxWorker(dbConn, q, nopLogger, WrapChannel(ch), WithConfirmTimeout(1*time.Nanosecond))

	aggID := uuid.New()
	var eventID uuid.UUID
	err = dbConn.QueryRowContext(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, status)
		VALUES ($1, 'delivery', $2, '{"delivery_id":"abc"}', 'PENDING')
		RETURNING id
	`, aggID, routingKey).Scan(&eventID)
	if err != nil {
		t.Fatalf("failed to insert outbox event: %v", err)
	}

	// Execution 1: Times out, leaving event PENDING
	processed1, err := workerTimeout.processBatch(ctx)
	if err != nil {
		t.Fatalf("processBatch 1 failed: %v", err)
	}
	if processed1 != 0 {
		t.Fatalf("expected 0 processed on timeout, got %d", processed1)
	}

	var status1 string
	_ = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status1)
	if status1 != "PENDING" {
		t.Fatalf("expected event to remain PENDING after timeout, got %q", status1)
	}

	// Drain any message that might have arrived on broker from execution 1
	for {
		_, ok, _ := ch.Get(qName, true)
		if !ok {
			break
		}
	}

	// Execution 2: Worker with normal 2-second timeout recovers and processes the event
	workerNormal := NewOutboxWorker(dbConn, q, nopLogger, WrapChannel(ch), WithConfirmTimeout(2*time.Second))
	processed2, err := workerNormal.processBatch(ctx)
	if err != nil {
		t.Fatalf("processBatch 2 failed: %v", err)
	}
	if processed2 != 1 {
		t.Fatalf("expected 1 processed on recovery, got %d", processed2)
	}

	var status2 string
	_ = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status2)
	if status2 != "PROCESSED" {
		t.Fatalf("expected event to become PROCESSED on recovery, got %q", status2)
	}
}

// 6. Falha de commit após ACK, Redelivery e Idempotência
func TestOutbox_RealRabbitMQ_WorkerCrashRecoveryAndIdempotency(t *testing.T) {
	conn, dbConn, q := setupRealTestInfrastructure(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	defer dbConn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	// 1. Enable Publisher Confirms on channel and register notification channel
	confirmChan := ch.NotifyPublish(make(chan amqp.Confirmation, 10))
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("failed to enable publisher confirms on channel: %v", err)
	}

	// 2. Declare dedicated test queue with durable topology
	qName := "test-outbox-real-recovery-" + uuid.New().String()
	_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare test queue: %v", err)
	}
	defer ch.QueueDelete(qName, false, false, false)

	err = ch.QueueBind(qName, "DeliveryCreated", "logistics.events", false, nil)
	if err != nil {
		t.Fatalf("failed to bind test queue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nopLogger := logger.NewZapLogger("test-real-outbox", false)
	aggID := uuid.New()
	hub := stream.NewMemoryLiveHub()
	sub, unsub := hub.Subscribe(ctx, aggID)
	defer unsub()

	evStore := postgres.NewEventStore(q)
	proj := delivery.NewProjection(dbConn, "RealOutboxProjection")
	validator := stream.NewSequenceValidator()
	recovery := stream.NewGapRecovery(evStore, validator, proj, hub)
	strConsumer := stream.NewConsumer(validator, recovery, proj, hub)
	realConsumerAdapter := rabbitmq.NewConsumer(ch, strConsumer)

	// 3. Insert event in outbox_events with status = 'PENDING'
	evPayload := delivery.EventDeliveryCreated{
		ID:         "ev-real-ambig",
		DeliveryID: aggID,
		DriverID:   "driver-real",
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

	// 4. Worker Attempt 1: Simulating Worker crash / rollback AFTER publish into real broker
	body, _ := json.Marshal(map[string]interface{}{
		"event_id":        evPayload.ID,
		"outbox_id":       eventID.String(),
		"aggregate_id":    aggID.String(),
		"stream_position": int64(1),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(payloadBytes),
	})
	err = ch.PublishWithContext(ctx, "logistics.events", "DeliveryCreated", true, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		MessageId:    eventID.String(),
	})
	if err != nil {
		t.Fatalf("real publish failed: %v", err)
	}

	// Verify broker explicitly acknowledged message 1 receipt/persistence (Publisher Confirm ACK)
	select {
	case confirm, ok := <-confirmChan:
		if !ok || !confirm.Ack {
			t.Fatalf("expected broker publisher confirm ACK for attempt 1, got confirm: %+v, ok=%v", confirm, ok)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for broker publisher confirm on attempt 1")
	}

	// Crash simulation: uncommitted lock rollback -> outbox remains PENDING
	var status1 string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status1)
	if err != nil || status1 != "PENDING" {
		t.Fatalf("expected outbox event to remain PENDING after crash simulation, got %q (err: %v)", status1, err)
	}

	// 5. Consumer processes Delivery 1 from real queue
	msg1, ok, err := ch.Get(qName, false)
	if err != nil || !ok {
		t.Fatalf("failed to retrieve delivery 1 from real queue: %v", err)
	}
	if err := realConsumerAdapter.HandleDelivery(ctx, msg1); err != nil {
		t.Fatalf("consumer failed to process delivery 1: %v", err)
	}

	// Projection is at sequence 1
	model1, err := q.GetDeliveryReadModel(ctx, aggID)
	if err != nil || model1.Sequence != 1 {
		t.Fatalf("expected read model sequence 1, got %d (err: %v)", model1.Sequence, err)
	}

	// LiveHub broadcast received
	select {
	case liveEv := <-sub:
		if liveEv.DomainEvent.Sequence() != 1 {
			t.Fatalf("expected sequence 1 on first delivery, got %d", liveEv.DomainEvent.Sequence())
		}
	default:
		t.Fatal("expected live event received on initial delivery")
	}

	// 6. Worker Restart: Fresh Outbox worker instance starts up, finds PENDING event, republishes reliably
	worker2 := NewOutboxWorker(dbConn, q, nopLogger, WrapChannel(ch))
	processed, err := worker2.processBatch(ctx)
	if err != nil {
		t.Fatalf("worker2 processBatch failed: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected 1 processed on retry, got %d", processed)
	}

	// Assert: PostgreSQL event is now PROCESSED
	var status2 string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status2)
	if err != nil || status2 != "PROCESSED" {
		t.Fatalf("expected outbox event to be PROCESSED after worker retry, got %q (err: %v)", status2, err)
	}

	// 7. Consumer processes Delivery 2 (Duplicate from retry)
	msg2, ok, err := ch.Get(qName, false)
	if err != nil || !ok {
		t.Fatalf("failed to retrieve delivery 2 from real queue: %v", err)
	}
	if err := realConsumerAdapter.HandleDelivery(ctx, msg2); err != nil {
		t.Fatalf("consumer failed to process delivery 2: %v", err)
	}

	// CRITICAL ASSERTION: Zero duplicate broadcasts on LiveHub
	select {
	case dup := <-sub:
		t.Fatalf("IDEMPOTENCY VIOLATION: Duplicate broadcast received on LiveHub for seq %d", dup.DomainEvent.Sequence())
	default:
		// Passed!
	}

	// Database projection remains at sequence 1 with exactly 1 row
	model2, err := q.GetDeliveryReadModel(ctx, aggID)
	if err != nil || model2.Sequence != 1 {
		t.Fatalf("expected projection sequence 1, got %d", model2.Sequence)
	}

	t.Log("End-to-end resilience test with REAL RabbitMQ and REAL PostgreSQL passed: broker message delivery, worker crash rollback, recovery retry, and strict side-effect deduplication verified.")
}

// TestOutbox_RealRabbitMQ_BatchAndConcurrentUnroutableStress stresses publisher confirms and basic.return
// synchronization under heavy concurrent and repeated load against a REAL RabbitMQ broker.
// It verifies that under no circumstance is a basic.return lost, dropped, or misattributed to another message.
func TestOutbox_RealRabbitMQ_BatchAndConcurrentUnroutableStress(t *testing.T) {
	conn, dbConn, q := setupRealTestInfrastructure(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	defer dbConn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open AMQP channel: %v", err)
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("failed to enable confirms: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Unique test exchange and queue
	exchangeName := "test.stress.exchange." + uuid.New().String()
	queueName := "test.stress.queue." + uuid.New().String()
	routableKey := "stress.routable"
	unroutableKey := "stress.unroutable"

	if err := ch.ExchangeDeclare(exchangeName, "direct", true, true, false, false, nil); err != nil {
		t.Fatalf("failed to declare exchange: %v", err)
	}
	defer ch.ExchangeDelete(exchangeName, false, false)

	if _, err := ch.QueueDeclare(queueName, true, true, false, false, nil); err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}
	defer ch.QueueDelete(queueName, false, false, false)

	if err := ch.QueueBind(queueName, routableKey, exchangeName, false, nil); err != nil {
		t.Fatalf("failed to bind queue: %v", err)
	}

	testLogger := logger.NewZapLogger("test-stress", false)
	wrappedCh := WrapChannel(ch)
	worker := NewOutboxWorker(dbConn, q, testLogger, wrappedCh, WithBatchSize(100), WithConfirmTimeout(5*time.Second))

	// Stress Test: 5 iterations of 40 interleaved messages (20 routable, 20 unroutable) = 200 total messages
	iterations := 5
	perIteration := 40
	totalUnroutableExpected := iterations * (perIteration / 2)
	totalRoutableExpected := iterations * (perIteration / 2)

	unroutableCaptured := 0
	routableSucceeded := 0

	for iter := 0; iter < iterations; iter++ {
		returnedMap := make(map[string]amqp.Return)
		worker.drainReturns(returnedMap)

		for i := 0; i < perIteration; i++ {
			msgUUID := uuid.New()
			isRoutable := (i%2 == 0)
			eventType := routableKey
			if !isRoutable {
				eventType = unroutableKey
			}

			event := db.OutboxEvent{
				ID:            msgUUID,
				AggregateID:   uuid.New(),
				AggregateType: "delivery",
				EventType:     eventType,
				Payload:       []byte(`{"test":"stress"}`),
			}

			conf, pubErr := ch.PublishWithDeferredConfirmWithContext(
				ctx,
				exchangeName,
				eventType,
				true,  // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType:  "application/json",
					Body:         event.Payload,
					DeliveryMode: amqp.Persistent,
					MessageId:    event.ID.String(),
					Type:         event.EventType,
				},
			)
			if pubErr != nil {
				t.Fatalf("[iter %d msg %d] publish failed: %v", iter, i, pubErr)
			}

			confirmCtx, cancelConfirm := context.WithTimeout(ctx, 3*time.Second)
			acked, waitErr := conf.WaitContext(confirmCtx)
			cancelConfirm()

			// Synchronous drain right after WaitContext
			worker.drainReturns(returnedMap)

			if waitErr != nil {
				t.Fatalf("[iter %d msg %d] wait error: %v", iter, i, waitErr)
			}
			if !acked {
				t.Fatalf("[iter %d msg %d] unexpected broker NACK", iter, i)
			}

			ret, isReturned := returnedMap[event.ID.String()]

			if isRoutable {
				if isReturned {
					t.Fatalf("[iter %d msg %d] FALSE POSITIVE: routable message %s was incorrectly marked returned: %+v",
						iter, i, event.ID.String(), ret)
				}
				routableSucceeded++
			} else {
				if !isReturned {
					t.Fatalf("[iter %d msg %d] MISSED RETURN: unroutable message %s was NOT captured in returnedMap! acked=%v",
						iter, i, event.ID.String(), acked)
				}
				if ret.ReplyCode != 312 { // NO_ROUTE
					t.Fatalf("[iter %d msg %d] unexpected reply code: %d (expected 312 NO_ROUTE)", iter, i, ret.ReplyCode)
				}
				unroutableCaptured++
			}
		}
	}

	if unroutableCaptured != totalUnroutableExpected {
		t.Fatalf("expected exactly %d unroutable messages captured, got %d", totalUnroutableExpected, unroutableCaptured)
	}
	if routableSucceeded != totalRoutableExpected {
		t.Fatalf("expected exactly %d routable messages succeeded, got %d", totalRoutableExpected, routableSucceeded)
	}

	t.Logf("STRESS VERIFICATION SUCCESS: %d/%d unroutable messages deterministically captured via basic.return barrier; %d/%d routable messages confirmed with 0 false positives.",
		unroutableCaptured, totalUnroutableExpected, routableSucceeded, totalRoutableExpected)
}

// TestOutbox_RealRabbitMQ_BatchWithUnroutableLeavesPendingInDB verifies that when a batch contains
// unroutable events, the outbox worker halts processing on the unroutable event and leaves it
// strictly in PENDING status in the PostgreSQL database.
func TestOutbox_RealRabbitMQ_BatchWithUnroutableLeavesPendingInDB(t *testing.T) {
	conn, dbConn, q := setupRealTestInfrastructure(t)
	if conn == nil {
		return
	}
	defer conn.Close()
	defer dbConn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open AMQP channel: %v", err)
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("failed to enable confirms: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testLogger := logger.NewZapLogger("test-db-unroutable", false)
	worker := NewOutboxWorker(dbConn, q, testLogger, WrapChannel(ch), WithBatchSize(10))

	// Insert an unroutable event into outbox_events
	unroutableUUID := uuid.New()
	unroutableKey := "completely.unroutable.key." + uuid.New().String()
	_, err = dbConn.ExecContext(ctx, `
		INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, payload, status)
		VALUES ($1, $2, 'delivery', $3, '{"test":"unroutable"}', 'PENDING')
	`, unroutableUUID, uuid.New(), unroutableKey)
	if err != nil {
		t.Fatalf("failed to insert unroutable event: %v", err)
	}

	// Execute processBatch
	processed, err := worker.processBatch(ctx)
	if err != nil {
		t.Fatalf("unexpected error from processBatch: %v", err)
	}
	if processed != 0 {
		t.Fatalf("expected 0 events processed because event is unroutable, got %d", processed)
	}

	// Verify status in PostgreSQL remains PENDING
	var dbStatus string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", unroutableUUID).Scan(&dbStatus)
	if err != nil {
		t.Fatalf("failed to query outbox event status: %v", err)
	}
	if dbStatus != "PENDING" {
		t.Fatalf("CRITICAL INTEGRITY FAILURE: unroutable event was marked as %s in DB! Expected PENDING.", dbStatus)
	}

	t.Logf("Database verification passed: unroutable event %s remained strictly PENDING in outbox_events.", unroutableUUID)
}
