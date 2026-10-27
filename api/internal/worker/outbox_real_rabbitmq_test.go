package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
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

// TestOutbox_RealRabbitMQ_WorkerCrashRecoveryAndIdempotency tests the Outbox pattern with a real
// RabbitMQ 4.3.5 broker and real PostgreSQL instance under partial failure conditions.
//
// SCOPE AND BOUNDARIES:
//  1. Tested Scenario: Publisher Confirms enabled (ch.Confirm(false)), durable queue topology,
//     and transaction abort/rollback before DB commit (modeling database connection loss or worker abort).
//  2. Functional Equivalence: From the broker's and DB's perspective, tx.Rollback() produces the identical
//     state as an abrupt worker crash (PostgreSQL rolls back uncommitted locks via connection termination,
//     leaving the outbox event in 'PENDING', while RabbitMQ has already accepted and acknowledged the message).
//  3. Operational Caveats: This test does not execute an OS-level SIGKILL on a separate running process.
//     Furthermore, in production (api/cmd/server/main.go), channels are not currently placed in Confirm mode,
//     making this test also serve as a validation benchmark for future publisher confirms adoption.
func TestOutbox_RealRabbitMQ_WorkerCrashRecoveryAndIdempotency(t *testing.T) {
	amqpURL := os.Getenv("TEST_AMQP_URL")
	if amqpURL == "" {
		amqpURL = os.Getenv("RABBITMQ_URL")
	}
	if amqpURL == "" {
		amqpURL = "amqp://root:password@rabbitmq:5672/"
	}

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skipf("Skipping real RabbitMQ integration test (broker unreachable: %v)", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	// 1. Declare real exchange (must match topology: direct, durable)
	err = ch.ExchangeDeclare("logistics.events", "direct", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare exchange: %v", err)
	}

	// 1b. Enable Publisher Confirms on the channel and register notification channel
	confirmChan := ch.NotifyPublish(make(chan amqp.Confirmation, 10))
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("failed to enable publisher confirms on channel: %v", err)
	}

	// 2. Declare dedicated test queue with durable topology (durable: true, exclusive: false, autoDelete: false)
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

	// 3. Real PostgreSQL connection
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://root:password@postgres:5432/logistics?sslmode=disable"
	}
	dbConn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}
	defer dbConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize tables cleanly
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
		t.Fatalf("failed to setup schema: %v", err)
	}

	q := db.New(dbConn)
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

	// 4. Insert event in outbox_events with status = 'PENDING'
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

	// 5. Worker Attempt 1: Simulating Worker crash / rollback AFTER publish into real broker
	tx, err := dbConn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	body, _ := json.Marshal(map[string]interface{}{
		"event_id":        eventID.String(),
		"aggregate_id":    aggID.String(),
		"stream_position": int64(1),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(payloadBytes),
	})
	err = ch.PublishWithContext(ctx, "logistics.events", "DeliveryCreated", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
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

	// Worker failure simulation: Worker loses DB connection or encounters error before commit.
	// NOTE: This models transaction abort/connection drop (tx.Rollback) rather than an OS-level SIGKILL.
	_ = tx.Rollback()

	// Assert: Event in PostgreSQL remains PENDING
	var status1 string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status1)
	if err != nil || status1 != "PENDING" {
		t.Fatalf("expected PENDING status in DB, got %q", status1)
	}

	// 6. Consumer receives Message 1 from the real RabbitMQ broker queue
	deliveryMsg1, ok, err := ch.Get(qName, false)
	if err != nil || !ok {
		t.Fatalf("expected message 1 from real RabbitMQ queue: ok=%v, err=%v", ok, err)
	}
	if err := realConsumerAdapter.HandleDelivery(ctx, deliveryMsg1); err != nil {
		t.Fatalf("consumer failed to handle delivery 1: %v", err)
	}

	// Verify Delivery 1 was applied in DB and broadcast to LiveHub
	select {
	case liveEv := <-sub:
		if liveEv.DomainEvent.Sequence() != 1 {
			t.Fatalf("expected sequence 1 on first delivery, got %d", liveEv.DomainEvent.Sequence())
		}
	default:
		t.Fatal("expected live event received on initial delivery")
	}

	// 7. Worker Restart: A fresh Outbox worker instance starts up, finds PENDING event, and republishes
	worker2 := NewOutboxWorker(dbConn, q, nopLogger, ch)
	worker2.processOutbox(ctx)

	// Verify broker explicitly acknowledged message 2 (retransmission) via Publisher Confirm ACK
	select {
	case confirm, ok := <-confirmChan:
		if !ok || !confirm.Ack {
			t.Fatalf("expected broker publisher confirm ACK for attempt 2, got confirm: %+v, ok=%v", confirm, ok)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for broker publisher confirm on attempt 2")
	}

	// Assert: PostgreSQL event is now PROCESSED
	var status2 string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE id = $1", eventID).Scan(&status2)
	if err != nil || status2 != "PROCESSED" {
		t.Fatalf("expected PROCESSED status after worker recovery, got %q", status2)
	}

	// 8. Consumer receives Message 2 (the duplicate re-publication) from the real RabbitMQ queue
	deliveryMsg2, ok, err := ch.Get(qName, false)
	if err != nil || !ok {
		t.Fatalf("expected message 2 (retransmitted) in real RabbitMQ queue: ok=%v, err=%v", ok, err)
	}
	if err := realConsumerAdapter.HandleDelivery(ctx, deliveryMsg2); err != nil {
		t.Fatalf("consumer failed to handle delivery 2: %v", err)
	}

	// 9. Verify Deduplication across all side effects:
	// - LiveHub must NOT receive duplicate broadcast:
	select {
	case dup := <-sub:
		t.Fatalf("IDEMPOTENCY VIOLATION: Duplicate broadcast received on LiveHub: %v", dup)
	default:
		// Passed! Zero duplicate broadcasts
	}

	// - Read model remains strictly at sequence 1, exactly 1 row:
	model, err := q.GetDeliveryReadModel(ctx, aggID)
	if err != nil || model.Sequence != 1 {
		t.Fatalf("expected read model sequence 1, got %d (err: %v)", model.Sequence, err)
	}

	t.Log("End-to-end resilience test with REAL RabbitMQ and REAL PostgreSQL passed: broker message delivery, worker crash rollback, recovery retry, and strict side-effect deduplication verified.")
}
