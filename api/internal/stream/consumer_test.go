package stream_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/postgres"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
)

func setupTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://root:password@127.0.0.1:5432/logistics?sslmode=disable"
	}
	dbConn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}

	_, err = dbConn.Exec(`
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
	return dbConn
}

func seedOutbox(t *testing.T, dbConn *sql.DB, aggID uuid.UUID, eventType string, payload interface{}) int64 {
	b, _ := json.Marshal(payload)
	var pos int64
	err := dbConn.QueryRow(`
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload)
		VALUES ($1, 'delivery', $2, $3) RETURNING stream_position
	`, aggID, eventType, b).Scan(&pos)
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}
	return pos
}

type mockAcknowledger struct {
	ackCalled  bool
	nackCalled bool
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error { m.ackCalled = true; return nil }
func (m *mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	m.nackCalled = true
	return nil
}
func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error { m.nackCalled = true; return nil }

func TestStreamConsumer_FullIntegration(t *testing.T) {
	dbConn := setupTestDB(t)
	defer dbConn.Close()
	ctx := context.Background()

	// Wiring
	q := db.New(dbConn)
	evStore := postgres.NewEventStore(q)
	proj := delivery.NewProjection(dbConn, "DeliveryProjection")

	validator := stream.NewSequenceValidator()
	recovery := stream.NewGapRecovery(evStore, validator, proj, nil)
	strConsumer := stream.NewConsumer(validator, recovery, proj, nil)

	amqpAdapter := rabbitmq.NewConsumer(nil, strConsumer)

	aggID := uuid.New()

	// Prepare events in DB (representing Outbox)
	ev1 := delivery.EventDeliveryCreated{ID: "e1", DeliveryID: aggID, DriverID: "d1", Seq: 1}
	pos1 := seedOutbox(t, dbConn, aggID, "DeliveryCreated", ev1)

	ev2 := delivery.EventDriverArrivedAtPickup{ID: "e2", DeliveryID: aggID, Seq: 2}
	pos2 := seedOutbox(t, dbConn, aggID, "DriverArrivedAtPickup", ev2)

	ev3 := delivery.EventDriverEnRouteToDropoff{ID: "e3", DeliveryID: aggID, Seq: 3}
	pos3 := seedOutbox(t, dbConn, aggID, "DriverEnRouteToDropoff", ev3)

	makeDelivery := func(pos int64, eventType string, payload interface{}) amqp.Delivery {
		b, _ := json.Marshal(payload)
		env := map[string]interface{}{
			"aggregate_id":    aggID,
			"stream_position": pos,
			"event_type":      eventType,
			"payload":         json.RawMessage(b),
		}
		envB, _ := json.Marshal(env)
		ack := &mockAcknowledger{}
		return amqp.Delivery{
			Body:         envB,
			Acknowledger: ack,
		}
	}

	// 1. Normal Flow (Send seq 1)
	del1 := makeDelivery(pos1, "DeliveryCreated", ev1)
	err := amqpAdapter.HandleDelivery(ctx, del1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !del1.Acknowledger.(*mockAcknowledger).ackCalled {
		t.Fatalf("expected ACK")
	}

	ckpt, _ := proj.GetCheckpoint(ctx)
	if ckpt != pos1 {
		t.Fatalf("expected checkpoint %d, got %d", pos1, ckpt)
	}

	// 2. Duplicate Delivery (Send seq 1 again)
	del1Dup := makeDelivery(pos1, "DeliveryCreated", ev1)
	err = amqpAdapter.HandleDelivery(ctx, del1Dup)
	if err != nil {
		t.Fatalf("unexpected error on duplicate: %v", err)
	}
	if !del1Dup.Acknowledger.(*mockAcknowledger).ackCalled {
		t.Fatalf("expected ACK on duplicate")
	}

	// 3. Gap Recovery (Send seq 3, missing 2)
	del3 := makeDelivery(pos3, "DriverEnRouteToDropoff", ev3)
	err = amqpAdapter.HandleDelivery(ctx, del3)
	if err != nil {
		t.Fatalf("unexpected error on gap recovery: %v", err)
	}

	// Verify read model is at seq 3
	model, _ := q.GetDeliveryReadModel(ctx, aggID)
	if model.State != "EN_ROUTE_TO_DROPOFF" {
		t.Fatalf("expected EN_ROUTE_TO_DROPOFF, got %s", model.State)
	}
	if model.Sequence != 3 {
		t.Fatalf("expected sequence 3, got %d", model.Sequence)
	}

	ckpt, _ = proj.GetCheckpoint(ctx)
	if ckpt != pos3 {
		t.Fatalf("expected checkpoint %d, got %d", pos3, ckpt)
	}

	// 4. Redelivery after missing ACK (Send seq 2 now - it should be ignored because we already recovered to 3)
	del2 := makeDelivery(pos2, "DriverArrivedAtPickup", ev2)
	err = amqpAdapter.HandleDelivery(ctx, del2)
	if err != nil {
		t.Fatalf("unexpected error on redelivery: %v", err)
	}
	if !del2.Acknowledger.(*mockAcknowledger).ackCalled {
		t.Fatalf("expected ACK on stale event")
	}

	// 5. Restart Projection (New Consumer Instance)
	// We simulate a crash/restart by creating a NEW consumer with an empty validator map
	newValidator := stream.NewSequenceValidator()
	newRecovery := stream.NewGapRecovery(evStore, newValidator, proj, nil)
	newStrConsumer := stream.NewConsumer(newValidator, newRecovery, proj, nil)
	newAmqpAdapter := rabbitmq.NewConsumer(nil, newStrConsumer)

	// Send seq 4 to the new instance (it has no memory of aggID!)
	ev4 := delivery.EventDeliveryCompleted{ID: "e4", DeliveryID: aggID, Seq: 4}
	pos4 := seedOutbox(t, dbConn, aggID, "DeliveryCompleted", ev4)
	del4 := makeDelivery(pos4, "DeliveryCompleted", ev4)
	err = newAmqpAdapter.HandleDelivery(ctx, del4)
	if err != nil {
		t.Fatalf("unexpected error on restart recovery: %v", err)
	}

	model, _ = q.GetDeliveryReadModel(ctx, aggID)
	if model.State != "DELIVERED" {
		t.Fatalf("expected DELIVERED, got %s", model.State)
	}
	if model.Sequence != 4 {
		t.Fatalf("expected sequence 4, got %d", model.Sequence)
	}

	ckpt, _ = proj.GetCheckpoint(ctx)
	if ckpt != pos4 {
		t.Fatalf("expected checkpoint %d, got %d", pos4, ckpt)
	}

	t.Log("6E.8.2: Stream Consumer successfully orchestrated RabbitMQ, Gap Recovery, Causal Validation and Read Models!")
}

func TestLiveHub_Publishing(t *testing.T) {
	dbConn := setupTestDB(t)
	defer dbConn.Close()
	ctx := context.Background()

	q := db.New(dbConn)
	evStore := postgres.NewEventStore(q)
	proj := delivery.NewProjection(dbConn, "DeliveryProjectionHub")

	validator := stream.NewSequenceValidator()
	hub := stream.NewMemoryLiveHub()
	recovery := stream.NewGapRecovery(evStore, validator, proj, hub)
	strConsumer := stream.NewConsumer(validator, recovery, proj, hub)

	amqpAdapter := rabbitmq.NewConsumer(nil, strConsumer)
	aggID := uuid.New()

	// Subscribers
	sub1, _ := hub.Subscribe(ctx, aggID)
	sub2, unsub2 := hub.Subscribe(ctx, aggID)

	// Seed and send seq 1
	ev1 := delivery.EventDeliveryCreated{ID: "e1", DeliveryID: aggID, Seq: 1}
	pos1 := seedOutbox(t, dbConn, aggID, "DeliveryCreated", ev1)

	b, _ := json.Marshal(ev1)
	envB, _ := json.Marshal(map[string]interface{}{
		"aggregate_id": aggID, "stream_position": pos1, "event_type": "DeliveryCreated", "payload": json.RawMessage(b),
	})

	err := amqpAdapter.HandleDelivery(ctx, amqp.Delivery{Body: envB, Acknowledger: &mockAcknowledger{}})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Check if subscribers got it
	recv1 := <-sub1
	if recv1.DomainEvent.Sequence() != 1 {
		t.Fatalf("sub1 expected seq 1")
	}
	recv2 := <-sub2
	if recv2.DomainEvent.Sequence() != 1 {
		t.Fatalf("sub2 expected seq 1")
	}

	// Unsubscribe 2
	unsub2()

	// Seed and send seq 3 directly to trigger Gap Recovery (needs seq 2 first)
	ev2 := delivery.EventDriverArrivedAtPickup{ID: "e2", DeliveryID: aggID, Seq: 2}
	seedOutbox(t, dbConn, aggID, "DriverArrivedAtPickup", ev2)

	ev3 := delivery.EventDriverEnRouteToDropoff{ID: "e3", DeliveryID: aggID, Seq: 3}
	pos3 := seedOutbox(t, dbConn, aggID, "DriverEnRouteToDropoff", ev3)

	b3, _ := json.Marshal(ev3)
	envB3, _ := json.Marshal(map[string]interface{}{
		"aggregate_id": aggID, "stream_position": pos3, "event_type": "DriverEnRouteToDropoff", "payload": json.RawMessage(b3),
	})

	err = amqpAdapter.HandleDelivery(ctx, amqp.Delivery{Body: envB3, Acknowledger: &mockAcknowledger{}})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// sub1 should receive seq 2 then seq 3 from recovery in order!
	recvRecovery2 := <-sub1
	if recvRecovery2.DomainEvent.Sequence() != 2 {
		t.Fatalf("expected recovered seq 2")
	}
	recvRecovery3 := <-sub1
	if recvRecovery3.DomainEvent.Sequence() != 3 {
		t.Fatalf("expected recovered seq 3")
	}

	// sub2 should receive nothing because unsubscribed
	select {
	case _, ok := <-sub2:
		if ok {
			t.Fatalf("sub2 should be closed")
		}
	default:
	}

	t.Log("6E.8.3: LiveHub successfully broadcasts in-order events to active subscribers after successful commits.")
}

func TestConsumer_AmbiguousPublishAndSideEffectsDeduplication(t *testing.T) {
	dbConn := setupTestDB(t)
	defer dbConn.Close()
	ctx := context.Background()

	q := db.New(dbConn)
	evStore := postgres.NewEventStore(q)
	proj := delivery.NewProjection(dbConn, "AmbiguousPublishProjection")

	aggID := uuid.New()

	// Spy Hub to record every external side-effect (e.g. WebSocket / SSE broadcasts)
	hub := stream.NewMemoryLiveHub()
	sub, unsub := hub.Subscribe(ctx, aggID)
	defer unsub()

	validator := stream.NewSequenceValidator()
	recovery := stream.NewGapRecovery(evStore, validator, proj, hub)
	strConsumer := stream.NewConsumer(validator, recovery, proj, hub)
	amqpAdapter := rabbitmq.NewConsumer(nil, strConsumer)

	// 1. Transactional Outbox State: Event is persisted as PENDING
	ev1 := delivery.EventDeliveryCreated{ID: "e1-ambiguous", DeliveryID: aggID, DriverID: "d1", Seq: 1}
	pos1 := seedOutbox(t, dbConn, aggID, "DeliveryCreated", ev1)

	b1, _ := json.Marshal(ev1)
	envB1, _ := json.Marshal(map[string]interface{}{
		"aggregate_id":    aggID,
		"stream_position": pos1,
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(b1),
	})

	ackSpy1 := &mockAcknowledger{}
	del1 := amqp.Delivery{
		Body:         envB1,
		Acknowledger: ackSpy1,
		Redelivered:  false,
	}

	// 2. First Publish: Message arrives at Consumer and is processed
	if err := amqpAdapter.HandleDelivery(ctx, del1); err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}
	if !ackSpy1.ackCalled {
		t.Fatal("expected ACK on first delivery")
	}

	// Verify first delivery created external side-effect (hub broadcast) and DB projection
	select {
	case publishedEv := <-sub:
		if publishedEv.DomainEvent.Sequence() != 1 {
			t.Fatalf("expected broadcast of sequence 1, got %d", publishedEv.DomainEvent.Sequence())
		}
	default:
		t.Fatal("expected external live hub broadcast on first delivery")
	}

	model, err := q.GetDeliveryReadModel(ctx, aggID)
	if err != nil || model.Sequence != 1 {
		t.Fatalf("expected DB projection sequence 1, got %d (err: %v)", model.Sequence, err)
	}

	// 3. Ambiguous failure scenario:
	// The outbox worker failed to commit (or lost confirm) during attempt 1.
	// Therefore, the outbox record in PostgreSQL remained PENDING.
	var outboxStatus string
	err = dbConn.QueryRowContext(ctx, "SELECT status FROM outbox_events WHERE stream_position = $1", pos1).Scan(&outboxStatus)
	if err != nil || outboxStatus != "PENDING" {
		t.Fatalf("expected outbox event to still be PENDING in ambiguous failure, got %s", outboxStatus)
	}

	// 4. Retry by Outbox Relay:
	// The outbox relay re-polls and re-transmits the identical message to RabbitMQ.
	// RabbitMQ delivers it to the consumer (marked with Redelivered = true).
	ackSpy2 := &mockAcknowledger{}
	del1Retry := amqp.Delivery{
		Body:         envB1,
		Acknowledger: ackSpy2,
		Redelivered:  true,
	}

	if err := amqpAdapter.HandleDelivery(ctx, del1Retry); err != nil {
		t.Fatalf("redelivered event failed: %v", err)
	}
	if !ackSpy2.ackCalled {
		t.Fatal("expected ACK on redelivered message")
	}

	// 5. Verification of Idempotence across ALL side-effects:
	// Side-effect A: External Hub Broadcast must NOT have received a duplicate event!
	select {
	case duplicateEv := <-sub:
		t.Fatalf("IDEMPOTENCY VIOLATION: Duplicate broadcast received on external hub for sequence %d", duplicateEv.DomainEvent.Sequence())
	default:
		// Passed! No duplicate broadcast.
	}

	// Side-effect B: Database projection must remain at sequence 1, exactly 1 row.
	modelAfter, err := q.GetDeliveryReadModel(ctx, aggID)
	if err != nil || modelAfter.Sequence != 1 {
		t.Fatalf("expected DB projection sequence 1 unchanged, got %d", modelAfter.Sequence)
	}

	// 6. After the successful confirm on retry, outbox marks the event as PROCESSED
	_, err = dbConn.ExecContext(ctx, "UPDATE outbox_events SET status = 'PROCESSED' WHERE stream_position = $1", pos1)
	if err != nil {
		t.Fatalf("failed to mark outbox processed: %v", err)
	}

	t.Log("Ambiguous publish and consumer side-effects idempotency test passed successfully!")
}
