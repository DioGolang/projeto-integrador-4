package rabbitmq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/postgres"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func getGate15B3AMQPURL() string {
	if u := os.Getenv("TEST_AMQP_URL"); u != "" {
		return u
	}
	return "amqp://root:password@127.0.0.1:5672/"
}

func getGate15B3DBDSN() string {
	d := os.Getenv("TEST_DB_DSN")
	if d == "" {
		d = "postgres://root:password@127.0.0.1:5432/logistics?sslmode=disable"
	}
	if !strings.Contains(d, "search_path") {
		if strings.Contains(d, "?") {
			d += "&search_path=gate15b3_test,public"
		} else {
			d += "?search_path=gate15b3_test,public"
		}
	}
	return d
}

func setupGate15B3DB(t *testing.T) *sql.DB {
	t.Helper()
	dbConn, err := sql.Open("postgres", getGate15B3DBDSN())
	if err != nil {
		t.Skipf("cannot connect to postgres: %v", err)
		return nil
	}
	if err := dbConn.Ping(); err != nil {
		t.Skipf("postgres ping failed: %v", err)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = dbConn.ExecContext(ctx, `
		CREATE SCHEMA IF NOT EXISTS gate15b3_test;
		SET search_path TO gate15b3_test, public;

		DROP TABLE IF EXISTS gate15b3_test.processed_events CASCADE;
		DROP TABLE IF EXISTS gate15b3_test.delivery_read_model CASCADE;
		DROP TABLE IF EXISTS gate15b3_test.projection_checkpoints CASCADE;
		DROP TABLE IF EXISTS gate15b3_test.outbox_events CASCADE;
		DROP TYPE IF EXISTS gate15b3_test.outbox_status CASCADE;

		CREATE TYPE gate15b3_test.outbox_status AS ENUM ('PENDING', 'PROCESSED', 'FAILED');

		CREATE TABLE gate15b3_test.processed_events (
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

		CREATE INDEX idx_processed_events_consumer_agg_seq
			ON gate15b3_test.processed_events (consumer_id, aggregate_id, sequence);

		CREATE TABLE gate15b3_test.delivery_read_model (
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

		CREATE TABLE gate15b3_test.projection_checkpoints (
			projection_name TEXT PRIMARY KEY,
			last_sequence BIGINT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE gate15b3_test.outbox_events (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			aggregate_id UUID NOT NULL,
			aggregate_type VARCHAR(50) NOT NULL,
			event_type VARCHAR(100) NOT NULL,
			payload JSONB NOT NULL,
			status gate15b3_test.outbox_status NOT NULL DEFAULT 'PENDING',
			stream_position BIGSERIAL,
			traceparent VARCHAR(255),
			tracestate VARCHAR(255),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			processed_at TIMESTAMPTZ
		);
	`)
	if err != nil {
		dbConn.Close()
		t.Fatalf("failed to initialize schema gate15b3_test: %v", err)
	}
	return dbConn
}

type testAMQPTopology struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
	dlqName   string
	dlxName   string
}

func setupGate15B3AMQP(t *testing.T, baseName string) *testAMQPTopology {
	t.Helper()
	conn, err := amqp.Dial(getGate15B3AMQPURL())
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return nil
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		t.Fatalf("failed to open channel: %v", err)
	}

	dlxName := fmt.Sprintf("dlx.%s", baseName)
	dlqName := fmt.Sprintf("dlq.%s", baseName)
	queueName := fmt.Sprintf("q.%s", baseName)

	// Declare Dead Letter Exchange (DLX) and Dead Letter Queue (DLQ)
	err = ch.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare dlx: %v", err)
	}
	_, err = ch.QueueDeclare(dlqName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare dlq: %v", err)
	}
	err = ch.QueueBind(dlqName, "dead-letter", dlxName, false, nil)
	if err != nil {
		t.Fatalf("failed to bind dlq: %v", err)
	}

	// Declare main queue with DLX routing
	qArgs := amqp.Table{
		"x-dead-letter-exchange":    dlxName,
		"x-dead-letter-routing-key": "dead-letter",
	}
	_, err = ch.QueueDeclare(queueName, true, false, false, false, qArgs)
	if err != nil {
		t.Fatalf("failed to declare main queue: %v", err)
	}

	return &testAMQPTopology{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
		dlqName:   dlqName,
		dlxName:   dlxName,
	}
}

func (topo *testAMQPTopology) Cleanup() {
	if topo == nil {
		return
	}
	topo.ch.QueueDelete(topo.queueName, false, false, false)
	topo.ch.QueueDelete(topo.dlqName, false, false, false)
	topo.ch.ExchangeDelete(topo.dlxName, false, false)
	topo.ch.Close()
	topo.conn.Close()
}

func waitForDLQCount(t *testing.T, ch *amqp.Channel, dlqName string, expected int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		q, err := ch.QueueInspect(dlqName)
		if err == nil && q.Messages == expected {
			return q.Messages
		}
		time.Sleep(25 * time.Millisecond)
	}
	q, _ := ch.QueueInspect(dlqName)
	return q.Messages
}

// -----------------------------------------------------------------------------
// Test 1: Duplicata exata na última sequência
// -----------------------------------------------------------------------------
func TestGate15B3_1_ExactDuplicateLastSequence(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-1-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	evID := fmt.Sprintf("evt-15b3-1-%s", uuid.New().String())
	ev := delivery.EventDeliveryCreated{
		ID:         evID,
		DeliveryID: delID,
		DriverID:   "driver-test-1",
		Seq:        1,
		PickupLat:  -23.55,
		PickupLng:  -46.63,
		DropoffLat: -23.56,
		DropoffLng: -46.64,
	}
	payloadBytes, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":        evID,
		"aggregate_id":    delID,
		"stream_position": int64(300101),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(payloadBytes),
	})

	// Setup pipeline
	validator := stream.NewSequenceValidator()
	rec := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), validator, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), delID)
	defer hubUnsub()

	streamConsumer := stream.NewConsumer(validator, rec, delivery.NewProjection(dbConn, "DeliveryProjection"), liveHub)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	// Step 1: Process initial event via real queue
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgChan, err := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	d1 := <-msgChan
	if err := rabbitConsumer.HandleDelivery(context.Background(), d1); err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}

	// Verify notification on LiveHub
	select {
	case <-hubSub:
		t.Log("Legitimate initial notification received.")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial notification")
	}

	// Step 2: Deliver EXACT DUPLICATE of last sequence via real queue
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	d1Dup := <-msgChan
	d1Dup.Redelivered = true
	if err := rabbitConsumer.HandleDelivery(context.Background(), d1Dup); err != nil {
		t.Fatalf("duplicate delivery should be handled and ACKed, got: %v", err)
	}

	// Verify ZERO duplicate notifications
	select {
	case extra := <-hubSub:
		t.Fatalf("spurious notification emitted for duplicate: %+v", extra)
	case <-time.After(300 * time.Millisecond):
		t.Log("Suppression confirmed: zero notifications emitted for duplicate.")
	}

	// Verify Read Model and Checkpoint strictly preserved
	var seq int64
	_ = dbConn.QueryRow("SELECT sequence FROM gate15b3_test.delivery_read_model WHERE delivery_id = $1", delID).Scan(&seq)
	if seq != 1 {
		t.Fatalf("read model sequence corrupted: %d", seq)
	}

	var cp int64
	_ = dbConn.QueryRow("SELECT last_sequence FROM gate15b3_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cp)
	if cp != 300101 {
		t.Fatalf("checkpoint corrupted: %d", cp)
	}

	t.Log("=== TEST 1 (DUPLICATA EXATA ÚLTIMA SEQUÊNCIA) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 2: Mesmo EventID com payload divergente -> rejeitado para DLQ
// -----------------------------------------------------------------------------
func TestGate15B3_2_SameEventIDDivergentPayload(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-2-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	evID := fmt.Sprintf("evt-15b3-2-%s", uuid.New().String())
	ev1 := delivery.EventDeliveryCreated{
		ID:         evID,
		DeliveryID: delID,
		DriverID:   "driver-legitimate",
		Seq:        1,
	}
	p1, _ := json.Marshal(ev1)
	env1, _ := json.Marshal(map[string]interface{}{
		"event_id":        evID,
		"aggregate_id":    delID,
		"stream_position": int64(300201),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p1),
	})

	validator := stream.NewSequenceValidator()
	streamConsumer := stream.NewConsumer(validator, nil, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	// Publish and consume legitimate event
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env1})
	msg1, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d1 := <-msg1
	if err := rabbitConsumer.HandleDelivery(context.Background(), d1); err != nil {
		t.Fatalf("initial delivery failed: %v", err)
	}

	// Construct conflicting event: SAME event_id, but payload has different DriverID
	evConflicting := delivery.EventDeliveryCreated{
		ID:         evID,
		DeliveryID: delID,
		DriverID:   "driver-impostor",
		Seq:        1,
	}
	pConflict, _ := json.Marshal(evConflicting)
	envConflict, _ := json.Marshal(map[string]interface{}{
		"event_id":        evID,
		"aggregate_id":    delID,
		"stream_position": int64(300202),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(pConflict),
	})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envConflict})
	dConflict := <-msg1

	// Handle conflicting delivery: MUST return error and reject to DLQ
	err := rabbitConsumer.HandleDelivery(context.Background(), dConflict)
	if err == nil {
		t.Fatal("expected error on conflicting payload, got nil")
	}

	// Verify message was routed to DLQ by the broker
	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", dlqCount)
	}

	// Verify Read Model preserved
	var driver string
	_ = dbConn.QueryRow("SELECT driver_id FROM gate15b3_test.delivery_read_model WHERE delivery_id = $1", delID).Scan(&driver)
	if driver != "driver-legitimate" {
		t.Fatalf("read model was modified on identity conflict: %s", driver)
	}

	t.Log("=== TEST 2 (MESMO EVENT_ID COM PAYLOAD DIVERGENTE) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 3: EventID diferente na mesma sequência -> Causal conflict para DLQ
// -----------------------------------------------------------------------------
func TestGate15B3_3_DifferentEventIDSameSequence(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-3-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	ev1 := delivery.EventDeliveryCreated{
		ID:         "evt-seq1-a",
		DeliveryID: delID,
		DriverID:   "driver-seq1",
		Seq:        1,
	}
	p1, _ := json.Marshal(ev1)
	env1, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-seq1-a",
		"aggregate_id":    delID,
		"stream_position": int64(300301),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p1),
	})

	validator := stream.NewSequenceValidator()
	streamConsumer := stream.NewConsumer(validator, nil, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env1})
	msgChan, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d1 := <-msgChan
	if err := rabbitConsumer.HandleDelivery(context.Background(), d1); err != nil {
		t.Fatalf("first event failed: %v", err)
	}

	// Send different event_id for same sequence 1
	ev2 := delivery.EventDeliveryCreated{
		ID:         "evt-seq1-b", // DIFFERENT event_id
		DeliveryID: delID,
		DriverID:   "driver-seq1-divergent",
		Seq:        1,
	}
	p2, _ := json.Marshal(ev2)
	env2, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-seq1-b",
		"aggregate_id":    delID,
		"stream_position": int64(300302),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p2),
	})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env2})
	d2 := <-msgChan

	err := rabbitConsumer.HandleDelivery(context.Background(), d2)
	if err == nil {
		t.Fatal("expected causal conflict error, got nil")
	}

	// Verify routed to DLQ
	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", dlqCount)
	}

	t.Log("=== TEST 3 (EVENT_ID DIFERENTE NA MESMA SEQUÊNCIA) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 4: Duplicata exata de sequência histórica
// -----------------------------------------------------------------------------
func TestGate15B3_4_ExactDuplicateHistoricalSequence(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-4-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	validator := stream.NewSequenceValidator()
	proj := delivery.NewProjection(dbConn, "DeliveryProjection")
	streamConsumer := stream.NewConsumer(validator, nil, proj, nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	// Step 1: Process Seq 1 and Seq 2
	ev1 := delivery.EventDeliveryCreated{ID: "evt-hist-1", DeliveryID: delID, DriverID: "d1", Seq: 1}
	p1, _ := json.Marshal(ev1)
	env1, _ := json.Marshal(map[string]interface{}{"event_id": "evt-hist-1", "aggregate_id": delID, "stream_position": int64(300401), "event_type": "DeliveryCreated", "payload": json.RawMessage(p1)})

	ev2 := delivery.EventDriverArrivedAtPickup{ID: "evt-hist-2", DeliveryID: delID, Seq: 2}
	p2, _ := json.Marshal(ev2)
	env2, _ := json.Marshal(map[string]interface{}{"event_id": "evt-hist-2", "aggregate_id": delID, "stream_position": int64(300402), "event_type": "DriverArrivedAtPickup", "payload": json.RawMessage(p2)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env1})
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env2})

	msgChan, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d1 := <-msgChan
	_ = rabbitConsumer.HandleDelivery(context.Background(), d1)
	d2 := <-msgChan
	_ = rabbitConsumer.HandleDelivery(context.Background(), d2)

	// Validator is now at seq=2
	last, _ := validator.Last(delID)
	if last != 2 {
		t.Fatalf("validator sequence expected 2, got %d", last)
	}

	// Step 2: Deliver historical exact duplicate of Seq 1
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env1})
	dHist := <-msgChan

	err := rabbitConsumer.HandleDelivery(context.Background(), dHist)
	if err != nil {
		t.Fatalf("historical exact duplicate should be ACKed cleanly, got: %v", err)
	}

	// Validator must NOT have regressed to 1!
	lastAfter, _ := validator.Last(delID)
	if lastAfter != 2 {
		t.Fatalf("validator sequence regressed to %d", lastAfter)
	}

	// Read model must still be at seq=2 (AT_PICKUP)
	var rmSeq int64
	var rmState string
	_ = dbConn.QueryRow("SELECT sequence, state FROM gate15b3_test.delivery_read_model WHERE delivery_id = $1", delID).Scan(&rmSeq, &rmState)
	if rmSeq != 2 || rmState != "AT_PICKUP" {
		t.Fatalf("read model regressed: seq=%d state=%s", rmSeq, rmState)
	}

	// DLQ must be empty
	qInspect, _ := topo.ch.QueueInspect(topo.dlqName)
	if qInspect.Messages != 0 {
		t.Fatalf("expected 0 messages in DLQ, got %d", qInspect.Messages)
	}

	t.Log("=== TEST 4 (DUPLICATA EXATA DE SEQUÊNCIA HISTÓRICA) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 5: EventID novo em sequência histórica -> Causal conflict para DLQ
// -----------------------------------------------------------------------------
func TestGate15B3_5_NewEventIDHistoricalSequence(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-5-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	validator := stream.NewSequenceValidator()
	proj := delivery.NewProjection(dbConn, "DeliveryProjection")
	streamConsumer := stream.NewConsumer(validator, nil, proj, nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	// Process Seq 1 and Seq 2
	ev1 := delivery.EventDeliveryCreated{ID: "evt-h5-1", DeliveryID: delID, DriverID: "d1", Seq: 1}
	p1, _ := json.Marshal(ev1)
	env1, _ := json.Marshal(map[string]interface{}{"event_id": "evt-h5-1", "aggregate_id": delID, "stream_position": int64(300501), "event_type": "DeliveryCreated", "payload": json.RawMessage(p1)})

	ev2 := delivery.EventDriverArrivedAtPickup{ID: "evt-h5-2", DeliveryID: delID, Seq: 2}
	p2, _ := json.Marshal(ev2)
	env2, _ := json.Marshal(map[string]interface{}{"event_id": "evt-h5-2", "aggregate_id": delID, "stream_position": int64(300502), "event_type": "DriverArrivedAtPickup", "payload": json.RawMessage(p2)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env1})
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env2})

	msgChan, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d1 := <-msgChan
	_ = rabbitConsumer.HandleDelivery(context.Background(), d1)
	d2 := <-msgChan
	_ = rabbitConsumer.HandleDelivery(context.Background(), d2)

	// Send NEW event_id for historical Seq 1
	evForged := delivery.EventDeliveryCreated{ID: "evt-h5-forged", DeliveryID: delID, DriverID: "hacker", Seq: 1}
	pForged, _ := json.Marshal(evForged)
	envForged, _ := json.Marshal(map[string]interface{}{"event_id": "evt-h5-forged", "aggregate_id": delID, "stream_position": int64(300503), "event_type": "DeliveryCreated", "payload": json.RawMessage(pForged)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envForged})
	dForged := <-msgChan

	err := rabbitConsumer.HandleDelivery(context.Background(), dForged)
	if err == nil {
		t.Fatal("expected causal conflict on new event_id for historical sequence, got nil")
	}

	// Must be routed to DLQ
	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", dlqCount)
	}

	t.Log("=== TEST 5 (EVENT_ID NOVO EM SEQUÊNCIA HISTÓRICA) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 6: Envelope EventID divergente do payload -> Poison message para DLQ
// -----------------------------------------------------------------------------
func TestGate15B3_6_EnvelopeEventIDDivergent(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-6-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	ev := delivery.EventDeliveryCreated{ID: "payload-id-123", DeliveryID: delID, Seq: 1}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":        "envelope-divergent-id", // DIVERGENT!
		"aggregate_id":    delID,
		"stream_position": int64(300601),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p),
	})

	validator := stream.NewSequenceValidator()
	streamConsumer := stream.NewConsumer(validator, nil, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgChan, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgChan

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err == nil {
		t.Fatal("expected poison message error for event_id mismatch, got nil")
	}

	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("expected 1 poison message in DLQ, got %d", dlqCount)
	}

	t.Log("=== TEST 6 (ENVELOPE EVENT_ID DIVERGENTE DO PAYLOAD) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 7: Envelope AggregateID divergente do payload -> Poison message para DLQ
// -----------------------------------------------------------------------------
func TestGate15B3_7_EnvelopeAggregateIDDivergent(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delIDPayload := uuid.New()
	delIDEnvelope := uuid.New() // DIVERGENT!
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-7-%s", delIDPayload))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	ev := delivery.EventDeliveryCreated{ID: "evt-7", DeliveryID: delIDPayload, Seq: 1}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-7",
		"aggregate_id":    delIDEnvelope, // DIVERGENT!
		"stream_position": int64(300701),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p),
	})

	validator := stream.NewSequenceValidator()
	streamConsumer := stream.NewConsumer(validator, nil, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgChan, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgChan

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err == nil {
		t.Fatal("expected poison message error for aggregate_id mismatch, got nil")
	}

	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("expected 1 poison message in DLQ, got %d", dlqCount)
	}

	t.Log("=== TEST 7 (ENVELOPE AGGREGATE_ID DIVERGENTE DO PAYLOAD) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 8: Envelope EventType divergente do payload -> Poison message para DLQ
// -----------------------------------------------------------------------------
func TestGate15B3_8_EnvelopeEventTypeDivergent(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-8-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// Payload is DeliveryCreated, but envelope says DriverArrivedAtPickup
	p, _ := json.Marshal(map[string]interface{}{
		"id":          "evt-8",
		"delivery_id": delID,
		"seq":         1,
		"event_type":  "DeliveryCreated",
	})
	envBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-8",
		"aggregate_id":    delID,
		"stream_position": int64(300801),
		"event_type":      "DriverArrivedAtPickup", // DIVERGENT!
		"payload":         json.RawMessage(p),
	})

	validator := stream.NewSequenceValidator()
	streamConsumer := stream.NewConsumer(validator, nil, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgChan, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgChan

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err == nil {
		t.Fatal("expected poison message error for event_type mismatch, got nil")
	}

	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("expected 1 poison message in DLQ, got %d", dlqCount)
	}

	t.Log("=== TEST 8 (ENVELOPE EVENT_TYPE DIVERGENTE DO PAYLOAD) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 9: Recuperação de Gap
// -----------------------------------------------------------------------------
func TestGate15B3_9_GapRecovery(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-9-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// Seed outbox_events with Seq 1 and Seq 2
	ev1 := delivery.EventDeliveryCreated{ID: "evt-g9-1", DeliveryID: delID, DriverID: "d1", Seq: 1}
	p1, _ := json.Marshal(ev1)
	ev2 := delivery.EventDriverArrivedAtPickup{ID: "evt-g9-2", DeliveryID: delID, Seq: 2}
	p2, _ := json.Marshal(ev2)

	_, err := dbConn.Exec(`
		INSERT INTO gate15b3_test.outbox_events (aggregate_id, aggregate_type, event_type, payload, status)
		VALUES ($1, 'delivery', 'DeliveryCreated', $2, 'PROCESSED'),
		       ($1, 'delivery', 'DriverArrivedAtPickup', $3, 'PROCESSED')
	`, delID, p1, p2)
	if err != nil {
		t.Fatalf("failed to seed outbox: %v", err)
	}

	// Setup consumer with empty in-memory validator
	validator := stream.NewSequenceValidator()
	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), delID)
	defer hubUnsub()

	rec := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), validator, delivery.NewProjection(dbConn, "DeliveryProjection"), liveHub)
	streamConsumer := stream.NewConsumer(validator, rec, delivery.NewProjection(dbConn, "DeliveryProjection"), liveHub)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	// Publish Seq 3 directly into queue (triggering gap recovery for 1 and 2)
	ev3 := delivery.EventDeliveryCompleted{ID: "evt-g9-3", DeliveryID: delID, Seq: 3}
	p3, _ := json.Marshal(ev3)
	env3, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-g9-3",
		"aggregate_id":    delID,
		"stream_position": int64(300903),
		"event_type":      "DeliveryCompleted",
		"payload":         json.RawMessage(p3),
	})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env3})
	msgChan, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d3 := <-msgChan

	err = rabbitConsumer.HandleDelivery(context.Background(), d3)
	if err != nil {
		t.Fatalf("gap recovery processing failed: %v", err)
	}

	// Read model must be DELIVERED at seq=3
	var rmSeq int64
	var rmState string
	_ = dbConn.QueryRow("SELECT sequence, state FROM gate15b3_test.delivery_read_model WHERE delivery_id = $1", delID).Scan(&rmSeq, &rmState)
	if rmSeq != 3 || rmState != "DELIVERED" {
		t.Fatalf("read model after gap recovery mismatch: seq=%d state=%s", rmSeq, rmState)
	}

	// LiveHub must have received notifications for all 3 recovered/applied events
	notifCount := 0
drainLoop:
	for {
		select {
		case <-hubSub:
			notifCount++
		case <-time.After(300 * time.Millisecond):
			break drainLoop
		}
	}
	if notifCount != 3 {
		t.Fatalf("expected 3 LiveHub notifications during gap recovery, got %d", notifCount)
	}

	t.Log("=== TEST 9 (RECUPERAÇÃO DE GAP) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 10: Cold restart e repetição de evento antigo
// -----------------------------------------------------------------------------
func TestGate15B3_10_ColdRestartAndHistoricalDuplicate(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3-10-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// Seed outbox with events 1, 2, 3
	ev1 := delivery.EventDeliveryCreated{ID: "evt-c10-1", DeliveryID: delID, DriverID: "d1", Seq: 1}
	p1, _ := json.Marshal(ev1)
	ev2 := delivery.EventDriverArrivedAtPickup{ID: "evt-c10-2", DeliveryID: delID, Seq: 2}
	p2, _ := json.Marshal(ev2)
	ev3 := delivery.EventDriverEnRouteToDropoff{ID: "evt-c10-3", DeliveryID: delID, Seq: 3, Lat: -23.55, Lng: -46.63}
	p3, _ := json.Marshal(ev3)

	_, _ = dbConn.Exec(`
		INSERT INTO gate15b3_test.outbox_events (aggregate_id, aggregate_type, event_type, payload, status)
		VALUES ($1, 'delivery', 'DeliveryCreated', $2, 'PROCESSED'),
		       ($1, 'delivery', 'DriverArrivedAtPickup', $3, 'PROCESSED'),
		       ($1, 'delivery', 'DriverEnRouteToDropoff', $4, 'PROCESSED')
	`, delID, p1, p2, p3)

	// Phase 1: Consumer 1 processes all 3 events
	val1 := stream.NewSequenceValidator()
	rec1 := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), val1, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	sc1 := stream.NewConsumer(val1, rec1, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rc1 := rabbitmq.NewConsumer(topo.ch, sc1)

	env1, _ := json.Marshal(map[string]interface{}{"event_id": "evt-c10-1", "aggregate_id": delID, "stream_position": int64(301001), "event_type": "DeliveryCreated", "payload": json.RawMessage(p1)})
	env2, _ := json.Marshal(map[string]interface{}{"event_id": "evt-c10-2", "aggregate_id": delID, "stream_position": int64(301002), "event_type": "DriverArrivedAtPickup", "payload": json.RawMessage(p2)})
	env3, _ := json.Marshal(map[string]interface{}{"event_id": "evt-c10-3", "aggregate_id": delID, "stream_position": int64(301003), "event_type": "DriverEnRouteToDropoff", "payload": json.RawMessage(p3)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env1})
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env2})
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env3})

	msgChan, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	_ = rc1.HandleDelivery(context.Background(), <-msgChan)
	_ = rc1.HandleDelivery(context.Background(), <-msgChan)
	_ = rc1.HandleDelivery(context.Background(), <-msgChan)

	// Phase 2: Consumer 1 is discarded (simulating cold restart).
	// Consumer 2 starts with empty in-memory validator and a fresh LiveHub
	val2 := stream.NewSequenceValidator()
	liveHub2 := stream.NewMemoryLiveHub()
	hubSub2, hubUnsub2 := liveHub2.Subscribe(context.Background(), delID)
	defer hubUnsub2()

	rec2 := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), val2, delivery.NewProjection(dbConn, "DeliveryProjection"), liveHub2)
	sc2 := stream.NewConsumer(val2, rec2, delivery.NewProjection(dbConn, "DeliveryProjection"), liveHub2)
	rc2 := rabbitmq.NewConsumer(topo.ch, sc2)

	// Now deliver a historical duplicate (Seq 2) followed by next legitimate event (Seq 4)
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env2})

	ev4 := delivery.EventDeliveryCompleted{ID: "evt-c10-4", DeliveryID: delID, Seq: 4}
	p4, _ := json.Marshal(ev4)
	env4, _ := json.Marshal(map[string]interface{}{"event_id": "evt-c10-4", "aggregate_id": delID, "stream_position": int64(301004), "event_type": "DeliveryCompleted", "payload": json.RawMessage(p4)})
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env4})

	// Process Seq 2 duplicate on Consumer 2
	dHist := <-msgChan
	if err := rc2.HandleDelivery(context.Background(), dHist); err != nil {
		t.Fatalf("cold restart handling of historical event failed: %v", err)
	}

	// Process Seq 4 on Consumer 2
	d4 := <-msgChan
	if err := rc2.HandleDelivery(context.Background(), d4); err != nil {
		t.Fatalf("processing event 4 failed: %v", err)
	}

	// Verify LiveHub only emitted notification for Seq 4 (zero for duplicate)
	notifCount := 0
drainLoop2:
	for {
		select {
		case notif := <-hubSub2:
			notifCount++
			if notif.DomainEvent.Sequence() != 4 {
				t.Fatalf("unexpected notification for sequence %d", notif.DomainEvent.Sequence())
			}
		case <-time.After(300 * time.Millisecond):
			break drainLoop2
		}
	}
	if notifCount != 1 {
		t.Fatalf("expected exactly 1 LiveHub notification (for seq 4), got %d", notifCount)
	}

	// Verify final read model sequence is 4
	var finalSeq int64
	_ = dbConn.QueryRow("SELECT sequence FROM gate15b3_test.delivery_read_model WHERE delivery_id = $1", delID).Scan(&finalSeq)
	if finalSeq != 4 {
		t.Fatalf("final read model sequence expected 4, got %d", finalSeq)
	}

	t.Log("=== TEST 10 (COLD RESTART E REPETIÇÃO DE EVENTO ANTIGO) APROVADO ===")
}
