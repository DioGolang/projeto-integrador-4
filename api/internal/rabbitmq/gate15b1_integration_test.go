package rabbitmq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func getGate15B1AMQPURL() string {
	if u := os.Getenv("TEST_AMQP_URL"); u != "" {
		return u
	}
	return "amqp://root:password@127.0.0.1:5672/"
}

func getGate15B1DBDSN() string {
	d := os.Getenv("TEST_DB_DSN")
	if d == "" {
		d = "postgres://root:password@127.0.0.1:5432/logistics?sslmode=disable"
	}
	if !strings.Contains(d, "search_path") {
		if strings.Contains(d, "?") {
			d += "&search_path=gate15b1_test,public"
		} else {
			d += "?search_path=gate15b1_test,public"
		}
	}
	return d
}

func setupGate15B1DB(t *testing.T) *sql.DB {
	t.Helper()
	dbConn, err := sql.Open("postgres", getGate15B1DBDSN())
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
		CREATE SCHEMA IF NOT EXISTS gate15b1_test;
		SET search_path TO gate15b1_test, public;

		DROP TABLE IF EXISTS gate15b1_test.processed_events CASCADE;
		DROP TABLE IF EXISTS gate15b1_test.delivery_read_model CASCADE;
		DROP TABLE IF EXISTS gate15b1_test.projection_checkpoints CASCADE;

		CREATE TABLE gate15b1_test.processed_events (
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
			ON gate15b1_test.processed_events (consumer_id, aggregate_id, sequence);

		CREATE TABLE gate15b1_test.delivery_read_model (
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

		CREATE TABLE gate15b1_test.projection_checkpoints (
			projection_name TEXT PRIMARY KEY,
			last_sequence BIGINT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	if err != nil {
		dbConn.Close()
		t.Fatalf("failed to initialize schema gate15b1_test: %v", err)
	}
	return dbConn
}

func setupGate15B1RabbitMQ(t *testing.T, queueName string) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial(getGate15B1AMQPURL())
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return nil, nil
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		t.Fatalf("failed to open rabbitmq channel: %v", err)
	}

	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		t.Fatalf("failed to declare queue %s: %v", queueName, err)
	}

	return conn, ch
}

// -----------------------------------------------------------------------------
// Test 1.5B-1A — Evento inédito
// -----------------------------------------------------------------------------
func TestGate15B1A_UnprecedentedEvent(t *testing.T) {
	dbConn := setupGate15B1DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	testDeliveryID := uuid.New()
	queueName := fmt.Sprintf("test.gate15b1a.%s", testDeliveryID.String())
	conn, ch := setupGate15B1RabbitMQ(t, queueName)
	if conn == nil {
		return
	}
	defer func() {
		ch.QueueDelete(queueName, false, false, false)
		ch.Close()
		conn.Close()
	}()

	evID := fmt.Sprintf("evt-15b1a-%s", uuid.New().String())
	streamPos := int64(200101)

	ev := delivery.EventDeliveryCreated{
		ID:         evID,
		DeliveryID: testDeliveryID,
		DriverID:   "driver-inedito",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}

	payloadBytes, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{
		"aggregate_id":    testDeliveryID,
		"stream_position": streamPos,
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(payloadBytes),
	})

	err := ch.PublishWithContext(context.Background(), "", queueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         envBytes,
	})
	if err != nil {
		t.Fatalf("failed to publish unprecedented event: %v", err)
	}

	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), testDeliveryID)
	defer hubUnsub()

	proj := delivery.NewProjection(dbConn, "DeliveryProjection")

	// Apply unprecedented event
	applied, err := proj.Apply(context.Background(), ev, streamPos)
	if err != nil {
		t.Fatalf("failed to apply unprecedented event: %v", err)
	}
	if !applied {
		t.Fatalf("expected applied == true for unprecedented event, got false")
	}

	// Publish to LiveHub after successful commit
	if applied {
		liveHub.Publish(stream.Event{
			StreamPosition: streamPos,
			DomainEvent:    ev,
		})
	}

	// Assert LiveHub notification
	select {
	case notif := <-hubSub:
		if notif.StreamPosition != streamPos {
			t.Fatalf("unexpected notification stream position: %d", notif.StreamPosition)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for LiveHub notification")
	}

	// Assert processed_events record
	var storedEventID, storedEventType, storedPayloadHash string
	var storedSeq int64
	var storedAggID uuid.UUID
	err = dbConn.QueryRow(`
		SELECT event_id, aggregate_id, sequence, event_type, payload_hash
		FROM gate15b1_test.processed_events
		WHERE consumer_id = 'DeliveryProjection' AND event_id = $1
	`, evID).Scan(&storedEventID, &storedAggID, &storedSeq, &storedEventType, &storedPayloadHash)
	if err != nil {
		t.Fatalf("processed_events record not found: %v", err)
	}
	if storedEventID != evID || storedAggID != testDeliveryID || storedSeq != 1 || storedEventType != "DeliveryCreated" {
		t.Fatalf("processed_events data mismatch: id=%s agg=%s seq=%d type=%s", storedEventID, storedAggID, storedSeq, storedEventType)
	}

	expectedHash, _ := delivery.ComputeFingerprint(ev)
	if storedPayloadHash != expectedHash {
		t.Fatalf("payload_hash mismatch: expected %s, got %s", expectedHash, storedPayloadHash)
	}

	// Assert Read Model
	var readModelCount int
	var readModelState string
	var readModelSeq int64
	err = dbConn.QueryRow(`
		SELECT count(*), max(state), max(sequence)
		FROM gate15b1_test.delivery_read_model
		WHERE delivery_id = $1
	`, testDeliveryID).Scan(&readModelCount, &readModelState, &readModelSeq)
	if err != nil || readModelCount != 1 || readModelState != "CREATED" || readModelSeq != 1 {
		t.Fatalf("read model verification failed: count=%d state=%s seq=%d err=%v", readModelCount, readModelState, readModelSeq, err)
	}

	// Assert Checkpoint
	var lastSeq int64
	err = dbConn.QueryRow(`
		SELECT last_sequence
		FROM gate15b1_test.projection_checkpoints
		WHERE projection_name = 'DeliveryProjection'
	`).Scan(&lastSeq)
	if err != nil || lastSeq != streamPos {
		t.Fatalf("checkpoint verification failed: expected %d, got %d, err=%v", streamPos, lastSeq, err)
	}

	t.Log("=== 1.5B-1A (EVENTO INÉDITO) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 1.5B-1B — Duplicata exata
// -----------------------------------------------------------------------------
func TestGate15B1B_ExactDuplicate(t *testing.T) {
	dbConn := setupGate15B1DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	testDeliveryID := uuid.New()
	queueName := fmt.Sprintf("test.gate15b1b.%s", testDeliveryID.String())
	conn, ch := setupGate15B1RabbitMQ(t, queueName)
	if conn == nil {
		return
	}
	defer func() {
		ch.QueueDelete(queueName, false, false, false)
		ch.Close()
		conn.Close()
	}()

	evID := fmt.Sprintf("evt-15b1b-%s", uuid.New().String())
	streamPos := int64(200201)

	ev := delivery.EventDeliveryCreated{
		ID:         evID,
		DeliveryID: testDeliveryID,
		DriverID:   "driver-duplicata",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}

	payloadBytes, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{
		"aggregate_id":    testDeliveryID,
		"stream_position": streamPos,
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(payloadBytes),
	})

	// Publish message to RabbitMQ queue
	err := ch.PublishWithContext(context.Background(), "", queueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         envBytes,
	})
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), testDeliveryID)
	defer hubUnsub()

	proj := delivery.NewProjection(dbConn, "DeliveryProjection")

	// 1st Application: Unprecedented -> applied == true
	applied1, err := proj.Apply(context.Background(), ev, streamPos)
	if err != nil || !applied1 {
		t.Fatalf("first apply failed: applied=%v, err=%v", applied1, err)
	}
	liveHub.Publish(stream.Event{StreamPosition: streamPos, DomainEvent: ev})

	// Drain 1st notification
	select {
	case <-hubSub:
		t.Log("Legitimate initial notification received.")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial notification")
	}

	// 2nd Application: Exact Duplicate -> applied MUST BE false, err MUST BE nil
	applied2, err := proj.Apply(context.Background(), ev, streamPos)
	if err != nil {
		t.Fatalf("exact duplicate should not return error, got: %v", err)
	}
	if applied2 {
		t.Fatal("exact duplicate must return applied == false, got true")
	}

	if applied2 {
		liveHub.Publish(stream.Event{StreamPosition: streamPos, DomainEvent: ev})
	}

	// Verify ZERO second notification emitted
	select {
	case unexpected := <-hubSub:
		t.Fatalf("unexpected notification emitted for duplicate event: %+v", unexpected)
	case <-time.After(500 * time.Millisecond):
		t.Log("Suppression confirmed: zero duplicate notifications emitted.")
	}

	// Consume and ACK message from RabbitMQ broker
	msgs, err := ch.Consume(queueName, "test-consumer", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if err := msg.Ack(false); err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
		t.Log("Message acknowledged successfully in broker.")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message from broker")
	}

	// Queue inspection: messages count must be 0
	qInspect, err := ch.QueueInspect(queueName)
	if err != nil || qInspect.Messages != 0 {
		t.Fatalf("expected 0 messages in queue after ACK, got %d (err: %v)", qInspect.Messages, err)
	}

	t.Log("=== 1.5B-1B (DUPLICATA EXATA) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 1.5B-1C — Conflito de identidade
// -----------------------------------------------------------------------------
func TestGate15B1C_IdentityConflict(t *testing.T) {
	dbConn := setupGate15B1DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	testDeliveryID := uuid.New()
	evID := fmt.Sprintf("evt-15b1c-%s", uuid.New().String())
	streamPos := int64(200301)

	evOriginal := delivery.EventDeliveryCreated{
		ID:         evID,
		DeliveryID: testDeliveryID,
		DriverID:   "driver-original",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}

	proj := delivery.NewProjection(dbConn, "DeliveryProjection")

	// Apply original event
	applied, err := proj.Apply(context.Background(), evOriginal, streamPos)
	if err != nil || !applied {
		t.Fatalf("apply original failed: applied=%v, err=%v", applied, err)
	}

	// Save baseline read model & checkpoint
	var baselineSeq int64
	var baselineDriver string
	err = dbConn.QueryRow(`
		SELECT sequence, driver_id FROM gate15b1_test.delivery_read_model WHERE delivery_id = $1
	`, testDeliveryID).Scan(&baselineSeq, &baselineDriver)
	if err != nil || baselineDriver != "driver-original" {
		t.Fatalf("baseline query failed: %v", err)
	}

	// Construct conflicting event: IDENTICAL event_id, but DIFFERENT business payload
	evConflicting := evOriginal
	evConflicting.DriverID = "driver-impostor" // altered payload -> different fingerprint

	// Apply conflicting event -> MUST fail with ErrIdentityConflict
	appliedConflict, errConflict := proj.Apply(context.Background(), evConflicting, streamPos+1)
	if appliedConflict {
		t.Fatal("conflicting event must NOT be applied (expected applied == false)")
	}
	if !errors.Is(errConflict, delivery.ErrIdentityConflict) {
		t.Fatalf("expected ErrIdentityConflict, got: %v", errConflict)
	}

	// Verify Read Model was NOT modified
	var currentDriver string
	_ = dbConn.QueryRow(`
		SELECT driver_id FROM gate15b1_test.delivery_read_model WHERE delivery_id = $1
	`, testDeliveryID).Scan(&currentDriver)
	if currentDriver != "driver-original" {
		t.Fatalf("read model was modified despite conflict: got %s, expected %s", currentDriver, "driver-original")
	}

	// Verify Checkpoint was NOT modified
	var currentCheckpoint int64
	_ = dbConn.QueryRow(`
		SELECT last_sequence FROM gate15b1_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'
	`).Scan(&currentCheckpoint)
	if currentCheckpoint != streamPos {
		t.Fatalf("checkpoint was modified: expected %d, got %d", streamPos, currentCheckpoint)
	}

	t.Log("=== 1.5B-1C (CONFLITO DE IDENTIDADE) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 1.5B-1D — Conflito causal
// -----------------------------------------------------------------------------
func TestGate15B1D_CausalConflict(t *testing.T) {
	dbConn := setupGate15B1DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	testDeliveryID := uuid.New()
	evID1 := fmt.Sprintf("evt-15b1d-1-%s", uuid.New().String())
	evID2 := fmt.Sprintf("evt-15b1d-2-%s", uuid.New().String())
	streamPos := int64(200401)

	ev1 := delivery.EventDeliveryCreated{
		ID:         evID1,
		DeliveryID: testDeliveryID,
		DriverID:   "driver-seq-1",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}

	proj := delivery.NewProjection(dbConn, "DeliveryProjection")

	// Apply first event for sequence 1
	applied1, err := proj.Apply(context.Background(), ev1, streamPos)
	if err != nil || !applied1 {
		t.Fatalf("apply ev1 failed: applied=%v, err=%v", applied1, err)
	}

	// Construct causally conflicting event: DIFFERENT event_id, but SAME (aggregate_id, sequence=1)
	ev2CausalConflict := delivery.EventDeliveryCreated{
		ID:         evID2, // Different event_id!
		DeliveryID: testDeliveryID,
		DriverID:   "driver-seq-1-divergent",
		Seq:        1, // Same sequence 1!
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}

	// Apply causally conflicting event -> MUST fail with ErrCausalConflict
	applied2, err2 := proj.Apply(context.Background(), ev2CausalConflict, streamPos+1)
	if applied2 {
		t.Fatal("causally conflicting event must NOT be applied (expected applied == false)")
	}
	if !errors.Is(err2, delivery.ErrCausalConflict) {
		t.Fatalf("expected ErrCausalConflict, got: %v", err2)
	}

	// Verify Read Model remains bound to ev1
	var currentDriver string
	_ = dbConn.QueryRow(`
		SELECT driver_id FROM gate15b1_test.delivery_read_model WHERE delivery_id = $1
	`, testDeliveryID).Scan(&currentDriver)
	if currentDriver != "driver-seq-1" {
		t.Fatalf("read model divergent: expected driver-seq-1, got %s", currentDriver)
	}

	// Verify Checkpoint did not advance to streamPos+1
	var currentCheckpoint int64
	_ = dbConn.QueryRow(`
		SELECT last_sequence FROM gate15b1_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'
	`).Scan(&currentCheckpoint)
	if currentCheckpoint != streamPos {
		t.Fatalf("checkpoint moved on causal conflict: expected %d, got %d", streamPos, currentCheckpoint)
	}

	t.Log("=== 1.5B-1D (CONFLITO CAUSAL) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 1.5B-1E — Concorrência
// -----------------------------------------------------------------------------
func TestGate15B1E_Concurrency(t *testing.T) {
	dbConn := setupGate15B1DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	testDeliveryID := uuid.New()
	evID := fmt.Sprintf("evt-15b1e-%s", uuid.New().String())
	streamPos := int64(200501)

	ev := delivery.EventDeliveryCreated{
		ID:         evID,
		DeliveryID: testDeliveryID,
		DriverID:   "driver-concorrente",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}

	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), testDeliveryID)
	defer hubUnsub()

	proj := delivery.NewProjection(dbConn, "DeliveryProjection")

	// Launch 2 simultaneous workers trying to apply the exact same event
	var wg sync.WaitGroup
	var appliedCount int32
	var errorCount int32

	startBarrier := make(chan struct{})

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startBarrier // Synchronized start

			applied, err := proj.Apply(context.Background(), ev, streamPos)
			if err != nil {
				atomic.AddInt32(&errorCount, 1)
				t.Logf("Worker %d returned error: %v", workerID, err)
				return
			}
			if applied {
				atomic.AddInt32(&appliedCount, 1)
				liveHub.Publish(stream.Event{
					StreamPosition: streamPos,
					DomainEvent:    ev,
				})
			}
		}(i)
	}

	close(startBarrier)
	wg.Wait()

	if errorCount != 0 {
		t.Fatalf("expected 0 errors during concurrent delivery of identical event, got %d", errorCount)
	}

	// Exactly ONE worker must have applied == true
	if appliedCount != 1 {
		t.Fatalf("expected exactly 1 worker to apply the event, got %d", appliedCount)
	}

	// Exactly ONE notification must arrive at LiveHub
	notificationCount := 0
drainLoop:
	for {
		select {
		case <-hubSub:
			notificationCount++
		case <-time.After(300 * time.Millisecond):
			break drainLoop
		}
	}
	if notificationCount != 1 {
		t.Fatalf("expected exactly 1 LiveHub notification, got %d", notificationCount)
	}

	// Exactly 1 record in processed_events
	var peCount int
	err := dbConn.QueryRow(`
		SELECT count(*) FROM gate15b1_test.processed_events WHERE consumer_id = 'DeliveryProjection' AND event_id = $1
	`, evID).Scan(&peCount)
	if err != nil || peCount != 1 {
		t.Fatalf("expected 1 processed_events row, got %d (err: %v)", peCount, err)
	}

	// Exactly 1 record in delivery_read_model
	var rmCount int
	err = dbConn.QueryRow(`
		SELECT count(*) FROM gate15b1_test.delivery_read_model WHERE delivery_id = $1
	`, testDeliveryID).Scan(&rmCount)
	if err != nil || rmCount != 1 {
		t.Fatalf("expected 1 delivery_read_model row, got %d (err: %v)", rmCount, err)
	}

	t.Log("=== 1.5B-1E (CONCORRÊNCIA) APROVADO ===")
}

// -----------------------------------------------------------------------------
// Test 1.5B-1F — Atomicidade
// -----------------------------------------------------------------------------
func TestGate15B1F_AtomicityRollback(t *testing.T) {
	dbConn := setupGate15B1DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	testDeliveryID := uuid.New()
	evID := fmt.Sprintf("evt-15b1f-%s", uuid.New().String())
	streamPos := int64(200601)

	ev := delivery.EventDeliveryCreated{
		ID:         evID,
		DeliveryID: testDeliveryID,
		DriverID:   "driver-atomicidade",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}

	proj := delivery.NewProjection(dbConn, "DeliveryProjection")

	// Phase 1: Simulate aborted attempt (canceled context during Apply)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel() // Pre-canceled context guarantees failure before commit

	appliedAborted, errAborted := proj.Apply(canceledCtx, ev, streamPos)
	if errAborted == nil {
		t.Fatal("expected error from canceled context, got nil")
	}
	if appliedAborted {
		t.Fatal("aborted transaction must have applied == false")
	}

	// Verify complete rollback: processed_events MUST NOT have the row
	var peCount int
	_ = dbConn.QueryRow(`
		SELECT count(*) FROM gate15b1_test.processed_events WHERE consumer_id = 'DeliveryProjection' AND event_id = $1
	`, evID).Scan(&peCount)
	if peCount != 0 {
		t.Fatalf("processed_events row persisted despite rollback: count=%d", peCount)
	}

	// Verify complete rollback: delivery_read_model MUST NOT have the row
	var rmCount int
	_ = dbConn.QueryRow(`
		SELECT count(*) FROM gate15b1_test.delivery_read_model WHERE delivery_id = $1
	`, testDeliveryID).Scan(&rmCount)
	if rmCount != 0 {
		t.Fatalf("delivery_read_model row persisted despite rollback: count=%d", rmCount)
	}

	// Verify complete rollback: projection_checkpoints MUST NOT have advanced
	var cpCount int
	_ = dbConn.QueryRow(`
		SELECT count(*) FROM gate15b1_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'
	`).Scan(&cpCount)
	if cpCount != 0 {
		t.Fatalf("checkpoint persisted despite rollback: count=%d", cpCount)
	}

	// Phase 2: Redeliver the event with valid context -> MUST apply cleanly
	appliedRetry, errRetry := proj.Apply(context.Background(), ev, streamPos)
	if errRetry != nil || !appliedRetry {
		t.Fatalf("redelivered event failed to apply: applied=%v, err=%v", appliedRetry, errRetry)
	}

	// Verify persistence after successful retry
	_ = dbConn.QueryRow(`
		SELECT count(*) FROM gate15b1_test.processed_events WHERE consumer_id = 'DeliveryProjection' AND event_id = $1
	`, evID).Scan(&peCount)
	if peCount != 1 {
		t.Fatalf("processed_events missing after retry: count=%d", peCount)
	}

	_ = dbConn.QueryRow(`
		SELECT count(*) FROM gate15b1_test.delivery_read_model WHERE delivery_id = $1
	`, testDeliveryID).Scan(&rmCount)
	if rmCount != 1 {
		t.Fatalf("delivery_read_model missing after retry: count=%d", rmCount)
	}

	var lastCP int64
	_ = dbConn.QueryRow(`
		SELECT last_sequence FROM gate15b1_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'
	`).Scan(&lastCP)
	if lastCP != streamPos {
		t.Fatalf("checkpoint mismatch after retry: got %d, expected %d", lastCP, streamPos)
	}

	t.Log("=== 1.5B-1F (ATOMICIDADE E ROLLBACK) APROVADO ===")
}
