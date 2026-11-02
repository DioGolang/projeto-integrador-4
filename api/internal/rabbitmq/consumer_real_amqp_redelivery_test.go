package rabbitmq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TestConsumer_RealRabbitMQ_UnackedChannelCrashRedelivery proves real broker-driven
// redelivery and idempotent absorption under actual channel/connection abrupt closure.
func TestConsumer_RealRabbitMQ_UnackedChannelCrashRedelivery(t *testing.T) {
	amqpURL := os.Getenv("TEST_AMQP_URL")
	if amqpURL == "" {
		amqpURL = os.Getenv("RABBITMQ_URL")
	}
	if amqpURL == "" {
		amqpURL = "amqp://root:password@rabbitmq:5672/"
	}

	dbDSN := os.Getenv("TEST_DB_DSN")
	if dbDSN == "" {
		dbDSN = "postgres://root:password@postgres:5432/logistics?sslmode=disable"
	}

	// 1. Connect to Real PostgreSQL
	dbConn, err := sql.Open("postgres", dbDSN)
	if err != nil {
		t.Skipf("Skipping real integration test: cannot connect to Postgres: %v", err)
		return
	}
	defer dbConn.Close()

	if err := dbConn.Ping(); err != nil {
		t.Skipf("Skipping real integration test: Postgres ping failed: %v", err)
		return
	}

	// 2. Connect to Real RabbitMQ
	setupConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skipf("Skipping real integration test: cannot connect to RabbitMQ: %v", err)
		return
	}
	defer setupConn.Close()

	setupCh, err := setupConn.Channel()
	if err != nil {
		t.Fatalf("failed to open setup channel: %v", err)
	}
	defer setupCh.Close()

	// 3. Declare isolated temporary queue (durable: true required by RabbitMQ 4.3+)
	testDeliveryID := uuid.New()
	testQueueName := fmt.Sprintf("test.redelivery.%s", testDeliveryID.String())
	_, err = setupCh.QueueDeclare(testQueueName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare test queue: %v", err)
	}
	defer setupCh.QueueDelete(testQueueName, false, false, false)

	// 4. Publish real DeliveryCreated envelope into the test queue
	payloadBytes, _ := json.Marshal(delivery.EventDeliveryCreated{
		ID:         uuid.New().String(),
		DeliveryID: testDeliveryID,
		DriverID:   "driver-crash-test",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	})

	envBytes, _ := json.Marshal(map[string]interface{}{
		"aggregate_id":    testDeliveryID,
		"stream_position": 999901,
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(payloadBytes),
	})

	err = setupCh.PublishWithContext(context.Background(), "", testQueueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         envBytes,
		DeliveryMode: amqp.Transient,
	})
	if err != nil {
		t.Fatalf("failed to publish initial message: %v", err)
	}

	// -------------------------------------------------------------------------
	// PHASE 1: Consumer 1 receives message, commits to DB, but CRASHES before ACK
	// -------------------------------------------------------------------------
	t.Log("Phase 1: Starting Consumer 1 to receive message and simulate unacked crash...")
	conn1, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}
	ch1, err := conn1.Channel()
	if err != nil {
		t.Fatalf("failed to open ch1: %v", err)
	}

	msgs1, err := ch1.Consume(testQueueName, "consumer-1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consume on ch1: %v", err)
	}

	select {
	case d1 := <-msgs1:
		if d1.Redelivered {
			t.Fatalf("expected d1.Redelivered == false on first delivery, got true")
		}
		t.Logf("Consumer 1 received delivery (redelivered=%v). Applying to DB...", d1.Redelivered)

		// Apply directly to real projection in DB (simulating successful tx.Commit)
		proj := delivery.NewProjection(dbConn, "DeliveryProjectionTest")
		var createdEv delivery.EventDeliveryCreated
		_ = json.Unmarshal(payloadBytes, &createdEv)
		if _, err := proj.Apply(context.Background(), createdEv, 999901); err != nil {
			t.Fatalf("failed to apply projection: %v", err)
		}

		t.Log("DB commit completed. Now abruptly CLOSING connection WITHOUT ACK to simulate crash...")
		_ = ch1.Close()
		_ = conn1.Close()

	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for delivery on consumer 1")
	}

	// Verify state in DB after Phase 1 commit
	var countPhase1 int
	var statePhase1 string
	var seqPhase1 int64
	err = dbConn.QueryRow("SELECT count(*), COALESCE(max(state), ''), COALESCE(max(sequence), 0) FROM delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&countPhase1, &statePhase1, &seqPhase1)
	if err != nil {
		t.Fatalf("failed to query db after phase 1: %v", err)
	}
	if countPhase1 != 1 || statePhase1 != "CREATED" || seqPhase1 != 1 {
		t.Fatalf("unexpected DB state after Phase 1: count=%d, state=%s, seq=%d", countPhase1, statePhase1, seqPhase1)
	}
	t.Logf("Phase 1 DB verified: count=%d, state=%s, seq=%d", countPhase1, statePhase1, seqPhase1)

	// -------------------------------------------------------------------------
	// PHASE 2 & 3: Broker redelivers with redelivered=true to Consumer 2
	// -------------------------------------------------------------------------
	t.Log("Phase 2: Connecting Consumer 2. Broker must redeliver with redelivered=true...")
	conn2, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("failed to open ch2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(testQueueName, "consumer-2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consume on ch2: %v", err)
	}

	select {
	case d2 := <-msgs2:
		// PROVE REAL BROKER REDELIVERY:
		if !d2.Redelivered {
			t.Fatalf("CRITICAL: expected d2.Redelivered == true from broker after unacked crash, got false!")
		}
		t.Logf("SUCCESS: Broker delivered message with redelivered=%v!", d2.Redelivered)

		// Set up real stream processor and consumer
		validator := stream.NewSequenceValidator()
		// Notice: validator in memory of Consumer 2 is cold (rebooted).
		// When Validate sees seq=1, it allows bootstrap and executes projection with ON CONFLICT DO NOTHING!
		proj2 := delivery.NewProjection(dbConn, "DeliveryProjectionTest")
		streamConsumer := stream.NewConsumer(validator, nil, proj2, nil)
		rabbitmqConsumer := rabbitmq.NewConsumer(ch2, streamConsumer)

		err := rabbitmqConsumer.HandleDelivery(context.Background(), d2)
		if err != nil {
			t.Fatalf("HandleDelivery failed on redelivered message: %v", err)
		}
		t.Log("Consumer 2 successfully handled and ACKed the redelivered message.")

	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for broker redelivery on consumer 2")
	}

	// -------------------------------------------------------------------------
	// PHASE 4: Final DB and Queue Verification
	// -------------------------------------------------------------------------
	var countFinal int
	var stateFinal string
	var seqFinal int64
	err = dbConn.QueryRow("SELECT count(*), COALESCE(max(state), ''), COALESCE(max(sequence), 0) FROM delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&countFinal, &stateFinal, &seqFinal)
	if err != nil {
		t.Fatalf("failed to query db after phase 4: %v", err)
	}

	if countFinal != 1 {
		t.Fatalf("IDEMPOTENCY FAILURE: expected exactly 1 row, got %d", countFinal)
	}
	if stateFinal != "CREATED" || seqFinal != 1 {
		t.Fatalf("IDEMPOTENCY FAILURE: state corrupted: state=%s, seq=%d", stateFinal, seqFinal)
	}

	// Clean up test row from read model
	_, _ = dbConn.Exec("DELETE FROM delivery_read_model WHERE delivery_id = $1", testDeliveryID)

	t.Logf("TEST PASSED: Real broker redelivery verified! count=%d, state=%s, seq=%d", countFinal, stateFinal, seqFinal)
}
