package db_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func getTestDSN() string {
	if dsn := os.Getenv("TEST_DB_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://root:password@127.0.0.1:5432/logistics?sslmode=disable"
}

func setupProcessedEventsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbConn, err := sql.Open("postgres", getTestDSN())
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	if err := dbConn.Ping(); err != nil {
		t.Skipf("postgres ping failed: %v", err)
		return nil
	}
	return dbConn
}

// TestProcessedEvents_ConstraintsContract verifies the relational constraints
// mandated for Gate 1.5B-0 on processed_events table:
// 1. Primary key (consumer_id, event_id) prevents duplicate event_id for the same consumer.
// 2. Unique constraint (consumer_id, aggregate_id, sequence) prevents causal divergence (two different event_ids for the same aggregate sequence).
// 3. Columns aggregate_id, sequence, event_type, payload_hash exist and are non-null.
func TestProcessedEvents_ConstraintsContract(t *testing.T) {
	dbConn := setupProcessedEventsTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testConsumerID := fmt.Sprintf("test-consumer-%s", uuid.New().String())
	aggID := uuid.New()
	evID1 := fmt.Sprintf("evt-contract-1-%s", uuid.New().String())
	evID2 := fmt.Sprintf("evt-contract-2-%s", uuid.New().String())
	hash1 := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	q := db.New(dbConn)

	// Test 1: Insert valid processed event with all mandated fields
	err := q.InsertProcessedEvent(ctx, db.InsertProcessedEventParams{
		ConsumerID:  testConsumerID,
		EventID:     evID1,
		AggregateID: aggID,
		Sequence:    1,
		EventType:   "DeliveryCreated",
		PayloadHash: hash1,
	})
	if err != nil {
		t.Fatalf("failed to insert initial processed event: %v", err)
	}

	// Read back and verify all fields are persisted
	pe, err := q.GetProcessedEvent(ctx, db.GetProcessedEventParams{
		ConsumerID: testConsumerID,
		EventID:    evID1,
	})
	if err != nil {
		t.Fatalf("failed to query processed event: %v", err)
	}
	if pe.ConsumerID != testConsumerID || pe.EventID != evID1 || pe.AggregateID != aggID || pe.Sequence != 1 || pe.EventType != "DeliveryCreated" || pe.PayloadHash != hash1 {
		t.Fatalf("persisted processed event mismatch: %+v", pe)
	}

	// Test 2: Primary Key violation - inserting same (consumer_id, event_id) must fail
	err = q.InsertProcessedEvent(ctx, db.InsertProcessedEventParams{
		ConsumerID:  testConsumerID,
		EventID:     evID1, // same event_id
		AggregateID: aggID,
		Sequence:    2,
		EventType:   "DriverArrivedAtPickup",
		PayloadHash: "different-hash",
	})
	if err == nil {
		t.Fatal("expected primary key violation when inserting duplicate (consumer_id, event_id), got nil")
	}
	t.Logf("Primary Key constraint confirmed: duplicate event_id rejected (%v)", err)

	// Test 3: Causal Uniqueness violation - inserting different event_id for same (consumer_id, aggregate_id, sequence) must fail
	err = q.InsertProcessedEvent(ctx, db.InsertProcessedEventParams{
		ConsumerID:  testConsumerID,
		EventID:     evID2, // different event_id
		AggregateID: aggID,
		Sequence:    1, // same sequence 1 for aggID!
		EventType:   "DeliveryCreatedConflicting",
		PayloadHash: "conflicting-hash",
	})
	if err == nil {
		t.Fatal("expected unique constraint violation on (consumer_id, aggregate_id, sequence), got nil")
	}
	t.Logf("Causal Unique constraint confirmed: conflicting event for identical sequence rejected (%v)", err)

	// Test 4: Different consumer can process the same event_id without collision
	otherConsumerID := fmt.Sprintf("other-consumer-%s", uuid.New().String())
	err = q.InsertProcessedEvent(ctx, db.InsertProcessedEventParams{
		ConsumerID:  otherConsumerID,
		EventID:     evID1,
		AggregateID: aggID,
		Sequence:    1,
		EventType:   "DeliveryCreated",
		PayloadHash: hash1,
	})
	if err != nil {
		t.Fatalf("independent consumer should be able to process event: %v", err)
	}
}
