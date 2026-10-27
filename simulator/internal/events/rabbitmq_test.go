package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/outbox"
	amqp "github.com/rabbitmq/amqp091-go"
)

// A mocked repository that simulates crashing the program after publishing to RabbitMQ
// but before the database transaction is committed (i.e. MarkPublished).
type CrashableRepository struct {
	outbox.Repository
	CrashOnMark string
}

func (r *CrashableRepository) MarkPublished(ctx context.Context, eventID string) error {
	if r.CrashOnMark == eventID {
		return errors.New("CRASH: process died before marking as published in DB")
	}
	return r.Repository.MarkPublished(ctx, eventID)
}

// In-memory mock of outbox DB for this test to isolate the network mechanics
type MemRepository struct {
	events []outbox.OutboxEvent
}

func (m *MemRepository) FetchPending(ctx context.Context, limit int) ([]outbox.OutboxEvent, error) {
	var pending []outbox.OutboxEvent
	for _, e := range m.events {
		if e.PublishedAt == nil {
			pending = append(pending, e)
		}
	}
	return pending, nil
}
func (m *MemRepository) MarkPublished(ctx context.Context, eventID string) error {
	for i, e := range m.events {
		if e.ID == eventID {
			t := time.Now()
			m.events[i].PublishedAt = &t
		}
	}
	return nil
}
func (m *MemRepository) MarkFailed(ctx context.Context, eventID string, attemptError error, nextAttemptAt time.Time) error {
	return nil
}
func (m *MemRepository) MarkDeadLetter(ctx context.Context, eventID string, attemptError error) error {
	return nil
}

func setupTestRabbit(t *testing.T) (*amqp.Connection, *amqp.Channel, string) {
	conn, err := amqp.Dial("amqp://root:password@localhost:5672/")
	if err != nil {
		t.Skipf("RabbitMQ not available: %v", err)
	}
	
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	
	exchangeName := "test_events_exchange"
	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("failed to declare exchange: %v", err)
	}

	return conn, ch, exchangeName
}

func TestRabbitMQ_AtLeastOncePublication(t *testing.T) {
	conn, ch, exchange := setupTestRabbit(t)
	if conn == nil {
		return // skipped
	}
	defer conn.Close()
	defer ch.Close()

	ctx := context.Background()

	// 1. Setup our RabbitMQ Queue bound to our Exchange
	q, err := ch.QueueDeclare("test_consumer_queue", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	// We bind to all delivery events
	err = ch.QueueBind(q.Name, "delivery.*", exchange, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	
	// Purge queue to ensure clean state
	ch.QueuePurge(q.Name, false)

	// 2. Setup the Relay with our crashable mock DB
	memDB := &MemRepository{
		events: []outbox.OutboxEvent{
			{
				ID: "ev-crash",
				AggregateType: "delivery",
				AggregateID: "D1",
				Sequence: 1,
				StreamPosition: 100,
				EventType: "DeliveryCompleted",
				Payload: []byte("{}"),
			},
		},
	}
	
	crashDB := &CrashableRepository{
		Repository: memDB,
		CrashOnMark: "ev-crash",
	}

	publisher := NewRabbitMQPublisher(ch, exchange)
	
	// Simple retry policy mock
	policy := &outbox.FixedPolicy{Delay: 0}
	failurePolicy := &outbox.NeverGiveUpPolicy{}
	
	relay := outbox.NewRelay(crashDB, publisher, policy, failurePolicy)

	// 3. RUN 1: The Crash Window
	// The relay will fetch "ev-crash", publish it to RabbitMQ, and then CRASH during MarkPublished.
	_, err = relay.Process(ctx, 10)
	if err == nil || !strings.Contains(err.Error(), "CRASH") {
		t.Fatalf("expected simulated crash, got: %v", err)
	}

	// 4. RUN 2: Recovery
	// The process restarts. We use the stable DB (no crash) to represent a rebooted healthy process.
	relay2 := outbox.NewRelay(memDB, publisher, policy, failurePolicy)
	_, err = relay2.Process(ctx, 10)
	if err != nil {
		t.Fatalf("unexpected error on run 2: %v", err)
	}

	// 5. Assert RabbitMQ contains duplicates!
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	receivedCount := 0
	
	// Drain messages
	for i := 0; i < 2; i++ {
		select {
		case msg := <-msgs:
			var body RabbitMQMessage
			json.Unmarshal(msg.Body, &body)
			if body.EventID == "ev-crash" {
				receivedCount++
			}
		case <-time.After(200 * time.Millisecond):
			break
		}
	}

	if receivedCount != 2 {
		t.Fatalf("expected exactly 2 messages in RabbitMQ due to crash, got %d", receivedCount)
	}
	t.Log("6E.7.10: Proved at-least-once publication. Crash after publish resulted in RabbitMQ receiving duplicate events.")
}
