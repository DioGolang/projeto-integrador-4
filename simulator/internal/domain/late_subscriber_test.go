package domain

import (
	"context"
	"testing"
	"time"
)

// NaiveLiveStream represents a pub/sub broker like RabbitMQ where messages
// are routed to active subscribers. If a consumer is not subscribed, it misses the message.
type NaiveLiveStream struct {
	subscribers []chan DomainEvent
}

func (s *NaiveLiveStream) Publish(event DomainEvent) {
	for _, sub := range s.subscribers {
		select {
		case sub <- event:
		default: // non-blocking for safety
		}
	}
}

func (s *NaiveLiveStream) Subscribe() <-chan DomainEvent {
	ch := make(chan DomainEvent, 10)
	s.subscribers = append(s.subscribers, ch)
	return ch
}

// NaiveSubscriber implements the flawed protocol: Replay -> Subscribe
type NaiveSubscriber struct {
	store    EventStore
	live     *NaiveLiveStream
	State    *Delivery
}

func NewNaiveSubscriber(store EventStore, live *NaiveLiveStream) *NaiveSubscriber {
	return &NaiveSubscriber{
		store: store,
		live:  live,
		State: &Delivery{Sequence: 0},
	}
}

func (n *NaiveSubscriber) Bootstrap(ctx context.Context, aggregateID string, pauseCh chan struct{}) {
	// 1. Fetch History
	events, _ := n.store.LoadEvents(ctx, aggregateID, 0)
	for _, ev := range events {
		n.State.Sequence = ev.Sequence()
	}

	// 2. Simulate delay (the critical race window)
	if pauseCh != nil {
		<-pauseCh
	}

	// 3. Subscribe to Live Events
	ch := n.live.Subscribe()

	go func() {
		for ev := range ch {
			n.State.Sequence = ev.Sequence()
		}
	}()
}

// This test intentionally demonstrates the fatal flaw in the Naive Handoff Protocol.
func TestLateSubscriber_NaiveProtocol_MissesEventsDuringHandoff(t *testing.T) {
	ctx := context.Background()
	
	// Pre-fill history with 1, 2, 3
	store := NewMemoryEventStore()
	store.events["D1"] = []DomainEvent{
		EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1},
		EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2},
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3},
	}

	live := &NaiveLiveStream{}
	consumer := NewNaiveSubscriber(store, live)

	pauseCh := make(chan struct{})

	// Start consumer in a goroutine so it pauses
	go consumer.Bootstrap(ctx, "D1", pauseCh)

	// Wait briefly to ensure Bootstrap hits the pause
	time.Sleep(50 * time.Millisecond)

	// Producer publishes seq 4 WHILE the consumer is in the gap!
	live.Publish(EventDeliveryCompleted{ID: "ev-4", DeliveryID: "D1", Seq: 4})

	// Unpause the consumer to let it subscribe
	close(pauseCh)

	// Wait briefly to allow subscription and any potential processing
	time.Sleep(50 * time.Millisecond)

	if consumer.State.Sequence != 4 {
		t.Fatalf("EXPECTED EXPECTED EXPECTED FAILURE: consumer missed event 4! Current sequence: %d. Naive handoff protocol is broken.", consumer.State.Sequence)
	}
}
