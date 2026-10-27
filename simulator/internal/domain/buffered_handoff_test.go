package domain

import (
	"context"
	"testing"
	"time"
)



// Start strictly implements the deterministic protocol.
// 1. SUBSCRIBE / BUFFER
// 2. CAPTURE POSITION
// 3. REPLAY THROUGH POSITION
// 4. DRAIN BUFFER
// 5. VALIDATE + APPLY (delegated to LiveConsumer)
// 6. LIVE

func setupBufferedHandoff() (*MemoryStreamBroker, *LiveConsumer, *MemoryEventStore) {
	broker := NewMemoryStreamBroker()
	
	// The GapRecoverer relies on EventStore, which is conceptually identical 
	// to the StreamStore in this test, but indexed by aggregate.
	// We'll wire a MemoryEventStore and keep it synced with the broker for simplicity.
	store := NewMemoryEventStore()
	recoverer := NewGapRecoverer(store)
	
	delivery := NewDelivery("D1", "DriverX", Coordinate{1,1}, Coordinate{2,2})
	validator := NewSequenceValidator(0)
	consumer := NewLiveConsumer(delivery, validator, recoverer)
	
	return broker, consumer, store
}

func syncStores(broker *MemoryStreamBroker, eventStore *MemoryEventStore) {
	events, _ := broker.ReadFrom(context.Background(), 0)
	
	// Group by aggregate
	mapped := make(map[string][]DomainEvent)
	for _, se := range events {
		agg := se.DomainEvent.AggregateID()
		mapped[agg] = append(mapped[agg], se.DomainEvent)
	}
	eventStore.events = mapped
}

func TestBufferedHandoff_Test1_NormalBuffer(t *testing.T) {
	ctx := context.Background()
	broker, consumer, eventStore := setupBufferedHandoff()
	
	broker.Publish(EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1})
	broker.Publish(EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2})
	syncStores(broker, eventStore)
	
	handoff := NewBufferedHandoff(broker, consumer)
	
	pause := make(chan struct{})
	
	// Background start
	go handoff.Start(ctx, broker.NaiveSubscribe(), &HandoffTestHooks{ReplayMayContinue: pause})
	
	// Publish live events DURING replay (while handoff is paused at Capture)
	broker.Publish(EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3})
	syncStores(broker, eventStore) // Sync event store so gaps could theoretically be recovered, though here there are no gaps
	
	// Unpause replay -> let drain happen
	close(pause)
	
	// wait for completion
	// In a real test we'd use a waitgroup, but we can just sleep briefly for the drain to finish
	time.Sleep(50 * time.Millisecond)

	if handoff.State != HandoffLive {
		t.Fatalf("expected state LIVE, got %v", handoff.State)
	}
	
	if consumer.delivery.Sequence != 3 {
		t.Fatalf("expected sequence 3, got %d", consumer.delivery.Sequence)
	}
	t.Log("6E.7.7.4-B: Buffered handoff cleanly drained events published during replay.")
}



func TestBufferedHandoff_Test2_DiscardBeforeCheckpoint(t *testing.T) {
	ctx := context.Background()
	broker, consumer, eventStore := setupBufferedHandoff()
	
	broker.Publish(EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1})
	syncStores(broker, eventStore)

	handoff := NewBufferedHandoff(broker, consumer)
	
	// We want to force an event into the buffer BEFORE CapturePosition happens.
	// So we subscribe manually, push to broker, then call Start.
	// We'll simulate this by tweaking the start sequence manually.
	
	liveCh := broker.NaiveSubscribe()
	
	// Pre-fill the live buffer BEFORE capture
	broker.Publish(EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2})
	syncStores(broker, eventStore)
	
	handoff.Buffer <- <-liveCh
	
	// Now Start. It will capture position 2. It will replay 1..2.
	// Then it will drain the buffer. It will find position 2 in the buffer.
	// Since pos 2 <= checkpoint 2, it MUST discard it and NOT process twice.
	err := handoff.Start(ctx, liveCh, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	
	if handoff.State != HandoffLive {
		t.Fatalf("expected state LIVE, got %v", handoff.State)
	}
	
	if consumer.delivery.Sequence != 2 {
		t.Fatalf("expected sequence 2, got %d. Did it apply twice or fail?", consumer.delivery.Sequence)
	}
	
	t.Log("6E.7.7.4-B: Buffered handoff successfully discarded event that arrived before checkpoint.")
}

func TestBufferedHandoff_Test3_AdversarialGap(t *testing.T) {
	ctx := context.Background()
	broker, consumer, eventStore := setupBufferedHandoff()
	
	broker.Publish(EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1})
	syncStores(broker, eventStore)
	
	handoff := NewBufferedHandoff(broker, consumer)
	pause := make(chan struct{})
	
	go handoff.Start(ctx, broker.NaiveSubscribe(), &HandoffTestHooks{ReplayMayContinue: pause})
	
	// During replay, broker receives 2 and 3!
	// BUT, we will only publish 3 to the LIVE feed to simulate dropping 2 in the network!
	// We'll achieve this by writing 2 directly to history without hitting the broker's live fanout,
	// and publishing 3 to both.
	
	pos2 := StreamPosition(2)
	ev2 := StreamEvent{Position: pos2, DomainEvent: EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2}}
	broker.log = append(broker.log, ev2)
	// Did not broadcast to subscribers! Network drop simulated.
	
	// Broadcast 3
	broker.Publish(EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3})
	syncStores(broker, eventStore)
	
	// Now unpause. Handoff will replay 1.
	// Then it drains buffer. It sees 3.
	// 3 > checkpoint 1.
	// Consumer gets 3. Validator detects GAP (expected 2, got 3).
	// Consumer triggers GapRecovery(3).
	// GapRecovery loads history from eventStore, finds 2 and 3.
	// Applies 2, Applies 3.
	// Handoff successfully enters LIVE.
	
	close(pause)
	time.Sleep(50 * time.Millisecond)
	
	if consumer.delivery.Sequence != 3 {
		t.Fatalf("expected sequence 3, got %d", consumer.delivery.Sequence)
	}
	
	t.Log("6E.7.7.4-B: Buffered handoff seamlessly delegates to GapRecovery when network drops events!")
}
