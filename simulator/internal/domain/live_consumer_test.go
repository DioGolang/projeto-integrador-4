package domain

import (
	"context"
	"testing"
)

func setupLiveConsumerTest() (*MemoryEventStore, *LiveConsumer) {
	store := NewMemoryEventStore()
	recoverer := NewGapRecoverer(store)
	
	delivery := NewDelivery("D1", "DriverX", Coordinate{1,1}, Coordinate{2,2})
	delivery.Apply(EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1})
	// Now state is AT_PICKUP, sequence is 1.
	
	validator := NewSequenceValidator(1)
	consumer := NewLiveConsumer(delivery, validator, recoverer)
	
	return store, consumer
}

func TestLiveConsumer_Test1_NormalFlow(t *testing.T) {
	ctx := context.Background()
	_, consumer := setupLiveConsumerTest() // cursor = 1

	liveEvent := EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2}
	err := consumer.Consume(ctx, liveEvent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if consumer.delivery.Sequence != 2 {
		t.Fatalf("expected state=2, got %d", consumer.delivery.Sequence)
	}
	if consumer.validator.lastSequence != 2 {
		t.Fatalf("expected cursor=2, got %d", consumer.validator.lastSequence)
	}
	t.Log("6E.7.7.3 - Test 1: Normal flow succeeds.")
}

func TestLiveConsumer_Test2_RecoverableGap(t *testing.T) {
	ctx := context.Background()
	store, consumer := setupLiveConsumerTest() // cursor = 1

	store.events["D1"] = []DomainEvent{
		EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2},
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3},
	}
	
	liveEvent := EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3}
	err := consumer.Consume(ctx, liveEvent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if consumer.delivery.Sequence != 3 {
		t.Fatalf("expected state=3, got %d", consumer.delivery.Sequence)
	}
	if consumer.validator.lastSequence != 3 {
		t.Fatalf("expected cursor=3, got %d", consumer.validator.lastSequence)
	}
	t.Log("6E.7.7.3 - Test 2: Recoverable gap handles history and live event cleanly without duplicates.")
}

func TestLiveConsumer_Test3_Duplicate(t *testing.T) {
	ctx := context.Background()
	_, consumer := setupLiveConsumerTest() // cursor = 1

	// Fast-forward consumer to 2 manually
	consumer.delivery.Apply(EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2})
	consumer.validator.Advance(2)
	// Now cursor = 2

	liveEvent := EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2}
	
	err := consumer.Consume(ctx, liveEvent)
	if err != nil {
		t.Fatalf("unexpected error for duplicate: %v", err)
	}

	if consumer.delivery.Sequence != 2 {
		t.Fatalf("state mutated on duplicate")
	}
	if consumer.validator.lastSequence != 2 {
		t.Fatalf("cursor mutated on duplicate")
	}
	t.Log("6E.7.7.3 - Test 3: Duplicates safely ignored.")
}

func TestLiveConsumer_Test4_Stale(t *testing.T) {
	ctx := context.Background()
	_, consumer := setupLiveConsumerTest() // cursor = 1

	// Fast-forward consumer to 3
	consumer.delivery.Apply(EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2})
	consumer.delivery.Apply(EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3})
	consumer.validator.Advance(3)
	// Now cursor = 3

	liveEvent := EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1} // Stale!
	
	err := consumer.Consume(ctx, liveEvent)
	if err != nil {
		t.Fatalf("unexpected error for stale: %v", err)
	}

	if consumer.delivery.Sequence != 3 || consumer.validator.lastSequence != 3 {
		t.Fatalf("state mutated on stale event")
	}
	t.Log("6E.7.7.3 - Test 4: Stale events safely ignored.")
}

func TestLiveConsumer_Test5_IncompleteRecovery(t *testing.T) {
	ctx := context.Background()
	store, consumer := setupLiveConsumerTest() // cursor = 1

	// history only has 2, but live is 3
	store.events["D1"] = []DomainEvent{
		EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2},
		// Missing 3!
	}
	
	liveEvent := EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3}
	err := consumer.Consume(ctx, liveEvent)
	
	if err == nil {
		t.Fatalf("expected error due to incomplete recovery")
	}

	if err.Error() != "recovery incomplete: expected to reach 3, but history stopped at 2" {
		t.Fatalf("unexpected error message: %v", err)
	}

	if consumer.validator.lastSequence != 2 {
		t.Fatalf("cursor should have advanced to 2, got %d", consumer.validator.lastSequence)
	}

	t.Log("6E.7.7.3 - Test 5: Incomplete recovery successfully identified.")
}

func TestLiveConsumer_Test6_CorruptedHistory(t *testing.T) {
	ctx := context.Background()
	store, consumer := setupLiveConsumerTest() // cursor = 1

	// history is corrupted (missing 2, has 3)
	store.events["D1"] = []DomainEvent{
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3}, // Corrupted gap
	}
	
	liveEvent := EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3}
	err := consumer.Consume(ctx, liveEvent)
	
	if err == nil {
		t.Fatalf("expected error due to corrupted history")
	}

	if err.Error() != "corrupted history: expected sequence 2, got gap to 3" {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Should remain at 1
	if consumer.validator.lastSequence != 1 {
		t.Fatalf("cursor should not advance on corrupted history")
	}

	t.Log("6E.7.7.3 - Test 6: Corrupted history safely aborted.")
}
