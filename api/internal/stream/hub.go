package stream

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type LiveHub interface {
	Publish(event Event)
	Subscribe(ctx context.Context, aggregateID uuid.UUID) (<-chan Event, func())
}

type MemoryLiveHub struct {
	mu   sync.RWMutex
	subs map[uuid.UUID][]chan Event
}

func NewMemoryLiveHub() *MemoryLiveHub {
	return &MemoryLiveHub{
		subs: make(map[uuid.UUID][]chan Event),
	}
}

func (h *MemoryLiveHub) Publish(event Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	aggID := event.DomainEvent.AggregateID()
	for _, ch := range h.subs[aggID] {
		select {
		case ch <- event:
		default:
			// Client lagging or buffer full.
			// LiveHub is strictly best-effort live delivery.
			// If dropped, client can rebuild from Read Model later.
		}
	}
}

func (h *MemoryLiveHub) Subscribe(ctx context.Context, aggregateID uuid.UUID) (<-chan Event, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch := make(chan Event, 100)
	h.subs[aggregateID] = append(h.subs[aggregateID], ch)

	unsub := func() {
		h.mu.Lock()
		defer h.mu.Unlock()

		chans := h.subs[aggregateID]
		for i, c := range chans {
			if c == ch {
				h.subs[aggregateID] = append(chans[:i], chans[i+1:]...)
				break
			}
		}
		close(ch)
	}

	return ch, unsub
}
