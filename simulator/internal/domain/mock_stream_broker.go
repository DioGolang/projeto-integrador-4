package domain

import (
	"context"
	"sync"
)

type MemoryStreamBroker struct {
	mu          sync.Mutex
	log         []StreamEvent
	subscribers []chan StreamEvent
}

func NewMemoryStreamBroker() *MemoryStreamBroker {
	return &MemoryStreamBroker{
		log: make([]StreamEvent, 0),
	}
}

func (b *MemoryStreamBroker) Publish(event DomainEvent) StreamPosition {
	b.mu.Lock()
	defer b.mu.Unlock()

	pos := StreamPosition(len(b.log) + 1)
	se := StreamEvent{Position: pos, DomainEvent: event}
	b.log = append(b.log, se)

	for _, sub := range b.subscribers {
		select {
		case sub <- se:
		default:
		}
	}
	return pos
}

func (b *MemoryStreamBroker) CapturePosition(ctx context.Context) (StreamPosition, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return StreamPosition(len(b.log)), nil
}

func (b *MemoryStreamBroker) ReadThrough(ctx context.Context, targetPosition StreamPosition) ([]StreamEvent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var res []StreamEvent
	for _, e := range b.log {
		if e.Position <= targetPosition {
			res = append(res, e)
		}
	}
	return res, nil
}

func (b *MemoryStreamBroker) ReadFrom(ctx context.Context, startPosition StreamPosition) ([]StreamEvent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var res []StreamEvent
	for _, e := range b.log {
		if e.Position > startPosition {
			res = append(res, e)
		}
	}
	return res, nil
}

func (b *MemoryStreamBroker) NaiveSubscribe() <-chan StreamEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan StreamEvent, 100)
	b.subscribers = append(b.subscribers, ch)
	return ch
}
