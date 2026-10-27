package domain

import (
	"context"
	"errors"
	"sync"
)

type DeliveryStore interface {
	Create(ctx context.Context, delivery *Delivery) error
	Commit(ctx context.Context, delivery *Delivery, events []DomainEvent) error
}

type MemoryDeliveryStore struct {
	mu         sync.Mutex
	Deliveries map[string]*Delivery
	Outbox     map[string][]DomainEvent
}

func NewMemoryDeliveryStore() *MemoryDeliveryStore {
	return &MemoryDeliveryStore{
		Deliveries: make(map[string]*Delivery),
		Outbox:     make(map[string][]DomainEvent),
	}
}

func (s *MemoryDeliveryStore) Create(ctx context.Context, d *Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stateCopy := *d
	s.Deliveries[d.ID] = &stateCopy
	return nil
}

func (s *MemoryDeliveryStore) Commit(ctx context.Context, d *Delivery, events []DomainEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Simulate Unique (aggregate_id, sequence) constraint
	existingEvents := s.Outbox[d.ID]
	for _, ev := range events {
		for _, ex := range existingEvents {
			if ex.Sequence() == ev.Sequence() {
				return errors.New("duplicate sequence number for aggregate")
			}
		}
	}

	// 1. Persist State
	stateCopy := *d // shallow copy for memory store
	s.Deliveries[d.ID] = &stateCopy

	// 2. Persist Events
	s.Outbox[d.ID] = append(s.Outbox[d.ID], events...)

	return nil
}
