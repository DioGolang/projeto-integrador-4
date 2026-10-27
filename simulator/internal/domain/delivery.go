package domain

import (
	"errors"
	"fmt"
)

// Delivery represents the state of a single delivery simulation
type Delivery struct {
	ID       string
	DriverID string
	State    DeliveryState
	Sequence int64
	Pickup   Coordinate
	Dropoff  Coordinate
}

// Input actions/stimuli for state transitions
type Action interface {
	ActionName() string
}

type ActionPickupReached struct{}
func (ActionPickupReached) ActionName() string { return "PickupReached" }
type ActionPickupCompleted struct{}
func (ActionPickupCompleted) ActionName() string { return "PickupCompleted" }
type ActionDropoffReached struct{}
func (ActionDropoffReached) ActionName() string { return "DropoffReached" }

// DomainEvent interface for output events
type DomainEvent interface {
	EventID() string
	AggregateID() string
	Sequence() int64
	EventName() string
}

type EventDriverArrivedAtPickup struct {
	ID         string
	DeliveryID string
	Seq        int64
}

func (e EventDriverArrivedAtPickup) EventID() string { return e.ID }
func (e EventDriverArrivedAtPickup) AggregateID() string { return e.DeliveryID }
func (e EventDriverArrivedAtPickup) Sequence() int64 { return e.Seq }
func (e EventDriverArrivedAtPickup) EventName() string { return "DriverArrivedAtPickup" }


type EventDriverEnRouteToDropoff struct {
	ID         string
	DeliveryID string
	Seq        int64
}

func (e EventDriverEnRouteToDropoff) EventID() string { return e.ID }
func (e EventDriverEnRouteToDropoff) AggregateID() string { return e.DeliveryID }
func (e EventDriverEnRouteToDropoff) Sequence() int64 { return e.Seq }
func (e EventDriverEnRouteToDropoff) EventName() string { return "DriverEnRouteToDropoff" }


type EventDeliveryCompleted struct {
	ID         string
	DeliveryID string
	Seq        int64
}

func (e EventDeliveryCompleted) EventID() string { return e.ID }
func (e EventDeliveryCompleted) AggregateID() string { return e.DeliveryID }
func (e EventDeliveryCompleted) Sequence() int64 { return e.Seq }
func (e EventDeliveryCompleted) EventName() string { return "DeliveryCompleted" }

var ErrInvalidTransition = errors.New("invalid state transition")

func NewDelivery(id, driverID string, pickup, dropoff Coordinate) *Delivery {
	return &Delivery{
		ID:       id,
		DriverID: driverID,
		State:    StateEnRouteToPickup,
		Sequence: 0,
		Pickup:   pickup,
		Dropoff:  dropoff,
	}
}

func (d *Delivery) Transition(action Action) ([]DomainEvent, error) {
	switch d.State {
	case StateEnRouteToPickup:
		switch action.(type) {
		case ActionPickupReached:
			d.State = StateAtPickup
			d.Sequence++
			return []DomainEvent{EventDriverArrivedAtPickup{
				ID:         fmt.Sprintf("%s-ev-%d", d.ID, d.Sequence),
				DeliveryID: d.ID,
				Seq:        d.Sequence,
			}}, nil
		default:
			return nil, fmt.Errorf("%w: cannot process %T in state %s", ErrInvalidTransition, action, d.State)
		}

	case StateAtPickup:
		switch action.(type) {
		case ActionPickupCompleted:
			d.State = StateEnRouteToDropoff
			d.Sequence++
			return []DomainEvent{EventDriverEnRouteToDropoff{
				ID:         fmt.Sprintf("%s-ev-%d", d.ID, d.Sequence),
				DeliveryID: d.ID,
				Seq:        d.Sequence,
			}}, nil
		default:
			return nil, fmt.Errorf("%w: cannot process %T in state %s", ErrInvalidTransition, action, d.State)
		}

	case StateEnRouteToDropoff:
		switch action.(type) {
		case ActionDropoffReached:
			d.State = StateDelivered
			d.Sequence++
			return []DomainEvent{EventDeliveryCompleted{
				ID:         fmt.Sprintf("%s-ev-%d", d.ID, d.Sequence),
				DeliveryID: d.ID,
				Seq:        d.Sequence,
			}}, nil
		default:
			return nil, fmt.Errorf("%w: cannot process %T in state %s", ErrInvalidTransition, action, d.State)
		}

	case StateDelivered:
		return nil, fmt.Errorf("%w: delivery is already completed", ErrInvalidTransition)

	default:
		return nil, fmt.Errorf("unknown state: %s", d.State)
	}
}

// Apply mutates the Delivery state by applying a historical event.
// It acts as a reducer for Event Sourcing.
func (d *Delivery) Apply(event DomainEvent) error {
	// Replay validation: check aggregate ID
	if event.AggregateID() != d.ID {
		return fmt.Errorf("event belongs to aggregate %s, but delivery is %s", event.AggregateID(), d.ID)
	}

	// Replay validation: strict sequence ordering
	expectedSeq := d.Sequence + 1
	if event.Sequence() != expectedSeq {
		return fmt.Errorf("expected sequence %d, got %d", expectedSeq, event.Sequence())
	}

	// Apply projection based on event type
	switch event.(type) {
	case EventDriverArrivedAtPickup:
		if d.State != StateEnRouteToPickup {
			return fmt.Errorf("invalid historical transition: EventDriverArrivedAtPickup applied to state %s", d.State)
		}
		d.State = StateAtPickup

	case EventDriverEnRouteToDropoff:
		if d.State != StateAtPickup {
			return fmt.Errorf("invalid historical transition: EventDriverEnRouteToDropoff applied to state %s", d.State)
		}
		d.State = StateEnRouteToDropoff

	case EventDeliveryCompleted:
		if d.State != StateEnRouteToDropoff {
			return fmt.Errorf("invalid historical transition: EventDeliveryCompleted applied to state %s", d.State)
		}
		d.State = StateDelivered

	default:
		return fmt.Errorf("unknown event type %T", event)
	}

	d.Sequence = event.Sequence()
	return nil
}

// RebuildDelivery recreates the Delivery state from a base object and a list of historical events.
// This is the core Event Sourcing mechanism.
func RebuildDelivery(base *Delivery, events []DomainEvent) (*Delivery, error) {
	// Clone the base delivery to avoid mutating the original reference
	d := &Delivery{
		ID:       base.ID,
		DriverID: base.DriverID,
		State:    base.State,
		Sequence: base.Sequence,
		Pickup:   base.Pickup,
		Dropoff:  base.Dropoff,
	}

	for _, event := range events {
		if err := d.Apply(event); err != nil {
			return nil, err
		}
	}
	return d, nil
}
