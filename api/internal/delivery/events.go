package delivery

import "github.com/google/uuid"

type DomainEvent interface {
	EventID() string
	AggregateID() uuid.UUID
	Sequence() int64
	EventName() string
}

type EventDeliveryCreated struct {
	ID         string    `json:"id"`
	DeliveryID uuid.UUID `json:"delivery_id"`
	DriverID   string    `json:"driver_id"`
	Seq        int64     `json:"seq"`
	PickupLat  float64   `json:"pickup_lat"`
	PickupLng  float64   `json:"pickup_lng"`
	DropoffLat float64   `json:"dropoff_lat"`
	DropoffLng float64   `json:"dropoff_lng"`
}

func (e EventDeliveryCreated) EventID() string        { return e.ID }
func (e EventDeliveryCreated) AggregateID() uuid.UUID { return e.DeliveryID }
func (e EventDeliveryCreated) Sequence() int64        { return e.Seq }
func (e EventDeliveryCreated) EventName() string      { return "DeliveryCreated" }

type EventDriverArrivedAtPickup struct {
	ID         string    `json:"id"`
	DeliveryID uuid.UUID `json:"delivery_id"`
	Seq        int64     `json:"seq"`
}

func (e EventDriverArrivedAtPickup) EventID() string        { return e.ID }
func (e EventDriverArrivedAtPickup) AggregateID() uuid.UUID { return e.DeliveryID }
func (e EventDriverArrivedAtPickup) Sequence() int64        { return e.Seq }
func (e EventDriverArrivedAtPickup) EventName() string      { return "DriverArrivedAtPickup" }

type EventDriverEnRouteToDropoff struct {
	ID         string    `json:"id"`
	DeliveryID uuid.UUID `json:"delivery_id"`
	Seq        int64     `json:"seq"`
	Lat        float64   `json:"lat"`
	Lng        float64   `json:"lng"`
}

func (e EventDriverEnRouteToDropoff) EventID() string        { return e.ID }
func (e EventDriverEnRouteToDropoff) AggregateID() uuid.UUID { return e.DeliveryID }
func (e EventDriverEnRouteToDropoff) Sequence() int64        { return e.Seq }
func (e EventDriverEnRouteToDropoff) EventName() string      { return "DriverEnRouteToDropoff" }

type EventDeliveryCompleted struct {
	ID         string    `json:"id"`
	DeliveryID uuid.UUID `json:"delivery_id"`
	Seq        int64     `json:"seq"`
}

func (e EventDeliveryCompleted) EventID() string        { return e.ID }
func (e EventDeliveryCompleted) AggregateID() uuid.UUID { return e.DeliveryID }
func (e EventDeliveryCompleted) Sequence() int64        { return e.Seq }
func (e EventDeliveryCompleted) EventName() string      { return "DeliveryCompleted" }

type CoordinatePayload struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type RouteGeometryPayload struct {
	RouteID             string              `json:"route_id"`
	Revision            int64               `json:"revision"`
	Leg                 string              `json:"leg"`
	Points              []CoordinatePayload `json:"points"`
	DistanceMeters      float64             `json:"distance_meters"`
	EstimatedDurationMs int64               `json:"estimated_duration_ms"`
}

type EventDeliveryRouteAssigned struct {
	ID         string               `json:"id"`
	DeliveryID uuid.UUID            `json:"delivery_id"`
	Seq        int64                `json:"seq"`
	Route      RouteGeometryPayload `json:"route"`
}

func (e EventDeliveryRouteAssigned) EventID() string        { return e.ID }
func (e EventDeliveryRouteAssigned) AggregateID() uuid.UUID { return e.DeliveryID }
func (e EventDeliveryRouteAssigned) Sequence() int64        { return e.Seq }
func (e EventDeliveryRouteAssigned) EventName() string      { return "DeliveryRouteAssigned" }
