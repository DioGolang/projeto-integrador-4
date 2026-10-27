package domain

import "time"

type Coordinate struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type Route struct {
	Points   []Coordinate  `json:"points"`
	Distance float64       `json:"distance"` // in meters
	Duration time.Duration `json:"duration"` // estimated duration
}

type DeliveryState string

const (
	StateEnRouteToPickup  DeliveryState = "EN_ROUTE_TO_PICKUP"
	StateAtPickup         DeliveryState = "AT_PICKUP"
	StateEnRouteToDropoff DeliveryState = "EN_ROUTE_TO_DROPOFF"
	StateDelivered        DeliveryState = "DELIVERED"
)

type DriverPosition struct {
	DriverID string     `json:"driver_id"`
	Position Coordinate `json:"position"`
}
