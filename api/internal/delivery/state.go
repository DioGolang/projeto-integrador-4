package delivery

const (
	StateCreated          = "CREATED"
	StateAtPickup         = "AT_PICKUP"
	StateEnRouteToDropoff = "EN_ROUTE_TO_DROPOFF"
	StateDelivered        = "DELIVERED"
)

func StateOf(ev DomainEvent) (string, bool) {
	switch ev.(type) {
	case EventDeliveryCreated:
		return StateCreated, true
	case EventDriverArrivedAtPickup:
		return StateAtPickup, true
	case EventDriverEnRouteToDropoff:
		return StateEnRouteToDropoff, true
	case EventDeliveryCompleted:
		return StateDelivered, true
	default:
		return "", false
	}
}
