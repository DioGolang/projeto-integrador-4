package delivery

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// FingerprintVersion identifies the canonical semantic hashing contract.
const FingerprintVersion = "v1"

var (
	ErrNilEvent           = errors.New("cannot compute fingerprint for nil domain event")
	ErrUnsupportedPayload = errors.New("unsupported domain event type for fingerprint")
)

// ComputeFingerprint calculates a semantic, deterministic, and versioned SHA-256
// hash over the immutable business payload of a DomainEvent.
//
// Properties:
// 1. Semantic: Formed strictly from business domain fields, independent of transient formatting.
// 2. Deterministic: Equal business data always yields the identical 64-character hex hash.
// 3. Versioned: Prepends the schema version ("v1|") to prevent collision across contract evolutions.
func ComputeFingerprint(ev DomainEvent) (string, error) {
	if ev == nil {
		return "", ErrNilEvent
	}

	var canonicalBuilder strings.Builder
	canonicalBuilder.WriteString(FingerprintVersion)
	canonicalBuilder.WriteByte('|')
	canonicalBuilder.WriteString(ev.EventName())
	canonicalBuilder.WriteByte('|')
	canonicalBuilder.WriteString(ev.AggregateID().String())
	canonicalBuilder.WriteByte('|')
	canonicalBuilder.WriteString(fmt.Sprintf("%d", ev.Sequence()))
	canonicalBuilder.WriteByte('|')

	switch e := ev.(type) {
	case EventDeliveryCreated:
		canonicalBuilder.WriteString(fmt.Sprintf("driver=%s;pickup=%.6f,%.6f;dropoff=%.6f,%.6f",
			e.DriverID,
			normalizeCoord(e.PickupLat),
			normalizeCoord(e.PickupLng),
			normalizeCoord(e.DropoffLat),
			normalizeCoord(e.DropoffLng),
		))

	case EventDriverArrivedAtPickup:
		canonicalBuilder.WriteString("status=arrived_at_pickup")

	case EventDriverEnRouteToDropoff:
		canonicalBuilder.WriteString(fmt.Sprintf("coords=%.6f,%.6f",
			normalizeCoord(e.Lat),
			normalizeCoord(e.Lng),
		))

	case EventDeliveryCompleted:
		canonicalBuilder.WriteString("status=completed")

	case EventDeliveryRouteAssigned:
		canonicalBuilder.WriteString(fmt.Sprintf("route_id=%s;rev=%d;leg=%s;dist=%.2f;dur=%d;points=%s",
			e.Route.RouteID,
			e.Route.Revision,
			e.Route.Leg,
			e.Route.DistanceMeters,
			e.Route.EstimatedDurationMs,
			formatPointsCanonical(e.Route.Points),
		))

	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedPayload, ev.EventName())
	}

	hash := sha256.Sum256([]byte(canonicalBuilder.String()))
	return hex.EncodeToString(hash[:]), nil
}

func normalizeCoord(c float64) float64 {
	if c == 0 {
		return 0
	}
	return c
}

func formatPointsCanonical(points []CoordinatePayload) string {
	if len(points) == 0 {
		return "none"
	}
	var sb strings.Builder
	for i, pt := range points {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("%.6f:%.6f", normalizeCoord(pt.Lat), normalizeCoord(pt.Lng)))
	}
	return sb.String()
}
