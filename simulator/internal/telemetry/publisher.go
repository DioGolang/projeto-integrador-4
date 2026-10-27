package telemetry

import (
	"context"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type Publisher interface {
	PublishLocation(ctx context.Context, position domain.DriverPosition) error
}
