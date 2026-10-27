package engine

import (
	"context"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type FakeRouteProvider struct {
	FixedDuration time.Duration
}

func (p *FakeRouteProvider) Route(ctx context.Context, from domain.Coordinate, to domain.Coordinate) (domain.Route, error) {
	return domain.Route{
		Points:   []domain.Coordinate{from, to},
		Distance: 1000.0,
		Duration: p.FixedDuration,
	}, nil
}
