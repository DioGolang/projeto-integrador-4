package stream

import (
	"context"

	"github.com/dvasconcellos/elo/api/internal/delivery"
)

type Target interface {
	Apply(ctx context.Context, ev delivery.DomainEvent, streamPosition int64) error
}
