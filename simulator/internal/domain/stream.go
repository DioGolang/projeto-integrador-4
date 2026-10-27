package domain

import (
	"context"
)

type StreamPosition int64

type StreamEvent struct {
	Position    StreamPosition
	DomainEvent DomainEvent
}

type StreamStore interface {
	CapturePosition(ctx context.Context) (StreamPosition, error)
	ReadThrough(ctx context.Context, targetPosition StreamPosition) ([]StreamEvent, error)
	ReadFrom(ctx context.Context, startPosition StreamPosition) ([]StreamEvent, error)
}
