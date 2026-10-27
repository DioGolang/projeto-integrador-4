package domain

import (
	"context"
	"errors"
	"fmt"
)

type HandoffState int

const (
	HandoffBuffering HandoffState = iota
	HandoffReplaying
	HandoffDraining
	HandoffLive
)

func (s HandoffState) String() string {
	switch s {
	case HandoffBuffering: return "BUFFERING"
	case HandoffReplaying: return "REPLAYING"
	case HandoffDraining:  return "DRAINING"
	case HandoffLive:      return "LIVE"
	default:               return "UNKNOWN"
	}
}

var ErrBufferFull = errors.New("buffer full: backpressure applied")

type BufferedHandoff struct {
	State      HandoffState
	Buffer     chan StreamEvent
	Checkpoint StreamPosition

	store    StreamStore
	consumer *LiveConsumer
}

func NewBufferedHandoff(store StreamStore, consumer *LiveConsumer) *BufferedHandoff {
	return NewBufferedHandoffWithCapacity(store, consumer, 100)
}

func NewBufferedHandoffWithCapacity(store StreamStore, consumer *LiveConsumer, capacity int) *BufferedHandoff {
	return &BufferedHandoff{
		State:    HandoffBuffering,
		Buffer:   make(chan StreamEvent, capacity),
		store:    store,
		consumer: consumer,
	}
}

func (h *BufferedHandoff) Enqueue(ev StreamEvent) error {
	select {
	case h.Buffer <- ev:
		return nil
	default:
		return ErrBufferFull
	}
}

type HandoffTestHooks struct {
	CheckpointCaptured chan struct{}
	ReplayMayContinue  chan struct{}
}

func (h *BufferedHandoff) Start(ctx context.Context, liveStream <-chan StreamEvent, hooks *HandoffTestHooks) error {
	// 1. SUBSCRIBE / BUFFER
	if liveStream != nil {
		go func() {
			for ev := range liveStream {
				_ = h.Enqueue(ev) // In real world this might block instead of drop
			}
		}()
	}

	// 2. CAPTURE POSITION
	checkpoint, err := h.store.CapturePosition(ctx)
	if err != nil {
		return fmt.Errorf("capture stream position: %w", err)
	}
	h.Checkpoint = checkpoint

	if hooks != nil && hooks.CheckpointCaptured != nil {
		close(hooks.CheckpointCaptured)
	}

	if hooks != nil && hooks.ReplayMayContinue != nil {
		<-hooks.ReplayMayContinue
	}

	// 3. REPLAY THROUGH POSITION
	h.State = HandoffReplaying
	history, err := h.store.ReadThrough(ctx, h.Checkpoint)
	if err != nil {
		return err
	}
	
	for _, ev := range history {
		err := h.consumer.Consume(ctx, ev.DomainEvent)
		if err != nil {
			return err
		}
	}

	// 4. DRAIN BUFFER
	h.State = HandoffDraining
	
drainLoop:
	for {
		select {
		case ev := <-h.Buffer:
			// 5. VALIDATE STREAM POSITION
			if ev.Position <= h.Checkpoint {
				continue
			}
			err := h.consumer.Consume(ctx, ev.DomainEvent)
			if err != nil {
				return err
			}
		default:
			break drainLoop
		}
	}

	// 6. LIVE
	h.State = HandoffLive
	go func() {
		for ev := range h.Buffer {
			// In LIVE state, skip anything older than checkpoint (in case of overlaps still arriving)
			if ev.Position <= h.Checkpoint {
				continue
			}
			_ = h.consumer.Consume(ctx, ev.DomainEvent)
		}
	}()
	
	return nil
}
