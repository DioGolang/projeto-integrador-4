package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/dvasconcellos/elo/simulator/internal/routing"
	"github.com/dvasconcellos/elo/simulator/internal/telemetry"
	"github.com/dvasconcellos/elo/simulator/internal/trajectory"
)

type Engine struct {
	routes       routing.RouteProvider
	store        domain.DeliveryStore
	telemetryPub telemetry.Publisher
	clock        Clock
	tickRate     time.Duration
}

func NewEngine(
	routes routing.RouteProvider,
	store domain.DeliveryStore,
	telemetryPub telemetry.Publisher,
	clock Clock,
) *Engine {
	return &Engine{
		routes:       routes,
		store:        store,
		telemetryPub: telemetryPub,
		clock:        clock,
		tickRate:     1 * time.Second, // Configure tick rate
	}
}

func (e *Engine) Run(ctx context.Context, exec *Execution) error {
	if err := e.store.Create(ctx, exec.Delivery); err != nil {
		return err
	}
	if err := e.preparePickup(ctx, exec); err != nil {
		return err
	}
	if err := e.executePickup(ctx, exec); err != nil {
		return err
	}
	if err := e.completePickup(ctx, exec); err != nil {
		return err
	}

	if err := e.prepareDropoff(ctx, exec); err != nil {
		return err
	}
	if err := e.executeDropoff(ctx, exec); err != nil {
		return err
	}
	if err := e.completeDropoff(ctx, exec); err != nil {
		return err
	}

	return nil
}

func (e *Engine) preparePickup(ctx context.Context, exec *Execution) error {
	route, err := e.routes.Route(ctx, exec.DriverInitPos, exec.Delivery.Pickup)
	if err != nil {
		return fmt.Errorf("failed to get pickup route: %w", err)
	}
	exec.PickupRoute = route

	traj, err := trajectory.NewTrajectory(route)
	if err != nil {
		return fmt.Errorf("failed to create pickup trajectory: %w", err)
	}
	exec.PickupTrajectory = traj
	return nil
}

func (e *Engine) executePickup(ctx context.Context, exec *Execution) error {
	startedAt := e.clock.Now()
	duration := exec.PickupRoute.Duration

	for {
		elapsed := e.clock.Now().Sub(startedAt)
		if elapsed >= duration {
			break
		}

		pos := exec.PickupTrajectory.PositionAt(elapsed)
		if err := e.telemetryPub.PublishLocation(ctx, domain.DriverPosition{
			DriverID: exec.Delivery.DriverID,
			Position: pos,
		}); err != nil {
			return fmt.Errorf("publish driver location: %w", err)
		}

		if err := e.clock.Sleep(ctx, e.tickRate); err != nil {
			return err
		}
	}

	// Final exact position at duration before transition
	finalPos := exec.PickupTrajectory.PositionAt(duration)
	if err := e.telemetryPub.PublishLocation(ctx, domain.DriverPosition{
		DriverID: exec.Delivery.DriverID,
		Position: finalPos,
	}); err != nil {
		return fmt.Errorf("publish final driver location: %w", err)
	}

	return nil
}

func (e *Engine) completePickup(ctx context.Context, exec *Execution) error {
	domainEvents, err := exec.Delivery.Transition(domain.ActionPickupReached{})
	if err != nil {
		return err
	}
	if err := e.store.Commit(ctx, exec.Delivery, domainEvents); err != nil {
		return fmt.Errorf("commit state+events: %w", err)
	}

	// For now, assume pickup is instantaneous.
	domainEvents, err = exec.Delivery.Transition(domain.ActionPickupCompleted{})
	if err != nil {
		return err
	}
	if err := e.store.Commit(ctx, exec.Delivery, domainEvents); err != nil {
		return fmt.Errorf("commit state+events: %w", err)
	}
	return nil
}

func (e *Engine) prepareDropoff(ctx context.Context, exec *Execution) error {
	route, err := e.routes.Route(ctx, exec.Delivery.Pickup, exec.Delivery.Dropoff)
	if err != nil {
		return fmt.Errorf("failed to get dropoff route: %w", err)
	}
	exec.DropoffRoute = route

	traj, err := trajectory.NewTrajectory(route)
	if err != nil {
		return fmt.Errorf("failed to create dropoff trajectory: %w", err)
	}
	exec.DropoffTrajectory = traj
	return nil
}

func (e *Engine) executeDropoff(ctx context.Context, exec *Execution) error {
	startedAt := e.clock.Now()
	duration := exec.DropoffRoute.Duration

	for {
		elapsed := e.clock.Now().Sub(startedAt)
		if elapsed >= duration {
			break
		}

		pos := exec.DropoffTrajectory.PositionAt(elapsed)
		if err := e.telemetryPub.PublishLocation(ctx, domain.DriverPosition{
			DriverID: exec.Delivery.DriverID,
			Position: pos,
		}); err != nil {
			return fmt.Errorf("publish driver location: %w", err)
		}

		if err := e.clock.Sleep(ctx, e.tickRate); err != nil {
			return err
		}
	}

	// Final exact position at duration before transition
	finalPos := exec.DropoffTrajectory.PositionAt(duration)
	if err := e.telemetryPub.PublishLocation(ctx, domain.DriverPosition{
		DriverID: exec.Delivery.DriverID,
		Position: finalPos,
	}); err != nil {
		return fmt.Errorf("publish final driver location: %w", err)
	}

	return nil
}

func (e *Engine) completeDropoff(ctx context.Context, exec *Execution) error {
	domainEvents, err := exec.Delivery.Transition(domain.ActionDropoffReached{})
	if err != nil {
		return err
	}
	if err := e.store.Commit(ctx, exec.Delivery, domainEvents); err != nil {
		return fmt.Errorf("commit state+events: %w", err)
	}
	return nil
}
