package engine

import (
	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/dvasconcellos/elo/simulator/internal/trajectory"
)

type Execution struct {
	Delivery      *domain.Delivery
	DriverInitPos domain.Coordinate

	PickupRoute  domain.Route
	DropoffRoute domain.Route

	PickupTrajectory  *trajectory.Trajectory
	DropoffTrajectory *trajectory.Trajectory
}
