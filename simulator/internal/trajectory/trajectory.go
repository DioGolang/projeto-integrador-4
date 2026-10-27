package trajectory

import (
	"errors"
	"math"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type Trajectory struct {
	route       domain.Route
	accDistance []float64
	totalDist   float64
}

// NewTrajectory creates a Trajectory for a given Route, precomputing accumulated distances.
func NewTrajectory(route domain.Route) (*Trajectory, error) {
	if len(route.Points) == 0 {
		return nil, errors.New("cannot create trajectory for empty route")
	}

	acc := make([]float64, len(route.Points))
	acc[0] = 0.0
	for i := 1; i < len(route.Points); i++ {
		dist := distance(route.Points[i-1], route.Points[i])
		acc[i] = acc[i-1] + dist
	}

	return &Trajectory{
		route:       route,
		accDistance: acc,
		totalDist:   acc[len(acc)-1],
	}, nil
}

// PositionAt returns the coordinate corresponding to the elapsed time.
func (t *Trajectory) PositionAt(elapsed time.Duration) domain.Coordinate {
	// If only 1 point, stay there
	if len(t.route.Points) == 1 {
		return t.route.Points[0]
	}
	
	// Before start
	if elapsed <= 0 {
		return t.route.Points[0]
	}
	
	// After end
	if elapsed >= t.route.Duration {
		return t.route.Points[len(t.route.Points)-1]
	}

	// Calculate progress
	progress := float64(elapsed) / float64(t.route.Duration)
	targetDistance := progress * t.totalDist

	// Find the segment containing the target distance
	for i := 0; i < len(t.accDistance)-1; i++ {
		startDist := t.accDistance[i]
		endDist := t.accDistance[i+1]
		
		if targetDistance >= startDist && targetDistance <= endDist {
			segmentDist := endDist - startDist
			if segmentDist == 0 {
				return t.route.Points[i+1]
			}
			
			localProgress := (targetDistance - startDist) / segmentDist
			return interpolate(t.route.Points[i], t.route.Points[i+1], localProgress)
		}
	}

	// Fallback to last point (should theoretically not reach here due to earlier bounds checks)
	return t.route.Points[len(t.route.Points)-1]
}

// interpolate performs linear interpolation between two coordinates.
func interpolate(a, b domain.Coordinate, fraction float64) domain.Coordinate {
	if fraction <= 0 {
		return a
	}
	if fraction >= 1 {
		return b
	}
	return domain.Coordinate{
		Lat: a.Lat + (b.Lat-a.Lat)*fraction,
		Lng: a.Lng + (b.Lng-a.Lng)*fraction,
	}
}

// distance calculates the Haversine distance between two coordinates in meters.
func distance(a, b domain.Coordinate) float64 {
	const R = 6371000 // Earth radius in meters
	phi1 := a.Lat * math.Pi / 180
	phi2 := b.Lat * math.Pi / 180
	deltaPhi := (b.Lat - a.Lat) * math.Pi / 180
	deltaLambda := (b.Lng - a.Lng) * math.Pi / 180

	sinDP := math.Sin(deltaPhi / 2)
	sinDL := math.Sin(deltaLambda / 2)

	x := sinDP*sinDP + math.Cos(phi1)*math.Cos(phi2)*sinDL*sinDL
	y := 2 * math.Atan2(math.Sqrt(x), math.Sqrt(1-x))

	return R * y
}
