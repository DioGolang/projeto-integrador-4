package trajectory

import (
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

func TestNewTrajectory_EmptyRoute(t *testing.T) {
	_, err := NewTrajectory(domain.Route{})
	if err == nil {
		t.Error("expected error for empty route, got nil")
	}
}

func TestPositionAt_SinglePoint(t *testing.T) {
	coord := domain.Coordinate{Lat: 10, Lng: 20}
	route := domain.Route{
		Points:   []domain.Coordinate{coord},
		Duration: 10 * time.Second,
	}

	traj, err := NewTrajectory(route)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return the same point at any time
	if pos := traj.PositionAt(0); pos != coord {
		t.Errorf("expected %v, got %v", coord, pos)
	}
	if pos := traj.PositionAt(5 * time.Second); pos != coord {
		t.Errorf("expected %v, got %v", coord, pos)
	}
}

func TestPositionAt_BeforeStart(t *testing.T) {
	start := domain.Coordinate{Lat: 10, Lng: 20}
	end := domain.Coordinate{Lat: 11, Lng: 21}
	route := domain.Route{
		Points:   []domain.Coordinate{start, end},
		Duration: 10 * time.Second,
	}

	traj, _ := NewTrajectory(route)
	
	if pos := traj.PositionAt(-1 * time.Second); pos != start {
		t.Errorf("expected %v for elapsed < 0, got %v", start, pos)
	}
}

func TestPositionAt_AfterEnd(t *testing.T) {
	start := domain.Coordinate{Lat: 10, Lng: 20}
	end := domain.Coordinate{Lat: 11, Lng: 21}
	route := domain.Route{
		Points:   []domain.Coordinate{start, end},
		Duration: 10 * time.Second,
	}

	traj, _ := NewTrajectory(route)
	
	if pos := traj.PositionAt(15 * time.Second); pos != end {
		t.Errorf("expected %v for elapsed > duration, got %v", end, pos)
	}
}

func TestPositionAt_Midpoint(t *testing.T) {
	start := domain.Coordinate{Lat: 0, Lng: 0}
	end := domain.Coordinate{Lat: 0, Lng: 10} // straight line on equator
	route := domain.Route{
		Points:   []domain.Coordinate{start, end},
		Duration: 10 * time.Second,
	}

	traj, _ := NewTrajectory(route)
	
	mid := traj.PositionAt(5 * time.Second)
	// We expect roughly {Lat: 0, Lng: 5}
	if mid.Lat != 0 || mid.Lng != 5 {
		t.Errorf("expected roughly mid point {0, 5}, got %v", mid)
	}
}

func TestPositionAt_ThreePointsInterpolation(t *testing.T) {
	// A route with 3 points, where A->B is same distance as B->C
	// So at 50% time, we should be exactly at B
	p0 := domain.Coordinate{Lat: 0, Lng: 0}
	p1 := domain.Coordinate{Lat: 0, Lng: 10}
	p2 := domain.Coordinate{Lat: 0, Lng: 20}
	
	route := domain.Route{
		Points:   []domain.Coordinate{p0, p1, p2},
		Duration: 20 * time.Second,
	}

	traj, _ := NewTrajectory(route)
	
	atB := traj.PositionAt(10 * time.Second)
	if atB.Lng != 10 {
		t.Errorf("expected Lng to be 10 at 50%% time, got %f", atB.Lng)
	}

	// At 25% time, we should be halfway between A and B
	at25 := traj.PositionAt(5 * time.Second)
	if at25.Lng != 5 {
		t.Errorf("expected Lng to be 5 at 25%% time, got %f", at25.Lng)
	}
}
