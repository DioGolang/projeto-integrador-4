package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type RouteProvider interface {
	Route(ctx context.Context, from domain.Coordinate, to domain.Coordinate) (domain.Route, error)
}

type OSRMRouteProvider struct {
	baseURL    string
	httpClient *http.Client
}

func NewOSRMRouteProvider(baseURL string) *OSRMRouteProvider {
	if baseURL == "" {
		baseURL = "http://router.project-osrm.org/route/v1/driving"
	}
	return &OSRMRouteProvider{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// OSRMResponse matches the basic structure returned by the OSRM Route API.
type OSRMResponse struct {
	Code   string `json:"code"`
	Routes []struct {
		Distance float64 `json:"distance"`
		Duration float64 `json:"duration"`
		Geometry struct {
			Coordinates [][]float64 `json:"coordinates"` // [lng, lat]
		} `json:"geometry"`
	} `json:"routes"`
}

func (p *OSRMRouteProvider) Route(ctx context.Context, from domain.Coordinate, to domain.Coordinate) (domain.Route, error) {
	// OSRM format: {lng},{lat};{lng},{lat}
	url := fmt.Sprintf("%s/%f,%f;%f,%f?overview=full&geometries=geojson", p.baseURL, from.Lng, from.Lat, to.Lng, to.Lat)
	
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return domain.Route{}, fmt.Errorf("failed to create osrm request: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return domain.Route{}, fmt.Errorf("failed to fetch from osrm: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return domain.Route{}, fmt.Errorf("osrm returned status %d: %s", resp.StatusCode, string(body))
	}

	var osrmResp OSRMResponse
	if err := json.NewDecoder(resp.Body).Decode(&osrmResp); err != nil {
		return domain.Route{}, fmt.Errorf("failed to decode osrm response: %w", err)
	}

	if osrmResp.Code != "Ok" || len(osrmResp.Routes) == 0 {
		return domain.Route{}, fmt.Errorf("osrm no route found or error code: %s", osrmResp.Code)
	}

	primaryRoute := osrmResp.Routes[0]
	var points []domain.Coordinate
	for _, coord := range primaryRoute.Geometry.Coordinates {
		if len(coord) >= 2 {
			points = append(points, domain.Coordinate{
				Lng: coord[0],
				Lat: coord[1], // OSRM gives Lng first
			})
		}
	}

	return domain.Route{
		Points:   points,
		Distance: primaryRoute.Distance,
		Duration: time.Duration(primaryRoute.Duration) * time.Second,
	}, nil
}
