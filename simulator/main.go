package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"connectrpc.com/connect"
	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/pkg/client"
	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/dvasconcellos/elo/simulator/internal/routing"
	"github.com/dvasconcellos/elo/simulator/internal/trajectory"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OutboxEventPayload struct {
	OrderID     string  `json:"order_id"`
	MerchantID  string  `json:"merchant_id"`
	PickupLat   float64 `json:"pickup_lat"`
	PickupLng   float64 `json:"pickup_lng"`
	DropoffLat  float64 `json:"dropoff_lat"`
	DropoffLng  float64 `json:"dropoff_lng"`
}

func main() {
	amqpUrl := os.Getenv("RABBITMQ_URL")
	if amqpUrl == "" {
		amqpUrl = "amqp://guest:guest@localhost:5672/"
	}

	var conn *amqp.Connection
	var err error
	for i := 0; i < 5; i++ {
		conn, err = amqp.Dial(amqpUrl)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to RabbitMQ, retrying... (%v)", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare("logistics.events", "direct", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}

	q, err := ch.QueueDeclare("simulator.queue", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	err = ch.QueueBind(q.Name, "OrderCreated", "logistics.events", false, nil)
	if err != nil {
		log.Fatalf("Failed to bind queue: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	apiURL := os.Getenv("API_URL")
	if apiURL == "" {
		apiURL = "http://localhost:8080"
	}

	delivererEmail := os.Getenv("DELIVERER_EMAIL")
	if delivererEmail == "" {
		delivererEmail = "entregador@elo.com"
	}
	delivererPassword := os.Getenv("DELIVERER_PASSWORD")
	if delivererPassword == "" {
		delivererPassword = "Entregador@123!"
	}
	delivererFingerprint := os.Getenv("DELIVERER_FINGERPRINT")
	if delivererFingerprint == "" {
		delivererFingerprint = "simulator-deliverer-worker-1"
	}

	authClient, err := client.NewAuthClient(client.Config{
		BaseURL:           apiURL,
		Email:             delivererEmail,
		Password:          delivererPassword,
		DeviceFingerprint: delivererFingerprint,
	})
	if err != nil {
		log.Fatalf("Failed to initialize auth client: %v", err)
	}

	authCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Perform initial login with retries for startup resilience
	for i := 0; i < 10; i++ {
		err = authClient.Login(authCtx)
		if err == nil {
			log.Printf("Simulator successfully authenticated as deliverer %s", delivererEmail)
			break
		}
		log.Printf("Auth login attempt %d/10 failed: %v, retrying in 2s...", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Fatal: simulator failed to authenticate with API: %v", err)
	}

	client := v1connect.NewLogisticsServiceClient(http.DefaultClient, apiURL, connect.WithInterceptors(authClient.Interceptor()))
	router := routing.NewOSRMRouteProvider(os.Getenv("OSRM_URL"))

	log.Printf("Simulator is running, waiting for OrderCreated events...")

	for d := range msgs {
		log.Printf("Received OrderCreated event")
		var env struct {
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(d.Body, &env); err != nil {
			log.Printf("Failed to parse envelope: %v", err)
			continue
		}
		var payload OutboxEventPayload
		log.Printf("Payload raw: %s", string(env.Payload))
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			log.Printf("Failed to parse payload: %v", err)
			continue
		}

		go simulateDelivery(client, router, payload)
	}
}

func getMotorcycleSpeedMps() float64 {
	speedKmhStr := os.Getenv("MOTO_SPEED_KMH")
	if speedKmhStr != "" {
		if v, err := strconv.ParseFloat(speedKmhStr, 64); err == nil && v > 0 {
			return v / 3.6
		}
	}
	// Padrão: 36.0 km/h = 10.0 m/s (velocidade média realista de motoboy urbano)
	return 10.0
}

func getSimulationSpeedFactor() float64 {
	factorStr := os.Getenv("SIM_SPEED_FACTOR")
	if factorStr == "" {
		factorStr = os.Getenv("SIMULATION_SPEED_FACTOR")
	}
	if factorStr != "" {
		if v, err := strconv.ParseFloat(factorStr, 64); err == nil && v > 0 {
			return v
		}
	}
	// Padrão: 1.0 (tempo real 1x baseado na duração estimada do trajeto)
	return 1.0
}

func getTelemetryInterval() time.Duration {
	intervalStr := os.Getenv("TELEMETRY_INTERVAL_MS")
	if intervalStr != "" {
		if ms, err := strconv.ParseInt(intervalStr, 10, 64); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return 1 * time.Second
}

func getRouteWithFallback(ctx context.Context, router routing.RouteProvider, from, to domain.Coordinate) domain.Route {
	route, err := router.Route(ctx, from, to)
	if err == nil && len(route.Points) >= 2 {
		log.Printf("Fetched real OSRM street route: %d points, %.1f meters, %v duration",
			len(route.Points), route.Distance, route.Duration)
		return route
	}
	log.Printf("Warning: OSRM route lookup failed (%v); using curved street-like grid fallback", err)
	const nPerLeg = 10
	var points []domain.Coordinate
	// Segment 1: from -> corner (from.Lat, to.Lng)
	for i := 0; i <= nPerLeg; i++ {
		frac := float64(i) / float64(nPerLeg)
		points = append(points, domain.Coordinate{
			Lat: from.Lat,
			Lng: from.Lng + (to.Lng-from.Lng)*frac,
		})
	}
	// Segment 2: corner -> to
	for i := 1; i <= nPerLeg; i++ {
		frac := float64(i) / float64(nPerLeg)
		points = append(points, domain.Coordinate{
			Lat: from.Lat + (to.Lat-from.Lat)*frac,
			Lng: to.Lng,
		})
	}

	motoSpeedMps := getMotorcycleSpeedMps()
	fallbackDistance := 1500.0
	fallbackDuration := time.Duration((fallbackDistance / motoSpeedMps) * float64(time.Second))

	return domain.Route{
		Points:   points,
		Distance: fallbackDistance,
		Duration: fallbackDuration,
	}
}

func simulateLegMovement(
	ctx context.Context,
	client v1connect.LogisticsServiceClient,
	orderID string,
	legName string,
	route domain.Route,
	tickInterval time.Duration,
	speedFactor float64,
) error {
	traj, err := trajectory.NewTrajectory(route)
	if err != nil {
		return fmt.Errorf("failed to build trajectory for %s: %w", legName, err)
	}

	totalDuration := route.Duration
	if totalDuration <= 0 {
		motoSpeedMps := getMotorcycleSpeedMps()
		totalDuration = time.Duration((route.Distance / motoSpeedMps) * float64(time.Second))
	}

	avgSpeedKmh := (route.Distance / totalDuration.Seconds()) * 3.6
	log.Printf("[%s] Starting movement: distance=%.1fm estimated_duration=%v avg_speed=%.1fkm/h (speed_factor=%.1fx)",
		legName, route.Distance, totalDuration, avgSpeedKmh, speedFactor)

	// Step simulation time per tick
	simStep := time.Duration(float64(tickInterval) * speedFactor)
	if simStep <= 0 {
		simStep = tickInterval
	}

	elapsed := time.Duration(0)
	stepCount := 0

	for elapsed < totalDuration {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		elapsed += simStep
		if elapsed > totalDuration {
			elapsed = totalDuration
		}
		stepCount++

		pos := traj.PositionAt(elapsed)
		_, err := client.UpdateLocation(ctx, connect.NewRequest(&pb.UpdateLocationRequest{
			OrderId:         orderID,
			CurrentPosition: &pb.Coordinates{Lat: pos.Lat, Lng: pos.Lng},
		}))
		if err != nil {
			log.Printf("[%s] Warning: UpdateLocation failed at step %d (elapsed %v): %v", legName, stepCount, elapsed, err)
		}

		if elapsed < totalDuration {
			time.Sleep(tickInterval)
		}
	}

	log.Printf("[%s] Arrived at destination: completed %d steps over %v simulation time", legName, stepCount, totalDuration)
	return nil
}

func simulateDelivery(client v1connect.LogisticsServiceClient, router routing.RouteProvider, payload OutboxEventPayload) {
	ctx := context.Background()
	log.Printf("Starting delivery for order: %s", payload.OrderID)

	_, err := client.AcceptDelivery(ctx, connect.NewRequest(&pb.AcceptDeliveryRequest{
		OrderId: payload.OrderID,
	}))
	if err != nil {
		log.Printf("Failed to accept delivery for order %s: %v", payload.OrderID, err)
		return
	}
	log.Printf("Accepted delivery for order %s", payload.OrderID)

	tickInterval := getTelemetryInterval()
	speedFactor := getSimulationSpeedFactor()

	// Leg 1: Deliverer initial location -> Merchant
	driverInitPos := domain.Coordinate{Lat: payload.PickupLat - 0.0050, Lng: payload.PickupLng - 0.0050}
	pickupPos := domain.Coordinate{Lat: payload.PickupLat, Lng: payload.PickupLng}
	dropoffPos := domain.Coordinate{Lat: payload.DropoffLat, Lng: payload.DropoffLng}

	route1 := getRouteWithFallback(ctx, router, driverInitPos, pickupPos)
	leg1Points := make([]*pb.Coordinates, len(route1.Points))
	for i, pt := range route1.Points {
		leg1Points[i] = &pb.Coordinates{Lat: pt.Lat, Lng: pt.Lng}
	}

	// Assign Leg 1 Route to API
	leg1Start := time.Now()
	res1, err := client.AssignRoute(ctx, connect.NewRequest(&pb.AssignRouteRequest{
		OrderId: payload.OrderID,
		Route: &pb.RouteGeometry{
			RouteId:             "route-leg1-" + payload.OrderID,
			Revision:            1,
			Leg:                 "TO_PICKUP",
			Points:              leg1Points,
			DistanceMeters:      route1.Distance,
			EstimatedDurationMs: route1.Duration.Milliseconds(),
		},
	}))
	if err != nil {
		log.Printf("Failed to assign Leg 1 route for order %s: %v", payload.OrderID, err)
	} else {
		log.Printf("[METRIC] Assigned Leg 1 route for order %s: seq=%d duration_ms=%d start_epoch_ns=%d points=%d",
			payload.OrderID, res1.Msg.Sequence, time.Since(leg1Start).Milliseconds(), leg1Start.UnixNano(), len(leg1Points))
	}

	if err := simulateLegMovement(ctx, client, payload.OrderID, "Leg 1 (TO_PICKUP)", route1, tickInterval, speedFactor); err != nil {
		log.Printf("Leg 1 simulation failed: %v", err)
	}
	log.Printf("Arrived at Merchant for order %s", payload.OrderID)

	restaurantWait := time.Duration(float64(5*time.Second) / speedFactor)
	if restaurantWait < 1*time.Second {
		restaurantWait = 1 * time.Second
	}
	time.Sleep(restaurantWait)

	// Leg 2: Merchant -> Customer
	route2 := getRouteWithFallback(ctx, router, pickupPos, dropoffPos)
	leg2Points := make([]*pb.Coordinates, len(route2.Points))
	for i, pt := range route2.Points {
		leg2Points[i] = &pb.Coordinates{Lat: pt.Lat, Lng: pt.Lng}
	}

	// Assign Leg 2 Route to API
	leg2Start := time.Now()
	res2, err := client.AssignRoute(ctx, connect.NewRequest(&pb.AssignRouteRequest{
		OrderId: payload.OrderID,
		Route: &pb.RouteGeometry{
			RouteId:             "route-leg2-" + payload.OrderID,
			Revision:            2,
			Leg:                 "TO_DROPOFF",
			Points:              leg2Points,
			DistanceMeters:      route2.Distance,
			EstimatedDurationMs: route2.Duration.Milliseconds(),
		},
	}))
	if err != nil {
		log.Printf("Failed to assign Leg 2 route for order %s: %v", payload.OrderID, err)
	} else {
		log.Printf("[METRIC] Assigned Leg 2 route for order %s: seq=%d duration_ms=%d start_epoch_ns=%d points=%d",
			payload.OrderID, res2.Msg.Sequence, time.Since(leg2Start).Milliseconds(), leg2Start.UnixNano(), len(leg2Points))
	}

	if err := simulateLegMovement(ctx, client, payload.OrderID, "Leg 2 (TO_DROPOFF)", route2, tickInterval, speedFactor); err != nil {
		log.Printf("Leg 2 simulation failed: %v", err)
	}
	log.Printf("Delivery for order %s completed simulation.", payload.OrderID)
}
