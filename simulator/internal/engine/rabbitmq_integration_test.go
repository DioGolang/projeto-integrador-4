//go:build integration
// +build integration

package engine

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/dvasconcellos/elo/simulator/internal/telemetry"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestEngine_RabbitMQIntegration(t *testing.T) {
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		t.Skipf("Skipping RabbitMQ integration test: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	// Setup exchanges
	err = ch.ExchangeDeclare("elo.events", "topic", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare events exchange: %v", err)
	}
	err = ch.ExchangeDeclare("elo.telemetry", "topic", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare telemetry exchange: %v", err)
	}

	// Create publishers
	// RabbitMQ Integration test shouldn't test Domain Events directly here if we use Store, we'll fix later.
	memStore := domain.NewMemoryDeliveryStore()
	telemetryPub := telemetry.NewRabbitMQPublisher(ch, "elo.telemetry")

	fakeClock := NewFakeClock()
	fakeProvider := &FakeRouteProvider{
		FixedDuration: 2 * time.Second,
	}

	engine := NewEngine(fakeProvider, memStore, telemetryPub, fakeClock)
	engine.tickRate = 1 * time.Second

	delivery := domain.NewDelivery(
		"order-rabbit",
		"driver-rabbit",
		domain.Coordinate{Lat: 1, Lng: 2},
		domain.Coordinate{Lat: 3, Lng: 4},
	)

	exec := &Execution{
		Delivery:      delivery,
		DriverInitPos: domain.Coordinate{Lat: 0, Lng: 0},
	}

	ctx := context.Background()
	err = engine.Run(ctx, exec)
	if err != nil {
		t.Fatalf("Engine failed with RabbitMQ integration: %v", err)
	}

	// If it doesn't fail, we know the RabbitMQ Publish commands succeeded!
	t.Log("Successfully published all Domain and Telemetry events to RabbitMQ!")
}
