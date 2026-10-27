package otel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

// ProviderConfig aggregates runtime configuration for OpenTelemetry initialization.
type ProviderConfig struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
	CollectorAddr  string
	SamplingRatio  float64
}

// NormalizeEndpoint cleans the endpoint string removing http(s) schemes and trailing slashes for gRPC transport.
func NormalizeEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimRight(endpoint, "/")
	if endpoint == "" {
		return "localhost:4317"
	}
	return endpoint
}

// InitProvider configures and registers the global TracerProvider and W3C TextMapPropagator.
// It returns a shutdown function that gracefully drains queued spans within the provided context.
func InitProvider(ctx context.Context, cfg ProviderConfig) (func(context.Context) error, error) {
	endpoint := NormalizeEndpoint(cfg.CollectorAddr)

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create otlp exporter: %w", err)
	}

	serviceVersion := cfg.ServiceVersion
	if serviceVersion == "" {
		serviceVersion = "1.0.0"
	}
	environment := cfg.Environment
	if environment == "" {
		environment = "development"
	}
	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "logistics-api"
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			"",
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(serviceVersion),
			semconv.DeploymentEnvironmentKey.String(environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var sampler sdktrace.Sampler
	if cfg.SamplingRatio >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if cfg.SamplingRatio <= 0.0 {
		sampler = sdktrace.NeverSample()
	} else {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplingRatio))
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(traceProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	shutdown := func(shutCtx context.Context) error {
		if err := traceProvider.Shutdown(shutCtx); err != nil {
			otel.Handle(err)
			return err
		}
		return nil
	}

	return shutdown, nil
}
