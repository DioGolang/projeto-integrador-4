package metrics

import "time"

type Metrics interface {
	// Business
	RecordOrderCreated(status string)
	RecordOrderDispatched(status string)
	RecordUseCaseExecution(useCaseName string, success bool, duration time.Duration)

	// Infrastructure (HTTP & gRPC)
	ObserveHTTPRequestDuration(method, path, statusCode string, duration float64)
	ObserveGRPCRequestDuration(service, method, code string, duration float64)

	// Performance and Resilience
	IncCacheHit(cacheType string)
	IncCacheMiss(cacheType string)
	IncOutboxEventsProcessed(status string)
	SetOutboxPending(count, oldestAgeSeconds float64)

	// Messaging Consumer
	RecordConsumerEvent(eventType, status string, redelivered bool, duration time.Duration)
}
