package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Prometheus struct {
	orderCreated     *prometheus.CounterVec
	orderDispatched  *prometheus.CounterVec
	useCaseTotal     *prometheus.CounterVec
	useCaseDuration  *prometheus.HistogramVec
	httpDuration     *prometheus.HistogramVec
	grpcDuration     *prometheus.HistogramVec
	cacheHits        *prometheus.CounterVec
	cacheMisses      *prometheus.CounterVec
	outboxEvents           *prometheus.CounterVec
	outboxPendingEvents    prometheus.Gauge
	outboxOldestPendingAge prometheus.Gauge
	consumerMessages       *prometheus.CounterVec
	consumerDuration       *prometheus.HistogramVec
}

func NewPrometheusMetrics(reg prometheus.Registerer, serviceName string) *Prometheus {
	m := &Prometheus{
		orderCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "gofleet_order_created_total",
			Help:        "Total orders created.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"status"}),
		orderDispatched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "gofleet_order_dispatched_total",
			Help:        "Total orders shipped.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"status"}),
		useCaseTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "app_usecase_total",
			Help:        "Total number of Use Case executions.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"use_case", "status"}),
		useCaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "app_usecase_duration_seconds",
			Help:        "Use Case execution latency.",
			Buckets:     []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"use_case", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "app_http_duration_seconds",
			Help:        "Use Case execution latency.",
			Buckets:     []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"method", "path", "status_code"}),
		grpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "grpc_duration_seconds",
			Help:        "Duration of HTTP requests.",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"grpc_service", "grpc_method", "status_code"}),
		cacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "app_cache_hits_total",
			Help:        "Total non-cache hits",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"cache_type"}),

		cacheMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "app_cache_misses_total",
			Help:        "Total non-cache misses..",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"cache_type"}),

		outboxEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "app_outbox_events_processed_total",
			Help:        "Total outbox events processed.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"status"}),

		outboxPendingEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "app_outbox_events_pending",
			Help:        "Current count of pending outbox events.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}),

		outboxOldestPendingAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "app_outbox_events_oldest_age_seconds",
			Help:        "Age in seconds of the oldest pending outbox event.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}),

		consumerMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "app_event_consumer_messages_total",
			Help:        "Total events handled by the consumer.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"event_type", "status", "redelivered"}),

		consumerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "app_event_consumer_duration_seconds",
			Help:        "Latency of event consumption processing.",
			Buckets:     []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"event_type", "status"}),
	}

	reg.MustRegister(
		m.orderCreated,
		m.orderDispatched,
		m.useCaseTotal,
		m.useCaseDuration,
		m.httpDuration,
		m.grpcDuration,
		m.cacheHits,
		m.cacheMisses,
		m.outboxEvents,
		m.outboxPendingEvents,
		m.outboxOldestPendingAge,
		m.consumerMessages,
		m.consumerDuration,
	)
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return m
}

func (p *Prometheus) RecordOrderCreated(status string) {
	p.orderCreated.WithLabelValues(status).Inc()
}

func (p *Prometheus) RecordOrderDispatched(status string) {
	p.orderDispatched.WithLabelValues(status).Inc()
}

func (p *Prometheus) RecordUseCaseExecution(useCase string, success bool, duration time.Duration) {
	status := "success"
	if !success {
		status = "failure"
	}
	p.useCaseTotal.WithLabelValues(useCase, status).Inc()
	p.useCaseDuration.WithLabelValues(useCase, status).Observe(duration.Seconds())
}

func (p *Prometheus) ObserveHTTPRequestDuration(method, path, code string, duration float64) {
	p.httpDuration.WithLabelValues(method, path, code).Observe(duration)
}

func (p *Prometheus) ObserveGRPCRequestDuration(service, method, code string, duration float64) {
	p.grpcDuration.WithLabelValues(service, method, code).Observe(duration)
}

func (p *Prometheus) IncCacheHit(cacheType string) {
	p.cacheHits.WithLabelValues(cacheType).Inc()
}

func (p *Prometheus) IncCacheMiss(cacheType string) {
	p.cacheMisses.WithLabelValues(cacheType).Inc()
}

func (p *Prometheus) IncOutboxEventsProcessed(status string) {
	p.outboxEvents.WithLabelValues(status).Inc()
}

func (p *Prometheus) SetOutboxPending(count, oldestAgeSeconds float64) {
	p.outboxPendingEvents.Set(count)
	p.outboxOldestPendingAge.Set(oldestAgeSeconds)
}

func (p *Prometheus) RecordConsumerEvent(eventType, status string, redelivered bool, duration time.Duration) {
	p.consumerMessages.WithLabelValues(eventType, status, strconv.FormatBool(redelivered)).Inc()
	p.consumerDuration.WithLabelValues(eventType, status).Observe(duration.Seconds())
}
