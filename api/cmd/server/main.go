package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/rs/cors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"connectrpc.com/connect"
	authv1connect "github.com/dvasconcellos/elo/api/gen/auth/v1/v1connect"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/internal/config"
	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/interceptor"
	"github.com/dvasconcellos/elo/api/internal/postgres"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/service"
	"github.com/dvasconcellos/elo/api/internal/store"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/internal/worker"
	"github.com/dvasconcellos/elo/api/pkg/auth"
	"github.com/dvasconcellos/elo/api/pkg/logger"
	"github.com/dvasconcellos/elo/api/pkg/metrics"
	"github.com/dvasconcellos/elo/api/pkg/otel"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Server encapsulate the HTTP server and its dependencies
type Server struct {
	httpServer *http.Server
	db         *sql.DB
	store      store.Store
	logger     logger.Logger
	metrics    metrics.Metrics
	registry   prometheus.Gatherer
	cfg        *config.Config
	hub        stream.LiveHub
}

// ServerOption defines the functional options pattern for Server
type ServerOption func(*Server)

func WithLogger(l logger.Logger) ServerOption {
	return func(s *Server) {
		s.logger = l
	}
}

func WithDB(database *sql.DB) ServerOption {
	return func(s *Server) {
		s.db = database
		s.store = store.NewStore(database)
	}
}

func WithConfig(cfg *config.Config) ServerOption {
	return func(s *Server) {
		s.cfg = cfg
	}
}

func WithMetrics(m metrics.Metrics) ServerOption {
	return func(s *Server) {
		s.metrics = m
	}
}

func WithRegistry(reg prometheus.Gatherer) ServerOption {
	return func(s *Server) {
		s.registry = reg
	}
}

func WithHub(hub stream.LiveHub) ServerOption {
	return func(s *Server) {
		s.hub = hub
	}
}

func NewServer(opts ...ServerOption) *Server {
	s := &Server{}

	for _, opt := range opts {
		opt(s)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Expose metrics endpoint using application registry
	mux.Handle("/metrics", s.metricsHandler())

	// Initialize Auth components
	hasher, _ := auth.NewBcryptHasher(auth.DefaultBcryptCost)
	refreshMgr := auth.NewRefreshTokenManager()
	var jwtMgr auth.JWTManager
	if s.cfg != nil && len(s.cfg.JWTSecret) >= auth.MinSecretKeyLength {
		jwtMgr, _ = auth.NewHMACJWTManager(auth.JWTConfig{
			SecretKey: []byte(s.cfg.JWTSecret),
			Issuer:    "gofleet.auth",
			Audience:  "gofleet.api",
			TTL:       15 * time.Minute,
		})
	}
	authInterceptor := interceptor.NewAuthInterceptor(jwtMgr)
	connectOpts := []connect.HandlerOption{connect.WithInterceptors(authInterceptor)}

	if s.cfg == nil {
		if s.logger != nil {
			s.logger.Error(context.Background(), "FATAL: server configuration is nil (Fail-Closed)")
		}
		panic("fail-closed: server configuration must not be nil")
	}

	var previousKey []byte
	if s.cfg.PreviousPINHMACSecret != "" {
		previousKey = []byte(s.cfg.PreviousPINHMACSecret)
	}

	pinMgr, pinErr := auth.NewHMACPINManager([]byte(s.cfg.PINHMACSecret), previousKey)
	if pinErr != nil {
		if s.logger != nil {
			s.logger.Error(context.Background(), "FATAL: invalid PIN HMAC secret configuration", logger.WithError(pinErr))
		}
		panic(fmt.Sprintf("fail-closed: invalid PIN HMAC secret: %v", pinErr))
	}
	baseLogisticsSvc := service.NewLogisticsService(s.logger, s.store, s.hub, pinMgr)
	logisticsSvc := service.NewMetricsLogisticsService(baseLogisticsSvc, s.metrics)

	// Register Connect RPC Handlers with Auth Interceptor
	path, handler := v1connect.NewLogisticsServiceHandler(logisticsSvc, connectOpts...)
	mux.Handle(path, handler)

	authSvc := service.NewAuthServiceServer(s.db, db.New(s.db), hasher, jwtMgr, refreshMgr, s.logger)
	authPath, authHandler := authv1connect.NewAuthServiceHandler(authSvc, connectOpts...)
	mux.Handle(authPath, authHandler)

	legacyAuthSvc := service.NewLegacyAuthServiceServer(authSvc)
	legacyPath, legacyHandler := v1connect.NewAuthServiceHandler(legacyAuthSvc, connectOpts...)
	mux.Handle(legacyPath, legacyHandler)

	port := "8080"
	if s.cfg != nil && s.cfg.APIPort != "" {
		port = s.cfg.APIPort
	}

	s.httpServer = &http.Server{
		Addr:    ":" + port,
		Handler: s.decorateHandler(mux),
	}

	return s
}

func (s *Server) metricsHandler() http.Handler {
	gatherer := s.registry
	if gatherer == nil {
		gatherer = prometheus.DefaultGatherer
	}
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}

func (s *Server) decorateHandler(h http.Handler) http.Handler {
	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000", "http://localhost:3001", "http://127.0.0.1:3000", "http://127.0.0.1:3001"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token", "Connect-Protocol-Version"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}).Handler(h)

	return otel.WithHTTPObservability(corsHandler, s.metrics)
}

func (s *Server) Start() error {
	s.logger.Info(context.Background(), "Starting server", logger.String("port", s.httpServer.Addr))
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info(ctx, "Shutting down server gracefully")
	return s.httpServer.Shutdown(ctx)
}

func main() {
	// 1. Config Loading (12-Factor App)
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load environment configuration: %v\n", err)
		os.Exit(1)
	}

	// 2. Logger Setup
	isProd := cfg.LogLevel != "debug"
	appLogger := logger.NewZapLogger("logistics-api", isProd)

	// 3. Metrics Setup (Prometheus)
	reg := prometheus.NewRegistry()
	appMetrics := metrics.NewPrometheusMetrics(reg, "logistics-api")

	// 4. OTel Provider Setup
	shutdownOTel, err := otel.InitProvider(context.Background(), otel.ProviderConfig{
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		Environment:    cfg.Environment,
		CollectorAddr:  cfg.NormalizedOTelEndpoint(),
		SamplingRatio:  cfg.OTelSamplingRatio,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize OTel provider: %v\n", err)
		os.Exit(1)
	}

	// 5. Graceful Shutdown Context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 6. Database Connection & Tuning (pgx driver)
	dsn := cfg.DSN()

	// Override se DB_URL existir (para Docker)
	if envDSN := os.Getenv("DB_URL"); envDSN != "" {
		dsn = envDSN
	}

	database, err := sql.Open("pgx", dsn)
	if err != nil {
		appLogger.Error(context.Background(), "Failed to open DB connection", logger.WithError(err))
		os.Exit(1)
	}
	defer database.Close()

	database.SetMaxOpenConns(50)
	database.SetMaxIdleConns(50)
	database.SetConnMaxLifetime(5 * time.Minute)

	if err := database.Ping(); err != nil {
		appLogger.Error(context.Background(), "Failed to ping database", logger.WithError(err))
		os.Exit(1)
	}

	reg.MustRegister(
		collectors.NewDBStatsCollector(database, "logistics_db"),
	)

	// 6.5 RabbitMQ Connection
	amqpUrl := os.Getenv("RABBITMQ_URL")
	if amqpUrl == "" {
		amqpUrl = fmt.Sprintf("amqp://%s:%s@%s:%s/", cfg.RabbitUser, cfg.RabbitPassword, cfg.RabbitHost, cfg.RabbitPort)
	}
	conn, err := amqp.Dial(amqpUrl)
	if err != nil {
		appLogger.Error(context.Background(), "Failed to connect to RabbitMQ", logger.WithError(err))
		os.Exit(1)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		appLogger.Error(context.Background(), "Failed to open RabbitMQ channel", logger.WithError(err))
		os.Exit(1)
	}
	defer ch.Close()

	// Start Outbox Worker
	outboxWorker := worker.NewOutboxWorker(database, db.New(database), appLogger, ch, worker.WithMetrics(appMetrics))
	go outboxWorker.Start(ctx)

	// Stream Consumer Pipeline:
	// RabbitMQ -> stream.Consumer -> (SequenceValidator | GapRecovery) -> Projection COMMIT -> LiveHub
	hub := stream.NewMemoryLiveHub()

	queries := db.New(database)
	eventStore := postgres.NewEventStore(queries)
	validator := stream.NewSequenceValidator()
	projection := delivery.NewProjection(database, "DeliveryProjection")

	recovery := stream.NewGapRecovery(eventStore, validator, projection, hub)
	strConsumer := stream.NewConsumer(validator, recovery, projection, hub)

	// Dedicated channel: consumption must not share flow control with the outbox publisher.
	consumerCh, err := conn.Channel()
	if err != nil {
		appLogger.Error(context.Background(), "Failed to open RabbitMQ consumer channel", logger.WithError(err))
		os.Exit(1)
	}
	defer consumerCh.Close()

	rabbitConsumer := rabbitmq.NewConsumer(
		consumerCh,
		strConsumer,
		rabbitmq.WithConsumerMetrics(appMetrics),
		rabbitmq.WithConsumerLogger(appLogger),
	)

	messages, err := declareProjectionTopology(consumerCh)
	if err != nil {
		appLogger.Error(context.Background(), "Failed to set up projection consumer topology", logger.WithError(err))
		os.Exit(1)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					appLogger.Error(context.Background(), "Projection consumer channel closed")
					return
				}
				if err := rabbitConsumer.HandleDelivery(ctx, msg); err != nil {
					appLogger.Error(context.Background(), "Consumer failed to handle delivery", logger.WithError(err))
				}
			}
		}
	}()

	// 7. Server Instantiation
	srv := NewServer(
		WithLogger(appLogger),
		WithDB(database),
		WithConfig(cfg),
		WithMetrics(appMetrics),
		WithRegistry(reg),
		WithHub(hub),
	)

	// 8. Start Server in a Goroutine
	go func() {
		if err := srv.Start(); err != nil {
			appLogger.Error(context.Background(), "Server failed", logger.WithError(err))
			os.Exit(1)
		}
	}()

	// 9. Block until signal is received
	<-ctx.Done()

	// 10. Graceful Shutdown Execution
	stop()
	appLogger.Info(context.Background(), "Shutdown signal received, initiating graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Stop(shutdownCtx); err != nil {
		appLogger.Error(context.Background(), "Server forced to shutdown", logger.WithError(err))
	}

	otelShutdownCtx, cancelOTel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelOTel()
	if err := shutdownOTel(otelShutdownCtx); err != nil {
		appLogger.Error(context.Background(), "Failed to cleanly shutdown OTel provider", logger.WithError(err))
	}

	appLogger.Info(context.Background(), "Server exited properly")
	os.Exit(0)
}
