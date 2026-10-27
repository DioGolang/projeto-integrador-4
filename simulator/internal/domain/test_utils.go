package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func SetupPGTestDB(t *testing.T) *pgxpool.Pool {
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		t.Fatalf("Could not connect to Docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "15-alpine",
		Env: []string{
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_USER=user_name",
			"POSTGRES_DB=dbname",
			"listen_addresses = '*'",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	// Tell docker to hard kill the container in 120 seconds
	resource.Expire(120)

	var dbPool *pgxpool.Pool
	ctx := context.Background()

	dsn := fmt.Sprintf("postgres://user_name:secret@127.0.0.1:%s/dbname?sslmode=disable", resource.GetPort("5432/tcp"))

	pool.MaxWait = 120 * time.Second
	if err = pool.Retry(func() error {
		dbPool, err = pgxpool.New(ctx, dsn)
		if err != nil {
			return err
		}
		return dbPool.Ping(ctx)
	}); err != nil {
		t.Fatalf("Could not connect to docker: %s", err)
	}

	t.Cleanup(func() {
		dbPool.Close()
		if err := pool.Purge(resource); err != nil {
			t.Fatalf("Could not purge resource: %s", err)
		}
	})

	_, err = dbPool.Exec(ctx, `
		CREATE TABLE processed_events (
			consumer_name TEXT NOT NULL,
			event_id TEXT NOT NULL,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (consumer_name, event_id)
		);
		CREATE TABLE mock_business_state (
			id TEXT PRIMARY KEY,
			count INTEGER NOT NULL DEFAULT 0
		);

		CREATE TABLE deliveries (
			id UUID PRIMARY KEY,
			driver_id TEXT NOT NULL,
			state TEXT NOT NULL,
			sequence BIGINT NOT NULL DEFAULT 0,
			pickup_lat DOUBLE PRECISION NOT NULL,
			pickup_lng DOUBLE PRECISION NOT NULL,
			dropoff_lat DOUBLE PRECISION NOT NULL,
			dropoff_lng DOUBLE PRECISION NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE outbox_events (
			id TEXT PRIMARY KEY,
			aggregate_type TEXT NOT NULL,
			aggregate_id UUID NOT NULL,
			sequence BIGINT NOT NULL,
			stream_position BIGSERIAL UNIQUE NOT NULL,
			event_type TEXT NOT NULL,
			payload JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			published_at TIMESTAMPTZ NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NULL,
			next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			dead_lettered_at TIMESTAMPTZ NULL,
			UNIQUE (aggregate_id, sequence)
		);
	`)
	if err != nil {
		t.Fatalf("failed to setup schema: %v", err)
	}

	return dbPool
}

func SeedEvent(ctx context.Context, pool *pgxpool.Pool, ev DomainEvent) (StreamPosition, error) {
	b, _ := json.Marshal(ev)
	query := `
		INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload)
		VALUES ($1, 'delivery', $2, $3, $4, $5)
		RETURNING stream_position
	`
	var pos int64
	err := pool.QueryRow(ctx, query, ev.EventID(), ev.AggregateID(), ev.Sequence(), ev.EventName(), b).Scan(&pos)
	return StreamPosition(pos), err
}
