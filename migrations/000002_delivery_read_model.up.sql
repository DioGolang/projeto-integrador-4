CREATE TABLE delivery_read_model (
    delivery_id UUID PRIMARY KEY,
    driver_id TEXT NOT NULL,
    state TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    pickup_lat DOUBLE PRECISION NOT NULL,
    pickup_lng DOUBLE PRECISION NOT NULL,
    dropoff_lat DOUBLE PRECISION NOT NULL,
    dropoff_lng DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_sequence BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
