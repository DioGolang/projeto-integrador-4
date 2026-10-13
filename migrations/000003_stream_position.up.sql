-- Add stream_position to outbox_events
ALTER TABLE outbox_events ADD COLUMN stream_position BIGSERIAL UNIQUE;

-- Create processed_events table for IdempotentConsumer
CREATE TABLE processed_events (
    consumer_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_id, event_id)
);
