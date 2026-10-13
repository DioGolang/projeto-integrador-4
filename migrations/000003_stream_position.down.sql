DROP TABLE IF EXISTS processed_events;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS stream_position;
