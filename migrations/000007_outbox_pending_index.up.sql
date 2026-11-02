CREATE INDEX IF NOT EXISTS idx_outbox_events_pending_stream
ON outbox_events (stream_position ASC)
WHERE status = 'PENDING';
