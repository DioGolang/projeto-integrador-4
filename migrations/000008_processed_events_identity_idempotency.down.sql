-- Revert processed_events expansion
DROP INDEX IF EXISTS idx_processed_events_consumer_agg_seq;

ALTER TABLE processed_events
    DROP CONSTRAINT IF EXISTS uq_processed_events_consumer_agg_seq;

ALTER TABLE processed_events
    DROP COLUMN IF EXISTS payload_hash,
    DROP COLUMN IF EXISTS event_type,
    DROP COLUMN IF EXISTS sequence,
    DROP COLUMN IF EXISTS aggregate_id;
