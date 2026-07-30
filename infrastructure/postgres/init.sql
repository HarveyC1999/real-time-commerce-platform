CREATE TABLE IF NOT EXISTS order_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_version INTEGER NOT NULL,

    order_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    order_status VARCHAR(30) NOT NULL,
    order_amount NUMERIC(12, 2) NOT NULL,

    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    raw_event JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_events_order_id
    ON order_events(order_id);

CREATE INDEX IF NOT EXISTS idx_order_events_event_time
    ON order_events(event_time);

CREATE INDEX IF NOT EXISTS idx_order_events_status
    ON order_events(order_status);