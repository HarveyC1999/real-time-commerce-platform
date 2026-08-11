CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.order_metrics_hourly (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    currency VARCHAR(3) NOT NULL,
    order_event_count BIGINT NOT NULL,
    created_order_count BIGINT NOT NULL,
    completed_order_count BIGINT NOT NULL,
    revenue NUMERIC(22, 2) NOT NULL,
    average_order_value NUMERIC(22, 6),
    aggregated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, currency),
    CHECK (window_end > window_start),
    CHECK (order_event_count >= 0),
    CHECK (created_order_count >= 0),
    CHECK (completed_order_count >= 0),
    CHECK (revenue >= 0)
);

CREATE TABLE IF NOT EXISTS analytics.order_status_hourly (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    currency VARCHAR(3) NOT NULL,
    order_status VARCHAR(30) NOT NULL,
    status_event_count BIGINT NOT NULL,
    aggregated_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, currency, order_status),
    CHECK (window_end > window_start),
    CHECK (status_event_count >= 0)
);

CREATE TABLE IF NOT EXISTS analytics.stream_batches (
    query_name TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (query_name, batch_id),
    CHECK (row_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_order_metrics_hourly_window_end
    ON analytics.order_metrics_hourly(window_end);

CREATE INDEX IF NOT EXISTS idx_order_status_hourly_window_end
    ON analytics.order_status_hourly(window_end);
