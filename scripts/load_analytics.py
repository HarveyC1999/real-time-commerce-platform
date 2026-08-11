from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from app.config import get_settings, postgres_connection_string

ORDER_METRICS_COLUMNS = [
    "window_start",
    "window_end",
    "currency",
    "order_event_count",
    "created_order_count",
    "completed_order_count",
    "revenue",
    "average_order_value",
    "aggregated_at",
]

ORDER_STATUS_COLUMNS = [
    "window_start",
    "window_end",
    "currency",
    "order_status",
    "status_event_count",
    "aggregated_at",
]

UPSERT_ORDER_METRICS_SQL = """
INSERT INTO analytics.order_metrics_hourly (
    window_start,
    window_end,
    currency,
    order_event_count,
    created_order_count,
    completed_order_count,
    revenue,
    average_order_value,
    aggregated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (window_start, currency) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    order_event_count = EXCLUDED.order_event_count,
    created_order_count = EXCLUDED.created_order_count,
    completed_order_count = EXCLUDED.completed_order_count,
    revenue = EXCLUDED.revenue,
    average_order_value = EXCLUDED.average_order_value,
    aggregated_at = EXCLUDED.aggregated_at,
    loaded_at = CURRENT_TIMESTAMP
"""

UPSERT_ORDER_STATUS_SQL = """
INSERT INTO analytics.order_status_hourly (
    window_start,
    window_end,
    currency,
    order_status,
    status_event_count,
    aggregated_at
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (window_start, currency, order_status) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    status_event_count = EXCLUDED.status_event_count,
    aggregated_at = EXCLUDED.aggregated_at,
    loaded_at = CURRENT_TIMESTAMP
"""


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)

    return value.astimezone(UTC)


def _read_rows(
    path: Path,
    columns: list[str],
) -> list[dict[str, Any]]:
    try:
        import pyarrow.dataset as dataset
    except ImportError as error:
        raise RuntimeError(
            "Analytics dependencies are missing. Install with: "
            "python -m pip install -e '.[analytics]'"
        ) from error

    parquet_files = sorted(path.rglob("part-*.parquet"))
    if not parquet_files:
        raise RuntimeError(f"No Gold Parquet files found in {path}.")

    parquet_dataset = dataset.dataset(
        [str(file) for file in parquet_files],
        format="parquet",
    )
    return parquet_dataset.to_table(columns=columns).to_pylist()


def _metric_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _as_utc(row["window_start"]),
        _as_utc(row["window_end"]),
        row["currency"],
        row["order_event_count"],
        row["created_order_count"],
        row["completed_order_count"],
        row["revenue"],
        row["average_order_value"],
        _as_utc(row["aggregated_at"]),
    )


def _status_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _as_utc(row["window_start"]),
        _as_utc(row["window_end"]),
        row["currency"],
        row["order_status"],
        row["status_event_count"],
        _as_utc(row["aggregated_at"]),
    )


def main() -> None:
    metrics_path = Path(
        os.getenv(
            "GOLD_ORDER_METRICS_PATH",
            "lakehouse/gold/order_metrics",
        )
    )
    status_path = Path(
        os.getenv(
            "GOLD_ORDER_STATUS_PATH",
            "lakehouse/gold/order_status_distribution",
        )
    )

    metric_rows = _read_rows(metrics_path, ORDER_METRICS_COLUMNS)
    status_rows = _read_rows(status_path, ORDER_STATUS_COLUMNS)

    settings = get_settings()
    with psycopg.connect(
        postgres_connection_string(settings)
    ) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                UPSERT_ORDER_METRICS_SQL,
                (_metric_values(row) for row in metric_rows),
            )
            cursor.executemany(
                UPSERT_ORDER_STATUS_SQL,
                (_status_values(row) for row in status_rows),
            )

    print(
        "Analytics load complete: "
        f"metrics={len(metric_rows)} statuses={len(status_rows)}"
    )


if __name__ == "__main__":
    main()
