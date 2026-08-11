from datetime import UTC, datetime

from scripts.load_analytics import (
    _metric_values,
    _status_values,
)


def test_metric_values_normalize_timestamps_to_utc() -> None:
    row = {
        "window_start": datetime(2026, 8, 6, 8),
        "window_end": datetime(2026, 8, 6, 9),
        "currency": "USD",
        "order_event_count": 10,
        "created_order_count": 4,
        "completed_order_count": 2,
        "revenue": 100,
        "average_order_value": 50,
        "aggregated_at": datetime(2026, 8, 7, 15),
    }

    values = _metric_values(row)

    assert values[0].tzinfo is UTC
    assert values[1].tzinfo is UTC
    assert values[8].tzinfo is UTC


def test_status_values_preserve_business_fields() -> None:
    row = {
        "window_start": datetime(2026, 8, 6, 8, tzinfo=UTC),
        "window_end": datetime(2026, 8, 6, 9, tzinfo=UTC),
        "currency": "USD",
        "order_status": "completed",
        "status_event_count": 5,
        "aggregated_at": datetime(2026, 8, 7, 15, tzinfo=UTC),
    }

    values = _status_values(row)

    assert values[2:5] == ("USD", "completed", 5)
