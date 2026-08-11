from __future__ import annotations

import psycopg

from app.config import (
    Settings,
    get_settings,
    postgres_connection_string,
)


def main() -> None:
    settings: Settings = get_settings()

    with psycopg.connect(
        postgres_connection_string(settings)
    ) as connection:
        tables = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'analytics'
            ORDER BY table_name
            """
        ).fetchall()

        print("Analytics tables:", ", ".join(row[0] for row in tables))

        for table_name in (
            "order_metrics_hourly",
            "order_status_hourly",
        ):
            row = connection.execute(
                f"SELECT COUNT(*) FROM analytics.{table_name}"
            ).fetchone()
            count = 0 if row is None else row[0]
            print(f"{table_name} row count: {count}")

        recent_metrics = connection.execute(
            """
            SELECT
                window_start,
                currency,
                created_order_count,
                completed_order_count,
                revenue,
                average_order_value
            FROM analytics.order_metrics_hourly
            ORDER BY window_start DESC, currency
            LIMIT 10
            """
        ).fetchall()

        if recent_metrics:
            print("Recent order metrics:")
            for row in recent_metrics:
                print(row)


if __name__ == "__main__":
    main()
