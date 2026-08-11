from __future__ import annotations

import time
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import Producer

from app.config import Settings
from app.models.order_event import (
    OrderEvent,
    OrderEventType,
    OrderStatus,
)
from app.producers.order_producer import (
    OrderEventProducer,
)

POLL_INTERVAL_SECONDS = 0.25
WAIT_TIMEOUT_SECONDS = 15.0


def _connection_string(
    settings: Settings,
) -> str:
    return (
        f"host={settings.postgres_host} "
        f"port={settings.postgres_port} "
        f"dbname={settings.postgres_database} "
        f"user={settings.postgres_user} "
        f"password={settings.postgres_password}"
    )


def _count_event(
    *,
    settings: Settings,
    event_id: object,
) -> int:
    with psycopg.connect(
        _connection_string(settings)
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM order_events
                WHERE event_id = %s
                """,
                (event_id,),
            )

            row = cursor.fetchone()

    if row is None:
        return 0

    return int(row[0])


def _wait_for_event(
    *,
    settings: Settings,
    event_id: object,
) -> int:
    deadline = (
        time.monotonic()
        + WAIT_TIMEOUT_SECONDS
    )

    while time.monotonic() < deadline:
        count = _count_event(
            settings=settings,
            event_id=event_id,
        )

        if count > 0:
            return count

        time.sleep(
            POLL_INTERVAL_SECONDS
        )

    return 0


def _publish_values(
    *,
    settings: Settings,
    key: str,
    values: list[str | None],
) -> None:
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    for value in values:
        producer.produce(
            settings.kafka_order_topic,
            key=key,
            value=value,
        )

    remaining_messages = producer.flush(10.0)
    if remaining_messages:
        raise RuntimeError(
            f"{remaining_messages} integration message(s) were not delivered."
        )


@pytest.mark.integration
def test_duplicate_event_is_stored_once() -> None:
    settings = Settings()

    event = OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("199.99"),
        currency="USD",
    )

    producer = OrderEventProducer(
        settings=settings
    )

    try:
        producer.publish(event)
        producer.publish(event)
    finally:
        producer.close()

    stored_count = _wait_for_event(
        settings=settings,
        event_id=event.event_id,
    )

    assert stored_count == 1

    time.sleep(1)

    assert (
        _count_event(
            settings=settings,
            event_id=event.event_id,
        )
        == 1
    )


@pytest.mark.integration
def test_poison_pills_do_not_block_following_valid_event() -> None:
    settings = Settings()
    event = OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("79.99"),
        currency="USD",
    )

    _publish_values(
        settings=settings,
        key=event.message_key(),
        values=[
            None,
            "this-is-not-json",
            event.to_json(),
        ],
    )

    assert (
        _wait_for_event(
            settings=settings,
            event_id=event.event_id,
        )
        == 1
    )
