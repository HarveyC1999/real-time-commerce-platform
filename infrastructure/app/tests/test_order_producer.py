from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from app.config import Settings
from app.models.order_event import (
    OrderEvent,
    OrderEventType,
    OrderStatus,
)
from app.producers.order_producer import (
    DeliveryCallback,
    OrderEventProducer,
)


class FakeProducer:
    def __init__(
        self,
        *,
        remaining_messages: int = 0,
    ) -> None:
        self.remaining_messages = remaining_messages
        self.produced_messages: list[
            dict[str, Any]
        ] = []
        self.poll_calls: list[float] = []
        self.flush_calls: list[
            float | None
        ] = []

    def produce(
        self,
        topic: str,
        *,
        key: str,
        value: str,
        on_delivery: DeliveryCallback,
    ) -> None:
        self.produced_messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "on_delivery": on_delivery,
            }
        )

    def poll(
        self,
        timeout: float,
    ) -> int:
        self.poll_calls.append(timeout)
        return 0

    def flush(
        self,
        timeout: float | None = None,
    ) -> int:
        self.flush_calls.append(timeout)
        return self.remaining_messages


@pytest.fixture
def event() -> OrderEvent:
    return OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("49.99"),
    )


def test_publish_sends_event_to_configured_topic(
    event: OrderEvent,
) -> None:
    fake_producer = FakeProducer()

    settings = Settings(
        kafka_bootstrap_servers="localhost:19092",
        kafka_order_topic="test.orders.v1",
    )

    producer = OrderEventProducer(
        settings=settings,
        producer=fake_producer,
    )

    producer.publish(event)

    assert len(
        fake_producer.produced_messages
    ) == 1

    produced = (
        fake_producer.produced_messages[0]
    )

    assert produced["topic"] == "test.orders.v1"
    assert produced["key"] == str(event.order_id)
    assert produced["value"] == event.to_json()
    assert callable(produced["on_delivery"])
    assert fake_producer.poll_calls == [0]


def test_close_flushes_messages() -> None:
    fake_producer = FakeProducer()

    producer = OrderEventProducer(
        producer=fake_producer,
    )

    producer.close(
        timeout_seconds=5.0,
    )

    assert fake_producer.flush_calls == [5.0]


def test_close_raises_when_messages_remain() -> None:
    fake_producer = FakeProducer(
        remaining_messages=2,
    )

    producer = OrderEventProducer(
        producer=fake_producer,
    )

    with pytest.raises(
        RuntimeError,
        match="2 undelivered",
    ):
        producer.close()