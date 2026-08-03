from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from app.config import Settings
from app.consumers.order_consumer import (
    OrderEventConsumer,
)
from app.models.order_event import (
    OrderEvent,
    OrderEventType,
    OrderStatus,
)


class FakeMessage:
    def __init__(
        self,
        *,
        value: bytes | None,
        error: Any = None,
    ) -> None:
        self._value = value
        self._error = error

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> Any:
        return self._error

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 10


class FakeConsumer:
    def __init__(
        self,
        message: FakeMessage | None,
    ) -> None:
        self.message = message
        self.subscriptions: list[
            list[str]
        ] = []
        self.commits: list[
            dict[str, object]
        ] = []
        self.closed = False

    def subscribe(
        self,
        topics: list[str],
    ) -> None:
        self.subscriptions.append(topics)

    def poll(
        self,
        timeout: float,
    ) -> FakeMessage | None:
        return self.message

    def commit(
        self,
        *,
        message: FakeMessage,
        asynchronous: bool,
    ) -> None:
        self.commits.append(
            {
                "message": message,
                "asynchronous": asynchronous,
            }
        )

    def close(self) -> None:
        self.closed = True


class FakeRepository:
    def __init__(
        self,
        *,
        inserted: bool = True,
        error: Exception | None = None,
    ) -> None:
        self.inserted = inserted
        self.error = error
        self.events: list[OrderEvent] = []

    def insert(
        self,
        event: OrderEvent,
    ) -> bool:
        if self.error is not None:
            raise self.error

        self.events.append(event)
        return self.inserted


@pytest.fixture
def event() -> OrderEvent:
    return OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("49.99"),
    )


@pytest.fixture
def settings() -> Settings:
    return Settings(
        kafka_order_topic="test.orders.v1",
        kafka_consumer_group="test-consumer",
    )


def test_consumer_subscribes_to_order_topic(
    settings: Settings,
) -> None:
    fake_consumer = FakeConsumer(None)

    OrderEventConsumer(
        settings=settings,
        consumer=fake_consumer,
        repository=FakeRepository(),
    )

    assert fake_consumer.subscriptions == [
        ["test.orders.v1"]
    ]


def test_process_next_persists_and_commits(
    event: OrderEvent,
    settings: Settings,
) -> None:
    message = FakeMessage(
        value=event.to_json().encode("utf-8")
    )

    fake_consumer = FakeConsumer(message)
    repository = FakeRepository()

    consumer = OrderEventConsumer(
        settings=settings,
        consumer=fake_consumer,
        repository=repository,
    )

    processed = consumer.process_next()

    assert processed is True
    assert repository.events == [event]

    assert fake_consumer.commits == [
        {
            "message": message,
            "asynchronous": False,
        }
    ]


def test_process_next_does_not_commit_when_database_fails(
    event: OrderEvent,
    settings: Settings,
) -> None:
    message = FakeMessage(
        value=event.to_json().encode("utf-8")
    )

    fake_consumer = FakeConsumer(message)

    consumer = OrderEventConsumer(
        settings=settings,
        consumer=fake_consumer,
        repository=FakeRepository(
            error=RuntimeError(
                "Database failure"
            )
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="Database failure",
    ):
        consumer.process_next()

    assert fake_consumer.commits == []


def test_process_next_returns_false_without_message(
    settings: Settings,
) -> None:
    consumer = OrderEventConsumer(
        settings=settings,
        consumer=FakeConsumer(None),
        repository=FakeRepository(),
    )

    assert consumer.process_next() is False


def test_process_next_rejects_empty_value(
    settings: Settings,
) -> None:
    fake_consumer = FakeConsumer(
        FakeMessage(value=None)
    )

    consumer = OrderEventConsumer(
        settings=settings,
        consumer=fake_consumer,
        repository=FakeRepository(),
    )

    with pytest.raises(
        ValueError,
        match="cannot be empty",
    ):
        consumer.process_next()

    assert fake_consumer.commits == []