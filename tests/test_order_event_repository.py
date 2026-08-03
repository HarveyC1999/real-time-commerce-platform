from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from app.models.order_event import (
    OrderEvent,
    OrderEventType,
    OrderStatus,
)
from app.repositories.order_event_repository import (
    OrderEventRepository,
)


class FakeCursor:
    def __init__(
        self,
        *,
        inserted: bool,
    ) -> None:
        self.inserted = inserted
        self.executed_query: str | None = None
        self.executed_params: tuple[
            object,
            ...,
        ] | None = None

    def execute(
        self,
        query: str,
        params: tuple[object, ...],
    ) -> None:
        self.executed_query = query
        self.executed_params = params

    def fetchone(
        self,
    ) -> tuple[object, ...] | None:
        if self.inserted:
            return ("event-id",)

        return None


class FakeConnection:
    def __init__(
        self,
        *,
        inserted: bool = True,
        error: Exception | None = None,
    ) -> None:
        self.fake_cursor = FakeCursor(
            inserted=inserted
        )
        self.error = error
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> FakeCursor:
        if self.error is not None:
            raise self.error

        return self.fake_cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def event() -> OrderEvent:
    return OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("49.99"),
    )


def test_insert_commits_new_event(
    event: OrderEvent,
) -> None:
    connection = FakeConnection(
        inserted=True
    )

    repository = OrderEventRepository(
        connection=connection
    )

    inserted = repository.insert(event)

    assert inserted is True
    assert connection.committed is True
    assert connection.rolled_back is False

    params = (
        connection.fake_cursor.executed_params
    )

    assert params is not None
    assert params[0] == event.event_id
    assert params[3] == event.order_id


def test_insert_returns_false_for_duplicate(
    event: OrderEvent,
) -> None:
    connection = FakeConnection(
        inserted=False
    )

    repository = OrderEventRepository(
        connection=connection
    )

    inserted = repository.insert(event)

    assert inserted is False
    assert connection.committed is True


def test_insert_rolls_back_on_failure(
    event: OrderEvent,
) -> None:
    connection = FakeConnection(
        error=RuntimeError(
            "Database failure"
        )
    )

    repository = OrderEventRepository(
        connection=connection
    )

    with pytest.raises(
        RuntimeError,
        match="Database failure",
    ):
        repository.insert(event)

    assert connection.committed is False
    assert connection.rolled_back is True