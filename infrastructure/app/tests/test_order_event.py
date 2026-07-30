from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.models.order_event import (
    OrderEvent,
    OrderEventType,
    OrderStatus,
)


def test_order_event_normalizes_currency() -> None:
    event = OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("99.99"),
        currency="usd",
    )

    assert event.currency == "USD"


def test_order_event_uses_order_id_as_message_key() -> None:
    order_id = uuid4()

    event = OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=order_id,
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("99.99"),
    )

    assert event.message_key() == str(order_id)


def test_order_event_serializes_to_json() -> None:
    event = OrderEvent(
        event_type=OrderEventType.ORDER_COMPLETED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.COMPLETED,
        order_amount=Decimal("150.00"),
    )

    serialized = event.to_json()

    assert '"event_type":"order_completed"' in serialized
    assert '"order_status":"completed"' in serialized
    assert '"order_amount":"150.00"' in serialized


def test_order_event_rejects_non_positive_amount() -> None:
    with pytest.raises(ValidationError):
        OrderEvent(
            event_type=OrderEventType.ORDER_CREATED,
            order_id=uuid4(),
            customer_id=uuid4(),
            order_status=OrderStatus.PENDING,
            order_amount=Decimal("0"),
        )


def test_order_event_rejects_naive_datetime() -> None:
    with pytest.raises(
        ValidationError,
        match="event_time must include timezone",
    ):
        OrderEvent(
            event_type=OrderEventType.ORDER_CREATED,
            order_id=uuid4(),
            customer_id=uuid4(),
            order_status=OrderStatus.PENDING,
            order_amount=Decimal("10.00"),
            event_time=datetime(2026, 7, 30, 10, 0),
        )


def test_order_event_converts_datetime_to_utc() -> None:
    event = OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("10.00"),
        event_time=datetime.now(UTC),
    )

    assert event.event_time.tzinfo == UTC


def test_order_event_is_immutable() -> None:
    event = OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("10.00"),
    )

    with pytest.raises(ValidationError):
        event.currency = "TWD"