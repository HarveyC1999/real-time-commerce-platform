from decimal import Decimal

import pytest

from app.models.order_event import (
    OrderEventType,
    OrderStatus,
)
from app.services.order_event_factory import (
    OrderEventFactory,
)


def test_create_batch_returns_requested_count() -> None:
    factory = OrderEventFactory(seed=42)

    events = factory.create_batch(10)

    assert len(events) == 10
    assert len(
        {
            event.event_id
            for event in events
        }
    ) == 10


def test_factory_generates_positive_amounts() -> None:
    factory = OrderEventFactory(seed=42)

    events = factory.create_batch(20)

    assert all(
        event.order_amount > Decimal("0")
        for event in events
    )


def test_factory_is_reproducible_for_business_fields() -> None:
    first_factory = OrderEventFactory(seed=42)
    second_factory = OrderEventFactory(seed=42)

    first_events = first_factory.create_batch(5)
    second_events = second_factory.create_batch(5)

    first_values = [
        (
            event.event_type,
            event.order_status,
            event.order_amount,
        )
        for event in first_events
    ]

    second_values = [
        (
            event.event_type,
            event.order_status,
            event.order_amount,
        )
        for event in second_events
    ]

    assert first_values == second_values


@pytest.mark.parametrize(
    ("status", "expected_event_type"),
    [
        (
            OrderStatus.PENDING,
            OrderEventType.ORDER_CREATED,
        ),
        (
            OrderStatus.PAID,
            OrderEventType.ORDER_UPDATED,
        ),
        (
            OrderStatus.PROCESSING,
            OrderEventType.ORDER_UPDATED,
        ),
        (
            OrderStatus.COMPLETED,
            OrderEventType.ORDER_COMPLETED,
        ),
        (
            OrderStatus.CANCELLED,
            OrderEventType.ORDER_CANCELLED,
        ),
    ],
)
def test_event_type_matches_status(
    status: OrderStatus,
    expected_event_type: OrderEventType,
) -> None:
    result = (
        OrderEventFactory
        ._event_type_for_status(status)
    )

    assert result == expected_event_type


def test_create_batch_rejects_invalid_count() -> None:
    factory = OrderEventFactory(seed=42)

    with pytest.raises(
        ValueError,
        match="greater than zero",
    ):
        factory.create_batch(0)