from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from app.models.order_event import (
    OrderEvent,
    OrderEventType,
    OrderStatus,
)


class OrderEventFactory:
    """Generate valid, reproducible commerce order events."""

    def __init__(
        self,
        *,
        seed: int | None = None,
    ) -> None:
        self._random = random.Random(seed)

    def create(
        self,
        *,
        event_time: datetime | None = None,
    ) -> OrderEvent:
        status = self._random.choice(
            [
                OrderStatus.PENDING,
                OrderStatus.PAID,
                OrderStatus.PROCESSING,
                OrderStatus.COMPLETED,
                OrderStatus.CANCELLED,
            ]
        )

        return OrderEvent(
            event_type=self._event_type_for_status(
                status
            ),
            order_id=uuid4(),
            customer_id=uuid4(),
            order_status=status,
            order_amount=self._random_amount(),
            currency="USD",
            event_time=(
                event_time
                or self._recent_event_time()
            ),
        )

    def create_batch(
        self,
        count: int,
    ) -> list[OrderEvent]:
        if count < 1:
            raise ValueError(
                "count must be greater than zero"
            )

        return [
            self.create()
            for _ in range(count)
        ]

    def _random_amount(self) -> Decimal:
        amount_in_cents = self._random.randint(
            500,
            50_000,
        )

        return (
            Decimal(amount_in_cents)
            / Decimal("100")
        )

    def _recent_event_time(self) -> datetime:
        seconds_ago = self._random.randint(
            0,
            86_400,
        )

        return (
            datetime.now(UTC)
            - timedelta(seconds=seconds_ago)
        )

    @staticmethod
    def _event_type_for_status(
        status: OrderStatus,
    ) -> OrderEventType:
        mapping = {
            OrderStatus.PENDING: (
                OrderEventType.ORDER_CREATED
            ),
            OrderStatus.PAID: (
                OrderEventType.ORDER_UPDATED
            ),
            OrderStatus.PROCESSING: (
                OrderEventType.ORDER_UPDATED
            ),
            OrderStatus.COMPLETED: (
                OrderEventType.ORDER_COMPLETED
            ),
            OrderStatus.CANCELLED: (
                OrderEventType.ORDER_CANCELLED
            ),
        }

        return mapping[status]