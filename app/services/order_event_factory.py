from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

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

    def create_lifecycle(
        self,
        *,
        start_time: datetime | None = None,
        cancelled: bool | None = None,
    ) -> list[OrderEvent]:
        """Generate an ordered lifecycle for one commerce order."""

        is_cancelled = (
            cancelled
            if cancelled is not None
            else self._random.random() < 0.2
        )

        statuses = (
            [
                OrderStatus.PENDING,
                OrderStatus.CANCELLED,
            ]
            if is_cancelled
            else [
                OrderStatus.PENDING,
                OrderStatus.PAID,
                OrderStatus.PROCESSING,
                OrderStatus.COMPLETED,
            ]
        )

        order_id = uuid4()
        customer_id = uuid4()
        order_amount = self._random_amount()
        lifecycle_start = (
            start_time
            or self._recent_lifecycle_start()
        )

        return [
            self._create_lifecycle_event(
                status=status,
                order_id=order_id,
                customer_id=customer_id,
                order_amount=order_amount,
                event_time=(
                    lifecycle_start
                    + timedelta(minutes=index * 5)
                ),
            )
            for index, status in enumerate(statuses)
        ]

    def create_lifecycles(
        self,
        count: int,
    ) -> list[OrderEvent]:
        """Generate and flatten lifecycles for multiple orders."""

        if count < 1:
            raise ValueError(
                "count must be greater than zero"
            )

        return [
            event
            for _ in range(count)
            for event in self.create_lifecycle()
        ]

    @staticmethod
    def _create_lifecycle_event(
        *,
        status: OrderStatus,
        order_id: UUID,
        customer_id: UUID,
        order_amount: Decimal,
        event_time: datetime,
    ) -> OrderEvent:
        return OrderEvent(
            event_type=(
                OrderEventFactory
                ._event_type_for_status(status)
            ),
            order_id=order_id,
            customer_id=customer_id,
            order_status=status,
            order_amount=order_amount,
            currency="USD",
            event_time=event_time,
        )

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

    def _recent_lifecycle_start(self) -> datetime:
        seconds_ago = self._random.randint(
            3_600,
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
