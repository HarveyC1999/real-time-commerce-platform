from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderEventType(StrEnum):
    ORDER_CREATED = "order_created"
    ORDER_UPDATED = "order_updated"
    ORDER_COMPLETED = "order_completed"
    ORDER_CANCELLED = "order_cancelled"


class OrderStatus(StrEnum):
    PENDING = "pending"
    PAID = "paid"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class OrderEvent(BaseModel):
    """Versioned commerce order event published to Kafka."""

    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
    )

    event_id: UUID = Field(
        default_factory=uuid4,
    )

    event_type: OrderEventType
    event_version: int = Field(
        default=1,
        ge=1,
    )

    order_id: UUID
    customer_id: UUID
    order_status: OrderStatus

    order_amount: Decimal = Field(
        gt=Decimal("0"),
        max_digits=12,
        decimal_places=2,
    )

    currency: str = Field(
        default="USD",
        min_length=3,
        max_length=3,
    )

    event_time: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )

    @field_validator("currency")
    @classmethod
    def normalize_currency(
        cls,
        value: str,
    ) -> str:
        return value.upper()

    @field_validator("event_time")
    @classmethod
    def require_timezone(
        cls,
        value: datetime,
    ) -> datetime:
        if value.tzinfo is None:
            raise ValueError(
                "event_time must include timezone information"
            )

        return value.astimezone(UTC)

    def message_key(self) -> str:
        """Return the Kafka partition key."""

        return str(self.order_id)

    def to_json(self) -> str:
        """Serialize the event for Kafka transport."""

        return self.model_dump_json()