from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from app.models.order_event import (
    OrderEvent,
    OrderEventType,
    OrderStatus,
)
from app.producers.order_producer import (
    OrderEventProducer,
)


def main() -> None:
    event = OrderEvent(
        event_type=OrderEventType.ORDER_CREATED,
        order_id=uuid4(),
        customer_id=uuid4(),
        order_status=OrderStatus.PENDING,
        order_amount=Decimal("129.99"),
        currency="USD",
    )

    producer = OrderEventProducer()

    try:
        producer.publish(event)
    finally:
        producer.close()

    print(
        "Published order event:",
        event.event_id,
    )


if __name__ == "__main__":
    main()