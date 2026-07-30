from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Protocol

from confluent_kafka import KafkaError, Message, Producer

from app.config import Settings, get_settings
from app.models.order_event import OrderEvent

logger = logging.getLogger(__name__)


DeliveryCallback = Callable[
    [KafkaError | None, Message],
    None,
]


class ProducerProtocol(Protocol):
    def produce(
        self,
        topic: str,
        *,
        key: str,
        value: str,
        on_delivery: DeliveryCallback,
    ) -> None: ...

    def poll(
        self,
        timeout: float,
    ) -> int: ...

    def flush(
        self,
        timeout: float | None = None,
    ) -> int: ...


class OrderEventProducer:
    """Publish validated order events to Kafka."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        producer: ProducerProtocol | None = None,
    ) -> None:
        self.settings = settings or get_settings()

        self._producer: ProducerProtocol = (
            producer
            or Producer(
                {
                    "bootstrap.servers": (
                        self.settings.kafka_bootstrap_servers
                    ),
                    "client.id": "commerce-order-producer",
                    "enable.idempotence": True,
                    "acks": "all",
                }
            )
        )

    def publish(
        self,
        event: OrderEvent,
    ) -> None:
        """
        Queue one order event for delivery.

        Delivery callbacks are served through poll() or flush().
        """

        self._producer.produce(
            self.settings.kafka_order_topic,
            key=event.message_key(),
            value=event.to_json(),
            on_delivery=self._on_delivery,
        )

        self._producer.poll(0)

    def close(
        self,
        *,
        timeout_seconds: float = 10.0,
    ) -> None:
        """Flush queued messages before shutting down."""

        remaining_messages = self._producer.flush(
            timeout_seconds
        )

        if remaining_messages:
            raise RuntimeError(
                "Kafka producer closed with "
                f"{remaining_messages} undelivered message(s)."
            )

    @staticmethod
    def _on_delivery(
        error: KafkaError | None,
        message: Message,
    ) -> None:
        if error is not None:
            logger.error(
                "Order event delivery failed: error=%s",
                error,
            )
            return

        logger.info(
            (
                "Order event delivered: "
                "topic=%s partition=%s offset=%s"
            ),
            message.topic(),
            message.partition(),
            message.offset(),
        )