from __future__ import annotations

import logging
from typing import Protocol

from confluent_kafka import Consumer, KafkaError, Message
from pydantic import ValidationError

from app.config import Settings, get_settings
from app.models.order_event import OrderEvent
from app.repositories.order_event_repository import (
    OrderEventRepository,
)

logger = logging.getLogger(__name__)


class ConsumerProtocol(Protocol):
    def subscribe(
        self,
        topics: list[str],
    ) -> None: ...

    def poll(
        self,
        timeout: float,
    ) -> Message | None: ...

    def commit(
        self,
        *,
        message: Message,
        asynchronous: bool,
    ) -> object: ...

    def close(self) -> None: ...


class OrderEventConsumer:
    """Consume order events and persist them in PostgreSQL."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        consumer: ConsumerProtocol | None = None,
        repository: OrderEventRepository | None = None,
    ) -> None:
        self.settings = settings or get_settings()

        self._consumer: ConsumerProtocol = (
            consumer
            or Consumer(
                {
                    "bootstrap.servers": (
                        self.settings.kafka_bootstrap_servers
                    ),
                    "group.id": (
                        self.settings.kafka_consumer_group
                    ),
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
        )

        self._repository = (
            repository
            or OrderEventRepository(
                settings=self.settings
            )
        )

        self._consumer.subscribe(
            [self.settings.kafka_order_topic]
        )

    def process_next(
        self,
        *,
        timeout_seconds: float = 1.0,
    ) -> bool:
        """
        Poll and process one Kafka message.

        Returns False when no message is available.
        """

        message = self._consumer.poll(
            timeout_seconds
        )

        if message is None:
            return False

        error = message.error()

        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return False

            raise RuntimeError(
                f"Kafka consumer error: {error}"
            )

        raw_value = message.value()

        if raw_value is None:
            logger.warning(
                (
                    "Empty Kafka message skipped: "
                    "partition=%s offset=%s"
                ),
                message.partition(),
                message.offset(),
            )

            self._consumer.commit(
                message=message,
                asynchronous=False,
            )

            return True

        try:
            event = OrderEvent.model_validate_json(
                raw_value
            )
        except ValidationError as error:
            logger.warning(
                (
                    "Invalid order event skipped: "
                    "partition=%s offset=%s error=%s"
                ),
                message.partition(),
                message.offset(),
                error,
            )

            self._consumer.commit(
                message=message,
                asynchronous=False,
            )

            return True

        inserted = self._repository.insert(
            event
        )

        self._consumer.commit(
            message=message,
            asynchronous=False,
        )

        logger.info(
            (
                "Order event processed: "
                "event_id=%s inserted=%s "
                "partition=%s offset=%s"
            ),
            event.event_id,
            inserted,
            message.partition(),
            message.offset(),
        )

        return True

    def run_forever(self) -> None:
        """Continuously consume messages until interrupted."""

        logger.info(
            "Order consumer started: topic=%s group=%s",
            self.settings.kafka_order_topic,
            self.settings.kafka_consumer_group,
        )

        try:
            while True:
                self.process_next()
        except KeyboardInterrupt:
            logger.info(
                "Order consumer interrupted."
            )
        finally:
            self.close()

    def close(self) -> None:
        """Close the Kafka consumer."""

        self._consumer.close()
