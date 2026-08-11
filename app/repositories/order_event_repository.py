from __future__ import annotations

import json
from typing import Protocol

import psycopg

from app.config import (
    Settings,
    get_settings,
    postgres_connection_string,
)
from app.models.order_event import OrderEvent

INSERT_ORDER_EVENT_SQL = """
INSERT INTO order_events (
    event_id,
    event_type,
    event_version,
    order_id,
    customer_id,
    order_status,
    order_amount,
    event_time,
    raw_event
)
VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"""


class CursorProtocol(Protocol):
    def execute(
        self,
        query: str,
        params: tuple[object, ...],
    ) -> object: ...

    def fetchone(self) -> tuple[object, ...] | None: ...


class ConnectionProtocol(Protocol):
    def cursor(self) -> CursorProtocol: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class OrderEventRepository:
    """Persist validated order events in PostgreSQL."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        connection: ConnectionProtocol | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._connection = connection

    def insert(
        self,
        event: OrderEvent,
    ) -> bool:
        """
        Insert an event if its event_id has not been stored.

        Returns True when inserted and False when the event was
        already present.
        """

        connection = (
            self._connection
            or psycopg.connect(
                postgres_connection_string(
                    self.settings
                )
            )
        )

        owns_connection = self._connection is None

        try:
            cursor = connection.cursor()

            cursor.execute(
                INSERT_ORDER_EVENT_SQL,
                (
                    event.event_id,
                    event.event_type.value,
                    event.event_version,
                    event.order_id,
                    event.customer_id,
                    event.order_status.value,
                    event.order_amount,
                    event.event_time,
                    json.dumps(
                        event.model_dump(
                            mode="json"
                        )
                    ),
                ),
            )

            inserted = cursor.fetchone() is not None
            connection.commit()

            return inserted

        except Exception:
            connection.rollback()
            raise

        finally:
            if owns_connection:
                connection.close()
