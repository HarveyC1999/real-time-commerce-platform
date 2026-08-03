from __future__ import annotations

import logging

from app.consumers.order_consumer import (
    OrderEventConsumer,
)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    consumer = OrderEventConsumer()
    consumer.run_forever()


if __name__ == "__main__":
    main()