from __future__ import annotations

import argparse
import logging

from app.producers.order_producer import (
    OrderEventProducer,
)
from app.services.order_event_factory import (
    OrderEventFactory,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish generated commerce order events."
        )
    )

    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of events to publish.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help=(
            "Random seed used for reproducible "
            "event generation."
        ),
    )

    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s %(message)s"
        ),
    )

    args = parse_args()

    factory = OrderEventFactory(
        seed=args.seed
    )

    events = factory.create_batch(
        args.count
    )

    producer = OrderEventProducer()

    try:
        for event in events:
            producer.publish(event)
    finally:
        producer.close()

    print(
        f"Published {len(events)} order events."
    )


if __name__ == "__main__":
    main()