from __future__ import annotations

import argparse
import logging

from app.producers.order_producer import (
    OrderEventProducer,
)
from app.services.order_event_factory import (
    OrderEventFactory,
)


def parse_args(
    argv: list[str] | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish generated commerce order events."
        )
    )

    generation_mode = (
        parser.add_mutually_exclusive_group()
    )

    generation_mode.add_argument(
        "--count",
        type=int,
        help=(
            "Number of independent events to publish "
            "(default: 100)."
        ),
    )

    generation_mode.add_argument(
        "--orders",
        type=int,
        help=(
            "Number of realistic order lifecycles "
            "to publish."
        ),
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

    return parser.parse_args(argv)


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

    if args.orders is not None:
        events = factory.create_lifecycles(
            args.orders
        )
        description = (
            f"{args.orders} order lifecycle(s)"
        )
    else:
        event_count = (
            args.count
            if args.count is not None
            else 100
        )
        events = factory.create_batch(
            event_count
        )
        description = (
            f"{event_count} independent event(s)"
        )

    producer = OrderEventProducer()

    try:
        for event in events:
            producer.publish(event)
    finally:
        producer.close()

    print(
        f"Published {len(events)} event(s) for "
        f"{description}."
    )


if __name__ == "__main__":
    main()
