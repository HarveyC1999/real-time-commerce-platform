from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDER_EVENT_SCHEMA = StructType(
    [
        StructField(
            "event_id",
            StringType(),
            nullable=False,
        ),
        StructField(
            "event_type",
            StringType(),
            nullable=False,
        ),
        StructField(
            "event_version",
            IntegerType(),
            nullable=False,
        ),
        StructField(
            "order_id",
            StringType(),
            nullable=False,
        ),
        StructField(
            "customer_id",
            StringType(),
            nullable=False,
        ),
        StructField(
            "order_status",
            StringType(),
            nullable=False,
        ),
        StructField(
            "order_amount",
            DecimalType(12, 2),
            nullable=False,
        ),
        StructField(
            "currency",
            StringType(),
            nullable=False,
        ),
        StructField(
            "event_time",
            TimestampType(),
            nullable=False,
        ),
    ]
)


def build_spark_session() -> SparkSession:
    """Create the local Structured Streaming session."""

    return (
        SparkSession.builder
        .appName("commerce-bronze-orders")
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )
        .getOrCreate()
    )


def read_order_events(
    *,
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    """Read order events and Kafka metadata as a stream."""

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            bootstrap_servers,
        )
        .option(
            "subscribe",
            topic,
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "failOnDataLoss",
            "true",
        )
        .load()
    )


def transform_to_bronze(
    kafka_events: DataFrame,
) -> DataFrame:
    """
    Parse event JSON while preserving transport metadata.

    Invalid JSON remains visible through a null parsed_event value and
    the original raw payload, allowing later quality processing.
    """

    return (
        kafka_events
        .select(
            F.col("key").cast("string").alias(
                "message_key"
            ),
            F.col("value").cast("string").alias(
                "raw_event"
            ),
            F.col("topic").alias(
                "kafka_topic"
            ),
            F.col("partition").alias(
                "kafka_partition"
            ),
            F.col("offset").alias(
                "kafka_offset"
            ),
            F.col("timestamp").alias(
                "kafka_timestamp"
            ),
        )
        .withColumn(
            "parsed_json",
            F.try_parse_json(F.col("raw_event")),
        )
        .withColumn(
            "parsed_event",
            F.from_json(
                F.col("raw_event"),
                ORDER_EVENT_SCHEMA,
            ),
        )
        .select(
            "message_key",
            "raw_event",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            F.col("parsed_event.event_id"),
            F.col("parsed_event.event_type"),
            F.col("parsed_event.event_version"),
            F.col("parsed_event.order_id"),
            F.col("parsed_event.customer_id"),
            F.col("parsed_event.order_status"),
            F.col("parsed_event.order_amount"),
            F.col("parsed_event.currency"),
            F.col("parsed_event.event_time"),
            F.col("parsed_json").isNotNull().alias(
                "is_valid_json"
            ),
        )
        .withColumn(
            "ingested_at",
            F.current_timestamp(),
        )
        .withColumn(
            "ingestion_date",
            F.to_date("ingested_at"),
        )
    )


def start_bronze_stream(
    *,
    bronze_events: DataFrame,
    output_path: str,
    checkpoint_path: str,
):
    """Start the append-only Bronze Parquet stream."""

    return (
        bronze_events.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            output_path,
        )
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .partitionBy(
            "ingestion_date"
        )
        .trigger(
            processingTime="10 seconds"
        )
        .queryName(
            "commerce_bronze_orders"
        )
        .start()
    )


def main() -> None:
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "broker:9092",
    )

    topic = os.getenv(
        "KAFKA_ORDER_TOPIC",
        "commerce.orders.v1",
    )

    output_path = os.getenv(
        "BRONZE_ORDER_EVENTS_PATH",
        "/workspace/lakehouse/bronze/order_events",
    )

    checkpoint_path = os.getenv(
        "BRONZE_CHECKPOINT_PATH",
        (
            "/workspace/lakehouse/checkpoints/"
            "bronze_order_events"
        ),
    )

    spark = build_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    kafka_events = read_order_events(
        spark=spark,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )

    bronze_events = transform_to_bronze(
        kafka_events
    )

    query = start_bronze_stream(
        bronze_events=bronze_events,
        output_path=output_path,
        checkpoint_path=checkpoint_path,
    )

    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
