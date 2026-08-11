from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_ORDER_EVENT_SCHEMA = StructType(
    [
        StructField(
            "message_key",
            StringType(),
            nullable=True,
        ),
        StructField(
            "raw_event",
            StringType(),
            nullable=True,
        ),
        StructField(
            "kafka_topic",
            StringType(),
            nullable=False,
        ),
        StructField(
            "kafka_partition",
            IntegerType(),
            nullable=False,
        ),
        StructField(
            "kafka_offset",
            LongType(),
            nullable=False,
        ),
        StructField(
            "kafka_timestamp",
            TimestampType(),
            nullable=False,
        ),
        StructField(
            "event_id",
            StringType(),
            nullable=True,
        ),
        StructField(
            "event_type",
            StringType(),
            nullable=True,
        ),
        StructField(
            "event_version",
            IntegerType(),
            nullable=True,
        ),
        StructField(
            "order_id",
            StringType(),
            nullable=True,
        ),
        StructField(
            "customer_id",
            StringType(),
            nullable=True,
        ),
        StructField(
            "order_status",
            StringType(),
            nullable=True,
        ),
        StructField(
            "order_amount",
            DecimalType(12, 2),
            nullable=True,
        ),
        StructField(
            "currency",
            StringType(),
            nullable=True,
        ),
        StructField(
            "event_time",
            TimestampType(),
            nullable=True,
        ),
        StructField(
            "ingested_at",
            TimestampType(),
            nullable=False,
        ),
        StructField(
            "is_valid_json",
            BooleanType(),
            nullable=False,
        ),
        StructField(
            "ingestion_date",
            DateType(),
            nullable=True,
        ),
    ]
)


ALLOWED_EVENT_TYPES = [
    "order_created",
    "order_updated",
    "order_completed",
    "order_cancelled",
]

ALLOWED_ORDER_STATUSES = [
    "pending",
    "paid",
    "processing",
    "completed",
    "cancelled",
]

UUID_PATTERN = (
    "^[0-9a-fA-F]{8}-"
    "[0-9a-fA-F]{4}-"
    "[0-9a-fA-F]{4}-"
    "[0-9a-fA-F]{4}-"
    "[0-9a-fA-F]{12}$"
)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("commerce-silver-orders")
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


def read_bronze_stream(
    *,
    spark: SparkSession,
    bronze_path: str,
) -> DataFrame:
    """
    Read new Bronze Parquet files as a stream.

    File-based Structured Streaming sources require an explicit
    schema instead of runtime schema inference.
    """

    return (
        spark.readStream
        .schema(BRONZE_ORDER_EVENT_SCHEMA)
        .format("parquet")
        .option(
            "maxFilesPerTrigger",
            "10",
        )
        .load(bronze_path)
    )


def add_validation_columns(
    bronze_events: DataFrame,
) -> DataFrame:
    """Apply Silver business and schema validation rules."""

    validation_errors = F.array(
        F.when(
            ~F.col("is_valid_json"),
            F.lit("invalid_json"),
        ),
        F.when(
            F.col("event_id").isNull(),
            F.lit("missing_event_id"),
        ),
        F.when(
            (
                F.col("event_id").isNotNull()
                & ~F.col("event_id").rlike(UUID_PATTERN)
            ),
            F.lit("invalid_event_id"),
        ),
        F.when(
            F.col("order_id").isNull(),
            F.lit("missing_order_id"),
        ),
        F.when(
            (
                F.col("order_id").isNotNull()
                & ~F.col("order_id").rlike(UUID_PATTERN)
            ),
            F.lit("invalid_order_id"),
        ),
        F.when(
            F.col("customer_id").isNull(),
            F.lit("missing_customer_id"),
        ),
        F.when(
            (
                F.col("customer_id").isNotNull()
                & ~F.col("customer_id").rlike(UUID_PATTERN)
            ),
            F.lit("invalid_customer_id"),
        ),
        F.when(
            F.col("event_time").isNull(),
            F.lit("missing_event_time"),
        ),
        F.when(
            F.col("event_version").isNull(),
            F.lit("missing_event_version"),
        ),
        F.when(
            F.col("event_version") < 1,
            F.lit("invalid_event_version"),
        ),
        F.when(
            F.col("event_type").isNull(),
            F.lit("missing_event_type"),
        ),
        F.when(
            (
                F.col("event_type").isNotNull()
                & ~F.col("event_type").isin(
                    ALLOWED_EVENT_TYPES
                )
            ),
            F.lit("invalid_event_type"),
        ),
        F.when(
            F.col("order_status").isNull(),
            F.lit("missing_order_status"),
        ),
        F.when(
            (
                F.col("order_status").isNotNull()
                & ~F.col("order_status").isin(
                    ALLOWED_ORDER_STATUSES
                )
            ),
            F.lit("invalid_order_status"),
        ),
        F.when(
            F.col("order_amount").isNull(),
            F.lit("missing_order_amount"),
        ),
        F.when(
            F.col("order_amount") <= 0,
            F.lit("non_positive_order_amount"),
        ),
        F.when(
            F.col("currency").isNull(),
            F.lit("missing_currency"),
        ),
        F.when(
            (
                F.col("currency").isNotNull()
                & ~F.col("currency").rlike(
                    "^[A-Z]{3}$"
                )
            ),
            F.lit("invalid_currency"),
        ),
        F.when(
            F.col("message_key").isNull(),
            F.lit("missing_message_key"),
        ),
        F.when(
            (
                F.col("message_key").isNotNull()
                & F.col("order_id").isNotNull()
                & (
                    F.col("message_key")
                    != F.col("order_id")
                )
            ),
            F.lit("message_key_order_id_mismatch"),
        ),
    )

    return (
        bronze_events
        .withColumn(
            "validation_errors_with_nulls",
            validation_errors,
        )
        .withColumn(
            "validation_errors",
            F.filter(
                F.col(
                    "validation_errors_with_nulls"
                ),
                lambda error: error.isNotNull(),
            ),
        )
        .drop(
            "validation_errors_with_nulls"
        )
        .withColumn(
            "is_valid",
            F.size(
                F.col("validation_errors")
            )
            == 0,
        )
        .withColumn(
            "validated_at",
            F.current_timestamp(),
        )
    )


def build_silver_events(
    validated_events: DataFrame,
    *,
    watermark_delay: str,
) -> DataFrame:
    """
    Keep valid events and deduplicate event_id within the watermark.

    The watermark bounds how long Spark retains deduplication state.
    """

    return (
        validated_events
        .filter(
            F.col("is_valid")
        )
        .withWatermark(
            "event_time",
            watermark_delay,
        )
        .dropDuplicatesWithinWatermark(
            ["event_id"]
        )
        .select(
            "event_id",
            "event_type",
            "event_version",
            "order_id",
            "customer_id",
            "order_status",
            "order_amount",
            "currency",
            "event_time",
            "message_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            "validated_at",
        )
        .withColumn(
            "event_date",
            F.to_date("event_time"),
        )
    )


def build_quarantine_events(
    validated_events: DataFrame,
) -> DataFrame:
    """Keep invalid records and the reasons they failed."""

    return (
        validated_events
        .filter(
            ~F.col("is_valid")
        )
        .select(
            "raw_event",
            "validation_errors",
            "message_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            "validated_at",
        )
        .withColumn(
            "quarantine_date",
            F.to_date("validated_at"),
        )
    )


def start_parquet_stream(
    *,
    events: DataFrame,
    query_name: str,
    output_path: str,
    checkpoint_path: str,
    partition_column: str,
):
    return (
        events.writeStream
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
            partition_column
        )
        .trigger(
            processingTime="10 seconds"
        )
        .queryName(query_name)
        .start()
    )


def main() -> None:
    bronze_path = os.getenv(
        "BRONZE_ORDER_EVENTS_PATH",
        "/workspace/lakehouse/bronze/order_events",
    )

    silver_path = os.getenv(
        "SILVER_ORDER_EVENTS_PATH",
        "/workspace/lakehouse/silver/order_events",
    )

    quarantine_path = os.getenv(
        "QUARANTINE_ORDER_EVENTS_PATH",
        (
            "/workspace/lakehouse/quarantine/"
            "order_events"
        ),
    )

    silver_checkpoint_path = os.getenv(
        "SILVER_CHECKPOINT_PATH",
        (
            "/workspace/lakehouse/checkpoints/"
            "silver_order_events"
        ),
    )

    quarantine_checkpoint_path = os.getenv(
        "QUARANTINE_CHECKPOINT_PATH",
        (
            "/workspace/lakehouse/checkpoints/"
            "quarantine_order_events"
        ),
    )

    watermark_delay = os.getenv(
        "SILVER_WATERMARK_DELAY",
        "7 days",
    )

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_events = read_bronze_stream(
        spark=spark,
        bronze_path=bronze_path,
    )

    validated_events = add_validation_columns(
        bronze_events
    )

    silver_events = build_silver_events(
        validated_events,
        watermark_delay=watermark_delay,
    )

    quarantine_events = build_quarantine_events(
        validated_events
    )

    silver_query = start_parquet_stream(
        events=silver_events,
        query_name="commerce_silver_orders",
        output_path=silver_path,
        checkpoint_path=silver_checkpoint_path,
        partition_column="event_date",
    )

    quarantine_query = start_parquet_stream(
        events=quarantine_events,
        query_name=(
            "commerce_quarantine_orders"
        ),
        output_path=quarantine_path,
        checkpoint_path=(
            quarantine_checkpoint_path
        ),
        partition_column="quarantine_date",
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        silver_query.stop()
        quarantine_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
