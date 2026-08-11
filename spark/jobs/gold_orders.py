from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SILVER_ORDER_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_version", IntegerType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("order_status", StringType(), nullable=False),
        StructField("order_amount", DecimalType(12, 2), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("message_key", StringType(), nullable=True),
        StructField("kafka_topic", StringType(), nullable=False),
        StructField("kafka_partition", IntegerType(), nullable=False),
        StructField("kafka_offset", LongType(), nullable=False),
        StructField("kafka_timestamp", TimestampType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("validated_at", TimestampType(), nullable=False),
        StructField("event_date", DateType(), nullable=True),
    ]
)


def build_spark_session() -> SparkSession:
    """Create the local Gold Structured Streaming session."""

    return (
        SparkSession.builder.appName("commerce-gold-orders")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def read_silver_stream(
    *,
    spark: SparkSession,
    silver_path: str,
) -> DataFrame:
    """Read newly committed Silver Parquet files with an explicit schema."""

    return (
        spark.readStream.schema(SILVER_ORDER_EVENT_SCHEMA)
        .format("parquet")
        .option("maxFilesPerTrigger", "10")
        .load(silver_path)
    )


def build_order_metrics(
    silver_events: DataFrame,
    *,
    watermark_delay: str,
    window_duration: str,
) -> DataFrame:
    """Aggregate finalized order lifecycle and revenue metrics by currency."""

    completed_amount = F.when(
        F.col("event_type") == "order_completed",
        F.col("order_amount"),
    )

    return (
        silver_events.withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window("event_time", window_duration).alias("event_window"),
            "currency",
        )
        .agg(
            F.count("*").alias("order_event_count"),
            F.sum(
                F.when(F.col("event_type") == "order_created", 1).otherwise(0)
            ).alias("created_order_count"),
            F.sum(
                F.when(F.col("event_type") == "order_completed", 1).otherwise(0)
            ).alias("completed_order_count"),
            F.coalesce(
                F.sum(completed_amount),
                F.lit(0).cast(DecimalType(20, 2)),
            ).alias("revenue"),
            F.avg(completed_amount).alias("average_order_value"),
        )
        .select(
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "currency",
            "order_event_count",
            "created_order_count",
            "completed_order_count",
            "revenue",
            "average_order_value",
        )
        .withColumn("window_date", F.to_date("window_start"))
        .withColumn("aggregated_at", F.current_timestamp())
    )


def build_status_distribution(
    silver_events: DataFrame,
    *,
    watermark_delay: str,
    window_duration: str,
) -> DataFrame:
    """Count lifecycle events by status in finalized event-time windows."""

    return (
        silver_events.withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window("event_time", window_duration).alias("event_window"),
            "currency",
            "order_status",
        )
        .agg(F.count("*").alias("status_event_count"))
        .select(
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "currency",
            "order_status",
            "status_event_count",
        )
        .withColumn("window_date", F.to_date("window_start"))
        .withColumn("aggregated_at", F.current_timestamp())
    )


def start_gold_stream(
    *,
    metrics: DataFrame,
    query_name: str,
    output_path: str,
    checkpoint_path: str,
):
    """Append only event-time windows finalized by the watermark."""

    return (
        metrics.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("window_date")
        .trigger(processingTime="10 seconds")
        .queryName(query_name)
        .start()
    )


def main() -> None:
    silver_path = os.getenv(
        "SILVER_ORDER_EVENTS_PATH",
        "/workspace/lakehouse/silver/order_events",
    )
    metrics_path = os.getenv(
        "GOLD_ORDER_METRICS_PATH",
        "/workspace/lakehouse/gold/order_metrics",
    )
    status_path = os.getenv(
        "GOLD_ORDER_STATUS_PATH",
        "/workspace/lakehouse/gold/order_status_distribution",
    )
    metrics_checkpoint_path = os.getenv(
        "GOLD_ORDER_METRICS_CHECKPOINT_PATH",
        "/workspace/lakehouse/checkpoints/gold_order_metrics",
    )
    status_checkpoint_path = os.getenv(
        "GOLD_ORDER_STATUS_CHECKPOINT_PATH",
        "/workspace/lakehouse/checkpoints/gold_order_status_distribution",
    )
    watermark_delay = os.getenv("GOLD_WATERMARK_DELAY", "1 day")
    window_duration = os.getenv("GOLD_WINDOW_DURATION", "1 hour")

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_events = read_silver_stream(spark=spark, silver_path=silver_path)
    order_metrics = build_order_metrics(
        silver_events,
        watermark_delay=watermark_delay,
        window_duration=window_duration,
    )
    status_distribution = build_status_distribution(
        silver_events,
        watermark_delay=watermark_delay,
        window_duration=window_duration,
    )

    metrics_query = start_gold_stream(
        metrics=order_metrics,
        query_name="commerce_gold_order_metrics",
        output_path=metrics_path,
        checkpoint_path=metrics_checkpoint_path,
    )
    status_query = start_gold_stream(
        metrics=status_distribution,
        query_name="commerce_gold_order_status_distribution",
        output_path=status_path,
        checkpoint_path=status_checkpoint_path,
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        metrics_query.stop()
        status_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
