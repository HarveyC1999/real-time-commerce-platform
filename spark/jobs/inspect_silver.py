from __future__ import annotations

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_PATH = (
    "/workspace/lakehouse/silver/order_events"
)

QUARANTINE_PATH = (
    "/workspace/lakehouse/quarantine/order_events"
)


def path_exists(
    spark: SparkSession,
    path: str,
) -> bool:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(
        path
    )

    filesystem = (
        hadoop_path
        .getFileSystem(
            spark._jsc.hadoopConfiguration()
        )
    )

    return filesystem.exists(hadoop_path)


def read_parquet_if_available(
    spark: SparkSession,
    path: str,
) -> DataFrame | None:
    """Read a Parquet dataset, treating an empty directory as no data."""

    if not path_exists(spark, path):
        return None

    try:
        return spark.read.parquet(path)
    except AnalysisException as error:
        if "Unable to infer schema" in str(error):
            return None

        raise


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("inspect-commerce-silver")
        .master("local[1]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    silver_events = read_parquet_if_available(
        spark,
        SILVER_PATH,
    )

    if silver_events is not None:
        silver_events.select(
            "event_id",
            "event_type",
            "order_status",
            "order_amount",
            "currency",
            "event_time",
            "event_date",
        ).orderBy(
            F.col("event_time").desc()
        ).show(
            20,
            truncate=False,
        )

        print(
            "Silver row count:",
            silver_events.count(),
        )

        duplicate_count = (
            silver_events
            .groupBy("event_id")
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        print(
            "Duplicate event_id count:",
            duplicate_count,
        )
    else:
        print(
            "Silver has no Parquet data yet."
        )

    quarantine_events = read_parquet_if_available(
        spark,
        QUARANTINE_PATH,
    )

    if quarantine_events is not None:
        quarantine_events.select(
            "validation_errors",
            "raw_event",
            "kafka_partition",
            "kafka_offset",
            "validated_at",
        ).show(
            20,
            truncate=False,
        )

        print(
            "Quarantine row count:",
            quarantine_events.count(),
        )
    else:
        print(
            "Quarantine has no Parquet data yet."
        )

    spark.stop()


if __name__ == "__main__":
    main()
