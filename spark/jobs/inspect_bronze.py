from pyspark.sql import SparkSession

BRONZE_PATH = (
    "/workspace/lakehouse/bronze/order_events"
)


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("inspect-commerce-bronze")
        .master("local[1]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.parquet(
        BRONZE_PATH
    )

    events.select(
        "event_id",
        "event_type",
        "order_status",
        "order_amount",
        "kafka_partition",
        "kafka_offset",
        "is_valid_json",
    ).show(
        20,
        truncate=False,
    )

    print(
        "Bronze row count:",
        events.count(),
    )

    spark.stop()


if __name__ == "__main__":
    main()