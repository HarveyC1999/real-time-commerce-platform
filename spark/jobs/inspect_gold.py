from __future__ import annotations

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ORDER_METRICS_PATH = "/workspace/lakehouse/gold/order_metrics"
ORDER_STATUS_PATH = "/workspace/lakehouse/gold/order_status_distribution"


def path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
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
        SparkSession.builder.appName("inspect-commerce-gold")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    order_metrics = read_parquet_if_available(spark, ORDER_METRICS_PATH)
    if order_metrics is None:
        print("Gold order metrics have no finalized windows yet.")
    else:
        order_metrics.orderBy(F.col("window_start").desc()).show(
            20,
            truncate=False,
        )
        print("Gold order metric row count:", order_metrics.count())

    status_distribution = read_parquet_if_available(spark, ORDER_STATUS_PATH)
    if status_distribution is None:
        print("Gold status distribution has no finalized windows yet.")
    else:
        status_distribution.orderBy(
            F.col("window_start").desc(),
            "order_status",
        ).show(20, truncate=False)
        print("Gold status row count:", status_distribution.count())

    spark.stop()


if __name__ == "__main__":
    main()
