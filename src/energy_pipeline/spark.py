"""SparkSession factory.

Centralises session creation so every entry point gets identical, configured
sessions (timezone, partition sizing, and — for streaming jobs — the Kafka
connector package).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from .config import Config


def build_spark(config: Config, *, with_kafka: bool = False) -> SparkSession:
    """Create (or fetch) a configured SparkSession.

    Parameters
    ----------
    config:
        Active configuration.
    with_kafka:
        When True, registers the spark-sql-kafka package so the session can
        read from / write to Kafka.
    """
    builder = (
        SparkSession.builder.appName(config.spark.app_name)
        .master(config.spark.master)
        .config("spark.sql.session.timeZone", config.spark.timezone)
        .config("spark.sql.files.maxPartitionBytes", config.spark.max_partition_bytes)
        .config("spark.sql.shuffle.partitions", config.spark.shuffle_partitions)
    )
    if with_kafka:
        builder = builder.config("spark.jars.packages", config.kafka.spark_kafka_package)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
