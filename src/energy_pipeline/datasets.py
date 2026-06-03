"""Dataset assembly for the batch training job.

Builds the labelled feature frame by aggregating meter and weather readings to
6-hour intervals and joining building metadata. The interval is represented as
a real ``interval_start`` timestamp so that :func:`features.engineer_features`
derives time-of-day features the same way it does for the streaming feed.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import features
from .config import Config
from .schemas import BUILDINGS_SCHEMA, METERS_SCHEMA, WEATHER_NUMERIC_COLS, WEATHER_SCHEMA

_HOURS_PER_INTERVAL = 6


def _interval_start(ts_col: str):
    """Floor a timestamp to the start of its 6-hour interval.

    Implemented as ``date_trunc('hour', ts) - (hour % 6) hours`` via a SQL
    expression so it works across Spark 3.4+ (``make_timestamp`` only became a
    ``pyspark.sql.functions`` API in 3.5).
    """
    return F.expr(
        f"date_trunc('hour', `{ts_col}`) "
        f"- make_interval(0, 0, 0, 0, hour(`{ts_col}`) % {_HOURS_PER_INTERVAL}, 0, 0)"
    )


def load_meters(spark: SparkSession, config: Config) -> DataFrame:
    return spark.read.csv(config.paths.meters, header=True, schema=METERS_SCHEMA)


def load_buildings(spark: SparkSession, config: Config) -> DataFrame:
    return spark.read.csv(config.paths.buildings, header=True, schema=BUILDINGS_SCHEMA)


def load_weather(spark: SparkSession, config: Config) -> DataFrame:
    return spark.read.csv(config.paths.weather, header=True, schema=WEATHER_SCHEMA)


def aggregate_meters(meters: DataFrame) -> DataFrame:
    """Sum hourly meter readings into 6-hour interval energy totals."""
    return (
        meters.withColumn("interval_start", _interval_start("ts"))
        .groupBy("building_id", "meter_type", "interval_start")
        .agg(F.sum("value").alias("energy_consumption"))
    )


def aggregate_weather(weather: DataFrame) -> DataFrame:
    """Average weather readings into the same 6-hour intervals per site."""
    return (
        weather.withColumn("interval_start", _interval_start("timestamp"))
        .groupBy("site_id", "interval_start")
        .agg(*[F.avg(c).alias(c) for c in WEATHER_NUMERIC_COLS])
    )


def build_training_frame(spark: SparkSession, config: Config) -> DataFrame:
    """Assemble the labelled, feature-engineered frame for model training.

    Imputation / encoding / assembly are intentionally left to the ML pipeline
    (see :func:`features.build_preprocessing_stages`).
    """
    meters = aggregate_meters(load_meters(spark, config))
    weather = aggregate_weather(load_weather(spark, config))
    buildings = load_buildings(spark, config)

    frame = meters.join(buildings, on="building_id", how="left").join(
        weather, on=["site_id", "interval_start"], how="left"
    )
    return features.engineer_features(frame, ts_col="interval_start")


def compute_outlier_bounds(df: DataFrame, label_col: str, sigma: float):
    """Fit ``(lo, hi)`` label bounds at ``sigma`` std-devs from the mean.

    Fit this on the *training* split only — fitting on the full frame before the
    train/test split would leak test-set statistics into the filter decision.
    Returns ``(None, None)`` when statistics are undefined (e.g. empty frame).
    """
    stats = df.select(F.mean(label_col).alias("mean"), F.stddev(label_col).alias("std")).first()
    mean_val, std_val = stats["mean"], stats["std"]
    if mean_val is None or std_val is None:
        return None, None
    return mean_val - sigma * std_val, mean_val + sigma * std_val


def filter_outliers(df: DataFrame, label_col: str, lo, hi) -> DataFrame:
    """Drop rows whose label falls outside precomputed ``[lo, hi]`` bounds."""
    if lo is None or hi is None:
        return df
    return df.filter((F.col(label_col) >= lo) & (F.col(label_col) <= hi))
