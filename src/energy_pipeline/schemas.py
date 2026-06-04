"""Explicit, centralised Spark schemas.

Defining schemas once (instead of re-declaring them in every notebook, or
relying on ``inferSchema``) keeps reads deterministic and gives a single
source of truth that the tests can assert against.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

METERS_SCHEMA = StructType(
    [
        StructField("building_id", IntegerType(), True),
        StructField("meter_type", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("row_id", IntegerType(), True),
    ]
)

BUILDINGS_SCHEMA = StructType(
    [
        StructField("site_id", IntegerType(), True),
        StructField("building_id", IntegerType(), True),
        StructField("primary_use", StringType(), True),
        StructField("square_feet", IntegerType(), True),
        StructField("floor_count", IntegerType(), True),
        StructField("row_id", IntegerType(), True),
        StructField("year_built", IntegerType(), True),
        StructField("latent_y", DoubleType(), True),
        StructField("latent_s", DoubleType(), True),
        StructField("latent_r", DoubleType(), True),
    ]
)

WEATHER_SCHEMA = StructType(
    [
        StructField("site_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("air_temperature", DoubleType(), True),
        StructField("cloud_coverage", DoubleType(), True),
        StructField("dew_temperature", DoubleType(), True),
        StructField("sea_level_pressure", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
    ]
)

# Payload schema for a single weather record published to Kafka. The producer
# emits typed JSON (numbers, not strings) so the consumer needs no casting.
WEATHER_EVENT_SCHEMA = StructType(
    [
        StructField("site_id", IntegerType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("air_temperature", DoubleType(), True),
        StructField("cloud_coverage", DoubleType(), True),
        StructField("dew_temperature", DoubleType(), True),
        StructField("sea_level_pressure", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
    ]
)

# A Kafka message is a batch of weather events.
WEATHER_BATCH_SCHEMA = StructType(
    [
        StructField("batch_ts", TimestampType(), True),
        StructField("rows", ArrayType(WEATHER_EVENT_SCHEMA), True),
    ]
)

# Weather columns subject to mean imputation (handled inside the ML pipeline).
WEATHER_NUMERIC_COLS = [
    "air_temperature",
    "cloud_coverage",
    "dew_temperature",
    "sea_level_pressure",
    "wind_direction",
    "wind_speed",
]
