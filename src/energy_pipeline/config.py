"""Centralised, environment-overridable configuration.

All runtime knobs (paths, Kafka coordinates, Spark settings, model
hyper-parameters) live here instead of being hard-coded across notebooks.
Values are read from environment variables with sane defaults, so the same
code runs unchanged on a laptop, in CI, and in a container.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


def _env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, default))


@dataclass(frozen=True)
class Paths:
    """Filesystem locations. Override the root with ``DATA_ROOT`` / ``ARTIFACT_ROOT``."""

    data_root: str = _env("DATA_ROOT", "data")
    artifact_root: str = _env("ARTIFACT_ROOT", "artifacts")

    @property
    def meters(self) -> str:
        return f"{self.data_root}/meters.csv"

    @property
    def buildings(self) -> str:
        return f"{self.data_root}/building_information.csv"

    @property
    def buildings_stream(self) -> str:
        return f"{self.data_root}/new_building_information.csv"

    @property
    def weather(self) -> str:
        return f"{self.data_root}/weather.csv"

    @property
    def model(self) -> str:
        return f"{self.artifact_root}/energy_pipeline_model"

    @property
    def predictions(self) -> str:
        return f"{self.artifact_root}/output/predictions"

    @property
    def hourly_aggregations(self) -> str:
        return f"{self.artifact_root}/output/hourly_aggregations"

    @property
    def daily_aggregations(self) -> str:
        return f"{self.artifact_root}/output/daily_aggregations"

    @property
    def checkpoint_root(self) -> str:
        return f"{self.artifact_root}/checkpoint"


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    weather_topic: str = _env("KAFKA_WEATHER_TOPIC", "weather-stream")
    predictions_topic: str = _env("KAFKA_PREDICTIONS_TOPIC", "predictions-stream")
    hourly_topic: str = _env("KAFKA_HOURLY_TOPIC", "hourly-stream")
    daily_topic: str = _env("KAFKA_DAILY_TOPIC", "daily-stream")
    # Kafka connector coordinates are pinned to the Spark/Scala version.
    spark_kafka_package: str = _env(
        "SPARK_KAFKA_PACKAGE",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
    )


@dataclass(frozen=True)
class SparkConfig:
    app_name: str = _env("SPARK_APP_NAME", "building-energy")
    master: str = _env("SPARK_MASTER", "local[*]")
    timezone: str = _env("SPARK_TIMEZONE", "Australia/Melbourne")
    max_partition_bytes: str = _env("SPARK_MAX_PARTITION_BYTES", "32m")
    shuffle_partitions: int = _env_int("SPARK_SHUFFLE_PARTITIONS", 64)


@dataclass(frozen=True)
class ModelConfig:
    label_col: str = "energy_consumption"
    seed: int = _env_int("MODEL_SEED", 2025)
    # Fraction of rows used for training. Default 1.0 (full data); set
    # MODEL_SAMPLE_FRACTION=0.05 for a fast laptop/demo run.
    sample_fraction: float = _env_float("MODEL_SAMPLE_FRACTION", 1.0)
    train_ratio: float = _env_float("MODEL_TRAIN_RATIO", 0.8)
    outlier_sigma: float = _env_float("MODEL_OUTLIER_SIGMA", 3.0)
    cv_folds: int = _env_int("MODEL_CV_FOLDS", 3)


@dataclass(frozen=True)
class StreamConfig:
    watermark: str = _env("STREAM_WATERMARK", "10 minutes")
    trigger_predictions: str = _env("STREAM_TRIGGER_PRED", "5 seconds")
    trigger_hourly: str = _env("STREAM_TRIGGER_HOURLY", "1 minute")
    trigger_daily: str = _env("STREAM_TRIGGER_DAILY", "5 minutes")
    hourly_window: str = _env("STREAM_HOURLY_WINDOW", "6 hours")
    daily_window: str = _env("STREAM_DAILY_WINDOW", "1 day")


@dataclass(frozen=True)
class DashboardConfig:
    """Settings for the Kafka -> Prometheus exporter behind the Grafana dashboard."""

    exporter_port: int = _env_int("DASHBOARD_EXPORTER_PORT", 8000)


@dataclass(frozen=True)
class Config:
    paths: Paths = field(default_factory=Paths)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    stream: StreamConfig = field(default_factory=StreamConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)

    def as_dict(self) -> dict:
        return asdict(self)


# Meter types present in the dataset. The streaming weather feed carries no
# meter information, so we fan each weather reading out across these types to
# match the feature space the model was trained on (see features.py).
METER_TYPES: list[str] = ["c", "e", "h", "s"]


def load_config() -> Config:
    """Build the active configuration from the current environment."""
    return Config()
