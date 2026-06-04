"""Spark Structured Streaming inference (replaces the streaming notebook).

Consumes the weather topic, reconstructs the exact training feature space via
the shared :mod:`features` module and the persisted pipeline, scores each
record, and writes per-record predictions plus windowed aggregations.

Design note — meter_type
-------------------------
The model is trained per (building, meter_type), but the weather feed carries
no meter information. To reconstruct the trained feature space we fan each
weather/building record out across the known meter types
(:data:`config.METER_TYPES`). This is an explicit, documented choice rather
than dropping the feature (which would silently change the model contract).

Run:
    python -m energy_pipeline.stream_infer
"""

from __future__ import annotations

import logging

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import features
from .config import METER_TYPES, Config, load_config
from .schemas import BUILDINGS_SCHEMA, WEATHER_BATCH_SCHEMA
from .spark import build_spark

log = logging.getLogger("energy_pipeline.stream_infer")


def _checkpoint(config: Config, name: str) -> str:
    return f"{config.paths.checkpoint_root}/{name}"


def build_feature_stream(spark: SparkSession, config: Config) -> DataFrame:
    """Read the weather topic and produce the model-ready feature stream."""
    buildings = spark.read.csv(config.paths.buildings_stream, header=True, schema=BUILDINGS_SCHEMA)
    meter_types = spark.createDataFrame([(m,) for m in METER_TYPES], ["meter_type"])

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("subscribe", config.kafka.weather_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), WEATHER_BATCH_SCHEMA).alias("d"))
        .select(F.explode("d.rows").alias("e"))
        .select("e.*")
        .withWatermark("event_time", config.stream.watermark)
    )

    enriched = events.join(buildings, on="site_id", how="left").crossJoin(meter_types)
    return features.engineer_features(enriched, ts_col="event_time")


def start_queries(spark: SparkSession, config: Config) -> list:
    """Load the pipeline, build prediction streams, and start all sinks."""
    model = PipelineModel.load(config.paths.model)
    predictions = model.transform(build_feature_stream(spark, config)).select(
        "event_time", "site_id", "building_id", "meter_type", "prediction"
    )

    queries = []

    # 1) Raw per-record predictions -> Parquet (append).
    queries.append(
        predictions.writeStream.format("parquet")
        .outputMode("append")
        .option("path", config.paths.predictions)
        .option("checkpointLocation", _checkpoint(config, "predictions"))
        .trigger(processingTime=config.stream.trigger_predictions)
        .start()
    )

    # 2) Windowed aggregations -> Parquet (append + watermark keeps state bounded).
    hourly = predictions.groupBy(
        F.window("event_time", config.stream.hourly_window), "building_id"
    ).agg(F.sum("prediction").alias("total_energy"))
    queries.append(
        hourly.writeStream.format("parquet")
        .outputMode("append")
        .option("path", config.paths.hourly_aggregations)
        .option("checkpointLocation", _checkpoint(config, "hourly"))
        .trigger(processingTime=config.stream.trigger_hourly)
        .start()
    )

    daily = predictions.groupBy(F.window("event_time", config.stream.daily_window), "site_id").agg(
        F.sum("prediction").alias("daily_total_energy")
    )
    queries.append(
        daily.writeStream.format("parquet")
        .outputMode("append")
        .option("path", config.paths.daily_aggregations)
        .option("checkpointLocation", _checkpoint(config, "daily"))
        .trigger(processingTime=config.stream.trigger_daily)
        .start()
    )

    # 3) Re-publish per-record predictions back to Kafka for downstream consumers.
    queries.append(
        predictions.selectExpr("CAST(building_id AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("topic", config.kafka.predictions_topic)
        .option("checkpointLocation", _checkpoint(config, "kafka_predictions"))
        .outputMode("append")
        .start()
    )
    return queries


def run(config: Config) -> None:
    spark = build_spark(config, with_kafka=True)
    queries = start_queries(spark, config)
    log.info("Started %d streaming queries", len(queries))
    for query in queries:
        query.awaitTermination()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run(load_config())


if __name__ == "__main__":
    main()
