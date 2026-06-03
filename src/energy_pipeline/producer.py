"""Kafka weather producer (replaces the producer notebook).

Streams rows from ``weather.csv`` to the weather topic as typed JSON batches.
Unlike the original notebook, each record carries its **real** observation
timestamp as ``event_time`` (not the wall-clock time of publishing), so the
downstream watermark and event-time windows are meaningful rather than
cosmetic.

Run:
    python -m energy_pipeline.producer --batch-size 120 --interval 5
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Iterator

import pandas as pd
from kafka import KafkaProducer

from .config import Config, load_config
from .schemas import WEATHER_NUMERIC_COLS

log = logging.getLogger("energy_pipeline.producer")


def _iter_batches(df: pd.DataFrame, batch_size: int) -> Iterator[list[dict]]:
    for start in range(0, len(df), batch_size):
        chunk = df.iloc[start : start + batch_size]
        yield chunk.to_dict(orient="records")


def _to_event(record: dict) -> dict:
    event = {"site_id": int(record["site_id"]), "event_time": str(record["timestamp"])}
    for col in WEATHER_NUMERIC_COLS:
        value = record.get(col)
        event[col] = None if pd.isna(value) else float(value)
    return event


def run(config: Config, weather_csv: str, batch_size: int, interval: float) -> None:
    producer = KafkaProducer(
        bootstrap_servers=config.kafka.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )
    df = pd.read_csv(weather_csv).sort_values("timestamp")
    log.info("Loaded %d weather rows; publishing in batches of %d", len(df), batch_size)

    try:
        for rows in _iter_batches(df, batch_size):
            payload = {
                "batch_ts": pd.Timestamp.utcnow().isoformat(),
                "rows": [_to_event(r) for r in rows],
            }
            producer.send(config.kafka.weather_topic, value=payload)
            producer.flush()
            log.info("Published batch of %d events", len(rows))
            time.sleep(interval)
    finally:
        producer.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config()
    parser = argparse.ArgumentParser(description="Publish weather events to Kafka.")
    parser.add_argument("--weather-csv", default=config.paths.weather)
    parser.add_argument("--batch-size", type=int, default=120)
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between batches.")
    args = parser.parse_args()
    run(config, args.weather_csv, args.batch_size, args.interval)


if __name__ == "__main__":
    main()
