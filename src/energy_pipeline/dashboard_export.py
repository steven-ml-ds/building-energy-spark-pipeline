"""Kafka -> Prometheus exporter for the streaming dashboard.

Grafana cannot query Kafka directly (Kafka is an append-only log, not a
time-range queryable store), so this process bridges the gap the way a
production pipeline would: it drains the ``predictions-stream`` topic that
:mod:`energy_pipeline.stream_infer` publishes and exposes the values as
Prometheus metrics on an HTTP ``/metrics`` endpoint. Prometheus scrapes that
endpoint on an interval, and Grafana visualises it (see ``docker/grafana``).

The data flow is::

    stream_infer -> Kafka(predictions-stream) -> THIS exporter -> Prometheus -> Grafana

Run:
    python -m energy_pipeline.dashboard_export            # serves :8000/metrics
    python -m energy_pipeline.dashboard_export --port 8001
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

from .config import Config, load_config

log = logging.getLogger("energy_pipeline.dashboard_export")

# Bucket boundaries for the predicted-consumption distribution. Kept coarse so
# the histogram stays cheap while still showing the shape of the predictions.
_CONSUMPTION_BUCKETS = (50, 100, 250, 500, 1000, 2500, 5000, 10000)


def build_metrics(registry: CollectorRegistry) -> dict:
    """Construct the metric family used by the dashboard.

    Labels are deliberately limited to ``meter_type`` and ``site_id`` (both
    low-cardinality) — ``building_id`` is intentionally NOT a label, since
    hundreds of buildings would explode Prometheus time-series cardinality.
    Per-building rollups belong in the Spark aggregations, not in metric labels.
    """
    return {
        "processed": Counter(
            "energy_predictions_processed_total",
            "Total prediction records consumed from the predictions topic.",
            registry=registry,
        ),
        "consumption": Gauge(
            "energy_predicted_consumption",
            "Latest predicted energy consumption for a (meter_type, site_id).",
            ["meter_type", "site_id"],
            registry=registry,
        ),
        "lag": Gauge(
            "energy_prediction_lag_seconds",
            "Seconds between an event's observation time and its arrival here.",
            registry=registry,
        ),
        "distribution": Histogram(
            "energy_predicted_consumption_distribution",
            "Distribution of predicted energy consumption values.",
            buckets=_CONSUMPTION_BUCKETS,
            registry=registry,
        ),
    }


def _parse_event_time(value) -> dt.datetime | None:
    """Parse the ISO-8601 ``event_time`` emitted by Spark's ``to_json``."""
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def update_metrics(metrics: dict, record: dict, now: dt.datetime | None = None) -> None:
    """Fold a single prediction record into the metric family.

    Pure and side-effect-isolated to the passed-in metrics, so it is unit
    testable without a running Kafka or Prometheus server.
    """
    prediction = float(record["prediction"])
    meter_type = str(record.get("meter_type", "unknown"))
    site_id = str(record.get("site_id", "unknown"))

    metrics["processed"].inc()
    metrics["consumption"].labels(meter_type=meter_type, site_id=site_id).set(prediction)
    metrics["distribution"].observe(prediction)

    event_time = _parse_event_time(record.get("event_time"))
    if event_time is not None:
        now = now or dt.datetime.now(tz=event_time.tzinfo)
        metrics["lag"].set((now - event_time).total_seconds())


def run(config: Config, port: int) -> None:  # pragma: no cover - needs Kafka
    """Serve ``/metrics`` and stream prediction records into it forever."""
    from kafka import KafkaConsumer

    registry = CollectorRegistry()
    metrics = build_metrics(registry)
    start_http_server(port, registry=registry)
    log.info("Serving Prometheus metrics on :%d/metrics", port)

    consumer = KafkaConsumer(
        config.kafka.predictions_topic,
        bootstrap_servers=config.kafka.bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-exporter",
    )
    log.info("Consuming topic %s", config.kafka.predictions_topic)
    for message in consumer:
        try:
            update_metrics(metrics, message.value)
        except (KeyError, ValueError, TypeError):
            log.warning("Skipping malformed record: %r", message.value)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config()
    parser = argparse.ArgumentParser(description="Export Kafka predictions to Prometheus.")
    parser.add_argument(
        "--port",
        type=int,
        default=config.dashboard.exporter_port,
        help="Port to serve the Prometheus /metrics endpoint on.",
    )
    args = parser.parse_args()
    run(config, args.port)


if __name__ == "__main__":
    main()
