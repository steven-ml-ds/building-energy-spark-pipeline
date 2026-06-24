"""Tests for the Kafka -> Prometheus exporter's record-folding logic.

These exercise ``update_metrics`` against a fresh ``CollectorRegistry`` so no
Kafka broker or HTTP server is required — only ``prometheus_client``.
"""

from __future__ import annotations

import datetime as dt

import pytest

prometheus_client = pytest.importorskip("prometheus_client")
from prometheus_client import CollectorRegistry  # noqa: E402

from energy_pipeline import dashboard_export  # noqa: E402


def _record(**overrides):
    base = {
        "event_time": "2022-06-01T12:00:00+00:00",
        "site_id": 1,
        "building_id": 100,
        "meter_type": "e",
        "prediction": 500.0,
    }
    base.update(overrides)
    return base


def test_update_metrics_records_consumption_and_count():
    registry = CollectorRegistry()
    metrics = dashboard_export.build_metrics(registry)

    dashboard_export.update_metrics(metrics, _record(prediction=512.5))

    assert registry.get_sample_value("energy_predictions_processed_total") == 1.0
    gauge = registry.get_sample_value(
        "energy_predicted_consumption", {"meter_type": "e", "site_id": "1"}
    )
    assert gauge == 512.5


def test_latest_value_wins_per_label_set():
    registry = CollectorRegistry()
    metrics = dashboard_export.build_metrics(registry)

    dashboard_export.update_metrics(metrics, _record(prediction=100.0))
    dashboard_export.update_metrics(metrics, _record(prediction=200.0))

    # Gauge reflects the most recent reading; counter accumulates both.
    assert (
        registry.get_sample_value(
            "energy_predicted_consumption", {"meter_type": "e", "site_id": "1"}
        )
        == 200.0
    )
    assert registry.get_sample_value("energy_predictions_processed_total") == 2.0


def test_lag_is_computed_from_event_time():
    registry = CollectorRegistry()
    metrics = dashboard_export.build_metrics(registry)

    now = dt.datetime(2022, 6, 1, 12, 0, 30, tzinfo=dt.timezone.utc)
    dashboard_export.update_metrics(metrics, _record(), now=now)

    assert registry.get_sample_value("energy_prediction_lag_seconds") == 30.0


def test_missing_event_time_does_not_crash():
    registry = CollectorRegistry()
    metrics = dashboard_export.build_metrics(registry)

    dashboard_export.update_metrics(metrics, _record(event_time=None))

    # Still counted; lag simply stays unset (0.0 default for an untouched gauge).
    assert registry.get_sample_value("energy_predictions_processed_total") == 1.0


def test_distribution_histogram_observes_value():
    registry = CollectorRegistry()
    metrics = dashboard_export.build_metrics(registry)

    dashboard_export.update_metrics(metrics, _record(prediction=300.0))

    count = registry.get_sample_value("energy_predicted_consumption_distribution_count")
    assert count == 1.0


def test_distribution_histogram_places_value_in_correct_bucket():
    registry = CollectorRegistry()
    metrics = dashboard_export.build_metrics(registry)

    # 300 falls in the (250, 500] bucket: it is counted in le=500 and above,
    # but not in le=250 or le=100.
    dashboard_export.update_metrics(metrics, _record(prediction=300.0))

    assert (
        registry.get_sample_value(
            "energy_predicted_consumption_distribution_bucket", {"le": "100.0"}
        )
        == 0.0
    )
    assert (
        registry.get_sample_value(
            "energy_predicted_consumption_distribution_bucket", {"le": "250.0"}
        )
        == 0.0
    )
    assert (
        registry.get_sample_value(
            "energy_predicted_consumption_distribution_bucket", {"le": "500.0"}
        )
        == 1.0
    )


def test_missing_labels_fall_back_to_unknown():
    registry = CollectorRegistry()
    metrics = dashboard_export.build_metrics(registry)

    record = _record(prediction=42.0)
    del record["meter_type"]
    del record["site_id"]
    dashboard_export.update_metrics(metrics, record)

    assert (
        registry.get_sample_value(
            "energy_predicted_consumption", {"meter_type": "unknown", "site_id": "unknown"}
        )
        == 42.0
    )
