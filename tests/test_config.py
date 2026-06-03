"""Configuration tests (no Spark required)."""

from __future__ import annotations

import importlib


def test_defaults():
    from energy_pipeline.config import load_config

    config = load_config()
    assert config.paths.meters.endswith("meters.csv")
    assert config.kafka.weather_topic == "weather-stream"
    assert config.model.sample_fraction == 1.0


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("DATA_ROOT", "/srv/data")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("MODEL_SAMPLE_FRACTION", "0.05")

    import energy_pipeline.config as cfg

    importlib.reload(cfg)
    config = cfg.load_config()
    assert config.paths.meters == "/srv/data/meters.csv"
    assert config.kafka.bootstrap_servers == "broker:9092"
    assert config.model.sample_fraction == 0.05
