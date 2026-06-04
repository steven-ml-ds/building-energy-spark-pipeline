"""Schema sanity tests."""

from __future__ import annotations

from energy_pipeline import schemas


def test_weather_event_schema_is_typed():
    # The producer emits typed numbers, so the event schema must be numeric,
    # not the all-strings schema the original notebook used.
    fields = {f.name: f.dataType.typeName() for f in schemas.WEATHER_EVENT_SCHEMA.fields}
    assert fields["air_temperature"] == "double"
    assert fields["site_id"] == "integer"
    assert fields["event_time"] == "timestamp"


def test_weather_numeric_cols_match_schema():
    schema_cols = {f.name for f in schemas.WEATHER_SCHEMA.fields}
    for col in schemas.WEATHER_NUMERIC_COLS:
        assert col in schema_cols


def test_batch_schema_wraps_events():
    names = [f.name for f in schemas.WEATHER_BATCH_SCHEMA.fields]
    assert names == ["batch_ts", "rows"]
