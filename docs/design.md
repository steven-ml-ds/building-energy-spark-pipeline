# Design notes & architecture decisions

## Context

The project predicts 6-hourly building energy consumption in two modes: a batch
job that trains and tunes a model on historical data, and a streaming job that
scores a live weather feed. The original implementation was three notebooks
with copy-pasted feature logic.

## Decision 1 — One feature module, shared by both paths

**Problem.** Feature engineering was duplicated between the batch and streaming
notebooks. The copies drifted and produced two silent training/serving skew
bugs:

1. **`time_interval` type mismatch.** Training derived cyclical time features
   from a *string* bucket (`"0:00-5:59"`); serving compared an *integer* hour
   to those strings, so the match always failed and `time_sin`/`time_cos`
   collapsed to `0` for every prediction.
2. **Inconsistent missing-value handling.** Training mean-imputed weather
   features; serving filled them with literal `0` (e.g. 0 hPa pressure), feeding
   the model out-of-distribution values.

**Decision.** All deterministic, row-local features are computed in
`energy_pipeline.features.engineer_features`, called identically by
`datasets.build_training_frame` and `stream_infer.build_feature_stream`.
`time_interval_num` is computed as `floor(hour / 6)` directly — no string
round-trip. A regression test (`tests/test_features.py::test_time_features_are_not_collapsed`)
fails if the cyclical features ever collapse again.

## Decision 2 — Imputation/encoding inside the persisted pipeline

**Decision.** The `Imputer`, `StringIndexer`, `OneHotEncoder` and
`VectorAssembler` are stages of the saved Spark ML `PipelineModel`
(`features.build_preprocessing_stages`). Consequences:

- The Imputer is fit on training folds only → removes the data leak from fitting
  the imputer on the full dataset before the train/test split.
- Training-time means and category indices are reused verbatim at serving time →
  removes the `fillna(0)` hack entirely.

## Decision 3 — Real event-time, bounded streaming state

- The producer stamps each record with its **actual observation timestamp**
  (`event_time`), not wall-clock publish time, so the watermark and event-time
  windows are meaningful.
- Windowed aggregations use `outputMode("append")` with a watermark, so
  streaming state is bounded (the original `complete` mode grew state forever).

## Decision 4 — `meter_type` in the streaming feed

The model is trained per `(building, meter_type)`, but the weather feed carries
no meter information. `stream_infer` cross-joins each weather/building record
with the known meter types (`config.METER_TYPES`) to reconstruct the trained
feature space. This is an explicit, documented contract rather than silently
dropping the feature.

## Decision 5 — Kafka in KRaft mode

The Docker setup uses single-node Kafka in KRaft mode (no Zookeeper), matching
current Kafka defaults.

## Known limitations / next steps

- No experiment tracking yet — wiring MLflow for metrics + model registry is the
  natural next step (metrics are currently logged as JSON).
- No orchestration — the batch job is a clean entry point ready to wrap in an
  Airflow/Dagster task.
- Data-quality assertions (Great Expectations / range & freshness checks) are
  not yet enforced as a pipeline gate.
