# Building Energy Consumption Prediction Pipeline

> End-to-end PySpark pipeline combining batch ML training and real-time
> streaming inference to predict 6-hourly building energy consumption.

![CI](https://github.com/steven-ml-ds/building-energy-spark-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.4.1-orange?logo=apachespark)
![Kafka](https://img.shields.io/badge/Kafka-KRaft-black?logo=apachekafka)

---

## Impact / Decision

**Question:** Can one pipeline predict 6-hourly building energy use for both batch
planning *and* live operations without the two paths quietly disagreeing?

**Decision this drives:** Forecasts feed both retrospective batch analysis and real-time
streaming inference. If those two paths compute features differently, the live system
silently degrades — the most expensive and least visible failure mode in production ML.

**What it's worth:** This repo's headline result is **eliminating training/serving skew
by construction**. Batch and streaming import the *same* `features.py` and load the *same*
persisted Spark ML `Pipeline`, so they physically cannot drift. The refactor from three
notebooks into one package surfaced **two silent skew bugs** that had been producing
different features in each path; both are now locked out by a **skew regression test** in CI
— a reviewer can change one path and watch the test fail before it ships.

**Why it matters operationally:** a model that's accurate offline but skewed online looks
fine on every dashboard until someone audits predictions. Making skew a *tested invariant*
turns a silent correctness risk into a loud build failure.

> Stack: PySpark (Imputer ▸ StringIndexer ▸ OHE ▸ VectorAssembler ▸ GBTRegressor),
> Kafka (KRaft) + Structured Streaming · config-driven, no hard-coded paths · CI on every push.

---

## Why this repo is structured the way it is

This started as three notebooks. It has been refactored into an installable
Python package because the headline risk in any ML system is **training/serving
skew** — the model behaving differently in production than in training. The
single most important design decision here is that **batch and streaming share
one feature-engineering module** (`energy_pipeline.features`) and one persisted
Spark ML `Pipeline`, so the two paths physically cannot drift apart.

See [`docs/design.md`](docs/design.md) for the architecture decisions and the
two skew bugs this refactor fixed.

---

## Architecture

```
                ┌─────────────────────────────────────────────┐
   BATCH        │  meters.csv ─┐                               │
   ──────       │  buildings ──┼─▶ datasets.build_training_frame│
                │  weather.csv ┘        │                      │
                │                       ▼                      │
                │              features.engineer_features      │
                │                       │                      │
                │   ┌───────────────────┴───────────────────┐  │
                │   │  Spark ML Pipeline (PERSISTED)         │  │
                │   │  Imputer ▸ StringIndexer ▸ OHE ▸       │  │
                │   │  VectorAssembler ▸ GBTRegressor        │  │
                │   └───────────────────┬───────────────────┘  │
                └───────────────────────┼──────────────────────┘
                                        ▼
                          artifacts/energy_pipeline_model
                                        │
   STREAMING                            │  (same pipeline, same features module)
   ─────────   weather.csv              ▼
   producer ──▶ Kafka(weather-stream) ──▶ stream_infer
                                        │
              ┌─────────────────────────┼──────────────────────────┐
              ▼                         ▼                          ▼
     artifacts/output/predictions   hourly_aggregations    daily_aggregations
              │
              └──▶ Kafka(predictions-stream)
                              │
   DASHBOARD                  ▼  (Kafka can't be queried directly)
   ─────────      dashboard_export ──▶ Prometheus ──▶ Grafana (live, :3000)
```

---

## Project layout

```
src/energy_pipeline/
├── config.py        # env-overridable configuration (no hard-coded paths)
├── schemas.py       # centralised, typed Spark schemas
├── features.py      # SHARED feature engineering + ML preprocessing stages
├── datasets.py      # batch dataset assembly (aggregate + join)
├── metrics.py       # RMSE / MAE / R² / RMSLE
├── spark.py         # SparkSession factory
├── batch_train.py   # entry point: train & persist the tuned pipeline
├── producer.py      # entry point: Kafka weather producer
├── stream_infer.py  # entry point: Structured Streaming inference
└── dashboard_export.py # entry point: Kafka predictions ▸ Prometheus exporter
tests/               # pytest suite (incl. skew regression test)
docker/              # Kafka (KRaft) + Prometheus + Grafana compose & provisioning
notebooks/           # original notebooks, kept for EDA / storytelling
docs/design.md       # architecture decisions & fixed bugs
docs/dashboard.md    # streaming dashboard: how it works & how to run it
```

---

## Quick start

```bash
# 1. Install (editable, with dev + viz extras)
make dev                      # or: pip install -e ".[dev,viz]"

# 2. Place the CSVs in data/ (see data/README.md)

# 3. Train the model (use a sample for a fast first run)
make train ARGS="--sample-fraction 0.05"

# 4. Start the full stack (Kafka + Prometheus + Grafana), the producer,
#    the streaming job, and the metrics exporter (separate terminals)
make up                # Kafka + Prometheus + Grafana
make produce           # publish weather events
make stream            # score them through the persisted pipeline
make dashboard-export  # bridge predictions ▸ Prometheus

# 5. Open the live dashboard
open http://localhost:3000   # Grafana ▸ "Building Energy — Streaming Predictions"
```

Console scripts are also installed: `energy-train`, `energy-produce`, `energy-stream`,
`energy-dashboard-export`.

See [`docs/dashboard.md`](docs/dashboard.md) for how the real-time visualization is wired.

---

## Configuration

Everything is configured via environment variables with safe defaults — see
[`.env.example`](.env.example). Examples:

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATA_ROOT` | `data` | Input CSV directory |
| `ARTIFACT_ROOT` | `artifacts` | Model / output / checkpoint root |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `MODEL_SAMPLE_FRACTION` | `1.0` | Training subsample (e.g. `0.05` for demos) |
| `SPARK_TIMEZONE` | `Australia/Melbourne` | Session timezone |
| `DASHBOARD_EXPORTER_PORT` | `8000` | Port the Prometheus exporter serves `/metrics` on |

---

## Development

```bash
make lint      # ruff
make fmt       # ruff --fix + format
make test      # pytest
make cov       # pytest with coverage
```

CI (GitHub Actions) provisions JDK 17 + Python 3.9, then runs lint and the full
test suite — including a regression test that fails if the streaming feature
path ever diverges from training again.

---

## Tech stack

| Layer | Technology |
|-------|------------|
| Processing / ML | PySpark 3.4.1 (Spark ML, Structured Streaming) |
| Messaging | Apache Kafka (KRaft mode) |
| Producer client | kafka-python |
| Packaging | setuptools (src layout), `pyproject.toml` |
| Quality | pytest, ruff, pre-commit, GitHub Actions |
| EDA | pandas, matplotlib, seaborn (notebooks) |

---

## Dataset

CSV files are not tracked by git. See [`data/README.md`](data/README.md) for the
expected files and column definitions.
