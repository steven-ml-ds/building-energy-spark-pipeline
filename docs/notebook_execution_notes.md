# Notebook Execution Notes

Status and setup notes for running the three notebooks in `notebooks/` locally
(macOS, host execution — not inside Docker).

The notebooks now **delegate their model and feature logic to the shared
`energy_pipeline` package** (`src/energy_pipeline/`) rather than re-deriving it
inline. This keeps the batch and streaming paths on a single feature
implementation — see `docs/design.md` Decisions 1, 2 and 4. Each notebook adds
`sys.path.insert(0, "src")` so the package imports without an editable install.

## Environment setup

The notebooks require a PySpark-compatible Python and a JVM. Python 3.14 cannot
run PySpark 3.4.1 (`ModuleNotFoundError: No module named 'typing.io'`), so use an
older interpreter.

| Item | Resolution |
|------|------------|
| Python | Build the venv on **Python 3.11** (`uv venv --python 3.11`); matches pinned `pyspark==3.4.1` (supports ≤3.11). |
| Java | Install **Temurin/OpenJDK 17** (`brew install openjdk@17` → `/opt/homebrew/opt/openjdk@17`). |
| Deps | `uv pip install -e ".[viz,dashboard]"`. Notebook 02 imports `KafkaProducer` from the pinned **`kafka-python==2.0.2`** — no extra Kafka package is needed. |
| Jupyter kernel | Register a kernel with `JAVA_HOME`, `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`, and `SPARK_DRIVER_MEMORY=10g` in its `env`, and select it in VSCode. |

> **Why `PYSPARK_PYTHON` matters:** without it, Spark's Python worker falls back to
> the system `python3` (3.14) and dies with `ModuleNotFoundError: No module named 'typing.io'`.

> **Outputs are cleared in version control.** The committed notebooks carry no
> cell outputs — re-run them locally (Spark + the data files under `data/`) to
> repopulate. Cell outputs are intentionally not committed so they cannot drift
> from the code.

## Per-notebook status

### 01 — `01_batch_ml_energy_prediction.ipynb`
- Loads the data and keeps the original exploratory analysis (EDA), then builds
  the **model** from the shared package:
  - `datasets.build_training_frame(spark, config)` assembles the labelled frame
    over real 6-hour `interval_start` timestamps and calls
    `features.engineer_features` (time-of-day from `floor(hour / 6)`, not a string
    bucket) — so batch and streaming compute features identically.
  - `features.build_estimator_pipeline(gbt)` wraps the estimator in the shared
    preprocessing stages: `Imputer → wind-direction SQLTransformer →
    StringIndexer/OneHotEncoder (primary_use **and** meter_type) →
    VectorAssembler → GBTRegressor`. The persisted model is therefore
    self-contained, and `meter_type` is kept as a predictor.
  - Outlier bounds are fit on the **training split only**
    (`datasets.compute_outlier_bounds` / `filter_outliers`), removing the earlier
    pre-split imputation/outlier leak. Evaluation uses `metrics.evaluate`
    (its RMSLE clamps negatives before `log1p`).
- **CV grid** is trimmed for tractable local runs:
  `maxDepth[3,5] × maxIter[10,20] × stepSize[0.1]` (4 combos × 3 folds = 12 GBT
  fits), versus the full `src` grid of `maxDepth[2,4,6] × maxIter[10,20,50] ×
  stepSize[0.05,0.1,0.2]` (81 fits) which is intractable on a single machine over
  the full dataset. Re-run locally to obtain current metrics.
- The model is saved to `config.paths.model` (`models/energy_pipeline_model`
  with `ARTIFACT_ROOT=models`), which notebook 03 loads.
- **Data paths:** the notebook uses root-relative paths (`data/meters.csv`). If
  you see `PATH_NOT_FOUND`, set `"jupyter.notebookFileRoot": "${workspaceFolder}"`.

### 02 — `02_kafka_producer.ipynb`
- Imports `KafkaProducer` from `kafka` (the pinned `kafka-python==2.0.2`).
- Broker host is `localhost` (the Docker service name only resolves inside the
  compose network; the broker advertises `PLAINTEXT://localhost:9092`). Requires
  the Kafka container: `make kafka-up`.
- The producer loop is **unbounded** by default; it catches `KeyboardInterrupt`,
  so interrupt the kernel to stop it cleanly. For a bounded/headless run set
  **`PRODUCER_MAX_BATCHES`** to cap the number of batches; unset (or `0`) keeps
  the original unbounded behaviour.

### 03 — `03_spark_streaming_prediction.ipynb`
- Feature engineering calls **`features.engineer_features`** — the same code path
  01 trains with — so there is no training/serving skew. Missing weather values
  are mean-imputed **inside the loaded pipeline** (no `fillna(0)`), and every
  weather numeric is cast to `double` to match the trained dtypes.
- `meter_type` is reconstructed by cross-joining each weather/building record with
  `config.METER_TYPES` (the weather feed carries no meter data) — the explicit
  contract in `design.md` Decision 4.
- The self-contained pipeline loads via `PipelineModel.load(config.paths.model)`
  and `transform()` runs directly on the engineered feature stream.
- **Run it with 02 publishing concurrently** — the Kafka source uses the default
  `startingOffsets=latest`, so 03 only sees messages produced after each query
  starts.
- The `start → show → stop` cells `wait_for_table` / `wait_for_path` before
  reading, so an unattended run captures real rows instead of empty results.

## Infra reference
- Start Kafka only: `make kafka-up` · full stack (Kafka+Prometheus+Grafana): `make up` · stop: `make down`.
- Broker reachable from the host at `localhost:9092` (advertised listener).
