# Notebook Execution Notes

Status and setup notes for running the three notebooks in `notebooks/` locally
(macOS, host execution — not inside Docker).

## Environment setup (done)

The notebooks require a PySpark-compatible Python and a JVM. The original `.venv`
was Python 3.14, which PySpark 3.4.1 cannot run on, and no JDK was installed.

| Item | Resolution |
|------|------------|
| Python | Rebuilt `.venv` on **Python 3.11** (`uv venv --python 3.11`); matches pinned `pyspark==3.4.1` (supports ≤3.11). |
| Java | Installed **Temurin/OpenJDK 17** via `brew install openjdk@17` → `/opt/homebrew/opt/openjdk@17`. |
| Deps | `uv pip install -e ".[viz,dashboard]"` plus `kafka-python3` (notebook 02 imports `kafka3`, provided by the `kafka-python3` PyPI package — **not** the pinned `kafka-python`). |
| Jupyter kernel | Registered **`Python 3.11 (energy-pipeline)`** with `JAVA_HOME`, `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`, and `SPARK_DRIVER_MEMORY=10g` baked into its `env`. Select this kernel in VSCode (Kernel picker → "Jupyter Kernel…"). |

> **Why `PYSPARK_PYTHON` matters:** without it, Spark's Python worker falls back to
> the system `python3` (3.14) and dies with `ModuleNotFoundError: No module named 'typing.io'`.

## Per-notebook status

### 01 — `01_batch_ml_energy_prediction.ipynb`  ✅ working
- Runs **end-to-end including cross-validation** in ~6 min (headless), 0 cell errors.
- **CV grid trimmed** to make it tractable on local Spark: the original
  `maxDepth[2,4,6] × maxIter[10,20,50] × stepSize[0.05,0.1,0.2]` (27 combos × 3 folds = 81 GBT
  fits) never finished on the 598 MB dataset (observed 7000+ Spark jobs after 5h). Now
  `maxDepth[3,5] × maxIter[10,20] × stepSize[0.1]` (4 × 3 = 12 fits). Tuned GBT RMSLE ≈ 2.365
  (best params `maxDepth=5, maxIter=10, stepSize=0.1`).
- **Saved model is now self-contained** (see the "Streaming alignment" note below):
  `models/energy_pipeline_model` = `[StringIndexer → OneHotEncoder → VectorAssembler → GBTRegressor]`.
- **Data paths:** the notebook uses root-relative paths (`data/meters.csv`). VSCode defaults
  Jupyter's working dir to the workspace root, so they resolve. If you ever see `PATH_NOT_FOUND`,
  set `"jupyter.notebookFileRoot": "${workspaceFolder}"`.

### 02 — `02_kafka_producer.ipynb`  ✅ working
- Host fix applied: broker host changed `"kafka"` → `"localhost"` (the Docker service name
  only resolves inside the compose network; the broker advertises `PLAINTEXT://localhost:9092`).
- Requires the Kafka container: `make kafka-up`.
- The producer loop is **unbounded** by default (~139k rows, 120/batch every 5s ≈ 1.6h to drain).
  It catches `KeyboardInterrupt`, so interrupt the kernel to stop it cleanly.
- For a bounded/headless run, set **`PRODUCER_MAX_BATCHES`** (env var) to cap the number of
  batches; unset (or `0`) keeps the original unbounded behaviour. The saved run shows 12 batches
  sent followed by a clean shutdown.

### 03 — `03_spark_streaming_prediction.ipynb`  ✅ working
- Infra verified: Kafka connector jars download, Spark streaming session starts, model loads, the
  Kafka `readStream` consumes live data (host `"kafka"` → `"localhost"`).
- End-to-end verified with the producer live: 03 runs headless in ~90s, **0 cell errors**, and
  every memory table / parquet read / Kafka re-publish cell produces real predictions (per-record,
  6h-per-building, daily-per-site).
- **Run it with 02 publishing concurrently** — the Kafka source uses the default
  `startingOffsets=latest`, so 03 only sees messages produced after each query starts.
- The `start → show → stop` cells now `wait_for_table` / `wait_for_path` before reading, so an
  unattended run captures real rows instead of empty results.

#### Streaming alignment fix (✅ applied — branch `fix/streaming-pipeline-alignment`, PR #3)
The previous blocker was a model/feature mismatch: the saved pipeline was `[VectorAssembler → GBT]`
whose assembler required pre-built `primary_use_ohe` **and** `meter_type_ohe`, neither of which 03
could produce. Resolved in **notebook 01**:
1. Moved the `primary_use` `StringIndexer` + `OneHotEncoder` **into the `Pipeline` stages**, so the
   saved model encodes `primary_use` itself and the index→category mapping matches training exactly.
   03 now only needs the raw `primary_use` column (already present from the buildings join).
2. **Dropped `meter_type`** as a feature — it is absent from the weather stream, so keeping it made
   batch/stream features impossible to align. (`meter_type` remains available for EDA in 01.)

Saved model is now `[StringIndexer → OneHotEncoder → VectorAssembler → GBTRegressor]` and
`PipelineModel.load(...).transform(...)` works directly on the streaming feature frame.

## Infra reference
- Start Kafka only: `make kafka-up` · full stack (Kafka+Prometheus+Grafana): `make up` · stop: `make down`.
- Broker reachable from the host at `localhost:9092` (advertised listener).
