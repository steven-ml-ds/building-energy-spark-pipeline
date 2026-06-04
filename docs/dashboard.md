# Streaming Dashboard

Real-time visualization of the model's predictions as they flow off the
streaming inference job.

## Why this design (and not matplotlib / Streamlit)

Real-time dashboards are **not** built from batch plotting tools. Matplotlib and
notebooks render a static snapshot; a polling Streamlit app does not scale and is
a prototype tool. The production-standard pattern for streaming telemetry is:

```
stream_infer ─▶ Kafka(predictions-stream) ─▶ exporter ─▶ Prometheus ─▶ Grafana
```

Each hop earns its place:

- **Kafka is a log, not a database.** It has no "average the last 5 minutes per
  meter type" query — it only replays records. So a consumer must drain it into
  something queryable.
- **The exporter** (`energy_pipeline.dashboard_export`) is that consumer. It
  subscribes to the `predictions-stream` topic the streaming job already
  publishes and exposes the values as Prometheus metrics on `/metrics`.
- **Prometheus** is the time-series store: it scrapes the exporter every 5s and
  keeps range-queryable history with retention and downsampling.
- **Grafana** is the dashboard layer — native auto-refresh, thresholds, and
  templating. It is what on-call engineers actually watch.

This is the same shape you'd run in production; only the scale and the managed
vs. self-hosted choice would change (e.g. Kinesis → Managed Grafana, or
Kafka → Druid/Pinot → Superset for sub-second OLAP at very high throughput).

## Metrics exposed

| Metric | Type | Meaning |
|--------|------|---------|
| `energy_predictions_processed_total` | Counter | Records consumed (throughput via `rate()`) |
| `energy_predicted_consumption{meter_type,site_id}` | Gauge | Latest predicted consumption |
| `energy_prediction_lag_seconds` | Gauge | Event-time → ingestion lag (data freshness) |
| `energy_predicted_consumption_distribution` | Histogram | Distribution of predicted values |

`building_id` is deliberately **not** a metric label — hundreds of buildings
would explode Prometheus time-series cardinality. Per-building rollups stay in
the Spark aggregations (`hourly_aggregations`, `daily_aggregations`).

## Running it

```bash
make up                # Kafka + Prometheus + Grafana via docker compose
make train             # produce artifacts/energy_pipeline_model (once)
make produce           # publish weather events to Kafka
make stream            # score them -> predictions-stream
make dashboard-export  # bridge predictions-stream -> Prometheus

open http://localhost:3000      # Grafana (anonymous viewer enabled)
open http://localhost:9090      # Prometheus (to debug scrape targets)
open http://localhost:8000      # raw exporter /metrics
```

The Grafana dashboard **"Building Energy — Streaming Predictions"** is
auto-provisioned (datasource + dashboard JSON under `docker/grafana/`), so it
appears on first launch with no manual import. Default refresh is 5s.

## Panels

1. **Predicted consumption (live)** — one series per `(meter_type, site_id)`.
2. **Records / sec** — ingestion throughput.
3. **Total predictions processed** — cumulative counter.
4. **Event-time lag** — freshness gauge (green < 60s, red > 5m).
5. **Predicted consumption distribution** — histogram buckets over time.

## Screenshot

> Add a screenshot of the running dashboard here once captured:
> `docs/figures/grafana-streaming.png`
