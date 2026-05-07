# Building Energy Consumption Prediction Pipeline

> End-to-end pipeline combining batch ML and real-time streaming to predict hourly building energy consumption.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.4-orange?logo=apache-spark)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-black?logo=apache-kafka)
![Docker](https://img.shields.io/badge/Docker-required-blue?logo=docker)

---

## Project Overview

This project implements a full end-to-end energy prediction system across two components:

| Component | Notebook | Description |
|-----------|----------|-------------|
| **Batch ML Pipeline** | `01_batch_ml_energy_prediction.ipynb` | Trains and tunes a GBT regression model on historical building, meter, and weather data |
| **Kafka Producer** | `02_kafka_producer.ipynb` | Simulates a real-time weather data stream by publishing to a Kafka topic |
| **Spark Streaming** | `03_spark_streaming_prediction.ipynb` | Consumes the Kafka stream, applies the saved ML pipeline, and persists predictions |

---

## Architecture

```
  BATCH PIPELINE
  ----------------------------------------
  data/meters.csv            +---------------------+
  data/building_info.csv --> |  01  Batch ML       |
  data/weather.csv           |  (PySpark + GBT)    |
                             +----------+----------+
                                        |
                             models/energy_pipeline_model
                                        |
  STREAMING PIPELINE                    |
  ----------------------------------------
  data/weather.csv                      |
       |                                |
       v                                v
  +---------------+  weather-stream  +------------------+
  | 02  Kafka     | ---------------> | 03  Spark        |
  |    Producer   |  (Kafka topic)   |    Streaming     |
  +---------------+                  +--------+---------+
                                              |
                                    +---------+---------+
                                    |                   |
                              output/predictions   output/hourly_*
                                                   output/daily_*
```

---

## Dataset Description

Place the following CSV files in the `data/` directory (not tracked by git):

| File | Description |
|------|-------------|
| `meters.csv` | Hourly energy consumption readings - columns: `building_id`, `meter_type`, `timestamp`, `value` |
| `building_information.csv` | Building metadata - columns: `site_id`, `primary_use`, `square_feet`, `floor_count`, `year_built`, latent features |
| `new_building_information.csv` | Updated building metadata used by the streaming pipeline |
| `weather.csv` | Hourly weather readings per site - columns: `site_id`, `timestamp`, `air_temperature`, `wind_speed`, `cloud_coverage`, `sea_level_pressure` |

---

## Methodology

### Notebook 01 - Batch ML Pipeline

1. **Data Loading** - Define typed schemas and load `meters.csv`, `building_information.csv`, and `weather.csv` into Spark DataFrames
2. **Aggregation** - Resample hourly meter readings into 6-hour interval totals per building
3. **Weather Imputation** - Fill missing weather values using Spark MLlib `Imputer` (mean strategy)
4. **Feature Engineering**
   - Log-transform `square_feet` to reduce skew
   - One-hot encode `primary_use` and `meter_type`
   - Cyclical encode `time_interval`, `wind_direction`, and `month` (sine/cosine)
   - Derive `decade` from `year_built`
5. **Model Training** - Compare Random Forest vs Gradient Boosted Trees using RMSE, MAE, R2, and RMSLE
6. **Hyperparameter Tuning** - `CrossValidator` with `ParamGridBuilder` over `maxDepth`, `maxIter`, `stepSize`
7. **Model Persistence** - Save best GBT pipeline to `models/energy_pipeline_model`

### Notebook 02 - Kafka Producer

1. Read `data/weather.csv` in chronological order, maintaining a file pointer
2. Every 5 seconds, publish a 5-day batch (120 records) as a JSON payload to the `weather-stream` Kafka topic
3. Stamp each day's records with the current Unix timestamp to simulate real-time event ordering

### Notebook 03 - Spark Structured Streaming

1. **Ingestion** - Subscribe to `weather-stream` Kafka topic; deserialise JSON into a typed schema
2. **Join** - Enrich weather stream with building metadata from `data/new_building_information.csv`
3. **Watermarking** - Discard records arriving more than 5 seconds late
4. **Feature Engineering** - Replicate the same transforms used in notebook 01
5. **Inference** - Load `models/energy_pipeline_model` and apply to each micro-batch
6. **Output**
   - `output/predictions` - raw per-record predictions (Parquet, append mode)
   - `output/hourly_aggregations` - 6-hour window aggregations per building (Parquet, every 7 s)
   - `output/daily_aggregations` - 1-day window aggregations per site (Parquet, every 14 s)
7. **Re-publish** - Stream each Parquet output back to dedicated Kafka topics

---

## How to Run

### Prerequisites

- Python 3.9+
- Docker (for Kafka)
- All packages from `requirements.txt`

```bash
pip install -r requirements.txt
```

### Step 1 - Train and save the ML pipeline

Open and run all cells in `01_batch_ml_energy_prediction.ipynb`.  
This produces `models/energy_pipeline_model/`.

### Step 2 - Start Kafka with Docker

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  confluentinc/cp-kafka:latest
```

Or use a `docker-compose.yml` that includes both Zookeeper and Kafka.

### Step 3 - Start the Kafka producer

Open and run `02_kafka_producer.ipynb`.  
It will begin publishing weather batches every 5 seconds.

### Step 4 - Start the streaming pipeline

Open and run `03_spark_streaming_prediction.ipynb`.  
Predictions will accumulate in the `output/` directory as Parquet files.

---

## Tech Stack

| Technology | Role |
|------------|------|
| Python 3.9+ | Primary language |
| Apache PySpark 3.4 | Distributed data processing and ML (batch + streaming) |
| Apache Kafka | Real-time message broker |
| Spark Structured Streaming | Stateful streaming with watermarks and windowed aggregations |
| Docker | Kafka and Zookeeper infrastructure |
| pandas / matplotlib / seaborn | Data exploration and visualisation |

---

## Project Structure

```
building-energy-spark-pipeline/
|-- 01_batch_ml_energy_prediction.ipynb   # Batch ML pipeline
|-- 02_kafka_producer.ipynb               # Kafka weather data producer
|-- 03_spark_streaming_prediction.ipynb   # Spark Structured Streaming pipeline
|-- requirements.txt
|-- .gitignore
|-- images/                               # Architecture diagrams (optional)
`-- data/
    `-- README.md                         # Dataset descriptions (CSVs not tracked)
```
