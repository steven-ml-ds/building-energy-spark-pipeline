"""Shared feature engineering — the single source of truth for both the
batch training job and the streaming inference job.

Why this module exists
-----------------------
In the original notebooks the feature logic was copy-pasted between the batch
notebook and the streaming notebook. The two copies drifted, which produced
silent training/serving skew:

* ``time_interval`` was a *string* bucket at training time but an *integer*
  hour at serving time, so the ``when(... == "0:00-5:59")`` comparison never
  matched and ``time_sin``/``time_cos`` collapsed to 0 for every prediction.
* missing features were mean-imputed at training time but filled with literal
  ``0`` at serving time.

By computing every derived column here, and by folding imputation / encoding /
assembly into the persisted Spark ML ``Pipeline`` (see
:func:`build_preprocessing_stages`), the exact same transformations run in both
paths. There is now no second copy to drift.
"""

from __future__ import annotations

import math

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    SQLTransformer,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schemas import WEATHER_NUMERIC_COLS

# Categorical columns one-hot encoded inside the pipeline.
CATEGORICAL_COLS = ["primary_use", "meter_type"]

# Final numeric/vector inputs handed to the VectorAssembler. Kept as a single
# constant so training and serving cannot disagree about the feature space.
ASSEMBLER_INPUTS: list[str] = [
    "log_square_feet",
    "floor_count",
    "decade",
    "air_temperature",
    "dew_temperature",
    "sea_level_pressure",
    "wind_speed",
    "cloud_coverage",
    "wind_dir_sin",
    "wind_dir_cos",
    "time_sin",
    "time_cos",
    "month_sin",
    "month_cos",
    "primary_use_ohe",
    "meter_type_ohe",
    "latent_y",
    "latent_s",
    "latent_r",
]

_HOURS_PER_INTERVAL = 6
_INTERVALS_PER_DAY = 24 // _HOURS_PER_INTERVAL  # 4


def add_time_features(df: DataFrame, ts_col: str) -> DataFrame:
    """Cyclical encodings for time-of-day and month, derived from ``ts_col``.

    ``time_interval_num`` is computed directly from the hour
    (``floor(hour / 6)`` -> 0..3) rather than via a fragile string label, so it
    is identical in batch and streaming.
    """
    return (
        df.withColumn("_hour", F.hour(F.col(ts_col)))
        .withColumn("month", F.month(F.col(ts_col)))
        .withColumn("time_interval_num", (F.col("_hour") / _HOURS_PER_INTERVAL).cast("int"))
        .withColumn(
            "time_sin", F.sin(2 * math.pi * F.col("time_interval_num") / _INTERVALS_PER_DAY)
        )
        .withColumn(
            "time_cos", F.cos(2 * math.pi * F.col("time_interval_num") / _INTERVALS_PER_DAY)
        )
        .withColumn("month_sin", F.sin(2 * math.pi * F.col("month") / 12))
        .withColumn("month_cos", F.cos(2 * math.pi * F.col("month") / 12))
        .drop("_hour")
    )


# Cyclical wind-direction encoding. This runs as a pipeline stage AFTER the
# Imputer (see build_preprocessing_stages) so it reads imputed wind_direction
# values rather than nulls — encoding it eagerly in engineer_features would
# bake nulls into wind_dir_sin/cos before imputation had a chance to run.
_WIND_ENCODING_SQL = (
    "SELECT *, "
    "SIN(2*PI()*wind_direction/360) AS wind_dir_sin, "
    "COS(2*PI()*wind_direction/360) AS wind_dir_cos "
    "FROM __THIS__"
)


def add_building_features(df: DataFrame) -> DataFrame:
    """Building-derived features: log floor area and construction decade."""
    return df.withColumn("log_square_feet", F.log1p(F.col("square_feet"))).withColumn(
        "decade", (F.col("year_built") / 10).cast("int") * 10
    )


def engineer_features(df: DataFrame, ts_col: str) -> DataFrame:
    """Apply the complete derived-feature set used by training and serving.

    Note: imputation, string indexing, one-hot encoding and vector assembly are
    NOT done here — they are stages of the persisted ML pipeline so that the
    statistics learned at training time (means, category indices) are reused
    verbatim at serving time. This function only computes deterministic,
    row-local transforms that do not depend on imputed columns. Wind-direction
    cyclical encoding is deferred to a pipeline stage so it runs after
    imputation (see :func:`build_preprocessing_stages`).
    """
    df = add_time_features(df, ts_col)
    df = add_building_features(df)
    return df


def build_preprocessing_stages() -> list:
    """ML pipeline stages that learn from training data and are persisted.

    Folding these into the saved ``PipelineModel`` is what removes the
    ``fillna(0)`` hack and the imputation data-leak from the original code:
    the Imputer is fit on training folds only, and the same means are applied
    at serving time.
    """
    stages: list = [
        Imputer(
            inputCols=WEATHER_NUMERIC_COLS,
            outputCols=WEATHER_NUMERIC_COLS,
            strategy="mean",
        ),
        # Encode wind direction only after wind_direction has been imputed above.
        SQLTransformer(statement=_WIND_ENCODING_SQL),
    ]
    for col in CATEGORICAL_COLS:
        stages.append(StringIndexer(inputCol=col, outputCol=f"{col}_idx", handleInvalid="keep"))
        stages.append(
            OneHotEncoder(inputCols=[f"{col}_idx"], outputCols=[f"{col}_ohe"], handleInvalid="keep")
        )
    stages.append(
        VectorAssembler(inputCols=ASSEMBLER_INPUTS, outputCol="features", handleInvalid="keep")
    )
    return stages


def build_estimator_pipeline(estimator) -> Pipeline:
    """Compose the shared preprocessing stages with a final estimator."""
    return Pipeline(stages=build_preprocessing_stages() + [estimator])
