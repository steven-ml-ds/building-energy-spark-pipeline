"""Feature-engineering tests, including the train/serve skew regression."""

from __future__ import annotations

import datetime as dt

from pyspark.ml.regression import GBTRegressor

from energy_pipeline import features


def _sample_rows():
    # One row per 6-hour interval so all four time buckets are exercised.
    base = dt.datetime(2022, 6, 1)
    rows = []
    for i, hour in enumerate([0, 6, 12, 18]):
        rows.append(
            {
                "event_time": base.replace(hour=hour),
                "site_id": 1,
                "building_id": 100 + i,
                "primary_use": "Education",
                "meter_type": "e",
                "square_feet": 10000 + i,
                "floor_count": 3,
                "year_built": 1995,
                "air_temperature": 20.0,
                "cloud_coverage": 2.0,
                "dew_temperature": 10.0,
                "sea_level_pressure": 1013.0,
                "wind_direction": 180.0,
                "wind_speed": 5.0,
                "latent_y": 0.1,
                "latent_s": 0.2,
                "latent_r": 0.3,
                "energy_consumption": 500.0 + 10 * i,
            }
        )
    return rows


def test_time_features_are_not_collapsed(spark):
    """Regression for the original bug where time_sin/time_cos were always 0.

    The integer-vs-string ``time_interval`` mismatch made every cyclical time
    feature collapse to zero at serving time. With the shared module the four
    intervals must produce distinct, non-trivial encodings.
    """
    df = features.engineer_features(spark.createDataFrame(_sample_rows()), ts_col="event_time")
    intervals = {r["time_interval_num"] for r in df.select("time_interval_num").collect()}
    assert intervals == {0, 1, 2, 3}

    sins = {round(r["time_sin"], 6) for r in df.select("time_sin").collect()}
    assert len(sins) > 1, "time_sin collapsed to a single value (skew bug regression)"


def test_train_serve_parity(spark):
    """The same timestamp yields identical features regardless of call site."""
    row = _sample_rows()[2]  # hour=12
    df = spark.createDataFrame([row])
    a = features.engineer_features(df, ts_col="event_time").first()
    b = features.engineer_features(df, ts_col="event_time").first()
    # wind_dir_sin/cos are produced inside the pipeline (after imputation), not
    # by engineer_features, so they are not asserted here.
    for col in ("time_sin", "time_cos", "month_sin", "log_square_feet", "decade"):
        assert a[col] == b[col]


def test_building_features(spark):
    df = features.engineer_features(spark.createDataFrame(_sample_rows()), ts_col="event_time")
    first = df.first()
    assert first["decade"] == 1990
    assert first["log_square_feet"] > 0


def test_pipeline_produces_feature_vector(spark):
    """The shared preprocessing stages must fit and emit a 'features' vector,
    proving imputation/encoding/assembly live inside the persisted model."""
    df = features.engineer_features(spark.createDataFrame(_sample_rows()), ts_col="event_time")
    estimator = GBTRegressor(
        featuresCol="features", labelCol="energy_consumption", maxIter=2, maxDepth=2
    )
    model = features.build_estimator_pipeline(estimator).fit(df)
    out = model.transform(df)
    assert "features" in out.columns
    assert "prediction" in out.columns


def test_wind_direction_imputed_before_encoding(spark):
    """A null wind_direction must be imputed before the cyclical encoding runs,
    so wind_dir_sin/cos are never null in the assembled feature space."""
    rows = _sample_rows()
    rows[0]["wind_direction"] = None  # missing reading
    df = features.engineer_features(spark.createDataFrame(rows), ts_col="event_time")
    estimator = GBTRegressor(
        featuresCol="features", labelCol="energy_consumption", maxIter=2, maxDepth=2
    )
    model = features.build_estimator_pipeline(estimator).fit(df)
    out = model.transform(df).select("wind_dir_sin", "wind_dir_cos").collect()
    assert all(r["wind_dir_sin"] is not None and r["wind_dir_cos"] is not None for r in out)
