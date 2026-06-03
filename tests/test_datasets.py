"""Tests for batch dataset helpers (outlier handling)."""

from __future__ import annotations

from energy_pipeline import datasets


def test_outlier_bounds_exclude_extreme_value(spark):
    # A tight cluster around 100 plus one extreme value at 100000.
    values = [(float(v),) for v in [98, 99, 100, 101, 102, 100, 99, 101, 100000]]
    df = spark.createDataFrame(values, ["energy_consumption"])

    lo, hi = datasets.compute_outlier_bounds(df, "energy_consumption", sigma=3.0)
    assert lo is not None and hi is not None

    filtered = datasets.filter_outliers(df, "energy_consumption", lo, hi)
    kept = {r["energy_consumption"] for r in filtered.collect()}
    assert 100000.0 not in kept
    assert 100.0 in kept


def test_filter_outliers_is_noop_without_bounds(spark):
    df = spark.createDataFrame([(1.0,), (2.0,)], ["energy_consumption"])
    assert datasets.filter_outliers(df, "energy_consumption", None, None).count() == 2
