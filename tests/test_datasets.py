"""Tests for batch dataset helpers (outlier handling)."""

from __future__ import annotations

from energy_pipeline import datasets


def test_outlier_bounds_exclude_extreme_value(spark):
    # A tight cluster around 10 (enough points that one outlier doesn't dominate
    # the std) plus one extreme value, which 3-sigma bounds should exclude.
    values = [(10.0,)] * 20 + [(20.0,)]
    df = spark.createDataFrame(values, ["energy_consumption"])

    lo, hi = datasets.compute_outlier_bounds(df, "energy_consumption", sigma=3.0)
    assert lo is not None and hi is not None

    filtered = datasets.filter_outliers(df, "energy_consumption", lo, hi)
    kept = {r["energy_consumption"] for r in filtered.collect()}
    assert 20.0 not in kept
    assert 10.0 in kept


def test_filter_outliers_is_noop_without_bounds(spark):
    df = spark.createDataFrame([(1.0,), (2.0,)], ["energy_consumption"])
    assert datasets.filter_outliers(df, "energy_consumption", None, None).count() == 2
