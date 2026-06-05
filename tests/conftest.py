"""Shared pytest fixtures.

Only tests that request the ``spark`` fixture are skipped when PySpark/Java are
not available (e.g. a bare local checkout); Spark-free tests (such as the
dashboard exporter's) still run. In CI, where PySpark is present, everything
runs.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("energy-pipeline-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
