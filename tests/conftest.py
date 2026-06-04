"""Shared pytest fixtures.

Spark-dependent tests are skipped automatically when PySpark/Java are not
available (e.g. a bare local checkout), and run in CI where they are present.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
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
