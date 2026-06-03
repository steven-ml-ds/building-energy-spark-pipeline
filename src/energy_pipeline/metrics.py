"""Regression metrics, including a Spark-native RMSLE."""

from __future__ import annotations

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def rmsle(predictions: DataFrame, label_col: str, prediction_col: str = "prediction") -> float:
    """Root Mean Squared Log Error, clamped to be non-negative before log1p."""
    log_pred = F.log1p(F.greatest(F.col(prediction_col), F.lit(0.0)))
    log_label = F.log1p(F.greatest(F.col(label_col), F.lit(0.0)))
    row = predictions.select(F.sqrt(F.mean((log_pred - log_label) ** 2)).alias("rmsle")).first()
    return float(row["rmsle"])


def evaluate(predictions: DataFrame, label_col: str) -> dict[str, float]:
    """Compute RMSE, MAE, R2 and RMSLE for a prediction frame."""
    metrics: dict[str, float] = {}
    for name in ("rmse", "mae", "r2"):
        evaluator = RegressionEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName=name
        )
        metrics[name] = float(evaluator.evaluate(predictions))
    metrics["rmsle"] = rmsle(predictions, label_col)
    return metrics
