"""Batch ML training entry point.

Trains a Gradient-Boosted-Tree regressor (benchmarked against Random Forest)
on historical building/meter/weather data, tunes it with cross-validation, and
persists the full preprocessing+model pipeline.

Run:
    python -m energy_pipeline.batch_train
    python -m energy_pipeline.batch_train --sample-fraction 0.05   # fast demo
"""

from __future__ import annotations

import argparse
import json
import logging

from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame

from . import datasets, features, metrics
from .config import Config, load_config
from .spark import build_spark

log = logging.getLogger("energy_pipeline.batch_train")


def _benchmark(train: DataFrame, test: DataFrame, config: Config) -> None:
    """Train RF and GBT once and log their metrics for comparison."""
    candidates = {
        "RandomForest": RandomForestRegressor(
            featuresCol="features", labelCol=config.model.label_col, maxDepth=3, numTrees=10
        ),
        "GBT": GBTRegressor(
            featuresCol="features", labelCol=config.model.label_col, maxDepth=3, maxIter=10
        ),
    }
    for name, estimator in candidates.items():
        model = features.build_estimator_pipeline(estimator).fit(train)
        scores = metrics.evaluate(model.transform(test), config.model.label_col)
        log.info("%s benchmark: %s", name, json.dumps(scores))


def train(config: Config) -> dict:
    """Run the full training workflow and persist the tuned pipeline."""
    spark = build_spark(config)
    try:
        frame = datasets.build_training_frame(spark, config)

        if config.model.sample_fraction < 1.0:
            frame = frame.sample(
                withReplacement=False,
                fraction=config.model.sample_fraction,
                seed=config.model.seed,
            )

        train_df, test_df = frame.randomSplit(
            [config.model.train_ratio, 1 - config.model.train_ratio], seed=config.model.seed
        )

        # Fit outlier bounds on the training split only (avoids test-set leakage),
        # then clean the training data. The test set stays representative.
        lo, hi = datasets.compute_outlier_bounds(
            train_df, config.model.label_col, config.model.outlier_sigma
        )
        train_df = datasets.filter_outliers(train_df, config.model.label_col, lo, hi)
        train_df.cache()

        _benchmark(train_df, test_df, config)

        gbt = GBTRegressor(featuresCol="features", labelCol=config.model.label_col)
        pipeline = features.build_estimator_pipeline(gbt)
        param_grid = (
            ParamGridBuilder()
            .addGrid(gbt.maxDepth, [2, 4, 6])
            .addGrid(gbt.maxIter, [10, 20, 50])
            .addGrid(gbt.stepSize, [0.05, 0.1, 0.2])
            .build()
        )
        from pyspark.ml.evaluation import RegressionEvaluator

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=RegressionEvaluator(
                labelCol=config.model.label_col, predictionCol="prediction", metricName="rmse"
            ),
            numFolds=config.model.cv_folds,
            seed=config.model.seed,
            parallelism=2,
        )
        cv_model = cv.fit(train_df)
        best_model = cv_model.bestModel

        scores = metrics.evaluate(best_model.transform(test_df), config.model.label_col)
        log.info("Tuned GBT test metrics: %s", json.dumps(scores))

        best_model.write().overwrite().save(config.paths.model)
        log.info("Saved tuned pipeline to %s", config.paths.model)
        return scores
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Train the energy-consumption model.")
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=None,
        help="Fraction of rows to train on (overrides MODEL_SAMPLE_FRACTION).",
    )
    args = parser.parse_args()

    config = load_config()
    if args.sample_fraction is not None:
        # Rebuild config with the CLI override applied to the model section.
        from dataclasses import replace

        config = replace(config, model=replace(config.model, sample_fraction=args.sample_fraction))
    train(config)


if __name__ == "__main__":
    main()
