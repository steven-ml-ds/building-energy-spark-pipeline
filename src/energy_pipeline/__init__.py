"""Building energy consumption prediction pipeline.

A production-structured PySpark project: batch ML training, a Kafka weather
producer, and Spark Structured Streaming inference, all sharing a single
feature-engineering module to guarantee train/serve parity.
"""

__version__ = "1.0.0"
