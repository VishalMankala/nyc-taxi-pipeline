"""SparkSession factory with S3 / MinIO support."""

from __future__ import annotations

import os
import re
from pathlib import Path

from pyspark.sql import SparkSession

from taxi_pipeline.config import Settings


def _bundled_hadoop_version() -> str:
    """hadoop-aws must match the Hadoop jars bundled with PySpark exactly."""
    import pyspark

    jars = Path(pyspark.__file__).parent / "jars"
    for jar in jars.glob("hadoop-client-api-*.jar"):
        match = re.search(r"hadoop-client-api-(.+)\.jar", jar.name)
        if match:
            return match.group(1)
    raise RuntimeError("Could not detect the Hadoop version bundled with PySpark")


def get_spark(settings: Settings | None = None, app_name: str = "nyc-taxi-pipeline") -> SparkSession:
    settings = settings or Settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        # Re-running a month replaces only that month's partitions (idempotent backfills).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if settings.is_s3:
        hadoop = _bundled_hadoop_version()
        builder = (
            builder.config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{hadoop}")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config(
                "spark.hadoop.fs.s3a.endpoint.region",
                os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")),
            )
        )
        if settings.s3_endpoint_url:  # MinIO / LocalStack
            builder = (
                builder.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_url)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config(
                    "spark.hadoop.fs.s3a.connection.ssl.enabled",
                    str(settings.s3_endpoint_url.startswith("https")).lower(),
                )
            )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
