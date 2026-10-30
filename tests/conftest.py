import os

import pytest
from pyspark.sql import SparkSession

from taxi_pipeline.config import Month, Settings

os.environ.setdefault("PYSPARK_PYTHON", "python3")


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("taxi-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def month() -> Month:
    return Month(2024, 1)


@pytest.fixture
def settings(tmp_path, monkeypatch) -> Settings:
    monkeypatch.setenv("LAKE_URI", str(tmp_path / "lake"))
    monkeypatch.setenv("MIN_ROWS", "1")
    return Settings()
