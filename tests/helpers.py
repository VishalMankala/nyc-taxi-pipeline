from pathlib import Path

import pandas as pd

from taxi_pipeline.sample_data import zone_lookup

BASE_ROW = {
    "VendorID": 2,
    "tpep_pickup_datetime": pd.Timestamp("2024-01-15 08:00:00"),
    "tpep_dropoff_datetime": pd.Timestamp("2024-01-15 08:20:00"),
    "passenger_count": 1.0,
    "trip_distance": 4.0,
    "RatecodeID": 1.0,
    "store_and_fwd_flag": "N",
    "PULocationID": 161,
    "DOLocationID": 236,
    "payment_type": 1,
    "fare_amount": 20.0,
    "extra": 1.0,
    "mta_tax": 0.5,
    "tip_amount": 4.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 1.0,
    "total_amount": 29.0,
    "congestion_surcharge": 2.5,
    "Airport_fee": 0.0,
}


def rows(*overrides: dict) -> pd.DataFrame:
    return pd.DataFrame([{**BASE_ROW, **o} for o in overrides])


def to_spark(spark, pdf: pd.DataFrame, tmp_path: Path, name: str = "raw.parquet"):
    """Round-trip through Parquet exactly like production (keeps timestamp semantics)."""
    path = tmp_path / name
    pdf.to_parquet(path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    return spark.read.parquet(str(path))


def zones_df(spark):
    return spark.createDataFrame(zone_lookup().astype({"LocationID": "str"}))
