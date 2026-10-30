"""Runtime configuration, read from environment variables.

The same code runs against a local folder (tests, laptop) or an S3 bucket: AWS, or a local
S3-compatible endpoint in Docker Compose. Only LAKE_URI changes.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass(frozen=True)
class Month:
    """A calendar month such as 2024-01. The unit of work for every pipeline run."""

    year: int
    month: int

    @classmethod
    def parse(cls, value: str) -> Month:
        match = re.fullmatch(r"(\d{4})-(\d{2})", value.strip())
        if not match:
            raise ValueError(f"Month must look like YYYY-MM, got {value!r}")
        year, month = int(match.group(1)), int(match.group(2))
        if not 1 <= month <= 12:
            raise ValueError(f"Month out of range: {value!r}")
        return cls(year, month)

    @classmethod
    def from_date(cls, d: date | datetime) -> Month:
        return cls(d.year, d.month)

    def shift(self, months: int) -> Month:
        index = self.year * 12 + (self.month - 1) + months
        return Month(index // 12, index % 12 + 1)

    @property
    def start(self) -> datetime:
        return datetime(self.year, self.month, 1)

    @property
    def end(self) -> datetime:
        """Exclusive upper bound: first instant of the next month."""
        return self.shift(1).start

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


@dataclass(frozen=True)
class Settings:
    lake_uri: str = field(default_factory=lambda: os.getenv("LAKE_URI", "./lake"))
    tlc_base_url: str = field(
        default_factory=lambda: os.getenv(
            "TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data"
        )
    )
    zone_lookup_url: str = field(
        default_factory=lambda: os.getenv(
            "ZONE_LOOKUP_URL", "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        )
    )
    s3_endpoint_url: str | None = field(default_factory=lambda: os.getenv("S3_ENDPOINT_URL"))
    max_reject_rate: float = field(
        default_factory=lambda: float(os.getenv("MAX_REJECT_RATE", "0.10"))
    )
    min_rows: int = field(default_factory=lambda: int(os.getenv("MIN_ROWS", "1")))
    spark_master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))
    spark_driver_memory: str = field(
        default_factory=lambda: os.getenv("SPARK_DRIVER_MEMORY", "4g")
    )

    # ---- lake layout ---------------------------------------------------------------
    @property
    def is_s3(self) -> bool:
        return self.lake_uri.startswith(("s3://", "s3a://"))

    @property
    def lake_root(self) -> str:
        """Root for Python I/O (boto3 / local filesystem)."""
        root = self.lake_uri.rstrip("/")
        return root.replace("s3a://", "s3://", 1) if self.is_s3 else os.path.abspath(root)

    @property
    def spark_root(self) -> str:
        """Root for Spark I/O. Spark talks to S3 through the s3a:// connector."""
        return self.lake_root.replace("s3://", "s3a://", 1) if self.is_s3 else self.lake_root

    def raw_key(self, month: Month) -> str:
        return (
            f"raw/yellow/year={month.year}/month={month.month:02d}/"
            f"yellow_tripdata_{month}.parquet"
        )

    def zone_lookup_key(self) -> str:
        return "raw/reference/taxi_zone_lookup.csv"

    def manifest_key(self, month: Month) -> str:
        return f"_manifests/yellow_trips/{month}.json"

    def spark_path(self, key: str) -> str:
        return f"{self.spark_root}/{key}"

    def staging_path(self, month: Month) -> str:
        return self.spark_path(f"staging/yellow_trips/run_month={month}")

    def curated_path(self) -> str:
        return self.spark_path("curated/yellow_trips")

    def quarantine_path(self) -> str:
        return self.spark_path("quarantine/yellow_trips")
