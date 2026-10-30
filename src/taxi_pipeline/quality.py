"""Data-quality gate for the write-audit-publish pattern.

All metrics are computed in ONE aggregation pass over the staged data, then each
check compares a metric to a threshold. "error" checks block publishing; "warn"
checks are recorded in the run manifest but let the run continue.
"""

from __future__ import annotations

import calendar
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from taxi_pipeline.config import Month
from taxi_pipeline.transform import (
    MAX_DISTANCE_MI,
    MAX_DURATION_S,
    MAX_LOCATION_ID,
    MIN_DURATION_S,
    ts_lit,
)

ERROR, WARN = "error", "warn"
NOT_NULL_COLUMNS = ("trip_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id",
                    "total_amount")
FEE_COLUMNS = ("fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
               "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee")


@dataclass
class CheckResult:
    name: str
    severity: str
    passed: bool
    observed: float
    threshold: str
    description: str


@dataclass
class QualityReport:
    month: str
    checks: list[CheckResult]

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks if c.severity == ERROR)

    @property
    def failures(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed]

    def as_dict(self) -> dict:
        return {
            "month": self.month,
            "passed": self.passed,
            "n_checks": len(self.checks),
            "n_failed_error": sum(1 for c in self.failures if c.severity == ERROR),
            "n_failed_warn": sum(1 for c in self.failures if c.severity == WARN),
            "checks": [asdict(c) for c in self.checks],
        }

    def summary(self) -> str:
        lines = [f"Data quality for {self.month}: {'PASSED' if self.passed else 'FAILED'}"]
        for c in self.checks:
            mark = "ok  " if c.passed else ("FAIL" if c.severity == ERROR else "warn")
            lines.append(f"  [{mark}] {c.name}: observed={c.observed:g} ({c.threshold})")
        return "\n".join(lines)


class DataQualityError(RuntimeError):
    def __init__(self, report: QualityReport):
        self.report = report
        failed = ", ".join(c.name for c in report.failures if c.severity == ERROR)
        super().__init__(f"Data-quality gate failed for {report.month}: {failed}")


def _count_if(cond) -> F.Column:
    return F.sum(F.when(cond, 1).otherwise(0))


def compute_metrics(df: DataFrame, month: Month) -> dict:
    duration = F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")
    fee_sum = sum((F.coalesce(F.col(c), F.lit(0.0)) for c in FEE_COLUMNS), F.lit(0.0))

    exprs = [
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("trip_id").alias("distinct_trip_ids"),
        *[_count_if(F.col(c).isNull()).alias(f"null_{c}") for c in NOT_NULL_COLUMNS],
        _count_if((F.col("pickup_ts") < ts_lit(month.start)) | (F.col("pickup_ts") >= ts_lit(month.end)))
        .alias("pickup_outside_month"),
        _count_if(F.col("dropoff_ts") <= F.col("pickup_ts")).alias("dropoff_not_after_pickup"),
        _count_if((duration < MIN_DURATION_S) | (duration > MAX_DURATION_S)).alias("bad_duration"),
        _count_if((F.col("trip_distance") <= 0) | (F.col("trip_distance") > MAX_DISTANCE_MI))
        .alias("bad_distance"),
        _count_if((F.col("fare_amount") < 0) | (F.col("total_amount") < 0)).alias("negative_amount"),
        _count_if(
            ~F.col("pu_location_id").between(1, MAX_LOCATION_ID)
            | ~F.col("do_location_id").between(1, MAX_LOCATION_ID)
        ).alias("bad_location"),
        _count_if(F.col("pickup_zone").isNull()).alias("null_pickup_zone"),
        _count_if(F.col("dropoff_zone").isNull()).alias("null_dropoff_zone"),
        _count_if(F.abs(F.col("total_amount") - fee_sum) > 0.05).alias("total_mismatch"),
        _count_if(F.col("tip_pct") > 100).alias("tip_pct_over_100"),
        _count_if(F.col("passenger_count").isNull()).alias("null_passenger_count"),
        F.countDistinct("pickup_year", "pickup_month").alias("distinct_partitions"),
        F.countDistinct("pickup_date").alias("distinct_days"),
    ]
    row = df.agg(*exprs).collect()[0].asDict()
    return {k: (v or 0) for k, v in row.items()}


def run_checks(
    df: DataFrame,
    month: Month,
    raw_rows: int,
    rejected_rows: int,
    max_reject_rate: float = 0.10,
    min_rows: int = 1,
) -> QualityReport:
    m = compute_metrics(df, month)
    rows = m["rows"]
    rate = lambda n: (n / rows) if rows else 0.0  # noqa: E731
    reject_rate = rejected_rows / raw_rows if raw_rows else 1.0
    days_in_month = calendar.monthrange(month.year, month.month)[1]

    checks: list[tuple[str, str, bool, float, str, str]] = [
        ("row_count_min", ERROR, rows >= min_rows, rows, f">= {min_rows}",
         "Curated table is not empty"),
        ("reject_rate", ERROR, reject_rate <= max_reject_rate, round(reject_rate, 5),
         f"<= {max_reject_rate}", "Share of raw rows sent to quarantine"),
        ("trip_id_unique", ERROR, m["distinct_trip_ids"] == rows, rows - m["distinct_trip_ids"],
         "== 0 duplicates", "No duplicate trips after dedup"),
    ]
    for col in NOT_NULL_COLUMNS:
        n = m[f"null_{col}"]
        checks.append((f"not_null_{col}", ERROR, n == 0, n, "== 0", f"{col} is never null"))

    checks += [
        ("pickup_within_month", ERROR, m["pickup_outside_month"] == 0, m["pickup_outside_month"],
         "== 0", "Every trip belongs to the run month"),
        ("dropoff_after_pickup", ERROR, m["dropoff_not_after_pickup"] == 0,
         m["dropoff_not_after_pickup"], "== 0", "Dropoff is strictly after pickup"),
        ("duration_in_range", ERROR, m["bad_duration"] == 0, m["bad_duration"], "== 0",
         f"{MIN_DURATION_S}s <= duration <= {MAX_DURATION_S}s"),
        ("distance_in_range", ERROR, m["bad_distance"] == 0, m["bad_distance"], "== 0",
         f"0 < distance <= {MAX_DISTANCE_MI} mi"),
        ("amounts_non_negative", ERROR, m["negative_amount"] == 0, m["negative_amount"], "== 0",
         "Fare and total are non-negative"),
        ("location_ids_valid", ERROR, m["bad_location"] == 0, m["bad_location"], "== 0",
         f"Location IDs in 1..{MAX_LOCATION_ID}"),
        ("single_partition", ERROR, m["distinct_partitions"] == (1 if rows else 0),
         m["distinct_partitions"], "== 1", "Run writes exactly one year/month partition"),
        ("pickup_zone_coverage", WARN, rate(m["null_pickup_zone"]) <= 0.01,
         round(1 - rate(m["null_pickup_zone"]), 5), ">= 0.99", "Pickup zone lookup hit rate"),
        ("dropoff_zone_coverage", WARN, rate(m["null_dropoff_zone"]) <= 0.01,
         round(1 - rate(m["null_dropoff_zone"]), 5), ">= 0.99", "Dropoff zone lookup hit rate"),
        ("total_amount_reconciles", WARN, rate(m["total_mismatch"]) <= 0.02,
         round(rate(m["total_mismatch"]), 5), "<= 0.02",
         "total_amount equals the sum of its fee components (+/- $0.05)"),
        ("tip_pct_plausible", WARN, rate(m["tip_pct_over_100"]) <= 0.01,
         round(rate(m["tip_pct_over_100"]), 5), "<= 0.01", "Card tips above 100% of fare are rare"),
        ("passenger_count_null_rate", WARN, rate(m["null_passenger_count"]) <= 0.20,
         round(rate(m["null_passenger_count"]), 5), "<= 0.20", "passenger_count mostly populated"),
        ("daily_coverage", WARN, m["distinct_days"] == days_in_month, m["distinct_days"],
         f"== {days_in_month}", "Every day of the month has trips (no missing data)"),
    ]
    return QualityReport(
        month=str(month),
        checks=[CheckResult(*c) for c in checks],
    )
