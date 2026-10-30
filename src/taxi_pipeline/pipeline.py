"""Write-Audit-Publish (WAP) orchestration for one month.

    stage   : raw -> transform -> write to an isolated staging location (+ quarantine)
    audit   : run the data-quality gate against staging; block on any "error" check
    publish : overwrite ONLY that month's partition in the curated table, then clean up

Consumers never see a month that failed its checks, and re-running a month
(backfill, bug fix) replaces exactly one partition, never duplicates it.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from taxi_pipeline.config import Month, Settings
from taxi_pipeline.ingest import ingest_month, ingest_zone_lookup
from taxi_pipeline.quality import DataQualityError, run_checks
from taxi_pipeline.spark_session import get_spark
from taxi_pipeline.storage import Storage
from taxi_pipeline.transform import transform

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _staging_key(month: Month) -> str:
    return f"staging/yellow_trips/run_month={month}"


def _stats_key(month: Month) -> str:
    return f"_manifests/yellow_trips/_staging/{month}.json"


def _write_json(storage: Storage, key: str, payload: dict) -> None:
    storage.put_bytes(json.dumps(payload, indent=2, default=str).encode(), key)


def _read_json(storage: Storage, key: str) -> dict:
    return json.loads(storage.get_bytes(key))


def stage(month: Month, settings: Settings | None = None, spark: SparkSession | None = None) -> dict:
    settings = settings or Settings()
    spark = spark or get_spark(settings)
    storage = Storage(settings)
    started = time.perf_counter()

    raw_path = settings.spark_path(settings.raw_key(month))
    raw = spark.read.parquet(raw_path)
    zones = spark.read.option("header", True).csv(settings.spark_path(settings.zone_lookup_key()))

    valid, rejected, schema_report = transform(raw, zones, month, source_file=raw_path)

    valid.write.mode("overwrite").parquet(settings.staging_path(month))

    # Clear this month's quarantine first: dynamic overwrite writes nothing for an
    # empty DataFrame, which would leave stale rejects from an earlier run behind.
    q_key = f"quarantine/yellow_trips/quarantine_year={month.year}/quarantine_month={month.month}"
    storage.delete_prefix(q_key)
    (
        rejected.write.mode("overwrite")
        .partitionBy("quarantine_year", "quarantine_month")
        .parquet(settings.quarantine_path())
    )

    raw_rows = raw.count()
    staged_rows = spark.read.parquet(settings.staging_path(month)).count()
    try:
        quarantine = spark.read.parquet(settings.spark_path(q_key))
        reasons = {
            r["reject_reason"]: r["count"]
            for r in quarantine.groupBy("reject_reason").count().collect()
        }
    except AnalysisException:  # no rejects this month -> partition folder never created
        reasons = {}
    rejected_rows = sum(reasons.values())

    stats = {
        "month": str(month),
        "raw_rows": raw_rows,
        "rejected_rows": rejected_rows,
        "staged_rows": staged_rows,
        "duplicates_removed": raw_rows - rejected_rows - staged_rows,
        "reject_reasons": dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
        "schema_drift": schema_report.as_dict(),
        "stage_seconds": round(time.perf_counter() - started, 2),
        "staged_at": _now(),
    }
    _write_json(storage, _stats_key(month), stats)
    log.info("staged %s: %s", month, stats)
    return stats


def audit(month: Month, settings: Settings | None = None, spark: SparkSession | None = None) -> dict:
    settings = settings or Settings()
    spark = spark or get_spark(settings)
    storage = Storage(settings)
    stats = _read_json(storage, _stats_key(month))

    staged = spark.read.parquet(settings.staging_path(month))
    report = run_checks(
        staged,
        month,
        raw_rows=stats["raw_rows"],
        rejected_rows=stats["rejected_rows"],
        max_reject_rate=settings.max_reject_rate,
        min_rows=settings.min_rows,
    )
    log.info("\n%s", report.summary())

    manifest = {**stats, "status": "audited" if report.passed else "failed_quality",
                "quality": report.as_dict(), "audited_at": _now()}
    _write_json(storage, settings.manifest_key(month), manifest)
    if not report.passed:
        raise DataQualityError(report)
    return manifest


def publish(month: Month, settings: Settings | None = None, spark: SparkSession | None = None) -> dict:
    settings = settings or Settings()
    spark = spark or get_spark(settings)
    storage = Storage(settings)

    manifest = _read_json(storage, settings.manifest_key(month))
    if manifest.get("status") not in ("audited", "published"):
        raise RuntimeError(f"Refusing to publish {month}: status is {manifest.get('status')!r}")

    staged = spark.read.parquet(settings.staging_path(month))
    (
        staged.repartition(4)  # ~3M rows/month -> a few well-sized files per partition
        .sortWithinPartitions("pickup_ts")  # tighter min/max stats -> better row-group skipping
        .write.mode("overwrite")  # dynamic partition overwrite: only this month is replaced
        .partitionBy("pickup_year", "pickup_month")
        .parquet(settings.curated_path())
    )
    storage.delete_prefix(_staging_key(month))

    manifest.update(status="published", published_at=_now())
    _write_json(storage, settings.manifest_key(month), manifest)
    log.info("published %s (%s rows)", month, f"{manifest['staged_rows']:,}")
    return manifest


def run_month(month: Month, settings: Settings | None = None, force_download: bool = False) -> dict:
    """End-to-end run without Airflow (used by the CLI and the e2e CI job)."""
    settings = settings or Settings()
    ingest_zone_lookup(settings)
    ingest_month(month, settings, force=force_download)
    spark = get_spark(settings)
    started = time.perf_counter()
    stage(month, settings, spark)
    audit(month, settings, spark)
    manifest = publish(month, settings, spark)
    manifest["total_seconds"] = round(time.perf_counter() - started, 2)
    _write_json(Storage(settings), settings.manifest_key(month), manifest)
    return manifest
