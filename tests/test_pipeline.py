"""End-to-end Write-Audit-Publish on a local lake with synthetic data."""

import dataclasses
import json

import pytest

from taxi_pipeline import pipeline
from taxi_pipeline.config import Month
from taxi_pipeline.quality import DataQualityError
from taxi_pipeline.sample_data import DEFECTS, seed_lake

N, DUPES, PER_KIND = 5_000, 10, 10


def _manifest(settings, month):
    path = f"{settings.lake_root}/{settings.manifest_key(month)}"
    with open(path) as fh:
        return json.load(fh)


def test_stage_audit_publish(spark, settings, month):
    seed_lake(month, settings, n_rows=N)

    stats = pipeline.stage(month, settings, spark)
    assert stats["raw_rows"] == N + DUPES
    assert stats["rejected_rows"] == len(DEFECTS) * PER_KIND
    assert stats["duplicates_removed"] == DUPES
    assert stats["staged_rows"] == N - len(DEFECTS) * PER_KIND
    assert set(stats["reject_reasons"]) == set(DEFECTS)

    pipeline.audit(month, settings, spark)
    manifest = pipeline.publish(month, settings, spark)
    assert manifest["status"] == "published"
    assert _manifest(settings, month)["quality"]["passed"] is True

    curated = spark.read.parquet(settings.curated_path())
    assert curated.count() == stats["staged_rows"]
    assert {(r.pickup_year, r.pickup_month) for r in curated.select(
        "pickup_year", "pickup_month").distinct().collect()} == {(2024, 1)}


def test_rerun_is_idempotent_and_months_are_isolated(spark, settings, month):
    feb = Month(2024, 2)
    for m in (month, feb):
        seed_lake(m, settings, n_rows=N)
        pipeline.stage(m, settings, spark)
        pipeline.audit(m, settings, spark)
        pipeline.publish(m, settings, spark)
    before = spark.read.parquet(settings.curated_path()).groupBy("pickup_month").count().collect()

    # Re-run January (a backfill): only January's partition is replaced.
    pipeline.stage(month, settings, spark)
    pipeline.audit(month, settings, spark)
    pipeline.publish(month, settings, spark)
    after = spark.read.parquet(settings.curated_path()).groupBy("pickup_month").count().collect()

    assert sorted(map(tuple, before)) == sorted(map(tuple, after))
    q = spark.read.parquet(settings.quarantine_path())
    assert q.filter("quarantine_month = 1").count() == len(DEFECTS) * PER_KIND


def test_failed_audit_blocks_publish(spark, settings, month):
    seed_lake(month, settings, n_rows=N)
    pipeline.stage(month, settings, spark)
    strict = dataclasses.replace(settings, max_reject_rate=0.001)

    with pytest.raises(DataQualityError) as err:
        pipeline.audit(month, strict, spark)
    assert "reject_rate" in str(err.value)
    assert _manifest(settings, month)["status"] == "failed_quality"

    with pytest.raises(RuntimeError, match="Refusing to publish"):
        pipeline.publish(month, settings, spark)
