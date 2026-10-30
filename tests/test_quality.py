from helpers import to_spark, zones_df
from pyspark.sql import functions as F

from taxi_pipeline.quality import ERROR, run_checks
from taxi_pipeline.sample_data import generate_month
from taxi_pipeline.transform import transform


def _curated(spark, tmp_path, month, n=2_000):
    pdf = generate_month(month, n_rows=n, defects_per_kind=5, duplicates=5)
    raw = to_spark(spark, pdf, tmp_path)
    valid, rejected, _ = transform(raw, zones_df(spark), month, "src")
    return valid.cache(), raw.count(), rejected.count()


def _by_name(report):
    return {c.name: c for c in report.checks}


def test_clean_month_passes_all_error_checks(spark, tmp_path, month):
    valid, raw_rows, rejected_rows = _curated(spark, tmp_path, month)
    report = run_checks(valid, month, raw_rows, rejected_rows)
    assert report.passed, report.summary()
    assert len(report.checks) >= 20
    assert _by_name(report)["reject_rate"].observed == round(rejected_rows / raw_rows, 5)


def test_duplicates_and_nulls_fail_the_gate(spark, tmp_path, month):
    valid, raw_rows, rejected_rows = _curated(spark, tmp_path, month)
    broken = valid.unionByName(valid.limit(3)).withColumn(
        "total_amount", F.when(F.rand(1) < 0.01, None).otherwise(F.col("total_amount"))
    )
    report = run_checks(broken, month, raw_rows, rejected_rows)
    checks = _by_name(report)
    assert not report.passed
    assert not checks["trip_id_unique"].passed
    assert checks["trip_id_unique"].observed == 3
    assert not checks["not_null_total_amount"].passed


def test_high_reject_rate_fails(spark, tmp_path, month):
    valid, raw_rows, _ = _curated(spark, tmp_path, month)
    report = run_checks(valid, month, raw_rows, rejected_rows=raw_rows // 2, max_reject_rate=0.1)
    assert [c.name for c in report.failures if c.severity == ERROR] == ["reject_rate"]


def test_warnings_do_not_block(spark, tmp_path, month):
    valid, raw_rows, rejected_rows = _curated(spark, tmp_path, month)
    no_zones = valid.withColumn("pickup_zone", F.lit(None).cast("string"))
    report = run_checks(no_zones, month, raw_rows, rejected_rows)
    assert report.passed
    assert not _by_name(report)["pickup_zone_coverage"].passed
