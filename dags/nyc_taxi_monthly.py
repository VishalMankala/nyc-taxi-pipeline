"""Monthly NYC yellow taxi pipeline (Airflow 3, TaskFlow API).

    resolve_month -> ingest_zones -> ingest_raw -> stage -> audit -> publish -> dbt_build

* Write-Audit-Publish: `audit` runs 20+ data-quality checks on staged data; a failed
  check stops the run before anything reaches the curated table.
* Idempotent: every task can be retried or re-run for any month (backfills) and the
  lake ends up in the same state.
* TLC publishes each month about two months late, so a run on 2024-05-03 processes
  2024-03 (`lag_months`). Pass `month` to process a specific month on demand.

Backfill a year:  airflow backfill create --dag-id nyc_taxi_monthly \
                      --from-date 2024-03-01 --to-date 2025-02-28
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, task

DBT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt/taxi")


@dag(
    dag_id="nyc_taxi_monthly",
    description="TLC yellow taxi: S3 raw -> PySpark -> quality gate -> partitioned Parquet -> dbt",
    schedule="0 6 3 * *",  # 06:00 UTC on the 3rd of each month
    start_date=pendulum.datetime(2024, 3, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # Spark runs locally inside the worker; one month at a time
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "month": Param(None, type=["null", "string"], pattern=r"^\d{4}-\d{2}$",
                       description="Override: process this YYYY-MM instead of the scheduled one"),
        "lag_months": Param(2, type="integer", minimum=0, maximum=12),
    },
    tags=["taxi", "spark", "dbt", "lakehouse"],
    doc_md=__doc__,
)
def nyc_taxi_monthly():
    @task
    def resolve_month(params=None, data_interval_start=None) -> str:
        from taxi_pipeline.config import Month

        if params and params.get("month"):
            return str(Month.parse(params["month"]))
        return str(Month.from_date(data_interval_start).shift(-int(params["lag_months"])))

    @task
    def ingest_zones() -> dict:
        from taxi_pipeline.ingest import ingest_zone_lookup

        return ingest_zone_lookup()

    # A month that is not published yet fails here; retry every 12h for ~3 days.
    @task(retries=6, retry_delay=timedelta(hours=12))
    def ingest_raw(month: str) -> dict:
        from taxi_pipeline.config import Month
        from taxi_pipeline.ingest import ingest_month

        return ingest_month(Month.parse(month))

    @task
    def stage(month: str) -> dict:
        from taxi_pipeline.config import Month
        from taxi_pipeline.pipeline import stage as run_stage

        return run_stage(Month.parse(month))

    @task(retries=0)  # a data-quality failure needs a human, not a retry
    def audit(month: str) -> dict:
        from taxi_pipeline.config import Month
        from taxi_pipeline.pipeline import audit as run_audit

        manifest = run_audit(Month.parse(month))
        return {"month": month, "checks": manifest["quality"]["n_checks"],
                "warnings": manifest["quality"]["n_failed_warn"]}

    @task
    def publish(month: str) -> dict:
        from taxi_pipeline.config import Month
        from taxi_pipeline.pipeline import publish as run_publish

        manifest = run_publish(Month.parse(month))
        return {"month": month, "rows": manifest["staged_rows"]}

    @task.bash(cwd=DBT_DIR)
    def dbt_build(month: str) -> str:
        # Incremental marts rebuild only this month (delete+insert).
        return f"dbt build --profiles-dir . --vars '{{run_month: {month}}}'"

    month = resolve_month()
    raw = ingest_raw(month)
    ingest_zones() >> raw
    staged = stage(month)
    raw >> staged
    audited = audit(month)
    staged >> audited
    published = publish(month)
    audited >> published
    published >> dbt_build(month)


nyc_taxi_monthly()
