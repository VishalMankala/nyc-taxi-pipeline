"""DAG integrity tests (run when Airflow is installed, e.g. in the Airflow CI job)."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    try:  # Airflow >= 3.2
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # Airflow 3.0 / 3.1
        from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert "nyc_taxi_monthly" in dagbag.dags


def test_task_order(dagbag):
    dag = dagbag.dags["nyc_taxi_monthly"]
    order = ["resolve_month", "ingest_raw", "stage", "audit", "publish", "dbt_build"]
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids
    assert "ingest_raw" in dag.get_task("ingest_zones").downstream_task_ids


def test_audit_is_not_retried(dagbag):
    dag = dagbag.dags["nyc_taxi_monthly"]
    assert dag.get_task("audit").retries == 0
    assert dag.max_active_runs == 1
