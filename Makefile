.PHONY: install test lint demo real dbt benchmark up down logs clean

MONTH ?= 2024-01
export LAKE_URI ?= $(CURDIR)/lake
export DBT_LAKE_PATH ?= $(CURDIR)/lake
export DBT_DUCKDB_PATH ?= $(CURDIR)/warehouse/taxi.duckdb

install:            ## Python deps for local development
	pip install -e ".[dev]"

lint:
	ruff check src tests dags

test:               ## Unit + end-to-end tests on synthetic data
	pytest

demo:               ## Offline demo: synthetic month -> lake -> dbt marts
	taxi-pipeline seed-sample --month $(MONTH) --rows 200000
	taxi-pipeline run --month $(MONTH) --skip-ingest
	$(MAKE) dbt

real:               ## Real TLC data for one month (downloads ~50 MB)
	taxi-pipeline run --month $(MONTH)
	$(MAKE) dbt

dbt:
	mkdir -p warehouse && cd dbt/taxi && dbt build --profiles-dir .

benchmark:          ## CSV vs Parquet on the raw month
	taxi-pipeline benchmark --parquet lake/raw/yellow/year=$(word 1,$(subst -, ,$(MONTH)))/month=$(word 2,$(subst -, ,$(MONTH)))/yellow_tripdata_$(MONTH).parquet

up:                 ## Airflow + MinIO + Postgres in Docker
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f airflow

clean:
	rm -rf lake warehouse benchmarks spark-warehouse dbt/taxi/target
