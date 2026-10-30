FROM apache/airflow:3.1.8-python3.12

USER root
# Spark needs a JVM (17+). default-jre-headless also creates /usr/lib/jvm/default-java,
# which works on both amd64 and arm64 (Apple Silicon).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# dbt lives in its own virtualenv so its pins never fight Airflow's.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-duckdb==1.11.1" \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

USER airflow
COPY --chown=airflow:root pyproject.toml README.md /opt/pipeline/
COPY --chown=airflow:root src /opt/pipeline/src
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pyspark==4.2.1" /opt/pipeline

# Pre-fetch the S3A connector jars at build time so runs never depend on Maven Central.
RUN python -c "from taxi_pipeline.spark_session import _bundled_hadoop_version as v; \
from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').config('spark.jars.packages', \
f'org.apache.hadoop:hadoop-aws:{v()}').getOrCreate().stop()"

RUN mkdir -p /opt/airflow/warehouse
