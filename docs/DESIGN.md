# Design decisions

## 1. Lake zones

| Zone | Path | Contents | Mutable? |
|---|---|---|---|
| raw | `raw/yellow/year=YYYY/month=MM/` | Byte-for-byte copy of the TLC file | Never edited; only added to |
| staging | `staging/yellow_trips/run_month=YYYY-MM/` | Output of one run, awaiting audit | Deleted after publish |
| curated | `curated/yellow_trips/pickup_year=/pickup_month=/` | Clean, deduplicated, enriched trips | One partition replaced per run |
| quarantine | `quarantine/yellow_trips/quarantine_year=/quarantine_month=/` | Rejected rows plus `reject_reason` | One partition replaced per run |
| manifests | `_manifests/yellow_trips/YYYY-MM.json` | Counts, drift, checks, timings | Overwritten per run |

Keeping raw immutable means any bug in the transform can be fixed by re-running from raw.
No re-download is needed, and the data is unchanged if TLC later edits a file.

## 2. Write-Audit-Publish instead of "validate then write"

Validation runs on data that has **already been written** to staging. This has three benefits:

1. The checks see exactly the bytes consumers would read, after Spark's writer, types, and dedup.
2. Readers of `curated/` never see a half-written or failing month.
3. A failed audit leaves staging in place, so the bad batch can be inspected.

`audit` has `retries=0` in Airflow. A data-quality failure means the data is wrong, and retrying
won't fix it; a person has to look at the manifest.

## 3. Idempotency and backfills

- `ingest_raw` skips a file that has already landed (`--force-download` overrides this).
- Local writes use write-to-temp then `os.replace`, so a crash never leaves a partial file under the final name.
- `publish` uses `spark.sql.sources.partitionOverwriteMode=dynamic`. Writing January only replaces
  `pickup_year=2024/pickup_month=1`; other months are untouched. The test
  `test_rerun_is_idempotent_and_months_are_isolated` covers this.
- The month's quarantine partition is cleared before writing, because dynamic overwrite writes nothing
  for an empty DataFrame and would leave stale rejects behind.
- dbt marts are `incremental` with `delete+insert` keyed on `(pickup_year, pickup_month)`, and Airflow passes
  `run_month`, so a backfill rebuilds only that month.

## 4. Partitioning: why `pickup_year/pickup_month` and not day

About 3M rows a month is roughly 50 to 70 MB of zstd Parquet. Partitioning by day would create about
30 tiny files a month (the small-file problem: slow listing on S3, high per-file overhead). Each month
is written as 4 files, sorted by `pickup_ts` within each file. That keeps Parquet row-group min/max
statistics tight, so a filter on a time range skips most row groups even inside a partition.

## 5. Reject rules (10) and why each threshold

| Rule | Threshold | Reason |
|---|---|---|
| `pickup_outside_month` | `[month start, next month start)` | TLC files contain stray rows dated years away (e.g. 2002, 2009). Boundaries are parsed in the session time zone (UTC) so a laptop in EST does not shift them. |
| `dropoff_not_after_pickup` | dropoff > pickup | Clock or meter errors |
| `duration_too_short` | < 60 s | Mostly cancelled or test trips |
| `duration_too_long` | > 6 h | Meter left running |
| `invalid_distance` | 0 < d ≤ 200 mi | 0 mi is no trip; > 200 mi is GPS or meter corruption |
| `implausible_speed` | > 100 mph average | Catches distance/time pairs that each pass alone |
| `negative_or_null_amount` | fare, total ≥ 0 | Negative rows are refunds or reversals, a different event type |
| `invalid_location` | IDs 1 to 265 | Must join to the zone lookup |
| `invalid_passenger_count` | 0 to 9 when present | Null is allowed; some vendors don't report it |
| `null_timestamp` | — | Unusable without times |

Rules are applied in order and the first failing rule wins, so the counts per reason add up exactly to the rejected total.

## 6. Spark details worth knowing

- **Broadcast join** for the 265-row zone lookup: no shuffle of the 3M-row side.
- **One aggregation pass** for all 22 quality metrics instead of 22 separate actions, which would
  mean 22 scans of the data.
- **AQE on** (`spark.sql.adaptive.enabled`) so shuffle partitions coalesce automatically.
- **Session time zone = UTC.** TLC timestamps are naive local wall-clock times. Keeping Spark in UTC and
  never converting preserves the wall-clock value end to end.
- `hadoop-aws` is pinned to the exact Hadoop version bundled with PySpark (detected at runtime) and pre-fetched in
  the Docker image. A mismatch here is the most common cause of `ClassNotFoundException` with S3A.

## 7. Why DuckDB for dbt (and not Spark SQL / Snowflake)

The marts are small: thousands of rows per month after aggregation. DuckDB reads the curated Parquet
directly from S3 (including `hive_partitioning` pruning), costs nothing to run, and runs in CI. The dbt
SQL is portable. Moving to Snowflake, BigQuery, or Databricks means changing the adapter and the
`read_parquet` source, not the models.

## 8. What I would change at 100x scale

- Swap in-process Spark for **EMR Serverless / Databricks / Spark on Kubernetes** via an Airflow operator.
  The `stage/audit/publish` functions stay the same.
- Replace plain Parquet with an **open table format (Iceberg / Delta)**. That gives atomic commits,
  time travel, and native WAP branches, and removes the custom staging directory.
- Store run manifests in a metadata table and alert on trends (for example, reject rate up 3σ month over month).
- Add data contracts on the raw schema, with alerts when TLC adds columns.
