-- Curated trips published by the Spark pipeline (already cleaned and quality-gated).
-- hive_partitioning exposes pickup_year / pickup_month from the folder names, so a
-- filter on them only opens that month's files (partition pruning).
select *
from read_parquet(
    '{{ var("lake_path") }}/curated/yellow_trips/*/*/*.parquet',
    hive_partitioning = true,
    union_by_name = true
)
