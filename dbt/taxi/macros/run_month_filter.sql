{#
  On an incremental run with run_month set (e.g. --vars '{run_month: 2024-01}'),
  only that month is re-aggregated and swapped in with delete+insert. A backfill of
  an old month therefore rewrites just that month, never the whole table.
#}
{% macro run_month_filter() %}
  {%- set m = var("run_month", "") -%}
  {%- if is_incremental() and m -%}
    where pickup_year = {{ m[:4] | int }} and pickup_month = {{ m[5:7] | int }}
  {%- endif -%}
{% endmacro %}
