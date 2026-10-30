{{ config(
    materialized = "incremental",
    incremental_strategy = "delete+insert",
    unique_key = ["pickup_year", "pickup_month"],
) }}

-- Executive KPIs per month, including medians/percentiles that cannot be
-- re-derived from the daily table.
select
    pickup_year,
    pickup_month,
    count(*)                                                        as trips,
    round(sum(total_amount), 2)                                     as revenue,
    round(sum(total_amount) / count(*), 2)                          as revenue_per_trip,
    round(sum(total_amount) / nullif(sum(trip_distance), 0), 2)     as revenue_per_mile,
    round(quantile_cont(fare_amount, 0.5), 2)                       as median_fare,
    round(quantile_cont(trip_duration_min, 0.9), 2)                 as p90_duration_min,
    round(avg(case when payment_type = 1 then 1 else 0 end), 4)     as card_payment_share,
    round(avg(case when is_airport_trip then 1 else 0 end), 4)      as airport_trip_share
from {{ ref("stg_yellow_trips") }}
{{ run_month_filter() }}
group by all
