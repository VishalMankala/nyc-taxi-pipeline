{{ config(
    materialized = "incremental",
    incremental_strategy = "delete+insert",
    unique_key = ["pickup_year", "pickup_month"],
) }}

-- One row per day and pickup borough. Feeds the ops dashboard.
select
    pickup_date,
    pickup_year,
    pickup_month,
    coalesce(pickup_borough, 'Unknown')                         as pickup_borough,
    count(*)                                                    as trips,
    round(sum(total_amount), 2)                                 as revenue,
    round(avg(fare_amount), 2)                                  as avg_fare,
    round(avg(trip_distance), 2)                                as avg_distance_mi,
    round(avg(trip_duration_min), 2)                            as avg_duration_min,
    round(avg(tip_pct), 2)                                      as avg_card_tip_pct,
    round(avg(case when is_airport_trip then 1 else 0 end), 4)  as airport_trip_share
from {{ ref("stg_yellow_trips") }}
{{ run_month_filter() }}
group by all
