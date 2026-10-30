-- Average trips per (pickup zone, ISO weekday, hour) across all loaded data.
-- "Where should drivers be at 6pm on a Friday?" Rebuilt in full (it is small).
with per_day as (
    select pickup_zone, pickup_borough, pickup_date, pickup_dow, pickup_hour, count(*) as trips
    from {{ ref("stg_yellow_trips") }}
    where pickup_zone is not null
    group by all
),

days_per_weekday as (
    select pickup_dow, count(distinct pickup_date) as n_days
    from {{ ref("stg_yellow_trips") }}
    group by all
)

select
    p.pickup_zone,
    p.pickup_borough,
    p.pickup_dow,
    p.pickup_hour,
    sum(p.trips)                                  as total_trips,
    round(sum(p.trips) / max(d.n_days), 2)        as avg_trips_per_day
from per_day p
join days_per_weekday d using (pickup_dow)
group by all
