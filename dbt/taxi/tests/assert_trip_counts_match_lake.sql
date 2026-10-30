-- Monthly trip counts in the mart must equal the rows in the curated lake.
select m.pickup_year, m.pickup_month, m.trips, s.trips as lake_trips
from {{ ref("mart_monthly_kpis") }} m
join (
    select pickup_year, pickup_month, count(*) as trips
    from {{ ref("stg_yellow_trips") }}
    group by all
) s using (pickup_year, pickup_month)
where m.trips != s.trips
