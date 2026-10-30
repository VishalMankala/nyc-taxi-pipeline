-- Origin -> destination borough matrix with fare economics.
select
    coalesce(pickup_borough, 'Unknown')   as origin_borough,
    coalesce(dropoff_borough, 'Unknown')  as destination_borough,
    count(*)                              as trips,
    round(avg(total_amount), 2)           as avg_total,
    round(avg(trip_distance), 2)          as avg_distance_mi,
    round(avg(avg_speed_mph), 2)          as avg_speed_mph
from {{ ref("stg_yellow_trips") }}
group by all
