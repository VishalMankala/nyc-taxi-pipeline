-- The daily and monthly marts are built independently; their revenue must agree
-- to the cent for every month. Any returned row is a failure.
with daily as (
    select pickup_year, pickup_month, sum(revenue) as revenue
    from {{ ref("fct_daily_borough_metrics") }}
    group by all
),

monthly as (
    select pickup_year, pickup_month, revenue
    from {{ ref("mart_monthly_kpis") }}
)

select d.pickup_year, d.pickup_month, d.revenue as daily_revenue, m.revenue as monthly_revenue
from daily d
full outer join monthly m using (pickup_year, pickup_month)
where m.revenue is null
   or d.revenue is null
   or abs(d.revenue - m.revenue) > 1.0
