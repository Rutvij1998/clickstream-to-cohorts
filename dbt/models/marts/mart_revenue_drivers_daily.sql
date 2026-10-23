select
    kpis.activity_date,
    kpis.sessions,
    kpis.purchasing_sessions,
    kpis.cvr,
    kpis.aov,
    kpis.revenue,
    lag(kpis.sessions) over (
        order by kpis.activity_date
    ) as sessions_previous_observed_day,
    lag(kpis.cvr) over (
        order by kpis.activity_date
    ) as cvr_previous_observed_day,
    lag(kpis.aov) over (
        order by kpis.activity_date
    ) as aov_previous_observed_day,
    lag(kpis.revenue) over (
        order by kpis.activity_date
    ) as revenue_previous_observed_day
from {{ ref('mart_daily_kpis') }} as kpis
