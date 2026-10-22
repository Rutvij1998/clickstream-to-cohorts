with daily as (
    select
        sessions.session_start_date as activity_date,
        count(*) as sessions,
        sum(sessions.is_purchase) as purchasing_sessions,
        sum(sessions.revenue) as revenue
    from {{ ref('fct_sessions') }} as sessions
    group by sessions.session_start_date
)

select
    daily.activity_date,
    daily.sessions,
    daily.purchasing_sessions,
    daily.revenue,
    daily.purchasing_sessions * 1.0 / daily.sessions as cvr,
    coalesce(daily.revenue * 1.0 / nullif(daily.purchasing_sessions, 0), 0) as aov
from daily
