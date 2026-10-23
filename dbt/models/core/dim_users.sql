with activity as (
    select
        events.user_id,
        count(*) as event_count,
        count(distinct events.session_id) as session_count,
        max(case when events.event_type = 'purchase' then 1 else 0 end) as has_purchased
    from {{ ref('fct_events') }} as events
    group by events.user_id
)

select
    first_activity.user_id,
    first_activity.first_seen_at,
    first_activity.first_seen_date,
    first_activity.last_seen_at,
    first_activity.first_purchase_at,
    cast(date_trunc('week', first_activity.first_seen_date) as date) as cohort_week,
    activity.event_count,
    activity.session_count,
    activity.has_purchased
from {{ ref('int_user_first_activity') }} as first_activity
inner join activity
    on first_activity.user_id = activity.user_id
