select
    events.session_id,
    min(events.user_id) as user_id,
    min(events.event_time) as session_start_at,
    max(events.event_time) as session_end_at,
    count(*) as event_count,
    sum(case when events.event_type = 'view' then 1 else 0 end) as view_events,
    sum(case when events.event_type = 'cart' then 1 else 0 end) as cart_events,
    sum(case when events.event_type = 'remove_from_cart' then 1 else 0 end) as remove_from_cart_events,
    sum(case when events.event_type = 'purchase' then 1 else 0 end) as purchase_events,
    max(case when events.event_type = 'purchase' then 1 else 0 end) as is_purchase,
    sum(case when events.event_type = 'purchase' then events.price else 0 end) as revenue,
    cast(min(events.event_time) as date) as session_start_date
from {{ ref('fct_events') }} as events
group by events.session_id
