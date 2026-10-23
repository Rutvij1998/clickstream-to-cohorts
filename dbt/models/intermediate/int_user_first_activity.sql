select
    events.user_id,
    min(events.event_time) as first_seen_at,
    cast(min(events.event_time) as date) as first_seen_date,
    max(events.event_time) as last_seen_at,
    min(
        case when events.event_type = 'purchase' then events.event_time end
    ) as first_purchase_at
from {{ ref('fct_events') }} as events
group by events.user_id
