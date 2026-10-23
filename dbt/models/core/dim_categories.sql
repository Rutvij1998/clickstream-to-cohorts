select
    events.category_id,
    events.category_code,
    events.category_l1,
    events.category_l2,
    events.category_l3
from {{ ref('fct_events') }} as events
qualify row_number() over (
    partition by events.category_id
    order by events.event_time desc, events.event_id desc
) = 1
